"""Reader ownership on controller restart; no venues, signals or orders."""
import asyncio
import logging
from types import SimpleNamespace

import pytest

from controllers.generic import meridian_v3_unified as module
from controllers.generic.meridian_v3_unified_core.market_context import MarketContextObserver, MarketContextPolicy
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
from hummingbot.strategy_v2.models.base import RunnableStatus


class ReaderRuntime:
    def __init__(self):
        self.pool = None
        self.close_entered = asyncio.Event()
        self.close_release = asyncio.Event()
        self.closed = []
        self.close_error = None

    def close_data(self):
        pool, self.pool = self.pool, None
        if pool is None:
            return None

        async def close():
            self.close_entered.set()
            await self.close_release.wait()
            if self.close_error is not None:
                raise self.close_error
            self.closed.append(pool)

        return asyncio.create_task(close())


class ReaderController(module.MeridianUnifiedController):
    """Exercise real lifecycle methods with a bounded reader transport fixture."""

    def __init__(self):
        ControllerBase.__init__(self, SimpleNamespace(data=SimpleNamespace(mode='shared')),
                                SimpleNamespace(ready=False), asyncio.Queue(), update_interval=0)
        self.runtime = ReaderRuntime()
        self.contexts = {'BTC-USDC': SimpleNamespace(runtime=self.runtime, config=object())}
        self.market_context = SimpleNamespace(close=lambda: None)
        self._data_close_tasks = set()
        self._data_close_error = None
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def control_task(self):
        self.entered.set()
        await self.release.wait()


@pytest.mark.asyncio
async def test_restart_waits_for_old_reader_cleanup_before_reattachment(monkeypatch):
    attached = []

    def attach(*_args):
        pool = object()
        attached.append(pool)
        return pool

    monkeypatch.setattr(module, 'attach_pool', attach)
    controller = ReaderController()
    before = set(asyncio.all_tasks())
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    controller.stop()
    await asyncio.wait_for(controller.runtime.close_entered.wait(), 1)
    controller.release.set()
    try:
        with pytest.raises(RuntimeError, match='still stopping'):
            controller.start()
        assert len(attached) == 1 and controller.runtime.pool is None
        assert not controller._control_loop_task.done()
    finally:
        controller.runtime.close_release.set()
        await asyncio.gather(*(set(asyncio.all_tasks()) - before))
    controller.entered.clear()
    controller.release.clear()
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    try:
        assert attached == [controller.runtime.closed[0], controller.runtime.pool]
        assert attached[0] is not attached[1]
        assert controller.status == RunnableStatus.RUNNING
    finally:
        controller.stop()
        controller.release.set()
        await asyncio.gather(*(set(asyncio.all_tasks()) - before))
    assert controller.runtime.closed == attached
    assert not controller._data_close_tasks


@pytest.mark.asyncio
async def test_prestart_cancel_closes_constructor_attached_reader(monkeypatch):
    def forbidden_attach(*_args):
        raise AssertionError('A cancelled generation must not attach readers')

    monkeypatch.setattr(module, 'attach_pool', forbidden_attach)
    controller = ReaderController()
    pool = controller.runtime.pool = object()
    before = set(asyncio.all_tasks())
    controller.start()
    controller._control_loop_task.cancel()
    await asyncio.gather(controller._control_loop_task, return_exceptions=True)
    await asyncio.wait_for(controller.runtime.close_entered.wait(), 1)
    try:
        assert controller.status == RunnableStatus.TERMINATED
        with pytest.raises(RuntimeError, match='readers are still stopping'):
            controller.start()
    finally:
        controller.runtime.close_release.set()
        await asyncio.gather(*(set(asyncio.all_tasks()) - before))
    assert controller.runtime.closed == [pool]
    assert not controller._data_close_tasks


@pytest.mark.asyncio
async def test_reader_cleanup_failure_is_observable_and_blocks_restart(monkeypatch, caplog):
    monkeypatch.setattr(module, 'attach_pool', lambda *_args: object())
    controller = ReaderController()
    controller.runtime.close_error = ValueError('reader cleanup rejected')
    before = set(asyncio.all_tasks())
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    controller.stop()
    controller.release.set()
    controller.runtime.close_release.set()
    await asyncio.gather(*(set(asyncio.all_tasks()) - before), return_exceptions=True)
    with pytest.raises(RuntimeError, match='cleanup failed'):
        controller.start()
    assert controller.status == RunnableStatus.TERMINATED
    assert 'reader cleanup rejected' in caplog.text
    assert not controller._data_close_tasks


@pytest.mark.asyncio
async def test_cancelled_reader_cleanup_blocks_restart(monkeypatch, caplog):
    monkeypatch.setattr(module, 'attach_pool', lambda *_args: object())
    controller = ReaderController()
    before = set(asyncio.all_tasks())
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    controller.stop()
    controller.release.set()
    await asyncio.wait_for(controller.runtime.close_entered.wait(), 1)
    [close_task] = controller._data_close_tasks
    close_task.cancel()
    await asyncio.gather(*(set(asyncio.all_tasks()) - before), return_exceptions=True)
    try:
        with pytest.raises(RuntimeError, match='cleanup failed'):
            controller.start()
        assert controller.status == RunnableStatus.TERMINATED
        assert 'cleanup was cancelled' in caplog.text
    finally:
        controller.stop()
        controller.release.set()
        controller.runtime.close_release.set()
        await asyncio.gather(*(set(asyncio.all_tasks()) - before), return_exceptions=True)


@pytest.mark.asyncio
async def test_observer_cleanup_rejection_keeps_client_for_retry():
    class Client:
        closed = False

        async def close(self):
            self.closed = True

    client = Client()
    observer = MarketContextObserver(MarketContextPolicy(), clock_ms=lambda: 0, logger=logging.getLogger(__name__))
    observer.client = client
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()

    def reject(*_args, **_kwargs):
        raise RuntimeError('observer cleanup scheduling rejected')

    loop.set_task_factory(reject)
    try:
        with pytest.raises(RuntimeError, match='observer cleanup scheduling rejected'):
            observer.close()
        assert observer.client is client and not client.closed
    finally:
        loop.set_task_factory(previous)
        await client.close()
    await observer.close()
    assert observer.client is None and client.closed


@pytest.mark.asyncio
async def test_sync_reader_release_failure_latches_without_losing_stop_status(monkeypatch, caplog):
    monkeypatch.setattr(module, 'attach_pool', lambda *_args: object())
    controller = ReaderController()
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    close_data = controller.runtime.close_data

    def reject():
        raise RuntimeError('reader release scheduling rejected')

    controller.runtime.close_data = reject
    try:
        with pytest.raises(RuntimeError, match='reader release scheduling rejected'):
            controller.stop()
        assert controller.status == RunnableStatus.TERMINATED
        assert controller.runtime.pool is not None
    finally:
        controller.runtime.close_data = close_data
        controller.release.set()
        controller.runtime.close_release.set()
        await asyncio.gather(controller._control_loop_task, return_exceptions=True)
    with pytest.raises(RuntimeError, match='cleanup failed'):
        controller.start()
    assert 'release scheduling rejected' in caplog.text


@pytest.mark.asyncio
async def test_partial_cleanup_failure_still_tracks_and_joins_accepted_close(monkeypatch):
    monkeypatch.setattr(module, 'attach_pool', lambda *_args: object())
    controller = ReaderController()
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    failed = asyncio.Event()

    def reject_observer():
        failed.set()
        raise RuntimeError('observer cleanup scheduling rejected')

    controller.market_context.close = reject_observer
    try:
        with pytest.raises(RuntimeError, match='observer cleanup scheduling rejected'):
            controller.stop()
        assert len(controller._data_close_tasks) == 1
        failed.clear()
        controller.release.set()
        await asyncio.wait_for(failed.wait(), 1)
        await asyncio.sleep(0)
        assert not controller._control_loop_task.done()
    finally:
        controller.market_context.close = lambda: None
        controller.release.set()
        controller.runtime.close_release.set()
        await asyncio.gather(controller._control_loop_task, return_exceptions=True)
    assert controller.status == RunnableStatus.TERMINATED
    assert len(controller.runtime.closed) == 1
    assert not controller._data_close_tasks
    with pytest.raises(RuntimeError, match='cleanup failed'):
        controller.start()
