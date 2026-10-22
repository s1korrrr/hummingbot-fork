"""Reader shutdown must finish before a modular controller may restart."""
import asyncio
import inspect
from types import SimpleNamespace

import pytest

from controllers.generic import modular_spot as spot_module
from controllers.generic.modular_spot_core import native_composition as native_module
from controllers.generic.modular_spot_core.native_composition import NativeComposition
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
from hummingbot.strategy_v2.models.base import RunnableStatus


class Reader:
    def __init__(self):
        self.started = 0
        self.close_entered = asyncio.Event()
        self.close_release = asyncio.Event()
        self.closed = 0
        self.close_error = None

    def start(self):
        self.started += 1

    async def close(self):
        self.close_entered.set()
        await self.close_release.wait()
        if self.close_error is not None:
            raise self.close_error
        self.closed += 1


class NativeReaderProbe(NativeComposition, ControllerBase):
    def __init__(self, reader):
        ControllerBase.__init__(self, SimpleNamespace(), SimpleNamespace(ready=False),
                                asyncio.Queue(), update_interval=0)
        self.shared_candles = SimpleNamespace(pool=reader)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.fail_initialize = False

    def initialize_candles(self):
        super().initialize_candles()
        if self.fail_initialize:
            raise RuntimeError('native reader startup failed')

    async def control_task(self):
        self.entered.set()
        await self.release.wait()


class RuntimeReader:
    def __init__(self):
        self.pool = object()  # Constructor-attached shared pool, not a started reader.
        self.active = False
        self.close_entered = asyncio.Event()
        self.close_release = asyncio.Event()
        self.closed = []
        self.released = []
        self.close_error = None

    def start_data(self):
        self.active = True

    def close_data(self):
        pool, self.pool = self.pool, None
        if pool is None:
            return None
        if not self.active:
            self.released.append(pool)
            return None
        self.active = False

        async def close():
            self.close_entered.set()
            await self.close_release.wait()
            if self.close_error is not None:
                raise self.close_error
            self.closed.append(pool)

        return asyncio.create_task(close())


class ModularReaderProbe(spot_module.ModularSpotController):
    def __init__(self):
        ControllerBase.__init__(self, SimpleNamespace(data=SimpleNamespace(mode='shared')),
                                SimpleNamespace(ready=False), asyncio.Queue(), update_interval=0)
        self.runtime = RuntimeReader()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def control_task(self):
        self.runtime.start_data()
        self.entered.set()
        await self.release.wait()


async def drain(controller, reader):
    reader.close_release.set()
    controller.release.set()
    task = controller._control_loop_task
    if task is not None:
        await asyncio.gather(task, return_exceptions=True)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_native_close_is_joined_before_control_loop_finishes():
    reader = Reader()
    controller = NativeReaderProbe(reader)
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    controller.stop()
    controller.release.set()
    try:
        await asyncio.wait_for(reader.close_entered.wait(), 1)
        assert not controller._control_loop_task.done()
        with pytest.raises(RuntimeError, match='still stopping'):
            controller.start()
    finally:
        await drain(controller, reader)
    assert reader.closed == 1
    controller.entered.clear()
    controller.release.clear()
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    controller.stop()
    await drain(controller, reader)
    assert reader.started == reader.closed == 2


@pytest.mark.asyncio
async def test_native_failed_start_joins_reader_cleanup():
    reader = Reader()
    controller = NativeReaderProbe(reader)
    controller.fail_initialize = True
    controller.start()
    try:
        await asyncio.wait_for(reader.close_entered.wait(), 1)
        assert reader.started == 1
        assert not controller._control_loop_task.done()
    finally:
        await drain(controller, reader)
    assert reader.closed == 1
    assert controller.status == RunnableStatus.TERMINATED


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['exception', 'cancel'])
async def test_native_reader_cleanup_failure_blocks_same_object_restart(failure, caplog):
    reader = Reader()
    controller = NativeReaderProbe(reader)
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    controller.stop()
    controller.release.set()
    await asyncio.wait_for(reader.close_entered.wait(), 1)
    try:
        if failure == 'exception':
            reader.close_error = ValueError('reader cleanup rejected')
            reader.close_release.set()
        else:
            controller._reader_close_task.cancel()
        await asyncio.gather(controller._control_loop_task, return_exceptions=True)
        with pytest.raises(RuntimeError, match='cleanup failed'):
            controller.start()
        assert 'reader cleanup' in caplog.text
    finally:
        await drain(controller, reader)


@pytest.mark.asyncio
async def test_native_prestart_cancel_never_starts_reader():
    reader = Reader()
    controller = NativeReaderProbe(reader)
    controller.start()
    controller._control_loop_task.cancel()
    await asyncio.gather(controller._control_loop_task, return_exceptions=True)
    assert reader.started == 0
    assert not reader.close_entered.is_set()
    assert controller.status == RunnableStatus.TERMINATED


@pytest.mark.asyncio
async def test_native_rejected_schedule_never_starts_reader():
    reader = Reader()
    controller = NativeReaderProbe(reader)
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()

    def reject_task(_loop, _coroutine):
        raise RuntimeError('task factory rejected')

    try:
        loop.set_task_factory(reject_task)
        with pytest.raises(RuntimeError, match='task factory rejected'):
            controller.start()
    finally:
        loop.set_task_factory(previous_factory)
    assert reader.started == 0
    assert not reader.close_entered.is_set()
    assert controller.status == RunnableStatus.NOT_STARTED


@pytest.mark.asyncio
async def test_native_close_schedule_rejection_closes_coroutine_and_holds_restart(monkeypatch, caplog):
    reader = Reader()
    controller = NativeReaderProbe(reader)

    async def close():
        raise AssertionError('rejected close must never run')

    coroutine = close()
    reader.close = lambda: coroutine

    def reject_task(*_args, **_kwargs):
        raise RuntimeError('close task rejected')

    with monkeypatch.context() as scoped:
        scoped.setattr(native_module.asyncio, 'create_task', reject_task)
        with pytest.raises(RuntimeError, match='close task rejected'):
            controller.on_stop()
    assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
    with pytest.raises(RuntimeError, match='cleanup failed'):
        controller.start()
    assert 'close task rejected' in caplog.text


@pytest.mark.asyncio
async def test_native_eager_failed_start_keeps_pending_reader_close_owned():
    reader = Reader()
    controller = NativeReaderProbe(reader)
    controller.fail_initialize = True
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    try:
        loop.set_task_factory(asyncio.eager_task_factory)
        controller.start()
    finally:
        loop.set_task_factory(previous_factory)
    try:
        await asyncio.wait_for(reader.close_entered.wait(), 1)
        assert not controller._control_loop_task.done()
        controller.stop()
        with pytest.raises(RuntimeError, match='still stopping'):
            controller.start()
    finally:
        await drain(controller, reader)


@pytest.mark.asyncio
async def test_modular_stop_does_not_reattach_while_old_close_is_pending(monkeypatch):
    attached = []

    def attach(*_args):
        pool = object()
        attached.append(pool)
        return pool

    monkeypatch.setattr(spot_module, 'attach_pool', attach)
    controller = ModularReaderProbe()
    reader = controller.runtime
    constructor_pool = reader.pool
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    controller.stop()
    try:
        await asyncio.wait_for(reader.close_entered.wait(), 1)
        with pytest.raises(RuntimeError, match='still stopping'):
            controller.start()
        assert reader.pool is None and attached == []
        controller.release.set()
        await asyncio.sleep(0)
        assert not controller._control_loop_task.done()
    finally:
        await drain(controller, reader)
    assert len(reader.closed) == 1
    assert controller.status == RunnableStatus.TERMINATED
    controller.entered.clear()
    controller.release.clear()
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    assert reader.pool is attached[0]
    controller.stop()
    await drain(controller, reader)
    assert reader.closed == [constructor_pool, attached[0]]


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['exception', 'cancel'])
async def test_modular_reader_cleanup_failure_blocks_same_object_restart(monkeypatch, failure, caplog):
    monkeypatch.setattr(spot_module, 'attach_pool', lambda *_args: object())
    controller = ModularReaderProbe()
    reader = controller.runtime
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    controller.stop()
    controller.release.set()
    await asyncio.wait_for(reader.close_entered.wait(), 1)
    try:
        if failure == 'exception':
            reader.close_error = ValueError('reader cleanup rejected')
            reader.close_release.set()
        else:
            controller._data_close_task.cancel()
        await asyncio.gather(controller._control_loop_task, return_exceptions=True)
        with pytest.raises(RuntimeError, match='cleanup failed'):
            controller.start()
        assert 'reader cleanup' in caplog.text
    finally:
        await drain(controller, reader)


@pytest.mark.asyncio
async def test_modular_prestart_cancellation_releases_constructor_attachment(monkeypatch):
    monkeypatch.setattr(spot_module, 'attach_pool',
                        lambda *_args: (_ for _ in ()).throw(AssertionError('unexpected reader attachment')))
    controller = ModularReaderProbe()
    reader = controller.runtime
    constructor_pool = reader.pool
    controller.start()
    controller._control_loop_task.cancel()
    await asyncio.gather(controller._control_loop_task, return_exceptions=True)
    assert reader.pool is None
    assert reader.released == [constructor_pool]
    assert not reader.close_entered.is_set()


@pytest.mark.asyncio
async def test_modular_rejected_schedule_releases_constructor_attachment(monkeypatch):
    controller = ModularReaderProbe()
    reader = controller.runtime
    constructor_pool = reader.pool
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()

    def reject_task(_loop, _coroutine):
        raise RuntimeError('task factory rejected')

    try:
        loop.set_task_factory(reject_task)
        with pytest.raises(RuntimeError, match='task factory rejected'):
            controller.start()
    finally:
        loop.set_task_factory(previous_factory)
    assert reader.pool is None
    assert reader.released == [constructor_pool]
    assert not reader.close_entered.is_set()


@pytest.mark.asyncio
async def test_modular_sync_release_failure_still_finishes_status_and_blocks_restart(caplog):
    controller = ModularReaderProbe()
    controller._status = RunnableStatus.RUNNING
    completed = asyncio.get_running_loop().create_future()
    completed.set_result(None)
    controller._control_loop_task = completed

    def reject_release():
        raise RuntimeError('close task rejected')

    controller.runtime.close_data = reject_release
    with pytest.raises(RuntimeError, match='close task rejected'):
        controller._control_loop_finished(completed)
    assert controller.status == RunnableStatus.TERMINATED
    assert controller.terminated.is_set()
    with pytest.raises(RuntimeError, match='cleanup failed'):
        controller.start()
    assert 'close task rejected' in caplog.text
