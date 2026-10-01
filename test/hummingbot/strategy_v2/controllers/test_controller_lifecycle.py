"""Controller loop ownership without market data, execution or economics."""
import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.runnable_base import RunnableBase


class LifecycleController(ControllerBase):
    def __init__(self):
        super().__init__(ControllerConfigBase(id='test', controller_name='test', controller_type='generic'),
                         SimpleNamespace(ready=False), asyncio.Queue(), update_interval=0)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.active = self.peak = self.starts = self.stops = 0

    async def on_start(self):
        await super().on_start()
        self.starts += 1

    def on_stop(self):
        self.stops += 1

    async def control_task(self):
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.entered.set()
        try:
            await self.release.wait()
        finally:
            self.active -= 1


@pytest.mark.asyncio
async def test_restart_rejects_retiring_loop_then_starts_one_new_loop():
    controller = LifecycleController()
    before = set(asyncio.all_tasks())
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    controller.stop()
    try:
        with pytest.raises(RuntimeError, match='still stopping'):
            controller.start()
        assert controller.status == RunnableStatus.TERMINATED
        assert controller.terminated.is_set()
        assert controller.starts == 1 and controller.peak == 1
    finally:
        controller.stop()
        controller.release.set()
        await asyncio.gather(*(set(asyncio.all_tasks()) - before))
    controller.entered.clear()
    controller.release.clear()
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    try:
        assert controller.status == RunnableStatus.RUNNING
        assert controller.starts == 2 and controller.peak == 1
    finally:
        controller.stop()
        controller.release.set()
        await asyncio.gather(*(set(asyncio.all_tasks()) - before))
    assert controller.stops == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('controller_type', [LifecycleController, RunnableBase])
async def test_rejected_start_preserves_status_and_can_retry(controller_type):
    controller = controller_type()
    loop = asyncio.get_running_loop()
    old_factory = loop.get_task_factory()

    def reject(*_args, **_kwargs):
        raise RuntimeError('scheduling rejected')

    loop.set_task_factory(reject)
    try:
        with pytest.raises(RuntimeError, match='scheduling rejected'):
            controller.start()
        assert controller.status == RunnableStatus.NOT_STARTED
        assert not controller.terminated.is_set()
        if isinstance(controller, ControllerBase):
            assert not controller.executors_update_event.is_set()
    finally:
        loop.set_task_factory(old_factory)
    before = set(asyncio.all_tasks())
    controller.start()
    assert controller.status == RunnableStatus.RUNNING
    controller.stop()
    if isinstance(controller, LifecycleController):
        controller.release.set()
    await asyncio.gather(*(set(asyncio.all_tasks()) - before))


@pytest.mark.asyncio
async def test_candle_initialization_failure_terminates_before_control_task():
    controller = LifecycleController()
    controller.initialize_candles = Mock(side_effect=ValueError('invalid candle configuration'))
    before = set(asyncio.all_tasks())
    controller.start()
    await asyncio.gather(*(set(asyncio.all_tasks()) - before))
    assert controller.status == RunnableStatus.TERMINATED
    assert controller.terminated.is_set()
    assert controller.peak == 0 and controller.starts == 0
    assert controller.stops == 1


@pytest.mark.asyncio
async def test_startup_failure_marks_controller_terminated_and_finalizes():
    controller = LifecycleController()

    async def fail_start():
        raise ValueError('startup rejected')

    controller.on_start = fail_start
    before = set(asyncio.all_tasks())
    controller.start()
    await asyncio.gather(*(set(asyncio.all_tasks()) - before))
    assert controller.status == RunnableStatus.TERMINATED
    assert controller.terminated.is_set()
    assert controller.stops == 1


@pytest.mark.asyncio
async def test_stopped_before_first_tick_skips_startup():
    controller = LifecycleController()
    controller.initialize_candles = Mock()
    before = set(asyncio.all_tasks())
    controller.start()
    controller.stop()
    controller.release.set()
    await asyncio.gather(*(set(asyncio.all_tasks()) - before))
    assert controller.starts == 0
    assert controller.stops == 1
    controller.initialize_candles.assert_not_called()


@pytest.mark.asyncio
async def test_cancelled_loop_finalizes_and_updates_status():
    controller = LifecycleController()
    before = set(asyncio.all_tasks())
    controller.start()
    await asyncio.wait_for(controller.entered.wait(), 1)
    [task] = set(asyncio.all_tasks()) - before
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert controller.status == RunnableStatus.TERMINATED
    assert controller.terminated.is_set()
    assert controller.active == 0 and controller.stops == 1
