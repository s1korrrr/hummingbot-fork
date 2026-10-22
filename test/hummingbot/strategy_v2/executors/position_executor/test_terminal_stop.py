from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor
from test.test_ok_rsi_down_only_entry_execution import make_executor as make_down_only_executor
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType


@pytest.mark.parametrize("down_only", [False, True])
@pytest.mark.parametrize("keep_position", [False, True])
@pytest.mark.parametrize("close_type", [CloseType.POSITION_HOLD, CloseType.STOP_LOSS])
def test_queued_repeated_stop_preserves_terminal_executor(down_only, keep_position, close_type):
    executor = make_down_only_executor()[0] if down_only else make_executor()
    executor._strategy.controllers = {}
    with patch.object(MarketsRecorder, "get_instance", return_value=MagicMock()):
        orchestrator = ExecutorOrchestrator(executor._strategy)
    owner = executor.config.controller_id
    orchestrator.active_executors[owner] = [executor]
    orchestrator.cached_performance[owner] = MagicMock()
    queued_stop = StopExecutorAction(
        controller_id=owner, executor_id=executor.config.id, keep_position=keep_position,
    )

    # A controller action was queued while running, but the control loop completed
    # before the action listener drained it. The executor is not archived yet.
    executor.close_type = close_type
    executor.stop()
    closed_at = executor.close_timestamp
    terminal_info = executor.executor_info
    executor._strategy.cancel.reset_mock()
    executor._strategy.buy.reset_mock()
    executor._strategy.sell.reset_mock()
    orchestrator.execute_actions([queued_stop, queued_stop])

    assert executor.status == RunnableStatus.TERMINATED
    assert executor.terminated.is_set()
    assert executor.close_type == close_type
    assert executor.close_timestamp == closed_at
    assert executor.executor_info == terminal_info
    executor._strategy.cancel.assert_not_called()
    executor._strategy.buy.assert_not_called()
    executor._strategy.sell.assert_not_called()
