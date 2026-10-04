"""Expected startup ordering is reported as such; real faults keep their warnings."""
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from test_native_composition_recovery import observe, owner, recorder  # noqa: F401

from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator


def _logged(controller, step):
    log = MagicMock()
    with patch.object(type(controller), 'logger', return_value=log):
        step()
    return log


def _unreconciled_pass(controller, now):
    controller.market_data_provider.time.return_value = now
    observe(controller, '101')  # positions held in memory, not yet in the recorder
    return controller.determine_executor_actions()


def test_unreconciled_hold_right_after_start_holds_and_logs_info(recorder):  # noqa: F811
    controller = owner()
    controller.market_data_provider.time.return_value = 1000.0
    assert controller.determine_executor_actions() == []
    log = _logged(controller, lambda: _unreconciled_pass(controller, 1010.0))
    assert 'not reconciled' in controller._recovery_hold
    assert log.warning.call_count == 0
    assert 'startup positions restore' in log.info.call_args.args[0]


def test_unreconciled_hold_after_the_startup_grace_still_warns(recorder):  # noqa: F811
    controller = owner()
    controller.market_data_provider.time.return_value = 1000.0
    assert controller.determine_executor_actions() == []
    log = _logged(controller, lambda: _unreconciled_pass(controller, 1000.0 + 61))
    assert 'not reconciled' in controller._recovery_hold
    assert log.warning.call_args.args[0] == 'native recovery hold: %s'


@pytest.mark.parametrize('amount,breakeven,closed', [
    ('0', '83769.0', True), ('0', '0', True), ('-1', '100', False), ('NaN', '100', False), ('1', '0', False)])
def test_only_a_zero_amount_stored_position_is_a_closed_summary(amount, breakeven, closed):
    position = SimpleNamespace(amount=D(amount), breakeven_price=D(breakeven))
    assert not ExecutorOrchestrator._is_valid_stored_position(position)
    assert ExecutorOrchestrator._is_closed_stored_position(position) is closed
