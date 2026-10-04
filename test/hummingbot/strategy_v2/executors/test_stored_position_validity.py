from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator


@pytest.mark.parametrize('amount,breakeven,closed', [
    ('0', '83769.0', True), ('0', '0', True), ('-1', '100', False), ('NaN', '100', False), ('1', '0', False)])
def test_only_a_zero_amount_stored_position_is_a_closed_summary(amount, breakeven, closed):
    position = SimpleNamespace(amount=D(amount), breakeven_price=D(breakeven))
    assert not ExecutorOrchestrator._is_valid_stored_position(position)
    assert ExecutorOrchestrator._is_closed_stored_position(position) is closed


def test_a_positive_stored_position_is_valid_and_not_closed():
    position = SimpleNamespace(amount=D('0.5'), breakeven_price=D('100'))
    assert ExecutorOrchestrator._is_valid_stored_position(position)
    assert not ExecutorOrchestrator._is_closed_stored_position(position)
