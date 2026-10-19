"""Funding replay retains every fill check without replaying BUY prefixes."""
from decimal import Decimal
from test.modular_spot.test_native_funding import fill
from unittest.mock import patch

import pytest

from controllers.generic.modular_spot_core.native_funding import funding_projection
from hummingbot.strategy_v2.executors.executor_orchestrator import PositionHold


def test_buy_history_does_not_replay_native_position_for_every_prefix():
    rows = [fill(f'buy-{i}', str(i + 1), 'BUY', '.01', '40', '.0004') for i in range(256)]
    original = PositionHold.get_position_summary
    calls = []

    def summarize(position, *args, **kwargs):
        calls.append(len(position._order_records))
        return original(position, *args, **kwargs)

    with patch.object(PositionHold, 'get_position_summary', summarize):
        result = funding_projection(rows, 'okx', 'BTC-USDC')
    assert result['inventory_base'] == Decimal('2.56')
    assert result['cash_used'] == Decimal('102.5024')
    assert result['inventory_cost'] == Decimal('102.5024')
    assert len(result['fills']) == 256
    assert calls == [256]


def test_sell_prefix_is_checked_even_when_later_buy_would_hide_unowned_sale():
    rows = [fill('buy', '1', 'BUY', '1', '40'),
            fill('sell', '2', 'SELL', '2', '40'),
            fill('later-buy', '3', 'BUY', '2', '40')]
    with pytest.raises(ValueError, match='sells unowned inventory'):
        funding_projection(rows, 'okx', 'BTC-USDC')


def test_every_fill_still_checks_fingerprint_and_actual_fees():
    rows = [fill(f'buy-{i}', str(i + 1), 'BUY', '.01', '40') for i in range(128)]
    with pytest.raises(ValueError, match='Duplicate native funding fill'):
        funding_projection([*rows, rows[-1]], 'okx', 'BTC-USDC')
    rows[-1].trade_fee = {'percent': '0', 'flat_fees': [{'token': 'USDC', 'amount': '.01'}]}
    with pytest.raises(ValueError, match='differs from actual currency fees'):
        funding_projection(rows, 'okx', 'BTC-USDC')
