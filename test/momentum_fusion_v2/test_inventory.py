from decimal import Decimal as D

import pytest

from controllers.generic.momentum_fusion_v2_core import runtime
from hummingbot.strategy_v2.components.domain import PolicyError


def test_sub_minimum_harvest_leg_collapses_to_single():
    result = runtime.split_or_single(total_base='1.0', harvest_base='0.04', amount_step='0.01', minimum_amount='0.1')
    assert result == {'mode': 'single', 'amount': D('1.0'), 'reason': 'sub_minimum_leg'}


def test_unknown_cost_cannot_become_zero():
    with pytest.raises(PolicyError):
        runtime.profit_floor(cost_quote=None, quantity='1', fee_fraction='0.001', margin='0', buffer='0',
                             price_step='0.01')
    with pytest.raises(PolicyError):
        runtime.profit_floor(cost_quote='0', quantity='1', fee_fraction='0.001', margin='0', buffer='0',
                             price_step='0.01')


def test_base_and_quote_fees_round_the_profit_floor_up():
    price = runtime.profit_floor(cost_quote='100', quantity='1', fee_fraction='0.001', margin='0',
                                 buffer='0.1', price_step='0.01')
    assert price >= D('100.21')
    assert price == price.quantize(D('0.01'))


def test_reserved_sales_reduce_uncommitted_inventory():
    assert runtime.net_uncommitted('10', '3') == D('7')
    assert runtime.remaining_after_partial('10', '3') == D('7')
    with pytest.raises(PolicyError):
        runtime.net_uncommitted('10', '11')
    with pytest.raises(PolicyError):
        runtime.remaining_after_partial('10', '11')


def test_open_mark_path_keeps_drawdown_through_recovery():
    assert runtime.maximum_marked_drawdown([100, 90, 100]) == D('-0.1')
