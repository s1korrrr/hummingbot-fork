"""Pure accounting fixtures: no executor loop, strategy, connector, or market replay."""
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.strategy_v2.executors.executor_orchestrator import PositionHold
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


def order(identifier, side, rows):
    result = InFlightOrder(identifier, 'ETH-USDC', OrderType.LIMIT, side,
                           sum((D(row[0]) for row in rows), D(0)), 1, D(rows[0][1]), identifier,
                           initial_state=OrderState.FILLED)
    for index, (amount, price, token, fee) in enumerate(rows):
        result.update_with_trade_update(TradeUpdate(
            str(index), identifier, identifier, 'ETH-USDC', 2 + index, D(price), D(amount),
            D(amount) * D(price), AddedToCostTradeFee(flat_fees=[TokenAmount(token, D(fee))])))
    return result


ENTRY = [('.05', '100', 'ETH', '.0001'), ('.05', '200', 'ETH', '.00005')]


def executor(rows=ENTRY):
    # Only property accounting is exercised; deliberately do not initialize a runtime.
    result = PositionExecutor.__new__(PositionExecutor)
    result.config = SimpleNamespace(side=TradeType.BUY, trading_pair='ETH-USDC', connector_name='okx',
                                    entry_order_policy='legacy')
    tracked = TrackedOrder(order_id='entry')
    tracked.order = order('entry', TradeType.BUY, rows)
    result._open_order = tracked
    result._completed_open_orders = []
    result._completed_close_orders = []
    result._close_order = result._take_profit_limit_order = None
    result.close_type = CloseType.TRAILING_STOP
    result._expected_close_fee_rate = lambda: D(0)
    result.get_price = lambda *args, **kwargs: D('150')
    result.trading_rules = SimpleNamespace(min_base_amount_increment=D('.00000001'))
    return result


def close(result, amount, price='150'):
    tracked = TrackedOrder(order_id='exit')
    tracked.order = order('exit', TradeType.SELL, [(amount, price, 'USDC', '0')])
    result._close_order = tracked


def assert_close(actual, expected):
    assert abs(actual - expected) < D('1e-22')


def test_unequal_partial_base_fees_preserve_cash_pnl_and_no_loss_floor():
    result = executor()
    assert result.open_filled_amount == D('.09985')
    assert result.entry_price == D('150')  # gross execution VWAP remains the public fill price
    assert result.open_filled_amount_quote == D('14.98')
    assert_close(result._min_no_loss_sell_price(), D('15') / D('.09985'))
    close(result, '.09985')
    assert result.get_net_pnl_quote() == D('-.02250')


@pytest.mark.parametrize('closed', ['0', '.04'])
def test_open_and_partially_closed_marks_use_exact_fee_net_cost(closed):
    result = executor()
    if D(closed):
        close(result, closed, '160')
    expected_cash_pnl = D(closed) * 160 + (D('.09985') - D(closed)) * 150 - 15
    assert_close(result.get_net_pnl_quote(), expected_cash_pnl)
    target = D('.005')
    target_price = result._price_for_net_pnl_pct(target)
    assert_close((target_price * result.open_filled_amount - D('15')) / D('14.98'), target)


@pytest.mark.parametrize('rows,cost,net', [
    ([('.05', '100', 'USDC', '.01'), ('.05', '200', 'USDC', '.01')], '15', '.1'),
    ([('.05', '100', 'ETH', '.0001'), ('.05', '200', 'USDC', '.01')], '14.99', '.0999'),
    ([('.05', '100', 'ETH', '.00005'), ('.05', '200', 'ETH', '.00005')], '14.985', '.0999'),
])
def test_quote_and_equal_rate_fee_cases_keep_their_basis(rows, cost, net):
    result = executor(rows)
    assert result.open_filled_amount_quote == D(cost)
    assert result.open_filled_amount == D(net)
    assert_close(result._min_no_loss_sell_price(), (D(cost) + result.cum_fees_quote) / D(net))


@pytest.mark.parametrize('side', [TradeType.BUY, TradeType.SELL])
def test_held_grouped_receipt_matches_individual_fills_and_roundtrip_restore(side):
    source = order('entry', side, ENTRY)
    grouped = PositionHold('okx', 'ETH-USDC', TradeType.BUY)
    grouped.add_order_record(source.to_json())
    grouped.add_order_record(source.to_json())  # duplicate cumulative delivery is idempotent
    separate = PositionHold('okx', 'ETH-USDC', TradeType.BUY)
    for index, row in enumerate(ENTRY):
        separate.add_order_record(order(str(index), side, [row]).to_json())
    restored = PositionHold('okx', 'ETH-USDC', TradeType.BUY)
    restored.add_order_record(InFlightOrder.from_json(source.to_json()).to_json())
    expected = separate.get_position_summary(D('150'))
    for actual in (grouped.get_position_summary(D('150')), restored.get_position_summary(D('150'))):
        assert actual.amount == expected.amount
        assert_close(actual.global_pnl_quote, expected.global_pnl_quote)
        assert_close(actual.breakeven_price, expected.breakeven_price)
        assert actual.remaining_fees_quote == expected.remaining_fees_quote


def test_explicit_base_fee_episode_receipt_keeps_gross_quote_cost():
    held = PositionHold('okx', 'ETH-USDC', TradeType.BUY)
    held.add_order_record(dict(order('entry', TradeType.BUY, ENTRY).to_json(),
                               actual_base_fee='.00015', cumulative_fee_paid_quote='0'))
    summary = held.get_position_summary(D('150'))
    assert_close(summary.amount * summary.breakeven_price + summary.remaining_fees_quote, D('15'))
    assert_close(summary.global_pnl_quote, D('-.0225'))


def test_held_partial_sale_with_base_fee_matches_cash_and_reconstructed_receipts():
    records = [order('entry', TradeType.BUY, ENTRY).to_json(),
               order('exit', TradeType.SELL, [('.04', '160', 'ETH', '.00004')]).to_json()]
    held = PositionHold('okx', 'ETH-USDC', TradeType.BUY)
    for record in records:
        held.add_order_record(record)
    summary = held.get_position_summary(D('150'))
    assert summary.amount == D('.05981')
    assert_close(summary.global_pnl_quote, D('6.4') + D('.05981') * 150 - 15)
    restored = PositionHold('okx', 'ETH-USDC', TradeType.BUY)
    for record in records:
        restored.add_order_record(record)
    assert restored.get_position_summary(D('150')) == summary


def test_prevalued_third_token_fee_stays_in_fees_without_changing_inventory_basis():
    held = PositionHold('okx', 'ETH-USDC', TradeType.BUY)
    record = order('entry', TradeType.BUY, [('.1', '150', 'USDC', '0')]).to_json()
    record['order_fills']['0']['fee']['flat_fees'] = [{'token': 'OTHER', 'amount': '.1'}]
    record['cumulative_fee_paid_quote'] = '.3'  # native valuation supplied; no conversion here
    held.add_order_record(record)
    summary = held.get_position_summary(D('150'))
    assert summary.amount == D('.1')
    assert summary.breakeven_price == D('150')
    assert summary.global_pnl_quote == D('-.3')


@pytest.mark.parametrize('side,fee,expected', [
    ('BUY', {'percent': '.002', 'percent_token': 'ETH'}, '.01'),
    ('SELL', {'percent': '.002', 'percent_token': 'ETH'}, '.01'),
    ('BUY', {'percent': '.002', 'fee_type': 'DeductedFromReturns'}, '.01'),
    ('SELL', {'percent': '.002', 'fee_type': 'DeductedFromReturns'}, '0'),
    ('BUY', {'percent': '.002', 'percent_token': 'USDC'}, '0'),
    ('BUY', {'percent': '0', 'flat_fees': [{'token': 'OTHER', 'amount': '.1'}]}, '0'),
])
def test_base_fee_quote_offset_uses_only_base_asset_without_other_token_conversion(side, fee, expected):
    from hummingbot.strategy_v2.executors.order_accounting import base_fee_quote_amount

    record = {'trade_type': side, 'order_fills': {'f': {
        'fill_base_amount': '.05', 'fill_price': '100', 'fee': fee}}}
    assert base_fee_quote_amount(record, 'ETH') == D(expected)


@pytest.mark.parametrize('price', [None, 'NaN', 'Infinity', '0', '-1'])
def test_base_fee_valuation_requires_valid_fill_price(price):
    from hummingbot.strategy_v2.executors.order_accounting import base_fee_quote_amount

    record = {'trade_type': 'BUY', 'order_fills': {'f': {
        'fill_base_amount': '.05', 'fill_price': price,
        'fee': {'flat_fees': [{'token': 'ETH', 'amount': '.0001'}]}}}}
    with pytest.raises(ValueError):
        base_fee_quote_amount(record, 'ETH')
