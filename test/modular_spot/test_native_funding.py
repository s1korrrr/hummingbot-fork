"""Candidate funding uses native SQLite fills and native executor ownership."""
from decimal import Decimal as D

import pytest

from controllers.generic.modular_spot_core.native_funding import CandidateFundingPolicy, funding_projection
from hummingbot.model.trade_fill import TradeFill


def fill(order, trade, side, amount, price, fee='0'):
    amount, price, fee = map(D, (amount, price, fee))
    return TradeFill(config_file_path='test', strategy='v2_with_controllers', market='okx', symbol='BTC-USDC',
                     base_asset='BTC', quote_asset='USDC', timestamp=int(trade) * 1000, order_id=order,
                     trade_type=side, order_type='LIMIT', price=price, amount=amount, leverage=1, position='NIL',
                     trade_fee={'percent': '0', 'percent_token': None, 'flat_fees': [{'token': 'USDC', 'amount': str(fee)}]},
                     trade_fee_in_quote=fee, exact_amount=str(amount), exact_price=str(price),
                     exact_trade_fee_in_quote=str(fee), exact_receipt_source='order_filled_event_v1', exchange_trade_id=trade)


def test_loss_remains_consumed_and_profit_does_not_enlarge_inventory_envelope():
    policy = CandidateFundingPolicy(allocation_quote='50', fee_reserve_fraction='.003')
    projection = funding_projection([fill('buy', '1', 'BUY', '1', '40', '.04'),
                                     fill('sell', '2', 'SELL', '1', '30', '.03')], 'okx', 'BTC-USDC')
    assert projection['cash_used'] == D('10.07')
    assert projection['inventory_cost'] == 0
    assert policy.allocation_quote - projection['used'] == D('39.93')
    profitable = funding_projection([fill('buy', '1', 'BUY', '1', '40'),
                                     fill('sell', '2', 'SELL', '.5', '100')], 'okx', 'BTC-USDC')
    assert profitable['cash_used'] == D('-10')
    assert profitable['inventory_cost'] == D('20')
    assert profitable['used'] == D('20')


def test_unknown_fees_legacy_fill_and_wallet_inventory_are_rejected():
    with pytest.raises(ValueError):
        funding_projection([fill('sell', '1', 'SELL', '1', '40')], 'okx', 'BTC-USDC')
    for field, value in [('exact_receipt_source', None), ('trade_fee_in_quote', None),
                         ('trade_fee', {'percent': '0.01', 'flat_fees': []}),
                         ('trade_fee', {'percent': '0', 'flat_fees': [{'token': 'BNB', 'amount': '.01'}]})]:
        row = fill('buy', '1', 'BUY', '1', '40')
        setattr(row, field, value)
        with pytest.raises(ValueError):
            funding_projection([row], 'okx', 'BTC-USDC')


def test_policy_requires_explicit_fees_and_candidate_ceiling():
    for values in ({'allocation_quote': '2501', 'fee_reserve_fraction': '.003'},
                   {'allocation_quote': '50'}, {'allocation_quote': '0', 'fee_reserve_fraction': '.003'}):
        with pytest.raises(ValueError):
            CandidateFundingPolicy(**values)
    CandidateFundingPolicy(allocation_quote='101', fee_reserve_fraction='.003', max_position_quote='101')
    CandidateFundingPolicy(allocation_quote='1000', fee_reserve_fraction='.003', max_position_quote='105')
    CandidateFundingPolicy(allocation_quote='2500', fee_reserve_fraction='.003', max_position_quote='505')


def test_live_enabled_requires_policy_and_disallows_wallet_adoption():
    from controllers.generic.modular_ok_rsi import ModularOKRSIControllerConfig
    for values in ({}, {'funding': None}):
        with pytest.raises(ValueError, match='requires explicit funding'):
            ModularOKRSIControllerConfig(id='modular_spot_funded', execution_enabled=True, **values)
    assert ModularOKRSIControllerConfig(id='modular_spot_funded').funding is None
    for extra in ({'trading_pair': 'SUI-USDC'}, {'trade_mode': 'sell_only'}):
        with pytest.raises(ValueError):
            ModularOKRSIControllerConfig(id='modular_spot_funded', funding={
                'allocation_quote': '50', 'fee_reserve_fraction': '.003'}, **extra)


def test_flat_quote_fees_consume_cash_after_flat_roundtrip_and_percent_receipts_fail_closed():
    rows = [fill('buy', '1', 'BUY', '1', '40', '.04'), fill('sell', '2', 'SELL', '1', '40', '.04')]
    projected = funding_projection(rows, 'okx', 'BTC-USDC')
    assert projected['inventory_cost'] == 0
    assert projected['used'] == D('.08')
    rows[1].trade_fee = {'percent': '.001', 'percent_token': 'USDC', 'flat_fees': []}
    with pytest.raises(ValueError, match='Exact actual-currency'):
        funding_projection(rows, 'okx', 'BTC-USDC')


def test_base_fee_inventory_uses_native_remaining_fee_attribution():
    buy = fill('buy', '1', 'BUY', '1', '40', '.4')
    buy.trade_fee['flat_fees'] = [{'token': 'BTC', 'amount': '.01'}]
    projected = funding_projection([buy, fill('sell', '2', 'SELL', '.495', '100')], 'okx', 'BTC-USDC')
    assert projected['cash_used'] == D('-9.5')
    assert projected['inventory_cost'] == D('20')


@pytest.mark.parametrize(('side', 'token', 'rebate', 'quantity', 'cash', 'cost'), [
    ('BUY', 'BTC', '-.01', '1.01', '40', '40'),
    ('BUY', 'USDC', '-.1', '1', '39.9', '39.9'),
    ('SELL', 'BTC', '-.01', '.51', '15', '20.4'),
    ('SELL', 'USDC', '-.1', '.5', '14.9', '20'),
])
def test_signed_native_rebate_preserves_independent_inventory_and_cash(side, token, rebate, quantity, cash, cost):
    """Actual-currency rebates change owned units or cash; they are not negative inventory."""
    rows = [] if side == 'BUY' else [fill('buy', '1', 'BUY', '1', '40')]
    row = fill('rebated', '2', side, '1' if side == 'BUY' else '.5', '40' if side == 'BUY' else '50')
    row.trade_fee = {'percent': '0', 'flat_fees': [{'token': token, 'amount': rebate}]}
    valued = D(rebate) * (row.price_decimal if token == 'BTC' else D(1))
    row.trade_fee_in_quote = valued
    row.exact_trade_fee_in_quote = str(valued)
    rows.append(row)
    projected = funding_projection(rows, 'okx', 'BTC-USDC')
    assert projected['inventory_base'] == D(quantity)
    assert projected['cash_used'] == D(cash)
    assert projected['inventory_cost'] == D(cost)


def test_signed_fee_validation_keeps_finite_supported_asset_and_cost_bounds():
    for token, amount in [('BNB', '-.1'), ('BTC', 'NaN'), ('USDC', 'Infinity')]:
        row = fill('buy', '1', 'BUY', '1', '40')
        row.trade_fee = {'percent': '0', 'flat_fees': [{'token': token, 'amount': amount}]}
        with pytest.raises(ValueError):
            funding_projection([row], 'okx', 'BTC-USDC')
    # A rebate greater than acquisition cash would manufacture negative cost.
    row = fill('buy', '1', 'BUY', '1', '40', '-41')
    with pytest.raises(ValueError, match='acquisition cost'):
        funding_projection([row], 'okx', 'BTC-USDC')


def test_single_attempt_release_accepts_post_only_repair_entry_set():
    """One post-only cancel plus one filled replacement releases under single_attempt.

    Uses the real recorder-backed legacy fixture so every per-order check
    (snapshot, recorder rows, fees, tracker) runs against complete receipts.
    """
    from test.acquisition_chain_v2.test_funding_edges import claim_for, legacy_case
    store, info, _ = legacy_case(2)
    assert store.reconcile()['released_intents'] == 1
    fills = store.recorder.get_trades_for_config('modular.yml')
    orders = store.recorder.get_orders_for_config_and_market('modular.yml', None)
    store._validate_release(info, info.custom_info['entry_reconciliation'], D('1'), fills, orders, claim_for(info.config))


def test_single_attempt_release_accepts_exact_signed_base_rebate_and_keeps_fee_cap():
    from test.acquisition_chain_v2.test_funding_edges import (
        base_fee,
        chain_config,
        executor_info,
        make_store,
        native_order,
        recorded_fills,
        recorded_orders,
    )

    from hummingbot.core.data_type.in_flight_order import OrderState
    from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount

    config = chain_config()
    orders = [native_order('first', '100', timestamp=1),
              native_order('second', '98', state=OrderState.FILLED, timestamp=2,
                           fills=[('fill-1', '.25', 3, base_fee('-.00025')),
                                  ('fill-2', '.75', 4, base_fee('-.00075'))])]
    info = executor_info(config, orders)
    store = make_store([info], recorded_fills(orders), recorded_orders(orders))
    assert store.reconcile()['released_intents'] == 1
    assert D(store.assess()['inventory_cost']) == D('98')
    assert D(store.assess()['available_quote']) == D('52')

    costly = native_order('costly', '98', state=OrderState.FILLED, timestamp=2,
                          fills=[('expensive', '1', 3, AddedToCostTradeFee(
                              flat_fees=[TokenAmount('USDC', D('1'))]))])
    costly_info = executor_info(config, [costly])
    costly_store = make_store([costly_info], recorded_fills([costly]), recorded_orders([costly]))
    with pytest.raises(ValueError, match='Actual native fee exceeds funded bound'):
        costly_store.reconcile()


def test_single_attempt_release_rejects_two_filled_entry_orders():
    import copy
    from test.acquisition_chain_v2.test_funding_edges import claim_for, legacy_case

    from controllers.generic.modular_spot_core.domain import PolicyError
    store, info, _ = legacy_case(2)
    fills = store.recorder.get_trades_for_config('modular.yml')
    orders = store.recorder.get_orders_for_config_and_market('modular.yml', None)
    receipt = copy.deepcopy(info.custom_info['entry_reconciliation'])
    receipt['orders'][0].update(filled_base='0.5', trade_ids=['ghost'])
    with pytest.raises(PolicyError, match='multiple filled entry orders'):
        store._validate_release(info, receipt, D('1.5'), fills, orders, claim_for(info.config))
