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
