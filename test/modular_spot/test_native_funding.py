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


def _release_fixture(order_amount='0.00047461'):
    """Live single-attempt down-only intent: one post-only cancel, one filled replacement."""
    from types import SimpleNamespace as NS
    canceled_id, filled_id = 'BTCUC1repair', 'BTCUCa4final'
    fills = [fill(filled_id, '5541826', 'BUY', '0.00011', '84105.6', '0.0074'),
             fill(filled_id, '5541827', 'BUY', '0.00036461', '84105.6', '0.0245')]
    proofs = [{'schema': 1, 'source': 'okx_terminal_status_v1', 'order_id': canceled_id, 'exchange_order_id': 'venue-1',
               'trading_pair': 'BTC-USDC', 'side': 'BUY', 'amount': order_amount, 'filled_base': '0',
               'state': 'CANCELED', 'trade_ids': []},
              {'schema': 1, 'source': 'okx_terminal_status_v1', 'order_id': filled_id, 'exchange_order_id': 'venue-2',
               'trading_pair': 'BTC-USDC', 'side': 'BUY', 'amount': order_amount, 'filled_base': order_amount,
               'state': 'FILLED', 'trade_ids': ['5541826', '5541827']}]
    snapshots = [{'client_order_id': proof['order_id'], 'exchange_order_id': proof['exchange_order_id'],
                  'trade_type': 'BUY', 'amount': order_amount} for proof in proofs]
    info = NS(connector_name='okx', trading_pair='BTC-USDC', is_active=True,
              config=NS(entry_order_policy='single_attempt', trading_pair='BTC-USDC', amount=D(order_amount)),
              custom_info={'order_ids': [canceled_id, filled_id], 'execution_order_snapshots': snapshots})
    orders = [NS(id=proof['order_id'], exchange_order_id=proof['exchange_order_id'], symbol='BTC-USDC', market='okx')
              for proof in proofs]
    from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
    store = NativeFundingStore.__new__(NativeFundingStore)
    store.owner = NS(config=NS(execution_mode='live', connector_name='okx'),
                     market_data_provider=NS(connectors={'okx': NS(_order_tracker=NS(all_orders={}))}))
    receipt = {'schema': 1, 'source': 'native_terminal_entry_set_v1', 'orders': proofs}
    return store, info, receipt, fills, orders


def test_single_attempt_release_accepts_post_only_repair_entry_set():
    from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
    store, info, receipt, fills, orders = _release_fixture()
    NativeFundingStore._validate_release(store, info, receipt, D('0.00047461'), fills, orders)
    single = {**receipt, 'orders': receipt['orders'][1:]}
    info.custom_info['order_ids'] = info.custom_info['order_ids'][1:]
    info.custom_info['execution_order_snapshots'] = info.custom_info['execution_order_snapshots'][1:]
    NativeFundingStore._validate_release(store, info, receipt['orders'][1], D('0.00047461'), fills, orders)
    NativeFundingStore._validate_release(store, info, single, D('0.00047461'), fills, orders)


def test_single_attempt_release_rejects_two_filled_entry_orders():
    from controllers.generic.modular_spot_core.domain import PolicyError
    from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
    store, info, receipt, fills, orders = _release_fixture()
    first, second = receipt['orders']
    fills = [fill(first['order_id'], '1', 'BUY', '0.00011', '84115.5'),
             fill(second['order_id'], '2', 'BUY', '0.00036461', '84105.6')]
    first.update(filled_base='0.00011', trade_ids=['1'])
    second.update(filled_base='0.00036461', trade_ids=['2'])
    with pytest.raises(PolicyError, match='multiple filled entry orders'):
        NativeFundingStore._validate_release(store, info, receipt, D('0.00047461'), fills, orders)
    info.config.entry_order_policy = 'retry'
    info.is_active = False
    NativeFundingStore._validate_release(store, info, receipt, D('0.00047461'), fills, orders)
