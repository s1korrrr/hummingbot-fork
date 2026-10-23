"""Shared-funding edges for one bounded V2 acquisition chain.

A hand-built recorder envelope (in-memory Metadata checkpoint, native fills,
recorded orders and one ExecutorInfo) drives the real NativeFundingStore
reconcile/assess/admit paths. Nothing here reaches a live stack or exchange.
"""
import copy
from decimal import Decimal
from test.test_ok_rsi_down_only_entry_execution import make_config
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from controllers.generic.modular_spot_core.domain import PolicyError, identity
from controllers.generic.modular_spot_core.native_funding import (
    CandidateFundingPolicy,
    NativeFundingStore,
    _config_identity,
)
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.model.metadata import Metadata
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

V2 = ACQUISITION_CHAIN_V1
PAIR = 'BTC-USDC'
ALLOCATION = Decimal('150')
FEE_FRACTION = Decimal('.003')


def base_fee(amount):
    return AddedToCostTradeFee(flat_fees=[TokenAmount('BTC', Decimal(amount))])


def native_order(order_id, price, *, amount='1', state=OrderState.CANCELED, fills=(), timestamp=1):
    """One BUY child; fills are (trade_id, base, timestamp, fee)."""
    order = InFlightOrder(order_id, PAIR, OrderType.LIMIT_MAKER, TradeType.BUY, Decimal(amount), timestamp,
                          Decimal(price), exchange_order_id='venue-' + order_id)
    for trade_id, base, fill_timestamp, fee in fills:
        order.update_with_trade_update(TradeUpdate(
            trade_id, order_id, order.exchange_order_id, PAIR, fill_timestamp, Decimal(price), Decimal(base),
            Decimal(price) * Decimal(base), fee or AddedToCostTradeFee()))
    order.current_state = state
    return order


def proof_of(order):
    return dict(schema=1, source='okx_terminal_status_v1', order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id, trading_pair=PAIR, side='BUY',
                amount=str(order.amount), filled_base=str(order.executed_amount_base),
                state=order.current_state.name, trade_ids=sorted(order.order_fills))


def receipt_of(orders):
    proofs = [proof_of(order) for order in orders]
    return proofs[0] if len(proofs) == 1 else {'schema': 1, 'source': 'native_terminal_entry_set_v1', 'orders': proofs}


def chain_of(config, orders, *, closed=True, pending=None):
    children = [{'generation': index, 'order_id': order.client_order_id,
                 'predecessor_id': None if index == 0 else orders[index - 1].client_order_id,
                 'purpose': 'initial' if index == 0 else 'downward_reprice',
                 'submitted_base': str(order.amount), 'limit_price': str(order.price)}
                for index, order in enumerate(orders)]
    return {'schema': 1, 'contract': V2, 'parent_id': config.id, 'authorized_base': str(config.amount),
            'maximum_price': str(config.maximum_acceptable_price), 'children': children,
            'successor_pending': pending, 'membership_closed': closed,
            'gross_filled_base': str(sum((order.executed_amount_base for order in orders), Decimal('0')))}


def executor_info(config, orders, *, active=False, chain=True, closed=True, pending=None):
    gross = sum((order.executed_amount_base for order in orders), Decimal('0'))
    custom = {'order_ids': [order.client_order_id for order in orders],
              'execution_order_snapshots': [order.to_json() for order in orders],
              'entry_reconciliation': receipt_of(orders),
              'execution_receipt': {'entry_gross_base': str(gross)},
              'entry_lifecycle': {'policy': 'single_attempt'},
              'resume_conflicting_fill': None}
    if chain:
        custom['acquisition_chain'] = chain_of(config, orders, closed=closed, pending=pending)
    return ExecutorInfo(id=config.id, timestamp=config.timestamp, type='position_executor',
                        status=RunnableStatus.RUNNING if active else RunnableStatus.TERMINATED,
                        config=config, net_pnl_pct=Decimal('0'), net_pnl_quote=Decimal('0'),
                        cum_fees_quote=Decimal('0'), filled_amount_quote=gross * config.entry_price,
                        is_active=active, is_trading=False, custom_info=custom, controller_id='owner')


def recorded_fill(order, trade_id):
    trade = order.order_fills[trade_id]
    valued = sum((fee.amount * (trade.fill_price if fee.token == 'BTC' else Decimal('1'))
                  for fee in trade.fee.flat_fees), Decimal('0'))
    return TradeFill(config_file_path='modular.yml', strategy='v2_with_controllers', market='okx', symbol=PAIR,
                     base_asset='BTC', quote_asset='USDC', timestamp=int(trade.fill_timestamp) * 1000,
                     order_id=order.client_order_id, trade_type='BUY', order_type='LIMIT_MAKER',
                     price=trade.fill_price, amount=trade.fill_base_amount, leverage=1, position='NIL',
                     trade_fee=trade.fee.to_json(), trade_fee_in_quote=valued,
                     exact_amount=str(trade.fill_base_amount), exact_price=str(trade.fill_price),
                     exact_trade_fee_in_quote=str(valued), exact_receipt_source='order_filled_event_v1',
                     exchange_trade_id=trade_id)


def recorded_fills(orders):
    return [recorded_fill(order, trade_id) for order in orders for trade_id in order.order_fills]


def recorded_orders(orders):
    return [SimpleNamespace(id=order.client_order_id, exchange_order_id=order.exchange_order_id, symbol=PAIR,
                            market='okx', last_status='BuyOrderCompleted' if order.current_state is OrderState.FILLED
                            else 'OrderCancelled') for order in orders]


def chain_config(**changes):
    values = dict(id='parent-btc', controller_id='owner', trading_pair=PAIR, entry_chain_contract=V2)
    values.update(changes)
    return make_config(**values)


def claim_for(config):
    return {'config': _config_identity(config, 'owner'), 'amount': str(config.amount),
            'price': str(config.entry_price), 'fee_fraction': str(FEE_FRACTION)}


def make_store(infos, fills, orders, *, allocation=ALLOCATION, max_position='505'):
    """Real NativeFundingStore over a disposable in-memory checkpoint and hand-built native evidence."""
    engine = create_engine('sqlite:///:memory:')
    Metadata.__table__.create(engine)
    policy = CandidateFundingPolicy(allocation_quote=allocation, fee_reserve_fraction=FEE_FRACTION,
                                    max_position_quote=max_position)
    connector = SimpleNamespace(_order_tracker=SimpleNamespace(all_orders={}),
                                get_fee=lambda **_: AddedToCostTradeFee(percent=Decimal('0.001')))
    owner = SimpleNamespace(config=SimpleNamespace(id='owner', connector_name='okx', trading_pair=PAIR,
                                                   execution_mode='live', funding=policy),
                            executors_info=list(infos), positions_held=[],
                            market_data_provider=SimpleNamespace(connectors={'okx': connector}),
                            _composition_unchanged=lambda: True)
    recorder = SimpleNamespace(config_file_path='modular.yml',
                               sql_manager=SimpleNamespace(get_new_session=lambda: Session(engine)),
                               get_all_executors=lambda: list(infos),
                               get_trades_for_config=lambda path: list(fills),
                               get_order_status_history=lambda order_id: [],
                               get_orders_for_config_and_market=lambda path, connector: list(orders))
    store = object.__new__(NativeFundingStore)
    store.owner, store.recorder, store.policy = owner, recorder, policy
    store.key = 'modular-candidate-funding:v1:' + identity([recorder.config_file_path])
    store.owners = {'owner': owner}
    store.binding = identity(['owner', 'okx', PAIR, 'composition', 'source'])
    store.status = {}
    claims = {info.id: claim_for(info.config) for info in infos if info.config.side is TradeType.BUY}
    assert store._write(None, {'revision': 0, 'claims': claims, 'fills': {}, 'releases': {}})
    store._engine = engine
    return store


def three_child_orders(*, fee=None):
    return [native_order('first', '100', timestamp=1),
            native_order('second', '99', timestamp=2),
            native_order('third', '98', state=OrderState.FILLED, timestamp=3,
                         fills=[('fill-1', '.25', 4, fee), ('fill-2', '.75', 5, fee)])]


def test_closed_three_child_chain_releases_its_parent_once():
    config = chain_config()
    orders = three_child_orders()
    info = executor_info(config, orders)
    store = make_store([info], recorded_fills(orders), recorded_orders(orders))
    assert Decimal(store.assess()['pending_quote']) == 0  # gross fully consumed; nothing left to reserve
    status = store.reconcile()
    assert status['released_intents'] == 1
    assert Decimal(status['pending_quote']) == 0
    assert Decimal(status['available_quote']) == ALLOCATION - Decimal('98')
    _, state = store._load()
    assert state['releases'] == {config.id: info.custom_info['entry_reconciliation']}
    assert len(state['fills']) == 2
    # Retrying is idempotent: the same receipt, still one release.
    again = store.reconcile()
    assert again['released_intents'] == 1
    assert Decimal(again['pending_quote']) == 0


def test_pending_successor_keeps_the_full_reserve_and_blocks_competing_admission():
    config = chain_config()
    first = native_order('first', '100', timestamp=1)
    info = executor_info(config, [first], active=True, closed=False,
                         pending={'predecessor_id': 'first', 'purpose': 'downward_reprice'})
    store = make_store([info], [], recorded_orders([first]))
    full_reserve = config.amount * config.entry_price * (1 + FEE_FRACTION)
    assert Decimal(store.assess()['pending_quote']) == full_reserve
    status = store.reconcile()
    assert status['released_intents'] == 0
    assert Decimal(status['pending_quote']) == full_reserve
    competitor = chain_config(id='competitor', amount=Decimal('.6'))
    with pytest.raises(PolicyError, match='allocation exhausted'):
        store.admit(competitor)
    _, state = store._load()
    assert set(state['claims']) == {config.id}
    assert state['releases'] == {}
    assert Decimal(store.assess()['pending_quote']) == full_reserve
    # Only an intent that fits beside the untouched reserve is admitted.
    assert store.admit(chain_config(id='fits', amount=Decimal('.4')))
    _, state = store._load()
    assert set(state['claims']) == {config.id, 'fits'}


def two_child_release():
    config = chain_config()
    orders = [native_order('first', '100', timestamp=1),
              native_order('second', '99', state=OrderState.FILLED, timestamp=2,
                           fills=[('fill-1', '.25', 3, None), ('fill-2', '.75', 4, None)])]
    info = executor_info(config, orders)
    store = make_store([info], recorded_fills(orders), recorded_orders(orders))
    assert store.reconcile()['released_intents'] == 1
    _, state = store._load()
    original = copy.deepcopy(state['releases'][config.id])
    return store, info, original


def test_changed_trade_ids_after_release_are_rejected_without_touching_the_release():
    store, info, original = two_child_release()
    info.custom_info['entry_reconciliation']['orders'][1]['trade_ids'] = ['fill-1', 'fill-9']
    with pytest.raises(ValueError, match='Acquisition chain terminal quantity or fills differ'):
        store.reconcile()
    _, state = store._load()
    assert state['releases'] == {info.id: original}


def test_reordered_terminal_set_after_release_is_a_changed_receipt():
    store, info, original = two_child_release()
    info.custom_info['entry_reconciliation']['orders'].reverse()
    with pytest.raises(PolicyError, match='changed after funding release'):
        store.reconcile()
    _, state = store._load()
    assert state['releases'] == {info.id: original}


def test_base_fees_release_on_gross_fills_without_fee_remainder_capacity():
    config = chain_config()
    orders = [native_order('first', '100', timestamp=1),
              native_order('second', '98', state=OrderState.FILLED, timestamp=2,
                           fills=[('fill-1', '.4', 3, base_fee('.0004')), ('fill-2', '.6', 4, base_fee('.0006'))])]
    gross = orders[1].executed_amount_base
    assert gross == config.amount
    net = gross - Decimal('.001')
    assert net < gross
    info = executor_info(config, orders)
    assert info.custom_info['entry_reconciliation']['orders'][1]['filled_base'] == str(gross)
    assert info.custom_info['execution_receipt']['entry_gross_base'] == str(gross)
    store = make_store([info], recorded_fills(orders), recorded_orders(orders))
    status = store.reconcile()
    assert status['released_intents'] == 1
    assert Decimal(status['pending_quote']) == 0
    # The fee remainder does not come back as BUY capacity: only the gross
    # notional is consumed and nothing beyond allocation minus gross is offered.
    assert Decimal(status['available_quote']) == ALLOCATION - gross * Decimal('98')
    assert store.entry_notional_ceiling() == (ALLOCATION - gross * Decimal('98')) / (1 + FEE_FRACTION)
    with pytest.raises(PolicyError, match='Duplicate or mutated funded native executor'):
        store.admit(config)
    # A net (fee-reduced) receipt cannot stand in for the gross fill total.
    tampered = executor_info(config, orders)
    tampered.custom_info['execution_receipt']['entry_gross_base'] = str(net)
    with pytest.raises(PolicyError, match='Native executor and funding fill coverage differ'):
        make_store([tampered], recorded_fills(orders), recorded_orders(orders)).reconcile()


def legacy_case(child_count):
    config = chain_config(id='legacy-btc', entry_chain_contract=None)
    assert config.entry_chain_contract is None and config.entry_order_policy == 'single_attempt'
    orders = [native_order(f'legacy-{index}', str(100 - index), timestamp=index + 1) for index in range(child_count - 1)]
    orders[0].cancel_source = '31'
    orders.append(native_order('legacy-last', str(100 - child_count), state=OrderState.FILLED,
                               timestamp=child_count, fills=[('fill-1', '1', child_count + 1, None)]))
    info = executor_info(config, orders, chain=False)
    assert 'acquisition_chain' not in info.custom_info
    assert 'planned_position_resume_compatibility_v1' not in info.custom_info
    store = make_store([info], recorded_fills(orders), recorded_orders(orders))
    return store, info, orders


def test_unclaimed_legacy_two_child_receipt_releases_under_single_filled_rule():
    """Legacy rows without the chain contract follow the merged single-filled-order rule."""
    store, info, _ = legacy_case(2)
    status = store.reconcile()
    _, state = store._load()
    assert info.id in state['releases']
    assert status['released_intents'] == 1
    fills, orders = store.recorder.get_trades_for_config('modular.yml'), store.recorder.get_orders_for_config_and_market('modular.yml', None)
    store._validate_release(info, info.custom_info['entry_reconciliation'], Decimal('1'), fills, orders,
                            claim_for(info.config))


def test_legacy_three_child_receipt_releases_with_one_filled_and_rejects_two_filled():
    store, info, _ = legacy_case(3)
    receipt = info.custom_info['entry_reconciliation']
    assert len(receipt['orders']) == 3
    assert store.reconcile()['released_intents'] == 1
    fills, orders = store.recorder.get_trades_for_config('modular.yml'), store.recorder.get_orders_for_config_and_market('modular.yml', None)
    store._validate_release(info, receipt, Decimal('1'), fills, orders, claim_for(info.config))
    two_filled = copy.deepcopy(receipt)
    two_filled['orders'][0].update(filled_base='0.5', trade_ids=['ghost'])
    with pytest.raises(PolicyError) as direct:
        store._validate_release(info, two_filled, Decimal('1.5'), fills, orders, claim_for(info.config))
    assert str(direct.value) == 'Single-attempt reconciliation contains multiple filled entry orders'


@pytest.mark.parametrize('damage', ['missing', 'null'])
def test_chain_executor_without_acquisition_chain_cannot_release(damage):
    config = chain_config()
    orders = three_child_orders()
    info = executor_info(config, orders)
    if damage == 'missing':
        del info.custom_info['acquisition_chain']
    else:
        info.custom_info['acquisition_chain'] = None
    store = make_store([info], recorded_fills(orders), recorded_orders(orders))
    with pytest.raises(ValueError, match='Acquisition chain identity or membership is invalid'):
        store.reconcile()
    with pytest.raises(ValueError, match='Acquisition chain identity or membership is invalid'):
        store.assess()
    _, state = store._load()
    assert state['releases'] == {}
    assert state['fills'] == {}
