"""Restart preservation for V2 bounded acquisition chains.

Offline regression only. A planned position resume (stop owner -> preflight copy
-> venue evidence -> prepare -> pending_manifest -> claim -> hydrate) must bring
every order, fill, fee, chain child, stop reason and trailing value back exactly
as the stopped owner recorded them, and must never place a new BUY.
"""
import copy
import hashlib
import json
import sqlite3
from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors import test_planned_position_resume as legacy
from test.test_ok_rsi_down_only_entry_execution import make_config
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import DownOnlyMakerPositionExecutor
from controllers.generic.rsi_modular import get_config_class
from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.model.order import Order
from hummingbot.model.position import Position
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.executors import planned_position_resume as resume
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1, validate_acquisition_chain
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

PAIR = 'ETH-USDC'
OWNER = 'modular_spot_resume'
BASE_FEE = D('.001')
# (client order id, exchange order id, limit price, chain purpose, native cancel source)
TWO_CHILD = (
    ('entry-old', 'venue-old', '100', 'initial', 31),
    ('entry', 'entry', '100', 'post_only_repair', None),
)
THREE_CHILD = (
    ('entry-older', 'venue-older', '100', 'initial', '1'),
    ('entry-old', 'venue-old', '99', 'downward_reprice', 31),
    ('entry', 'entry', '99', 'post_only_repair', None),
)
CHAINS = {'two_child': TWO_CHILD, 'three_child': THREE_CHILD}
# Base increment finer than the fee so the exit can sell the exact net inventory.
RULE = TradingRule(PAIR, min_order_size=D('.01'), min_price_increment=D('.01'),
                   min_base_amount_increment=D('.001'), min_notional_size=D('1'))


@pytest.fixture
def stopped_recorder_scope():
    """Finalize only the recorder created by this test's stopped-owner factory."""
    previous = MarketsRecorder._shared_instance
    try:
        yield
    finally:
        current = MarketsRecorder._shared_instance
        try:
            if current is not None and current is not previous:
                try:
                    current.stop()
                finally:
                    current._sql_manager.engine.dispose()
        finally:
            MarketsRecorder._shared_instance = previous


def make_recorder(db):
    manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                   db_path=str(db))
    return MarketsRecorder(manager, [], 'owner.yml', 'v2_with_controllers',
                           market_data_collection=MarketDataCollectionConfigMap(market_data_collection_enabled=False))


def make_chain_executor():
    """Genuine down-only executor with the V2 chain contract on the legacy strategy/rule mocks."""
    old = legacy.make_executor()
    barriers = old.config.triple_barrier_config.model_copy(update={'open_order_limit_max_age_seconds': 0})
    config = make_config(id=old.config.id, timestamp=old.config.timestamp, controller_id=OWNER,
                         trading_pair=PAIR, triple_barrier_config=barriers,
                         entry_chain_contract=ACQUISITION_CHAIN_V1)
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=RULE):
        executor = DownOnlyMakerPositionExecutor(old._strategy, config)
    executor._status = RunnableStatus.RUNNING
    executor._sleep = AsyncMock()
    executor.get_price = MagicMock(return_value=D('100'))
    executor.get_in_flight_order = MagicMock(return_value=None)
    return executor


def base_fee_object(amount):
    return AddedToCostTradeFee(flat_fees=[TokenAmount('ETH', D(amount))]) if D(amount) else AddedToCostTradeFee()


def make_order(oid, exchange_id, price, filled, state, *, cancel_source=None, fee=D('0')):
    order = InFlightOrder(client_order_id=oid, exchange_order_id=exchange_id, trading_pair=PAIR,
                          order_type=OrderType.LIMIT_MAKER, trade_type=TradeType.BUY, amount=D('1'),
                          price=D(price), creation_timestamp=1, initial_state=state)
    if D(filled):
        order.update_with_trade_update(TradeUpdate(
            trade_id=oid, client_order_id=oid, exchange_order_id=exchange_id, trading_pair=PAIR,
            fill_price=D(price), fill_base_amount=D(filled), fill_quote_amount=D(filled) * D(price),
            fee=base_fee_object(fee), fill_timestamp=2))
    if cancel_source is not None:
        order.cancel_source = cancel_source
    tracked = TrackedOrder(order_id=oid)
    tracked.order = order
    return tracked


def chain_children(spec):
    return [{'generation': index, 'order_id': oid, 'predecessor_id': spec[index - 1][0] if index else None,
             'purpose': purpose, 'submitted_base': '1', 'limit_price': price}
            for index, (oid, _, price, purpose, _) in enumerate(spec)]


def terminal_proof(tracked):
    order = tracked.order
    return {'schema': 1, 'source': 'okx_terminal_status_v1', 'order_id': order.client_order_id,
            'exchange_order_id': order.exchange_order_id, 'trading_pair': PAIR, 'side': 'BUY',
            'amount': str(order.amount), 'filled_base': str(order.executed_amount_base),
            'state': 'CANCELED' if order.current_state is OrderState.CANCELED else 'FILLED',
            'trade_ids': sorted(str(key) for key in order.order_fills)}


def write_runtime_root(root):
    for file in (*resume.SOURCES, 'conf/scripts/owner.yml', 'conf/controllers/owner.yml',
                 'conf/conf_client.yml', 'conf/connectors/okx.yml'):
        path = root / file
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text((resume.ROOT / file).read_text() if file in resume.SOURCES else 'id: owner\n')
    (root / 'conf/scripts/owner.yml').write_text(
        'script_file_name: v2_with_controllers.py\ncontrollers_config: [owner.yml]\n')
    (root / 'conf/controllers/owner.yml').write_text(
        f'id: {OWNER}\nconnector_name: okx\ntrading_pair: {PAIR}\n')


async def stopped_chain_fixture(tmp_path, spec, *, armed=True, base_fee=BASE_FEE):
    """Stopped owner with a CLOSED chain: zero-fill CANCELED predecessors then one full fill.

    Mirrors legacy.stopped_fixture but builds the chain from `spec` so a
    three-generation chain and real base fees are covered.
    """
    db = tmp_path / 'stopped.sqlite'
    preflight = tmp_path / 'preflight.sqlite'
    root = tmp_path / 'runtime'
    write_runtime_root(root)
    recorder = make_recorder(db)
    executor = make_chain_executor()
    *predecessor_specs, final_spec = spec
    predecessors = [make_order(oid, xid, price, '0', OrderState.CANCELED, cancel_source=source)
                    for oid, xid, price, _, source in predecessor_specs]
    final = make_order(final_spec[0], final_spec[1], final_spec[2], '1', OrderState.FILLED, fee=base_fee)
    executor._completed_open_orders = predecessors
    executor._open_order = final
    executor._entry_submitted = True
    executor._entry_reprice_count = len(predecessors)
    executor._entry_stop_reason = 'fully_acquired'
    executor._entry_chain_children = chain_children(spec)
    executor._entry_lifecycle.update(state='managing_fill', order_id=final_spec[0], generation=len(predecessors))
    executor._entry_child_lifecycles = {
        oid: {'cancel_source': source, 'replacement_purpose': spec[index + 1][3]}
        for index, (oid, _, _, _, source) in enumerate(predecessor_specs)}
    executor._entry_reconciliation = {'schema': 1, 'source': 'native_terminal_entry_set_v1',
                                      'orders': [terminal_proof(item) for item in (*predecessors, final)]}
    if armed:
        executor._trailing_stop_trigger_pct = D('.035')
        executor._trailing_stop_move_count = 4
        executor._trailing_stop_armed_timestamp = 100
        executor._trailing_stop_last_update_timestamp = 120
    recorder.store_or_update_executor(executor)
    with sqlite3.connect(db) as source, sqlite3.connect(preflight) as destination:
        source.backup(destination)
    executor.early_stop(keep_position=True)
    await executor.control_task()
    assert executor.is_closed
    recorder.store_or_update_executor(executor)
    recorder.store_or_update_position_hold_ledger_entries(OWNER, executor.config.id, 'okx', PAIR,
                                                          executor._held_position_orders)
    entry = executor._held_position_orders[0]
    net = D(entry['executed_amount_base']) - D(base_fee)
    with recorder.sql_manager.get_new_session() as session:
        for oid, xid, price, _, _ in predecessor_specs:
            session.add(Order(id=oid, config_file_path='owner.yml', strategy='v2_with_controllers', market='okx',
                              symbol=PAIR, base_asset='ETH', quote_asset='USDC', creation_timestamp=1000,
                              order_type='LIMIT', amount=D('1'), leverage=1, price=D(price),
                              last_status='OrderCancelled', last_update_timestamp=2000, exchange_order_id=xid,
                              position='NIL'))
        session.add(Order(id=entry['client_order_id'], config_file_path='owner.yml', strategy='v2_with_controllers',
                          market='okx', symbol=PAIR, base_asset='ETH', quote_asset='USDC', creation_timestamp=1000,
                          order_type='LIMIT', amount=D(entry['amount']), leverage=1, price=D(entry['price']),
                          last_status='BuyOrderCompleted', last_update_timestamp=2000,
                          exchange_order_id=entry['exchange_order_id'], position='NIL'))
        for tid, fill in entry['order_fills'].items():
            session.add(TradeFill(config_file_path='owner.yml', strategy='v2_with_controllers', market='okx',
                                  symbol=PAIR, base_asset='ETH', quote_asset='USDC', timestamp=2000,
                                  order_id=entry['client_order_id'], trade_type='BUY', order_type='LIMIT',
                                  price=D(fill['fill_price']), amount=D(fill['fill_base_amount']), leverage=1,
                                  trade_fee=fill['fee'], trade_fee_in_quote=D(0), exchange_trade_id=tid,
                                  position='NIL'))
        session.add(Position(id='aggregate', controller_id=OWNER, connector_name='okx', side='BUY',
                             trading_pair=PAIR, timestamp=1000, volume_traded_quote=D(entry['executed_amount_quote']),
                             amount=net, breakeven_price=D(entry['price']), unrealized_pnl_quote=D('0'),
                             realized_pnl_quote=D('0'), cum_fees_quote=D('0')))
        session.commit()
    fills = {tid: {'fill_base_amount': fill['fill_base_amount'], 'fill_price': fill['fill_price'],
                   'fee': fill['fee']} for tid, fill in entry['order_fills'].items()}
    custom = executor.get_custom_info()
    evidence = {
        'schema': 1, 'owner': 'test-owner', 'script_config': 'owner.yml', 'owner_stopped': True,
        'venue_open_orders': [], 'observed_at': 1000, 'expires_at': 1800,
        'config_paths': ['conf/scripts/owner.yml', 'conf/controllers/owner.yml', 'conf/conf_client.yml',
                         'conf/connectors/okx.yml'],
        'executors': [{'executor_id': executor.config.id, 'client_order_id': entry['client_order_id'],
                       'exchange_order_id': entry['exchange_order_id'], 'trading_pair': PAIR, 'state': 'filled',
                       'filled_base': entry['executed_amount_base'], 'filled_quote': entry['executed_amount_quote'],
                       'net_base': str(net), 'fills': fills,
                       'entry_reconciliation': custom['entry_reconciliation'],
                       'child_fills': {entry['client_order_id']: fills}}],
    }
    from datetime import datetime, timezone
    with sqlite3.connect(db) as checkpoint:
        checkpoint.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    sha = hashlib.sha256(db.read_bytes()).hexdigest()
    pending = [{'pair': PAIR, 'regular': [], 'regular_count': 0, 'algo_count': 0,
                'algos': {kind: [] for kind in resume.SPOT_ALGO_TYPES}}]
    venue_orders = [{'raw_order': {'clOrdId': oid, 'ordId': xid, 'instId': PAIR, 'side': 'buy',
                                   'state': 'canceled', 'tdMode': 'cash', 'sz': '1', 'accFillSz': '0'},
                     'raw_fills': [], 'receipt': {'blockers': []}, 'local_fill_comparisons': []}
                    for oid, xid, _, _, _ in predecessor_specs]
    venue_orders.append({
        'raw_order': {'clOrdId': entry['client_order_id'], 'ordId': entry['exchange_order_id'], 'instId': PAIR,
                      'side': 'buy', 'state': 'filled', 'tdMode': 'cash', 'sz': entry['amount'],
                      'accFillSz': entry['executed_amount_base']},
        'raw_fills': [{'tradeId': tid, 'ordId': entry['exchange_order_id'], 'instId': PAIR, 'side': 'buy',
                       'fillSz': fill['fill_base_amount'], 'fillPx': fill['fill_price'],
                       'fee': str(-D(base_fee)), 'feeCcy': 'ETH'} for tid, fill in fills.items()],
        'receipt': {'blockers': []}, 'local_fill_comparisons': [{'matched': True}]})
    evidence['reconciliation'] = {
        'schema_version': 1, 'kind': 'okx_stopped_owner_get_reconciliation', 'venue_queried': True,
        'read_only': True, 'result': 'EVIDENCE_COMPLETE', 'blockers': [], 'owner': 'test-owner',
        'database': {'sha256': sha, 'sha256_after': sha},
        'started_at': datetime.fromtimestamp(990, timezone.utc).isoformat(),
        'finished_at': datetime.fromtimestamp(1000, timezone.utc).isoformat(),
        'controller_pairs': {OWNER: PAIR}, 'selected_executor_ids': [executor.config.id],
        'pending_before': pending, 'pending_after': pending,
        'balance_coverage': [{'asset': 'ETH', 'owned_residual': str(net), 'cash_balance': '10',
                              'available_balance': '10', 'covered': True}],
        'selected_executors': [{'executor_id': executor.config.id, 'controller_id': OWNER, 'pair': PAIR,
                                'venue_orders': venue_orders}],
    }
    return db, preflight, root, recorder, executor, evidence


def ready_strategy(old):
    strategy = old._strategy
    controller_config = get_config_class('ok_rsi')(profile='ok_rsi', id=OWNER, trading_pair=PAIR,
                                                   entry_acquisition_policy='down_only_maker_v1')
    strategy.controllers = {OWNER: SimpleNamespace(config=controller_config)}
    strategy.markets = {'okx': {PAIR}}
    strategy.ready_to_trade = True
    strategy.market_data_provider = MagicMock(ready=True)
    strategy.market_data_provider.get_price_by_type.return_value = D('100')
    connector = strategy.connectors['okx']
    connector.in_flight_orders = {}
    connector.get_price_by_type.return_value = D('100')
    connector._order_tracker.fetch_order.return_value = None
    connector.get_available_balance.return_value = D('10')
    return strategy


class orchestrator_patches:
    """Bind the orchestrator's module-level resume helpers to a fixed clock and runtime root."""

    def __init__(self, recorder, root, old, now):
        self._patches = [
            patch.object(MarketsRecorder, 'get_instance', return_value=recorder),
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=now, root=root)),
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim',
                  side_effect=lambda path, value: resume.claim(path, value, now=now)),
            patch.object(PositionExecutor, 'get_trading_rules', return_value=RULE),
        ]

    def __enter__(self):
        for item in self._patches:
            item.start()
        return self

    def __exit__(self, *exc):
        for item in reversed(self._patches):
            item.stop()
        return False


def hydrate_directly(db, root, old, now=1001):
    """prepare has already run; consume the manifest exactly as the orchestrator would."""
    manifest = resume.pending_manifest(db, now=now, root=root)
    position = manifest['positions'][0]
    resume.claim(db, manifest, now=now)
    strategy = old._strategy
    strategy.connectors['okx'].get_available_balance.return_value = D('10')
    strategy.executor_orchestrator = SimpleNamespace(recovery_required={})
    config = resume.parse_position_config(position['config'])
    strategy.connectors['okx'].get_price_by_type.return_value = D('100')
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=RULE):
        recovered = DownOnlyMakerPositionExecutor(strategy, config)
    recovered.hydrate_planned_position(position, manifest['id'])
    # Same mocks as the stopped owner: a missing connector lookup must not replace a sealed order.
    recovered.get_price = MagicMock(return_value=D('100'))
    recovered.get_in_flight_order = MagicMock(return_value=None)
    return recovered, manifest, position


def assert_exact_restart_state(recovered, old, stopped_custom, spec, manifest_id, base_fee=BASE_FEE):
    predecessor_ids = [oid for oid, *_ in spec[:-1]]
    final_id = spec[-1][0]
    net = D('1') - D(base_fee)
    assert type(recovered) is DownOnlyMakerPositionExecutor
    assert recovered.config == old.config
    assert recovered.config.entry_chain_contract == ACQUISITION_CHAIN_V1
    assert recovered._planned_resume_id == manifest_id
    # Every child order, including terminal predecessors, is byte-identical to the stopped owner's
    # persisted snapshots (the recorder row is the restart truth, not the in-memory object).
    snapshots = stopped_custom['execution_order_snapshots']
    assert [row['client_order_id'] for row in snapshots] == [*predecessor_ids, final_id]
    assert [order.order_id for order in recovered._completed_open_orders] == predecessor_ids
    for restored, snapshot in zip(recovered._completed_open_orders, snapshots[:-1]):
        assert restored.order.current_state is OrderState.CANCELED
        assert restored.order.executed_amount_base == 0
        assert restored.order.order_fills == {}
        assert restored.order.to_json() == snapshot
    assert recovered._open_order.order_id == final_id
    assert recovered._open_order.order.current_state is OrderState.FILLED
    assert recovered._open_order.order.to_json() == snapshots[-1]
    assert list(recovered._open_order.order.order_fills) == list(old._open_order.order.order_fills)
    for trade_id, fill in recovered._open_order.order.order_fills.items():
        original = old._open_order.order.order_fills[trade_id]
        assert fill.fill_base_amount == original.fill_base_amount
        assert fill.fill_price == original.fill_price
        assert fill.fill_quote_amount == original.fill_quote_amount
        assert fill.fee.to_json() == original.fee.to_json()
        assert fill.fee.to_json() == base_fee_object(base_fee).to_json()
    assert recovered._entry_reconciliation == stopped_custom['entry_reconciliation']
    custom = recovered.get_custom_info()
    assert custom['acquisition_chain'] == stopped_custom['acquisition_chain']
    assert custom['acquisition_chain']['membership_closed'] is True
    assert custom['acquisition_chain']['successor_pending'] is None
    assert custom['further_acquisition_permission'] == 'closed'
    assert custom['acquisition_outcome'] == 'fully_acquired'
    assert custom['execution_order_snapshots'] == stopped_custom['execution_order_snapshots']
    assert custom['order_ids'] == stopped_custom['order_ids'] == [*predecessor_ids, final_id]
    for oid, lifecycle in stopped_custom['entry_child_lifecycles'].items():
        assert custom['entry_child_lifecycles'][oid] == lifecycle
    assert custom['planned_position_resume'] == manifest_id
    assert custom['resume_conflicting_fill'] is None
    assert recovered._entry_reprice_count == len(spec) - 1
    assert recovered._entry_replacement_purpose is None
    assert recovered._entry_child_submission_authorized is False
    assert recovered._trailing_stop_trigger_pct == old._trailing_stop_trigger_pct == D('.035')
    assert recovered._trailing_stop_move_count == old._trailing_stop_move_count == 4
    assert recovered._trailing_stop_armed_timestamp == old._trailing_stop_armed_timestamp == 100
    assert recovered._trailing_stop_last_update_timestamp == old._trailing_stop_last_update_timestamp == 120
    assert custom['trailing_state'] == 'armed'
    assert D(str(custom['trailing_stop_trigger_pct'])) == D('.035')
    assert recovered.amount_to_close == net
    assert recovered.open_filled_amount == net
    assert D(str(custom['remaining_position_amount_base'])) == D(str(stopped_custom['remaining_position_amount_base'])) == net


@pytest.mark.asyncio
@pytest.mark.parametrize('shape', ['two_child', 'three_child'])
async def test_orchestrator_round_trip_preserves_closed_chain_and_places_no_buy(tmp_path, shape, stopped_recorder_scope):
    spec = CHAINS[shape]
    db, before, root, recorder, old, evidence = await stopped_chain_fixture(tmp_path, spec)
    stopped_row = recorder.get_all_executors()[0]
    assert stopped_row.is_done
    stopped_custom = copy.deepcopy(stopped_row.custom_info)
    assert len(stopped_custom['acquisition_chain']['children']) == len(spec)
    manifest = resume.prepare(db, before, evidence, now=1000, root=root)
    assert resume.pending_manifest(db, now=1001, root=root) == manifest
    strategy = ready_strategy(old)
    connector = strategy.connectors['okx']
    with orchestrator_patches(recorder, root, old, now=1001), patch.object(PositionExecutor, 'start') as start:
        orchestrator = ExecutorOrchestrator(strategy)
        assert orchestrator.resume_planned_positions_if_ready()
        start.assert_called_once()
        assert resume.inspect_manifest(db)[0]['state'] == 'claimed'
        recovered = orchestrator.active_executors[OWNER][0]
        assert_exact_restart_state(recovered, old, stopped_custom, spec, manifest['id'])
        assert orchestrator.recovery_required == {}
        # No new BUY: neither the entry control loop nor the strategy may submit an order.
        recovered._status = RunnableStatus.RUNNING
        recovered.place_order = MagicMock(return_value='protective-exit')
        recovered._sleep = AsyncMock()
        recovered.place_open_order()
        recovered.control_open_order()
        recovered.place_order.assert_not_called()
        strategy.buy.assert_not_called()
        assert recovered.get_custom_info()['acquisition_chain'] == stopped_custom['acquisition_chain']
        # A price move that crosses the armed trailing trigger sells exactly the net inventory once.
        trigger_price = D(spec[-1][2]) * D('1.03')
        connector.get_price_by_type.return_value = trigger_price
        strategy.market_data_provider.get_price_by_type.return_value = trigger_price
        await recovered.control_task()
        await recovered.control_task()
        recovered.place_order.assert_called_once()
        assert recovered.place_order.call_args.kwargs['side'] is TradeType.SELL
        assert recovered.place_order.call_args.kwargs['amount'] == D('1') - BASE_FEE
        assert recovered.close_type == CloseType.TRAILING_STOP
        strategy.buy.assert_not_called()
        # The chain itself is untouched by the exit.
        assert recovered.get_custom_info()['acquisition_chain'] == stopped_custom['acquisition_chain']
        assert [order.order_id for order in recovered._completed_open_orders] == [oid for oid, *_ in spec[:-1]]


@pytest.mark.asyncio
async def test_resume_keeps_the_original_acquisition_stop_reason(tmp_path, stopped_recorder_scope):
    # Regression: hydrate_planned_position used to replace the owner's terminal reason
    # with a resume marker. The manifest id already lives in
    # custom_info['planned_position_resume']; the original stop reason must survive.
    db, before, root, recorder, old, evidence = await stopped_chain_fixture(tmp_path, TWO_CHILD)
    stopped_custom = copy.deepcopy(recorder.get_all_executors()[0].custom_info)
    assert stopped_custom['entry_acquisition']['terminal_reason'] == 'fully_acquired'
    resume.prepare(db, before, evidence, now=1000, root=root)
    recovered, _, _ = hydrate_directly(db, root, old)
    custom = recovered.get_custom_info()
    assert custom['entry_acquisition']['terminal_reason'] == stopped_custom['entry_acquisition']['terminal_reason']
    assert custom['entry_lifecycle']['state'] == stopped_custom['entry_lifecycle']['state'] == 'managing_fill'


@pytest.mark.asyncio
@pytest.mark.parametrize('shape', ['two_child', 'three_child'])
async def test_persisting_the_resumed_executor_is_idempotent(tmp_path, shape, stopped_recorder_scope):
    spec = CHAINS[shape]
    db, before, root, recorder, old, evidence = await stopped_chain_fixture(tmp_path, spec)
    stopped_custom = copy.deepcopy(recorder.get_all_executors()[0].custom_info)
    resume.prepare(db, before, evidence, now=1000, root=root)
    recovered, manifest, _ = hydrate_directly(db, root, old)
    assert_exact_restart_state(recovered, old, stopped_custom, spec, manifest['id'])
    chain = validate_acquisition_chain(recovered.config, recovered.get_custom_info(), recovered._entry_reconciliation)
    assert chain == stopped_custom['acquisition_chain']
    recorder.store_or_update_executor(recovered)
    rows = recorder.get_all_executors()
    assert len(rows) == 1
    persisted = rows[0]
    assert persisted.config == old.config
    assert persisted.custom_info['acquisition_chain'] == stopped_custom['acquisition_chain']
    assert persisted.custom_info['entry_reconciliation'] == stopped_custom['entry_reconciliation']
    assert persisted.custom_info['execution_order_snapshots'] == stopped_custom['execution_order_snapshots']
    assert persisted.custom_info['order_ids'] == stopped_custom['order_ids']
    assert persisted.custom_info['planned_position_resume'] == manifest['id']
    for key in resume.TRAILING_KEYS:
        assert persisted.custom_info[key] == stopped_custom[key]
    assert validate_acquisition_chain(persisted.config, persisted.custom_info,
                                      persisted.custom_info['entry_reconciliation']) == chain
    # Storing again changes nothing.
    recorder.store_or_update_executor(recovered)
    again = recorder.get_all_executors()[0]
    assert again.custom_info['acquisition_chain'] == persisted.custom_info['acquisition_chain']
    assert again.custom_info['execution_order_snapshots'] == persisted.custom_info['execution_order_snapshots']


@pytest.mark.asyncio
async def test_second_handover_keeps_chain_and_consumed_history(tmp_path, stopped_recorder_scope):
    spec = TWO_CHILD
    db, before, root, recorder, old, evidence = await stopped_chain_fixture(tmp_path, spec)
    stopped_custom = copy.deepcopy(recorder.get_all_executors()[0].custom_info)
    first = resume.prepare(db, before, evidence, now=1000, root=root)
    strategy = ready_strategy(old)
    second_preflight = tmp_path / 'second-preflight.sqlite'
    with orchestrator_patches(recorder, root, old, now=1001), patch.object(PositionExecutor, 'start'):
        orchestrator = ExecutorOrchestrator(strategy)
        assert orchestrator.resume_planned_positions_if_ready()
        recovered = orchestrator.active_executors[OWNER][0]
        assert_exact_restart_state(recovered, old, stopped_custom, spec, first['id'])
        consumed = resume.inspect_manifest(db)[0]
        assert consumed['state'] == 'claimed'
        recovered._status = RunnableStatus.RUNNING
        recovered._sleep = AsyncMock()
        recovered.place_order = MagicMock()
        recorder.store_or_update_executor(recovered)
        with sqlite3.connect(db) as source, sqlite3.connect(second_preflight) as destination:
            source.backup(destination)
        recovered.early_stop(keep_position=True)
        await recovered.control_task()
        assert recovered.is_closed
        result = await orchestrator.stop(0, keep_position_by_controller={OWNER: True}, strict=True)
        assert result.reconciliation_complete
        recovered.place_order.assert_not_called()
        strategy.buy.assert_not_called()
    restopped = recorder.get_all_executors()[0]
    assert restopped.is_done
    assert restopped.custom_info['acquisition_chain'] == stopped_custom['acquisition_chain']
    assert restopped.custom_info['execution_order_snapshots'] == stopped_custom['execution_order_snapshots']
    assert restopped.custom_info['entry_reconciliation'] == stopped_custom['entry_reconciliation']
    fresh = legacy.refresh_stopped_evidence(db, evidence, 1100)
    second = resume.prepare(db, second_preflight, fresh, now=1100, root=root)
    assert second['id'] != first['id']
    assert second['claimed_history_digest'] == resume.digest([consumed])
    for key in ('executor_id', 'config', 'entry', 'entry_orders', 'entry_reconciliation', 'acquisition_chain',
                'trailing', 'net_base'):
        assert second['positions'][0][key] == first['positions'][0][key], key
    assert [child['order_id'] for child in second['positions'][0]['acquisition_chain']['children']] == [
        oid for oid, *_ in spec]
    assert resume.inspect_manifest(db)[0] == consumed
    assert resume.pending_manifest(db, now=1101, root=root) == second
    second_recovered, second_manifest, _ = hydrate_directly(db, root, old, now=1101)
    assert second_manifest['id'] == second['id']
    assert_exact_restart_state(second_recovered, old, stopped_custom, spec, second['id'])
    rows = resume.inspect_manifest(db)
    assert len(rows) == 2 and all(row['state'] == 'claimed' for row in rows)
    assert next(row for row in rows if row['id'] == first['id']) == consumed
    with pytest.raises(ValueError, match='consumed'):
        resume.claim(db, first, now=1102)
    assert resume.pending_manifest(db, now=1102, root=root) is None


def extra_buy_snapshot(custom):
    extra = copy.deepcopy(custom['execution_order_snapshots'][-1])
    extra['client_order_id'] = 'entry-extra'
    extra['exchange_order_id'] = 'venue-extra'
    for fill in extra.get('order_fills', {}).values():
        fill['client_order_id'] = 'entry-extra'
        fill['exchange_order_id'] = 'venue-extra'
    return extra


def tamper_fill_amount(custom):
    custom['execution_order_snapshots'][-1]['order_fills']['entry']['fill_base_amount'] = '0.9'


def tamper_predecessor_fill(custom):
    custom['execution_order_snapshots'][0]['executed_amount_base'] = '0.5'


def tamper_child_removed(custom):
    custom['acquisition_chain']['children'].pop(0)


def tamper_trailing_regression(custom):
    custom['trailing_stop_trigger_pct'] = '0.03'


def tamper_terminal_reordered(custom):
    custom['entry_reconciliation']['orders'].reverse()


def tamper_terminal_removed(custom):
    custom['entry_reconciliation']['orders'].pop(0)


def tamper_extra_buy(custom):
    custom['execution_order_snapshots'].append(extra_buy_snapshot(custom))


def tamper_chain_gross(custom):
    custom['acquisition_chain']['gross_filled_base'] = '0.5'


def tamper_terminal_filled(custom):
    custom['entry_reconciliation']['orders'][-1]['filled_base'] = '0'


STOPPED_ONLY_TAMPERS = {
    'fill_amount_changed': (tamper_fill_amount, 'preflight acquisition order receipt differs'),
    'predecessor_fill_nonzero': (tamper_predecessor_fill, 'preflight acquisition order receipt differs'),
    'chain_child_removed': (tamper_child_removed, 'preflight acquisition chain differs'),
    'trailing_regressed': (tamper_trailing_regression, 'trailing high-water regressed'),
    # Regression: prepare venue-checks the evidence's terminal set, then seals the stopped
    # row's copy. The two must be identical child for child, so a reordered persisted set
    # is refused instead of being sealed unchecked.
    'terminal_set_reordered': (tamper_terminal_reordered, 'terminal proof|reconciliation'),
    'terminal_proof_removed': (tamper_terminal_removed, 'terminal proof coverage differs'),
    'extra_buy_snapshot': (tamper_extra_buy, 'preflight acquisition snapshot coverage differs'),
}


def mutate_executor_row(path, executor_id, mutate):
    with sqlite3.connect(path) as connection:
        row = connection.execute('SELECT config, custom_info FROM Executors WHERE id=?', (executor_id,)).fetchone()
        config, custom = map(json.loads, row)
        mutate(custom)
        connection.execute('UPDATE Executors SET config=?, custom_info=? WHERE id=?',
                           (json.dumps(config), json.dumps(custom), executor_id))


@pytest.mark.asyncio
@pytest.mark.parametrize('tamper', sorted(STOPPED_ONLY_TAMPERS))
async def test_prepare_rejects_tampered_stopped_row_without_mutating_owner(tmp_path, tamper, stopped_recorder_scope):
    mutate, reason = STOPPED_ONLY_TAMPERS[tamper]
    db, before, root, recorder, old, evidence = await stopped_chain_fixture(tmp_path, TWO_CHILD)
    mutate_executor_row(db, old.config.id, mutate)
    fresh = legacy.refresh_stopped_evidence(db, evidence, 1000)
    preflight_sha = hashlib.sha256(before.read_bytes()).hexdigest()
    stopped_sha = hashlib.sha256(db.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match=reason):
        resume.prepare(db, before, fresh, now=1000, root=root)
    assert hashlib.sha256(db.read_bytes()).hexdigest() == stopped_sha
    assert hashlib.sha256(before.read_bytes()).hexdigest() == preflight_sha
    assert resume.inspect_manifest(db) == []
    assert recorder.get_all_executors()[0].is_done
    assert resume.pending_manifest(db, now=1001, root=root) is None


BOTH_CAPTURE_TAMPERS = {
    'fill_amount_changed': (tamper_fill_amount, 'native fill total differs'),
    'predecessor_fill_nonzero': (tamper_predecessor_fill, 'fill total differs|fills differ'),
    'chain_child_removed': (tamper_child_removed, 'child identity is invalid|native child coverage differs'),
    'extra_buy_snapshot': (tamper_extra_buy, 'native child coverage differs'),
    'chain_gross_changed': (tamper_chain_gross, 'gross fill snapshot differs'),
    'terminal_filled_changed': (tamper_terminal_filled, 'terminal quantity or fills differ'),
    'terminal_proof_removed': (tamper_terminal_removed, 'terminal proof coverage differs'),
}


@pytest.mark.asyncio
@pytest.mark.parametrize('tamper', sorted(BOTH_CAPTURE_TAMPERS))
async def test_validate_position_rejects_chain_damage_present_in_both_captures(tmp_path, tamper, stopped_recorder_scope):
    """The preflight diff is not the only guard: the chain reader itself rejects damaged receipts."""
    mutate, reason = BOTH_CAPTURE_TAMPERS[tamper]
    db, before, _, _, _, _ = await stopped_chain_fixture(tmp_path, TWO_CHILD)
    with resume.connect(before) as connection:
        preflight = resume.decode_row(connection.execute('SELECT * FROM Executors').fetchone())
    with resume.connect(db) as connection:
        stopped = resume.decode_row(connection.execute('SELECT * FROM Executors').fetchone())
    assert resume.validate_position(preflight, stopped)['acquisition_chain']['membership_closed'] is True
    for row in (preflight, stopped):
        mutate(row['custom_info'])
    with pytest.raises(ValueError, match=reason):
        resume.validate_position(preflight, stopped)


def make_mid_chain_executor():
    """Predecessor canceled with zero fill; the replacement child is still resting on the book."""
    executor = make_chain_executor()
    predecessor = make_order('entry-old', 'venue-old', '100', '0', OrderState.CANCELED, cancel_source=31)
    resting = make_order('entry', 'entry', '100', '0', OrderState.OPEN)
    executor._completed_open_orders = [predecessor]
    executor._open_order = resting
    executor._entry_submitted = True
    executor._entry_reprice_count = 1
    executor._entry_chain_children = chain_children(TWO_CHILD)
    executor._entry_lifecycle.update(state='awaiting_fill', order_id='entry', generation=1)
    executor._entry_child_lifecycles = {'entry-old': {'cancel_source': 31, 'replacement_purpose': 'post_only_repair'}}
    executor._entry_reconciliation = {'schema': 1, 'source': 'native_terminal_entry_set_v1',
                                      'orders': [terminal_proof(predecessor)]}
    return executor


@pytest.mark.asyncio
async def test_unfinished_chain_is_rejected_by_validate_position(tmp_path, stopped_recorder_scope):
    db = tmp_path / 'running.sqlite'
    recorder = make_recorder(db)
    executor = make_mid_chain_executor()
    custom = executor.get_custom_info()
    assert custom['acquisition_chain']['membership_closed'] is False
    assert custom['further_acquisition_permission'] == 'open'
    recorder.store_or_update_executor(executor)
    with resume.connect(db) as connection:
        stored = resume.decode_row(connection.execute('SELECT * FROM Executors').fetchone())
    assert stored['status'] == 2 and stored['is_active'] and stored['close_type'] is None
    stopped = {**copy.deepcopy(stored), 'status': 4, 'is_active': 0, 'close_type': CloseType.POSITION_HOLD.value}
    with pytest.raises(ValueError, match='still authorizes a successor|unfinished child'):
        resume.validate_position(stored, stopped)
    # Even a persisted claim of closed membership cannot certify a child without a terminal receipt.
    forced_preflight, forced_stopped = copy.deepcopy(stored), copy.deepcopy(stopped)
    for row in (forced_preflight, forced_stopped):
        row['custom_info']['acquisition_chain']['membership_closed'] = True
    with pytest.raises(ValueError, match='terminal proof coverage differs|unfinished child'):
        resume.validate_position(forced_preflight, forced_stopped)


@pytest.mark.asyncio
async def test_plain_restart_with_unfinished_chain_requires_recovery_and_creates_no_executor(tmp_path, stopped_recorder_scope):
    db = tmp_path / 'running.sqlite'
    recorder = make_recorder(db)
    executor = make_mid_chain_executor()
    recorder.store_or_update_executor(executor)
    assert not recorder.get_all_executors()[0].is_done
    strategy = ready_strategy(executor)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(PositionExecutor, 'get_trading_rules', return_value=RULE), \
            patch.object(PositionExecutor, 'start') as start, \
            patch.object(DownOnlyMakerPositionExecutor, 'hydrate_planned_position') as hydrate:
        orchestrator = ExecutorOrchestrator(strategy)
        assert orchestrator.recovery_required == {OWNER: [executor.config.id]}
        assert orchestrator.active_executors[OWNER] == []
        assert orchestrator.resume_planned_positions_if_ready()
        assert orchestrator.recovery_required == {OWNER: [executor.config.id]}
        assert orchestrator.active_executors[OWNER] == []
        start.assert_not_called()
        hydrate.assert_not_called()
        from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
        with pytest.raises(RuntimeError, match='reconciliation'):
            orchestrator.create_executor(CreateExecutorAction(controller_id=OWNER, executor_config=executor.config))
    strategy.buy.assert_not_called()
    assert resume.inspect_manifest(db) == []
    assert not recorder.get_all_executors()[0].is_done


@pytest.mark.asyncio
async def test_duplicate_final_fill_is_ignored_and_late_predecessor_fill_holds_everything(tmp_path, stopped_recorder_scope):
    db, before, root, recorder, old, evidence = await stopped_chain_fixture(tmp_path, TWO_CHILD)
    stopped_custom = copy.deepcopy(recorder.get_all_executors()[0].custom_info)
    resume.prepare(db, before, evidence, now=1000, root=root)
    recovered, manifest, _ = hydrate_directly(db, root, old)
    assert_exact_restart_state(recovered, old, stopped_custom, TWO_CHILD, manifest['id'])
    recovered._status = RunnableStatus.RUNNING
    recovered._sleep = AsyncMock()
    recovered.place_order = MagicMock()
    net = D('1') - BASE_FEE
    duplicate = OrderFilledEvent(1002, 'entry', PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, D('100'), D('1'),
                                 base_fee_object(BASE_FEE), exchange_trade_id='entry', exchange_order_id='entry')
    recovered.process_order_filled_event(0, None, duplicate)
    assert recovered._resume_conflicting_fill is None
    assert recovered._open_order.order.executed_amount_base == D('1')
    assert list(recovered._open_order.order.order_fills) == ['entry']
    assert recovered._open_order.order.to_json() == stopped_custom['execution_order_snapshots'][-1]
    assert recovered.amount_to_close == net
    assert recovered.get_custom_info()['further_acquisition_permission'] == 'closed'
    assert recovered.get_custom_info()['acquisition_chain'] == stopped_custom['acquisition_chain']
    assert recovered._strategy.executor_orchestrator.recovery_required == {}

    late = OrderFilledEvent(1003, 'entry-old', PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, D('100'), D('.1'),
                            AddedToCostTradeFee(), exchange_trade_id='late-old-fill', exchange_order_id='venue-old')
    recovered.process_order_filled_event(0, None, late)
    assert recovered._entry_lifecycle['state'] == 'reconciliation_required'
    assert recovered._resume_conflicting_fill is not None
    assert recovered._strategy.executor_orchestrator.recovery_required == {OWNER: [old.config.id]}
    custom = recovered.get_custom_info()
    assert custom['further_acquisition_permission'] == 'held'
    assert custom['acquisition_chain']['successor_pending'] is None
    assert custom['acquisition_chain']['children'] == stopped_custom['acquisition_chain']['children']
    # Neither a new BUY nor a SELL may follow: the held inventory waits for explicit reconciliation.
    recovered.place_open_order()
    recovered.control_open_order()
    recovered.place_close_order_and_cancel_open_orders(CloseType.TRAILING_STOP)
    connector = recovered._strategy.connectors['okx']
    connector.get_price_by_type.return_value = D('103')
    await recovered.control_task()
    await recovered.control_task()
    recovered.place_order.assert_not_called()
    recovered._strategy.buy.assert_not_called()
    assert recovered.status == RunnableStatus.RUNNING
    assert recovered.close_type is None
    assert recovered.amount_to_close == net
    assert recovered._open_order.order.to_json() == stopped_custom['execution_order_snapshots'][-1]
