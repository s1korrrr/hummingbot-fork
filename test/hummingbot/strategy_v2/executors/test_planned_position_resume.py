import json
import sqlite3
from decimal import Decimal
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor, tracked
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import MarketOrderFailureEvent, OrderCancelledEvent
from hummingbot.model.order import Order
from hummingbot.model.position import Position
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.executors import planned_position_resume as resume
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.planned_position_resume import validate_position
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType


class ResumeValidationTests(TestCase):
    def setUp(self):
        self.config = PositionExecutorConfig(id='resume-1', controller_id='owner', timestamp=100,
                                             connector_name='okx', trading_pair='ETH-USDT', side=TradeType.BUY,
                                             amount=Decimal('1'), entry_price=Decimal('100'),
                                             triple_barrier_config=TripleBarrierConfig(
                                                trailing_stop=TrailingStop(activation_price=Decimal('.05'),
                                                                           trailing_delta=Decimal('.02'))))
        order = InFlightOrder('entry-1', 'ETH-USDT', OrderType.LIMIT, TradeType.BUY,
                              Decimal('1'), 100, Decimal('100'), exchange_order_id='venue-1')
        order.update_with_trade_update(TradeUpdate('fill-1', 'entry-1', 'venue-1', 'ETH-USDT', 101,
                                                   Decimal('100'), Decimal('1'), Decimal('100'), AddedToCostTradeFee()))
        order.current_state = OrderState.FILLED
        self.order = order.to_json()
        self.custom = {'held_position_orders': [self.order], 'order_ids': ['entry-1'],
                       'open_order_filled_amount_base': '1', 'open_order_filled_amount_quote': '100',
                       'close_order_filled_amount_base': '0', 'remaining_position_amount_base': '1',
                       'trailing_state': 'waiting', 'trailing_stop_trigger_pct': None, 'trailing_move_count': 0,
                       'trailing_armed_timestamp': None, 'trailing_last_update_timestamp': None,
                       'exit_trigger_reason': None}
        self.row = {'id': self.config.id, 'controller_id': 'owner', 'type': 'position_executor',
                    'timestamp': 100, 'config': json.loads(self.config.model_dump_json()), 'custom_info': self.custom,
                    'status': 4, 'close_type': CloseType.POSITION_HOLD.value, 'is_active': 0}
        self.preflight = {**self.row, 'status': 2, 'close_type': None, 'is_active': 1,
                          'custom_info': {**self.custom, 'held_position_orders': []}}

    def test_prepare_scopes_legacy_transition_to_named_executor_in_mixed_manifest(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as directory:
            root = Path(directory)
            for path in ('conf/scripts', 'conf/controllers', 'conf/connectors'):
                (root / path).mkdir(parents=True, exist_ok=True)
            (root / 'conf/scripts/owner.yml').write_text(
                'script_file_name: v2_with_controllers.py\ncontrollers_config: [legacy.yml, singleton.yml]\n')
            (root / 'conf/controllers/legacy.yml').write_text(
                'id: legacy-owner\nconnector_name: okx\ntrading_pair: ETH-USDT\n')
            (root / 'conf/controllers/singleton.yml').write_text(
                'id: singleton-owner\nconnector_name: okx\ntrading_pair: BTC-USDT\n')
            for path in ('conf/conf_client.yml', 'conf/connectors/okx.yml'):
                (root / path).write_text('fixture: true\n')
            evidence = {
                'schema': 1, 'owner_stopped': True, 'venue_open_orders': [],
                'observed_at': 1000, 'expires_at': 1800, 'owner': 'test-owner',
                'script_config': 'owner.yml',
                'config_paths': ['conf/scripts/owner.yml', 'conf/controllers/legacy.yml',
                                 'conf/controllers/singleton.yml', 'conf/conf_client.yml',
                                 'conf/connectors/okx.yml'],
                'executors': [{'executor_id': 'legacy', 'entry_reconciliation': {'source': 'legacy-proof'}},
                              {'executor_id': 'singleton'}],
                'reconciliation': {'controller_pairs': {'legacy-owner': 'ETH-USDT',
                                                        'singleton-owner': 'BTC-USDT'}},
            }
            db, preflight = root / 'stopped.sqlite', root / 'preflight.sqlite'
            for path, status in ((db, 4), (preflight, 2)):
                with sqlite3.connect(path) as connection:
                    connection.execute('CREATE TABLE Executors (id TEXT, controller_id TEXT, timestamp REAL, '
                                       'config TEXT, custom_info TEXT, type TEXT, status INTEGER, is_active INTEGER, '
                                       'close_type TEXT)')
                    connection.execute('CREATE TABLE "Order" (id TEXT, config_file_path TEXT)')
                    for executor_id, controller_id in (('legacy', 'legacy-owner'),
                                                       ('singleton', 'singleton-owner')):
                        connection.execute('INSERT INTO Executors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                           (executor_id, controller_id, 100, '{}', '{}', 'position_executor',
                                            status, int(status == 2), None if status == 2 else 'POSITION_HOLD'))
                        connection.execute('INSERT INTO "Order" VALUES (?, ?)', (f'{executor_id}-entry', 'owner.yml'))
            calls = []

            def validate_position(
                    _before, after, *, allow_legacy_post_only_repair_transition=False, terminal_proofs=None):
                calls.append((after['id'], allow_legacy_post_only_repair_transition, terminal_proofs))
                return {'executor_id': after['id'], 'controller_id': after['controller_id'],
                        'entry': {'client_order_id': f"{after['id']}-entry"},
                        'entry_reconciliation': {'source': f"{after['id']}-proof"},
                        'acquisition_chain': {'children': []} if allow_legacy_post_only_repair_transition else None}

            with patch.object(resume, 'validate_reconciliation'), \
                    patch.object(resume, 'bound_files', return_value={}), \
                    patch.object(resume, 'validate_position', side_effect=validate_position), \
                    patch.object(resume, 'validate_receipts', return_value={}):
                manifest = resume.prepare(db, preflight, evidence, now=1000, root=root,
                                          legacy_transition_executor_ids=('legacy',))
            self.assertEqual([(row[0], row[1]) for row in calls], [('legacy', True), ('singleton', False)])
            self.assertEqual([position['executor_id'] for position in manifest['positions']],
                             ['legacy', 'singleton'])

    def test_valid_terminal_receipt_preserves_original_config(self):
        result = validate_position(self.preflight, self.row)
        self.assertEqual(result['config']['timestamp'], 100)
        self.assertEqual(result['entry']['client_order_id'], 'entry-1')
        self.assertEqual(result['net_base'], '1')

    def test_zero_fill_exit_identity_is_rejected(self):
        self.custom['order_ids'].append('unreported-exit')
        with self.assertRaisesRegex(ValueError, 'singleton'):
            validate_position(self.preflight, self.row)

    def test_preflight_configuration_change_is_rejected(self):
        self.preflight = {**self.preflight, 'config': {**self.row['config'], 'timestamp': 99}}
        with self.assertRaisesRegex(ValueError, 'configuration'):
            validate_position(self.preflight, self.row)

    def test_non_terminal_entry_is_rejected(self):
        self.order['last_state'] = str(OrderState.OPEN.value)
        with self.assertRaisesRegex(ValueError, 'FILLED'):
            validate_position(self.preflight, self.row)

    def test_armed_state_requires_all_original_timestamps(self):
        self.custom.update(trailing_state='armed', trailing_stop_trigger_pct='.04', trailing_move_count=3,
                           trailing_armed_timestamp=110, trailing_last_update_timestamp=120)
        result = validate_position(self.preflight, self.row)
        self.assertEqual(result['trailing']['trailing_move_count'], 3)
        self.custom['trailing_armed_timestamp'] = None
        with self.assertRaisesRegex(ValueError, 'trailing'):
            validate_position(self.preflight, self.row)

    def test_trailing_high_water_cannot_regress_after_preflight(self):
        before = {'trailing_state': 'armed', 'trailing_stop_trigger_pct': '.04',
                  'trailing_move_count': 3, 'trailing_armed_timestamp': 110,
                  'trailing_last_update_timestamp': 120}
        lowered = before | {'trailing_stop_trigger_pct': '.03'}
        with self.assertRaisesRegex(ValueError, 'high-water regressed'):
            resume._validate_trailing_evolution(self.config, before, lowered)
        waiting = {'trailing_state': 'waiting', 'trailing_stop_trigger_pct': None,
                   'trailing_move_count': 0, 'trailing_armed_timestamp': None,
                   'trailing_last_update_timestamp': None}
        with self.assertRaisesRegex(ValueError, 'state regressed'):
            resume._validate_trailing_evolution(self.config, before, waiting)

    def test_trailing_high_water_can_advance_after_preflight(self):
        before = {'trailing_state': 'armed', 'trailing_stop_trigger_pct': '.04',
                  'trailing_move_count': 3, 'trailing_armed_timestamp': 110,
                  'trailing_last_update_timestamp': 120}
        advanced = before | {'trailing_stop_trigger_pct': '.05', 'trailing_move_count': 4,
                             'trailing_last_update_timestamp': 130}
        self.assertEqual(resume._validate_trailing_evolution(self.config, before, advanced), advanced)
        impossible = advanced | {'trailing_move_count': 3}
        with self.assertRaisesRegex(ValueError, 'move count disagree'):
            resume._validate_trailing_evolution(self.config, before, impossible)
        waiting = {'trailing_state': 'waiting', 'trailing_stop_trigger_pct': None,
                   'trailing_move_count': 0, 'trailing_armed_timestamp': None,
                   'trailing_last_update_timestamp': None}
        armed = {'trailing_state': 'armed', 'trailing_stop_trigger_pct': '.02',
                 'trailing_move_count': 1, 'trailing_armed_timestamp': 115,
                 'trailing_last_update_timestamp': 115}
        self.assertEqual(resume._validate_trailing_evolution(self.config, waiting, armed), armed)

    def test_funded_resume_retains_stopped_terminal_entry_proof(self):
        self.config.require_terminal_entry_reconciliation = True
        self.row['config'] = json.loads(self.config.model_dump_json())
        self.preflight['config'] = self.row['config']
        proof = {'schema': 1, 'source': 'okx_terminal_status_v1', 'order_id': 'entry-1',
                 'exchange_order_id': 'venue-1', 'trading_pair': 'ETH-USDT', 'side': 'BUY',
                 'amount': '1', 'filled_base': '1', 'state': 'FILLED', 'trade_ids': ['fill-1']}
        self.custom['entry_reconciliation'] = proof
        position = validate_position(self.preflight, self.row)
        self.assertEqual(position['entry_reconciliation'], proof)
        strategy = MagicMock()
        strategy.connectors = {'okx': MagicMock()}
        strategy.connectors['okx'].get_price_by_type.return_value = Decimal('100')
        with patch.object(PositionExecutor, 'get_trading_rules', return_value=MagicMock()):
            restored = PositionExecutor(strategy, self.config)
        restored.hydrate_planned_position(position, 'sealed')
        self.assertEqual(restored.get_custom_info()['entry_reconciliation'], proof)
        proof['filled_base'] = '0'
        self.assertEqual(restored.get_custom_info()['entry_reconciliation']['filled_base'], '1')
        position['entry_reconciliation']['filled_base'] = '0'
        with patch.object(PositionExecutor, 'get_trading_rules', return_value=MagicMock()):
            changed = PositionExecutor(strategy, self.config)
        with self.assertRaisesRegex(ValueError, 'terminal entry reconciliation'):
            changed.hydrate_planned_position(position, 'sealed')

    def test_funded_resume_rejects_missing_or_changed_terminal_proof(self):
        self.config.require_terminal_entry_reconciliation = True
        self.row['config'] = json.loads(self.config.model_dump_json())
        self.preflight['config'] = self.row['config']
        proof = {'schema': 1, 'source': 'okx_terminal_status_v1', 'order_id': 'entry-1',
                 'exchange_order_id': 'venue-1', 'trading_pair': 'ETH-USDT', 'side': 'BUY',
                 'amount': '1', 'filled_base': '1', 'state': 'FILLED', 'trade_ids': ['fill-1']}
        with self.assertRaisesRegex(ValueError, 'terminal entry reconciliation'):
            validate_position(self.preflight, self.row)
        for change in ({'exchange_order_id': 'different'}, {'filled_base': '0'},
                       {'trade_ids': ['different']}, {'source': 'invented'},
                       {'source': 'native_paper_terminal_v1'}):
            self.custom['entry_reconciliation'] = proof | change
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'terminal entry reconciliation'):
                validate_position(self.preflight, self.row)

    def test_funded_single_attempt_accepts_one_terminal_proof_in_entry_set(self):
        self.config.require_terminal_entry_reconciliation = True
        self.config.entry_order_policy = 'single_attempt'
        self.row['config'] = json.loads(self.config.model_dump_json())
        self.preflight['config'] = self.row['config']
        proof = {'schema': 1, 'source': 'okx_terminal_status_v1', 'order_id': 'entry-1',
                 'exchange_order_id': 'venue-1', 'trading_pair': 'ETH-USDT', 'side': 'BUY',
                 'amount': '1', 'filled_base': '1', 'state': 'FILLED', 'trade_ids': ['fill-1']}
        wrapped = {'schema': 1, 'source': 'native_terminal_entry_set_v1', 'orders': [proof]}
        self.custom['entry_reconciliation'] = wrapped
        self.assertEqual(validate_position(self.preflight, self.row)['entry_reconciliation'], wrapped)
        self.custom['entry_reconciliation'] = {**wrapped, 'orders': [proof, proof]}
        with self.assertRaisesRegex(ValueError, 'terminal entry reconciliation'):
            validate_position(self.preflight, self.row)

    def test_legacy_post_only_repair_transition_requires_exact_persisted_lifecycle(self):
        from test.test_ok_rsi_down_only_entry_execution import make_config

        config = make_config(id='legacy-chain', controller_id='owner', timestamp=100,
                             trading_pair='ETH-USDT', entry_chain_contract=None,
                             triple_barrier_config=TripleBarrierConfig(
                                 trailing_stop=TrailingStop(activation_price=Decimal('.05'),
                                                            trailing_delta=Decimal('.02')),
                                 open_order_type=OrderType.LIMIT_MAKER))
        predecessor = InFlightOrder('entry-old', 'ETH-USDT', OrderType.LIMIT_MAKER, TradeType.BUY,
                                    Decimal('1'), 100, Decimal('100'), exchange_order_id='venue-old')
        predecessor.current_state = OrderState.CANCELED
        predecessor.cancel_source = 31
        final = InFlightOrder('entry-new', 'ETH-USDT', OrderType.LIMIT_MAKER, TradeType.BUY,
                              Decimal('1'), 100, Decimal('100'), exchange_order_id='venue-new')
        final.update_with_trade_update(TradeUpdate('fill-new', 'entry-new', 'venue-new', 'ETH-USDT', 101,
                                                   Decimal('100'), Decimal('1'), Decimal('100'), AddedToCostTradeFee()))
        final.current_state = OrderState.FILLED
        old_ids = ['entry-old', 'entry-new']
        custom = {
            'order_ids': old_ids,
            'execution_order_snapshots': [predecessor.to_json(), final.to_json()],
            'entry_acquisition': {'intent_id': config.id, 'policy': 'down_only_maker_v1',
                                  'order_ids': old_ids, 'reprice_count': 1},
            'entry_lifecycle': {'cancel_source': '31', 'replacement_purpose': 'post_only_repair',
                                'replacement_of_order_id': 'entry-old', 'order_id': 'entry-new',
                                'reprice_count': 1, 'cancel_event_timestamp': 200},
            'held_position_orders': [final.to_json()],
            'remaining_position_amount_base': '1', 'open_order_filled_amount_base': '1',
            'open_order_filled_amount_quote': '100', 'close_order_filled_amount_base': '0',
            'exit_trigger_reason': None, 'trailing_state': 'waiting', 'trailing_stop_trigger_pct': None,
            'trailing_move_count': 0, 'trailing_armed_timestamp': None, 'trailing_last_update_timestamp': None,
        }
        row = {'id': config.id, 'controller_id': config.controller_id, 'timestamp': config.timestamp,
               'config': json.loads(config.model_dump_json()), 'custom_info': custom,
               'type': 'position_executor', 'status': 4, 'is_active': 0,
               'close_type': CloseType.POSITION_HOLD.value}
        before = {**row, 'status': 2, 'is_active': 1, 'close_type': None,
                  'custom_info': json.loads(json.dumps(custom))}
        result = resume.validate_legacy_post_only_repair_transition(before, row)
        self.assertEqual(result['acquisition_chain']['children'][1]['predecessor_id'], 'entry-old')
        self.assertEqual(result['resume_config']['entry_chain_contract'], resume.ACQUISITION_CHAIN_V1)
        self.assertEqual(result['entry_child_lifecycles'], {})
        terminal = {'schema': 1, 'source': 'native_terminal_entry_set_v1', 'orders': [
            {'schema': 1, 'source': 'okx_terminal_status_v1', 'order_id': 'entry-old',
             'exchange_order_id': 'venue-old', 'trading_pair': 'ETH-USDT', 'side': 'BUY',
             'amount': '1', 'filled_base': '0', 'state': 'CANCELED', 'trade_ids': []},
            {'schema': 1, 'source': 'okx_terminal_status_v1', 'order_id': 'entry-new',
             'exchange_order_id': 'venue-new', 'trading_pair': 'ETH-USDT', 'side': 'BUY',
             'amount': '1', 'filled_base': '1', 'state': 'FILLED', 'trade_ids': ['fill-new']},
        ]}
        with self.assertRaisesRegex(ValueError, 'singleton'):
            validate_position(before, row)
        resumed = validate_position(before, row, allow_legacy_post_only_repair_transition=True,
                                    terminal_proofs=terminal)
        self.assertEqual(resumed['config'], row['config'])
        self.assertEqual(resumed['resume_config']['entry_chain_contract'], resume.ACQUISITION_CHAIN_V1)
        row['custom_info']['entry_lifecycle']['cancel_source'] = '1'
        with self.assertRaisesRegex(ValueError, 'source-31'):
            resume.validate_legacy_post_only_repair_transition(before, row)


async def stopped_fixture(tmp_path, armed=False, unrelated_amount='.2', trading_pair='ETH-USDT', controller_id='owner'):
    db = tmp_path / 'stopped.sqlite'
    preflight = tmp_path / 'preflight.sqlite'
    root = tmp_path / 'runtime'
    for file in (*resume.SOURCES, 'conf/scripts/owner.yml', 'conf/controllers/owner.yml', 'conf/conf_client.yml', 'conf/connectors/okx.yml'):
        path = root / file
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text((resume.ROOT / file).read_text() if file in resume.SOURCES else 'id: owner\n')
    (root / 'conf/scripts/owner.yml').write_text('script_file_name: v2_with_controllers.py\ncontrollers_config: [owner.yml]\n')
    (root / 'conf/controllers/owner.yml').write_text(f'id: {controller_id}\nconnector_name: okx\ntrading_pair: {trading_pair}\n')
    manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db))
    recorder = MarketsRecorder(manager, [], 'owner.yml', 'v2_with_controllers',
                               market_data_collection=MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    executor = make_executor()
    executor.config.controller_id = controller_id
    executor.config.trading_pair = trading_pair

    def pair_tracked(*args):
        from hummingbot.strategy_v2.models.executors import TrackedOrder
        original = tracked(*args)
        payload = original.order.to_json()
        payload['trading_pair'] = trading_pair
        for fill in payload['order_fills'].values():
            fill['trading_pair'] = trading_pair
        result = TrackedOrder(original.order_id)
        result.order = InFlightOrder.from_json(payload)
        return result

    executor._open_order = pair_tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    if getattr(executor.config, 'entry_chain_contract', None) == resume.ACQUISITION_CHAIN_V1:
        predecessor = pair_tracked('entry-old', TradeType.BUY, '0', OrderState.CANCELED)
        predecessor.order.order_type = OrderType.LIMIT_MAKER
        predecessor.order.price = Decimal('100')
        predecessor.order.exchange_order_id = 'venue-old'
        predecessor.order.cancel_source = 31
        executor._open_order.order.order_type = OrderType.LIMIT_MAKER
        executor._completed_open_orders = [predecessor]
        executor._entry_reprice_count = 1
        executor._entry_stop_reason = 'fully_acquired'
        executor._entry_chain_children = [
            {'generation': 0, 'order_id': 'entry-old', 'predecessor_id': None, 'purpose': 'initial',
             'submitted_base': '1', 'limit_price': '100'},
            {'generation': 1, 'order_id': 'entry', 'predecessor_id': 'entry-old',
             'purpose': 'post_only_repair', 'submitted_base': '1', 'limit_price': '100'},
        ]
        executor._entry_lifecycle.update(state='managing_fill', order_id='entry', generation=1)
        executor._entry_child_lifecycles = {
            'entry-old': {'cancel_source': 31, 'replacement_purpose': 'post_only_repair'}}
        terminal = []
        for tracked_order in (predecessor, executor._open_order):
            order = tracked_order.order
            terminal.append({'schema': 1, 'source': 'okx_terminal_status_v1',
                             'order_id': order.client_order_id, 'exchange_order_id': order.exchange_order_id,
                             'trading_pair': trading_pair, 'side': 'BUY', 'amount': str(order.amount),
                             'filled_base': str(order.executed_amount_base),
                             'state': 'CANCELED' if order.current_state is OrderState.CANCELED else 'FILLED',
                             'trade_ids': sorted(str(key) for key in order.order_fills)})
        executor._entry_reconciliation = {
            'schema': 1, 'source': 'native_terminal_entry_set_v1', 'orders': terminal}
    if armed:
        executor._trailing_stop_trigger_pct = Decimal('.035')
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
    recorder.store_or_update_position_hold_ledger_entries(controller_id, executor.config.id, 'okx', trading_pair,
                                                          executor._held_position_orders)
    # An independent held receipt in the SAME pair must survive ownership transfer.
    unrelated = pair_tracked('unrelated', TradeType.BUY, unrelated_amount, OrderState.FILLED).order.to_json()
    if Decimal(unrelated_amount):
        recorder.store_or_update_position_hold_ledger_entries(controller_id, 'other-executor', 'okx', trading_pair, [unrelated])
    with manager.get_new_session() as session:
        receipts = [executor._held_position_orders[0]] + ([unrelated] if Decimal(unrelated_amount) else [])
        if getattr(executor.config, 'entry_chain_contract', None) == resume.ACQUISITION_CHAIN_V1:
            session.add(Order(id='entry-old', config_file_path='owner.yml', strategy='v2_with_controllers', market='okx',
                              symbol=trading_pair, base_asset='ETH', quote_asset=trading_pair.split('-')[1],
                              creation_timestamp=1000, order_type='LIMIT', amount=Decimal('1'), leverage=1,
                              price=Decimal('100'), last_status='OrderCancelled', last_update_timestamp=2000,
                              exchange_order_id='venue-old', position='NIL'))
        for receipt in receipts:
            oid = receipt['client_order_id']
            session.add(Order(id=oid, config_file_path='owner.yml', strategy='v2_with_controllers', market='okx',
                              symbol=trading_pair, base_asset='ETH', quote_asset=trading_pair.split('-')[1], creation_timestamp=1000,
                              order_type='LIMIT', amount=Decimal(receipt['amount']), leverage=1, price=Decimal('100'),
                              last_status='BuyOrderCompleted', last_update_timestamp=2000, exchange_order_id=oid,
                              position='NIL'))
            for tid, fill in receipt['order_fills'].items():
                session.add(TradeFill(config_file_path='owner.yml', strategy='v2_with_controllers', market='okx',
                                      symbol=trading_pair, base_asset='ETH', quote_asset=trading_pair.split('-')[1], timestamp=2000,
                                      order_id=oid, trade_type='BUY', order_type='LIMIT', price=Decimal(fill['fill_price']),
                                      amount=Decimal(fill['fill_base_amount']), leverage=1, trade_fee=fill['fee'],
                                      trade_fee_in_quote=Decimal(0), exchange_trade_id=tid, position='NIL'))
        session.add(Position(id='aggregate', controller_id=controller_id, connector_name='okx', side='BUY',
                             trading_pair=trading_pair, timestamp=1000, volume_traded_quote=Decimal('120'),
                             amount=Decimal('1') + Decimal(unrelated_amount), breakeven_price=Decimal('100'),
                             unrealized_pnl_quote=Decimal('0'), realized_pnl_quote=Decimal('0'), cum_fees_quote=Decimal('0')))
        session.commit()
    entry = executor._held_position_orders[0]
    evidence = {'schema': 1, 'owner': 'test-owner', 'script_config': 'owner.yml', 'owner_stopped': True, 'venue_open_orders': [], 'observed_at': 1000,
                'expires_at': 1800, 'config_paths': ['conf/scripts/owner.yml', 'conf/controllers/owner.yml', 'conf/conf_client.yml', 'conf/connectors/okx.yml'],
                'executors': [{'executor_id': executor.config.id, 'client_order_id': 'entry',
                               'exchange_order_id': 'entry', 'trading_pair': trading_pair, 'state': 'filled',
                               'filled_base': '1', 'filled_quote': '100', 'net_base': '1',
                               'fills': {tid: {'fill_base_amount': f['fill_base_amount'], 'fill_price': f['fill_price'],
                                               'fee': f['fee']} for tid, f in entry['order_fills'].items()}}]}
    import hashlib
    from datetime import datetime, timezone
    with sqlite3.connect(db) as checkpoint:
        checkpoint.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    sha = hashlib.sha256(db.read_bytes()).hexdigest()
    pending = [{'pair': trading_pair, 'regular': [], 'regular_count': 0, 'algo_count': 0, 'algos': {kind: [] for kind in resume.SPOT_ALGO_TYPES}}]
    evidence['reconciliation'] = {
        'schema_version': 1, 'kind': 'okx_stopped_owner_get_reconciliation', 'venue_queried': True,
        'read_only': True, 'result': 'EVIDENCE_COMPLETE', 'blockers': [], 'owner': 'test-owner',
        'database': {'sha256': sha, 'sha256_after': sha},
        'started_at': datetime.fromtimestamp(990, timezone.utc).isoformat(),
        'finished_at': datetime.fromtimestamp(1000, timezone.utc).isoformat(),
        'controller_pairs': {controller_id: trading_pair}, 'selected_executor_ids': [executor.config.id],
        'pending_before': pending, 'pending_after': pending,
        'balance_coverage': [{'asset': 'ETH', 'owned_residual': '1', 'cash_balance': '10',
                              'available_balance': '10', 'covered': True}],
        'selected_executors': [{'executor_id': executor.config.id, 'controller_id': controller_id, 'pair': trading_pair,
                               'venue_orders': [{'raw_order': {'clOrdId': 'entry', 'ordId': 'entry',
                                                               'instId': trading_pair, 'side': 'buy',
                                                               'state': 'filled', 'tdMode': 'cash',
                                                               'sz': '1', 'accFillSz': '1'},
                                                 'raw_fills': [{'tradeId': 'entry', 'ordId': 'entry',
                                                                'instId': trading_pair, 'side': 'buy',
                                                                'fillSz': '1', 'fillPx': '100',
                                                                'fee': '0', 'feeCcy': 'ETH'}],
                                                 'receipt': {'blockers': []},
                                                 'local_fill_comparisons': [{'matched': True}]}]}]}
    if getattr(executor.config, 'entry_chain_contract', None) == resume.ACQUISITION_CHAIN_V1:
        proof = evidence['executors'][0]
        proof['entry_reconciliation'] = executor.get_custom_info()['entry_reconciliation']
        proof['child_fills'] = {'entry': proof['fills']}
        selected = evidence['reconciliation']['selected_executors'][0]
        selected['venue_orders'].insert(0, {
            'raw_order': {'clOrdId': 'entry-old', 'ordId': 'venue-old', 'instId': trading_pair,
                          'side': 'buy', 'state': 'canceled', 'tdMode': 'cash', 'sz': '1', 'accFillSz': '0'},
            'raw_fills': [], 'receipt': {'blockers': []}, 'local_fill_comparisons': [],
        })
    return db, preflight, root, recorder, executor, evidence


@pytest.mark.asyncio
@pytest.mark.parametrize('armed', [False, True])
@pytest.mark.parametrize('unrelated_amount', ['0', '.2'])
async def test_old_stop_sqlite_prepare_native_resume_no_buy_and_one_trailing_sell(tmp_path, armed, unrelated_amount):
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path, armed, unrelated_amount)
    manifest = resume.prepare(db, before, evidence, now=1000, root=root)
    assert recorder.get_all_executors()[0].is_done
    strategy = old._strategy
    strategy.controllers = {'owner': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT'))}
    strategy.markets = {'okx': {'ETH-USDT'}}
    strategy.ready_to_trade = False
    strategy.market_data_provider = MagicMock()
    strategy.market_data_provider.ready = True
    strategy.market_data_provider.get_price_by_type.return_value = Decimal('100')
    connector = strategy.connectors['okx']
    connector.in_flight_orders = {}
    connector.get_price_by_type.return_value = Decimal('100')
    connector._order_tracker.fetch_order.return_value = None
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=1001, root=root)), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim',
                  side_effect=lambda path, value: resume.claim(path, value, now=1001)), \
            patch.object(PositionExecutor, 'get_trading_rules', return_value=old.trading_rules), \
            patch.object(PositionExecutor, 'start') as start:
        orchestrator = ExecutorOrchestrator(strategy)
        assert not orchestrator.resume_planned_positions_if_ready()
        start.assert_not_called()
        with pytest.raises(RuntimeError, match='pending'):
            orchestrator.create_executor(CreateExecutorAction(controller_id='owner', executor_config=old.config))
        # Reports may be requested by queued controller actions before readiness.
        for _ in range(2):
            with pytest.raises(RuntimeError, match="pending"):
                orchestrator.get_all_reports()
        strategy.ready_to_trade = True
        assert orchestrator.resume_planned_positions_if_ready()
        start.assert_called_once()
        recovered = orchestrator.active_executors['owner'][0]
        assert recovered.config == old.config
        assert recovered.config.timestamp == 1
        assert recovered._planned_resume_id == manifest['id']
        assert recovered.amount_to_close == Decimal('1')
        assert recovered._trailing_stop_trigger_pct == old._trailing_stop_trigger_pct
        assert recovered._trailing_stop_move_count == old._trailing_stop_move_count
        assert recovered._trailing_stop_armed_timestamp == old._trailing_stop_armed_timestamp
        assert sum(p.buy_amount_base - p.sell_amount_base for p in orchestrator.positions_held['owner']) == Decimal(unrelated_amount)
        report = orchestrator.get_all_reports()['owner']
        assert len(report['executors']) == 1
        assert sum(p.amount for p in report['positions']) == Decimal(unrelated_amount)
        recovered._status = RunnableStatus.RUNNING
        recovered.place_order = MagicMock(return_value='exit-1')
        recovered._sleep = AsyncMock()
        with patch.object(recovered, 'validate_sufficient_balance', new=AsyncMock()) as validate:
            await recovered.on_start()
            validate.assert_not_called()
        recovered.process_order_canceled_event(0, connector, OrderCancelledEvent(1002, 'entry'))
        recovered.process_order_failed_event(0, connector, MarketOrderFailureEvent(1002, 'entry', OrderType.LIMIT))
        recovered.place_open_order()
        recovered.control_open_order()
        recovered.place_order.assert_not_called()
        if not armed:
            connector.get_price_by_type.return_value = Decimal('105')
            await recovered.control_task()
        connector.get_price_by_type.return_value = Decimal('103')
        await recovered.control_task()
        await recovered.control_task()
        assert recovered.close_type == CloseType.TRAILING_STOP
        recovered.place_order.assert_called_once()
        assert recovered.place_order.call_args.kwargs['side'] == TradeType.SELL
        assert recovered.place_order.call_args.kwargs['amount'] == Decimal('1')
        with pytest.raises(ValueError, match='consumed'):
            resume.claim(db, manifest, now=1002)
        assert resume.pending_manifest(db, now=1002, root=root) is None
        # Complete the native exit and restart from persisted state. No original
        # BUY receipt may reappear as held inventory after the new process starts.
        recovered._close_order = tracked('exit-1', TradeType.SELL, '1', OrderState.FILLED)
        await recovered.control_task()
        assert recovered.is_closed
        assert recovered.amount_to_close == 0
        recorder.store_or_update_executor(recovered)
        with recorder.sql_manager.get_new_session() as session:
            session.add(Order(id='exit-1', config_file_path='owner.yml', strategy='v2_with_controllers', market='okx',
                              symbol='ETH-USDT', base_asset='ETH', quote_asset='USDT', creation_timestamp=1001000,
                              order_type='LIMIT', amount=Decimal('1'), leverage=1, price=Decimal('100'),
                              last_status='SellOrderCompleted', last_update_timestamp=1002000, exchange_order_id='exit-1',
                              position='NIL'))
            session.add(TradeFill(config_file_path='owner.yml', strategy='v2_with_controllers', market='okx',
                                  symbol='ETH-USDT', base_asset='ETH', quote_asset='USDT', timestamp=1002000,
                                  order_id='exit-1', trade_type='SELL', order_type='LIMIT', price=Decimal('100'),
                                  amount=Decimal('1'), leverage=1, trade_fee=AddedToCostTradeFee().to_json(),
                                  trade_fee_in_quote=Decimal(0), exchange_trade_id='exit-1', position='NIL'))
            session.commit()
        restarted = ExecutorOrchestrator(strategy)
        assert restarted.recovery_required == {}
        assert restarted.active_executors['owner'] == []
        assert sum(p.buy_amount_base - p.sell_amount_base for p in restarted.positions_held['owner']) == Decimal(unrelated_amount)
        assert sum(p.amount for p in restarted.get_all_reports()['owner']['positions']) == Decimal(unrelated_amount)
        assert {row.client_order_id for row in recorder.get_all_position_hold_ledger_entries()} == (
            {'unrelated'} if Decimal(unrelated_amount) else set())
        assert recorder.get_all_positions() == []
        strategy.buy.assert_not_called()


@pytest.mark.asyncio
async def test_prepare_rejects_venue_mismatch_without_mutating_owner(tmp_path):
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path)
    evidence['executors'][0]['net_base'] = '.9'
    with pytest.raises(ValueError, match='raw venue net'):
        resume.prepare(db, before, evidence, now=1000, root=root)
    assert resume.inspect_manifest(db) == []
    assert recorder.get_all_executors()[0].is_done


@pytest.mark.asyncio
async def test_stale_configuration_database_and_reuse_are_blocked(tmp_path):
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path)
    manifest = resume.prepare(db, before, evidence, now=1000, root=root)
    with pytest.raises(ValueError, match='expired'):
        resume.pending_manifest(db, now=1800, root=root)
    config = root / 'conf/controllers/owner.yml'
    original = config.read_text()
    config.write_text('changed')
    with pytest.raises(ValueError, match='binding'):
        resume.pending_manifest(db, now=1001, root=root)
    config.write_text(original)
    with resume.connect(db, writable=True) as connection:
        connection.execute('UPDATE Executors SET timestamp=timestamp+1')
    with pytest.raises(ValueError, match='database'):
        resume.claim(db, manifest, now=1001)
    assert recorder.get_all_executors()[0].is_done
    with pytest.raises(ValueError, match='already exists|database bytes|checkpoint'):
        resume.prepare(db, before, evidence, now=1001, root=root)


@pytest.mark.asyncio
async def test_old_database_missing_empty_sidecars_survives_new_schema_creation(tmp_path):
    import hashlib

    from hummingbot.model.exact_order_values import OrderExact, TradeFillExact
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path)
    with resume.connect(db, writable=True) as connection:
        connection.execute('DROP TABLE OrderExact')
        connection.execute('DROP TABLE TradeFillExact')
    with sqlite3.connect(db) as checkpoint:
        checkpoint.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    sha = hashlib.sha256(db.read_bytes()).hexdigest()
    evidence['reconciliation']['database'].update(sha256=sha, sha256_after=sha)
    manifest = resume.prepare(db, before, evidence, now=1000, root=root)
    OrderExact.__table__.create(recorder.sql_manager.engine)
    TradeFillExact.__table__.create(recorder.sql_manager.engine)
    assert resume.pending_manifest(db, now=1001, root=root) == manifest
    resume.claim(db, manifest, now=1001)
    assert not recorder.get_all_executors()[0].is_done


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['start', 'restore'])
async def test_post_claim_failure_fences_ownership_without_retry_or_record_overwrite(tmp_path, failure):
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path)
    resume.prepare(db, before, evidence, now=1000, root=root)
    strategy = old._strategy
    strategy.controllers = {'owner': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT'))}
    strategy.markets = {'okx': {'ETH-USDT'}}
    strategy.ready_to_trade = True
    strategy.market_data_provider = MagicMock()
    strategy.market_data_provider.ready = True
    strategy.market_data_provider.get_price_by_type.return_value = Decimal('100')
    connector = strategy.connectors['okx']
    connector.in_flight_orders = {}
    connector.get_price_by_type.return_value = Decimal('100')
    target = PositionExecutor if failure == 'start' else ExecutorOrchestrator
    method = 'start' if failure == 'start' else '_restore_positions_from_ledger'
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=1001, root=root)), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim',
                  side_effect=lambda path, value: resume.claim(path, value, now=1001)), \
            patch.object(PositionExecutor, 'get_trading_rules', return_value=old.trading_rules), \
            patch.object(target, method, side_effect=RuntimeError('injected after claim')) as broken:
        orchestrator = ExecutorOrchestrator(strategy)
        strategy.executor_orchestrator = orchestrator
        assert orchestrator.resume_planned_positions_if_ready()  # reporting and healthy management continue
        assert broken.call_count == 1
        assert orchestrator.resume_planned_positions_if_ready()
        assert broken.call_count == 1
        assert 'injected after claim' in orchestrator._planned_resume_failure
        assert orchestrator._planned_resume_pending is False
        assert resume.inspect_manifest(db)[0]['state'] == 'claimed'
        for side in (TradeType.BUY, TradeType.SELL):
            with pytest.raises(RuntimeError, match='claimed.*reconciliation'):
                old.place_order('okx', 'ETH-USDT', OrderType.LIMIT, side, Decimal('.1'), price=Decimal('100'))
        with pytest.raises(RuntimeError, match='claimed.*reconciliation'):
            orchestrator.create_executor(CreateExecutorAction(controller_id='owner', executor_config=old.config))
        orchestrator.store_all_executors(clear=False)
        orchestrator.sync_live_executors_to_db()
        assert not recorder.get_all_executors()[0].is_done
        await orchestrator.stop(strict=True)
        assert not recorder.get_all_executors()[0].is_done  # the claimed row survives for the next boot
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        after_crash = ExecutorOrchestrator(strategy)
        # The durable unfinished claim still fences this owner on the next boot.
        assert after_crash._planned_resume_pending is False
        assert after_crash.active_executors['owner'] == []
        assert after_crash.recovery_required['owner'] == [old.config.id]
        assert after_crash.claimed_resume_controller_blocked('owner') is True
        assert after_crash.claimed_resume_controller_blocked('healthy-owner') is False
        with pytest.raises(RuntimeError, match='claimed.*reconciliation'):
            after_crash.create_executor(CreateExecutorAction(controller_id='owner', executor_config=old.config))
    strategy.buy.assert_not_called()
    strategy.sell.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['expired', 'malformed', 'after_commit'])
async def test_unverified_resume_preflight_cannot_reopen_order_admission(tmp_path, failure):
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path)
    manifest = resume.prepare(db, before, evidence, now=1000, root=root)
    if failure == 'malformed':
        with resume.connect(db, writable=True) as connection:
            connection.execute(f'UPDATE {resume.TABLE} SET payload=?', ('{}',))
    strategy = old._strategy
    strategy.controllers = {'owner': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT'))}
    strategy.markets = {'okx': {'ETH-USDT'}}
    strategy.ready_to_trade = True
    strategy.market_data_provider = MagicMock(ready=True)
    strategy.market_data_provider.get_price_by_type.return_value = Decimal('100')
    strategy.connectors['okx'].in_flight_orders = {}
    strategy.connectors['okx'].get_price_by_type.return_value = Decimal('100')

    def fail_after_commit(path, value):
        resume.claim(path, value, now=1001)
        raise RuntimeError('injected immediately after durable claim')

    now = manifest['expires_at'] if failure == 'expired' else 1001
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=now, root=root)), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim', side_effect=fail_after_commit):
        orchestrator = ExecutorOrchestrator(strategy)
        strategy.executor_orchestrator = orchestrator
        assert orchestrator.resume_planned_positions_if_ready()
        assert orchestrator._resume_preflight_failed is True
        assert orchestrator.claimed_resume_controller_blocked('owner') is True
        for side in (TradeType.BUY, TradeType.SELL):
            with pytest.raises(RuntimeError, match='claimed.*reconciliation'):
                old.place_order('okx', 'ETH-USDT', OrderType.LIMIT, side, Decimal('.1'), price=Decimal('100'))
        with pytest.raises(RuntimeError, match='claimed.*reconciliation'):
            orchestrator.create_executor(CreateExecutorAction(controller_id='owner', executor_config=old.config))
        assert resume.inspect_manifest(db)[0]['state'] == ('claimed' if failure == 'after_commit' else 'prepared')
        if failure == 'after_commit':
            assert old.config.id in orchestrator._claimed_resume_unresolved['owner']
            orchestrator.store_all_executors(clear=False)
            orchestrator.sync_live_executors_to_db()
            assert not recorder.get_all_executors()[0].is_done
    strategy.buy.assert_not_called()
    strategy.sell.assert_not_called()


@pytest.mark.asyncio
async def test_claim_missing_owner_identity_keeps_admission_closed_and_status_readable(tmp_path):
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path)
    manifest = resume.prepare(db, before, evidence, now=1000, root=root)
    resume.claim(db, manifest, now=1001)
    with resume.connect(db, writable=True) as connection:
        connection.execute('UPDATE Executors SET controller_id=NULL')
    strategy = old._strategy
    strategy.controllers = {'owner': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT'))}
    strategy.markets = {'okx': {'ETH-USDT'}}
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=1001, root=root)):
        orchestrator = ExecutorOrchestrator(strategy)
    strategy.executor_orchestrator = orchestrator
    observation = StrategyV2Base._runtime_position_resume_observation(strategy)
    assert observation['preflight_failed'] is True
    assert observation['quarantined_controllers'] == []
    assert orchestrator.claimed_resume_controller_blocked('owner') is True
    assert orchestrator.claimed_resume_order_allowed('okx', 'ETH-USDT', TradeType.BUY) is False
    assert not recorder.get_all_executors()[0].is_done


@pytest.mark.asyncio
async def test_sealed_entry_duplicate_fill_ignored_but_extra_fill_blocks_exit(tmp_path):
    from hummingbot.core.event.events import OrderFilledEvent
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path)
    with resume.connect(before) as connection:
        preflight = connection.execute('SELECT * FROM Executors').fetchone()
    with resume.connect(db) as connection:
        stopped = connection.execute('SELECT * FROM Executors').fetchone()
    position = resume.validate_position(preflight, stopped)
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=old.trading_rules):
        recovered = PositionExecutor(old._strategy, old.config)
    recovered.hydrate_planned_position(position, 'sealed')
    recovered._status = RunnableStatus.RUNNING
    recovered.place_order = MagicMock()
    recovered._strategy.executor_orchestrator = SimpleNamespace(recovery_required={})
    event = OrderFilledEvent(1000, 'entry', 'ETH-USDT', TradeType.BUY, OrderType.LIMIT,
                             Decimal('100'), Decimal('1'), AddedToCostTradeFee(), 'entry', 'entry')
    recovered.process_order_filled_event(0, None, event)
    assert recovered._resume_conflicting_fill is None
    recovered.process_order_filled_event(0, None, event._replace(exchange_trade_id='unexpected', amount=Decimal('.1')))
    assert recovered.get_custom_info()['entry_lifecycle']['state'] == 'reconciliation_required'
    assert recovered._strategy.executor_orchestrator.recovery_required == {'owner': [old.config.id]}
    recovered.place_open_order()
    recovered.place_close_order_and_cancel_open_orders(CloseType.TRAILING_STOP)
    await recovered.control_task()
    recovered.place_order.assert_not_called()
    assert recovered.amount_to_close == Decimal('1')


def refresh_stopped_evidence(db, evidence, now):
    import copy
    import hashlib
    from datetime import datetime, timezone

    fresh = copy.deepcopy(evidence)
    with sqlite3.connect(db) as connection:
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    sha = hashlib.sha256(db.read_bytes()).hexdigest()
    fresh.update(observed_at=now, expires_at=now + 800)
    fresh['reconciliation']['database'].update(sha256=sha, sha256_after=sha)
    fresh['reconciliation']['started_at'] = datetime.fromtimestamp(now - 10, timezone.utc).isoformat()
    fresh['reconciliation']['finished_at'] = datetime.fromtimestamp(now, timezone.utc).isoformat()
    return fresh


@pytest.mark.asyncio
@pytest.mark.parametrize('history_change', ['payload', 'delete'])
async def test_second_explicit_handover_preserves_consumed_manifest_and_same_position(tmp_path, history_change):
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path, armed=True)
    first = resume.prepare(db, before, evidence, now=1000, root=root)
    strategy = old._strategy
    strategy.controllers = {'owner': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT'))}
    strategy.markets = {'okx': {'ETH-USDT'}}
    strategy.ready_to_trade = True
    strategy.market_data_provider = MagicMock(ready=True)
    strategy.market_data_provider.get_price_by_type.return_value = Decimal('100')
    strategy.connectors['okx'].in_flight_orders = {}
    strategy.connectors['okx'].get_price_by_type.return_value = Decimal('100')
    strategy.connectors['okx']._order_tracker.fetch_order.return_value = None
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=1001, root=root)), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim',
                  side_effect=lambda path, value: resume.claim(path, value, now=1001)), \
            patch.object(PositionExecutor, 'get_trading_rules', return_value=old.trading_rules), \
            patch.object(PositionExecutor, 'start'):
        orchestrator = ExecutorOrchestrator(strategy)
        assert orchestrator.resume_planned_positions_if_ready()
        recovered = orchestrator.active_executors['owner'][0]
        assert recovered.config == old.config
        assert recovered._planned_resume_id == first['id']
        consumed = resume.inspect_manifest(db)[0]
        assert consumed['state'] == 'claimed'
        recovered._status = RunnableStatus.RUNNING
        recovered._sleep = AsyncMock()
        recovered.place_order = MagicMock()
        recorder.store_or_update_executor(recovered)
        second_preflight = tmp_path / 'second-preflight.sqlite'
        with sqlite3.connect(db) as source, sqlite3.connect(second_preflight) as destination:
            source.backup(destination)
        recovered.early_stop(keep_position=True)
        await recovered.control_task()
        assert recovered.is_closed
        result = await orchestrator.stop(0, keep_position_by_controller={'owner': True}, strict=True)
        assert result.reconciliation_complete
        recovered.place_order.assert_not_called()
        fresh = refresh_stopped_evidence(db, evidence, 1100)
        second = resume.prepare(db, second_preflight, fresh, now=1100, root=root)
        assert second['id'] != first['id']
        assert second['positions'][0]['executor_id'] == first['positions'][0]['executor_id']
        assert second['positions'][0]['trailing'] == first['positions'][0]['trailing']
        assert second['positions'][0]['net_base'] == '1'
        assert resume.inspect_manifest(db)[0] == consumed
        assert resume.pending_manifest(db, now=1101, root=root) == second
        # A later handover binds the old immutable receipts, including their exact payload bytes.
        with resume.connect(db, writable=True) as connection:
            if history_change == 'delete':
                connection.execute(f'DELETE FROM {resume.TABLE} WHERE id=?', (first['id'],))
            else:
                changed = json.loads(consumed['payload'])
                changed['evidence']['owner'] = 'tampered-history'
                connection.execute(f'UPDATE {resume.TABLE} SET payload=? WHERE id=?',
                                   (resume.canonical(changed), first['id']))
        with pytest.raises(ValueError, match='history'):
            resume.pending_manifest(db, now=1101, root=root)
        with pytest.raises(ValueError, match='history'):
            resume.claim(db, second, now=1101)
        with resume.connect(db, writable=True) as connection:
            if history_change == 'delete':
                connection.execute(f'INSERT INTO {resume.TABLE} VALUES (?, ?, ?, ?)',
                                   (consumed['id'], consumed['state'], consumed['payload'], consumed['claimed_at']))
            else:
                connection.execute(f'UPDATE {resume.TABLE} SET payload=? WHERE id=?',
                                   (consumed['payload'], first['id']))
        with pytest.raises(ValueError, match='consumed'):
            resume.claim(db, first, now=1101)
        resume.claim(db, second, now=1101)
        rows = resume.inspect_manifest(db)
        assert next(row for row in rows if row['id'] == first['id']) == consumed
        assert len(rows) == 2 and all(row['state'] == 'claimed' for row in rows)
        assert resume.pending_manifest(db, now=1102, root=root) is None
        with pytest.raises(ValueError, match='consumed'):
            resume.claim(db, second, now=1102)


@pytest.mark.asyncio
@pytest.mark.parametrize('history_state', ['prepared', 'unknown', 'claimed-without-time', 'claimed-malformed-payload'])
async def test_new_prepare_rejects_unconsumed_or_invalid_history(tmp_path, history_state):
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path)
    manifest = resume.prepare(db, before, evidence, now=1000, root=root)
    with resume.connect(db, writable=True) as connection:
        if history_state == 'unknown':
            connection.execute(f'UPDATE {resume.TABLE} SET state=?', ('unknown',))
        elif history_state.startswith('claimed'):
            connection.execute(f'UPDATE {resume.TABLE} SET state=?', ('claimed',))
            if history_state == 'claimed-malformed-payload':
                connection.execute(f'UPDATE {resume.TABLE} SET claimed_at=1001, payload=?', ('{}',))
    original = resume.inspect_manifest(db)
    fresh = refresh_stopped_evidence(db, evidence, 1100)
    with pytest.raises(ValueError, match='manifest|history'):
        resume.prepare(db, before, fresh, now=1100, root=root)
    assert resume.inspect_manifest(db) == original
    assert len(original) == 1 and original[0]['id'] == manifest['id']


@pytest.mark.asyncio
async def test_pending_and_claim_reject_unknown_manifest_history(tmp_path):
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path)
    manifest = resume.prepare(db, before, evidence, now=1000, root=root)
    with resume.connect(db, writable=True) as connection:
        connection.execute(f'INSERT INTO {resume.TABLE} VALUES (?, ?, ?, ?)', ('unknown', 'unknown', '{}', None))
    with pytest.raises(ValueError, match='history|manifest'):
        resume.pending_manifest(db, now=1001, root=root)
    with pytest.raises(ValueError, match='history|manifest'):
        resume.claim(db, manifest, now=1001)
    assert recorder.get_all_executors()[0].is_done


async def stop_resumed_owner_again(db, root, recorder, old, now, before):
    strategy = old._strategy
    strategy.controllers = {'owner': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT'))}
    strategy.markets = {'okx': {'ETH-USDT'}}
    strategy.ready_to_trade = True
    strategy.market_data_provider = MagicMock(ready=True)
    strategy.market_data_provider.get_price_by_type.return_value = Decimal('100')
    strategy.connectors['okx'].in_flight_orders = {}
    strategy.connectors['okx'].get_price_by_type.return_value = Decimal('100')
    strategy.connectors['okx']._order_tracker.fetch_order.return_value = None
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=now, root=root)), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim',
                  side_effect=lambda path, value: resume.claim(path, value, now=now)), \
            patch.object(PositionExecutor, 'get_trading_rules', return_value=old.trading_rules), \
            patch.object(PositionExecutor, 'start'):
        orchestrator = ExecutorOrchestrator(strategy)
        assert orchestrator.resume_planned_positions_if_ready()
        recovered = orchestrator.active_executors['owner'][0]
        assert recovered.config == old.config
        recovered._status = RunnableStatus.RUNNING
        recovered._sleep = AsyncMock()
        recovered.place_order = MagicMock()
        recorder.store_or_update_executor(recovered)
        with sqlite3.connect(db) as source, sqlite3.connect(before) as destination:
            source.backup(destination)
        recovered.early_stop(keep_position=True)
        await recovered.control_task()
        assert (await orchestrator.stop(0, keep_position_by_controller={'owner': True}, strict=True)).reconciliation_complete
        recovered.place_order.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('rounds,damage', [(2, 'payload'), (2, 'first'), (2, 'all'), (1, 'all'), (1, 'table'), (2, 'legacy-later')])
async def test_consumed_history_cannot_be_changed_before_later_prepare_or_startup(tmp_path, rounds, damage):
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path, armed=True)
    first = None
    for index in range(rounds):
        now = 1000 + index * 100
        fresh = refresh_stopped_evidence(db, evidence, now)
        manifest = resume.prepare(db, before, fresh, now=now, root=root)
        first = first or manifest['id']
        before = tmp_path / f'next-preflight-{index}.sqlite'
        await stop_resumed_owner_again(db, root, recorder, old, now + 1, before)
    with resume.connect(db, writable=True) as connection:
        if damage == 'payload':
            row = connection.execute(f'SELECT payload FROM {resume.TABLE} WHERE id=?', (first,)).fetchone()
            changed = json.loads(row[0])
            changed['evidence']['owner'] = 'tampered-before-prepare'
            connection.execute(f'UPDATE {resume.TABLE} SET payload=? WHERE id=?', (resume.canonical(changed), first))
        elif damage == 'legacy-later':
            row = connection.execute(f'SELECT id, payload FROM {resume.TABLE} WHERE id!=?', (first,)).fetchone()
            changed = json.loads(row['payload'])
            changed.pop('claimed_history_digest')
            connection.execute(f'UPDATE {resume.TABLE} SET payload=? WHERE id=?', (resume.canonical(changed), row['id']))
        elif damage == 'first':
            connection.execute(f'DELETE FROM {resume.TABLE} WHERE id=?', (first,))
        elif damage == 'all':
            connection.execute(f'DELETE FROM {resume.TABLE}')
        else:
            connection.execute(f'DROP TABLE {resume.TABLE}')
    damaged = resume.inspect_manifest(db)
    now = 1000 + rounds * 100
    with pytest.raises(ValueError, match='history|manifest'):
        resume.pending_manifest(db, now=now, root=root)
    with pytest.raises(ValueError, match='history|manifest'):
        resume.prepare(db, before, refresh_stopped_evidence(db, evidence, now), now=now, root=root)
    assert resume.inspect_manifest(db) == damaged


@pytest.mark.asyncio
async def test_legacy_first_claim_remains_valid_history_for_next_explicit_handover(tmp_path):
    db, before, root, recorder, old, evidence = await stopped_fixture(tmp_path)
    first = resume.prepare(db, before, evidence, now=1000, root=root)
    # Reproduce the original released schema, before predecessor digests existed.
    first.pop('claimed_history_digest')
    with resume.connect(db, writable=True) as connection:
        connection.execute(f'UPDATE {resume.TABLE} SET payload=? WHERE id=?', (resume.canonical(first), first['id']))
    before = tmp_path / 'legacy-next-preflight.sqlite'
    await stop_resumed_owner_again(db, root, recorder, old, 1001, before)
    retained = resume.inspect_manifest(db)
    second = resume.prepare(db, before, refresh_stopped_evidence(db, evidence, 1100), now=1100, root=root)
    assert second['claimed_history_digest'] == resume.digest(retained)
    assert resume.pending_manifest(db, now=1101, root=root) == second
    resume.claim(db, second, now=1101)
    assert resume.inspect_manifest(db)[0] == retained[0]
