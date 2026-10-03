import json
import os
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
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
from hummingbot.strategy_v2.executors.planned_position_resume import (
    validate_position,
    validate_suspended_terminal_position,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType

TEST_NATIVE_OKX_KEYS = {
    'okx_api_key': 'test-api-key',
    'okx_secret_key': 'test-secret-key',
    'okx_passphrase': 'test-passphrase',
    'okx_registration_sub_domain': 'www',
}


@pytest.fixture(autouse=True)
def native_okx_test_keys(monkeypatch):
    monkeypatch.setattr(resume.Security, 'api_keys',
                        classmethod(lambda cls, connector: TEST_NATIVE_OKX_KEYS.copy()))


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

    def _suspended_terminal_pair(self):
        config = {**json.loads(json.dumps(self.row['config'])), 'id': 'ok-rsi-bnb',
                  'controller_id': 'ok-rsi-bnb', 'trading_pair': 'BNB-USDC'}
        order = json.loads(json.dumps(self.order))
        order['trading_pair'] = 'BNB-USDC'
        for fill in order['order_fills'].values():
            fill['trading_pair'] = 'BNB-USDC'
        custom = {**self.custom, 'order_ids': ['entry-1']}
        before_custom = {**custom, 'execution_order_snapshots': [order]}
        before_custom.pop('held_position_orders', None)
        after_custom = {**before_custom, 'restart_suspended': {
            'reason': 'planned_restart', 'timestamp': 200,
            'remaining_position_amount_base': '1'}}
        stopped = {**self.row, 'id': 'ok-rsi-bnb', 'controller_id': 'ok-rsi-bnb', 'config': config,
                   'custom_info': after_custom, 'close_type': None, 'close_timestamp': None}
        before = json.loads(json.dumps(stopped))
        return before, stopped

    def test_restart_suspended_terminal_requires_and_preserves_exact_single_entry(self):
        before, stopped = self._suspended_terminal_pair()
        result = validate_suspended_terminal_position(before, stopped)
        self.assertEqual(result['resume_kind'], 'restart_suspended_terminal_v1')
        self.assertEqual(result['net_base'], '1')
        self.assertEqual(result['trailing']['trailing_state'], 'waiting')
        self.assertEqual(stopped['close_type'], None)
        self.assertNotIn('held_position_orders', stopped['custom_info'])

    def test_restart_suspended_terminal_rejects_foreign_controller_scope(self):
        before, stopped = self._suspended_terminal_pair()
        for row in (before, stopped):
            row['controller_id'] = 'other-controller'
            row['config']['controller_id'] = 'other-controller'
            row['config']['id'] = row['id']
        with self.assertRaisesRegex(ValueError, 'V1 BNB owner'):
            validate_suspended_terminal_position(before, stopped)

    def test_signed_base_fee_matches_native_buy_and_sell_ownership(self):
        receipt = {'executed_amount_base': '.2', 'order_fills': {
            'fill': {'fill_base_amount': '.2', 'fee': {'percent': '0', 'flat_fees': [
                {'token': 'BNB', 'amount': '.001'}]}}}}
        self.assertEqual(resume.position_hold_net_base({**receipt, 'trade_type': 'BUY'}, 'BNB'), Decimal('.199'))
        self.assertEqual(resume.position_hold_net_base({**receipt, 'trade_type': 'SELL'}, 'BNB'), Decimal('-.201'))

    def test_restart_suspended_terminal_rejects_lifecycle_and_ownership_changes(self):
        cases = [
            ('status', 'inactive with no close type'),
            ('close_type', 'inactive with no close type'),
            ('close_timestamp', 'no close type or close timestamp'),
            ('marker', 'terminal preflight must match stopped row exactly'),
            ('exit', 'terminal preflight must match stopped row exactly'),
            ('trailing', 'terminal preflight must match stopped row exactly'),
        ]
        for mutation, match in cases:
            with self.subTest(mutation=mutation):
                before, stopped = self._suspended_terminal_pair()
                if mutation == 'status':
                    stopped['status'] = 2
                elif mutation == 'close_type':
                    stopped['close_type'] = CloseType.TRAILING_STOP.value
                elif mutation == 'close_timestamp':
                    stopped['close_timestamp'] = 201
                elif mutation == 'marker':
                    stopped['custom_info']['restart_suspended'] = None
                elif mutation == 'exit':
                    stopped['custom_info']['execution_order_snapshots'].append(self.order)
                else:
                    stopped['custom_info']['trailing_move_count'] = -1
                with self.assertRaisesRegex(ValueError, match):
                    validate_suspended_terminal_position(before, stopped)

    def test_stopped_terminal_preflight_must_be_identical_and_unclosed(self):
        before, stopped = self._suspended_terminal_pair()
        stopped['close_timestamp'] = 201
        with self.assertRaisesRegex(ValueError, 'no close type or close timestamp'):
            validate_suspended_terminal_position(stopped, stopped)

        before, stopped = self._suspended_terminal_pair()
        before['status'] = 2
        before['is_active'] = True
        with self.assertRaisesRegex(ValueError, 'already be inactive and unclosed'):
            validate_suspended_terminal_position(before, stopped)

        before, stopped = self._suspended_terminal_pair()
        before['custom_info']['restart_suspended']['timestamp'] = 199
        with self.assertRaisesRegex(ValueError, 'terminal preflight must match stopped row exactly'):
            validate_suspended_terminal_position(before, stopped)

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


async def stopped_fixture(tmp_path, armed=False, unrelated_amount='.2', trading_pair='ETH-USDT', controller_id='owner',
                          executor_id=None, script_config='owner.yml', controller_config='owner.yml'):
    db = tmp_path / 'stopped.sqlite'
    preflight = tmp_path / 'preflight.sqlite'
    root = tmp_path / 'runtime'
    for file in (*resume.SOURCES, f'conf/scripts/{script_config}', f'conf/controllers/{controller_config}',
                 'conf/conf_client.yml', 'conf/connectors/okx.yml'):
        path = root / file
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text((resume.ROOT / file).read_text() if file in resume.SOURCES else 'id: owner\n')
    (root / f'conf/scripts/{script_config}').write_text(
        f'script_file_name: v2_with_controllers.py\ncontrollers_config: [{controller_config}]\n')
    (root / f'conf/controllers/{controller_config}').write_text(
        f'id: {controller_id}\nconnector_name: okx\ntrading_pair: {trading_pair}\n')
    manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db))
    recorder = MarketsRecorder(manager, [], script_config, 'v2_with_controllers',
                               market_data_collection=MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    from hummingbot.model.exact_order_values import TradeFillExact
    TradeFill.__table__.create(recorder.sql_manager.engine, checkfirst=True)
    TradeFillExact.__table__.create(recorder.sql_manager.engine, checkfirst=True)
    executor = make_executor()
    if executor_id is not None:
        executor.config.id = executor_id
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
    base_asset, quote_asset = trading_pair.split('-')
    with manager.get_new_session() as session:
        receipts = [executor._held_position_orders[0]] + ([unrelated] if Decimal(unrelated_amount) else [])
        if getattr(executor.config, 'entry_chain_contract', None) == resume.ACQUISITION_CHAIN_V1:
            session.add(Order(id='entry-old', config_file_path=script_config, strategy='v2_with_controllers', market='okx',
                              symbol=trading_pair, base_asset=base_asset, quote_asset=quote_asset,
                              creation_timestamp=1000, order_type='LIMIT', amount=Decimal('1'), leverage=1,
                              price=Decimal('100'), last_status='OrderCancelled', last_update_timestamp=2000,
                              exchange_order_id='venue-old', position='NIL'))
        for receipt in receipts:
            oid = receipt['client_order_id']
            session.add(Order(id=oid, config_file_path=script_config, strategy='v2_with_controllers', market='okx',
                              symbol=trading_pair, base_asset=base_asset, quote_asset=quote_asset, creation_timestamp=1000,
                              order_type='LIMIT', amount=Decimal(receipt['amount']), leverage=1, price=Decimal('100'),
                              last_status='BuyOrderCompleted', last_update_timestamp=2000, exchange_order_id=oid,
                              position='NIL'))
            for tid, fill in receipt['order_fills'].items():
                session.add(TradeFill(config_file_path=script_config, strategy='v2_with_controllers', market='okx',
                                      symbol=trading_pair, base_asset=base_asset, quote_asset=quote_asset, timestamp=2000,
                                      order_id=oid, trade_type='BUY', order_type='LIMIT', price=Decimal(fill['fill_price']),
                                      amount=Decimal(fill['fill_base_amount']), leverage=1, trade_fee=fill['fee'],
                                      trade_fee_in_quote=Decimal(0), exact_amount=str(fill['fill_base_amount']),
                                      exact_price=str(fill['fill_price']), exact_trade_fee_in_quote='0',
                                      exact_receipt_source='order_filled_event_v1',
                                      exchange_trade_id=tid, position='NIL'))
                session.add(TradeFillExact(market='okx', order_id=oid, exchange_trade_id=tid,
                                           amount=str(fill['fill_base_amount']), price=str(fill['fill_price']),
                                           fee_in_quote='0'))
        session.add(Position(id='aggregate', controller_id=controller_id, connector_name='okx', side='BUY',
                             trading_pair=trading_pair, timestamp=1000, volume_traded_quote=Decimal('120'),
                             amount=Decimal('1') + Decimal(unrelated_amount), breakeven_price=Decimal('100'),
                             unrealized_pnl_quote=Decimal('0'), realized_pnl_quote=Decimal('0'), cum_fees_quote=Decimal('0')))
        session.commit()
    entry = executor._held_position_orders[0]
    evidence = {'schema': 1, 'owner': 'test-owner', 'script_config': script_config, 'owner_stopped': True, 'venue_open_orders': [], 'observed_at': 1000,
                'expires_at': 1800, 'config_paths': [f'conf/scripts/{script_config}', f'conf/controllers/{controller_config}', 'conf/conf_client.yml', 'conf/connectors/okx.yml'],
                'continuity_executors': [],
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
        'continuity_executors': [],
        'balance_coverage': [{'asset': base_asset, 'owned_residual': '1', 'cash_balance': '10',
                              'available_balance': '10', 'covered': True}],
        'selected_executors': [{'executor_id': executor.config.id, 'controller_id': controller_id, 'pair': trading_pair,
                               'venue_orders': [{'raw_order': {'clOrdId': 'entry', 'ordId': 'entry',
                                                               'instId': trading_pair, 'side': 'buy',
                                                               'state': 'filled', 'tdMode': 'cash',
                                                               'sz': '1', 'accFillSz': '1'},
                                                 'raw_fills': [{'tradeId': 'entry', 'ordId': 'entry',
                                                                'instId': trading_pair, 'side': 'buy',
                                                                'fillSz': '1', 'fillPx': '100',
                                                                'fee': '0', 'feeCcy': base_asset}],
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


def write_fleet_owner_snapshot(path, owner, *, active_amount=None):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from hummingbot.model.exact_order_values import TradeFillExact

    base = 'BNB'
    native_positions = []
    native_fills = []
    with sqlite3.connect(path) as connection:
        connection.execute('CREATE TABLE Metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        connection.execute('CREATE TABLE Executors (id TEXT PRIMARY KEY, controller_id TEXT, status INTEGER, '
                           'is_active INTEGER, close_type INTEGER, config TEXT, custom_info TEXT)')
        connection.execute('CREATE TABLE PositionHoldLedger (id TEXT PRIMARY KEY, controller_id TEXT, '
                           'executor_id TEXT, connector_name TEXT, trading_pair TEXT, order_side TEXT, '
                           'client_order_id TEXT, order_payload TEXT)')
        connection.execute('INSERT INTO Metadata VALUES (?, ?)', ('native-test', 'fixture'))
        ledger_amount = Decimal('.25') if owner == 'rsi_modular_v2' else Decimal(0)
        if ledger_amount:
            receipt = {'trade_type': 'BUY', 'executed_amount_base': str(ledger_amount),
                       'order_fills': {'v2-trade': {
                           'fill_base_amount': str(ledger_amount), 'fill_price': '100',
                           'fill_quote_amount': str(ledger_amount * 100),
                           'fee': {'percent': '0', 'flat_fees': [{'token': base, 'amount': '.001'}]}}}}
            connection.execute('INSERT INTO PositionHoldLedger VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                               ('v2-held-row', 'v2-owner', 'v2-held-executor', 'okx', 'BNB-USDC',
                                'BUY', 'v2-held-order', json.dumps(receipt, sort_keys=True)))
            native_positions.append(('v2-position', 'v2-owner', 'BNB-USDC', Decimal('.249')))
            native_fills.append(('rsi_modular_v2.yml', 'v2-held-order', 'v2-trade', 'BUY',
                                 Decimal('.25'), Decimal('100'),
                                 {'percent': '0', 'flat_fees': [{'token': base, 'amount': '.001'}]}))
        if active_amount is not None:
            executor = make_executor()
            executor.config.id = 'v3-active-bnb'
            executor.config.controller_id = 'v3-bnb-controller'
            executor.config.trading_pair = 'BNB-USDC'
            executor.config.amount = Decimal(active_amount)
            active_amount = Decimal(active_amount)
            native_order = InFlightOrder('v3-entry-order', 'BNB-USDC', OrderType.LIMIT, TradeType.BUY,
                                         active_amount, 1, Decimal('100'), exchange_order_id='v3-entry-order')
            native_order.update_with_trade_update(TradeUpdate(
                'v3-trade', 'v3-entry-order', 'v3-entry-order', 'BNB-USDC', 2, Decimal('100'),
                active_amount, active_amount * Decimal('100'), AddedToCostTradeFee()))
            native_order.current_state = OrderState.FILLED
            order_payload = native_order.to_json()
            order_payload['trading_pair'] = 'BNB-USDC'
            for fill in order_payload['order_fills'].values():
                fill['trading_pair'] = 'BNB-USDC'
            connection.execute('INSERT INTO Executors VALUES (?, ?, ?, ?, ?, ?, ?)',
                               (executor.config.id, executor.config.controller_id, 2, 1, None,
                                executor.config.model_dump_json(),
                                json.dumps({'remaining_position_amount_base': str(active_amount),
                                            'execution_order_snapshots': [order_payload],
                                            'order_ids': ['v3-entry-order']}, sort_keys=True)))
            native_positions.append(('v3-position', 'v3-bnb-controller', 'BNB-USDC', active_amount))
            native_fills.append(('meridian_v3.yml', 'v3-entry-order', 'v3-trade', 'BUY',
                                 active_amount, Decimal('100'), AddedToCostTradeFee().to_json()))
        elif owner == 'meridian_v3':
            native_positions.append(('v3-position', 'v3-bnb-controller', 'BNB-USDC', Decimal(0)))

    from hummingbot.model.position import Position as NativePosition

    engine = create_engine(f'sqlite:///{Path(path).resolve()}')
    NativePosition.__table__.create(engine, checkfirst=True)
    Order.__table__.create(engine, checkfirst=True)
    TradeFill.__table__.create(engine, checkfirst=True)
    TradeFillExact.__table__.create(engine, checkfirst=True)
    with Session(engine) as session:
        for position_id, controller_id, pair, amount in native_positions:
            session.add(NativePosition(id=position_id, controller_id=controller_id, connector_name='okx',
                                       side='BUY', trading_pair=pair, timestamp=1,
                                       volume_traded_quote=amount * Decimal('100'), amount=amount,
                                       breakeven_price=Decimal('100'), unrealized_pnl_quote=Decimal(0),
                                       realized_pnl_quote=Decimal(0), cum_fees_quote=Decimal(0)))
        for script_config, order_id, trade_id, trade_type, amount, price, fee in native_fills:
            session.add(Order(id=order_id, config_file_path=script_config, strategy='v2_with_controllers',
                              market='okx', symbol='BNB-USDC', base_asset='BNB', quote_asset='USDC',
                              creation_timestamp=1, order_type='LIMIT', amount=amount, leverage=1,
                              price=price, last_status='BuyOrderCompleted', last_update_timestamp=2,
                              exchange_order_id=order_id, position='NIL'))
            quote_fee = sum((Decimal(row['amount']) * (price if row['token'] == base else 1)
                             for row in fee['flat_fees']), Decimal(0))
            session.add(TradeFill(config_file_path=script_config, strategy='v2_with_controllers', market='okx',
                                  symbol='BNB-USDC', base_asset='BNB', quote_asset='USDC', timestamp=2,
                                  order_id=order_id, trade_type=trade_type, order_type='LIMIT', price=price,
                                  amount=amount, leverage=1, trade_fee=fee, trade_fee_in_quote=quote_fee,
                                  exact_amount=str(amount), exact_price=str(price), exact_trade_fee_in_quote=str(quote_fee),
                                  exact_receipt_source='order_filled_event_v1', exchange_trade_id=trade_id,
                                  position='NIL'))
            session.add(TradeFillExact(market='okx', order_id=order_id, exchange_trade_id=trade_id,
                                       amount=str(amount), price=str(price), fee_in_quote=str(quote_fee)))
        session.commit()
    engine.dispose()
    return path


def attach_fleet_ownership(evidence, tmp_path, *, now=1000):
    snapshots = {
        'rsi_modular_v2': write_fleet_owner_snapshot(tmp_path / 'v2-owner.sqlite', 'rsi_modular_v2'),
        'meridian_v3': write_fleet_owner_snapshot(
            tmp_path / 'v3-owner.sqlite', 'meridian_v3', active_amount=Decimal('.2')),
    }
    observations = []
    total = defaultdict(Decimal)
    for owner, script_config in resume.FLEET_OWNERS.items():
        path = snapshots[owner]
        inspected = resume.inspect_fleet_owner_snapshot(path, owner)
        tables_hash = inspected['tables_digest']
        residuals = inspected['owned_residual_by_asset']
        for asset, amount in residuals.items():
            total[asset] += Decimal(amount)
        observations.append({
            'owner': owner, 'script_config': script_config,
            'observed_at': now, 'database_snapshot_path': str(path),
            'database_snapshot_sha256': __import__('hashlib').sha256(path.read_bytes()).hexdigest(),
            'source_identity': {'container_id': 'c' * 64, 'image_id': 'sha256:' + 'b' * 64,
                                'engine_source_sha256': 'd' * 64},
            'tables_digest': tables_hash, 'owned_residual_by_asset': residuals,
            'quantity_increments': {},
        })
    bundle = {'schema': 1, 'observed_at': now,
              'expires_at': now + 200, 'owner_observations': observations}
    evidence['fleet_ownership'] = bundle
    report = evidence['reconciliation']
    for row in report['balance_coverage']:
        row['owned_residual'] = str(Decimal(row['owned_residual']) + total.get(row['asset'], Decimal(0)))
    resume.seal_fleet_ownership(bundle, TEST_NATIVE_OKX_KEYS, evidence)
    report['fleet_ownership_authenticator'] = bundle['authenticator']
    return evidence


async def suspended_terminal_fixture(tmp_path):
    db, preflight, root, recorder, executor, evidence = await stopped_fixture(
        tmp_path, unrelated_amount='.2', trading_pair='BNB-USDC', controller_id='ok-rsi-bnb',
        executor_id='terminal-executor-id', script_config='ok_rsi.yml', controller_config='ok_rsi_bnb.yml')
    with sqlite3.connect(db) as connection:
        row = connection.execute('SELECT custom_info FROM Executors WHERE id=?', (executor.config.id,)).fetchone()
        custom = json.loads(row[0])
        snapshots = [executor._open_order.order.to_json()]
        custom['execution_order_snapshots'] = snapshots
        custom['order_ids'] = [executor._open_order.order.client_order_id]
        custom.pop('held_position_orders', None)
        custom['restart_suspended'] = {
            'timestamp': 200, 'wall_timestamp': 200, 'reason': 'planned_restart',
            'remaining_position_amount_base': '1',
        }
        connection.execute('UPDATE Executors SET status=4, is_active=0, close_type=NULL, close_timestamp=NULL, '
                           'custom_info=? WHERE id=?', (resume.canonical(custom), executor.config.id))
        connection.execute('DELETE FROM PositionHoldLedger WHERE executor_id=?', (executor.config.id,))
        connection.execute('UPDATE Position SET amount=? WHERE controller_id=? AND connector_name=? AND trading_pair=?',
                           (200000, executor.config.controller_id, 'okx', executor.config.trading_pair))
        risk_state = {'version': 1, 'controllers': ['ok-rsi-bnb'], 'max_global_pnl': '0',
                      'max_pnl_by_controller': {'ok-rsi-bnb': '0'}, 'drawdown_exited_controllers': [],
                      'stopped': False, 'operator_paused': {'ok-rsi-bnb': True}}
        connection.execute('INSERT OR REPLACE INTO Metadata(key, value) VALUES (?, ?)',
                           ('strategy-risk:ok_rsi.yml', json.dumps(risk_state, sort_keys=True)))
    with sqlite3.connect(db) as source, sqlite3.connect(preflight) as destination:
        source.backup(destination)
    evidence = refresh_stopped_evidence(db, evidence, 1000)
    evidence['reconciliation']['balance_coverage'][0]['owned_residual'] = '1.2'
    evidence = attach_fleet_ownership(evidence, tmp_path)
    return db, preflight, root, recorder, executor, evidence


async def terminal_with_suspended_sol_fixture(tmp_path):
    db, preflight, root, recorder, terminal, evidence = await suspended_terminal_fixture(tmp_path)
    sol = make_executor()
    sol.config.id = 'sol-continuity-executor'
    sol.config.controller_id = 'ok-rsi-sol'
    sol.config.trading_pair = 'SOL-USDC'
    sol.config.entry_order_policy = 'single_attempt'
    sol._open_order = tracked('sol-entry', TradeType.BUY, '1', OrderState.FILLED)
    order_payload = sol._open_order.order.to_json()
    order_payload['trading_pair'] = 'SOL-USDC'
    for fill in order_payload['order_fills'].values():
        fill['trading_pair'] = 'SOL-USDC'
    sol._open_order.order = InFlightOrder.from_json(order_payload)
    sol._entry_submitted = True
    sol._entry_lifecycle = {'state': 'managing_fill', 'stop_reason': None}
    sol._trailing_stop_trigger_pct = Decimal('.017')
    sol._trailing_stop_move_count = 2
    sol._trailing_stop_armed_timestamp = 900
    sol._trailing_stop_last_update_timestamp = 950
    recorder.store_or_update_executor(sol)
    from hummingbot.model.exact_order_values import TradeFillExact
    sol_base, sol_quote = sol.config.trading_pair.split('-')
    sol_order = sol._open_order.order
    with recorder.sql_manager.get_new_session() as session:
        session.add(Order(id=sol_order.client_order_id, config_file_path='ok_rsi.yml',
                          strategy='v2_with_controllers', market='okx', symbol=sol.config.trading_pair,
                          base_asset=sol_base, quote_asset=sol_quote, creation_timestamp=1000,
                          order_type='LIMIT', amount=sol_order.amount, leverage=1, price=Decimal('100'),
                          last_status='BuyOrderCompleted', last_update_timestamp=1000,
                          exchange_order_id=sol_order.exchange_order_id, position='NIL'))
        for trade_id, fill in sol_order.to_json()['order_fills'].items():
            amount = str(fill['fill_base_amount'])
            price = str(fill['fill_price'])
            session.add(TradeFill(config_file_path='ok_rsi.yml', strategy='v2_with_controllers', market='okx',
                                  symbol=sol.config.trading_pair, base_asset=sol_base, quote_asset=sol_quote,
                                  timestamp=1000, order_id=sol_order.client_order_id, trade_type='BUY',
                                  order_type='LIMIT', price=Decimal(price), amount=Decimal(amount), leverage=1,
                                  trade_fee=fill['fee'], trade_fee_in_quote=Decimal(0), exact_amount=amount,
                                  exact_price=price, exact_trade_fee_in_quote='0',
                                  exact_receipt_source='order_filled_event_v1', exchange_trade_id=str(trade_id),
                                  position='NIL'))
            session.add(TradeFillExact(market='okx', order_id=sol_order.client_order_id,
                                       exchange_trade_id=str(trade_id), amount=amount, price=price,
                                       fee_in_quote='0'))
        session.commit()
    risk_state = {'version': 1, 'controllers': ['ok-rsi-bnb', 'ok-rsi-sol'], 'max_global_pnl': '0',
                  'max_pnl_by_controller': {'ok-rsi-bnb': '0', 'ok-rsi-sol': '0'},
                  'drawdown_exited_controllers': [], 'stopped': False,
                  'operator_paused': {'ok-rsi-bnb': True, 'ok-rsi-sol': True}}
    with sqlite3.connect(db) as connection:
        connection.execute('UPDATE Metadata SET value=? WHERE key=?',
                           (json.dumps(risk_state, sort_keys=True), 'strategy-risk:ok_rsi.yml'))
    with sqlite3.connect(db) as source, sqlite3.connect(preflight) as destination:
        source.backup(destination)
    with patch('hummingbot.strategy_v2.executors.executor_orchestrator.Security.api_keys', return_value=TEST_NATIVE_OKX_KEYS.copy()), \
            patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.position_executor.position_executor.time.time', return_value=1000):
        assert sol.suspend_for_restart()
        sol._advance_restart_suspend()
    assert sol.is_restart_suspended
    assert sol.status == RunnableStatus.RUNNING and sol.close_type is None
    script_path = root / 'conf/scripts/ok_rsi.yml'
    script_path.write_text('script_file_name: v2_with_controllers.py\n'
                           'controllers_config: [ok_rsi_bnb.yml, ok_rsi_sol.yml]\n')
    sol_controller_path = root / 'conf/controllers/ok_rsi_sol.yml'
    sol_controller_path.write_text('id: ok-rsi-sol\nconnector_name: okx\ntrading_pair: SOL-USDC\n')
    evidence['config_paths'].append('conf/controllers/ok_rsi_sol.yml')
    evidence['continuity_executors'] = [{'executor_id': sol.config.id}]
    report = evidence['reconciliation']
    report['controller_pairs']['ok-rsi-sol'] = 'SOL-USDC'
    for phase in ('pending_before', 'pending_after'):
        report[phase].append({'pair': 'SOL-USDC', 'regular': [], 'regular_count': 0, 'algo_count': 0,
                              'algos': {kind: [] for kind in resume.SPOT_ALGO_TYPES}})
    report['balance_coverage'].append({'asset': 'SOL', 'owned_residual': '1', 'cash_balance': '10',
                                       'available_balance': '10', 'covered': True})
    sol_order = sol._open_order.order
    raw_fills = []
    for trade_id, fill in sol_order.to_json()['order_fills'].items():
        flat_fees = fill['fee']['flat_fees']
        fee_ccy = flat_fees[0]['token'] if flat_fees else 'SOL'
        fee_amount = sum((Decimal(row['amount']) for row in flat_fees), Decimal(0))
        raw_fills.append({'tradeId': str(trade_id), 'ordId': sol_order.exchange_order_id,
                          'instId': 'SOL-USDC', 'side': 'buy',
                          'fillSz': fill['fill_base_amount'], 'fillPx': fill['fill_price'],
                          'fee': str(-fee_amount), 'feeCcy': fee_ccy})
    report['continuity_executors'] = [{
        'executor_id': sol.config.id, 'controller_id': 'ok-rsi-sol', 'pair': 'SOL-USDC',
        'venue_orders': [{
            'raw_order': {'clOrdId': sol_order.client_order_id, 'ordId': sol_order.exchange_order_id,
                          'instId': 'SOL-USDC', 'side': 'buy', 'state': 'filled', 'tdMode': 'cash',
                          'sz': str(sol_order.amount), 'accFillSz': str(sol_order.executed_amount_base)},
            'raw_fills': raw_fills, 'receipt': {'blockers': []},
            'local_fill_comparisons': [{'matched': True} for _ in raw_fills],
        }],
    }]
    evidence = refresh_stopped_evidence(db, evidence, 1000)
    return db, preflight, root, recorder, terminal, sol, evidence


def apply_prospective_daily_enrollment(db, *, limit_quote='1000', command_id='approved-risk-1000'):
    from hummingbot.strategy_v2.migrations.entry_risk import apply_entry_risk_transition, plan_entry_risk_transition

    request = {
        'command_id': command_id, 'requested_at': 1000,
        'limit_quote': limit_quote, 'controllers': ['ok-rsi-bnb', 'ok-rsi-sol'],
        'owner_stopped': True, 'entries_fenced': True, 'allow_new_risk_scope': False,
    }
    with sqlite3.connect(db) as connection:
        proposal = plan_entry_risk_transition(connection, 'ok_rsi.yml', request)
        apply_entry_risk_transition(connection, proposal, now=1001)
    return proposal


@pytest.mark.asyncio
async def test_terminal_claim_accepts_only_archived_prospective_daily_risk_enrollment(tmp_path):
    db, preflight, root, _, terminal, sol, evidence = await terminal_with_suspended_sol_fixture(tmp_path)
    with sqlite3.connect(preflight) as connection:
        raw_preflight_risk = connection.execute(
            'SELECT value FROM Metadata WHERE key=?', ('strategy-risk:ok_rsi.yml',)).fetchone()[0]
    assert raw_preflight_risk != resume.canonical(json.loads(raw_preflight_risk))
    script_path = root / 'conf/scripts/ok_rsi.yml'
    script_path.write_text(script_path.read_text() + "max_daily_loss_quote: '1000'\n")
    proposal = apply_prospective_daily_enrollment(db)

    with sqlite3.connect(preflight) as connection:
        original = json.loads(connection.execute(
            'SELECT value FROM Metadata WHERE key=?', ('strategy-risk:ok_rsi.yml',)).fetchone()[0])
    assert original['version'] == 1 and 'entry_risk_enrollment' not in original

    evidence = refresh_stopped_evidence(db, evidence, 1002)
    manifest = resume.prepare(
        db, preflight, evidence, now=1002, root=root,
        terminal_suspended_executor_ids=(terminal.config.id,),
        continuity_executor_ids=(sol.config.id,),
    )
    binding = manifest['risk_transition_binding']
    assert binding['archive_key'] == proposal['archive_key']
    assert binding['transition_id'] == proposal['transition_id']
    assert binding['limit_quote'] == '1000'
    assert resume.pending_manifest(db, now=1003, root=root) == manifest
    resume.claim(db, manifest, now=1003, root=root)


@pytest.mark.asyncio
async def test_terminal_daily_risk_enrollment_requires_exact_staged_limit(tmp_path):
    db, preflight, root, _, terminal, sol, evidence = await terminal_with_suspended_sol_fixture(tmp_path)
    (root / 'conf/scripts/ok_rsi.yml').write_text(
        (root / 'conf/scripts/ok_rsi.yml').read_text() + "max_daily_loss_quote: '1000'\n")
    apply_prospective_daily_enrollment(db, limit_quote='50', command_id='wrong-limit')
    evidence = refresh_stopped_evidence(db, evidence, 1002)

    with pytest.raises(ValueError, match='limit differs from staged runner configuration'):
        resume.prepare(db, preflight, evidence, now=1002, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,),
                       continuity_executor_ids=(sol.config.id,))


@pytest.mark.asyncio
async def test_terminal_daily_risk_enrollment_rejects_unarchived_native_state_drift(tmp_path):
    db, preflight, root, _, terminal, sol, evidence = await terminal_with_suspended_sol_fixture(tmp_path)
    (root / 'conf/scripts/ok_rsi.yml').write_text(
        (root / 'conf/scripts/ok_rsi.yml').read_text() + "max_daily_loss_quote: '1000'\n")
    apply_prospective_daily_enrollment(db)
    with sqlite3.connect(db) as connection:
        state = json.loads(connection.execute(
            'SELECT value FROM Metadata WHERE key=?', ('strategy-risk:ok_rsi.yml',)).fetchone()[0])
        state['max_global_pnl'] = '1'
        connection.execute('UPDATE Metadata SET value=? WHERE key=?',
                           (resume.canonical(state), 'strategy-risk:ok_rsi.yml'))
    evidence = refresh_stopped_evidence(db, evidence, 1002)

    with pytest.raises(ValueError, match='changed preserved history or current pending state'):
        resume.prepare(db, preflight, evidence, now=1002, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,),
                       continuity_executor_ids=(sol.config.id,))


@pytest.mark.asyncio
async def test_terminal_daily_risk_enrollment_rejects_unrelated_metadata_drift(tmp_path):
    db, preflight, root, _, terminal, sol, evidence = await terminal_with_suspended_sol_fixture(tmp_path)
    (root / 'conf/scripts/ok_rsi.yml').write_text(
        (root / 'conf/scripts/ok_rsi.yml').read_text() + "max_daily_loss_quote: '1000'\n")
    apply_prospective_daily_enrollment(db)
    with sqlite3.connect(db) as connection:
        connection.execute('INSERT INTO Metadata(key, value) VALUES (?, ?)',
                           ('unrelated-maintenance-state', '{"unchanged":true}'))
    evidence = refresh_stopped_evidence(db, evidence, 1002)

    with pytest.raises(ValueError, match='pre-state digest does not match'):
        resume.prepare(db, preflight, evidence, now=1002, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,),
                       continuity_executor_ids=(sol.config.id,))


@pytest.mark.asyncio
@pytest.mark.parametrize('tamper', ['delete', 'rehashed_edit'])
async def test_terminal_daily_risk_transition_archive_is_rechecked_before_startup(tmp_path, tamper):
    import hashlib

    db, preflight, root, _, terminal, sol, evidence = await terminal_with_suspended_sol_fixture(tmp_path)
    (root / 'conf/scripts/ok_rsi.yml').write_text(
        (root / 'conf/scripts/ok_rsi.yml').read_text() + "max_daily_loss_quote: '1000'\n")
    proposal = apply_prospective_daily_enrollment(db)
    evidence = refresh_stopped_evidence(db, evidence, 1002)
    resume.prepare(db, preflight, evidence, now=1002, root=root,
                   terminal_suspended_executor_ids=(terminal.config.id,),
                   continuity_executor_ids=(sol.config.id,))
    with sqlite3.connect(db) as connection:
        if tamper == 'delete':
            connection.execute('DELETE FROM Metadata WHERE key=?', (proposal['archive_key'],))
        else:
            archive = json.loads(connection.execute('SELECT value FROM Metadata WHERE key=?',
                                                    (proposal['archive_key'],)).fetchone()[0])
            archive['native_state_sha256'] = hashlib.sha256(b'changed').hexdigest()
            unsigned = {key: value for key, value in archive.items() if key != 'transition_id'}
            archive['transition_id'] = resume.digest(unsigned)
            connection.execute('UPDATE Metadata SET value=? WHERE key=?',
                               (resume.canonical(archive), proposal['archive_key']))

    with pytest.raises(ValueError, match=(
        'database binding changed|transition archive is unavailable|transition receipt changed|'
            'pre-state digest does not match')):
        resume.pending_manifest(db, now=1003, root=root)
    assert resume.inspect_manifest(db)[0]['state'] == 'prepared'


@pytest.mark.asyncio
async def test_terminal_singleton_claim_preserves_independent_suspended_sol_continuity(tmp_path):
    db, preflight, root, recorder, terminal, sol, evidence = await terminal_with_suspended_sol_fixture(tmp_path)
    assert evidence['reconciliation']['balance_coverage'][0]['owned_residual'] == '1.649'
    assert resume.inspect_fleet_owner_snapshot(
        next(row['database_snapshot_path'] for row in evidence['fleet_ownership']['owner_observations']
             if row['owner'] == 'rsi_modular_v2'), 'rsi_modular_v2')['owned_residual_by_asset'] == {'BNB': '0.249'}
    assert resume.inspect_fleet_owner_snapshot(
        next(row['database_snapshot_path'] for row in evidence['fleet_ownership']['owner_observations']
             if row['owner'] == 'meridian_v3'), 'meridian_v3')['owned_residual_by_asset'] == {'BNB': '0.2'}
    manifest = resume.prepare(db, preflight, evidence, now=1000, root=root,
                              terminal_suspended_executor_ids=(terminal.config.id,),
                              continuity_executor_ids=(sol.config.id,))
    assert [row['executor_id'] for row in manifest['continuity_rows']] == [sol.config.id]
    assert resume.pending_manifest(db, now=1001, root=root) == manifest
    with resume.connect(db) as connection:
        before_sol = dict(connection.execute('SELECT * FROM Executors WHERE id=?', (sol.config.id,)).fetchone())
    strategy = terminal._strategy
    strategy.controllers = {
        'ok-rsi-bnb': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='BNB-USDC')),
        'ok-rsi-sol': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='SOL-USDC')),
    }
    strategy.markets = {'okx': {'BNB-USDC', 'SOL-USDC'}}
    strategy.ready_to_trade = True
    strategy.market_data_provider = MagicMock(ready=True)
    strategy.market_data_provider.get_price_by_type.return_value = Decimal('100')
    connector = strategy.connectors['okx']
    connector.in_flight_orders = {}
    connector.get_price_by_type.return_value = Decimal('100')
    connector.get_balance.side_effect = lambda asset: Decimal('1.649') if asset == 'BNB' else Decimal('10')
    connector.get_available_balance.side_effect = lambda asset: Decimal('1.649') if asset == 'BNB' else Decimal('10')
    connector._order_tracker.fetch_order.return_value = None
    with patch('hummingbot.strategy_v2.executors.executor_orchestrator.Security.api_keys', return_value=TEST_NATIVE_OKX_KEYS.copy()), \
            patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=1001, root=root)), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim',
                  side_effect=lambda path, value: resume.claim(path, value, now=1001)), \
            patch.object(PositionExecutor, 'get_trading_rules', return_value=terminal.trading_rules), \
            patch.object(PositionExecutor, 'start') as start:
        orchestrator = ExecutorOrchestrator(strategy)
        assert orchestrator._planned_resume_pending
        assert orchestrator.resume_planned_positions_if_ready()
    restored = {item.config.id: item for group in orchestrator.active_executors.values() for item in group}
    assert set(restored) == {terminal.config.id, sol.config.id}
    assert restored[terminal.config.id]._planned_resume_id == manifest['id']
    assert restored[sol.config.id]._restart_restored['suspended'] is True
    assert restored[sol.config.id]._trailing_stop_trigger_pct == Decimal('.017')
    assert restored[sol.config.id]._trailing_stop_move_count == 2
    assert restored[sol.config.id].amount_to_close == Decimal('1')
    assert start.call_count == 2
    with resume.connect(db) as connection:
        after_sol = connection.execute('SELECT * FROM Executors WHERE id=?', (sol.config.id,)).fetchone()
        assert before_sol == dict(after_sol)
        assert connection.execute('SELECT amount FROM Position WHERE controller_id=? AND connector_name=? AND trading_pair=?',
                                  ('ok-rsi-bnb', 'okx', 'BNB-USDC')).fetchone()[0] == 200000
    assert {row.client_order_id for row in recorder.get_all_position_hold_ledger_entries()} == {'unrelated'}


@pytest.mark.asyncio
@pytest.mark.parametrize(('mutation', 'message'), [
    ('unmarked', 'exact planned-restart marker'),
    ('trailing', 'state, fills, orders, or trailing state changed'),
    ('config', 'identity/configuration changed'),
])
async def test_terminal_mode_rejects_unproven_or_drifting_sol_continuity(tmp_path, mutation, message):
    db, preflight, root, _, terminal, sol, evidence = await terminal_with_suspended_sol_fixture(tmp_path)
    with sqlite3.connect(db) as connection:
        if mutation == 'unmarked':
            custom = json.loads(connection.execute('SELECT custom_info FROM Executors WHERE id=?',
                                                   (sol.config.id,)).fetchone()[0])
            custom.pop('restart_suspended')
            connection.execute('UPDATE Executors SET custom_info=? WHERE id=?',
                               (resume.canonical(custom), sol.config.id))
        elif mutation == 'trailing':
            custom = json.loads(connection.execute('SELECT custom_info FROM Executors WHERE id=?',
                                                   (sol.config.id,)).fetchone()[0])
            custom['trailing_move_count'] = 3
            connection.execute('UPDATE Executors SET custom_info=? WHERE id=?',
                               (resume.canonical(custom), sol.config.id))
        else:
            config = json.loads(connection.execute('SELECT config FROM Executors WHERE id=?',
                                                   (sol.config.id,)).fetchone()[0])
            config['trading_pair'] = 'ETH-USDC'
            connection.execute('UPDATE Executors SET config=? WHERE id=?',
                               (resume.canonical(config), sol.config.id))
    evidence = refresh_stopped_evidence(db, evidence, 1000)
    with pytest.raises(ValueError, match=message):
        resume.prepare(db, preflight, evidence, now=1000, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,),
                       continuity_executor_ids=(sol.config.id,))


@pytest.mark.asyncio
async def test_terminal_mode_rejects_unlisted_running_executor_and_generic_mode_stays_strict(tmp_path):
    db, preflight, root, _, terminal, sol, evidence = await terminal_with_suspended_sol_fixture(tmp_path)
    with pytest.raises(ValueError, match='unfinished executor set differs from explicit'):
        resume.prepare(db, preflight, evidence, now=1000, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,))
    with pytest.raises(ValueError, match='database still has unfinished executors'):
        resume.prepare(db, preflight, evidence, now=1000, root=root)


@pytest.mark.asyncio
async def test_terminal_mode_rejects_changed_durable_risk_state(tmp_path):
    db, preflight, root, _, terminal, sol, evidence = await terminal_with_suspended_sol_fixture(tmp_path)
    with sqlite3.connect(db) as connection:
        state = json.loads(connection.execute('SELECT value FROM Metadata WHERE key=?',
                                              ('strategy-risk:ok_rsi.yml',)).fetchone()[0])
        state['max_global_pnl'] = '0.01'
        connection.execute('UPDATE Metadata SET value=? WHERE key=?',
                           (json.dumps(state, sort_keys=True), 'strategy-risk:ok_rsi.yml'))
    evidence = refresh_stopped_evidence(db, evidence, 1000)
    with pytest.raises(ValueError, match='durable V1 risk state changed'):
        resume.prepare(db, preflight, evidence, now=1000, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,),
                       continuity_executor_ids=(sol.config.id,))


@pytest.mark.asyncio
@pytest.mark.parametrize('marked', [False, True])
async def test_terminal_mode_rejects_unselected_positive_terminal_inventory(tmp_path, marked):
    db, preflight, root, _, terminal, evidence = await suspended_terminal_fixture(tmp_path)
    with sqlite3.connect(db) as connection:
        columns = [row[1] for row in connection.execute('PRAGMA table_info(Executors)')]
        selected = dict(zip(columns, connection.execute('SELECT * FROM Executors WHERE id=?',
                                                        (terminal.config.id,)).fetchone()))
        custom = json.loads(selected['custom_info'])
        custom['remaining_position_amount_base'] = '.05'
        custom['order_ids'] = ['unselected-terminal-entry']
        if marked:
            custom['restart_suspended']['remaining_position_amount_base'] = '.05'
        else:
            custom.pop('restart_suspended', None)
        selected.update(id='unselected-terminal', custom_info=resume.canonical(custom))
        placeholders = ', '.join('?' for _ in columns)
        connection.execute(f"INSERT INTO Executors ({', '.join(columns)}) VALUES ({placeholders})",
                           [selected[column] for column in columns])
    evidence = refresh_stopped_evidence(db, evidence, 1000)
    with pytest.raises(ValueError, match='unselected executor has unresolved lifecycle ownership'):
        resume.prepare(db, preflight, evidence, now=1000, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,))


@pytest.mark.asyncio
@pytest.mark.parametrize(('field', 'value'), [
    ('stopped', True),
    ('max_global_pnl', 'NaN'),
    ('max_pnl_by_controller', {}),
])
async def test_terminal_mode_rejects_native_invalid_risk_state_before_claim(tmp_path, field, value):
    db, preflight, root, _, terminal, evidence = await suspended_terminal_fixture(tmp_path)
    for database_path in (db, preflight):
        with sqlite3.connect(database_path) as connection:
            state = json.loads(connection.execute('SELECT value FROM Metadata WHERE key=?',
                                                  ('strategy-risk:ok_rsi.yml',)).fetchone()[0])
            state[field] = value
            connection.execute('UPDATE Metadata SET value=? WHERE key=?',
                               (json.dumps(state, sort_keys=True), 'strategy-risk:ok_rsi.yml'))
    evidence = refresh_stopped_evidence(db, evidence, 1000)
    with pytest.raises(ValueError, match='durably paused|risk peak state'):
        resume.prepare(db, preflight, evidence, now=1000, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,))


@pytest.mark.asyncio
async def test_terminal_mode_rejects_tampered_fleet_owner_residual_even_with_recomputed_bundle_hash(tmp_path):
    db, preflight, root, _, terminal, evidence = await suspended_terminal_fixture(tmp_path)
    bundle = evidence['fleet_ownership']
    v3 = next(row for row in bundle['owner_observations'] if row['owner'] == 'meridian_v3')
    v3['owned_residual_by_asset']['BNB'] = '.1'
    resume.seal_fleet_ownership(bundle, TEST_NATIVE_OKX_KEYS, evidence)
    evidence['reconciliation']['fleet_ownership_authenticator'] = bundle['authenticator']
    with pytest.raises(ValueError, match='residual differs from native ledger and executor receipts'):
        resume.prepare(db, preflight, evidence, now=1000, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,))


@pytest.mark.asyncio
async def test_fleet_owner_snapshot_order_script_must_match_declared_owner(tmp_path):
    db, preflight, root, _, terminal, evidence = await suspended_terminal_fixture(tmp_path)
    v3 = next(row for row in evidence['fleet_ownership']['owner_observations']
              if row['owner'] == 'meridian_v3')
    with sqlite3.connect(v3['database_snapshot_path']) as connection:
        connection.execute('UPDATE "Order" SET config_file_path=? WHERE id=?',
                           ('rsi_modular_v2.yml', 'v3-entry-order'))
    with pytest.raises(ValueError, match='native fill lacks its owner Order'):
        resume.inspect_fleet_owner_snapshot(v3['database_snapshot_path'], 'meridian_v3')


def test_fleet_owner_snapshot_rechecks_sidecars_after_read(tmp_path, monkeypatch):
    path = write_fleet_owner_snapshot(tmp_path / 'v2-owner.sqlite', 'rsi_modular_v2')
    original = resume._derive_owner_residuals

    def materialize_wal(connection, owner, quantity_increments=None):
        result = original(connection, owner, quantity_increments=quantity_increments)
        Path(str(path) + '-wal').write_bytes(b'wal appeared while snapshot was read')
        return result

    monkeypatch.setattr(resume, '_derive_owner_residuals', materialize_wal)
    with pytest.raises(ValueError, match='uncheckpointed SQLite sidecars'):
        resume.inspect_fleet_owner_snapshot(path, 'rsi_modular_v2')


@pytest.mark.asyncio
async def test_fleet_bundle_authenticator_must_match_native_v1_credentials(tmp_path):
    db, preflight, root, _, terminal, evidence = await suspended_terminal_fixture(tmp_path)
    evidence['fleet_ownership']['authenticator'] = 'f' * 64
    with pytest.raises(ValueError, match='authenticator does not match'):
        resume.prepare(db, preflight, evidence, now=1000, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,))


@pytest.mark.parametrize('keys', [{}, {'okx_api_key': 'a', 'okx_secret_key': 'b'},
                                  {'okx_api_key': 'a', 'okx_secret_key': 'b', 'okx_passphrase': ''}])
def test_fleet_authentication_rejects_missing_native_okx_credentials(keys):
    with pytest.raises(ValueError, match='decrypted native OKX credential set'):
        resume.verify_fleet_ownership({'authenticator': 'x'}, keys)


def test_fleet_authentication_binds_optional_native_okx_subdomain():
    bundle = {'schema': 1, 'observed_at': 1, 'expires_at': 2, 'owner_observations': []}
    keys = {**TEST_NATIVE_OKX_KEYS, 'okx_registration_sub_domain': 'app'}
    resume.seal_fleet_ownership(bundle, keys, {})
    assert resume.verify_fleet_ownership(bundle, keys)
    with pytest.raises(ValueError, match='authenticator does not match'):
        resume.verify_fleet_ownership(bundle, {**keys, 'okx_registration_sub_domain': 'my'})


@pytest.mark.asyncio
async def test_fleet_bundle_authenticator_binds_all_source_and_snapshot_evidence(tmp_path):
    _, _, _, _, _, evidence = await suspended_terminal_fixture(tmp_path)
    bundle = evidence['fleet_ownership']
    bundle['owner_observations'][0]['source_identity']['container_id'] = 'e' * 64
    with pytest.raises(ValueError, match='authenticator does not match'):
        resume.validate_fleet_ownership(evidence, 1000, TEST_NATIVE_OKX_KEYS)


@pytest.mark.asyncio
async def test_fleet_bundle_authenticator_binds_v1_wallet_and_raw_venue_evidence(tmp_path):
    _, _, _, _, _, evidence = await suspended_terminal_fixture(tmp_path)
    evidence['reconciliation']['balance_coverage'][0]['cash_balance'] = '999'
    with pytest.raises(ValueError, match='sealed recovery evidence changed'):
        resume.validate_fleet_ownership(evidence, 1000, TEST_NATIVE_OKX_KEYS)


@pytest.mark.asyncio
@pytest.mark.skipif(os.environ.get('RSIBOT_EPHEMERAL_SECURITY_TEST') != '1',
                    reason='runs only inside a disposable native Hummingbot test container')
async def test_terminal_prepare_cli_unlocks_encrypted_native_config_in_fresh_process(tmp_path):
    from pydantic import SecretStr

    from hummingbot.client.config.config_crypt import (
        PASSWORD_VERIFICATION_PATH,
        ETHKeyFileSecretManger,
        store_password_verification,
    )
    from hummingbot.client.config.config_helpers import get_connector_config_yml_path
    from hummingbot.connector.exchange.okx.okx_utils import OKXConfigMap

    connector_path = get_connector_config_yml_path('okx')
    if connector_path.exists() or PASSWORD_VERIFICATION_PATH.exists():
        pytest.skip('disposable image contains pre-existing encrypted connector state')
    db, preflight, root, _, terminal, evidence = await suspended_terminal_fixture(tmp_path)
    now = time.time()
    evidence = refresh_stopped_evidence(db, evidence, now)
    bundle = evidence['fleet_ownership']
    bundle['observed_at'] = now
    bundle['expires_at'] = now + 200
    for observation in bundle['owner_observations']:
        observation['observed_at'] = now
    resume.seal_fleet_ownership(bundle, TEST_NATIVE_OKX_KEYS, evidence)
    evidence['reconciliation']['fleet_ownership_authenticator'] = bundle['authenticator']
    evidence_path = tmp_path / 'prepare-evidence.json'
    evidence_path.write_text(resume.canonical(evidence))
    password = 'ephemeral-test-password'
    password_path = tmp_path / 'native-password'
    password_path.write_text(password + '\n')
    password_path.chmod(0o600)
    manager = ETHKeyFileSecretManger(password)
    Security = resume.Security
    Security.secrets_manager = manager
    try:
        store_password_verification(manager)
        Security.update_secure_config(ClientConfigAdapter(OKXConfigMap(
            okx_api_key=SecretStr(TEST_NATIVE_OKX_KEYS['okx_api_key']),
            okx_secret_key=SecretStr(TEST_NATIVE_OKX_KEYS['okx_secret_key']),
            okx_passphrase=SecretStr(TEST_NATIVE_OKX_KEYS['okx_passphrase']),
            okx_registration_sub_domain=TEST_NATIVE_OKX_KEYS['okx_registration_sub_domain'],
        )))
        command = [sys.executable, '-m', 'hummingbot.strategy_v2.executors.planned_position_resume',
                   'prepare', '--db', str(db), '--preflight-db', str(preflight), '--evidence', str(evidence_path),
                   '--runtime-root', str(root), '--password-file', str(password_path),
                   '--restart-suspended-terminal-executor-id', terminal.config.id]
        process = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)
        assert process.returncode == 0, process.stderr
        assert terminal.config.id in process.stdout
        assert all(secret not in process.stdout + process.stderr
                   for secret in (*TEST_NATIVE_OKX_KEYS.values(), password))
    finally:
        Security.remove_secure_config('okx')
        PASSWORD_VERIFICATION_PATH.unlink(missing_ok=True)
        Security.secrets_manager = None


@pytest.mark.asyncio
async def test_terminal_mode_aggregates_v1_held_inventory_across_unselected_pairs(tmp_path):
    db, preflight, root, recorder, terminal, evidence = await suspended_terminal_fixture(tmp_path)
    receipt = {'trade_type': 'BUY', 'executed_amount_base': '.5', 'executed_amount_quote': '50',
               'order_fills': {'eth-held-trade': {'fill_base_amount': '.5', 'fill_price': '100',
                                                  'fill_quote_amount': '50',
                                                  'fee': {'percent': '0', 'flat_fees': []}}}}
    with sqlite3.connect(db) as connection:
        connection.execute('INSERT INTO PositionHoldLedger '
                           '(id, controller_id, executor_id, connector_name, trading_pair, position_side, order_side, '
                           'timestamp, client_order_id, exchange_order_id, price, amount, executed_amount_base, '
                           'executed_amount_quote, cumulative_fee_paid_quote, order_payload) '
                           'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                           ('eth-held-row', 'ok-rsi-eth', 'eth-held-executor', 'okx', 'ETH-USDC', 'BUY', 'BUY',
                            1000, 'eth-held-order', 'eth-held-order', 100, 500000, 500000, 50000000, 0,
                            json.dumps(receipt, sort_keys=True)))
    from hummingbot.model.exact_order_values import TradeFillExact
    with recorder.sql_manager.get_new_session() as session:
        session.add(Order(id='eth-held-order', config_file_path='ok_rsi.yml', strategy='v2_with_controllers',
                          market='okx', symbol='ETH-USDC', base_asset='ETH', quote_asset='USDC',
                          creation_timestamp=1000, order_type='LIMIT', amount=Decimal('.5'), leverage=1,
                          price=Decimal('100'), last_status='BuyOrderCompleted', last_update_timestamp=1000,
                          exchange_order_id='eth-held-order', position='NIL'))
        session.add(TradeFill(config_file_path='ok_rsi.yml', strategy='v2_with_controllers', market='okx',
                              symbol='ETH-USDC', base_asset='ETH', quote_asset='USDC', timestamp=1000,
                              order_id='eth-held-order', trade_type='BUY', order_type='LIMIT',
                              price=Decimal('100'), amount=Decimal('.5'), leverage=1,
                              trade_fee=receipt['order_fills']['eth-held-trade']['fee'],
                              trade_fee_in_quote=Decimal(0), exact_amount='.5', exact_price='100',
                              exact_trade_fee_in_quote='0', exact_receipt_source='order_filled_event_v1',
                              exchange_trade_id='eth-held-trade', position='NIL'))
        session.add(TradeFillExact(market='okx', order_id='eth-held-order',
                                   exchange_trade_id='eth-held-trade', amount='.5', price='100', fee_in_quote='0'))
        session.commit()
    evidence = refresh_stopped_evidence(db, evidence, 1000)
    evidence['reconciliation']['balance_coverage'].append({
        'asset': 'ETH', 'owned_residual': '.5', 'cash_balance': '10',
        'available_balance': '10', 'covered': True})
    bundle = evidence['fleet_ownership']
    resume.seal_fleet_ownership(bundle, TEST_NATIVE_OKX_KEYS, evidence)
    evidence['reconciliation']['fleet_ownership_authenticator'] = bundle['authenticator']
    manifest = resume.prepare(db, preflight, evidence, now=1000, root=root,
                              terminal_suspended_executor_ids=(terminal.config.id,))
    assert any(row['asset'] == 'ETH' and row['owned_residual'] == '.5'
               for row in manifest['evidence']['reconciliation']['balance_coverage'])


@pytest.mark.asyncio
@pytest.mark.parametrize(('mutation', 'message'), [
    ('missing', 'fresh raw SOL venue order selection differs'),
    ('fill', 'fresh SOL venue fill values differ'),
    ('fee', 'fresh SOL venue fee amount or currency differs'),
])
async def test_terminal_mode_requires_fresh_exact_sol_raw_order_and_fill_proof(tmp_path, mutation, message):
    db, preflight, root, _, terminal, sol, evidence = await terminal_with_suspended_sol_fixture(tmp_path)
    proof = evidence['reconciliation']['continuity_executors'][0]['venue_orders'][0]
    if mutation == 'missing':
        evidence['reconciliation']['continuity_executors'] = []
    elif mutation == 'fill':
        proof['raw_fills'][0]['fillPx'] = '101'
    else:
        proof['raw_fills'][0]['fee'] = '-0.01'
    with pytest.raises(ValueError, match=message):
        resume.prepare(db, preflight, evidence, now=1000, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,),
                       continuity_executor_ids=(sol.config.id,))


@pytest.mark.asyncio
async def test_terminal_mode_rejects_sol_inventory_not_conserved_by_fresh_net_entry(tmp_path):
    db, preflight, root, _, terminal, sol, evidence = await terminal_with_suspended_sol_fixture(tmp_path)
    for database_path in (db, preflight):
        with sqlite3.connect(database_path) as connection:
            row = connection.execute('SELECT custom_info FROM Executors WHERE id=?',
                                     (sol.config.id,)).fetchone()
            custom = json.loads(row[0])
            custom['remaining_position_amount_base'] = '0.9'
            if custom.get('restart_suspended'):
                custom['restart_suspended']['remaining_position_amount_base'] = '0.9'
            connection.execute('UPDATE Executors SET custom_info=? WHERE id=?',
                               (resume.canonical(custom), sol.config.id))
    evidence = refresh_stopped_evidence(db, evidence, 1000)
    with pytest.raises(ValueError, match='fresh SOL venue net entry differs from suspended inventory'):
        resume.prepare(db, preflight, evidence, now=1000, root=root,
                       terminal_suspended_executor_ids=(terminal.config.id,),
                       continuity_executor_ids=(sol.config.id,))


@pytest.mark.asyncio
async def test_restart_suspended_terminal_claims_exact_target_and_rebuilds_other_bags_once(tmp_path):
    db, preflight, root, recorder, old, evidence = await suspended_terminal_fixture(tmp_path)
    manifest = resume.prepare(db, preflight, evidence, now=1000, root=root,
                              terminal_suspended_executor_ids=(old.config.id,))
    position = manifest['positions'][0]
    assert old.config.id != old.config.controller_id
    assert position['preflight']['status'] == 4
    assert position['preflight']['custom_info']['restart_suspended']['reason'] == 'planned_restart'
    assert position['resume_kind'] == 'restart_suspended_terminal_v1'
    assert position['net_base'] == '1'
    assert position['ownership_archive']['ledger_rows'] == []
    assert len(position['ownership_archive']['position_rows']) == 1
    assert resume.pending_manifest(db, now=1001, root=root) == manifest

    strategy = old._strategy
    strategy.controllers = {'ok-rsi-bnb': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='BNB-USDC'))}
    strategy.markets = {'okx': {'BNB-USDC'}}
    strategy.ready_to_trade = True
    strategy.market_data_provider = MagicMock(ready=True)
    strategy.market_data_provider.get_price_by_type.return_value = Decimal('100')
    connector = strategy.connectors['okx']
    connector.in_flight_orders = {}
    connector.get_price_by_type.return_value = Decimal('100')
    connector.get_available_balance.return_value = Decimal('1.649')
    connector.get_balance.return_value = Decimal('1.649')
    connector._order_tracker.fetch_order.return_value = None
    with patch('hummingbot.strategy_v2.executors.executor_orchestrator.Security.api_keys', return_value=TEST_NATIVE_OKX_KEYS.copy()), \
            patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=1001, root=root)), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim',
                  side_effect=lambda path, value: resume.claim(path, value, now=1001)), \
            patch.object(PositionExecutor, 'get_trading_rules', return_value=old.trading_rules), \
            patch.object(PositionExecutor, 'start') as start:
        orchestrator = ExecutorOrchestrator(strategy)
        assert orchestrator._planned_resume_pending
        assert orchestrator.resume_planned_positions_if_ready()
        start.assert_called_once()

    restored = orchestrator.active_executors['ok-rsi-bnb']
    assert len(restored) == 1
    assert restored[0].config.id == old.config.id
    assert restored[0].amount_to_close == Decimal('1')
    assert restored[0]._planned_resume_id == manifest['id']
    assert sum(p.buy_amount_base - p.sell_amount_base for p in orchestrator.positions_held['ok-rsi-bnb']) == Decimal('.2')
    assert len(orchestrator.get_all_reports()['ok-rsi-bnb']['executors']) == 1
    assert {row.client_order_id for row in recorder.get_all_position_hold_ledger_entries()} == {'unrelated'}
    with resume.connect(db) as connection:
        assert connection.execute('SELECT amount FROM Position WHERE controller_id=? AND connector_name=? AND trading_pair=?',
                                  ('ok-rsi-bnb', 'okx', 'BNB-USDC')).fetchone()[0] == 200000
    claimed = resume.inspect_manifest(db)[0]
    assert claimed['state'] == 'claimed'
    assert recorder.get_all_executors()[0].status == RunnableStatus.RUNNING
    with pytest.raises(ValueError, match='already consumed'):
        resume.claim(db, manifest, now=1002)


@pytest.mark.asyncio
async def test_restart_suspended_terminal_rejects_mixed_executor_selection(tmp_path):
    db, preflight, root, _, old, evidence = await suspended_terminal_fixture(tmp_path)
    evidence['executors'].append({'executor_id': 'ordinary-executor-id'})

    with pytest.raises(ValueError, match='selection must contain only the suspended terminal executor'):
        resume.prepare(db, preflight, evidence, now=1000, root=root,
                       terminal_suspended_executor_ids=(old.config.id,))


@pytest.mark.asyncio
async def test_restart_suspended_terminal_startup_checks_total_verified_owner_inventory(tmp_path):
    db, preflight, root, recorder, old, evidence = await suspended_terminal_fixture(tmp_path)
    resume.prepare(db, preflight, evidence, now=1000, root=root,
                   terminal_suspended_executor_ids=(old.config.id,))
    strategy = old._strategy
    strategy.controllers = {'ok-rsi-bnb': SimpleNamespace(
        config=SimpleNamespace(connector_name='okx', trading_pair='BNB-USDC'))}
    strategy.markets = {'okx': {'BNB-USDC'}}
    strategy.ready_to_trade = True
    strategy.market_data_provider = MagicMock(ready=True)
    strategy.market_data_provider.get_price_by_type.return_value = Decimal('100')
    connector = strategy.connectors['okx']
    connector.in_flight_orders = {}
    connector.get_price_by_type.return_value = Decimal('100')
    connector.get_available_balance.return_value = Decimal('1.1')
    connector.get_balance.return_value = Decimal('1.1')
    connector._order_tracker.fetch_order.return_value = None
    with patch('hummingbot.strategy_v2.executors.executor_orchestrator.Security.api_keys', return_value=TEST_NATIVE_OKX_KEYS.copy()), \
            patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=1001, root=root)), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim',
                  side_effect=lambda path, value: resume.claim(path, value, now=1001)):
        orchestrator = ExecutorOrchestrator(strategy)
        assert orchestrator.resume_planned_positions_if_ready()

    assert orchestrator._resume_preflight_failed is True
    assert 'all verified owner inventory' in orchestrator._planned_resume_failure
    assert resume.inspect_manifest(db)[0]['state'] == 'prepared'
    assert old.config.id not in {executor.config.id for executors in orchestrator.active_executors.values()
                                 for executor in executors}


@pytest.mark.asyncio
async def test_restart_suspended_terminal_startup_rechecks_unlocked_credential_identity(tmp_path):
    db, preflight, root, recorder, old, evidence = await suspended_terminal_fixture(tmp_path)
    resume.prepare(db, preflight, evidence, now=1000, root=root,
                   terminal_suspended_executor_ids=(old.config.id,))
    strategy = old._strategy
    strategy.controllers = {'ok-rsi-bnb': SimpleNamespace(
        config=SimpleNamespace(connector_name='okx', trading_pair='BNB-USDC'))}
    strategy.markets = {'okx': {'BNB-USDC'}}
    strategy.ready_to_trade = True
    strategy.market_data_provider = MagicMock(ready=True)
    strategy.market_data_provider.get_price_by_type.return_value = Decimal('100')
    connector = strategy.connectors['okx']
    connector.in_flight_orders = {}
    connector.get_price_by_type.return_value = Decimal('100')
    connector.get_available_balance.return_value = Decimal('2')
    connector.get_balance.return_value = Decimal('2')
    connector._order_tracker.fetch_order.return_value = None
    with (
        patch(
            'hummingbot.strategy_v2.executors.executor_orchestrator.Security.api_keys',
            return_value={'okx_api_key': 'different-api-key', 'okx_secret_key': 'different-secret',
                          'okx_passphrase': 'different-passphrase'},
        ),
        patch.object(MarketsRecorder, 'get_instance', return_value=recorder),
        patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
              side_effect=lambda path: resume.pending_manifest(path, now=1001, root=root)),
        patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim',
              side_effect=lambda path, value: resume.claim(path, value, now=1001)),
    ):
        orchestrator = ExecutorOrchestrator(strategy)
        orchestrator.resume_planned_positions_if_ready()
    assert orchestrator._resume_preflight_failed is True
    assert 'authenticator does not match current native OKX credentials' in orchestrator._planned_resume_failure
    assert resume.inspect_manifest(db)[0]['state'] == 'prepared'


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
async def test_post_claim_failure_is_reported_never_retried_and_never_blocks(tmp_path, failure):
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
        assert orchestrator.resume_planned_positions_if_ready()  # reported; trading is not fenced
        assert broken.call_count == 1
        assert orchestrator.resume_planned_positions_if_ready()
        assert broken.call_count == 1
        assert 'injected after claim' in orchestrator._planned_resume_failure
        assert orchestrator._planned_resume_pending is False
        assert resume.inspect_manifest(db)[0]['state'] == 'claimed'
        assert old.config.id in orchestrator._claimed_resume_unresolved['owner']  # reported for reconciliation
        assert old.config.id in orchestrator.recovery_required['owner']
        for side in (TradeType.BUY, TradeType.SELL):  # never an order fence
            old.place_order('okx', 'ETH-USDT', OrderType.LIMIT, side, Decimal('.1'), price=Decimal('100'))
        orchestrator.store_all_executors(clear=False)
        orchestrator.sync_live_executors_to_db()
        assert not recorder.get_all_executors()[0].is_done
        await orchestrator.stop(strict=True)
        assert not recorder.get_all_executors()[0].is_done  # the claimed row survives for the next boot
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        after_crash = ExecutorOrchestrator(strategy)
        # The claimed row has no order snapshots to rebuild from: reported on the
        # next boot as well, never a fence.
        assert after_crash._planned_resume_pending is False
        assert after_crash.active_executors['owner'] == []
        assert after_crash.recovery_required['owner'] == [old.config.id]
        assert after_crash._claimed_resume_unresolved == {'owner': {old.config.id: ('okx', 'ETH-USDT')}}
        assert not hasattr(after_crash, 'claimed_resume_controller_blocked')
    assert strategy.buy.call_count == 1
    assert strategy.sell.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['expired', 'malformed', 'after_commit'])
async def test_unverified_resume_preflight_is_reported_and_never_blocks(tmp_path, failure):
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
        assert orchestrator._planned_resume_failure is not None
        for side in (TradeType.BUY, TradeType.SELL):  # reported, never an order fence
            old.place_order('okx', 'ETH-USDT', OrderType.LIMIT, side, Decimal('.1'), price=Decimal('100'))
        assert resume.inspect_manifest(db)[0]['state'] == ('claimed' if failure == 'after_commit' else 'prepared')
        if failure == 'after_commit':
            assert old.config.id in orchestrator._claimed_resume_unresolved['owner']
            orchestrator.store_all_executors(clear=False)
            orchestrator.sync_live_executors_to_db()
            assert not recorder.get_all_executors()[0].is_done
    assert strategy.buy.call_count == 1
    assert strategy.sell.call_count == 1


@pytest.mark.asyncio
async def test_claim_missing_owner_identity_is_reported_and_status_readable(tmp_path):
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
    assert observation['unfinished_claim_controllers'] == []
    assert orchestrator._planned_resume_failure == 'Unfinished claim has unavailable controller/market identity'
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
    if 'fleet_ownership' in fresh:
        bundle = fresh['fleet_ownership']
        resume.seal_fleet_ownership(bundle, TEST_NATIVE_OKX_KEYS, fresh)
        fresh['reconciliation']['fleet_ownership_authenticator'] = bundle['authenticator']
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
