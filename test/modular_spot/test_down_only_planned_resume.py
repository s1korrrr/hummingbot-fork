"""Exact native down-only receipts survive serialization and supported planned resume."""
import json
import sqlite3
from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors import test_planned_position_resume as legacy
from test.test_ok_rsi_down_only_entry_execution import make_config
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from test_native_lifecycle import recorder as _recorder_fixture

from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import DownOnlyMakerPositionExecutor
from controllers.generic.modular_spot_core.native_funding import _config_identity
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors import planned_position_resume as resume
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus

recorder = _recorder_fixture


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


def down_only_executor(entry_chain_contract=None):
    old = legacy.make_executor()
    barriers = old.config.triple_barrier_config.model_copy(update={'open_order_limit_max_age_seconds': 0})
    config = make_config(id=old.config.id, timestamp=old.config.timestamp,
                         controller_id='owner', trading_pair=old.config.trading_pair,
                         triple_barrier_config=barriers,
                         entry_chain_contract=entry_chain_contract)
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=old.trading_rules):
        result = DownOnlyMakerPositionExecutor(old._strategy, config)
    result._status = RunnableStatus.RUNNING
    result._sleep = AsyncMock()
    result.get_price = MagicMock(return_value=D('100'))
    result.get_in_flight_order = MagicMock(return_value=None)
    return result


@pytest.mark.asyncio
async def test_closed_two_child_chain_prepare_claim_hydrate_keeps_predecessor_and_blocks_extra_fill(
        tmp_path, stopped_recorder_scope):
    from types import SimpleNamespace

    from hummingbot.core.data_type.common import OrderType
    from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
    from hummingbot.core.event.events import OrderFilledEvent

    contract = resume.ACQUISITION_CHAIN_V1
    original_factory = legacy.make_executor

    def make_chain_executor():
        with patch.object(legacy, 'make_executor', original_factory):
            return down_only_executor(entry_chain_contract=contract)

    with patch.object(legacy, 'make_executor', make_chain_executor):
        db, before, root, recorder, old, evidence = await legacy.stopped_fixture(
            tmp_path, True, '0', trading_pair='ETH-USDC', controller_id='modular_spot_resume')
    terminal_children = evidence['executors'][0]['entry_reconciliation']['orders']
    assert all(set(child) == {'schema', 'source', 'order_id', 'exchange_order_id', 'trading_pair',
                              'side', 'amount', 'filled_base', 'state', 'trade_ids'}
               for child in terminal_children)
    manifest = resume.prepare(db, before, evidence, now=1000, root=root)
    assert all('fills' not in child for child in terminal_children)
    position = resume.pending_manifest(db, now=1001, root=root)['positions'][0]
    resume.claim(db, manifest, now=1001)
    strategy = old._strategy
    strategy.connectors['okx'].get_available_balance.return_value = D('10')
    strategy.executor_orchestrator = SimpleNamespace(recovery_required={})
    config = resume.parse_position_config(position['config'])
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=old.trading_rules):
        recovered = DownOnlyMakerPositionExecutor(strategy, config)
    recovered.hydrate_planned_position(position, manifest['id'])
    assert [order.order_id for order in recovered._completed_open_orders] == ['entry-old']
    assert recovered._open_order.order_id == 'entry'
    assert recovered.amount_to_close == D('1')
    recovered.place_order = MagicMock()
    recovered.place_open_order()
    recovered.control_open_order()
    recovered.place_order.assert_not_called()

    recovered._status = RunnableStatus.RUNNING
    event = OrderFilledEvent(1002, 'entry-old', 'ETH-USDC', TradeType.BUY, OrderType.LIMIT_MAKER,
                             D('99'), D('.1'), AddedToCostTradeFee(), 'venue-old', 'late-old-fill')
    recovered.process_order_filled_event(0, None, event)
    await recovered.control_task()
    assert recovered._entry_lifecycle['state'] == 'reconciliation_required'
    recovered.place_order.assert_not_called()


@pytest.mark.asyncio
async def test_explicit_legacy_transition_prepares_and_claims_from_fresh_two_child_proof(
        tmp_path, stopped_recorder_scope):
    from hashlib import sha256

    contract = resume.ACQUISITION_CHAIN_V1
    original_factory = legacy.make_executor

    def make_chain_executor():
        with patch.object(legacy, 'make_executor', original_factory):
            return down_only_executor(entry_chain_contract=contract)

    with patch.object(legacy, 'make_executor', make_chain_executor):
        db, before, root, _, _, evidence = await legacy.stopped_fixture(
            tmp_path, True, '0', trading_pair='ETH-USDC', controller_id='modular_spot_resume')

    executor_id = evidence['executors'][0]['executor_id']
    for path in (db, before):
        with sqlite3.connect(path) as connection:
            row = connection.execute('SELECT config, custom_info FROM Executors WHERE id=?', (executor_id,)).fetchone()
            config, custom = map(json.loads, row)
            config['entry_chain_contract'] = None
            custom.pop('acquisition_chain')
            custom['entry_lifecycle'] = {
                'cancel_source': '31', 'replacement_purpose': 'post_only_repair',
                'replacement_of_order_id': 'entry-old', 'order_id': 'entry',
                'reprice_count': 1, 'cancel_event_timestamp': 200,
            }
            connection.execute('UPDATE Executors SET config=?, custom_info=? WHERE id=?',
                               (json.dumps(config), json.dumps(custom), executor_id))
    with sqlite3.connect(db) as stopped_connection, sqlite3.connect(before) as preflight_connection:
        stopped_custom = json.loads(stopped_connection.execute(
            'SELECT custom_info FROM Executors WHERE id=?', (executor_id,)).fetchone()[0])
        preflight_row = preflight_connection.execute(
            'SELECT custom_info FROM Executors WHERE id=?', (executor_id,)).fetchone()
        preflight_custom = json.loads(preflight_row[0])
        preflight_custom['order_ids'] = stopped_custom['order_ids']
        preflight_custom['execution_order_snapshots'] = stopped_custom['execution_order_snapshots']
        preflight_connection.execute('UPDATE Executors SET custom_info=? WHERE id=?',
                                     (json.dumps(preflight_custom), executor_id))
    with sqlite3.connect(db) as connection:
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    database_sha = sha256(db.read_bytes()).hexdigest()
    evidence['reconciliation']['database']['sha256'] = database_sha
    evidence['reconciliation']['database']['sha256_after'] = database_sha

    manifest = resume.prepare(db, before, evidence, now=1000, root=root,
                              legacy_transition_executor_ids=(executor_id,))
    position = resume.pending_manifest(db, now=1001, root=root)['positions'][0]
    assert position['config']['entry_chain_contract'] is None
    assert position['resume_config']['entry_chain_contract'] == resume.ACQUISITION_CHAIN_V1
    assert position['legacy_transition']['original_config_sha256']
    resume.claim(db, manifest, now=1001)
    with resume.connect(db) as connection:
        stored = connection.execute('SELECT custom_info FROM Executors WHERE id=?', (executor_id,)).fetchone()
    marker = json.loads(stored['custom_info'])['planned_position_resume_compatibility_v1']
    assert marker['manifest_id'] == manifest['id']
    assert marker['acquisition_chain_sha256'] == resume.digest(position['acquisition_chain'])


@pytest.mark.asyncio
async def test_legacy_transition_executor_ids_must_be_in_explicit_selection(tmp_path, stopped_recorder_scope):
    original_factory = legacy.make_executor

    def make_down():
        with patch.object(legacy, 'make_executor', original_factory):
            return down_only_executor()

    with patch.object(legacy, 'make_executor', make_down):
        db, before, root, _, _, evidence = await legacy.stopped_fixture(
            tmp_path, True, '0', trading_pair='ETH-USDC', controller_id='modular_spot_resume')
    with pytest.raises(ValueError, match='not in the explicit selection'):
        resume.prepare(db, before, evidence, now=1000, root=root,
                       legacy_transition_executor_ids=('unselected-executor',))


@pytest.mark.asyncio
async def test_actual_recorder_preserves_down_only_config_and_funding_identity(recorder):
    executor = down_only_executor()
    recorder.store_or_update_executor(executor)
    restored = recorder.get_all_executors()[0]
    assert type(restored.config) is type(executor.config)
    assert restored.config == executor.config
    assert _config_identity(restored.config, 'owner') == _config_identity(executor.config, 'owner')
    assert restored.model_dump(mode='json')['config'] == executor.config.model_dump(mode='json')


@pytest.mark.asyncio
@pytest.mark.parametrize('owner_kind', ['public', 'spoof'])
async def test_supported_native_resume_retains_class_config_trailing_and_no_buy(tmp_path, owner_kind, stopped_recorder_scope):
    from types import SimpleNamespace

    from controllers.generic.rsi_modular import get_config_class
    original_factory = legacy.make_executor
    # Use native legacy fixture ownership/SQLite/venue proof; replace only the
    # executor constructor with the genuine down-only native implementation.

    def make_down():
        with patch.object(legacy, 'make_executor', original_factory):
            return down_only_executor()
    with patch.object(legacy, 'make_executor', make_down):
        db, before, root, recorder, old, evidence = await legacy.stopped_fixture(tmp_path, True, '0', trading_pair='ETH-USDC', controller_id='modular_spot_resume')
    manifest = resume.prepare(db, before, evidence, now=1000, root=root)
    strategy = old._strategy
    controller_config = get_config_class('ok_rsi')(profile='ok_rsi', id='modular_spot_resume', trading_pair='ETH-USDC',
                                                   entry_acquisition_policy='down_only_maker_v1')
    if owner_kind == 'spoof':
        controller_config = SimpleNamespace(controller_name='ok_rsi', connector_name='okx', trading_pair='ETH-USDC',
                                            entry_acquisition_policy='down_only_maker_v1')
    strategy.controllers = {'modular_spot_resume': SimpleNamespace(config=controller_config)}
    strategy.markets = {'okx': {'ETH-USDC'}}
    strategy.ready_to_trade = True
    strategy.market_data_provider = MagicMock()
    strategy.market_data_provider.ready = True
    strategy.market_data_provider.get_price_by_type.return_value = D('100')
    connector = strategy.connectors['okx']
    connector.in_flight_orders = {}
    connector.get_price_by_type.return_value = D('100')
    connector._order_tracker.fetch_order.return_value = None
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=1001, root=root)), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim',
                  side_effect=lambda path, value: resume.claim(path, value, now=1001)), \
            patch.object(PositionExecutor, 'get_trading_rules', return_value=old.trading_rules), \
            patch.object(PositionExecutor, 'start') as start:
        orchestrator = ExecutorOrchestrator(strategy)
        if owner_kind == 'spoof':
            assert not orchestrator.resume_planned_positions_if_ready()
            assert 'native owner policy differs' in orchestrator._planned_resume_failure
            start.assert_not_called()
            assert resume.inspect_manifest(db)[0]['state'] == 'prepared'
            return
        assert orchestrator.resume_planned_positions_if_ready()
        recovered = orchestrator.active_executors['modular_spot_resume'][0]
        assert type(recovered) is DownOnlyMakerPositionExecutor
        assert recovered.config == old.config
        assert _config_identity(recovered.config, 'modular_spot_resume') == _config_identity(old.config, 'modular_spot_resume')
        assert recovered._planned_resume_id == manifest['id']
        assert recovered._trailing_stop_trigger_pct == old._trailing_stop_trigger_pct
        assert recovered._trailing_stop_move_count == old._trailing_stop_move_count
        recovered._status = RunnableStatus.RUNNING
        recovered.place_order = MagicMock(return_value='protective-exit')
        recovered._sleep = AsyncMock()
        recovered.place_open_order()
        recovered.control_open_order()
        recovered.place_order.assert_not_called()
        connector.get_price_by_type.return_value = D('103')
        await recovered.control_task()
        await recovered.control_task()
        recovered.place_order.assert_called_once()
        assert recovered.place_order.call_args.kwargs['side'] is TradeType.SELL
        assert recovered.place_order.call_args.kwargs['amount'] == D('1')


@pytest.mark.asyncio
@pytest.mark.parametrize('damage,reason', [
    ('reprice', 'unreplaced singleton'),
    ('partial', 'completely filled'),
    ('missing_policy', 'missing native down-only'),
    ('unknown_policy', 'Unsupported persisted'),
])
async def test_resume_rejects_unsupported_down_only_history(tmp_path, damage, reason, stopped_recorder_scope):
    from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
    original_factory = legacy.make_executor

    def make_down():
        with patch.object(legacy, 'make_executor', original_factory):
            return down_only_executor()
    with patch.object(legacy, 'make_executor', make_down):
        db, before, _, _, _, _ = await legacy.stopped_fixture(tmp_path, True, '0')
    with resume.connect(before) as connection:
        preflight = resume.decode_row(connection.execute('SELECT * FROM Executors').fetchone())
    with resume.connect(db) as connection:
        stopped = resume.decode_row(connection.execute('SELECT * FROM Executors').fetchone())
    for row in (preflight, stopped):
        if damage == 'reprice':
            row['custom_info']['entry_acquisition']['reprice_count'] = 1
        elif damage == 'partial':
            row['custom_info']['entry_acquisition']['cumulative_filled_base'] = '.5'
        elif damage == 'missing_policy':
            row['config'] = {key: value for key, value in row['config'].items() if key in PositionExecutorConfig.model_fields}
        else:
            row['config']['entry_acquisition_policy'] = 'unknown_policy'
    with pytest.raises(ValueError, match=reason):
        resume.validate_position(preflight, stopped)
