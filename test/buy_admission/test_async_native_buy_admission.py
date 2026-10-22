"""Offline real-controller coverage for the ACK-before-activation buy boundary."""
import asyncio
from pathlib import Path
from test.hummingbot.strategy_v2.paper.test_paper_funding import funded_config, paper
from test.hummingbot.strategy_v2.paper.test_paper_session import PAIR, causal_reversal_frames
from test.meridian_v3.lifecycle_harness import Scenario, creates, rebound
from test.meridian_v3.test_unified_lifecycle import BTC
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.model.order import Order
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.paper_session import NativePaperSession
from scripts.v2_with_controllers import V2WithControllers


async def _capture_funded_action(session):
    captured = []
    orchestrator = session.executor_orchestrator
    original = orchestrator.execute_actions
    orchestrator.execute_actions = lambda actions: captured.extend(actions)
    try:
        for index, frame in enumerate(causal_reversal_frames()):
            now = 600000 + index * 60
            book = dict(trading_pair=PAIR, sequence=2 + index, timestamp=now,
                        bids=[(90, 10)], asks=[(90.1, 10)])
            candle = dict(connector_name='okx', trading_pair=PAIR, interval='1m', frame=frame, available_at=now)
            await session.step(now, books=[book], candles=[candle])
    finally:
        orchestrator.execute_actions = original
    return [item for item in captured if isinstance(item, CreateExecutorAction)]


@pytest.mark.asyncio
async def test_v2_funds_exact_committed_native_intent_before_activation(tmp_path):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    with patch('socket.socket.connect', side_effect=AssertionError('network forbidden')):
        session = NativePaperSession(paper(), [funded_config()], tmp_path / 'funded.sqlite')
        try:
            orchestrator = session.executor_orchestrator
            [action] = await _capture_funded_action(session)
            owner = session.controllers[action.controller_id]
            assert session.recorder.get_all_executors() == []
            await orchestrator.create_executor_async(action)
            assert [item.config.id for item in orchestrator.active_executors[action.controller_id]] == [action.executor_config.id]
            _, state = owner._funding_store._load()
            assert action.executor_config.id in state['claims']
            assert owner._committed_create_intent is None
        finally:
            await session.close()
            MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_v2_historical_refused_intent_requires_no_native_order_or_fill(tmp_path):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    with patch('socket.socket.connect', side_effect=AssertionError('network forbidden')):
        session = NativePaperSession(paper(), [funded_config()], tmp_path / 'orphan.sqlite')
        try:
            [action] = await _capture_funded_action(session)
            orchestrator = session.executor_orchestrator
            executor, owner = orchestrator._prepare_executor_for_action(action, validate_controller=False)
            terminal = orchestrator._terminalize_unstarted_executor_info(executor.executor_info.model_dump_json())
            await session.recorder.store_executor_info_snapshot_async(terminal)
            info = session.recorder.get_all_executors()[0]
            assert ExecutorOrchestrator.is_refused_unstarted_intent(info)
            store = NativeFundingStore(owner, session.recorder)
            assert store.reconcile()['pending_quote'] == '0'
            assert store._load()[1]['claims'] == {}
            assert session.recorder.get_all_executors()[0].id == action.executor_config.id
            assert owner._executor_row_exists(info.id) is False
            assert owner._get_entry_decision_gate().store.load()['authorization'] is not None
            owner.create_action_refused(action)
            assert owner._get_entry_decision_gate().store.load()['authorization'] is None

            # A contradictory independent native order is never erased by a
            # terminal marker, even though the executor row lists no order ids.
            with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
                sql.add(Order(id='unowned-contradiction', config_file_path=session.recorder.config_file_path,
                              strategy='v2_with_controllers', market=owner.config.connector_name,
                              symbol=PAIR, base_asset='BTC', quote_asset='USDC',
                              creation_timestamp=600000000, order_type='LIMIT_MAKER', amount='0.1',
                              leverage=1, price='90', last_status='BuyOrderCreated',
                              last_update_timestamp=600000000))
            with pytest.raises(ValueError, match='checkpoint|ownership'):
                store.reconcile()
            assert owner._executor_row_exists(info.id) is True
        finally:
            await session.close()
            MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_v3_cancellation_after_commit_keeps_terminal_intent_and_never_activates(tmp_path):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    with patch('socket.socket.connect', side_effect=AssertionError('network forbidden')):
        scenario = Scenario(Path(tmp_path))
        try:
            await scenario.step()
            orchestrator = scenario.session.executor_orchestrator
            original = orchestrator.execute_actions
            captured = []
            orchestrator.execute_actions = lambda actions: captured.extend(actions)
            await scenario.step(rsi={BTC: rebound()})
            orchestrator.execute_actions = original
            [action] = creates({'actions': captured}, BTC)
            with patch.object(orchestrator, '_recheck_executor_activation',
                              AsyncMock(side_effect=asyncio.CancelledError)):
                with pytest.raises(asyncio.CancelledError):
                    await orchestrator.create_executor_async(action)
            [row] = [row for row in scenario.session.recorder.get_all_executors()
                     if row.id == action.executor_config.id]
            assert ExecutorOrchestrator.is_refused_unstarted_intent(row)
            assert not any(item.config.id == action.executor_config.id
                           for item in orchestrator.active_executors[action.controller_id])
        finally:
            await scenario.close()
            MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_v3_factory_dispatch_is_revalidated_without_consuming_it_twice(tmp_path):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    with patch('socket.socket.connect', side_effect=AssertionError('network forbidden')):
        scenario = Scenario(Path(tmp_path))
        try:
            await scenario.step()
            orchestrator = scenario.session.executor_orchestrator
            original = orchestrator.execute_actions
            captured = []
            orchestrator.execute_actions = lambda actions: captured.extend(actions)
            await scenario.step(rsi={BTC: rebound()})
            orchestrator.execute_actions = original
            [action] = creates({'actions': captured}, BTC)
            context = scenario.controller.contexts[BTC]
            assert action.executor_config.id not in context.runtime.dispatched
            await orchestrator.create_executor_async(action)
            assert action.executor_config.id in context.runtime.dispatched
            assert [item.config.id for item in orchestrator.active_executors[action.controller_id]
                    if item.config.trading_pair == BTC] == [action.executor_config.id]
            assert not scenario.controller.validate_executor_config(action.executor_config)
            orchestrator._note_native_buy_outcome(action)
            outcome = orchestrator._native_buy_outcomes[action.controller_id][BTC]
            assert outcome['failure_count'] == 0
            assert outcome['last_activated_at'] is not None
            with patch('time.time', side_effect=[2000.0, 900.0]):
                orchestrator._note_native_buy_outcome(action, RuntimeError(
                    'Executor admission changed after intent commit: controller_permission_changed'))
                orchestrator._note_native_buy_outcome(action)
            assert outcome['last_failure_at'] == 2000.0
            assert outcome['last_activated_at'] == 900.0
            assert outcome['last_failure_sequence'] < outcome['last_activated_sequence']
        finally:
            await scenario.close()
            MarketsRecorder._shared_instance = previous


def test_native_buy_health_retains_system_refusal_until_progress_and_detects_stale_auth():
    strategy = V2WithControllers.__new__(V2WithControllers)
    controller = SimpleNamespace(config=SimpleNamespace(id='v1', trading_pair='BTC-USDC',
                                                        manual_kill_switch=False),
                                 _entry_decision_gate=SimpleNamespace(
                                     last_state={'authorization': {'executor_id': 'abandoned', 'candle': 100,
                                                                   'dispatched': False}, 'consumed_candle': 0},
                                     _local_authorization=('abandoned', 'fingerprint', 100), _awaiting_commit=None),
                                 _last_evaluated_candle_timestamp=100, executors_info=[], entries_held=lambda: None)
    outcome = {'failure_count': 1, 'last_failure_at': 1000.0, 'last_failure_sequence': 2,
               'last_failure_code': 'controller_permission_changed', 'last_activated_at': 2000.0,
               'last_activated_sequence': 1}
    first = strategy._native_buy_admission(controller, {'BTC-USDC': outcome})
    assert first['status'] == 'blocked' and first['hold_reason'] == 'system'
    assert [issue['code'] for issue in first['issues']] == ['native_buy_create_refused']
    controller._last_evaluated_candle_timestamp = 200
    second = strategy._native_buy_admission(controller, {'BTC-USDC': outcome})
    assert [issue['code'] for issue in second['issues']] == [
        'native_buy_create_refused', 'durable_buy_authorization_unresolved']
    assert first['issues'][0]['since'] == second['issues'][0]['since']
    auth_since = second['issues'][1]['since']
    assert strategy._native_buy_admission(controller, {'BTC-USDC': outcome})['issues'][1]['since'] == auth_since
    controller._funding_hold = 'Native BUY lacks immutable funding admission'
    exact = strategy._native_buy_admission(controller, None)
    assert exact['status'] == 'blocked' and exact['hold_reason'] == 'system'
    assert 'native_funding_reconciliation_required' in [issue['code'] for issue in exact['issues']]
    controller._funding_hold = None
    controller._entry_decision_gate.last_state['authorization'] = None
    outcome['last_activated_at'] = 900.0  # The wall clock moved backward after actual progress.
    outcome['last_activated_sequence'] = 3
    cleared = strategy._native_buy_admission(controller, {'BTC-USDC': outcome})
    assert cleared['status'] == 'healthy' and cleared['issues'] == []
    assert cleared['failure_count'] == 1
    assert cleared['last_failure_at'] is not None and cleared['last_activated_at'] is not None
    assert strategy._native_buy_issue_since == {}

    controller.config.funding = object()
    assert strategy._native_buy_admission(controller, None)['status'] == 'unavailable'
    controller._funding_store = SimpleNamespace(status={'pending_quote': '0'})
    controller.operator_entry_paused = True
    operator = strategy._native_buy_admission(controller, None)
    assert operator['status'] == 'healthy' and operator['hold_reason'] == 'operator'


def test_native_buy_failure_for_one_v3_pair_survives_another_pairs_activation():
    strategy = V2WithControllers.__new__(V2WithControllers)
    controller = SimpleNamespace(config=SimpleNamespace(id='v3', manual_kill_switch=False),
                                 entries_held=lambda: None)
    btc = {'failure_count': 1, 'last_failure_at': 1000.0,
           'last_failure_code': 'controller_permission_changed',
           'last_failure_sequence': 1, 'last_activated_sequence': 0, 'last_activated_at': None}
    eth = {'failure_count': 0, 'last_failure_at': None, 'last_failure_code': None,
           'last_failure_sequence': 0, 'last_activated_sequence': 1, 'last_activated_at': 1001.0}
    outcomes = {'BTC-USDC': btc, 'ETH-USDC': eth}
    blocked = strategy._native_buy_admission(controller, outcomes)
    assert blocked['status'] == 'blocked'
    assert [(issue['code'], issue['pair']) for issue in blocked['issues']] == [
        ('native_buy_create_refused', 'BTC-USDC')]
    eth['last_activated_at'] = 1002.0
    eth['last_activated_sequence'] = 2
    assert strategy._native_buy_admission(controller, outcomes)['issues'][0]['pair'] == 'BTC-USDC'
    btc['last_activated_at'] = 900.0  # Clock regression cannot hide the BTC progress.
    btc['last_activated_sequence'] = 2
    assert strategy._native_buy_admission(controller, outcomes)['status'] == 'healthy'
    assert strategy._native_buy_issue_since == {}


def test_native_runtime_snapshot_projects_exact_pair_failure_without_sql_read():
    from test.meridian_v3.test_unified_runtime import OWNER, unified

    strategy = V2WithControllers.__new__(V2WithControllers)
    config = unified(execution_enabled=True)
    controller = SimpleNamespace(config=config, entries_held=lambda: None)
    strategy.controllers = {OWNER: controller}
    strategy._daily_entry_risk = None
    strategy._entry_risk_enrollment = None
    strategy.executor_orchestrator = SimpleNamespace(_native_buy_outcomes={OWNER: {
        'BTC-USDC': {'failure_count': 1, 'last_failure_at': 1000.0,
                     'last_failure_code': 'controller_permission_changed',
                     'last_failure_sequence': 1, 'last_activated_sequence': 0,
                     'last_activated_at': None}}})
    snapshot = {'controllers': [{'controller_id': OWNER}], 'schema_version': 1}
    recorder = SimpleNamespace(_async_financial_events=True)
    with patch.object(StrategyV2Base, '_build_runtime_status_snapshot', return_value=snapshot), \
            patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        result = strategy._build_runtime_status_snapshot()
    admission = result['controllers'][0]['native_buy_admission']
    assert admission['schema_version'] == 1 and admission['status'] == 'blocked'
    assert admission['issues'][0]['pair'] == 'BTC-USDC'
    assert admission['issues'][0]['last_failure_at'].endswith('+00:00')
