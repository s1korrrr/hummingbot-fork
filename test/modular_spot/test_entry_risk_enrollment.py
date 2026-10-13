"""Explicit first enrollment uses the first fresh native mark, never invented history."""
import asyncio
import copy
import json
import sqlite3
import tempfile
from collections import deque
from decimal import Decimal as D
from pathlib import Path
from test.meridian_v3.test_operator_durable_adapter import OWNER, OperatorController
from test.test_ok_rsi_restart_safety import recorder as _recorder_fixture
from types import MethodType, SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import pytest

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from scripts.v2_with_controllers import V2WithControllers, V2WithControllersConfig

recorder = _recorder_fixture


def pending_state():
    return {'version': 2, 'controllers': [OWNER], 'max_global_pnl': None,
            'max_pnl_by_controller': {OWNER: None}, 'drawdown_exited_controllers': [],
            'stopped': False, 'operator_paused': {OWNER: True},
            'entry_risk_enrollment': {'schema_version': 1, 'command_id': 'approved-fixture',
                                      'limit_quote': '1000', 'requested_at': 1000.0,
                                      'historical_peaks_available': False,
                                      'initialized_at': None, 'initial_pnl_quote': None}}


def make_runner(recorder, **config):
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['BTC-USDC'])

    def initialize(strategy):
        strategy.controllers = {OWNER: OperatorController()}

    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', initialize), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()), \
            patch('scripts.v2_with_controllers.time.time', return_value=1001):
        return V2WithControllers({'okx': connector}, V2WithControllersConfig(
            max_daily_loss_quote='1000', runtime_db_sync_enabled=False, runtime_status_snapshot_enabled=False,
            chart_snapshots_enabled=False, **config))


async def close(strategy):
    strategy.listen_to_executor_actions_task.cancel()
    await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_pending_enrollment_tick_waits_for_report_initialization_without_rewriting_state(recorder):
    recorder.store_strategy_risk_state(pending_state())
    strategy = make_runner(recorder)
    try:
        before = recorder.load_strategy_risk_state()
        strategy._controller_reports_valid = False
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
                patch.object(StrategyV2Base, 'on_tick'), \
                patch.object(strategy, 'check_manual_kill_switch'):
            strategy.on_tick()
        assert strategy._daily_entry_risk is None
        assert strategy._entry_risk_enrollment.pending
        assert strategy.controllers[OWNER].paused
        assert not strategy._is_stop_triggered
        assert recorder.load_strategy_risk_state() == before
    finally:
        await close(strategy)


@pytest.mark.asyncio
async def test_pending_ownership_does_not_initialize_daily_risk(recorder):
    recorder.store_strategy_risk_state(pending_state())
    strategy = make_runner(recorder)
    try:
        before = recorder.load_strategy_risk_state()
        strategy._controller_reports_valid = True
        strategy.controller_reports = {OWNER: {'performance': SimpleNamespace(
            pnl_available=True, global_pnl_quote=D('-12.5'))}}
        strategy.executor_orchestrator._planned_resume_pending = True

        with patch.object(StrategyV2Base, 'on_tick'), \
                patch('scripts.v2_with_controllers.time.time', return_value=1002):
            strategy.on_tick()
            assert not strategy.validate_create_action(SimpleNamespace(
                controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.BUY)))

        assert strategy._daily_entry_risk is None
        assert strategy._entry_risk_enrollment.pending
        assert strategy.max_global_pnl is None
        assert recorder.load_strategy_risk_state() == before
    finally:
        await close(strategy)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['preflight_failed', 'claim_failed', 'unresolved'])
async def test_failed_or_unresolved_ownership_keeps_risk_rails_and_creation(recorder, failure):
    """Restart continuity D2: a failed or unresolved claim is reported and held, never a fence."""
    recorder.store_strategy_risk_state(pending_state())
    strategy = make_runner(recorder)
    try:
        strategy._controller_reports_valid = True
        strategy.controller_reports = {OWNER: {'performance': SimpleNamespace(
            pnl_available=True, global_pnl_quote=D('-12.5'))}}
        orchestrator = strategy.executor_orchestrator
        if failure == 'preflight_failed':
            orchestrator._resume_preflight_failed = True
        elif failure == 'claim_failed':
            orchestrator._planned_resume_failure = 'sealed claim rejected'
        else:
            orchestrator._claimed_resume_unresolved = {OWNER: {'executor': ('okx', 'BTC-USDC')}}

        with patch.object(StrategyV2Base, 'on_tick'), \
                patch('scripts.v2_with_controllers.time.time', return_value=1002):
            strategy.on_tick()
            # The first fresh native mark enrolls the daily rail; SELLs are never refused by it.
            assert strategy.validate_create_action(SimpleNamespace(
                controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.SELL)))

        assert strategy._daily_entry_risk is not None
        assert recorder.load_strategy_risk_state()['daily_entry_risk']['baseline_quote'] == '-12.5'
    finally:
        await close(strategy)


@pytest.mark.asyncio
async def test_successful_ownership_claim_initializes_daily_risk_once(recorder):
    recorder.store_strategy_risk_state(pending_state())
    strategy = make_runner(recorder)
    try:
        strategy._controller_reports_valid = True
        strategy.controller_reports = {OWNER: {'performance': SimpleNamespace(
            pnl_available=True, global_pnl_quote=D('-12.5'))}}
        with patch.object(StrategyV2Base, 'on_tick'), \
                patch('scripts.v2_with_controllers.time.time', side_effect=[1002, 1003]):
            strategy.on_tick()
            first = recorder.load_strategy_risk_state()
            strategy.on_tick()
            second = recorder.load_strategy_risk_state()

        assert first['daily_entry_risk']['baseline_quote'] == '-12.5'
        assert first['entry_risk_enrollment']['initialized_at'] == 1002
        assert second['daily_entry_risk']['baseline_quote'] == first['daily_entry_risk']['baseline_quote']
        assert second['entry_risk_enrollment']['initialized_at'] == first['entry_risk_enrollment']['initialized_at']
        assert second['daily_entry_risk']['last_timestamp'] == 1003
    finally:
        await close(strategy)


@pytest.mark.asyncio
async def test_first_create_validation_waits_for_fresh_report_after_claim(recorder):
    from hummingbot.core.data_type.common import TradeType

    recorder.store_strategy_risk_state(pending_state())
    strategy = make_runner(recorder)
    try:
        strategy.executor_orchestrator._planned_resume_pending = False
        strategy._controller_reports_valid = False
        strategy.controller_reports = {OWNER: {'performance': SimpleNamespace(
            pnl_available=True, global_pnl_quote=D('9999'))}}
        sell = SimpleNamespace(controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.SELL))

        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
                patch('scripts.v2_with_controllers.time.time', return_value=1002):
            assert not strategy.validate_create_action(sell)
            assert strategy._daily_entry_risk is None
            assert strategy._entry_risk_enrollment.pending
            assert recorder.load_strategy_risk_state().get('daily_entry_risk') is None

            strategy.controller_reports = {OWNER: {'performance': SimpleNamespace(
                pnl_available=True, global_pnl_quote=D('-7.25'))}}
            strategy._controller_reports_valid = True
            assert strategy.validate_create_action(sell)

        state = recorder.load_strategy_risk_state()
        assert state['daily_entry_risk']['baseline_quote'] == '-7.25'
        assert state['entry_risk_enrollment']['initialized_at'] == 1002
    finally:
        await close(strategy)


@pytest.mark.asyncio
async def test_operator_pause_remains_durable_but_resume_waits_for_claim(recorder):
    from hummingbot.client.hummingbot_application import HummingbotApplication

    state = {'version': 1, 'controllers': [OWNER], 'max_global_pnl': '0',
             'max_pnl_by_controller': {OWNER: '0'}, 'drawdown_exited_controllers': [], 'stopped': False,
             'operator_paused': {OWNER: True},
             'daily_entry_risk': {'schema_version': 1, 'limit_quote': '1000', 'utc_day': 0,
                                  'baseline_quote': '0', 'last_pnl_quote': '0', 'last_timestamp': 1000,
                                  'paused': False, 'breach_day': None}}
    recorder.store_strategy_risk_state(state)
    strategy = make_runner(recorder)
    try:
        before_daily = recorder.load_strategy_risk_state()['daily_entry_risk']
        strategy.executor_orchestrator._planned_resume_pending = True
        strategy._operator_started_at = 999
        pause = {'action': 'bot_pause_entries', 'bot_name': 'candidate', 'controller_id': OWNER,
                 'requested_at': 1002.0, 'command_id': 'pause-during-recovery'}
        resume = {**pause, 'action': 'bot_resume_entries', 'requested_at': 1003.0,
                  'command_id': 'resume-during-recovery'}
        with patch.object(StrategyV2Base, 'on_tick'), \
                patch('scripts.v2_with_controllers.time.time', return_value=1004), \
                patch.object(HummingbotApplication, 'main_application',
                             return_value=SimpleNamespace(instance_id='candidate')):
            strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=pause)))
            strategy.on_tick()
            strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=resume)))
            strategy.on_tick()

        assert strategy.controllers[OWNER].paused
        assert recorder.load_strategy_risk_state()['daily_entry_risk'] == before_daily
        assert recorder.has_operator_command(OWNER, 'pause-during-recovery')
        assert not recorder.has_operator_command(OWNER, 'resume-during-recovery')
    finally:
        await close(strategy)


@pytest.mark.asyncio
async def test_approved_pending_enrollment_stays_fenced_and_samples_actual_native_pnl(recorder):
    recorder.store_strategy_risk_state(pending_state())
    strategy = make_runner(recorder)
    try:
        assert strategy._risk_reconciliation_error is None
        assert strategy.controllers[OWNER].paused
        assert strategy._daily_entry_risk is None
        assert recorder.load_strategy_risk_state()['max_global_pnl'] is None
        strategy._controller_reports_valid = True
        strategy.controller_reports = {OWNER: {'performance': SimpleNamespace(
            pnl_available=True, global_pnl_quote=D('-7.125'))}}
        buy = SimpleNamespace(controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.BUY))
        sell = SimpleNamespace(controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.SELL))
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
                patch('scripts.v2_with_controllers.time.time', return_value=1002), \
                patch.object(strategy, 'check_manual_kill_switch'):
            assert not strategy.validate_create_action(buy)
            assert strategy.validate_create_action(sell)
        saved = recorder.load_strategy_risk_state()
        assert saved['daily_entry_risk']['baseline_quote'] == '-7.125'
        assert saved['daily_entry_risk']['last_pnl_quote'] == '-7.125'
        assert saved['entry_risk_enrollment']['initialized_at'] == 1002
        assert saved['entry_risk_enrollment']['historical_peaks_available'] is False
        assert saved['max_global_pnl'] == '-7.125'
        assert saved['operator_paused'] == {OWNER: True}
    finally:
        await close(strategy)

    restarted = make_runner(recorder)
    try:
        assert restarted._risk_reconciliation_error is None
        assert restarted._daily_entry_risk.baseline_quote == D('-7.125')
        assert restarted.controllers[OWNER].paused
    finally:
        await close(restarted)


@pytest.mark.asyncio
@pytest.mark.parametrize('mutation', ['unfenced', 'invented_peak', 'daily_already_present', 'wrong_limit',
                                      'missing_receipt', 'old_version', 'historical_drawdown'])
async def test_incomplete_or_conflicting_enrollment_stays_blocked_without_rewriting_evidence(recorder, mutation):
    state = pending_state()
    config = {}
    if mutation == 'unfenced':
        state['operator_paused'][OWNER] = False
    elif mutation == 'invented_peak':
        state['max_global_pnl'] = '0'
    elif mutation == 'daily_already_present':
        state['daily_entry_risk'] = {}
    elif mutation == 'wrong_limit':
        state['entry_risk_enrollment']['limit_quote'] = '50'
    elif mutation == 'missing_receipt':
        del state['entry_risk_enrollment']
    elif mutation == 'old_version':
        state['version'] = 1
    else:
        config['max_global_drawdown_quote'] = 1000
    recorder.store_strategy_risk_state(state)
    strategy = make_runner(recorder, **config)
    try:
        assert strategy._risk_reconciliation_error
        assert strategy._is_stop_triggered
        assert not strategy.validate_create_action(SimpleNamespace(controller_id=OWNER))
        strategy._persist_risk_state()
        assert recorder.load_strategy_risk_state() == state
    finally:
        await close(strategy)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['unavailable_pnl', 'nonfinite_pnl', 'removed_fence', 'write_failure'])
async def test_failed_initial_sample_cannot_admit_orders_or_publish_a_baseline(recorder, failure):
    recorder.store_strategy_risk_state(pending_state())
    strategy = make_runner(recorder)
    try:
        before = recorder.load_strategy_risk_state()
        strategy._controller_reports_valid = True
        strategy.controller_reports = {OWNER: {'performance': SimpleNamespace(
            pnl_available=failure != 'unavailable_pnl',
            global_pnl_quote=D('NaN') if failure == 'nonfinite_pnl' else D('4'))}}
        if failure == 'removed_fence':
            strategy.controllers[OWNER].paused = False
        original = recorder.store_strategy_risk_state
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
                patch('scripts.v2_with_controllers.time.time', return_value=1002), \
                patch.object(strategy, 'check_manual_kill_switch'), \
                patch.object(recorder, 'store_strategy_risk_state',
                             side_effect=OSError('injected write failure') if failure == 'write_failure' else original):
            buy = SimpleNamespace(controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.BUY))
            assert not strategy.validate_create_action(buy)
            assert not strategy.validate_create_action(buy)  # refused for as long as the failure holds
        # Refusal is per action: a failed sample is not a durable strategy stop.
        assert not strategy._is_stop_triggered
        assert recorder.load_strategy_risk_state() == before
    finally:
        await close(strategy)


def request(**updates):
    return {'command_id': 'approved-fixture', 'requested_at': 1000, 'limit_quote': '1000',
            'controllers': [OWNER], 'owner_stopped': True, 'entries_fenced': True,
            'allow_new_risk_scope': False, **updates}


def database(state):
    connection = sqlite3.connect(':memory:')
    connection.execute('CREATE TABLE Metadata (key TEXT PRIMARY KEY, value TEXT)')
    connection.execute('CREATE TABLE TradeFill (id TEXT PRIMARY KEY, amount TEXT)')
    connection.execute('CREATE TABLE OrderExact (id TEXT PRIMARY KEY, amount TEXT)')
    connection.execute('INSERT INTO OrderExact VALUES (?,?)', ('existing-order', '1.23456789'))
    connection.execute('INSERT INTO TradeFill VALUES (?,?)', ('existing-fill', '1.23456789'))
    if state is not None:
        connection.execute('INSERT INTO Metadata VALUES (?,?)', ('strategy-risk:owner.yml', json.dumps(state)))
    connection.execute('INSERT INTO Metadata VALUES (?,?)', ('strategy-operator:preserved', '{"old":"receipt"}'))
    connection.commit()
    return connection


def legacy_state():
    from test.meridian_v3.test_operator_durable_adapter import approved_test_risk_state
    state = approved_test_risk_state()
    state['max_global_pnl'] = '123.4567890123456789012345678'
    state['max_pnl_by_controller'][OWNER] = '98.7654321098765432109876543'
    state['operator_watermarks'] = {OWNER: {'requested_at': 900.0, 'command_id': 'preserved-pause'}}
    return state


def test_existing_peaks_latches_and_operator_receipts_survive_prospective_daily_enrollment():
    from hummingbot.strategy_v2.migrations.entry_risk import apply_entry_risk_transition, plan_entry_risk_transition
    state = legacy_state()
    state.update(stopped=True, drawdown_exited_controllers=[OWNER])
    connection = database(state)
    try:
        planned = plan_entry_risk_transition(connection, 'owner.yml', request())
        for key in ('max_global_pnl', 'max_pnl_by_controller', 'stopped', 'drawdown_exited_controllers', 'operator_watermarks'):
            assert planned['after'][key] == state[key]
        assert planned['after']['operator_paused'] == {OWNER: True}
        assert planned['after']['entry_risk_enrollment']['historical_peaks_available'] is True
        assert 'daily_entry_risk' not in planned['after']
        before_fills = connection.execute('SELECT * FROM TradeFill').fetchall()
        apply_entry_risk_transition(connection, planned, now=1001)
        assert connection.execute('SELECT * FROM TradeFill').fetchall() == before_fills
        assert connection.execute("SELECT value FROM Metadata WHERE key='strategy-operator:preserved'").fetchone()[0] == '{"old":"receipt"}'
        archive = json.loads(connection.execute('SELECT value FROM Metadata WHERE key=?', (planned['archive_key'],)).fetchone()[0])
        assert archive['before'] == state and archive['after'] == planned['after']
        with pytest.raises(ValueError, match='already applied|Pending enrollment'):
            apply_entry_risk_transition(connection, planned, now=1002)
    finally:
        connection.close()


def test_limit_only_transition_retains_existing_daily_baseline_breach_and_timestamp():
    from hummingbot.strategy_v2.migrations.entry_risk import apply_entry_risk_transition, plan_entry_risk_transition
    from hummingbot.strategy_v2.models.daily_entry_risk import DailyEntryRisk
    state = legacy_state()
    daily = DailyEntryRisk.new(D('50'), 900)
    daily.observe(950, D('-60'))
    state['daily_entry_risk'] = daily.snapshot()
    connection = database(state)
    try:
        planned = plan_entry_risk_transition(connection, 'owner.yml', request())
        expected = copy.deepcopy(state)
        expected['daily_entry_risk']['limit_quote'] = '1000'
        assert planned['after'] == expected
        apply_entry_risk_transition(connection, planned, now=1001)
        saved = json.loads(connection.execute("SELECT value FROM Metadata WHERE key='strategy-risk:owner.yml'").fetchone()[0])
        restored = DailyEntryRisk.restore(saved['daily_entry_risk'], D('1000'))
        assert restored.paused and restored.breach_day == 0
        assert restored.baseline_quote == 0 and restored.last_pnl_quote == D('-60')
        assert restored.last_timestamp == 950
    finally:
        connection.close()


@pytest.mark.parametrize('autocommit', [sqlite3.LEGACY_TRANSACTION_CONTROL, True])
@pytest.mark.parametrize('fail_archive', [False, True])
def test_transition_owns_commit_and_rollback_in_each_sqlite_connection_mode(autocommit, fail_archive):
    from hummingbot.strategy_v2.migrations.entry_risk import apply_entry_risk_transition, plan_entry_risk_transition
    connection = database(legacy_state())
    connection.autocommit = autocommit
    try:
        if fail_archive:
            connection.execute("""CREATE TRIGGER fail_archive BEFORE INSERT ON Metadata
                WHEN NEW.key LIKE 'strategy-risk-transition:%'
                BEGIN SELECT RAISE(ABORT, 'injected archive failure'); END""")
        planned = plan_entry_risk_transition(connection, 'owner.yml', request())
        before = list(connection.iterdump())
        if fail_archive:
            with pytest.raises(sqlite3.IntegrityError, match='injected archive failure'):
                apply_entry_risk_transition(connection, planned, now=1001)
            assert list(connection.iterdump()) == before
        else:
            apply_entry_risk_transition(connection, planned, now=1001)
            assert connection.execute('SELECT 1 FROM Metadata WHERE key=?', (planned['archive_key'],)).fetchone()
        assert not connection.in_transaction
    finally:
        connection.close()


@pytest.mark.parametrize('mutation', ['changed_fill', 'changed_exact_order', 'changed_state', 'changed_proposal', 'expired', 'future'])
def test_transition_refuses_drift_and_rolls_back_every_write(mutation):
    from hummingbot.strategy_v2.migrations.entry_risk import apply_entry_risk_transition, plan_entry_risk_transition
    connection = database(legacy_state())
    try:
        planned = plan_entry_risk_transition(connection, 'owner.yml', request())
        now = 1001
        if mutation == 'changed_fill':
            connection.execute("UPDATE TradeFill SET amount='2'")
        elif mutation == 'changed_exact_order':
            connection.execute("UPDATE OrderExact SET amount='2'")
        elif mutation == 'changed_state':
            connection.execute("UPDATE Metadata SET value='{}' WHERE key='strategy-operator:preserved'")
        elif mutation == 'changed_proposal':
            planned['after']['stopped'] = True
        elif mutation == 'expired':
            now = 1300
        else:
            now = 999
        connection.commit()
        before = list(connection.iterdump())
        with pytest.raises(ValueError, match='changed|Fresh approved'):
            apply_entry_risk_transition(connection, planned, now=now)
        assert list(connection.iterdump()) == before
    finally:
        connection.close()


def release_request(**updates):
    return {'command_id': 'approved-latch-release', 'requested_at': 1000, 'controllers': [OWNER],
            'owner_stopped': True, **updates}


def test_interrupted_shutdown_latch_release_changes_only_the_latch_and_archives_it():
    from hummingbot.strategy_v2.migrations.entry_risk import (
        apply_interrupted_shutdown_latch_release,
        plan_interrupted_shutdown_latch_release,
    )
    state = legacy_state()
    state['stopped'] = True
    connection = database(state)
    try:
        planned = plan_interrupted_shutdown_latch_release(connection, 'owner.yml', release_request())
        assert planned['kind'] == 'interrupted_shutdown_latch_release'
        assert planned['after'] == {**state, 'stopped': False}
        before_fills = connection.execute('SELECT * FROM TradeFill').fetchall()
        apply_interrupted_shutdown_latch_release(connection, planned, now=1001)
        saved = json.loads(connection.execute("SELECT value FROM Metadata WHERE key='strategy-risk:owner.yml'").fetchone()[0])
        assert saved == {**state, 'stopped': False}
        assert connection.execute('SELECT * FROM TradeFill').fetchall() == before_fills
        assert connection.execute("SELECT value FROM Metadata WHERE key='strategy-operator:preserved'").fetchone()[0] == '{"old":"receipt"}'
        archive = json.loads(connection.execute('SELECT value FROM Metadata WHERE key=?', (planned['archive_key'],)).fetchone()[0])
        assert archive['before'] == state and archive['after'] == saved
        with pytest.raises(ValueError, match='not latched|already applied'):
            apply_interrupted_shutdown_latch_release(connection, planned, now=1002)
    finally:
        connection.close()


@pytest.mark.parametrize('case, message', [
    ('drawdown_exit', 'risk stop'),
    ('not_stopped', 'not latched'),
    ('missing_state', 'No durable risk state'),
    ('other_controllers', 'scope mismatch'),
    ('owner_running', 'Stopped owner proof'),
])
def test_latch_release_refuses_a_risk_stop_or_unproven_scope_without_writing(case, message):
    from hummingbot.strategy_v2.migrations.entry_risk import plan_interrupted_shutdown_latch_release
    state = legacy_state()
    state['stopped'] = case != 'not_stopped'
    if case == 'drawdown_exit':
        state['drawdown_exited_controllers'] = [OWNER]
    updates = {'controllers': ['other-owner']} if case == 'other_controllers' else {}
    if case == 'owner_running':
        updates = {'owner_stopped': False}
    connection = database(None if case == 'missing_state' else state)
    try:
        before = list(connection.iterdump())
        with pytest.raises(ValueError, match=message):
            plan_interrupted_shutdown_latch_release(connection, 'owner.yml', release_request(**updates))
        assert list(connection.iterdump()) == before
    finally:
        connection.close()


@pytest.mark.parametrize('mutation', ['changed_fill', 'changed_state', 'expired'])
def test_latch_release_refuses_drift_after_review_and_rolls_back(mutation):
    from hummingbot.strategy_v2.migrations.entry_risk import (
        apply_interrupted_shutdown_latch_release,
        plan_interrupted_shutdown_latch_release,
    )
    state = legacy_state()
    state['stopped'] = True
    connection = database(state)
    try:
        planned = plan_interrupted_shutdown_latch_release(connection, 'owner.yml', release_request())
        now = 1001
        if mutation == 'changed_fill':
            connection.execute("UPDATE TradeFill SET amount='2'")
        elif mutation == 'changed_state':
            connection.execute("UPDATE Metadata SET value='{}' WHERE key='strategy-operator:preserved'")
        else:
            now = 1300
        connection.commit()
        before = list(connection.iterdump())
        with pytest.raises(ValueError, match='changed|Fresh approved'):
            apply_interrupted_shutdown_latch_release(connection, planned, now=now)
        assert list(connection.iterdump()) == before
    finally:
        connection.close()


def test_missing_state_requires_explicit_new_scope_and_never_invents_a_peak_or_pnl():
    from hummingbot.strategy_v2.migrations.entry_risk import plan_entry_risk_transition
    connection = database(None)
    try:
        with pytest.raises(ValueError, match='explicit new-scope'):
            plan_entry_risk_transition(connection, 'owner.yml', request())
        planned = plan_entry_risk_transition(connection, 'owner.yml', request(allow_new_risk_scope=True))
        assert planned['after'] == pending_state()
        assert connection.execute("SELECT 1 FROM Metadata WHERE key='strategy-risk:owner.yml'").fetchone() is None
    finally:
        connection.close()


@pytest.mark.parametrize('field,value', [('owner_stopped', False), ('entries_fenced', False),
                                         ('limit_quote', True), ('limit_quote', 'Infinity'), ('limit_quote', '0'),
                                         ('requested_at', float('nan')), ('command_id', ''),
                                         ('controllers', []), ('controllers', [OWNER, OWNER])])
def test_transition_rejects_invalid_approval_input(field, value):
    from hummingbot.strategy_v2.migrations.entry_risk import plan_entry_risk_transition
    connection = database(legacy_state())
    try:
        before = list(connection.iterdump())
        with pytest.raises(ValueError):
            plan_entry_risk_transition(connection, 'owner.yml', request(**{field: value}))
        assert list(connection.iterdump()) == before
    finally:
        connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('mutation', ['earlier_clock', 'reset_baseline', 'later_day'])
async def test_completed_enrollment_cannot_restore_a_reset_daily_baseline(recorder, mutation):
    from hummingbot.strategy_v2.models.daily_entry_risk import DailyEntryRisk
    state = pending_state()
    state.update(max_global_pnl='-7.125', max_pnl_by_controller={OWNER: '-7.125'})
    state['entry_risk_enrollment'].update(initialized_at=1001.0, initial_pnl_quote='-7.125')
    daily = DailyEntryRisk(limit_quote='1000', utc_day=0, baseline_quote='-7.125',
                           last_pnl_quote='-10', last_timestamp=1002)
    if mutation == 'earlier_clock':
        daily.last_timestamp = 999
    elif mutation == 'reset_baseline':
        daily.baseline_quote = D('0')
    else:
        daily.observe(86400, D('-11'))
    state['daily_entry_risk'] = daily.snapshot()
    recorder.store_strategy_risk_state(state)
    strategy = make_runner(recorder)
    try:
        if mutation == 'later_day':
            assert strategy._risk_reconciliation_error is None
            assert strategy._daily_entry_risk.baseline_quote == D('-10')
        else:
            assert strategy._risk_reconciliation_error
            assert strategy._is_stop_triggered
            assert recorder.load_strategy_risk_state() == state
    finally:
        await close(strategy)


@pytest.mark.asyncio
async def test_early_operator_resume_cannot_remove_pending_enrollment_fence(recorder):
    recorder.store_strategy_risk_state(pending_state())
    strategy = make_runner(recorder)
    try:
        strategy._operator_started_at = 1001
        before = recorder.load_strategy_risk_state()
        payload = {'action': 'bot_resume_entries', 'bot_name': 'fixture', 'controller_id': OWNER,
                   'requested_at': 1002, 'command_id': 'early-resume'}
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
                patch('scripts.v2_with_controllers.time.time', return_value=1003), \
                patch('scripts.v2_with_controllers.HummingbotApplication.main_application',
                      return_value=SimpleNamespace(instance_id='fixture')):
            strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=payload)))
            strategy.process_operator_events()
        assert strategy.controllers[OWNER].paused
        assert recorder.load_strategy_risk_state() == before
        assert not recorder.has_operator_command(OWNER, 'early-resume')
        assert strategy._risk_reconciliation_error is None
    finally:
        await close(strategy)


@pytest.mark.parametrize('field,value', [
    ('schema_version', True), ('limit_quote', True),
    ('limit_quote', 'Infinity'), ('initial_pnl_quote', 'NaN'),
    ('requested_at', -1), ('historical_peaks_available', 'yes')])
def test_enrollment_receipt_rejects_invalid_types_and_nonfinite_values(field, value):
    from hummingbot.strategy_v2.models.entry_risk_enrollment import EntryRiskEnrollment
    receipt = pending_state()['entry_risk_enrollment']
    with pytest.raises(ValueError):
        EntryRiskEnrollment.model_validate({**receipt, field: value})


def test_enrollment_receipt_requires_explicit_schema_version():
    from hummingbot.strategy_v2.models.entry_risk_enrollment import EntryRiskEnrollment
    receipt = pending_state()['entry_risk_enrollment']
    del receipt['schema_version']
    with pytest.raises(ValueError, match='schema_version'):
        EntryRiskEnrollment.model_validate(receipt)


class NativePaperEnrollmentTests(IsolatedAsyncioTestCase):
    from test.hummingbot.strategy_v2.paper.test_paper_session import NativePaperSessionTests
    setUp = NativePaperSessionTests.setUp  # denies socket connections independently of container isolation

    async def test_actual_native_fill_report_sets_baseline_and_keeps_bnb_executor_managed(self):
        from test.meridian_v3.test_unified_runtime import (
            OWNER as PAPER_OWNER,
            PAIRS,
            MeridianUnifiedManagementTests,
            books,
            paper,
            unified,
        )

        from hummingbot.strategy_v2.models.entry_risk_enrollment import EntryRiskEnrollment
        from hummingbot.strategy_v2.paper_session import NativePaperSession

        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(PAIRS, '500'),
                                         [unified(execution_enabled=True, disabled=tuple(p for p in PAIRS if p != 'BNB-USDC'))],
                                         Path(directory) / 'native.sqlite')
            try:
                executor, amount = await MeridianUnifiedManagementTests.filled(self, session, 'BNB-USDC')
                controller = session.controllers[PAPER_OWNER]
                controller.set_operator_entry_pause(True, reason='risk-enrollment', scope='bot')
                receipt = await session.step(600062, books=books(3, 600062))
                self.assertEqual(receipt['actions'], [])
                report = controller.performance_report
                self.assertTrue(report.pnl_available)
                # Independent conservative spot equity: native long positions use the
                # executable bid from the fixture book, not its mid-price.
                expected = (session._connector.get_balance('USDC')
                            + session._connector.get_balance('BNB') * D('99.5') - D('500'))
                self.assertEqual(report.global_pnl_quote, expected)
                runner = SimpleNamespace(
                    controllers=session.controllers, config=SimpleNamespace(max_daily_loss_quote=D('1000')),
                    _entry_risk_enrollment=EntryRiskEnrollment(schema_version=1, command_id='offline-native-fixture', limit_quote='1000',
                                                               requested_at=600061, historical_peaks_available=False),
                    _daily_entry_risk=None, _risk_reconciliation_error=None, _risk_persistence_enabled=True,
                    _controller_reports_valid=True, _last_risk_state=None, _operator_watermarks={},
                    max_global_pnl=None, max_pnl_by_controller={PAPER_OWNER: None},
                    drawdown_exited_controllers=[], _is_stop_triggered=False,
                    get_performance_report=lambda cid: session.controllers[cid].performance_report,
                    _operator_paused=V2WithControllers._operator_paused)
                for name in ('_drawdown_pnl', '_persist_risk_state', '_apply_daily_entry_pause', 'durable_stop_latch'):
                    setattr(runner, name, MethodType(getattr(V2WithControllers, name), runner))
                with patch('scripts.v2_with_controllers.time.time', return_value=600062):
                    V2WithControllers._observe_daily_entry_risk(runner)
                saved = MarketsRecorder.get_instance().load_strategy_risk_state()
                self.assertEqual(D(saved['daily_entry_risk']['baseline_quote']), expected)
                self.assertEqual(saved['operator_paused'], {PAPER_OWNER: True})
                self.assertTrue(executor.is_active)
                self.assertEqual(session._connector.get_balance('BNB'), amount)
                self.assertEqual(len(session.executor_orchestrator.active_executors[PAPER_OWNER]), 1)
                again = await session.step(600063, books=books(4, 600063))
                self.assertEqual(again['actions'], [])
                self.assertTrue(executor.is_active)
            finally:
                await session.close()


def enrolled_state():
    return {'version': 1, 'controllers': [OWNER], 'max_global_pnl': '0',
            'max_pnl_by_controller': {OWNER: '0'}, 'drawdown_exited_controllers': [], 'stopped': False,
            'operator_paused': {OWNER: True},
            'daily_entry_risk': {'schema_version': 1, 'limit_quote': '1000', 'utc_day': 0,
                                 'baseline_quote': '0', 'last_pnl_quote': '0', 'last_timestamp': 1000,
                                 'paused': False, 'breach_day': None}}


@pytest.mark.asyncio
@pytest.mark.parametrize('claim', ['pending', 'preflight_failed', 'claim_failed', 'unresolved'])
async def test_operator_resume_waits_only_for_a_pending_claim(recorder, claim):
    recorder.store_strategy_risk_state(enrolled_state())
    strategy = make_runner(recorder)
    try:
        orchestrator = strategy.executor_orchestrator
        orchestrator._planned_resume_pending = claim == 'pending'
        orchestrator._resume_preflight_failed = claim == 'preflight_failed'
        orchestrator._planned_resume_failure = 'sealed claim rejected' if claim == 'claim_failed' else None
        orchestrator._claimed_resume_unresolved = (
            {OWNER: {'executor': ('okx', 'BTC-USDC')}} if claim == 'unresolved' else {})
        strategy._operator_started_at = 999
        strategy._controller_reports_valid = True
        strategy.controller_reports = {OWNER: {'performance': SimpleNamespace(
            pnl_available=True, global_pnl_quote=D('0'))}}
        resume = {'action': 'bot_resume_entries', 'bot_name': 'candidate', 'controller_id': OWNER,
                  'requested_at': 1003.0, 'command_id': 'resume-1'}
        with patch.object(StrategyV2Base, 'on_tick'), \
                patch('scripts.v2_with_controllers.time.time', return_value=1004), \
                patch.object(HummingbotApplication, 'main_application',
                             return_value=SimpleNamespace(instance_id='candidate')):
            strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=resume)))
            strategy.on_tick()
        assert strategy.controllers[OWNER].paused is (claim == 'pending')
        assert not strategy._is_stop_triggered
    finally:
        await close(strategy)
