"""Actual native runner and orchestrator gates for the opt-in OK RSI composition."""
import asyncio
import subprocess
import sys
from collections import deque
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor
from test.hummingbot.strategy_v2.executors.test_wallet_sale_accounting import wallet_sale
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from test_native_lifecycle import recorder as _recorder_fixture

from controllers.generic.modular_ok_rsi import ModularOKRSIControllerConfig
from controllers.generic.modular_rsi_v5 import ModularRSIV5ControllerConfig
from controllers.generic.modular_spot import ModularSpotControllerConfig
from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import PositionAction, TradeType
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.models.base import RunnableStatus
from scripts.v2_with_controllers import V2WithControllers, V2WithControllersConfig

recorder = _recorder_fixture
OWNER = 'modular_spot_runner'


def test_async_operator_unknown_controller_is_consumed_without_busy_loop():
    code = r'''
import asyncio
from collections import deque
from types import SimpleNamespace
from unittest.mock import patch
from hummingbot.connector.markets_recorder import MarketsRecorder
from scripts.v2_with_controllers import V2WithControllers

async def main():
    strategy = V2WithControllers.__new__(V2WithControllers)
    strategy._operator_drain_lock = asyncio.Lock()
    strategy._operator_event_queue = deque([("rsibot.operator", SimpleNamespace(data={
        "action": "bot_pause_entries", "controller_id": "missing-owner",
        "command_id": "unknown-owner", "requested_at": 100.0,
    }))])
    strategy.controllers = {}
    strategy._risk_persistence_enabled = True
    strategy._risk_write_task = None
    with patch.object(MarketsRecorder, "get_existing_instance", return_value=object()):
        await strategy.process_operator_events_async()
    assert not strategy._operator_event_queue

asyncio.run(main())
'''
    result = subprocess.run(
        [sys.executable, '-B', '-c', code], cwd=Path(__file__).resolve().parents[2],
        capture_output=True, text=True, timeout=3,
    )
    assert result.returncode == 0, f"operator child failed: {result.stderr[-1000:]}"


def risk_state():
    return {'version': 1, 'controllers': [OWNER], 'max_global_pnl': '100.125',
            'max_pnl_by_controller': {OWNER: '75.25'}, 'drawdown_exited_controllers': [],
            'stopped': False, 'operator_paused': {OWNER: True},
            'operator_watermarks': {OWNER: {'requested_at': 100.0, 'command_id': 'old-command'}}}


def initialize_modular(strategy, model=ModularOKRSIControllerConfig):
    config = model(id=OWNER, connector_name='okx', trading_pair='ETH-USDC')
    strategy.controllers = {OWNER: config.get_controller_class()(
        config, strategy.market_data_provider, strategy.actions_queue)}


@pytest.mark.asyncio
@pytest.mark.parametrize('model', [ModularOKRSIControllerConfig, ModularRSIV5ControllerConfig, ModularSpotControllerConfig])
@pytest.mark.parametrize('state_kind', ['valid', 'missing_with_history', 'invalid', 'non_finite', 'wrong_scope'])
async def test_real_modular_runner_restores_or_holds_durable_risk_state(recorder, state_kind, model):
    state = risk_state()
    if state_kind == 'missing_with_history':
        state = None
        executor = make_executor()
        executor.config.controller_id = OWNER
        executor._status = RunnableStatus.TERMINATED
        recorder.store_or_update_executor(executor)
    elif state_kind == 'invalid':
        state = {'version': 999}
    elif state_kind == 'non_finite':
        state['max_global_pnl'] = 'NaN'
    elif state_kind == 'wrong_scope':
        state['controllers'] = ['other']
    if state is not None:
        recorder.store_strategy_risk_state(state)
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDC'])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', lambda strategy: initialize_modular(strategy, model)), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig(
            runtime_db_sync_enabled=False, runtime_status_snapshot_enabled=False, chart_snapshots_enabled=False,
            max_global_drawdown_quote=100, max_controller_drawdown_quote=20))
        try:
            assert strategy._risk_persistence_enabled
            if state_kind == 'valid':
                assert not strategy._is_stop_triggered
                assert strategy.max_global_pnl == D('100.125')
                assert strategy.max_pnl_by_controller == {OWNER: D('75.25')}
                assert strategy.controllers[OWNER]._operator_entry_paused
                assert strategy._operator_watermarks == state['operator_watermarks']
                strategy.apply_initial_setting()
                assert strategy.max_pnl_by_controller[OWNER] == D('75.25')
                assert strategy.controllers[OWNER].config.execution_enabled is False
                # Genuine event queue + controller pause + SQLite receipt, all three families.
                strategy._operator_started_at = 999
                fresh = {'action': 'bot_resume_entries', 'bot_name': 'candidate', 'controller_id': OWNER,
                         'requested_at': 1000.25, 'command_id': 'resume-once'}
                with patch('scripts.v2_with_controllers.time.time', return_value=1000.5), \
                        patch('scripts.v2_with_controllers.HummingbotApplication.main_application',
                              return_value=SimpleNamespace(instance_id='candidate')):
                    strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=fresh)))
                    strategy.process_operator_events()
                    assert not strategy.controllers[OWNER]._operator_entry_paused
                    assert recorder.load_strategy_risk_state()['operator_watermarks'][OWNER]['command_id'] == 'resume-once'
                    # A duplicate must not reapply even when the controller is paused again.
                    strategy.controllers[OWNER].set_operator_entry_pause(True)
                    strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=fresh)))
                    strategy.process_operator_events()
                    assert strategy.controllers[OWNER]._operator_entry_paused
                    pause = {**fresh, 'action': 'bot_pause_entries', 'requested_at': 1000.3, 'command_id': 'pause-newer'}
                    strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=pause)))
                    strategy.process_operator_events()
                    # API retry refreshes A's timestamp; durable IDs must still reject A after B.
                    retry = {**fresh, 'requested_at': 1000.4}
                    strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=retry)))
                    strategy.process_operator_events()
                    assert strategy.controllers[OWNER]._operator_entry_paused
                    assert recorder.has_operator_command(OWNER, 'resume-once')
                    assert recorder.has_operator_command(OWNER, 'pause-newer')
            else:
                assert strategy._is_stop_triggered and strategy._risk_reconciliation_error
                assert not strategy.validate_create_action(SimpleNamespace(controller_id=OWNER))
                strategy._persist_risk_state()
                assert recorder.load_strategy_risk_state() == state
            assert not connector.in_flight_orders
        finally:
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_native_operator_async_path_commits_receipt_and_pause_through_writer(recorder):
    state = risk_state()
    recorder.store_strategy_risk_state(state)
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDC'])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', lambda strategy: initialize_modular(strategy)), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig(
            runtime_db_sync_enabled=False, runtime_status_snapshot_enabled=False, chart_snapshots_enabled=False,
            max_global_drawdown_quote=100, max_controller_drawdown_quote=20))
        try:
            strategy._operator_started_at = 1000
            command = {'action': 'bot_pause_entries', 'bot_name': 'candidate', 'controller_id': OWNER,
                       'requested_at': 1000.25, 'command_id': 'pause-through-writer'}
            with patch('scripts.v2_with_controllers.time.time', return_value=1000.5), \
                    patch('scripts.v2_with_controllers.HummingbotApplication.main_application',
                          return_value=SimpleNamespace(instance_id='candidate')), \
                    patch.object(recorder, 'store_strategy_risk_state', side_effect=AssertionError('sync SQL path')):
                strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=command)))
                await strategy.process_operator_events_async()
            assert strategy.controllers[OWNER]._operator_entry_paused
            saved = recorder.load_strategy_risk_state()
            assert saved['operator_paused'][OWNER] is True
            assert recorder.has_operator_command(OWNER, 'pause-through-writer')
        finally:
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel_phase', ['freshness', 'risk_commit'])
async def test_native_operator_event_survives_precommit_cancellation(recorder, cancel_phase):
    from hummingbot.client.hummingbot_application import HummingbotApplication

    recorder.store_strategy_risk_state(risk_state())
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDC'])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', lambda strategy: initialize_modular(strategy)), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig(
            runtime_db_sync_enabled=False, runtime_status_snapshot_enabled=False, chart_snapshots_enabled=False,
            max_global_drawdown_quote=100, max_controller_drawdown_quote=20))
        started = asyncio.Event()
        command = {'action': 'bot_pause_entries', 'bot_name': 'candidate', 'controller_id': OWNER,
                   'requested_at': 1000.25, 'command_id': f'cancel-{cancel_phase}'}
        event = SimpleNamespace(data=command)
        strategy._operator_started_at = 1000
        strategy._operator_event_queue.append(('rsibot.operator', event))
        has_command = recorder.has_operator_command_async
        write = recorder._sql_manager.write_queue.write
        try:
            with patch('scripts.v2_with_controllers.time.time', return_value=1000.5), \
                    patch.object(HummingbotApplication, 'main_application',
                                 return_value=SimpleNamespace(instance_id='candidate')):
                if cancel_phase == 'freshness':
                    async def blocked_freshness(*args, **kwargs):
                        started.set()
                        await asyncio.Event().wait()
                    recorder.has_operator_command_async = blocked_freshness
                else:
                    async def blocked_write(operation):
                        started.set()
                        await asyncio.Event().wait()
                    recorder._sql_manager.write_queue.write = blocked_write
                task = asyncio.create_task(strategy.process_operator_events_async())
                await asyncio.wait_for(started.wait(), timeout=2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert strategy._operator_event_queue[0][1] is event
                assert strategy._operator_watermarks[OWNER] == {
                    'requested_at': 100.0, 'command_id': 'old-command'}
                assert not recorder.has_operator_command(OWNER, command['command_id'])
                recorder.has_operator_command_async = has_command
                recorder._sql_manager.write_queue.write = write
                await strategy.process_operator_events_async()
            assert not strategy._operator_event_queue
            assert recorder.has_operator_command(OWNER, command['command_id'])
        finally:
            recorder.has_operator_command_async = has_command
            recorder._sql_manager.write_queue.write = write
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_native_operator_commit_cancellation_consumes_known_durable_event(recorder):
    from hummingbot.model.sql_connection_manager import SQLWriteCancelledAfterCommit

    recorder.store_strategy_risk_state(risk_state())
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDC'])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', lambda strategy: initialize_modular(strategy)), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig(
            runtime_db_sync_enabled=False, runtime_status_snapshot_enabled=False, chart_snapshots_enabled=False,
            max_global_drawdown_quote=100, max_controller_drawdown_quote=20))
        command = {'action': 'bot_pause_entries', 'bot_name': 'candidate', 'controller_id': OWNER,
                   'requested_at': 1000.25, 'command_id': 'cancel-after-commit'}
        strategy._operator_started_at = 1000
        strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=command)))
        persist = recorder.store_strategy_risk_state_async

        async def commit_then_cancel(*args, **kwargs):
            result = await persist(*args, **kwargs)
            raise SQLWriteCancelledAfterCommit(result)

        try:
            with patch('scripts.v2_with_controllers.time.time', return_value=1000.5), \
                    patch('scripts.v2_with_controllers.HummingbotApplication.main_application',
                          return_value=SimpleNamespace(instance_id='candidate')):
                recorder.store_strategy_risk_state_async = commit_then_cancel
                with pytest.raises(SQLWriteCancelledAfterCommit):
                    await strategy.process_operator_events_async()
            assert not strategy._operator_event_queue
            assert strategy._operator_watermarks[OWNER]['command_id'] == command['command_id']
            assert recorder.has_operator_command(OWNER, command['command_id'])
        finally:
            recorder.store_strategy_risk_state_async = persist
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_native_risk_stop_async_acknowledges_before_return(recorder):
    recorder.store_strategy_risk_state(risk_state())
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDC'])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', initialize_modular), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig(
            runtime_db_sync_enabled=False, runtime_status_snapshot_enabled=False, chart_snapshots_enabled=False,
            max_global_drawdown_quote=100, max_controller_drawdown_quote=20))
        try:
            with patch.object(recorder, 'store_strategy_risk_state', side_effect=AssertionError('sync SQL path')):
                await strategy.latch_risk_stop_async()
            assert strategy._is_stop_triggered
            assert recorder.load_strategy_risk_state()['stopped'] is True
        finally:
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


def wallet_policy(**updates):
    return ModularOKRSIControllerConfig(id=OWNER, trading_pair='SUI-USDC', trade_mode='sell_only',
                                        sell_only_strict_fraction=True, sell_only_if_profitable=False,
                                        **updates)


def native_sale():
    orchestrator, executor, _, _ = wallet_sale()
    orchestrator.strategy.controllers['controller'].config = wallet_policy()
    executor.config.trading_pair = 'SUI-USDC'
    return orchestrator, executor.executor_info


def test_actual_orchestrator_recognizes_genuine_modular_wallet_sale():
    orchestrator, info = native_sale()
    assert orchestrator._is_unknown_wallet_sale(info)


@pytest.mark.parametrize('field,value', [
    ('trade_mode', 'buy_sell'), ('sell_only_strict_fraction', False), ('sell_only_if_profitable', True),
])
def test_modular_wallet_sale_keeps_every_native_policy_constraint(field, value):
    orchestrator, info = native_sale()
    policy = orchestrator.strategy.controllers['controller'].config
    setattr(policy, field, value)
    assert not orchestrator._is_unknown_wallet_sale(info)


@pytest.mark.parametrize('field,value', [
    ('connector_name', 'binance'), ('side', TradeType.BUY), ('position_action', PositionAction.OPEN),
    ('level_id', 'recovery_exit'), ('min_sell_price', D('1')), ('leverage', 2),
])
def test_modular_wallet_sale_keeps_every_native_executor_constraint(field, value):
    orchestrator, info = native_sale()
    setattr(info.config, field, value)
    assert not orchestrator._is_unknown_wallet_sale(info)


def test_modular_wallet_sale_keeps_order_executor_type_constraint():
    orchestrator, info = native_sale()
    info.type = 'position_executor'
    assert not orchestrator._is_unknown_wallet_sale(info)


@pytest.mark.parametrize('policy', [
    SimpleNamespace(controller_name='modular_ok_rsi', _native_family='ok_rsi', trade_mode='sell_only',
                    sell_only_strict_fraction=True, sell_only_if_profitable=False),
    ModularRSIV5ControllerConfig(id=OWNER),
])
def test_modular_name_or_family_claim_does_not_grant_native_wallet_exception(policy):
    orchestrator, info = native_sale()
    orchestrator.strategy.controllers['controller'].config = policy
    assert not orchestrator._is_unknown_wallet_sale(info)


@pytest.mark.parametrize('policy,allowed', [
    (wallet_policy(), True),
    (SimpleNamespace(controller_name='modular_ok_rsi', trade_mode='sell_only',
                     sell_only_strict_fraction=True, sell_only_if_profitable=False), False),
    (ModularRSIV5ControllerConfig(id=OWNER), False),
])
def test_runner_unknown_cost_exception_requires_trusted_native_policy(policy, allowed):
    report = SimpleNamespace(pnl_available=False, global_pnl_quote=None,
                             pnl_unavailable_reason='unknown_wallet_acquisition_cost')
    strategy = SimpleNamespace(controllers={OWNER: SimpleNamespace(config=policy)},
                               get_performance_report=lambda _: report)
    if allowed:
        assert V2WithControllers._drawdown_pnl(strategy, OWNER) is None
    else:
        with pytest.raises(ValueError, match='PnL unavailable'):
            V2WithControllers._drawdown_pnl(strategy, OWNER)


@pytest.mark.asyncio
@pytest.mark.parametrize('global_limit,controller_limit', [(0, 20), (float('nan'), 20), (float('inf'), 20), (100, 0), (100, float('nan')), (100, 101)])
async def test_direct_native_runner_invalid_loss_rails_hold_before_dispatch(recorder, global_limit, controller_limit):
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDC'])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', initialize_modular), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig(
            max_global_drawdown_quote=global_limit, max_controller_drawdown_quote=controller_limit))
        try:
            assert strategy._is_stop_triggered
            assert strategy._risk_reconciliation_error
            assert not strategy.validate_create_action(SimpleNamespace(controller_id=OWNER))
            assert recorder.load_strategy_risk_state() is None
            assert not connector.in_flight_orders
        finally:
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('global_limit,controller_limit', [(None, 20), (100, None), (None, None)])
async def test_direct_native_runner_optional_loss_rails_remain_unset(recorder, global_limit, controller_limit):
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDC'])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', initialize_modular), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig(
            max_global_drawdown_quote=global_limit, max_controller_drawdown_quote=controller_limit))
        try:
            assert not strategy._is_stop_triggered
            assert strategy._risk_reconciliation_error is None
            assert strategy.config.max_global_drawdown_quote == global_limit
            assert strategy.config.max_controller_drawdown_quote == controller_limit
            assert recorder.load_strategy_risk_state() is not None
            assert not connector.in_flight_orders
        finally:
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


@pytest.mark.parametrize('field', ['max_global_drawdown_quote', 'max_controller_drawdown_quote'])
def test_direct_runner_rejects_boolean_loss_limits_before_coercion(field):
    with pytest.raises(ValueError, match='boolean'):
        V2WithControllersConfig(**{field: True})


@pytest.mark.parametrize('change', [
    {'bot_name': 'another-bot'}, {'requested_at': 99}, {'requested_at': 139},
    {'requested_at': 206}, {'requested_at': float('nan')}, {'requested_at': True},
    {'command_id': ''}, {'command_id': 'replayed'}, {'requested_at': 150},
])
def test_modular_operator_command_rejects_stale_replayed_or_misdirected_resume(change):
    from scripts import v2_with_controllers as runner
    controller = SimpleNamespace(config=ModularRSIV5ControllerConfig(id=OWNER))
    strategy = SimpleNamespace(_operator_started_at=100,
                               _operator_watermarks={OWNER: {'requested_at': 150, 'command_id': 'replayed'}})
    payload = {'bot_name': 'candidate', 'requested_at': 200, 'command_id': 'fresh'}
    payload.update(change)
    with patch.object(runner.time, 'time', return_value=200), \
            patch.object(runner.HummingbotApplication, 'main_application', return_value=SimpleNamespace(instance_id='candidate')):
        assert not V2WithControllers._native_operator_command_is_fresh(strategy, payload, [controller])


def test_modular_operator_command_accepts_fresh_exact_owner_and_keeps_legacy_contract(recorder):
    from scripts import v2_with_controllers as runner
    controller = SimpleNamespace(config=ModularRSIV5ControllerConfig(id=OWNER))
    strategy = SimpleNamespace(_operator_started_at=100, _operator_watermarks={})
    payload = {'bot_name': 'candidate', 'requested_at': 200.125, 'command_id': 'fresh'}
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(runner.time, 'time', return_value=200.2), \
            patch.object(runner.HummingbotApplication, 'main_application', return_value=SimpleNamespace(instance_id='candidate')):
        assert V2WithControllers._native_operator_command_is_fresh(strategy, payload, [controller])
        assert V2WithControllers._native_operator_command_is_fresh(strategy, {}, [
            SimpleNamespace(config=SimpleNamespace(controller_name='ok_rsi'))])


def test_native_operator_reconciliation_rejects_before_receipt_access():
    controller = SimpleNamespace(config=ModularRSIV5ControllerConfig(id=OWNER))
    strategy = SimpleNamespace(_risk_reconciliation_error='disk reconciliation required')
    assert not V2WithControllers._native_operator_command_is_fresh(strategy, {}, [controller])


def test_operator_receipts_and_risk_state_commit_atomically(recorder):
    initial = risk_state()
    recorder.store_strategy_risk_state(initial)
    receipt = {'controller_id': OWNER, 'command_id': 'A', 'requested_at': 200.0}
    revised = {**initial, 'stopped': True}
    recorder.store_strategy_risk_state(revised, operator_receipts=[receipt])
    assert recorder.has_operator_command(OWNER, 'A')
    # A duplicate after a new receipt in the same transaction rolls both state and that receipt back.
    with pytest.raises(ValueError, match='already applied'):
        recorder.store_strategy_risk_state(initial, operator_receipts=[
            {**receipt, 'command_id': 'B'}, receipt])
    assert recorder.load_strategy_risk_state() == revised
    assert not recorder.has_operator_command(OWNER, 'B')


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['observe', 'persist'])
async def test_transient_risk_check_failure_refuses_the_action_without_latching_a_stop(recorder, failure):
    """One failed risk read or write refuses that action only; it must not become a durable stop."""
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDC'])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', initialize_modular), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig(
            runtime_db_sync_enabled=False, runtime_status_snapshot_enabled=False, chart_snapshots_enabled=False,
            max_global_drawdown_quote=100, max_controller_drawdown_quote=20))
        try:
            action = SimpleNamespace(controller_id=OWNER,
                                     executor_config=SimpleNamespace(side=TradeType.BUY))
            assert strategy._risk_reconciliation_error is None and not strategy._is_stop_triggered
            # A running owner: current reports, no pending resume claim.
            strategy._controller_reports_valid = True
            strategy.executor_orchestrator._planned_resume_pending = False
            permitted = strategy.validate_create_action(action)
            target = '_observe_daily_entry_risk' if failure == 'observe' else '_persist_risk_state'
            with patch.object(strategy, target, side_effect=RuntimeError('database is locked')) as failing:
                assert strategy.validate_create_action(action) is False
            assert failing.called
            assert not strategy._is_stop_triggered
            strategy._persist_risk_state()
            assert recorder.load_strategy_risk_state()['stopped'] is False
            assert strategy.validate_create_action(action) == permitted
        finally:
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('async_mode', [False, True])
async def test_on_tick_uses_async_operator_drain_only_for_acknowledged_recorder(async_mode):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    strategy = V2WithControllers.__new__(V2WithControllers)
    strategy._risk_persistence_enabled = True
    strategy._operator_drain_task = None
    strategy._risk_latch_task = None
    strategy._ownership_recovery_complete = Mock(return_value=False)
    strategy.process_operator_events = Mock()
    strategy.process_operator_events_async = AsyncMock()
    strategy._report_operator_drain_result = Mock()
    recorder_instance = SimpleNamespace(_async_financial_events=async_mode)

    with patch.object(StrategyV2Base, 'on_tick'), \
            patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder_instance):
        strategy.on_tick()
        if async_mode:
            assert strategy._operator_drain_task is not None
            await strategy._operator_drain_task
            strategy.process_operator_events.assert_not_called()
            strategy.process_operator_events_async.assert_awaited_once()
        else:
            assert strategy._operator_drain_task is None
            strategy.process_operator_events.assert_called_once()
            strategy.process_operator_events_async.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_tick_rail_failure_status_and_log_clear_once_the_rails_recover(recorder):
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDC'])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', initialize_modular), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig(
            runtime_db_sync_enabled=False, runtime_status_snapshot_enabled=False, chart_snapshots_enabled=False,
            max_global_drawdown_quote=100, max_controller_drawdown_quote=20))
        try:
            strategy._controller_reports_valid = True
            strategy.executor_orchestrator._planned_resume_pending = False
            with patch.object(StrategyV2Base, 'on_tick'), \
                    patch.object(StrategyV2Base, 'format_status', return_value='status'), \
                    patch.object(strategy, 'send_performance_report') as report, \
                    patch.object(strategy, 'logger') as logger:
                with patch.object(strategy, '_observe_daily_entry_risk', side_effect=RuntimeError('database is locked')):
                    strategy.on_tick()
                assert 'NEW EXECUTIONS REFUSED' in strategy.format_status()
                assert report.called  # reporting continues while a rail fails
                drain = strategy._operator_drain_task
                if drain is not None:
                    await drain
                strategy.on_tick()  # rails healthy again, with no create attempted in between
                write = strategy._risk_write_task
                if write is not None:
                    await write
                assert 'NEW EXECUTIONS REFUSED' not in strategy.format_status()
                assert any('recovered' in str(c) for c in logger.return_value.info.call_args_list)
        finally:
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)
