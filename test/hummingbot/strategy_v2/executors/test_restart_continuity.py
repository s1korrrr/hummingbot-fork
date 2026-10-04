"""Restart continuity: a planned or crash restart restores executors exactly (real SQLite)."""
import asyncio
import logging
from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor, tracked
from test.test_ok_rsi_down_only_entry_execution import make_config as make_down_only_config
from types import SimpleNamespace
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from controllers.directional_trading.ok_rsi_core.config import OKRSIControllerConfig
from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import DownOnlyMakerPositionExecutor
from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.strategy_v2.executors import planned_position_resume as resume
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.executors.restart_continuity import (
    RESTART_ENTRY_STOP_REASON,
    RestartContinuityError,
    continuity_plan,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType

RULE = TradingRule('ETH-USDT', min_order_size=D('.01'), min_price_increment=D('.01'),
                   min_base_amount_increment=D('.01'), min_notional_size=D('1'))


@pytest.fixture
def recorder(tmp_path):
    owned_loop = None
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        owned_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(owned_loop)
    manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                   db_path=str(tmp_path / 'continuity.sqlite'))
    result = MarketsRecorder(manager, [], 'continuity.yml', 'v2_with_controllers',
                             market_data_collection=MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    yield result
    manager.engine.dispose()
    if owned_loop is not None:
        owned_loop.close()
        asyncio.set_event_loop(None)


def live_executor(entry_fill='1', entry_state=OrderState.FILLED, trigger=None, moves=0):
    executor = make_executor()
    executor.config.controller_id = 'controller'
    executor.config.entry_order_policy = 'single_attempt'
    executor._open_order = tracked('entry', TradeType.BUY, entry_fill, entry_state)
    executor._entry_submitted = True
    if entry_state == OrderState.FILLED:
        executor._entry_lifecycle = {'state': 'managing_fill', 'stop_reason': None}
    if trigger is not None:
        executor._trailing_stop_trigger_pct = D(trigger)
        executor._trailing_stop_move_count = moves
        executor._trailing_stop_armed_timestamp = 900.0
        executor._trailing_stop_last_update_timestamp = 950.0
    return executor


def booting_strategy(balance='10'):
    strategy = make_executor()._strategy
    strategy.controllers = {'controller': SimpleNamespace(
        config=SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT'))}
    strategy.markets = {'okx': {'ETH-USDT'}}
    strategy.market_data_provider = MagicMock()
    strategy.market_data_provider.get_price_by_type.return_value = D('100')
    strategy.market_data_provider.ready = True
    strategy.ready_to_trade = True
    connector = strategy.connectors['okx']
    connector._order_tracker.fetch_order.return_value = None
    connector.get_balance.return_value = D(balance)
    connector.in_flight_orders = {}
    return strategy


def boot(recorder, strategy):
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(PositionExecutor, 'get_trading_rules', return_value=RULE), \
            patch.object(PositionExecutor, 'start') as start:
        orchestrator = ExecutorOrchestrator(strategy)
        assert orchestrator._planned_resume_pending is True  # waits for connectors, restores nothing yet
        assert orchestrator.resume_planned_positions_if_ready() is True
    return orchestrator, start


def restored(orchestrator):
    executors = orchestrator.active_executors['controller']
    assert len(executors) == 1
    return executors[0]


def test_planned_suspend_keeps_row_running_with_exact_trailing(recorder):
    executor = live_executor(trigger='.012', moves=3)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        assert executor.suspend_for_restart() is True
        executor._advance_restart_suspend()
    assert executor.is_restart_suspended
    assert executor.status == RunnableStatus.RUNNING and executor.close_type is None
    row = recorder.get_all_executors()[0]
    assert row.status == RunnableStatus.RUNNING and row.close_type is None
    assert row.custom_info['restart_suspended']['reason'] == 'planned_restart'
    assert D(str(row.custom_info['trailing_stop_trigger_pct'])) == D('.012')
    assert row.custom_info['trailing_move_count'] == 3


def test_planned_suspend_control_loop_completion_keeps_durable_row_running(recorder):
    executor = live_executor(trigger='.012', moves=3)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        executor.suspend_for_restart()
        executor._advance_restart_suspend()
        task = asyncio.get_event_loop().create_future()
        task.set_result(None)
        executor._control_loop_task = task
        executor._control_loop_finished(task)
        recorder.store_or_update_executor(executor)
    assert executor.status == RunnableStatus.RUNNING
    row = recorder.get_all_executors()[0]
    assert row.status == RunnableStatus.RUNNING and row.is_active


def test_planned_suspend_faulted_control_loop_does_not_preserve_running_row(recorder, caplog):
    executor = live_executor(trigger='.012', moves=3)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        executor.suspend_for_restart()
        executor._advance_restart_suspend()
        task = asyncio.get_event_loop().create_future()
        task.set_exception(RuntimeError('suspended control task failed'))
        executor._control_loop_task = task
        with caplog.at_level(logging.ERROR):
            executor._control_loop_finished(task)
        recorder.store_or_update_executor(executor)
    assert executor.status == RunnableStatus.TERMINATED
    assert recorder.get_all_executors()[0].status == RunnableStatus.TERMINATED
    assert 'Suspended executor control loop failed' in caplog.text


def test_planned_suspend_explicit_stop_keeps_durable_row_running(recorder):
    executor = live_executor(trigger='.012', moves=3)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        executor.suspend_for_restart()
        executor._advance_restart_suspend()
        executor.stop()
        recorder.store_or_update_executor(executor)
    assert executor.status == RunnableStatus.RUNNING
    assert recorder.get_all_executors()[0].status == RunnableStatus.RUNNING


def test_suspend_waits_for_resting_entry_cancel_then_keeps_partial_fill(recorder):
    executor = live_executor(entry_fill='.4', entry_state=OrderState.PARTIALLY_FILLED)
    executor.cancel_open_order = MagicMock()
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        executor.suspend_for_restart()
        executor._advance_restart_suspend()
    assert not executor.is_restart_suspended
    assert executor._entry_stop_reason == RESTART_ENTRY_STOP_REASON


@pytest.mark.parametrize('suspended', [True, False], ids=['planned', 'crash'])
def test_restart_restores_armed_trailing_executor_exactly(recorder, suspended):
    executor = live_executor(trigger='.015', moves=5)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        if suspended:
            executor.suspend_for_restart()
            executor._advance_restart_suspend()
        else:
            recorder.store_or_update_executor(executor)  # last synchronous checkpoint before a crash
    orchestrator, start = boot(recorder, booting_strategy())
    again = restored(orchestrator)
    assert again.config.id == executor.config.id
    assert again._trailing_stop_trigger_pct == D('.015')
    assert again._trailing_stop_move_count == 5
    assert again._trailing_stop_armed_timestamp == 900.0
    assert again.amount_to_close == executor.amount_to_close == D('1')
    assert again._resume_entry_sealed is True
    assert again._restart_restored['suspended'] is suspended
    assert orchestrator.recovery_required == {}
    start.assert_called_once()


def test_cancelled_partial_entry_restores_as_managed_position(recorder):
    executor = live_executor(entry_fill='.4', entry_state=OrderState.CANCELED)
    executor._open_order_entry_expired = True
    executor._open_order_expiry_reconciled = True
    executor._entry_lifecycle = {'state': 'managing_fill', 'stop_reason': 'exchange_canceled'}
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(executor)
    orchestrator, _ = boot(recorder, booting_strategy())
    again = restored(orchestrator)
    assert again.amount_to_close == D('.4')
    assert again._entry_terminal_confirmed()
    assert again._restart_stop_entry_pending is False


def test_open_entry_at_crash_is_retracked_and_its_acquisition_stopped(recorder):
    executor = live_executor(entry_fill='.3', entry_state=OrderState.PARTIALLY_FILLED)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(executor)
    strategy = booting_strategy()
    orchestrator, _ = boot(recorder, strategy)
    again = restored(orchestrator)
    tracker = strategy.connectors['okx']._order_tracker
    tracker.start_tracking_order.assert_called_once()
    assert tracker.start_tracking_order.call_args.args[0].client_order_id == 'entry'
    assert again._restart_stop_entry_pending is True
    again._status = RunnableStatus.RUNNING
    again.cancel_open_order = MagicMock()
    again.control_barriers = MagicMock()
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        asyncio.get_event_loop().run_until_complete(again.control_task())
    assert again._entry_stop_reason == RESTART_ENTRY_STOP_REASON
    again.cancel_open_order.assert_called_once()
    assert again.open_filled_amount == D('.3')


def test_live_tracker_order_is_shared_after_restore(recorder):
    executor = live_executor(entry_fill='.5', entry_state=OrderState.PARTIALLY_FILLED)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(executor)
    strategy = booting_strategy()
    live = tracked('entry', TradeType.BUY, '1', OrderState.FILLED).order  # the venue filled it while down
    strategy.connectors['okx']._order_tracker.fetch_order.return_value = live
    orchestrator, _ = boot(recorder, strategy)
    again = restored(orchestrator)
    assert again._open_order.order is live
    assert again.open_filled_amount == D('1')


def test_exit_in_flight_at_crash_resumes_shutdown_with_its_close_order(recorder):
    executor = live_executor(trigger='.02', moves=2)
    executor._close_order = tracked('exit', TradeType.SELL, '.25', OrderState.PARTIALLY_FILLED)
    executor.close_type = CloseType.TRAILING_STOP
    executor._status = RunnableStatus.SHUTTING_DOWN
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(executor)
    orchestrator, _ = boot(recorder, booting_strategy())
    again = restored(orchestrator)
    assert again._close_order.order_id == 'exit'
    assert again.amount_to_close == D('.75')
    asyncio.get_event_loop().run_until_complete(again.on_start())
    assert again.status == RunnableStatus.SHUTTING_DOWN and again.close_type == CloseType.TRAILING_STOP


def test_contradictory_row_is_reported_and_never_blocks_new_execution(recorder):
    executor = live_executor()
    executor._completed_close_orders = [tracked('exit', TradeType.SELL, '1', OrderState.FILLED),
                                        tracked('exit-2', TradeType.SELL, '1', OrderState.FILLED)]
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(executor)
    strategy = booting_strategy()
    orchestrator, _ = boot(recorder, strategy)
    assert orchestrator.recovery_required == {'controller': [executor.config.id]}
    assert orchestrator.active_executors['controller'] == []
    fresh = make_executor()
    fresh.config.controller_id = 'controller'
    fresh.config.id = 'fresh'
    created = MagicMock()
    orchestrator._validate_okx_wallet_reservation = MagicMock()
    orchestrator._resolve_executor_class = MagicMock(return_value=MagicMock(return_value=created))
    with patch.object(MarketsRecorder, 'get_instance', return_value=MagicMock()):
        orchestrator.create_executor(CreateExecutorAction(controller_id='controller', executor_config=fresh.config))
    created.start.assert_called_once()


def test_plan_rejects_exits_larger_than_entries():
    executor = live_executor(entry_fill='.5', entry_state=OrderState.CANCELED)
    executor._completed_close_orders = [tracked('exit', TradeType.SELL, '.6', OrderState.FILLED)]
    with pytest.raises(RestartContinuityError):
        continuity_plan(executor.executor_info)


def test_wallet_shortfall_hands_restored_position_to_held_ledger(recorder):
    executor = live_executor(trigger='.01', moves=1)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(executor)
    orchestrator, _ = boot(recorder, booting_strategy(balance='.5'))
    again = restored(orchestrator)
    again._status = RunnableStatus.RUNNING
    again.control_barriers = MagicMock()
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        asyncio.get_event_loop().run_until_complete(again.control_task())
    assert again.close_type == CloseType.POSITION_HOLD


def test_orchestrator_shutdown_suspends_instead_of_demoting(recorder):
    executor = live_executor(trigger='.012', moves=1)
    orchestrator = ExecutorOrchestrator.__new__(ExecutorOrchestrator)
    orchestrator.active_executors = {'controller': [executor]}
    orchestrator._planned_resume_pending = False
    orchestrator.store_all_positions = MagicMock()
    orchestrator.store_all_executors = MagicMock()

    async def drive():
        stopping = asyncio.ensure_future(orchestrator.stop(keep_position_by_controller={'controller': True}))
        await asyncio.sleep(0)
        executor._advance_restart_suspend()
        with patch.object(asyncio, 'sleep', return_value=None):
            await stopping

    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        asyncio.get_event_loop().run_until_complete(drive())
    assert executor.close_type is None and executor.is_restart_suspended
    orchestrator.store_all_executors.assert_called_once()


def test_sell_on_shutdown_preference_is_still_honoured():
    executor = live_executor()
    executor.early_stop = MagicMock()
    assert ExecutorOrchestrator._begin_executor_shutdown(executor, keep_position=False) is False
    executor.early_stop.assert_called_once_with(keep_position=False)


def test_acquisition_only_stop_keeps_filled_position_trailing():
    executor = live_executor(entry_fill='.4', entry_state=OrderState.PARTIALLY_FILLED)
    executor.early_stop = MagicMock()
    orchestrator = ExecutorOrchestrator.__new__(ExecutorOrchestrator)
    orchestrator.active_executors = {'controller': [executor]}
    orchestrator.stop_executor(StopExecutorAction(controller_id='controller', executor_id=executor.config.id,
                                                  keep_position=True, acquisition_only=True))
    executor.early_stop.assert_not_called()
    assert executor._entry_stop_reason == 'external_invalidation'
    assert executor.status == RunnableStatus.RUNNING and executor.close_type is None


def test_unreadable_fill_accounting_never_aborts_a_stop(caplog):
    executor = live_executor()
    orchestrator = ExecutorOrchestrator.__new__(ExecutorOrchestrator)
    orchestrator.active_executors = {'controller': [executor]}
    with patch.object(PositionExecutor, 'open_filled_amount', new_callable=PropertyMock,
                      side_effect=ValueError('Non-finite base fee')), caplog.at_level(logging.WARNING):
        orchestrator.stop_executor(StopExecutorAction(controller_id='controller', executor_id=executor.config.id,
                                                      keep_position=True))
    assert executor.close_type == CloseType.POSITION_HOLD
    assert executor.status == RunnableStatus.SHUTTING_DOWN
    assert 'filled_base=UNAVAILABLE pending_close_type=POSITION_HOLD' in caplog.text


def test_acquisition_only_stop_is_a_no_op_for_a_filled_entry():
    executor = live_executor()
    executor.stop_entry_acquisition('external_invalidation')
    assert executor._entry_stop_reason is None
    assert executor.status == RunnableStatus.RUNNING


def test_unresolved_rows_do_not_fence_executor_orders():
    executor = make_executor()
    executor._strategy.executor_orchestrator = SimpleNamespace(
        recovery_required={'other': ['stale']}, _planned_resume_pending=False)
    assert executor._stack_recovery_required() is False


def test_legacy_v1_sealed_restore_marker_does_not_break_boot(recorder):
    finished = live_executor()
    finished.config.id = 'legacy-sealed'
    finished._planned_resume_id = 'startup-sealed-restore'
    finished.close_type = CloseType.TRAILING_STOP
    finished._status = RunnableStatus.TERMINATED
    running = live_executor(trigger='.01', moves=1)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(finished)
        recorder.store_or_update_executor(running)
    assert resume.pending_manifest(recorder.sql_manager.db_path) is None  # the marker is not manifest history
    orchestrator, _ = boot(recorder, booting_strategy())
    assert restored(orchestrator).config.id == running.config.id


def test_unreadable_manifest_history_never_stops_boot(recorder):
    running = live_executor(trigger='.01', moves=1)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(running)
    with patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
               side_effect=ValueError('executor references missing or mismatched consumed manifest history')):
        orchestrator, _ = boot(recorder, booting_strategy())
    assert restored(orchestrator).config.id == running.config.id


def test_unowned_tracked_order_is_cancelled_after_restart(recorder):
    running = live_executor(trigger='.01', moves=1)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(running)
    strategy = booting_strategy()
    orphan = tracked('orphan-buy', TradeType.BUY, '0', OrderState.OPEN).order  # placed just before a crash
    strategy.connectors['okx'].in_flight_orders = {'orphan-buy': orphan}
    orchestrator, _ = boot(recorder, strategy)
    strategy.cancel.assert_called_once_with('okx', 'ETH-USDT', 'orphan-buy')
    assert orchestrator.recovery_required['unowned_orders'] == ['orphan-buy']
    assert restored(orchestrator).config.id == running.config.id


def test_restored_owner_orders_are_not_cancelled(recorder):
    running = live_executor(entry_fill='.2', entry_state=OrderState.PARTIALLY_FILLED)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(running)
    strategy = booting_strategy()
    live = tracked('entry', TradeType.BUY, '.2', OrderState.PARTIALLY_FILLED).order
    strategy.connectors['okx'].in_flight_orders = {'entry': live}
    strategy.connectors['okx']._order_tracker.fetch_order.return_value = live
    boot(recorder, strategy)
    strategy.cancel.assert_not_called()


def test_later_boot_failure_still_binds_restored_executors(recorder):
    running = live_executor(trigger='.01', moves=1)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(running)
    strategy = booting_strategy()
    with patch.object(ExecutorOrchestrator, '_create_initial_positions', side_effect=RuntimeError('boom')):
        orchestrator, start = boot(recorder, strategy)
    again = restored(orchestrator)
    assert again.config.id == running.config.id
    assert running.config.id in orchestrator.recovery_required['controller']
    start.assert_called_once()


def test_row_checkpointed_before_its_entry_order_never_submits_a_new_entry(recorder):
    running = make_executor()
    running.config.controller_id = 'controller'
    running.config.entry_order_policy = 'single_attempt'  # crash before the order snapshot was written
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(running)
    orchestrator, _ = boot(recorder, booting_strategy())
    again = restored(orchestrator)
    again._status = RunnableStatus.RUNNING
    again.place_order = MagicMock()
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        asyncio.get_event_loop().run_until_complete(again.control_task())
    again.place_order.assert_not_called()
    assert again.close_type == CloseType.EXPIRED


def test_wallet_shortfall_also_demotes_a_restored_exit(recorder):
    executor = live_executor(trigger='.02', moves=2)
    executor._close_order = tracked('exit', TradeType.SELL, '0', OrderState.OPEN)
    executor.close_type = CloseType.TRAILING_STOP
    executor._status = RunnableStatus.SHUTTING_DOWN
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(executor)
    orchestrator, _ = boot(recorder, booting_strategy(balance='.5'))
    again = restored(orchestrator)
    asyncio.get_event_loop().run_until_complete(again.on_start())
    again.control_shutdown_process = MagicMock(side_effect=lambda: asyncio.sleep(0))
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        asyncio.get_event_loop().run_until_complete(again.control_task())
    assert again.close_type == CloseType.POSITION_HOLD


def test_resting_take_profit_is_restored_as_take_profit_not_close(recorder):
    executor = live_executor()
    executor._take_profit_limit_order = tracked('tp', TradeType.SELL, '0', OrderState.OPEN)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(executor)
    orchestrator, _ = boot(recorder, booting_strategy())
    again = restored(orchestrator)
    assert again._take_profit_limit_order.order_id == 'tp' and again._close_order is None


def test_shutdown_lets_an_exiting_executor_settle():
    executor = live_executor(trigger='.02', moves=1)
    executor.close_type = CloseType.TRAILING_STOP
    executor._status = RunnableStatus.SHUTTING_DOWN
    executor.early_stop = MagicMock()
    assert ExecutorOrchestrator._begin_executor_shutdown(executor, keep_position=True) is True
    executor.early_stop.assert_not_called()


def test_unrestorable_row_fills_are_not_rebuilt_into_held_bags(recorder):
    broken = live_executor()
    broken._completed_close_orders = [tracked('x1', TradeType.SELL, '1', OrderState.FILLED),
                                      tracked('x2', TradeType.SELL, '1', OrderState.FILLED)]
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(broken)
    orchestrator, _ = boot(recorder, booting_strategy())
    assert ('okx', 'ETH-USDT', 'entry') in orchestrator._non_hold_order_ids


class _SubclassExecutor(PositionExecutor):
    pass


def test_controller_named_continuity_class_restores_its_tranche_subclass(recorder):
    executor = live_executor(trigger='.015', moves=2)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(executor)
    strategy = booting_strategy()
    strategy.controllers['controller'].restart_continuity_executor_class = lambda config: _SubclassExecutor
    orchestrator, _ = boot(recorder, strategy)
    again = restored(orchestrator)
    assert type(again) is _SubclassExecutor
    assert again._trailing_stop_trigger_pct == D('.015')


def test_controller_refusing_a_row_is_reported_not_rebuilt_as_plain_executor(recorder):
    executor = live_executor()
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(executor)
    strategy = booting_strategy()

    def refuse(config):
        raise RestartContinuityError('not a tranche of this controller')
    strategy.controllers['controller'].restart_continuity_executor_class = refuse
    orchestrator, start = boot(recorder, strategy)
    assert orchestrator.active_executors.get('controller', []) == []
    assert orchestrator.recovery_required == {'controller': [executor.config.id]}
    start.assert_not_called()
    strategy.controllers['controller'].restart_continuity_executor_class = lambda config: object
    with pytest.raises(RestartContinuityError):
        orchestrator._continuity_executor_class(strategy.controllers['controller'], executor.config)


def sealed_down_only_executor():
    base = make_executor()
    barriers = base.config.triple_barrier_config.model_copy(update={'open_order_limit_max_age_seconds': 0})
    config = make_down_only_config(id=base.config.id, timestamp=base.config.timestamp, controller_id='controller',
                                   trading_pair='ETH-USDT', triple_barrier_config=barriers)
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=RULE):
        executor = DownOnlyMakerPositionExecutor(base._strategy, config)
    executor._status = RunnableStatus.RUNNING
    executor.get_price = MagicMock(return_value=D('96'))  # below the 100 entry: a drawdown hold is due
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    executor._entry_submitted = True
    executor._entry_lifecycle = {'state': 'managing_fill', 'stop_reason': None}
    return executor


def test_sealed_restored_down_only_fill_keeps_executor_and_drawdown_hold_is_visible(recorder, caplog):
    """2026-09-28 ok-rsi-xrp: a sealed restored fill closed as POSITION_HOLD under an 'external_invalidation' label."""
    executor = sealed_down_only_executor()
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        assert executor.suspend_for_restart() is True
        executor._advance_restart_suspend()
    strategy = booting_strategy()
    strategy.controllers['controller'] = SimpleNamespace(
        config=OKRSIControllerConfig(id='controller', connector_name='okx', trading_pair='ETH-USDT',
                                     entry_acquisition_policy='down_only_maker_v1'),
        get_executor_class_for_config=lambda config: DownOnlyMakerPositionExecutor)
    orchestrator, _ = boot(recorder, strategy)
    again = restored(orchestrator)
    assert isinstance(again, DownOnlyMakerPositionExecutor) and again._resume_entry_sealed is True
    again._status = RunnableStatus.RUNNING
    again.get_price = MagicMock(return_value=D('96'))
    executor_id = again.config.id

    # D1: the opposing-signal / kill-switch stop only ends the entry. The filled,
    # sealed position keeps its executor and trailing exit.
    orchestrator.stop_executor(StopExecutorAction(controller_id='controller', executor_id=executor_id,
                                                  keep_position=True, acquisition_only=True))
    assert again.status == RunnableStatus.RUNNING and again.close_type is None
    assert again._entry_stop_reason is None

    # The configured drawdown hold (early_stop_drawdown_pct + early_stop_keep_position) is a full
    # keep-position stop. It hands the fill to the held ledger and must say so in the live log.
    with caplog.at_level(logging.INFO):
        orchestrator.stop_executor(StopExecutorAction(controller_id='controller', executor_id=executor_id,
                                                      keep_position=True))
    assert again.close_type == CloseType.POSITION_HOLD
    assert again._entry_stop_reason == 'position_hold'
    assert again.get_custom_info()['entry_acquisition']['terminal_reason'] == 'position_hold'
    assert f'Executor stop requested | controller=controller executor={executor_id} keep_position=True' in caplog.text
    assert 'pending_close_type=POSITION_HOLD' in caplog.text
    assert 'Entry acquisition stopped reason=position_hold' in caplog.text


def test_terminal_suspended_row_requires_evidence_bound_planned_resume(recorder):
    """An old terminal marker alone cannot revive a position or bypass the recovery manifest."""
    executor = live_executor(trigger='.015', moves=5)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        executor.suspend_for_restart()
        executor._advance_restart_suspend()
        executor._status = RunnableStatus.TERMINATED  # RunnableBase.stop() during process shutdown
        recorder.store_or_update_executor(executor)
    row = recorder.get_all_executors()[0]
    assert row.status == RunnableStatus.TERMINATED and row.close_type is None
    assert row.custom_info['restart_suspended']['reason'] == 'planned_restart'
    orchestrator, start = boot(recorder, booting_strategy())
    assert continuity_plan(row) is None
    assert orchestrator.active_executors.get('controller', []) == []
    assert orchestrator.recovery_required == {'controller': [executor.config.id]}
    start.assert_not_called()


def test_terminal_suspended_row_with_marker_mismatch_is_reported_not_restored(recorder):
    executor = live_executor(trigger='.015', moves=5)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        executor.suspend_for_restart()
        executor._advance_restart_suspend()
        executor._restart_suspended['remaining_position_amount_base'] = '0.5'
        executor._status = RunnableStatus.TERMINATED
        recorder.store_or_update_executor(executor)
    orchestrator, start = boot(recorder, booting_strategy())
    assert orchestrator.active_executors.get('controller', []) == []
    assert orchestrator.recovery_required == {'controller': [executor.config.id]}
    start.assert_not_called()


def test_terminal_position_with_missing_marker_remains_unresolved(recorder):
    executor = live_executor(trigger='.015', moves=5)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        executor.suspend_for_restart()
        executor._advance_restart_suspend()
        executor._restart_suspended = None
        executor._status = RunnableStatus.TERMINATED
        recorder.store_or_update_executor(executor)
    orchestrator, start = boot(recorder, booting_strategy())
    assert orchestrator.active_executors.get('controller', []) == []
    assert orchestrator.recovery_required == {'controller': [executor.config.id]}
    start.assert_not_called()


def test_terminal_closed_row_is_not_a_continuity_candidate(recorder):
    executor = live_executor()
    executor._status = RunnableStatus.TERMINATED
    executor.close_type = CloseType.TAKE_PROFIT
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        recorder.store_or_update_executor(executor)
    row = recorder.get_all_executors()[0]
    assert continuity_plan(row) is None
