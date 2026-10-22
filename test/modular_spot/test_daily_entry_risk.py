"""Daily native-PnL entry pause is durable and never an order/liquidation engine."""
import asyncio
from collections import deque
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from test_native_lifecycle import recorder as _recorder_fixture

recorder = _recorder_fixture


def test_daily_loss_latches_through_rollover_and_requires_explicit_acknowledgement():
    from hummingbot.strategy_v2.models.daily_entry_risk import DailyEntryRisk
    risk = DailyEntryRisk.new(D('5'), 86400)
    assert not risk.observe(86401, D('-4.99'))
    assert risk.observe(86402, D('-5'))
    restored = DailyEntryRisk.restore(risk.snapshot(), D('5'))
    assert restored.paused
    assert not restored.acknowledge(86403, D('-4'))
    assert restored.observe(172800, D('-4'))  # UTC rollover never resumes implicitly.
    assert restored.acknowledge(172801, D('-4'))
    assert not restored.paused
    assert restored.observe(172802, D('-10'))
    with pytest.raises(ValueError):
        restored.observe(172801, D('-10'))
    with pytest.raises(ValueError):
        DailyEntryRisk.restore(restored.snapshot(), D('6'))


@pytest.mark.asyncio
async def test_genuine_runner_persists_daily_entry_pause_without_stopping_native_exits(recorder):
    from test.modular_spot.test_native_runner_policy import OWNER, initialize_modular

    from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.core.data_type.common import TradeType
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    from hummingbot.strategy_v2.models.base import RunnableStatus
    from scripts.v2_with_controllers import V2WithControllers, V2WithControllersConfig
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDC'])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', initialize_modular), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()), \
            patch('scripts.v2_with_controllers.time.time', return_value=86401), \
            patch('socket.socket.connect', side_effect=AssertionError('network forbidden')):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig(max_daily_loss_quote='5',
                                                                                 max_global_drawdown_quote=50, max_controller_drawdown_quote=25))
        try:
            owner = strategy.controllers[OWNER]
            owner._status = RunnableStatus.RUNNING
            report = SimpleNamespace(pnl_available=True, global_pnl_quote=D('0'))
            strategy.controller_reports = {OWNER: {'performance': report}}
            strategy._controller_reports_valid = True  # fresh reports, as on every live tick
            buy = SimpleNamespace(controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.BUY))
            sell = SimpleNamespace(controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.SELL))
            assert strategy.validate_create_action(buy)
            report.global_pnl_quote = D('-5')
            with patch.object(strategy.executor_orchestrator, 'execute_actions') as actions:
                assert not strategy.validate_create_action(buy)
                assert strategy.validate_create_action(sell)
                actions.assert_not_called()
            assert owner.status == RunnableStatus.RUNNING
            assert owner._operator_entry_paused
            saved = recorder.load_strategy_risk_state()
            assert saved['daily_entry_risk']['paused']
            assert saved['daily_entry_risk']['limit_quote'] == '5'
            with patch.object(StrategyV2Base, '_build_runtime_status_snapshot', return_value={'schema_version': 1}):
                assert strategy._build_runtime_status_snapshot()['daily_entry_risk'] == saved['daily_entry_risk']
            assert not strategy._is_stop_triggered
            disabled = V2WithControllers({'okx': connector}, V2WithControllersConfig(
                max_daily_loss_quote=None, max_global_drawdown_quote=50, max_controller_drawdown_quote=25))
            try:
                assert disabled._is_stop_triggered
                assert 'removed' in disabled._risk_reconciliation_error
                assert recorder.load_strategy_risk_state() == saved
                assert not disabled.validate_create_action(buy)
            finally:
                disabled.listen_to_executor_actions_task.cancel()
                await asyncio.gather(disabled.listen_to_executor_actions_task, return_exceptions=True)
            with patch('scripts.v2_with_controllers.HummingbotApplication.main_application',
                       return_value=SimpleNamespace(instance_id='v2')):
                strategy._operator_started_at = 86400
                command = dict(action='global_resume_entries', bot_name='v2', requested_at=86401,
                               command_id='same-day')
                strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=command)))
                strategy.process_operator_events()
                assert owner._operator_entry_paused
                with patch('scripts.v2_with_controllers.time.time', return_value=172801):
                    assert not strategy.validate_create_action(buy)
                    command.update(requested_at=172801, command_id='next-day')
                    strategy._operator_event_queue.append(('rsibot.operator', SimpleNamespace(data=command)))
                    strategy.process_operator_events()
                    assert not owner._operator_entry_paused
                    assert strategy.validate_create_action(buy)
                    assert not recorder.load_strategy_risk_state()['daily_entry_risk']['paused']
                    assert recorder.has_operator_command(OWNER, 'next-day')
        finally:
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)
