import asyncio
import json
import tempfile
import types
import unittest
from decimal import Decimal
from pathlib import Path
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from types import SimpleNamespace
from typing import List
from unittest.mock import AsyncMock, MagicMock, PropertyMock, call, patch

import pandas as pd

import hummingbot.strategy.strategy_v2_base as strategy_v2_base_module
from controllers.directional_trading.ok_rsi_core.config import OKRSIControllerConfig
from hummingbot.connector.exchange.paper_trade.paper_trade_exchange import QuantizationParams
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.test_support.mock_paper_exchange import MockPaperExchange
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.common import PositionAction, PositionMode, TradeType
from hummingbot.core.event.events import OrderType
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.components.data_reader import DataPolicy
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo, PerformanceReport


class MockScriptStrategy(StrategyV2Base):
    pass


class DummyReloadControllerBase(ControllerBase):
    def __init__(self, config, market_data_provider, actions_queue):
        super().__init__(config, market_data_provider, actions_queue)
        self.start_calls = 0
        self.stop_calls = 0

    def start(self):
        self.start_calls += 1
        self._status = RunnableStatus.RUNNING

    def stop(self):
        self.stop_calls += 1
        self._status = RunnableStatus.TERMINATED


class DummyReloadControllerV1(DummyReloadControllerBase):
    pass


class DummyReloadControllerV2(DummyReloadControllerBase):
    pass


class TestStrategyV2Base(IsolatedAsyncioWrapperTestCase):
    CONTROLLERS_TITLE = "📊 CONTROLLERS"
    SIGNALS_TITLE = "📡 SIGNALS"
    PLANS_TITLE = "🧭 PLANS"
    INVENTORY_TITLE = "🎒 INVENTORY"

    def setUp(self):
        application_patch = patch("hummingbot.client.hummingbot_application.HummingbotApplication.main_application",
                                  return_value=MagicMock(_mqtt=None))
        application_patch.start()
        self.addCleanup(application_patch.stop)
        self.start: pd.Timestamp = pd.Timestamp("2021-01-01", tz="UTC")
        self.end: pd.Timestamp = pd.Timestamp("2021-01-01 01:00:00", tz="UTC")
        self.start_timestamp: float = self.start.timestamp()
        self.end_timestamp: float = self.end.timestamp()
        self.clock_tick_size = 1
        self.clock: Clock = Clock(ClockMode.BACKTEST, self.clock_tick_size, self.start_timestamp, self.end_timestamp)
        self.connector: MockPaperExchange = MockPaperExchange()
        self.connector_name: str = "mock_paper_exchange"
        self.trading_pair: str = "HBOT-USDT"
        self.strategy_config = StrategyV2ConfigBase()
        with patch('asyncio.create_task', side_effect=lambda coro: (coro.close(), MagicMock())[1]):
            # Initialize the strategy with mock components
            with patch("hummingbot.strategy.strategy_v2_base.StrategyV2Base.listen_to_executor_actions", return_value=AsyncMock()):
                with patch('hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator') as MockExecutorOrchestrator:
                    with patch('hummingbot.strategy.strategy_v2_base.MarketDataProvider') as MockMarketDataProvider:
                        self.strategy = StrategyV2Base({self.connector_name: self.connector}, config=self.strategy_config)
                        # Set mocks to strategy attributes
                        self.strategy.executor_orchestrator = MockExecutorOrchestrator.return_value
                        self.strategy.market_data_provider = MockMarketDataProvider.return_value
                        self.strategy.controllers = {'controller_1': MagicMock(), 'controller_2': MagicMock()}
        self.strategy.logger().setLevel(1)

    def assert_no_status_separator_blocks(self, status: str):
        self.assertFalse(
            any(line.strip() and set(line.strip()) == {"="} for line in status.splitlines()),
            status,
        )

    def test_direct_strategy_orders_are_never_fenced_by_reported_unfinished_executor(self):
        self.strategy.executor_orchestrator.recovery_required = {"removed-owner": ["entry-1"]}
        self.strategy.executor_orchestrator._planned_resume_pending = False
        with patch.object(self.strategy, "buy_with_specific_market") as buy, \
                patch.object(self.strategy, "sell_with_specific_market") as sell:
            self.strategy.buy(self.connector_name, self.trading_pair, Decimal("1"), OrderType.LIMIT)
            self.strategy.sell(self.connector_name, self.trading_pair, Decimal("1"), OrderType.LIMIT)
        buy.assert_called_once()
        sell.assert_called_once()

    def test_direct_strategy_sell_blocked_during_pending_planned_resume(self):
        self.strategy.executor_orchestrator.recovery_required = {}
        self.strategy.executor_orchestrator._planned_resume_pending = True
        with patch.object(self.strategy, "sell_with_specific_market") as sell:
            with self.assertRaisesRegex(RuntimeError, "startup reconciliation"):
                self.strategy.sell(self.connector_name, self.trading_pair, Decimal("1"), OrderType.LIMIT)
        sell.assert_not_called()

    def test_controller_config_persistence_waits_for_successful_startup_resume(self):
        controller_config = MagicMock(id='controller-1')
        recorder = MagicMock()
        events = []
        recorder.store_controller_config.side_effect = lambda config: events.append('store')
        self.strategy.executor_orchestrator._planned_resume_pending = True
        self.strategy.executor_orchestrator._resume_preflight_failed = False
        self.strategy.executor_orchestrator._planned_resume_failure = None
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
                patch.object(StrategyV2ConfigBase, 'load_controller_configs', return_value=[controller_config]), \
                patch.object(self.strategy, 'add_controller'):
            self.strategy.initialize_controllers()
            recorder.store_controller_config.assert_not_called()

            def successful_claim():
                events.append('claim')
                self.strategy.executor_orchestrator._planned_resume_pending = False
                return True

            self.strategy.executor_orchestrator.resume_planned_positions_if_ready.side_effect = successful_claim
            self.strategy.update_executors_info = MagicMock()
            self.strategy._sync_runtime_state_to_db_if_due = MagicMock()
            self.strategy._write_chart_snapshots_if_due = MagicMock()
            self.strategy._write_runtime_status_snapshot_if_due = MagicMock()
            self.strategy.update_controllers_configs = MagicMock()
            self.strategy.market_data_provider.ready = True
            self.strategy.determine_executor_actions = MagicMock(return_value=[])

            self.strategy.on_tick()

        recorder.store_controller_config.assert_called_once_with(controller_config)
        self.assertEqual(events, ['claim', 'store'])
        self.assertEqual(self.strategy._controller_configs_pending_persistence, [])

    @staticmethod
    def _direct_ok_rsi_config(controller_id: str, *, origin: str = None):
        data = (DataPolicy(mode="shared", transport="stream", origin=origin,
                           token_file="/tmp/test-candle-token") if origin else None)
        return OKRSIControllerConfig(id=controller_id, data=data)

    def test_mixed_direct_ok_rsi_candle_policies_fail_before_controller_creation(self):
        configs = [self._direct_ok_rsi_config("service", origin="http://127.0.0.1:8100"),
                   self._direct_ok_rsi_config("native")]
        with patch.object(StrategyV2ConfigBase, "load_controller_configs", return_value=configs), \
                patch.object(self.strategy, "add_controller") as add_controller:
            with self.assertRaisesRegex(ValueError, "direct OK RSI candle policy"):
                self.strategy.initialize_controllers()
        add_controller.assert_not_called()
        self.assertEqual(self.strategy._controller_configs_pending_persistence, [])

    def test_conflicting_direct_ok_rsi_service_sources_fail_before_controller_creation(self):
        configs = [self._direct_ok_rsi_config("first", origin="http://127.0.0.1:8100"),
                   self._direct_ok_rsi_config("second", origin="http://127.0.0.1:8101")]
        with patch.object(StrategyV2ConfigBase, "load_controller_configs", return_value=configs), \
                patch.object(self.strategy, "add_controller") as add_controller:
            with self.assertRaisesRegex(ValueError, "direct OK RSI candle policy"):
                self.strategy.initialize_controllers()
        add_controller.assert_not_called()

    def test_uniform_direct_ok_rsi_candle_policies_allow_controller_creation(self):
        for origin in (None, "http://127.0.0.1:8100"):
            with self.subTest(origin=origin):
                configs = [self._direct_ok_rsi_config("first", origin=origin),
                           self._direct_ok_rsi_config("second", origin=origin)]
                with patch.object(StrategyV2ConfigBase, "load_controller_configs", return_value=configs), \
                        patch.object(self.strategy, "add_controller") as add_controller, \
                        patch.object(MarketsRecorder, "get_instance", return_value=MagicMock()):
                    self.strategy.initialize_controllers()
                self.assertEqual(add_controller.call_count, 2)

    def test_direct_ok_rsi_policy_guard_does_not_restrict_subclass_composition(self):
        class CompositionConfig(OKRSIControllerConfig):
            pass

        configs = [self._direct_ok_rsi_config("direct"),
                   CompositionConfig(id="composed", data=DataPolicy(
                       mode="shared", transport="stream", origin="http://127.0.0.1:8100",
                       token_file="/tmp/test-candle-token"))]
        with patch.object(StrategyV2ConfigBase, "load_controller_configs", return_value=configs), \
                patch.object(self.strategy, "add_controller") as add_controller, \
                patch.object(MarketsRecorder, "get_instance", return_value=MagicMock()):
            self.strategy.initialize_controllers()
        self.assertEqual(add_controller.call_count, 2)

    def test_direct_ok_rsi_candle_policy_reload_does_not_change_existing_source(self):
        native = self._direct_ok_rsi_config("direct")
        self.strategy.controllers = {"direct": MagicMock(config=native)}
        self.strategy.config_update_interval = 0
        service = self._direct_ok_rsi_config("direct", origin="http://127.0.0.1:8100")
        with patch.object(StrategyV2ConfigBase, "load_controller_configs", return_value=[service]), \
                patch.object(type(self.strategy), "current_timestamp", new_callable=PropertyMock, return_value=100), \
                patch.object(self.strategy, "add_controller") as add_controller:
            self.strategy.update_controllers_configs()
        add_controller.assert_not_called()
        self.strategy.controllers["direct"].update_config.assert_not_called()
        self.assertIn("direct OK RSI candle policy", self.strategy._last_controller_reload_error)

    def test_controller_config_persistence_is_immediate_without_pending_resume(self):
        controller_config = MagicMock(id='controller-1')
        recorder = MagicMock()
        self.strategy.executor_orchestrator._planned_resume_pending = False
        self.strategy.executor_orchestrator._resume_preflight_failed = False
        self.strategy.executor_orchestrator._planned_resume_failure = None
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
                patch.object(StrategyV2ConfigBase, 'load_controller_configs', return_value=[controller_config]), \
                patch.object(self.strategy, 'add_controller'):
            self.strategy.initialize_controllers()

        recorder.store_controller_config.assert_called_once_with(controller_config)
        self.assertEqual(self.strategy._controller_configs_pending_persistence, [])

    def test_controller_config_persistence_stays_deferred_after_resume_failure(self):
        controller_config = MagicMock(id='controller-1')
        recorder = MagicMock()
        self.strategy.executor_orchestrator._planned_resume_pending = True
        self.strategy.executor_orchestrator._resume_preflight_failed = False
        self.strategy.executor_orchestrator._planned_resume_failure = None
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
                patch.object(StrategyV2ConfigBase, 'load_controller_configs', return_value=[controller_config]), \
                patch.object(self.strategy, 'add_controller'):
            self.strategy.initialize_controllers()
            self.strategy.executor_orchestrator._planned_resume_pending = True

            def failed_claim():
                self.strategy.executor_orchestrator._planned_resume_pending = False
                self.strategy.executor_orchestrator._resume_preflight_failed = True
                self.strategy.executor_orchestrator._planned_resume_failure = 'preflight rejected'
                return True

            self.strategy.executor_orchestrator.resume_planned_positions_if_ready.side_effect = failed_claim
            self.strategy.update_executors_info = MagicMock()
            self.strategy._sync_runtime_state_to_db_if_due = MagicMock()
            self.strategy._write_chart_snapshots_if_due = MagicMock()
            self.strategy._write_runtime_status_snapshot_if_due = MagicMock()
            self.strategy.update_controllers_configs = MagicMock()
            self.strategy.market_data_provider.ready = True
            self.strategy.determine_executor_actions = MagicMock(return_value=[])

            self.strategy.on_tick()

        recorder.store_controller_config.assert_not_called()
        self.assertEqual(self.strategy._controller_configs_pending_persistence, [controller_config])

    def unfinished_claim_orchestrator(self):
        # A real orchestrator instance, so any admission guard it defines is consulted.
        from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
        orchestrator = ExecutorOrchestrator.__new__(ExecutorOrchestrator)
        orchestrator.strategy = self.strategy
        orchestrator._planned_resume_pending = False
        orchestrator._resume_preflight_failed = False
        orchestrator._claimed_resume_unresolved = {'controller_1': {'failed-entry': (self.connector_name, self.trading_pair)}}
        orchestrator.active_executors = {'controller_1': []}
        orchestrator.recovery_required = {'controller_1': ['failed-entry']}
        orchestrator.execute_actions = MagicMock()
        self.strategy.executor_orchestrator = orchestrator
        return orchestrator

    def test_unfinished_claim_and_failed_preflight_never_fence_orders(self):
        # "We never want to block orders": an unfinished resume claim is reported,
        # while direct and executor BUY/SELL on the same pair keep trading.
        from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
        orchestrator = self.unfinished_claim_orchestrator()
        config = OrderExecutorConfig(id='held-exit', controller_id='controller_1', timestamp=1,
                                     connector_name=self.connector_name, trading_pair=self.trading_pair,
                                     side=TradeType.SELL, amount=Decimal('1'), price=Decimal('1'),
                                     execution_strategy=ExecutionStrategy.LIMIT, position_action=PositionAction.CLOSE)
        executor = ExecutorBase(self.strategy, [self.connector_name], config)
        for preflight_failed in (False, True):
            orchestrator._resume_preflight_failed = preflight_failed
            with self.subTest(preflight_failed=preflight_failed), \
                    patch.object(self.strategy, 'buy_with_specific_market', return_value='buy') as buy, \
                    patch.object(self.strategy, 'sell_with_specific_market', return_value='sell') as sell:
                self.assertEqual(self.strategy.buy(self.connector_name, self.trading_pair, Decimal('1'), OrderType.LIMIT), 'buy')
                self.assertEqual(self.strategy.sell(self.connector_name, self.trading_pair, Decimal('1'), OrderType.LIMIT), 'sell')
                self.assertEqual(executor.place_order(self.connector_name, self.trading_pair, OrderType.LIMIT,
                                                      TradeType.SELL, Decimal('1'), position_action=PositionAction.CLOSE), 'sell')
                self.assertEqual(executor.place_order(self.connector_name, self.trading_pair, OrderType.LIMIT,
                                                      TradeType.BUY, Decimal('1')), 'buy')
                self.assertEqual(buy.call_count, 2)
                self.assertEqual(sell.call_count, 2)

    def test_unfinished_claim_and_failed_preflight_are_visible_in_native_status(self):
        orchestrator = self.unfinished_claim_orchestrator()
        observation = self.strategy._runtime_position_resume_observation()
        self.assertEqual(observation['unfinished_claim_controllers'], ['controller_1'])
        status = self.strategy.format_status()
        self.assertIn('POSITION OWNERSHIP RECONCILIATION REQUIRED', status)
        self.assertIn('Orders are never fenced', status)
        orchestrator._resume_preflight_failed = True
        observation = self.strategy._runtime_position_resume_observation()
        self.assertTrue(observation['preflight_failed'])
        self.assertIsNone(self.strategy._runtime_reconciliation_observation()['pending_count'])

    def test_unattended_v2_restart_hold_blocks_direct_buy_and_sell(self):
        self.strategy._unattended_restart_hold = True
        with patch.object(self.strategy, "buy_with_specific_market") as buy, \
                patch.object(self.strategy, "sell_with_specific_market") as sell:
            with self.assertRaisesRegex(RuntimeError, "unattended restart hold"):
                self.strategy.buy(self.connector_name, self.trading_pair, Decimal("1"), OrderType.LIMIT)
            with self.assertRaisesRegex(RuntimeError, "unattended restart hold"):
                self.strategy.sell(self.connector_name, self.trading_pair, Decimal("1"), OrderType.LIMIT)
        buy.assert_not_called()
        sell.assert_not_called()

    def test_unattended_v2_restart_hold_does_not_resume_or_dispatch_on_tick(self):
        self.strategy._unattended_restart_hold = True
        self.strategy.executor_orchestrator.resume_planned_positions_if_ready = MagicMock(return_value=True)
        self.strategy.determine_executor_actions = MagicMock()
        self.strategy._write_runtime_status_snapshot_if_due = MagicMock()

        self.strategy.on_tick()

        self.strategy.executor_orchestrator.resume_planned_positions_if_ready.assert_not_called()
        self.strategy.determine_executor_actions.assert_not_called()
        self.strategy._write_runtime_status_snapshot_if_due.assert_called_once()

    def test_unattended_v2_restart_hold_reports_while_connector_is_unready(self):
        self.strategy._unattended_restart_hold = True
        self.strategy.ready_to_trade = False
        self.strategy._write_runtime_status_snapshot_if_due = MagicMock()
        with patch.object(self.strategy, 'connectors', {'unready': MagicMock(ready=False, name='mock')}):
            self.strategy.tick(self.start_timestamp)
        self.strategy._write_runtime_status_snapshot_if_due.assert_called_once()

    def test_unattended_v2_restart_hold_persists_status_when_market_is_unready(self):
        self.strategy._unattended_restart_hold = True
        self.strategy.ready_to_trade = False
        self.strategy._controller_reports_valid = False
        self.strategy._last_runtime_status_snapshot_ts = 0
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(self.strategy, '_runtime_status_snapshot_enabled', return_value=True), \
                patch.object(self.strategy, '_runtime_status_snapshot_dir', return_value=Path(tmp)), \
                patch.object(self.strategy, '_build_runtime_status_snapshot',
                             return_value={'bot_name': 'rsi_modular_v2', 'unattended_restart_hold': True}), \
                patch.object(type(self.strategy), 'current_timestamp', new_callable=PropertyMock, return_value=100):
            self.strategy._write_runtime_status_snapshot_if_due()
            snapshot = json.loads((Path(tmp) / 'rsi_modular_v2.status.json').read_text())
        self.assertTrue(snapshot['unattended_restart_hold'])

    async def test_unattended_v2_restart_hold_discards_queued_actions(self):
        action = CreateExecutorAction(controller_id="controller_1",
                                      executor_config=self.get_position_config_market_short())
        self.strategy._unattended_restart_hold = True
        self.strategy.actions_queue = MagicMock()
        self.strategy.actions_queue.get = AsyncMock(side_effect=[[action], asyncio.CancelledError])
        self.strategy.executor_orchestrator.execute_actions = MagicMock()
        self.strategy.update_executors_info = MagicMock()

        with self.assertRaises(asyncio.CancelledError):
            await self.strategy.listen_to_executor_actions()

        self.strategy.executor_orchestrator.execute_actions.assert_not_called()
        self.strategy.update_executors_info.assert_not_called()

    async def test_start(self):
        self.assertFalse(self.strategy.ready_to_trade)
        self.strategy.start(Clock(ClockMode.BACKTEST), self.start_timestamp)
        self.strategy.tick(self.start_timestamp + 10)
        self.assertTrue(self.strategy.ready_to_trade)

    def test_init_markets(self):
        StrategyV2Base.init_markets(self.strategy_config)
        # With no controllers configured, markets should be empty
        self.assertEqual(StrategyV2Base.markets, {})

    def test_store_actions_proposal(self):
        # Setup test executors with all required fields
        executor_1 = ExecutorInfo(
            id="1",
            controller_id="controller_1",
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            timestamp=10,
            config=PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                          connector_name="binance",
                                          side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1")),
            net_pnl_pct=Decimal(0),
            net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0),
            filled_amount_quote=Decimal(0),
            is_active=False,
            is_trading=False,
            custom_info={}
        )
        executor_2 = ExecutorInfo(
            id="2",
            controller_id="controller_2",
            type="position_executor",
            status=RunnableStatus.RUNNING,
            timestamp=20,
            config=PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                          connector_name="binance",
                                          side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1")),
            net_pnl_pct=Decimal(0),
            net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0),
            filled_amount_quote=Decimal(0),
            is_active=True,
            is_trading=True,
            custom_info={}
        )
        self.strategy.executor_orchestrator.active_executors = {
            "controller_1": [SimpleNamespace(executor_info=executor_1)],
            "controller_2": [SimpleNamespace(executor_info=executor_2)],
        }
        self.strategy.closed_executors_buffer = 0

        actions = self.strategy.store_actions_proposal()
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].executor_id, "1")

    def test_store_actions_proposal_ignores_stored_history(self):
        stored_executor = ExecutorInfo(
            id="stored-db-executor",
            controller_id="controller_1",
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            timestamp=10,
            config=PositionExecutorConfig(id="stored-db-executor", timestamp=1234567890, trading_pair="ETH-USDT",
                                          connector_name="binance",
                                          side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1")),
            net_pnl_pct=Decimal(0),
            net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0),
            filled_amount_quote=Decimal(0),
            is_active=False,
            is_trading=False,
            custom_info={"persistence_source": "db"},
        )
        self.strategy.controller_reports = {
            "controller_1": {"executors": [stored_executor], "positions": [], "performance": None},
        }
        self.strategy.executor_orchestrator.active_executors = {"controller_1": []}
        self.strategy.closed_executors_buffer = 0

        actions = self.strategy.store_actions_proposal()
        self.assertEqual(actions, [])

    def test_get_executors_by_controller(self):
        # Set up controller_reports with the new structure
        self.strategy.controller_reports = {
            "controller_1": {"executors": [MagicMock(), MagicMock()], "positions": [], "performance": None},
            "controller_2": {"executors": [MagicMock()], "positions": [], "performance": None}
        }

        executors = self.strategy.get_executors_by_controller("controller_1")
        self.assertEqual(len(executors), 2)

    def test_get_all_executors(self):
        # Set up controller_reports with the new structure
        self.strategy.controller_reports = {
            "controller_1": {"executors": [MagicMock(), MagicMock()], "positions": [], "performance": None},
            "controller_2": {"executors": [MagicMock()], "positions": [], "performance": None}
        }

        executors = self.strategy.get_all_executors()
        self.assertEqual(len(executors), 3)

    def test_get_active_executors(self):
        active_executor_1 = MagicMock()
        active_executor_2 = MagicMock()
        self.strategy.executor_orchestrator.active_executors = {
            "controller_1": [SimpleNamespace(executor_info=active_executor_1)],
            "controller_2": [SimpleNamespace(executor_info=active_executor_2)],
        }

        executors = self.strategy.get_active_executors()
        self.assertEqual(executors, [active_executor_1, active_executor_2])

    def test_set_leverage(self):
        mock_connector = MagicMock()
        self.strategy.connectors = {"mock": mock_connector}
        self.strategy.set_leverage("mock", "HBOT-USDT", 2)
        mock_connector.set_leverage.assert_called_with("HBOT-USDT", 2)

    def test_set_position_mode(self):
        mock_connector = MagicMock()
        self.strategy.connectors = {"mock": mock_connector}
        self.strategy.set_position_mode("mock", PositionMode.HEDGE)
        mock_connector.set_position_mode.assert_called_with(PositionMode.HEDGE)

    def test_filter_executors(self):
        executors = [MagicMock(status=RunnableStatus.RUNNING), MagicMock(status=RunnableStatus.TERMINATED)]
        # Use existing strategy from setUp instead of creating a new one
        filtered = self.strategy.filter_executors(executors, filter_func=lambda x: x.status == RunnableStatus.RUNNING)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].status, RunnableStatus.RUNNING)

    def test_is_perpetual(self):
        self.assertTrue(StrategyV2Base.is_perpetual("binance_perpetual"))
        self.assertFalse(StrategyV2Base.is_perpetual("binance"))

    @patch.object(StrategyV2Base, "create_actions_proposal", return_value=[])
    @patch.object(StrategyV2Base, "stop_actions_proposal", return_value=[])
    @patch.object(StrategyV2Base, "store_actions_proposal", return_value=[])
    @patch.object(StrategyV2Base, "update_controllers_configs")
    @patch.object(StrategyV2Base, "update_executors_info")
    @patch("hummingbot.data_feed.market_data_provider.MarketDataProvider.ready", new_callable=PropertyMock)
    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.ExecutorOrchestrator.execute_action")
    async def test_on_tick(self, mock_execute_action, mock_ready, mock_update_executors_info,
                           mock_update_controllers_configs,
                           mock_store_actions_proposal, mock_stop_actions_proposal, mock_create_actions_proposal):
        mock_ready.return_value = True
        self.strategy.on_tick()

        # Assertions to ensure that methods are called
        mock_update_executors_info.assert_called_once()
        mock_update_controllers_configs.assert_called_once()

        # Verify that the respective action proposal methods are called
        mock_create_actions_proposal.assert_called_once()
        mock_stop_actions_proposal.assert_called_once()
        mock_store_actions_proposal.assert_called_once()

        # Since no actions are returned, execute_action should not be called
        mock_execute_action.assert_not_called()

    async def test_on_stop(self):
        # Make the executor orchestrator stop method async
        self.strategy.executor_orchestrator.stop = AsyncMock()
        self.strategy.controllers["controller_1"].config = MagicMock()
        self.strategy.controllers["controller_1"].config.early_stop_keep_position = True
        self.strategy.controllers["controller_2"].config = MagicMock()
        self.strategy.controllers["controller_2"].config.early_stop_keep_position = False

        await self.strategy.on_stop()

        # Check if stop methods are called on each component
        self.strategy.executor_orchestrator.stop.assert_called_once_with(
            self.strategy.max_executors_close_attempts,
            keep_position_by_controller={"controller_1": True, "controller_2": False},
        )
        self.strategy.market_data_provider.stop.assert_called_once()

        # Check if stop is called on each controller
        for controller in self.strategy.controllers.values():
            controller.stop.assert_called_once()

    def test_shutdown_without_explicit_preference_keeps_position(self):
        self.strategy.controllers["controller_1"].config = MagicMock(spec=[])
        self.assertTrue(self.strategy._shutdown_keep_position_preferences()["controller_1"])

    def test_parse_markets_str_valid(self):
        test_input = "binance.JASMY-USDT,RLC-USDT:kucoin.BTC-USDT"
        expected_output = {
            "binance": {"JASMY-USDT", "RLC-USDT"},
            "kucoin": {"BTC-USDT"}
        }
        result = StrategyV2ConfigBase.parse_markets_str(test_input)
        self.assertEqual(result, expected_output)

    def test_parse_markets_str_invalid(self):
        test_input = "invalid format"
        with self.assertRaises(ValueError):
            StrategyV2ConfigBase.parse_markets_str(test_input)

    def test_parse_candles_config_str_valid(self):
        test_input = "binance.JASMY-USDT.1m.500:kucoin.BTC-USDT.5m.200"
        result = StrategyV2ConfigBase.parse_candles_config_str(test_input)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].connector, "binance")
        self.assertEqual(result[0].trading_pair, "JASMY-USDT")
        self.assertEqual(result[0].interval, "1m")
        self.assertEqual(result[0].max_records, 500)

    def test_parse_candles_config_str_invalid_format(self):
        test_input = "invalid.format"
        with self.assertRaises(ValueError):
            StrategyV2ConfigBase.parse_candles_config_str(test_input)

    def test_parse_candles_config_str_invalid_max_records(self):
        test_input = "binance.JASMY-USDT.1m.invalid"
        with self.assertRaises(ValueError):
            StrategyV2ConfigBase.parse_candles_config_str(test_input)

    def test_load_controller_module_reloads_when_source_changes(self):
        mock_module = types.ModuleType("test_controller_module")
        mock_module.__file__ = "/tmp/rsi_v5.py"

        with patch.object(strategy_v2_base_module.importlib, "import_module", return_value=mock_module):
            with patch.object(strategy_v2_base_module.importlib, "reload", return_value=mock_module) as mock_reload:
                with patch.object(strategy_v2_base_module.inspect, "getsourcefile", return_value="/tmp/rsi_v5.py"):
                    with patch.object(strategy_v2_base_module.os.path, "exists", return_value=True):
                        with patch.object(strategy_v2_base_module.os.path, "getmtime", side_effect=[10, 20]):
                            self.strategy_config._load_controller_module("controllers.directional_trading.rsi_v5")
                            self.strategy_config._load_controller_module(
                                "controllers.directional_trading.rsi_v5",
                                reload_changed_modules=True,
                            )

        mock_reload.assert_called_once_with(mock_module)

    def test_update_controllers_configs_contains_reload_errors(self):
        mock_logger = MagicMock()
        self.strategy.config_update_interval = 0

        with patch.object(StrategyV2Base, "logger", return_value=mock_logger):
            with patch.object(StrategyV2ConfigBase, "load_controller_configs", side_effect=ValueError("boom")):
                with patch.object(type(self.strategy), "current_timestamp", new_callable=PropertyMock) as mock_timestamp:
                    mock_timestamp.return_value = 100
                    self.strategy.update_controllers_configs()
                    self.strategy.update_controllers_configs()

        self.assertEqual(self.strategy._last_controller_reload_error, "ValueError: boom")
        mock_logger.error.assert_called_once()

    def test_update_controllers_configs_replaces_controller_when_class_changes(self):
        self.strategy.config_update_interval = 0
        self.strategy._controllers_started = True
        current_controller = DummyReloadControllerV1(
            config=MagicMock(id="controller_1"),
            market_data_provider=self.strategy.market_data_provider,
            actions_queue=self.strategy.actions_queue,
        )
        current_controller.executors_info = [MagicMock(name="executor")]
        current_controller.positions_held = [MagicMock(name="position")]
        current_controller.performance_report = MagicMock(name="performance")
        current_controller.processed_data = {"signal": 1, "note": "existing"}
        self.strategy.controllers = {"controller_1": current_controller}

        replacement_config = MagicMock()
        replacement_config.id = "controller_1"
        replacement_config.get_controller_class.return_value = DummyReloadControllerV2

        with patch.object(StrategyV2ConfigBase, "load_controller_configs", return_value=[replacement_config]):
            with patch.object(type(self.strategy), "current_timestamp", new_callable=PropertyMock) as mock_timestamp:
                mock_timestamp.return_value = 100
                self.strategy.update_controllers_configs()

        replacement_controller = self.strategy.controllers["controller_1"]
        self.assertIsInstance(replacement_controller, DummyReloadControllerV2)
        self.assertEqual(current_controller.stop_calls, 1)
        self.assertEqual(replacement_controller.start_calls, 1)
        self.assertEqual(replacement_controller.executors_info, current_controller.executors_info)
        self.assertEqual(replacement_controller.positions_held, current_controller.positions_held)
        self.assertEqual(replacement_controller.performance_report, current_controller.performance_report)
        self.assertEqual(replacement_controller.processed_data, current_controller.processed_data)

    def test_source_change_latches_admission_block_and_preserves_controller(self):
        from hummingbot.strategy.strategy_v2_base import ControllerImplementationChanged
        current = DummyReloadControllerV1(
            config=MagicMock(id='controller_1'),
            market_data_provider=self.strategy.market_data_provider,
            actions_queue=self.strategy.actions_queue)
        self.strategy.controllers = {'controller_1': current}
        self.strategy.config_update_interval = 0
        error = ControllerImplementationChanged(current.__class__.__module__)
        with patch.object(StrategyV2ConfigBase, 'load_controller_configs', side_effect=error), \
                patch.object(type(self.strategy), 'current_timestamp', new_callable=PropertyMock, return_value=100):
            self.strategy.update_controllers_configs()
        assert self.strategy.controllers['controller_1'] is current
        assert current.stop_calls == 0
        assert not self.strategy.validate_create_action(SimpleNamespace(controller_id='controller_1'))
        assert self.strategy.validate_create_action(SimpleNamespace(controller_id='other'))
        assert 'RESTART REQUIRED' in self.strategy.format_status()
        assert not self.strategy._is_stop_triggered
        with patch.object(StrategyV2ConfigBase, 'load_controller_configs', return_value=[]), \
                patch.object(type(self.strategy), 'current_timestamp', new_callable=PropertyMock, return_value=101):
            self.strategy.update_controllers_configs()
        assert not self.strategy.validate_create_action(SimpleNamespace(controller_id='controller_1'))

    def test_zero_spot_base_is_a_sell_note_not_a_blanket_order_failure(self):
        self.strategy.markets = {self.connector_name: {self.trading_pair}}
        self.strategy.ready_to_trade = True
        self.strategy.controllers = {}
        self.connector.set_balance('HBOT', Decimal('0'))
        self.connector.set_balance('USDT', Decimal('1000'))
        market = MarketTradingPairTuple(self.connector, self.trading_pair, 'HBOT', 'USDT')
        with patch.object(self.strategy, 'get_market_trading_pair_tuples', return_value=[market]):
            status = self.strategy.format_status()
        assert 'Cannot place order' not in status
        assert 'BALANCE NOTES' in status
        assert 'no available HBOT to sell' in status
        assert 'buys require USDT' in status

    def test_status_surfaces_stale_conversion_prices(self):
        self.strategy.markets = {self.connector_name: {self.trading_pair}}
        self.strategy.ready_to_trade = True
        self.strategy.controllers = {}
        self.strategy.market_data_provider.get_rate_health.return_value = [
            {'connector': 'okx', 'trading_pair': 'BTC-USDT', 'state': 'stale',
             'age_seconds': 180, 'error': 'TimeoutError', 'consecutive_failures': 3}]
        status = self.strategy.format_status()
        assert 'BTC-USDT' in status
        assert 'STALE' in status
        assert '180s' in status

    async def test_source_restart_blocks_queued_creates_but_preserves_stop_actions(self):
        from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction
        create = CreateExecutorAction(controller_id='controller_1', executor_config=self.get_position_config_market_short())
        stop = StopExecutorAction(controller_id='controller_1', executor_id='existing', keep_position=True)
        self.strategy._controller_restart_required = {'controller_1': 'source changed'}
        self.strategy.actions_queue = MagicMock()
        self.strategy.actions_queue.get = AsyncMock(side_effect=[[create, stop], asyncio.CancelledError])
        try:
            await self.strategy.listen_to_executor_actions()
        except asyncio.CancelledError:
            pass
        self.strategy.executor_orchestrator.execute_actions.assert_called_once_with([stop])

    async def test_unfinished_claim_never_skips_queued_creates(self):
        from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction
        create = CreateExecutorAction(controller_id='controller_1', executor_config=self.get_position_config_market_short())
        stop = StopExecutorAction(controller_id='controller_1', executor_id='proven-entry', keep_position=True)
        self.unfinished_claim_orchestrator()._resume_preflight_failed = True
        self.strategy.update_executors_info = MagicMock()
        self.strategy.validate_create_action = MagicMock(return_value=True)
        self.strategy._is_stop_triggered = False
        self.strategy._controller_reports_valid = True
        self.strategy.controllers['controller_1'] = MagicMock(status=RunnableStatus.RUNNING)
        self.strategy.controllers['controller_1'].config.manual_kill_switch = False
        self.strategy.actions_queue = MagicMock()
        self.strategy.actions_queue.get = AsyncMock(side_effect=[[create, stop], asyncio.CancelledError])
        try:
            await self.strategy.listen_to_executor_actions()
        except asyncio.CancelledError:
            pass
        self.assertEqual(self.strategy.executor_orchestrator.execute_actions.call_args_list, [call([create]), call([stop])])

    def create_mock_executor_config(self):
        return MagicMock(
            timestamp=1234567890,
            trading_pair="ETH-USDT",
            connector_name="binance",
            side="BUY",
            entry_price=Decimal("100"),
            amount=Decimal("1"),
            other_required_field=MagicMock()  # Add other fields as required by specific executor config
        )

    def test_executors_info_to_df(self):
        executor_1 = ExecutorInfo(
            id="1",
            controller_id="controller_1",
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            timestamp=10,
            config=PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                          connector_name="binance",
                                          side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1")),
            net_pnl_pct=Decimal(0),
            net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0),
            filled_amount_quote=Decimal(0),
            is_active=False,
            is_trading=False,
            custom_info={}
        )
        executor_2 = ExecutorInfo(
            id="2",
            controller_id="controller_2",
            type="position_executor",
            status=RunnableStatus.RUNNING,
            timestamp=20,
            config=PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                          connector_name="binance",
                                          side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1")),
            net_pnl_pct=Decimal(0),
            net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0),
            filled_amount_quote=Decimal(0),
            is_active=True,
            is_trading=True,
            custom_info={
                "level_id": "entry_1",
                "current_retries": 1,
                "max_retries": 3,
                "order_ids": ["OID-1", "OID-2"],
                "held_position_orders": [{"order_id": "HP-1"}],
                "current_position_average_price": Decimal("101"),
                "trailing_state": "armed",
                "trailing_move_count": 1,
                "trailing_activation_price": Decimal("104.7"),
                "trailing_trigger_price": Decimal("102.3"),
                "distance_to_trailing_trigger_pct": Decimal("0.018"),
            }
        )

        executors_info = [executor_1, executor_2]
        df = StrategyV2Base.executors_info_to_df(executors_info)

        # Assertions to validate the DataFrame structure and content
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertTrue({
            "id",
            "timestamp",
            "type",
            "status",
            "config",
            "net_pnl_pct",
            "net_pnl_quote",
            "cum_fees_quote",
            "filled_amount_quote",
            "is_active",
            "is_trading",
            "custom_info",
            "close_timestamp",
            "close_type",
            "controller_id",
            "side",
            "level",
            "retries",
            "order_ref",
            "held_orders",
            "avg_entry",
            "trail",
            "trail_arm",
            "trail_stop",
            "trail_gap",
        }.issubset(df.columns))
        self.assertEqual(df.iloc[0]['id'], '2')  # Since the dataframe is sorted by status
        self.assertEqual(df.iloc[1]['id'], '1')
        self.assertEqual(df.iloc[0]['status'], RunnableStatus.RUNNING)
        self.assertEqual(df.iloc[1]['status'], RunnableStatus.TERMINATED)
        self.assertEqual(df.iloc[0]["level"], "entry_1")
        self.assertEqual(df.iloc[0]["retries"], "1/3")
        self.assertEqual(df.iloc[0]["held_orders"], 1)
        self.assertEqual(df.iloc[0]["avg_entry"], Decimal("101"))
        self.assertEqual(df.iloc[0]["trail"], "armed/1")
        self.assertEqual(df.iloc[0]["trail_arm"], "104.700000")
        self.assertEqual(df.iloc[0]["trail_stop"], "102.300000")
        self.assertEqual(df.iloc[0]["trail_gap"], "1.80%")
        self.assertIn("OID-1", df.iloc[0]["order_ref"])

    def create_mock_performance_report(self):
        return PerformanceReport(
            realized_pnl_quote=Decimal('100'),
            unrealized_pnl_quote=Decimal('50'),
            unrealized_pnl_pct=Decimal('5'),
            realized_pnl_pct=Decimal('10'),
            global_pnl_quote=Decimal('150'),
            global_pnl_pct=Decimal('15'),
            volume_traded=Decimal('1000'),
            close_type_counts={CloseType.TAKE_PROFIT: 10, CloseType.STOP_LOSS: 5}
        )

    def test_format_status(self):
        # Mock dependencies
        self.strategy.ready_to_trade = True
        self.strategy.markets = {"mock_paper_exchange": {"ETH-USDT"}}
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567905)
        controller_mock = MagicMock()
        controller_mock.to_format_status.return_value = ["Mock status for controller"]
        self.strategy.controllers = {"controller_1": controller_mock}

        mock_report_controller_1 = MagicMock()
        mock_report_controller_1.realized_pnl_quote = Decimal("100.00")
        mock_report_controller_1.unrealized_pnl_quote = Decimal("50.00")
        mock_report_controller_1.global_pnl_quote = Decimal("150.00")
        mock_report_controller_1.global_pnl_pct = Decimal("15.00")
        mock_report_controller_1.volume_traded = Decimal("1000.00")
        mock_report_controller_1.close_type_counts = {CloseType.TAKE_PROFIT: 10, CloseType.STOP_LOSS: 5}

        # Mock executor for the table
        mock_executor = ExecutorInfo(
            id="12312", timestamp=1234567890, status=RunnableStatus.TERMINATED,
            config=self.get_position_config_market_short(), net_pnl_pct=Decimal(0), net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0), filled_amount_quote=Decimal(0), is_active=False, is_trading=False,
            custom_info={}, type="position_executor", controller_id="controller_1")

        # Set up controller_reports with the new structure
        self.strategy.controller_reports = {
            "controller_1": {
                "executors": [mock_executor],
                "positions": [],
                "performance": mock_report_controller_1
            }
        }

        # Call format_status
        status = self.strategy.format_status()

        # Assertions
        self.assertIn("Mock status for controller", status)
        self.assertIn("Controller: controller_1", status)
        self.assertIn("$100.00", status)  # Check for performance data in the summary table
        self.assertIn("$50.00", status)
        self.assertIn("$150.00", status)
        self.assert_no_status_separator_blocks(status)

    def test_format_status_includes_controller_summary_price(self):
        self.strategy.ready_to_trade = True
        self.strategy.markets = {"mock_paper_exchange": {"ETH-USDT"}}
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567905)

        controller_mock = MagicMock()
        controller_mock.to_format_status.return_value = ["Mock status for controller"]
        controller_mock.get_status_summary.return_value = {
            "pair": "ETH-USDC",
            "price": "2191.5",
            "timestamp": "2009-02-13 23:30:00 UTC",
            "state": "ENTERING",
            "signal": "BUY",
            "score": "3/3",
            "regime": "HV-down",
            "avg_buy": "2188.0",
            "exposure": "$0/$500 (0%)",
            "execs": "B0 S0 H0",
            "u_pnl": "n/a",
            "rsi": "28.90",
            "rsi_proximity": "28.9/35.0",
            "buy_gauge": "██████████  BUY",
            "sell_gauge": "░░░░░░░░░░    —",
            "gates_mask": "buy:ok",
            "gate": "ready",
            "note": "monitoring",
            "focus": "monitoring",
            "attention_score": 10,
            "plan_mode": "ENTRIES",
            "plan_target": "rsi≤35.00",
            "plan_anchor": "scout $25.00 (35%)",
            "plan_next": "runner $46.00 (65%)",
            "plan_slice": "—",
            "plan_liquidity": "idle $1303.42",
        }
        self.strategy.controllers = {"controller_1": controller_mock}
        self.strategy.controller_reports = {
            "controller_1": {
                "executors": [],
                "positions": [],
                "performance": None,
            }
        }

        status = self.strategy.format_status()

        # Shared operator-console layout: SIGNALS / PLANS / runtime tables.
        self.assertNotIn(f"\n{self.CONTROLLERS_TITLE}\n", status)
        self.assertIn(f"\n{self.SIGNALS_TITLE}\n", status)
        self.assertIn(f"\n{self.PLANS_TITLE}\n", status)
        self.assertNotIn(f"\n{self.INVENTORY_TITLE}\n", status)
        self.assertNotIn("CONTROLLERS SUMMARY", status)
        self.assert_no_status_separator_blocks(status)

        # SIGNALS section owns the per-pair overview plus RSI proximity + gates.
        signals_block = status.split(f"\n{self.SIGNALS_TITLE}\n", 1)[1].split(f"\n{self.PLANS_TITLE}\n", 1)[0]
        self.assertLess(signals_block.index("pair"), signals_block.index("timestamp"))
        self.assertLess(signals_block.index("timestamp"), signals_block.index("price"))
        self.assertLess(signals_block.index("price"), signals_block.index("avg"))
        self.assertIn("ETH-USDC", signals_block)
        self.assertIn("2191.5", signals_block)
        self.assertIn("2009-02-13 23:30:00 UTC", signals_block)
        self.assertIn("2188", signals_block)
        self.assertIn("28.9/35.0", signals_block)
        self.assertIn("buy:ok", signals_block)
        self.assertIn("monitoring", signals_block)

        # PLANS section owns the per-controller exit/entry decision detail and compact inventory scale.
        plans_block = status.split(f"\n{self.PLANS_TITLE}\n", 1)[1]
        self.assertIn("ENTRIES", plans_block)
        self.assertIn("rsi≤35.00", plans_block)
        self.assertIn("scout $25.00 (35%)", plans_block)
        self.assertIn("runner $46.00 (65%)", plans_block)
        self.assertIn("inventory", plans_block)
        self.assertIn("idle $1303.42", plans_block)

        # Legacy columns are gone (no verbose breakeven / u% in any controller-summary table).
        controller_tables = status.split("PERFORMANCE SUMMARY")[0].split("POSITIONS HELD")[0]
        self.assertNotIn("breakeven", controller_tables)
        self.assertNotIn("u%", controller_tables)

    def test_format_status_surfaces_focus_queue_and_active_executor_pnl(self):
        self.strategy.ready_to_trade = True
        self.strategy.markets = {"mock_paper_exchange": {"ETH-USDT"}}
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567905)

        controller_mock = MagicMock()
        controller_mock.to_format_status.return_value = ["Mock status for controller"]
        controller_mock.get_status_summary.return_value = {
            "pair": "ETH-USDC",
            "price": "2191.5",
            "state": "BUILDING",
            "signal": "BUY",
            "score": "3/3",
            "regime": "HV-down",
            "avg_buy": "2188.0",
            "exposure": "$50/$500 (10%)",
            "execs": "B1 S0 H1",
            "u_pnl": "n/a",
            "rsi": "28.90",
            "rsi_proximity": "28.9/35.0",
            "buy_gauge": "██████████  BUY",
            "sell_gauge": "██░░░░░░░░ 24%",
            "gates_mask": "P✗ R✗ V✓",
            "gate": "ready",
            "note": "1 buy active",
            "focus": "1 buy active",
            "attention_score": 90,
        }
        self.strategy.controllers = {"controller_1": controller_mock}

        mock_report_controller_1 = self.create_mock_performance_report()
        active_executor = ExecutorInfo(
            id="exec-active",
            controller_id="controller_1",
            type="position_executor",
            status=RunnableStatus.RUNNING,
            timestamp=1234567890,
            config=self.get_position_config_market_short(),
            net_pnl_pct=Decimal("0.025"),
            net_pnl_quote=Decimal("12.50"),
            cum_fees_quote=Decimal("0"),
            filled_amount_quote=Decimal("250"),
            is_active=True,
            is_trading=True,
            custom_info={},
        )

        self.strategy.controller_reports = {
            "controller_1": {
                "executors": [active_executor],
                "positions": [],
                "performance": mock_report_controller_1,
            }
        }

        status = self.strategy.format_status()

        # Focus information is now a column inside SIGNALS, not a separate
        # standalone "Focus Queue (attention):" block. The dashboard runtime-status
        # parser falls back to the column when the bullet list is absent.
        self.assertNotIn("Focus Queue (attention):", status)
        self.assertIn("1 buy active", status)
        self.assertNotIn("uPnL $50.00 / 5.00%", status)
        self.assertNotIn("gPnL $150.00 / 15.00%", status)
        self.assertIn("ACTIVE EXECUTORS (1)", status)
        self.assertIn("ETH-USDC", status)
        self.assertIn("$12.50 / 2.50%", status)
        self.assertIn("$250.00", status)
        self.assertIn("PERFORMANCE SUMMARY", status)
        self.assertIn("$150.00", status)

    def test_format_status_surfaces_positions_once_for_all_pairs(self):
        self.strategy.ready_to_trade = True
        self.strategy.markets = {"mock_paper_exchange": {"ETH-USDT"}}
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567905)

        controller_1 = MagicMock()
        controller_1.to_format_status.return_value = ["Controller 1 status"]
        controller_1.get_status_summary.return_value = {
            "pair": "ETH-USDC",
            "price": "2191.5",
            "state": "HOLDING",
            "signal": "NEUTRAL",
            "score": "0/3",
            "regime": "HV-down",
            "avg_buy": "2188",
            "exposure": "$25/$500 (5%)",
            "execs": "B0 S0 H1",
            "u_pnl": "$5.00 / 1.25%",
            "u_pnl_pct": "1.25%",
            "gate": "ready",
            "note": "held bags 1",
            "attention_score": 50,
        }
        controller_2 = MagicMock()
        controller_2.to_format_status.return_value = ["Controller 2 status"]
        controller_2.get_status_summary.return_value = {
            "pair": "BTC-USDC",
            "price": "68000",
            "state": "HOLDING",
            "signal": "NEUTRAL",
            "score": "0/3",
            "regime": "HV-down",
            "avg_buy": "67000",
            "exposure": "$20/$500 (4%)",
            "execs": "B0 S0 H1",
            "u_pnl": "$2.00 / 0.50%",
            "u_pnl_pct": "0.50%",
            "gate": "ready",
            "note": "held bags 1",
            "attention_score": 45,
        }
        self.strategy.controllers = {"controller_1": controller_1, "controller_2": controller_2}

        self.strategy.controller_reports = {
            "controller_1": {
                "executors": [],
                "positions": [
                    SimpleNamespace(
                        connector_name="binance",
                        trading_pair="ETH-USDC",
                        side=TradeType.BUY,
                        amount=Decimal("0.0100"),
                        breakeven_price=Decimal("2188"),
                        unrealized_pnl_quote=Decimal("5"),
                        realized_pnl_quote=Decimal("0"),
                        cum_fees_quote=Decimal("0.20"),
                    )
                ],
                "performance": None,
            },
            "controller_2": {
                "executors": [],
                "positions": [
                    SimpleNamespace(
                        connector_name="binance",
                        trading_pair="BTC-USDC",
                        side=TradeType.BUY,
                        amount=Decimal("0.0003"),
                        breakeven_price=Decimal("67000"),
                        unrealized_pnl_quote=Decimal("2"),
                        realized_pnl_quote=Decimal("0"),
                        cum_fees_quote=Decimal("0.10"),
                    )
                ],
                "performance": None,
            },
        }

        status = self.strategy.format_status()

        self.assertIn("POSITIONS HELD (2)", status)
        self.assertEqual(status.count("POSITIONS HELD"), 1)
        positions_block = status.split("POSITIONS HELD (2)", 1)[1].split("\n\nController:", 1)[0]
        self.assertIn("ETH-USDC", positions_block)
        self.assertIn("BTC-USDC", positions_block)
        self.assertIn("breakeven", positions_block)
        self.assertNotIn("controller", positions_block)
        self.assertNotIn("side", positions_block)
        self.assertNotIn("No positions held.", status)

    def test_format_status_keeps_controller_sections_in_stable_pair_order(self):
        self.strategy.ready_to_trade = True
        self.strategy.markets = {"mock_paper_exchange": {"ETH-USDT"}}
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567905)

        controller_1 = MagicMock()
        controller_1.to_format_status.return_value = ["Controller 1 status"]
        controller_1.get_status_summary.return_value = {
            "pair": "ETH-USDC",
            "price": "2191.5",
            "state": "BUILDING",
            "signal": "NEUTRAL",
            "score": "1/2",
            "regime": "HV-up",
            "avg_buy": "2188",
            "exposure": "$100/$500 (20%)",
            "execs": "B1 S0 H0",
            "u_pnl": "$5.00 / 2.50%",
            "u_pnl_pct": "2.50%",
            "gate": "ready",
            "note": "monitoring",
            "attention_score": 10,
        }
        controller_2 = MagicMock()
        controller_2.to_format_status.return_value = ["Controller 2 status"]
        controller_2.get_status_summary.return_value = {
            "pair": "BTC-USDC",
            "price": "74800",
            "state": "EXITING",
            "signal": "SELL",
            "score": "3/2",
            "regime": "HV-down",
            "avg_buy": "71791",
            "exposure": "$400/$500 (80%)",
            "execs": "B0 S1 H1",
            "u_pnl": "$20.00 / 4.00%",
            "u_pnl_pct": "4.00%",
            "gate": "trend-hold",
            "note": "sell active",
            "attention_score": 999,
        }
        self.strategy.controllers = {"controller_1": controller_1, "controller_2": controller_2}
        self.strategy.controller_reports = {
            "controller_1": {
                "executors": [],
                "positions": [],
                "performance": None,
            },
            "controller_2": {
                "executors": [],
                "positions": [],
                "performance": None,
            },
        }

        status = self.strategy.format_status()

        btc_index = status.index("Controller: controller_2 | BTC-USDC | EXITING | SELL")
        eth_index = status.index("Controller: controller_1 | ETH-USDC | BUILDING | NEUTRAL")
        self.assertLess(btc_index, eth_index)

    def test_format_status_handles_controller_formatter_exception(self):
        self.strategy.ready_to_trade = True
        self.strategy.markets = {"mock_paper_exchange": {"ETH-USDT"}}
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567905)
        controller_mock = MagicMock()
        controller_mock.to_format_status.side_effect = RuntimeError("formatter exploded")
        self.strategy.controllers = {"controller_1": controller_mock}

        mock_report_controller_1 = MagicMock()
        mock_report_controller_1.realized_pnl_quote = Decimal("0")
        mock_report_controller_1.unrealized_pnl_quote = Decimal("0")
        mock_report_controller_1.global_pnl_quote = Decimal("0")
        mock_report_controller_1.global_pnl_pct = Decimal("0")
        mock_report_controller_1.volume_traded = Decimal("1")
        mock_report_controller_1.close_type_counts = {}

        mock_executor = ExecutorInfo(
            id="12312", timestamp=1234567890, status=RunnableStatus.RUNNING,
            config=self.get_position_config_market_short(), net_pnl_pct=Decimal(0), net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0), filled_amount_quote=Decimal(0), is_active=True, is_trading=True,
            custom_info={
                "level_id": "entry_2",
                "current_retries": 0,
                "max_retries": 5,
                "order_ids": ["OID-1", "OID-2"],
                "trailing_state": "waiting",
                "trailing_activation_price": Decimal("96.2"),
                "distance_to_trailing_activation_pct": Decimal("0.021"),
            },
            type="position_executor", controller_id="controller_1", close_type=CloseType.EARLY_STOP)

        self.strategy.controller_reports = {
            "controller_1": {
                "executors": [mock_executor],
                "positions": [],
                "performance": mock_report_controller_1
            }
        }

        status = self.strategy.format_status()

        self.assertIn("Status formatter error for `controller_1`", status)
        self.assertIn("Controller-specific status is unavailable", status)
        self.assertIn("ACTIVE EXECUTORS (1)", status)
        self.assertIn("active_exec total=1 | buy=0 sell=1", status)
        self.assertIn("pnl=$0.00 / 0.00%", status)
        self.assertIn("fill=$0", status)
        self.assertIn("trail=waiting:1", status)
        self.assertIn("entry_2", status)
        self.assertIn("2.10%", status)
        self.assertIn("0/5", status)
        self.assertNotIn("🕘 Recent Executors (Last 3):", status)
        self.assertNotIn("🏷 Refs OID-1, OID-2", status)

    def test_format_status_suppresses_empty_per_controller_block(self):
        """Per-controller section disappears when
        ``to_format_status()`` returns ``[]`` and the controller has no active
        executor needing a runtime line — preventing 4 lines of pure noise per
        controller in the operator console (RSIv5Controller is the canonical
        empty-body case post-2026-04-28)."""
        self.strategy.ready_to_trade = True
        self.strategy.markets = {"mock_paper_exchange": {"ETH-USDT"}}
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567905)

        controller_mock = MagicMock()
        controller_mock.to_format_status.return_value = []
        controller_mock.get_status_summary.return_value = {
            "pair": "ETH-USDC",
            "price": "2191.5",
            "state": "HOLDING",
            "signal": "NEUTRAL",
            "score": "0/3",
            "regime": "HV-down",
            "avg_buy": "2188.0",
            "exposure": "$25/$500 (5%)",
            "execs": "B0 S0 H1",
            "u_pnl": "$0.00 / 0.00%",
            "u_pnl_pct": "0.00%",
            "rsi": "50.00",
            "rsi_proximity": "50.0/80.0",
            "buy_gauge": "░░░░░░░░░░    —",
            "sell_gauge": "██░░░░░░░░ 24%",
            "gates_mask": "P✗ R✗ V✗",
            "gate": "ready",
            "note": "sell buffer 5.0%",
            "focus": "sell buffer 5.0%",
            "attention_score": 80,
            "plan_mode": "EXITS",
            "plan_target": "7.00%",
            "plan_anchor": "min 2192.38 (+0.04%)",
            "plan_next": "arm 2341.16 (+6.83%)",
            "plan_slice": "15% ≈ 0.0017 ETH ($3.97)",
            "plan_liquidity": "sellable 0.0114 ETH · idle $0.00",
            "plan_sellable": "0.0114 ETH",
            "plan_held": "$25",
            "plan_active": "$0",
            "plan_untracked": "$0",
            "plan_free": "$0",
            "plan_managed": "$25",
        }
        self.strategy.controllers = {"controller_1": controller_mock}
        self.strategy.controller_reports = {
            "controller_1": {
                "executors": [],
                "positions": [],
                "performance": None,
            }
        }

        status = self.strategy.format_status()

        self.assertNotIn("Controller: controller_1", status)
        self.assertNotIn(f"\n{self.CONTROLLERS_TITLE}\n", status)
        self.assertIn(f"\n{self.SIGNALS_TITLE}\n", status)
        self.assertIn(f"\n{self.PLANS_TITLE}\n", status)
        self.assertNotIn(f"\n{self.INVENTORY_TITLE}\n", status)
        self.assert_no_status_separator_blocks(status)
        # Plan and inventory information live in the parent PLANS table, not in a per-controller block.
        plans_block = status.split(f"\n{self.PLANS_TITLE}\n", 1)[1]
        self.assertIn("EXITS", plans_block)
        self.assertIn("min 2192.38 (+0.04%)", plans_block)
        self.assertIn("arm 2341.16 (+6.83%)", plans_block)
        self.assertIn("15% ≈ 0.0017 ETH ($3.97)", plans_block)
        self.assertIn("inventory", plans_block)
        self.assertIn("sell 0.0114 ETH", plans_block)
        self.assertIn("bag $25", plans_block)
        self.assertNotIn("sellable", plans_block)
        self.assertNotIn("active buys$", plans_block)
        self.assertNotIn("frees$", plans_block)
        self.assertNotIn("managed$", plans_block)

    async def test_queued_create_is_rejected_after_controller_kill(self):
        action = CreateExecutorAction(controller_id="killed", executor_config=self.get_position_config_market_short())
        self.strategy.actions_queue = MagicMock()
        self.strategy.actions_queue.get = AsyncMock(side_effect=[[action], asyncio.CancelledError])
        killed = MagicMock()
        killed.status = RunnableStatus.TERMINATED
        killed.config.manual_kill_switch = True
        self.strategy.controllers = {"killed": killed}
        try:
            await self.strategy.listen_to_executor_actions()
        except asyncio.CancelledError:
            pass
        self.strategy.executor_orchestrator.execute_actions.assert_not_called()

    async def test_queued_actions_do_not_read_reports_while_planned_resume_is_pending(self):
        action = CreateExecutorAction(controller_id="controller_1",
                                      executor_config=self.get_position_config_market_short())
        self.strategy.actions_queue = MagicMock()
        self.strategy.actions_queue.get = AsyncMock(side_effect=[[action], asyncio.CancelledError])
        self.strategy.executor_orchestrator._planned_resume_pending = True
        self.strategy.executor_orchestrator.execute_actions = MagicMock()
        self.strategy.update_executors_info = MagicMock()

        try:
            await self.strategy.listen_to_executor_actions()
        except asyncio.CancelledError:
            pass

        self.strategy.update_executors_info.assert_not_called()
        self.strategy.executor_orchestrator.execute_actions.assert_not_called()

    async def test_listen_to_executor_actions(self):
        self.strategy.actions_queue = MagicMock()
        # Simulate some actions being returned, followed by an exception to break the loop.
        self.strategy.actions_queue.get = AsyncMock(side_effect=[
            [CreateExecutorAction(controller_id="controller_1",
                                  executor_config=self.get_position_config_market_short())],
            Exception,
            asyncio.CancelledError,
        ])
        self.strategy.executor_orchestrator.execute_actions = MagicMock()
        controller_mock = MagicMock()
        controller_mock.status = RunnableStatus.RUNNING
        controller_mock.config.manual_kill_switch = False
        self.strategy.controllers = {"controller_1": controller_mock}

        # Test for exception handling inside the method.
        try:
            await self.strategy.listen_to_executor_actions()
        except asyncio.CancelledError:
            pass

        # Check assertions here to verify the actions were handled as expected.
        self.assertEqual(self.strategy.executor_orchestrator.execute_actions.call_count, 1)

    def get_position_config_market_short(self):
        return PositionExecutorConfig(id="test-2", timestamp=1234567890, trading_pair="ETH-USDT",
                                      connector_name="binance",
                                      side=TradeType.SELL, entry_price=Decimal("100"), amount=Decimal("1"),
                                      triple_barrier_config=TripleBarrierConfig())


class StrategyV2BaseBasicTest(unittest.TestCase):
    """Legacy tests for basic StrategyV2Base functionality"""
    level = 0

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage().startswith(message)
                   for record in self.log_records)

    def setUp(self):
        application_patch = patch("hummingbot.client.hummingbot_application.HummingbotApplication.main_application",
                                  return_value=MagicMock(_mqtt=None))
        application_patch.start()
        self.addCleanup(application_patch.stop)
        self.log_records = []
        self.start: pd.Timestamp = pd.Timestamp("2019-01-01", tz="UTC")
        self.end: pd.Timestamp = pd.Timestamp("2019-01-01 01:00:00", tz="UTC")
        self.start_timestamp: float = self.start.timestamp()
        self.end_timestamp: float = self.end.timestamp()
        self.connector_name: str = "mock_paper_exchange"
        self.trading_pair: str = "HBOT-USDT"
        self.base_asset, self.quote_asset = self.trading_pair.split("-")
        self.base_balance: int = 500
        self.quote_balance: int = 5000
        self.initial_mid_price: int = 100
        self.clock_tick_size = 1
        self.clock: Clock = Clock(ClockMode.BACKTEST, self.clock_tick_size, self.start_timestamp, self.end_timestamp)
        self.connector: MockPaperExchange = MockPaperExchange()
        self.connector.set_balanced_order_book(trading_pair=self.trading_pair,
                                               mid_price=100,
                                               min_price=50,
                                               max_price=150,
                                               price_step_size=1,
                                               volume_step_size=10)
        self.connector.set_balance(self.base_asset, self.base_balance)
        self.connector.set_balance(self.quote_asset, self.quote_balance)
        self.connector.set_quantization_param(
            QuantizationParams(
                self.trading_pair, 6, 6, 6, 6
            )
        )
        self.clock.add_iterator(self.connector)
        StrategyV2Base.markets = {self.connector_name: {self.trading_pair}}
        with patch('asyncio.create_task', side_effect=lambda coro: (coro.close(), MagicMock())[1]):
            with patch("hummingbot.strategy.strategy_v2_base.StrategyV2Base.listen_to_executor_actions", return_value=AsyncMock()):
                with patch('hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator'):
                    with patch('hummingbot.strategy.strategy_v2_base.MarketDataProvider'):
                        self.strategy = StrategyV2Base({self.connector_name: self.connector})
        self.strategy.logger().setLevel(1)
        self.strategy.logger().addHandler(self)

    def test_start_basic(self):
        self.assertFalse(self.strategy.ready_to_trade)
        self.strategy.start(Clock(ClockMode.BACKTEST), self.start_timestamp)
        self.strategy.tick(self.start_timestamp + 10)
        self.assertTrue(self.strategy.ready_to_trade)

    def test_get_assets_basic(self):
        self.strategy.markets = {"con_a": {"HBOT-USDT", "BTC-USDT"}, "con_b": {"HBOT-BTC", "HBOT-ETH"}}
        self.assertRaises(KeyError, self.strategy.get_assets, "con_c")
        assets = self.strategy.get_assets("con_a")
        self.assertEqual(3, len(assets))
        self.assertEqual("BTC", assets[0])
        self.assertEqual("HBOT", assets[1])
        self.assertEqual("USDT", assets[2])

        assets = self.strategy.get_assets("con_b")
        self.assertEqual(3, len(assets))
        self.assertEqual("BTC", assets[0])
        self.assertEqual("ETH", assets[1])
        self.assertEqual("HBOT", assets[2])

    def test_get_market_trading_pair_tuples_basic(self):
        market_infos: List[MarketTradingPairTuple] = self.strategy.get_market_trading_pair_tuples()
        self.assertEqual(1, len(market_infos))
        market_info = market_infos[0]
        self.assertEqual(market_info.market, self.connector)
        self.assertEqual(market_info.trading_pair, self.trading_pair)
        self.assertEqual(market_info.base_asset, self.base_asset)
        self.assertEqual(market_info.quote_asset, self.quote_asset)

    def test_active_orders_basic(self):
        self.clock.add_iterator(self.strategy)
        self.clock.backtest_til(self.start_timestamp + self.clock_tick_size)
        self.strategy.buy(self.connector_name, self.trading_pair, Decimal("1"), OrderType.LIMIT, Decimal("90"))
        self.strategy.sell(self.connector_name, self.trading_pair, Decimal("1.1"), OrderType.LIMIT, Decimal("110"))
        orders = self.strategy.get_active_orders(self.connector_name)
        self.assertEqual(2, len(orders))
        self.assertTrue(orders[0].is_buy)
        self.assertEqual(Decimal("1"), orders[0].quantity)
        self.assertEqual(Decimal("90"), orders[0].price)
        self.assertFalse(orders[1].is_buy)
        self.assertEqual(Decimal("1.1"), orders[1].quantity)
        self.assertEqual(Decimal("110"), orders[1].price)

    def test_format_status_basic(self):
        self.clock.add_iterator(self.strategy)
        self.clock.backtest_til(self.start_timestamp + self.clock_tick_size)
        self.strategy.buy(self.connector_name, self.trading_pair, Decimal("1"), OrderType.LIMIT, Decimal("90"))
        self.strategy.sell(self.connector_name, self.trading_pair, Decimal("1.1"), OrderType.LIMIT, Decimal("110"))
        status = self.strategy.format_status()

        self.assertIn("Balances:", status)
        self.assertIn("Value (USD)", status)
        self.assertIn("mock_paper_exchange  HBOT", status)
        self.assertIn("mock_paper_exchange  USDT", status)
        self.assertIn("mock_paper_exchange HBOT-USDT  buy     90       1", status)
        self.assertIn("mock_paper_exchange HBOT-USDT sell    110     1.1 ", status)

    @patch("hummingbot.strategy.strategy_v2_base.RateOracle.get_instance")
    def test_get_balance_df_includes_usd_value_column(self, rate_oracle_get_instance):
        mock_oracle = MagicMock()
        mock_oracle.quote_token = "USD"
        mock_oracle.get_pair_rate.side_effect = lambda pair: {
            "HBOT-USD": Decimal("2"),
            "USDT-USD": Decimal("1"),
        }.get(pair)
        rate_oracle_get_instance.return_value = mock_oracle

        df = self.strategy.get_balance_df()

        self.assertEqual(
            ["Exchange", "Asset", "Total Balance", "Available Balance", "Value (USD)"],
            list(df.columns),
        )
        hbot_row = df[df["Asset"] == "HBOT"].iloc[0]
        usdt_row = df[df["Asset"] == "USDT"].iloc[0]
        self.assertEqual(1000.0, hbot_row["Value (USD)"])
        self.assertEqual(5000.0, usdt_row["Value (USD)"])

    def test_cancel_buy_order_basic(self):
        self.clock.add_iterator(self.strategy)
        self.clock.backtest_til(self.start_timestamp)

        order_id = self.strategy.buy(
            connector_name=self.connector_name,
            trading_pair=self.trading_pair,
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
            price=Decimal("1000"),
        )

        self.assertIn(order_id,
                      [order.client_order_id for order in self.strategy.get_active_orders(self.connector_name)])

        self.strategy.cancel(
            connector_name=self.connector_name,
            trading_pair=self.trading_pair,
            order_id=order_id
        )

        self.assertTrue(
            self._is_logged(
                log_level="INFO",
                message=f"({self.trading_pair}) Canceling the limit order {order_id}."
            )
        )
