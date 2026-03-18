import asyncio
import types
import unittest
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import List
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pandas as pd

import hummingbot.strategy.strategy_v2_base as strategy_v2_base_module
from hummingbot.connector.exchange.paper_trade.paper_trade_exchange import QuantizationParams
from hummingbot.connector.test_support.mock_paper_exchange import MockPaperExchange
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.common import PositionMode, TradeType
from hummingbot.core.event.events import OrderType
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
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
    def setUp(self):
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
        with patch('asyncio.create_task', return_value=MagicMock()):
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
        # Set up controller_reports with the new structure
        self.strategy.controller_reports = {
            "controller_1": {"executors": [executor_1], "positions": [], "performance": None},
            "controller_2": {"executors": [executor_2], "positions": [], "performance": None}
        }
        self.strategy.closed_executors_buffer = 0

        actions = self.strategy.store_actions_proposal()
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].executor_id, "1")

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
        self.assertIn("🕘 Recent Executors (Last 3):", status)
        self.assertIn("1. 🔴 SELL position | 🟢 running | 🧯 early-stop", status)
        self.assertIn("💰 PnL 0.00% / $0", status)
        self.assertIn("🎯 Entry 100", status)
        self.assertIn("🪜 Level entry_2", status)
        self.assertIn("🧵 Trail waiting", status)
        self.assertIn("🚀 Arm 96.2", status)
        self.assertIn("↔ Gap 2.10%", status)
        self.assertIn("🏷 Refs OID-1, OID-2", status)

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
        with patch('asyncio.create_task', return_value=MagicMock()):
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
        expected_status = """
  Balances:
               Exchange Asset  Total Balance  Available Balance
    mock_paper_exchange  HBOT            500              498.9
    mock_paper_exchange  USDT           5000               4910

  Orders:
               Exchange    Market Side  Price  Amount      Age
    mock_paper_exchange HBOT-USDT  buy     90       1"""
        self.assertTrue(expected_status in self.strategy.format_status())
        self.assertTrue("mock_paper_exchange HBOT-USDT sell    110     1.1 " in self.strategy.format_status())

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
