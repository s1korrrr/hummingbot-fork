import asyncio
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, PropertyMock, patch

from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import (
    DownOnlyMakerPositionExecutor,
    DownOnlyMakerPositionExecutorConfig,
)
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.model.position import Position
from hummingbot.model.position_hold_ledger import PositionHoldLedger
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor import ArbitrageExecutor
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.dca_executor.data_types import DCAExecutorConfig
from hummingbot.strategy_v2.executors.dca_executor.dca_executor import DCAExecutor
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator, PositionHold
from hummingbot.strategy_v2.executors.grid_executor.data_types import GridExecutorConfig
from hummingbot.strategy_v2.executors.grid_executor.grid_executor import GridExecutor
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.executors.rsi_position_executor import RSIPositionExecutor, RSIPositionExecutorConfig
from hummingbot.strategy_v2.executors.twap_executor.data_types import TWAPExecutorConfig
from hummingbot.strategy_v2.executors.twap_executor.twap_executor import TWAPExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction, StoreExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo, PerformanceReport


class TestExecutorOrchestrator(unittest.TestCase):

    def setUp(self):
        recorder_patch = patch.object(MarketsRecorder, "get_instance")
        markets_recorder = recorder_patch.start()
        self.addCleanup(recorder_patch.stop)
        markets_recorder.return_value = MagicMock(spec=MarketsRecorder)
        markets_recorder.return_value.get_all_executors = MagicMock(return_value=[])
        markets_recorder.return_value.get_all_positions = MagicMock(return_value=[])
        markets_recorder.return_value.get_all_position_hold_ledger_entries = MagicMock(return_value=[])
        markets_recorder.return_value.get_trades_for_config = MagicMock(return_value=[])
        markets_recorder.return_value.config_file_path = "test.yml"
        markets_recorder.return_value.store_or_update_executor = MagicMock(return_value=None)
        markets_recorder.return_value.store_or_update_position_hold_ledger_entries = MagicMock(return_value=None)
        markets_recorder.return_value.update_or_store_position = MagicMock(return_value=None)
        self.mock_strategy = self.create_mock_strategy()
        self.orchestrator = ExecutorOrchestrator(strategy=self.mock_strategy)

    @staticmethod
    def create_mock_strategy():
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market

        strategy = MagicMock(spec=StrategyV2Base)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="ETH-USDT")
        connector = MagicMock(spec=ExchangePyBase)
        type(connector).trading_rules = PropertyMock(return_value={"ETH-USDT": TradingRule(trading_pair="ETH-USDT")})
        strategy.connectors = {
            "binance": connector,
        }
        strategy.market_data_provider = MagicMock(spec=MarketDataProvider)
        strategy.market_data_provider.get_price_by_type = MagicMock(return_value=Decimal(230))
        # Add the controllers attribute that ExecutorOrchestrator now checks for
        strategy.controllers = {}
        # Add the markets attribute that ExecutorOrchestrator now checks for
        strategy.markets = {"binance": {"ETH-USDT", "BTC-USDT"}}
        return strategy

    @staticmethod
    def create_mock_controller(connector_name: str, trading_pair: str):
        controller = MagicMock()
        controller.config = MagicMock()
        controller.config.connector_name = connector_name
        controller.config.trading_pair = trading_pair
        return controller

    def test_executor_class_override_is_scoped_to_owning_controller(self):
        config = RSIPositionExecutorConfig(
            timestamp=1234,
            connector_name="binance",
            trading_pair="ETH-USDT",
            side=TradeType.BUY,
            entry_price=Decimal("100"),
            amount=Decimal("1"),
        )
        rsi_controller = MagicMock()
        rsi_controller.get_executor_class_for_config.return_value = RSIPositionExecutor
        plain_controller = MagicMock()
        plain_controller.get_executor_class_for_config.return_value = None
        self.mock_strategy.controllers = {
            "rsi": rsi_controller,
            "plain": plain_controller,
        }

        self.assertIs(
            self.orchestrator._resolve_executor_class("rsi", config),
            RSIPositionExecutor,
        )
        self.assertIs(
            self.orchestrator._resolve_executor_class("plain", config),
            PositionExecutor,
        )

    @patch.object(DownOnlyMakerPositionExecutor, "start")
    def test_down_only_executor_persists_before_start_and_duplicate_is_rejected(self, start_mock):
        connector = MagicMock(spec=ExchangePyBase)
        connector.get_available_balance.return_value = Decimal("1000")
        connector.in_flight_orders = {}
        connector.trading_rules = {
            "ETH-USDC": TradingRule(
                "ETH-USDC",
                min_order_size=Decimal("0.01"),
                min_price_increment=Decimal("0.01"),
                min_base_amount_increment=Decimal("0.01"),
                min_notional_size=Decimal("1"),
            )
        }
        self.mock_strategy.connectors["okx"] = connector
        self.mock_strategy.markets["okx"] = {"ETH-USDC"}
        controller = MagicMock()
        controller.validate_executor_config.return_value = True
        controller.get_executor_class_for_config.return_value = DownOnlyMakerPositionExecutor
        self.mock_strategy.controllers["ok-rsi"] = controller
        config = DownOnlyMakerPositionExecutorConfig(
            id="down-only-intent",
            timestamp=1_000,
            controller_id="ok-rsi",
            connector_name="okx",
            trading_pair="ETH-USDC",
            side=TradeType.BUY,
            entry_price=Decimal("100"),
            amount=Decimal("1"),
            leverage=1,
            triple_barrier_config=TripleBarrierConfig(
                open_order_type=OrderType.LIMIT_MAKER,
                open_order_limit_max_age_seconds=0,
            ),
            activation_bounds=[Decimal("0.005")],
            entry_order_policy="single_attempt",
            decision_best_bid=Decimal("100"),
            decision_best_ask=Decimal("100.01"),
            decision_atr_1m=Decimal("1"),
            decision_price_step=Decimal("0.01"),
            maximum_acceptable_price=Decimal("100"),
            entry_total_max_age_seconds=300,
            entry_max_downward_reprices=3,
            entry_reprice_cooldown_seconds=5,
            entry_reprice_min_ticks=3,
            entry_reprice_atr_fraction=Decimal("0.05"),
        )
        action = CreateExecutorAction(controller_id="ok-rsi", executor_config=config)
        self.orchestrator.active_executors["ok-rsi"] = []
        self.orchestrator.stored_executors["ok-rsi"] = []
        self.orchestrator.positions_held["ok-rsi"] = []
        self.orchestrator.cached_performance["ok-rsi"] = PerformanceReport()
        recorder = MarketsRecorder.get_instance()
        calls = MagicMock()
        calls.attach_mock(recorder.store_or_update_executor, "persist")
        calls.attach_mock(start_mock, "start")

        self.orchestrator.create_executor(action)

        self.assertEqual([call[0] for call in calls.mock_calls], ["persist", "start"])
        self.assertIsInstance(self.orchestrator.active_executors["ok-rsi"][0], DownOnlyMakerPositionExecutor)
        with self.assertRaisesRegex(ValueError, "Duplicate executor id"):
            self.orchestrator.create_executor(action)
        self.assertEqual(start_mock.call_count, 1)

    def test_unfinished_down_only_executor_blocks_restart_and_retains_receipt(self):
        config = DownOnlyMakerPositionExecutorConfig(
            id="intent-1",
            timestamp=1_000,
            controller_id="ok-rsi-eth",
            connector_name="okx",
            trading_pair="ETH-USDC",
            side=TradeType.BUY,
            entry_price=Decimal("100"),
            amount=Decimal("1"),
            leverage=1,
            triple_barrier_config=TripleBarrierConfig(
                open_order_type=OrderType.LIMIT_MAKER,
                open_order_limit_max_age_seconds=0,
            ),
            activation_bounds=[Decimal("0.005")],
            entry_order_policy="single_attempt",
            decision_best_bid=Decimal("100"),
            decision_best_ask=Decimal("100.01"),
            decision_atr_1m=Decimal("1"),
            decision_price_step=Decimal("0.01"),
            maximum_acceptable_price=Decimal("100"),
            entry_total_max_age_seconds=300,
            entry_max_downward_reprices=3,
            entry_reprice_cooldown_seconds=5,
            entry_reprice_min_ticks=3,
            entry_reprice_atr_fraction=Decimal("0.05"),
        )
        receipt = {
            "policy": "down_only_maker_v1",
            "intent_id": "intent-1",
            "state": "cancel_pending_reprice",
            "terminal_reason": None,
            "initial_price": "100",
            "current_order_price": "99.90",
            "maximum_acceptable_price": "100",
            "decision_atr_1m": "1",
            "authorized_base": "1",
            "cumulative_filled_base": "0",
            "remaining_base": "1",
            "reprice_count": 1,
            "rest_seconds": 30,
            "total_max_age_seconds": 300,
            "order_ids": ["parent", "child-1"],
            "queue_fill_status": "UNAVAILABLE",
        }
        info = ExecutorInfo(
            id=config.id,
            timestamp=config.timestamp,
            type=config.type,
            status=RunnableStatus.RUNNING,
            config=config,
            net_pnl_pct=Decimal("0"),
            net_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            filled_amount_quote=Decimal("0"),
            is_active=True,
            is_trading=False,
            custom_info={"side": TradeType.BUY, "entry_acquisition": receipt},
            controller_id="ok-rsi-eth",
        )
        recorder = MarketsRecorder.get_instance()
        recorder.get_all_executors.return_value = [info]
        self.mock_strategy.controllers = {"ok-rsi-eth": MagicMock()}
        self.mock_strategy.connectors["okx"] = MagicMock(spec=ExchangePyBase)
        restarted = ExecutorOrchestrator(strategy=self.mock_strategy)

        self.assertEqual(restarted.recovery_required["ok-rsi-eth"], ["intent-1"])
        self.assertEqual(restarted.stored_executors["ok-rsi-eth"], [])
        persisted = recorder.get_all_executors.return_value[0]
        self.assertEqual(persisted.custom_info["entry_acquisition"], receipt)
        with self.assertRaisesRegex(RuntimeError, "startup reconciliation"):
            restarted.create_executor(CreateExecutorAction(controller_id="ok-rsi-eth", executor_config=config))

    def test_unfinished_owner_blocks_new_execution_on_other_controller(self):
        config = PositionExecutorConfig(
            id="new-btc", timestamp=1234, controller_id="safe-btc",
            connector_name="binance", trading_pair="BTC-USDT", side=TradeType.BUY,
            entry_price=Decimal("100"), amount=Decimal("1"),
            triple_barrier_config=TripleBarrierConfig())
        self.orchestrator.recovery_required = {"unresolved-eth": ["old-entry"]}
        self.orchestrator.active_executors["safe-btc"] = []
        type(self.mock_strategy.connectors["binance"]).trading_rules = PropertyMock(return_value={
            "ETH-USDT": TradingRule(trading_pair="ETH-USDT"),
            "BTC-USDT": TradingRule(trading_pair="BTC-USDT"),
        })
        recorder = MarketsRecorder.get_instance()

        with self.assertRaisesRegex(RuntimeError, "startup reconciliation"):
            self.orchestrator.create_executor(CreateExecutorAction(
                controller_id=config.controller_id, executor_config=config))

        recorder.store_or_update_executor.assert_not_called()

    @patch.object(PositionExecutor, "start")
    @patch.object(DCAExecutor, "start")
    @patch.object(ArbitrageExecutor, "start")
    @patch.object(TWAPExecutor, "start")
    @patch.object(GridExecutor, "start")
    @patch.object(GridExecutor, "_generate_grid_levels")
    @patch.object(MarketsRecorder, "get_instance")
    def test_execute_actions_create_executor(self, markets_recorder_mock, grid_start_mock: MagicMock,
                                             generate_grid_levels_mock: MagicMock,
                                             arbitrage_start_mock: MagicMock, dca_start_mock: MagicMock,
                                             position_start_mock: MagicMock, twap_start_mock: MagicMock):
        markets_recorder_mock.return_value = MagicMock(spec=MarketsRecorder)
        markets_recorder_mock.store_or_update_executor = MagicMock(return_value=None)
        position_executor_config = PositionExecutorConfig(
            timestamp=1234, connector_name="binance",
            trading_pair="ETH-USDT", side=TradeType.BUY, entry_price=Decimal(100), amount=Decimal(10))
        arbitrage_executor_config = ArbitrageExecutorConfig(
            timestamp=1234, order_amount=Decimal(10), min_profitability=Decimal(0.01),
            buying_market=ConnectorPair(connector_name="binance", trading_pair="ETH-USDT"),
            selling_market=ConnectorPair(connector_name="coinbase", trading_pair="ETH-USDT"),
        )
        dca_executor_config = DCAExecutorConfig(
            timestamp=1234, connector_name="binance", trading_pair="ETH-USDT",
            side=TradeType.BUY, amounts_quote=[Decimal(10)], prices=[Decimal(100)],)
        twap_executor_config = TWAPExecutorConfig(
            timestamp=1234, connector_name="binance", trading_pair="ETH-USDT",
            side=TradeType.BUY, total_amount_quote=Decimal(100), total_duration=10, order_interval=5,
        )
        grid_executor_config = GridExecutorConfig(
            timestamp=1234, connector_name="binance", trading_pair="ETH-USDT",
            side=TradeType.BUY, total_amount_quote=Decimal(100), start_price=Decimal(100),
            end_price=Decimal(200), limit_price=Decimal(90),
            triple_barrier_config=TripleBarrierConfig(take_profit=Decimal(0.01), stop_loss=Decimal(0.2))
        )
        actions = [
            CreateExecutorAction(executor_config=position_executor_config, controller_id="test"),
            CreateExecutorAction(executor_config=arbitrage_executor_config, controller_id="test"),
            CreateExecutorAction(executor_config=dca_executor_config, controller_id="test"),
            CreateExecutorAction(executor_config=twap_executor_config, controller_id="test"),
            CreateExecutorAction(executor_config=grid_executor_config, controller_id="test"),
        ]
        self.orchestrator.execute_actions(actions)
        self.assertEqual(len(self.orchestrator.active_executors["test"]), 5)

    def test_execute_actions_store_executor_active(self):
        position_executor = MagicMock(spec=PositionExecutor)
        position_executor.is_active = True
        config_mock = MagicMock(PositionExecutorConfig)
        config_mock.id = "test"
        config_mock.controller_id = "test"
        position_executor.config = config_mock
        self.orchestrator.cached_performance["test"] = PerformanceReport()
        self.orchestrator.active_executors["test"] = [position_executor]
        actions = [StoreExecutorAction(executor_id="test", controller_id="test")]
        self.orchestrator.execute_actions(actions)
        self.assertEqual(len(self.orchestrator.active_executors["test"]), 1)

    @patch.object(MarketsRecorder, "get_instance")
    def test_execute_actions_store_executor_inactive(self, markets_recorder_mock):
        markets_recorder_mock.return_value = MagicMock(spec=MarketsRecorder)
        markets_recorder_mock.store_or_update_executor = MagicMock(return_value=None)
        position_executor = MagicMock(spec=PositionExecutor)
        position_executor.is_active = False
        config_mock = MagicMock(PositionExecutorConfig)
        config_mock.id = "test"
        config_mock.controller_id = "test"
        position_executor.config = config_mock
        self.orchestrator.active_executors["test"] = [position_executor]
        self.orchestrator.cached_performance["test"] = PerformanceReport()
        actions = [StoreExecutorAction(executor_id="test", controller_id="test")]
        self.orchestrator.execute_actions(actions)
        self.assertEqual(len(self.orchestrator.active_executors["test"]), 0)

    @patch('hummingbot.connector.markets_recorder.MarketsRecorder.get_instance')
    def test_generate_performance_report(self, mock_get_instance):
        # Create a mock for MarketsRecorder and its get_executors_by_controller method
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_markets_recorder.get_executors_by_controller.return_value = []
        mock_get_instance.return_value = mock_markets_recorder
        config_mock = PositionExecutorConfig(
            timestamp=1234, trading_pair="ETH-USDT", connector_name="binance",
            side=TradeType.BUY, amount=Decimal(10), entry_price=Decimal(100),
        )
        position_executor_non_active = MagicMock(spec=PositionExecutor)
        position_executor_non_active.executor_info = ExecutorInfo(
            id="123", timestamp=1234, type="position_executor",
            status=RunnableStatus.RUNNING, config=config_mock,
            filled_amount_quote=Decimal(0), net_pnl_quote=Decimal(0), net_pnl_pct=Decimal(0),
            cum_fees_quote=Decimal(0), is_trading=False, is_active=True, custom_info={"side": TradeType.BUY}
        )
        position_executor_active = MagicMock(spec=PositionExecutor)
        position_executor_active.executor_info = ExecutorInfo(
            id="123", timestamp=1234, type="position_executor",
            status=RunnableStatus.RUNNING, config=config_mock,
            filled_amount_quote=Decimal(100), net_pnl_quote=Decimal(10), net_pnl_pct=Decimal(10),
            cum_fees_quote=Decimal(1), is_trading=True, is_active=True, custom_info={"side": TradeType.BUY}
        )
        position_executor_failed = MagicMock(spec=PositionExecutor)
        position_executor_failed.executor_info = ExecutorInfo(
            id="123", timestamp=1234, type="position_executor",
            status=RunnableStatus.TERMINATED, config=config_mock,
            close_type=CloseType.FAILED,
            filled_amount_quote=Decimal(100), net_pnl_quote=Decimal(0), net_pnl_pct=Decimal(0),
            cum_fees_quote=Decimal(1), is_trading=True, is_active=True, custom_info={"side": TradeType.BUY}
        )
        position_executor_tp = MagicMock(spec=PositionExecutor)
        position_executor_tp.executor_info = ExecutorInfo(
            id="123", timestamp=1234, type="position_executor",
            status=RunnableStatus.TERMINATED, config=config_mock,
            close_type=CloseType.TAKE_PROFIT,
            filled_amount_quote=Decimal(100), net_pnl_quote=Decimal(10), net_pnl_pct=Decimal(10),
            cum_fees_quote=Decimal(1), is_trading=False, is_active=False, custom_info={"side": TradeType.BUY}
        )
        self.orchestrator.active_executors["test"] = [position_executor_non_active, position_executor_active,
                                                      position_executor_failed, position_executor_tp]
        report = self.orchestrator.generate_performance_report(controller_id="test")
        self.assertEqual(report.realized_pnl_quote, Decimal(10))
        self.assertEqual(report.unrealized_pnl_quote, Decimal(10))

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_initialize_cached_performance(self, mock_get_instance: MagicMock):
        # Create mock markets recorder
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder

        # Create mock executor info
        executor_info = ExecutorInfo(
            id="123", timestamp=1234, type="position_executor",
            status=RunnableStatus.RUNNING, config=PositionExecutorConfig(
                timestamp=1234, trading_pair="ETH-USDT", connector_name="binance",
                side=TradeType.BUY, amount=Decimal(10), entry_price=Decimal(100),
            ),
            filled_amount_quote=Decimal(100), net_pnl_quote=Decimal(10), net_pnl_pct=Decimal(10),
            cum_fees_quote=Decimal(1), is_trading=True, is_active=True, custom_info={"side": TradeType.BUY},
            controller_id="test",
        )

        # Set up mock to return executor info
        mock_markets_recorder.get_all_executors.return_value = [executor_info]
        mock_markets_recorder.get_all_positions.return_value = []

        # Add the controller to the strategy's controllers dict
        self.mock_strategy.controllers = {"test": MagicMock()}

        orchestrator = ExecutorOrchestrator(strategy=self.mock_strategy)
        self.assertEqual(len(orchestrator.cached_performance), 1)
        self.assertEqual(len(orchestrator.stored_executors["test"]), 0)

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_initialize_cached_performance_loads_stored_recent_executors(self, mock_get_instance: MagicMock):
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder

        stored_executor = ExecutorInfo(
            id="stored-executor",
            timestamp=1234,
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            close_type=CloseType.POSITION_HOLD,
            close_timestamp=1250,
            config=PositionExecutorConfig(
                id="stored-executor",
                timestamp=1234,
                trading_pair="ETH-USDT",
                connector_name="binance",
                side=TradeType.BUY,
                amount=Decimal(10),
                entry_price=Decimal(100),
            ),
            filled_amount_quote=Decimal(100),
            net_pnl_quote=Decimal(10),
            net_pnl_pct=Decimal("0.1"),
            cum_fees_quote=Decimal("1"),
            is_trading=False,
            is_active=False,
            custom_info={"current_position_average_price": Decimal("100")},
            controller_id="test",
        )
        mock_markets_recorder.get_all_executors.return_value = [stored_executor]
        mock_markets_recorder.get_all_positions.return_value = []
        self.mock_strategy.controllers = {"test": MagicMock()}

        orchestrator = ExecutorOrchestrator(strategy=self.mock_strategy)

        self.assertEqual(len(orchestrator.stored_executors["test"]), 1)
        restored_executor = orchestrator.stored_executors["test"][0]
        self.assertEqual(restored_executor.id, "stored-executor")
        self.assertEqual(restored_executor.custom_info["persistence_source"], "db")

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_initialize_cached_performance_with_positions(self, mock_get_instance: MagicMock):
        # Create mock markets recorder
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder

        # Create mock position from database
        position1 = Position(
            id="pos1",
            timestamp=1234,
            controller_id="controller1",
            connector_name="binance",
            trading_pair="ETH-USDT",
            side=TradeType.BUY.name,
            amount=Decimal("1"),
            breakeven_price=Decimal("1000"),
            unrealized_pnl_quote=Decimal("50"),
            realized_pnl_quote=Decimal("25"),
            cum_fees_quote=Decimal("5"),
            volume_traded_quote=Decimal("1000")
        )

        position2 = Position(
            id="pos2",
            timestamp=1235,
            controller_id="controller2",
            connector_name="binance",
            trading_pair="BTC-USDT",
            side=TradeType.SELL.name,
            amount=Decimal("0.1"),
            breakeven_price=Decimal("50000"),
            unrealized_pnl_quote=Decimal("-100"),
            realized_pnl_quote=Decimal("-50"),
            cum_fees_quote=Decimal("10"),
            volume_traded_quote=Decimal("5000")
        )

        # Set up mock to return executor info and positions
        mock_markets_recorder.get_all_executors.return_value = []
        mock_markets_recorder.get_all_positions.return_value = [position1, position2]

        # Add the controllers to the strategy's controllers dict
        self.mock_strategy.controllers = {"controller1": MagicMock(), "controller2": MagicMock()}

        orchestrator = ExecutorOrchestrator(strategy=self.mock_strategy)

        # Check that positions were loaded
        self.assertEqual(len(orchestrator.cached_performance), 2)
        self.assertIn("controller1", orchestrator.cached_performance)
        self.assertIn("controller2", orchestrator.cached_performance)

        # Check that positions were converted to PositionHold objects
        self.assertEqual(len(orchestrator.positions_held["controller1"]), 1)
        self.assertEqual(len(orchestrator.positions_held["controller2"]), 1)

        # Verify position data was correctly loaded
        position_hold1 = orchestrator.positions_held["controller1"][0]
        self.assertEqual(position_hold1.connector_name, "binance")
        self.assertEqual(position_hold1.trading_pair, "ETH-USDT")
        self.assertEqual(position_hold1.side, TradeType.BUY)
        self.assertEqual(position_hold1.buy_amount_base, Decimal("1"))
        self.assertEqual(position_hold1.buy_amount_quote, Decimal("1000"))
        self.assertEqual(position_hold1.volume_traded_quote, Decimal("1000"))
        self.assertEqual(position_hold1.cum_fees_quote, Decimal("5"))

        position_hold2 = orchestrator.positions_held["controller2"][0]
        self.assertEqual(position_hold2.connector_name, "binance")
        self.assertEqual(position_hold2.trading_pair, "BTC-USDT")
        self.assertEqual(position_hold2.side, TradeType.SELL)
        self.assertEqual(position_hold2.sell_amount_base, Decimal("0.1"))
        self.assertEqual(position_hold2.sell_amount_quote, Decimal("5000"))
        self.assertEqual(position_hold2.volume_traded_quote, Decimal("5000"))
        self.assertEqual(position_hold2.cum_fees_quote, Decimal("10"))

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_initialize_cached_performance_recovers_positions_from_executor_history_when_db_rows_are_invalid(
        self, mock_get_instance: MagicMock
    ):
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder

        invalid_position = Position(
            id="pos1",
            timestamp=1235,
            controller_id="controller1",
            connector_name="binance",
            trading_pair="ETH-USDT",
            side=TradeType.BUY.name,
            amount=Decimal("0"),
            breakeven_price=Decimal("0"),
            unrealized_pnl_quote=Decimal("0"),
            realized_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            volume_traded_quote=Decimal("0")
        )
        buy_hold_executor = ExecutorInfo(
            id="buy-held",
            timestamp=1234,
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            close_type=CloseType.POSITION_HOLD,
            close_timestamp=1234,
            config=PositionExecutorConfig(
                id="buy-held",
                timestamp=1234,
                trading_pair="ETH-USDT",
                connector_name="binance",
                side=TradeType.BUY,
                amount=Decimal("1"),
                entry_price=Decimal("100"),
            ),
            filled_amount_quote=Decimal("0"),
            net_pnl_quote=Decimal("0"),
            net_pnl_pct=Decimal("0"),
            cum_fees_quote=Decimal("1"),
            is_trading=False,
            is_active=False,
            custom_info={"held_position_orders": [
                {"client_order_id": "buy-order", "executed_amount_base": Decimal("1"),
                 "executed_amount_quote": Decimal("100"), "trade_type": "BUY",
                 "cumulative_fee_paid_quote": Decimal("1")}
            ]},
            controller_id="controller1",
        )
        sell_hold_executor = ExecutorInfo(
            id="sell-held",
            timestamp=1236,
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            close_type=CloseType.POSITION_HOLD,
            close_timestamp=1236,
            config=PositionExecutorConfig(
                id="sell-held",
                timestamp=1236,
                trading_pair="ETH-USDT",
                connector_name="binance",
                side=TradeType.SELL,
                amount=Decimal("0.4"),
                entry_price=Decimal("120"),
            ),
            filled_amount_quote=Decimal("0"),
            net_pnl_quote=Decimal("0"),
            net_pnl_pct=Decimal("0"),
            cum_fees_quote=Decimal("0.5"),
            is_trading=False,
            is_active=False,
            custom_info={"held_position_orders": [
                {"client_order_id": "sell-order", "executed_amount_base": Decimal("0.4"),
                 "executed_amount_quote": Decimal("48"), "trade_type": "SELL",
                 "cumulative_fee_paid_quote": Decimal("0.5")}
            ]},
            controller_id="controller1",
        )

        mock_markets_recorder.get_all_executors.return_value = [buy_hold_executor, sell_hold_executor]
        mock_markets_recorder.get_all_positions.return_value = [invalid_position]
        self.mock_strategy.controllers = {"controller1": MagicMock()}

        orchestrator = ExecutorOrchestrator(strategy=self.mock_strategy)

        self.assertEqual(len(orchestrator.positions_held["controller1"]), 1)
        recovered_position = orchestrator.positions_held["controller1"][0]
        self.assertEqual(recovered_position.buy_amount_base, Decimal("1"))
        self.assertEqual(recovered_position.sell_amount_base, Decimal("0.4"))

        recovered_summary = recovered_position.get_position_summary(Decimal("110"))
        self.assertEqual(recovered_summary.amount, Decimal("0.6"))
        self.assertEqual(recovered_summary.breakeven_price, Decimal("100"))

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_initialize_cached_performance_restores_positions_from_ledger_before_snapshot(
        self, mock_get_instance: MagicMock
    ):
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder

        ledger_entry_buy = PositionHoldLedger(
            id="controller1|binance|ETH-USDT|spot|buy-order",
            controller_id="controller1",
            executor_id="buy-held",
            connector_name="binance",
            trading_pair="ETH-USDT",
            position_side=None,
            order_side="BUY",
            timestamp=1234,
            client_order_id="buy-order",
            exchange_order_id="buy-ex",
            price=Decimal("100"),
            amount=Decimal("1"),
            executed_amount_base=Decimal("1"),
            executed_amount_quote=Decimal("100"),
            cumulative_fee_paid_quote=Decimal("1"),
            order_payload={
                "client_order_id": "buy-order",
                "trade_type": "BUY",
                "executed_amount_base": "1",
                "executed_amount_quote": "100",
                "cumulative_fee_paid_quote": "1",
            },
        )
        ledger_entry_sell = PositionHoldLedger(
            id="controller1|binance|ETH-USDT|spot|sell-order",
            controller_id="controller1",
            executor_id="sell-held",
            connector_name="binance",
            trading_pair="ETH-USDT",
            position_side=None,
            order_side="SELL",
            timestamp=1236,
            client_order_id="sell-order",
            exchange_order_id="sell-ex",
            price=Decimal("120"),
            amount=Decimal("0.4"),
            executed_amount_base=Decimal("0.4"),
            executed_amount_quote=Decimal("48"),
            cumulative_fee_paid_quote=Decimal("0.5"),
            order_payload={
                "client_order_id": "sell-order",
                "trade_type": "SELL",
                "executed_amount_base": "0.4",
                "executed_amount_quote": "48",
                "cumulative_fee_paid_quote": "0.5",
            },
        )
        snapshot_position = Position(
            id="snapshot1",
            timestamp=1300,
            controller_id="controller1",
            connector_name="binance",
            trading_pair="ETH-USDT",
            side=TradeType.BUY.name,
            amount=Decimal("5"),
            breakeven_price=Decimal("200"),
            unrealized_pnl_quote=Decimal("0"),
            realized_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            volume_traded_quote=Decimal("1000"),
        )

        mock_markets_recorder.get_all_executors.return_value = []
        mock_markets_recorder.get_all_position_hold_ledger_entries.return_value = [ledger_entry_buy, ledger_entry_sell]
        mock_markets_recorder.get_all_positions.return_value = [snapshot_position]
        self.mock_strategy.controllers = {"controller1": MagicMock()}
        self.mock_strategy.markets = {"binance": {"ETH-USDT"}}

        orchestrator = ExecutorOrchestrator(strategy=self.mock_strategy)

        self.assertEqual(len(orchestrator.positions_held["controller1"]), 1)
        recovered_summary = orchestrator.positions_held["controller1"][0].get_position_summary(Decimal("110"))
        self.assertEqual(recovered_summary.amount, Decimal("0.6"))
        self.assertEqual(recovered_summary.breakeven_price, Decimal("100"))

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_initialize_cached_performance_backfills_ledger_from_executor_history_when_ledger_missing(
        self, mock_get_instance: MagicMock
    ):
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder

        invalid_position = Position(
            id="pos1",
            timestamp=1235,
            controller_id="controller1",
            connector_name="binance",
            trading_pair="ETH-USDT",
            side=TradeType.BUY.name,
            amount=Decimal("0"),
            breakeven_price=Decimal("0"),
            unrealized_pnl_quote=Decimal("0"),
            realized_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            volume_traded_quote=Decimal("0"),
        )
        buy_hold_executor = ExecutorInfo(
            id="buy-held",
            timestamp=1234,
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            close_type=CloseType.POSITION_HOLD,
            close_timestamp=1234,
            config=PositionExecutorConfig(
                id="buy-held",
                timestamp=1234,
                trading_pair="ETH-USDT",
                connector_name="binance",
                side=TradeType.BUY,
                amount=Decimal("1"),
                entry_price=Decimal("100"),
            ),
            filled_amount_quote=Decimal("0"),
            net_pnl_quote=Decimal("0"),
            net_pnl_pct=Decimal("0"),
            cum_fees_quote=Decimal("1"),
            is_trading=False,
            is_active=False,
            custom_info={"held_position_orders": [
                {"client_order_id": "buy-order", "executed_amount_base": Decimal("1"),
                 "executed_amount_quote": Decimal("100"), "trade_type": "BUY",
                 "cumulative_fee_paid_quote": Decimal("1")}
            ]},
            controller_id="controller1",
        )

        mock_markets_recorder.get_all_executors.return_value = [buy_hold_executor]
        mock_markets_recorder.get_all_position_hold_ledger_entries.return_value = []
        mock_markets_recorder.get_all_positions.return_value = [invalid_position]
        mock_markets_recorder.store_or_update_position_hold_ledger_entries = MagicMock(return_value=None)
        self.mock_strategy.controllers = {"controller1": MagicMock()}

        ExecutorOrchestrator(strategy=self.mock_strategy)

        mock_markets_recorder.store_or_update_position_hold_ledger_entries.assert_called_once()
        self.assertEqual(
            mock_markets_recorder.store_or_update_position_hold_ledger_entries.call_args.kwargs["controller_id"],
            "controller1",
        )
        self.assertEqual(
            mock_markets_recorder.store_or_update_position_hold_ledger_entries.call_args.kwargs["executor_id"],
            "buy-held",
        )

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_initialize_cached_performance_recovers_positions_from_trade_fills_when_ledger_and_executor_history_are_stale(
        self, mock_get_instance: MagicMock
    ):
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder
        mock_markets_recorder.config_file_path = "rsi_v5.yml"

        invalid_position = Position(
            id="pos1",
            timestamp=1235,
            controller_id="controller1",
            connector_name="binance",
            trading_pair="ETH-USDT",
            side=TradeType.BUY.name,
            amount=Decimal("0"),
            breakeven_price=Decimal("0"),
            unrealized_pnl_quote=Decimal("0"),
            realized_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            volume_traded_quote=Decimal("0"),
        )
        trade_fill_buy_part_1 = TradeFill(
            config_file_path="rsi_v5.yml",
            strategy="v2_with_controllers",
            market="binance",
            symbol="ETH-USDT",
            base_asset="ETH",
            quote_asset="USDT",
            timestamp=1234,
            order_id="buy-order",
            trade_type=TradeType.BUY.name,
            order_type="LIMIT",
            price=Decimal("100"),
            amount=Decimal("0.4"),
            leverage=1,
            trade_fee={"percent": "0", "flat_fees": []},
            trade_fee_in_quote=Decimal("0.4"),
            exact_amount="0.4", exact_price="100",
            exact_trade_fee_in_quote="0.4", exact_receipt_source="order_filled_event_v1",
            exchange_trade_id="buy-fill-1",
            position="NIL",
        )
        trade_fill_buy_part_2 = TradeFill(
            config_file_path="rsi_v5.yml",
            strategy="v2_with_controllers",
            market="binance",
            symbol="ETH-USDT",
            base_asset="ETH",
            quote_asset="USDT",
            timestamp=1235,
            order_id="buy-order",
            trade_type=TradeType.BUY.name,
            order_type="LIMIT",
            price=Decimal("100"),
            amount=Decimal("0.6"),
            leverage=1,
            trade_fee={"percent": "0", "flat_fees": []},
            trade_fee_in_quote=Decimal("0.6"),
            exact_amount="0.6", exact_price="100",
            exact_trade_fee_in_quote="0.6", exact_receipt_source="order_filled_event_v1",
            exchange_trade_id="buy-fill-2",
            position="NIL",
        )
        trade_fill_sell = TradeFill(
            config_file_path="rsi_v5.yml",
            strategy="v2_with_controllers",
            market="binance",
            symbol="ETH-USDT",
            base_asset="ETH",
            quote_asset="USDT",
            timestamp=1236,
            order_id="sell-order",
            trade_type=TradeType.SELL.name,
            order_type="LIMIT",
            price=Decimal("120"),
            amount=Decimal("0.4"),
            leverage=1,
            trade_fee={"percent": "0", "flat_fees": []},
            trade_fee_in_quote=Decimal("0.5"),
            exact_amount="0.4", exact_price="120",
            exact_trade_fee_in_quote="0.5", exact_receipt_source="order_filled_event_v1",
            exchange_trade_id="sell-fill-1",
            position="NIL",
        )

        mock_markets_recorder.get_all_executors.return_value = []
        mock_markets_recorder.get_all_position_hold_ledger_entries.return_value = []
        mock_markets_recorder.get_all_positions.return_value = [invalid_position]
        mock_markets_recorder.get_trades_for_config.return_value = [
            trade_fill_sell,
            trade_fill_buy_part_2,
            trade_fill_buy_part_1,
        ]
        self.mock_strategy.controllers = {
            "controller1": self.create_mock_controller("binance", "ETH-USDT"),
        }
        self.mock_strategy.markets = {"binance": {"ETH-USDT"}}

        orchestrator = ExecutorOrchestrator(strategy=self.mock_strategy)

        mock_markets_recorder.get_trades_for_config.assert_called_once_with("rsi_v5.yml")
        self.assertEqual(len(orchestrator.positions_held["controller1"]), 1)
        recovered_position = orchestrator.positions_held["controller1"][0]
        recovered_summary = recovered_position.get_position_summary(Decimal("110"))
        self.assertEqual(recovered_summary.amount, Decimal("0.6"))
        self.assertEqual(recovered_summary.breakeven_price, Decimal("100"))
        self.assertEqual(recovered_summary.cum_fees_quote, Decimal("1.5"))

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_initialize_cached_performance_recovers_trade_fills_when_ledger_scope_is_flat(
        self, mock_get_instance: MagicMock
    ):
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder
        mock_markets_recorder.config_file_path = "rsi_v5.yml"

        ledger_entry_buy = PositionHoldLedger(
            id="controller1|binance|BTC-USDT|spot|ledger-buy",
            controller_id="controller1",
            executor_id="buy-held",
            connector_name="binance",
            trading_pair="BTC-USDT",
            position_side=None,
            order_side=TradeType.BUY.name,
            timestamp=1234,
            client_order_id="ledger-buy",
            exchange_order_id="ledger-buy-ex",
            price=Decimal("70000"),
            amount=Decimal("0.01"),
            executed_amount_base=Decimal("0.01"),
            executed_amount_quote=Decimal("700"),
            cumulative_fee_paid_quote=Decimal("0.7"),
            order_payload={
                "client_order_id": "ledger-buy",
                "executed_amount_base": "0.01",
                "executed_amount_quote": "700",
                "trade_type": TradeType.BUY.name,
                "cumulative_fee_paid_quote": "0.7",
            },
        )
        ledger_entry_sell = PositionHoldLedger(
            id="controller1|binance|BTC-USDT|spot|ledger-sell",
            controller_id="controller1",
            executor_id="sell-held",
            connector_name="binance",
            trading_pair="BTC-USDT",
            position_side=None,
            order_side=TradeType.SELL.name,
            timestamp=1235,
            client_order_id="ledger-sell",
            exchange_order_id="ledger-sell-ex",
            price=Decimal("71000"),
            amount=Decimal("0.01"),
            executed_amount_base=Decimal("0.01"),
            executed_amount_quote=Decimal("710"),
            cumulative_fee_paid_quote=Decimal("0.71"),
            order_payload={
                "client_order_id": "ledger-sell",
                "executed_amount_base": "0.01",
                "executed_amount_quote": "710",
                "trade_type": TradeType.SELL.name,
                "cumulative_fee_paid_quote": "0.71",
            },
        )
        trade_fill_buy = TradeFill(
            config_file_path="rsi_v5.yml",
            strategy="v2_with_controllers",
            market="binance",
            symbol="BTC-USDT",
            base_asset="BTC",
            quote_asset="USDT",
            timestamp=1236,
            order_id="live-buy",
            trade_type=TradeType.BUY.name,
            order_type="LIMIT",
            price=Decimal("80000"),
            amount=Decimal("0.00985"),
            leverage=1,
            trade_fee={"percent": "0", "flat_fees": []},
            trade_fee_in_quote=Decimal("0.8"),
            exact_amount="0.00985", exact_price="80000",
            exact_trade_fee_in_quote="0.8", exact_receipt_source="order_filled_event_v1",
            exchange_trade_id="live-buy-fill",
            position="NIL",
        )

        mock_markets_recorder.get_all_executors.return_value = []
        mock_markets_recorder.get_all_position_hold_ledger_entries.return_value = [
            ledger_entry_buy,
            ledger_entry_sell,
        ]
        mock_markets_recorder.get_all_positions.return_value = []
        mock_markets_recorder.get_trades_for_config.return_value = [trade_fill_buy]
        self.mock_strategy.controllers = {
            "controller1": self.create_mock_controller("binance", "BTC-USDT"),
        }
        self.mock_strategy.markets = {"binance": {"BTC-USDT"}}

        orchestrator = ExecutorOrchestrator(strategy=self.mock_strategy)

        self.assertEqual(len(orchestrator.positions_held["controller1"]), 1)
        recovered_position = orchestrator.positions_held["controller1"][0]
        recovered_summary = recovered_position.get_position_summary(Decimal("80000"))
        self.assertEqual(recovered_summary.amount, Decimal("0.00985"))
        self.assertEqual(recovered_summary.breakeven_price, Decimal("80000"))

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_get_all_reports_reconciles_trade_fill_position_net_of_active_buy_inventory(
        self, mock_get_instance: MagicMock
    ):
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder
        mock_markets_recorder.config_file_path = "rsi_v5.yml"
        mock_markets_recorder.get_all_executors.return_value = []
        mock_markets_recorder.get_all_position_hold_ledger_entries.return_value = []
        mock_markets_recorder.get_all_positions.return_value = []
        mock_markets_recorder.get_trades_for_config.return_value = [
            TradeFill(
                config_file_path="rsi_v5.yml",
                strategy="v2_with_controllers",
                market="binance",
                symbol="BTC-USDT",
                base_asset="BTC",
                quote_asset="USDT",
                timestamp=1234,
                order_id="historical-buy",
                trade_type=TradeType.BUY.name,
                order_type="LIMIT",
                price=Decimal("80000"),
                amount=Decimal("0.00985"),
                leverage=1,
                trade_fee={"percent": "0", "flat_fees": []},
                trade_fee_in_quote=Decimal("0.8"),
                exact_amount="0.00985", exact_price="80000",
                exact_trade_fee_in_quote="0.8", exact_receipt_source="order_filled_event_v1",
                exchange_trade_id="historical-buy-fill",
                position="NIL",
            )
        ]
        self.mock_strategy.controllers = {
            "controller1": self.create_mock_controller("binance", "BTC-USDT"),
        }
        self.mock_strategy.markets = {"binance": {"BTC-USDT"}}

        orchestrator = ExecutorOrchestrator(strategy=self.mock_strategy)
        active_config = PositionExecutorConfig(
            id="active-buy",
            timestamp=1235,
            trading_pair="BTC-USDT",
            connector_name="binance",
            side=TradeType.BUY,
            amount=Decimal("0.005"),
            entry_price=Decimal("80000"),
        )
        active_executor = MagicMock()
        active_executor.executor_info = ExecutorInfo(
            id="active-buy",
            timestamp=1235,
            type="position_executor",
            status=RunnableStatus.RUNNING,
            close_type=None,
            close_timestamp=None,
            config=active_config,
            filled_amount_quote=Decimal("400"),
            net_pnl_quote=Decimal("0"),
            net_pnl_pct=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            is_trading=True,
            is_active=True,
            custom_info={"order_ids": ["active-entry"]},
            controller_id="controller1",
        )
        orchestrator.active_executors = {"controller1": [active_executor]}
        orchestrator.positions_held = {"controller1": []}

        report = orchestrator.get_all_reports()

        self.assertEqual(len(report["controller1"]["positions"]), 1)
        position = report["controller1"]["positions"][0]
        self.assertEqual(position.amount, Decimal("0.00985"))
        self.assertEqual(position.breakeven_price, Decimal("80000"))

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_initialize_cached_performance_skips_trade_fill_recovery_when_controller_mapping_is_ambiguous(
        self, mock_get_instance: MagicMock
    ):
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder
        mock_markets_recorder.config_file_path = "rsi_v5.yml"

        trade_fill_buy = TradeFill(
            config_file_path="rsi_v5.yml",
            strategy="v2_with_controllers",
            market="binance",
            symbol="ETH-USDT",
            base_asset="ETH",
            quote_asset="USDT",
            timestamp=1234,
            order_id="buy-order",
            trade_type=TradeType.BUY.name,
            order_type="LIMIT",
            price=Decimal("100"),
            amount=Decimal("1"),
            leverage=1,
            trade_fee={},
            trade_fee_in_quote=Decimal("1"),
            exchange_trade_id="buy-fill-1",
            position="NIL",
        )

        mock_markets_recorder.get_all_executors.return_value = []
        mock_markets_recorder.get_all_position_hold_ledger_entries.return_value = []
        mock_markets_recorder.get_all_positions.return_value = []
        mock_markets_recorder.get_trades_for_config.return_value = [trade_fill_buy]
        self.mock_strategy.controllers = {
            "controller1": self.create_mock_controller("binance", "ETH-USDT"),
            "controller2": self.create_mock_controller("binance", "ETH-USDT"),
        }
        self.mock_strategy.markets = {"binance": {"ETH-USDT"}}

        orchestrator = ExecutorOrchestrator(strategy=self.mock_strategy)

        self.assertEqual(orchestrator.positions_held["controller1"], [])
        self.assertEqual(orchestrator.positions_held["controller2"], [])

    @patch.object(MarketsRecorder, "get_instance")
    def test_store_all_positions(self, markets_recorder_mock):
        markets_recorder_mock.return_value = MagicMock(spec=MarketsRecorder)
        markets_recorder_mock.update_or_store_position = MagicMock(return_value=None)
        position_held = PositionHold("binance", "SOL-USDT", side=TradeType.BUY)
        executor_info = ExecutorInfo(
            id="123", timestamp=1234, type="position_executor",
            status=RunnableStatus.TERMINATED, config=PositionExecutorConfig(
                timestamp=1234, trading_pair="SOL-USDT", connector_name="binance",
                side=TradeType.BUY, amount=Decimal(10), entry_price=Decimal(100),
            ), net_pnl_pct=Decimal(0), net_pnl_quote=Decimal(0), cum_fees_quote=Decimal(0),
            filled_amount_quote=Decimal(100), is_active=False, is_trading=False,
            custom_info={"held_position_orders": [
                {"order_id": "123", "amount": Decimal(10), "trade_type": "BUY",
                 "executed_amount_base": Decimal("10"), "executed_amount_quote": Decimal("2300"),
                 "cumulative_fee_paid_quote": Decimal(0)}]},
            controller_id="main"
        )
        position_held.add_orders_from_executor(executor_info)
        self.orchestrator.positions_held = {
            "main": [position_held]
        }
        self.orchestrator.store_all_positions()
        self.assertEqual(len(self.orchestrator.positions_held), 0)

    def test_get_positions_report(self):
        position_held = PositionHold("binance", "SOL-USDT", side=TradeType.BUY)
        executor_info = ExecutorInfo(
            id="123", timestamp=1234, type="position_executor",
            status=RunnableStatus.TERMINATED, config=PositionExecutorConfig(
                timestamp=1234, trading_pair="SOL-USDT", connector_name="binance",
                side=TradeType.BUY, amount=Decimal(10), entry_price=Decimal(100),
            ), net_pnl_pct=Decimal(0), net_pnl_quote=Decimal(0), cum_fees_quote=Decimal(0),
            filled_amount_quote=Decimal(100), is_active=False, is_trading=False,
            custom_info={"held_position_orders": [
                {"order_id": "123", "amount": Decimal(10), "trade_type": "SELL",
                 "executed_amount_base": Decimal("10"), "executed_amount_quote": Decimal("2300"),
                 "cumulative_fee_paid_quote": Decimal(0)}]},
            controller_id="main"
        )
        position_held.add_orders_from_executor(executor_info)
        self.orchestrator.positions_held = {
            "main": [position_held]
        }
        report = self.orchestrator.get_positions_report()
        self.assertEqual(len(report), 1)
        self.assertEqual(report["main"][0].amount, Decimal(10))

    @patch.object(MarketsRecorder, "get_instance")
    def test_store_all_executors(self, markets_recorder_mock):
        markets_recorder_mock.return_value = MagicMock(spec=MarketsRecorder)
        markets_recorder_mock.store_or_update_executor = MagicMock(return_value=None)
        position_executor = MagicMock(spec=PositionExecutor)
        position_executor.is_active = False
        config_mock = MagicMock(PositionExecutorConfig)
        config_mock.id = "test"
        config_mock.controller_id = "test"
        position_executor.config = config_mock
        self.orchestrator.active_executors["test"] = [position_executor]
        self.orchestrator.store_all_executors()
        self.assertEqual(self.orchestrator.active_executors, {})

    @patch.object(ExecutorOrchestrator, "store_all_positions")
    @patch.object(ExecutorOrchestrator, "store_all_executors")
    def test_stop(self, store_all_executors, store_all_positions):
        async def test_async():
            store_all_positions.return_value = None
            store_all_executors.return_value = None
            position_executor = MagicMock(spec=PositionExecutor)
            position_executor.is_closed = False
            position_executor.early_stop = MagicMock(return_value=None)
            position_executor.executor_info = MagicMock()
            position_executor.executor_info.is_done = True
            position_executor.config = MagicMock()
            self.orchestrator.active_executors["test"] = [position_executor]
            await self.orchestrator.stop()
            position_executor.early_stop.assert_called_once_with(keep_position=True)

        asyncio.run(test_async())

    @patch.object(ExecutorOrchestrator, "store_all_positions")
    @patch.object(ExecutorOrchestrator, "store_all_executors")
    def test_stop_respects_keep_position_by_controller(self, store_all_executors, store_all_positions):
        async def test_async():
            store_all_positions.return_value = None
            store_all_executors.return_value = None
            position_executor = MagicMock(spec=PositionExecutor)
            position_executor.is_closed = False
            position_executor.early_stop = MagicMock(return_value=None)
            position_executor.executor_info = MagicMock()
            position_executor.executor_info.is_done = True
            position_executor.config = MagicMock()
            self.orchestrator.active_executors["rsi-v5"] = [position_executor]

            await self.orchestrator.stop(
                keep_position_by_controller={"rsi-v5": True}
            )

            position_executor.early_stop.assert_called_once_with(keep_position=True)

        asyncio.run(test_async())

    @patch.object(MarketsRecorder, "get_instance")
    def test_store_all_positions_materializes_done_position_hold_executors_before_persisting(self, markets_recorder_mock):
        markets_recorder_mock.return_value = MagicMock(spec=MarketsRecorder)
        markets_recorder_mock.update_or_store_position = MagicMock(return_value=None)

        config = PositionExecutorConfig(
            timestamp=1234,
            trading_pair="ETH-USDT",
            connector_name="binance",
            side=TradeType.BUY,
            amount=Decimal("1"),
            entry_price=Decimal("100"),
        )
        config.id = "held_executor"

        executor = MagicMock()
        executor.executor_info = ExecutorInfo(
            id="held_executor",
            timestamp=1234,
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            config=config,
            filled_amount_quote=Decimal("100"),
            net_pnl_quote=Decimal("0"),
            net_pnl_pct=Decimal("0"),
            cum_fees_quote=Decimal("1"),
            is_trading=False,
            is_active=False,
            custom_info={"held_position_orders": [
                {"client_order_id": "order_1", "executed_amount_base": Decimal("1"),
                 "executed_amount_quote": Decimal("100"), "trade_type": "BUY",
                 "cumulative_fee_paid_quote": Decimal("1")}
            ]},
            close_type=CloseType.POSITION_HOLD,
            controller_id="test_controller",
        )

        self.orchestrator.active_executors = {"test_controller": [executor]}
        self.orchestrator.positions_held = {"test_controller": []}
        self.orchestrator.executors_ids_position_held = []

        self.orchestrator.store_all_positions()

        markets_recorder_mock.return_value.update_or_store_position.assert_called_once()
        self.assertEqual(self.orchestrator.positions_held, {})

    @patch.object(MarketsRecorder, "get_instance")
    def test_store_all_positions_clears_flat_positions_in_db(self, markets_recorder_mock):
        markets_recorder_mock.return_value = MagicMock(spec=MarketsRecorder)
        markets_recorder_mock.update_or_store_position = MagicMock(return_value=None)

        flat_position = PositionHold("binance", "ETH-USDT", side=TradeType.BUY)
        flat_position.buy_amount_base = Decimal("1")
        flat_position.buy_amount_quote = Decimal("100")
        flat_position.sell_amount_base = Decimal("1")
        flat_position.sell_amount_quote = Decimal("110")

        self.orchestrator.positions_held = {"main": [flat_position]}
        self.orchestrator.store_all_positions()

        stored_position = markets_recorder_mock.return_value.update_or_store_position.call_args.args[0]
        self.assertEqual(stored_position.amount, Decimal("0"))
        self.assertEqual(self.orchestrator.positions_held, {})

    def test_stop_executor(self):
        position_executor = MagicMock(spec=PositionExecutor)
        position_executor.is_closed = False
        position_executor.early_stop = MagicMock(return_value=None)
        position_executor.config = MagicMock(PositionExecutorConfig)
        position_executor.config.id = "123"
        self.orchestrator.active_executors["test"] = [position_executor]
        self.orchestrator.stop_executor(StopExecutorAction(executor_id="123", controller_id="test"))

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_generate_performance_report_with_loaded_positions(self, mock_get_instance: MagicMock):
        # Create mock markets recorder
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder

        # Create a position from database
        db_position = Position(
            id="pos1",
            timestamp=1234,
            controller_id="test",
            connector_name="binance",
            trading_pair="ETH-USDT",
            side=TradeType.BUY.name,
            amount=Decimal("2"),
            breakeven_price=Decimal("1000"),
            unrealized_pnl_quote=Decimal("100"),
            realized_pnl_quote=Decimal("50"),
            cum_fees_quote=Decimal("10"),
            volume_traded_quote=Decimal("2000")
        )

        # Set up mock to return position
        mock_markets_recorder.get_all_executors.return_value = []
        mock_markets_recorder.get_all_positions.return_value = [db_position]

        # Add the controller to the strategy's controllers dict
        self.mock_strategy.controllers = {"test": MagicMock()}

        # Create orchestrator which will load the position
        orchestrator = ExecutorOrchestrator(strategy=self.mock_strategy)

        # Generate performance report
        report = orchestrator.generate_performance_report(controller_id="test")

        # Verify the report includes data from the loaded position
        self.assertEqual(report.volume_traded, Decimal("2000"))
        # The unrealized PnL should be calculated fresh based on current price (230)
        # For a BUY position: (current_price - breakeven_price) * amount = (230 - 1000) * 2 = -1540
        self.assertEqual(report.unrealized_pnl_quote, Decimal("-1540"))
        # Check that the report has the position summary
        self.assertTrue(hasattr(report, "positions_summary"))
        self.assertEqual(len(report.positions_summary), 1)
        self.assertEqual(report.positions_summary[0].amount, Decimal("2"))
        self.assertEqual(report.positions_summary[0].breakeven_price, Decimal("1000"))

    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance")
    def test_initial_positions_override(self, mock_get_instance: MagicMock):
        # Create mock markets recorder
        mock_markets_recorder = MagicMock(spec=MarketsRecorder)
        mock_get_instance.return_value = mock_markets_recorder

        # Create a database position that should be ignored due to override
        db_position = Position(
            id="db_pos1",
            timestamp=1234,
            controller_id="test_controller",
            connector_name="binance",
            trading_pair="ETH-USDT",
            side=TradeType.BUY.name,
            amount=Decimal("5"),
            breakeven_price=Decimal("2000"),
            unrealized_pnl_quote=Decimal("0"),
            realized_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            volume_traded_quote=Decimal("10000")
        )

        # Import the shared InitialPositionConfig
        from hummingbot.strategy_v2.models.position_config import InitialPositionConfig

        # Create initial position configs that should override the database
        initial_positions = {
            "test_controller": [
                InitialPositionConfig(
                    connector_name="binance",
                    trading_pair="ETH-USDT",
                    amount=Decimal("2"),
                    side=TradeType.BUY
                ),
                InitialPositionConfig(
                    connector_name="binance",
                    trading_pair="BTC-USDT",
                    amount=Decimal("0.1"),
                    side=TradeType.SELL
                )
            ]
        }

        # Set up mock to return both executors and positions
        mock_markets_recorder.get_all_executors.return_value = []
        mock_markets_recorder.get_all_positions.return_value = [db_position]

        # Add the controller to the strategy's controllers dict
        self.mock_strategy.controllers = {"test_controller": MagicMock()}

        # Create orchestrator with initial position overrides
        orchestrator = ExecutorOrchestrator(
            strategy=self.mock_strategy,
            initial_positions_by_controller=initial_positions
        )

        # Verify that the database position was NOT loaded
        # and instead the initial positions were created
        self.assertEqual(len(orchestrator.positions_held["test_controller"]), 2)

        # Check first position (ETH-USDT BUY)
        eth_position = orchestrator.positions_held["test_controller"][0]
        self.assertEqual(eth_position.connector_name, "binance")
        self.assertEqual(eth_position.trading_pair, "ETH-USDT")
        self.assertEqual(eth_position.side, TradeType.BUY)
        self.assertEqual(eth_position.buy_amount_base, Decimal("2"))
        self.assertTrue(eth_position.buy_amount_quote.is_nan())  # Initially NaN
        self.assertEqual(eth_position.volume_traded_quote, Decimal("0"))  # Fresh start
        self.assertEqual(eth_position.cum_fees_quote, Decimal("0"))  # Fresh start

        # Check second position (BTC-USDT SELL)
        btc_position = orchestrator.positions_held["test_controller"][1]
        self.assertEqual(btc_position.connector_name, "binance")
        self.assertEqual(btc_position.trading_pair, "BTC-USDT")
        self.assertEqual(btc_position.side, TradeType.SELL)
        self.assertEqual(btc_position.sell_amount_base, Decimal("0.1"))
        self.assertTrue(btc_position.sell_amount_quote.is_nan())  # Initially NaN
        self.assertEqual(btc_position.volume_traded_quote, Decimal("0"))  # Fresh start
        self.assertEqual(btc_position.cum_fees_quote, Decimal("0"))  # Fresh start

        # Test that lazy calculation works when getting position summary
        eth_summary = eth_position.get_position_summary(Decimal("230"))
        self.assertEqual(eth_position.buy_amount_quote, Decimal("2") * Decimal("230"))  # Now calculated
        self.assertEqual(eth_summary.breakeven_price, Decimal("230"))

        btc_summary = btc_position.get_position_summary(Decimal("230"))
        self.assertEqual(btc_position.sell_amount_quote, Decimal("0.1") * Decimal("230"))  # Now calculated
        self.assertEqual(btc_summary.breakeven_price, Decimal("230"))

    # This fixture exercises aggregation; exact receipt reconciliation has its own public-path tests.
    @patch.object(ExecutorOrchestrator, "_reconcile_positions_from_trade_fills", lambda self: None)
    def test_get_all_reports_with_done_position_hold_executors(self):
        """Test get_all_reports with executors that need position updates"""
        # This tests the high-level functionality that exercises lines 413,415,423-424,426,428-430,433,436,438-439,442,447-448

        # Create an executor that meets criteria for position hold processing
        config = PositionExecutorConfig(
            timestamp=1234, trading_pair="ETH-USDT", connector_name="binance",
            side=TradeType.BUY, amount=Decimal(10), entry_price=Decimal(100),
        )
        config.id = "test_executor_id"

        executor = MagicMock()
        executor.executor_info = ExecutorInfo(
            id="test_executor_id", timestamp=1234, type="position_executor",
            status=RunnableStatus.TERMINATED, config=config,
            filled_amount_quote=Decimal(1000), net_pnl_quote=Decimal(50), net_pnl_pct=Decimal(5),
            cum_fees_quote=Decimal(5), is_trading=False, is_active=False,
            custom_info={"held_position_orders": [
                {"client_order_id": "order_1", "executed_amount_base": Decimal("5"),
                 "executed_amount_quote": Decimal("1000"), "trade_type": "BUY",
                 "cumulative_fee_paid_quote": Decimal("5")}
            ]},
            close_type=CloseType.POSITION_HOLD,
            connector_name="binance",
            trading_pair="ETH-USDT"
        )
        # Since is_done is a computed property based on status, and we set status=TERMINATED, is_done will be True

        # Set up orchestrator with the executor
        self.orchestrator.active_executors = {"test_controller": [executor]}
        self.orchestrator.positions_held = {"test_controller": []}
        self.orchestrator.executors_ids_position_held = []
        self.orchestrator.cached_performance = {"test_controller": PerformanceReport()}

        # Call get_all_reports which should trigger position processing
        result = self.orchestrator.get_all_reports()

        # Verify that the executor was processed and position created
        self.assertIn("test_executor_id", self.orchestrator.executors_ids_position_held)
        self.assertEqual(len(self.orchestrator.positions_held["test_controller"]), 1)

        # Verify the position was created correctly
        position = self.orchestrator.positions_held["test_controller"][0]
        self.assertEqual(position.connector_name, "binance")
        self.assertEqual(position.trading_pair, "ETH-USDT")
        self.assertEqual(position.side, TradeType.BUY)

        # Verify report structure
        self.assertIn("test_controller", result)
        self.assertIn("executors", result["test_controller"])
        self.assertIn("positions", result["test_controller"])
        self.assertIn("performance", result["test_controller"])

    @patch.object(MarketsRecorder, "get_instance")
    def test_get_all_reports_persists_position_hold_ledger_for_done_executors(self, markets_recorder_mock):
        markets_recorder_mock.return_value = MagicMock(spec=MarketsRecorder)
        markets_recorder_mock.return_value.store_or_update_position_hold_ledger_entries = MagicMock(return_value=None)
        markets_recorder_mock.return_value.get_all_executors = MagicMock(return_value=[])
        markets_recorder_mock.return_value.get_all_positions = MagicMock(return_value=[])
        markets_recorder_mock.return_value.get_all_position_hold_ledger_entries = MagicMock(return_value=[])

        config = PositionExecutorConfig(
            timestamp=1234,
            trading_pair="ETH-USDT",
            connector_name="binance",
            side=TradeType.BUY,
            amount=Decimal("1"),
            entry_price=Decimal("100"),
        )
        config.id = "persist-ledger"

        executor = MagicMock()
        executor.executor_info = ExecutorInfo(
            id="persist-ledger",
            timestamp=1234,
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            config=config,
            filled_amount_quote=Decimal("100"),
            net_pnl_quote=Decimal("0"),
            net_pnl_pct=Decimal("0"),
            cum_fees_quote=Decimal("1"),
            is_trading=False,
            is_active=False,
            custom_info={"held_position_orders": [
                {"client_order_id": "order_1", "executed_amount_base": Decimal("1"),
                 "executed_amount_quote": Decimal("100"), "trade_type": "BUY",
                 "cumulative_fee_paid_quote": Decimal("1")}
            ]},
            close_type=CloseType.POSITION_HOLD,
            controller_id="test_controller",
        )

        self.orchestrator.active_executors = {"test_controller": [executor]}
        self.orchestrator.positions_held = {"test_controller": []}
        self.orchestrator.executors_ids_position_held = []
        self.orchestrator.cached_performance = {"test_controller": PerformanceReport()}

        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            self.orchestrator.get_all_reports()

        markets_recorder_mock.return_value.store_or_update_position_hold_ledger_entries.assert_called_once()

    def test_get_all_reports_with_perpetual_executors(self):
        """Test get_all_reports with perpetual market executors to exercise position side logic"""
        # This tests lines 454-456,458-460,462-465,467 through high-level functionality

        from hummingbot.core.data_type.common import PositionAction, PositionMode
        from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig

        # Create config with position_action for perpetual market using OrderExecutorConfig
        config = OrderExecutorConfig(
            timestamp=1234, trading_pair="ETH-USDT", connector_name="binance_perpetual",
            side=TradeType.BUY, amount=Decimal(10), execution_strategy=ExecutionStrategy.MARKET,
            position_action=PositionAction.CLOSE
        )
        config.id = "perp_executor_id"

        executor = MagicMock()
        executor.executor_info = ExecutorInfo(
            id="perp_executor_id", timestamp=1234, type="order_executor",
            status=RunnableStatus.TERMINATED, config=config,
            filled_amount_quote=Decimal(1000), net_pnl_quote=Decimal(50), net_pnl_pct=Decimal(5),
            cum_fees_quote=Decimal(5), is_trading=False, is_active=False,
            custom_info={"held_position_orders": [
                {"client_order_id": "order_2", "executed_amount_base": Decimal("3"),
                 "executed_amount_quote": Decimal("600"), "trade_type": "SELL",
                 "cumulative_fee_paid_quote": Decimal("3")}
            ]},
            close_type=CloseType.POSITION_HOLD,
            connector_name="binance_perpetual",
            trading_pair="ETH-USDT"
        )
        # Since status=TERMINATED, is_done will be True

        # Set up perpetual market with HEDGE mode
        mock_market = MagicMock()
        mock_market.position_mode = PositionMode.HEDGE
        self.mock_strategy.connectors = {"binance_perpetual": mock_market}

        # Set up orchestrator
        self.orchestrator.active_executors = {"perp_controller": [executor]}
        self.orchestrator.positions_held = {"perp_controller": []}
        self.orchestrator.executors_ids_position_held = []
        self.orchestrator.cached_performance = {"perp_controller": PerformanceReport()}

        # Call get_all_reports
        self.orchestrator.get_all_reports()

        # Verify that the executor was processed
        self.assertIn("perp_executor_id", self.orchestrator.executors_ids_position_held)
        self.assertEqual(len(self.orchestrator.positions_held["perp_controller"]), 1)

        # Verify the position side logic was applied (CLOSE action should use opposite side)
        position = self.orchestrator.positions_held["perp_controller"][0]
        self.assertEqual(position.side, TradeType.SELL)  # Opposite of BUY due to CLOSE action

    # This fixture exercises aggregation; exact receipt reconciliation has its own public-path tests.
    @patch.object(ExecutorOrchestrator, "_reconcile_positions_from_trade_fills", lambda self: None)
    def test_get_all_reports_with_existing_positions(self):
        """Test get_all_reports with existing positions to exercise find_existing_position logic"""
        # This tests lines 475-476,480-482,485,487 through high-level functionality

        # Create existing position
        existing_position = PositionHold("binance", "ETH-USDT", TradeType.BUY)
        existing_position.buy_amount_base = Decimal("2")
        existing_position.buy_amount_quote = Decimal("400")
        existing_position.volume_traded_quote = Decimal("400")

        # Create executor that should add to existing position
        config = PositionExecutorConfig(
            timestamp=1234, trading_pair="ETH-USDT", connector_name="binance",
            side=TradeType.BUY, amount=Decimal(10), entry_price=Decimal(100),
        )
        config.id = "add_to_position_id"

        executor = MagicMock()
        executor.executor_info = ExecutorInfo(
            id="add_to_position_id", timestamp=1234, type="position_executor",
            status=RunnableStatus.TERMINATED, config=config,
            filled_amount_quote=Decimal(600), net_pnl_quote=Decimal(30), net_pnl_pct=Decimal(5),
            cum_fees_quote=Decimal(3), is_trading=False, is_active=False,
            custom_info={"held_position_orders": [
                {"client_order_id": "order_3", "executed_amount_base": Decimal("3"),
                 "executed_amount_quote": Decimal("600"), "trade_type": "BUY",
                 "cumulative_fee_paid_quote": Decimal("3")}
            ]},
            close_type=CloseType.POSITION_HOLD
        )
        # Since status=TERMINATED, is_done will be True

        # Set up orchestrator with existing position
        self.orchestrator.active_executors = {"existing_pos_controller": [executor]}
        self.orchestrator.positions_held = {"existing_pos_controller": [existing_position]}
        self.orchestrator.executors_ids_position_held = []
        self.orchestrator.cached_performance = {"existing_pos_controller": PerformanceReport()}

        # Call get_all_reports
        result = self.orchestrator.get_all_reports()

        # Verify that the executor was processed and added to existing position
        self.assertIn("add_to_position_id", self.orchestrator.executors_ids_position_held)
        self.assertEqual(len(self.orchestrator.positions_held["existing_pos_controller"]), 1)  # Still one position

        # Verify the existing position was updated
        self.assertEqual(existing_position.buy_amount_base, Decimal("5"))  # 2 + 3
        self.assertEqual(existing_position.buy_amount_quote, Decimal("1000"))  # 400 + 600
        self.assertEqual(existing_position.volume_traded_quote, Decimal("1000"))  # 400 + 600

        # Verify position appears in the report
        positions_in_report = result["existing_pos_controller"]["positions"]
        self.assertEqual(len(positions_in_report), 1)
        self.assertEqual(positions_in_report[0].amount, Decimal("5"))

    # This fixture exercises aggregation; exact receipt reconciliation has its own public-path tests.
    @patch.object(ExecutorOrchestrator, "_reconcile_positions_from_trade_fills", lambda self: None)
    def test_get_all_reports_comprehensive_controller_aggregation(self):
        """Test get_all_reports aggregating controllers from different sources"""
        # This tests lines 545,548-549,552,557 comprehensively

        # Set up orchestrator with controllers spread across different data structures
        # Controller 1: Has active executors only
        self.orchestrator.active_executors = {"controller1": [MagicMock()]}

        # Controller 2: Has positions only
        position = PositionHold("binance", "BTC-USDT", TradeType.SELL)
        self.orchestrator.positions_held = {"controller2": [position]}

        # Controller 3: Has cached performance only
        self.orchestrator.cached_performance = {"controller3": PerformanceReport()}

        # Controller 4: Has multiple data types
        self.orchestrator.active_executors["controller4"] = [MagicMock()]
        self.orchestrator.positions_held["controller4"] = [PositionHold("binance", "ADA-USDT", TradeType.BUY)]
        self.orchestrator.cached_performance["controller4"] = PerformanceReport()

        # Call get_all_reports
        result = self.orchestrator.get_all_reports()

        # Verify all controllers are included
        expected_controllers = {"controller1", "controller2", "controller3", "controller4"}
        self.assertEqual(set(result.keys()), expected_controllers)

        # Verify each controller has the expected structure
        for controller_id in expected_controllers:
            self.assertIn("executors", result[controller_id])
            self.assertIn("positions", result[controller_id])
            self.assertIn("performance", result[controller_id])

        # Verify that controllers with no data have empty lists/reports
        self.assertEqual(len(result["controller1"]["positions"]), 0)
        self.assertEqual(len(result["controller2"]["executors"]), 0)
        self.assertEqual(len(result["controller3"]["executors"]), 0)
        self.assertEqual(len(result["controller3"]["positions"]), 0)

    def test_get_all_reports_includes_stored_recent_executors(self):
        stored_executor = ExecutorInfo(
            id="stored-executor",
            timestamp=1234,
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            close_type=CloseType.POSITION_HOLD,
            close_timestamp=1250,
            config=PositionExecutorConfig(
                id="stored-executor",
                timestamp=1234,
                trading_pair="ETH-USDT",
                connector_name="binance",
                side=TradeType.BUY,
                amount=Decimal(10),
                entry_price=Decimal(100),
            ),
            filled_amount_quote=Decimal(100),
            net_pnl_quote=Decimal(10),
            net_pnl_pct=Decimal("0.1"),
            cum_fees_quote=Decimal("1"),
            is_trading=False,
            is_active=False,
            custom_info={"current_position_average_price": Decimal("100"), "persistence_source": "db"},
            controller_id="stored-controller",
        )
        self.orchestrator.active_executors = {"stored-controller": []}
        self.orchestrator.stored_executors = {"stored-controller": [stored_executor]}
        self.orchestrator.positions_held = {"stored-controller": []}
        self.orchestrator.cached_performance = {"stored-controller": PerformanceReport()}

        result = self.orchestrator.get_all_reports()

        self.assertEqual(len(result["stored-controller"]["executors"]), 1)
        self.assertEqual(result["stored-controller"]["executors"][0].id, "stored-executor")
