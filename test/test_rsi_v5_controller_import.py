import asyncio
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction

ROOT_PATH = Path(__file__).resolve().parent.parent
ROOT_PATH_STR = str(ROOT_PATH)
while ROOT_PATH_STR in sys.path:
    sys.path.remove(ROOT_PATH_STR)
sys.path.insert(0, ROOT_PATH_STR)


class RSIv5ControllerImportTest(unittest.TestCase):
    def test_rsi_v5_controller_imports_from_local_hummingbot_tree(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        self.assertIsNotNone(RSIv5Controller)
        self.assertIsNotNone(RSIv5ControllerConfig)

    def test_rsi_v5_buy_executor_config_uses_supported_barrier_order_types(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_trading_rules.return_value = SimpleNamespace(min_price_increment=Decimal("0.01"))
        market_data_provider.get_price_by_type.side_effect = lambda connector, pair, price_type: (
            Decimal("100.00") if price_type == PriceType.BestBid else Decimal("100.50")
        )

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {"signal_state": {"size_multiplier": 1.0}}

        executor_config = controller.get_executor_config(
            trade_type=TradeType.BUY,
            price=Decimal("100"),
            amount=Decimal("1"),
        )

        self.assertEqual(executor_config.triple_barrier_config.open_order_type, OrderType.LIMIT_MAKER)
        self.assertEqual(executor_config.entry_price, Decimal("99.99"))
        self.assertEqual(
            executor_config.triple_barrier_config.take_profit_order_type,
            controller.config.take_profit_order_type,
        )
        self.assertEqual(executor_config.triple_barrier_config.trailing_stop_order_type, OrderType.LIMIT)
        self.assertEqual(executor_config.triple_barrier_config.trailing_stop_limit_max_age_seconds, 3)
        self.assertEqual(executor_config.triple_barrier_config.stop_loss_order_type, OrderType.MARKET)
        self.assertEqual(executor_config.triple_barrier_config.time_limit_order_type, OrderType.MARKET)

    def test_rsi_v5_interval_candles_helper_returns_copy(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        candles = pd.DataFrame({"close": [Decimal("100"), Decimal("101")]})
        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_candles_df.return_value = candles

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                trend_confirmation_medium_interval="5m",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        result = controller._get_interval_candles_df("5m")

        self.assertIsNotNone(result)
        self.assertEqual(list(result["close"]), [Decimal("100"), Decimal("101")])
        self.assertIsNot(result, candles)

    def test_rsi_v5_operator_entry_pause_sets_state_without_explicit_timestamp(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("100")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        controller.set_operator_entry_pause(True, reason="manual", scope="pair", command_id="cmd-1")

        state = controller.get_operator_state()
        self.assertTrue(state["entry_paused"])
        self.assertEqual(state["reason"], "manual")
        self.assertEqual(state["scope"], "pair")
        self.assertEqual(state["last_command_id"], "cmd-1")
        self.assertIsNotNone(state["updated_at"])

    def test_rsi_v5_sell_executor_config_shades_one_tick_above_best_ask(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_trading_rules.return_value = SimpleNamespace(min_price_increment=Decimal("0.01"))
        market_data_provider.get_price_by_type.side_effect = lambda connector, pair, price_type: (
            Decimal("100.00") if price_type == PriceType.BestBid else Decimal("100.50")
        )

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {"signal_state": {"size_multiplier": 1.0}}
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="BNB-USDT",
                side=TradeType.BUY,
                amount=Decimal("0.5"),
            )
        ]

        executor_config = controller.get_executor_config(
            trade_type=TradeType.SELL,
            price=Decimal("100"),
            amount=Decimal("1"),
        )

        self.assertEqual(executor_config.price, Decimal("100.51"))

    def test_rsi_v5_buy_executor_config_quantizes_off_grid_touch_price(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_trading_rules.return_value = SimpleNamespace(min_price_increment=Decimal("0.01"))
        market_data_provider.get_price_by_type.side_effect = lambda connector, pair, price_type: (
            Decimal("100.005") if price_type == PriceType.BestBid else Decimal("100.50")
        )

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {"signal_state": {"size_multiplier": 1.0}}

        executor_config = controller.get_executor_config(
            trade_type=TradeType.BUY,
            price=Decimal("101"),
            amount=Decimal("1"),
        )

        self.assertEqual(executor_config.entry_price, Decimal("100.00"))

    def test_rsi_v5_buy_executor_config_uplifts_undersized_scout_to_min_notional(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_trading_rules.return_value = SimpleNamespace(
            min_price_increment=Decimal("0.01"),
            min_order_size=Decimal("0"),
            min_base_amount_increment=Decimal("0.0001"),
            min_notional_size=Decimal("5"),
            min_order_value=Decimal("5"),
        )
        market_data_provider.get_price_by_type.side_effect = lambda connector, pair, price_type: (
            Decimal("2146.60") if price_type == PriceType.BestBid else Decimal("2146.80")
        )
        market_data_provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                usd_per_entry=Decimal("25"),
                dynamic_position_sizing=True,
                use_split_entries=True,
                scout_entry_fraction=Decimal("0.3"),
                hostile_trend_scout_fraction=Decimal("0.1"),
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal_state": {
                "buy_entry_role": "scout",
                "buy_context_bias": "hostile",
                "size_multiplier": 0.5,
            }
        }

        executor_config = controller.get_executor_config(
            trade_type=TradeType.BUY,
            price=Decimal("2146.595"),
            amount=Decimal("1"),
        )

        self.assertGreaterEqual(executor_config.amount * executor_config.entry_price, Decimal("5"))

    def test_rsi_v5_max_position_gate_uses_min_notional_uplifted_buy_size(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1000.0
        market_data_provider.get_price_by_type.return_value = Decimal("2146.595")
        market_data_provider.get_balance.return_value = Decimal("1000")
        market_data_provider.get_trading_rules.return_value = SimpleNamespace(
            min_order_size=Decimal("0"),
            min_base_amount_increment=Decimal("0.0001"),
            min_notional_size=Decimal("5"),
            min_order_value=Decimal("5"),
        )
        market_data_provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                usd_per_entry=Decimal("25"),
                max_total_position_usd=Decimal("27"),
                dynamic_position_sizing=True,
                use_split_entries=True,
                scout_entry_fraction=Decimal("0.3"),
                hostile_trend_scout_fraction=Decimal("0.1"),
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal_state": {
                "condition_ok": True,
                "buy_entry_role": "scout",
                "buy_context_bias": "hostile",
                "size_multiplier": 0.5,
            }
        }
        controller.executors_info = [
            SimpleNamespace(
                is_active=True,
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                timestamp=0.0,
                filled_amount_quote=Decimal("23"),
                custom_info={"current_position_average_price": Decimal("2140")},
            )
        ]

        self.assertFalse(controller.can_create_executor(1))
        self.assertEqual(controller._last_gate_reason[TradeType.BUY], "max_position")

    def test_rsi_v5_recovery_sell_amount_uplifts_partial_to_min_notional(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_trading_rules.return_value = SimpleNamespace(
            min_order_size=Decimal("0"),
            min_base_amount_increment=Decimal("0.00000001"),
            min_notional_size=Decimal("5"),
            min_order_value=Decimal("5"),
        )
        market_data_provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BTC-USDC",
                recovery_partial_exit_fraction=0.15,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        amount = controller._resolve_recovery_sell_amount(
            position_amount=Decimal("0.00044"),
            target_amount=Decimal("0.00044") * Decimal("0.15"),
            price=Decimal("72000"),
            available_base=Decimal("0.00044"),
        )

        self.assertGreaterEqual(amount * Decimal("72000"), Decimal("5"))
        self.assertLess(amount, Decimal("0.00044"))

    def test_rsi_v5_recovery_sell_amount_cleans_up_dust_remainder(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_trading_rules.return_value = SimpleNamespace(
            min_order_size=Decimal("0"),
            min_base_amount_increment=Decimal("0.00000001"),
            min_notional_size=Decimal("5"),
            min_order_value=Decimal("5"),
        )
        market_data_provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BTC-USDC",
                recovery_partial_exit_fraction=0.15,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        amount = controller._resolve_recovery_sell_amount(
            position_amount=Decimal("0.00008"),
            target_amount=Decimal("0.00008") * Decimal("0.15"),
            price=Decimal("72000"),
            available_base=Decimal("0.00008"),
        )

        self.assertEqual(amount, Decimal("0.00008"))

    def test_rsi_v5_live_buy_reversal_is_suppressed_on_same_candle(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        self.assertTrue(controller._should_emit_live_buy_reversal(True, 1710785820.0))
        self.assertFalse(controller._should_emit_live_buy_reversal(True, 1710785820.0))
        self.assertTrue(controller._should_emit_live_buy_reversal(True, 1710785880.0))

    def test_rsi_v5_active_buy_executor_counts_toward_position_utilization_and_max_position_gate(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1000.0
        market_data_provider.get_price_by_type.return_value = Decimal("110")
        market_data_provider.get_balance.return_value = Decimal("0")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                usd_per_entry=Decimal("20"),
                max_total_position_usd=Decimal("40"),
                dynamic_position_sizing=False,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {"signal_state": {"condition_ok": True}}
        controller.executors_info = [
            SimpleNamespace(
                is_active=True,
                connector_name="binance",
                trading_pair="BNB-USDT",
                side=TradeType.BUY,
                timestamp=0.0,
                filled_amount_quote=Decimal("25"),
                custom_info={"current_position_average_price": Decimal("100")},
            )
        ]

        self.assertAlmostEqual(controller._get_position_utilization(), 27.5 / 40.0, places=6)
        self.assertFalse(controller.can_create_executor(1))
        self.assertEqual(controller._last_gate_reason[TradeType.BUY], "max_position")

    def test_rsi_v5_buy_signal_log_includes_controller_and_pair_context(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("100")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        with patch.object(controller, "logger") as mock_logger:
            signal = controller._determine_signal(
                last_rsi=31.02,
                signal_score=3,
                rsi_buy_eff=35.0,
                rsi_sell_eff=72.0,
                last_close=Decimal("100"),
                raw_buy_reversal=True,
                signal_timestamp=1710785820.0,
                candle_timestamp=1710785820.0,
            )

        self.assertEqual(signal, 1)
        log_line = mock_logger.return_value.info.call_args[0][0]
        self.assertIn("controller=rsi-v5-test", log_line)
        self.assertIn("pair=BNB-USDT", log_line)
        self.assertIn("active_buy_execs=0", log_line)

    def test_rsi_v5_rebound_confirmation_arms_then_confirms_buy(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("100")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                buy_confirmation_mode="rebound_confirm",
                buy_confirmation_rsi_delta=1.0,
                buy_confirmation_max_wait_seconds=75,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        with patch.object(controller, "logger") as mock_logger:
            first_signal = controller._determine_signal(
                last_rsi=29.58,
                signal_score=3,
                rsi_buy_eff=35.0,
                rsi_sell_eff=72.0,
                last_close=Decimal("2195.54"),
                raw_buy_reversal=True,
                signal_timestamp=1710785823.0,
                candle_timestamp=1710785820.0,
            )

            second_signal = controller._determine_signal(
                last_rsi=27.86,
                signal_score=3,
                rsi_buy_eff=35.0,
                rsi_sell_eff=72.0,
                last_close=Decimal("2190.00"),
                raw_buy_reversal=False,
                signal_timestamp=1710785853.0,
                candle_timestamp=1710785820.0,
            )

            final_signal = controller._determine_signal(
                last_rsi=28.90,
                signal_score=3,
                rsi_buy_eff=35.0,
                rsi_sell_eff=72.0,
                last_close=Decimal("2191.50"),
                raw_buy_reversal=False,
                signal_timestamp=1710785865.0,
                candle_timestamp=1710785880.0,
            )

        self.assertEqual(first_signal, 0)
        self.assertEqual(second_signal, 0)
        self.assertEqual(final_signal, 1)
        self.assertIsNone(controller._pending_buy_confirmation)

        info_messages = [call.args[0] for call in mock_logger.return_value.info.call_args_list]
        self.assertTrue(any("BUY setup armed" in message for message in info_messages))
        self.assertTrue(any("BUY not ready" in message and "reason=need-rebound" in message for message in info_messages))
        self.assertTrue(any("BUY signal (rebound-confirmed)" in message for message in info_messages))

    def test_rsi_v5_rebound_confirmation_preserves_arm_time_score_quality(self):
        from controllers.directional_trading.rsi_v5 import BuyConfirmationSetup, RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("100")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                buy_confirmation_mode="rebound_confirm",
                buy_confirmation_rsi_delta=1.0,
                min_signal_score=3,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller._pending_buy_confirmation = BuyConfirmationSetup(
            armed_timestamp=1710785820.0,
            trough_rsi=18.0,
            trough_price=99.0,
            armed_signal_score=3,
            armed_min_signal_score=3,
        )

        with patch.object(controller, "logger") as mock_logger:
            signal = controller._determine_signal(
                last_rsi=20.5,
                signal_score=2,
                rsi_buy_eff=35.0,
                rsi_sell_eff=72.0,
                last_close=Decimal("101"),
                raw_buy_reversal=False,
                signal_timestamp=1710785825.0,
                candle_timestamp=1710785880.0,
            )

        self.assertEqual(signal, 1)
        info_messages = [call.args[0] for call in mock_logger.return_value.info.call_args_list]
        self.assertTrue(any("BUY signal (rebound-confirmed)" in message for message in info_messages))
        self.assertFalse(any("BUY not ready" in message and "reason=need-score" in message for message in info_messages))
        self.assertIsNone(controller._pending_buy_confirmation)

    def test_rsi_v5_split_entries_emit_scout_then_runner_signals(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("100")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                buy_confirmation_mode="rebound_confirm",
                use_split_entries=True,
                scout_entry_fraction=0.35,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        with patch.object(controller, "logger") as mock_logger:
            scout_signal = controller._determine_signal(
                last_rsi=29.2,
                signal_score=3,
                rsi_buy_eff=35.0,
                rsi_sell_eff=72.0,
                last_close=Decimal("100.00"),
                raw_buy_reversal=True,
                signal_timestamp=1710785823.0,
                candle_timestamp=1710785820.0,
            )

            self.assertEqual(scout_signal, 1)
            self.assertEqual(controller._last_buy_signal_role, "scout")
            self.assertIsNotNone(controller._pending_buy_confirmation)

            controller.executors_info = [
                SimpleNamespace(
                    is_active=True,
                    connector_name="binance",
                    trading_pair="BNB-USDT",
                    side=TradeType.BUY,
                    timestamp=1710785823.0,
                    filled_amount_quote=Decimal("8.75"),
                    custom_info={"level_id": "scout", "role": "scout", "current_position_average_price": Decimal("100.00")},
                )
            ]

            runner_signal = controller._determine_signal(
                last_rsi=30.6,
                signal_score=3,
                rsi_buy_eff=35.0,
                rsi_sell_eff=72.0,
                last_close=Decimal("101.00"),
                raw_buy_reversal=False,
                signal_timestamp=1710785885.0,
                candle_timestamp=1710785880.0,
            )

        self.assertEqual(runner_signal, 1)
        self.assertEqual(controller._last_buy_signal_role, "runner")
        self.assertIsNone(controller._pending_buy_confirmation)

        info_messages = [call.args[0] for call in mock_logger.return_value.info.call_args_list]
        self.assertTrue(any("BUY signal (scout-reversal)" in message for message in info_messages))
        self.assertTrue(any("BUY signal (runner-confirmed)" in message for message in info_messages))

    def test_rsi_v5_scout_signal_log_uses_effective_hostile_fraction(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("100")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                buy_confirmation_mode="rebound_confirm",
                use_split_entries=True,
                scout_entry_fraction=Decimal("0.3"),
                hostile_trend_scout_fraction=Decimal("0.1"),
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "regime": "LV_Trend_Down",
            "signal_state": {"buy_context_bias": "hostile"},
        }

        with patch.object(controller, "logger") as mock_logger:
            signal = controller._determine_signal(
                last_rsi=31.0,
                signal_score=3,
                rsi_buy_eff=35.0,
                rsi_sell_eff=72.0,
                last_close=Decimal("100"),
                raw_buy_reversal=True,
                signal_timestamp=1710785820.0,
                candle_timestamp=1710785820.0,
            )

        self.assertEqual(signal, 1)
        info_messages = [call.args[0] for call in mock_logger.return_value.info.call_args_list]
        scout_messages = [message for message in info_messages if "BUY signal (scout-reversal)" in message]
        self.assertTrue(scout_messages)
        self.assertIn("scout=10%", scout_messages[0])

    def test_rsi_v5_runner_entry_bypasses_scout_cooldown_and_gap_and_uses_remaining_budget(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_trading_rules.return_value = SimpleNamespace(min_price_increment=Decimal("0.01"))
        market_data_provider.get_price_by_type.side_effect = lambda connector, pair, price_type: (
            Decimal("100.00") if price_type != PriceType.BestAsk else Decimal("100.50")
        )
        market_data_provider.get_balance.return_value = Decimal("1000")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                usd_per_entry=Decimal("25"),
                cooldown_time=300,
                executor_price_gap_threshold=Decimal("0.02"),
                dynamic_position_sizing=False,
                buy_confirmation_mode="rebound_confirm",
                use_split_entries=True,
                scout_entry_fraction=0.4,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal": 1,
            "timestamp": 1234567890.0,
            "signal_state": {
                "timestamp": 1234567890.0,
                "condition_ok": True,
                "buy_entry_role": "runner",
                "buy_entry_fraction": 0.6,
                "buy_context_bias": "neutral",
                "size_multiplier": 1.0,
                "signal_score": 3,
            },
        }
        controller.executors_info = [
            SimpleNamespace(
                is_active=True,
                connector_name="binance",
                trading_pair="BNB-USDT",
                side=TradeType.BUY,
                timestamp=1234567880.0,
                filled_amount_quote=Decimal("10"),
                net_pnl_pct=0.0,
                custom_info={"level_id": "scout", "role": "scout", "current_position_average_price": Decimal("100.00")},
            )
        ]
        controller._has_sufficient_price_gap = MagicMock(return_value=False)

        self.assertTrue(controller.can_create_executor(1))
        self.assertEqual(controller._last_gate_reason[TradeType.BUY], "ready")

        executor_config = controller.get_executor_config(
            trade_type=TradeType.BUY,
            price=Decimal("100"),
            amount=Decimal("1"),
        )

        self.assertEqual(executor_config.level_id, "runner")
        self.assertEqual(executor_config.amount, Decimal("0.15000000"))

    def test_rsi_v5_rebound_confirmation_can_be_restored_after_post_signal_veto(self):
        from controllers.directional_trading.rsi_v5 import BuyConfirmationSetup, RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("100")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                buy_confirmation_mode="rebound_confirm",
                buy_confirmation_rsi_delta=1.0,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        previous_setup = BuyConfirmationSetup(
            armed_timestamp=1710785820.0,
            trough_rsi=28.0,
            trough_price=99.0,
        )
        controller._pending_buy_confirmation = previous_setup

        signal = controller._determine_signal(
            last_rsi=29.4,
            signal_score=3,
            rsi_buy_eff=35.0,
            rsi_sell_eff=72.0,
            last_close=Decimal("100.50"),
            raw_buy_reversal=False,
            signal_timestamp=1710785880.0,
            candle_timestamp=1710785880.0,
        )

        self.assertEqual(signal, 1)
        self.assertIsNone(controller._pending_buy_confirmation)

        controller._restore_buy_confirmation_after_veto(previous_setup)

        self.assertIsNotNone(controller._pending_buy_confirmation)
        self.assertEqual(controller._pending_buy_confirmation.trough_rsi, 28.0)

    def test_rsi_v5_backtest_signal_frame_models_scout_runner_and_atr_veto(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                rsi_length=4,
                min_signal_score=1,
                buy_confirmation_mode="rebound_confirm",
                use_split_entries=True,
                scout_entry_fraction=0.35,
                min_atr_pct_to_trade=Decimal("0.01"),
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        df = pd.DataFrame(
            {
                "timestamp": [1.0, 2.0, 3.0, 4.0],
                "close": [100.0, 101.0, 101.5, 102.0],
                "high": [100.5, 101.5, 102.0, 102.5],
                "low": [99.5, 100.5, 101.0, 101.5],
                "RSI_4": [29.0, 30.5, 30.8, 31.2],
                "BBL_20_2.0": [101.0, 101.0, 101.0, 101.0],
                "MACDh_12_26_9": [0.0, 0.0, 0.0, 0.0],
                "EMA_9": [99.5, 100.0, 100.5, 101.0],
            }
        )

        with patch.object(
            controller,
            "_compute_raw_buy_reversal_series",
            return_value=pd.Series([True, False, False, False], index=df.index, dtype=bool),
        ), patch.object(
            controller,
            "_compute_atr_series",
            return_value=(pd.Series([2.0, 0.2, 2.0, 2.0], index=df.index), 2.0),
        ):
            signal_frame = controller._build_backtest_signal_frame(
                df,
                rsi_buy_eff=35.0,
                rsi_sell_eff=75.0,
            )

        self.assertEqual(signal_frame["signal"].tolist(), [1, 0, 1, 0])
        self.assertEqual(signal_frame["signal_state"].iloc[0]["buy_entry_role"], "scout")
        self.assertEqual(signal_frame["signal_state"].iloc[0]["buy_entry_fraction"], 0.35)
        self.assertEqual(signal_frame["signal_state"].iloc[1]["condition_reason"], "low_vol")
        self.assertEqual(signal_frame["signal_state"].iloc[2]["buy_entry_role"], "runner")
        self.assertEqual(signal_frame["signal_state"].iloc[2]["buy_entry_fraction"], 0.65)

    def test_rsi_v5_sell_not_ready_reports_inventory_flat_before_cost_basis(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        with patch.object(controller, "logger") as mock_logger:
            signal = controller._determine_signal(
                last_rsi=76.0,
                signal_score=0,
                rsi_buy_eff=35.0,
                rsi_sell_eff=74.0,
                last_close=Decimal("100"),
                raw_buy_reversal=False,
                signal_timestamp=1710785820.0,
                candle_timestamp=1710785820.0,
            )

        self.assertEqual(signal, 0)
        info_messages = [call.args[0] for call in mock_logger.return_value.info.call_args_list]
        self.assertTrue(any("SELL not ready" in message and "reason=inventory-flat" in message for message in info_messages))
        self.assertFalse(any("SELL not ready" in message and "reason=no-cost-basis" in message for message in info_messages))

    def test_rsi_v5_sell_requires_reversal_confirmation_before_signal(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("101.50")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.01"),
                breakeven_price=Decimal("100.00"),
            )
        ]

        with patch.object(controller, "logger") as mock_logger:
            signal = controller._determine_signal(
                last_rsi=76.0,
                signal_score=0,
                rsi_buy_eff=35.0,
                rsi_sell_eff=74.0,
                last_close=Decimal("101.50"),
                rsi_prev=75.7,
                ema_fast=101.0,
                macd_hist=0.8,
                macd_hist_prev=0.6,
                raw_buy_reversal=False,
                signal_timestamp=1710785820.0,
                candle_timestamp=1710785820.0,
            )

        self.assertEqual(signal, 0)
        info_messages = [call.args[0] for call in mock_logger.return_value.info.call_args_list]
        self.assertTrue(any("SELL not ready" in message and "reason=need-reversal" in message for message in info_messages))

    def test_rsi_v5_sell_uses_aggregated_cost_basis_for_profit_gate(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("104.00")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                sell_only_if_profitable=True,
                min_profit_pct_for_sell=Decimal("0.01"),
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.4"),
                breakeven_price=Decimal("100.00"),
            ),
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.6"),
                breakeven_price=Decimal("110.00"),
            ),
        ]

        with patch.object(controller, "logger") as mock_logger:
            signal = controller._determine_signal(
                last_rsi=76.0,
                signal_score=0,
                rsi_buy_eff=35.0,
                rsi_sell_eff=74.0,
                last_close=Decimal("104.00"),
                rsi_prev=76.2,
                ema_fast=105.0,
                macd_hist=0.2,
                macd_hist_prev=0.5,
                raw_buy_reversal=False,
                signal_timestamp=1710785820.0,
                candle_timestamp=1710785820.0,
            )

        self.assertEqual(signal, 0)
        self.assertEqual(controller._get_position_cost_basis(), Decimal("106"))
        info_messages = [call.args[0] for call in mock_logger.return_value.info.call_args_list]
        self.assertTrue(any("SELL not ready" in message and "reason=need-profit" in message for message in info_messages))

    def test_rsi_v5_direct_sell_requires_held_inventory_not_only_active_buy_executor(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("101.50")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                sell_only_if_profitable=False,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.executors_info = [
            SimpleNamespace(
                is_active=True,
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                timestamp=1710785800.0,
                filled_amount_quote=Decimal("20"),
                custom_info={"current_position_average_price": Decimal("100.00")},
            )
        ]

        with patch.object(controller, "logger") as mock_logger:
            signal = controller._determine_signal(
                last_rsi=82.0,
                signal_score=0,
                rsi_buy_eff=35.0,
                rsi_sell_eff=74.0,
                last_close=Decimal("101.50"),
                rsi_prev=82.2,
                ema_fast=101.0,
                macd_hist=0.2,
                macd_hist_prev=0.5,
                raw_buy_reversal=False,
                signal_timestamp=1710785820.0,
                candle_timestamp=1710785820.0,
            )

        self.assertEqual(signal, 0)
        info_messages = [call.args[0] for call in mock_logger.return_value.info.call_args_list]
        self.assertTrue(any("SELL not ready" in message and "reason=inventory-flat" in message for message in info_messages))

    def test_rsi_v5_sell_without_profit_gate_still_requires_reversal_confirmation(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("101.50")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                sell_only_if_profitable=False,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.01"),
                breakeven_price=Decimal("100.00"),
            )
        ]

        with patch.object(controller, "logger") as mock_logger:
            first_signal = controller._determine_signal(
                last_rsi=76.0,
                signal_score=0,
                rsi_buy_eff=35.0,
                rsi_sell_eff=74.0,
                last_close=Decimal("101.50"),
                rsi_prev=75.7,
                ema_fast=101.0,
                macd_hist=0.8,
                macd_hist_prev=0.6,
                raw_buy_reversal=False,
                signal_timestamp=1710785820.0,
                candle_timestamp=1710785820.0,
            )
            second_signal = controller._determine_signal(
                last_rsi=75.2,
                signal_score=0,
                rsi_buy_eff=35.0,
                rsi_sell_eff=74.0,
                last_close=Decimal("100.80"),
                rsi_prev=76.1,
                ema_fast=101.0,
                macd_hist=0.4,
                macd_hist_prev=0.7,
                raw_buy_reversal=False,
                signal_timestamp=1710785880.0,
                candle_timestamp=1710785880.0,
            )

        self.assertEqual(first_signal, 0)
        self.assertEqual(second_signal, -1)
        info_messages = [call.args[0] for call in mock_logger.return_value.info.call_args_list]
        self.assertTrue(any("SELL not ready" in message and "reason=need-reversal" in message for message in info_messages))
        self.assertTrue(any("SELL signal" in message for message in info_messages))

    def test_rsi_v5_sell_without_profit_gate_still_respects_trend_hold(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("2143.81")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                sell_only_if_profitable=False,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "trend_confirmation": {"direction": "up", "confidence": 0.72, "alignment_score": 0.72, "states": {}},
            "signal_state": {"rsi": 82.0, "rsi_prev": 82.7, "ema_fast": 2145.0},
        }
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.01"),
                breakeven_price=Decimal("2135.12"),
            )
        ]

        with patch.object(controller, "logger") as mock_logger:
            signal = controller._determine_signal(
                last_rsi=82.0,
                signal_score=0,
                rsi_buy_eff=35.0,
                rsi_sell_eff=74.0,
                last_close=Decimal("2143.81"),
                rsi_prev=82.2,
                ema_fast=2140.0,
                macd_hist=0.9,
                macd_hist_prev=1.1,
                raw_buy_reversal=False,
                signal_timestamp=1710785820.0,
                candle_timestamp=1710785820.0,
            )

        self.assertEqual(signal, 0)
        info_messages = [call.args[0] for call in mock_logger.return_value.info.call_args_list]
        self.assertTrue(any("SELL not ready" in message and "reason=trend-hold" in message for message in info_messages))

    def test_rsi_v5_direct_sell_executor_aggregates_bags_and_caps_to_available_balance(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_trading_rules.return_value = SimpleNamespace(min_price_increment=Decimal("0.01"))
        market_data_provider.get_price_by_type.side_effect = lambda connector, pair, price_type: (
            Decimal("108.50") if price_type == PriceType.BestAsk else Decimal("108.00")
        )
        market_data_provider.get_balance.return_value = Decimal("0.75")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {"signal_state": {}}
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.4"),
                breakeven_price=Decimal("100.00"),
            ),
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.6"),
                breakeven_price=Decimal("110.00"),
            ),
        ]

        self.assertFalse(controller.can_create_executor(-1))
        self.assertEqual(controller._last_gate_reason[TradeType.SELL], "held_bag_recovery")

        executor_config = controller.get_executor_config(
            trade_type=TradeType.SELL,
            price=Decimal("108"),
            amount=Decimal("1"),
        )

        self.assertEqual(executor_config.amount, Decimal("0.75000000"))
        self.assertEqual(executor_config.level_id, "signal_exit")

    def test_rsi_v5_stale_signal_sell_executor_is_cancelled_for_repricing(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_price_by_type.return_value = Decimal("100.00")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                recovery_cancel_stale_order_pct=Decimal("0.0025"),
                use_bag_freeze=False,
                early_stop_drawdown_pct=Decimal("0"),
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal": 0,
            "trend_confirmation": {"direction": "neutral", "confidence": 0.0, "alignment_score": 0.0, "states": {}},
            "signal_state": {},
        }
        controller.executors_info = [
            SimpleNamespace(
                id="sell-1",
                is_active=True,
                type="order_executor",
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.SELL,
                timestamp=1234567800.0,
                config=SimpleNamespace(price=Decimal("101.00")),
                custom_info={"level_id": "signal_exit", "side": TradeType.SELL},
            )
        ]

        actions = controller.stop_actions_proposal()

        self.assertEqual(len(actions), 1)
        self.assertIsInstance(actions[0], StopExecutorAction)
        self.assertEqual(actions[0].executor_id, "sell-1")
        self.assertTrue(actions[0].keep_position)

    def test_rsi_v5_active_buy_executor_underwater_enters_position_hold_without_existing_hold(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_price_by_type.return_value = Decimal("94.99")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                early_stop_drawdown_pct=Decimal("0.05"),
                early_stop_keep_position=True,
                use_bag_freeze=False,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = []
        controller.processed_data = {
            "signal": 0,
            "trend_confirmation": {"direction": "neutral", "confidence": 0.0, "alignment_score": 0.0, "states": {}},
            "signal_state": {},
        }
        controller.executors_info = [
            SimpleNamespace(
                id="buy-1",
                is_active=True,
                type="position_executor",
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                timestamp=1234567800.0,
                config=SimpleNamespace(entry_price=Decimal("100.00")),
                custom_info={},
            )
        ]

        actions = controller.stop_actions_proposal()

        self.assertEqual(len(actions), 1)
        self.assertIsInstance(actions[0], StopExecutorAction)
        self.assertEqual(actions[0].executor_id, "buy-1")
        self.assertTrue(actions[0].keep_position)

    def test_rsi_v5_active_sell_executor_blocks_direct_sell(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_price_by_type.return_value = Decimal("108.00")
        market_data_provider.get_balance.return_value = Decimal("1")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                max_executors_per_side=5,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("1"),
                breakeven_price=Decimal("100.00"),
            ),
        ]
        controller.executors_info = [
            SimpleNamespace(
                is_active=True,
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.SELL,
                timestamp=1234567880.0,
                custom_info={"level_id": "signal_exit"},
            )
        ]

        self.assertFalse(controller.can_create_executor(-1))
        self.assertEqual(controller._last_gate_reason[TradeType.SELL], "sell_active")

    def test_rsi_v5_recent_filled_sell_sets_sell_gap_reference_price(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                executor_price_gap_threshold=Decimal("0.02"),
                use_adaptive_filters=False,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.executors_info = [
            SimpleNamespace(
                is_active=False,
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.SELL,
                timestamp=1234567800.0,
                close_timestamp=1234567850.0,
                filled_amount_quote=Decimal("104.80"),
                custom_info={"level_id": "signal_exit", "order_price": Decimal("105.00")},
            )
        ]

        self.assertEqual(controller._last_same_side_ref_price(TradeType.SELL), Decimal("105.00"))
        self.assertFalse(controller._has_sufficient_price_gap(TradeType.SELL, Decimal("105.20")))

    def test_rsi_v5_recovery_sell_is_blocked_when_last_filled_sell_is_within_gap(self):
        from controllers.directional_trading.rsi_v5 import RecoveryTrailState, RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_trading_rules.return_value = SimpleNamespace(min_price_increment=Decimal("0.01"))
        market_data_provider.get_price_by_type.side_effect = lambda connector, pair, price_type: (
            Decimal("105.20") if price_type != PriceType.BestAsk else Decimal("105.19")
        )
        market_data_provider.get_balance.return_value = Decimal("1")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                use_bag_freeze=False,
                early_stop_drawdown_pct=Decimal("0"),
                bag_recovery_target_pct=Decimal("0.05"),
                recovery_partial_exit_fraction=0.5,
                executor_price_gap_threshold=Decimal("0.01"),
                use_adaptive_filters=False,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        position = SimpleNamespace(
            connector_name="binance",
            trading_pair="ETH-USDC",
            side=TradeType.BUY,
            amount=Decimal("1"),
            breakeven_price=Decimal("100.00"),
        )
        position_key = controller._recovery_position_key(position)
        controller.positions_held = [position]
        controller.executors_info = [
            SimpleNamespace(
                is_active=False,
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.SELL,
                timestamp=1234567800.0,
                close_timestamp=1234567850.0,
                filled_amount_quote=Decimal("52.60"),
                custom_info={"level_id": "recovery_exit:old", "order_price": Decimal("105.20")},
            )
        ]
        controller._recovery_trails[position_key] = RecoveryTrailState(
            position_key=position_key,
            armed_timestamp=1234567800.0,
            arm_price=Decimal("105.00"),
            peak_price=Decimal("106.50"),
            tracked_amount=Decimal("1"),
            peak_rsi=82.0,
            target_profit_pct=Decimal("0.05"),
            last_reason="armed",
        )
        controller.processed_data = {
            "signal": 0,
            "trend_confirmation": {"direction": "neutral", "confidence": 0.0, "alignment_score": 0.0, "states": {}},
            "signal_state": {"rsi": 79.0, "ema_fast": 106.0},
        }

        actions = controller._check_held_bag_recovery(
            current_price=Decimal("105.20"),
            now_ts=1234567890.0,
            current_signal=0,
        )

        self.assertEqual(actions, [])
        self.assertEqual(controller._recovery_trails[position_key].last_reason, "price-gap")

    def test_rsi_v5_recovery_sell_is_suppressed_when_signal_sell_owner_exists(self):
        from controllers.directional_trading.rsi_v5 import RecoveryTrailState, RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_price_by_type.return_value = Decimal("101.60")
        market_data_provider.get_balance.return_value = Decimal("1")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                use_bag_freeze=False,
                early_stop_drawdown_pct=Decimal("0"),
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal": 0,
            "trend_confirmation": {"direction": "neutral", "confidence": 0.0, "alignment_score": 0.0, "states": {}},
            "signal_state": {"rsi": 81.5, "rsi_prev": 83.0, "ema_fast": 101.9},
        }
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("1"),
                breakeven_price=Decimal("100.00"),
            )
        ]
        controller.executors_info = [
            SimpleNamespace(
                is_active=True,
                type="order_executor",
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.SELL,
                timestamp=1234567885.0,
                custom_info={"level_id": "signal_exit"},
            )
        ]
        controller._recovery_trails = {
            "binance|ETH-USDC|BUY|100.00000000": RecoveryTrailState(
                position_key="binance|ETH-USDC|BUY|100.00000000",
                armed_timestamp=1234567800.0,
                arm_price=Decimal("101.00"),
                peak_price=Decimal("102.00"),
                tracked_amount=Decimal("1"),
                peak_rsi=84.0,
                target_profit_pct=Decimal("0.003"),
            )
        }

        actions = controller.stop_actions_proposal()

        self.assertEqual(actions, [])

    def test_rsi_v5_sell_gate_holds_while_multi_timeframe_trend_is_strongly_bullish(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_price_by_type.return_value = Decimal("2143.81")
        market_data_provider.get_balance.return_value = Decimal("0.5")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal": -1,
            "trend_confirmation": {"direction": "up", "confidence": 0.72, "alignment_score": 0.72, "states": {}},
            "signal_state": {"rsi": 82.0, "rsi_prev": 82.2, "ema_fast": 2140.0},
        }
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.01"),
                breakeven_price=Decimal("2135.12"),
            )
        ]

        self.assertFalse(controller.can_create_executor(-1))
        self.assertEqual(controller._last_gate_reason[TradeType.SELL], "trend_hold")

    def test_rsi_v5_bag_recovery_skips_when_no_base_balance_is_available(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_balance.return_value = Decimal("0")
        market_data_provider.get_price_by_type.return_value = Decimal("2143.81")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                use_bag_freeze=False,
                early_stop_drawdown_pct=Decimal("0"),
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal": 0,
            "signal_state": {"rsi": 60.0},
        }
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.01"),
                breakeven_price=Decimal("2135.12"),
            )
        ]

        actions = controller.stop_actions_proposal()

        self.assertEqual(actions, [])

    def test_rsi_v5_recovery_trail_arms_then_exits_on_pullback(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        price_box = {"value": Decimal("101.00")}
        market_data_provider.get_price_by_type.side_effect = lambda *args, **kwargs: price_box["value"]
        market_data_provider.get_balance.return_value = Decimal("1")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                use_bag_freeze=False,
                early_stop_drawdown_pct=Decimal("0"),
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal": 0,
            "trend_confirmation": {"direction": "up", "confidence": 0.74, "alignment_score": 0.74, "states": {}},
            "signal_state": {"rsi": 82.0, "rsi_prev": 82.4, "ema_fast": 100.6},
        }
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("1"),
                breakeven_price=Decimal("100.00"),
            )
        ]

        first_actions = controller.stop_actions_proposal()
        self.assertEqual(first_actions, [])
        self.assertEqual(len(controller._recovery_trails), 1)

        price_box["value"] = Decimal("102.00")
        controller.processed_data["signal_state"]["rsi"] = 84.0
        second_actions = controller.stop_actions_proposal()
        self.assertEqual(second_actions, [])

        price_box["value"] = Decimal("101.60")
        controller.processed_data["signal_state"]["rsi"] = 81.5
        controller.processed_data["signal_state"]["rsi_prev"] = 83.0
        controller.processed_data["signal_state"]["ema_fast"] = 101.9
        third_actions = controller.stop_actions_proposal()

        self.assertEqual(len(third_actions), 1)
        self.assertIsInstance(third_actions[0], CreateExecutorAction)
        self.assertEqual(third_actions[0].executor_config.side, TradeType.SELL)
        self.assertEqual(third_actions[0].executor_config.amount, Decimal("0.50000000"))
        self.assertEqual(third_actions[0].executor_config.execution_strategy, ExecutionStrategy.LIMIT_CHASER)
        self.assertEqual(third_actions[0].executor_config.chaser_config.distance, Decimal("0.002"))
        self.assertEqual(third_actions[0].executor_config.chaser_config.refresh_threshold, Decimal("0.003"))
        trail = next(iter(controller._recovery_trails.values()))
        self.assertFalse(trail.partial_exit_done)

        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.5"),
                breakeven_price=Decimal("100.00"),
            )
        ]
        price_box["value"] = Decimal("102.20")
        fourth_actions = controller.stop_actions_proposal()

        self.assertEqual(fourth_actions, [])
        trail = next(iter(controller._recovery_trails.values()))
        self.assertTrue(trail.partial_exit_done)

    def test_rsi_v5_bag_recovery_requires_overbought_rsi_to_arm(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        price_box = {"value": Decimal("105.00")}
        market_data_provider.get_price_by_type.side_effect = lambda *args, **kwargs: price_box["value"]
        market_data_provider.get_balance.return_value = Decimal("1")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                use_bag_freeze=False,
                early_stop_drawdown_pct=Decimal("0"),
                bag_recovery_target_pct=Decimal("0.05"),
                sell_rsi_overbought=80.0,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal": 0,
            "trend_confirmation": {"direction": "neutral", "confidence": 0.0, "alignment_score": 0.0, "states": {}},
            "signal_state": {"rsi": 79.0, "rsi_prev": 78.0, "ema_fast": 104.0},
        }
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("1"),
                breakeven_price=Decimal("100.00"),
            )
        ]

        first_actions = controller.stop_actions_proposal()

        self.assertEqual(first_actions, [])
        self.assertEqual(len(controller._recovery_trails), 0)

        controller.processed_data["signal_state"]["rsi"] = 81.0
        second_actions = controller.stop_actions_proposal()

        self.assertEqual(second_actions, [])
        self.assertEqual(len(controller._recovery_trails), 1)

    def test_rsi_v5_signal_sell_is_blocked_while_held_bag_recovery_owns_exit(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.get_price_by_type.return_value = Decimal("105.00")
        market_data_provider.get_balance.return_value = Decimal("1")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                use_bag_freeze=False,
                early_stop_drawdown_pct=Decimal("0"),
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal": -1,
            "trend_confirmation": {"direction": "neutral", "confidence": 0.0, "alignment_score": 0.0, "states": {}},
            "signal_state": {"rsi": 82.0, "rsi_prev": 81.0, "ema_fast": 104.0},
        }
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("1"),
                breakeven_price=Decimal("100.00"),
            )
        ]

        self.assertFalse(controller.can_create_executor(-1))
        self.assertEqual(controller._last_gate_reason[TradeType.SELL], "held_bag_recovery")

    def test_rsi_v5_bag_recovery_partial_exit_fraction_repeats(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        price_box = {"value": Decimal("105.00")}
        market_data_provider.get_price_by_type.side_effect = lambda *args, **kwargs: price_box["value"]
        market_data_provider.get_balance.return_value = Decimal("1")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                use_bag_freeze=False,
                early_stop_drawdown_pct=Decimal("0"),
                bag_recovery_target_pct=Decimal("0.05"),
                recovery_partial_exit_fraction=0.1,
                sell_rsi_overbought=80.0,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal": -1,
            "trend_confirmation": {"direction": "neutral", "confidence": 0.0, "alignment_score": 0.0, "states": {}},
            "signal_state": {"rsi": 82.0, "rsi_prev": 81.0, "ema_fast": 104.0},
        }
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("1"),
                breakeven_price=Decimal("100.00"),
            )
        ]

        first_actions = controller.stop_actions_proposal()
        self.assertEqual(first_actions, [])
        self.assertEqual(len(controller._recovery_trails), 1)

        second_actions = controller.stop_actions_proposal()
        self.assertEqual(len(second_actions), 1)
        self.assertEqual(second_actions[0].executor_config.amount, Decimal("0.10000000"))
        self.assertEqual(second_actions[0].executor_config.execution_strategy, ExecutionStrategy.LIMIT_CHASER)
        self.assertEqual(second_actions[0].executor_config.chaser_config.distance, Decimal("0.002"))
        self.assertEqual(second_actions[0].executor_config.chaser_config.refresh_threshold, Decimal("0.003"))

        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.9"),
                breakeven_price=Decimal("100.00"),
            )
        ]
        price_box["value"] = Decimal("105.40")
        third_actions = controller.stop_actions_proposal()
        self.assertEqual(third_actions, [])

        price_box["value"] = Decimal("105.35")
        fourth_actions = controller.stop_actions_proposal()
        self.assertEqual(len(fourth_actions), 1)
        self.assertEqual(fourth_actions[0].executor_config.amount, Decimal("0.09000000"))

    def test_rsi_v5_neutral_zone_does_not_log_not_ready_messages(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        with patch.object(controller, "logger") as mock_logger:
            signal = controller._determine_signal(
                last_rsi=52.0,
                signal_score=0,
                rsi_buy_eff=35.0,
                rsi_sell_eff=72.0,
                last_close=Decimal("100"),
                raw_buy_reversal=False,
                signal_timestamp=1710785820.0,
                candle_timestamp=1710785820.0,
            )

        self.assertEqual(signal, 0)
        info_messages = [call.args[0] for call in mock_logger.return_value.info.call_args_list]
        self.assertFalse(any("BUY not ready" in message for message in info_messages))
        self.assertFalse(any("SELL not ready" in message for message in info_messages))

    def test_rsi_v5_status_shows_detailed_buy_and_sell_diagnostics(self):
        from controllers.directional_trading.rsi_v5 import RecoveryTrailState, RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_price_by_type.return_value = Decimal("2191.50")
        market_data_provider.get_balance.return_value = Decimal("0.0060")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                buy_confirmation_mode="rebound_confirm",
                buy_confirmation_rsi_delta=1.0,
                sell_only_if_profitable=True,
                min_profit_pct_for_sell=Decimal("0.002"),
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "indicators": {"rsi": 28.9, "cost_basis": Decimal("2188.0")},
            "thresholds": {"rsi_buy": 35.0, "rsi_sell": 75.0},
            "signal_state": {
                "close": 2191.5,
                "signal_score": 3,
                "rsi_reversal": False,
                "raw_reversal_prev_was_min": True,
                "raw_reversal_turning_up": True,
                "raw_reversal_was_oversold": True,
                "raw_reversal_near_bottom": True,
                "score_rsi_oversold": True,
                "score_bb_touch": True,
                "score_macd_turn": False,
                "score_mean_reversion": False,
                "condition_ok": True,
                "condition_reason": "ok",
                "buy_decision": "armed",
                "buy_reason": "need-rebound",
                "buy_confirmation_active": True,
                "buy_confirmation_rebound_delta": 1.04,
                "buy_confirmation_rebound_target": 1.0,
                "buy_confirmation_price_rebounded": True,
                "buy_entry_role": "scout",
                "buy_entry_fraction": 0.35,
                "buy_context_bias": "hostile",
                "sell_decision": "blocked",
                "sell_reason": "need-profit",
                "sell_profitability_ok": False,
                "sell_trend_hold": True,
            },
            "signal": 0,
            "regime": "HV_Trend_Down",
        }
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.0079"),
                breakeven_price=Decimal("2188.0"),
            )
        ]
        controller.executors_info = [
            SimpleNamespace(
                id="buy-scout-1",
                is_active=True,
                type="position_executor",
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                timestamp=1234567860.0,
                filled_amount_quote=Decimal("80"),
                custom_info={
                    "level_id": "scout",
                    "role": "scout",
                    "entry_price": Decimal("2189.50"),
                    "current_position_average_price": Decimal("2189.50"),
                    "trailing_state": "armed",
                    "trailing_activation_pct": 0.006,
                    "trailing_activation_price": Decimal("2202.64"),
                    "trailing_stop_trigger_pct": 0.009,
                    "trailing_trigger_price": Decimal("2209.10"),
                    "trailing_move_count": 2,
                },
            ),
            SimpleNamespace(
                id="sell-1",
                is_active=True,
                type="order_executor",
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.SELL,
                timestamp=1234567850.0,
                config=SimpleNamespace(price=Decimal("2196.0")),
                custom_info={
                    "level_id": "signal_exit",
                    "current_retries": 1,
                    "max_retries": 10,
                    "order_last_update": 1234567880.0,
                },
            ),
        ]
        controller._recovery_trails = {
            "binance|ETH-USDC|BUY|2188.00000000": RecoveryTrailState(
                position_key="binance|ETH-USDC|BUY|2188.00000000",
                armed_timestamp=1234567800.0,
                arm_price=Decimal("2196.00"),
                peak_price=Decimal("2204.00"),
                tracked_amount=Decimal("0.0040"),
                peak_rsi=81.0,
                target_profit_pct=Decimal("0.003"),
                last_reason="partial-pullback-trail",
                partial_exit_done=True,
            )
        }
        controller.processed_data["recovery_trails"] = {
            key: trail.as_dict() for key, trail in controller._recovery_trails.items()
        }

        status = "\n".join(controller.to_format_status())
        self.assertIn("BUY", status)
        self.assertIn("🎯 need-rebound", status)
        self.assertIn("📉 RSI 28.90/35.00", status)
        self.assertIn("🔁 1.04/1.00 ✓", status)
        self.assertIn("SELL", status)
        self.assertIn("🎯 need-profit", status)
        self.assertIn("💰 +0.16%/0.20%", status)
        self.assertIn("⏳ rsi 46.10, pnl 0.04%", status)
        self.assertIn("🌿 trend-hold", status)
        self.assertNotIn("market   ◉", status)
        self.assertNotIn("position ", status)
        self.assertNotIn("orders ", status)
        self.assertNotIn("buy_exec", status)
        self.assertNotIn("sell_exec", status)
        self.assertNotIn("recovery ", status)
        self.assertNotIn("   checks", status)
        self.assertNotIn("Hold Plan:", status)
        self.assertNotIn("Active Executors:", status)
        self.assertNotIn("raw(prev_min=", status)
        self.assertNotIn("score_parts(", status)

    def test_rsi_v5_backtest_signal_series_matches_live_score_components(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                rsi_length=4,
                min_signal_score=3,
                buy_confirmation_mode="none",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        df = pd.DataFrame(
            {
                "timestamp": [1.0, 2.0, 3.0, 4.0],
                "close": [100.0, 99.0, 98.0, 97.0],
                "RSI_4": [40.0, 32.0, 28.0, 30.0],
                "BBL_20_2.0": [98.0, 98.0, 98.0, 98.0],
                "MACDh_12_26_9": [0.0, 0.0, 0.0, 0.0],
            }
        )

        signal_series = controller._build_backtest_signal_series(
            df,
            rsi_buy_eff=35.0,
            rsi_sell_eff=75.0,
        )

        self.assertEqual(signal_series.iloc[-1], 0)

    def test_rsi_v5_backtest_sell_series_tracks_inventory_and_profitability(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                rsi_length=4,
                min_signal_score=3,
                buy_confirmation_mode="none",
                sell_only_if_profitable=True,
                min_profit_pct_for_sell=Decimal("0.015"),
                sell_rsi_overbought=75.0,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        df = pd.DataFrame(
            {
                "timestamp": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
                "close": [100.0, 99.0, 97.0, 98.0, 99.0, 98.0, 99.0, 100.0],
                "RSI_4": [40.0, 32.0, 28.0, 30.0, 55.0, 78.0, 76.0, 75.0],
                "BBL_20_2.0": [98.5, 98.5, 98.5, 98.5, 98.5, 98.5, 98.5, 98.5],
                "MACDh_12_26_9": [0.0, -0.2, -0.6, -0.1, 0.2, 0.4, 0.1, -0.1],
                "EMA_9": [100.0, 99.5, 98.5, 98.4, 98.8, 99.2, 99.1, 99.4],
            }
        )

        signal_series = controller._build_backtest_signal_series(
            df,
            rsi_buy_eff=35.0,
            rsi_sell_eff=75.0,
        )

        self.assertEqual(signal_series.iloc[3], 1)
        self.assertEqual(signal_series.iloc[6], 0)
        self.assertEqual(signal_series.iloc[7], -1)

    def test_rsi_v5_sell_threshold_harmonization_prefers_sell_rsi_overbought(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                rsi_sell_threshold=75.0,
                sell_rsi_overbought=82.0,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        threshold_buy, threshold_sell = controller._get_dynamic_rsi_thresholds(None, 0.0)

        self.assertEqual(threshold_buy, 31.0)
        self.assertEqual(threshold_sell, 82.0)
        self.assertEqual(controller.config.rsi_sell_threshold, 82.0)
        self.assertEqual(controller.config.sell_rsi_overbought, 82.0)

    def test_rsi_v5_cost_basis_does_not_fall_back_to_recent_buy_history(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.4"),
                breakeven_price=Decimal("100.00"),
            ),
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.6"),
            ),
        ]

        self.assertIsNone(controller._get_position_cost_basis())

    def test_rsi_v5_status_summary_includes_price(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_price_by_type.return_value = Decimal("2191.50")

        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "indicators": {"cost_basis": Decimal("2188.0")},
            "signal_state": {"signal_score": 0},
            "signal": 0,
            "regime": "HV_Trend_Down",
        }

        summary = controller.get_status_summary()

        self.assertEqual(summary["pair"], "ETH-USDC")
        self.assertEqual(summary["price"], "2191.5")
        self.assertEqual(summary["avg_buy"], "2188")
        self.assertEqual(summary["u_pnl_pct"], "0.2%")

    def test_rsi_v6_controller_imports_from_local_hummingbot_tree(self):
        from controllers.directional_trading.rsi_v6 import RSIv6Controller, RSIv6ControllerConfig

        self.assertIsNotNone(RSIv6Controller)
        self.assertIsNotNone(RSIv6ControllerConfig)

    def test_rsi_v6_supportive_pullback_reclaim_detects_wick_volume_setup(self):
        from controllers.directional_trading.rsi_v6 import RSIv6Controller, RSIv6ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None

        controller = RSIv6Controller(
            config=RSIv6ControllerConfig(
                id="rsi-v6-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                trend_confirmation_enabled=False,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        state = controller._compute_supportive_pullback_reclaim_state(
            context_bias="neutral",
            last_open=101.4,
            last_high=102.0,
            last_low=100.0,
            last_close=101.8,
            last_rsi=40.0,
            rsi_prev=39.6,
            ema_fast=101.5,
            ema_slow=100.8,
            macd_hist=0.15,
            macd_hist_prev=0.02,
            volume_ratio=1.45,
        )

        self.assertTrue(state["supportive_context"])
        self.assertTrue(state["ready"])
        self.assertGreaterEqual(state["score"], 4)

    def test_rsi_v6_backtest_signal_series_can_enter_supportive_pullback_lane(self):
        from controllers.directional_trading.rsi_v6 import RSIv6Controller, RSIv6ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None

        controller = RSIv6Controller(
            config=RSIv6ControllerConfig(
                id="rsi-v6-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                rsi_length=4,
                min_signal_score=3,
                buy_confirmation_mode="none",
                trend_confirmation_enabled=False,
                use_split_entries=False,
                rsi_buy_threshold=32.0,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

        df = pd.DataFrame(
            {
                "timestamp": [1.0, 2.0, 3.0, 4.0],
                "open": [100.8, 101.6, 101.7, 101.4],
                "high": [101.4, 102.1, 102.0, 102.0],
                "low": [100.4, 101.0, 100.9, 100.0],
                "close": [101.2, 101.9, 101.2, 101.8],
                "volume": [100.0, 104.0, 108.0, 180.0],
                "RSI_4": [52.0, 48.0, 39.0, 40.0],
                "BBL_20_2.0": [99.0, 99.0, 99.0, 99.0],
                "MACDh_12_26_9": [0.10, 0.12, 0.02, 0.15],
                "EMA_9": [100.7, 101.1, 101.4, 101.5],
                "EMA_21": [100.1, 100.4, 100.6, 100.8],
            }
        )

        signal_series = controller._build_backtest_signal_series(
            df,
            rsi_buy_eff=32.0,
            rsi_sell_eff=75.0,
        )

        self.assertEqual(signal_series.iloc[-1], 1)

    def test_rsi_v6_sell_trend_hold_releases_on_exhaustion(self):
        from controllers.directional_trading.rsi_v6 import RSIv6Controller, RSIv6ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_price_by_type.return_value = Decimal("101.60")

        controller = RSIv6Controller(
            config=RSIv6ControllerConfig(
                id="rsi-v6-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "trend_confirmation": {"direction": "up", "confidence": 0.82, "alignment_score": 0.82, "states": {}},
            "signal_state": {"rsi": 86.5, "rsi_prev": 88.2, "ema_fast": 100.0},
        }

        should_hold, reason = controller._sell_trend_hold_active(current_price=Decimal("101.60"))

        self.assertFalse(should_hold)
        self.assertEqual(reason, "exhaustion-release")

    def test_rsi_v6_buy_executor_config_applies_contextual_trailing_multiplier(self):
        from controllers.directional_trading.rsi_v6 import RSIv6Controller, RSIv6ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_trading_rules.return_value = SimpleNamespace(min_price_increment=Decimal("0.01"))
        market_data_provider.get_price_by_type.side_effect = lambda connector, pair, price_type: (
            Decimal("100.00") if price_type == PriceType.BestBid else Decimal("100.50")
        )

        controller = RSIv6Controller(
            config=RSIv6ControllerConfig(
                id="rsi-v6-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                use_adaptive_trailing=False,
                trailing_activation_pct=0.01,
                trailing_delta_pct=0.004,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )
        controller.processed_data = {
            "signal_state": {
                "size_multiplier": 1.0,
                "buy_context_bias": "supportive",
                "close": 101.8,
                "ema_fast": 101.5,
                "ema_slow": 100.8,
            }
        }

        executor_config = controller.get_executor_config(
            trade_type=TradeType.BUY,
            price=Decimal("100"),
            amount=Decimal("1"),
        )

        self.assertEqual(executor_config.triple_barrier_config.trailing_stop.activation_price, Decimal("0.0115"))
        self.assertEqual(executor_config.triple_barrier_config.trailing_stop.trailing_delta, Decimal("0.0046"))


if __name__ == "__main__":
    unittest.main()
