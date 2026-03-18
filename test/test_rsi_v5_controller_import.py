import asyncio
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from hummingbot.core.data_type.common import OrderType, TradeType

ROOT_PATH = Path(__file__).resolve().parent.parent
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))


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
        self.assertEqual(
            executor_config.triple_barrier_config.take_profit_order_type,
            controller.config.take_profit_order_type,
        )
        self.assertEqual(executor_config.triple_barrier_config.stop_loss_order_type, OrderType.MARKET)
        self.assertEqual(executor_config.triple_barrier_config.time_limit_order_type, OrderType.MARKET)

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
        self.assertTrue(any("BUY signal (rebound-confirmed)" in message for message in info_messages))

    def test_rsi_v5_status_shows_compact_buy_and_sell_signal_distance(self):
        from controllers.directional_trading.rsi_v5 import BuyConfirmationSetup, RSIv5Controller, RSIv5ControllerConfig

        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        market_data_provider.get_price_by_type.return_value = Decimal("2191.50")

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
                "condition_ok": True,
                "condition_reason": "ok",
            },
            "signal": 0,
            "regime": "HV_Trend_Down",
        }
        controller._pending_buy_confirmation = BuyConfirmationSetup(
            armed_timestamp=1234567800.0,
            trough_rsi=27.86,
            trough_price=2190.0,
        )
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("0.0079"),
            )
        ]

        status = "\n".join(controller.to_format_status())
        self.assertIn("📊 RSI v5 Signal Distance", status)
        self.assertIn("pair=ETH-USDC", status)
        self.assertIn("🟢 BUY  [", status)
        self.assertIn("setup=armed", status)
        self.assertIn("rebound=1.04/1.00", status)
        self.assertIn("price_vs_trough=+0.07%", status)
        self.assertIn("🔴 SELL [", status)
        self.assertIn("pnl=+0.16%/0.20%", status)
        self.assertIn("need_pnl=0.04%", status)
        self.assertNotIn("Hold Plan:", status)
        self.assertNotIn("Active Executors:", status)


if __name__ == "__main__":
    unittest.main()
