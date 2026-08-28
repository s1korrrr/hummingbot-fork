import asyncio
import importlib
import math
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import yaml

from hummingbot.core.data_type.common import TradeType

ROOT_PATH = Path(__file__).resolve().parent.parent
ROOT_PATH_STR = str(ROOT_PATH)
FIXTURES_PATH = ROOT_PATH / "test" / "fixtures" / "ok_rsi"


def ensure_local_controller_imports():
    while ROOT_PATH_STR in sys.path:
        sys.path.remove(ROOT_PATH_STR)
    sys.path.insert(0, ROOT_PATH_STR)
    for module_name in list(sys.modules):
        if module_name == "controllers" or module_name.startswith("controllers."):
            sys.modules.pop(module_name, None)
    importlib.invalidate_caches()


class OKRSIControllerTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        ensure_local_controller_imports()

    @staticmethod
    def _provider(*, base_balance: Decimal, price: Decimal):
        provider = MagicMock()
        provider.initialize_rate_sources.return_value = None
        provider.time.return_value = 1_725_000_000.0
        provider.get_available_balance.side_effect = lambda connector, asset: (
            base_balance if asset == "BTC" else Decimal("1000")
        )
        provider.get_trading_rules.return_value = SimpleNamespace(
            min_price_increment=Decimal("0.01"),
            min_order_size=Decimal("0.00000001"),
            min_base_amount_increment=Decimal("0.00000001"),
            min_notional_size=Decimal("0.01"),
            min_order_value=Decimal("0.01"),
        )
        provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
        provider.get_price_by_type.return_value = price
        return provider

    def _controller(self, *, base_balance: Decimal, price: Decimal):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        controller = OKRSIController(
            config=OKRSIControllerConfig(
                id="ok-rsi-btc-usdt-sell-only",
                connector_name="okx",
                trading_pair="BTC-USDT",
                trade_mode="sell_only",
                sell_only_if_profitable=False,
                strict_hmm_mode=False,
            ),
            market_data_provider=self._provider(base_balance=base_balance, price=price),
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = []
        controller.executors_info = []
        controller.processed_data = {"signal": -1, "signal_state": {"condition_ok": True}}
        return controller

    def _buy_controller(self, *, atr_pct: float | None):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        controller = OKRSIController(
            config=OKRSIControllerConfig(
                id="ok-rsi-btc-usdt-buy-sell",
                connector_name="okx",
                trading_pair="BTC-USDT",
                trade_mode="buy_sell",
                strict_hmm_mode=False,
                usd_per_entry=Decimal("30"),
                trailing_activation_pct=0.015,
                trailing_delta_pct=0.0035,
                use_adaptive_trailing=True,
            ),
            market_data_provider=self._provider(base_balance=Decimal("0"), price=Decimal("50000")),
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = []
        controller.executors_info = []
        controller.processed_data = {
            "signal": 1,
            "signal_state": {"condition_ok": True, "size_multiplier": 1.0},
        }
        controller._latest_atr_pct = atr_pct
        return controller

    def test_module_exposes_okx_spot_identity(self):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        config = OKRSIControllerConfig(id="ok-rsi-default")

        self.assertIsNotNone(OKRSIController)
        self.assertEqual(config.controller_name, "ok_rsi")
        self.assertEqual(config.connector_name, "okx")
        self.assertEqual(config.trading_pair, "BTC-USDT")

    def test_okx_controller_is_a_thin_specialization_of_shared_rsi_logic(self):
        from controllers.directional_trading.hl_rsi import HLRSIController, HLRSIControllerConfig
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        self.assertTrue(issubclass(OKRSIController, HLRSIController))
        self.assertTrue(issubclass(OKRSIControllerConfig, HLRSIControllerConfig))

    def test_controller_accepts_strategy_v2_positional_constructor_contract(self):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        config = OKRSIControllerConfig(id="ok-rsi-runtime-constructor")
        provider = self._provider(base_balance=Decimal("0"), price=Decimal("50000"))
        actions_queue = asyncio.Queue()

        controller = OKRSIController(config, provider, actions_queue)

        self.assertIs(controller.config, config)
        self.assertIs(controller.market_data_provider, provider)
        self.assertIs(controller.actions_queue, actions_queue)

    def test_config_rejects_non_okx_connector(self):
        from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

        with self.assertRaisesRegex(ValueError, "OKX spot"):
            OKRSIControllerConfig(id="invalid", connector_name="binance", trading_pair="BTC-USDT")

    def test_config_normalizes_pair_and_rejects_cross_market_candles(self):
        from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

        config = OKRSIControllerConfig(id="normalized-pair", trading_pair="btc-usdt")
        self.assertEqual(config.trading_pair, "BTC-USDT")
        self.assertEqual(config.candles_trading_pair, "BTC-USDT")

        with self.assertRaisesRegex(ValueError, "same trading pair"):
            OKRSIControllerConfig(
                id="cross-market-candles",
                trading_pair="BTC-USDT",
                candles_trading_pair="ETH-USDT",
            )

        with self.assertRaisesRegex(ValueError, "BASE-QUOTE"):
            OKRSIControllerConfig(id="malformed-pair", trading_pair="BTCUSDT")

    def test_paper_trade_connector_uses_real_okx_candles_feed(self):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        config = OKRSIControllerConfig(
            id="ok-rsi-paper",
            connector_name="okx_paper_trade",
            trading_pair="BTC-USDT",
        )

        self.assertEqual(config.candles_connector, "okx")
        controller = OKRSIController(
            config=config,
            market_data_provider=self._provider(base_balance=Decimal("0"), price=Decimal("50000")),
            actions_queue=asyncio.Queue(),
        )
        self.assertTrue(all(candle.connector == "okx" for candle in controller.get_candles_config()))

    def test_config_rejects_unsupported_okx_candle_interval(self):
        from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

        with self.assertRaisesRegex(ValueError, "OKX candle interval"):
            OKRSIControllerConfig(id="invalid-interval", interval="7m")

    def test_config_rejects_non_spot_leverage_and_position_mode(self):
        from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

        invalid_overrides = (
            {"leverage": 2},
            {"position_mode": "HEDGE"},
        )
        for invalid_override in invalid_overrides:
            with self.subTest(invalid_override=invalid_override):
                with self.assertRaisesRegex(ValueError, "OKX spot"):
                    OKRSIControllerConfig(id="invalid-spot-mode", **invalid_override)

    def test_buy_sell_config_requires_an_ordinary_exit(self):
        from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

        with self.assertRaisesRegex(ValueError, "trailing exit"):
            OKRSIControllerConfig(
                id="invalid-no-exit",
                trade_mode="buy_sell",
                use_trailing_exit=False,
            )

    def test_config_rejects_exit_settings_that_executor_ignores(self):
        from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

        ignored_exit_overrides = (
            {"stop_loss": Decimal("0.01")},
            {"take_profit": Decimal("0.02")},
            {"time_limit": 60},
            {"take_profit_pct": 0.02},
            {"max_hold_time_seconds": 60},
        )
        for invalid_override in ignored_exit_overrides:
            with self.subTest(invalid_override=invalid_override):
                with self.assertRaisesRegex(ValueError, "unsupported exit"):
                    OKRSIControllerConfig(id="invalid-ignored-exit", **invalid_override)

    def test_continuous_trailing_scales_activation_faster_than_delta(self):
        from controllers.directional_trading.ok_rsi import calculate_continuous_adaptive_trailing

        selection = calculate_continuous_adaptive_trailing(
            atr_pct=0.004,
            baseline_atr_pct=0.002,
            base_activation=Decimal("0.015"),
            base_delta=Decimal("0.0035"),
            activation_min=Decimal("0.003"),
            activation_max=Decimal("0.05"),
            delta_min=Decimal("0.001"),
            delta_max=Decimal("0.02"),
            min_locked_profit=Decimal("0.004"),
            activation_exponent=0.70,
            delta_exponent=0.35,
            min_multiplier=0.5,
            max_multiplier=2.5,
        )

        self.assertEqual(selection.activation, Decimal("0.024367571890687065"))
        self.assertEqual(selection.delta, Decimal("0.00446096219561741770"))
        self.assertEqual(selection.volatility_ratio, Decimal("2.0"))
        self.assertGreater(selection.activation / Decimal("0.015"), selection.delta / Decimal("0.0035"))

    def test_config_rejects_trailing_contract_without_locked_profit_floor(self):
        from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

        with self.assertRaisesRegex(ValueError, "locked profit"):
            OKRSIControllerConfig(
                id="invalid-trailing",
                trailing_activation_pct=0.005,
                trailing_delta_pct=0.003,
                trailing_min_locked_profit_pct=0.004,
            )

    def test_market_trailing_rejects_irrelevant_limit_fallback_timeout(self):
        from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

        with self.assertRaisesRegex(ValueError, "fallback timeout"):
            OKRSIControllerConfig(
                id="invalid-market-fallback",
                trailing_stop_order_type="MARKET",
                trailing_stop_limit_max_age_seconds=3,
            )

        config = OKRSIControllerConfig(
            id="market-trailing",
            trailing_stop_order_type="MARKET",
            trailing_stop_limit_max_age_seconds=0,
        )
        self.assertEqual(config.trailing_stop_limit_max_age_seconds, 0)

    def test_adaptive_buy_executor_uses_frozen_continuous_selection(self):
        controller = self._buy_controller(atr_pct=0.004)

        executor = controller.get_executor_config(TradeType.BUY, Decimal("50000"), Decimal("0"))
        trailing = executor.triple_barrier_config.trailing_stop

        self.assertEqual(trailing.activation_price, Decimal("0.024367571890687065"))
        self.assertEqual(trailing.trailing_delta, Decimal("0.00446096219561741770"))
        self.assertEqual(executor.triple_barrier_config.open_order_limit_max_age_seconds, 30)
        trailing_policy = controller.get_custom_info()["trailing_policy"]
        self.assertEqual(trailing_policy["policy"], "adaptive_continuous")
        self.assertEqual(trailing_policy["volatility_ratio"], 2.0)
        self.assertAlmostEqual(trailing_policy["initial_locked_profit_pct"], 0.01990660969506965)

    def test_adaptive_buy_executor_fails_closed_without_atr(self):
        controller = self._buy_controller(atr_pct=None)

        with self.assertRaisesRegex(ValueError, "ATR is unavailable"):
            controller.get_executor_config(TradeType.BUY, Decimal("50000"), Decimal("0"))

        self.assertFalse(controller.can_create_executor(1))
        self.assertEqual(controller._last_gate_reason[TradeType.BUY], "trailing_input_unavailable")

    def test_sell_only_sells_ten_percent_when_inventory_value_is_at_least_twenty_usd(self):
        controller = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))

        executor = controller.get_executor_config(TradeType.SELL, Decimal("50000"), Decimal("999"))

        self.assertEqual(executor.amount, Decimal("0.001"))

    def test_sell_only_sells_all_when_inventory_value_is_below_twenty_usd(self):
        controller = self._controller(base_balance=Decimal("0.0003"), price=Decimal("50000"))

        executor = controller.get_executor_config(TradeType.SELL, Decimal("50000"), Decimal("999"))

        self.assertEqual(executor.amount, Decimal("0.0003"))

    def test_sell_only_uses_ten_percent_at_exact_twenty_usd_boundary(self):
        controller = self._controller(base_balance=Decimal("0.0004"), price=Decimal("50000"))

        executor = controller.get_executor_config(TradeType.SELL, Decimal("50000"), Decimal("999"))

        self.assertEqual(executor.amount, Decimal("0.00004"))

    def test_sell_executor_rejects_zero_inventory_at_construction_boundary(self):
        controller = self._controller(base_balance=Decimal("0"), price=Decimal("50000"))

        with self.assertRaisesRegex(ValueError, "sellable inventory"):
            controller.get_executor_config(TradeType.SELL, Decimal("50000"), Decimal("0"))

    def test_sell_balance_lookup_does_not_fallback_to_reserved_total_balance(self):
        controller = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
        controller.market_data_provider.get_available_balance.side_effect = RuntimeError("not ready")
        controller.market_data_provider.get_balance.return_value = Decimal("1")

        self.assertFalse(controller.can_create_executor(-1))
        self.assertEqual(controller._last_gate_reason[TradeType.SELL], "base_balance_low")
        controller.market_data_provider.get_balance.assert_not_called()

    def test_executor_gate_fails_closed_when_trading_rule_is_unavailable(self):
        controller = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
        controller.market_data_provider.get_trading_rules.return_value = None

        self.assertFalse(controller.can_create_executor(-1))
        self.assertEqual(controller._last_gate_reason[TradeType.SELL], "trading_rule_unavailable")

        with self.assertRaisesRegex(ValueError, "trading rule"):
            controller.get_executor_config(TradeType.SELL, Decimal("50000"), Decimal("0"))

    def test_executor_gate_rejects_non_finite_market_price(self):
        controller = self._controller(base_balance=Decimal("0.01"), price=Decimal("NaN"))

        self.assertFalse(controller.can_create_executor(-1))
        self.assertEqual(controller._last_gate_reason[TradeType.SELL], "price_unavailable")

    def test_executor_construction_fails_closed_when_amount_quantization_fails(self):
        controller = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
        controller.market_data_provider.quantize_order_amount.side_effect = RuntimeError("connector unavailable")

        with self.assertRaisesRegex(ValueError, "quantize"):
            controller.get_executor_config(TradeType.SELL, Decimal("50000"), Decimal("0"))

    def test_executor_construction_rejects_non_positive_reference_price(self):
        controller = self._buy_controller(atr_pct=0.004)

        with self.assertRaisesRegex(ValueError, "positive finite price"):
            controller.get_executor_config(TradeType.BUY, Decimal("0"), Decimal("0"))

    def test_create_actions_proposal_emits_validated_sell_executor(self):
        controller = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))

        actions = controller.create_actions_proposal()

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].controller_id, controller.config.id)
        self.assertEqual(actions[0].executor_config.side, TradeType.SELL)
        self.assertEqual(actions[0].executor_config.amount, Decimal("0.001"))

    def test_create_actions_proposal_emits_nothing_when_execution_inputs_are_missing(self):
        controller = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
        controller.market_data_provider.get_trading_rules.return_value = None

        self.assertEqual(controller.create_actions_proposal(), [])

    def test_multi_timeframe_market_data_pipeline_produces_finite_causal_state(self):
        controller = self._buy_controller(atr_pct=None)
        now = int(controller.market_data_provider.time())
        rows = []
        for index in range(260):
            close = 50000 + (index * 2) + (120 * math.sin(index / 9))
            rows.append(
                {
                    "timestamp": now - ((259 - index + 1) * 60),
                    "open": close - 8,
                    "high": close + 25,
                    "low": close - 25,
                    "close": close,
                    "volume": 100 + (index % 11),
                    "quote_asset_volume": (100 + (index % 11)) * close,
                }
            )
        candles = pd.DataFrame(rows)
        controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: candles.copy()

        asyncio.run(controller.update_processed_data())

        self.assertTrue(controller.processed_data["data_health"]["ready"])
        self.assertIn(controller.processed_data["signal"], {-1, 0, 1})
        self.assertIsNone(controller.processed_data["regime_error"])
        self.assertTrue(math.isfinite(controller.processed_data["indicators"]["atr_pct"]))
        self.assertGreater(controller.processed_data["indicators"]["atr_pct"], 0)
        self.assertEqual(controller.processed_data["timestamp"], float(now))
        self.assertLess(controller.processed_data["candle_timestamp"], controller.processed_data["timestamp"])

    def test_market_data_pipeline_blocks_stale_duplicate_and_non_finite_candles(self):
        now = 1_725_000_000
        healthy_rows = [
            {
                "timestamp": now - ((40 - index) * 60),
                "open": 50000 + index,
                "high": 50010 + index,
                "low": 49990 + index,
                "close": 50000 + index,
                "volume": 100,
            }
            for index in range(40)
        ]
        invalid_frames = {}
        stale = pd.DataFrame(healthy_rows)
        stale["timestamp"] = stale["timestamp"] - 3600
        invalid_frames["stale_candle"] = stale
        duplicate = pd.DataFrame(healthy_rows)
        duplicate.loc[duplicate.index[-1], "timestamp"] = duplicate.loc[duplicate.index[-2], "timestamp"]
        invalid_frames["duplicate_timestamp"] = duplicate
        non_finite = pd.DataFrame(healthy_rows)
        non_finite.loc[non_finite.index[-1], "close"] = float("nan")
        invalid_frames["invalid_close"] = non_finite

        for expected_reason, candles in invalid_frames.items():
            with self.subTest(expected_reason=expected_reason):
                controller = self._buy_controller(atr_pct=None)
                controller.market_data_provider.get_candles_df.return_value = candles
                asyncio.run(controller.update_processed_data())

                self.assertEqual(controller.processed_data["signal"], 0)
                self.assertFalse(controller.processed_data["data_health"]["ready"])
                self.assertEqual(controller.processed_data["data_health"]["reason"], expected_reason)

    def test_controller_lifecycle_restarts_cleanly_without_emitting_orders(self):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
        from hummingbot.strategy_v2.models.base import RunnableStatus

        async def exercise_lifecycle():
            provider = self._provider(base_balance=Decimal("0"), price=Decimal("50000"))
            provider.ready = False
            actions_queue = asyncio.Queue()
            controller = OKRSIController(
                config=OKRSIControllerConfig(
                    id="ok-rsi-lifecycle",
                    strict_hmm_mode=False,
                ),
                market_data_provider=provider,
                actions_queue=actions_queue,
                update_interval=0.001,
            )

            for expected_initializations in (3, 6):
                controller.start()
                await asyncio.sleep(0.005)
                self.assertEqual(controller.status, RunnableStatus.RUNNING)
                controller.stop()
                await asyncio.wait_for(controller.terminated.wait(), timeout=1)
                await asyncio.sleep(0.005)
                self.assertEqual(controller.status, RunnableStatus.TERMINATED)
                self.assertEqual(
                    provider.initialize_candles_feed.call_count,
                    expected_initializations,
                )

            self.assertTrue(actions_queue.empty())

        asyncio.run(exercise_lifecycle())

    def _load_script_through_trading_core(self, strategy_name: str, config_name: str):
        from hummingbot.core.trading_core import TradingCore

        payload = yaml.safe_load((FIXTURES_PATH / "scripts" / config_name).read_text())
        loader = SimpleNamespace(_load_strategy_config=lambda: payload)
        return TradingCore.load_v2_class(loader, strategy_name)

    def test_buy_sell_script_loads_only_requested_buy_sell_pairs(self):
        from scripts.v2_with_controllers import V2WithControllersConfig

        payload = yaml.safe_load((FIXTURES_PATH / "scripts" / "ok_rsi_buy_sell.yml").read_text())
        config = V2WithControllersConfig(**payload)
        with patch(
            "hummingbot.client.settings.CONTROLLERS_CONF_DIR_PATH",
            str(FIXTURES_PATH / "controllers"),
        ):
            loaded = config.load_controller_configs()

        self.assertEqual(payload["script_file_name"], "v2_with_controllers.py")
        self.assertEqual(
            set(payload["controllers_config"]),
            {
                "ok_rsi_btc.yml",
                "ok_rsi_eth.yml",
                "ok_rsi_sol.yml",
                "ok_rsi_xrp.yml",
                "ok_rsi_bnb.yml",
            },
        )
        self.assertEqual(
            {controller.trading_pair for controller in loaded},
            {"BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT"},
        )
        self.assertTrue(all(controller.trade_mode == "buy_sell" for controller in loaded))
        self.assertTrue(all(controller.sell_only_if_profitable for controller in loaded))
        self.assertEqual(len({controller.id for controller in loaded}), 5)

    def test_sell_only_script_loads_only_requested_sell_only_pairs(self):
        strategy_class, config = self._load_script_through_trading_core(
            "ok_rsi_sell_only", "ok_rsi_sell_only.yml"
        )
        loaded = config.load_controller_configs()

        self.assertEqual(strategy_class.__name__, "OKRSISellOnly")
        self.assertEqual(config.__class__.__name__, "OKRSISellOnlyConfig")
        self.assertEqual(
            {controller.trading_pair for controller in loaded},
            {"SUI-USDT", "XLM-USDT", "ENA-USDT"},
        )
        self.assertTrue(all(controller.trade_mode == "sell_only" for controller in loaded))
        self.assertTrue(all(not controller.sell_only_if_profitable for controller in loaded))
        self.assertTrue(all(controller.sell_only_inventory_fraction == Decimal("0.10") for controller in loaded))
        self.assertTrue(all(controller.sell_all_below_quote == Decimal("20") for controller in loaded))
        self.assertEqual(strategy_class.markets, {"okx": {controller.trading_pair for controller in loaded}})

    def test_single_mode_script_rejects_duplicate_pair(self):
        from scripts.ok_rsi_sell_only import OKRSISellOnlyConfig

        with self.assertRaisesRegex(ValueError, "unique"):
            OKRSISellOnlyConfig(trading_pairs=["SUI-USDT", "SUI-USDT"])

        for empty_pairs in (None, [], ""):
            with self.subTest(empty_pairs=empty_pairs):
                with self.assertRaisesRegex(ValueError, "At least one"):
                    OKRSISellOnlyConfig(trading_pairs=empty_pairs)

    def test_sell_only_script_rejects_invalid_venue_and_sizing_at_config_boundary(self):
        from scripts.ok_rsi_sell_only import OKRSISellOnlyConfig

        invalid_overrides = (
            {"connector_name": "binance"},
            {"leverage": 2},
            {"usd_per_entry": Decimal("0")},
            {"max_total_position_usd": Decimal("NaN")},
            {"sell_only_inventory_fraction": Decimal("1.01")},
            {"sell_all_below_quote": Decimal("-1")},
        )
        for invalid_override in invalid_overrides:
            with self.subTest(invalid_override=invalid_override):
                with self.assertRaises(ValueError):
                    OKRSISellOnlyConfig(
                        trading_pairs=["SUI-USDT"],
                        **invalid_override,
                    )


if __name__ == "__main__":
    unittest.main()
