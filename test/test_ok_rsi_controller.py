import asyncio
import importlib
import math
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import yaml

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.model.controller_candle_watermark import (
    ControllerCandleWatermarkNamespace,
    ControllerCandleWatermarkOutcome,
    ControllerCandleWatermarkResult,
    ControllerCandleWatermarkState,
)
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction

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
                durable_candle_watermark_enabled=True,
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
                durable_candle_watermark_enabled=True,
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

    def _shadow_controller(self):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        prices = {
            PriceType.MidPrice: Decimal("100.005"),
            PriceType.BestBid: Decimal("100"),
            PriceType.BestAsk: Decimal("100.01"),
        }
        provider = self._provider(base_balance=Decimal("0"), price=Decimal("100.005"))
        provider.get_price_by_type.side_effect = lambda connector, pair, price_type: prices[price_type]
        controller = OKRSIController(
            config=OKRSIControllerConfig(
                id="ok-rsi-shadow",
                connector_name="okx",
                trading_pair="BTC-USDT",
                trade_mode="buy_sell",
                strict_hmm_mode=False,
                entry_acquisition_shadow_policy="down_only_maker_v1",
            ),
            market_data_provider=provider,
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = []
        controller.executors_info = []
        controller.processed_data = {"signal": 1, "signal_state": {"condition_ok": True}}
        controller._latest_atr_pct = 0.01
        return controller, provider, prices

    @staticmethod
    def _entry_action(*, side: TradeType = TradeType.BUY):
        return CreateExecutorAction(
            controller_id="ok-rsi-shadow",
            executor_config=PositionExecutorConfig(
                id="executor-1",
                timestamp=1_000,
                controller_id="ok-rsi-shadow",
                connector_name="okx",
                trading_pair="BTC-USDT",
                side=side,
                entry_price=Decimal("100"),
                amount=Decimal("1"),
                leverage=1,
                triple_barrier_config=TripleBarrierConfig(
                    open_order_type=OrderType.LIMIT_MAKER,
                    open_order_limit_max_age_seconds=30,
                ),
                entry_order_policy="single_attempt",
            ),
        )

    def test_shadow_observes_buy_without_changing_executor_actions(self):
        from controllers.directional_trading import hl_rsi

        controller, _, _ = self._shadow_controller()
        action = self._entry_action()
        proposed = [action]
        with patch.object(hl_rsi.HLRSIController, "determine_executor_actions", return_value=proposed) as parent:
            result = controller.determine_executor_actions()

        self.assertIs(result, proposed)
        parent.assert_called_once_with()
        self.assertEqual(controller._entry_acquisition_shadow.snapshot()["intent_id"], "shadow:executor-1")
        self.assertTrue(controller.actions_queue.empty())

    def test_shadow_ignores_sell_and_advances_after_real_action_disappears(self):
        from controllers.directional_trading import hl_rsi

        controller, provider, prices = self._shadow_controller()
        with patch.object(
            hl_rsi.HLRSIController,
            "determine_executor_actions",
            return_value=[self._entry_action(side=TradeType.SELL)],
        ):
            controller.determine_executor_actions()
        self.assertFalse(controller._entry_acquisition_shadow.is_active)

        with patch.object(
            hl_rsi.HLRSIController,
            "determine_executor_actions",
            return_value=[self._entry_action()],
        ):
            controller.determine_executor_actions()
        provider.time.return_value += 10
        prices[PriceType.BestBid] = Decimal("99.90")
        prices[PriceType.BestAsk] = Decimal("100.01")
        empty_actions = []
        with patch.object(hl_rsi.HLRSIController, "determine_executor_actions", return_value=empty_actions):
            result = controller.determine_executor_actions()
        self.assertIs(result, empty_actions)
        self.assertEqual(controller._entry_acquisition_shadow.snapshot()["reprice_count"], 1)

    def test_shadow_input_failure_does_not_block_legacy_buy(self):
        from controllers.directional_trading import hl_rsi

        controller, _, prices = self._shadow_controller()
        prices[PriceType.BestAsk] = None
        action = self._entry_action()
        with patch.object(hl_rsi.HLRSIController, "determine_executor_actions", return_value=[action]):
            result = controller.determine_executor_actions()
        self.assertEqual(result, [action])
        shadow = controller.get_custom_info()["entry_acquisition_shadow"]
        self.assertEqual(shadow["last_event"]["reason"], "input_unavailable")
        self.assertFalse(shadow["active"])

    def test_unresolved_shadow_cannot_hot_reload_to_active(self):
        from controllers.directional_trading import hl_rsi

        controller, _, _ = self._shadow_controller()
        with patch.object(
            hl_rsi.HLRSIController,
            "determine_executor_actions",
            return_value=[self._entry_action()],
        ):
            controller.determine_executor_actions()
        controller.config.entry_acquisition_shadow_policy = None
        controller.config.entry_acquisition_policy = "down_only_maker_v1"
        with patch.object(hl_rsi.HLRSIController, "determine_executor_actions", return_value=[]):
            controller.determine_executor_actions()
        self.assertEqual(controller._entry_acquisition_policy, "legacy_single_attempt")
        self.assertEqual(
            controller._entry_acquisition_shadow.snapshot()["terminal_reason"],
            "configuration_changed",
        )

    def _install_temp_watermark(self, controller, *, timestamp: int) -> MarketsRecorder:
        controller.config.durable_candle_watermark_enabled = True
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()),
            SQLConnectionType.TRADE_FILLS,
            db_path=str(Path(temp_dir.name) / "controller-watermark.sqlite"),
        )
        recorder = MarketsRecorder.__new__(MarketsRecorder)
        recorder._sql_manager = manager
        state = ControllerCandleWatermarkState(
            namespace=ControllerCandleWatermarkNamespace(
                controller_id=controller.config.id,
                connector_name=controller.config.candles_connector or controller.config.connector_name,
                trading_pair=controller.config.candles_trading_pair or controller.config.trading_pair,
                interval=controller.config.interval,
            ),
            candle_open_timestamp_s=timestamp,
            revision=0,
            schema_version=1,
            updated_at_ms=(timestamp + 60) * 1000,
        )
        self.assertEqual(
            recorder.seed_controller_candle_watermark(state).outcome,
            ControllerCandleWatermarkOutcome.SEEDED,
        )
        recorder_patcher = patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder)
        recorder_patcher.start()
        self.addCleanup(recorder_patcher.stop)
        return recorder

    @staticmethod
    def _c01_4_causal_candles(*, first_timestamp: int, count: int = 263) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "timestamp": first_timestamp + (index * 60),
                    "open": 50_000 + (index * 2) + (120 * math.sin(index / 9)) - 8,
                    "high": 50_000 + (index * 2) + (120 * math.sin(index / 9)) + 25,
                    "low": 50_000 + (index * 2) + (120 * math.sin(index / 9)) - 25,
                    "close": 50_000 + (index * 2) + (120 * math.sin(index / 9)),
                    "volume": 100 + (index % 11),
                    "quote_asset_volume": (100 + (index % 11)) * (50_000 + (index * 2) + (120 * math.sin(index / 9))),
                }
                for index in range(count)
            ]
        )

    def _configure_c01_4_causal_feed(
        self,
        controller,
        *,
        persisted_timestamp: int,
        successor_count: int = 2,
    ) -> pd.DataFrame:
        candles = self._c01_4_causal_candles(
            first_timestamp=persisted_timestamp - (259 * 60),
            count=260 + successor_count,
        )
        controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: (
            candles.tail(kwargs["max_records"]).copy()
        )
        return candles

    def _complete_c01_4_owner_reconstruction(self, controller, *, persisted_timestamp: int) -> None:
        asyncio.run(controller.update_processed_data())

        self.assertFalse(controller._watermark_permanent_hold)
        self.assertEqual(controller._last_evaluated_candle_timestamp, float(persisted_timestamp))
        self.assertEqual(controller.processed_data["candle_timestamp"], float(persisted_timestamp))
        self.assertEqual(
            controller.processed_data["signal_suppression_reason"],
            "durable_watermark_owner_reconstruction",
        )
        self.assertFalse(controller._watermark_proposal_grant)
        self.assertEqual(controller.create_actions_proposal(), [])

    def test_module_exposes_okx_spot_identity(self):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        config = OKRSIControllerConfig(id="ok-rsi-default")

        self.assertIsNotNone(OKRSIController)
        self.assertEqual(config.controller_name, "ok_rsi")
        self.assertEqual(config.connector_name, "okx")
        self.assertEqual(config.trading_pair, "BTC-USDC")
        self.assertFalse(config.durable_candle_watermark_enabled)

        with self.assertRaisesRegex(ValueError, "legacy action path"):
            OKRSIControllerConfig(
                id="invalid-combined-runtime",
                durable_candle_watermark_enabled=True,
                episode_enabled=True,
            )

    def test_derived_higher_timeframe_uses_only_complete_consecutive_base_candles(self):
        from controllers.directional_trading.ok_rsi import derive_higher_timeframe_candles

        base = pd.DataFrame(
            [
                {
                    "timestamp": 1_800_000_000 + minute * 60,
                    "open": str(100 + minute),
                    "high": str(102 + minute),
                    "low": str(99 + minute),
                    "close": str(101 + minute),
                    "volume": str(10 + minute),
                    "quote_asset_volume": str(1_000 + minute),
                }
                for minute in range(6)
            ]
        )

        derived = derive_higher_timeframe_candles(
            base,
            base_interval="1m",
            target_interval="5m",
            current_time=1_800_000_390,
            max_records=10,
        )

        self.assertEqual(
            derived.to_dict(orient="records"),
            [
                {
                    "timestamp": 1_800_000_000,
                    "open": 100,
                    "high": 106,
                    "low": 99,
                    "close": 105,
                    "volume": 60,
                    "quote_asset_volume": 5_010,
                }
            ],
        )

    def test_derived_higher_timeframe_drops_bucket_with_missing_base_candle(self):
        from controllers.directional_trading.ok_rsi import derive_higher_timeframe_candles

        base = pd.DataFrame(
            {
                "timestamp": [1_800_000_000, 1_800_000_060, 1_800_000_180, 1_800_000_240],
                "open": [100, 101, 103, 104],
                "high": [101, 102, 104, 105],
                "low": [99, 100, 102, 103],
                "close": [100.5, 101.5, 103.5, 104.5],
                "volume": [1, 1, 1, 1],
            }
        )

        derived = derive_higher_timeframe_candles(
            base,
            base_interval="1m",
            target_interval="5m",
            current_time=1_800_000_360,
            max_records=10,
        )

        self.assertTrue(derived.empty)

    def test_derived_higher_timeframe_matches_reference_on_mixed_complete_buckets(self):
        from controllers.directional_trading.ok_rsi import derive_higher_timeframe_candles

        start = 1_800_000_000
        base = pd.DataFrame(
            [
                {
                    "timestamp": start + minute * 60,
                    "open": 100 + minute / 10,
                    "high": 101 + minute / 10,
                    "low": 99 + minute / 10,
                    "close": 100.5 + minute / 10,
                    "volume": 1 + (minute % 7),
                    "quote_asset_volume": 100 + minute,
                    "n_trades": 2 + (minute % 5),
                }
                for minute in range(180)
            ]
        )
        base = base.drop(index=[31, 94]).reset_index(drop=True)
        base = pd.concat([base, base.iloc[[60]]], ignore_index=True)
        base.loc[base["timestamp"].eq(start + (125 * 60)), "volume"] = float("nan")

        actual = derive_higher_timeframe_candles(
            base,
            base_interval="1m",
            target_interval="15m",
            current_time=start + (177 * 60),
            max_records=7,
        )

        frame = base.copy()
        numeric = [column for column in frame.columns if column != "timestamp"]
        frame[numeric] = frame[numeric].apply(pd.to_numeric, errors="coerce")
        frame = frame.sort_values("timestamp")
        frame["bucket"] = (frame["timestamp"] // 900) * 900
        frame = frame[frame["bucket"] + 900 <= start + (177 * 60)]
        expected_records = []
        for bucket_timestamp, bucket in frame.groupby("bucket", sort=True):
            if bucket["timestamp"].tolist() != list(range(int(bucket_timestamp), int(bucket_timestamp) + 900, 60)):
                continue
            if bucket[numeric].isna().any().any():
                continue
            expected_records.append(
                {
                    "timestamp": int(bucket_timestamp),
                    "open": bucket["open"].iloc[0],
                    "high": bucket["high"].max(),
                    "low": bucket["low"].min(),
                    "close": bucket["close"].iloc[-1],
                    "volume": bucket["volume"].sum(),
                    "quote_asset_volume": bucket["quote_asset_volume"].sum(),
                    "n_trades": bucket["n_trades"].sum(),
                }
            )
        expected = pd.DataFrame(expected_records).tail(7).reset_index(drop=True)

        pd.testing.assert_frame_equal(actual, expected, check_dtype=False)

    def test_controller_creates_only_base_feed_with_history_for_derived_intervals(self):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        config = OKRSIControllerConfig(
            id="derived-feed",
            interval="1m",
            trend_confirmation_medium_interval="5m",
            trend_confirmation_long_interval="15m",
            derived_higher_timeframe_max_records=100,
        )
        controller = OKRSIController(
            config,
            self._provider(base_balance=Decimal("0"), price=Decimal("50000")),
            asyncio.Queue(),
        )

        feeds = controller.get_candles_config()

        self.assertEqual(len(feeds), 1)
        self.assertEqual(feeds[0].interval, "1m")
        self.assertEqual(feeds[0].max_records, 1500)

    def test_controller_derives_configured_interval_without_requesting_native_feed(self):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        provider = self._provider(base_balance=Decimal("0"), price=Decimal("50000"))
        start = 1_799_998_200
        provider.time.return_value = 1_800_000_060
        provider.get_candles_df.return_value = pd.DataFrame(
            {
                "timestamp": [start + minute * 60 for minute in range(30)],
                "open": [100 + minute for minute in range(30)],
                "high": [101 + minute for minute in range(30)],
                "low": [99 + minute for minute in range(30)],
                "close": [100.5 + minute for minute in range(30)],
                "volume": [1] * 30,
            }
        )
        controller = OKRSIController(
            OKRSIControllerConfig(
                id="configurable-derived-interval",
                interval="1m",
                trend_confirmation_medium_interval="30m",
                trend_confirmation_long_interval="1h",
            ),
            provider,
            asyncio.Queue(),
        )

        derived = controller._get_interval_candles_df("30m")

        self.assertEqual(len(derived), 1)
        self.assertEqual(derived.iloc[0]["timestamp"], start)
        self.assertTrue(all(call.kwargs["interval"] == "1m" for call in provider.get_candles_df.call_args_list))

    def test_derived_interval_returns_unavailable_when_base_feed_read_fails(self):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        provider = self._provider(base_balance=Decimal("0"), price=Decimal("50000"))
        provider.get_candles_df.side_effect = RuntimeError("feed unavailable")
        controller = OKRSIController(
            OKRSIControllerConfig(id="derived-feed-unavailable"),
            provider,
            asyncio.Queue(),
        )

        self.assertIsNone(controller._get_interval_candles_df("5m"))

    def test_config_rejects_higher_timeframe_that_is_not_a_base_interval_multiple(self):
        from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

        with self.assertRaisesRegex(ValueError, "multiple of the base interval"):
            OKRSIControllerConfig(
                id="invalid-derived-ratio",
                interval="5m",
                trend_confirmation_medium_interval="3m",
            )

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
        controller._watermark_proposal_grant = True
        controller._watermark_result = ControllerCandleWatermarkResult(
            outcome=ControllerCandleWatermarkOutcome.ADVANCED_BY_CALLER
        )

        actions = controller.create_actions_proposal()

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].controller_id, controller.config.id)
        self.assertEqual(actions[0].executor_config.side, TradeType.SELL)
        self.assertEqual(actions[0].executor_config.amount, Decimal("0.001"))

    def test_create_actions_proposal_emits_nothing_when_execution_inputs_are_missing(self):
        controller = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
        controller.market_data_provider.get_trading_rules.return_value = None
        controller._watermark_proposal_grant = True
        controller._watermark_result = ControllerCandleWatermarkResult(
            outcome=ControllerCandleWatermarkOutcome.ADVANCED_BY_CALLER
        )

        self.assertEqual(controller.create_actions_proposal(), [])

    def test_durable_watermark_loads_before_evaluation_commits_before_one_shot_proposal(self):
        from controllers.directional_trading import hl_rsi

        controller = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
        recorder = self._install_temp_watermark(controller, timestamp=1_700_033_100)
        self._configure_c01_4_causal_feed(controller, persisted_timestamp=1_700_033_100)
        self._complete_c01_4_owner_reconstruction(controller, persisted_timestamp=1_700_033_100)

        async def evaluate_after_load(instance):
            self.assertEqual(instance._last_evaluated_candle_timestamp, 1_700_033_100)
            self.assertEqual(instance._watermark_state.candle_open_timestamp_s, 1_700_033_100)
            instance._last_evaluated_candle_timestamp = 1_700_033_160
            instance.processed_data = {"signal": -1, "signal_state": {"condition_ok": True}}

        with patch.object(hl_rsi.HLRSIController, "update_processed_data", new=evaluate_after_load):
            asyncio.run(controller.update_processed_data())

        committed = recorder.load_controller_candle_watermark(controller._watermark_namespace)
        self.assertEqual(committed.state.candle_open_timestamp_s, 1_700_033_160)
        self.assertEqual(committed.state.revision, 1)
        self.assertEqual(committed.state.updated_at_ms, 1_725_000_000_000)
        self.assertEqual(controller._watermark_result.outcome, ControllerCandleWatermarkOutcome.ADVANCED_BY_CALLER)
        self.assertTrue(controller._watermark_proposal_grant)

        first_actions = controller.create_actions_proposal()
        second_actions = controller.create_actions_proposal()
        self.assertEqual(len(first_actions), 1)
        self.assertEqual(second_actions, [])
        self.assertFalse(controller._watermark_proposal_grant)
        watermark_info = controller.get_custom_info()["durable_evaluation_watermark"]
        self.assertEqual(watermark_info["outcome"], "ADVANCED_BY_CALLER")
        self.assertFalse(watermark_info["permanent_hold"])
        self.assertFalse(watermark_info["proposal_eligible"])

        reconstructed = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
        self._configure_c01_4_causal_feed(reconstructed, persisted_timestamp=1_700_033_160)
        self._complete_c01_4_owner_reconstruction(reconstructed, persisted_timestamp=1_700_033_160)
        self.assertEqual(reconstructed.create_actions_proposal(), [])
        self.assertEqual(
            reconstructed._watermark_result.outcome,
            ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
        )

    def test_missing_watermark_initializes_from_owner_evaluated_closed_candle_without_action(self):
        from controllers.directional_trading import hl_rsi

        controller = self._controller(
            base_balance=Decimal("0.01"), price=Decimal("50000")
        )
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()),
            SQLConnectionType.TRADE_FILLS,
            db_path=str(Path(temp_dir.name) / "controller-watermark.sqlite"),
        )
        recorder = MarketsRecorder.__new__(MarketsRecorder)
        recorder._sql_manager = manager

        async def evaluate_latest_closed(instance):
            instance._last_evaluated_candle_timestamp = 1_700_033_160
            instance.processed_data = {
                "signal": -1,
                "candle_timestamp": 1_700_033_160.0,
                "data_health": {"ready": True},
            }

        with (
            patch.object(
                MarketsRecorder, "get_existing_instance", return_value=recorder
            ),
            patch.object(
                hl_rsi.HLRSIController,
                "update_processed_data",
                new=evaluate_latest_closed,
            ),
        ):
            asyncio.run(controller.update_processed_data())

        initialized = recorder.load_controller_candle_watermark(
            controller._watermark_namespace
        )
        self.assertEqual(
            initialized.outcome,
            ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
        )
        self.assertEqual(
            initialized.state.candle_open_timestamp_s, 1_700_033_160
        )
        self.assertEqual(controller._watermark_result.outcome, ControllerCandleWatermarkOutcome.SEEDED)
        self.assertFalse(controller._watermark_initialization_required)
        self.assertFalse(controller._watermark_proposal_grant)
        self.assertEqual(controller.create_actions_proposal(), [])
        self.assertEqual(
            controller.processed_data["signal_suppression_reason"],
            "durable_watermark_initialized",
        )

    def test_durable_watermark_absence_and_cas_failure_permanently_hold_without_retry(self):
        from controllers.directional_trading import hl_rsi

        absent = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=None):
            asyncio.run(absent.update_processed_data())
        self.assertTrue(absent._watermark_permanent_hold)
        self.assertEqual(absent._watermark_result.outcome, ControllerCandleWatermarkOutcome.STORE_UNAVAILABLE)
        self.assertEqual(absent.processed_data["signal"], 0)
        absent.market_data_provider.get_candles_df.assert_not_called()

        namespace = ControllerCandleWatermarkNamespace(
            controller_id=absent.config.id,
            connector_name="okx",
            trading_pair="BTC-USDT",
            interval="1m",
        )
        state = ControllerCandleWatermarkState(
            namespace=namespace,
            candle_open_timestamp_s=1_700_033_100,
            revision=0,
            schema_version=1,
            updated_at_ms=1_700_033_160_000,
        )
        recorder = MagicMock()
        recorder.load_controller_candle_watermark.return_value = ControllerCandleWatermarkResult(
            outcome=ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
            state=state,
        )
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            asyncio.run(absent.update_processed_data())
        recorder.load_controller_candle_watermark.assert_not_called()

        failed = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
        failed_recorder = MagicMock()
        failed_recorder.load_controller_candle_watermark.return_value = ControllerCandleWatermarkResult(
            outcome=ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
            state=state,
        )
        failed_recorder.compare_and_advance_controller_candle_watermark.return_value = (
            ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.COMMIT_FAILED,
                state=state,
                reason="injected",
            )
        )

        async def evaluate(instance):
            instance._last_evaluated_candle_timestamp = 1_700_033_160
            instance.processed_data = {"signal": 1, "signal_state": {"condition_ok": True}}

        self._configure_c01_4_causal_feed(failed, persisted_timestamp=1_700_033_100)
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=failed_recorder):
            self._complete_c01_4_owner_reconstruction(failed, persisted_timestamp=1_700_033_100)
            # This test intentionally isolates the post-reconstruction CAS failure contract.
            with patch.object(hl_rsi.HLRSIController, "update_processed_data", new=evaluate):
                asyncio.run(failed.update_processed_data())
                asyncio.run(failed.update_processed_data())

        self.assertTrue(failed._watermark_permanent_hold)
        self.assertEqual(failed.processed_data["signal"], 0)
        self.assertEqual(failed.create_actions_proposal(), [])
        self.assertEqual(failed_recorder.load_controller_candle_watermark.call_count, 1)
        self.assertEqual(failed_recorder.compare_and_advance_controller_candle_watermark.call_count, 1)
        self.assertEqual(
            failed_recorder.compare_and_advance_controller_candle_watermark.call_args.kwargs["updated_at_ms"],
            1_725_000_000_000,
        )
        status = failed.get_status_summary()["durable_evaluation_watermark"]
        self.assertEqual(status["outcome"], "COMMIT_FAILED")
        self.assertTrue(status["permanent_hold"])

    def test_durable_watermark_stale_writer_refreshes_already_committed_without_permanent_hold(self):
        from controllers.directional_trading import hl_rsi

        controller = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
        namespace = ControllerCandleWatermarkNamespace(
            controller_id=controller.config.id,
            connector_name="okx",
            trading_pair="BTC-USDT",
            interval="1m",
        )
        loaded = ControllerCandleWatermarkState(
            namespace=namespace,
            candle_open_timestamp_s=1_700_033_100,
            revision=0,
            schema_version=1,
            updated_at_ms=1_700_033_160_000,
        )
        committed_by_peer = ControllerCandleWatermarkState(
            namespace=namespace,
            candle_open_timestamp_s=1_700_033_160,
            revision=1,
            schema_version=1,
            updated_at_ms=1_700_033_220_000,
        )
        next_committed = ControllerCandleWatermarkState(
            namespace=namespace,
            candle_open_timestamp_s=1_700_033_220,
            revision=2,
            schema_version=1,
            updated_at_ms=1_700_033_280_000,
        )
        recorder = MagicMock()
        recorder.load_controller_candle_watermark.return_value = ControllerCandleWatermarkResult(
            outcome=ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
            state=loaded,
        )
        recorder.compare_and_advance_controller_candle_watermark.side_effect = [
            ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
                state=committed_by_peer,
            ),
            ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.ADVANCED_BY_CALLER,
                state=next_committed,
            ),
        ]

        async def evaluate(instance):
            instance._last_evaluated_candle_timestamp = 1_700_033_160
            instance.processed_data = {"signal": -1, "signal_state": {"condition_ok": True}}

        self._configure_c01_4_causal_feed(controller, persisted_timestamp=1_700_033_100)
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            self._complete_c01_4_owner_reconstruction(controller, persisted_timestamp=1_700_033_100)
            # This test intentionally isolates same-candle stale-writer CAS after reconstruction.
            with patch.object(hl_rsi.HLRSIController, "update_processed_data", new=evaluate):
                asyncio.run(controller.update_processed_data())

        self.assertFalse(controller._watermark_permanent_hold)
        self.assertFalse(controller._watermark_proposal_grant)
        self.assertEqual(controller._watermark_state, committed_by_peer)
        self.assertEqual(controller._watermark_result.outcome, ControllerCandleWatermarkOutcome.ALREADY_COMMITTED)
        self.assertEqual(controller.processed_data["signal"], 0)
        self.assertEqual(
            controller.processed_data["signal_suppression_reason"],
            "durable_watermark_already_committed",
        )
        self.assertEqual(controller.create_actions_proposal(), [])

        async def evaluate_next(instance):
            self.assertEqual(instance._last_evaluated_candle_timestamp, 1_700_033_160)
            instance._last_evaluated_candle_timestamp = 1_700_033_220
            instance.processed_data = {"signal": -1, "signal_state": {"condition_ok": True}}

        with patch.object(hl_rsi.HLRSIController, "update_processed_data", new=evaluate_next):
            asyncio.run(controller.update_processed_data())
        self.assertFalse(controller._watermark_permanent_hold)
        self.assertEqual(controller._watermark_state, next_committed)
        self.assertTrue(controller._watermark_proposal_grant)
        self.assertEqual(len(controller.create_actions_proposal()), 1)

    def test_durable_watermark_non_advancing_owner_candle_is_a_permanent_regression_hold(self):
        from controllers.directional_trading import hl_rsi

        controller = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
        recorder = self._install_temp_watermark(controller, timestamp=1_700_033_160)
        self._configure_c01_4_causal_feed(controller, persisted_timestamp=1_700_033_160)
        self._complete_c01_4_owner_reconstruction(controller, persisted_timestamp=1_700_033_160)
        original_cas = recorder.compare_and_advance_controller_candle_watermark

        async def observe_older_candle(instance):
            self.assertEqual(instance._last_evaluated_candle_timestamp, 1_700_033_160)
            instance.processed_data = {
                "signal": 0,
                "signal_suppression_reason": "non_advancing_candle",
                "candle_timestamp": 1_700_033_100,
            }

        with patch.object(
            recorder,
            "compare_and_advance_controller_candle_watermark",
            wraps=original_cas,
        ) as cas:
            with patch.object(hl_rsi.HLRSIController, "update_processed_data", new=observe_older_candle):
                asyncio.run(controller.update_processed_data())

        cas.assert_not_called()
        self.assertTrue(controller._watermark_permanent_hold)
        self.assertEqual(controller._watermark_result.outcome, ControllerCandleWatermarkOutcome.REGRESSION)
        self.assertFalse(controller._watermark_proposal_grant)
        self.assertEqual(controller.create_actions_proposal(), [])
        persisted = recorder.load_controller_candle_watermark(controller._watermark_namespace).state
        self.assertEqual(persisted.candle_open_timestamp_s, 1_700_033_160)
        self.assertEqual(persisted.revision, 0)

    def test_durable_watermark_reconstruction_covers_all_three_preregistered_crash_boundaries(self):
        from controllers.directional_trading import hl_rsi

        recorder = None

        def new_controller(*, reconstruction_timestamp: int):
            nonlocal recorder
            instance = self._controller(base_balance=Decimal("0.01"), price=Decimal("50000"))
            if recorder is None:
                recorder = self._install_temp_watermark(instance, timestamp=1_700_033_100)
            self._configure_c01_4_causal_feed(instance, persisted_timestamp=reconstruction_timestamp)
            self._complete_c01_4_owner_reconstruction(
                instance,
                persisted_timestamp=reconstruction_timestamp,
            )
            return instance

        async def evaluate_3160(instance):
            instance._last_evaluated_candle_timestamp = 1_700_033_160
            instance.processed_data = {"signal": -1, "signal_state": {"condition_ok": True}}

        before_commit = new_controller(reconstruction_timestamp=1_700_033_100)
        # Each crash boundary is intentionally injected after mandatory owner reconstruction.
        with patch.object(hl_rsi.HLRSIController, "update_processed_data", new=evaluate_3160):
            with patch.object(
                recorder,
                "compare_and_advance_controller_candle_watermark",
                side_effect=SystemExit("simulated process crash before commit"),
            ):
                with self.assertRaisesRegex(SystemExit, "before commit"):
                    asyncio.run(before_commit.update_processed_data())
        self.assertEqual(
            recorder.load_controller_candle_watermark(before_commit._watermark_namespace).state.candle_open_timestamp_s,
            1_700_033_100,
        )

        after_commit_before_proposal = new_controller(reconstruction_timestamp=1_700_033_100)
        with patch.object(hl_rsi.HLRSIController, "update_processed_data", new=evaluate_3160):
            asyncio.run(after_commit_before_proposal.update_processed_data())
        self.assertTrue(after_commit_before_proposal._watermark_proposal_grant)

        reconstructed_after_commit = new_controller(reconstruction_timestamp=1_700_033_160)
        self.assertEqual(reconstructed_after_commit.create_actions_proposal(), [])

        after_proposal_before_queue = new_controller(reconstruction_timestamp=1_700_033_160)

        async def evaluate_3220(instance):
            self.assertEqual(instance._last_evaluated_candle_timestamp, 1_700_033_160)
            instance._last_evaluated_candle_timestamp = 1_700_033_220
            instance.processed_data = {"signal": -1, "signal_state": {"condition_ok": True}}

        with patch.object(hl_rsi.HLRSIController, "update_processed_data", new=evaluate_3220):
            asyncio.run(after_proposal_before_queue.update_processed_data())
        proposal_fingerprint = after_proposal_before_queue.create_actions_proposal()
        self.assertEqual(len(proposal_fingerprint), 1)

        reconstructed_after_fingerprint = new_controller(reconstruction_timestamp=1_700_033_220)
        self.assertEqual(reconstructed_after_fingerprint.create_actions_proposal(), [])

    def test_c01_4_reconstructs_persisted_candle_before_advancing_visible_successor(self):
        controller = self._buy_controller(atr_pct=None)
        candles = self._c01_4_causal_candles(first_timestamp=1_700_017_500)
        persisted_timestamp = int(candles.iloc[259]["timestamp"])
        successor_timestamp = int(candles.iloc[260]["timestamp"])
        persisted_close = Decimal(str(candles.iloc[259]["close"]))
        successor_close = Decimal(str(candles.iloc[260]["close"]))
        visible = candles.iloc[:261].copy()
        controller.market_data_provider.time.return_value = float(successor_timestamp + 60)
        controller.market_data_provider.get_price_by_type.return_value = successor_close
        controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: (
            visible.tail(kwargs["max_records"]).copy()
        )
        cost_basis = Decimal("50000")
        controller.positions_held = [
            SimpleNamespace(
                connector_name="okx",
                trading_pair="BTC-USDT",
                side=TradeType.BUY,
                amount=Decimal("1"),
                breakeven_price=cost_basis,
                cum_fees_quote=Decimal("0"),
            )
        ]
        recorder = self._install_temp_watermark(controller, timestamp=persisted_timestamp)
        original_cas = recorder.compare_and_advance_controller_candle_watermark
        original_detect = controller._regime_detector.detect
        controller._regime_detector.detect = MagicMock(wraps=original_detect)

        with patch.object(
            recorder,
            "compare_and_advance_controller_candle_watermark",
            wraps=original_cas,
        ) as cas:
            asyncio.run(controller.update_processed_data())

            self.assertEqual(controller._last_evaluated_candle_timestamp, float(persisted_timestamp))
            self.assertEqual(controller.processed_data["candle_timestamp"], float(persisted_timestamp))
            self.assertEqual(
                controller.processed_data["signal_suppression_reason"],
                "durable_watermark_owner_reconstruction",
            )
            self.assertEqual(controller.processed_data["signal"], 0)
            self.assertAlmostEqual(
                controller.processed_data["signal_state"]["sell_pnl_pct"],
                float((persisted_close - cost_basis) / cost_basis),
            )
            self.assertFalse(controller._watermark_proposal_grant)
            self.assertEqual(controller.create_actions_proposal(), [])
            self.assertEqual(controller._regime_detector.detect.call_count, 1)
            cas.assert_not_called()
            persisted = recorder.load_controller_candle_watermark(controller._watermark_namespace).state
            self.assertEqual(persisted.candle_open_timestamp_s, persisted_timestamp)
            self.assertEqual(persisted.revision, 0)

            asyncio.run(controller.update_processed_data())

            self.assertEqual(controller._last_evaluated_candle_timestamp, float(successor_timestamp))
            self.assertEqual(controller.processed_data["candle_timestamp"], float(successor_timestamp))
            self.assertEqual(controller._regime_detector.detect.call_count, 2)
            self.assertEqual(cas.call_count, 1)
            committed = recorder.load_controller_candle_watermark(controller._watermark_namespace).state
            self.assertEqual(committed.candle_open_timestamp_s, successor_timestamp)
            self.assertEqual(committed.revision, 1)
            self.assertEqual(
                controller._watermark_result.outcome,
                ControllerCandleWatermarkOutcome.ADVANCED_BY_CALLER,
            )
            self.assertTrue(controller._watermark_proposal_grant)

    def test_c01_4_paper_execution_connector_reconstructs_with_normalized_candle_connector_price(self):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        candles = self._c01_4_causal_candles(first_timestamp=1_700_017_500)
        persisted_timestamp = int(candles.iloc[259]["timestamp"])
        persisted_close = Decimal(str(candles.iloc[259]["close"]))
        successor_timestamp = int(candles.iloc[260]["timestamp"])
        successor_close = Decimal(str(candles.iloc[260]["close"]))
        self.assertNotEqual(persisted_close, successor_close)
        provider = self._provider(base_balance=Decimal("1"), price=successor_close)
        provider.time.return_value = float(successor_timestamp + 60)
        provider.get_candles_df.side_effect = lambda **kwargs: (
            candles.iloc[:261].tail(kwargs["max_records"]).copy()
        )
        config = OKRSIControllerConfig(
            id="ok-rsi-paper-reconstruction",
            connector_name="okx_paper_trade",
            trading_pair="BTC-USDT",
            candles_connector="okx_paper_trade",
            candles_trading_pair="BTC-USDT",
            trade_mode="buy_sell",
            strict_hmm_mode=False,
        )
        self.assertEqual(config.candles_connector, "okx")
        controller = OKRSIController(
            config=config,
            market_data_provider=provider,
            actions_queue=asyncio.Queue(),
        )
        controller.executors_info = []
        cost_basis = Decimal("50000")
        controller.positions_held = [
            SimpleNamespace(
                connector_name="okx_paper_trade",
                trading_pair="BTC-USDT",
                side=TradeType.BUY,
                amount=Decimal("1"),
                breakeven_price=cost_basis,
                cum_fees_quote=Decimal("0"),
            )
        ]
        recorder = self._install_temp_watermark(controller, timestamp=persisted_timestamp)
        original_cas = recorder.compare_and_advance_controller_candle_watermark

        with patch.object(
            recorder,
            "compare_and_advance_controller_candle_watermark",
            wraps=original_cas,
        ) as cas:
            asyncio.run(controller.update_processed_data())

        self.assertFalse(controller._watermark_permanent_hold)
        self.assertEqual(controller._last_evaluated_candle_timestamp, float(persisted_timestamp))
        self.assertEqual(controller.processed_data["candle_timestamp"], float(persisted_timestamp))
        self.assertEqual(
            controller.processed_data["signal_suppression_reason"],
            "durable_watermark_owner_reconstruction",
        )
        self.assertTrue(controller.processed_data["signal_state"]["sell_has_inventory"])
        reconstructed_pnl = controller.processed_data["signal_state"]["sell_pnl_pct"]
        self.assertIsNotNone(reconstructed_pnl)
        self.assertAlmostEqual(
            reconstructed_pnl,
            float((persisted_close - cost_basis) / cost_basis),
        )
        self.assertEqual(controller.processed_data["signal"], 0)
        self.assertFalse(controller._watermark_proposal_grant)
        self.assertEqual(controller.create_actions_proposal(), [])
        cas.assert_not_called()
        persisted = recorder.load_controller_candle_watermark(controller._watermark_namespace).state
        self.assertEqual(persisted.candle_open_timestamp_s, persisted_timestamp)
        self.assertEqual(persisted.revision, 0)

    def test_c01_4_reconstruction_rejects_causal_prefix_below_hmm_fit_threshold(self):
        controller = self._buy_controller(atr_pct=None)
        candles = self._c01_4_causal_candles(
            first_timestamp=1_700_033_040 - (218 * 60),
            count=219,
        )
        persisted_timestamp = int(candles.iloc[-1]["timestamp"])
        controller.market_data_provider.time.return_value = float(persisted_timestamp + 60)
        controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: (
            candles.tail(kwargs["max_records"]).copy()
        )
        recorder = self._install_temp_watermark(controller, timestamp=persisted_timestamp)
        original_cas = recorder.compare_and_advance_controller_candle_watermark
        original_detect = controller._regime_detector.detect
        controller._regime_detector.detect = MagicMock(wraps=original_detect)

        with patch.object(
            recorder,
            "compare_and_advance_controller_candle_watermark",
            wraps=original_cas,
        ) as cas:
            asyncio.run(controller.update_processed_data())

        observed = {
            "permanent_hold": controller._watermark_permanent_hold,
            "outcome": controller._watermark_result.outcome.value,
            "detect_count": controller._regime_detector.detect.call_count,
            "cas_count": cas.call_count,
            "proposal_eligible": controller._watermark_proposal_grant,
            "action_count": len(controller.create_actions_proposal()),
            "reconstruction_target": controller._watermark_reconstruction_target,
        }
        self.assertEqual(
            observed,
            {
                "permanent_hold": True,
                "outcome": "INVALID_STATE",
                "detect_count": 0,
                "cas_count": 0,
                "proposal_eligible": False,
                "action_count": 0,
                "reconstruction_target": persisted_timestamp,
            },
        )

    def test_c01_4_missing_or_behind_persisted_candle_holds_before_owner_admission(self):
        candles = self._c01_4_causal_candles(first_timestamp=1_700_017_500)
        persisted_timestamp = int(candles.iloc[259]["timestamp"])
        successor_timestamp = int(candles.iloc[260]["timestamp"])
        frames = {
            "missing": candles.iloc[:261].drop(index=259).copy(),
            "behind": candles.iloc[:259].copy(),
        }

        for case_id, frame in frames.items():
            with self.subTest(case_id=case_id):
                controller = self._buy_controller(atr_pct=None)
                controller.market_data_provider.time.return_value = float(successor_timestamp + 60)
                controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: (
                    frame.tail(kwargs["max_records"]).copy()
                )
                recorder = self._install_temp_watermark(controller, timestamp=persisted_timestamp)
                original_cas = recorder.compare_and_advance_controller_candle_watermark
                original_detect = controller._regime_detector.detect
                controller._regime_detector.detect = MagicMock(wraps=original_detect)

                with patch.object(
                    recorder,
                    "compare_and_advance_controller_candle_watermark",
                    wraps=original_cas,
                ) as cas:
                    asyncio.run(controller.update_processed_data())

                self.assertTrue(controller._watermark_permanent_hold)
                self.assertEqual(
                    controller._watermark_result.outcome,
                    ControllerCandleWatermarkOutcome.INVALID_STATE,
                )
                self.assertEqual(controller.processed_data["signal"], 0)
                self.assertEqual(
                    controller.processed_data["signal_suppression_reason"],
                    "durable_watermark_invalid_state",
                )
                self.assertFalse(controller._watermark_proposal_grant)
                self.assertEqual(controller.create_actions_proposal(), [])
                self.assertEqual(controller._regime_detector.detect.call_count, 0)
                cas.assert_not_called()
                persisted = recorder.load_controller_candle_watermark(controller._watermark_namespace).state
                self.assertEqual(persisted.candle_open_timestamp_s, persisted_timestamp)
                self.assertEqual(persisted.revision, 0)

    def test_c01_4_reconstruction_neutralizes_owner_buy_without_discarding_decision_state(self):
        controller = self._buy_controller(atr_pct=None)
        candles = self._c01_4_causal_candles(first_timestamp=1_700_017_500)
        persisted_timestamp = int(candles.iloc[259]["timestamp"])
        successor_timestamp = int(candles.iloc[260]["timestamp"])
        visible = candles.iloc[:261].copy()
        controller.market_data_provider.time.return_value = float(successor_timestamp + 60)
        controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: (
            visible.tail(kwargs["max_records"]).copy()
        )
        controller._determine_signal = MagicMock(return_value=1)
        recorder = self._install_temp_watermark(controller, timestamp=persisted_timestamp)
        original_cas = recorder.compare_and_advance_controller_candle_watermark

        with patch.object(
            recorder,
            "compare_and_advance_controller_candle_watermark",
            wraps=original_cas,
        ) as cas:
            asyncio.run(controller.update_processed_data())

        self.assertEqual(controller._last_evaluated_candle_timestamp, float(persisted_timestamp))
        self.assertEqual(controller.processed_data["candle_timestamp"], float(persisted_timestamp))
        self.assertEqual(controller.processed_data["signal_state"]["buy_decision"], "ready")
        self.assertEqual(controller.processed_data["signal_state"]["buy_reason"], "signal-generated")
        self.assertEqual(controller.processed_data["signal"], 0)
        self.assertEqual(
            controller.processed_data["signal_suppression_reason"],
            "durable_watermark_owner_reconstruction",
        )
        self.assertFalse(controller._watermark_proposal_grant)
        self.assertEqual(controller.create_actions_proposal(), [])
        self.assertEqual(controller.determine_executor_actions(), [])
        cas.assert_not_called()

    def test_c01_4_reconstruction_owner_exception_restores_provider_and_holds_without_cas(self):
        from controllers.directional_trading import hl_rsi

        controller = self._buy_controller(atr_pct=None)
        persisted_timestamp = 1_700_033_040
        self._configure_c01_4_causal_feed(
            controller,
            persisted_timestamp=persisted_timestamp,
        )
        original_provider = controller.market_data_provider
        recorder = self._install_temp_watermark(controller, timestamp=persisted_timestamp)
        original_cas = recorder.compare_and_advance_controller_candle_watermark

        async def fail_inside_owner(instance):
            self.assertIsNot(instance.market_data_provider, original_provider)
            self.assertEqual(instance.market_data_provider.time(), float(persisted_timestamp + 60))
            visible = instance.market_data_provider.get_candles_df(
                connector_name="okx",
                trading_pair="BTC-USDT",
                interval="1m",
                max_records=500,
            )
            self.assertEqual(float(visible.iloc[-1]["timestamp"]), float(persisted_timestamp))
            raise RuntimeError("injected reconstruction owner failure")

        with patch.object(
            recorder,
            "compare_and_advance_controller_candle_watermark",
            wraps=original_cas,
        ) as cas:
            with patch.object(hl_rsi.HLRSIController, "update_processed_data", new=fail_inside_owner):
                asyncio.run(controller.update_processed_data())

        self.assertIs(controller.market_data_provider, original_provider)
        self.assertTrue(controller._watermark_permanent_hold)
        self.assertEqual(
            controller._watermark_result.outcome,
            ControllerCandleWatermarkOutcome.INVALID_STATE,
        )
        self.assertIn("RuntimeError", controller._watermark_result.reason)
        self.assertEqual(controller.processed_data["signal"], 0)
        self.assertFalse(controller._watermark_proposal_grant)
        self.assertEqual(controller.create_actions_proposal(), [])
        cas.assert_not_called()
        persisted = recorder.load_controller_candle_watermark(controller._watermark_namespace).state
        self.assertEqual(persisted.candle_open_timestamp_s, persisted_timestamp)
        self.assertEqual(persisted.revision, 0)

    def test_c01_4_reconstruction_source_oserror_holds_once_before_owner_or_cas(self):
        controller = self._buy_controller(atr_pct=None)
        persisted_timestamp = 1_700_033_040
        controller.market_data_provider.get_candles_df.side_effect = OSError("injected source failure")
        recorder = self._install_temp_watermark(controller, timestamp=persisted_timestamp)
        original_cas = recorder.compare_and_advance_controller_candle_watermark
        original_detect = controller._regime_detector.detect
        controller._regime_detector.detect = MagicMock(wraps=original_detect)

        with patch.object(
            recorder,
            "compare_and_advance_controller_candle_watermark",
            wraps=original_cas,
        ) as cas:
            asyncio.run(controller.update_processed_data())
            asyncio.run(controller.update_processed_data())

        self.assertTrue(controller._watermark_permanent_hold)
        self.assertEqual(
            controller._watermark_result.outcome,
            ControllerCandleWatermarkOutcome.INVALID_STATE,
        )
        self.assertIn("OSError", controller._watermark_result.reason)
        self.assertEqual(controller.market_data_provider.get_candles_df.call_count, 1)
        self.assertEqual(controller._regime_detector.detect.call_count, 0)
        self.assertEqual(controller._watermark_reconstruction_target, persisted_timestamp)
        self.assertFalse(controller._watermark_proposal_grant)
        self.assertEqual(controller.create_actions_proposal(), [])
        cas.assert_not_called()
        persisted = recorder.load_controller_candle_watermark(controller._watermark_namespace).state
        self.assertEqual(persisted.candle_open_timestamp_s, persisted_timestamp)
        self.assertEqual(persisted.revision, 0)

    def test_c01_4_reconstruction_validates_causal_geometry_before_owner_or_cas(self):
        base = self._c01_4_causal_candles(
            first_timestamp=1_700_017_500,
            count=260,
        )
        persisted_timestamp = int(base.iloc[-1]["timestamp"])

        duplicated = pd.concat([base, base.iloc[[-1]]], ignore_index=True)
        out_of_order = base.copy()
        out_of_order.iloc[[257, 258]] = out_of_order.iloc[[258, 257]].to_numpy()
        gapped = base.drop(index=258).reset_index(drop=True)
        non_finite_target = base.copy()
        non_finite_target.loc[non_finite_target.index[-1], "close"] = float("nan")
        defective_frames = {
            "duplicate_persisted_candle": duplicated,
            "out_of_order_prefix": out_of_order,
            "gapped_prefix": gapped,
            "non_finite_target_close": non_finite_target,
        }

        for case_id, frame in defective_frames.items():
            with self.subTest(case_id=case_id):
                controller = self._buy_controller(atr_pct=None)
                controller.market_data_provider.time.return_value = float(persisted_timestamp + 60)
                controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: (
                    frame.tail(kwargs["max_records"]).copy()
                )
                recorder = self._install_temp_watermark(controller, timestamp=persisted_timestamp)
                original_cas = recorder.compare_and_advance_controller_candle_watermark
                original_detect = controller._regime_detector.detect
                controller._regime_detector.detect = MagicMock(wraps=original_detect)

                with patch.object(
                    recorder,
                    "compare_and_advance_controller_candle_watermark",
                    wraps=original_cas,
                ) as cas:
                    asyncio.run(controller.update_processed_data())

                self.assertTrue(controller._watermark_permanent_hold)
                self.assertEqual(
                    controller._watermark_result.outcome,
                    ControllerCandleWatermarkOutcome.INVALID_STATE,
                )
                self.assertEqual(controller._regime_detector.detect.call_count, 0)
                self.assertEqual(controller._watermark_reconstruction_target, persisted_timestamp)
                self.assertFalse(controller._watermark_proposal_grant)
                self.assertEqual(controller.create_actions_proposal(), [])
                cas.assert_not_called()
                persisted = recorder.load_controller_candle_watermark(controller._watermark_namespace).state
                self.assertEqual(persisted.candle_open_timestamp_s, persisted_timestamp)
                self.assertEqual(persisted.revision, 0)

        milliseconds = base.copy()
        milliseconds["timestamp"] = milliseconds["timestamp"] * 1_000

        seconds_controller = self._buy_controller(atr_pct=None)
        seconds_controller.market_data_provider.time.return_value = float(persisted_timestamp + 60)
        seconds_controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: (
            base.tail(kwargs["max_records"]).copy()
        )
        self._install_temp_watermark(seconds_controller, timestamp=persisted_timestamp)
        asyncio.run(seconds_controller.update_processed_data())
        expected_trend_confirmation = seconds_controller.processed_data["trend_confirmation"]

        controller = self._buy_controller(atr_pct=None)
        controller.market_data_provider.time.return_value = float(persisted_timestamp + 60)
        controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: (
            milliseconds.tail(kwargs["max_records"]).copy()
        )
        recorder = self._install_temp_watermark(controller, timestamp=persisted_timestamp)
        original_cas = recorder.compare_and_advance_controller_candle_watermark
        original_detect = controller._regime_detector.detect
        controller._regime_detector.detect = MagicMock(wraps=original_detect)

        with patch.object(
            recorder,
            "compare_and_advance_controller_candle_watermark",
            wraps=original_cas,
        ) as cas:
            asyncio.run(controller.update_processed_data())

        self.assertFalse(controller._watermark_permanent_hold)
        self.assertEqual(controller._last_evaluated_candle_timestamp, float(persisted_timestamp))
        self.assertEqual(controller.processed_data["candle_timestamp"], float(persisted_timestamp))
        self.assertEqual(
            float(controller.processed_data["features"]["timestamp"].iloc[-1]),
            float(persisted_timestamp),
        )
        self.assertEqual(controller.processed_data["trend_confirmation"], expected_trend_confirmation)
        self.assertEqual(controller._regime_detector.detect.call_count, 1)
        self.assertEqual(
            controller.processed_data["signal_suppression_reason"],
            "durable_watermark_owner_reconstruction",
        )
        self.assertFalse(controller._watermark_proposal_grant)
        self.assertEqual(controller.create_actions_proposal(), [])
        cas.assert_not_called()
        persisted = recorder.load_controller_candle_watermark(controller._watermark_namespace).state
        self.assertEqual(persisted.candle_open_timestamp_s, persisted_timestamp)
        self.assertEqual(persisted.revision, 0)

    def test_c01_4_two_real_controllers_share_one_first_writer_action(self):
        candles = self._c01_4_causal_candles(
            first_timestamp=1_700_017_500,
            count=261,
        )
        persisted_timestamp = int(candles.iloc[259]["timestamp"])
        successor_timestamp = int(candles.iloc[260]["timestamp"])
        controllers = [
            self._buy_controller(atr_pct=None),
            self._buy_controller(atr_pct=None),
        ]
        for controller in controllers:
            controller.market_data_provider.time.return_value = float(successor_timestamp + 60)
            controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: (
                candles.tail(kwargs["max_records"]).copy()
            )
            controller._determine_signal = MagicMock(return_value=1)
            original_detect = controller._regime_detector.detect
            controller._regime_detector.detect = MagicMock(wraps=original_detect)

        recorder = self._install_temp_watermark(controllers[0], timestamp=persisted_timestamp)
        # This test exercises watermark first-writer admission after a completed
        # clear setup. Entry admission uses an explicit shared durable test store.
        from controllers.directional_trading.ok_rsi_core.entry_decisions import (
            EntryDecisionGate,
            MetadataEntryDecisionStore,
        )
        store = MetadataEntryDecisionStore(
            recorder._sql_manager, "test-ok-rsi.yml", controllers[0].config.id,
            "okx", "BTC-USDT", "1m",
        )
        EntryDecisionGate(store).observe({
            "candle_timestamp": persisted_timestamp,
            "signal": 0,
            "data_health": {"ready": True},
            "signal_state": {"raw_buy_candidate": False, "condition_ok": True},
        })
        for controller in controllers:
            controller._entry_decision_store = store
        original_cas = recorder.compare_and_advance_controller_candle_watermark
        with patch.object(
            recorder,
            "compare_and_advance_controller_candle_watermark",
            wraps=original_cas,
        ) as cas:
            for controller in controllers:
                asyncio.run(controller.update_processed_data())
                self.assertEqual(controller._last_evaluated_candle_timestamp, float(persisted_timestamp))
                self.assertEqual(
                    controller.processed_data["signal_suppression_reason"],
                    "durable_watermark_owner_reconstruction",
                )
                self.assertFalse(controller._watermark_proposal_grant)
                self.assertEqual(controller.create_actions_proposal(), [])
            cas.assert_not_called()

            for controller in controllers:
                asyncio.run(controller.update_processed_data())

        outcomes = [controller._watermark_result.outcome for controller in controllers]
        action_sets = [controller.create_actions_proposal() for controller in controllers]
        self.assertEqual(
            outcomes,
            [
                ControllerCandleWatermarkOutcome.ADVANCED_BY_CALLER,
                ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
            ],
        )
        self.assertEqual([len(actions) for actions in action_sets], [1, 0])
        self.assertEqual([controller._regime_detector.detect.call_count for controller in controllers], [2, 2])
        self.assertEqual(cas.call_count, 2)
        self.assertEqual(
            [
                call.kwargs["desired_candle_open_timestamp_s"]
                for call in cas.call_args_list
            ],
            [successor_timestamp, successor_timestamp],
        )
        self.assertEqual(controllers[1].processed_data["signal"], 0)
        self.assertEqual(
            controllers[1].processed_data["signal_suppression_reason"],
            "durable_watermark_already_committed",
        )
        self.assertEqual(controllers[0].create_actions_proposal(), [])
        persisted = recorder.load_controller_candle_watermark(controllers[0]._watermark_namespace).state
        self.assertEqual(persisted.candle_open_timestamp_s, successor_timestamp)
        self.assertEqual(persisted.revision, 1)

    def test_c01_4_higher_peer_commit_holds_before_later_owner_or_admission(self):
        controller = self._buy_controller(atr_pct=None)
        candles = self._c01_4_causal_candles(first_timestamp=1_700_017_500)
        persisted_timestamp = int(candles.iloc[259]["timestamp"])
        successor_timestamp = int(candles.iloc[260]["timestamp"])
        peer_timestamp = int(candles.iloc[261]["timestamp"])
        visible_rows = {"count": 261}
        controller.market_data_provider.time.side_effect = lambda: float(
            candles.iloc[visible_rows["count"] - 1]["timestamp"] + 60
        )
        controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: (
            candles.iloc[: visible_rows["count"]].tail(kwargs["max_records"]).copy()
        )

        namespace = ControllerCandleWatermarkNamespace(
            controller_id=controller.config.id,
            connector_name="okx",
            trading_pair="BTC-USDT",
            interval="1m",
        )
        loaded = ControllerCandleWatermarkState(
            namespace=namespace,
            candle_open_timestamp_s=persisted_timestamp,
            revision=0,
            schema_version=1,
            updated_at_ms=(persisted_timestamp + 60) * 1000,
        )
        committed_by_peer = ControllerCandleWatermarkState(
            namespace=namespace,
            candle_open_timestamp_s=peer_timestamp,
            revision=1,
            schema_version=1,
            updated_at_ms=(peer_timestamp + 60) * 1000,
        )
        recorder = MagicMock()
        recorder.load_controller_candle_watermark.return_value = ControllerCandleWatermarkResult(
            outcome=ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
            state=loaded,
        )
        recorder.compare_and_advance_controller_candle_watermark.return_value = (
            ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
                state=committed_by_peer,
            )
        )
        original_detect = controller._regime_detector.detect
        controller._regime_detector.detect = MagicMock(wraps=original_detect)

        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            asyncio.run(controller.update_processed_data())
            self.assertEqual(controller._last_evaluated_candle_timestamp, float(persisted_timestamp))
            self.assertEqual(recorder.compare_and_advance_controller_candle_watermark.call_count, 0)

            asyncio.run(controller.update_processed_data())
            self.assertEqual(controller._watermark_state, committed_by_peer)
            self.assertTrue(controller._watermark_permanent_hold)
            self.assertEqual(
                controller._watermark_result.outcome,
                ControllerCandleWatermarkOutcome.INVALID_STATE,
            )
            self.assertIn("ahead", controller._watermark_result.reason)
            self.assertIsNone(controller._watermark_reconstruction_target)
            self.assertFalse(controller._watermark_proposal_grant)
            self.assertEqual(controller.create_actions_proposal(), [])
            self.assertEqual(recorder.compare_and_advance_controller_candle_watermark.call_count, 1)
            self.assertEqual(
                recorder.compare_and_advance_controller_candle_watermark.call_args.kwargs[
                    "desired_candle_open_timestamp_s"
                ],
                successor_timestamp,
            )

            detect_count_at_hold = controller._regime_detector.detect.call_count
            visible_rows["count"] = 263
            asyncio.run(controller.update_processed_data())
            self.assertEqual(controller._regime_detector.detect.call_count, detect_count_at_hold)
            self.assertFalse(controller._watermark_proposal_grant)
            self.assertEqual(controller.create_actions_proposal(), [])
            self.assertEqual(recorder.compare_and_advance_controller_candle_watermark.call_count, 1)

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
        self._install_temp_watermark(controller, timestamp=int(candles.iloc[-2]["timestamp"]))

        asyncio.run(controller.update_processed_data())
        self.assertEqual(
            controller.processed_data["signal_suppression_reason"],
            "durable_watermark_owner_reconstruction",
        )
        self.assertEqual(controller.processed_data["timestamp"], float(now - 60))

        asyncio.run(controller.update_processed_data())

        self.assertTrue(controller.processed_data["data_health"]["ready"])
        self.assertIn(controller.processed_data["signal"], {-1, 0, 1})
        self.assertIsNone(controller.processed_data["regime_error"])
        self.assertTrue(math.isfinite(controller.processed_data["indicators"]["atr_pct"]))
        self.assertGreater(controller.processed_data["indicators"]["atr_pct"], 0)
        self.assertEqual(controller.processed_data["timestamp"], float(now))
        self.assertLess(controller.processed_data["candle_timestamp"], controller.processed_data["timestamp"])

    def test_market_data_pipeline_evaluates_each_confirmed_candle_only_once(self):
        controller = self._buy_controller(atr_pct=None)
        first_timestamp = 1_725_000_000 - (260 * 60)
        rows = []
        for index in range(261):
            close = 50000 + (index * 2) + (120 * math.sin(index / 9))
            rows.append(
                {
                    "timestamp": first_timestamp + (index * 60),
                    "open": close - 8,
                    "high": close + 25,
                    "low": close - 25,
                    "close": close,
                    "volume": 100 + (index % 11),
                    "quote_asset_volume": (100 + (index % 11)) * close,
                }
            )
        candles = pd.DataFrame(rows)
        visible_rows = {"count": 260}
        controller.market_data_provider.time.side_effect = lambda: float(
            candles.iloc[visible_rows["count"] - 1]["timestamp"] + 60
        )
        controller.market_data_provider.get_candles_df.side_effect = lambda **kwargs: (
            candles.iloc[: visible_rows["count"]].tail(kwargs["max_records"]).copy()
        )
        self._install_temp_watermark(controller, timestamp=int(candles.iloc[visible_rows["count"] - 2]["timestamp"]))
        original_detect = controller._regime_detector.detect
        controller._regime_detector.detect = MagicMock(wraps=original_detect)

        asyncio.run(controller.update_processed_data())
        reconstructed_timestamp = controller.processed_data["candle_timestamp"]
        self.assertEqual(controller._regime_detector.detect.call_count, 1)
        self.assertEqual(
            controller.processed_data["signal_suppression_reason"],
            "durable_watermark_owner_reconstruction",
        )

        asyncio.run(controller.update_processed_data())
        first_candle_timestamp = controller.processed_data["candle_timestamp"]
        self.assertGreater(first_candle_timestamp, reconstructed_timestamp)
        self.assertEqual(controller._regime_detector.detect.call_count, 2)
        self.assertIsNone(controller.processed_data.get("signal_suppression_reason"))

        controller.processed_data["signal"] = 1
        asyncio.run(controller.update_processed_data())

        self.assertEqual(controller._regime_detector.detect.call_count, 2)
        self.assertEqual(controller.processed_data["signal"], 0)
        self.assertEqual(controller.processed_data["signal_suppression_reason"], "same_candle")
        self.assertEqual(controller.processed_data["candle_timestamp"], first_candle_timestamp)
        self.assertNotIn("durable_watermark_hold", controller.processed_data)

        visible_rows["count"] = 261
        asyncio.run(controller.update_processed_data())

        self.assertEqual(controller._regime_detector.detect.call_count, 3)
        self.assertIsNone(controller.processed_data.get("signal_suppression_reason"))
        self.assertGreater(controller.processed_data["candle_timestamp"], first_candle_timestamp)

    def test_market_data_pipeline_blocks_stale_duplicate_and_non_finite_candles(self):
        now = 1_725_000_000
        healthy_rows = self._c01_4_causal_candles(
            first_timestamp=now - (260 * 60),
            count=260,
        ).to_dict(orient="records")
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
                healthy_frame = pd.DataFrame(healthy_rows)
                persisted_timestamp = int(healthy_frame.iloc[-1]["timestamp"])
                controller.market_data_provider.get_candles_df.return_value = healthy_frame
                recorder = self._install_temp_watermark(controller, timestamp=persisted_timestamp)
                self._complete_c01_4_owner_reconstruction(
                    controller,
                    persisted_timestamp=persisted_timestamp,
                )
                controller.market_data_provider.get_candles_df.return_value = candles
                original_cas = recorder.compare_and_advance_controller_candle_watermark
                with patch.object(
                    recorder,
                    "compare_and_advance_controller_candle_watermark",
                    wraps=original_cas,
                ) as cas:
                    asyncio.run(controller.update_processed_data())

                self.assertEqual(controller.processed_data["signal"], 0)
                self.assertFalse(controller.processed_data["data_health"]["ready"])
                self.assertEqual(controller.processed_data["data_health"]["reason"], expected_reason)
                self.assertNotIn("signal_suppression_reason", controller.processed_data)
                self.assertNotIn("durable_watermark_hold", controller.processed_data)
                self.assertFalse(controller._watermark_permanent_hold)
                self.assertFalse(controller._watermark_proposal_grant)
                cas.assert_not_called()
                persisted = recorder.load_controller_candle_watermark(controller._watermark_namespace).state
                self.assertEqual(persisted.candle_open_timestamp_s, persisted_timestamp)
                self.assertEqual(persisted.revision, 0)

    def test_data_health_requires_exact_interval_after_timestamp_normalization(self):
        controller = self._buy_controller(atr_pct=None)
        now = 1_725_000_000.0
        violations = (
            ("missing", [now - 240, now - 180, now - 60], "missing_interval"),
            ("subinterval", [now - 180, now - 150, now - 90], "unexpected_interval"),
        )

        for violation, timestamps, expected_reason in violations:
            for unit, multiplier in (("seconds", 1), ("milliseconds", 1_000)):
                with self.subTest(violation=violation, unit=unit):
                    candles = pd.DataFrame(
                        {
                            "timestamp": [timestamp * multiplier for timestamp in timestamps],
                            "open": [50_000, 50_001, 50_002],
                            "high": [50_010, 50_011, 50_012],
                            "low": [49_990, 49_991, 49_992],
                            "close": [50_005, 50_006, 50_007],
                        }
                    )

                    health = controller._build_data_health(candles, now_ts=now)

                    self.assertFalse(health["ready"])
                    self.assertEqual(health["reason"], expected_reason)

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

            for expected_initializations in (1, 2):
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

    def test_buy_sell_script_uses_canonical_ok_rsi_identity(self):
        from scripts.v2_with_controllers import V2WithControllersConfig

        payload = yaml.safe_load((FIXTURES_PATH / "scripts" / "ok_rsi.yml").read_text())
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
        expected = {
            "ok-rsi-btc": ("BTC-USDC", "rsi-v5-btc-usdc-1m"),
            "ok-rsi-eth": ("ETH-USDC", "rsi-v5-eth-usdc-1m"),
            "ok-rsi-sol": ("SOL-USDC", "rsi-v5-sol-usdc-1m"),
            "ok-rsi-xrp": ("XRP-USDC", "rsi-v5-xrp-usdc-1m"),
            "ok-rsi-bnb": ("BNB-USDC", "rsi-v5-bnb-usdc-1m"),
        }
        self.assertEqual(set(expected), {controller.id for controller in loaded})
        for controller in loaded:
            self.assertEqual(controller.connector_name, "okx")
            self.assertEqual(controller.trading_pair, expected[controller.id][0])
            self.assertEqual(controller.legacy_controller_ids, [expected[controller.id][1]])
        self.assertTrue(all(controller.trade_mode == "buy_sell" for controller in loaded))
        self.assertTrue(all(controller.sell_only_if_profitable for controller in loaded))
        self.assertEqual(len({controller.id for controller in loaded}), 5)

    def test_buy_sell_profiles_never_force_sell(self):
        from scripts.v2_with_controllers import V2WithControllersConfig

        payload = yaml.safe_load((FIXTURES_PATH / "scripts" / "ok_rsi.yml").read_text())
        config = V2WithControllersConfig(**payload)
        with patch(
            "hummingbot.client.settings.CONTROLLERS_CONF_DIR_PATH",
            str(FIXTURES_PATH / "controllers"),
        ):
            loaded = config.load_controller_configs()

        for controller in loaded:
            self.assertEqual(controller.leverage, 1)
            self.assertFalse(controller.manual_kill_switch)
            self.assertTrue(controller.sell_only_if_profitable)
            self.assertIsNone(controller.stop_loss)
            self.assertIsNone(controller.take_profit)
            self.assertIsNone(controller.time_limit)
            self.assertTrue(controller.early_stop_keep_position)

    def test_legacy_controller_ids_reject_blanks_and_duplicates(self):
        from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

        with self.assertRaisesRegex(ValueError, "blank"):
            OKRSIControllerConfig(id="ok-rsi-btc", legacy_controller_ids=[""])
        with self.assertRaisesRegex(ValueError, "unique"):
            OKRSIControllerConfig(
                id="ok-rsi-btc",
                legacy_controller_ids=["rsi-v5-btc-usdc-1m", "rsi-v5-btc-usdc-1m"],
            )

    def test_sell_only_script_loads_only_requested_sell_only_pairs(self):
        strategy_class, config = self._load_script_through_trading_core(
            "ok_rsi_sell_only", "ok_rsi_sell_only.yml"
        )
        loaded = config.load_controller_configs()

        self.assertEqual(strategy_class.__name__, "OKRSISellOnly")
        self.assertEqual(config.__class__.__name__, "OKRSISellOnlyConfig")
        self.assertEqual(
            {controller.trading_pair for controller in loaded},
            {"SUI-USDC", "XLM-USDC", "ENA-USDC"},
        )
        self.assertTrue(all(controller.trade_mode == "sell_only" for controller in loaded))
        self.assertTrue(all(not controller.sell_only_if_profitable for controller in loaded))
        self.assertTrue(all(controller.sell_only_inventory_fraction == Decimal("0.10") for controller in loaded))
        self.assertTrue(all(controller.sell_all_below_quote == Decimal("20") for controller in loaded))
        self.assertTrue(all(controller.trend_confirmation_medium_interval == "5m" for controller in loaded))
        self.assertTrue(all(controller.trend_confirmation_long_interval == "15m" for controller in loaded))
        self.assertTrue(all(controller.derived_higher_timeframe_max_records == 100 for controller in loaded))
        self.assertEqual(strategy_class.markets, {"okx": {controller.trading_pair for controller in loaded}})

    def test_single_mode_script_rejects_duplicate_pair(self):
        from scripts.ok_rsi_sell_only import OKRSISellOnlyConfig

        with self.assertRaisesRegex(ValueError, "unique"):
            OKRSISellOnlyConfig(trading_pairs=["SUI-USDC", "SUI-USDC"])

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
                        trading_pairs=["SUI-USDC"],
                        **invalid_override,
                    )


if __name__ == "__main__":
    unittest.main()
