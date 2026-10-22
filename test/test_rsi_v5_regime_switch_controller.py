import asyncio
import importlib
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

ROOT_PATH = Path(__file__).resolve().parent.parent
ROOT_PATH_STR = str(ROOT_PATH)


def ensure_local_controller_imports():
    while ROOT_PATH_STR in sys.path:
        sys.path.remove(ROOT_PATH_STR)
    sys.path.insert(0, ROOT_PATH_STR)
    for module_name in list(sys.modules):
        if module_name == "controllers" or module_name.startswith("controllers."):
            sys.modules.pop(module_name, None)
    importlib.invalidate_caches()


class RSIv5RegimeSwitchControllerTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        ensure_local_controller_imports()
        from controllers.directional_trading.rsi_v5_regime_switch import (
            RSIv5RegimeSwitchController,
            RSIv5RegimeSwitchControllerConfig,
        )

        self.controller_cls = RSIv5RegimeSwitchController
        self.config_cls = RSIv5RegimeSwitchControllerConfig

    def _controller(self, **config_kwargs):
        market_data_provider = MagicMock()
        market_data_provider.initialize_rate_sources.return_value = None
        market_data_provider.time.return_value = 1234567890.0
        return self.controller_cls(
            config=self.config_cls(
                id="rsi-v5-regime-switch-test",
                connector_name="binance",
                trading_pair="BNB-USDC",
                **config_kwargs,
            ),
            market_data_provider=market_data_provider,
            actions_queue=asyncio.Queue(),
        )

    def test_additive_controller_defaults_are_safe(self):
        config = self.config_cls(id="rsi-v5-regime-switch-test")

        self.assertEqual("rsi_v5_regime_switch", config.controller_name)
        self.assertEqual("stateful_regime_switch_stack_rsi_atr_v1", config.selected_candidate_label)
        self.assertEqual("next50_stack_guardian_network_queue_k20", config.healthy_source_label)
        self.assertEqual("dip_candle_rsi_atr_decile_entry_k20", config.defensive_source_label)
        self.assertTrue(config.regime_switch_enabled)
        self.assertEqual("rebound_confirm", config.buy_confirmation_mode)
        self.assertEqual(Decimal("300"), config.usd_per_entry)
        self.assertEqual(Decimal("2500"), config.max_total_position_usd)
        self.assertEqual(8, config.max_executors_per_side)
        self.assertEqual(0.25, config.buy_confirmation_rsi_delta)
        self.assertEqual(90, config.buy_confirmation_max_wait_seconds)
        self.assertEqual(0.035, config.trailing_activation_pct)
        self.assertEqual(0.025, config.trailing_delta_pct)
        self.assertEqual(22.0, config.trend_confirmation_adx_threshold)
        self.assertFalse(config.local_low_overlay_enabled)
        self.assertEqual(0.5, config.defensive_size_multiplier)
        self.assertLessEqual(config.max_position_size_multiplier, 1.0)

    def test_selected_profile_summary_is_operator_readable(self):
        config = self.config_cls(id="rsi-v5-regime-switch-test")

        summary = config.selected_profile_summary()

        self.assertEqual("stateful_regime_switch_stack_rsi_atr_v1", summary["selected_candidate_label"])
        self.assertEqual("next50_stack_guardian_network_queue_k20", summary["healthy_source_label"])
        self.assertEqual("dip_candle_rsi_atr_decile_entry_k20", summary["defensive_source_label"])
        self.assertEqual("disabled_requires_acknowledgement", summary["local_low_overlay"])
        self.assertEqual("paper_candidate_only", summary["promotion_stage"])

    def test_local_low_overlay_requires_explicit_acknowledgement(self):
        with self.assertRaises(ValueError):
            self.config_cls(id="rsi-v5-regime-switch-test", local_low_overlay_enabled=True)

        config = self.config_cls(
            id="rsi-v5-regime-switch-test",
            local_low_overlay_enabled=True,
            local_low_overlay_acknowledgement=True,
        )

        self.assertTrue(config.local_low_overlay_enabled)

    def test_defensive_policy_blocks_weak_buy_signal(self):
        controller = self._controller(defensive_min_signal_score=3)
        signal_state = {
            "signal_score": 2,
            "condition_ok": True,
            "condition_reason": "ok",
            "size_multiplier": 1.0,
            "buy_decision": "ready",
            "buy_reason": "signal-generated",
            "regime_label": "Trend_Down",
        }

        signal, adjusted = controller._apply_regime_policy_to_signal(
            signal=1,
            signal_state=signal_state,
            regime_label="Trend_Down",
        )

        self.assertEqual(0, signal)
        self.assertFalse(adjusted["condition_ok"])
        self.assertEqual("regime_switch_defensive_score", adjusted["condition_reason"])
        self.assertEqual("blocked", adjusted["buy_decision"])

    def test_defensive_policy_caps_live_size_multiplier(self):
        controller = self._controller(
            defensive_min_signal_score=2,
            defensive_size_multiplier=Decimal("0.50"),
            defensive_max_position_size_multiplier=Decimal("0.75"),
        )
        signal_state = {
            "signal_score": 3,
            "condition_ok": True,
            "condition_reason": "ok",
            "size_multiplier": 1.4,
            "buy_context_bias": "neutral",
            "regime_label": "capitulation",
        }

        signal, adjusted = controller._apply_regime_policy_to_signal(
            signal=1,
            signal_state=signal_state,
            regime_label="capitulation",
        )

        self.assertEqual(1, signal)
        self.assertEqual(0.375, adjusted["size_multiplier"])
        self.assertEqual("hostile", adjusted["buy_context_bias"])
        self.assertEqual("defensive", adjusted["regime_switch_policy"])

    def test_backtest_signal_frame_uses_same_defensive_policy(self):
        controller = self._controller(defensive_min_signal_score=3)
        df = pd.DataFrame(
            {
                "timestamp": [1.0, 2.0],
                "close": [100.0, 97.0],
                "regime_label": ["normal_mean_reversion", "defensive_downtrend"],
            }
        )
        frame = pd.DataFrame(
            {
                "signal": [1, 1],
                "signal_state": [
                    {"signal_score": 3, "condition_ok": True, "size_multiplier": 1.0},
                    {"signal_score": 2, "condition_ok": True, "size_multiplier": 1.0},
                ],
            }
        )

        adjusted = controller._apply_regime_policy_to_backtest_frame(df, frame)

        self.assertEqual(1, int(adjusted.loc[0, "signal"]))
        self.assertEqual(0, int(adjusted.loc[1, "signal"]))
        self.assertEqual("defensive", adjusted.loc[1, "regime_switch_policy"])
        self.assertEqual(
            "regime_switch_defensive_score",
            adjusted.loc[1, "signal_state"]["condition_reason"],
        )


if __name__ == "__main__":
    unittest.main()
