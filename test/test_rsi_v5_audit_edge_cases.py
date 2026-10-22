"""
RSI V5 audit edge-case regression tests (2026-06-09 live-controller audit).

These tests pin boundary behavior of risk rails and data-health gates in
``controllers/directional_trading/rsi_v5.py`` that the main suite does not
cover explicitly:

- early-stop drawdown threshold boundary (inclusive at exactly -threshold)
- recovery sell sizing fail-closed paths (zero balance, sub-min-notional bag)
- candle data-health age boundary and duplicate-timestamp rejection
- consecutive-loss pause engage/reset boundary at the cooldown edge
- flash-crash drop boundary (inclusive at exactly drop_pct)
- trailing_stop_order_type config validation at the boundary

They are read-only with respect to the live controller: they assert current
behavior and must keep passing without any live-file change.
"""
import ast
import asyncio
import importlib
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction

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


def _make_market_data_provider(**overrides):
    provider = MagicMock()
    provider.initialize_rate_sources.return_value = None
    provider.time.return_value = 1234567890.0
    for key, value in overrides.items():
        setattr(provider, key, value)
    return provider


class RSIv5AuditEdgeCaseTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        ensure_local_controller_imports()

    def test_rsi_v5_has_no_generic_exception_handlers(self):
        controller_path = ROOT_PATH / "controllers" / "directional_trading" / "rsi_v5.py"
        tree = ast.parse(controller_path.read_text())
        generic_handlers = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler):
                continue
            if node.type is None:
                generic_handlers.append(node.lineno)
            elif isinstance(node.type, ast.Name) and node.type.id in {"BaseException", "Exception"}:
                generic_handlers.append(node.lineno)

        self.assertEqual(generic_handlers, [])

    def _build_controller(self, market_data_provider=None, **config_overrides):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig

        provider = market_data_provider or _make_market_data_provider()
        controller = RSIv5Controller(
            config=RSIv5ControllerConfig(
                id="rsi-v5-audit-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                **config_overrides,
            ),
            market_data_provider=provider,
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = []
        controller.executors_info = []
        return controller

    @staticmethod
    def _active_buy_executor(entry_price: str, executor_id: str = "buy-1"):
        return SimpleNamespace(
            id=executor_id,
            is_active=True,
            type="position_executor",
            connector_name="binance",
            trading_pair="ETH-USDC",
            side=TradeType.BUY,
            timestamp=1234567800.0,
            config=SimpleNamespace(entry_price=Decimal(entry_price)),
            custom_info={},
        )

    # ------------------------------------------------------------------
    # Early-stop drawdown boundary
    # ------------------------------------------------------------------

    def test_early_stop_triggers_exactly_at_drawdown_threshold(self):
        controller = self._build_controller(
            early_stop_drawdown_pct=0.05,
            early_stop_keep_position=True,
        )
        controller.executors_info = [self._active_buy_executor("100.00")]

        actions = controller._check_active_executor_drawdown_stop(Decimal("95.00"))

        self.assertEqual(len(actions), 1)
        self.assertIsInstance(actions[0], StopExecutorAction)
        self.assertEqual(actions[0].executor_id, "buy-1")
        self.assertTrue(actions[0].keep_position)

    def test_early_stop_does_not_trigger_just_inside_threshold(self):
        controller = self._build_controller(
            early_stop_drawdown_pct=0.05,
            early_stop_keep_position=True,
        )
        controller.executors_info = [self._active_buy_executor("100.00")]

        actions = controller._check_active_executor_drawdown_stop(Decimal("95.01"))

        self.assertEqual(actions, [])

    def test_early_stop_propagates_keep_position_false(self):
        controller = self._build_controller(
            early_stop_drawdown_pct=0.05,
            early_stop_keep_position=False,
        )
        controller.executors_info = [self._active_buy_executor("100.00")]

        actions = controller._check_active_executor_drawdown_stop(Decimal("90.00"))

        self.assertEqual(len(actions), 1)
        self.assertFalse(actions[0].keep_position)

    def test_early_stop_disabled_when_threshold_zero(self):
        controller = self._build_controller(
            early_stop_drawdown_pct=0.0,
            early_stop_keep_position=True,
        )
        controller.executors_info = [self._active_buy_executor("100.00")]

        actions = controller._check_active_executor_drawdown_stop(Decimal("1.00"))

        self.assertEqual(actions, [])

    # ------------------------------------------------------------------
    # Recovery sell sizing fail-closed paths
    # ------------------------------------------------------------------

    def _recovery_provider(self):
        provider = _make_market_data_provider()
        provider.get_trading_rules.return_value = SimpleNamespace(
            min_order_size=Decimal("0"),
            min_base_amount_increment=Decimal("0.00000001"),
            min_notional_size=Decimal("5"),
            min_order_value=Decimal("5"),
        )
        provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
        return provider

    def test_recovery_sell_amount_zero_when_available_balance_zero(self):
        controller = self._build_controller(market_data_provider=self._recovery_provider())

        amount = controller._resolve_recovery_sell_amount(
            position_amount=Decimal("0.5"),
            target_amount=Decimal("0.25"),
            price=Decimal("100"),
            available_base=Decimal("0"),
        )

        self.assertEqual(amount, Decimal("0"))

    def test_recovery_sell_amount_zero_when_whole_bag_below_min_notional(self):
        controller = self._build_controller(market_data_provider=self._recovery_provider())

        # 0.00006 * 72000 = $4.32 < $5 min notional: fail closed, no dust order.
        amount = controller._resolve_recovery_sell_amount(
            position_amount=Decimal("0.00006"),
            target_amount=Decimal("0.00003"),
            price=Decimal("72000"),
            available_base=Decimal("0.00006"),
        )

        self.assertEqual(amount, Decimal("0"))

    def test_recovery_sell_amount_zero_when_price_non_positive(self):
        controller = self._build_controller(market_data_provider=self._recovery_provider())

        amount = controller._resolve_recovery_sell_amount(
            position_amount=Decimal("1"),
            target_amount=Decimal("1"),
            price=Decimal("0"),
            available_base=Decimal("1"),
        )

        self.assertEqual(amount, Decimal("0"))

    # ------------------------------------------------------------------
    # Data health boundaries
    # ------------------------------------------------------------------

    @staticmethod
    def _healthy_candles(last_ts: float, rows: int = 30, interval_seconds: float = 60.0) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": [last_ts - (rows - 1 - idx) * interval_seconds for idx in range(rows)],
                "open": [100.0 + idx * 0.01 for idx in range(rows)],
                "high": [101.0 + idx * 0.01 for idx in range(rows)],
                "low": [99.0 + idx * 0.01 for idx in range(rows)],
                "close": [100.0 + idx * 0.01 for idx in range(rows)],
                "volume": [10.0 for _ in range(rows)],
            }
        )

    def test_data_health_candle_age_boundary_is_inclusive_healthy(self):
        controller = self._build_controller(interval="1m", max_candle_age_intervals=2.5)
        last_ts = 1000.0
        df = self._healthy_candles(last_ts)

        # max age = 60s * 2.5 = 150s; age exactly 150s is still healthy (strict >).
        health_at_boundary = controller._build_data_health(df, now_ts=last_ts + 150.0)
        self.assertTrue(health_at_boundary["ready"])
        self.assertEqual(health_at_boundary["reason"], "ok")

        health_past_boundary = controller._build_data_health(df, now_ts=last_ts + 150.1)
        self.assertFalse(health_past_boundary["ready"])
        self.assertEqual(health_past_boundary["reason"], "stale_candle")

    def test_data_health_rejects_duplicate_timestamps(self):
        controller = self._build_controller(interval="1m")
        df = self._healthy_candles(1000.0)
        df.loc[df.index[-1], "timestamp"] = df.loc[df.index[-2], "timestamp"]

        health = controller._build_data_health(df, now_ts=1010.0)

        self.assertFalse(health["ready"])
        self.assertEqual(health["reason"], "duplicate_timestamp")

    def test_data_health_rejects_out_of_order_timestamps(self):
        controller = self._build_controller(interval="1m")
        df = self._healthy_candles(1000.0)
        df.loc[df.index[-1], "timestamp"] = df.loc[df.index[0], "timestamp"] - 60.0

        health = controller._build_data_health(df, now_ts=1010.0)

        self.assertFalse(health["ready"])
        self.assertEqual(health["reason"], "out_of_order_timestamp")

    # ------------------------------------------------------------------
    # Consecutive-loss pause boundary
    # ------------------------------------------------------------------

    def test_consecutive_loss_pause_engages_then_resets_at_cooldown_edge(self):
        provider = _make_market_data_provider()
        controller = self._build_controller(
            market_data_provider=provider,
            max_consecutive_losses=3,
            consecutive_loss_cooldown=3600,
        )
        loss_ts = 1000000.0
        controller._consecutive_losses = 3
        controller._last_loss_timestamp = loss_ts

        provider.time.return_value = loss_ts + 3599.0
        self.assertTrue(controller._check_consecutive_loss_pause())

        # elapsed == cooldown unpauses and resets the counter (strict <).
        provider.time.return_value = loss_ts + 3600.0
        self.assertFalse(controller._check_consecutive_loss_pause())
        self.assertEqual(controller._consecutive_losses, 0)
        self.assertIsNone(controller._last_loss_timestamp)

    def test_consecutive_loss_pause_disabled_when_max_losses_zero(self):
        controller = self._build_controller(max_consecutive_losses=0)
        controller._consecutive_losses = 99
        controller._last_loss_timestamp = 1000000.0

        self.assertFalse(controller._check_consecutive_loss_pause())

    def test_zero_pnl_executor_does_not_clear_consecutive_loss_streak(self):
        provider = _make_market_data_provider()
        controller = self._build_controller(market_data_provider=provider)
        controller._consecutive_losses = 2
        controller._last_loss_timestamp = 1000000.0

        controller._track_executor_result(SimpleNamespace(net_pnl_quote=Decimal("0")))

        self.assertEqual(controller._consecutive_losses, 2)
        self.assertEqual(controller._last_loss_timestamp, 1000000.0)

    def test_executor_result_is_counted_only_once_after_terminal_settlement(self):
        controller = self._build_controller()
        shutting_down = SimpleNamespace(
            id="entry",
            is_done=False,
            status=RunnableStatus.SHUTTING_DOWN,
            net_pnl_quote=Decimal("-1"),
        )
        terminal = SimpleNamespace(
            id="entry",
            is_done=True,
            status=RunnableStatus.TERMINATED,
            net_pnl_quote=Decimal("-2"),
        )

        controller._track_executor_result(shutting_down)
        self.assertEqual(controller._consecutive_losses, 0)
        controller._track_executor_result(terminal)
        controller._track_executor_result(terminal)

        self.assertEqual(controller._consecutive_losses, 1)

    def test_changed_terminal_result_blocks_future_entries(self):
        controller = self._build_controller()
        terminal = SimpleNamespace(
            id="entry",
            is_done=True,
            status=RunnableStatus.TERMINATED,
            net_pnl_quote=Decimal("1"),
        )
        controller._track_executor_result(terminal)
        terminal.net_pnl_quote = Decimal("2")

        controller._track_executor_result(terminal)

        self.assertIn("entry", controller._executor_result_conflict_ids)
        self.assertFalse(controller.can_create_executor(1))

    # ------------------------------------------------------------------
    # Flash-crash drop boundary
    # ------------------------------------------------------------------

    def test_flash_crash_boundary_is_inclusive(self):
        controller = self._build_controller(flash_crash_lookback=5, flash_crash_drop_pct=0.03)
        rows = 10
        df = self._healthy_candles(1000.0, rows=rows)
        df["high"] = [100.0] * rows
        df["low"] = [99.0] * rows
        # Reference high is taken lookback+1 candles back; current low sets the drop.
        df.loc[df.index[-6], "high"] = 100.0
        df.loc[df.index[-1], "low"] = 97.0  # exactly 3% below the reference high

        self.assertTrue(controller._detect_flash_crash(df))

        df.loc[df.index[-1], "low"] = 97.01  # just inside the threshold
        self.assertFalse(controller._detect_flash_crash(df))

    def test_flash_crash_requires_enough_candles(self):
        controller = self._build_controller(flash_crash_lookback=5, flash_crash_drop_pct=0.03)
        df = self._healthy_candles(1000.0, rows=5)

        self.assertFalse(controller._detect_flash_crash(df))

    # ------------------------------------------------------------------
    # Config validation boundaries
    # ------------------------------------------------------------------

    def test_trailing_stop_order_type_rejects_limit_maker(self):
        from controllers.directional_trading.rsi_v5 import RSIv5ControllerConfig

        with self.assertRaises(Exception):
            RSIv5ControllerConfig(
                id="rsi-v5-audit-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                trailing_stop_order_type="LIMIT_MAKER",
            )

    def test_trailing_stop_order_type_parses_prefixed_string(self):
        from controllers.directional_trading.rsi_v5 import RSIv5ControllerConfig

        config = RSIv5ControllerConfig(
            id="rsi-v5-audit-test",
            connector_name="binance",
            trading_pair="ETH-USDC",
            trailing_stop_order_type="OrderType.MARKET",
        )

        self.assertEqual(config.trailing_stop_order_type, OrderType.MARKET)

    def test_config_rejects_inverted_rsi_thresholds(self):
        from controllers.directional_trading.rsi_v5 import RSIv5ControllerConfig

        with self.assertRaises(Exception):
            RSIv5ControllerConfig(
                id="rsi-v5-audit-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                rsi_buy_threshold=80.0,
                rsi_sell_threshold=70.0,
            )

    def test_config_rejects_entry_size_above_position_cap(self):
        from controllers.directional_trading.rsi_v5 import RSIv5ControllerConfig

        with self.assertRaises(Exception):
            RSIv5ControllerConfig(
                id="rsi-v5-audit-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                usd_per_entry=Decimal("100"),
                max_total_position_usd=Decimal("50"),
            )

    def test_config_rejects_inverted_dynamic_size_bounds(self):
        from controllers.directional_trading.rsi_v5 import RSIv5ControllerConfig

        with self.assertRaises(Exception):
            RSIv5ControllerConfig(
                id="rsi-v5-audit-test",
                connector_name="binance",
                trading_pair="ETH-USDC",
                min_position_size_multiplier=2.0,
                max_position_size_multiplier=1.0,
            )

    def test_interval_to_seconds_handles_invalid_inputs(self):
        from controllers.directional_trading.rsi_v5 import RSIv5Controller

        self.assertEqual(RSIv5Controller._interval_to_seconds("1m"), 60.0)
        self.assertEqual(RSIv5Controller._interval_to_seconds("1s"), 1.0)
        self.assertIsNone(RSIv5Controller._interval_to_seconds(None))
        self.assertIsNone(RSIv5Controller._interval_to_seconds(""))
        self.assertIsNone(RSIv5Controller._interval_to_seconds("xyz"))
        self.assertIsNone(RSIv5Controller._interval_to_seconds("0m"))

    # ------------------------------------------------------------------
    # Sellable inventory fail-closed when balance lookup fails
    # ------------------------------------------------------------------

    def test_sellable_inventory_is_zero_when_balance_lookup_fails(self):
        provider = _make_market_data_provider()
        provider.get_available_balance.side_effect = RuntimeError("exchange down")
        provider.get_balance.side_effect = RuntimeError("exchange down")
        controller = self._build_controller(market_data_provider=provider)
        controller.positions_held = [
            SimpleNamespace(
                connector_name="binance",
                trading_pair="ETH-USDC",
                side=TradeType.BUY,
                amount=Decimal("1"),
                breakeven_price=Decimal("100.00"),
            ),
        ]

        self.assertEqual(controller._get_sellable_inventory_amount(), Decimal("0"))


if __name__ == "__main__":
    unittest.main()
