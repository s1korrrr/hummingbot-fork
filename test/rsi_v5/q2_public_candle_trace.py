from __future__ import annotations

import asyncio
import math
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pandas as pd

from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig
from hummingbot.core.data_type.common import PriceType
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction

_BASE_TIME = 1_800_000_000.0
_EVIDENCE_SCOPE = [
    "generated_ohlcv",
    "public_update_processed_data",
    "public_determine_executor_actions",
]


class PublicCandleProvider:
    """Deterministic test double for the controller's public data-provider boundary."""

    def __init__(self, snapshots: list[pd.DataFrame], higher_timeframes: dict[str, pd.DataFrame]) -> None:
        self._snapshots = snapshots
        self._higher_timeframes = higher_timeframes
        self.step = 0
        self.trading_rule = SimpleNamespace(
            min_price_increment=Decimal("0.01"),
            min_order_size=Decimal("0.0001"),
            min_base_amount_increment=Decimal("0.0001"),
            min_notional_size=Decimal("1"),
            min_order_value=Decimal("1"),
        )

    def initialize_rate_sources(self, _sources: list[Any]) -> None:
        return None

    def time(self) -> float:
        return float(self._snapshots[self.step]["timestamp"].iloc[-1]) + 1.0

    def get_candles_df(self, **kwargs: Any) -> pd.DataFrame:
        interval = str(kwargs["interval"])
        if interval == "1m":
            return self._snapshots[self.step].copy()
        return self._higher_timeframes[interval].copy()

    def get_price_by_type(self, _connector: str, _pair: str, price_type: PriceType) -> Decimal:
        close = Decimal(str(self._snapshots[self.step]["close"].iloc[-1]))
        if price_type == PriceType.BestBid:
            return close - Decimal("0.05")
        if price_type == PriceType.BestAsk:
            return close + Decimal("0.05")
        return close

    def get_balance(self, _connector: str, _asset: str) -> Decimal:
        return Decimal("10000")

    def get_available_balance(self, _connector: str, _asset: str) -> Decimal:
        return Decimal("10000")

    def get_trading_rules(self, _connector: str, _pair: str) -> SimpleNamespace:
        return self.trading_rule

    def quantize_order_amount(self, _connector: str, _pair: str, amount: Decimal) -> Decimal:
        return amount.quantize(self.trading_rule.min_base_amount_increment)


def observe_q2_public_candle_trace() -> list[dict[str, Any]]:
    """Run deterministic candle evidence through public RSI v5 controller methods.

    This test-only observer proves controller behavior for generated frames. It
    does not represent exchange feeds, deployed runtime, orders, fills, or
    inventory mutation.
    """

    arm = _generated_candles(240, tail=[102.0, 101.0, 100.0, 99.0, 98.0, 98.25])
    rebound = _append_candle(arm, close=99.10, high=99.30, low=98.10, volume=121.0)
    higher_timeframes = {
        "5m": _generated_candles(100, interval_seconds=300),
        "15m": _generated_candles(100, interval_seconds=900),
    }

    healthy_max_atr = 1.0
    healthy = _observe_scenario(
        scenario_id="healthy_two_update_rebound",
        snapshots=[arm, rebound],
        step_ids=["arm", "rebound"],
        higher_timeframes=higher_timeframes,
        max_atr_pct_to_trade=healthy_max_atr,
    )
    strict_219 = _observe_scenario(
        scenario_id="strict_hmm_219_fail_close",
        snapshots=[_generated_candles(219, tail=[102.0, 101.0, 100.0, 99.0, 98.0, 98.25])],
        step_ids=["strict_hmm_fail_close"],
        higher_timeframes=higher_timeframes,
        max_atr_pct_to_trade=healthy_max_atr,
    )
    veto_max_atr = 0.0001
    atr_veto = _observe_scenario(
        scenario_id="atr_veto_after_rebound",
        snapshots=[arm, rebound],
        step_ids=["arm", "atr_veto"],
        higher_timeframes=higher_timeframes,
        max_atr_pct_to_trade=veto_max_atr,
    )
    return [healthy, strict_219, atr_veto]


def _observe_scenario(
    *,
    scenario_id: str,
    snapshots: list[pd.DataFrame],
    step_ids: list[str],
    higher_timeframes: dict[str, pd.DataFrame],
    max_atr_pct_to_trade: float,
) -> dict[str, Any]:
    provider = PublicCandleProvider(snapshots, higher_timeframes)
    controller = RSIv5Controller(
        config=RSIv5ControllerConfig(
            id=f"rsi-v5-{scenario_id}",
            connector_name="binance",
            trading_pair="BNB-USDT",
            interval="1m",
            strict_hmm_mode=True,
            trend_confirmation_enabled=True,
            trend_confirmation_medium_interval="5m",
            trend_confirmation_long_interval="15m",
            rsi_length=4,
            rsi_buy_threshold=60.0,
            rsi_sell_threshold=90.0,
            sell_rsi_overbought=90.0,
            min_signal_score=1,
            buy_confirmation_mode="rebound_confirm",
            buy_confirmation_rsi_delta=1.0,
            use_split_entries=False,
            dynamic_position_sizing=False,
            use_bag_freeze=False,
            flash_crash_drop_pct=1.0,
            min_atr_pct_to_trade=0.0,
            max_atr_pct_to_trade=max_atr_pct_to_trade,
            usd_per_entry=Decimal("10"),
            max_total_position_usd=Decimal("100"),
        ),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )

    steps = []
    for index, step_id in enumerate(step_ids):
        provider.step = index
        asyncio.run(controller.update_processed_data())
        actions = controller.determine_executor_actions()
        steps.append(_normalize_step(controller, step_id, actions))
    return {
        "scenario_id": scenario_id,
        "steps": steps,
        "parameters": {"max_atr_pct_to_trade": max_atr_pct_to_trade},
        "evidence_scope": list(_EVIDENCE_SCOPE),
    }


def _normalize_step(controller: RSIv5Controller, step_id: str, actions: list[Any]) -> dict[str, Any]:
    processed = controller.processed_data
    health = processed["data_health"]
    signal_state = processed["signal_state"]
    indicators = processed.get("indicators", {})
    trend = processed.get("trend_confirmation", {})
    trend_states = trend.get("states", {}) if isinstance(trend, dict) else {}
    regime_error = processed.get("regime_error")
    return {
        "step_id": step_id,
        "data_health": {
            "ready": bool(health.get("ready")),
            "reason": str(health.get("reason")),
            "row_count": int(health.get("row_count", 0)),
        },
        "indicators": {
            name: _finite_float(indicators[name])
            for name in ("rsi", "atr", "atr_pct", "ema_fast", "ema_slow", "bb_lower", "bb_mid", "bb_upper", "macd_hist")
        },
        "strict_hmm": {
            "required": bool(controller._regime_detector.require_hmm),
            "backend": type(controller._regime_detector.model).__name__,
            "fit": bool(controller._regime_detector._is_fit),
            "detected": processed.get("regime") is not None and regime_error is None,
            "label": processed.get("regime"),
            "error": regime_error,
        },
        "trend": {
            "observed": bool(trend_states),
            "direction": trend.get("direction"),
            "confidence": _rounded_float(trend.get("confidence")),
            "valid_timeframes": sorted(
                name for name, state in trend_states.items() if bool(state.get("is_valid"))
            ),
        },
        "signal": int(processed.get("signal", 0)),
        "signal_state": {
            "raw_buy_reversal": bool(signal_state.get("rsi_reversal")),
            "confirmation_active": bool(signal_state.get("buy_confirmation_active")),
            "decision": signal_state.get("buy_decision"),
            "reason": signal_state.get("buy_reason"),
            "condition_ok": bool(signal_state.get("condition_ok")),
            "condition_reason": signal_state.get("condition_reason"),
        },
        "actions": [_normalize_action(action) for action in actions],
    }


def _normalize_action(action: Any) -> dict[str, Any]:
    if isinstance(action, CreateExecutorAction):
        config = action.executor_config
        return {
            "action_type": type(action).__name__,
            "side": config.side.name,
            "executor_type": config.type,
            "entry_price": str(config.entry_price),
            "amount": str(config.amount),
        }
    if isinstance(action, StopExecutorAction):
        return {
            "action_type": type(action).__name__,
            "executor_id": str(action.executor_id),
            "keep_position": bool(action.keep_position),
        }
    return {"action_type": type(action).__name__}


def _generated_candles(
    row_count: int,
    *,
    interval_seconds: int = 60,
    tail: list[float] | None = None,
) -> pd.DataFrame:
    closes = []
    close = 100.0
    regime_drifts = (0.0, 0.0, 0.06, -0.06, 0.10, -0.10)
    regime_volatility = (0.02, 0.20, 0.05, 0.05, 0.25, 0.25)
    for index in range(row_count):
        regime = (index // 10) % len(regime_drifts)
        oscillation = 0.65 * math.sin(index * 1.7) + 0.35 * math.sin(index * 0.37)
        close += regime_drifts[regime] + regime_volatility[regime] * oscillation
        closes.append(close)
    if tail:
        closes[-len(tail):] = tail
    return pd.DataFrame(
        {
            "timestamp": [
                _BASE_TIME - (row_count - 1 - index) * interval_seconds
                for index in range(row_count)
            ],
            "open": [close - 0.03 for close in closes],
            "high": [close + 0.18 for close in closes],
            "low": [close - 0.18 for close in closes],
            "close": closes,
            "volume": [100.0 + ((index * 17) % 23) for index in range(row_count)],
        }
    )


def _append_candle(
    frame: pd.DataFrame,
    *,
    close: float,
    high: float,
    low: float,
    volume: float,
) -> pd.DataFrame:
    row = pd.DataFrame(
        {
            "timestamp": [float(frame["timestamp"].iloc[-1]) + 60.0],
            "open": [float(frame["close"].iloc[-1])],
            "high": [high],
            "low": [low],
            "close": [close],
            "volume": [volume],
        }
    )
    return pd.concat([frame, row], ignore_index=True)


def _finite_float(value: Any) -> float:
    numeric = float(value)
    if not math.isfinite(numeric):
        raise AssertionError(f"expected finite public indicator, got {value!r}")
    return round(numeric, 10)


def _rounded_float(value: Any) -> float:
    return round(float(value), 10)
