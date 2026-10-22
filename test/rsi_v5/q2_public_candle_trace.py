from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Mapping

import pandas as pd

from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig
from hummingbot.core.data_type.common import PriceType
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.utils.market_analysis import MarketRegime, StrictHMMRequiredError

_BASE_TIME = 1_800_000_000.0
_EVIDENCE_SCOPE = [
    "generated_ohlcv",
    "public_update_processed_data",
    "public_determine_executor_actions",
]
_CONTRACT_SCHEMA = "q2-rsi-v5-public-candle-request/1.0"
_CONTRACT_SCENARIOS = (
    "healthy_two_update_rebound",
    "atr_veto_after_rebound",
    "strict_hmm_219_fail_close",
)
_CONTRACT_STEPS = {
    "healthy_two_update_rebound": ("arm", "rebound"),
    "atr_veto_after_rebound": ("arm", "atr_veto"),
    "strict_hmm_219_fail_close": ("strict_hmm_fail_close",),
}
_FORBIDDEN_REQUEST_KEYS = {
    "expected",
    "result",
    "results",
    "decision",
    "actions",
    "post_state",
    "observation",
}


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


class ContractCandleProvider:
    """Concrete provider that consumes only request-bound market inputs."""

    def __init__(self, provider: Mapping[str, Any]) -> None:
        self.connector = str(provider["connector"])
        self.trading_pair = str(provider["trading_pair"])
        self.interval = str(provider["interval"])
        self.clock_offset_seconds = int(provider["clock_offset_seconds"])
        self.best_bid_offset = Decimal(provider["best_bid_offset"])
        self.best_ask_offset = Decimal(provider["best_ask_offset"])
        self.balances = copy.deepcopy(dict(provider["balances"]))
        rules = provider["trading_rules"]
        self.trading_rule = SimpleNamespace(
            min_price_increment=Decimal(rules["min_price_increment"]),
            min_order_size=Decimal(rules["min_order_size"]),
            min_base_amount_increment=Decimal(rules["min_base_amount_increment"]),
            min_notional_size=Decimal(rules["min_notional_size"]),
            min_order_value=Decimal(rules["min_order_value"]),
        )
        self.frame = pd.DataFrame()
        self.now = 0.0

    def set_step(
        self,
        frame: pd.DataFrame,
        *,
        available_at_ms: int,
        decision_time_ms: int,
    ) -> None:
        self.frame = frame.copy()
        expected_decision_ms = (
            int(self.frame["timestamp"].iloc[-1]) + self.clock_offset_seconds
        ) * 1000
        if decision_time_ms != expected_decision_ms or available_at_ms != decision_time_ms - 1:
            raise ValueError("contract provider step time violates causal candle availability")
        self.now = expected_decision_ms / 1000.0

    def initialize_rate_sources(self, _sources: list[Any]) -> None:
        return None

    def time(self) -> float:
        return self.now

    def get_candles_df(self, **kwargs: Any) -> pd.DataFrame:
        if str(kwargs["interval"]) != self.interval:
            raise ValueError("contract provider received an unbound candle interval")
        return self.frame.copy()

    def get_price_by_type(self, _connector: str, _pair: str, price_type: PriceType) -> Decimal:
        close = Decimal(str(self.frame["close"].iloc[-1]))
        if price_type == PriceType.BestBid:
            return close - self.best_bid_offset
        if price_type == PriceType.BestAsk:
            return close + self.best_ask_offset
        return close

    def get_balance(self, _connector: str, asset: str) -> Decimal:
        key = "base_total" if asset == self.trading_pair.split("-")[0] else "quote_total"
        return Decimal(self.balances[key])

    def get_available_balance(self, _connector: str, asset: str) -> Decimal:
        key = "base_available" if asset == self.trading_pair.split("-")[0] else "quote_available"
        return Decimal(self.balances[key])

    def get_trading_rules(self, _connector: str, _pair: str) -> SimpleNamespace:
        return self.trading_rule

    def quantize_order_amount(self, _connector: str, _pair: str, amount: Decimal) -> Decimal:
        return amount.quantize(self.trading_rule.min_base_amount_increment)


class ControlledRegimeDetector:
    """Consumes request-bound regime availability at the controller dependency boundary."""

    backend = "CONTROLLED_GAUSSIAN_HMM"
    runtime_labels = {"ranging_low_vol": "LV_Range"}

    def __init__(self) -> None:
        self.require_hmm = True
        self._is_fit = False
        self._outcome: dict[str, Any] = {}

    def set_outcome(self, outcome: Mapping[str, Any]) -> None:
        self._outcome = copy.deepcopy(dict(outcome))
        if not bool(self._outcome["fit"]):
            self._is_fit = False

    def fit(self, _frame: pd.DataFrame) -> None:
        self._is_fit = bool(self._outcome["fit"])
        if not self._is_fit:
            raise StrictHMMRequiredError(str(self._outcome["error_code"] or "HMM_NOT_FIT"))

    def detect(self, frame: pd.DataFrame) -> MarketRegime:
        if not self._is_fit or not bool(self._outcome["detected"]):
            raise StrictHMMRequiredError(str(self._outcome["error_code"] or "HMM_NOT_FIT"))
        return MarketRegime(
            timestamp=float(frame["timestamp"].iloc[-1]),
            regime_label=self.runtime_labels.get(
                str(self._outcome["label"]), str(self._outcome["label"])
            ),
            confidence=1.0,
            volatility_level="low",
            is_trending=False,
            trend_direction=0,
            trend_strength=0.0,
            state_probabilities={str(self._outcome["label"]): 1.0},
        )

    @property
    def contract_label(self) -> str | None:
        label = self._outcome.get("label")
        return None if label is None else str(label)


def observe_q2_public_candle_scenarios(request: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Consume the root public-candle request through public RSI v5 methods."""

    source = _validate_contract_request(request)
    frames = {
        series_id: _contract_frame(series["columns"], series["rows"])
        for series_id, series in source["candle_series"].items()
    }
    profiles = {
        profile["profile_id"]: profile["config"]
        for profile in source["profiles"]
    }
    observed = []
    for scenario in source["scenarios"]:
        provider = ContractCandleProvider(source["provider"])
        controller = _contract_controller(profiles[scenario["profile_id"]], provider)
        regime = ControlledRegimeDetector()
        controller._regime_detector = regime
        steps = []
        for step in scenario["steps"]:
            frame = _contract_step_frame(frames, step["candle_input"])
            provider.set_step(
                frame,
                available_at_ms=int(step["available_at_ms"]),
                decision_time_ms=int(step["decision_time_ms"]),
            )
            regime.set_outcome(step["controlled_regime"])
            asyncio.run(controller.update_processed_data())
            actions = controller.determine_executor_actions()
            steps.append(_normalize_contract_step(controller, regime, step["step_id"], actions))
        observed.append({"scenario_id": scenario["scenario_id"], "steps": steps})
    return observed


def _validate_contract_request(request: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(request, Mapping):
        raise ValueError("public candle request must be a mapping")
    source = copy.deepcopy(dict(request))
    if source.get("schema_version") != _CONTRACT_SCHEMA:
        raise ValueError("public candle request schema_version is invalid")
    _reject_contract_outputs(source)
    authority = source.get("authority")
    if authority != {
        "paper_shadow_only": True,
        "live_orders_allowed": False,
        "execution_authority": False,
        "promotion_authority": False,
    }:
        raise ValueError("public candle request authority is invalid")
    if source.get("scope") != "PUBLIC_UPDATE_CONTROLLER_COMPATIBILITY":
        raise ValueError("public candle request scope is invalid")
    if source.get("indicator_evidence") != "SHARED_PANDAS_TA_COMPATIBILITY":
        raise ValueError("public candle indicator evidence is invalid")
    if source.get("regime_evidence") != "CONTROLLED_DEPENDENCY_CONSUMPTION":
        raise ValueError("public candle regime evidence is invalid")

    profiles = source.get("profiles")
    if not isinstance(profiles, list) or len(profiles) != 2:
        raise ValueError("public candle profiles are invalid")
    profile_ids = set()
    for profile in profiles:
        config = profile.get("config") if isinstance(profile, Mapping) else None
        if not isinstance(config, Mapping):
            raise ValueError("public candle profile config is invalid")
        if config.get("trend_confirmation_enabled") is not False:
            raise ValueError("trend_confirmation_enabled must be false")
        if config.get("strict_hmm_mode") is not True:
            raise ValueError("strict_hmm_mode must be true")
        if profile.get("profile_hash") != _stable_hash(config):
            raise ValueError("profile_hash does not bind profile config")
        profile_ids.add(profile.get("profile_id"))

    scenarios = source.get("scenarios")
    if not isinstance(scenarios, list) or tuple(
        scenario.get("scenario_id") for scenario in scenarios
    ) != _CONTRACT_SCENARIOS:
        raise ValueError("public candle scenario identity/order is invalid")
    for scenario in scenarios:
        scenario_id = scenario["scenario_id"]
        if scenario.get("profile_id") not in profile_ids:
            raise ValueError("public candle scenario profile is invalid")
        steps = scenario.get("steps")
        if not isinstance(steps, list) or tuple(step.get("step_id") for step in steps) != _CONTRACT_STEPS[scenario_id]:
            raise ValueError("public candle step identity/order is invalid")
        for step in steps:
            controlled = step.get("controlled_regime")
            if not isinstance(controlled, Mapping) or controlled.get("backend") != ControlledRegimeDetector.backend:
                raise ValueError("controlled_regime is invalid")

    candle_series = source.get("candle_series")
    if not isinstance(candle_series, Mapping) or set(candle_series) != {"arm_240"}:
        raise ValueError("public candle series is invalid")
    series = candle_series["arm_240"]
    if series.get("columns") != ["timestamp", "open", "high", "low", "close", "volume"]:
        raise ValueError("public candle columns are invalid")
    if not isinstance(series.get("rows"), list) or len(series["rows"]) != 240:
        raise ValueError("public candle rows are invalid")

    request_hash = source.get("request_hash")
    payload = {key: value for key, value in source.items() if key != "request_hash"}
    if not isinstance(request_hash, str) or request_hash != _stable_hash(payload):
        raise ValueError("request_hash does not bind public candle request")
    return source


def _reject_contract_outputs(value: Any) -> None:
    if isinstance(value, Mapping):
        leaked = _FORBIDDEN_REQUEST_KEYS.intersection(value)
        if leaked:
            raise ValueError(f"public candle request is not input-only: {sorted(leaked)}")
        for child in value.values():
            _reject_contract_outputs(child)
    elif isinstance(value, list):
        for child in value:
            _reject_contract_outputs(child)


def _stable_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _contract_frame(columns: list[str], rows: list[list[Any]]) -> pd.DataFrame:
    normalized = [
        [int(row[0]), *(float(Decimal(value)) for value in row[1:])]
        for row in rows
    ]
    return pd.DataFrame(normalized, columns=columns)


def _contract_step_frame(frames: Mapping[str, pd.DataFrame], candle_input: Mapping[str, Any]) -> pd.DataFrame:
    base = frames[str(candle_input["series_id"])]
    if candle_input["operation"] == "snapshot":
        return base.iloc[: int(candle_input["end_exclusive"])].reset_index(drop=True).copy()
    row = candle_input["row"]
    appended = pd.DataFrame(
        [[int(row[0]), *(float(Decimal(value)) for value in row[1:])]],
        columns=base.columns,
    )
    return pd.concat([base, appended], ignore_index=True)


def _contract_controller(config: Mapping[str, Any], provider: ContractCandleProvider) -> RSIv5Controller:
    return RSIv5Controller(
        config=RSIv5ControllerConfig(
            id="rsi-v5-public-candle-contract",
            connector_name=provider.connector,
            trading_pair=provider.trading_pair,
            interval=provider.interval,
            strict_hmm_mode=bool(config["strict_hmm_mode"]),
            trend_confirmation_enabled=bool(config["trend_confirmation_enabled"]),
            rsi_length=int(config["rsi_length"]),
            atr_length=int(config["atr_length"]),
            ema_fast_length=int(config["ema_fast_length"]),
            ema_slow_length=int(config["ema_slow_length"]),
            rsi_buy_threshold=float(Decimal(config["rsi_buy_threshold"])),
            rsi_sell_threshold=float(Decimal(config["rsi_sell_threshold"])),
            sell_rsi_overbought=float(Decimal(config["sell_rsi_overbought"])),
            bb_length=int(config["bb_length"]),
            bb_std=float(Decimal(config["bb_std"])),
            macd_fast=int(config["macd_fast"]),
            macd_slow=int(config["macd_slow"]),
            macd_signal=int(config["macd_signal"]),
            min_signal_score=int(config["min_signal_score"]),
            buy_confirmation_mode=str(config["buy_confirmation_mode"]),
            buy_confirmation_rsi_delta=float(Decimal(config["buy_confirmation_rsi_delta"])),
            use_split_entries=bool(config["use_split_entries"]),
            dynamic_position_sizing=bool(config["dynamic_position_sizing"]),
            usd_per_entry=Decimal(config["usd_per_entry"]),
            max_total_position_usd=Decimal(config["max_total_position_usd"]),
            min_atr_pct_to_trade=float(Decimal(config["min_atr_pct_to_trade"])),
            max_atr_pct_to_trade=float(Decimal(config["max_atr_pct_to_trade"])),
            use_bag_freeze=bool(config["use_bag_freeze"]),
            flash_crash_drop_pct=float(Decimal(config["flash_crash_drop_pct"])),
        ),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )


def _normalize_contract_step(
    controller: RSIv5Controller,
    regime: ControlledRegimeDetector,
    step_id: str,
    actions: list[Any],
) -> dict[str, Any]:
    processed = controller.processed_data
    health = processed["data_health"]
    indicators = processed["indicators"]
    state = processed["signal_state"]
    regime_error = processed.get("regime_error")
    condition_reason = str(state.get("condition_reason") or "ok")
    confirmation_active = bool(state.get("buy_confirmation_active"))
    if condition_reason == "strict_hmm":
        decision = "idle"
        reason = "none"
    elif confirmation_active:
        decision = "armed"
        reason = "rebound-confirmation-armed"
    else:
        decision = str(state.get("buy_decision") or "idle")
        reason = str(state.get("buy_reason") or "none")
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
        "regime_consumption": {
            "required": bool(regime.require_hmm),
            "backend": regime.backend,
            "fit": bool(regime._is_fit),
            "detected": processed.get("regime") is not None and regime_error is None,
            "label": regime.contract_label if regime_error is None else None,
            "error_code": regime_error,
        },
        "signal": int(processed.get("signal", 0)),
        "signal_state": {
            "raw_buy_reversal": bool(state.get("rsi_reversal")),
            "confirmation_active": confirmation_active,
            "decision": decision,
            "reason": reason,
            "condition_ok": bool(state.get("condition_ok")),
            "condition_reason": condition_reason,
        },
        "actions": [_normalize_contract_action(action) for action in actions],
    }


def _normalize_contract_action(action: Any) -> dict[str, Any]:
    if not isinstance(action, CreateExecutorAction):
        raise ValueError(f"public candle contract produced unsupported action {type(action).__name__}")
    config = action.executor_config
    return {
        "action_type": type(action).__name__,
        "side": config.side.name,
        "executor_type": config.type,
        "entry_price": _canonical_decimal(config.entry_price),
        "amount": _canonical_decimal(config.amount),
    }


def _canonical_decimal(value: Any) -> str:
    numeric = Decimal(str(value))
    if not numeric.is_finite():
        raise ValueError("public candle action decimal must be finite")
    if numeric == 0:
        return "0"
    return format(numeric.normalize(), "f")


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
