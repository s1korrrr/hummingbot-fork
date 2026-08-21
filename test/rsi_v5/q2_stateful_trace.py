from __future__ import annotations

import asyncio
import copy
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

from controllers.directional_trading.rsi_v5 import (
    BuyConfirmationSetup,
    RecoveryTrailState,
    RSIv5Controller,
    RSIv5ControllerConfig,
)
from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class ScenarioMarketDataProvider:
    """Concrete, deterministic controller boundary used only by the Q2 tests."""

    def __init__(self, initial_state: Mapping[str, Any]) -> None:
        self.now = _seconds(initial_state["clock"]["now_ms"])
        market = initial_state["market"]
        self.mid_price = Decimal(market["mid_price"])
        self.best_bid = Decimal(market["best_bid"])
        self.best_ask = Decimal(market["best_ask"])
        self.balances = copy.deepcopy(initial_state["balances"])
        rules = initial_state["trading_rules"]
        self.trading_rule = SimpleNamespace(
            min_price_increment=Decimal(rules["min_price_increment"]),
            min_order_size=Decimal("0"),
            min_base_amount_increment=Decimal(rules["min_base_amount_increment"]),
            min_notional_size=Decimal(rules["min_notional_size"]),
            min_order_value=Decimal(rules["min_notional_size"]),
        )

    def initialize_rate_sources(self, _config) -> None:
        return None

    def time(self) -> float:
        return self.now

    def get_price_by_type(self, _connector: str, _pair: str, price_type: PriceType) -> Decimal:
        if price_type == PriceType.BestBid:
            return self.best_bid
        if price_type == PriceType.BestAsk:
            return self.best_ask
        return self.mid_price

    def get_balance(self, _connector: str, asset: str) -> Decimal:
        key = "base_total" if asset == "BNB" else "quote_total"
        return Decimal(self.balances[key])

    def get_available_balance(self, _connector: str, asset: str) -> Decimal:
        key = "base_available" if asset == "BNB" else "quote_available"
        return Decimal(self.balances[key])

    def get_trading_rules(self, _connector: str, _pair: str):
        return self.trading_rule

    def quantize_order_amount(self, _connector: str, _pair: str, amount: Decimal) -> Decimal:
        return amount.quantize(self.trading_rule.min_base_amount_increment)


def observe_q2_scenarios(
    request: Mapping[str, Any] | str | Path,
    *,
    admission_outcomes: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Observe the seven request scenarios through the actual RSI v5 controller.

    The returned values are deliberately raw and unhashed. The root contract
    owns admission evaluation and observation/source hashes.
    """

    request_value = _load_request(request)
    _validate_request_shape(request_value)
    normalized_admissions = {
        action: _admission_outcome(admission_outcomes, action)
        for action in ("dca", "runner")
    }

    observed = []
    for scenario in request_value["scenarios"]:
        action = scenario["action"]
        controller, provider = _build_controller(request_value, scenario["initial_state"])
        if action == "entry":
            steps = _observe_entry(controller, provider, scenario)
        elif action == "dca":
            steps = _observe_dca(controller, provider, scenario, normalized_admissions[action])
        elif action == "cooldown":
            steps = _observe_cooldown(controller, provider, scenario)
        elif action == "partial_harvest":
            steps = _observe_partial_harvest(controller, provider, scenario)
        elif action == "runner":
            steps = _observe_runner(controller, provider, scenario, normalized_admissions[action])
        elif action == "trailing":
            steps = _observe_trailing(controller, provider, scenario)
        elif action == "exit":
            steps = _observe_exit(controller, provider, scenario)
        else:  # guarded by _validate_request_shape
            raise ValueError(f"unsupported Q2 action: {action}")
        observed.append(
            {
                "scenario_id": scenario["scenario_id"],
                "action": action,
                "steps": steps,
            }
        )
    return observed


def _load_request(request: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(request, Mapping):
        return copy.deepcopy(dict(request))
    path = Path(request)
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_request_shape(request: Mapping[str, Any]) -> None:
    scenarios = request.get("scenarios")
    if not isinstance(scenarios, list):
        raise ValueError("Q2 request scenarios must be a list")
    actions = [scenario.get("action") for scenario in scenarios if isinstance(scenario, Mapping)]
    expected = ["entry", "dca", "cooldown", "partial_harvest", "runner", "trailing", "exit"]
    if actions != expected:
        raise ValueError(f"Q2 request must contain the ordered seven scenarios: {expected}")


def _admission_outcome(admissions: Mapping[str, Any], action: str) -> dict[str, Any]:
    if action not in admissions:
        raise ValueError(f"missing evaluated admission outcome for {action}")
    outcome = admissions[action]
    if isinstance(outcome, Mapping):
        allowed = outcome.get("allowed")
        rejection_code = outcome.get("rejection_code")
    else:
        allowed = getattr(outcome, "allowed", None)
        rejection_code = getattr(outcome, "rejection_code", None)
    if not isinstance(allowed, bool):
        raise ValueError(f"evaluated admission outcome for {action} must contain boolean allowed")
    if rejection_code is not None and not isinstance(rejection_code, str):
        raise ValueError(f"evaluated admission rejection_code for {action} must be a string or null")
    return {"allowed": allowed, "rejection_code": rejection_code}


def _build_controller(
    request: Mapping[str, Any], initial_state: Mapping[str, Any]
) -> tuple[RSIv5Controller, ScenarioMarketDataProvider]:
    profile = request["controller_profile"]["config"]
    adapter = request["instrument_adapter"]
    config = RSIv5ControllerConfig(
        id="rsi-v5-q2-stateful-observer",
        connector_name=adapter["connector"],
        trading_pair=adapter["trading_pair"],
        interval=request["program_binding"]["interval"],
        rsi_length=profile["rsi_length"],
        rsi_buy_threshold=float(profile["rsi_buy_threshold"]),
        rsi_sell_threshold=float(profile["rsi_sell_threshold"]),
        sell_rsi_overbought=float(profile["recovery_sell_overbought_threshold"]),
        min_signal_score=profile["min_signal_score"],
        dca_score_boost=profile["dca_score_boost"],
        buy_confirmation_mode=profile["buy_confirmation_mode"],
        buy_confirmation_rsi_delta=float(profile["rebound_delta"]),
        buy_confirmation_max_wait_seconds=profile["rebound_max_wait_seconds"],
        use_split_entries=profile["use_split_entries"],
        scout_entry_fraction=float(profile["scout_fraction"]),
        usd_per_entry=Decimal(profile["usd_per_entry"]),
        max_total_position_usd=Decimal(profile["max_total_position_usd"]),
        executor_price_gap_threshold=Decimal(profile["executor_price_gap_fraction"]),
        min_executor_price_gap_threshold=Decimal(profile["min_executor_price_gap_threshold"]),
        max_executor_price_gap_threshold=Decimal(profile["max_executor_price_gap_threshold"]),
        price_gap_atr_multiplier=float(profile["price_gap_atr_multiplier"]),
        cooldown_time=profile["cooldown_seconds"],
        min_cooldown_time=profile["minimum_cooldown_seconds"],
        cooldown_atr_low_pct=float(profile["cooldown_atr_low_fraction"]),
        cooldown_atr_high_pct=float(profile["cooldown_atr_high_fraction"]),
        use_adaptive_filters=profile["use_adaptive_filters"],
        dynamic_position_sizing=profile["dynamic_position_sizing"],
        sell_only_if_profitable=profile["sell_only_if_profitable"],
        min_profit_pct_for_sell=Decimal(profile["minimum_sell_profit_fraction"]),
        use_bag_freeze=profile["use_bag_freeze"],
        bag_recovery_target_pct=float(profile["recovery_target_fraction"]),
        recovery_partial_exit_fraction=float(profile["recovery_partial_exit_fraction"]),
        recovery_trail_pullback_pct=float(profile["recovery_pullback_fraction"]),
        recovery_rsi_rollover_delta=float(profile["recovery_rsi_rollover_delta"]),
        recovery_sell_execution_strategy=profile["recovery_execution_strategy"],
        recovery_limit_chaser_distance_pct=Decimal(profile["recovery_chaser_distance_fraction"]),
        recovery_limit_chaser_refresh_threshold_pct=Decimal(profile["recovery_chaser_refresh_fraction"]),
        use_trailing_exit=profile["use_trailing_exit"],
        trailing_activation_pct=float(profile["trailing_activation_fraction"]),
        trailing_delta_pct=float(profile["trailing_distance_fraction"]),
        trailing_stop_order_type=profile["trailing_order_type"],
        early_stop_drawdown_pct=float(profile["hard_stop_fraction"]),
        early_stop_keep_position=profile["hard_stop_keep_position"],
        trend_confirmation_enabled=False,
    )
    config.manual_kill_switch = bool(initial_state["controller_state"]["manual_kill_switch"])
    provider = ScenarioMarketDataProvider(initial_state)
    controller = RSIv5Controller(
        config=config,
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    _hydrate_controller(controller, initial_state)
    return controller, provider


def _hydrate_controller(controller: RSIv5Controller, initial_state: Mapping[str, Any]) -> None:
    state = initial_state["controller_state"]
    signal_state = initial_state["processed_signal_state"]
    controller.positions_held = [_position(value) for value in initial_state["inventory"]]
    controller.executors_info = [_executor(value, controller) for value in initial_state["executors"]]
    controller._latest_atr_pct = float(state["latest_atr_fraction"])
    controller._current_cooldown = int(state["current_cooldown_seconds"])
    controller._last_buy_signal_role = state["last_buy_signal_role"]
    controller._last_buy_signal_candle_ts = _optional_seconds(state["last_emitted_buy_candle_ms"])
    controller._pending_buy_confirmation = _buy_confirmation(state["pending_buy_confirmation"])
    controller._operator_entry_paused = bool(state["operator_entry_paused"])
    controller._executor_result_conflict_ids = set(state["executor_result_conflict_ids"])
    controller._recovery_trails = {
        trail.position_key: trail
        for trail in (_recovery_trail(item) for item in state["recovery_trails"])
    }
    controller.processed_data = {
        "signal": int(signal_state["signal"]),
        "timestamp": _seconds(signal_state["timestamp_ms"]),
        "signal_state": _signal_state(signal_state),
        "trend_confirmation": {
            "direction": signal_state["trend_direction"],
            "confidence": float(signal_state["trend_confidence"]),
            "alignment_score": float(signal_state["trend_confidence"]),
            "states": {},
        },
    }


def _position(value: Mapping[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
        id=value["position_id"],
        connector_name=value["connector"],
        trading_pair=value["trading_pair"],
        side=_trade_type(value["side"]),
        amount=Decimal(value["amount_base"]),
        breakeven_price=Decimal(value["breakeven_price"]),
        cum_fees_quote=Decimal(value["cum_fees_quote"]),
    )


def _executor(value: Mapping[str, Any], controller: RSIv5Controller) -> SimpleNamespace:
    level_id = value.get("level_id")
    return SimpleNamespace(
        id=value["executor_id"],
        is_active=bool(value["is_active"]),
        is_done=not bool(value["is_active"]),
        type=value["executor_type"],
        connector_name=controller.config.connector_name,
        trading_pair=controller.config.trading_pair,
        side=_trade_type(value["side"]),
        timestamp=_seconds(value["timestamp_ms"]),
        close_timestamp=None,
        close_type=value.get("close_type"),
        filled_amount_quote=Decimal(value["filled_amount_quote"]),
        net_pnl_pct=float(value["net_pnl_fraction"]),
        config=SimpleNamespace(entry_price=Decimal(value["entry_price"])),
        custom_info={
            "level_id": level_id,
            "role": value.get("role"),
            "current_position_average_price": Decimal(value["entry_price"]),
            "order_price": Decimal(value["entry_price"]),
        },
    )


def _buy_confirmation(value: Mapping[str, Any] | None) -> BuyConfirmationSetup | None:
    if value is None:
        return None
    return BuyConfirmationSetup(
        armed_timestamp=_seconds(value["armed_timestamp_ms"]),
        trough_rsi=float(value["trough_rsi"]),
        trough_price=float(value["trough_price"]),
        armed_signal_score=int(value["armed_signal_score"]),
        armed_min_signal_score=int(value["armed_min_signal_score"]),
    )


def _recovery_trail(value: Mapping[str, Any]) -> RecoveryTrailState:
    return RecoveryTrailState(
        position_key=value["position_key"],
        armed_timestamp=_seconds(value["armed_timestamp_ms"]),
        arm_price=Decimal(value["arm_price"]),
        peak_price=Decimal(value["peak_price"]),
        tracked_amount=Decimal(value["tracked_amount"]),
        peak_rsi=float(value["peak_rsi"]) if value["peak_rsi"] is not None else None,
        target_profit_pct=Decimal(value["target_profit_fraction"]),
        last_reason=value["last_reason"],
        partial_exit_done=bool(value["partial_exit_done"]),
    )


def _signal_state(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": _seconds(value["timestamp_ms"]),
        "condition_ok": bool(value["condition_ok"]),
        "condition_reason": value["condition_reason"],
        "signal_score": int(value["signal_score"]),
        "buy_entry_role": value["buy_entry_role"],
        "buy_entry_fraction": float(value["buy_entry_fraction"]),
        "buy_context_bias": value["buy_context_bias"],
        "size_multiplier": float(value["size_multiplier"]),
        "rsi": float(value["rsi"]),
        "rsi_prev": float(value["rsi_previous"]),
        "ema_fast": float(value["ema_fast"]),
        "macd_hist": float(value["macd_hist"]),
        "macd_hist_prev": float(value["macd_hist_previous"]),
    }


def _observe_entry(
    controller: RSIv5Controller, provider: ScenarioMarketDataProvider, scenario: Mapping[str, Any]
) -> list[dict[str, Any]]:
    signal_step, config_step = scenario["steps"]
    signal = _apply_signal(controller, provider, signal_step)
    role = controller._last_buy_signal_role
    result = [_observed_step(signal_step, {"kind": "SIGNAL", "signal": signal, "role": role}, [], controller, provider)]
    config = _apply_config(controller, provider, config_step)
    result.append(_observed_step(config_step, {"kind": "EXECUTOR_CONFIG"}, [_config_action(config, controller)], controller, provider))
    return result


def _observe_dca(
    controller: RSIv5Controller,
    provider: ScenarioMarketDataProvider,
    scenario: Mapping[str, Any],
    admission: Mapping[str, Any],
) -> list[dict[str, Any]]:
    admission_step, gate_step, config_step = scenario["steps"]
    result = [_admission_step(admission_step, admission, controller, provider)]
    if not admission["allowed"]:
        result.extend(_externally_blocked_steps((gate_step, config_step), admission, controller, provider))
        return result
    allowed = _apply_gate(controller, provider, gate_step)
    result.append(_gate_step(gate_step, allowed, controller, provider))
    config = _apply_config(controller, provider, config_step)
    result.append(_observed_step(config_step, {"kind": "EXECUTOR_CONFIG"}, [_config_action(config, controller)], controller, provider))
    return result


def _observe_cooldown(
    controller: RSIv5Controller, provider: ScenarioMarketDataProvider, scenario: Mapping[str, Any]
) -> list[dict[str, Any]]:
    result = []
    for step in scenario["steps"]:
        event = step["inputs"]["event"]
        controller.config.use_adaptive_filters = bool(event["use_adaptive_filters"])
        controller._latest_atr_pct = float(event["latest_atr_fraction"])
        controller.executors_info[0].timestamp = _seconds(event["last_entry_timestamp_ms"])
        allowed = _apply_gate(controller, provider, step)
        result.append(_gate_step(step, allowed, controller, provider))
    return result


def _observe_partial_harvest(
    controller: RSIv5Controller, provider: ScenarioMarketDataProvider, scenario: Mapping[str, Any]
) -> list[dict[str, Any]]:
    result = []
    for step in scenario["steps"]:
        event = step["inputs"]["event"]
        _set_clock_and_mid(provider, step["decision_time_ms"], event["mid_price"])
        _set_recovery_signal_state(controller, event)
        actions = controller.stop_actions_proposal()
        trail = next(iter(controller._recovery_trails.values()), None)
        decision = {
            "kind": "RECOVERY",
            "reason": trail.last_reason if trail is not None else "below_fee_aware_target",
        }
        result.append(_observed_step(step, decision, _serialize_actions(actions), controller, provider))
    return result


def _observe_runner(
    controller: RSIv5Controller,
    provider: ScenarioMarketDataProvider,
    scenario: Mapping[str, Any],
    admission: Mapping[str, Any],
) -> list[dict[str, Any]]:
    confirm_step, admission_step, gate_step, config_step = scenario["steps"]
    signal = _apply_signal(controller, provider, confirm_step)
    role = controller._last_buy_signal_role
    result = [_observed_step(confirm_step, {"kind": "SIGNAL", "signal": signal, "role": role}, [], controller, provider)]
    result.append(_admission_step(admission_step, admission, controller, provider))
    if not admission["allowed"]:
        result.extend(_externally_blocked_steps((gate_step, config_step), admission, controller, provider))
        return result
    allowed = _apply_gate(controller, provider, gate_step)
    result.append(_gate_step(gate_step, allowed, controller, provider))
    config = _apply_config(controller, provider, config_step)
    result.append(_observed_step(config_step, {"kind": "EXECUTOR_CONFIG"}, [_config_action(config, controller)], controller, provider))
    return result


def _observe_trailing(
    controller: RSIv5Controller, provider: ScenarioMarketDataProvider, scenario: Mapping[str, Any]
) -> list[dict[str, Any]]:
    step = scenario["steps"][0]
    config = _apply_config(controller, provider, step)
    return [_observed_step(step, {"kind": "TRAILING_CONFIG_ONLY"}, [_config_action(config, controller)], controller, provider)]


def _observe_exit(
    controller: RSIv5Controller, provider: ScenarioMarketDataProvider, scenario: Mapping[str, Any]
) -> list[dict[str, Any]]:
    before_step, at_step, signal_step, owned_step = scenario["steps"]
    result = []
    for step in (before_step, at_step):
        event = step["inputs"]["event"]
        _set_clock_and_mid(provider, step["decision_time_ms"], event["mid_price"])
        controller.processed_data["signal"] = int(event["current_signal"])
        actions = controller.stop_actions_proposal()
        result.append(
            _observed_step(
                step,
                {"kind": "HARD_STOP", "triggered": any(isinstance(item, StopExecutorAction) for item in actions)},
                _serialize_actions(actions),
                controller,
                provider,
            )
        )

    signal = _apply_signal(controller, provider, signal_step)
    result.append(_observed_step(signal_step, {"kind": "SIGNAL", "signal": signal, "role": controller._last_buy_signal_role}, [], controller, provider))

    event = owned_step["inputs"]["event"]
    _set_clock_and_mid(provider, owned_step["decision_time_ms"], event["mid_price"])
    controller.processed_data["signal"] = int(event["current_signal"])
    controller.executors_info.append(_executor(event["active_signal_sell_executor"], controller))
    trail = _recovery_trail(event["recovery_trail"])
    controller._recovery_trails[trail.position_key] = trail
    owned_actions = controller.stop_actions_proposal()
    result.append(
        _observed_step(
            owned_step,
            {
                "kind": "EXIT_OWNERSHIP",
                "owner": "active_signal_sell_executor",
                "recovery_action_suppressed": not any(isinstance(item, CreateExecutorAction) for item in owned_actions),
            },
            _serialize_actions(owned_actions),
            controller,
            provider,
        )
    )
    return result


def _apply_signal(
    controller: RSIv5Controller, provider: ScenarioMarketDataProvider, step: Mapping[str, Any]
) -> int:
    event = step["inputs"]["event"]
    provider.now = _seconds(step["decision_time_ms"])
    _set_market_from_event(provider, event)
    signal = controller._determine_signal(
        last_rsi=float(event["last_rsi"]),
        signal_score=int(event["signal_score"]),
        rsi_buy_eff=float(event["rsi_buy_effective"]),
        rsi_sell_eff=float(event["rsi_sell_effective"]),
        last_close=float(event["last_close"]),
        rsi_prev=float(event["rsi_previous"]),
        ema_fast=float(event["ema_fast"]),
        macd_hist=float(event["macd_hist"]),
        macd_hist_prev=float(event["macd_hist_previous"]),
        raw_buy_reversal=bool(event["raw_buy_reversal"]),
        signal_timestamp=_seconds(event["signal_timestamp_ms"]),
        candle_timestamp=_seconds(event["candle_timestamp_ms"]),
    )
    state = controller.processed_data["signal_state"]
    state.update(
        timestamp=_seconds(event["signal_timestamp_ms"]),
        condition_ok=True,
        condition_reason="ready",
        signal_score=int(event["signal_score"]),
        rsi=float(event["last_rsi"]),
        rsi_prev=float(event["rsi_previous"]),
        ema_fast=float(event["ema_fast"]),
        macd_hist=float(event["macd_hist"]),
        macd_hist_prev=float(event["macd_hist_previous"]),
    )
    if signal > 0:
        role = controller._last_buy_signal_role or "full"
        state["buy_entry_role"] = role
        state["buy_entry_fraction"] = float(
            controller._planned_buy_entry_fraction(
                {"buy_entry_role": role, "buy_context_bias": state.get("buy_context_bias", "neutral")}
            )
        )
    elif signal < 0:
        state["buy_entry_role"] = "none"
        state["buy_entry_fraction"] = 0.0
    controller.processed_data["signal"] = signal
    controller.processed_data["timestamp"] = _seconds(event["signal_timestamp_ms"])
    return signal


def _apply_gate(
    controller: RSIv5Controller, provider: ScenarioMarketDataProvider, step: Mapping[str, Any]
) -> bool:
    event = step["inputs"]["event"]
    provider.now = _seconds(step["decision_time_ms"])
    _set_market_from_event(provider, event)
    controller.processed_data["signal"] = int(event["signal"])
    controller.processed_data["timestamp"] = _seconds(step["decision_time_ms"])
    controller.processed_data["signal_state"].update(
        timestamp=_seconds(step["decision_time_ms"]),
        signal_score=int(
            event.get(
                "signal_score",
                controller.processed_data["signal_state"].get("signal_score", 0),
            )
        ),
    )
    return controller.can_create_executor(int(event["signal"]))


def _apply_config(
    controller: RSIv5Controller, provider: ScenarioMarketDataProvider, step: Mapping[str, Any]
):
    event = step["inputs"]["event"]
    provider.now = _seconds(step["decision_time_ms"])
    _set_market_from_event(provider, event)
    return controller.get_executor_config(
        trade_type=_trade_type(event["side"]),
        price=Decimal(event["reference_price"]),
        amount=Decimal(event["requested_amount_base"]),
    )


def _admission_step(
    step: Mapping[str, Any],
    admission: Mapping[str, Any],
    controller: RSIv5Controller,
    provider: ScenarioMarketDataProvider,
) -> dict[str, Any]:
    provider.now = _seconds(step["decision_time_ms"])
    decision = {
        "kind": "EXTERNAL_ADMISSION",
        "allowed": admission["allowed"],
        "rejection_code": admission["rejection_code"],
        "enforced_by": "root",
    }
    return _observed_step(step, decision, [], controller, provider)


def _externally_blocked_steps(
    steps: tuple[Mapping[str, Any], ...],
    admission: Mapping[str, Any],
    controller: RSIv5Controller,
    provider: ScenarioMarketDataProvider,
) -> list[dict[str, Any]]:
    result = []
    for step in steps:
        provider.now = _seconds(step["decision_time_ms"])
        result.append(
            _observed_step(
                step,
                {
                    "kind": "EXTERNAL_ADMISSION_BLOCKED",
                    "allowed": False,
                    "rejection_code": admission["rejection_code"],
                },
                [],
                controller,
                provider,
            )
        )
    return result


def _gate_step(
    step: Mapping[str, Any],
    allowed: bool,
    controller: RSIv5Controller,
    provider: ScenarioMarketDataProvider,
) -> dict[str, Any]:
    reason = controller._last_gate_reason[TradeType.BUY]
    return _observed_step(
        step,
        {"kind": "CONTROLLER_GATE", "allowed": allowed, "reason": reason},
        [],
        controller,
        provider,
    )


def _set_recovery_signal_state(controller: RSIv5Controller, event: Mapping[str, Any]) -> None:
    controller.processed_data["signal"] = int(event["current_signal"])
    controller.processed_data["signal_state"].update(
        rsi=float(event["rsi"]),
        rsi_prev=float(event["rsi_previous"]),
        ema_fast=float(event["ema_fast"]),
    )


def _set_clock_and_mid(provider: ScenarioMarketDataProvider, decision_time_ms: int, mid_price: str) -> None:
    provider.now = _seconds(decision_time_ms)
    provider.mid_price = Decimal(mid_price)


def _set_market_from_event(provider: ScenarioMarketDataProvider, event: Mapping[str, Any]) -> None:
    if "mid_price" in event:
        provider.mid_price = Decimal(event["mid_price"])
    if "best_bid_price" in event:
        provider.best_bid = Decimal(event["best_bid_price"])
    if "best_ask_price" in event:
        provider.best_ask = Decimal(event["best_ask_price"])


def _observed_step(
    step: Mapping[str, Any],
    decision: Mapping[str, Any],
    actions: list[dict[str, Any]],
    controller: RSIv5Controller,
    provider: ScenarioMarketDataProvider,
) -> dict[str, Any]:
    return {
        "step_id": step["step_id"],
        "decision": copy.deepcopy(dict(decision)),
        "actions": copy.deepcopy(actions),
        "post_state": _post_state(controller, provider),
    }


def _post_state(controller: RSIv5Controller, provider: ScenarioMarketDataProvider) -> dict[str, Any]:
    pending = controller._pending_buy_confirmation
    return {
        "clock_ms": _milliseconds(provider.now),
        "mid_price": _decimal(provider.mid_price),
        "signal": int((controller.processed_data or {}).get("signal", 0) or 0),
        "signal_state": _post_signal_state((controller.processed_data or {}).get("signal_state", {})),
        "pending_buy_confirmation": None
        if pending is None
        else {
            "armed_timestamp_ms": _milliseconds(pending.armed_timestamp),
            "trough_rsi": _decimal(pending.trough_rsi),
            "trough_price": _decimal(pending.trough_price),
            "armed_signal_score": pending.armed_signal_score,
            "armed_min_signal_score": pending.armed_min_signal_score,
        },
        "last_buy_signal_role": controller._last_buy_signal_role,
        "last_emitted_buy_candle_ms": None
        if controller._last_buy_signal_candle_ts is None
        else _milliseconds(controller._last_buy_signal_candle_ts),
        "last_gate_reason": {
            "buy": controller._last_gate_reason[TradeType.BUY],
            "sell": controller._last_gate_reason[TradeType.SELL],
        },
        "current_cooldown_seconds": controller._current_cooldown,
        "effective_price_gap_fraction": _decimal(controller._current_gap_threshold),
        "recovery_trails": [_post_recovery_trail(value) for _, value in sorted(controller._recovery_trails.items())],
        "inventory": [_post_position(value) for value in controller.positions_held],
        "executors": [_post_executor(value) for value in controller.executors_info],
    }


def _post_signal_state(state: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "condition_ok",
        "condition_reason",
        "signal_score",
        "buy_entry_role",
        "buy_entry_fraction",
        "buy_context_bias",
        "size_multiplier",
        "rsi",
        "rsi_prev",
        "ema_fast",
        "macd_hist",
        "macd_hist_prev",
    )
    return {
        key: _decimal(state[key]) if isinstance(state.get(key), (Decimal, float)) else state.get(key)
        for key in keys
    }


def _post_recovery_trail(trail: RecoveryTrailState) -> dict[str, Any]:
    return {
        "position_key": trail.position_key,
        "armed_timestamp_ms": _milliseconds(trail.armed_timestamp),
        "arm_price": _decimal(trail.arm_price),
        "peak_price": _decimal(trail.peak_price),
        "tracked_amount": _decimal(trail.tracked_amount),
        "peak_rsi": None if trail.peak_rsi is None else _decimal(trail.peak_rsi),
        "target_profit_fraction": _decimal(trail.target_profit_pct),
        "last_reason": trail.last_reason,
        "partial_exit_done": trail.partial_exit_done,
    }


def _post_position(position: SimpleNamespace) -> dict[str, Any]:
    return {
        "position_id": position.id,
        "side": position.side.name,
        "amount_base": _decimal(position.amount),
        "breakeven_price": _decimal(position.breakeven_price),
        "cum_fees_quote": _decimal(position.cum_fees_quote),
    }


def _post_executor(executor: SimpleNamespace) -> dict[str, Any]:
    return {
        "executor_id": executor.id,
        "executor_type": executor.type,
        "is_active": executor.is_active,
        "side": executor.side.name,
        "timestamp_ms": _milliseconds(executor.timestamp),
        "filled_amount_quote": _decimal(executor.filled_amount_quote),
        "net_pnl_fraction": _decimal(executor.net_pnl_pct),
        "entry_price": _decimal(executor.config.entry_price),
        "level_id": executor.custom_info.get("level_id"),
        "role": executor.custom_info.get("role"),
    }


def _config_action(config, controller: RSIv5Controller) -> dict[str, Any]:
    if hasattr(config, "triple_barrier_config"):
        trailing = config.triple_barrier_config.trailing_stop
        return {
            "action_type": "POSITION_EXECUTOR_CONFIG",
            "side": config.side.name,
            "entry_price": _decimal(config.entry_price),
            "amount_base": _decimal(config.amount),
            "level_id": config.level_id,
            "requested_fraction": _decimal(controller._planned_buy_entry_fraction()),
            "trailing_stop": None
            if trailing is None
            else {
                "activation_fraction": _decimal(trailing.activation_price),
                "distance_fraction": _decimal(trailing.trailing_delta),
                "order_type": config.triple_barrier_config.trailing_stop_order_type.name,
                "limit_max_age_seconds": config.triple_barrier_config.trailing_stop_limit_max_age_seconds,
            },
        }
    return {
        "action_type": "ORDER_EXECUTOR_CONFIG",
        "side": config.side.name,
        "price": None if config.price is None else _decimal(config.price),
        "amount_base": _decimal(config.amount),
        "level_id": config.level_id,
        "execution_strategy": config.execution_strategy.name,
    }


def _serialize_actions(actions: list[Any]) -> list[dict[str, Any]]:
    serialized = []
    for action in actions:
        if isinstance(action, StopExecutorAction):
            serialized.append(
                {
                    "action_type": "STOP_EXECUTOR",
                    "executor_id": action.executor_id,
                    "keep_position": bool(action.keep_position),
                }
            )
        elif isinstance(action, CreateExecutorAction):
            config = action.executor_config
            serialized.append(
                {
                    "action_type": "CREATE_ORDER_EXECUTOR",
                    "side": config.side.name,
                    "price": None if config.price is None else _decimal(config.price),
                    "amount_base": _decimal(config.amount),
                    "level_id": config.level_id,
                    "execution_strategy": config.execution_strategy.name,
                }
            )
        else:
            raise TypeError(f"unsupported controller action: {type(action).__name__}")
    return serialized


def _trade_type(value: str) -> TradeType:
    try:
        return TradeType[value]
    except KeyError as exc:
        raise ValueError(f"unsupported trade side: {value}") from exc


def _seconds(milliseconds: int) -> float:
    return int(milliseconds) / 1000.0


def _optional_seconds(milliseconds: int | None) -> float | None:
    return None if milliseconds is None else _seconds(milliseconds)


def _milliseconds(seconds: float) -> int:
    return int(round(float(seconds) * 1000))


def _decimal(value: Any) -> str:
    parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    if parsed.is_zero():
        return "0"
    fixed = format(parsed, "f")
    return fixed.rstrip("0").rstrip(".") if "." in fixed else fixed
