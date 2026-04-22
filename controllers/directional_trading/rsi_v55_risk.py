from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Dict, Optional


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


@dataclass
class RSIv55RiskModelSnapshot:
    meta_accept_prob: Optional[float] = None
    downside_quantile: Optional[float] = None
    continuation_prob: Optional[float] = None
    recovery_success_prob: Optional[float] = None
    confidence: Optional[float] = None
    extras: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "meta_accept_prob": self.meta_accept_prob,
            "downside_quantile": self.downside_quantile,
            "continuation_prob": self.continuation_prob,
            "recovery_success_prob": self.recovery_success_prob,
            "confidence": self.confidence,
        }
        if self.extras:
            payload["extras"] = dict(self.extras)
        return payload


@dataclass
class RSIv55RiskContext:
    signal_state: Dict[str, Any]
    market_structure: str
    regime_label: Optional[str]
    regime_confidence: float
    trend_direction: Optional[str]
    trend_confidence: float
    atr_pct: Optional[float]
    atr_percentile: float
    signal_quality: float
    inventory_utilization: float
    held_bag_load: float
    consecutive_loss_active: bool
    flash_crash_active: bool
    current_price: Optional[Decimal]
    cost_basis: Optional[Decimal]
    active_buy_executors: int
    active_sell_executors: int
    buy_entry_role: str
    base_entry_fraction: float
    base_entry_budget_usd: Decimal
    size_multiplier: float
    usd_per_entry: Decimal
    max_total_position_usd: Decimal
    has_inventory: bool
    sell_profitability_ok: bool
    sell_trend_hold: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "market_structure": self.market_structure,
            "regime_label": self.regime_label,
            "regime_confidence": self.regime_confidence,
            "trend_direction": self.trend_direction,
            "trend_confidence": self.trend_confidence,
            "atr_pct": self.atr_pct,
            "atr_percentile": self.atr_percentile,
            "signal_quality": self.signal_quality,
            "inventory_utilization": self.inventory_utilization,
            "held_bag_load": self.held_bag_load,
            "consecutive_loss_active": self.consecutive_loss_active,
            "flash_crash_active": self.flash_crash_active,
            "current_price": float(self.current_price) if self.current_price is not None else None,
            "cost_basis": float(self.cost_basis) if self.cost_basis is not None else None,
            "active_buy_executors": self.active_buy_executors,
            "active_sell_executors": self.active_sell_executors,
            "buy_entry_role": self.buy_entry_role,
            "base_entry_fraction": self.base_entry_fraction,
            "base_entry_budget_usd": float(self.base_entry_budget_usd),
            "size_multiplier": self.size_multiplier,
            "usd_per_entry": float(self.usd_per_entry),
            "max_total_position_usd": float(self.max_total_position_usd),
            "has_inventory": self.has_inventory,
            "sell_profitability_ok": self.sell_profitability_ok,
            "sell_trend_hold": self.sell_trend_hold,
        }


@dataclass
class RSIv55RiskDecision:
    risk_mode: str
    allow_entry: bool
    allow_runner_add: bool
    entry_fraction: float
    entry_budget_usd: Decimal
    effective_max_position_usd: Decimal
    sell_patience: str
    trailing_profile: str
    held_bag_exit_pressure: str
    gate_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "risk_mode": self.risk_mode,
            "allow_entry": self.allow_entry,
            "allow_runner_add": self.allow_runner_add,
            "entry_fraction": self.entry_fraction,
            "entry_budget_usd": float(self.entry_budget_usd),
            "effective_max_position_usd": float(self.effective_max_position_usd),
            "sell_patience": self.sell_patience,
            "trailing_profile": self.trailing_profile,
            "held_bag_exit_pressure": self.held_bag_exit_pressure,
            "gate_reason": self.gate_reason,
        }


class RSIv55RiskEngine:
    def __init__(
        self,
        model_snapshot_provider: Optional[Callable[[RSIv55RiskContext], Optional[RSIv55RiskModelSnapshot]]] = None,
    ):
        self._model_snapshot_provider = model_snapshot_provider

    def get_model_snapshot(self, context: RSIv55RiskContext) -> RSIv55RiskModelSnapshot:
        if self._model_snapshot_provider is None:
            return RSIv55RiskModelSnapshot()
        snapshot = self._model_snapshot_provider(context)
        if snapshot is None:
            return RSIv55RiskModelSnapshot()
        if isinstance(snapshot, RSIv55RiskModelSnapshot):
            return snapshot
        return RSIv55RiskModelSnapshot(**dict(snapshot))

    def classify_market_structure(
        self,
        *,
        risk_profile: str,
        trend_direction: Optional[str],
        trend_confidence: float,
        regime_label: Optional[str],
        atr_percentile: float,
        signal_quality: float,
        inventory_utilization: float,
        held_bag_load: float,
        consecutive_loss_active: bool,
        flash_crash_active: bool,
        trend_sensitivity: float,
        regime_sensitivity: float,
    ) -> str:
        if flash_crash_active or consecutive_loss_active:
            return "defensive"

        score = 0.0
        trend_sensitivity = max(0.1, float(trend_sensitivity))
        regime_sensitivity = max(0.1, float(regime_sensitivity))
        normalized_trend_conf = _clamp(float(trend_confidence or 0.0), 0.0, 1.0)

        if trend_direction == "up":
            score += (0.9 + normalized_trend_conf * 0.8) * trend_sensitivity
        elif trend_direction == "down":
            score -= (1.0 + normalized_trend_conf * 0.9) * trend_sensitivity

        normalized_regime = (regime_label or "").lower()
        if "trend_up" in normalized_regime:
            score += 0.65 * regime_sensitivity
        elif "trend_down" in normalized_regime:
            score -= 0.75 * regime_sensitivity
        elif "range" in normalized_regime:
            score += 0.25 * regime_sensitivity

        if atr_percentile >= 0.9:
            score -= 0.9
        elif atr_percentile >= 0.75:
            score -= 0.35
        elif 0.2 <= atr_percentile <= 0.65:
            score += 0.15

        if signal_quality >= 0.8:
            score += 0.35
        elif signal_quality <= 0.3:
            score -= 0.4

        if inventory_utilization >= 0.9:
            score -= 1.0
        elif inventory_utilization >= 0.7:
            score -= 0.5

        if held_bag_load >= 0.75:
            score -= 1.0
        elif held_bag_load > 0:
            score -= 0.35

        if risk_profile == "conservative":
            score -= 0.25
        elif risk_profile == "aggressive":
            score += 0.2

        if score <= -1.6:
            return "defensive"
        if score <= -0.45:
            return "hostile"
        if score >= 1.05:
            return "supportive"
        return "neutral"

    def evaluate(
        self,
        context: RSIv55RiskContext,
        config: Any,
        model_snapshot: Optional[RSIv55RiskModelSnapshot] = None,
    ) -> RSIv55RiskDecision:
        snapshot = model_snapshot or self.get_model_snapshot(context)

        risk_mode = context.market_structure or "neutral"
        allow_entry = True
        allow_runner_add = True
        gate_reason: Optional[str] = "ready"

        entry_fraction = _clamp(
            float(context.base_entry_fraction or 0.0),
            float(getattr(config, "risk_entry_floor_fraction", 0.0)),
            float(getattr(config, "risk_entry_ceiling_fraction", 1.0)),
        )
        entry_budget = _decimal_or_zero(context.base_entry_budget_usd)
        effective_cap = _decimal_or_zero(context.max_total_position_usd)
        sell_patience = "balanced"
        trailing_profile = "baseline"
        held_bag_exit_pressure = "balanced"

        if not bool(getattr(config, "risk_management_enabled", True)):
            return RSIv55RiskDecision(
                risk_mode="neutral",
                allow_entry=True,
                allow_runner_add=True,
                entry_fraction=entry_fraction,
                entry_budget_usd=entry_budget,
                effective_max_position_usd=effective_cap,
                sell_patience=sell_patience,
                trailing_profile=trailing_profile,
                held_bag_exit_pressure=held_bag_exit_pressure,
                gate_reason=gate_reason,
            )

        if risk_mode == "supportive":
            supportive_scale = float(getattr(config, "risk_supportive_entry_budget_scale", 1.0))
            entry_fraction = _clamp(
                entry_fraction * supportive_scale,
                float(getattr(config, "risk_entry_floor_fraction", 0.0)),
                float(getattr(config, "risk_entry_ceiling_fraction", 1.0)),
            )
            entry_budget = _decimal_or_zero(context.base_entry_budget_usd) * Decimal(str(supportive_scale))
            effective_cap = _decimal_or_zero(context.max_total_position_usd) * Decimal(
                str(getattr(config, "risk_supportive_cap_fraction", 1.0))
            )
            sell_patience = "patient"
            trailing_profile = "loose"
            held_bag_exit_pressure = "patient"
        elif risk_mode == "hostile":
            hostile_scale = float(getattr(config, "risk_hostile_entry_budget_scale", 0.5))
            entry_fraction = _clamp(
                entry_fraction * hostile_scale,
                float(getattr(config, "risk_entry_floor_fraction", 0.0)),
                float(getattr(config, "risk_entry_ceiling_fraction", 1.0)),
            )
            entry_budget = _decimal_or_zero(context.base_entry_budget_usd) * Decimal(str(hostile_scale))
            effective_cap = _decimal_or_zero(context.max_total_position_usd) * Decimal(
                str(getattr(config, "risk_hostile_cap_fraction", 0.75))
            )
            allow_runner_add = False
            sell_patience = "tight"
            trailing_profile = "tight"
            held_bag_exit_pressure = "elevated"
        elif risk_mode == "defensive":
            entry_fraction = 0.0
            entry_budget = Decimal("0")
            effective_cap = _decimal_or_zero(context.max_total_position_usd) * Decimal(
                str(getattr(config, "risk_defensive_cap_fraction", 0.5))
            )
            allow_entry = False
            allow_runner_add = False
            gate_reason = "risk_defensive"
            sell_patience = "urgent"
            trailing_profile = "defensive"
            held_bag_exit_pressure = "urgent"

        if context.buy_entry_role == "runner" and not allow_runner_add:
            allow_entry = False
            gate_reason = "risk_runner_block"

        confidence = float(snapshot.confidence or 0.0)
        min_conf = float(getattr(config, "risk_model_min_confidence", 0.0))
        if confidence >= min_conf:
            meta_accept = snapshot.meta_accept_prob
            if meta_accept is not None and meta_accept < float(getattr(config, "risk_model_min_meta_accept_prob", 0.0)):
                allow_entry = False
                gate_reason = "risk_model_accept"

            downside = snapshot.downside_quantile
            if downside is not None and downside > float(getattr(config, "risk_model_max_downside_quantile", 1.0)):
                allow_entry = False
                gate_reason = "risk_model_downside"

            continuation = snapshot.continuation_prob
            if continuation is not None and continuation < float(getattr(config, "risk_model_min_continuation_prob", 0.0)):
                allow_runner_add = False
                if context.buy_entry_role == "runner":
                    allow_entry = False
                    gate_reason = "risk_model_runner"

            recovery_success = snapshot.recovery_success_prob
            if recovery_success is not None and recovery_success < float(getattr(config, "risk_model_min_meta_accept_prob", 0.0)):
                if held_bag_exit_pressure == "balanced":
                    held_bag_exit_pressure = "elevated"
                elif held_bag_exit_pressure == "elevated":
                    held_bag_exit_pressure = "urgent"

        if entry_budget <= Decimal("0") or entry_fraction <= 0:
            allow_entry = False
            if gate_reason == "ready":
                gate_reason = "risk_budget_zero"

        if effective_cap <= Decimal("0"):
            effective_cap = Decimal("0")

        if allow_entry and gate_reason not in (None, "", "ready"):
            gate_reason = "ready"

        return RSIv55RiskDecision(
            risk_mode=risk_mode,
            allow_entry=allow_entry,
            allow_runner_add=allow_runner_add,
            entry_fraction=entry_fraction,
            entry_budget_usd=entry_budget.quantize(Decimal("1e-8")),
            effective_max_position_usd=effective_cap.quantize(Decimal("1e-8")),
            sell_patience=sell_patience,
            trailing_profile=trailing_profile,
            held_bag_exit_pressure=held_bag_exit_pressure,
            gate_reason=gate_reason,
        )


# Backwards-compatible aliases for controller imports that use the v5 prefix
# while the module/file name uses v55.
RSIv5RiskContext = RSIv55RiskContext
RSIv5RiskDecision = RSIv55RiskDecision
RSIv5RiskEngine = RSIv55RiskEngine
RSIv5RiskModelSnapshot = RSIv55RiskModelSnapshot
