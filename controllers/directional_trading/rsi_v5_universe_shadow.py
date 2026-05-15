"""Backtest-only RSI V5 universe shadow planner."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

from controllers.directional_trading.rsi_v55_risk import RSIv55RiskContext, RSIv55RiskEngine

SUPPORTED_VARIANTS = {
    "baseline_v5",
    "composite_gate_only",
    "composite_risk_v55",
    "composite_risk_trailing",
    "composite_exit_pressure",
}


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class UniversePairState:
    pair: str
    timestamp: float
    action: str = "hold"
    score: float = 0.0
    signal_score: int = 0
    signal_state: Dict[str, Any] = field(default_factory=dict)
    screener_rank: int = 0
    ranking_score: float = 0.0
    blockers: List[str] = field(default_factory=list)
    size_multiplier: float = 1.0
    base_entry_fraction: float = 1.0
    buy_entry_role: Optional[str] = None
    forecast_row: Dict[str, Any] = field(default_factory=dict)
    features: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class KronosCompositeState:
    expected_return_short: float = 0.0
    expected_return_medium: float = 0.0
    expected_return_long: float = 0.0
    adverse_excursion: float = 0.0
    confidence: float = 0.0
    coverage_pct: float = 0.0
    fresh_count: int = 0
    fallback_count: int = 0
    stale_count: int = 0
    missing_count: int = 0
    generated_at: Optional[float] = None
    feature_cutoff: Optional[float] = None
    provider: str = "unknown"
    is_fallback: bool = False
    stale: bool = False
    missing: bool = False
    source_metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PortfolioDecisionState:
    quote_balance: float = 0.0
    available_quote: float = 0.0
    active_positions: int = 0
    active_quote_exposure: float = 0.0
    pair_exposure_quote: Dict[str, float] = field(default_factory=dict)
    held_bag_count: int = 0
    cooldown_pairs: List[str] = field(default_factory=list)
    kill_switch: bool = False
    realized_pnl_quote: float = 0.0
    unrealized_pnl_quote: float = 0.0
    source_metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class KronosCompositeDecision:
    risk_mode: str
    blockers: List[str] = field(default_factory=list)
    reason: str = "ready"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UniverseDecision:
    pair: str
    action: str
    score: float
    risk_mode: str
    allow_entry: bool
    allow_runner_add: bool
    entry_fraction: float
    entry_budget_usd: float
    effective_max_position_usd: float
    size_multiplier: float
    trailing_profile: str
    held_bag_exit_pressure: str
    blockers: List[str] = field(default_factory=list)
    attribution: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RSIv5UniverseShadowControllerConfig:
    usd_per_entry: float = 30.0
    max_total_position_usd: float = 500.0
    min_fresh_coverage_pct: float = 80.0
    min_confidence: float = 0.25
    directional_confidence: float = 0.55
    supportive_long_return: float = 0.002
    hostile_long_return: float = -0.002
    hostile_adverse_excursion: float = -0.03
    risk_management_enabled: bool = True
    risk_profile: str = "balanced"
    risk_entry_floor_fraction: float = 0.15
    risk_entry_ceiling_fraction: float = 1.0
    risk_supportive_entry_budget_scale: float = 1.15
    risk_hostile_entry_budget_scale: float = 0.55
    risk_supportive_cap_fraction: float = 1.0
    risk_hostile_cap_fraction: float = 0.75
    risk_defensive_cap_fraction: float = 0.5
    risk_model_min_confidence: float = 0.55
    risk_model_min_meta_accept_prob: float = 0.5
    risk_model_max_downside_quantile: float = 0.08
    risk_model_min_continuation_prob: float = 0.5


def classify_kronos_composite(
    composite: KronosCompositeState,
    portfolio: PortfolioDecisionState,
    config: RSIv5UniverseShadowControllerConfig,
) -> KronosCompositeDecision:
    blockers: List[str] = []
    if portfolio.kill_switch:
        blockers.append("portfolio_kill_switch")
    if composite.missing:
        blockers.append("kronos_missing")
    if composite.stale:
        blockers.append("kronos_stale")
    if composite.is_fallback:
        blockers.append("kronos_fallback")
    if composite.coverage_pct < config.min_fresh_coverage_pct:
        blockers.append("kronos_coverage_low")
    if composite.confidence < config.min_confidence:
        blockers.append("kronos_confidence_low")
    if blockers:
        return KronosCompositeDecision("defensive", [*blockers, "kronos_defensive"], "fail_closed")
    if composite.confidence >= config.directional_confidence and (
        composite.expected_return_long <= config.hostile_long_return
        or composite.adverse_excursion <= config.hostile_adverse_excursion
    ):
        return KronosCompositeDecision("hostile", reason="negative_composite")
    if (
        composite.confidence >= config.directional_confidence
        and composite.expected_return_long >= config.supportive_long_return
        and composite.adverse_excursion > config.hostile_adverse_excursion
        and composite.coverage_pct >= config.min_fresh_coverage_pct
    ):
        return KronosCompositeDecision("supportive", reason="positive_composite")
    return KronosCompositeDecision("neutral", reason="mixed_composite")


def _pair_exposure(pair: UniversePairState, portfolio: PortfolioDecisionState) -> float:
    return _safe_float(portfolio.pair_exposure_quote.get(pair.pair), 0.0)


def _build_risk_context(
    pair: UniversePairState,
    composite_decision: KronosCompositeDecision,
    portfolio: PortfolioDecisionState,
    config: RSIv5UniverseShadowControllerConfig,
) -> RSIv55RiskContext:
    exposure = _pair_exposure(pair, portfolio)
    max_position = max(_safe_float(config.max_total_position_usd), 0.0)
    role = pair.buy_entry_role or str(pair.signal_state.get("buy_entry_role") or "full")
    entry_fraction = _clamp(float(pair.base_entry_fraction or 0.0), 0.0, 1.5)
    size_multiplier = max(float(pair.size_multiplier or 0.0), 0.0)
    base_budget = Decimal(str(config.usd_per_entry)) * Decimal(str(entry_fraction)) * Decimal(str(size_multiplier))

    return RSIv55RiskContext(
        signal_state=dict(pair.signal_state),
        market_structure=composite_decision.risk_mode,
        regime_label=f"kronos_{composite_decision.risk_mode}",
        regime_confidence=_clamp(pair.score / 100.0, 0.0, 1.0),
        trend_direction="up" if composite_decision.risk_mode == "supportive" else "down" if composite_decision.risk_mode == "hostile" else "range",
        trend_confidence=_clamp(pair.score / 100.0, 0.0, 1.0),
        atr_pct=None,
        atr_percentile=0.5,
        signal_quality=_clamp(pair.score / 100.0, 0.0, 1.0),
        inventory_utilization=(exposure / max_position) if max_position > 0 else 0.0,
        held_bag_load=_clamp(float(portfolio.held_bag_count) / 10.0, 0.0, 1.0),
        consecutive_loss_active=False,
        flash_crash_active=False,
        current_price=None,
        cost_basis=None,
        active_buy_executors=int(portfolio.active_positions),
        active_sell_executors=0,
        buy_entry_role=role,
        base_entry_fraction=entry_fraction,
        base_entry_budget_usd=base_budget,
        size_multiplier=size_multiplier,
        usd_per_entry=Decimal(str(config.usd_per_entry)),
        max_total_position_usd=Decimal(str(config.max_total_position_usd)),
        has_inventory=exposure > 0,
        sell_profitability_ok=bool(pair.signal_state.get("sell_profitability_ok", False)),
        sell_trend_hold=bool(pair.signal_state.get("sell_trend_hold", False)),
    )


def build_universe_decision(
    *,
    pair: UniversePairState,
    composite: KronosCompositeState,
    portfolio: PortfolioDecisionState,
    config: RSIv5UniverseShadowControllerConfig,
    variant: str = "composite_risk_v55",
    risk_engine: Optional[RSIv55RiskEngine] = None,
) -> UniverseDecision:
    if variant not in SUPPORTED_VARIANTS:
        raise ValueError(f"Unsupported universe shadow variant: {variant}")
    composite_decision = classify_kronos_composite(composite, portfolio, config)
    blockers = list(dict.fromkeys([*pair.blockers, *composite_decision.blockers]))
    role = pair.buy_entry_role or str(pair.signal_state.get("buy_entry_role") or "full")
    size_multiplier = max(float(pair.size_multiplier or 0.0), 0.0)
    base_entry_fraction = _clamp(float(pair.base_entry_fraction or 0.0), 0.0, 1.5)
    base_entry_budget = float(config.usd_per_entry) * base_entry_fraction * size_multiplier

    if variant == "baseline_v5":
        risk_mode = "neutral"
        allow_entry = not portfolio.kill_switch
        allow_runner_add = True
        entry_fraction = base_entry_fraction
        entry_budget_usd = base_entry_budget
        effective_max_position_usd = float(config.max_total_position_usd)
        trailing_profile = "baseline"
        held_bag_exit_pressure = "balanced"
    elif variant == "composite_gate_only":
        risk_mode = composite_decision.risk_mode
        allow_entry = composite_decision.risk_mode != "defensive"
        allow_runner_add = allow_entry and composite_decision.risk_mode != "hostile"
        entry_fraction = base_entry_fraction
        entry_budget_usd = base_entry_budget if allow_entry else 0.0
        effective_max_position_usd = float(config.max_total_position_usd)
        trailing_profile = "baseline"
        held_bag_exit_pressure = "balanced"
    else:
        risk_context = _build_risk_context(pair, composite_decision, portfolio, config)
        risk_decision = (risk_engine or RSIv55RiskEngine()).evaluate(risk_context, config)
        risk_mode = risk_decision.risk_mode
        allow_entry = bool(risk_decision.allow_entry)
        allow_runner_add = bool(risk_decision.allow_runner_add)
        entry_fraction = float(risk_decision.entry_fraction)
        entry_budget_usd = float(risk_decision.entry_budget_usd)
        effective_max_position_usd = float(risk_decision.effective_max_position_usd)
        trailing_profile = "baseline" if variant == "composite_risk_v55" else risk_decision.trailing_profile
        held_bag_exit_pressure = risk_decision.held_bag_exit_pressure if variant in {"composite_exit_pressure", "composite_risk_trailing"} else "balanced"
        if risk_decision.gate_reason and risk_decision.gate_reason != "ready":
            blockers.append(str(risk_decision.gate_reason))

    if role == "runner" and not allow_runner_add:
        allow_entry = False
        blockers.append("risk_runner_block")
    requested_action = pair.action.lower()
    action = requested_action if requested_action in {"buy", "sell", "hold"} else "hold"
    if action == "buy" and (not allow_entry or blockers):
        action = "hold"
    return UniverseDecision(
        pair=pair.pair,
        action=action,
        score=float(pair.score),
        risk_mode=risk_mode,
        allow_entry=allow_entry,
        allow_runner_add=allow_runner_add,
        entry_fraction=entry_fraction,
        entry_budget_usd=entry_budget_usd,
        effective_max_position_usd=effective_max_position_usd,
        size_multiplier=size_multiplier,
        trailing_profile=trailing_profile,
        held_bag_exit_pressure=held_bag_exit_pressure,
        blockers=list(dict.fromkeys(blockers)),
        attribution={
            "variant": variant,
            "requested_action": requested_action,
            "composite": composite.to_dict(),
            "composite_decision": composite_decision.to_dict(),
            "portfolio": portfolio.to_dict(),
            "pair": pair.to_dict(),
        },
    )


class RSIv5UniverseShadowController:
    def __init__(self, config: Optional[RSIv5UniverseShadowControllerConfig] = None):
        self.config = config or RSIv5UniverseShadowControllerConfig()
        self.processed_data: Dict[str, Any] = {}

    def evaluate(
        self,
        *,
        pairs: Iterable[UniversePairState],
        composite: KronosCompositeState,
        portfolio: PortfolioDecisionState,
        variant: str = "composite_risk_v55",
    ) -> List[UniverseDecision]:
        decisions = [
            build_universe_decision(pair=pair, composite=composite, portfolio=portfolio, config=self.config, variant=variant)
            for pair in pairs
        ]
        self.processed_data = {
            "variant": variant,
            "decisions": [decision.to_dict() for decision in decisions],
            "composite": composite.to_dict(),
            "portfolio": portfolio.to_dict(),
        }
        return decisions

    def determine_executor_actions(self) -> List[Any]:
        return []


__all__ = [
    "KronosCompositeDecision",
    "KronosCompositeState",
    "PortfolioDecisionState",
    "RSIv5UniverseShadowController",
    "RSIv5UniverseShadowControllerConfig",
    "SUPPORTED_VARIANTS",
    "UniverseDecision",
    "UniversePairState",
    "build_universe_decision",
    "classify_kronos_composite",
]
