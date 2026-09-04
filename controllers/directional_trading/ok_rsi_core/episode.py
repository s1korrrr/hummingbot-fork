"""Pure, fill-driven spot inventory lifecycle; no exchange, clock, or storage I/O.

Amounts are Decimal in base/quote units. Quote fees exclude converted base fees:
base fees change inventory, quote fees change cash. Each request is one network
attempt; uncertain attempts remain reserved until a native terminal receipt.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any

from controllers.directional_trading.ok_rsi_core.entry_pricing import (
    ADAPTIVE_REFERENCE_FALLBACK_POLICY,
    ADAPTIVE_RESERVED_ENTRY_POLICY,
    AdaptiveEntryPriceInputError,
    AdaptiveEntryPriceSelection,
    select_adaptive_entry_price,
)

ZERO = Decimal("0")
ONE = Decimal("1")
PHASES = {
    "WATCH",
    "SCOUT",
    "ACCUMULATE",
    "DEFENSIVE",
    "EXHAUSTED",
    "RECOVER",
    "MARKUP",
    "DISTRIBUTE",
    "EXIT",
    "COOLDOWN",
}
ORDER_INTENTS = {
    "UNSPECIFIED",
    "SCOUT_BUY",
    "SCALE_IN_BUY",
    "RECOVERY_TAKE_PROFIT",
    "DISTRIBUTION_SELL",
    "THIN_SCOUT_RELEASE",
    "RECOVERY_CAPITAL_RELEASE",
    "RECOVERY_ROTATION_EXIT",
    "RECOVERY_STRENGTH_RELEASE",
    "TRAILING_PROFIT_EXIT",
    "TERMINAL_PROFIT_EXIT",
}
SELL_INTENTS = ORDER_INTENTS - {"UNSPECIFIED", "SCOUT_BUY", "SCALE_IN_BUY"}
EXIT_POLICIES = {
    "DEFAULT",
    "STRUCTURE_HELD_RUNNER_V1",
    "MARKUP_RELEASED_RUNNER_V1",
    "BOUNDED_MARKUP_RUNNER_V7",
    "THIN_SCOUT_RELEASE_V1",
    "DEEP_RECOVERY_CAPITAL_RELEASE_V4",
    "DEEP_RECOVERY_ROTATION_EXIT_V5",
    "DEEP_RECOVERY_STRENGTH_RUNNER_V6",
}
EXIT_TRAIL_BREACHES = {
    "NONE",
    "PRICE",
    "EQUITY",
    "PRICE_AND_EQUITY",
    "RISK_OVERRIDE",
    "MARKUP_LOSS",
}
RESERVED_ENTRY_POLICIES = {
    ADAPTIVE_RESERVED_ENTRY_POLICY,
    ADAPTIVE_REFERENCE_FALLBACK_POLICY,
}
TERMINAL_CAUSES = {
    "UNRECORDED",
    "PENDING",
    "FILLED",
    "NATURAL_EXPIRY",
    "EXTERNAL_TERMINAL",
    "HARD_RAIL",
    "LIFECYCLE",
    "CONTEXT",
}
TERMINATION_REQUESTS = {
    "NONE",
    "NATURAL_EXPIRY",
    "HARD_RAIL",
    "LIFECYCLE",
    "CONTEXT",
}


class EpisodeInputError(ValueError):
    """Unreconciled, contradictory, or invalid policy input; do not dispatch."""


def _decimal(value: Any, name: str, *, positive: bool = False) -> Decimal:
    try:
        if isinstance(value, bool):
            raise ValueError("boolean amount")
        result = Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise EpisodeInputError(f"invalid {name}") from exc
    if not result.is_finite() or result < 0 or (positive and result <= 0):
        raise EpisodeInputError(f"invalid {name}: must be finite and {'positive' if positive else 'nonnegative'}")
    return result


def _signed_decimal(value: Any, name: str) -> Decimal:
    try:
        if isinstance(value, bool):
            raise ValueError("boolean amount")
        result = Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise EpisodeInputError(f"invalid {name}") from exc
    if not result.is_finite():
        raise EpisodeInputError(f"invalid {name}: must be finite")
    return result


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(_json_value(value), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _floor(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def _ceil(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


@dataclass(frozen=True)
class EpisodeConfig:
    controller_id: str
    connector: str
    trading_pair: str
    mode: str
    budget_quote: Decimal
    mark_cap_quote: Decimal
    fee_rate: Decimal = Decimal("0.001")
    min_profit: Decimal = Decimal("0.002")
    level_fractions: tuple[Decimal, ...] = tuple(map(Decimal, (".10", ".15", ".20", ".25", ".30")))
    dip_atr_levels: tuple[Decimal, ...] = tuple(map(Decimal, ("2", "3", "4", "5", "6")))
    distribution_atr_levels: tuple[Decimal, ...] = tuple(map(Decimal, ("4", "7", "10")))
    remaining_fractions: tuple[Decimal, ...] = tuple(map(Decimal, (".8", ".6", ".4")))
    hostile_scout_fraction: Decimal = Decimal(".25")
    rebound_atr: Decimal = Decimal(".25")
    basis_improvement_atr: Decimal = Decimal(".05")
    last_fill_gap_atr: Decimal = Decimal(".25")
    trail_atr: Decimal = Decimal("2")
    trail_min: Decimal = Decimal(".005")
    trail_max: Decimal = Decimal(".04")
    setup_bars: int = 10
    stabilization_bars: int = 2
    interval_seconds: int = 60
    require_profit: bool = True
    sequential_scale_ins: bool = False
    require_scale_in_signal: bool = False
    hostile_risk_latch: bool = False
    recovery_exit_enabled: bool = False
    adaptive_entry_pricing: bool = False
    adaptive_reserved_entry: bool = False
    adaptive_reference_fallback: bool = False
    thin_scout_release: bool = False
    thin_scout_rearm: bool = False
    profitable_episode_rearm: bool = False
    deep_recovery_capital_release: bool = False
    deep_recovery_rotation_exit: bool = False
    deep_recovery_strength_runner: bool = False
    scout_probe_rebound: bool = False
    scout_probe_fraction: Decimal = Decimal(".5")
    scout_rebound_confirmation: bool = False
    recovery_aware_scout: bool = False
    recovery_rebound_scout: bool = False
    structure_held_runner: bool = False
    markup_release_runner: bool = False
    bounded_markup_runner: bool = False
    assignment_id: str | None = None
    initial_base: Decimal = ZERO
    initial_cost: Decimal | None = None

    def __post_init__(self):
        for name in ("controller_id", "connector", "trading_pair"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise EpisodeInputError(f"invalid {name}")
        if self.mode not in ("buy_sell", "sell_only"):
            raise EpisodeInputError("unsupported mode")
        for name in (
            "require_profit",
            "sequential_scale_ins",
            "require_scale_in_signal",
            "hostile_risk_latch",
            "recovery_exit_enabled",
            "adaptive_entry_pricing",
            "adaptive_reserved_entry",
            "adaptive_reference_fallback",
            "thin_scout_release",
            "thin_scout_rearm",
            "profitable_episode_rearm",
            "deep_recovery_capital_release",
            "deep_recovery_rotation_exit",
            "deep_recovery_strength_runner",
            "scout_probe_rebound",
            "scout_rebound_confirmation",
            "recovery_aware_scout",
            "recovery_rebound_scout",
            "structure_held_runner",
            "markup_release_runner",
            "bounded_markup_runner",
        ):
            if type(getattr(self, name)) is not bool:
                raise EpisodeInputError(f"invalid boolean {name}")
        for name in ("budget_quote", "mark_cap_quote", "trail_atr", "trail_min", "trail_max"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name, positive=True))
        for name in (
            "fee_rate",
            "min_profit",
            "hostile_scout_fraction",
            "scout_probe_fraction",
            "rebound_atr",
            "basis_improvement_atr",
            "last_fill_gap_atr",
            "initial_base",
        ):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        if (
            self.fee_rate >= ONE
            or self.hostile_scout_fraction > ONE
            or not ZERO < self.scout_probe_fraction <= ONE
            or not ZERO < self.trail_min <= self.trail_max < ONE
        ):
            raise EpisodeInputError("invalid fee, sizing factor or trailing range")
        if sum(
            (
                self.adaptive_entry_pricing,
                self.scout_rebound_confirmation,
                self.recovery_aware_scout,
                self.recovery_rebound_scout,
            )
        ) > 1:
            raise EpisodeInputError("entry treatments cannot be combined")
        if self.scout_probe_rebound and (
            self.scout_probe_fraction >= ONE
            or self.scout_rebound_confirmation
            or self.recovery_aware_scout
            or self.recovery_rebound_scout
        ):
            raise EpisodeInputError("scout probe treatment cannot be combined")
        if self.markup_release_runner and not self.structure_held_runner:
            raise EpisodeInputError("markup release runner requires structure held runner")
        if self.bounded_markup_runner and not self.markup_release_runner:
            raise EpisodeInputError("bounded markup runner requires markup release runner")
        if self.adaptive_reserved_entry and not self.adaptive_entry_pricing:
            raise EpisodeInputError("reserved adaptive entry requires adaptive entry pricing")
        if self.adaptive_reference_fallback and not self.adaptive_reserved_entry:
            raise EpisodeInputError("reference fallback requires reserved adaptive entry")
        if self.thin_scout_rearm and not self.thin_scout_release:
            raise EpisodeInputError("thin scout rearm requires thin scout release")
        if self.deep_recovery_rotation_exit and not self.deep_recovery_capital_release:
            raise EpisodeInputError(
                "deep recovery rotation requires deep recovery capital release"
            )
        if self.deep_recovery_strength_runner and not self.deep_recovery_capital_release:
            raise EpisodeInputError(
                "deep recovery strength runner requires deep recovery capital release"
            )
        if self.deep_recovery_strength_runner and self.deep_recovery_rotation_exit:
            raise EpisodeInputError(
                "deep recovery strength runner and rotation are mutually exclusive"
            )
        for name in ("level_fractions", "dip_atr_levels", "distribution_atr_levels", "remaining_fractions"):
            object.__setattr__(self, name, tuple(_decimal(x, name, positive=True) for x in getattr(self, name)))
        if (
            not 1 <= len(self.level_fractions) <= 20
            or len(self.level_fractions) != len(self.dip_atr_levels)
            or sum(self.level_fractions) > ONE
        ):
            raise EpisodeInputError("invalid level allocations")
        for levels in (self.dip_atr_levels, self.distribution_atr_levels):
            if any(a >= b for a, b in zip(levels, levels[1:])):
                raise EpisodeInputError("levels must strictly increase")
        if (
            not self.remaining_fractions
            or len(self.remaining_fractions) != len(self.distribution_atr_levels)
            or self.remaining_fractions[0] >= ONE
            or any(a <= b for a, b in zip(self.remaining_fractions, self.remaining_fractions[1:]))
        ):
            raise EpisodeInputError("remaining fractions must strictly decrease within (0,1)")
        for name in ("setup_bars", "stabilization_bars", "interval_seconds"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise EpisodeInputError(f"invalid {name}")
        if self.stabilization_bars > self.setup_bars:
            raise EpisodeInputError("stabilization_bars cannot exceed setup_bars")
        if self.initial_cost is not None:
            object.__setattr__(self, "initial_cost", _decimal(self.initial_cost, "initial_cost", positive=True))
        if self.mode == "sell_only" and (not self.assignment_id or self.initial_base <= ZERO):
            raise EpisodeInputError("sell_only requires an explicit positive inventory assignment")
        if self.mode == "sell_only" and self.require_profit and self.initial_cost is None:
            raise EpisodeInputError("sell_only profit guard requires assigned cost")
        if self.mode == "buy_sell" and (self.initial_base or self.initial_cost is not None or self.assignment_id):
            raise EpisodeInputError("buy_sell starts from an empty attributed episode")

    @property
    def identity(self) -> str:
        values = asdict(self)
        # Preserve pre-adaptive checkpoint identity while the opt-in is off.
        # An unresolved episode may not be stranded by a code-only upgrade.
        if not self.adaptive_entry_pricing:
            values.pop("adaptive_entry_pricing")
        if not self.adaptive_reserved_entry:
            values.pop("adaptive_reserved_entry")
        if not self.adaptive_reference_fallback:
            values.pop("adaptive_reference_fallback")
        if not self.thin_scout_release:
            values.pop("thin_scout_release")
        if not self.thin_scout_rearm:
            values.pop("thin_scout_rearm")
        if not self.profitable_episode_rearm:
            values.pop("profitable_episode_rearm")
        if not self.deep_recovery_capital_release:
            values.pop("deep_recovery_capital_release")
        if not self.deep_recovery_rotation_exit:
            values.pop("deep_recovery_rotation_exit")
        if not self.deep_recovery_strength_runner:
            values.pop("deep_recovery_strength_runner")
        if not self.scout_probe_rebound:
            values.pop("scout_probe_rebound")
            values.pop("scout_probe_fraction")
        if not self.scout_rebound_confirmation:
            values.pop("scout_rebound_confirmation")
        if not self.recovery_aware_scout:
            values.pop("recovery_aware_scout")
        if not self.recovery_rebound_scout:
            values.pop("recovery_rebound_scout")
        if not self.structure_held_runner:
            values.pop("structure_held_runner")
        if not self.markup_release_runner:
            values.pop("markup_release_runner")
        if not self.bounded_markup_runner:
            values.pop("bounded_markup_runner")
        return _hash(values)


@dataclass(frozen=True)
class MarketSnapshot:
    timestamp: int
    bid: Decimal
    ask: Decimal
    low: Decimal
    atr_1m: Decimal
    atr_15m: Decimal
    atr_1h: Decimal
    anchor: Decimal
    quote_available: Decimal
    base_available: Decimal
    min_amount: Decimal
    min_notional: Decimal
    amount_step: Decimal
    price_step: Decimal
    htf_timestamp: int = 0
    valid: bool = True
    entry_signal: bool = False
    recovery: bool = False
    hourly_recovered: bool = False
    markup: bool = False
    reset: bool = False
    hostile: bool = False
    cascade: bool = False
    buy_allowed: bool = True
    reservation_allowed: bool = True
    quality_factor: Decimal = ONE
    entry_confidence: Decimal = ONE
    trend_alignment: Decimal = ZERO

    def __post_init__(self):
        if type(self.timestamp) is not int or self.timestamp < 0 or type(self.htf_timestamp) is not int:
            raise EpisodeInputError("invalid market timestamp")
        if self.htf_timestamp < 0 or self.htf_timestamp > self.timestamp:
            raise EpisodeInputError("future higher-timeframe observation")
        for name in (
            "bid",
            "ask",
            "low",
            "atr_1m",
            "atr_15m",
            "atr_1h",
            "anchor",
            "quote_available",
            "base_available",
            "min_amount",
            "min_notional",
            "amount_step",
            "price_step",
        ):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        if self.amount_step <= 0 or self.price_step <= 0:
            raise EpisodeInputError("quantization steps must be positive")
        object.__setattr__(self, "quality_factor", _decimal(self.quality_factor, "quality_factor"))
        if self.quality_factor > ONE:
            raise EpisodeInputError("quality factor cannot enlarge allocation")
        object.__setattr__(self, "entry_confidence", _decimal(self.entry_confidence, "entry_confidence"))
        object.__setattr__(self, "trend_alignment", _signed_decimal(self.trend_alignment, "trend_alignment"))
        if self.entry_confidence > ONE:
            raise EpisodeInputError("entry confidence must be within [0,1]")
        if not -ONE <= self.trend_alignment <= ONE:
            raise EpisodeInputError("trend alignment must be within [-1,1]")
        for name in (
            "valid",
            "entry_signal",
            "recovery",
            "hourly_recovered",
            "markup",
            "reset",
            "hostile",
            "cascade",
            "buy_allowed",
            "reservation_allowed",
        ):
            if type(getattr(self, name)) is not bool:
                raise EpisodeInputError(f"invalid boolean {name}")
        if self.valid and (
            min(self.bid, self.ask, self.low, self.atr_1m, self.atr_15m, self.atr_1h, self.anchor) <= 0
            or self.bid > self.ask
        ):
            raise EpisodeInputError("invalid prices, ATR or crossed book")


@dataclass(frozen=True)
class OrderRequest:
    id: str
    episode_id: str
    level: int
    side: str
    amount: Decimal
    price: Decimal
    reserved_quote: Decimal
    expires_at: int
    revision: int
    intent: str = "UNSPECIFIED"
    pricing_policy: str = "UNRECORDED"
    reference_price: Decimal = ZERO
    pricing_discount: Decimal = ZERO
    pricing_discount_atr: Decimal = ZERO
    signal_confidence: Decimal = ZERO
    trend_alignment: Decimal = ZERO
    parent_request_id: str = ""
    setup_policy: str = "IMMEDIATE"
    setup_started_at: int = 0
    setup_confirmation_started_at: int = 0
    setup_low: Decimal = ZERO
    setup_rebound: Decimal = ZERO
    setup_no_new_low: int = 0
    setup_stage: str = "NONE"
    setup_plan_quote: Decimal = ZERO
    exit_policy: str = "DEFAULT"
    exit_hourly_recovered: bool = False
    exit_risk_clear: bool = False
    exit_trail_breach: str = "NONE"
    exit_target_base: Decimal = ZERO
    exit_target_revision: int = 0

    def __post_init__(self):
        for name in ("id", "episode_id"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise EpisodeInputError("invalid request identity")
        for name in (
            "level",
            "expires_at",
            "revision",
            "setup_started_at",
            "setup_confirmation_started_at",
            "setup_no_new_low",
            "exit_target_revision",
        ):
            if type(getattr(self, name)) is not int:
                raise EpisodeInputError("invalid request integer")
        if self.side not in ("BUY", "SELL") or self.expires_at <= 0 or self.revision <= 0:
            raise EpisodeInputError("invalid request boundary")
        if self.intent not in ORDER_INTENTS:
            raise EpisodeInputError("invalid request intent")
        if self.pricing_policy not in {
            "UNRECORDED",
            "TOP_OF_BOOK",
            "ADAPTIVE_MAKER_V1",
            ADAPTIVE_RESERVED_ENTRY_POLICY,
            ADAPTIVE_REFERENCE_FALLBACK_POLICY,
        }:
            raise EpisodeInputError("invalid entry pricing policy")
        if not isinstance(self.parent_request_id, str):
            raise EpisodeInputError("invalid parent request identity")
        if self.pricing_policy == ADAPTIVE_REFERENCE_FALLBACK_POLICY:
            if not self.parent_request_id or self.parent_request_id == self.id:
                raise EpisodeInputError("reference fallback requires a distinct parent request")
        elif self.parent_request_id:
            raise EpisodeInputError("non-fallback request cannot carry a parent request")
        if self.setup_policy not in {
            "IMMEDIATE",
            "SCOUT_PROBE_REBOUND_V1",
            "SCOUT_REBOUND_V1",
            "RECOVERY_AWARE_SCOUT_V1",
            "RECOVERY_REBOUND_SCOUT_V1",
        }:
            raise EpisodeInputError("invalid entry setup policy")
        if self.setup_stage not in {"NONE", "PROBE", "REMAINDER"}:
            raise EpisodeInputError("invalid entry setup stage")
        object.__setattr__(
            self,
            "setup_plan_quote",
            _decimal(self.setup_plan_quote, "setup_plan_quote"),
        )
        if self.setup_policy != "SCOUT_PROBE_REBOUND_V1" and (
            self.setup_stage != "NONE" or self.setup_plan_quote
        ):
            raise EpisodeInputError("non-probe setup cannot carry probe receipt")
        if self.exit_policy not in EXIT_POLICIES or self.exit_trail_breach not in EXIT_TRAIL_BREACHES:
            raise EpisodeInputError("invalid exit policy receipt")
        if type(self.exit_hourly_recovered) is not bool or type(self.exit_risk_clear) is not bool:
            raise EpisodeInputError("invalid exit context receipt")
        object.__setattr__(self, "exit_target_base", _decimal(self.exit_target_base, "exit_target_base"))
        if self.exit_target_revision < 0:
            raise EpisodeInputError("invalid exit target revision")
        if self.exit_policy == "DEFAULT":
            if any(
                (
                    self.exit_hourly_recovered,
                    self.exit_risk_clear,
                    self.exit_trail_breach != "NONE",
                    self.exit_target_base,
                    self.exit_target_revision,
                )
            ):
                raise EpisodeInputError("default exit policy cannot carry structure receipt")
        elif self.side != "SELL":
            raise EpisodeInputError("structure exit policy requires a sell request")
        elif self.exit_trail_breach == "MARKUP_LOSS" and self.exit_policy not in {
            "MARKUP_RELEASED_RUNNER_V1",
            "BOUNDED_MARKUP_RUNNER_V7",
        }:
            raise EpisodeInputError("markup loss receipt requires markup release policy")
        elif self.exit_trail_breach == "RISK_OVERRIDE" and self.exit_risk_clear:
            raise EpisodeInputError("risk override receipt contradicts clear context")
        if self.intent != "UNSPECIFIED" and (
            (self.side == "BUY" and self.intent not in {"SCOUT_BUY", "SCALE_IN_BUY"})
            or (self.side == "SELL" and self.intent not in SELL_INTENTS)
        ):
            raise EpisodeInputError("request intent contradicts side")
        if (self.side == "BUY" and self.level < 0) or (self.side == "SELL" and self.level != -1):
            raise EpisodeInputError("invalid request level")
        for name in ("amount", "price", "reserved_quote"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name, positive=name != "reserved_quote"))
        for name in (
            "reference_price",
            "pricing_discount",
            "pricing_discount_atr",
            "signal_confidence",
            "setup_low",
            "setup_rebound",
        ):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        object.__setattr__(self, "trend_alignment", _signed_decimal(self.trend_alignment, "trend_alignment"))
        if self.signal_confidence > ONE or not -ONE <= self.trend_alignment <= ONE:
            raise EpisodeInputError("invalid entry pricing context")
        if self.pricing_policy != "UNRECORDED":
            if self.side != "BUY" or self.reference_price <= 0 or self.reference_price - self.price != self.pricing_discount:
                raise EpisodeInputError("entry pricing receipt contradicts request")
            if self.pricing_policy == "TOP_OF_BOOK" and self.pricing_discount != 0:
                raise EpisodeInputError("top-of-book receipt cannot contain a discount")
            if self.pricing_policy in {
                "ADAPTIVE_MAKER_V1",
                ADAPTIVE_RESERVED_ENTRY_POLICY,
            } and self.pricing_discount <= 0:
                raise EpisodeInputError("adaptive receipt requires a passive discount")
            if (
                self.pricing_policy == ADAPTIVE_REFERENCE_FALLBACK_POLICY
                and self.pricing_discount < 0
            ):
                raise EpisodeInputError("reference fallback cannot exceed its parent reference")
        if self.setup_policy == "IMMEDIATE":
            if any(
                (
                    self.setup_started_at,
                    self.setup_confirmation_started_at,
                    self.setup_low,
                    self.setup_rebound,
                    self.setup_no_new_low,
                    self.setup_stage != "NONE",
                    self.setup_plan_quote,
                )
            ):
                raise EpisodeInputError("immediate entry cannot carry rebound setup state")
        elif self.setup_policy == "SCOUT_PROBE_REBOUND_V1":
            probe = (
                self.setup_stage == "PROBE"
                and self.setup_started_at > 0
                and self.setup_confirmation_started_at == 0
                and self.setup_low > 0
                and self.setup_rebound == 0
                and self.setup_no_new_low == 0
                and self.setup_plan_quote > self.reserved_quote
            )
            remainder = (
                self.setup_stage == "REMAINDER"
                and self.setup_started_at > 0
                and self.setup_confirmation_started_at == 0
                and self.setup_low > 0
                and self.setup_rebound > 0
                and self.setup_no_new_low > 0
                and self.setup_plan_quote >= self.reserved_quote > 0
                and self.reference_price >= self.setup_low + self.setup_rebound
            )
            if self.side != "BUY" or self.level != 0 or not (probe or remainder):
                raise EpisodeInputError("scout probe receipt contradicts request")
        elif self.setup_policy == "RECOVERY_AWARE_SCOUT_V1":
            if (
                self.side != "BUY"
                or self.level != 0
                or any(
                    (
                        self.setup_started_at,
                        self.setup_confirmation_started_at,
                        self.setup_low,
                        self.setup_rebound,
                        self.setup_no_new_low,
                        self.setup_stage != "NONE",
                        self.setup_plan_quote,
                    )
                )
            ):
                raise EpisodeInputError("recovery-aware scout receipt contradicts request")
        elif self.setup_policy == "RECOVERY_REBOUND_SCOUT_V1":
            immediate = not any(
                (
                    self.setup_started_at,
                    self.setup_confirmation_started_at,
                    self.setup_low,
                    self.setup_rebound,
                    self.setup_no_new_low,
                )
            )
            confirmed = (
                self.setup_started_at > 0
                and self.setup_confirmation_started_at > self.setup_started_at
                and self.setup_low > 0
                and self.setup_rebound > 0
                and self.setup_no_new_low > 0
                and self.price >= self.setup_low + self.setup_rebound
            )
            if self.side != "BUY" or self.level != 0 or not (immediate or confirmed):
                raise EpisodeInputError("recovery rebound scout receipt contradicts request")
        elif (
            self.side != "BUY"
            or self.level != 0
            or self.setup_started_at <= 0
            or self.setup_confirmation_started_at != 0
            or self.setup_low <= 0
            or self.setup_rebound <= 0
            or self.setup_no_new_low <= 0
            or self.price < self.setup_low + self.setup_rebound
        ):
            raise EpisodeInputError("rebound entry receipt contradicts request")


@dataclass(frozen=True)
class OrderUpdate:
    request_id: str
    filled_base: Decimal
    filled_quote: Decimal
    fee_quote: Decimal = ZERO
    fee_base: Decimal = ZERO
    done: bool = False
    fees_known: bool = True
    terminal_cause: str = "UNRECORDED"

    def __post_init__(self):
        for name in ("filled_base", "filled_quote", "fee_quote", "fee_base"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        if type(self.done) is not bool or type(self.fees_known) is not bool:
            raise EpisodeInputError("invalid execution status")
        if self.terminal_cause not in TERMINAL_CAUSES:
            raise EpisodeInputError("invalid terminal cause")
        if not self.done and self.terminal_cause not in {"UNRECORDED", "PENDING"}:
            raise EpisodeInputError("nonterminal update cannot carry a terminal cause")
        if self.done and self.terminal_cause == "PENDING":
            raise EpisodeInputError("terminal update cannot remain pending")
        if not self.request_id or (self.filled_base > 0 and self.filled_quote <= 0):
            raise EpisodeInputError("invalid execution identity or quote amount")


@dataclass
class OrderProgress:
    request: OrderRequest
    filled_base: Decimal = ZERO
    filled_quote: Decimal = ZERO
    fee_quote: Decimal = ZERO
    fee_base: Decimal = ZERO
    done: bool = False
    seen: bool = False
    termination_requested: str = "NONE"
    terminal_cause: str = "UNRECORDED"

    def __post_init__(self):
        if self.termination_requested not in TERMINATION_REQUESTS:
            raise EpisodeInputError("invalid requested termination cause")
        if self.terminal_cause not in TERMINAL_CAUSES:
            raise EpisodeInputError("invalid stored terminal cause")

    @property
    def reserved_quote(self) -> Decimal:
        return ZERO if self.done else max(ZERO, self.request.reserved_quote - self.filled_quote - self.fee_quote)

    @property
    def reserved_base(self) -> Decimal:
        return ZERO if self.done or self.request.side != "SELL" else max(ZERO, self.request.amount - self.filled_base)


@dataclass
class EpisodeState:
    schema_version: int = 1
    config_hash: str = ""
    phase: str = "WATCH"
    episode_number: int = 0
    episode_id: str | None = None
    timestamp: int = -1
    levels: list[str] = field(default_factory=list)
    orders: dict[str, OrderProgress] = field(default_factory=dict)
    base: Decimal = ZERO
    cost: Decimal = ZERO
    gross_spent: Decimal = ZERO
    sell_proceeds: Decimal = ZERO
    frozen_anchor: Decimal = ZERO
    frozen_atr: Decimal = ZERO
    last_fill_price: Decimal = ZERO
    last_valid_bid: Decimal = ZERO
    setup_level: int = -1
    setup_low: Decimal = ZERO
    setup_start: int = 0
    setup_confirmation_start: int = 0
    no_new_low: int = 0
    scout_plan_quote: Decimal = ZERO
    scout_probe_request_id: str = ""
    distribution_start_base: Decimal = ZERO
    distribution_basis: Decimal = ZERO
    distribution_atr: Decimal = ZERO
    target_base: Decimal = ZERO
    peak: Decimal = ZERO
    floor: Decimal = ZERO
    equity_peak: Decimal | None = None
    equity_floor: Decimal | None = None
    command_revision: int = 0
    cooldown_htf_timestamp: int = 0
    assignment_consumed: bool = False
    cost_known: bool = True
    adverse_entry_latched: bool = False
    exit_intent: str | None = None
    structure_runner_active: bool = False
    exit_target_revision: int = 0
    last_exit_trail_breach: str = "NONE"
    last_exit_hourly_recovered: bool = False
    last_exit_risk_clear: bool = False
    fault: str | None = None

    def to_dict(self) -> dict[str, Any]:
        values = asdict(self)
        if not self.structure_runner_active:
            values.pop("structure_runner_active")
        if not self.exit_target_revision:
            values.pop("exit_target_revision")
        if not self.scout_plan_quote:
            values.pop("scout_plan_quote")
        if not self.scout_probe_request_id:
            values.pop("scout_probe_request_id")
        if self.last_exit_trail_breach == "NONE":
            values.pop("last_exit_trail_breach")
            values.pop("last_exit_hourly_recovered")
            values.pop("last_exit_risk_clear")
        for order in values["orders"].values():
            if order["termination_requested"] == "NONE":
                order.pop("termination_requested")
            if order["terminal_cause"] == "UNRECORDED":
                order.pop("terminal_cause")
            request = order["request"]
            if request["setup_stage"] == "NONE":
                request.pop("setup_stage")
                request.pop("setup_plan_quote")
            if not request["parent_request_id"]:
                request.pop("parent_request_id")
            if request["exit_policy"] == "DEFAULT":
                for name in (
                    "exit_policy",
                    "exit_hourly_recovered",
                    "exit_risk_clear",
                    "exit_trail_breach",
                    "exit_target_base",
                    "exit_target_revision",
                ):
                    request.pop(name)
        return _json_value(values)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> EpisodeState:
        try:
            values = copy.deepcopy(payload)
            if values.get("schema_version") != 1 or values.get("phase") not in PHASES:
                raise EpisodeInputError("unsupported episode schema/phase")
            values.setdefault("adverse_entry_latched", False)
            values.setdefault("exit_intent", None)
            values.setdefault("setup_confirmation_start", 0)
            values.setdefault("scout_plan_quote", "0")
            values.setdefault("scout_probe_request_id", "")
            if not isinstance(values["scout_probe_request_id"], str):
                raise EpisodeInputError("invalid scout probe request identity")
            values.setdefault("structure_runner_active", False)
            values.setdefault("exit_target_revision", 0)
            values.setdefault("last_exit_trail_breach", "NONE")
            values.setdefault("last_exit_hourly_recovered", False)
            values.setdefault("last_exit_risk_clear", False)
            for name in (
                "assignment_consumed",
                "cost_known",
                "adverse_entry_latched",
                "structure_runner_active",
                "last_exit_hourly_recovered",
                "last_exit_risk_clear",
            ):
                if type(values[name]) is not bool:
                    raise EpisodeInputError(f"invalid checkpoint boolean {name}")
            for name in (
                "schema_version",
                "episode_number",
                "timestamp",
                "setup_level",
                "setup_start",
                "setup_confirmation_start",
                "no_new_low",
                "command_revision",
                "cooldown_htf_timestamp",
                "exit_target_revision",
            ):
                if type(values[name]) is not int or values[name] < (-1 if name in ("timestamp", "setup_level") else 0):
                    raise EpisodeInputError(f"invalid checkpoint integer {name}")
            if any(level not in ("OPEN", "AUTHORIZED", "SKIPPED") for level in values["levels"]):
                raise EpisodeInputError("invalid checkpoint level")
            if values["exit_intent"] is not None and values["exit_intent"] not in SELL_INTENTS:
                raise EpisodeInputError("invalid checkpoint exit intent")
            if values["last_exit_trail_breach"] not in EXIT_TRAIL_BREACHES:
                raise EpisodeInputError("invalid checkpoint exit trail breach")
            decimal_fields = (
                "base",
                "cost",
                "gross_spent",
                "frozen_anchor",
                "frozen_atr",
                "last_fill_price",
                "last_valid_bid",
                "setup_low",
                "scout_plan_quote",
                "distribution_start_base",
                "distribution_basis",
                "distribution_atr",
                "target_base",
                "peak",
                "floor",
            )
            for name in decimal_fields:
                values[name] = _decimal(values[name], name)
            for name in ("equity_peak", "equity_floor", "sell_proceeds"):
                if values[name] is not None:
                    values[name] = Decimal(values[name])
                    if not values[name].is_finite():
                        raise EpisodeInputError(f"invalid {name}")
            orders = {}
            for key, raw in values["orders"].items():
                raw.setdefault("termination_requested", "NONE")
                raw.setdefault("terminal_cause", "UNRECORDED")
                if type(raw["done"]) is not bool or type(raw["seen"]) is not bool:
                    raise EpisodeInputError("invalid checkpoint execution status")
                for name in ("filled_base", "filled_quote", "fee_quote", "fee_base"):
                    raw[name] = _decimal(raw[name], name)
                request = raw["request"]
                request.setdefault("parent_request_id", "")
                request.setdefault("setup_stage", "NONE")
                request.setdefault("setup_plan_quote", "0")
                for name in (
                    "amount",
                    "price",
                    "reserved_quote",
                    "reference_price",
                    "pricing_discount",
                    "pricing_discount_atr",
                    "signal_confidence",
                    "exit_target_base",
                ):
                    if name not in request:
                        continue
                    request[name] = _decimal(request[name], name)
                if "trend_alignment" in request:
                    request["trend_alignment"] = _signed_decimal(request["trend_alignment"], "trend_alignment")
                raw["request"] = OrderRequest(**request)
                if key != raw["request"].id or raw["request"].side not in ("BUY", "SELL"):
                    raise EpisodeInputError("invalid stored order identity")
                if (
                    raw["request"].amount <= 0
                    or raw["request"].price <= 0
                    or raw["filled_base"] > raw["request"].amount
                ):
                    raise EpisodeInputError("invalid stored execution amounts")
                orders[key] = OrderProgress(**raw)
            values["orders"] = orders
            return cls(**values)
        except (KeyError, TypeError, ArithmeticError) as exc:
            raise EpisodeInputError("invalid episode checkpoint") from exc


@dataclass(frozen=True)
class Transition:
    state: EpisodeState
    requests: tuple[OrderRequest, ...] = ()
    cancel_ids: tuple[str, ...] = ()
    reason: str = "watch"


def remaining_budget(config: EpisodeConfig, state: EpisodeState) -> Decimal:
    if config.mode == "sell_only":
        return ZERO
    return (
        config.budget_quote
        - state.gross_spent
        - sum((order.reserved_quote for order in state.orders.values() if order.request.side == "BUY"), ZERO)
    )


def acquisition_levels(state: EpisodeState) -> tuple[int, ...]:
    """Return current-episode BUY levels with positive attributed inventory."""
    return tuple(
        sorted(
            {
                order.request.level
                for order in state.orders.values()
                if order.request.episode_id == state.episode_id
                and order.request.side == "BUY"
                and order.filled_base - order.fee_base > ZERO
            }
        )
    )


def _accounting(config: EpisodeConfig, state: EpisodeState) -> dict[str, Decimal]:
    """Project the one accumulation then liquidation episode from fill truth.

    Delayed acquisition fees restate average basis of both sold and remaining
    inventory. Recomputing also makes checkpoint contradictions observable.
    """
    assigned = config.mode == "sell_only" and state.assignment_consumed
    acquired = config.initial_base if assigned else ZERO
    spent = (config.initial_cost or ZERO) if assigned else ZERO
    sold = proceeds = ZERO
    for order in state.orders.values():
        if order.request.episode_id != state.episode_id:
            if not order.done:
                raise EpisodeInputError("unresolved order outside current episode")
            continue
        if order.request.side == "BUY":
            if config.mode != "buy_sell":
                raise EpisodeInputError("BUY receipt in sell_only assignment")
            acquired += order.filled_base - order.fee_base
            spent += order.filled_quote + order.fee_quote
        else:
            sold += order.filled_base + order.fee_base
            proceeds += order.filled_quote - order.fee_quote
    base = acquired - sold
    if acquired < ZERO or base < ZERO:
        raise EpisodeInputError("receipt accounting exceeds attributed inventory")
    cost = spent * base / acquired if acquired else ZERO
    return dict(base=base, cost=cost, gross_spent=spent, sell_proceeds=proceeds)


def _validate_accounting(config: EpisodeConfig, state: EpisodeState) -> None:
    if any(getattr(state, name) != value for name, value in _accounting(config, state).items()):
        raise EpisodeInputError("checkpoint accounting contradicts execution receipts")
    if state.setup_confirmation_start and (
        state.setup_level != 0
        or state.setup_start <= 0
        or state.setup_confirmation_start <= state.setup_start
    ):
        raise EpisodeInputError("checkpoint recovery confirmation clock is invalid")
    if state.episode_id and len(state.levels) != len(config.level_fractions):
        raise EpisodeInputError("checkpoint level allocation mismatch")
    if state.scout_plan_quote or state.scout_probe_request_id:
        probe = state.orders.get(state.scout_probe_request_id)
        if (
            not config.scout_probe_rebound
            or state.scout_plan_quote <= ZERO
            or not state.scout_probe_request_id
            or state.setup_level != 0
            or state.setup_start <= 0
            or state.setup_low <= ZERO
            or probe is None
            or probe.request.episode_id != state.episode_id
            or probe.request.setup_policy != "SCOUT_PROBE_REBOUND_V1"
            or probe.request.setup_stage != "PROBE"
            or probe.request.setup_plan_quote != state.scout_plan_quote
            or state.scout_plan_quote > config.budget_quote
        ):
            raise EpisodeInputError("checkpoint scout probe state is invalid")
    elif state.setup_level == 0 and config.scout_probe_rebound:
        raise EpisodeInputError("checkpoint scout probe plan is missing")
    current = [order for order in state.orders.values() if order.request.episode_id == state.episode_id]
    fallback_parents = set()
    for child in current:
        parent_id = child.request.parent_request_id
        if not parent_id:
            continue
        if parent_id in fallback_parents:
            raise EpisodeInputError("fallback parent has multiple children")
        fallback_parents.add(parent_id)
        parent = state.orders.get(parent_id)
        if (
            parent is None
            or parent.request.episode_id != child.request.episode_id
            or parent.request.level != child.request.level
            or parent.request.pricing_policy != ADAPTIVE_RESERVED_ENTRY_POLICY
            or not parent.done
            or parent.terminal_cause != "NATURAL_EXPIRY"
            or parent.request.revision >= child.request.revision
            or parent.request.reference_price != child.request.reference_price
            or child.request.amount > parent.request.amount
            or child.request.reserved_quote > parent.request.reserved_quote
        ):
            raise EpisodeInputError("fallback parent receipt is invalid")
    for order in current:
        request = order.request
        expected = _hash((state.episode_id, request.revision, request.side, request.level))
        if request.id != expected or request.revision > state.command_revision:
            raise EpisodeInputError("checkpoint authorization identity mismatch")
        reserve = request.amount * request.price * (ONE + config.fee_rate) if request.side == "BUY" else ZERO
        if reserve != request.reserved_quote:
            raise EpisodeInputError("checkpoint reservation mismatch")
        if request.side == "BUY":
            if request.level >= len(state.levels):
                raise EpisodeInputError("checkpoint authorization level mismatch")
            rearmed_without_fill = (
                config.adaptive_entry_pricing
                and state.levels[request.level] == "OPEN"
                and order.done
                and not any((order.filled_base, order.filled_quote, order.fee_quote, order.fee_base))
            )
            if state.levels[request.level] != "AUTHORIZED" and not rearmed_without_fill:
                raise EpisodeInputError("checkpoint authorization level mismatch")
    if sum(not order.done for order in current) > 1:
        raise EpisodeInputError("multiple unresolved episode requests")
    if state.structure_runner_active and (
        not config.structure_held_runner
        or state.phase != "DISTRIBUTE"
        or state.target_base <= ZERO
    ):
        raise EpisodeInputError("checkpoint structure runner state is invalid")


def _apply_update(config: EpisodeConfig, state: EpisodeState, market: MarketSnapshot, update: OrderUpdate) -> None:
    if update.request_id not in state.orders:
        raise EpisodeInputError("unknown execution request")
    if not update.fees_known:
        raise EpisodeInputError("execution fee currency/value unavailable")
    order = state.orders[update.request_id]
    values = ("filled_base", "filled_quote", "fee_quote", "fee_base")
    if any(getattr(update, name) < getattr(order, name) for name in values):
        raise EpisodeInputError("cumulative execution receipt decreased")
    if order.done and not update.done:
        raise EpisodeInputError("terminal order became pending")
    became_terminal = not order.done and update.done
    if (
        config.adaptive_reference_fallback
        and became_terminal
        and update.terminal_cause == "UNRECORDED"
    ):
        raise EpisodeInputError("reference fallback requires an explicit terminal cause")
    db, dq, fq, fb = (getattr(update, name) - getattr(order, name) for name in values)
    if any((db, dq, fq, fb)) and order.request.episode_id != state.episode_id:
        raise EpisodeInputError("closed episode receipt changed; explicit reconciliation required")
    if update.filled_base > order.request.amount:
        raise EpisodeInputError("fill exceeds authorized base amount")
    if order.request.side == "BUY":
        if db > 0:
            state.last_fill_price = dq / db
        if update.filled_quote + update.fee_quote > order.request.reserved_quote:
            state.fault = "buy_reservation_overrun"
    for name in values:
        setattr(order, name, getattr(update, name))
    order.done = update.done
    order.seen = True
    if config.adaptive_reference_fallback and became_terminal:
        if any((update.filled_base, update.filled_quote, update.fee_quote, update.fee_base)):
            order.terminal_cause = "FILLED"
        elif (
            not market.valid
            or market.cascade
            or (
                order.request.side == "BUY"
                and order.request.pricing_policy in RESERVED_ENTRY_POLICIES
                and not market.reservation_allowed
            )
        ):
            order.terminal_cause = "HARD_RAIL"
        elif order.termination_requested != "NONE":
            order.terminal_cause = order.termination_requested
        else:
            order.terminal_cause = update.terminal_cause
    if (
        order.request.side == "BUY"
        and config.adaptive_entry_pricing
        and became_terminal
        and not any((update.filled_base, update.filled_quote, update.fee_quote, update.fee_base))
        and state.levels[order.request.level] == "AUTHORIZED"
        and not any(
            other.request.episode_id == state.episode_id
            and other.request.side == "BUY"
            and other.request.level == order.request.level
            and other.filled_base > ZERO
            for other in state.orders.values()
        )
    ):
        state.levels[order.request.level] = "OPEN"
    for name, value in _accounting(config, state).items():
        setattr(state, name, value)
    if db == ZERO and dq == ZERO and (fq or fb):
        # Missing books cannot erase an accounting correction. Use the last
        # explicitly valid causal valuation while preserving actual fee units.
        restatement = fq + fb * state.last_valid_bid * (ONE - config.fee_rate)
        for name in ("equity_peak", "equity_floor"):
            if getattr(state, name) is not None:
                setattr(state, name, getattr(state, name) - restatement)


def _start_episode(config: EpisodeConfig, state: EpisodeState, market: MarketSnapshot) -> None:
    state.episode_number += 1
    state.episode_id = _hash(
        (
            config.controller_id,
            config.connector,
            config.trading_pair,
            state.episode_number,
            market.timestamp,
            config.assignment_id,
        )
    )[:24]
    state.levels = ["OPEN"] * len(config.level_fractions)
    state.frozen_anchor = market.anchor
    state.frozen_atr = market.atr_1h
    state.adverse_entry_latched = config.hostile_risk_latch and (market.hostile or market.reset)


def _abandon_unfunded_episode(
    config: EpisodeConfig,
    state: EpisodeState,
    market: MarketSnapshot,
) -> EpisodeState:
    current = [order for order in state.orders.values() if order.request.episode_id == state.episode_id]
    if state.base or state.gross_spent or any(not order.done for order in current):
        raise EpisodeInputError("cannot abandon a funded or unresolved episode")
    return EpisodeState(
        config_hash=config.identity,
        episode_number=state.episode_number,
        timestamp=market.timestamp,
        orders=state.orders,
        last_valid_bid=state.last_valid_bid,
        assignment_consumed=state.assignment_consumed,
    )


def _request(
    config: EpisodeConfig,
    state: EpisodeState,
    market: MarketSnapshot,
    *,
    side: str,
    amount: Decimal,
    price: Decimal,
    reserve: Decimal,
    level: int,
    intent: str,
    entry_price_selection: AdaptiveEntryPriceSelection | None = None,
    parent_request_id: str = "",
    setup_receipt: dict | None = None,
    exit_receipt: dict | None = None,
) -> OrderRequest:
    state.command_revision += 1
    identity = _hash((state.episode_id, state.command_revision, side, level))
    pricing = {} if entry_price_selection is None else {
        "pricing_policy": entry_price_selection.policy,
        "reference_price": entry_price_selection.reference_price,
        "pricing_discount": entry_price_selection.discount,
        "pricing_discount_atr": entry_price_selection.discount_atr,
        "signal_confidence": entry_price_selection.signal_confidence,
        "trend_alignment": entry_price_selection.trend_alignment,
    }
    request = OrderRequest(
        identity,
        state.episode_id or "",
        level,
        side,
        amount,
        price,
        reserve,
        market.timestamp + config.setup_bars * config.interval_seconds,
        state.command_revision,
        intent,
        **pricing,
        parent_request_id=parent_request_id,
        **(setup_receipt or {}),
        **(exit_receipt or {}),
    )
    state.orders[identity] = OrderProgress(request)
    return request


def _entry_price_selection(
    config: EpisodeConfig,
    market: MarketSnapshot,
) -> AdaptiveEntryPriceSelection:
    reference_price = _floor(market.ask, market.price_step)
    if config.adaptive_entry_pricing:
        try:
            selection = select_adaptive_entry_price(
                bid=market.bid,
                ask=market.ask,
                atr_1m=market.atr_1m,
                price_step=market.price_step,
                signal_confidence=market.entry_confidence,
                trend_alignment=market.trend_alignment,
            )
        except AdaptiveEntryPriceInputError as exc:
            raise EpisodeInputError(str(exc)) from exc
        if config.adaptive_reserved_entry:
            selection = replace(
                selection,
                policy=ADAPTIVE_RESERVED_ENTRY_POLICY,
            )
        return selection
    return AdaptiveEntryPriceSelection(
        policy="TOP_OF_BOOK",
        reference_price=reference_price,
        price=reference_price,
        discount=ZERO,
        discount_atr=ZERO,
        signal_confidence=market.entry_confidence,
        trend_alignment=market.trend_alignment,
    )


def _clear_scout_probe(state: EpisodeState) -> None:
    state.scout_plan_quote = ZERO
    state.scout_probe_request_id = ""
    state.setup_level = -1
    state.setup_low = ZERO
    state.setup_start = 0
    state.setup_confirmation_start = 0
    state.no_new_low = 0


def _scout_probe_remainder(
    config: EpisodeConfig,
    state: EpisodeState,
    market: MarketSnapshot,
) -> Transition | None:
    if not config.scout_probe_rebound or state.scout_plan_quote <= ZERO:
        return None
    probe = state.orders.get(state.scout_probe_request_id)
    if (
        probe is None
        or probe.request.episode_id != state.episode_id
        or probe.request.level != 0
        or probe.request.setup_policy != "SCOUT_PROBE_REBOUND_V1"
        or probe.request.setup_stage != "PROBE"
        or probe.request.setup_plan_quote != state.scout_plan_quote
    ):
        raise EpisodeInputError("scout probe checkpoint contradicts authorization")
    if (
        not market.valid
        or market.cascade
        or market.hostile
        or market.reset
        or market.recovery
        or market.markup
        or not market.buy_allowed
        or not market.reservation_allowed
        or state.phase in {"MARKUP", "DISTRIBUTE", "EXIT", "COOLDOWN"}
        or market.timestamp - state.setup_start >= config.setup_bars * config.interval_seconds
    ):
        _clear_scout_probe(state)
        return None
    if probe.done and probe.filled_base <= ZERO:
        _clear_scout_probe(state)
        return None
    if market.low < state.setup_low:
        state.setup_low = market.low
        state.no_new_low = 0
    else:
        state.no_new_low += 1
    current = [
        order
        for order in state.orders.values()
        if order.request.episode_id == state.episode_id
    ]
    if not probe.done or any(not order.done for order in current):
        return None
    rebound = max(
        config.rebound_atr * market.atr_1m,
        2 * (market.ask - market.bid),
    )
    if (
        state.no_new_low < config.stabilization_bars
        or market.ask < state.setup_low + rebound
    ):
        return Transition(state, reason="need_scout_probe_rebound")
    spent = sum(
        order.filled_quote + order.fee_quote
        for order in current
        if order.request.side == "BUY" and order.request.level == 0
    )
    cap = state.scout_plan_quote - spent
    if cap <= ZERO:
        _clear_scout_probe(state)
        return None
    selection = _entry_price_selection(config, market)
    price = selection.price
    if price <= ZERO:
        _clear_scout_probe(state)
        return Transition(state, reason="price_below_tick")
    amount = _floor(cap / (price * (ONE + config.fee_rate)), market.amount_step)
    reserve = amount * price * (ONE + config.fee_rate)
    if amount <= ZERO or amount < market.min_amount or amount * price < market.min_notional:
        _clear_scout_probe(state)
        return Transition(state, reason="scout_probe_remainder_below_exchange_minimum")
    if reserve > remaining_budget(config, state) or reserve > market.quote_available:
        return Transition(state, reason="scout_probe_remainder_insufficient_cash")
    if state.base * market.ask + amount * market.ask > config.mark_cap_quote:
        return Transition(state, reason="scout_probe_remainder_mark_cap")
    if state.base <= ZERO:
        raise EpisodeInputError("scout probe remainder requires attributed inventory")
    before = state.cost / state.base
    after = (state.cost + reserve) / (state.base + amount)
    if before - after < config.basis_improvement_atr * state.frozen_atr:
        return Transition(state, reason="scout_probe_remainder_basis_guard")
    setup_receipt = {
        "setup_policy": "SCOUT_PROBE_REBOUND_V1",
        "setup_started_at": state.setup_start,
        "setup_low": state.setup_low,
        "setup_rebound": rebound,
        "setup_no_new_low": state.no_new_low,
        "setup_stage": "REMAINDER",
        "setup_plan_quote": state.scout_plan_quote,
    }
    _clear_scout_probe(state)
    state.phase = "SCOUT"
    request = _request(
        config,
        state,
        market,
        side="BUY",
        amount=amount,
        price=price,
        reserve=reserve,
        level=0,
        intent="SCOUT_BUY",
        entry_price_selection=selection,
        setup_receipt=setup_receipt,
    )
    return Transition(state, (request,), reason="scout_probe_remainder")


def _mark_termination(orders: list[OrderProgress], cause: str) -> tuple[str, ...]:
    if cause not in TERMINATION_REQUESTS - {"NONE"}:
        raise EpisodeInputError("invalid requested termination cause")
    for order in orders:
        if order.termination_requested == "NONE" or cause == "HARD_RAIL":
            order.termination_requested = cause
    return tuple(order.request.id for order in orders)


def _pending_cancellations(
    config: EpisodeConfig,
    pending: list[OrderProgress],
    market: MarketSnapshot,
) -> tuple[str, ...]:
    cancelled = []
    for order in pending:
        request = order.request
        reserved_entry = bool(
            config.adaptive_reserved_entry
            and request.pricing_policy in RESERVED_ENTRY_POLICIES
        )
        hard_rail = bool(
            not market.valid
            or market.cascade
            or (
                request.side == "BUY"
                and reserved_entry
                and not market.reservation_allowed
            )
        )
        expired = market.timestamp >= request.expires_at
        context_cancel = bool(
            request.side == "BUY"
            and not reserved_entry
            and (
                market.hostile
                or not market.buy_allowed
                or market.markup
                or market.recovery
            )
        )
        cause = (
            "HARD_RAIL"
            if hard_rail
            else "NATURAL_EXPIRY"
            if expired
            else "CONTEXT"
            if context_cancel
            else None
        )
        if cause is not None:
            _mark_termination([order], cause)
            cancelled.append(request.id)
    return tuple(cancelled)


def _adaptive_reference_fallback(
    config: EpisodeConfig,
    state: EpisodeState,
    market: MarketSnapshot,
) -> Transition | None:
    if (
        not config.adaptive_reference_fallback
        or config.mode != "buy_sell"
        or state.phase in {"MARKUP", "DISTRIBUTE", "EXIT", "COOLDOWN"}
        or state.exit_intent is not None
        or not market.reservation_allowed
        or market.cascade
        or not market.valid
        or state.base > market.base_available
    ):
        return None
    child_parents = {
        order.request.parent_request_id
        for order in state.orders.values()
        if order.request.parent_request_id
    }
    eligible = [
        order
        for order in state.orders.values()
        if order.request.episode_id == state.episode_id
        and order.request.pricing_policy == ADAPTIVE_RESERVED_ENTRY_POLICY
        and order.done
        and order.terminal_cause == "NATURAL_EXPIRY"
        and not any((order.filled_base, order.filled_quote, order.fee_quote, order.fee_base))
        and order.request.id not in child_parents
        and order.request.expires_at <= market.timestamp
        <= order.request.expires_at + config.interval_seconds
    ]
    if len(eligible) > 1:
        raise EpisodeInputError("multiple adaptive reference fallbacks are eligible")
    if not eligible:
        return None
    source = eligible[0]
    request = source.request
    if state.levels[request.level] != "OPEN":
        raise EpisodeInputError("reference fallback source level is not open")
    maker_ceiling = _floor(market.bid - market.price_step, market.price_step)
    price = min(request.reference_price, maker_ceiling)
    if price <= 0:
        return Transition(state, reason="fallback_price_below_tick")
    amount = min(
        request.amount,
        _floor(
            request.reserved_quote / (price * (ONE + config.fee_rate)),
            market.amount_step,
        ),
    )
    reserve = amount * price * (ONE + config.fee_rate)
    if amount <= 0 or amount < market.min_amount or amount * price < market.min_notional:
        return Transition(state, reason="fallback_below_exchange_minimum")
    if reserve > request.reserved_quote:
        raise EpisodeInputError("reference fallback enlarged the parent reservation")
    if reserve > remaining_budget(config, state) or reserve > market.quote_available:
        return Transition(state, reason="fallback_insufficient_episode_cash")
    if state.base * market.ask + amount * market.ask > config.mark_cap_quote:
        return Transition(state, reason="fallback_mark_exposure_cap")
    if state.base > 0:
        before = state.cost / state.base
        after = (state.cost + reserve) / (state.base + amount)
        if before - after < config.basis_improvement_atr * state.frozen_atr:
            return Transition(state, reason="fallback_insufficient_basis_improvement")
    selection = AdaptiveEntryPriceSelection(
        policy=ADAPTIVE_REFERENCE_FALLBACK_POLICY,
        reference_price=request.reference_price,
        price=price,
        discount=request.reference_price - price,
        discount_atr=(request.reference_price - price) / market.atr_1m,
        signal_confidence=request.signal_confidence,
        trend_alignment=request.trend_alignment,
    )
    setup_receipt = {
        "setup_policy": request.setup_policy,
        "setup_started_at": request.setup_started_at,
        "setup_confirmation_started_at": request.setup_confirmation_started_at,
        "setup_low": request.setup_low,
        "setup_rebound": request.setup_rebound,
        "setup_no_new_low": request.setup_no_new_low,
        "setup_stage": request.setup_stage,
        "setup_plan_quote": request.setup_plan_quote,
    }
    state.levels[request.level] = "AUTHORIZED"
    state.setup_level = -1
    state.setup_confirmation_start = 0
    state.phase = "SCOUT" if request.level == 0 else "ACCUMULATE"
    child = _request(
        config,
        state,
        market,
        side="BUY",
        amount=amount,
        price=price,
        reserve=reserve,
        level=request.level,
        intent=request.intent,
        entry_price_selection=selection,
        parent_request_id=request.id,
        setup_receipt=setup_receipt,
    )
    return Transition(state, (child,), reason="adaptive_reference_fallback")


def _finish_if_flat(
    config: EpisodeConfig,
    state: EpisodeState,
    market: MarketSnapshot,
) -> Transition | None:
    current = [order for order in state.orders.values() if order.request.episode_id == state.episode_id]
    if config.adaptive_entry_pricing and state.gross_spent == 0 and any(level == "OPEN" for level in state.levels):
        return None
    if state.episode_id and state.base == 0 and current and all(order.done for order in current):
        profitable_rearm = bool(
            config.profitable_episode_rearm
            and config.mode == "buy_sell"
            and state.sell_proceeds > state.gross_spent
        )
        thin_scout_rearm = bool(
            config.thin_scout_rearm and state.exit_intent == "THIN_SCOUT_RELEASE"
        )
        if profitable_rearm or thin_scout_rearm:
            return Transition(
                EpisodeState(
                    config_hash=config.identity,
                    episode_number=state.episode_number,
                    timestamp=market.timestamp,
                    orders=state.orders,
                    last_valid_bid=state.last_valid_bid,
                    assignment_consumed=state.assignment_consumed,
                ),
                reason=(
                    "thin_scout_rearmed"
                    if state.exit_intent == "THIN_SCOUT_RELEASE"
                    else "profitable_episode_rearmed"
                ),
            )
        state.phase = "COOLDOWN"
        state.cooldown_htf_timestamp = market.htf_timestamp
        return Transition(state, reason="episode_completed")
    return None


def minimum_profit_price(config: EpisodeConfig, state: EpisodeState) -> Decimal | None:
    if not state.base or not state.cost_known:
        return None
    return (state.cost / state.base) * (ONE + config.min_profit) / (ONE - config.fee_rate)


def _trail_distance(config: EpisodeConfig, market: MarketSnapshot) -> Decimal:
    return min(config.trail_max, max(config.trail_min, config.trail_atr * market.atr_15m / market.bid))


def _arm_recovery_trail(config: EpisodeConfig, state: EpisodeState, market: MarketSnapshot) -> None:
    distance = _trail_distance(config, market)
    state.phase = "RECOVER"
    state.target_base = state.base
    state.peak = market.bid
    state.floor = state.peak * (ONE - distance)
    equity = state.sell_proceeds + state.base * market.bid * (ONE - config.fee_rate) - state.gross_spent
    state.equity_peak = equity
    state.equity_floor = equity - state.base * state.peak * distance
    state.exit_intent = "RECOVERY_TAKE_PROFIT"


def _reduce_exit_target(config: EpisodeConfig, state: EpisodeState, target: Decimal) -> None:
    target = min(state.target_base, target)
    if target < state.target_base:
        state.target_base = target
        if config.structure_held_runner:
            state.exit_target_revision += 1


def _exit_trail_breach(*, price_breached: bool, equity_breached: bool) -> str:
    if price_breached and equity_breached:
        return "PRICE_AND_EQUITY"
    if price_breached:
        return "PRICE"
    if equity_breached:
        return "EQUITY"
    return "NONE"


def _exit_receipt(
    config: EpisodeConfig,
    state: EpisodeState,
    market: MarketSnapshot,
    trail_breach: str,
    request_intent: str,
) -> dict[str, Any]:
    if request_intent == "THIN_SCOUT_RELEASE":
        return {
            "exit_policy": "THIN_SCOUT_RELEASE_V1",
            "exit_hourly_recovered": market.hourly_recovered,
            "exit_risk_clear": not (market.cascade or market.hostile or market.reset),
            "exit_trail_breach": trail_breach,
            "exit_target_base": state.target_base,
            "exit_target_revision": state.exit_target_revision,
        }
    if request_intent == "RECOVERY_CAPITAL_RELEASE":
        return {
            "exit_policy": "DEEP_RECOVERY_CAPITAL_RELEASE_V4",
            "exit_hourly_recovered": market.hourly_recovered,
            "exit_risk_clear": not (market.cascade or market.hostile or market.reset),
            "exit_trail_breach": trail_breach,
            "exit_target_base": state.target_base,
            "exit_target_revision": state.exit_target_revision,
        }
    if request_intent == "RECOVERY_ROTATION_EXIT":
        return {
            "exit_policy": "DEEP_RECOVERY_ROTATION_EXIT_V5",
            "exit_hourly_recovered": market.hourly_recovered,
            "exit_risk_clear": not (market.cascade or market.hostile or market.reset),
            "exit_trail_breach": trail_breach,
            "exit_target_base": state.target_base,
            "exit_target_revision": state.exit_target_revision,
        }
    if request_intent == "RECOVERY_STRENGTH_RELEASE":
        return {
            "exit_policy": "DEEP_RECOVERY_STRENGTH_RUNNER_V6",
            "exit_hourly_recovered": market.hourly_recovered,
            "exit_risk_clear": not (market.cascade or market.hostile or market.reset),
            "exit_trail_breach": trail_breach,
            "exit_target_base": state.target_base,
            "exit_target_revision": state.exit_target_revision,
        }
    if not config.structure_held_runner:
        return {}
    use_target_context = (
        state.last_exit_trail_breach != "NONE"
        and (state.structure_runner_active or state.phase == "EXIT")
    )
    return {
        "exit_policy": (
            "BOUNDED_MARKUP_RUNNER_V7"
            if config.bounded_markup_runner
            else "MARKUP_RELEASED_RUNNER_V1"
            if config.markup_release_runner
            else "STRUCTURE_HELD_RUNNER_V1"
        ),
        "exit_hourly_recovered": (
            state.last_exit_hourly_recovered if use_target_context else market.hourly_recovered
        ),
        "exit_risk_clear": (
            state.last_exit_risk_clear
            if use_target_context
            else not (market.cascade or market.hostile or market.reset)
        ),
        "exit_trail_breach": state.last_exit_trail_breach if use_target_context else trail_breach,
        "exit_target_base": state.target_base,
        "exit_target_revision": state.exit_target_revision,
    }


def _sell(
    config: EpisodeConfig,
    state: EpisodeState,
    market: MarketSnapshot,
    *,
    intent: str | None = None,
    trail_breach: str = "NONE",
    reason: str | None = None,
) -> Transition:
    request_intent = intent or state.exit_intent or (
        "TERMINAL_PROFIT_EXIT" if state.phase == "EXIT" else "DISTRIBUTION_SELL"
    )
    profit_protected = config.require_profit or request_intent in {
        "RECOVERY_TAKE_PROFIT",
        "THIN_SCOUT_RELEASE",
        "RECOVERY_CAPITAL_RELEASE",
        "RECOVERY_ROTATION_EXIT",
        "RECOVERY_STRENGTH_RELEASE",
    }
    if not state.cost_known and profit_protected:
        return Transition(state, reason="basis_unavailable")
    min_price = minimum_profit_price(config, state) or ZERO
    if profit_protected and market.bid < min_price:
        return Transition(state, reason="profit_guard_blocked")
    # Price-bounded limit: no unconstrained market fallback can defeat the floor.
    price = _ceil(max(market.bid, min_price if profit_protected else ZERO), market.price_step)
    pending = sum((order.reserved_base for order in state.orders.values()), ZERO)
    needed = max(ZERO, state.base - state.target_base - pending)
    amount = _floor(min(needed, market.base_available), market.amount_step)
    if amount <= 0 or amount < market.min_amount or amount * price < market.min_notional:
        return Transition(state, reason="dust" if needed > 0 else "target_satisfied")
    request = _request(
        config,
        state,
        market,
        side="SELL",
        amount=amount,
        price=price,
        reserve=ZERO,
        level=-1,
        intent=request_intent,
        exit_receipt=_exit_receipt(
            config,
            state,
            market,
            trail_breach,
            request_intent,
        ),
    )
    mapped_reason = {
        "RECOVERY_TAKE_PROFIT": "recovery_take_profit",
        "DISTRIBUTION_SELL": "distribution",
        "THIN_SCOUT_RELEASE": "thin_scout_release",
        "RECOVERY_CAPITAL_RELEASE": "deep_recovery_capital_release",
        "RECOVERY_ROTATION_EXIT": "deep_recovery_rotation_exit",
        "RECOVERY_STRENGTH_RELEASE": "deep_recovery_strength_runner",
        "TRAILING_PROFIT_EXIT": "trailing_profit_exit",
        "TERMINAL_PROFIT_EXIT": "terminal_exit",
    }[request_intent]
    return Transition(state, (request,), reason=reason or mapped_reason)


def transition(
    config: EpisodeConfig,
    previous: EpisodeState,
    market: MarketSnapshot,
    updates: tuple[OrderUpdate, ...] | list[OrderUpdate] = (),
) -> Transition:
    """Reconcile actual cumulative receipts, then process at most one closed bar.

    Caller must durably store returned state before emitting requests/cancels.
    Existing commands remain reserved on missing acknowledgement or restart.
    """
    state = copy.deepcopy(previous)
    if state.config_hash and state.config_hash != config.identity:
        raise EpisodeInputError("episode configuration changed without migration")
    state.config_hash = config.identity
    if state.phase not in PHASES or state.schema_version != 1:
        raise EpisodeInputError("invalid episode state")
    _validate_accounting(config, state)
    if market.timestamp < state.timestamp:
        raise EpisodeInputError("out-of-order market observation")
    if market.valid:
        state.last_valid_bid = market.bid
    global_rebound_setup = bool(
        config.scout_rebound_confirmation
        and state.episode_id
        and state.base == 0
        and state.gross_spent == 0
        and state.setup_level == 0
    )
    recovery_rebound_setup = bool(
        config.recovery_rebound_scout
        and state.episode_id
        and state.base == 0
        and state.gross_spent == 0
        and state.setup_level == 0
    )
    scout_probe_setup = bool(
        config.scout_probe_rebound
        and state.episode_id
        and state.scout_plan_quote > ZERO
        and state.scout_probe_request_id
    )
    setup_context_reset = bool(
        (global_rebound_setup or recovery_rebound_setup)
        and (
            not market.valid
            or market.cascade
            or market.hostile
            or market.reset
            or not market.buy_allowed
            or (global_rebound_setup and market.recovery)
            or (state.timestamp >= 0 and market.timestamp > state.timestamp + config.interval_seconds)
        )
    )
    reset_setup = bool(
        not market.valid
        or market.cascade
        or market.hostile
        or not market.buy_allowed
        or (market.recovery and not recovery_rebound_setup)
        or (state.timestamp >= 0 and market.timestamp > state.timestamp + config.interval_seconds)
        or (scout_probe_setup and market.reset)
    )
    if reset_setup:
        if scout_probe_setup:
            _clear_scout_probe(state)
        else:
            state.setup_level, state.no_new_low = -1, 0
            state.setup_confirmation_start = 0
    for update in updates:
        _apply_update(config, state, market, update)
    if setup_context_reset:
        state = _abandon_unfunded_episode(config, state, market)
        if recovery_rebound_setup:
            return Transition(
                state,
                reason=(
                    "data_unavailable"
                    if not market.valid
                    else "recovery_rebound_abandoned"
                ),
            )
    pending = [order for order in state.orders.values() if not order.done]
    cancellations = _pending_cancellations(config, pending, market)
    if state.fault or remaining_budget(config, state) < 0:
        state.fault = state.fault or "episode_budget_overrun"
        return Transition(
            state,
            cancel_ids=_mark_termination(pending, "HARD_RAIL"),
            reason=state.fault,
        )
    if not market.valid:
        return Transition(state, cancel_ids=cancellations, reason="data_unavailable")
    if market.timestamp == state.timestamp:
        return Transition(state, cancel_ids=cancellations, reason="same_candle")
    state.timestamp = market.timestamp
    if (
        config.hostile_risk_latch
        and state.episode_id
        and state.base
        and (market.hostile or market.reset)
    ):
        state.adverse_entry_latched = True
    elif state.adverse_entry_latched and market.hourly_recovered:
        state.adverse_entry_latched = False
    probe_remainder = _scout_probe_remainder(config, state, market)
    if probe_remainder is not None:
        return probe_remainder
    fallback = _adaptive_reference_fallback(config, state, market)
    if fallback is not None:
        return fallback
    if state.phase == "COOLDOWN":
        if state.base > 0:
            state.phase, state.target_base = "EXIT", ZERO
            if pending:
                return Transition(
                    state,
                    cancel_ids=_mark_termination(pending, "LIFECYCLE"),
                    reason="late_fill_pending",
                )
            return _sell(config, state, market)
        if config.mode == "buy_sell" and market.reset and market.htf_timestamp > state.cooldown_htf_timestamp:
            state = EpisodeState(
                config_hash=config.identity,
                episode_number=state.episode_number,
                timestamp=market.timestamp,
                assignment_consumed=state.assignment_consumed,
                orders=state.orders,
            )
            return Transition(state, reason="cooldown_reset")
        return Transition(state, reason="cooldown")
    if config.mode == "sell_only" and not state.assignment_consumed:
        if market.base_available < config.initial_base:
            return Transition(state, reason="assignment_balance_shortfall")
        _start_episode(config, state, market)
        state.base = config.initial_base
        state.cost = config.initial_cost or ZERO
        state.cost_known = config.initial_cost is not None
        state.gross_spent = config.initial_cost or ZERO
        state.assignment_consumed = True
        state.phase = "RECOVER"
    completed = _finish_if_flat(config, state, market)
    if completed is not None:
        return completed
    recovery_trail = state.phase == "RECOVER" and state.exit_intent == "RECOVERY_TAKE_PROFIT"
    if (
        state.phase == "DISTRIBUTE"
        and state.exit_intent
        in {"RECOVERY_CAPITAL_RELEASE", "RECOVERY_STRENGTH_RELEASE"}
        and not pending
        and _floor(max(ZERO, state.base - state.target_base), market.amount_step) == ZERO
    ):
        state.exit_intent = "DISTRIBUTION_SELL"
    if state.phase in ("MARKUP", "DISTRIBUTE", "EXIT") or recovery_trail:
        distance = _trail_distance(config, market)
        state.peak = max(state.peak, market.bid)
        state.floor = max(state.floor, state.peak * (ONE - distance))
        equity = state.sell_proceeds + state.base * market.bid * (ONE - config.fee_rate) - state.gross_spent
        state.equity_peak = max(state.equity_peak if state.equity_peak is not None else equity, equity)
        floor = state.equity_peak - state.base * state.peak * distance
        state.equity_floor = max(state.equity_floor if state.equity_floor is not None else floor, floor)
        price_breached = market.bid <= state.floor
        equity_breached = equity <= state.equity_floor
        trail_breach = _exit_trail_breach(price_breached=price_breached, equity_breached=equity_breached)
        risk_clear = not (market.cascade or market.hostile or market.reset)
        entering_exit = state.phase != "EXIT"
        distribution_extension = (
            (state.peak - state.distribution_basis) / state.distribution_atr
            if not recovery_trail
            else None
        )
        bounded_runner_eligible = (
            not config.bounded_markup_runner
            or (
                distribution_extension is not None
                and distribution_extension >= ZERO
                and distribution_extension < config.distribution_atr_levels[-1]
            )
        )
        hold_structure = (
            config.structure_held_runner
            and state.phase != "EXIT"
            and not recovery_trail
            and trail_breach != "NONE"
            and market.hourly_recovered
            and risk_clear
            and bounded_runner_eligible
            and (not config.markup_release_runner or market.markup)
        )
        markup_release = (
            config.markup_release_runner
            and state.phase != "EXIT"
            and not recovery_trail
            and not market.markup
            and (state.structure_runner_active or trail_breach != "NONE")
        )
        force_structure_exit = (
            config.structure_held_runner
            and state.phase != "EXIT"
            and not recovery_trail
            and (
                not risk_clear
                or (trail_breach != "NONE" and not market.hourly_recovered)
                or markup_release
            )
        )
        exit_breach = trail_breach
        structure_runner_distribution = False
        if hold_structure:
            state.exit_intent = "DISTRIBUTION_SELL"
            state.phase = "DISTRIBUTE"
            _reduce_exit_target(config, state, state.distribution_start_base * config.remaining_fractions[-1])
            state.structure_runner_active = True
            state.last_exit_trail_breach = trail_breach
            state.last_exit_hourly_recovered = market.hourly_recovered
            state.last_exit_risk_clear = risk_clear
            structure_runner_distribution = True
        elif trail_breach != "NONE" or force_structure_exit:
            if state.phase != "EXIT" and not recovery_trail:
                state.exit_intent = "TRAILING_PROFIT_EXIT"
            state.phase = "EXIT"
            _reduce_exit_target(config, state, ZERO)
            if config.structure_held_runner and not recovery_trail and entering_exit:
                state.structure_runner_active = False
                if not risk_clear:
                    exit_breach = "RISK_OVERRIDE"
                elif markup_release:
                    exit_breach = "MARKUP_LOSS"
                else:
                    exit_breach = trail_breach
                state.last_exit_trail_breach = exit_breach
                state.last_exit_hourly_recovered = market.hourly_recovered
                state.last_exit_risk_clear = risk_clear
        elif state.phase != "EXIT" and not recovery_trail:
            if distribution_extension is None:
                raise EpisodeInputError("distribution extension unavailable")
            extension = distribution_extension
            thin_scout = (
                config.thin_scout_release
                and config.mode == "buy_sell"
                and state.target_base == state.distribution_start_base
                and extension >= config.distribution_atr_levels[0]
                and acquisition_levels(state) == (0,)
            )
            if thin_scout:
                episode_amount = _floor(state.base, market.amount_step)
                wallet_amount = _floor(market.base_available, market.amount_step)
                if wallet_amount < episode_amount:
                    return Transition(state, reason="inventory_balance_shortfall")
                previous_revision = state.exit_target_revision
                state.exit_intent = "THIN_SCOUT_RELEASE"
                state.phase = "EXIT"
                _reduce_exit_target(config, state, ZERO)
                if state.exit_target_revision == previous_revision:
                    state.exit_target_revision += 1
            else:
                for threshold, fraction in zip(config.distribution_atr_levels, config.remaining_fractions):
                    if extension >= threshold:
                        _reduce_exit_target(config, state, state.distribution_start_base * fraction)
                        state.phase = "DISTRIBUTE"
        if pending:
            if state.phase == "EXIT":
                cancellations = _mark_termination(pending, "LIFECYCLE")
            return Transition(state, cancel_ids=cancellations, reason="pending_execution")
        if state.base > state.target_base:
            return _sell(
                config,
                state,
                market,
                trail_breach=exit_breach,
                reason="structure_runner_distribution" if structure_runner_distribution else None,
            )
        return Transition(state, reason="recovery_runner" if recovery_trail else "runner")
    if pending:
        return Transition(state, cancel_ids=cancellations, reason="pending_execution")
    # Available balance can exclude other reservations; never adopt the surplus.
    if state.base > market.base_available:
        return Transition(state, reason="inventory_balance_shortfall")
    if state.base and market.markup:
        origin_phase = state.phase
        if config.require_profit and (
            not state.cost_known
            or market.bid * (ONE - config.fee_rate) * state.base < state.cost * (ONE + config.min_profit)
        ):
            return Transition(state, reason="markup_profit_guard")
        state.distribution_start_base = state.target_base = state.base
        state.distribution_basis = state.cost / state.base if state.cost_known else market.bid
        state.distribution_atr = market.atr_1h
        state.peak = market.bid
        state.floor = ZERO
        state.exit_intent = None
        state.structure_runner_active = False
        state.exit_target_revision = 0
        state.last_exit_trail_breach = "NONE"
        state.last_exit_hourly_recovered = False
        state.last_exit_risk_clear = False
        profit_floor = minimum_profit_price(config, state)
        weak_extension = (
            (state.peak - state.distribution_basis) / state.distribution_atr
            < config.distribution_atr_levels[0]
        )
        attributed_deep_inventory = (
            len(config.level_fractions) - 1 in acquisition_levels(state)
            or (config.mode == "sell_only" and state.assignment_consumed)
        )
        if (
            config.deep_recovery_capital_release
            and origin_phase in {"RECOVER", "DEFENSIVE", "EXHAUSTED"}
            and attributed_deep_inventory
            and profit_floor is not None
            and market.bid >= profit_floor
            and weak_extension
        ):
            rotation_exit = config.deep_recovery_rotation_exit
            strength_runner = config.deep_recovery_strength_runner
            extension = max(
                ZERO,
                (state.peak - state.distribution_basis) / state.distribution_atr,
            )
            recovery_strength = min(
                ONE,
                extension / config.distribution_atr_levels[0],
            )
            if rotation_exit:
                state.phase = "EXIT"
                state.exit_intent = "RECOVERY_ROTATION_EXIT"
                target = ZERO
                reason = "deep_recovery_rotation_exit"
            elif strength_runner:
                state.phase = "DISTRIBUTE"
                state.exit_intent = "RECOVERY_STRENGTH_RELEASE"
                target = (
                    state.distribution_start_base
                    * config.remaining_fractions[-1]
                    * recovery_strength
                )
                reason = "deep_recovery_strength_runner"
            else:
                state.phase = "DISTRIBUTE"
                state.exit_intent = "RECOVERY_CAPITAL_RELEASE"
                target = (
                    state.distribution_start_base * config.remaining_fractions[-1]
                )
                reason = "deep_recovery_capital_release"
            _reduce_exit_target(config, state, target)
            if state.exit_target_revision == 0:
                state.exit_target_revision = 1
            return _sell(
                config,
                state,
                market,
                intent=state.exit_intent,
                reason=reason,
            )
        state.phase = "MARKUP"
        return Transition(state, reason="markup_armed")
    if market.cascade or (market.hostile and state.base > 0):
        if state.base:
            state.phase = "DEFENSIVE"
        return Transition(state, reason="defensive")
    if state.base and market.recovery:
        state.phase = "RECOVER"
        if config.recovery_exit_enabled:
            profit_floor = minimum_profit_price(config, state)
            if profit_floor is None or market.bid < profit_floor:
                return Transition(state, reason="recovery_profit_guard")
            _arm_recovery_trail(config, state, market)
            return Transition(state, reason="recovery_exit_armed")
        return Transition(state, reason="recovering")
    if config.mode == "sell_only":
        return Transition(state, reason="sell_only_observe")
    if not market.buy_allowed:
        return Transition(state, reason="buy_veto")
    if (
        config.scout_rebound_confirmation
        and state.base == 0
        and (market.hostile or market.reset or market.recovery or market.cascade)
    ):
        return Transition(state, reason="scout_rebound_context_blocked")
    if not state.episode_id:
        if not market.entry_signal or market.ask > market.anchor - config.dip_atr_levels[0] * market.atr_1h:
            return Transition(state, reason="need_dislocation")
        if config.recovery_aware_scout and market.recovery:
            return Transition(state, reason="recovery_scout_deferred")
        _start_episode(config, state, market)
        if config.recovery_rebound_scout and market.recovery:
            state.setup_level = 0
            state.setup_low = market.low
            state.setup_start = market.timestamp
            state.setup_confirmation_start = 0
            state.no_new_low = 0
            state.phase = "WATCH"
            return Transition(state, reason="recovery_rebound_armed")
    elif config.adaptive_entry_pricing and state.gross_spent == 0 and state.levels[0] == "OPEN" and market.entry_signal:
        # A fresh signal after a zero-fill expiry gets fresh causal levels while
        # retaining the original episode/order identity for late-fill safety.
        state.frozen_anchor = market.anchor
        state.frozen_atr = market.atr_1h
    setup_receipt = None
    recovery_rebound_setup = bool(
        config.recovery_rebound_scout
        and state.gross_spent == 0
        and state.levels[0] == "OPEN"
        and state.setup_level == 0
    )
    if recovery_rebound_setup:
        if market.low < state.setup_low:
            state.setup_low = market.low
            state.no_new_low = 0
        if market.recovery:
            state.setup_confirmation_start = 0
            state.no_new_low = 0
            return Transition(state, reason="recovery_rebound_wait")
        if state.setup_confirmation_start == 0:
            state.setup_confirmation_start = market.timestamp
            state.no_new_low = 0
            return Transition(state, reason="recovery_rebound_cleared")
        if (
            market.timestamp - state.setup_confirmation_start
            >= config.setup_bars * config.interval_seconds
        ):
            return Transition(
                _abandon_unfunded_episode(config, state, market),
                reason="recovery_rebound_expired",
            )
        if market.low < state.setup_low:
            state.setup_low, state.no_new_low = market.low, 0
        else:
            state.no_new_low += 1
        rebound = max(config.rebound_atr * market.atr_1m, 2 * (market.ask - market.bid))
        if state.no_new_low < config.stabilization_bars or market.ask < state.setup_low + rebound:
            return Transition(state, reason="need_recovery_rebound")
        level = 0
        setup_receipt = {
            "setup_policy": "RECOVERY_REBOUND_SCOUT_V1",
            "setup_started_at": state.setup_start,
            "setup_confirmation_started_at": state.setup_confirmation_start,
            "setup_low": state.setup_low,
            "setup_rebound": rebound,
            "setup_no_new_low": state.no_new_low,
        }
    elif config.scout_rebound_confirmation and state.gross_spent == 0 and state.levels[0] == "OPEN":
        if state.setup_level != 0:
            if not market.entry_signal:
                return Transition(state, reason="need_entry_confirmation")
            state.setup_level = 0
            state.setup_low = market.low
            state.setup_start = market.timestamp
            state.no_new_low = 0
            state.phase = "WATCH"
            return Transition(state, reason="scout_rebound_armed")
        if market.timestamp - state.setup_start >= config.setup_bars * config.interval_seconds:
            return Transition(
                _abandon_unfunded_episode(config, state, market),
                reason="scout_rebound_expired",
            )
        if market.low < state.setup_low:
            state.setup_low, state.no_new_low = market.low, 0
        else:
            state.no_new_low += 1
        rebound = max(config.rebound_atr * market.atr_1m, 2 * (market.ask - market.bid))
        if state.no_new_low < config.stabilization_bars or market.ask < state.setup_low + rebound:
            return Transition(state, reason="need_scout_rebound")
        level = 0
        setup_receipt = {
            "setup_policy": "SCOUT_REBOUND_V1",
            "setup_started_at": state.setup_start,
            "setup_low": state.setup_low,
            "setup_rebound": rebound,
            "setup_no_new_low": state.no_new_low,
        }
    else:
        open_levels = [index for index, value in enumerate(state.levels) if value == "OPEN"]
        if not open_levels or remaining_budget(config, state) <= 0:
            state.phase = "EXHAUSTED"
            return Transition(state, reason="exhausted")
        eligible = [
            index
            for index in open_levels
            if market.ask <= state.frozen_anchor - config.dip_atr_levels[index] * state.frozen_atr
        ]
        if not eligible:
            state.phase = "ACCUMULATE" if state.base else "WATCH"
            return Transition(state, reason="need_deeper_level")
        # First purchase is always the scout; a gap cannot multiply initial risk.
        level = 0 if state.gross_spent == 0 and state.levels[0] == "OPEN" else (
            min(eligible) if config.sequential_scale_ins else max(eligible)
        )
        if level not in eligible:
            return Transition(state, reason="need_scout_level")
    if config.recovery_aware_scout and level == 0:
        setup_receipt = {"setup_policy": "RECOVERY_AWARE_SCOUT_V1"}
    elif config.recovery_rebound_scout and level == 0 and setup_receipt is None:
        setup_receipt = {"setup_policy": "RECOVERY_REBOUND_SCOUT_V1"}
    factor = (
        config.hostile_scout_fraction
        if market.hostile or (level == 0 and state.adverse_entry_latched)
        else ONE
    ) * market.quality_factor
    factor *= min(ONE, state.frozen_atr / market.atr_1h)
    if (market.hostile and level > 0) or factor == 0:
        return Transition(state, reason="zero_allocation" if factor == 0 else "regime_veto")
    if level == 0:
        if setup_receipt is None and not market.entry_signal:
            return Transition(state, reason="need_entry_confirmation")
    else:
        if state.adverse_entry_latched:
            return Transition(state, reason="adverse_regime_latch")
        if state.setup_level != level:
            state.setup_level, state.setup_low, state.setup_start, state.no_new_low = (
                level,
                market.low,
                market.timestamp,
                0,
            )
            return Transition(state, reason="level_armed")
        if market.timestamp - state.setup_start >= config.setup_bars * config.interval_seconds:
            state.setup_level = -1
            if config.sequential_scale_ins:
                return Transition(state, reason="setup_expired_rearm")
            state.levels[level] = "SKIPPED"
            return Transition(state, reason="setup_expired")
        if market.low < state.setup_low:
            state.setup_low, state.no_new_low = market.low, 0
        else:
            state.no_new_low += 1
        rebound = max(config.rebound_atr * market.atr_1m, 2 * (market.ask - market.bid))
        if state.no_new_low < config.stabilization_bars or market.ask < state.setup_low + rebound:
            return Transition(state, reason="need_stabilization")
        if config.require_scale_in_signal and not market.entry_signal:
            return Transition(state, reason="need_scale_in_signal")
        if market.ask > state.last_fill_price - config.last_fill_gap_atr * market.atr_1m:
            return Transition(state, reason="last_fill_gap")
    full_cap = config.budget_quote * config.level_fractions[level] * factor
    if full_cap <= 0:
        return Transition(state, reason="zero_allocation")
    if full_cap > remaining_budget(config, state) or full_cap > market.quote_available:
        return Transition(state, reason="insufficient_episode_cash")
    probe_plan = bool(
        config.scout_probe_rebound
        and level == 0
        and state.gross_spent == ZERO
    )
    cap = full_cap * config.scout_probe_fraction if probe_plan else full_cap
    if probe_plan:
        setup_receipt = {
            "setup_policy": "SCOUT_PROBE_REBOUND_V1",
            "setup_started_at": market.timestamp,
            "setup_low": market.low,
            "setup_stage": "PROBE",
            "setup_plan_quote": full_cap,
        }
    entry_price_selection = _entry_price_selection(config, market)
    price = entry_price_selection.price
    if price <= 0:
        return Transition(state, reason="price_below_tick")
    amount = _floor(cap / (price * (ONE + config.fee_rate)), market.amount_step)
    reserve = amount * price * (ONE + config.fee_rate)
    if amount <= 0 or amount < market.min_amount or amount * price < market.min_notional:
        return Transition(state, reason="below_exchange_minimum")
    if state.base * market.ask + amount * market.ask > config.mark_cap_quote:
        return Transition(state, reason="mark_exposure_cap")
    if state.base > 0:
        before = state.cost / state.base
        after = (state.cost + reserve) / (state.base + amount)
        if before - after < config.basis_improvement_atr * state.frozen_atr:
            return Transition(state, reason="insufficient_basis_improvement")
    if not config.sequential_scale_ins:
        for index in range(level):
            if state.levels[index] == "OPEN":
                state.levels[index] = "SKIPPED"
    state.levels[level] = "AUTHORIZED"
    state.setup_level = -1
    state.setup_confirmation_start = 0
    state.phase = "SCOUT" if level == 0 else "ACCUMULATE"
    request = _request(
        config,
        state,
        market,
        side="BUY",
        amount=amount,
        price=price,
        reserve=reserve,
        level=level,
        intent="SCOUT_BUY" if level == 0 else "SCALE_IN_BUY",
        entry_price_selection=entry_price_selection,
        setup_receipt=setup_receipt,
    )
    if probe_plan:
        state.scout_plan_quote = full_cap
        state.scout_probe_request_id = request.id
        state.setup_level = 0
        state.setup_low = market.low
        state.setup_start = market.timestamp
        state.no_new_low = 0
    return Transition(state, (request,), reason="scout" if level == 0 else "scale_in")
