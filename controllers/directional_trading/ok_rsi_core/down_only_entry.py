from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from enum import Enum


class DownOnlyMakerInputError(ValueError):
    """Raised when a policy decision receives unsafe or inconsistent input."""


class DownOnlyMakerAction(str, Enum):
    HOLD = "hold"
    CANCEL_FOR_REPRICE = "cancel_for_reprice"
    CANCEL_TERMINAL = "cancel_terminal"


class DownOnlyMakerReason(str, Enum):
    RESTING = "resting"
    PARTIAL_FILL_RESTING = "partial_fill_resting"
    DOWNWARD_REPRICE = "downward_reprice"
    REST_LEASE_EXPIRED = "rest_lease_expired"
    TOTAL_AGE_EXPIRED = "total_age_expired"
    DOWNWARD_BOUND_BREACHED = "downward_bound_breached"
    OPPOSING_SIGNAL = "opposing_signal"
    RISK_BLOCKED = "risk_blocked"
    REPRICE_BUDGET_EXHAUSTED = "reprice_budget_exhausted"


@dataclass(frozen=True)
class DownOnlyMakerParameters:
    rest_seconds: int
    total_max_age_seconds: int
    max_reprices: int
    cooldown_seconds: int
    min_ticks: int
    atr_fraction: Decimal
    max_downward_atr: Decimal


@dataclass(frozen=True)
class DownOnlyMakerSnapshot:
    now: Decimal
    intent_created_at: Decimal
    order_created_at: Decimal
    last_reprice_at: Decimal | None
    initial_best_bid: Decimal
    initial_best_ask: Decimal
    best_bid: Decimal
    best_ask: Decimal
    current_order_price: Decimal
    decision_atr_1m: Decimal
    price_step: Decimal
    maximum_acceptable_price: Decimal
    authorized_base: Decimal
    cumulative_filled_base: Decimal
    reprice_count: int
    opposing_signal: bool
    risk_allowed: bool


@dataclass(frozen=True)
class DownOnlyMakerDecision:
    action: DownOnlyMakerAction
    reason: DownOnlyMakerReason
    target_price: Decimal | None = None


def _positive_finite(value: Decimal, name: str) -> None:
    if not value.is_finite() or value <= 0:
        raise DownOnlyMakerInputError(f"{name} must be positive and finite")


def _non_negative_finite(value: Decimal, name: str) -> None:
    if not value.is_finite() or value < 0:
        raise DownOnlyMakerInputError(f"{name} must be non-negative and finite")


def _validate(parameters: DownOnlyMakerParameters, snapshot: DownOnlyMakerSnapshot) -> None:
    for name in ("rest_seconds", "total_max_age_seconds", "min_ticks"):
        value = getattr(parameters, name)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise DownOnlyMakerInputError(f"{name} must be a positive integer")
    for name in ("max_reprices", "cooldown_seconds"):
        value = getattr(parameters, name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise DownOnlyMakerInputError(f"{name} must be a non-negative integer")
    _positive_finite(parameters.atr_fraction, "atr_fraction")
    _positive_finite(parameters.max_downward_atr, "max_downward_atr")

    for name in (
        "initial_best_bid",
        "initial_best_ask",
        "best_bid",
        "best_ask",
        "current_order_price",
        "decision_atr_1m",
        "price_step",
        "maximum_acceptable_price",
        "authorized_base",
    ):
        _positive_finite(getattr(snapshot, name), name)
    for name in ("now", "intent_created_at", "order_created_at", "cumulative_filled_base"):
        _non_negative_finite(getattr(snapshot, name), name)
    if snapshot.last_reprice_at is not None:
        _non_negative_finite(snapshot.last_reprice_at, "last_reprice_at")
    if snapshot.best_bid > snapshot.best_ask:
        raise DownOnlyMakerInputError("crossed book: best_bid exceeds best_ask")
    if snapshot.initial_best_bid > snapshot.initial_best_ask:
        raise DownOnlyMakerInputError("crossed book: initial_best_bid exceeds initial_best_ask")
    if snapshot.now < snapshot.intent_created_at:
        raise DownOnlyMakerInputError("intent age cannot be negative")
    if snapshot.order_created_at < snapshot.intent_created_at or snapshot.now < snapshot.order_created_at:
        raise DownOnlyMakerInputError("order age cannot be negative or precede the intent")
    if snapshot.last_reprice_at is not None:
        if snapshot.last_reprice_at < snapshot.intent_created_at or snapshot.last_reprice_at > snapshot.now:
            raise DownOnlyMakerInputError("last_reprice_at must be within the intent lifetime")
    if isinstance(snapshot.reprice_count, bool) or not isinstance(snapshot.reprice_count, int) or snapshot.reprice_count < 0:
        raise DownOnlyMakerInputError("reprice_count must be a non-negative integer")
    if snapshot.cumulative_filled_base > snapshot.authorized_base:
        raise DownOnlyMakerInputError("cumulative fill exceeds authorized_base")
    if snapshot.current_order_price > snapshot.maximum_acceptable_price:
        raise DownOnlyMakerInputError("current order price exceeds maximum acceptable price")
    if not isinstance(snapshot.opposing_signal, bool) or not isinstance(snapshot.risk_allowed, bool):
        raise DownOnlyMakerInputError("signal and risk flags must be booleans")


def decide_down_only_maker(
    parameters: DownOnlyMakerParameters,
    snapshot: DownOnlyMakerSnapshot,
) -> DownOnlyMakerDecision:
    """Return a deterministic action for one maker-only buy-entry observation."""
    _validate(parameters, snapshot)

    if not snapshot.risk_allowed:
        return DownOnlyMakerDecision(DownOnlyMakerAction.CANCEL_TERMINAL, DownOnlyMakerReason.RISK_BLOCKED)
    if snapshot.opposing_signal:
        return DownOnlyMakerDecision(DownOnlyMakerAction.CANCEL_TERMINAL, DownOnlyMakerReason.OPPOSING_SIGNAL)

    intent_age = snapshot.now - snapshot.intent_created_at
    if intent_age >= Decimal(parameters.total_max_age_seconds):
        return DownOnlyMakerDecision(DownOnlyMakerAction.CANCEL_TERMINAL, DownOnlyMakerReason.TOTAL_AGE_EXPIRED)

    downward_floor = snapshot.initial_best_bid - parameters.max_downward_atr * snapshot.decision_atr_1m
    if snapshot.best_bid < downward_floor:
        return DownOnlyMakerDecision(
            DownOnlyMakerAction.CANCEL_TERMINAL,
            DownOnlyMakerReason.DOWNWARD_BOUND_BREACHED,
        )

    order_age = snapshot.now - snapshot.order_created_at
    if snapshot.cumulative_filled_base > 0:
        if order_age >= Decimal(parameters.rest_seconds):
            return DownOnlyMakerDecision(
                DownOnlyMakerAction.CANCEL_TERMINAL,
                DownOnlyMakerReason.REST_LEASE_EXPIRED,
            )
        return DownOnlyMakerDecision(DownOnlyMakerAction.HOLD, DownOnlyMakerReason.PARTIAL_FILL_RESTING)

    decision_spread = snapshot.initial_best_ask - snapshot.initial_best_bid
    trigger = max(
        Decimal(parameters.min_ticks) * snapshot.price_step,
        decision_spread,
        parameters.atr_fraction * snapshot.decision_atr_1m,
    )
    target = min(snapshot.best_bid, snapshot.maximum_acceptable_price)
    target = (target / snapshot.price_step).to_integral_value(rounding=ROUND_FLOOR) * snapshot.price_step
    qualifying_decline = target > 0 and snapshot.current_order_price - target >= trigger
    cooldown_anchor = snapshot.last_reprice_at if snapshot.last_reprice_at is not None else snapshot.intent_created_at
    cooldown_elapsed = snapshot.now - cooldown_anchor >= Decimal(parameters.cooldown_seconds)

    if qualifying_decline and snapshot.reprice_count >= parameters.max_reprices:
        return DownOnlyMakerDecision(
            DownOnlyMakerAction.CANCEL_TERMINAL,
            DownOnlyMakerReason.REPRICE_BUDGET_EXHAUSTED,
        )
    if qualifying_decline and cooldown_elapsed:
        return DownOnlyMakerDecision(
            DownOnlyMakerAction.CANCEL_FOR_REPRICE,
            DownOnlyMakerReason.DOWNWARD_REPRICE,
            target,
        )
    if order_age >= Decimal(parameters.rest_seconds):
        return DownOnlyMakerDecision(
            DownOnlyMakerAction.CANCEL_TERMINAL,
            DownOnlyMakerReason.REST_LEASE_EXPIRED,
        )
    return DownOnlyMakerDecision(DownOnlyMakerAction.HOLD, DownOnlyMakerReason.RESTING)
