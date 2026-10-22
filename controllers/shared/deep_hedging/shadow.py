"""Non-executing decisions from caller-supplied, owner-reconciled snapshots.

The caller must verify native receipts and aggregate the oldest source times.
A receipt string is a provenance reference, not proof of reconciliation. This
module deliberately does not implement a live owner snapshot adapter.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Context as DecimalContext, Decimal, localcontext
from fractions import Fraction
from typing import Mapping

from .policy import Context, FrozenBandPolicy, _hash, _number, _text


def _decimal(value: object, *, positive: bool = False) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or (positive and value <= 0):
        raise ValueError("expected finite Decimal" + (" > 0" if positive else ""))
    if len(value.as_tuple().digits) > 64 or abs(value.adjusted()) > 100:
        raise ValueError("Decimal exceeds bounded quantity representation")
    return value


@dataclass(frozen=True)
class Snapshot:
    context: Context
    owner_id: str
    snapshot_id: str
    sequence: int
    market_event_at: float
    market_available_at: float
    account_event_at: float
    account_available_at: float
    reconciliation_ref: str | None
    position: Decimal
    unresolved_orders: int
    values: Mapping[str, float]
    units: Mapping[str, str]


@dataclass(frozen=True)
class Decision:
    reason: str
    policy_sha256: str
    context: Context
    owner_id: str
    snapshot_id: str | None
    target: Decimal | None = None
    delta: Decimal | None = None
    band: tuple[float, float] | None = None
    valid_until: float | None = None

    @property
    def execution_permitted(self) -> bool:
        return False

    @property
    def executor_actions(self) -> tuple:
        return ()

    def to_record(self) -> dict:
        result = asdict(self)
        result.update(schema="deep_hedging.shadow_decision.v1", authority="NONE", execution_permitted=False)
        result["target"] = None if self.target is None else str(self.target)
        result["delta"] = None if self.delta is None else str(self.delta)
        return result


class ShadowController:
    """Feed-forward advisory controller, intentionally not a deployable ControllerBase."""

    def __init__(self, policy: FrozenBandPolicy, *, expected_owner: str, max_age: float,
                 decision_ttl: float, max_change: Decimal, lot_size: Decimal):
        self.policy = policy
        self.expected_owner = _text(expected_owner)
        self.max_age, self.decision_ttl = _number(max_age), _number(decision_ttl)
        self.max_change, self.lot_size = _decimal(max_change, positive=True), _decimal(lot_size, positive=True)
        if self.max_age <= 0 or self.decision_ttl <= 0:
            raise ValueError("freshness limits must be positive")
        self._last_sequence = -1
        self._last_time = -1.0

    def advise(self, state: Snapshot, *, now: float) -> Decision:
        def held(reason: str) -> Decision:
            try:
                snapshot_id = _hash(state.snapshot_id)
            except ValueError:
                snapshot_id = None
            return Decision(reason, self.policy.sha256, self.policy.context, self.expected_owner, snapshot_id)

        try:
            now = _number(now)
            _decimal(state.position)
            _hash(state.snapshot_id)
            if type(state.sequence) is not int or state.sequence < 0:
                raise ValueError("invalid sequence")
            if type(state.unresolved_orders) is not int or state.unresolved_orders < 0:
                raise ValueError("invalid order count")
            times = tuple(_number(t) for t in (state.market_event_at, state.market_available_at,
                                               state.account_event_at, state.account_available_at))
            if now < 0 or min(times) < 0 or max(now, *times) > 253402300799:
                raise ValueError("clock outside supported Unix timestamp range")
        except ValueError:
            return held("HOLD_INVALID_STATE")
        if state.owner_id != self.expected_owner:
            return held("HOLD_OWNER")
        if state.context != self.policy.context:
            return held("HOLD_CONTEXT")
        if now < self._last_time:
            return held("HOLD_CLOCK")
        if state.sequence <= self._last_sequence:
            return held("HOLD_SEQUENCE")
        if not (times[0] <= times[1] <= now and times[2] <= times[3] <= now):
            return held("HOLD_TIMING")
        if now - min(times[0], times[2]) >= self.max_age:
            return held("HOLD_STALE")
        try:
            _hash(state.reconciliation_ref)
        except ValueError:
            return held("HOLD_RECONCILIATION")
        if state.unresolved_orders:
            return held("HOLD_UNRESOLVED_ORDERS")
        lower_limit, upper_limit = (Decimal(str(x)) for x in self.policy.position_bounds)
        if not lower_limit <= state.position <= upper_limit:
            return held("HOLD_POSITION_LIMIT")
        try:
            band = self.policy.band(state.values, state.units)
        except (ValueError, TypeError, KeyError, OverflowError):
            return held("HOLD_FEATURES")
        lower, upper = (Decimal(str(x)) for x in band)
        desired = min(max(state.position, lower), upper)
        difference = Fraction(desired) - Fraction(state.position)
        cap = min(abs(difference), Fraction(self.max_change))
        lots = cap // Fraction(self.lot_size)
        with localcontext(DecimalContext(prec=512)):
            size = Decimal(lots) * self.lot_size
            delta = size if difference >= 0 else -size
            target = state.position + delta
        self._last_sequence, self._last_time = state.sequence, now
        return Decision("SHADOW_READY", self.policy.sha256, self.policy.context, self.expected_owner,
                        state.snapshot_id, target, delta, band,
                        min(now + self.decision_ttl, times[0] + self.max_age, times[2] + self.max_age))


def bounded_spot_entry(*, baseline_amount: Decimal, multiplier: Decimal, free_quote: Decimal,
                       unit_price: Decimal, cost_fraction: Decimal, max_increment: Decimal,
                       lot_size: Decimal, min_amount: Decimal, min_notional: Decimal,
                       eligible: bool, fresh: bool, reconciled: bool, cost_basis_valid: bool,
                       unresolved_orders: int) -> Decimal:
    """BUY-only proposal cap, not an admission or an order.

    free_quote excludes native reservations. max_increment is the independently
    computed portfolio/owner cap. The caller must bind the final amount before
    fingerprinting/reserving the executor configuration. No RSI hook is changed.
    """
    for value in (baseline_amount, multiplier, free_quote, cost_fraction, max_increment, min_amount, min_notional):
        if _decimal(value) < 0:
            raise ValueError("spot inputs cannot be negative")
    _decimal(unit_price, positive=True)
    _decimal(lot_size, positive=True)
    if multiplier > 1:
        raise ValueError("the overlay cannot increase the baseline amount")
    flags = (eligible, fresh, reconciled, cost_basis_valid)
    if any(type(flag) is not bool for flag in flags):
        raise ValueError("eligibility fields must be explicit booleans")
    if type(unresolved_orders) is not int or unresolved_orders < 0:
        raise ValueError("invalid unresolved order count")
    if not all(flags) or unresolved_orders:
        return Decimal(0)
    cap = min(Fraction(baseline_amount) * Fraction(multiplier), Fraction(max_increment),
              Fraction(free_quote) / (Fraction(unit_price) * (1 + Fraction(cost_fraction))))
    lots = cap // Fraction(lot_size)
    with localcontext(DecimalContext(prec=512)):
        amount = Decimal(lots) * lot_size
    notional = Fraction(amount) * Fraction(unit_price)
    return amount if amount >= min_amount and notional >= Fraction(min_notional) else Decimal(0)
