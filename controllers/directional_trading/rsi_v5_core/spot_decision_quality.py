from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from enum import Enum

_ZERO = Decimal("0")


@dataclass(frozen=True)
class DeferredScaleIn:
    program_id: str
    original_quantity: Decimal
    requested_event_id: str
    requested_bar_close_ms: int
    bar_interval_ms: int
    resolved: bool = False
    resolution: str | None = None

    @property
    def eligible_bar_close_ms(self) -> int:
        return self.requested_bar_close_ms + self.bar_interval_ms


@dataclass(frozen=True)
class OneShotHarvestState:
    program_id: str
    consumed: bool = False
    filled_quantity: Decimal = _ZERO
    submission_id: str | None = None
    submitted_quantity: Decimal = _ZERO
    processed_fill_event_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class SpotDecisionQualityProposal:
    side: str
    quantity: Decimal
    reason: str
    program_id: str


class D1ResolutionStatus(str, Enum):
    PENDING = "PENDING"
    PROPOSED = "PROPOSED"
    DISCARDED_NO_FRESH_INTENT = "DISCARDED_NO_FRESH_INTENT"
    DISCARDED_DECLINING_CLOSE = "DISCARDED_DECLINING_CLOSE"
    DISCARDED_PROTECTIVE_EXIT = "DISCARDED_PROTECTIVE_EXIT"
    DISCARDED_STALE = "DISCARDED_STALE"
    ALREADY_RESOLVED = "ALREADY_RESOLVED"


@dataclass(frozen=True)
class D1Resolution:
    deferred: DeferredScaleIn
    status: D1ResolutionStatus
    proposal: SpotDecisionQualityProposal | None = None


def _validate_deferred(deferred: DeferredScaleIn) -> None:
    if not deferred.program_id.strip() or not deferred.requested_event_id.strip():
        raise ValueError("D1 identities must be non-empty")
    if not deferred.original_quantity.is_finite() or deferred.original_quantity <= _ZERO:
        raise ValueError("D1 quantity must be finite and positive")
    for name, value in (
        ("requested bar close", deferred.requested_bar_close_ms),
        ("bar interval", deferred.bar_interval_ms),
    ):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"D1 {name} must be an integer millisecond value")
    if deferred.requested_bar_close_ms < 0 or deferred.bar_interval_ms <= 0:
        raise ValueError("D1 bar times must be non-negative with a positive interval")
    if deferred.resolved != (deferred.resolution is not None):
        raise ValueError("D1 resolution marker is inconsistent")


def _terminal_d1(
    deferred: DeferredScaleIn,
    status: D1ResolutionStatus,
    proposal: SpotDecisionQualityProposal | None = None,
) -> D1Resolution:
    return D1Resolution(
        deferred=replace(deferred, resolved=True, resolution=status.value),
        status=status,
        proposal=proposal,
    )


def evaluate_d1(
    *,
    deferred: DeferredScaleIn,
    evaluation_bar_close_ms: int,
    evaluation_bar_closed: bool,
    fresh_reference_quantity: Decimal | None,
    fresh_reference_intent: bool,
    previous_close: Decimal,
    latest_close: Decimal,
    protective_exit_pending: bool = False,
) -> D1Resolution:
    _validate_deferred(deferred)
    if isinstance(evaluation_bar_close_ms, bool) or not isinstance(
        evaluation_bar_close_ms, int
    ):
        raise ValueError("D1 evaluation bar close must be an integer millisecond value")
    if not isinstance(evaluation_bar_closed, bool):
        raise ValueError("D1 evaluation bar closed marker must be boolean")
    if not isinstance(fresh_reference_intent, bool) or not isinstance(
        protective_exit_pending, bool
    ):
        raise ValueError("D1 control inputs must be boolean")
    if deferred.resolved:
        return D1Resolution(deferred, D1ResolutionStatus.ALREADY_RESOLVED)

    values = (previous_close, latest_close)
    if any(not value.is_finite() for value in values):
        raise ValueError("D1 inputs must be finite")
    if previous_close <= _ZERO or latest_close <= _ZERO:
        raise ValueError("D1 closes must be positive")
    if fresh_reference_quantity is not None and (
        not fresh_reference_quantity.is_finite()
        or fresh_reference_quantity <= _ZERO
    ):
        raise ValueError("fresh reference quantity must be finite and positive")

    eligible_bar_close_ms = deferred.eligible_bar_close_ms
    if evaluation_bar_close_ms < eligible_bar_close_ms:
        if evaluation_bar_closed:
            raise ValueError("D1 evaluation bar precedes the eligible successor bar")
        return D1Resolution(deferred, D1ResolutionStatus.PENDING)
    if evaluation_bar_close_ms > eligible_bar_close_ms:
        if not evaluation_bar_closed:
            raise ValueError("D1 cannot evaluate an unclosed future bar")
        return _terminal_d1(deferred, D1ResolutionStatus.DISCARDED_STALE)
    if not evaluation_bar_closed:
        return D1Resolution(deferred, D1ResolutionStatus.PENDING)

    if protective_exit_pending:
        return _terminal_d1(
            deferred, D1ResolutionStatus.DISCARDED_PROTECTIVE_EXIT
        )
    if not fresh_reference_intent or fresh_reference_quantity is None:
        return _terminal_d1(
            deferred, D1ResolutionStatus.DISCARDED_NO_FRESH_INTENT
        )
    if latest_close < previous_close:
        return _terminal_d1(
            deferred, D1ResolutionStatus.DISCARDED_DECLINING_CLOSE
        )
    proposal = SpotDecisionQualityProposal(
        side="BUY",
        quantity=min(deferred.original_quantity, fresh_reference_quantity),
        reason="D1_DEFERRED_STABILIZED_ADD",
        program_id=deferred.program_id,
    )
    return _terminal_d1(deferred, D1ResolutionStatus.PROPOSED, proposal)


def evaluate_x1(
    *,
    state: OneShotHarvestState,
    eligible_unreserved_inventory: Decimal,
    activation_reached: bool,
    net_profitable: bool,
    lot_size: Decimal,
    tranche_fraction: Decimal = Decimal("0.25"),
    protective_exit_pending: bool = False,
) -> SpotDecisionQualityProposal | None:
    _validate_x1_state(state)
    if any(
        not isinstance(value, bool)
        for value in (activation_reached, net_profitable, protective_exit_pending)
    ):
        raise ValueError("X1 control inputs must be boolean")
    values = (eligible_unreserved_inventory, lot_size, tranche_fraction)
    if any(not value.is_finite() for value in values):
        raise ValueError("X1 inputs must be finite")
    if eligible_unreserved_inventory < _ZERO or lot_size <= _ZERO:
        raise ValueError("inventory must be non-negative and lot size positive")
    if tranche_fraction <= _ZERO or tranche_fraction > Decimal("1"):
        raise ValueError("tranche fraction must be in (0, 1]")
    if (
        state.consumed
        or protective_exit_pending
        or not activation_reached
        or not net_profitable
    ):
        return None
    raw = eligible_unreserved_inventory * tranche_fraction
    lots = (raw / lot_size).to_integral_value(rounding="ROUND_FLOOR")
    quantity = lots * lot_size
    if quantity <= _ZERO:
        return None
    return SpotDecisionQualityProposal(
        "SELL", quantity, "X1_ONE_SHOT_PROFIT_HARVEST", state.program_id
    )


def _validate_identity(value: str | None, *, name: str) -> str:
    if value is None or not isinstance(value, str) or value != value.strip() or not value:
        raise ValueError(f"X1 {name} must be a non-empty canonical string")
    return value


def _validate_x1_state(state: OneShotHarvestState) -> None:
    _validate_identity(state.program_id, name="program identity")
    if not isinstance(state.consumed, bool):
        raise ValueError("X1 consumed marker must be boolean")
    quantities = (state.filled_quantity, state.submitted_quantity)
    if any(not isinstance(value, Decimal) or not value.is_finite() for value in quantities):
        raise ValueError("X1 state quantities must be finite Decimals")
    if state.filled_quantity < _ZERO or state.submitted_quantity < _ZERO:
        raise ValueError("X1 state quantities must be non-negative")
    if not state.consumed:
        if (
            state.submission_id is not None
            or state.submitted_quantity != _ZERO
            or state.filled_quantity != _ZERO
            or state.processed_fill_event_ids
        ):
            raise ValueError("unconsumed X1 state cannot contain submission or fill state")
        return
    _validate_identity(state.submission_id, name="submission identity")
    if state.submitted_quantity <= _ZERO:
        raise ValueError("consumed X1 state requires positive submitted quantity")
    if state.filled_quantity > state.submitted_quantity:
        raise ValueError("X1 filled quantity exceeds submitted quantity")
    if not isinstance(state.processed_fill_event_ids, tuple):
        raise ValueError("X1 processed fill identities must be a tuple")
    if len(set(state.processed_fill_event_ids)) != len(state.processed_fill_event_ids):
        raise ValueError("X1 processed fill identities must be unique")
    for event_id in state.processed_fill_event_ids:
        _validate_identity(event_id, name="fill event identity")


def bind_x1_submission(
    state: OneShotHarvestState,
    *,
    proposal: SpotDecisionQualityProposal,
    submission_id: str,
) -> OneShotHarvestState:
    _validate_x1_state(state)
    _validate_identity(submission_id, name="submission identity")
    if state.consumed:
        raise ValueError("X1 tranche already has a bound submission")
    if (
        proposal.program_id != state.program_id
        or proposal.side != "SELL"
        or proposal.reason != "X1_ONE_SHOT_PROFIT_HARVEST"
    ):
        raise ValueError("X1 proposal does not match harvest state")
    if not isinstance(proposal.quantity, Decimal) or not proposal.quantity.is_finite() or proposal.quantity <= _ZERO:
        raise ValueError("X1 proposal quantity must be a finite positive Decimal")
    return replace(
        state,
        consumed=True,
        submission_id=submission_id,
        submitted_quantity=proposal.quantity,
    )


def apply_x1_fill(
    state: OneShotHarvestState,
    *,
    submission_id: str,
    fill_event_id: str,
    fill_quantity: Decimal,
) -> OneShotHarvestState:
    _validate_x1_state(state)
    _validate_identity(submission_id, name="submission identity")
    _validate_identity(fill_event_id, name="fill event identity")
    if not isinstance(fill_quantity, Decimal) or not fill_quantity.is_finite() or fill_quantity <= _ZERO:
        raise ValueError("fill quantity must be a finite positive Decimal")
    if not state.consumed or state.submission_id is None:
        raise ValueError("X1 fill is not bound to a submitted harvest order")
    if submission_id != state.submission_id:
        raise ValueError("X1 fill submission does not match bound order")
    if fill_event_id in state.processed_fill_event_ids:
        raise ValueError("X1 fill event was already processed")
    filled_quantity = state.filled_quantity + fill_quantity
    if filled_quantity > state.submitted_quantity:
        raise ValueError("X1 accumulated fill exceeds submitted quantity")
    return replace(
        state,
        filled_quantity=filled_quantity,
        processed_fill_event_ids=state.processed_fill_event_ids + (fill_event_id,),
    )
