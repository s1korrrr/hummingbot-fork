from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

_ZERO = Decimal("0")


@dataclass(frozen=True)
class DeferredScaleIn:
    program_id: str
    original_quantity: Decimal
    requested_event_id: str


@dataclass(frozen=True)
class OneShotHarvestState:
    program_id: str
    consumed: bool = False
    filled_quantity: Decimal = _ZERO


@dataclass(frozen=True)
class SpotDecisionQualityProposal:
    side: str
    quantity: Decimal
    reason: str
    program_id: str


def evaluate_d1(
    *,
    deferred: DeferredScaleIn,
    fresh_reference_quantity: Decimal | None,
    fresh_reference_intent: bool,
    previous_close: Decimal,
    latest_close: Decimal,
) -> SpotDecisionQualityProposal | None:
    values = (deferred.original_quantity, previous_close, latest_close)
    if any(not value.is_finite() for value in values):
        raise ValueError("D1 inputs must be finite")
    if (
        deferred.original_quantity <= _ZERO
        or previous_close <= _ZERO
        or latest_close <= _ZERO
    ):
        raise ValueError("D1 quantities and closes must be positive")
    if not fresh_reference_intent or fresh_reference_quantity is None:
        return None
    if not fresh_reference_quantity.is_finite() or fresh_reference_quantity <= _ZERO:
        raise ValueError("fresh reference quantity must be finite and positive")
    if latest_close < previous_close:
        return None
    return SpotDecisionQualityProposal(
        "BUY",
        min(deferred.original_quantity, fresh_reference_quantity),
        "D1_DEFERRED_STABILIZED_ADD",
        deferred.program_id,
    )


def evaluate_x1(
    *,
    state: OneShotHarvestState,
    eligible_unreserved_inventory: Decimal,
    activation_reached: bool,
    net_profitable: bool,
    lot_size: Decimal,
    tranche_fraction: Decimal = Decimal("0.25"),
) -> SpotDecisionQualityProposal | None:
    values = (eligible_unreserved_inventory, lot_size, tranche_fraction)
    if any(not value.is_finite() for value in values):
        raise ValueError("X1 inputs must be finite")
    if eligible_unreserved_inventory < _ZERO or lot_size <= _ZERO:
        raise ValueError("inventory must be non-negative and lot size positive")
    if tranche_fraction <= _ZERO or tranche_fraction > Decimal("1"):
        raise ValueError("tranche fraction must be in (0, 1]")
    if state.consumed or not activation_reached or not net_profitable:
        return None
    raw = eligible_unreserved_inventory * tranche_fraction
    lots = (raw / lot_size).to_integral_value(rounding="ROUND_FLOOR")
    quantity = lots * lot_size
    if quantity <= _ZERO:
        return None
    return SpotDecisionQualityProposal(
        "SELL", quantity, "X1_ONE_SHOT_PROFIT_HARVEST", state.program_id
    )


def apply_x1_fill(state: OneShotHarvestState, fill_quantity: Decimal) -> OneShotHarvestState:
    if not fill_quantity.is_finite() or fill_quantity < _ZERO:
        raise ValueError("fill quantity must be finite and non-negative")
    if state.consumed and fill_quantity > _ZERO:
        raise ValueError("X1 tranche already consumed")
    return OneShotHarvestState(
        program_id=state.program_id,
        consumed=state.consumed or fill_quantity > _ZERO,
        filled_quantity=state.filled_quantity + fill_quantity,
    )
