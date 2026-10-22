"""Pure, continuous buy-limit placement for an authorized OK RSI entry."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from typing import Any

ZERO = Decimal("0")
ONE = Decimal("1")
ADAPTIVE_MAKER_POLICY = "ADAPTIVE_MAKER_V1"
ADAPTIVE_RESERVED_ENTRY_POLICY = "ADAPTIVE_RESERVED_ENTRY_V2"
ADAPTIVE_REFERENCE_FALLBACK_POLICY = "ADAPTIVE_REFERENCE_FALLBACK_V3"


class AdaptiveEntryPriceInputError(ValueError):
    """Invalid price-selection input; callers must not dispatch an order."""


def _decimal(value: Any, name: str, *, positive: bool = False) -> Decimal:
    try:
        if isinstance(value, bool):
            raise ValueError("boolean input")
        result = Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise AdaptiveEntryPriceInputError(f"invalid {name}") from exc
    if not result.is_finite() or (positive and result <= 0):
        qualifier = "positive and finite" if positive else "finite"
        raise AdaptiveEntryPriceInputError(f"{name} must be {qualifier}")
    return result


def _floor(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


@dataclass(frozen=True)
class AdaptiveEntryPriceSelection:
    policy: str
    reference_price: Decimal
    price: Decimal
    discount: Decimal
    discount_atr: Decimal
    signal_confidence: Decimal
    trend_alignment: Decimal


def select_adaptive_entry_price(
    *,
    bid: Any,
    ask: Any,
    atr_1m: Any,
    price_step: Any,
    signal_confidence: Any,
    trend_alignment: Any,
) -> AdaptiveEntryPriceSelection:
    """Select one passive price from decision-time context without pair tables.

    The discount grows continuously with signal uncertainty and adverse trend
    alignment. A tick and two observed spreads provide the passive floor.
    """
    bid = _decimal(bid, "bid", positive=True)
    ask = _decimal(ask, "ask", positive=True)
    atr_1m = _decimal(atr_1m, "atr_1m", positive=True)
    price_step = _decimal(price_step, "price_step", positive=True)
    signal_confidence = _decimal(signal_confidence, "signal_confidence")
    trend_alignment = _decimal(trend_alignment, "trend_alignment")
    if bid > ask:
        raise AdaptiveEntryPriceInputError("bid cannot exceed ask")
    if not ZERO <= signal_confidence <= ONE:
        raise AdaptiveEntryPriceInputError("signal_confidence must be within [0,1]")
    if not -ONE <= trend_alignment <= ONE:
        raise AdaptiveEntryPriceInputError("trend_alignment must be within [-1,1]")

    reference_price = _floor(ask, price_step)
    uncertainty = ONE - signal_confidence
    adversity = max(ZERO, -trend_alignment)
    atr_component = atr_1m * (uncertainty + adversity) / Decimal("2")
    target_discount = max(price_step, Decimal("2") * (ask - bid), atr_component)
    price = _floor(reference_price - target_discount, price_step)
    if reference_price <= 0 or price <= 0 or price >= reference_price:
        raise AdaptiveEntryPriceInputError("adaptive entry price is outside the positive passive range")
    discount = reference_price - price
    return AdaptiveEntryPriceSelection(
        policy=ADAPTIVE_MAKER_POLICY,
        reference_price=reference_price,
        price=price,
        discount=discount,
        discount_atr=discount / atr_1m,
        signal_confidence=signal_confidence,
        trend_alignment=trend_alignment,
    )
