"""Pure, additive candle-derived breadth diagnostics.

These functions do not replace the legacy breadth or pressure definitions. They
accept already selected, decision-time observations and preserve missingness.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from fractions import Fraction
from typing import Mapping

from hummingbot.strategy_v2.components.recipe_contract import canonical_json

ONE_BASIS_POINT = Decimal("0.0001")
ZERO_GROSS_FLOOR = Decimal("1e-12")


@dataclass(frozen=True)
class NormalizedBreadth:
    advances: int
    declines: int
    unchanged: int
    valid_count: int
    expected_count: int
    pressure: Decimal | None


@dataclass(frozen=True)
class PressureTrajectory:
    level: Decimal | None
    change: Decimal | None
    velocity: Decimal | None
    acceleration: Decimal | None
    matched_count: int
    expected_count: int
    matched_membership_hash: str | None
    share_change_percentage_points: Decimal | None = None


def normalized_breadth(
    returns: Mapping[str, Decimal | str | int | float | None], expected_count: int
) -> NormalizedBreadth:
    """Classify returns with a 1bp neutral band; values are simple return fractions."""
    if type(expected_count) is not int or expected_count < 0 or expected_count > 500:
        raise ValueError("expected breadth count must be an integer in [0, 500]")
    if len(returns) > expected_count or any(not isinstance(key, str) or not key for key in returns):
        raise ValueError("breadth identities exceed or invalidate the expected cohort")
    advances = declines = unchanged = 0
    for value in returns.values():
        if value is None:
            continue
        try:
            number = value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError("breadth returns must be finite numbers or null") from exc
        if not number.is_finite() or number < Decimal(-1):
            raise ValueError("breadth return is outside the finite simple-return range")
        if number > ONE_BASIS_POINT:
            advances += 1
        elif number < -ONE_BASIS_POINT:
            declines += 1
        else:
            unchanged += 1
    valid_count = advances + declines + unchanged
    pressure = (Decimal(advances - declines) / Decimal(valid_count)) if valid_count else None
    return NormalizedBreadth(advances, declines, unchanged, valid_count, expected_count, pressure)


def matched_pressure_trajectory(
    observations: Mapping[int, Mapping[str, Decimal | str | int | float | None]],
    *,
    cutoff_ms: int,
    horizon_minutes: int,
    interval_minutes: int,
    expected_count: int,
) -> PressureTrajectory:
    """Compute level/change/velocity/acceleration on the exact matched cohort.

    Inputs must contain returns for cutoff, cutoff-tau and cutoff-2*tau. A member
    is included only when its return is present at all three endpoints. Callers
    must provide actual contiguous closed-candle endpoints; this function does
    not interpolate or bridge gaps.
    """
    if (
        type(cutoff_ms) is not int
        or cutoff_ms < 0
        or type(horizon_minutes) is not int
        or horizon_minutes < 1
        or type(interval_minutes) is not int
        or interval_minutes not in (5, 15, 60)
        or expected_count < 0
    ):
        raise ValueError("invalid breadth trajectory boundary")
    tau_ms = interval_minutes * 60_000
    times = (cutoff_ms, cutoff_ms - tau_ms, cutoff_ms - 2 * tau_ms)
    if min(times) < 0 or any(timestamp not in observations for timestamp in times):
        return PressureTrajectory(None, None, None, None, 0, expected_count, None)
    slices = [observations[timestamp] for timestamp in times]
    matched = {
        key for key in set(slices[0]) & set(slices[1]) & set(slices[2]) if all(row[key] is not None for row in slices)
    }
    if len(slices[0]) > expected_count or len(matched) > expected_count:
        raise ValueError("trajectory membership exceeds the frozen expected cohort")
    matched_values = [{key: row[key] for key in matched} for row in slices]
    if not matched:
        return PressureTrajectory(None, None, None, None, 0, expected_count, None)
    pressures = [normalized_breadth(row, expected_count).pressure for row in matched_values]
    if any(value is None for value in pressures):
        return PressureTrajectory(None, None, None, None, len(matched), expected_count, None)
    current, previous, oldest = pressures
    change = current - previous
    velocity = change / Decimal(interval_minutes)
    acceleration = (current - Decimal(2) * previous + oldest) / Decimal(interval_minutes ** 2)
    current_advance_share = Decimal(
        sum(Decimal(str(value)) > ONE_BASIS_POINT for value in matched_values[0].values())
    ) / Decimal(len(matched))
    previous_advance_share = Decimal(
        sum(Decimal(str(value)) > ONE_BASIS_POINT for value in matched_values[1].values())
    ) / Decimal(len(matched))
    share_change = Decimal(100) * (current_advance_share - previous_advance_share)
    member_hash = hashlib.sha256(canonical_json(sorted(matched)).encode()).hexdigest()
    return PressureTrajectory(
        current, change, velocity, acceleration, len(matched), expected_count, member_hash, share_change
    )


def price_breadth_divergence(
    *,
    current_close: Decimal | str | int | float | None,
    prior_closes: tuple[Decimal | str | int | float, ...],
    pressure_change: Decimal | str | int | float | None,
) -> bool | None:
    """Flag a confirmed close beyond a fully observed prior range against breadth.

    None means a required causal input is unavailable. The caller supplies only
    already closed prior bars and a matched-cohort pressure change.
    """
    if current_close is None or not prior_closes or pressure_change is None:
        return None
    try:
        close = Decimal(str(current_close))
        prior = tuple(Decimal(str(value)) for value in prior_closes)
        change = Decimal(str(pressure_change))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("divergence inputs must be finite numbers") from exc
    if (
        not close.is_finite()
        or close <= 0
        or not all(value.is_finite() and value > 0 for value in prior)
        or not change.is_finite()
    ):
        raise ValueError("divergence inputs must be positive finite closes and finite pressure")
    new_high = close > max(prior)
    new_low = close < min(prior)
    return (new_high and change < 0) or (new_low and change > 0)


def top_five_absolute_contribution_share(
    contributions: Mapping[str, Decimal | str | int | float | None],
    *,
    expected_ids: tuple[str, ...],
    gross_floor: Decimal = ZERO_GROSS_FLOOR,
) -> Decimal | None:
    """Return top-five absolute contribution / gross absolute contribution.

    Missing expected members and near-zero gross movement remain unavailable.
    """
    if tuple(sorted(set(expected_ids))) != expected_ids or not expected_ids:
        raise ValueError("expected contribution identities must be sorted, unique and non-empty")
    if set(contributions) != set(expected_ids):
        raise ValueError("contributions must contain every frozen expected identity")
    if any(contributions[key] is None for key in expected_ids):
        return None
    values = []
    for key in expected_ids:
        try:
            value = Decimal(str(contributions[key]))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError("contributions must be finite numbers or null") from exc
        if not value.is_finite():
            raise ValueError("contributions must be finite numbers or null")
        values.append(Fraction(abs(value)))
    gross = sum(values, Fraction(0))
    if gross <= Fraction(gross_floor):
        return None
    top_five = sum(sorted(values, reverse=True)[:5], Fraction(0))
    ratio = top_five / gross
    # Sum Decimal contributions as exact rationals before dividing. Decimal's
    # ambient precision can round the full-cohort denominator down while the
    # separately sorted top-five numerator rounds up, producing a share just
    # above one when the cohort has five or fewer members.
    return Decimal(ratio.numerator) / Decimal(ratio.denominator)


def weighted_cohort_return(
    returns: Mapping[str, Decimal | str | int | float | None],
    weights: Mapping[str, Decimal | str | int | float],
    *,
    expected_ids: tuple[str, ...],
) -> Decimal | None:
    """Return a weighted index only when every original positive weight is observed."""
    if tuple(sorted(set(expected_ids))) != expected_ids or not expected_ids:
        raise ValueError("expected cohort identities must be sorted, unique and non-empty")
    if set(weights) != set(expected_ids) or set(returns) != set(expected_ids):
        return None
    parsed_weights: dict[str, Decimal] = {}
    parsed_returns: dict[str, Decimal] = {}
    for identity in expected_ids:
        try:
            weight = Decimal(str(weights[identity]))
            value = returns[identity]
            if value is None:
                return None
            parsed_return = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError("weights and returns must be finite numbers") from exc
        if not weight.is_finite() or weight <= 0 or not parsed_return.is_finite():
            raise ValueError("weights must be positive and values finite")
        parsed_weights[identity] = weight
        parsed_returns[identity] = parsed_return
    with localcontext() as context:
        context.prec = 28
        total_weight = sum(parsed_weights.values(), Decimal(0))
        return sum((parsed_weights[key] * parsed_returns[key] for key in expected_ids), Decimal(0)) / total_weight


def capped_weights(
    prior_liquidity: Mapping[str, Decimal | str | int | float],
    *,
    expected_ids: tuple[str, ...],
    cap: Decimal = Decimal("0.10"),
) -> dict[str, Decimal] | None:
    """Normalize positive prior-liquidity weights under a per-member cap.

    Returns ``None`` when the cap cannot be met (fewer than ten positive names
    for the 10% default) or when the full expected cohort is not represented.
    Excess is redistributed proportionally among the remaining uncapped names.
    """
    if tuple(sorted(set(expected_ids))) != expected_ids or not expected_ids:
        raise ValueError("expected cohort identities must be sorted, unique and non-empty")
    if not cap.is_finite() or cap <= 0 or cap > 1:
        raise ValueError("weight cap must be in (0, 1]")
    if set(prior_liquidity) != set(expected_ids) or Decimal(len(expected_ids)) * cap < 1:
        return None
    raw = {}
    for identity in expected_ids:
        try:
            value = Decimal(str(prior_liquidity[identity]))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError("prior liquidity weights must be finite positive numbers") from exc
        if not value.is_finite() or value <= 0:
            return None
        raw[identity] = value
    remaining = set(expected_ids)
    result: dict[str, Decimal] = {}
    remaining_weight = Decimal(1)
    while remaining:
        available_total = sum((raw[key] for key in remaining), Decimal(0))
        if available_total <= 0:
            return None
        newly_capped = {key for key in remaining if remaining_weight * raw[key] / available_total > cap}
        if not newly_capped:
            for key in remaining:
                result[key] = remaining_weight * raw[key] / available_total
            break
        for key in newly_capped:
            result[key] = cap
            remaining_weight -= cap
        remaining -= newly_capped
    if set(result) != set(expected_ids) or sum(result.values(), Decimal(0)) != Decimal(1):
        # Decimal division may leave a final ulp; assign it deterministically to
        # the last uncapped identity, without changing any capped member.
        last = next((key for key in reversed(expected_ids) if result[key] < cap), None)
        if last is None:
            return None
        result[last] += Decimal(1) - sum(result.values(), Decimal(0))
    if any(value > cap for value in result.values()) or sum(result.values(), Decimal(0)) != Decimal(1):
        return None
    return result
