"""Pure RSI-family trailing policy calculations with no venue dependencies."""

import math
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class ContinuousTrailingSelection:
    activation: Decimal
    delta: Decimal
    volatility_ratio: Decimal
    initial_locked_profit: Decimal
    policy: str = "adaptive_continuous"


def calculate_continuous_adaptive_trailing(
    *,
    atr_pct: float,
    baseline_atr_pct: float,
    base_activation: Decimal,
    base_delta: Decimal,
    activation_min: Decimal,
    activation_max: Decimal,
    delta_min: Decimal,
    delta_max: Decimal,
    min_locked_profit: Decimal,
    activation_exponent: float,
    delta_exponent: float,
    min_multiplier: float,
    max_multiplier: float,
) -> ContinuousTrailingSelection:
    numeric_values = (
        atr_pct,
        baseline_atr_pct,
        activation_exponent,
        delta_exponent,
        min_multiplier,
        max_multiplier,
    )
    decimal_values = (
        base_activation,
        base_delta,
        activation_min,
        activation_max,
        delta_min,
        delta_max,
        min_locked_profit,
    )
    if not all(math.isfinite(float(value)) for value in numeric_values):
        raise ValueError("continuous trailing inputs must be finite")
    if not all(value.is_finite() for value in decimal_values):
        raise ValueError("continuous trailing inputs must be finite")
    if atr_pct <= 0 or baseline_atr_pct <= 0:
        raise ValueError("continuous trailing ATR inputs must be positive")
    if min_multiplier <= 0 or max_multiplier < min_multiplier:
        raise ValueError("continuous trailing multiplier bounds are invalid")
    if not (Decimal("0") < activation_min < activation_max):
        raise ValueError("continuous trailing activation bounds are invalid")
    if not (Decimal("0") < delta_min < delta_max):
        raise ValueError("continuous trailing delta bounds are invalid")
    if base_activation <= 0 or base_delta <= 0 or min_locked_profit <= 0:
        raise ValueError("continuous trailing price distances must be positive")

    ratio = atr_pct / baseline_atr_pct
    activation_multiplier = max(min_multiplier, min(max_multiplier, ratio ** activation_exponent))
    delta_multiplier = max(min_multiplier, min(max_multiplier, ratio ** delta_exponent))
    activation = base_activation * Decimal(str(activation_multiplier))
    delta = base_delta * Decimal(str(delta_multiplier))
    activation = max(activation_min, min(activation_max, activation))
    delta = max(delta_min, min(delta_max, delta))
    activation = max(activation, delta + min_locked_profit)
    if activation > activation_max or activation - delta < min_locked_profit:
        raise ValueError("continuous trailing bounds cannot preserve minimum locked profit")
    return ContinuousTrailingSelection(
        activation=activation,
        delta=delta,
        volatility_ratio=Decimal(str(ratio)),
        initial_locked_profit=activation - delta,
    )
