"""Additive hourly RVOL, compression and extreme-volatility calculations."""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from typing import Sequence

import numpy as np

from .metrics import annualized_realized_volatility, ema, wilder_atr

HOUR_MS = 3_600_000
RVOL_BASELINE_BARS = 20
COMPRESSION_WINDOW = 20
EMA_WARMUP_BARS = 60
REALIZED_RETURN_HOURS = 24
EXTREME_REFERENCE_OBSERVATIONS = 720
EXTREME_REQUIRED_CLOSES = EXTREME_REFERENCE_OBSERVATIONS + REALIZED_RETURN_HOURS + 1
MAX_INPUT_BARS = EXTREME_REQUIRED_CLOSES
HOURS_PER_YEAR = 365 * 24


@dataclass(frozen=True)
class HourlyAnomalies:
    turnover_rvol_1h: float | None
    high_rvol_1h: bool | None
    compression_1h: bool | None
    realized_volatility_24h_hourly_annualized: float | None
    extreme_realized_volatility_1h_95p: bool | None
    turnover_rvol_reason: str | None
    compression_reason: str | None
    realized_volatility_reason: str | None
    extreme_volatility_reason: str | None
    valid_source_bars: int
    extreme_reference_count: int


def _decimal(value: Decimal | str | float | int, *, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, str, float, int)):
        raise ValueError(f"{name} must contain decimal-compatible finite values")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must contain decimal-compatible finite values") from exc
    if not parsed.is_finite():
        raise ValueError(f"{name} must contain decimal-compatible finite values")
    return parsed


def calculate_hourly_anomalies(
    *,
    open_times_ms: Sequence[int],
    bar_available_at_ms: Sequence[int],
    source_epochs: Sequence[str],
    highs: Sequence[Decimal | str | float | int],
    lows: Sequence[Decimal | str | float | int],
    closes: Sequence[Decimal | str | float | int],
    quote_turnovers: Sequence[Decimal | str | float | int],
    cutoff_ms: int,
    available_at_ms: int,
) -> HourlyAnomalies:
    """Calculate dictionary candidates from exact, confirmed 1h bars.

    Inputs are already selected from one contiguous source-epoch suffix. At
    most 745 bars are accepted: 60 for the fixed EMA warmup and 745 for a
    current 24-hour sample-volatility value against 720 prior observations.
    Missing minimum history yields explicit per-feature reasons; malformed,
    future, gapped or cross-epoch inputs are rejected.
    """
    columns = (
        open_times_ms,
        bar_available_at_ms,
        source_epochs,
        highs,
        lows,
        closes,
        quote_turnovers,
    )
    count = len(open_times_ms)
    if count < 1 or count > MAX_INPUT_BARS or any(len(column) != count for column in columns[1:]):
        raise ValueError("hourly anomaly columns must have one bounded row per bar")
    if (
        type(cutoff_ms) is not int
        or type(available_at_ms) is not int
        or cutoff_ms < HOUR_MS
        or cutoff_ms % HOUR_MS
        or available_at_ms < cutoff_ms
    ):
        raise ValueError("hourly anomaly cutoff or decision availability is invalid")
    if any(type(opened) is not int or opened < 0 or opened % HOUR_MS for opened in open_times_ms):
        raise ValueError("hourly anomaly bars must start on UTC hour boundaries")
    if any(right - left != HOUR_MS for left, right in zip(open_times_ms, open_times_ms[1:])):
        raise ValueError("hourly anomaly bars must be contiguous")
    if cutoff_ms != open_times_ms[-1] + HOUR_MS:
        raise ValueError("hourly anomaly cutoff must follow the latest completed bar")
    if any(not isinstance(epoch, str) or not epoch for epoch in source_epochs) or len(set(source_epochs)) != 1:
        raise ValueError("hourly anomaly bars must share one source epoch")
    if any(
        type(receipt) is not int or receipt < opened + HOUR_MS or receipt > available_at_ms
        for opened, receipt in zip(open_times_ms, bar_available_at_ms)
    ):
        raise ValueError("hourly anomaly bars violate decision availability")

    high_values = tuple(_decimal(value, name="hourly highs") for value in highs)
    low_values = tuple(_decimal(value, name="hourly lows") for value in lows)
    close_values = tuple(_decimal(value, name="hourly closes") for value in closes)
    turnover_values = tuple(_decimal(value, name="hourly quote turnovers") for value in quote_turnovers)
    if any(value <= 0 for values in (high_values, low_values, close_values) for value in values):
        raise ValueError("hourly prices must be positive")
    if any(
        low > min(high, close) or high < max(low, close)
        for high, low, close in zip(high_values, low_values, close_values)
    ):
        raise ValueError("hourly high and low must contain the close")
    if any(value < 0 for value in turnover_values):
        raise ValueError("hourly quote turnovers must be nonnegative")

    turnover_rvol = None
    high_rvol = None
    turnover_reason = None
    if count < RVOL_BASELINE_BARS + 1:
        turnover_reason = "INSUFFICIENT_HISTORY"
    else:
        baseline = sorted(turnover_values[-(RVOL_BASELINE_BARS + 1):-1])
        middle_sum = Fraction(baseline[9]) + Fraction(baseline[10])
        if middle_sum <= 0:
            turnover_reason = "ZERO_BASELINE"
        else:
            current = Fraction(turnover_values[-1])
            # Evaluate the predicate before converting its display value to a
            # float: both Decimal context rounding and binary64 rounding can
            # turn an observation just below the boundary into a false hit.
            high_rvol = current >= middle_sum
            try:
                turnover_rvol = float(2 * current / middle_sum)
            except OverflowError as exc:
                raise ValueError("hourly turnover ratio must be finite") from exc
            if not math.isfinite(turnover_rvol):
                raise ValueError("hourly turnover ratio must be finite")

    compression = None
    compression_reason = None
    if count < EMA_WARMUP_BARS:
        compression_reason = "INSUFFICIENT_HISTORY"
    else:
        high_array = np.asarray([float(value) for value in high_values[-EMA_WARMUP_BARS:]], dtype=float)
        low_array = np.asarray([float(value) for value in low_values[-EMA_WARMUP_BARS:]], dtype=float)
        close_array = np.asarray([float(value) for value in close_values[-EMA_WARMUP_BARS:]], dtype=float)
        current_closes = close_array[-COMPRESSION_WINDOW:]
        sma = float(np.mean(current_closes))
        deviation = float(np.std(current_closes, ddof=0))
        ema_value = float(ema(close_array, COMPRESSION_WINDOW)[-1])
        atr_value = float(wilder_atr(high_array, low_array, close_array, COMPRESSION_WINDOW)[-1])
        if not all(math.isfinite(value) for value in (sma, deviation, ema_value, atr_value)):
            compression_reason = "INSUFFICIENT_HISTORY"
        else:
            lower_bollinger, upper_bollinger = sma - 2.0 * deviation, sma + 2.0 * deviation
            lower_atr, upper_atr = ema_value - 1.5 * atr_value, ema_value + 1.5 * atr_value
            compression = lower_bollinger > lower_atr and upper_bollinger < upper_atr

    realized_volatility = None
    realized_reason = None
    if count < REALIZED_RETURN_HOURS + 1:
        realized_reason = "INSUFFICIENT_HISTORY"
    else:
        realized_volatility = annualized_realized_volatility(
            [float(value) for value in close_values[-(REALIZED_RETURN_HOURS + 1):]],
            HOURS_PER_YEAR,
            REALIZED_RETURN_HOURS,
        )
        if realized_volatility is None or not math.isfinite(realized_volatility):
            realized_volatility = None
            realized_reason = "INSUFFICIENT_HISTORY"

    extreme = None
    extreme_reason = None
    reference_count = 0
    if count < EXTREME_REQUIRED_CLOSES:
        extreme_reason = "INSUFFICIENT_HISTORY"
    else:
        hourly_returns = np.diff(np.log(np.asarray([float(value) for value in close_values], dtype=float)))
        windows = np.lib.stride_tricks.sliding_window_view(hourly_returns, REALIZED_RETURN_HOURS)
        annualized = np.std(windows, axis=1, ddof=1) * math.sqrt(HOURS_PER_YEAR)
        prior = annualized[:-1]
        current = float(annualized[-1])
        if len(prior) != EXTREME_REFERENCE_OBSERVATIONS or not np.isfinite(annualized).all():
            extreme_reason = "INSUFFICIENT_HISTORY"
        else:
            threshold = float(np.percentile(prior, 95, method="linear"))
            extreme = current > threshold
            realized_volatility = current
            realized_reason = None
            reference_count = len(prior)

    return HourlyAnomalies(
        turnover_rvol_1h=turnover_rvol,
        high_rvol_1h=high_rvol,
        compression_1h=compression,
        realized_volatility_24h_hourly_annualized=realized_volatility,
        extreme_realized_volatility_1h_95p=extreme,
        turnover_rvol_reason=turnover_reason,
        compression_reason=compression_reason,
        realized_volatility_reason=realized_reason,
        extreme_volatility_reason=extreme_reason,
        valid_source_bars=count,
        extreme_reference_count=reference_count,
    )
