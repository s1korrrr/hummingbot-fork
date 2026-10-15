"""Causal daily-bar internals for breadth and trend participation.

This module accepts only the existing confirmed daily candle projection. It
does not request more history, substitute minute bars, or reinterpret EMA21.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Literal, Sequence

DAY_MS = 86_400_000
SMA_WINDOWS = (20, 50, 200)
Cross = Literal["cross_up", "cross_down", "none"]


@dataclass(frozen=True)
class MovingAverageInternals:
    window_days: int
    sma: Decimal | None
    above_close: bool | None
    slope_percent_per_day: Decimal | None
    cross: Cross | None


@dataclass(frozen=True)
class DailyInternals:
    cutoff_ms: int
    available_at_ms: int
    valid_daily_bars: int
    expected_daily_bars: int
    high_break_20d: bool | None
    low_break_20d: bool | None
    close_break_high_20d: bool | None
    close_break_low_20d: bool | None
    moving_averages: tuple[MovingAverageInternals, ...]


def _decimal(value: Decimal | str | int | float, name: str) -> Decimal:
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must contain finite decimal values") from exc
    if not number.is_finite():
        raise ValueError(f"{name} must contain finite decimal values")
    return number


def _epoch_valid(source_epochs: Sequence[str], start: int, end: int) -> bool:
    return bool(source_epochs) and len(set(source_epochs[start:end])) == 1


def calculate_daily_internals(
    *,
    open_times_ms: Sequence[int],
    bar_available_at_ms: Sequence[int],
    source_epochs: Sequence[str],
    highs: Sequence[Decimal | str | int | float],
    lows: Sequence[Decimal | str | int | float],
    closes: Sequence[Decimal | str | int | float],
    cutoff_ms: int,
    available_at_ms: int,
    expected_daily_bars: int,
) -> DailyInternals:
    """Compute prior-20-day breaks and 20/50/200 SMA internals.

    Bars must be an exactly contiguous UTC-day series. ``cutoff_ms`` is the
    latest included daily close boundary, and every bar's recorded availability
    must be no later than the decision-time ``available_at_ms``. Features that
    lack their required same-epoch history are independently unavailable.
    """
    lengths = {len(open_times_ms), len(bar_available_at_ms), len(source_epochs), len(highs), len(lows), len(closes)}
    if len(lengths) != 1:
        raise ValueError("daily internals inputs must have identical lengths")
    if (
        type(cutoff_ms) is not int
        or type(available_at_ms) is not int
        or cutoff_ms < DAY_MS
        or cutoff_ms % DAY_MS
        or available_at_ms < cutoff_ms
        or type(expected_daily_bars) is not int
        or expected_daily_bars < len(open_times_ms)
    ):
        raise ValueError("daily internals timestamps or coverage are invalid")
    count = len(open_times_ms)
    if count and open_times_ms[-1] + DAY_MS != cutoff_ms:
        raise ValueError("daily internals cutoff must follow the latest included UTC day")
    if any(type(open_ms) is not int or open_ms % DAY_MS for open_ms in open_times_ms):
        raise ValueError("daily internals bars must start on UTC day boundaries")
    if any(right - left != DAY_MS for left, right in zip(open_times_ms, open_times_ms[1:])):
        raise ValueError("daily internals require contiguous UTC daily bars")
    if any(
        type(value) is not int or value < open_ms + DAY_MS for open_ms, value in zip(open_times_ms, bar_available_at_ms)
    ):
        raise ValueError("daily candle availability cannot precede its close")
    if any(value > available_at_ms for value in bar_available_at_ms):
        raise ValueError("daily candle was unavailable at the decision time")
    if any(not isinstance(epoch, str) or not epoch for epoch in source_epochs):
        raise ValueError("daily source epoch identity is required")

    parsed_highs = tuple(_decimal(value, "daily highs") for value in highs)
    parsed_lows = tuple(_decimal(value, "daily lows") for value in lows)
    parsed_closes = tuple(_decimal(value, "daily closes") for value in closes)
    if any(value <= 0 for group in (parsed_highs, parsed_lows, parsed_closes) for value in group):
        raise ValueError("daily prices must be positive")
    if any(
        low > min(high, close) or high < max(low, close)
        for high, low, close in zip(parsed_highs, parsed_lows, parsed_closes)
    ):
        raise ValueError("daily high and low must contain the close")

    range_ok = count >= 21 and _epoch_valid(source_epochs, count - 21, count)
    high_break = low_break = close_break_high = close_break_low = None
    if range_ok:
        prior_highs = parsed_highs[-21:-1]
        prior_lows = parsed_lows[-21:-1]
        high_break = parsed_highs[-1] > max(prior_highs)
        low_break = parsed_lows[-1] < min(prior_lows)
        close_break_high = parsed_closes[-1] > max(prior_highs)
        close_break_low = parsed_closes[-1] < min(prior_lows)

    averages = []
    for window in SMA_WINDOWS:
        enough_sma_history = count >= window and _epoch_valid(source_epochs, count - window, count)
        if not enough_sma_history:
            averages.append(MovingAverageInternals(window, None, None, None, None))
            continue
        current_sma = sum(parsed_closes[-window:], Decimal(0)) / Decimal(window)
        above_close = parsed_closes[-1] > current_sma
        enough_slope_history = count >= window + 1 and _epoch_valid(source_epochs, count - window - 1, count)
        if not enough_slope_history:
            averages.append(MovingAverageInternals(window, current_sma, above_close, None, None))
            continue
        previous_window = parsed_closes[-(window + 1):-1]
        previous_sma = sum(previous_window, Decimal(0)) / Decimal(window)
        current_close = parsed_closes[-1]
        previous_close = parsed_closes[-2]
        slope = (current_sma - previous_sma) / previous_sma * Decimal(100)
        if previous_close <= previous_sma and current_close > current_sma:
            cross: Cross = "cross_up"
        elif previous_close >= previous_sma and current_close < current_sma:
            cross = "cross_down"
        else:
            cross = "none"
        averages.append(MovingAverageInternals(window, current_sma, above_close, slope, cross))

    return DailyInternals(
        cutoff_ms=cutoff_ms,
        available_at_ms=available_at_ms,
        valid_daily_bars=count,
        expected_daily_bars=expected_daily_bars,
        high_break_20d=high_break,
        low_break_20d=low_break,
        close_break_high_20d=close_break_high,
        close_break_low_20d=close_break_low,
        moving_averages=tuple(averages),
    )
