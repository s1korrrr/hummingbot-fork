"""Pure calculations for additive confirmed hourly Market Picture internals."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from typing import Mapping, Sequence

HOUR_MS = 3_600_000
SMA_WINDOWS = (20, 50, 200)
MAX_HOURLY_BARS = 201


@dataclass(frozen=True)
class MovingAverageInternals:
    sma: Decimal | None
    above_close: bool | None
    slope_percent_per_hour: Decimal | None
    cross: str | None


@dataclass(frozen=True)
class HourlyInternals:
    moving_averages: Mapping[int, MovingAverageInternals]
    trend_class: str | None
    efficiency_ratio_20: Decimal | None
    contiguous_bars: int


def calculate_hourly_internals(
    *,
    open_times_ms: Sequence[int],
    bar_available_at_ms: Sequence[int],
    source_epochs: Sequence[str],
    closes: Sequence[Decimal | str | int],
    cutoff_ms: int,
    available_at_ms: int,
) -> HourlyInternals:
    """Calculate hourly SMA, trend and path efficiency from confirmed contiguous closes.

    At most 201 bars are needed: 201 for SMA200's prior/current comparison,
    53 for the SMA20/50 trend label and 21 for efficiency ratio. The caller
    supplies the contiguous suffix from one source epoch.
    """
    count = len(closes)
    if count > MAX_HOURLY_BARS or any(
        len(values) != count for values in (open_times_ms, bar_available_at_ms, source_epochs)
    ):
        raise ValueError("hourly inputs must have one aligned bounded row per close")
    if type(cutoff_ms) is not int or type(available_at_ms) is not int or cutoff_ms < 0 or available_at_ms < 0:
        raise ValueError("hourly observation times must be nonnegative integers")
    if count == 0:
        if cutoff_ms != 0:
            raise ValueError("empty hourly input cannot declare a cutoff")
        return HourlyInternals(
            {window: MovingAverageInternals(None, None, None, None) for window in SMA_WINDOWS},
            None,
            None,
            0,
        )
    if cutoff_ms % HOUR_MS or cutoff_ms != open_times_ms[-1] + HOUR_MS:
        raise ValueError("hourly cutoff must equal the last confirmed UTC hourly close")
    if any(type(opened) is not int or opened % HOUR_MS for opened in open_times_ms):
        raise ValueError("hourly opens must align to UTC hour boundaries")
    if any(right - left != HOUR_MS for left, right in zip(open_times_ms, open_times_ms[1:])):
        raise ValueError("hourly bars must be contiguous")
    if len(set(source_epochs)) != 1 or any(not isinstance(epoch, str) or not epoch for epoch in source_epochs):
        raise ValueError("hourly bars must share one nonempty source epoch")
    if any(
        type(available) is not int or available < opened + HOUR_MS or available > available_at_ms
        for opened, available in zip(open_times_ms, bar_available_at_ms)
    ):
        raise ValueError("hourly bars must be confirmed by the decision availability time")
    try:
        values = tuple(value if isinstance(value, Decimal) else Decimal(str(value)) for value in closes)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("hourly closes must be finite positive decimals") from exc
    if any(not value.is_finite() or value <= 0 for value in values):
        raise ValueError("hourly closes must be finite positive decimals")

    averages: dict[int, MovingAverageInternals] = {}
    with localcontext() as context:
        context.prec = 28
        for window in SMA_WINDOWS:
            sma = above = slope = cross = None
            if count >= window:
                sma = sum(values[-window:], Decimal(0)) / Decimal(window)
                above = values[-1] > sma
            if count >= window + 1:
                previous_window = values[-(window + 1):-1]
                prior_sma = sum(previous_window, Decimal(0)) / Decimal(window)
                slope = Decimal(100) * (sma / prior_sma - Decimal(1))
                if values[-2] <= prior_sma and values[-1] > sma:
                    cross = "cross_up"
                elif values[-2] >= prior_sma and values[-1] < sma:
                    cross = "cross_down"
                else:
                    cross = "none"
            averages[window] = MovingAverageInternals(sma, above, slope, cross)

        trend_class = None
        if count >= 53:
            sma20 = averages[20].sma
            sma50 = averages[50].sma
            sma20_three_hours_ago = sum(values[-23:-3], Decimal(0)) / Decimal(20)
            if values[-1] > sma20 > sma50 and sma20 > sma20_three_hours_ago:
                trend_class = "bullish"
            elif values[-1] < sma20 < sma50 and sma20 < sma20_three_hours_ago:
                trend_class = "bearish"
            else:
                trend_class = "neutral"

        efficiency = None
        if count >= 21:
            path = sum((abs(right - left) for left, right in zip(values[-21:-1], values[-20:])), Decimal(0))
            efficiency = Decimal(0) if path == 0 else abs(values[-1] - values[-21]) / path

    return HourlyInternals(averages, trend_class, efficiency, count)
