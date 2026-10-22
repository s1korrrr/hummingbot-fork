"""Tidewater V4.1 spacing: hourly-ATR buy gap (M2) and price-zone memory (M4).

Evidence (BNB May–Aug 2026 data study, `docs/evidence/2026-10-06-bnb-data-deep-dive/SYNTHESIS.md`):
V3's ATR(1m)-scaled gap sits at its 0.5% floor 96% of the time, while the drawdown a buy suffers before
it can recover is stable across regimes in hourly-ATR units, not in percent. M2 spaces buys by
`atr_multiplier × ATR_1h`; M4 keeps that distance from every open price zone, not only the latest buy.

ATR_1h is the simple mean of the last `atr_hours` completed UTC-hour true ranges, divided by the last
completed hour's close. It is computed from the native 1m frame cut at the native decision candle, has no
seed and no persisted state, so the same frame always gives the same value (a restart recomputes it).

These are entry rules like the native buy gap. They never cancel an order, block an exit or a recovery
sell, or demote a position. They are pure: the candle research owner calls them unchanged.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Optional

import numpy as np
from pydantic import Field

from controllers.generic.modular_spot_core.domain import DataUnavailable
from hummingbot.strategy_v2.components.config import PolicyModel

HOUR, MINUTE = 3600, 60


class SpacingConfig(PolicyModel):
    """`zones='latest'` spaces from the latest acquired tranche (else the held-bag breakeven), as the
    native gap does; `zones='all'` spaces from every active acquired tranche and the held bag (M4)."""
    atr_multiplier: Decimal = Field(gt=0)
    zones: Literal['latest', 'all']
    atr_hours: int = Field(default=14, ge=1)

    def gap(self, atr_fraction) -> Decimal:
        return self.atr_multiplier * Decimal(atr_fraction)


@dataclass(frozen=True)
class SpacingDecision:
    allowed: bool
    reason: str  # spacing_first | spacing_ok | spacing_gap
    nearest: Optional[Decimal]
    distance: Optional[Decimal]


def _d(value) -> Decimal:
    return Decimal(str(float(value)))


def hourly_atr_fraction(timestamps, highs, lows, closes, *, cutoff_open: int, hours: int = 14) -> Decimal:
    """ATR_1h / last completed hourly close from 1m bars (timestamps = bar opens, UTC seconds).

    Bars opened after `cutoff_open` (the native last closed minute) are ignored, and the frame must contain
    that minute. An hour is complete when it ended by `cutoff_open + 60` and the frame starts at or before
    the hour's start. The last `hours + 1` complete hours must be consecutive (each true range needs the
    previous hour's close). Raises DataUnavailable otherwise; the caller waits.
    """
    t = np.asarray(timestamps, dtype=np.int64)
    h, lo, c = (np.asarray(x, dtype=float) for x in (highs, lows, closes))
    if not (len(t) == len(h) == len(lo) == len(c)):
        raise DataUnavailable('spacing candle columns differ in length')
    kept = t <= int(cutoff_open)
    t, h, lo, c = t[kept], h[kept], lo[kept], c[kept]
    if not len(t) or int(t[-1]) != int(cutoff_open):
        raise DataUnavailable('spacing frame lacks the native last closed minute')
    if np.any(np.diff(t) <= 0):
        raise DataUnavailable('spacing frame timestamps are not increasing')
    close_cutoff = int(cutoff_open) + MINUTE
    first_hour = -(-int(t[0]) // HOUR)  # the first hour whose start the frame covers
    hour = t // HOUR
    complete = [k for k in np.unique(hour) if k >= first_hour and (k + 1) * HOUR <= close_cutoff]
    need = hours + 1
    if len(complete) < need:
        raise DataUnavailable(f'spacing needs {need} completed hours, has {len(complete)}')
    window = complete[-need:]
    if any(b - a != 1 for a, b in zip(window, window[1:])):
        raise DataUnavailable('spacing hours are not consecutive')
    bars = []
    for k in window:
        rows = hour == k
        bars.append((_d(h[rows].max()), _d(lo[rows].min()), _d(c[rows][-1])))
    ranges = []
    for (_, _, previous_close), (high, low, _) in zip(bars, bars[1:]):
        ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
    last_close = bars[-1][2]
    if last_close <= 0:
        raise DataUnavailable('spacing last hourly close is not positive')
    return sum(ranges, Decimal(0)) / len(ranges) / last_close


def spacing_zones(cfg: SpacingConfig, *, tranches, bag) -> tuple:
    """Zones a buy must keep its gap from. `tranches`: entry prices of active acquired tranches, oldest
    first; `bag`: the non-dust held-bag breakeven or None."""
    tranches = tuple(Decimal(x) for x in tranches)
    if cfg.zones == 'latest':
        if tranches:
            return (tranches[-1],)
        return () if bag is None else (Decimal(bag),)
    return tranches + (() if bag is None else (Decimal(bag),))


def spacing_decision(cfg: SpacingConfig, *, bid, zones, gap) -> SpacingDecision:
    """Allowed when `|bid - z| / z >= gap` for every zone (in either direction, as the native gap)."""
    bid, gap = Decimal(bid), Decimal(gap)
    if bid <= 0 or gap < 0:
        raise ValueError('spacing needs a positive bid and a nonnegative gap')
    if not zones:
        return SpacingDecision(True, 'spacing_first', None, None)
    distances = [(abs(bid - z) / z, z) for z in (Decimal(x) for x in zones)]
    if any(z <= 0 for _, z in distances):
        raise ValueError('spacing zones must be positive')
    distance, nearest = min(distances)
    return SpacingDecision(distance >= gap, 'spacing_ok' if distance >= gap else 'spacing_gap', nearest, distance)
