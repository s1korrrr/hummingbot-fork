"""Tidewater protected runner: the pure policy (single source for Tidewater).

These definitions are the research protected-runner policy of hummingbot commit a42150506
("feat(modular_spot): pure research protected-runner policy", `modular_spot_core/bag.py`), copied
without change so V3's sealed closure does not move. `test/tidewater_v4/test_runner_policy.py`
pins each definition's syntax tree to that commit and replays its test vectors, so this copy
cannot diverge silently. The native lifecycle that uses them is `runner_book.py` (state and
durability) and `runner_execution.py` (orders).

When a tranche's trailing (harvest) exit fires, `runner_fraction` of its base is kept as a runner.
The runner exits when a closed bar's close is at or below a monotone floor
max(previous floor, cost floor, high water x (1 - atr_multiplier x daily ATR%)); it never sells
below the cost floor (cost x (1 + both sides' costs + floor_margin)) and has no stop-loss.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Tuple

import numpy as np
from pydantic import Field

from controllers.generic.modular_spot_core.domain import PolicyError, decimal
from hummingbot.strategy_v2.components.config import PolicyModel


class RunnerConfig(PolicyModel):
    """Research protected runner: when a tranche's trailing (harvest) exit fills, ``runner_fraction`` of its base is
    kept as a runner lot instead of sold. The lot exits when a closed bar's close is at or below a monotone floor
    max(previous floor, cost floor, high water x (1 - atr_multiplier x daily ATR%)), and never realises a loss: the
    cost floor is the cost basis plus both sides' costs plus ``floor_margin``. ATR% is the mean true range of the
    last ``atr_days`` completed UTC days over the last completed daily close. Hypothesis: harvested tranches exit
    rallies early; a protected runner participates in the trend. No live counterpart (research extension only).
    """
    runner_fraction: Decimal = Field(gt=0, lt=1)
    floor_margin: Decimal = Field(ge=0)
    atr_multiplier: Decimal = Field(gt=0)
    atr_days: int = Field(ge=2, strict=True)


RUNNER_FIELDS = ('runner_fraction', 'floor_margin', 'atr_multiplier', 'atr_days')


def runner_config(settings) -> RunnerConfig:
    """RunnerConfig from sealed decimal-string research settings (exactly the four fields; atr_days an integer)."""
    if set(settings) != set(RUNNER_FIELDS):
        raise PolicyError(f'runner requires exactly {sorted(RUNNER_FIELDS)}')
    days = decimal(settings['atr_days'], 'runner atr_days')
    if days != days.to_integral_value():
        raise PolicyError('runner atr_days must be an integer')
    return RunnerConfig(runner_fraction=decimal(settings['runner_fraction'], 'runner fraction'),
                        floor_margin=decimal(settings['floor_margin'], 'runner floor margin', minimum=None),
                        atr_multiplier=decimal(settings['atr_multiplier'], 'runner atr multiplier', minimum=None),
                        atr_days=int(days))


@dataclass(frozen=True)
class RunnerSplit:
    runner_base: Decimal
    sold_base: Decimal
    skipped: Optional[str]  # None, 'runner_below_min_notional' or 'sold_below_min_notional'


def runner_split(cfg: RunnerConfig, *, amount, price, amount_step, minimum_notional) -> RunnerSplit:
    """Keep ``runner_fraction`` of a harvested tranche's base (rounded down to the amount step) and sell the rest.

    When either part would be below the minimum notional at the harvest price nothing is kept (the whole tranche
    is sold, as without a runner) and the reason is returned."""
    amount = decimal(amount, 'tranche amount', positive=True)
    price = decimal(price, 'harvest price', positive=True)
    notional = decimal(minimum_notional, 'minimum notional')
    runner = (amount * cfg.runner_fraction).quantize(decimal(amount_step, 'amount step', positive=True),
                                                     rounding='ROUND_DOWN')
    sold = amount - runner
    if runner <= 0 or runner * price < notional:
        return RunnerSplit(Decimal('0'), amount, 'runner_below_min_notional')
    if sold <= 0 or sold * price < notional:
        return RunnerSplit(Decimal('0'), amount, 'sold_below_min_notional')
    return RunnerSplit(runner, sold, None)


def runner_cost_floor(cfg: RunnerConfig, *, cost_basis, fee_fraction) -> Decimal:
    """Lowest runner sale price: ``cost_basis`` x (1 + ``fee_fraction`` + floor_margin), ``fee_fraction`` being the
    buy plus sell fee and displacement, so the net sale never realises a loss."""
    basis = decimal(cost_basis, 'runner cost basis', positive=True)
    return basis * (1 + decimal(fee_fraction, 'runner fee fraction') + cfg.floor_margin)


def runner_floor(cfg: RunnerConfig, *, previous_floor, cost_floor, high_water, atr_pct) -> Decimal:
    """Monotone runner floor: max(previous, cost floor, high water x (1 - atr_multiplier x ATR%)); the ATR trail
    only applies once ``atr_pct`` exists (enough completed days)."""
    floor = max(decimal(previous_floor, 'previous runner floor'), decimal(cost_floor, 'runner cost floor', positive=True))
    if atr_pct is not None:
        trail = decimal(high_water, 'runner high water', positive=True) * (
            1 - cfg.atr_multiplier * decimal(atr_pct, 'runner ATR fraction'))
        floor = max(floor, trail)
    return floor


@dataclass(frozen=True)
class DailyBar:
    start: int  # UTC day start (epoch seconds)
    high: Decimal
    low: Decimal
    close: Decimal
    previous_close: Optional[Decimal]  # close of the minute before the day, when that minute exists


DAY_SECONDS = 86400


def runner_daily_bars(timestamps, highs, lows, closes, *, interval: int = 60) -> Tuple[DailyBar, ...]:
    """Whole UTC daily bars from closed ``interval`` bars; days not fully covered (partial first/last) are no bars."""
    t = np.asarray(timestamps, dtype=np.int64)
    h, lo, c = (np.asarray(x, dtype=float) for x in (highs, lows, closes))
    if not (len(t) == len(h) == len(lo) == len(c)) or np.any(np.diff(t) <= 0):
        raise PolicyError('runner daily bars need ordered, equal-length minute inputs')
    per_day = DAY_SECONDS // interval
    index = {int(stamp): i for i, stamp in enumerate(t)}
    bars = []
    for day in np.unique(t // DAY_SECONDS):
        start = int(day) * DAY_SECONDS
        first = index.get(start)
        last = index.get(start + DAY_SECONDS - interval)
        if first is None or last is None or last - first + 1 != per_day:
            continue
        before = index.get(start - interval)
        bars.append(DailyBar(start, Decimal(str(float(h[first:last + 1].max()))),
                             Decimal(str(float(lo[first:last + 1].min()))), Decimal(str(float(c[last]))),
                             None if before is None else Decimal(str(float(c[before])))))
    return tuple(bars)


def runner_atr_pct(days, *, now: int, atr_days: int) -> Optional[Decimal]:
    """Mean true range of the last ``atr_days`` days completed by ``now`` (day end <= now) over the last completed
    close; None until that many days exist. The first bar without a previous close uses its high - low."""
    completed = [day for day in days if day.start + DAY_SECONDS <= now]
    if len(completed) < atr_days:
        return None
    window = completed[-atr_days:]
    ranges = [day.high - day.low if day.previous_close is None
              else max(day.high - day.low, abs(day.high - day.previous_close), abs(day.low - day.previous_close))
              for day in window]
    return sum(ranges, Decimal('0')) / atr_days / window[-1].close


def runner_exit_fill(*, next_close, cost_floor) -> Tuple[str, Decimal]:
    """An exit decided on a closed bar fills at the next bar's close (``MARKET``) unless that is below the cost
    floor; then a limit rests at the cost floor (``LIMIT``) and fills on touch."""
    close = decimal(next_close, 'runner exit close', positive=True)
    floor = decimal(cost_floor, 'runner cost floor', positive=True)
    return ('MARKET', close) if close >= floor else ('LIMIT', floor)


def runner_limit_fill(limit, path) -> Optional[Tuple[int, Decimal]]:
    """(path point, fill price) of a resting runner limit on a bar's price path: the first point at or above it,
    filled at the limit price; None when the bar never reaches it."""
    price = decimal(limit, 'runner limit', positive=True)
    for point, value in enumerate(path):
        if decimal(value, 'path price', positive=True) >= price:
            return point, price
    return None


@dataclass(frozen=True)
class RunnerLot:
    base: Decimal
    cost_basis: Decimal
    cost_floor: Decimal
    high_water: Decimal
    floor: Decimal


@dataclass(frozen=True)
class RunnerStep:
    lot: RunnerLot
    exit: bool
    floor_changed: bool


def runner_bar(cfg: RunnerConfig, lot: RunnerLot, *, high, close, atr_pct) -> RunnerStep:
    """One closed bar for an open runner: floor from the high water of earlier bars, the exit check on the close,
    then (only when it stays open) the high water takes this bar's high, so no bar sees its own high first."""
    floor = runner_floor(cfg, previous_floor=lot.floor, cost_floor=lot.cost_floor, high_water=lot.high_water,
                         atr_pct=atr_pct)
    changed = floor != lot.floor
    if decimal(close, 'close', positive=True) <= floor:
        return RunnerStep(RunnerLot(lot.base, lot.cost_basis, lot.cost_floor, lot.high_water, floor), True, changed)
    return RunnerStep(RunnerLot(lot.base, lot.cost_basis, lot.cost_floor,
                                max(lot.high_water, decimal(high, 'high', positive=True)), floor), False, changed)
