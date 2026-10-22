from dataclasses import dataclass
from decimal import Decimal

from .config import RiskPolicy
from .domain import PolicyError, decimal


@dataclass(frozen=True)
class RiskState:
    peak: Decimal
    day: int
    day_start: Decimal
    paused: bool
    reason: str


def risk_update(policy: RiskPolicy, *, budget, pnl, day, peak, previous_day, day_start, paused):
    budget = decimal(budget, 'assigned budget', positive=True)
    pnl = decimal(pnl, 'native marked PnL', minimum=None)
    if type(day) is not int or day < 0 or type(paused) is not bool:
        raise PolicyError('invalid risk state')
    if previous_day is not None and (type(previous_day) is not int or day < previous_day):
        raise PolicyError('risk clock regressed')
    peak = max(pnl, Decimal('0') if peak is None else decimal(peak, 'risk peak', minimum=None))
    if previous_day != day:
        day_start = pnl
    else:
        day_start = decimal(day_start, 'daily starting PnL', minimum=None)
    reason = 'risk_latched' if paused else 'risk_clear'
    if peak - pnl >= budget * policy.max_drawdown_fraction:
        paused, reason = True, 'drawdown_limit'
    elif day_start - pnl >= budget * policy.max_daily_loss_fraction:
        paused, reason = True, 'daily_loss_limit'
    return RiskState(peak, day, day_start, paused, reason)
