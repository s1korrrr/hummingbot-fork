from .config import SignalPolicy
from .domain import PolicyError, SignalDecision, decimal


def signal_decision(policy: SignalPolicy, *, previous_rsi, rsi, breakout, trend_up):
    previous = decimal(previous_rsi, 'previous RSI')
    current = decimal(rsi, 'RSI')
    if previous > 100 or current > 100 or type(breakout) is not bool or type(trend_up) is not bool:
        raise PolicyError('invalid signal context')
    if policy.kind == 'rsi_rebound':
        enter = previous <= policy.oversold < current
        return SignalDecision(enter, 'rsi_rebound' if enter else 'no_rebound')
    enter = breakout and (trend_up or not policy.require_uptrend)
    return SignalDecision(enter, 'breakout' if enter else 'no_breakout')
