from .config import SignalPolicy
from .domain import PolicyError, SignalDecision, decimal


def signal_decision(policy: SignalPolicy, *, previous_rsi, rsi, breakout, trend_up,
                    turn=False, score=0, atr_fraction=None):
    previous = decimal(previous_rsi, 'previous RSI')
    current = decimal(rsi, 'RSI')
    if (previous > 100 or current > 100 or type(breakout) is not bool or type(trend_up) is not bool
            or type(turn) is not bool or type(score) is not int or score < 0):
        raise PolicyError('invalid signal context')
    if policy.min_atr_fraction is not None or policy.max_atr_fraction is not None:
        if atr_fraction is None:
            return SignalDecision(False, 'atr_unavailable')
        volatility = decimal(atr_fraction, 'ATR fraction')
        if policy.min_atr_fraction is not None and volatility < policy.min_atr_fraction:
            return SignalDecision(False, 'low_volatility')
        if policy.max_atr_fraction is not None and volatility > policy.max_atr_fraction:
            return SignalDecision(False, 'high_volatility')
    if policy.kind == 'rsi_rebound':
        enter = previous <= policy.oversold < current
        return SignalDecision(enter, 'rsi_rebound' if enter else 'no_rebound')
    if policy.kind == 'rsi_turn':
        if not turn:
            return SignalDecision(False, 'no_turn')
        if score < policy.minimum_score:
            return SignalDecision(False, 'score_below_minimum')
        return SignalDecision(True, 'rsi_turn')
    enter = breakout and (trend_up or not policy.require_uptrend)
    return SignalDecision(enter, 'breakout' if enter else 'no_breakout')
