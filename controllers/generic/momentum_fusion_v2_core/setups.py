"""Explicit setup state machines. Not an order ledger and not a mutable feature."""
from decimal import Decimal
from typing import Literal

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.domain import decimal
from pydantic import Field

Phase = Literal['IDLE', 'ARMED', 'CONFIRMED', 'PROPOSED', 'INVALIDATED', 'EXPIRED', 'COOLDOWN']
SetupName = Literal['b0_breakout', 'c1_shelf', 'c2_compression', 'c3_reclaim']


class SetupState(PolicyModel):
    setup: SetupName
    phase: Phase = 'IDLE'
    reason: str = 'idle'
    armed_event_ms: int | None = None
    confirmed_event_ms: int | None = None
    reference_price: Decimal | None = None
    invalidation_price: Decimal | None = None
    expires_at_ms: int | None = None
    last_event_ms: int = Field(default=0, ge=0, strict=True)
    cooldown_until_ms: int | None = None
    shelf_bars: int = Field(default=0, ge=0, strict=True)


def idle(setup: SetupName) -> SetupState:
    return SetupState(setup=setup)


def _advance(state: SetupState, **changes) -> SetupState:
    payload = state.model_dump(mode='json')
    payload.update(changes)
    return SetupState.model_validate(payload)


def _cooldown(state: SetupState, now_ms: int, cooldown_ms: int, reason: str, phase: Phase) -> SetupState:
    return _advance(state, phase=phase, reason=reason, cooldown_until_ms=now_ms + cooldown_ms,
                    expires_at_ms=None)


def _maybe_rearm(state: SetupState, now_ms: int) -> SetupState | None:
    if state.phase in {'COOLDOWN', 'INVALIDATED', 'EXPIRED', 'PROPOSED'} and state.cooldown_until_ms is not None:
        if now_ms >= state.cooldown_until_ms:
            return idle(state.setup)
    return None


def step_b0(state: SetupState, *, close, prior_high, event_ms, now_ms, lifetime_ms, cooldown_ms) -> SetupState:
    rearmed = _maybe_rearm(state, now_ms)
    if rearmed is not None:
        state = rearmed
    close = decimal(close, 'close', positive=True)
    prior_high = decimal(prior_high, 'prior high', positive=True)
    if state.phase == 'IDLE':
        if close > prior_high:
            return _advance(state, phase='CONFIRMED', reason='channel_break', confirmed_event_ms=event_ms,
                            last_event_ms=event_ms, reference_price=prior_high, invalidation_price=prior_high,
                            expires_at_ms=now_ms + lifetime_ms)
        return _advance(state, last_event_ms=event_ms, reason='no_break')
    if state.phase == 'CONFIRMED':
        return _advance(state, phase='PROPOSED', reason='b0_proposed', last_event_ms=event_ms)
    if state.phase == 'PROPOSED':
        return _cooldown(state, now_ms, cooldown_ms, 'b0_consumed', 'COOLDOWN')
    return _advance(state, last_event_ms=event_ms)


def step_c1(state: SetupState, *, close, high, low, impulse, bandwidth, event_ms, now_ms,
            max_pullback, max_bandwidth, min_shelf_bars, lifetime_ms, cooldown_ms) -> SetupState:
    rearmed = _maybe_rearm(state, now_ms)
    if rearmed is not None:
        state = rearmed
    close = decimal(close, 'close', positive=True)
    high = decimal(high, 'high', positive=True)
    low = decimal(low, 'low', positive=True)
    bandwidth = decimal(bandwidth, 'bandwidth')
    pullback = decimal(max_pullback, 'max pullback', positive=True)
    cap = decimal(max_bandwidth, 'max bandwidth', positive=True)
    if state.phase == 'IDLE':
        if int(impulse) == 1:
            return _advance(state, phase='ARMED', reason='impulse_armed', armed_event_ms=event_ms,
                            last_event_ms=event_ms, reference_price=high, invalidation_price=low,
                            expires_at_ms=now_ms + lifetime_ms)
        return _advance(state, last_event_ms=event_ms, reason='no_impulse')
    if state.expires_at_ms is not None and now_ms >= state.expires_at_ms:
        return _cooldown(state, now_ms, cooldown_ms, 'candidate_expired', 'EXPIRED')
    if state.phase == 'ARMED':
        floor = decimal(state.invalidation_price, 'invalidation', positive=True)
        ceiling = decimal(state.reference_price, 'reference', positive=True)
        if close < floor:
            return _cooldown(state, now_ms, cooldown_ms, 'false_shelf', 'INVALIDATED')
        depth = (ceiling - close) / ceiling
        needed = int(min_shelf_bars)
        if close > ceiling:
            if state.shelf_bars >= needed:
                return _advance(state, phase='CONFIRMED', reason='shelf_break', confirmed_event_ms=event_ms,
                                last_event_ms=event_ms)
            return _cooldown(state, now_ms, cooldown_ms, 'overextended_break', 'INVALIDATED')
        if depth > pullback:
            return _cooldown(state, now_ms, cooldown_ms, 'false_shelf', 'INVALIDATED')
        if bandwidth > cap:
            return _advance(state, last_event_ms=event_ms, reason='false_shelf')
        return _advance(state, last_event_ms=event_ms, reason='shelf_forming', shelf_bars=state.shelf_bars + 1)
    if state.phase == 'CONFIRMED':
        return _advance(state, phase='PROPOSED', reason='c1_proposed', last_event_ms=event_ms)
    if state.phase == 'PROPOSED':
        return _cooldown(state, now_ms, cooldown_ms, 'c1_consumed', 'COOLDOWN')
    return _advance(state, last_event_ms=event_ms)


def step_c2(state: SetupState, *, close, prior_high, bandwidth, expansion_ratio, event_ms, now_ms,
            compression_max, expand_min, lifetime_ms, cooldown_ms) -> SetupState:
    rearmed = _maybe_rearm(state, now_ms)
    if rearmed is not None:
        state = rearmed
    close = decimal(close, 'close', positive=True)
    prior_high = decimal(prior_high, 'prior high', positive=True)
    bandwidth = decimal(bandwidth, 'bandwidth')
    expansion = decimal(expansion_ratio, 'expansion')
    if state.phase == 'IDLE':
        if bandwidth <= decimal(compression_max, 'compression max'):
            return _advance(state, phase='ARMED', reason='compressed', armed_event_ms=event_ms,
                            last_event_ms=event_ms, reference_price=prior_high, invalidation_price=close,
                            expires_at_ms=now_ms + lifetime_ms)
        return _advance(state, last_event_ms=event_ms, reason='no_compression')
    if state.expires_at_ms is not None and now_ms >= state.expires_at_ms:
        return _cooldown(state, now_ms, cooldown_ms, 'candidate_expired', 'EXPIRED')
    if state.phase == 'ARMED':
        if close > decimal(state.reference_price, 'reference', positive=True) and expansion >= decimal(expand_min, 'expand min', positive=True):
            return _advance(state, phase='CONFIRMED', reason='compression_break', confirmed_event_ms=event_ms,
                            last_event_ms=event_ms)
        return _advance(state, last_event_ms=event_ms, reason='awaiting_expansion')
    if state.phase == 'CONFIRMED':
        return _advance(state, phase='PROPOSED', reason='c2_proposed', last_event_ms=event_ms)
    if state.phase == 'PROPOSED':
        return _cooldown(state, now_ms, cooldown_ms, 'c2_consumed', 'COOLDOWN')
    return _advance(state, last_event_ms=event_ms)


def step_c3(state: SetupState, *, close, high, low, uptrend, event_ms, now_ms, pullback_max,
            lifetime_ms, cooldown_ms) -> SetupState:
    rearmed = _maybe_rearm(state, now_ms)
    if rearmed is not None:
        state = rearmed
    close = decimal(close, 'close', positive=True)
    high = decimal(high, 'high', positive=True)
    low = decimal(low, 'low', positive=True)
    limit = decimal(pullback_max, 'pullback max', positive=True)
    if state.phase == 'IDLE':
        if int(uptrend) == 1:
            return _advance(state, phase='ARMED', reason='htf_uptrend', armed_event_ms=event_ms,
                            last_event_ms=event_ms, reference_price=close, invalidation_price=low,
                            expires_at_ms=now_ms + lifetime_ms)
        return _advance(state, last_event_ms=event_ms, reason='no_uptrend')
    if state.expires_at_ms is not None and now_ms >= state.expires_at_ms:
        return _cooldown(state, now_ms, cooldown_ms, 'candidate_expired', 'EXPIRED')
    if state.phase == 'ARMED':
        reference = decimal(state.reference_price, 'reference', positive=True)
        depth = (reference - close) / reference
        if depth > limit or close < decimal(state.invalidation_price, 'invalidation', positive=True):
            return _cooldown(state, now_ms, cooldown_ms, 'pullback_invalid', 'INVALIDATED')
        if close >= reference and high >= reference:
            return _advance(state, phase='CONFIRMED', reason='reclaim', confirmed_event_ms=event_ms,
                            last_event_ms=event_ms)
        return _advance(state, last_event_ms=event_ms, reason='pullback')
    if state.phase == 'CONFIRMED':
        return _advance(state, phase='PROPOSED', reason='c3_proposed', last_event_ms=event_ms)
    if state.phase == 'PROPOSED':
        return _cooldown(state, now_ms, cooldown_ms, 'c3_consumed', 'COOLDOWN')
    return _advance(state, last_event_ms=event_ms)
