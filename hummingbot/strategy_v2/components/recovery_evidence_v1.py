"""Causal recovery evidence, independent of capital, alpha and execution policy."""
from decimal import Decimal
from fractions import Fraction
from typing import Literal

from pydantic import Field, field_validator, model_validator

from .config import PolicyModel
from .decision_protocol import HASH, NAME, ObservationRef
from .domain import PolicyError
from .market_stress_v1 import MAX_CLOCK_MS, StressEvaluation, bounded_decimal, checked, clock
from .recipe_contract import digest


class RecoveryParameters(PolicyModel):
    method: Literal['closed_reclaim_v1']
    stabilization_ms: int = Field(ge=60000, le=86_400_000, strict=True)
    reclaim_fraction: Decimal = Field(gt=0, lt=1)
    new_low_fraction: Decimal = Field(ge=0, lt=1)
    eligibility_ttl_ms: int = Field(ge=1, le=86_400_000, strict=True)

    _amount = field_validator('reclaim_fraction', 'new_low_fraction', mode='before')(bounded_decimal)

    @property
    def fingerprint(self):
        return digest(self.model_dump(mode='json'))


class RecoveryState(PolicyModel):
    parameters_sha256: str = Field(pattern=HASH)
    stress_parameters_sha256: str = Field(pattern=HASH)
    observation: ObservationRef
    clock_ms: int = Field(ge=0, le=MAX_CLOCK_MS, strict=True)
    phase: Literal['NORMAL', 'SHOCK_HOLD', 'STABILIZING', 'RECOVERY_ELIGIBLE']
    running_low: Decimal | None = Field(default=None, gt=0)
    qualifying_low: Decimal | None = Field(default=None, gt=0)
    stable_since_ms: int | None = Field(default=None, ge=0, le=MAX_CLOCK_MS, strict=True)
    eligible_until_ms: int | None = Field(default=None, ge=0, le=MAX_CLOCK_MS, strict=True)

    @model_validator(mode='after')
    def consistent(self):
        if self.clock_ms < self.observation.available_time_ms:
            raise ValueError('recovery checkpoint clock precedes availability')
        if self.phase == 'NORMAL':
            if any(x is not None for x in (self.running_low, self.qualifying_low,
                                           self.stable_since_ms, self.eligible_until_ms)):
                raise ValueError('normal checkpoint contains recovery state')
        else:
            if self.running_low is None or self.qualifying_low is None:
                raise ValueError('recovery checkpoint lost its low')
            bounded_decimal(self.running_low)
            bounded_decimal(self.qualifying_low)
            if self.running_low > self.qualifying_low:
                raise ValueError('running low exceeds qualifying reference')
        if self.phase in ('STABILIZING', 'RECOVERY_ELIGIBLE'):
            if self.stable_since_ms is None or self.stable_since_ms > self.clock_ms:
                raise ValueError('invalid stabilization clock')
        elif self.stable_since_ms is not None:
            raise ValueError('unexpected stabilization clock')
        if (self.phase == 'RECOVERY_ELIGIBLE') != (self.eligible_until_ms is not None):
            raise ValueError('eligibility checkpoint lacks a fixed deadline')
        return self


class RecoveryEvaluation(PolicyModel):
    state: RecoveryState | None
    evaluated_at_ms: int = Field(ge=0, le=MAX_CLOCK_MS, strict=True)
    status: Literal['PASS', 'HOLD', 'UNAVAILABLE', 'STALE']
    reason: str = Field(pattern=NAME)


def evaluate_recovery(state: RecoveryState | None, snapshot: StressEvaluation,
                      parameters: RecoveryParameters, now_ms: int) -> RecoveryEvaluation:
    now_ms = clock(now_ms)
    parameters = checked(RecoveryParameters, parameters)
    stress = checked(StressEvaluation, snapshot)
    if stress.evaluated_at_ms != now_ms:
        raise PolicyError('stress and recovery clocks differ')
    if state is not None:
        state = checked(RecoveryState, state)
        if state.parameters_sha256 != parameters.fingerprint or now_ms < state.clock_ms:
            raise PolicyError('recovery policy or clock changed')
        if stress.state is None:
            raise PolicyError('stress checkpoint is missing beside recovery state')
        if (state.stress_parameters_sha256 != stress.state.parameters_sha256
                or state.observation.dataset_sha256 != stress.observation.dataset_sha256
                or state.observation.event_time_ms > stress.observation.event_time_ms):
            raise PolicyError('recovery and stress identity differ')
        state = RecoveryState(**{**state.model_dump(), 'clock_ms': now_ms,
                                 'observation': stress.observation})
    elif stress.state is not None:
        if stress.reason != 'seeded':
            raise PolicyError('missing recovery checkpoint for existing stress history')
        state = RecoveryState(parameters_sha256=parameters.fingerprint,
                              stress_parameters_sha256=stress.state.parameters_sha256,
                              observation=stress.observation, clock_ms=now_ms, phase='NORMAL')

    def result(status, reason):
        return RecoveryEvaluation(state=state, evaluated_at_ms=now_ms, status=status, reason=reason)

    def change(**changes):
        return RecoveryState(**{**state.model_dump(), **changes})

    if state is None:
        return result(stress.status, stress.reason)
    if stress.shocked and stress.low is not None and (stress.fresh or stress.reason == 'seeded'):
        # A new market shock is not a new capital campaign. Do not reuse an old recovered low.
        low = (min(state.running_low, stress.low) if state.phase == 'SHOCK_HOLD'
               else stress.low)
        state = change(phase='SHOCK_HOLD', running_low=low, qualifying_low=low,
                       stable_since_ms=None, eligible_until_ms=None)
        return result('HOLD', 'shock_hold')
    if state.eligible_until_ms is not None and now_ms >= state.eligible_until_ms:
        state = change(phase='STABILIZING', stable_since_ms=now_ms, eligible_until_ms=None)
        return result('HOLD', 'recovery_expired')
    if not stress.fresh or stress.status != 'PASS':
        if stress.reason != 'already_observed' and state.phase in ('STABILIZING', 'RECOVERY_ELIGIBLE'):
            state = change(phase='STABILIZING', stable_since_ms=now_ms, eligible_until_ms=None)
        return result(stress.status if stress.status != 'PASS' else 'HOLD', stress.reason)
    if state.phase == 'NORMAL':
        return result('PASS', 'normal_market')
    event = stress.observation.event_time_ms
    low = min(state.running_low, stress.low)
    new_low = Fraction(stress.low) < Fraction(state.qualifying_low) * (1 - Fraction(parameters.new_low_fraction))
    if state.phase == 'SHOCK_HOLD' or new_low:
        state = change(phase='STABILIZING', running_low=low, qualifying_low=low,
                       stable_since_ms=event, eligible_until_ms=None)
        return result('HOLD', 'recovery_new_low' if new_low else 'stabilizing')
    state = change(running_low=low)
    if event - state.stable_since_ms < parameters.stabilization_ms:
        return result('HOLD', 'recovery_wait_time')
    reclaimed = Fraction(stress.close) >= Fraction(low) * (1 + Fraction(parameters.reclaim_fraction))
    if not reclaimed:
        if state.phase == 'RECOVERY_ELIGIBLE':
            state = change(phase='STABILIZING', stable_since_ms=event, eligible_until_ms=None)
        return result('HOLD', 'recovery_wait_reclaim')
    until = state.eligible_until_ms or stress.observation.available_time_ms + parameters.eligibility_ttl_ms
    if now_ms >= until:
        return result('HOLD', 'recovery_expired')
    state = change(phase='RECOVERY_ELIGIBLE', eligible_until_ms=until)
    return result('PASS', 'recovery_eligible')
