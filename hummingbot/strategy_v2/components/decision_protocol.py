"""Pure, bounded eligibility replay. No orders, reservations, fills or capital authority."""
from decimal import Decimal
from typing import Literal

from pydantic import Field, StrictBool, model_validator

from .config import PolicyModel
from .domain import PolicyError, decimal
from .recipe_contract import digest

HASH = r'^[0-9a-f]{64}$'
NAME = r'^[A-Za-z0-9_.:-]{1,128}$'


class DecisionScope(PolicyModel):
    program_id: str = Field(pattern=NAME)
    owner_id: str = Field(pattern=NAME)
    connector: str = Field(pattern=r'^[a-z0-9_]{1,80}$')
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$', max_length=80)
    recipe_sha256: str = Field(pattern=HASH)
    acquisition_allowed: StrictBool = True


class ObservationRef(PolicyModel):
    dataset_sha256: str = Field(pattern=HASH)
    epoch: str = Field(pattern=NAME)
    revision: int = Field(ge=1, strict=True)
    content_sha256: str = Field(pattern=HASH)
    event_time_ms: int = Field(ge=0, strict=True)
    available_time_ms: int = Field(ge=0, strict=True)

    @model_validator(mode='after')
    def causal(self):
        if self.available_time_ms < self.event_time_ms:
            raise ValueError('availability cannot precede the observation event')
        return self


class Opportunity(PolicyModel):
    scope: DecisionScope
    setup: str = Field(pattern=NAME)
    action: Literal['acquire', 'reduce']
    primary: ObservationRef
    dependencies: tuple[ObservationRef, ...] = Field(default=(), max_length=64)
    created_at_ms: int = Field(ge=0, strict=True)
    lifetime_ms: int = Field(ge=1, le=86_400_000, strict=True)
    expected_return: Decimal | None = None

    @model_validator(mode='after')
    def valid_evidence(self):
        refs = (self.primary, *self.dependencies)
        if len({ref.dataset_sha256 for ref in refs}) != len(refs):
            raise ValueError('one immutable observation per required dataset is required')
        if any(ref.available_time_ms > self.created_at_ms for ref in refs):
            raise ValueError('input was not available when opportunity was created')
        if not self.primary.available_time_ms <= self.created_at_ms < self.expires_at_ms:
            raise ValueError('opportunity is outside its fixed evidence-relative lifetime')
        if self.expected_return is not None:
            decimal(self.expected_return, 'expected return', minimum=None)
        return self

    @property
    def expires_at_ms(self):
        return self.primary.available_time_ms + self.lifetime_ms

    @property
    def opportunity_id(self):
        return digest(self.model_dump(mode='json', exclude={'created_at_ms'}))

    @property
    def consumption_key(self):
        # Revisions/reconnects cannot turn an already observed event into a new entry.
        return digest([self.scope.model_dump(mode='json'), self.setup, self.action,
                       self.primary.dataset_sha256, self.primary.event_time_ms])


class Prerequisite(PolicyModel):
    name: str = Field(pattern=NAME)
    status: Literal['PASS', 'HOLD', 'UNAVAILABLE', 'STALE']
    evidence_sha256: str | None = Field(default=None, pattern=HASH)

    @model_validator(mode='after')
    def sourced_pass(self):
        if self.status == 'PASS' and self.evidence_sha256 is None:
            raise ValueError('a passing prerequisite requires source evidence')
        return self


class DecisionCursor(PolicyModel):
    scope: DecisionScope
    setup: str = Field(pattern=NAME)
    action: Literal['acquire', 'reduce']
    primary_dataset_sha256: str = Field(pattern=HASH)
    last_event_ms: int = Field(ge=0, strict=True)
    last_opportunity_id: str = Field(pattern=HASH)
    last_clock_ms: int = Field(ge=0, strict=True)


class DecisionTrace(PolicyModel):
    opportunity_id: str = Field(pattern=HASH)
    consumption_key: str = Field(pattern=HASH)
    decision_time_ms: int = Field(ge=0, strict=True)
    status: Literal['HOLD', 'ELIGIBLE_FOR_NATIVE_ADMISSION']
    reason: str = Field(pattern=NAME)
    blockers: tuple[str, ...] = ()
    prerequisite_sha256: str = Field(pattern=HASH)
    execution_authorized: Literal[False] = False


def decide(previous: DecisionCursor | None, opportunity: Opportunity, *, now_ms: int,
           prerequisites: tuple[Prerequisite, ...]) -> tuple[DecisionCursor, DecisionTrace]:
    opportunity = Opportunity.model_validate(opportunity.model_dump(mode='json'))
    if type(now_ms) is not int or now_ms < opportunity.created_at_ms:
        raise PolicyError('decision clock precedes opportunity creation')
    if len(prerequisites) > 64:
        raise PolicyError('bounded prerequisite set required')
    checks = tuple(Prerequisite.model_validate(item.model_dump(mode='json')) for item in prerequisites)
    if len({item.name for item in checks}) != len(checks):
        raise PolicyError('duplicate prerequisite identities')
    checks = tuple(sorted(checks, key=lambda item: item.name))
    event = opportunity.primary.event_time_ms
    if previous is not None:
        previous = DecisionCursor.model_validate(previous.model_dump(mode='json'))
        if (previous.scope != opportunity.scope or previous.setup != opportunity.setup
                or previous.action != opportunity.action
                or previous.primary_dataset_sha256 != opportunity.primary.dataset_sha256):
            raise PolicyError('decision cursor scope differs from the opportunity')
        if now_ms < previous.last_clock_ms or event < previous.last_event_ms:
            raise PolicyError('decision clock or observed event regressed')
    blockers = tuple(f'{item.name}:{item.status}' for item in checks if item.status != 'PASS')
    reason = 'eligible'
    if opportunity.action == 'acquire' and not opportunity.scope.acquisition_allowed:
        reason = 'acquisition_forbidden'
    elif now_ms >= opportunity.expires_at_ms:
        reason = 'opportunity_expired'
    elif previous is None:
        reason = 'seeded'
    elif event == previous.last_event_ms:
        reason = ('already_observed' if previous.last_opportunity_id == opportunity.opportunity_id
                  else 'revised_observation_requires_new_event')
    elif blockers:
        reason = 'prerequisite_hold'
    revised = DecisionCursor(scope=opportunity.scope, setup=opportunity.setup, action=opportunity.action,
                             primary_dataset_sha256=opportunity.primary.dataset_sha256, last_event_ms=event,
                             last_opportunity_id=opportunity.opportunity_id, last_clock_ms=now_ms)
    trace = DecisionTrace(opportunity_id=opportunity.opportunity_id, consumption_key=opportunity.consumption_key,
                          decision_time_ms=now_ms, status='ELIGIBLE_FOR_NATIVE_ADMISSION' if reason == 'eligible' else 'HOLD',
                          reason=reason, blockers=blockers, prerequisite_sha256=digest([item.model_dump(mode='json') for item in checks]))
    return revised, trace


def replay(events, *, max_events: int = 10000) -> dict:
    if type(max_events) is not int or not 1 <= max_events <= 100000:
        raise PolicyError('bounded replay capacity required')
    state, traces = None, []
    for opportunity, now_ms, prerequisites in events:
        if len(traces) >= max_events:
            raise PolicyError('bounded replay event capacity exceeded')
        state, trace = decide(state, opportunity, now_ms=now_ms, prerequisites=prerequisites)
        traces.append(trace.model_dump(mode='json'))
    payload = {'schema_version': 1, 'economic_backtest': False, 'execution_authorized': False,
               'eligible_count': sum(item['status'] == 'ELIGIBLE_FOR_NATIVE_ADMISSION' for item in traces),
               'traces': traces, 'final_state': None if state is None else state.model_dump(mode='json')}
    return {**payload, 'replay_sha256': digest(payload)}
