"""Versioned event recipe. Pure reducer; never calls exchange I/O or mutates decide()."""
from typing import Literal

from pydantic import Field, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import (
    DecisionScope,
    DecisionTrace,
    ObservationRef,
    Opportunity,
    Prerequisite,
    decide,
)
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.events_v2.contracts import EventEvidenceV2, QualifiedEventSnapshotV2
from hummingbot.strategy_v2.components.events_v2.validation import validate_preexisting_market
from hummingbot.strategy_v2.components.recipe_contract import (
    RecipeBinding,
    configuration_digest,
    digest,
    verify_binding,
)

HASH = r'^[0-9a-f]{64}$'
NAME = r'^[A-Za-z0-9_.:-]{1,128}$'
Phase = Literal['WAIT', 'HOLD', 'READY_RESEARCH_ONLY', 'REJECTED', 'VETO_HOLD']


class EventRecipeV2(PolicyModel):
    recipe_id: Literal['cve_observe_v2', 'cve_entry_guard_v2', 'cve_listing_v2']
    binding: RecipeBinding
    program_id: str = Field(pattern=NAME)
    owner_id: str = Field(pattern=NAME)
    connector: str = Field(pattern=r'^[a-z0-9_]{1,80}$')
    wait_ms: int = Field(ge=0, lt=1_200_000, strict=True)
    lifetime_ms: int = Field(ge=1, le=1_200_000, strict=True)
    capital_model: Literal['SPOT_UNLEVERAGED'] = 'SPOT_UNLEVERAGED'
    capability_sha256: str = Field(pattern=HASH)
    module_graph_sha256: str = Field(pattern=HASH)

    @model_validator(mode='after')
    def bound_schedule(self):
        if self.wait_ms >= self.lifetime_ms:
            raise ValueError('delay must be shorter than event lifetime')
        if self.recipe_id != self.binding.recipe_id:
            raise ValueError('recipe id does not match its binding')
        body = self.model_dump(mode='json', exclude={'binding'})
        if self.binding.recipe_sha256 != digest(body):
            raise ValueError('recipe content hash does not match its binding')
        return self


class EventSetupRecordV2(PolicyModel):
    consumption_id: str = Field(pattern=HASH)
    occurrence_id: str
    asset_id: str
    kind: str
    phase: Phase
    first_seen_ms: int
    wait_until_ms: int
    expires_at_ms: int
    last_clock_ms: int
    last_event_sha256: str = Field(pattern=HASH)
    native_claim_id: str | None = None
    reported: bool = False

    @model_validator(mode='after')
    def causal_schedule(self):
        if self.wait_until_ms < self.first_seen_ms or self.expires_at_ms <= self.wait_until_ms:
            raise ValueError('corrupted event schedule')
        if self.last_clock_ms < self.first_seen_ms:
            raise ValueError('setup clock precedes first observation')
        return self


class EventSetupStateV2(PolicyModel):
    recipe: EventRecipeV2
    last_clock_ms: int = Field(ge=0, strict=True)
    snapshot_sequence: int = Field(ge=0, strict=True)
    setups: tuple[EventSetupRecordV2, ...] = Field(default=(), max_length=10_000)


class EventStepV2(PolicyModel):
    state: EventSetupStateV2
    opportunities: tuple[Opportunity, ...] = ()
    traces: tuple[DecisionTrace, ...] = ()
    prerequisites: tuple[Prerequisite, ...] = ()
    reasons: tuple[str, ...] = ()
    execution_authorized: Literal[False] = False


def consumption_id(recipe: EventRecipeV2, evidence: EventEvidenceV2) -> str:
    return digest([recipe.recipe_id, recipe.owner_id, evidence.asset_id, evidence.source_id, evidence.occurrence_id])


def bind_observe_recipe(*, source_sha256: str, configuration: dict, wait_ms: int = 60_000,
                        lifetime_ms: int = 1_200_000, program_id: str = 'okx_spot_events_v2',
                        owner_id: str = 'observer', connector: str = 'okx',
                        recipe_id: str = 'cve_observe_v2') -> EventRecipeV2:
    capability = digest(['observe', 'no_orders', recipe_id])
    modules = digest(['events_v2', 'cross_venue_events_v2', recipe_id])
    body = dict(recipe_id=recipe_id, program_id=program_id, owner_id=owner_id, connector=connector,
                wait_ms=wait_ms, lifetime_ms=lifetime_ms, capital_model='SPOT_UNLEVERAGED',
                capability_sha256=capability, module_graph_sha256=modules)
    binding = RecipeBinding(recipe_id=recipe_id, recipe_sha256=digest(body),
                            source_sha256=source_sha256, configuration_sha256=configuration_digest(configuration))
    return EventRecipeV2(binding=binding, **body)


def _observation(snapshot: QualifiedEventSnapshotV2, evidence: EventEvidenceV2) -> ObservationRef:
    return ObservationRef(
        dataset_sha256=snapshot.dataset_sha256, epoch=evidence.source_id, revision=evidence.revision,
        content_sha256=evidence.content_sha256, event_time_ms=evidence.event_available_ms,
        available_time_ms=evidence.event_available_ms)


def _opportunity(recipe: EventRecipeV2, snapshot: QualifiedEventSnapshotV2, evidence: EventEvidenceV2,
                 *, now_ms: int, expires_at_ms: int) -> Opportunity | None:
    if evidence.instrument_pair is None:
        return None
    remaining = expires_at_ms - now_ms
    if remaining < 1:
        return None
    scope = DecisionScope(program_id=recipe.program_id, owner_id=recipe.owner_id, connector=recipe.connector,
                          trading_pair=evidence.instrument_pair, recipe_sha256=recipe.binding.recipe_sha256,
                          acquisition_allowed=False)
    return Opportunity(scope=scope, setup=recipe.recipe_id, action='acquire',
                       primary=_observation(snapshot, evidence), created_at_ms=now_ms, lifetime_ms=remaining)


def _coverage(snapshot: QualifiedEventSnapshotV2, source_id: str) -> tuple[str, bool, bool]:
    for item in snapshot.completeness:
        if item.source_id == source_id:
            return item.coverage_state, item.complete_for_entry, item.complete_for_risk
    return 'UNAVAILABLE', False, False


def step(state: EventSetupStateV2 | None, snapshot: QualifiedEventSnapshotV2, *, now_ms: int,
         recipe: EventRecipeV2, configuration: dict | None = None) -> EventStepV2:
    recipe = EventRecipeV2.model_validate(recipe.model_dump(mode='json'))
    snapshot = QualifiedEventSnapshotV2.model_validate(snapshot.model_dump(mode='json'))
    if type(now_ms) is not int or now_ms < snapshot.asof_ms:
        raise PolicyError('decision clock precedes snapshot availability')
    if configuration is not None:
        verify_binding(recipe.binding, configuration, source_sha256=recipe.binding.source_sha256)
    if state is None:
        state = EventSetupStateV2(recipe=recipe, last_clock_ms=0, snapshot_sequence=0, setups=())
    else:
        state = EventSetupStateV2.model_validate(state.model_dump(mode='json'))
        if state.recipe.binding != recipe.binding or state.recipe.recipe_id != recipe.recipe_id:
            raise PolicyError('recipe or source identity changed; explicit revision required')
        if now_ms < state.last_clock_ms:
            raise PolicyError('event clock regressed')
        if snapshot.sequence < state.snapshot_sequence:
            raise PolicyError('snapshot sequence regressed')
    known = {item.consumption_id: item for item in state.setups}
    reasons, prereqs, opportunities, setups = [], [], [], []
    seen = set()
    for evidence in snapshot.events:
        key = consumption_id(recipe, evidence)
        if key in seen:
            raise PolicyError('duplicate occurrence in one step')
        seen.add(key)
        previous = known.get(key)
        wait_until = previous.wait_until_ms if previous else evidence.event_available_ms + recipe.wait_ms
        expires = previous.expires_at_ms if previous else evidence.event_available_ms + recipe.lifetime_ms
        if wait_until < evidence.event_available_ms or expires <= wait_until:
            raise PolicyError('corrupted event schedule')
        coverage, entry_ok, _risk_ok = _coverage(snapshot, evidence.source_id)
        phase: Phase
        reason: str
        if evidence.negative:
            phase, reason = 'VETO_HOLD', 'negative_event_veto'
            prereqs.append(Prerequisite(name=f'veto.{key}', status='HOLD', evidence_sha256=evidence.content_sha256))
        elif coverage == 'UNAVAILABLE':
            phase, reason = 'HOLD', 'source_coverage_unavailable'
            prereqs.append(Prerequisite(name=f'coverage.{evidence.source_id}', status='UNAVAILABLE'))
        elif evidence.asset_id not in snapshot.observation_universe:
            phase, reason = 'HOLD', 'asset_not_in_observation_universe'
            prereqs.append(Prerequisite(name=f'observation.{key}', status='HOLD'))
        elif recipe.recipe_id != 'cve_observe_v2' and evidence.asset_id not in snapshot.entry_universe:
            phase, reason = 'HOLD', 'asset_not_in_entry_universe'
            prereqs.append(Prerequisite(name=f'entry_scope.{key}', status='HOLD'))
        elif now_ms < wait_until and now_ms < expires:
            phase, reason = 'WAIT', 'event_wait'
            prereqs.append(Prerequisite(name=f'wait.{key}', status='HOLD'))
        elif now_ms >= expires:
            phase, reason = 'REJECTED', 'event_expired'
        elif recipe.recipe_id == 'cve_listing_v2' and evidence.kind != 'SPOT_LISTING_ANNOUNCED':
            phase, reason = 'REJECTED', 'event_type_not_admitted'
        elif recipe.recipe_id == 'cve_listing_v2':
            if evidence.first_public_ms is None or evidence.listed_ms is None:
                phase, reason = 'HOLD', 'listing_market_evidence_unavailable'
                prereqs.append(Prerequisite(name=f'public.{key}', status='UNAVAILABLE'))
            else:
                try:
                    validate_preexisting_market(first_public_ms=evidence.first_public_ms,
                                                listed_ms=evidence.listed_ms)
                    phase, reason = 'READY_RESEARCH_ONLY', 'ready_research_only'
                except ValueError:
                    phase, reason = 'REJECTED', 'okx_market_not_preexisting'
        elif recipe.recipe_id == 'cve_entry_guard_v2' and not entry_ok:
            phase, reason = 'HOLD', 'entry_coverage_incomplete'
            prereqs.append(Prerequisite(name=f'entry.{evidence.source_id}', status='HOLD'))
        else:
            phase, reason = 'READY_RESEARCH_ONLY', 'ready_research_only'
        if previous is not None and previous.phase == 'VETO_HOLD':
            phase, reason = 'VETO_HOLD', 'negative_event_veto'
        elif previous is not None and previous.phase == 'REJECTED':
            phase, reason = 'REJECTED', 'setup_already_rejected'
        reported = bool(previous.reported) if previous is not None else False
        already_reported = previous is not None and previous.reported and previous.phase == phase
        record = EventSetupRecordV2(
            consumption_id=key, occurrence_id=evidence.occurrence_id, asset_id=evidence.asset_id,
            kind=evidence.kind, phase=phase, first_seen_ms=(previous.first_seen_ms if previous else evidence.event_available_ms),
            wait_until_ms=wait_until, expires_at_ms=expires, last_clock_ms=now_ms,
            last_event_sha256=evidence.content_sha256,
            native_claim_id=previous.native_claim_id if previous else None,
            reported=True if phase in ('READY_RESEARCH_ONLY', 'VETO_HOLD', 'REJECTED') else reported)
        setups.append(record)
        if already_reported or (previous is not None and previous.native_claim_id is not None):
            continue
        reasons.append(reason)
        if phase == 'READY_RESEARCH_ONLY':
            opportunity = _opportunity(recipe, snapshot, evidence, now_ms=now_ms,
                                       expires_at_ms=expires)
            if opportunity is not None:
                opportunities.append(opportunity)
    for item in state.setups:
        if item.consumption_id not in seen:
            if item.phase == 'VETO_HOLD':
                setups.append(item.model_copy(update={'last_clock_ms': now_ms}))
                prereqs.append(Prerequisite(name=f'veto.{item.consumption_id}', status='HOLD',
                                            evidence_sha256=item.last_event_sha256))
            elif item.native_claim_id is not None or item.phase != 'WAIT':
                setups.append(item.model_copy(update={'last_clock_ms': now_ms}))
    names = [item.name for item in prereqs]
    if len(names) != len(set(names)):
        unique, seen_names = [], set()
        for item in prereqs:
            if item.name in seen_names:
                continue
            seen_names.add(item.name)
            unique.append(item)
        prereqs = unique
    next_state = EventSetupStateV2(recipe=recipe, last_clock_ms=now_ms, snapshot_sequence=snapshot.sequence,
                                   setups=tuple(setups))
    traces = tuple(decide(None, opportunity, now_ms=now_ms, prerequisites=())[1]
                   for opportunity in opportunities)
    return EventStepV2(state=next_state, opportunities=tuple(opportunities), traces=traces,
                       prerequisites=tuple(prereqs), reasons=tuple(reasons))
