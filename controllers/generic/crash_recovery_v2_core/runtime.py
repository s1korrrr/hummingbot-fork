"""Pure observation/decision composition. Not a native runtime or an order adapter."""
from hashlib import sha256
from typing import Literal

from pydantic import Field, field_validator, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import (
    HASH,
    DecisionCursor,
    DecisionTrace,
    Opportunity,
    Prerequisite,
    decide,
)
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.market_stress_v1 import (
    MAX_CLOCK_MS,
    ReceivedCandles,
    StressEvaluation,
    StressState,
    checked,
    clock,
    evaluate_stress,
)
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, canonical_json, digest
from hummingbot.strategy_v2.components.recovery_evidence_v1 import (
    RecoveryEvaluation,
    RecoveryState,
    evaluate_recovery,
)
from hummingbot.strategy_v2.components.strict_json import strict_json

from .recipe import REQUIRED_ACQUISITION_CHECKS, CrashRecoveryRecipe, validate_binding

MAX_CHECKPOINT_BYTES = 2_000_000


class CompositionState(PolicyModel):
    schema_version: Literal[1] = 1
    binding_sha256: str = Field(pattern=HASH)
    clock_ms: int = Field(ge=0, le=MAX_CLOCK_MS, strict=True)
    stress: StressState | None = None
    recovery: RecoveryState | None = None
    acquire_cursor: DecisionCursor | None = None
    reduce_cursor: DecisionCursor | None = None

    @field_validator('schema_version', mode='before')
    @classmethod
    def integer_schema(cls, value):
        if type(value) is not int:
            raise ValueError('integer checkpoint schema required')
        return value

    @model_validator(mode='after')
    def consistent(self):
        if (self.stress is None) != (self.recovery is None):
            raise ValueError('partial market checkpoint')
        if self.stress is not None:
            if (self.stress.clock_ms != self.clock_ms or self.recovery.clock_ms != self.clock_ms
                    or self.stress.observation != self.recovery.observation
                    or self.stress.parameters_sha256 != self.recovery.stress_parameters_sha256):
                raise ValueError('market checkpoint identities differ')
        for cursor, action in ((self.acquire_cursor, 'acquire'), (self.reduce_cursor, 'reduce')):
            if cursor is not None and (cursor.action != action or cursor.last_clock_ms > self.clock_ms):
                raise ValueError('invalid decision checkpoint')
        return self


class CompositionResult(PolicyModel):
    state: CompositionState
    stress: StressEvaluation
    recovery: RecoveryEvaluation
    trace: DecisionTrace | None
    execution_authorized: Literal[False] = False


def evaluate(state: CompositionState | None, snapshot: ReceivedCandles | None,
             recipe: CrashRecoveryRecipe, binding: RecipeBinding, *, expected_source_sha256: str,
             now_ms: int, has_obligations: bool, opportunity: Opportunity | None = None,
             prerequisites: tuple[Prerequisite, ...] = ()) -> CompositionResult:
    """Caller supplies observations and native prerequisite declarations, never orders."""
    now_ms = clock(now_ms)
    if type(has_obligations) is not bool:
        raise PolicyError('explicit native-obligation indicator required')
    recipe, binding = validate_binding(recipe, binding, expected_source_sha256)
    identity = digest(binding.model_dump(mode='json'))
    if state is None:
        if has_obligations:
            raise PolicyError('missing checkpoint with native obligations')
        state = CompositionState(binding_sha256=identity, clock_ms=now_ms)
    else:
        state = checked(CompositionState, state)
        if state.binding_sha256 != identity or now_ms < state.clock_ms:
            raise PolicyError('composition identity or clock changed')
    if snapshot is not None:
        snapshot = checked(ReceivedCandles, snapshot)
        if snapshot.snapshot.dataset != recipe.dataset:
            raise PolicyError('observation dataset differs from recipe')
    se = evaluate_stress(state.stress, snapshot, recipe.stress, now_ms)
    re = evaluate_recovery(state.recovery, se, recipe.recovery, now_ms)
    cursors = {'acquire_cursor': state.acquire_cursor, 'reduce_cursor': state.reduce_cursor}
    trace = None
    if opportunity is not None:
        opportunity = checked(Opportunity, opportunity)
        if opportunity.scope != recipe.scope:
            raise PolicyError('opportunity belongs to another owner or recipe')
        if opportunity.lifetime_ms != recipe.lifetime_ms:
            raise PolicyError('opportunity lifetime differs from recipe')
        if type(prerequisites) is not tuple or len(prerequisites) > 56:
            raise PolicyError('bounded prerequisite tuple required')
        values = tuple(checked(Prerequisite, item) for item in prerequisites)
        if len({item.name for item in values}) != len(values) or any(item.name.startswith('crash.') for item in values):
            raise PolicyError('duplicate or reserved prerequisite name')
        checks = {item.name: item for item in values}
        if opportunity.action == 'acquire':
            if opportunity.dependencies:
                raise PolicyError('additional feature datasets are not supported by this recipe revision')
            for name in REQUIRED_ACQUISITION_CHECKS:
                checks.setdefault(name, Prerequisite(name=name, status='UNAVAILABLE'))
            valid_input = (snapshot is not None and opportunity.primary == se.observation
                           and (se.fresh or se.reason in ('seeded', 'already_observed')))
            checks['crash.observation'] = Prerequisite(
                name='crash.observation', status='PASS' if valid_input else 'HOLD',
                evidence_sha256=digest(se.model_dump(mode='json')))
            if recipe.mode != 'off':
                checks['crash.stress'] = Prerequisite(name='crash.stress', status=se.status,
                                                     evidence_sha256=digest(se.model_dump(mode='json')))
            if recipe.mode == 'recovery':
                checks['crash.recovery'] = Prerequisite(name='crash.recovery', status=re.status,
                                                       evidence_sha256=digest(re.model_dump(mode='json')))
        key = 'acquire_cursor' if opportunity.action == 'acquire' else 'reduce_cursor'
        cursors[key], trace = decide(cursors[key], opportunity, now_ms=now_ms, prerequisites=tuple(checks.values()))
    next_state = CompositionState(binding_sha256=identity, clock_ms=now_ms, stress=se.state,
                                  recovery=re.state, **cursors)
    return CompositionResult(state=next_state, stress=se, recovery=re, trace=trace)


def encode_checkpoint(state: CompositionState) -> str:
    """Encode decision state only; the native adapter must persist it before effects."""
    raw = canonical_json(checked(CompositionState, state))
    if len(raw.encode('utf-8')) > MAX_CHECKPOINT_BYTES:
        raise PolicyError('checkpoint exceeds capacity')
    return raw


def decode_checkpoint(raw: str, *, expected_sha256: str, binding: RecipeBinding) -> CompositionState:
    """Check against an independently retained receipt, not a digest inside the JSON."""
    if type(raw) is not str or len(raw) > MAX_CHECKPOINT_BYTES or len(raw.encode('utf-8')) > MAX_CHECKPOINT_BYTES:
        raise PolicyError('bounded checkpoint text required')
    if sha256(raw.encode('utf-8')).hexdigest() != expected_sha256:
        raise PolicyError('checkpoint receipt mismatch')
    binding = checked(RecipeBinding, binding)
    state = CompositionState.model_validate(strict_json(raw))
    if state.binding_sha256 != digest(binding.model_dump(mode='json')):
        raise PolicyError('checkpoint belongs to another binding')
    return state
