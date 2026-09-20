"""Explicit B0/C1 observation state; canonical traces never grant native authority."""
from decimal import Decimal
from typing import Literal

from pydantic import Field, field_validator, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import (
    DecisionCursor, DecisionScope, DecisionTrace, Opportunity, Prerequisite, decide,
)
from hummingbot.strategy_v2.components.domain import DataUnavailable, PolicyError
from hummingbot.strategy_v2.components.recipe_contract import digest

from .features import ObservedFeatures, clock_ms
from .recipe import HASH, MomentumRecipe, normalized

REQUIRED = frozenset({'owner_ready', 'risk', 'capital'})


class Shelf(PolicyModel):
    armed_event_ms: int = Field(ge=0, strict=True)
    armed_available_ms: int = Field(ge=0, strict=True)
    expires_at_ms: int = Field(ge=0, strict=True)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    atr: Decimal = Field(gt=0)
    shelf_high: Decimal = Field(gt=0)
    pv: Decimal = Field(gt=0)
    volume: Decimal = Field(gt=0)
    bars: int = Field(default=0, ge=0, le=10, strict=True)

    @model_validator(mode='after')
    def consistent(self):
        if (self.low >= self.high or self.armed_available_ms < self.armed_event_ms
                or self.expires_at_ms <= self.armed_available_ms):
            raise PolicyError('invalid frozen shelf geometry or clock')
        return self


class PolicyState(PolicyModel):
    schema_version: Literal[1] = 1
    scope: DecisionScope
    feature_contract_sha256: str = Field(pattern=HASH)
    last_input_sha256: str = Field(pattern=HASH)
    cursor: DecisionCursor
    epoch: str
    revision: int = Field(ge=1, strict=True)
    candidate: Shelf | None = None
    needs_reset: bool = Field(default=False, strict=True)
    issued: bool = Field(default=False, strict=True)

    @field_validator('schema_version', mode='before')
    @classmethod
    def strict_version(cls, value):
        if type(value) is not int:
            raise ValueError('state version must be an integer')
        return value

    @model_validator(mode='after')
    def coherent(self):
        if self.scope != self.cursor.scope or self.cursor.action != 'acquire':
            raise PolicyError('state cursor belongs to a different scope')
        if self.candidate is not None and (self.candidate.armed_event_ms > self.cursor.last_event_ms
                                          or self.issued or self.needs_reset):
            raise PolicyError('candidate conflicts with episode state')
        return self


class PolicyDecision(PolicyModel):
    reason: str
    trace: DecisionTrace
    reference_price: Decimal | None = None
    invalidation_price: Decimal | None = None
    candidate_expires_at_ms: int | None = None
    economic_backtest: Literal[False] = False


def _common(recipe, values):
    p = recipe.features
    return (values.trend_5m == values.trend_15m == 1
            and values.return_1m > p.min_return and values.relative_volume >= p.min_relative_volume)


def _recognize(recipe, candidate, needs_reset, values, event, available, now):
    p, s = recipe.features, recipe.shelf
    common = _common(recipe, values)
    breakout = common and values.close > values.prior_high + values.atr*p.breakout_buffer_atr
    if needs_reset:
        return None, values.close > values.prior_high, False, 'waiting_rearm', None, None
    if recipe.variant == 'B0':
        return None, False, breakout, 'breakout' if breakout else 'no_setup', values.prior_high, values.low
    if candidate is None:
        if not breakout or values.high <= values.low or values.volume <= 0:
            return None, False, False, 'no_setup', None, None
        typical = (values.high+values.low+values.close)/3
        new = Shelf(armed_event_ms=event, armed_available_ms=available, expires_at_ms=available+s.ttl_ms,
                    high=values.high, low=values.low, atr=values.atr, shelf_high=values.high,
                    pv=typical*values.volume, volume=values.volume)
        return new, False, False, 'shelf_armed', None, None
    if now >= candidate.expires_at_ms:
        return None, True, False, 'shelf_expired', None, None
    floor = candidate.high-(candidate.high-candidate.low)*s.max_pullback
    if values.low < floor or values.trend_5m != 1 or values.trend_15m != 1:
        return None, True, False, 'shelf_invalidated', None, None
    pv = candidate.pv + ((values.high+values.low+values.close)/3)*values.volume
    volume = candidate.volume+values.volume
    avwap = pv/volume
    reference = max(candidate.high, candidate.shelf_high)
    if common and values.close > reference + candidate.atr*p.breakout_buffer_atr:
        if candidate.bars < s.min_bars:
            return None, True, False, 'premature_breakout', None, None
        if values.close/avwap-1 > s.max_avwap_extension:
            return None, True, False, 'avwap_extension', None, None
        return None, True, True, 'shelf_confirmed', reference, floor
    if candidate.bars >= s.max_bars or values.high-values.low > (candidate.high-candidate.low)*s.range_ratio:
        return None, True, False, 'shelf_invalidated', None, None
    revised = Shelf.model_validate({**candidate.model_dump(), 'bars': candidate.bars+1,
        'shelf_high': max(candidate.shelf_high, values.high), 'pv': pv, 'volume': volume})
    return revised, False, False, 'shelf_building', None, None


def step(recipe: MomentumRecipe, previous: PolicyState | None, features: ObservedFeatures, *,
         scope: DecisionScope, now_ms: int, prerequisites: tuple[Prerequisite, ...]):
    recipe, now_ms = normalized(recipe), clock_ms(now_ms)
    scope = DecisionScope.model_validate(scope.model_dump(mode='json'))
    features = ObservedFeatures.model_validate(features.model_dump(mode='json'))
    if (scope.recipe_sha256 != recipe.digest or features.recipe_sha256 != recipe.digest
            or scope.connector != 'okx' or scope.trading_pair != recipe.symbol
            or not scope.program_id.startswith('momentum_v2_')
            or not scope.owner_id.startswith('momentum_v2_')
            or not scope.owner_id.endswith((':observation', ':shadow'))):
        raise PolicyError('recipe/feature/scope identity mismatch')
    refs = (features.primary, *features.dependencies)
    if tuple(ref.dataset_sha256 for ref in refs) != tuple(d.key for d in recipe.datasets):
        raise PolicyError('feature observations do not match recipe datasets')
    event, available = features.primary.event_time_ms, features.primary.available_time_ms
    if event % 60000 or not 0 <= now_ms-event <= recipe.max_candle_age_ms:
        raise DataUnavailable('invalid or stale primary decision bar')
    if any(not 0 <= now_ms-ref.available_time_ms <= recipe.max_transport_age_ms for ref in refs):
        raise DataUnavailable('feature evidence is unavailable or transport is stale')
    for dataset, ref in zip(recipe.datasets[1:], refs[1:]):
        expected = (event // (dataset.seconds*1000))*(dataset.seconds*1000)
        if ref.event_time_ms != expected:
            raise DataUnavailable('higher-timeframe event does not match the completed bucket')
    input_sha256 = digest([ref.model_dump(mode='json', exclude={'available_time_ms'}) for ref in refs])
    created = max(ref.available_time_ms for ref in refs)
    if created >= available + recipe.opportunity_lifetime_ms:
        raise DataUnavailable('dependencies arrived after opportunity expiry')
    if len(prerequisites) > len(REQUIRED):
        raise PolicyError('bounded declared prerequisite names required')
    supplied = tuple(Prerequisite.model_validate(x.model_dump(mode='json')) for x in prerequisites)
    if len({p.name for p in supplied}) != len(supplied) or any(p.name not in REQUIRED for p in supplied):
        raise PolicyError('unknown, reserved or repeated prerequisite')
    checks = {p.name: p for p in supplied}
    for name in REQUIRED-checks.keys():
        checks[name] = Prerequisite(name=name, status='UNAVAILABLE')
    previous = None if previous is None else PolicyState.model_validate(previous.model_dump(mode='json', warnings=False))
    candidate = None if previous is None else previous.candidate
    issued = False if previous is None else previous.issued
    reset = False if previous is None else previous.needs_reset
    old_cursor = None if previous is None else previous.cursor
    signal, reference, invalidation = False, None, None
    if previous is not None:
        if candidate is not None:
            if (recipe.variant != 'C1' or candidate.bars > recipe.shelf.max_bars
                    or candidate.armed_event_ms % 60000
                    or not 0 <= candidate.armed_available_ms-candidate.armed_event_ms <= recipe.max_candle_age_ms
                    or candidate.expires_at_ms != candidate.armed_available_ms+recipe.shelf.ttl_ms
                    or previous.cursor.last_event_ms != candidate.armed_event_ms+candidate.bars*60000):
                raise PolicyError('restored shelf exceeds bound recipe state')
        if previous.scope != scope or previous.feature_contract_sha256 != features.contract_sha256:
            raise PolicyError('state scope or feature contract changed; new revision required')
        if now_ms < previous.cursor.last_clock_ms or event < previous.cursor.last_event_ms:
            raise PolicyError('policy clock regressed')
        if features.primary.epoch == previous.epoch and features.primary.revision < previous.revision:
            raise PolicyError('input revision regressed')
    opportunity = Opportunity(scope=scope, setup=f'momentum_v2.{recipe.variant}', action='acquire',
        primary=features.primary, dependencies=features.dependencies, created_at_ms=created,
        lifetime_ms=recipe.opportunity_lifetime_ms)
    repeated = previous is not None and event == previous.cursor.last_event_ms
    if previous is None:
        reason = 'seeded'
    elif repeated:
        reason = 'already_observed'
        if input_sha256 != previous.last_input_sha256:
            candidate, reset, reason = None, True, 'revised_observation'
    elif issued:
        reason = 'episode_consumed'
    elif (features.primary.epoch != previous.epoch
          or features.primary.revision != previous.revision+1
          or event != previous.cursor.last_event_ms+60000):
        candidate, reset, reason = None, True, 'stream_reseed'
    else:
        candidate, reset, signal, reason, reference, invalidation = _recognize(
            recipe, candidate, reset, features.values, event, available, now_ms)
    checks['setup'] = Prerequisite(name='setup', status='PASS' if signal else 'HOLD',
                                  evidence_sha256=features.result_sha256 if signal else None)
    cursor, trace = decide(old_cursor, opportunity, now_ms=now_ms, prerequisites=tuple(checks.values()))
    eligible = trace.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    if signal and not eligible:
        candidate, reset, reason = None, True, 'prerequisite_hold'
    state = PolicyState(scope=scope, feature_contract_sha256=features.contract_sha256,
        last_input_sha256=input_sha256, cursor=cursor,
        epoch=features.primary.epoch, revision=features.primary.revision,
        candidate=candidate, needs_reset=reset, issued=issued or eligible)
    return state, PolicyDecision(reason=reason, trace=trace,
        reference_price=reference if signal else None, invalidation_price=invalidation if signal else None,
        candidate_expires_at_ms=None if candidate is None else candidate.expires_at_ms)
