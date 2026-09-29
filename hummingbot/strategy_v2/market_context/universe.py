"""Point-in-time reference cohorts and lagged weights. Membership is observation scope, never entry authority."""
import math
import re
from decimal import Decimal
from typing import Literal, Mapping, Sequence

from pydantic import Field, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.recipe_contract import digest

from .contracts import (
    ASSET_PATTERN,
    INSTRUMENT_PATTERN,
    MAX_ASSETS,
    MAX_TIMESTAMP_MS,
    SHA256_PATTERN,
    VENUE_PATTERN,
    ClockView,
    ContextError,
    ReasonCode,
    canonical_reasons,
)

DAY_MS = 86_400_000
MAX_INSTRUMENTS = 2000
MAX_TURNOVER_ROWS = MAX_INSTRUMENTS * 31
UNIVERSE_ID_PATTERN = r'^[a-z0-9][a-z0-9_.-]{0,63}$'
WEIGHT_TOLERANCE = Decimal('1e-20')
Obligation = Literal['inventory', 'working_order', 'uncertain_fill', 'child_authority', 'recovery']


class InstrumentMetadata(PolicyModel):
    instrument_id: str = Field(pattern=INSTRUMENT_PATTERN)
    asset_id: str = Field(pattern=ASSET_PATTERN)
    venue: str = Field(pattern=VENUE_PATTERN)
    numeraire: str = Field(pattern=ASSET_PATTERN)
    classification: str | None = Field(pattern=r'^[a-z_]{1,32}$')
    listed_at_ms: int | None = Field(ge=0, le=MAX_TIMESTAMP_MS, strict=True)


class MetadataSnapshot(PolicyModel):
    revision: str = Field(min_length=1, max_length=128)
    classification_revision: str = Field(min_length=1, max_length=128)
    available_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS, strict=True)
    instruments: tuple[InstrumentMetadata, ...] = Field(max_length=MAX_INSTRUMENTS)

    @model_validator(mode='after')
    def unique(self):
        ids = [row.instrument_id for row in self.instruments]
        if len(set(ids)) != len(ids):
            raise ValueError('duplicate instrument metadata')
        return self


class TurnoverDay(PolicyModel):
    """Complete UTC-day quote turnover, usable only after its own availability."""
    instrument_id: str = Field(pattern=INSTRUMENT_PATTERN)
    day_start_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS, strict=True)
    quote_turnover: Decimal = Field(ge=0)
    available_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS, strict=True)

    @model_validator(mode='after')
    def complete_day(self):
        if self.day_start_ms % DAY_MS or self.available_at_ms < self.day_start_ms + DAY_MS:
            raise ValueError('turnover must describe a complete UTC day available after it closed')
        return self


class TurnoverPanel(PolicyModel):
    numeraire: str = Field(pattern=ASSET_PATTERN)
    rows: tuple[TurnoverDay, ...] = Field(max_length=MAX_TURNOVER_ROWS)

    @model_validator(mode='after')
    def unique(self):
        keys = [(row.instrument_id, row.day_start_ms) for row in self.rows]
        if len(set(keys)) != len(keys):
            raise ValueError('duplicate instrument-day turnover')
        return self


class UniverseSpec(PolicyModel):
    universe_id: str = Field(pattern=UNIVERSE_ID_PATTERN)
    mode: Literal['basket_5', 'liquid_top_n', 'broad_eligible']
    count: int = Field(ge=1, le=MAX_ASSETS, strict=True)
    venue: str = Field(pattern=VENUE_PATTERN)
    numeraire: str = Field(pattern=ASSET_PATTERN)
    basket_assets: tuple[str, ...]
    benchmarks: tuple[str, ...]
    lookback_days: int = Field(ge=1, le=30, strict=True)
    min_listing_age_days: int = Field(ge=0, le=3650, strict=True)
    hysteresis: Decimal = Field(ge=1, le=3)
    eligible_classifications: tuple[str, ...] = Field(min_length=1, max_length=16)
    cap_floor: Decimal = Field(gt=0, le=1)
    algorithm_version: Literal['mc-universe-1']

    @model_validator(mode='after')
    def explicit_mode(self):
        for group in (self.basket_assets, self.benchmarks):
            if list(group) != sorted(set(group)) or any(re.fullmatch(ASSET_PATTERN, a) is None for a in group):
                raise ValueError('asset lists must be sorted unique canonical asset ids')
        if self.mode == 'basket_5':
            if self.count != 5 or len(self.basket_assets) != 5 or not set(self.benchmarks) <= set(self.basket_assets):
                raise ValueError('basket_5 names exactly five assets including its benchmarks')
        elif self.basket_assets:
            raise ValueError('only basket_5 names its members')
        if len(self.benchmarks) > self.count:
            raise ValueError('benchmarks exceed the cohort')
        return self


class Exclusion(PolicyModel):
    instrument_id: str = Field(pattern=INSTRUMENT_PATTERN)
    asset_id: str = Field(pattern=ASSET_PATTERN)
    reasons: tuple[ReasonCode, ...] = Field(min_length=1)

    @model_validator(mode='after')
    def known(self):
        canonical_reasons(self.reasons)
        return self


class UniverseBinding(PolicyModel):
    """A versioned reference cohort, frozen with the weights known before the returns it will weight."""
    role: Literal['reference']
    universe_id: str = Field(pattern=UNIVERSE_ID_PATTERN)
    mode: Literal['basket_5', 'liquid_top_n', 'broad_eligible']
    requested_count: int = Field(ge=1, le=MAX_ASSETS, strict=True)
    venue: str = Field(pattern=VENUE_PATTERN)
    numeraire: str = Field(pattern=ASSET_PATTERN)
    member_asset_ids: tuple[str, ...] = Field(min_length=1, max_length=MAX_ASSETS)
    member_instrument_ids: tuple[str, ...] = Field(min_length=1, max_length=MAX_ASSETS)
    benchmarks: tuple[str, ...]
    selection_as_of_ms: int = Field(ge=0, strict=True)
    effective_from_ms: int = Field(ge=0, strict=True)
    available_at_ms: int = Field(ge=0, strict=True)
    metadata_revision: str
    classification_revision: str
    algorithm_version: Literal['mc-universe-1']
    weighting_algorithm: Literal['mc-weights-1']
    trailing_turnover: tuple[Decimal, ...]
    equal_weights: tuple[Decimal, ...]
    capped_weights: tuple[Decimal, ...]
    excluded: tuple[Exclusion, ...] = Field(max_length=MAX_INSTRUMENTS)
    membership_hash: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode='after')
    def consistent(self):
        n = len(self.member_asset_ids)
        if list(self.member_asset_ids) != sorted(set(self.member_asset_ids)):
            raise ValueError('members must be sorted unique asset ids')
        if any(len(values) != n for values in (self.member_instrument_ids, self.trailing_turnover,
                                               self.equal_weights, self.capped_weights)):
            raise ValueError('member arrays must align')
        if not set(self.benchmarks) <= set(self.member_asset_ids):
            raise ValueError('benchmarks must be members')
        for weights in (self.equal_weights, self.capped_weights):
            if any(w < 0 for w in weights) or abs(sum(weights) - 1) > WEIGHT_TOLERANCE:
                raise ValueError('nominal weights must be nonnegative and sum to one')
        if self.membership_hash != binding_hash(self):
            raise ValueError('membership hash does not match the binding')
        return self


def binding_hash(binding) -> str:
    values = binding.model_dump(mode='json') if isinstance(binding, PolicyModel) else dict(binding)
    values.pop('membership_hash', None)
    return digest(values)


def capped_weights(turnover: Sequence[Decimal], cap: Decimal) -> tuple[Decimal, ...]:
    """Turnover-proportional weights, iteratively capped and redistributed; infeasible input rejects."""
    values = tuple(Decimal(v) for v in turnover)
    if not values or any(not v.is_finite() or v < 0 for v in values):
        raise ContextError(('TURNOVER_UNAVAILABLE',), 'nonnegative finite turnover required')
    if sum(values) <= 0:
        raise ContextError(('TURNOVER_UNAVAILABLE',), 'zero total turnover')
    if cap * len(values) < 1 - WEIGHT_TOLERANCE:
        raise ContextError(('CAPACITY_EXCEEDED',), 'weight cap is infeasible for this cohort')
    fixed: set[int] = set()
    while True:
        free = [i for i in range(len(values)) if i not in fixed]
        remaining = 1 - cap * len(fixed)
        free_total = sum(values[i] for i in free)
        if free_total <= 0:
            if remaining > WEIGHT_TOLERANCE:
                raise ContextError(('CAPACITY_EXCEEDED',), 'capped mass cannot be redistributed')
            return tuple(cap if i in fixed else Decimal(0) for i in range(len(values)))
        tentative = {i: values[i] * remaining / free_total for i in free}
        over = {i for i, w in tentative.items() if w > cap}
        if not over:
            return tuple(cap if i in fixed else tentative[i] for i in range(len(values)))
        fixed |= over


def _exclusion_reason(row: InstrumentMetadata, spec: UniverseSpec, day: int, complete: set) -> str | None:
    if row.venue != spec.venue:
        return 'MEMBERSHIP_UNAVAILABLE'
    if row.numeraire != spec.numeraire:
        return 'QUOTE_MISMATCH'
    if row.classification is None:
        return 'CLASSIFICATION_UNKNOWN'
    if row.classification not in spec.eligible_classifications:
        return 'CLASSIFICATION_EXCLUDED'
    if row.listed_at_ms is None:
        return 'LISTING_UNKNOWN'
    if row.listed_at_ms > day - spec.min_listing_age_days * DAY_MS:
        return 'LISTING_TOO_RECENT'
    if row.instrument_id not in complete:
        return 'TURNOVER_UNAVAILABLE'
    return None


def select_reference(spec: UniverseSpec, metadata: MetadataSnapshot, turnover: TurnoverPanel,
                     previous: UniverseBinding | None, clock: ClockView) -> UniverseBinding:
    """Select a UTC-daily reference cohort from information available at ``clock.decision_ms`` only."""
    decision = clock.decision_ms
    if metadata.available_at_ms > decision:
        raise ContextError(('MEMBERSHIP_UNAVAILABLE',), 'metadata was not available at the decision')
    if turnover.numeraire != spec.numeraire:
        raise ContextError(('QUOTE_MISMATCH',), 'turnover numeraire differs from the universe')
    if previous is not None and (
            previous.universe_id != spec.universe_id or previous.mode != spec.mode
            or previous.requested_count != spec.count or previous.algorithm_version != spec.algorithm_version
            or previous.numeraire != spec.numeraire or previous.venue != spec.venue
            or previous.effective_from_ms > decision):
        raise ValueError('previous binding belongs to a different universe definition or the future')
    day = decision - decision % DAY_MS
    window = {day - k * DAY_MS for k in range(1, spec.lookback_days + 1)}
    sums: dict[str, Decimal] = {}
    seen: dict[str, set] = {}
    for row in turnover.rows:
        if row.day_start_ms in window and row.available_at_ms <= decision:
            sums[row.instrument_id] = sums.get(row.instrument_id, Decimal(0)) + row.quote_turnover
            seen.setdefault(row.instrument_id, set()).add(row.day_start_ms)
    complete = {instrument for instrument, days in seen.items() if days == window}
    excluded, eligible = [], {}
    for row in sorted(metadata.instruments, key=lambda r: r.instrument_id):
        reason = _exclusion_reason(row, spec, day, complete)
        if reason is not None:
            excluded.append((row.instrument_id, row.asset_id, reason))
            continue
        current = eligible.get(row.asset_id)
        candidate = (sums[row.instrument_id], row.instrument_id)
        if current is None:
            eligible[row.asset_id] = candidate
        else:  # one canonical instrument per underlying: highest turnover, then instrument id
            keep, drop = sorted((current, candidate), key=lambda x: (-x[0], x[1]))
            eligible[row.asset_id] = keep
            excluded.append((drop[1], row.asset_id, 'DUPLICATE_UNDERLYING'))
    ranked = sorted(eligible, key=lambda asset: (-eligible[asset][0], asset))
    if spec.mode == 'basket_5':
        if not set(spec.basket_assets) <= set(eligible):
            raise ContextError(('MEMBERSHIP_UNAVAILABLE',), 'a basket member is not eligible')
        members = list(spec.basket_assets)
    elif spec.mode == 'broad_eligible':
        if len(ranked) > spec.count:
            raise ContextError(('CAPACITY_EXCEEDED',), 'eligible assets exceed the declared cap')
        members = list(ranked)
    else:
        if len(ranked) < spec.count:
            raise ContextError(('MEMBERSHIP_UNAVAILABLE',), 'fewer eligible assets than the declared count')
        band = math.ceil(spec.hysteresis * spec.count)
        position = {asset: index + 1 for index, asset in enumerate(ranked)}
        incumbents = [] if previous is None else [a for a in previous.member_asset_ids
                                                  if position.get(a, band + 1) <= band]
        members = list(incumbents)
        for asset in ranked:
            if len(members) == spec.count:
                break
            if asset not in members:
                members.append(asset)
    members.sort()
    if not set(spec.benchmarks) <= set(members):
        raise ContextError(('BENCHMARK_MISSING',), 'a required benchmark is not a member')
    trailing = tuple(eligible[a][0] for a in members)
    cap = max(spec.cap_floor, Decimal(1) / Decimal(len(members)))
    values = dict(
        role='reference', universe_id=spec.universe_id, mode=spec.mode, requested_count=spec.count,
        venue=spec.venue, numeraire=spec.numeraire, member_asset_ids=tuple(members),
        member_instrument_ids=tuple(eligible[a][1] for a in members), benchmarks=spec.benchmarks,
        selection_as_of_ms=day, effective_from_ms=max(day, decision), available_at_ms=decision,
        metadata_revision=metadata.revision, classification_revision=metadata.classification_revision,
        algorithm_version=spec.algorithm_version, weighting_algorithm='mc-weights-1', trailing_turnover=trailing,
        equal_weights=tuple(Decimal(1) / Decimal(len(members)) for _ in members),
        capped_weights=capped_weights(trailing, cap),
        excluded=tuple(Exclusion(instrument_id=i, asset_id=a, reasons=(r,)) for i, a, r in sorted(excluded)))
    values['membership_hash'] = binding_hash(UniverseBinding.model_construct(**values))
    return UniverseBinding(**values)


class ConsumerUniverses(PolicyModel):
    """Consumer-owned entry/management scopes; management is never reconstructed from the reference list."""
    reference: tuple[str, ...]
    entry: tuple[str, ...]
    management: tuple[str, ...]
    management_obligations: tuple[tuple[str, tuple[Obligation, ...]], ...]
    observation: tuple[str, ...]


def bind_consumer_universes(reference: UniverseBinding, *, entry: Sequence[str],
                            management: Mapping[str, Sequence[str]]) -> ConsumerUniverses:
    entry = tuple(sorted(set(entry)))
    obligations = []
    for asset in sorted(management):
        reasons = tuple(sorted(set(management[asset])))
        if re.fullmatch(ASSET_PATTERN, asset) is None or not reasons:
            raise ValueError('every managed asset needs a canonical id and at least one obligation')
        obligations.append((asset, reasons))
    if any(re.fullmatch(ASSET_PATTERN, asset) is None for asset in entry):
        raise ValueError('invalid entry asset id')
    managed = tuple(asset for asset, _ in obligations)
    observation = tuple(sorted(set(reference.member_asset_ids) | set(entry) | set(managed)))
    return ConsumerUniverses(reference=reference.member_asset_ids, entry=entry, management=managed,
                             management_obligations=tuple(obligations), observation=observation)


__all__ = ['DAY_MS', 'ConsumerUniverses', 'Exclusion', 'InstrumentMetadata', 'MetadataSnapshot', 'TurnoverDay',
           'TurnoverPanel', 'UniverseBinding', 'UniverseSpec', 'bind_consumer_universes', 'capped_weights',
           'select_reference']
