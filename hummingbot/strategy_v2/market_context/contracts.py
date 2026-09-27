"""Immutable shared market-context contracts. Observations never grant order, capital or risk authority.

Canonical bytes (serializer ``mc-json-1``): UTF-8 JSON of the pydantic JSON dump with lexically sorted keys,
compact separators, no NaN/Infinity and Python's shortest round-trip float representation; arrays keep
their specified order. This is not RFC 8785.
"""
import json
import re
from decimal import Decimal
from hashlib import sha256
from typing import Literal, get_args

from pydantic import ConfigDict, Field, field_validator, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import canonical_json
from hummingbot.strategy_v2.components.strict_json import strict_json

SCHEMA_VERSION = '1.0'
SCHEMA_MAJOR = 1
SERIALIZER_VERSION = 'mc-json-1'
MINUTE_MS = 60_000
MAX_ASSETS = 500
MAX_FEATURES = 256
MAX_REASONS = 32
MAX_ARTIFACT_REFS = 32
MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024
MAX_TIMESTAMP_MS = 253_402_300_799_000
SHA256_PATTERN = r'^[0-9a-f]{64}$'
ASSET_PATTERN = r'^[A-Z0-9]{1,32}$'
INSTRUMENT_PATTERN = r'^[A-Z0-9]{1,32}-[A-Z0-9]{1,32}$'
STREAM_PATTERN = r'^[a-z0-9][a-z0-9_.-]{0,63}$'
EPOCH_PATTERN = r'^[A-Za-z0-9_-]{1,128}$'
FEATURE_PATTERN = r'^[a-z][a-z0-9_]{0,63}$'
VENUE_PATTERN = r'^[a-z0-9_]{1,32}$'

ReasonCode = Literal[
    # Source
    'INPUT_UNCONFIRMED', 'INPUT_GAP', 'INPUT_STALE', 'INPUT_REVISION_CONFLICT', 'INPUT_NONFINITE', 'INPUT_MISSING',
    'INPUT_INVALID', 'CUTOFF_NOT_MONOTONIC',
    # Universe
    'MEMBERSHIP_UNAVAILABLE', 'QUOTE_MISMATCH', 'DUPLICATE_UNDERLYING', 'CLASSIFICATION_UNKNOWN',
    'CLASSIFICATION_EXCLUDED', 'LISTING_UNKNOWN', 'LISTING_TOO_RECENT', 'TURNOVER_UNAVAILABLE', 'COVERAGE_LOW',
    'BENCHMARK_MISSING',
    # Estimation
    'WARMUP_INCOMPLETE', 'SCALE_DEGENERATE', 'FACTOR_VARIANCE_LOW', 'MODEL_UNAVAILABLE', 'RANK_POPULATION_LOW',
    # Publication
    'STORE_UNAVAILABLE', 'WRITER_FENCED', 'PAYLOAD_TOO_LARGE', 'CAPACITY_EXCEEDED', 'SNAPSHOT_CONFLICT',
    # Consumer
    'CONTEXT_EXPIRED', 'CONTEXT_FUTURE', 'VERSION_MISMATCH', 'SOURCE_KIND_REJECTED', 'SNAPSHOT_HASH_MISMATCH',
    'STREAM_MISMATCH', 'VENUE_MISMATCH', 'PROVENANCE_MISMATCH', 'FEATURE_UNAVAILABLE', 'EPOCH_UNCONFIRMED',
    'CONTEXT_UNAVAILABLE', 'PAYLOAD_INVALID',
]
REASONS = frozenset(get_args(ReasonCode))
SourceKind = Literal['observed', 'modeled_availability', 'synthetic']
FeatureStatus = Literal['VALID', 'WARMUP_INCOMPLETE', 'INPUT_MISSING', 'INPUT_STALE', 'MODEL_UNAVAILABLE', 'INVALID']
EnvelopeStatus = Literal['READY', 'DEGRADED', 'WARMING', 'INVALID']
Unit = Literal['return_fraction', 'share', 'share_change', 'ordinal', 'regression_coefficient', 'standard_score']

STRICT = ConfigDict(extra='forbid', frozen=True, allow_inf_nan=False, validate_default=True, strict=True)


class ContextError(PolicyError):
    """Market context is unavailable or invalid; the exact stable reasons are attached."""

    def __init__(self, reasons, detail=''):
        reasons = tuple(sorted(set(reasons)))
        if not reasons or not set(reasons) <= REASONS:
            raise ValueError(f'unknown market-context reason: {reasons!r}')
        self.reasons = reasons
        super().__init__(f"{','.join(reasons)}{': ' + detail if detail else ''}")


def canonical_reasons(values) -> tuple:
    values = tuple(values)
    if len(values) > MAX_REASONS or values != tuple(sorted(set(values))) or not set(values) <= REASONS:
        raise ValueError('reasons must be bounded, sorted, unique and known')
    return values


class ContractModel(PolicyModel):
    model_config = STRICT


class NamedFeature(ContractModel):
    """One published feature value; null with a reason is never a zero or flat market."""
    name: str = Field(pattern=FEATURE_PATTERN)
    value: float | None
    unit: Unit
    horizon_minutes: int = Field(ge=1, le=1440)
    status: FeatureStatus
    reasons: tuple[ReasonCode, ...]
    valid_count: int = Field(ge=0, le=MAX_ASSETS)
    expected_count: int = Field(ge=0, le=MAX_ASSETS)
    valid_weight_fraction: float | None = Field(ge=0, le=1)
    model_id: str | None = Field(pattern=SHA256_PATTERN)
    input_available_at_ms: int | None = Field(ge=0, le=MAX_TIMESTAMP_MS)

    @model_validator(mode='after')
    def honest_missingness(self):
        canonical_reasons(self.reasons)
        if (self.status == 'VALID') != (self.value is not None):
            raise ValueError('only a VALID feature carries a value; every other status is null')
        if self.status != 'VALID' and not self.reasons:
            raise ValueError('an unavailable feature requires a stable reason')
        if self.valid_count > self.expected_count:
            raise ValueError('valid count exceeds expected cohort')
        return self


class MissingAsset(ContractModel):
    asset_id: str = Field(pattern=ASSET_PATTERN)
    reasons: tuple[ReasonCode, ...] = Field(min_length=1)

    @field_validator('reasons')
    @classmethod
    def known(cls, value):
        return canonical_reasons(value)


class Coverage(ContractModel):
    expected_count: int = Field(ge=0, le=MAX_ASSETS)
    valid_count: int = Field(ge=0, le=MAX_ASSETS)
    valid_weight_fraction: float = Field(ge=0, le=1)
    missing: tuple[MissingAsset, ...] = Field(max_length=MAX_ASSETS)

    @model_validator(mode='after')
    def consistent(self):
        ids = [row.asset_id for row in self.missing]
        if self.valid_count > self.expected_count or ids != sorted(set(ids)):
            raise ValueError('coverage counts or missing assets are inconsistent')
        if len(ids) != self.expected_count - self.valid_count:
            raise ValueError('every missing member needs exactly one missing-asset record')
        return self


class Provenance(ContractModel):
    code_digest: str = Field(pattern=SHA256_PATTERN)
    config_digest: str = Field(pattern=SHA256_PATTERN)
    universe_hash: str = Field(pattern=SHA256_PATTERN)
    input_manifest_digest: str = Field(pattern=SHA256_PATTERN)
    model_digest: str = Field(pattern=SHA256_PATTERN)
    serializer_version: Literal['mc-json-1']
    artifact_refs: tuple[str, ...] = Field(max_length=MAX_ARTIFACT_REFS)

    @field_validator('artifact_refs')
    @classmethod
    def bounded_refs(cls, value):
        if any(not 1 <= len(ref) <= 256 for ref in value):
            raise ValueError('artifact references must be bounded strings')
        return value


class FitRecord(ContractModel):
    """Leave-one-out factor fit identity for one asset/horizon; beta/alpha live in named features."""
    horizon_minutes: int = Field(ge=1, le=1440)
    status: FeatureStatus
    reasons: tuple[ReasonCode, ...]
    training_samples: int = Field(ge=0)
    history_cutoff_ms: int | None = Field(ge=0, le=MAX_TIMESTAMP_MS)
    factor_id: str | None = Field(pattern=SHA256_PATTERN)

    @model_validator(mode='after')
    def explicit(self):
        canonical_reasons(self.reasons)
        if (self.status == 'VALID') != (self.factor_id is not None and self.history_cutoff_ms is not None):
            raise ValueError('only a valid fit carries a factor identity')
        if self.status != 'VALID' and not self.reasons:
            raise ValueError('an unavailable fit requires a stable reason')
        return self


class AssetRecord(ContractModel):
    asset_id: str = Field(pattern=ASSET_PATTERN)
    instrument_id: str = Field(pattern=INSTRUMENT_PATTERN)
    reasons: tuple[ReasonCode, ...]
    features: tuple[NamedFeature, ...] = Field(max_length=MAX_FEATURES)
    fits: tuple[FitRecord, ...] = Field(max_length=16)

    @model_validator(mode='after')
    def ordered(self):
        canonical_reasons(self.reasons)
        names = [f.name for f in self.features]
        horizons = [f.horizon_minutes for f in self.fits]
        if names != sorted(set(names)) or horizons != sorted(set(horizons)):
            raise ValueError('asset features and fits must be ordered and unique')
        return self


class SnapshotContent(ContractModel):
    """Proposed payload from ``compute_context``; publication identity/timing is supplied by the owner."""
    venue: str = Field(pattern=VENUE_PATTERN)
    numeraire: str = Field(pattern=ASSET_PATTERN)
    cutoff_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    max_input_available_at_ms: int | None = Field(ge=0, le=MAX_TIMESTAMP_MS)
    provenance: Provenance
    coverage: Coverage
    status: EnvelopeStatus
    reasons: tuple[ReasonCode, ...]
    market: tuple[NamedFeature, ...] = Field(max_length=MAX_FEATURES)
    assets: tuple[AssetRecord, ...] = Field(max_length=MAX_ASSETS)

    @model_validator(mode='after')
    def canonical_order(self):
        canonical_reasons(self.reasons)
        if self.cutoff_ms % MINUTE_MS:
            raise ValueError('cutoff must lie on a UTC minute boundary')
        names = [f.name for f in self.market]
        ids = [a.asset_id for a in self.assets]
        if names != sorted(set(names)) or ids != sorted(set(ids)):
            raise ValueError('market features and assets must be ordered and unique')
        if self.status == 'READY' and self.reasons:
            raise ValueError('a READY envelope carries no degradation reason')
        if self.status != 'READY' and not self.reasons:
            raise ValueError('a non-READY envelope requires reasons')
        return self


class ContextSnapshot(SnapshotContent):
    schema_version: str = Field(pattern=r'^[0-9]{1,3}\.[0-9]{1,3}$')
    snapshot_id: str = Field(pattern=SHA256_PATTERN)
    stream_id: str = Field(pattern=STREAM_PATTERN)
    epoch: str = Field(pattern=EPOCH_PATTERN)
    sequence: int = Field(ge=1)
    source_kind: SourceKind
    available_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    expires_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    supersedes: str | None = Field(pattern=SHA256_PATTERN)

    @model_validator(mode='after')
    def publication_timing(self):
        if int(self.schema_version.split('.')[0]) != SCHEMA_MAJOR:
            raise ValueError('unsupported market-context schema major version')
        if self.available_at_ms < self.cutoff_ms or (
                self.max_input_available_at_ms is not None and self.available_at_ms < self.max_input_available_at_ms):
            raise ValueError('publication cannot precede its cutoff or included inputs')
        if self.expires_at_ms <= self.available_at_ms:
            raise ValueError('expiry must be strictly later than availability')
        return self

    def market_feature(self, name: str) -> NamedFeature | None:
        return next((f for f in self.market if f.name == name), None)


def canonical_bytes(snapshot: ContextSnapshot) -> bytes:
    return canonical_json(snapshot.model_dump(mode='json', exclude={'snapshot_id'})).encode('utf-8')


def serialize_snapshot(snapshot: ContextSnapshot) -> bytes:
    """Wire/storage bytes: the same canonical encoding, including ``snapshot_id``."""
    return canonical_json(snapshot.model_dump(mode='json')).encode('utf-8')


def snapshot_digest(snapshot: ContextSnapshot) -> str:
    return sha256(canonical_bytes(snapshot)).hexdigest()


def seal_snapshot(content: SnapshotContent, *, stream_id: str, epoch: str, sequence: int, source_kind: str,
                  available_at_ms: int, expires_at_ms: int, supersedes: str | None) -> ContextSnapshot:
    values = content.model_dump()
    values.update(schema_version=SCHEMA_VERSION, snapshot_id='0' * 64, stream_id=stream_id, epoch=epoch,
                  sequence=sequence, source_kind=source_kind, available_at_ms=available_at_ms,
                  expires_at_ms=expires_at_ms, supersedes=supersedes)
    draft = ContextSnapshot.model_validate_json(json.dumps(values, allow_nan=False))
    sealed = draft.model_copy(update={'snapshot_id': snapshot_digest(draft)})
    if len(serialize_snapshot(sealed)) > MAX_SNAPSHOT_BYTES:
        raise ContextError(('PAYLOAD_TOO_LARGE',))
    return sealed


def _decode(raw: bytes | str, maximum: int):
    if isinstance(raw, str):
        raw = raw.encode('utf-8')
    if not isinstance(raw, bytes):
        raise ContextError(('PAYLOAD_INVALID',), 'bytes required')
    if len(raw) > maximum:
        raise ContextError(('PAYLOAD_TOO_LARGE',))
    try:
        decoded = strict_json(raw.decode('utf-8'))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ContextError(('PAYLOAD_INVALID',), str(exc)) from exc
    if not isinstance(decoded, dict):
        raise ContextError(('PAYLOAD_INVALID',), 'object required')
    return raw, decoded


def parse_snapshot(raw: bytes | str) -> ContextSnapshot:
    """Strictly parse and hash-verify one snapshot. Numeric strings, NaN and duplicate keys reject."""
    raw, decoded = _decode(raw, MAX_SNAPSHOT_BYTES)
    version = decoded.get('schema_version')
    if not isinstance(version, str) or re.fullmatch(r'[0-9]{1,3}\.[0-9]{1,3}', version) is None \
            or int(version.split('.')[0]) != SCHEMA_MAJOR:
        raise ContextError(('VERSION_MISMATCH',))
    try:
        snapshot = ContextSnapshot.model_validate_json(raw)
    except ValueError as exc:
        raise ContextError(('PAYLOAD_INVALID',), 'snapshot schema violation') from exc
    if snapshot_digest(snapshot) != snapshot.snapshot_id:
        raise ContextError(('SNAPSHOT_HASH_MISMATCH',))
    return snapshot


class ClockView(ContractModel):
    """Explicit time observations; pure functions never read wall time."""
    decision_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    receipt_ms: int | None = Field(ge=0, le=MAX_TIMESTAMP_MS)


class ContextRequirement(PolicyModel):
    consumer: Literal['execution', 'research']
    schema_major: Literal[1]
    stream_id: str = Field(pattern=STREAM_PATTERN)
    venue: str = Field(pattern=VENUE_PATTERN)
    numeraire: str = Field(pattern=ASSET_PATTERN)
    config_digest: str = Field(pattern=SHA256_PATTERN)
    universe_hash: str = Field(pattern=SHA256_PATTERN)
    max_age_ms: int = Field(ge=1, le=3_600_000, strict=True)
    min_count_coverage: Decimal = Field(ge=0, le=1)
    min_weight_coverage: Decimal = Field(ge=0, le=1)
    mandatory_features: tuple[str, ...] = Field(max_length=MAX_FEATURES)
    allowed_source_kinds: tuple[SourceKind, ...] = Field(min_length=1, max_length=3)

    @model_validator(mode='after')
    def admitted(self):
        if len(set(self.allowed_source_kinds)) != len(self.allowed_source_kinds):
            raise ValueError('duplicate source kinds')
        if self.consumer == 'execution' and 'synthetic' in self.allowed_source_kinds:
            raise ValueError('an execution consumer can never admit synthetic context')
        if any(re.fullmatch(FEATURE_PATTERN, name) is None for name in self.mandatory_features) \
                or len(set(self.mandatory_features)) != len(self.mandatory_features):
            raise ValueError('mandatory features must be unique feature names')
        return self


class ContextSelection(ContractModel):
    """Either one accepted snapshot or a rejection with reasons; never a fallback snapshot."""
    accepted: bool
    snapshot: ContextSnapshot | None
    reasons: tuple[ReasonCode, ...]
    snapshot_id: str | None = Field(pattern=SHA256_PATTERN)
    effective_available_at_ms: int | None = Field(ge=0, le=MAX_TIMESTAMP_MS)
    decision_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)

    @model_validator(mode='after')
    def exclusive(self):
        canonical_reasons(self.reasons)
        if self.accepted != (self.snapshot is not None) or self.accepted == bool(self.reasons):
            raise ValueError('a selection is either one accepted snapshot or a reasoned rejection')
        return self


def _reject(clock: ClockView, reasons, snapshot=None, effective=None) -> ContextSelection:
    return ContextSelection(accepted=False, snapshot=None, reasons=tuple(sorted(set(reasons))),
                            snapshot_id=None if snapshot is None else snapshot.snapshot_id,
                            effective_available_at_ms=effective, decision_ms=clock.decision_ms)


def validate_context(snapshot: ContextSnapshot, requirement: ContextRequirement,
                     clock: ClockView) -> ContextSelection:
    """Admit one snapshot for one decision. Checks run in a fixed order and stop at the first failure."""
    if snapshot_digest(snapshot) != snapshot.snapshot_id:
        return _reject(clock, ('SNAPSHOT_HASH_MISMATCH',))
    if int(snapshot.schema_version.split('.')[0]) != requirement.schema_major:
        return _reject(clock, ('VERSION_MISMATCH',), snapshot)
    if snapshot.source_kind not in requirement.allowed_source_kinds or (
            requirement.consumer == 'execution' and snapshot.source_kind == 'synthetic'):
        return _reject(clock, ('SOURCE_KIND_REJECTED',), snapshot)
    if snapshot.stream_id != requirement.stream_id:
        return _reject(clock, ('STREAM_MISMATCH',), snapshot)
    if snapshot.venue != requirement.venue:
        return _reject(clock, ('VENUE_MISMATCH',), snapshot)
    if snapshot.numeraire != requirement.numeraire:
        return _reject(clock, ('QUOTE_MISMATCH',), snapshot)
    if (snapshot.provenance.config_digest != requirement.config_digest
            or snapshot.provenance.universe_hash != requirement.universe_hash):
        return _reject(clock, ('PROVENANCE_MISMATCH',), snapshot)
    effective = max(snapshot.available_at_ms, clock.receipt_ms if clock.receipt_ms is not None else 0)
    if clock.decision_ms < effective:
        return _reject(clock, ('CONTEXT_FUTURE',), snapshot, effective)
    if clock.decision_ms >= snapshot.expires_at_ms or clock.decision_ms - snapshot.cutoff_ms > requirement.max_age_ms:
        return _reject(clock, ('CONTEXT_EXPIRED',), snapshot, effective)
    coverage = snapshot.coverage
    count = Decimal(coverage.valid_count) / Decimal(coverage.expected_count) if coverage.expected_count else Decimal(0)
    if (snapshot.status == 'INVALID' or count < requirement.min_count_coverage
            or Decimal(repr(coverage.valid_weight_fraction)) < requirement.min_weight_coverage):
        return _reject(clock, ('COVERAGE_LOW',), snapshot, effective)
    for name in requirement.mandatory_features:
        value = snapshot.market_feature(name)
        if value is None or value.status != 'VALID':
            return _reject(clock, ('FEATURE_UNAVAILABLE',), snapshot, effective)
    return ContextSelection(accepted=True, snapshot=snapshot, reasons=(), snapshot_id=snapshot.snapshot_id,
                            effective_available_at_ms=effective, decision_ms=clock.decision_ms)


class NotificationEnvelope(ContractModel):
    """Broker fanout pointer; a notification is never a capital permission or the payload itself."""
    stream_id: str = Field(pattern=STREAM_PATTERN)
    epoch: str = Field(pattern=EPOCH_PATTERN)
    sequence: int = Field(ge=1)
    snapshot_id: str = Field(pattern=SHA256_PATTERN)
    available_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    expires_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    locator: str = Field(pattern=r'^/screener/market-context/v1/snapshots/[0-9a-f]{64}$')

    @model_validator(mode='after')
    def bound(self):
        if self.locator.rsplit('/', 1)[1] != self.snapshot_id or self.expires_at_ms <= self.available_at_ms:
            raise ValueError('notification locator or timing is inconsistent')
        return self


class ContextStreamStatus(ContractModel):
    """Canonical-store identity used for an epoch handshake; carries no trade readiness."""
    stream_id: str = Field(pattern=STREAM_PATTERN)
    epoch: str = Field(pattern=EPOCH_PATTERN)
    lease_generation: int = Field(ge=1)
    sequence: int | None = Field(ge=1)
    snapshot_id: str | None = Field(pattern=SHA256_PATTERN)
    status: EnvelopeStatus | None
    available_at_ms: int | None = Field(ge=0, le=MAX_TIMESTAMP_MS)
    expires_at_ms: int | None = Field(ge=0, le=MAX_TIMESTAMP_MS)
    authority: Literal['observation_only']

    @model_validator(mode='after')
    def complete(self):
        fields = (self.sequence, self.snapshot_id, self.status, self.available_at_ms, self.expires_at_ms)
        if len({value is None for value in fields}) != 1:
            raise ValueError('status pointer fields are all present or all absent')
        return self


def parse_model(raw: bytes | str, model, maximum: int = 65_536):
    raw, _ = _decode(raw, maximum)
    try:
        return model.model_validate_json(raw)
    except ValueError as exc:
        raise ContextError(('PAYLOAD_INVALID',), f'{model.__name__} schema violation') from exc
