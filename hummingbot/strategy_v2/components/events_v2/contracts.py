"""Immutable event evidence. Structure and timing only; not authenticity or permission to trade."""
from decimal import Decimal
from typing import Literal

from pydantic import Field, StrictBool, StrictInt, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel

HASH = r'^[0-9a-f]{64}$'
NAME = r'^[A-Za-z0-9_.:-]{1,128}$'
OCCURRENCE = r'^[A-Za-z0-9_.:/-]{1,256}$'
ASSET = r'^[a-z0-9]+:[a-zA-Z0-9.-]+/[a-z0-9-]+:[a-zA-Z0-9._-]+$'
HOST = r'^[A-Za-z0-9.-]{1,253}$'
UTC_MS_MIN = 946684800000
UTC_MS_MAX = 4102444800000

EventKind = Literal[
    'SPOT_LISTING_ANNOUNCED', 'ROADMAP', 'TRADING_LAUNCH', 'PERP_LISTING',
    'PAIR_REMOVAL', 'TOKEN_DELISTING', 'SECURITY_INCIDENT', 'TRADING_HALT',
    'RISK_WARNING', 'MIGRATION', 'STATUS', 'OTHER',
]
CoverageState = Literal['UNAVAILABLE', 'UNVERIFIED', 'PARTIAL', 'COMPLETE']
EventStage = Literal['INITIAL', 'CORRECTION', 'REMINDER', 'RETRACTION']
InstrumentType = Literal['SPOT', 'PERPETUAL', 'FUTURE', 'OTHER']
NEGATIVE_KINDS = frozenset({
    'TOKEN_DELISTING', 'SECURITY_INCIDENT', 'TRADING_HALT', 'RISK_WARNING', 'PAIR_REMOVAL',
})


def utc_ms(value: int, name: str) -> int:
    if type(value) is not int or not UTC_MS_MIN <= value < UTC_MS_MAX:
        raise ValueError(f'{name}: expected UTC milliseconds')
    return value


class InstrumentScopeV2(PolicyModel):
    venue: str = Field(pattern=NAME)
    pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$')
    instrument_type: InstrumentType
    region: str = Field(pattern=NAME)
    quote_asset: str = Field(pattern=ASSET)
    fee_asset: str = Field(pattern=ASSET)
    lot_size_base: Decimal = Field(gt=0)

    @model_validator(mode='after')
    def finite_native_units(self):
        if not self.lot_size_base.is_finite():
            raise ValueError('native lot size must be finite')
        return self


class EventEvidenceV2(PolicyModel):
    schema_version: Literal[2] = 2
    source_id: str = Field(pattern=NAME)
    occurrence_id: str = Field(pattern=OCCURRENCE)
    cluster_id: str = Field(pattern=OCCURRENCE)
    asset_id: str = Field(pattern=ASSET)
    kind: EventKind
    stage: EventStage
    original_sha256: str = Field(pattern=HASH)
    content_sha256: str = Field(pattern=HASH)
    source_host: str = Field(pattern=HOST)
    monotonic_clock_domain: str = Field(pattern=NAME)
    source_qualification: CoverageState
    source_qualification_receipt_sha256: str | None = Field(default=None, pattern=HASH)
    publisher_display_date: str | None = Field(default=None, max_length=64)
    instrument_pair: str | None = Field(default=None, pattern=r'^[A-Z0-9]+-[A-Z0-9]+$')
    listed_ms: StrictInt | None = None
    published_ms: StrictInt | None = None
    first_public_ms: StrictInt | None = None
    received_ms: StrictInt
    parsed_ms: StrictInt
    processed_ms: StrictInt
    label_available_ms: StrictInt | None = None
    mapping_available_ms: StrictInt | None = None
    mapping_valid_from_ms: StrictInt | None = None
    mapping_receipt_sha256: str | None = Field(default=None, pattern=HASH)
    revision: int = Field(ge=1, le=10_000, strict=True)
    predecessor_sha256: str | None = Field(default=None, pattern=HASH)
    source_proof_sha256: str | None = Field(default=None, pattern=HASH)
    monotonic_ns: int = Field(ge=0, strict=True)
    novel: StrictBool = False
    affected_instruments: tuple[InstrumentScopeV2, ...] = Field(default=(), max_length=128)

    @model_validator(mode='after')
    def structure_and_timing(self):
        utc_ms(self.received_ms, 'received_ms')
        utc_ms(self.processed_ms, 'processed_ms')
        utc_ms(self.parsed_ms, 'parsed_ms')
        if self.parsed_ms < self.received_ms or self.processed_ms < self.parsed_ms:
            raise ValueError('processing cannot precede local receipt')
        for name in ('published_ms', 'first_public_ms', 'listed_ms', 'label_available_ms', 'mapping_available_ms', 'mapping_valid_from_ms'):
            value = getattr(self, name)
            if value is not None:
                utc_ms(value, name)
        if self.published_ms is not None and self.published_ms > self.received_ms:
            raise ValueError('SOURCE_CLOCK_FAULT')
        if self.first_public_ms is not None and self.published_ms is not None and self.first_public_ms < self.published_ms:
            raise ValueError('first-public evidence cannot precede publication')
        for name in ('label_available_ms', 'mapping_available_ms'):
            value = getattr(self, name)
            if value is not None and value < self.received_ms:
                raise ValueError(f'{name} cannot precede local receipt')
        if (self.mapping_available_ms is None) != (self.mapping_receipt_sha256 is None):
            raise ValueError('mapping availability requires its source receipt')
        if self.mapping_valid_from_ms is not None and self.mapping_available_ms is None:
            raise ValueError('mapping validity requires mapping availability')
        if self.source_qualification == 'COMPLETE' and self.source_qualification_receipt_sha256 is None:
            raise ValueError('complete source qualification requires its receipt')
        if self.revision > 1 and self.predecessor_sha256 is None:
            raise ValueError('a revision requires its predecessor hash')
        if self.revision == 1 and self.predecessor_sha256 is not None:
            raise ValueError('a first revision cannot name a predecessor')
        return self

    @property
    def event_available_ms(self) -> int:
        times = [self.received_ms, self.parsed_ms, self.processed_ms]
        if self.label_available_ms is not None:
            times.append(self.label_available_ms)
        if self.mapping_available_ms is not None:
            times.append(self.mapping_available_ms)
        return max(times)

    @property
    def negative(self) -> bool:
        return self.kind in NEGATIVE_KINDS


class CoverageScope(PolicyModel):
    source_id: str = Field(pattern=NAME)
    coverage_state: CoverageState
    asof_ms: StrictInt
    complete_for_entry: StrictBool = False
    complete_for_risk: StrictBool = False
    reason: str = Field(pattern=NAME)

    @model_validator(mode='after')
    def explicit_completeness(self):
        utc_ms(self.asof_ms, 'asof_ms')
        if self.coverage_state in ('UNAVAILABLE', 'UNVERIFIED') and (self.complete_for_entry or self.complete_for_risk):
            raise ValueError('unverified coverage cannot certify entry or risk completeness')
        return self


class SnapshotBindingV2(PolicyModel):
    source_session: str = Field(pattern=NAME)
    mapping_sha256: str = Field(pattern=HASH)
    feature_lineage_sha256: str | None = Field(default=None, pattern=HASH)
    risk_projection_sha256: str | None = Field(default=None, pattern=HASH)
    qualification_receipt_sha256: str | None = Field(default=None, pattern=HASH)
    market_share_scope: Literal['process_local', 'unsupported'] = 'process_local'


class QualifiedEventSnapshotV2(PolicyModel):
    schema: Literal['cross_venue_event.snapshot.v2'] = 'cross_venue_event.snapshot.v2'
    asof_ms: StrictInt
    sequence: int = Field(ge=1, le=1_000_000, strict=True)
    generation: int = Field(ge=1, le=1_000_000, strict=True)
    predecessor_sha256: str | None = Field(default=None, pattern=HASH)
    dataset_sha256: str = Field(pattern=HASH)
    events: tuple[EventEvidenceV2, ...] = Field(default=(), max_length=10_000)
    completeness: tuple[CoverageScope, ...] = Field(min_length=1, max_length=64)
    observation_universe: tuple[str, ...] = Field(default=(), max_length=10_000)
    entry_universe: tuple[str, ...] = Field(default=(), max_length=10_000)
    management_universe: tuple[str, ...] = Field(default=(), max_length=10_000)
    binding: SnapshotBindingV2
    execution_allowed: Literal[False] = False

    @model_validator(mode='after')
    def causal_snapshot(self):
        utc_ms(self.asof_ms, 'asof_ms')
        if self.sequence > 1 and self.predecessor_sha256 is None:
            raise ValueError('a successor snapshot requires its predecessor hash')
        if self.sequence == 1 and self.predecessor_sha256 is not None:
            raise ValueError('the first snapshot cannot name a predecessor')
        keys = [f'{item.source_id}:{item.occurrence_id}:{item.asset_id}' for item in self.events]
        if len(keys) != len(set(keys)):
            raise ValueError('duplicate event identity: resolve revisions explicitly')
        if any(item.event_available_ms > self.asof_ms for item in self.events):
            raise ValueError('snapshot contains knowledge that was not available at generation')
        sources = {item.source_id for item in self.completeness}
        if len(sources) != len(self.completeness):
            raise ValueError('duplicate coverage identity')
        for asset in (*self.observation_universe, *self.entry_universe, *self.management_universe):
            if not isinstance(asset, str) or not asset:
                raise ValueError('universe membership requires a canonical asset id')
        return self
