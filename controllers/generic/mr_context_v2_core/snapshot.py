"""Causal context envelopes. These are not a Market Pulse collector or a trading signal."""
from decimal import Decimal, localcontext
from typing import Annotated, Literal, TypeVar

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, StrictBool, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import ObservationRef
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.components.strict_json import strict_json


def exact_decimal(value):
    if type(value) not in (str, int, Decimal):
        raise ValueError('use a finite Decimal, integer or decimal string, not bool/float')
    if isinstance(value, str) and len(value) > 96:
        raise ValueError('decimal input exceeds bounded size')
    return value


Number = Annotated[Decimal, BeforeValidator(exact_decimal), Field(max_digits=28, decimal_places=18)]
Fraction = Annotated[Number, Field(ge=0, le=1)]
Nonnegative = Annotated[Number, Field(ge=0)]
Hash = Annotated[str, Field(strict=True, pattern=r'^[0-9a-f]{64}$')]
Name = Annotated[str, Field(strict=True, pattern=r'^[A-Za-z0-9_.:-]{1,128}$')]
Millis = Annotated[int, Field(strict=True, ge=0, le=2**53 - 1)]
Count = Annotated[int, Field(strict=True, ge=0, le=10000)]
T = TypeVar('T', bound=BaseModel)


class ContextModel(PolicyModel):
    model_config = ConfigDict(revalidate_instances='always')


def _plain(value):
    if isinstance(value, BaseModel):
        # model_copy/model_construct are not validation. Retain injected unknown keys.
        return {key: _plain(item) for key, item in
                {**vars(value), **(value.__pydantic_extra__ or {})}.items()}
    if isinstance(value, tuple):
        return tuple(_plain(item) for item in value)
    if isinstance(value, list):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    return value


def checked(value: T, cls: type[T]) -> T:
    if not isinstance(value, cls):
        raise ValueError(f'expected {cls.__name__}')
    return cls.model_validate(_plain(value))


def check_clock(now_ms: int) -> None:
    if type(now_ms) is not int or not 0 <= now_ms < 2**53:
        raise ValueError('clock must be a nonnegative UTC millisecond integer')


class ContextComponentsV2(ContextModel):
    return_1m_fraction: Annotated[Number, Field(ge=-1)] | None = None
    return_5m_fraction: Annotated[Number, Field(ge=-1)] | None = None
    breadth_fraction: Fraction | None = None
    quote_volume_5m: Nonnegative | None = None
    realized_vol_15m_fraction: Nonnegative | None = None


class ContextSnapshotV2(ContextModel):
    schema_version: Literal['mr_context.snapshot.v2'] = 'mr_context.snapshot.v2'
    source: Name
    market_type: Literal['spot']
    quote_asset: Literal['USDC']
    interval: Literal['1m']
    dataset_sha256: Hash
    epoch: Name
    revision: Annotated[int, Field(strict=True, ge=1)]
    implementation_sha256: Hash
    parameters_sha256: Hash
    initialization_sha256: Hash
    source_content_sha256: Hash
    universe_sha256: Hash
    universe_revision: Annotated[int, Field(strict=True, ge=1)]
    bar_open_ms: Millis
    bar_close_ms: Millis
    confirmed: StrictBool
    available_time_ms: Millis
    received_at_ms: Millis
    expires_at_ms: Millis
    expected_members: Annotated[Count, Field(ge=1)]
    ready_members: Count
    fresh_members: Count
    history_bars: Annotated[Count, Field(ge=1)]
    missing_required: tuple[Name, ...] = Field(default=(), max_length=64)
    evidence_kind: Literal['observed', 'reconstructed']
    components: ContextComponentsV2

    @model_validator(mode='after')
    def coherent(self):
        if not self.confirmed or self.bar_open_ms % 60000 or self.bar_close_ms != self.bar_open_ms + 60000:
            raise ValueError('a confirmed, aligned, half-open one-minute bar is required')
        if not self.bar_close_ms <= self.available_time_ms <= self.received_at_ms:
            raise ValueError('availability/receipt must follow confirmed close')
        if not 0 < self.expires_at_ms - self.available_time_ms <= 86400000:
            raise ValueError('context expiry must be fixed and bounded after availability')
        if max(self.ready_members, self.fresh_members) > self.expected_members:
            raise ValueError('coverage counts exceed the contemporaneous universe')
        if len(set(self.missing_required)) != len(self.missing_required):
            raise ValueError('duplicate missing dependency')
        return self

    @property
    def content_sha256(self) -> str:
        # A reread can change transport metadata, but cannot refresh the observation.
        return digest(self.model_dump(mode='json', exclude={'received_at_ms'}))

    @property
    def observation(self) -> ObservationRef:
        return ObservationRef(dataset_sha256=self.dataset_sha256, epoch=self.epoch,
                              revision=self.revision, content_sha256=self.content_sha256,
                              event_time_ms=self.bar_close_ms, available_time_ms=self.available_time_ms)


class ContextRequirementsV2(ContextModel):
    source: Name
    dataset_sha256: Hash
    implementation_sha256: Hash
    parameters_sha256: Hash
    initialization_sha256: Hash
    universe_sha256: Hash
    universe_revision: Annotated[int, Field(strict=True, ge=1)]
    min_coverage_fraction: Annotated[Fraction, Field(gt=0)]
    min_members: Annotated[Count, Field(ge=1)]
    min_history_bars: Annotated[Count, Field(ge=1)]
    max_source_age_ms: Annotated[int, Field(strict=True, ge=1, le=86400000)]
    max_transport_age_ms: Annotated[int, Field(strict=True, ge=1, le=86400000)]


def context_blockers(snapshot: ContextSnapshotV2 | None, requirements: ContextRequirementsV2,
                     *, now_ms: int) -> tuple[str, ...]:
    check_clock(now_ms)
    requirements = checked(requirements, ContextRequirementsV2)
    if snapshot is None:
        return ('context_unavailable',)
    snapshot = checked(snapshot, ContextSnapshotV2)
    blockers = []
    if snapshot.source != requirements.source:
        blockers.append('source_mismatch')
    if snapshot.dataset_sha256 != requirements.dataset_sha256:
        blockers.append('dataset_mismatch')
    if any(getattr(snapshot, field) != getattr(requirements, field) for field in
           ('implementation_sha256', 'parameters_sha256', 'initialization_sha256')):
        blockers.append('implementation_mismatch')
    if (snapshot.universe_sha256, snapshot.universe_revision) != (
            requirements.universe_sha256, requirements.universe_revision):
        blockers.append('membership_changed')
    if snapshot.evidence_kind != 'observed':
        blockers.append('availability_unobserved')
    if snapshot.received_at_ms > now_ms:
        blockers.append('context_not_yet_available')
    if now_ms >= snapshot.expires_at_ms:
        blockers.append('context_expired')
    if now_ms - snapshot.bar_close_ms > requirements.max_source_age_ms:
        blockers.append('source_stale')
    if now_ms - snapshot.received_at_ms > requirements.max_transport_age_ms:
        blockers.append('transport_stale')
    # Separate ready/fresh counts do not prove overlap; use the worst-case intersection.
    joint_lower_bound = max(0, snapshot.ready_members + snapshot.fresh_members - snapshot.expected_members)
    with localcontext() as ctx:
        ctx.prec = 80
        if (snapshot.expected_members < requirements.min_members or
                joint_lower_bound < requirements.min_coverage_fraction * snapshot.expected_members):
            blockers.append('coverage_insufficient')
    if snapshot.history_bars < requirements.min_history_bars:
        blockers.append('warmup_incomplete')
    if snapshot.missing_required:
        blockers.append('dependency_missing')
    return tuple(sorted(blockers))


def select_as_of(snapshots: tuple[ContextSnapshotV2, ...] | list[ContextSnapshotV2],
                 *, now_ms: int) -> ContextSnapshotV2 | None:
    """Select from a bounded single-dataset archive, never falling back on validity."""
    check_clock(now_ms)
    if type(snapshots) not in (tuple, list) or len(snapshots) > 256:
        raise ValueError('at most 256 explicit snapshot records are supported')
    eligible, identities, datasets = [], {}, set()
    for raw in snapshots:
        snapshot = checked(raw, ContextSnapshotV2)
        if snapshot.received_at_ms > now_ms:
            continue
        datasets.add(snapshot.dataset_sha256)
        key = (snapshot.dataset_sha256, snapshot.epoch, snapshot.revision)
        if key in identities and identities[key] != snapshot.content_sha256:
            raise ValueError('conflicting content for an immutable snapshot revision')
        identities[key] = snapshot.content_sha256
        eligible.append(snapshot)
    if len(datasets) > 1:
        raise ValueError('a single dataset is required')
    if not eligible:
        return None

    def ordering(item):
        return item.bar_close_ms, item.available_time_ms, item.revision

    latest_key = max(map(ordering, eligible))
    latest = [item for item in eligible if ordering(item) == latest_key]
    if len({item.content_sha256 for item in latest}) != 1:
        raise ValueError('conflicting latest snapshots; producer epoch is ambiguous')
    return max(latest, key=lambda item: item.received_at_ms)


def decode_snapshot(raw: bytes) -> ContextSnapshotV2:
    """Decode only the v2 envelope; legacy scalar/history payloads are not adapted."""
    if type(raw) is not bytes or len(raw) > 65536:
        raise ValueError('snapshot wire payload must be at most 64 KiB of UTF-8 bytes')
    try:
        data = strict_json(raw.decode('utf-8'))
        if not isinstance(data, dict):
            raise ValueError('snapshot must be an object')
        if any(key in data for key in ('composite_score', 'momentum_score', 'feature_status')):
            raise ValueError('unsupported producer payload; public Market Pulse composite is not context evidence')
        return ContextSnapshotV2.model_validate(data)
    except (UnicodeError, RecursionError) as exc:
        raise ValueError('invalid snapshot encoding or nesting') from exc
