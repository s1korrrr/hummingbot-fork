"""Strict lossless wire contracts for Market Picture metrics and events.

`metric-series.v1` interns per-metric definition/provider/window metadata once and
stores only changing cell data per instrument. `resolve_metric_series` expands
that representation to the complete MetricValue shape without recalculation.
"""

import json
import re
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from hummingbot.strategy_v2.components.recipe_contract import canonical_json

ENCODING_V1 = "metric-series.v1"
MAX_CELLS = 500
MAX_CAPABILITIES = 64
MAX_REASONS = 32
MAX_TIMESTAMP_MS = 253_402_300_799_000
SHA256_PATTERN = r"^[0-9a-f]{64}$"
INSTRUMENT_PATTERN = r"^[a-z0-9_]{1,24}:spot:[A-Z0-9]{1,32}-[A-Z0-9]{1,32}$"
IDENTITY_PATTERN = r"^[a-z0-9][a-z0-9_.:-]{0,127}$"
EPOCH_PATTERN = r"^[A-Za-z0-9_-]{1,128}$"
DECIMAL_PATTERN = r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$"

ReasonCode = Literal[
    "SOURCE_UNAVAILABLE",
    "SOURCE_DISABLED",
    "SOURCE_IDENTITY_MISMATCH",
    "QUOTE_MISMATCH",
    "DUPLICATE_ASSET",
    "BAR_UNCONFIRMED",
    "BAR_GAP",
    "LAGGING_MEMBER",
    "STALE_SOURCE",
    "INSUFFICIENT_HISTORY",
    "INSUFFICIENT_COVERAGE",
    "CONSTANT_SERIES",
    "ZERO_BASELINE",
    "UNQUALIFIED_MODEL",
    "UNKNOWN_CONFIDENCE_SCALE",
    "UNKNOWN_SECTOR",
    "CAPACITY_EXCEEDED",
    "EXPIRED_SNAPSHOT",
    "HASH_MISMATCH",
    "SEQUENCE_REGRESSION",
    "RECONSTRUCTED_AVAILABILITY",
    "RETENTION_EXPIRED",
    "INVALID_INPUT",
    "CORRECTION_PENDING",
    "NO_CROSSING",
    "NO_PRIOR_STATE",
]
REASONS = frozenset(ReasonCode.__args__)
ComputationStatus = Literal["VALID", "WARMING", "MISSING", "INVALID", "UNAVAILABLE"]
CoverageStatus = Literal["COMPLETE", "PARTIAL"]
SourceKind = Literal["observed", "reconstructed"]
Unit = Literal[
    "price_quote",
    "return_percent",
    "percentage_points",
    "share_fraction",
    "volume_quote",
    "ratio",
    "adx_0_100",
    "rsi_0_100",
    "correlation_minus1_to1",
    "index_minus3_to3",
    "volatility_fraction_annualized",
    "instruments",
    "milliseconds",
    "percentile_0_100",
    "count",
    "percent",
    "atr_percent",
    "price_percent",
    "quote_volume",
    "return_fraction",
]


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, allow_inf_nan=False)


def _canonical_reasons(values: tuple[str, ...]) -> tuple[str, ...]:
    if len(values) > MAX_REASONS or tuple(sorted(set(values))) != values or not set(values) <= REASONS:
        raise ValueError("reason codes must be bounded, sorted, unique and known")
    return values


def _timestamp(value: int | None) -> int | None:
    if value is not None and (type(value) is not int or not 0 <= value <= MAX_TIMESTAMP_MS):
        raise ValueError("timestamp is outside the supported UTC millisecond range")
    return value


class DefinitionRefV1(ContractModel):
    definition_id: str = Field(pattern=IDENTITY_PATTERN)
    definition_version: str = Field(pattern=r"^[0-9]+\.[0-9]+\.[0-9]+$")
    parameter_hash: str = Field(pattern=SHA256_PATTERN)
    code_hash: str = Field(pattern=SHA256_PATTERN)
    unit: Unit


class ProviderCoverageV1(ContractModel):
    expected_count: int = Field(ge=0, le=MAX_CELLS)
    valid_count: int = Field(ge=0, le=MAX_CELLS)
    coverage_fraction: str = Field(pattern=DECIMAL_PATTERN)

    @model_validator(mode="after")
    def consistent(self):
        value = Decimal(self.coverage_fraction)
        expected = Decimal(self.valid_count) / self.expected_count if self.expected_count else Decimal(0)
        if not value.is_finite() or not Decimal(0) <= value <= Decimal(1) or value != expected:
            raise ValueError("provider coverage fraction must equal valid/expected")
        if self.valid_count > self.expected_count:
            raise ValueError("provider valid count exceeds expected count")
        return self


class ProviderRefV1(ContractModel):
    provider_id: str = Field(pattern=IDENTITY_PATTERN)
    stream_id: str = Field(pattern=IDENTITY_PATTERN)
    epoch: str = Field(pattern=EPOCH_PATTERN)
    sequence: int = Field(ge=1, le=9_223_372_036_854_775_807)
    source_snapshot_id: str = Field(pattern=SHA256_PATTERN)
    source_digest: str = Field(pattern=SHA256_PATTERN)
    source_kind: SourceKind
    observed_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    available_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    expires_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    coverage: ProviderCoverageV1
    capabilities: tuple[str, ...] = Field(max_length=MAX_CAPABILITIES)

    @model_validator(mode="after")
    def timing_and_capabilities(self):
        if self.available_at_ms < self.observed_at_ms or self.expires_at_ms <= self.available_at_ms:
            raise ValueError("provider availability/expiry is inconsistent")
        if self.capabilities != tuple(sorted(set(self.capabilities))) or any(not c for c in self.capabilities):
            raise ValueError("provider capabilities must be sorted, unique and non-empty")
        return self


class MetricCellV1(ContractModel):
    instrument_id: str = Field(pattern=INSTRUMENT_PATTERN)
    value: str | None = Field(default=None, max_length=128)
    computation_status: ComputationStatus
    coverage_status: CoverageStatus
    reason_codes: tuple[ReasonCode, ...] = Field(max_length=MAX_REASONS)
    window_start_ms: int | None = Field(default=None, ge=0, le=MAX_TIMESTAMP_MS)
    window_end_ms: int | None = Field(default=None, ge=0, le=MAX_TIMESTAMP_MS)
    observation_interval_ms: int | None = Field(default=None, ge=1, le=604_800_000)
    available_at_ms: int | None = Field(default=None, ge=0, le=MAX_TIMESTAMP_MS)
    expires_at_ms: int | None = Field(default=None, ge=0, le=MAX_TIMESTAMP_MS)
    sample_count: int = Field(ge=0, le=20_161)
    required_samples: int | None = Field(default=None, ge=0, le=20_161)
    numerator: int | None = Field(default=None, ge=0, le=MAX_CELLS)
    denominator: int | None = Field(default=None, ge=0, le=MAX_CELLS)
    expected_denominator: int | None = Field(default=None, ge=0, le=MAX_CELLS)

    @model_validator(mode="after")
    def semantically_valid(self):
        _canonical_reasons(self.reason_codes)
        if self.value is not None:
            if not isinstance(self.value, str) or re.fullmatch(DECIMAL_PATTERN, self.value) is None:
                raise ValueError("metric values must be finite, canonical decimal strings")
            try:
                if not Decimal(self.value).is_finite():
                    raise ValueError("metric value is not finite")
            except InvalidOperation as exc:
                raise ValueError("metric value is invalid") from exc
        if (self.computation_status == "VALID") != (self.value is not None):
            raise ValueError("only a VALID cell carries a value")
        if self.computation_status != "VALID" and not self.reason_codes:
            raise ValueError("unavailable cells require a reason")
        if (
            self.window_start_ms is not None
            and self.window_end_ms is not None
            and self.window_end_ms < self.window_start_ms
        ):
            raise ValueError("metric window is reversed")
        if (
            self.available_at_ms is not None
            and self.expires_at_ms is not None
            and self.expires_at_ms <= self.available_at_ms
        ):
            raise ValueError("metric expiry must follow availability")
        if (self.numerator is None) != (self.denominator is None):
            raise ValueError("numerator and denominator are an inseparable pair")
        if self.numerator is not None and self.denominator is not None and self.numerator > self.denominator:
            raise ValueError("numerator exceeds denominator")
        if (
            self.denominator is not None
            and self.expected_denominator is not None
            and self.denominator > self.expected_denominator
        ):
            raise ValueError("denominator exceeds expected denominator")
        return self


class MetricSeriesV1(ContractModel):
    encoding: Literal["metric-series.v1"]
    metric_id: str = Field(pattern=IDENTITY_PATTERN)
    definition_ref: DefinitionRefV1
    provider_ref: ProviderRefV1
    horizon_minutes: int = Field(ge=1, le=10_080)
    series_unit: Unit
    window_start_ms: int | None = Field(default=None, ge=0, le=MAX_TIMESTAMP_MS)
    window_end_ms: int | None = Field(default=None, ge=0, le=MAX_TIMESTAMP_MS)
    observation_interval_ms: int | None = Field(default=None, ge=1, le=604_800_000)
    available_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    expires_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    required_samples: int = Field(ge=0, le=20_161)
    expected_denominator: int = Field(ge=0, le=MAX_CELLS)
    fixture_only: bool = False
    cells: tuple[MetricCellV1, ...] = Field(max_length=MAX_CELLS)
    payload_digest: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def coherent_series(self):
        if self.series_unit != self.definition_ref.unit:
            raise ValueError("series unit must match its definition")
        if self.expires_at_ms <= self.available_at_ms:
            raise ValueError("series expiry must follow availability")
        if (
            self.window_start_ms is not None
            and self.window_end_ms is not None
            and self.window_end_ms < self.window_start_ms
        ):
            raise ValueError("series window is reversed")
        ids = [cell.instrument_id for cell in self.cells]
        if ids != sorted(set(ids)):
            raise ValueError("metric cells must be sorted by unique instrument identity")
        if len(self.cells) != self.expected_denominator:
            raise ValueError("metric cell population must equal the pinned denominator")
        if self.provider_ref.coverage.expected_count != self.expected_denominator:
            raise ValueError("provider and metric denominators must agree")
        if self.fixture_only and self.provider_ref.source_kind != "reconstructed":
            raise ValueError("fixtures must use reconstructed source kind")
        if metric_series_digest(self) != self.payload_digest:
            raise ValueError("metric series digest mismatch")
        resolve_metric_series(self)  # also enforces the declared unit's numeric bounds on every cell
        return self


def metric_series_digest(value) -> str:
    """Hash canonical compact JSON with the digest field omitted."""
    if isinstance(value, BaseModel):
        payload = value.model_dump(mode="json", exclude={"payload_digest"}, exclude_none=True, exclude_defaults=True)
    else:
        payload = {key: item for key, item in value.items() if key != "payload_digest"}

    def omit_nulls(item):
        if isinstance(item, dict):
            return {
                key: omit_nulls(child)
                for key, child in item.items()
                if child is not None and not (key == "fixture_only" and child is False)
            }
        if isinstance(item, list):
            return [omit_nulls(child) for child in item]
        return item

    payload = omit_nulls(payload)
    return sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def parse_metric_series(raw: bytes | str, *, maximum_bytes: int = 2 * 1024 * 1024) -> MetricSeriesV1:
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    if not isinstance(raw, bytes) or len(raw) > maximum_bytes:
        raise ValueError("metric series payload must be bytes within the size limit")
    data = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_unique_object,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON number")),
    )
    if not isinstance(data, dict):
        raise ValueError("metric series JSON must be an object")
    return MetricSeriesV1.model_validate_json(raw)


def resolve_metric_series(series: MetricSeriesV1) -> dict[str, dict]:
    """Expand shared series metadata into per-instrument values; performs no metric computation."""
    result = {}
    for cell in series.cells:
        expanded = {
            "metric_id": series.metric_id,
            "value": cell.value,
            "unit": series.series_unit,
            "computation_status": cell.computation_status,
            "coverage_status": cell.coverage_status,
            "reason_codes": list(cell.reason_codes),
            "definition": series.definition_ref.model_dump(mode="json"),
            "source_ref": series.provider_ref.model_dump(mode="json"),
            "horizon_minutes": series.horizon_minutes,
            "window_start_ms": cell.window_start_ms if cell.window_start_ms is not None else series.window_start_ms,
            "window_end_ms": cell.window_end_ms if cell.window_end_ms is not None else series.window_end_ms,
            "observation_interval_ms": (
                cell.observation_interval_ms
                if cell.observation_interval_ms is not None
                else series.observation_interval_ms
            ),
            "available_at_ms": cell.available_at_ms if cell.available_at_ms is not None else series.available_at_ms,
            "expires_at_ms": cell.expires_at_ms if cell.expires_at_ms is not None else series.expires_at_ms,
            "sample_count": cell.sample_count,
            "required_samples": cell.required_samples if cell.required_samples is not None else series.required_samples,
            "numerator": cell.numerator,
            "denominator": cell.denominator,
            "expected_denominator": (
                cell.expected_denominator if cell.expected_denominator is not None else series.expected_denominator
            ),
        }
        result[cell.instrument_id] = MetricValueV1.model_validate_json(json.dumps(expanded)).model_dump(mode="json")
    return result


class MetricValueV1(ContractModel):
    """Expanded per-instrument value; consumers receive this shape after series resolution."""

    metric_id: str = Field(pattern=IDENTITY_PATTERN)
    value: str | None = Field(default=None, max_length=128)
    unit: Unit
    computation_status: ComputationStatus
    coverage_status: CoverageStatus
    reason_codes: tuple[ReasonCode, ...] = Field(max_length=MAX_REASONS)
    definition: DefinitionRefV1
    source_ref: ProviderRefV1
    horizon_minutes: int = Field(ge=1, le=10_080)
    window_start_ms: int | None = Field(default=None, ge=0, le=MAX_TIMESTAMP_MS)
    window_end_ms: int | None = Field(default=None, ge=0, le=MAX_TIMESTAMP_MS)
    observation_interval_ms: int | None = Field(default=None, ge=1, le=604_800_000)
    available_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    expires_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    sample_count: int = Field(ge=0, le=20_161)
    required_samples: int = Field(ge=0, le=20_161)
    numerator: int | None = Field(default=None, ge=0, le=MAX_CELLS)
    denominator: int | None = Field(default=None, ge=0, le=MAX_CELLS)
    expected_denominator: int = Field(ge=0, le=MAX_CELLS)

    @model_validator(mode="after")
    def bounded_value_and_counts(self):
        _canonical_reasons(self.reason_codes)
        if (self.computation_status == "VALID") != (self.value is not None):
            raise ValueError("only a valid metric value carries a numeric value")
        if self.value is not None:
            if re.fullmatch(DECIMAL_PATTERN, self.value) is None:
                raise ValueError("value must be a canonical finite decimal string")
            number = Decimal(self.value)
            limits = {
                "share_fraction": (Decimal(0), Decimal(1)),
                "rsi_0_100": (Decimal(0), Decimal(100)),
                "adx_0_100": (Decimal(0), Decimal(100)),
                "correlation_minus1_to1": (Decimal(-1), Decimal(1)),
                "index_minus3_to3": (Decimal(-3), Decimal(3)),
                "percentile_0_100": (Decimal(0), Decimal(100)),
                "volatility_fraction_annualized": (Decimal(0), None),
                "count": (Decimal(0), None),
                "instruments": (Decimal(0), None),
                "milliseconds": (Decimal(0), None),
                "volume_quote": (Decimal(0), None),
                "quote_volume": (Decimal(0), None),
                "price_quote": (Decimal(0), None),
                "atr_percent": (Decimal(0), None),
                "ratio": (Decimal(0), None),
                "percent": (Decimal(0), Decimal(100)),
                "return_percent": (Decimal(-100), None),
                "return_fraction": (Decimal(-1), None),
            }
            if self.unit in limits:
                lower, upper = limits[self.unit]
                if (lower is not None and number < lower) or (upper is not None and number > upper):
                    raise ValueError(f"value is outside the bounds for unit {self.unit}")
                if self.unit in ("count", "instruments", "milliseconds") and number != number.to_integral_value():
                    raise ValueError(f"value must be integral for unit {self.unit}")
        if self.expires_at_ms <= self.available_at_ms:
            raise ValueError("metric expiry must follow availability")
        if self.denominator is not None and self.numerator is not None and self.numerator > self.denominator:
            raise ValueError("numerator exceeds denominator")
        if self.denominator is not None and self.denominator > self.expected_denominator:
            raise ValueError("denominator exceeds expected denominator")
        return self


def validate_frame(frame: dict) -> None:
    """Validate a frame's compact series collection without deriving any market values."""
    if not isinstance(frame, dict) or frame.get("encoding") != ENCODING_V1:
        raise ValueError("frame encoding must be metric-series.v1")
    raw_series = frame.get("metric_series")
    if not isinstance(raw_series, list) or len(raw_series) > 4096:
        raise ValueError("frame metric_series must be a bounded list")
    parsed = [MetricSeriesV1.model_validate_json(json.dumps(row)) for row in raw_series]
    ids = [row.metric_id for row in parsed]
    if ids != sorted(set(ids)):
        raise ValueError("frame series must be sorted by unique metric id")


class MarketEventV1(ContractModel):
    event_id: str = Field(pattern=SHA256_PATTERN)
    occurrence_key: str = Field(pattern=SHA256_PATTERN)
    event_type: str = Field(pattern=IDENTITY_PATTERN)
    severity: Literal["info", "notice", "warning", "critical"]
    instrument_id: str | None = Field(default=None, pattern=INSTRUMENT_PATTERN)
    definition_ref: DefinitionRefV1
    provider_ref: ProviderRefV1
    source_window_start_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    source_window_end_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    observed_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    available_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    persisted_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    before: dict[str, str | None] = Field(max_length=64)
    value: dict[str, str | None] = Field(max_length=64)
    status: Literal["original", "corrected", "superseded", "retracted"]
    supersedes_event_id: str | None = Field(default=None, pattern=SHA256_PATTERN)
    snapshot_id: str = Field(pattern=SHA256_PATTERN)
    source_epoch: str = Field(pattern=EPOCH_PATTERN)
    source_sequence: int = Field(ge=1, le=9_223_372_036_854_775_807)
    is_historical_reconstruction: bool

    @model_validator(mode="after")
    def event_lineage(self):
        if self.source_window_end_ms < self.source_window_start_ms:
            raise ValueError("event source window is reversed")
        if not self.observed_at_ms <= self.available_at_ms <= self.persisted_at_ms:
            raise ValueError("event availability and persistence must follow observation")
        if self.observed_at_ms < self.source_window_end_ms or self.available_at_ms < self.provider_ref.available_at_ms:
            raise ValueError("event observation must follow its source window and provider availability")
        if self.snapshot_id != self.provider_ref.source_snapshot_id:
            raise ValueError("event snapshot and provider snapshot identity differ")
        if self.is_historical_reconstruction != (self.provider_ref.source_kind == "reconstructed"):
            raise ValueError("reconstructed source events require explicit historical reconstruction status")
        if self.source_epoch != self.provider_ref.epoch or self.source_sequence != self.provider_ref.sequence:
            raise ValueError("event and provider lineage differ")
        if self.status == "original" and self.supersedes_event_id is not None:
            raise ValueError("an original event cannot supersede another event")
        if self.status != "original" and self.supersedes_event_id is None:
            raise ValueError("event revisions must name the superseded event")
        for values in (self.before, self.value):
            for name, value in values.items():
                if re.fullmatch(IDENTITY_PATTERN, name) is None:
                    raise ValueError("event metric keys must be stable IDs")
                if value is not None and (
                    len(value) > 64 or re.fullmatch(DECIMAL_PATTERN, value) is None or not Decimal(value).is_finite()
                ):
                    raise ValueError("event metrics must be finite decimal strings or null")
        return self
