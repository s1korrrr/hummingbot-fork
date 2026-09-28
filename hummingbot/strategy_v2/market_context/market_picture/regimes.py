"""Strict observation records for exact recorded/estimated regime provenance."""

import re
from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import Field, model_validator

from .contracts import EPOCH_PATTERN, INSTRUMENT_PATTERN, MAX_TIMESTAMP_MS, SHA256_PATTERN, ContractModel, ProviderRefV1

ConfidenceUnit = Literal["fraction", "percent", "named_score"]
ConfidenceKind = Literal["model_probability", "model_score", "reported_unknown"]
CalibrationStatus = Literal["calibrated", "uncalibrated", "unknown", "not_applicable"]
RegimeOrigin = Literal["controller_recorded", "market_observation"]


class RegimeObservationV1(ContractModel):
    instrument_id: str = Field(pattern=INSTRUMENT_PATTERN)
    source_bar_close_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    origin: RegimeOrigin
    producer_id: str = Field(min_length=1, max_length=128)
    controller_id: str | None = Field(default=None, min_length=1, max_length=128)
    regime_label: str | None = Field(default=None, min_length=1, max_length=128)
    regime_definition_id: str | None = Field(default=None, min_length=1, max_length=128)
    confidence_value: str | None = Field(default=None, max_length=64)
    confidence_unit: ConfidenceUnit | None = None
    confidence_kind: ConfidenceKind | None = None
    calibration_status: CalibrationStatus
    model_id: str | None = Field(default=None, pattern=SHA256_PATTERN)
    model_version: str | None = Field(default=None, max_length=64)
    fit_id: str | None = Field(default=None, max_length=128)
    feature_schema_hash: str | None = Field(default=None, pattern=SHA256_PATTERN)
    trend_direction: Literal["up", "down", "flat"] | None = None
    trend_confidence_value: str | None = Field(default=None, max_length=64)
    trend_confidence_unit: ConfidenceUnit | None = None
    context_bias: str | None = Field(default=None, max_length=128)
    provider_ref: ProviderRefV1
    available_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    expires_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    source_epoch: str = Field(pattern=EPOCH_PATTERN)
    source_sequence: int = Field(ge=1)

    @model_validator(mode="after")
    def semantically_bound(self):
        decimal_pattern = r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$"
        for name, value in (
            ("confidence_value", self.confidence_value),
            ("trend_confidence_value", self.trend_confidence_value),
        ):
            if value is not None and (not isinstance(value, str) or re.fullmatch(decimal_pattern, value) is None):
                raise ValueError(f"{name} must be a finite decimal string")
            if value is not None:
                try:
                    if not Decimal(value).is_finite():
                        raise ValueError(f"{name} must be finite")
                except InvalidOperation as exc:
                    raise ValueError(f"{name} must be a valid decimal") from exc
        if (self.confidence_value is None) != (self.confidence_unit is None):
            raise ValueError("confidence value and unit are inseparable")
        if (self.confidence_value is None) != (self.confidence_kind is None):
            raise ValueError("confidence value and semantics are inseparable")
        if self.confidence_value is not None:
            value = Decimal(self.confidence_value)
            if self.confidence_unit == "fraction" and not 0 <= value <= 1:
                raise ValueError("fraction confidence must be within [0,1]")
            if self.confidence_unit == "percent" and not 0 <= value <= 100:
                raise ValueError("percent confidence must be within [0,100]")
            if self.confidence_kind == "model_probability" and self.confidence_unit not in ("fraction", "percent"):
                raise ValueError("model probability must declare fraction or percent units")
        if (self.trend_confidence_value is None) != (self.trend_confidence_unit is None):
            raise ValueError("trend confidence value and unit are inseparable")
        if self.trend_confidence_value is not None:
            value = Decimal(self.trend_confidence_value)
            if self.trend_confidence_unit == "fraction" and not 0 <= value <= 1:
                raise ValueError("trend fraction confidence must be within [0,1]")
            if self.trend_confidence_unit == "percent" and not 0 <= value <= 100:
                raise ValueError("trend percent confidence must be within [0,100]")
        if (
            self.expires_at_ms <= self.available_at_ms
            or self.available_at_ms < self.source_bar_close_ms
            or self.available_at_ms < self.provider_ref.available_at_ms
            or self.expires_at_ms > self.provider_ref.expires_at_ms
        ):
            raise ValueError("regime availability or expiry is invalid")
        if self.source_epoch != self.provider_ref.epoch or self.source_sequence != self.provider_ref.sequence:
            raise ValueError("regime and provider source lineage differ")
        if self.origin == "controller_recorded" and self.controller_id is None:
            raise ValueError("recorded regime requires its controller identity")
        if self.origin == "market_observation" and self.controller_id is not None:
            raise ValueError("market observation cannot impersonate a controller")
        if self.regime_label is None or self.regime_definition_id is None:
            raise ValueError("a regime observation requires its label and definition identity")
        if self.confidence_value is None and self.calibration_status not in ("unknown", "not_applicable"):
            raise ValueError("calibration state without confidence must be unknown or not applicable")
        return self


def validate_regime(
    record: RegimeObservationV1,
    *,
    expected_instrument_id: str,
    expected_source_bar_close_ms: int,
    expected_producer_id: str | None = None,
) -> RegimeObservationV1:
    """Bind a record to one exact instrument and bar; no cross-asset or stale-label fallback."""
    if record.instrument_id != expected_instrument_id:
        raise ValueError("regime instrument does not match the requested instrument")
    if record.source_bar_close_ms != expected_source_bar_close_ms:
        raise ValueError("regime record is not from the requested source bar")
    if expected_producer_id is not None and record.producer_id != expected_producer_id:
        raise ValueError("regime producer identity does not match")
    return record
