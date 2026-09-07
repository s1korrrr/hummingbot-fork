"""Durable admission watermark for one controller candle namespace."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from sqlalchemy import BigInteger, CheckConstraint, Column, Integer, Text

from hummingbot.model import HummingbotBase

CONTROLLER_CANDLE_WATERMARK_SCHEMA_VERSION = 1

# Calendar-month intervals are deliberately excluded: fixed-second alignment is
# not a truthful model for them.
SAFE_INTERVAL_SECONDS = {
    "1s": 1,
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1_800,
    "1h": 3_600,
    "2h": 7_200,
    "4h": 14_400,
    "6h": 21_600,
    "8h": 28_800,
    "12h": 43_200,
    "1d": 86_400,
    "3d": 259_200,
    "1w": 604_800,
}


class ControllerCandleWatermarkOutcome(str, Enum):
    SEEDED = "SEEDED"
    ADVANCED_BY_CALLER = "ADVANCED_BY_CALLER"
    ALREADY_COMMITTED = "ALREADY_COMMITTED"
    REGRESSION = "REGRESSION"
    CAS_CONFLICT = "CAS_CONFLICT"
    INVALID_STATE = "INVALID_STATE"
    STORE_UNAVAILABLE = "STORE_UNAVAILABLE"
    COMMIT_FAILED = "COMMIT_FAILED"


def _require_exact_nonempty(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be an exact nonempty string")
    return value


def _require_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


@dataclass(frozen=True)
class ControllerCandleWatermarkNamespace:
    controller_id: str
    connector_name: str
    trading_pair: str
    interval: str

    def __post_init__(self) -> None:
        _require_exact_nonempty(self.controller_id, "controller_id")
        _require_exact_nonempty(self.connector_name, "connector_name")
        _require_exact_nonempty(self.trading_pair, "trading_pair")
        _require_exact_nonempty(self.interval, "interval")
        if self.interval not in SAFE_INTERVAL_SECONDS:
            raise ValueError(f"interval is not registered for fixed-second watermark alignment: {self.interval}")

    @property
    def interval_seconds(self) -> int:
        return SAFE_INTERVAL_SECONDS[self.interval]


@dataclass(frozen=True)
class ControllerCandleWatermarkState:
    namespace: ControllerCandleWatermarkNamespace
    candle_open_timestamp_s: int
    revision: int
    schema_version: int
    updated_at_ms: int

    def __post_init__(self) -> None:
        if not isinstance(self.namespace, ControllerCandleWatermarkNamespace):
            raise ValueError("namespace must be a ControllerCandleWatermarkNamespace")
        timestamp = _require_int(self.candle_open_timestamp_s, "candle_open_timestamp_s")
        revision = _require_int(self.revision, "revision")
        schema_version = _require_int(self.schema_version, "schema_version")
        updated_at_ms = _require_int(self.updated_at_ms, "updated_at_ms")
        if timestamp <= 0 or timestamp % self.namespace.interval_seconds != 0:
            raise ValueError("candle_open_timestamp_s must be positive and aligned to interval")
        if revision < 0:
            raise ValueError("revision must be greater than or equal to zero")
        if schema_version != CONTROLLER_CANDLE_WATERMARK_SCHEMA_VERSION:
            raise ValueError("schema_version must equal 1")
        if updated_at_ms <= 0:
            raise ValueError("updated_at_ms must be positive")


@dataclass(frozen=True)
class ControllerCandleWatermarkResult:
    outcome: ControllerCandleWatermarkOutcome
    state: Optional[ControllerCandleWatermarkState] = None
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, ControllerCandleWatermarkOutcome):
            raise ValueError("outcome must be a ControllerCandleWatermarkOutcome")

    @property
    def proposal_eligible(self) -> bool:
        return self.outcome is ControllerCandleWatermarkOutcome.ADVANCED_BY_CALLER


class ControllerCandleWatermark(HummingbotBase):
    __tablename__ = "controller_candle_watermark"

    controller_id = Column(
        Text, CheckConstraint("length(controller_id) > 0"), primary_key=True, nullable=False
    )
    connector_name = Column(
        Text, CheckConstraint("length(connector_name) > 0"), primary_key=True, nullable=False
    )
    trading_pair = Column(
        Text, CheckConstraint("length(trading_pair) > 0"), primary_key=True, nullable=False
    )
    interval = Column(Text, CheckConstraint("length(interval) > 0"), primary_key=True, nullable=False)
    candle_open_timestamp_s = Column(
        BigInteger, CheckConstraint("candle_open_timestamp_s > 0"), nullable=False
    )
    revision = Column(BigInteger, CheckConstraint("revision >= 0"), nullable=False)
    schema_version = Column(Integer, CheckConstraint("schema_version = 1"), nullable=False)
    updated_at_ms = Column(BigInteger, CheckConstraint("updated_at_ms > 0"), nullable=False)

    def to_state(self) -> ControllerCandleWatermarkState:
        return ControllerCandleWatermarkState(
            namespace=ControllerCandleWatermarkNamespace(
                controller_id=self.controller_id,
                connector_name=self.connector_name,
                trading_pair=self.trading_pair,
                interval=self.interval,
            ),
            candle_open_timestamp_s=self.candle_open_timestamp_s,
            revision=self.revision,
            schema_version=self.schema_version,
            updated_at_ms=self.updated_at_ms,
        )
