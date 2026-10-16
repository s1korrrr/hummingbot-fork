"""One-level OKX BBO observation. This is not a depth book or a fill model."""

import re
from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Mapping

_PROFILE = {"okx_eea_public_v1": ("public", "eea"), "okx_global_public_v1": ("public", "global")}
_PAIR = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")


def _decimal(value: Decimal, name: str) -> Decimal:
    if (
        not isinstance(value, Decimal)
        or not value.is_finite()
        or len(value.as_tuple().digits) > 100
        or value
        and abs(value.adjusted()) > 100
    ):
        raise ValueError(f"{name} must be an exact finite decimal")
    return value


@dataclass(frozen=True)
class QualifiedTopOfBook:
    instrument_id: str
    environment: str
    region: str
    source_profile: str
    metadata_revision: str
    source_epoch: str
    selection_epoch: str
    event_at_ms: int
    available_at_ms: int
    age_ms: int | None
    bid_quantity: Decimal | None
    ask_quantity: Decimal | None
    midpoint: Decimal | None
    spread_bps: Decimal | None
    reason: str | None
    depth_levels: int = 1

    @property
    def qualified(self) -> bool:
        return self.reason is None


@dataclass(frozen=True)
class TopOfBookObservation:
    instrument_id: str
    region: str
    environment: str
    source_profile: str
    metadata_revision: str
    source_epoch: str
    selection_epoch: str
    event_at_ms: int
    available_at_ms: int
    sequence: int | None
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal
    channel: str = "bbo-tbt"
    quality: str = "UNQUALIFIED"
    reason_codes: tuple[str, ...] = ()
    raw_sequence: str | None = None

    def __post_init__(self) -> None:
        if self.channel != "bbo-tbt":
            raise ValueError("a distinct bbo-tbt channel is required")
        if not (
            isinstance(self.instrument_id, str)
            and len(self.instrument_id) <= 80
            and self.instrument_id.startswith("okx:spot:")
            and _PAIR.fullmatch(self.instrument_id.split(":", 2)[2]) is not None
            and all(
                isinstance(item, str) and 1 <= len(item) <= 128
                for item in (
                    self.region,
                    self.environment,
                    self.source_profile,
                    self.metadata_revision,
                    self.source_epoch,
                    self.selection_epoch,
                )
            )
        ):
            raise ValueError("complete spot and source identity is required")
        if self.source_profile not in _PROFILE or (self.environment, self.region) != _PROFILE[self.source_profile]:
            raise ValueError("BBO regional source profile mismatch")
        if (
            type(self.event_at_ms) is not int
            or self.event_at_ms < 0
            or type(self.available_at_ms) is not int
            or self.available_at_ms < 0
            or self.sequence is not None
            and (type(self.sequence) is not int or self.sequence < 0)
        ):
            raise ValueError("invalid BBO time or sequence")
        if self.raw_sequence is not None and (
            not isinstance(self.raw_sequence, str)
            or not self.raw_sequence.isascii()
            or not self.raw_sequence.isdigit()
            or len(self.raw_sequence) > 19
            or int(self.raw_sequence) != self.sequence
        ):
            raise ValueError("raw BBO sequence disagrees with numeric sequence")
        if self.quality not in {"QUALIFIED", "UNQUALIFIED"}:
            raise ValueError("invalid BBO quality")
        if (
            not isinstance(self.reason_codes, tuple)
            or len(self.reason_codes) > 32
            or any(not isinstance(reason, str) or not reason or len(reason) > 128 for reason in self.reason_codes)
        ):
            raise ValueError("invalid bounded BBO reasons")
        for name in ("bid_price", "bid_quantity", "ask_price", "ask_quantity"):
            _decimal(getattr(self, name), name)

    @classmethod
    def from_rust_record(cls, record: Mapping, *, source_profile: str, selection_epoch: str) -> "TopOfBookObservation":
        if not isinstance(record, Mapping) or record.get("schema_version") != "okx.observations.v1":
            raise ValueError("unsupported observation schema")
        if record.get("venue") != "okx" or record.get("channel") != "bbo-tbt":
            raise ValueError("wrong venue or BBO channel")
        if record.get("snapshot") is not True or record.get("depth_scope") != "single_level":
            raise ValueError("BBO requires a single-level snapshot")
        raw_instrument = record.get("instrument_id")
        if (
            not isinstance(raw_instrument, str)
            or len(raw_instrument) > 80
            or not raw_instrument.startswith("okx:spot:")
            or _PAIR.fullmatch(raw_instrument.split(":", 2)[2]) is None
        ):
            raise ValueError("invalid raw spot instrument")
        raw_sequence = record.get("sequence")
        if raw_sequence is not None and (
            not isinstance(raw_sequence, str)
            or not raw_sequence.isascii()
            or not raw_sequence.isdigit()
            or len(raw_sequence) > 19
        ):
            raise ValueError("invalid raw BBO sequence")
        raw_quality = record.get("quality")
        reasons = record.get("reason_codes")
        if (
            raw_quality not in {"qualified", "unqualified"}
            or not isinstance(reasons, list)
            or raw_quality == "qualified"
            and reasons
        ):
            raise ValueError("invalid source qualification")
        values = {}
        for name in ("bid_price", "bid_quantity", "ask_price", "ask_quantity"):
            raw = record.get(name)
            if not isinstance(raw, str) or len(raw) > 80:
                raise ValueError("BBO decimal must be a bounded source string")
            try:
                values[name] = Decimal(raw)
            except Exception as exc:
                raise ValueError("invalid BBO decimal") from exc
        return cls(
            instrument_id=raw_instrument,
            environment=record.get("environment"),
            region=record.get("region"),
            source_profile=source_profile,
            metadata_revision=record.get("metadata_revision"),
            source_epoch=record.get("source_epoch"),
            selection_epoch=selection_epoch,
            event_at_ms=record.get("event_at_ms"),
            available_at_ms=record.get("available_at_ms"),
            sequence=int(raw_sequence) if raw_sequence is not None else None,
            raw_sequence=raw_sequence,
            quality=raw_quality.upper(),
            reason_codes=tuple(reasons),
            **values,
        )

    def as_of(self, *, as_of_ms: int, max_age_ms: int) -> QualifiedTopOfBook:
        if type(as_of_ms) is not int or as_of_ms < 0:
            raise ValueError("invalid as-of time")
        if type(max_age_ms) is not int or not 0 < max_age_ms <= 60_000:
            raise ValueError("a bounded positive quote age is required")
        age = as_of_ms - self.event_at_ms if as_of_ms >= self.event_at_ms else None
        reason = None
        if self.available_at_ms < self.event_at_ms:
            reason = "CLOCK_ANOMALY"
        elif self.available_at_ms > as_of_ms:
            reason = "NOT_YET_AVAILABLE"
        elif age is None:
            reason = "FUTURE_EVENT"
        elif self.quality != "QUALIFIED" or self.reason_codes:
            reason = "SOURCE_UNQUALIFIED"
        elif self.bid_price <= 0 or self.ask_price <= 0:
            reason = "INVALID_PRICE"
        elif self.bid_quantity <= 0 or self.ask_quantity <= 0:
            reason = "INVALID_SIZE"
        elif self.bid_price >= self.ask_price:
            reason = "LOCKED_OR_CROSSED"
        elif age > max_age_ms:
            reason = "STALE"
        with localcontext() as context:
            context.prec = 256
            midpoint = (self.bid_price + self.ask_price) / 2 if reason is None else None
            spread_bps = (
                Decimal(10_000) * (self.ask_price - self.bid_price) / midpoint if midpoint is not None else None
            )
        return QualifiedTopOfBook(
            instrument_id=self.instrument_id,
            environment=self.environment,
            region=self.region,
            source_profile=self.source_profile,
            metadata_revision=self.metadata_revision,
            source_epoch=self.source_epoch,
            selection_epoch=self.selection_epoch,
            event_at_ms=self.event_at_ms,
            available_at_ms=self.available_at_ms,
            age_ms=age,
            bid_quantity=self.bid_quantity if reason is None else None,
            ask_quantity=self.ask_quantity if reason is None else None,
            midpoint=midpoint,
            spread_bps=spread_bps,
            reason=reason,
        )
