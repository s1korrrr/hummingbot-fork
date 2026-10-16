"""Strict lineage-preserving adapter from Rust records to native spot flow input.

Only a qualified, causally available individual match is returned as a
``SpotTrade``. The raw journal remains the owner of rejected observations.
"""

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from typing import Mapping

from hummingbot.strategy_v2.market_context.market_picture.microstructure import SpotTrade, normalize_spot_trade

_PROFILE = {"okx_eea_public_v1": ("public", "eea"), "okx_global_public_v1": ("public", "global")}
_PAIR = re.compile(r"^okx:spot:([A-Z0-9]+-[A-Z0-9]+)$")


def _exact(value: object, name: str) -> Decimal:
    if not isinstance(value, str) or not value or len(value) > 80:
        raise ValueError(f"{name} requires a bounded decimal source string")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"invalid {name}") from exc
    if not result.is_finite() or result <= 0 or len(result.as_tuple().digits) > 100 or abs(result.adjusted()) > 100:
        raise ValueError(f"invalid {name}")
    return result


@dataclass(frozen=True)
class NativeTradeObservation:
    instrument_id: str
    environment: str
    region: str
    source_profile: str
    metadata_revision: str
    source_epoch: str
    connection_id: str
    receive_sequence: int
    raw_sha256: str
    raw_trade_id: str
    event_at_ms: int
    available_at_ms: int
    source_reasons: tuple[str, ...]
    trade: SpotTrade | None
    reason: str | None

    @property
    def qualified(self) -> bool:
        return self.reason is None and self.trade is not None


def parse_rust_trade(record: Mapping, *, source_profile: str) -> NativeTradeObservation:
    if not isinstance(record, Mapping) or record.get("schema_version") != "okx.observations.v1":
        raise ValueError("unsupported observation schema")
    if record.get("venue") != "okx" or record.get("channel") != "trades-all":
        raise ValueError("individual OKX trade channel is required")
    identity = record.get("instrument_id")
    trade_id = record.get("trade_id")
    if (
        not isinstance(identity, str)
        or len(identity) > 80
        or _PAIR.fullmatch(identity) is None
        or not isinstance(trade_id, str)
        or not trade_id
    ):
        raise ValueError("invalid raw trade identity")
    for key, value in (
        ("environment", record.get("environment")),
        ("region", record.get("region")),
        ("source_profile", source_profile),
        ("metadata_revision", record.get("metadata_revision")),
        ("source_epoch", record.get("source_epoch")),
        ("connection_id", record.get("connection_id")),
    ):
        if not isinstance(value, str) or not 1 <= len(value) <= 128:
            raise ValueError(f"missing {key} lineage")
    if source_profile not in _PROFILE or (record["environment"], record["region"]) != _PROFILE[source_profile]:
        raise ValueError("trade regional source profile mismatch")
    receive_sequence = record.get("receive_sequence")
    raw_sha256 = record.get("raw_sha256")
    quality = record.get("quality")
    if type(receive_sequence) is not int or receive_sequence < 0:
        raise ValueError("invalid receive sequence")
    if (
        not isinstance(raw_sha256, str)
        or raw_sha256
        and (len(raw_sha256) != 64 or any(char not in "0123456789abcdef" for char in raw_sha256))
        or quality == "qualified"
        and not raw_sha256
    ):
        raise ValueError("raw payload hash is required")
    event = record.get("event_at_ms")
    available = record.get("available_at_ms")
    if type(event) is not int or event < 0 or type(available) is not int or available < 0:
        raise ValueError("invalid observation time")
    count = record.get("aggregate_count")
    if type(count) is not int or count != 1:
        raise ValueError("individual trade requires one match")
    if record.get("side") not in {"buy", "sell", "unknown"}:
        raise ValueError("invalid source aggressor side")
    price = _exact(record.get("price"), "trade price")
    quantity = _exact(record.get("base_quantity"), "base quantity")
    notional = _exact(record.get("quote_notional"), "quote notional")
    with localcontext() as context:
        context.prec = 256
        exact_notional = price * quantity
    if exact_notional != notional:
        raise ValueError("quote notional differs from exact price times base quantity")
    reasons = record.get("reason_codes")
    if (
        quality not in {"qualified", "unqualified"}
        or not isinstance(reasons, list)
        or len(reasons) > 32
        or any(not isinstance(value, str) or not value or len(value) > 128 for value in reasons)
        or quality == "qualified"
        and reasons
    ):
        raise ValueError("invalid source qualification")
    instrument_id = identity
    reason = "CLOCK_ANOMALY" if available < event else "SOURCE_UNQUALIFIED" if quality != "qualified" else None
    trade = None
    if reason is None:
        trade = normalize_spot_trade(
            {
                "instId": identity.split(":", 2)[2],
                "tradeId": trade_id,
                "px": str(price),
                "sz": str(quantity),
                "side": record.get("side"),
                "ts": str(event),
                "count": "1",
            },
            instrument_id=instrument_id,
            channel="trades-all",
            available_at_ms=available,
        )
    return NativeTradeObservation(
        instrument_id=instrument_id,
        environment=record["environment"],
        region=record["region"],
        source_profile=source_profile,
        metadata_revision=record["metadata_revision"],
        source_epoch=record["source_epoch"],
        connection_id=record["connection_id"],
        receive_sequence=receive_sequence,
        raw_sha256=raw_sha256,
        raw_trade_id=trade_id,
        event_at_ms=event,
        available_at_ms=available,
        source_reasons=tuple(reasons),
        trade=trade,
        reason=reason,
    )
