"""Enrich completed Rust bars using complete journal membership and native flow.

This projection does not qualify a source. Callers supply independently verified
coverage and the complete ordered membership, never the bounded ID sample alone.
"""

import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal, localcontext
from typing import Mapping, Sequence

from hummingbot.strategy_v2.market_context.market_picture.microstructure import MAX_TRADES, FlowWindow, flow_window
from hummingbot.strategy_v2.market_context.observation_adapter import NativeTradeObservation


@dataclass(frozen=True)
class EnrichedObservationBar:
    flow: FlowWindow
    base_by_side: tuple[Decimal, Decimal, Decimal]
    count_by_side: tuple[int, int, int]
    vwap: Decimal | None
    quote_per_second: Decimal | None
    duration_ms: int
    available_at_ms: int


def bar_membership_sha256(
    bar: Mapping,
    observations: Sequence[NativeTradeObservation],
    *,
    instrument_id: str,
    source_epoch: str,
    source_profile: str,
    metadata_revision: str,
) -> str:
    """Bind full ordered normalized members and bar/source identity.

    This hash must be retained by an external verified journal traversal. A
    caller-computed hash alone does not establish completeness or quiet coverage.
    """
    if len(observations) > MAX_TRADES or len(bar) > 32:
        raise ValueError("unbounded bar membership")
    receipt = dict(
        bar=dict(bar),
        instrument_id=instrument_id,
        source_epoch=source_epoch,
        source_profile=source_profile,
        metadata_revision=metadata_revision,
        observations=[asdict(value) for value in observations],
    )
    payload = json.dumps(receipt, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def enrich_completed_bar(
    bar: Mapping,
    observations: Sequence[NativeTradeObservation],
    *,
    instrument_id: str,
    source_epoch: str,
    source_profile: str,
    metadata_revision: str,
    as_of_ms: int,
    coverage_start_ms: int,
    coverage_end_ms: int,
    side_verified: bool,
    verified_membership_sha256: str,
    calibrated_quote_threshold: Decimal | None = None,
) -> EnrichedObservationBar:
    """Check externally verified membership, then reuse the native flow kernel.

    verified_membership_sha256 must come from the caller's independently
    verified journal traversal for this bar. Count/OHLC/totals alone cannot
    establish time-bar membership; this pure function does not qualify that
    external receipt or its producer.

    Time bars use [start,end). Whole-trade notional bars include the closing
    event at end; their flow window ends one millisecond later, while rates use
    the actual end-start duration. Zero-duration event rates are unavailable.
    """
    if not isinstance(verified_membership_sha256, str) or verified_membership_sha256 != bar_membership_sha256(
        bar,
        observations,
        instrument_id=instrument_id,
        source_epoch=source_epoch,
        source_profile=source_profile,
        metadata_revision=metadata_revision,
    ):
        raise ValueError("external journal membership receipt mismatch")
    schema = bar.get("schema_version")
    if schema not in {"okx.observations.time-bar.v1", "okx.observations.notional-bar.v1"}:
        raise ValueError("unsupported completed bar")
    notional = schema == "okx.observations.notional-bar.v1"
    start, end, available = (bar.get(key) for key in ("start_ms", "end_ms", "available_at_ms"))
    if (
        any(
            type(value) is not int or value < 0
            for value in (start, end, available, as_of_ms, coverage_start_ms, coverage_end_ms)
        )
        or start > end
        or not notional
        and start == end
        or not end <= available <= as_of_ms
        or len(observations) > MAX_TRADES
    ):
        raise ValueError("invalid bar time, availability or membership bound")
    if not notional and (end - start != 60000 or start % 60000):
        raise ValueError("time-bar.v1 requires aligned complete 60-second intervals")
    flow_end = end + int(notional)
    if flow_end > as_of_ms or coverage_start_ms > start or coverage_end_ms < flow_end:
        raise ValueError("bar coverage is incomplete")
    if not notional and (bar.get("quality") not in {"qualified", "qualified_empty"} or bar.get("reason_codes") != []):
        raise ValueError("unavailable bar cannot become a qualified projection")
    identities = set()
    trades = []
    for observation in observations:
        if (
            not isinstance(observation, NativeTradeObservation)
            or not observation.qualified
            or (
                observation.instrument_id,
                observation.source_epoch,
                observation.source_profile,
                observation.metadata_revision,
            )
            != (instrument_id, source_epoch, source_profile, metadata_revision)
            or observation.available_at_ms > available
            or not start <= observation.event_at_ms < flow_end
            or observation.raw_trade_id in identities
        ):
            raise ValueError("bar member scope, availability or duplicate mismatch")
        identities.add(observation.raw_trade_id)
        trades.append(observation.trade)
    count = bar.get("trade_id_count" if notional else "trade_count")
    if (
        type(count) is not int
        or count != len(trades)
        or not notional
        and (bar["quality"] == "qualified_empty") != (count == 0)
    ):
        raise ValueError("complete bar membership is required")
    if notional:
        if (
            not isinstance(calibrated_quote_threshold, Decimal)
            or not calibrated_quote_threshold.is_finite()
            or calibrated_quote_threshold <= 0
        ):
            raise ValueError("independently frozen calibration threshold is required")
        digest = "0" * 64
        ids = [value.raw_trade_id for value in observations]
        for identity in ids:
            digest = hashlib.sha256(f"{digest}:{identity}".encode()).hexdigest()
        if (
            not ids
            or bar.get("close_reason") != "THRESHOLD"
            or bar.get("trade_ids") != ids[:16]
            or type(bar.get("trade_ids_truncated")) is not bool
            or bar["trade_ids_truncated"] != (len(ids) > 16)
            or bar.get("trade_ids_sha256") != digest
            or bar.get("first_trade_id") != ids[0]
            or bar.get("last_trade_id") != ids[-1]
            or trades[0].event_at_ms != start
            or trades[-1].event_at_ms != end
        ):
            raise ValueError("notional bar membership digest mismatch")
    with localcontext() as context:
        context.prec = 256
        flow = flow_window(
            trades,
            instrument_id=instrument_id,
            channel="trades-all",
            start_ms=start,
            end_ms=flow_end,
            as_of_ms=as_of_ms,
            coverage_start_ms=coverage_start_ms,
            coverage_end_ms=coverage_end_ms,
            reset_id=source_epoch,
            side_verified=side_verified,
        )
        bases = [Decimal(0), Decimal(0), Decimal(0)]
        counts = [0, 0, 0]
        for trade in trades:
            index = ("buy", "sell", "unknown").index(trade.side if side_verified else "unknown")
            bases[index] += trade.quantity
            counts[index] += 1
        base = sum(bases)
        quote = flow.buy_quote + flow.sell_quote + flow.unknown_quote
        if (
            Decimal(str(bar.get("quote_notional"))) != quote
            or not notional
            and Decimal(str(bar.get("base_quantity"))) != base
        ):
            raise ValueError("bar totals differ from native member totals")
        if notional and (
            quote < calibrated_quote_threshold
            or Decimal(str(bar.get("overshoot"))) != quote - calibrated_quote_threshold
            or quote - trades[-1].quote_notional >= calibrated_quote_threshold
        ):
            raise ValueError("notional threshold crossing or overshoot mismatch")
        prices = [trade.price for trade in trades]
        expected = (prices[0], max(prices), min(prices), prices[-1]) if prices else (None,) * 4
        actual = tuple(
            None if bar.get(key) is None else Decimal(str(bar[key])) for key in ("open", "high", "low", "close")
        )
        if actual != expected:
            raise ValueError("bar OHLC differs from ordered membership")
        duration = end - start
        return EnrichedObservationBar(
            flow,
            tuple(bases),
            tuple(counts),
            quote / base if base else None,
            quote * 1000 / duration if duration else None,
            duration,
            available,
        )
