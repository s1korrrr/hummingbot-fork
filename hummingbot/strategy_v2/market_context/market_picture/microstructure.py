"""Observation-only spot trade and visible-book calculations.

No network, clock, credential or execution imports. Callers supply recorded
availability, coverage and access evidence. OKX field semantics were checked
against https://my.okx.com/docs-v5/en/ on 2026-10-15. These calculations do not
qualify a provider, promise execution, or reconstruct expired raw observations.
"""

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Mapping, Sequence

from .contracts import INSTRUMENT_PATTERN, MAX_TIMESTAMP_MS

ZERO = Decimal(0)
ONE = Decimal(1)
BPS = Decimal(10_000)
MAX_TRADES = 100_000
REFERENCE_MINUTES = 7 * 24 * 60


def _number(value, *, positive=False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, str, int, float)):
        raise ValueError("expected a finite decimal observation")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError("expected a finite decimal observation") from error
    if (not result.is_finite() or len(result.as_tuple().digits) > 100
            or (result and abs(result.adjusted()) > 100) or (positive and result <= 0)):
        raise ValueError("observation is outside supported finite numeric bounds")
    return result


def _time(value) -> int:
    if isinstance(value, str) and value.isascii() and value.isdigit() and len(value) <= 15:
        value = int(value)
    if type(value) is not int or not 0 <= value <= MAX_TIMESTAMP_MS:
        raise ValueError("invalid UTC millisecond observation time")
    return value


def _identity(instrument_id):
    if not isinstance(instrument_id, str) or re.fullmatch(INSTRUMENT_PATTERN, instrument_id) is None:
        raise ValueError("an exact spot instrument identity is required")


@dataclass(frozen=True)
class SpotTrade:
    instrument_id: str
    channel: str
    trade_id: int
    aggregate_count: int
    price: Decimal
    quantity: Decimal
    side: str
    event_at_ms: int
    available_at_ms: int

    def __post_init__(self):
        _identity(self.instrument_id)
        if (self.channel not in {"trades", "trades-all", "rest-trades"}
                or type(self.trade_id) is not int or self.trade_id < 0
                or type(self.aggregate_count) is not int or not 1 <= self.aggregate_count <= 1_000_000
                or self.aggregate_count > self.trade_id + 1
                or (self.channel != "trades" and self.aggregate_count != 1)
                or self.side not in {"buy", "sell", "unknown"}):
            raise ValueError("invalid normalized trade identity")
        for value in (self.price, self.quantity):
            if not isinstance(value, Decimal):
                raise ValueError("normalized trades require exact decimal quantities")
            _number(value, positive=True)
        if _time(self.available_at_ms) < _time(self.event_at_ms):
            raise ValueError("trade availability precedes its event")

    @property
    def quote_notional(self):
        # sz is already the aggregate quantity. count is never a multiplier.
        return self.price * self.quantity


def normalize_spot_trade(payload: Mapping, *, instrument_id: str, channel: str,
                         available_at_ms: int) -> SpotTrade:
    _identity(instrument_id)
    if channel not in {"trades", "trades-all", "rest-trades"}:
        raise ValueError("unsupported trade channel")
    if payload.get("instId") != instrument_id.split(":", 2)[2]:
        raise ValueError("trade instrument scope mismatch")
    event = _time(payload.get("ts"))
    available = _time(available_at_ms)
    if available < event:
        raise ValueError("trade was not available before its event")
    raw_id, raw_count = payload.get("tradeId"), payload.get("count", "1")
    if (not isinstance(raw_id, str) or not raw_id.isascii() or not raw_id.isdigit() or len(raw_id) > 30
            or not isinstance(raw_count, str) or not raw_count.isascii() or not raw_count.isdigit()
            or len(raw_count) > 7):
        raise ValueError("invalid trade identity or aggregate count")
    identity, count = int(raw_id), int(raw_count)
    if not 1 <= count <= 1_000_000 or count > identity + 1 or (channel != "trades" and count != 1):
        raise ValueError("invalid aggregate count for trade channel")
    side = payload.get("side")
    if side not in {None, "", "unknown", "buy", "sell"}:
        raise ValueError("unsupported aggressor-side encoding")
    return SpotTrade(instrument_id, channel, identity, count,
                     _number(payload.get("px"), positive=True), _number(payload.get("sz"), positive=True),
                     side if side in {"buy", "sell"} else "unknown", event, available)


@dataclass(frozen=True)
class FlowWindow:
    instrument_id: str
    channel: str
    start_ms: int
    end_ms: int
    reset_id: str
    buy_quote: Decimal
    sell_quote: Decimal
    unknown_quote: Decimal
    known_imbalance: Decimal | None
    confirmation_imbalance: Decimal | None
    unknown_share: Decimal | None
    trade_records: int
    duplicate_records: int
    complete: bool
    reasons: tuple[str, ...]

    @property
    def signed_quote(self):
        return self.buy_quote - self.sell_quote


def flow_window(trades: Sequence[SpotTrade], *, instrument_id: str, channel: str,
                start_ms: int, end_ms: int, as_of_ms: int, coverage_start_ms: int,
                coverage_end_ms: int, reset_id: str, side_verified: bool, gaps=()) -> FlowWindow:
    """Aggregate one half-open window from one source channel and epoch.

    Coverage bounds must come from collector receipts, never first/last trade.
    Unknown aggressors retain observed notional but block qualified confirmation.
    """
    _identity(instrument_id)
    for instant in (start_ms, end_ms, as_of_ms, coverage_start_ms, coverage_end_ms):
        _time(instant)
    if (not start_ms < end_ms <= as_of_ms or coverage_end_ms < coverage_start_ms
            or len(trades) > MAX_TRADES or len(gaps) > 10_000
            or not isinstance(reset_id, str) or not 1 <= len(reset_id) <= 128
            or channel not in {"trades", "trades-all", "rest-trades"} or type(side_verified) is not bool):
        raise ValueError("invalid or unbounded flow window")
    reasons = set()
    if coverage_start_ms > start_ms or coverage_end_ms < end_ms:
        reasons.add("INCOMPLETE_STREAM")
    for left, right in gaps:
        _time(left)
        _time(right)
        if right <= left:
            raise ValueError("invalid stream gap")
        if left < end_ms and right > start_ms:
            reasons.add("INCOMPLETE_STREAM")
    if not side_verified:
        reasons.add("AGGRESSOR_UNVERIFIED")
    accepted = {}
    duplicates = 0
    for trade in trades:
        if not isinstance(trade, SpotTrade) or (trade.instrument_id, trade.channel) != (instrument_id, channel):
            raise ValueError("trade scope does not match flow window")
        if not start_ms <= trade.event_at_ms < end_ms:
            continue
        if trade.available_at_ms > as_of_ms:
            reasons.add("INCOMPLETE_STREAM")
            continue
        if trade.trade_id in accepted:
            original = accepted[trade.trade_id]
            # Later retransmission receipt must not change first availability.
            if (original.price, original.quantity, original.side, original.event_at_ms, original.aggregate_count) != (
                    trade.price, trade.quantity, trade.side, trade.event_at_ms, trade.aggregate_count):
                raise ValueError("conflicting duplicate trade")
            duplicates += 1
            continue
        accepted[trade.trade_id] = trade
    previous_end = -1
    totals = {"buy": ZERO, "sell": ZERO, "unknown": ZERO}
    for identity, trade in sorted(accepted.items()):
        if identity - trade.aggregate_count + 1 <= previous_end:
            raise ValueError("overlap in aggregate trade identities")
        previous_end = identity
        totals[trade.side if side_verified else "unknown"] += trade.quote_notional
    buy, sell, unknown = totals["buy"], totals["sell"], totals["unknown"]
    known, total = buy + sell, buy + sell + unknown
    imbalance = (buy - sell) / known if known else None
    if unknown:
        reasons.add("UNKNOWN_AGGRESSOR")
    complete = "INCOMPLETE_STREAM" not in reasons
    return FlowWindow(instrument_id, channel, start_ms, end_ms, reset_id, buy, sell, unknown,
                      imbalance, imbalance if not reasons else None, unknown / total if total else None,
                      len(accepted), duplicates, complete, tuple(sorted(reasons)))


class CumulativeDelta:
    """A bounded explicit epoch state. Gaps cannot be repaired by a later window."""

    def __init__(self):
        self.scope = None
        self.end_ms = None
        self.value = None
        self._last_window = None

    def append(self, window: FlowWindow) -> Decimal | None:
        if window == self._last_window:
            return self.value
        scope = (window.instrument_id, window.channel, window.reset_id)
        if self.scope != scope:
            self.scope, self.end_ms, self.value = scope, window.start_ms, ZERO
        if window.start_ms != self.end_ms or not window.complete or window.reasons:
            self.value = None
        elif self.value is not None:
            self.value += window.signed_quote
        self.end_ms = window.end_ms
        self._last_window = window
        return self.value


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    quantity: Decimal
    non_rpi_quantity: Decimal | None

    def __post_init__(self):
        if not isinstance(self.price, Decimal) or not isinstance(self.quantity, Decimal):
            raise ValueError("normalized book levels require exact decimals")
        _number(self.price, positive=True)
        if _number(self.quantity) < 0:
            raise ValueError("negative book quantity")
        if self.non_rpi_quantity is not None and (
                not isinstance(self.non_rpi_quantity, Decimal)
                or not ZERO <= _number(self.non_rpi_quantity) <= self.quantity):
            raise ValueError("organic book subset is not contained in total depth")


@dataclass(frozen=True)
class BookObservation:
    instrument_id: str
    channel: str
    sequence: int
    event_at_ms: int
    available_at_ms: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]

    def __post_init__(self):
        _identity(self.instrument_id)
        if (self.channel not in {"books", "books50-l2-tbt", "books-l2-tbt", "books-rpi"}
                or type(self.sequence) is not int or not 0 <= self.sequence < 2**63
                or _time(self.available_at_ms) < _time(self.event_at_ms)):
            raise ValueError("invalid normalized book identity")
        for levels, reverse in ((self.bids, True), (self.asks, False)):
            if not isinstance(levels, tuple) or not 1 <= len(levels) <= (50 if self.channel == "books50-l2-tbt" else 400):
                raise ValueError("normalized book exceeds channel bounds")
            if any(not isinstance(row, BookLevel) or row.quantity <= 0
                   or (self.channel == "books-rpi" and row.non_rpi_quantity is None) for row in levels):
                raise ValueError("invalid normalized book level")
            prices = tuple(row.price for row in levels)
            if prices != tuple(sorted(set(prices), reverse=reverse)):
                raise ValueError("normalized book levels must be sorted and unique")
        if self.bids[0].price >= self.asks[0].price:
            raise ValueError("locked or crossed normalized book")

    @property
    def mid(self):
        return (self.bids[0].price + self.asks[0].price) / 2


class BookStream:
    """Channel-specific bounded book reconstruction; malformed updates fail closed.

    RPI is supported as snapshots only: its seqId does not imply the ordinary
    incremental prevSeqId contract. Fixed-zero checksum is never integrity proof.
    """

    def __init__(self, instrument_id: str, *, channel="books"):
        _identity(instrument_id)
        if channel not in {"books", "books50-l2-tbt", "books-l2-tbt", "books-rpi"}:
            raise ValueError("unsupported book channel")
        self.instrument_id, self.channel = instrument_id, channel
        self.view = None
        self.reason = "SNAPSHOT_REQUIRED"
        self._last_message = None
        self._max_levels = 50 if channel == "books50-l2-tbt" else 400

    def apply(self, action: str, payload: Mapping, *, available_at_ms: int):
        try:
            return self._apply(action, payload, available_at_ms)
        except (ValueError, TypeError, KeyError):
            self.view, self.reason = None, "INVALID_BOOK"
            raise

    def _apply(self, action, payload, available_at_ms):
        if action not in {"snapshot", "update"} or (self.channel == "books-rpi" and action != "snapshot"):
            raise ValueError("action is unsupported for book channel")
        event, available = _time(payload.get("ts")), _time(available_at_ms)
        sequence, previous = payload.get("seqId"), payload.get("prevSeqId")
        if (type(sequence) is not int or not 0 <= sequence < 2**63 or available < event
                or (self.channel != "books-rpi" and (type(previous) is not int or not -1 <= previous < 2**63))):
            raise ValueError("invalid book sequence or availability")
        parsed = []
        for name in ("bids", "asks"):
            rows = payload.get(name)
            if not isinstance(rows, (list, tuple)) or len(rows) > self._max_levels:
                raise ValueError("book update exceeds channel depth bound")
            levels = {}
            for row in rows:
                if not isinstance(row, (list, tuple)) or len(row) != 4:
                    raise ValueError("invalid book level")
                price, quantity = _number(row[0], positive=True), _number(row[1])
                organic = _number(row[2]) if self.channel == "books-rpi" else None
                if (quantity < 0 or (organic is not None and not ZERO <= organic <= quantity)
                        or price in levels):
                    raise ValueError("invalid or duplicate book level")
                levels[price] = BookLevel(price, quantity, organic)
            parsed.append(levels)
        signature = (action, sequence, previous, event, tuple(parsed[0].values()), tuple(parsed[1].values()))
        if signature == self._last_message and self.view is not None:
            return self.view
        if action == "snapshot":
            if self.channel != "books-rpi" and previous != -1:
                raise ValueError("snapshot requires snapshot sequence marker")
            books = [{}, {}]
        else:
            if self.view is None:
                return None
            if previous != self.view.sequence or event < self.view.event_at_ms:
                self.view, self.reason = None, "SEQUENCE_GAP"
                return None
            if sequence == previous and any(parsed):
                raise ValueError("same-sequence keepalive must not change depth")
            # seqId < prevSeqId with an exact chain is the documented reset.
            books = [{level.price: level for level in levels} for levels in (self.view.bids, self.view.asks)]
        for book, updates in zip(books, parsed):
            for price, level in updates.items():
                if level.quantity:
                    book[price] = level
                else:
                    book.pop(price, None)
            if len(book) > self._max_levels:
                raise ValueError("retained book exceeds channel depth bound; resnapshot required")
        bids = tuple(books[0][price] for price in sorted(books[0], reverse=True))
        asks = tuple(books[1][price] for price in sorted(books[1]))
        if not bids or not asks or bids[0].price >= asks[0].price:
            raise ValueError("empty, locked or crossed book")
        self.view = BookObservation(self.instrument_id, self.channel, sequence, event, available, bids, asks)
        self.reason, self._last_message = None, signature
        return self.view


def _book_reason(book, as_of_ms, max_age_ms, access_basis):
    _time(as_of_ms)
    if type(max_age_ms) is not int or not 0 < max_age_ms <= 60_000:
        raise ValueError("invalid book freshness bound")
    if access_basis not in {"non_rpi", "rpi_authorized", "unknown"}:
        raise ValueError("unsupported book access basis")
    if book is None:
        return "SNAPSHOT_REQUIRED"
    if book.event_at_ms > as_of_ms or book.available_at_ms > as_of_ms:
        return "FUTURE_TIMESTAMP"
    if as_of_ms - book.event_at_ms > max_age_ms:
        return "STALE"
    if access_basis == "unknown":
        return "ACCESS_BASIS_UNKNOWN"
    return None


def _quantity(level, book, access_basis):
    if book.channel == "books-rpi" and access_basis == "non_rpi":
        return level.non_rpi_quantity
    return level.quantity


@dataclass(frozen=True)
class BookCost:
    requested_size: Decimal
    size_unit: str
    covered_base: Decimal
    covered_quote: Decimal
    uncovered_size: Decimal
    covered_vwap: Decimal | None
    vwap: Decimal | None
    cost_bps: Decimal | None
    fee_bps: Decimal | None
    fee_source: str | None
    total_cost_bps: Decimal | None
    reasons: tuple[str, ...]


def visible_book_cost(book: BookObservation | None, *, side: str, requested_size: Decimal,
                      size_unit: str, as_of_ms: int, access_basis="non_rpi", max_age_ms=5_000,
                      fee_bps=None, fee_source=None) -> BookCost:
    size = _number(requested_size, positive=True)
    if side not in {"buy", "sell"} or size_unit not in {"base", "quote"}:
        raise ValueError("size basis and side must be explicit")
    if (fee_bps is None) != (fee_source is None) or (fee_source is not None and (
            not isinstance(fee_source, str) or not 1 <= len(fee_source) <= 128)):
        raise ValueError("a fee estimate requires explicit fee provenance")
    fee = _number(fee_bps) if fee_bps is not None else None
    if fee is not None and not ZERO <= fee <= BPS:
        raise ValueError("invalid fee scenario")
    reason = _book_reason(book, as_of_ms, max_age_ms, access_basis)
    if reason:
        return BookCost(size, size_unit, ZERO, ZERO, size, None, None, None, fee, fee_source, None, (reason,))
    remaining, base, quote = size, ZERO, ZERO
    for level in book.asks if side == "buy" else book.bids:
        quantity = _quantity(level, book, access_basis)
        if size_unit == "base":
            consumed_base = min(quantity, remaining)
            consumed_quote = consumed_base * level.price
            remaining -= consumed_base
        else:
            consumed_quote = min(quantity * level.price, remaining)
            consumed_base = consumed_quote / level.price
            remaining -= consumed_quote
        base += consumed_base
        quote += consumed_quote
        if remaining == 0:
            break
    covered_vwap = quote / base if base else None
    full_vwap = covered_vwap if remaining == 0 else None
    price_cost = None if full_vwap is None else BPS * (
        full_vwap / book.mid - ONE if side == "buy" else ONE - full_vwap / book.mid)
    total_cost = price_cost + fee * full_vwap / book.mid if price_cost is not None and fee is not None else None
    reasons = []
    if remaining:
        reasons.append("BOOK_DEPTH_INSUFFICIENT")
    if fee is None:
        reasons.append("FEE_UNAVAILABLE")
    return BookCost(size, size_unit, base, quote, remaining, covered_vwap, full_vwap,
                    price_cost, fee, fee_source, total_cost, tuple(reasons))


@dataclass(frozen=True)
class BandDepth:
    bid_covered_quote: Decimal
    ask_covered_quote: Decimal
    complete: bool
    imbalance: Decimal | None
    reasons: tuple[str, ...]


def visible_depth(book: BookObservation | None, *, band_bps: Decimal, as_of_ms: int,
                  access_basis="non_rpi", max_age_ms=5_000) -> BandDepth:
    band = _number(band_bps, positive=True)
    if band > BPS:
        raise ValueError("band exceeds 100 percent")
    reason = _book_reason(book, as_of_ms, max_age_ms, access_basis)
    if reason:
        return BandDepth(ZERO, ZERO, False, None, (reason,))
    lower, upper = book.mid * (ONE - band / BPS), book.mid * (ONE + band / BPS)
    bid = sum((row.price * _quantity(row, book, access_basis) for row in book.bids if row.price >= lower), ZERO)
    ask = sum((row.price * _quantity(row, book, access_basis) for row in book.asks if row.price <= upper), ZERO)
    complete = book.bids[-1].price <= lower and book.asks[-1].price >= upper
    total = bid + ask
    reasons = ("BOOK_DEPTH_INSUFFICIENT",) if not complete else (("ZERO_DENOMINATOR",) if not total else ())
    return BandDepth(bid, ask, complete, (bid - ask) / total if complete and total else None, reasons)


def reference_percentile(current: Decimal, observations, *, key: tuple, as_of_ms: int) -> Decimal | None:
    """Midrank against 7d completed-minute same-size/access observations, >=90%.

    The identity must bind instrument, metric definition, side, size unit/amount
    and access basis. Records include original availability. One observation per
    UTC minute; the current minute and later-arriving observations are excluded.
    """
    value = _number(current)
    _time(as_of_ms)
    if not isinstance(key, tuple) or len(key) < 6 or len(observations) > REFERENCE_MINUTES * 2:
        raise ValueError("bounded observations with full comparable metric identity required")
    cutoff = as_of_ms - as_of_ms % 60_000
    start = cutoff - REFERENCE_MINUTES * 60_000
    samples = {}
    for timestamp, identity, observed, available_at_ms in observations:
        _time(timestamp)
        _time(available_at_ms)
        if available_at_ms < timestamp:
            raise ValueError("reference observation predates its measurement")
        if identity != key or not start <= timestamp < cutoff or timestamp % 60_000 or available_at_ms > as_of_ms:
            continue
        observed = _number(observed)
        if timestamp in samples and samples[timestamp] != observed:
            raise ValueError("conflicting reference observations at one minute")
        samples[timestamp] = observed
    if len(samples) * 10 < REFERENCE_MINUTES * 9:
        return None
    below = sum(sample < value for sample in samples.values())
    equal = sum(sample == value for sample in samples.values())
    return (Decimal(below) + Decimal(equal) / 2) / len(samples)
