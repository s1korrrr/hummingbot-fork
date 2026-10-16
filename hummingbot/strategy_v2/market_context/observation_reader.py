"""Bounded, socket-free consumer for durable Rust observation updates.

The caller owns transport and independent source/coverage qualification. This
reader never turns a health flag, subscription ACK, or producer quality label
into a source permit. A transport reset invalidates every buffered window.
"""

import re
from collections import deque
from dataclasses import dataclass
from typing import Callable, Mapping

from .market_picture.top_of_book import TopOfBookObservation
from .observation_adapter import NativeTradeObservation, parse_rust_trade
from .observation_entry import EntryObservation
from .observation_entry_builder import (
    BuiltEntryObservation,
    DurableCursor,
    EntryFeaturePolicy,
    JointCoverageReceipt,
    build_entry_observation,
)

_HASH = re.compile(r"[a-f0-9]{64}\Z")
_ID = re.compile(r"[a-z0-9][a-z0-9_.:-]{0,127}\Z")
_PROFILE = {"okx_eea_public_v1", "okx_global_public_v1"}
_CHANNELS = ("trades-all", "bbo-tbt")


@dataclass(frozen=True)
class SourceQualification:
    """An external owner's source review, verified by an injected trust boundary."""

    instrument_id: str
    source_id: str
    source_contract_hash: str
    source_profile: str
    metadata_revision: str
    metadata_observed_sha256: str
    selection_epoch: str
    qualified_at_ms: int
    review_available_at_ms: int
    expires_at_ms: int
    review_receipt_sha256: str

    def __post_init__(self):
        if (
            not isinstance(self.instrument_id, str)
            or not self.instrument_id.startswith("okx:spot:")
            or not isinstance(self.source_id, str)
            or _ID.fullmatch(self.source_id) is None
            or not isinstance(self.source_contract_hash, str)
            or _HASH.fullmatch(self.source_contract_hash) is None
            or self.source_profile not in _PROFILE
            or not isinstance(self.metadata_revision, str)
            or not self.metadata_revision
            or not isinstance(self.metadata_observed_sha256, str)
            or _HASH.fullmatch(self.metadata_observed_sha256) is None
            or not isinstance(self.selection_epoch, str)
            or not self.selection_epoch
            or any(
                type(value) is not int
                for value in (self.qualified_at_ms, self.review_available_at_ms, self.expires_at_ms)
            )
            or not 0 <= self.qualified_at_ms <= self.review_available_at_ms < self.expires_at_ms
            or not isinstance(self.review_receipt_sha256, str)
            or _HASH.fullmatch(self.review_receipt_sha256) is None
        ):
            raise ValueError("independent bounded source qualification is required")


@dataclass
class _Channel:
    epoch: str | None = None
    acknowledged: bool = False
    acknowledged_at_ms: int | None = None
    last_cursor: DurableCursor | None = None
    last_available_at_ms: int | None = None
    evicted_at_ms: int | None = None


class ObservationReader:
    """Consumes one pair's pair-scoped stream; no acquisition or order effects."""

    def __init__(
        self,
        *,
        instrument_id: str,
        source_id: str,
        source_contract_hash: str,
        source_profile: str,
        max_trades: int,
        max_quotes: int,
    ):
        if (
            not isinstance(instrument_id, str)
            or not instrument_id.startswith("okx:spot:")
            or not isinstance(source_id, str)
            or _ID.fullmatch(source_id) is None
            or not isinstance(source_contract_hash, str)
            or _HASH.fullmatch(source_contract_hash) is None
            or source_profile not in _PROFILE
            or type(max_trades) is not int
            or not 1 <= max_trades <= 100_000
            or type(max_quotes) is not int
            or not 3 <= max_quotes <= 100_000
        ):
            raise ValueError("explicit bounded observation reader identity and capacity required")
        self.instrument_id = instrument_id
        self.source_id = source_id
        self.source_contract_hash = source_contract_hash
        self.source_profile = source_profile
        self.max_trades = max_trades
        self.max_quotes = max_quotes
        self.qualification: SourceQualification | None = None
        self.coverage: JointCoverageReceipt | None = None
        self.channels = {name: _Channel() for name in _CHANNELS}
        self.trades: deque[NativeTradeObservation] = deque()
        self.quotes: deque[TopOfBookObservation] = deque()
        self._trade_visible_at_ms: deque[int] = deque()
        self._quote_visible_at_ms: deque[int] = deque()
        self.coverage_received_at_ms: int | None = None
        self.last_update_sequence = 0
        self.last_received_at_ms: int | None = None
        self.reason = "source_unqualified"

    def attach_verified_source(
        self, qualification: SourceQualification, *, verify: Callable[[SourceQualification], bool]
    ) -> None:
        if not isinstance(qualification, SourceQualification) or not callable(verify):
            raise ValueError("independent source verifier is required")
        if verify(qualification) is not True:
            raise ValueError("source qualification was not independently verified")
        if (
            qualification.instrument_id,
            qualification.source_id,
            qualification.source_contract_hash,
            qualification.source_profile,
        ) != (self.instrument_id, self.source_id, self.source_contract_hash, self.source_profile):
            raise ValueError("source qualification differs from reader contract")
        self.invalidate("source_qualification_changed")
        self.qualification = qualification
        self.reason = "awaiting_durable_source_acks"

    def invalidate(self, reason: str, channel: str | None = None) -> None:
        if channel is not None and channel not in self.channels:
            raise ValueError("unsupported source channel")
        names = self.channels if channel is None else (channel,)
        for name in names:
            self.channels[name] = _Channel()
        if channel is None or channel == "trades-all":
            self.trades.clear()
            self._trade_visible_at_ms.clear()
        if channel is None or channel == "bbo-tbt":
            self.quotes.clear()
            self._quote_visible_at_ms.clear()
        self.coverage = None
        self.coverage_received_at_ms = None
        self.reason = reason[:128]

    @staticmethod
    def _cursor_after(left: DurableCursor, right: DurableCursor) -> bool:
        return (left.segment, left.sequence) > (right.segment, right.sequence)

    def _scope(self, message: Mapping) -> str:
        channel = message.get("channel")
        if channel not in self.channels:
            raise ValueError("unexpected observation channel")
        instrument = message.get("instrument_id")
        payload = message.get("payload")
        if instrument is None and isinstance(payload, Mapping):
            instrument = payload.get("instrument_id")
            if instrument is None and payload.get("inst_id") == self.instrument_id.split(":", 2)[2]:
                instrument = self.instrument_id
        if instrument != self.instrument_id:
            raise ValueError("observation escaped pair scope")
        return channel

    def ingest(self, message: Mapping, *, received_at_ms: int | None = None) -> None:
        """Accept a pair-scoped transport message; malformed input invalidates prior evidence."""
        try:
            self._ingest(message, received_at_ms=received_at_ms)
        except (TypeError, ValueError):
            self.invalidate("invalid_observation_update")
            self.qualification = None
            raise

    def _ingest(self, message: Mapping, *, received_at_ms: int | None) -> None:
        if not isinstance(message, Mapping):
            raise ValueError("observation update mapping required")
        kind = message.get("type")
        if kind == "reset":
            self.invalidate("transport_reset")
            self.qualification = None
            return
        if kind == "source_status":
            if message.get("instrument_id") != self.instrument_id:
                raise ValueError("source status escaped pair scope")
            self.invalidate(str(message.get("reason") or "source_status"), message.get("channel"))
            self.qualification = None
            return
        if kind == "snapshot":
            if self.last_update_sequence > 0 or self.coverage is not None:
                self.invalidate("new_transport_snapshot")
                self.qualification = None
            return  # A health snapshot contains no event replay.
        if kind == "heartbeat":
            return  # Heartbeats never grant source or coverage qualification.
        if message.get("schema_version") != "okx.observations.update.v1":
            raise ValueError("unsupported Rust observation update schema")
        sequence = message.get("sequence")
        if type(sequence) is not int or sequence <= self.last_update_sequence:
            self.invalidate("out_of_order_update")
            raise ValueError("pair-scoped update order regressed")
        self.last_update_sequence = sequence  # Holes are normal after pair filtering.
        record_kind = message.get("record_kind")
        # Receipt time comes from the owner clock, never from the producer wire.
        # Every durable input that can grant coverage needs both a publication
        # witness and the time at which this consumer actually saw it.
        timing_kinds = {"metadata_available", "exchange_metadata_observed", "subscription_ack", "trade", "bbo", "timer"}
        visible_at_ms = None
        if record_kind in timing_kinds and message.get("durable_cursor") is not None:
            source_at = message.get("available_at_ms")
            accepted = message.get("accepted_at_ms")
            durable = message.get("durable_acknowledged_at_ms")
            published = message.get("published_at_ms")
            if (
                any(type(value) is not int for value in (source_at, accepted, durable, published, received_at_ms))
                or not 0 <= source_at <= accepted <= durable <= published <= received_at_ms
            ):
                raise ValueError("causal durable publication and consumer receipt required")
            computed = message.get("computed_at_ms")
            if computed is not None and (type(computed) is not int or not 0 <= computed <= published):
                raise ValueError("causal computation time required")
            if self.last_received_at_ms is not None and received_at_ms < self.last_received_at_ms:
                raise ValueError("consumer receipt clock regressed")
            self.last_received_at_ms = received_at_ms
            visible_at_ms = received_at_ms
        if record_kind in {"metadata_available", "exchange_metadata_observed"}:
            if message.get("instrument_id") != self.instrument_id:
                raise ValueError("metadata escaped pair scope")
            payload = message.get("payload")
            permit = self.qualification
            if not isinstance(payload, Mapping) or permit is None:
                self.invalidate("metadata_unqualified")
                return
            revision = payload.get("metadata_revision")
            observed = payload.get("observed_revision_sha256")
            if (
                revision != permit.metadata_revision
                or record_kind == "exchange_metadata_observed"
                and observed != permit.metadata_observed_sha256
            ):
                self.invalidate("metadata_revision_changed")
                self.qualification = None
            return  # Metadata does not create an ACK, window, or source permit.
        channel = self._scope(message)
        epoch = message.get("epoch")
        if not isinstance(epoch, str) or not 1 <= len(epoch) <= 128:
            raise ValueError("bounded source epoch required")
        if record_kind in {"disconnect", "gap", "queue_rejected", "source_switch"}:
            self.invalidate(record_kind, channel)
            self.qualification = None
            return
        # Derived bars and raw/disposition rows are valid producer traffic, but
        # only source ACKs, individual observations and timer markers advance
        # live input coverage. In particular, derived bars may have no clock.
        if record_kind not in {"subscription_ack", "trade", "bbo", "timer"}:
            return
        cursor_raw = message.get("durable_cursor")
        if cursor_raw is None:
            return
        cursor = DurableCursor.from_mapping(cursor_raw)
        state = self.channels[channel]
        if state.last_cursor is not None and not self._cursor_after(cursor, state.last_cursor):
            self.invalidate("durable_cursor_regressed", channel)
            raise ValueError("durable journal cursor regressed")
        if state.epoch != epoch:
            prior_epoch = state.epoch
            self.invalidate("source_epoch_changed", channel)
            if prior_epoch is not None:
                self.qualification = None
            state = self.channels[channel]
            state.epoch = epoch
        state.last_cursor = cursor
        stamp = message.get("available_at_ms")
        if type(stamp) is not int or stamp < 0:
            raise ValueError("durable observation availability required")
        state.last_available_at_ms = max(state.last_available_at_ms or 0, stamp)
        payload = message.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("durable observation payload required")
        if record_kind == "subscription_ack":
            if payload.get("inst_id") != self.instrument_id.split(":", 2)[2]:
                raise ValueError("subscription ACK pair mismatch")
            if state.acknowledged:
                self.invalidate("repeated_subscription_ack", channel)
                self.qualification = None
                return
            state.acknowledged = True
            state.acknowledged_at_ms = stamp
            return
        if record_kind == "timer":
            return
        if record_kind not in {"trade", "bbo"}:
            return
        if not state.acknowledged or self.qualification is None:
            self.reason = "unqualified_or_unacknowledged_source"
            return
        observation = payload.get("observation")
        if not isinstance(observation, Mapping):
            raise ValueError("normalized durable observation required")
        if record_kind == "trade" and channel == "trades-all":
            trade = parse_rust_trade(observation, source_profile=self.source_profile)
            if (
                not trade.qualified
                or trade.source_epoch != epoch
                or trade.metadata_revision != self.qualification.metadata_revision
            ):
                self.invalidate("unqualified_trade", channel)
                self.qualification = None
                return
            if len(self.trades) == self.max_trades:
                evicted = self.trades.popleft()
                self._trade_visible_at_ms.popleft()
                state.evicted_at_ms = max(state.evicted_at_ms or 0, evicted.event_at_ms)
            self.trades.append(trade)
            self._trade_visible_at_ms.append(max(visible_at_ms, trade.available_at_ms))
        elif record_kind == "bbo" and channel == "bbo-tbt":
            quote = TopOfBookObservation.from_rust_record(
                observation, source_profile=self.source_profile, selection_epoch=self.qualification.selection_epoch
            )
            if (
                quote.quality != "QUALIFIED"
                or quote.source_epoch != epoch
                or quote.metadata_revision != self.qualification.metadata_revision
            ):
                self.invalidate("unqualified_quote", channel)
                self.qualification = None
                return
            if self.quotes:
                previous = self.quotes[-1]
                if (
                    quote.event_at_ms < previous.event_at_ms
                    or quote.sequence is not None
                    and previous.sequence is not None
                    and quote.sequence < previous.sequence
                ):
                    raise ValueError("BBO snapshot order regressed within the source epoch")
                if (
                    quote.sequence is not None
                    and quote.sequence == previous.sequence
                    and (quote.bid_price, quote.bid_quantity, quote.ask_price, quote.ask_quantity)
                    != (previous.bid_price, previous.bid_quantity, previous.ask_price, previous.ask_quantity)
                ):
                    raise ValueError("conflicting BBO snapshot sequence")
            if len(self.quotes) == self.max_quotes:
                evicted = self.quotes.popleft()
                self._quote_visible_at_ms.popleft()
                state.evicted_at_ms = max(state.evicted_at_ms or 0, evicted.available_at_ms)
            self.quotes.append(quote)
            self._quote_visible_at_ms.append(max(visible_at_ms, quote.available_at_ms))
        else:
            raise ValueError("record kind disagrees with channel")
        self.reason = "awaiting_independent_coverage"

    def attach_verified_coverage(
        self, coverage: JointCoverageReceipt, *, verify: Callable[[JointCoverageReceipt], bool], received_at_ms: int
    ) -> None:
        if not isinstance(coverage, JointCoverageReceipt) or not callable(verify):
            raise ValueError("independent coverage verifier is required")
        if verify(coverage) is not True:
            raise ValueError("joint coverage was not independently verified")
        if type(received_at_ms) is not int or received_at_ms < coverage.available_at_ms:
            raise ValueError("joint coverage consumer receipt must follow availability")
        permit = self.qualification
        if permit is None or (
            coverage.instrument_id,
            coverage.source_id,
            coverage.source_contract_hash,
            coverage.source_profile,
            coverage.metadata_revision,
            coverage.selection_epoch,
        ) != (
            permit.instrument_id,
            permit.source_id,
            permit.source_contract_hash,
            permit.source_profile,
            permit.metadata_revision,
            permit.selection_epoch,
        ):
            raise ValueError("joint coverage differs from independently qualified source")
        for channel, epoch, cursor, start, end in (
            (
                "trades-all",
                coverage.trade_source_epoch,
                coverage.trade_cursor,
                coverage.trade_coverage_start_ms,
                coverage.trade_coverage_end_ms,
            ),
            (
                "bbo-tbt",
                coverage.quote_source_epoch,
                coverage.quote_cursor,
                coverage.quote_coverage_start_ms,
                coverage.quote_coverage_end_ms,
            ),
        ):
            state = self.channels[channel]
            if (
                not state.acknowledged
                or state.epoch != epoch
                or state.last_cursor is None
                or state.acknowledged_at_ms is None
                or state.acknowledged_at_ms > start
                or self._cursor_after(cursor, state.last_cursor)
                or state.last_available_at_ms is None
                or state.last_available_at_ms < end
                or state.evicted_at_ms is not None
                and state.evicted_at_ms >= end
            ):
                raise ValueError("verified coverage exceeds observed durable channel boundary")
        if coverage.quality != "qualified" or not coverage.side_verified:
            raise ValueError("independent joint coverage is unqualified")
        self.coverage = coverage
        self.coverage_received_at_ms = received_at_ms
        self.reason = "qualified_window_available"

    def check_active(self, observation: EntryObservation | None, *, now_ms: int) -> None:
        permit, coverage = self.qualification, self.coverage
        if (
            observation is None
            or not isinstance(observation, EntryObservation)
            or permit is None
            or coverage is None
            or type(now_ms) is not int
            or not permit.review_available_at_ms <= now_ms < permit.expires_at_ms
            or self.coverage_received_at_ms is None
            or self.coverage_received_at_ms > now_ms
            or observation.instrument_id != self.instrument_id
            or observation.source_epoch != permit.selection_epoch
            or coverage.selection_epoch != permit.selection_epoch
            or coverage.receipt_sha256 is None
        ):
            raise ValueError("source qualification or joint coverage is unavailable")
        for channel, epoch, cursor in (
            ("trades-all", coverage.trade_source_epoch, coverage.trade_cursor),
            ("bbo-tbt", coverage.quote_source_epoch, coverage.quote_cursor),
        ):
            state = self.channels[channel]
            if (
                not state.acknowledged
                or state.epoch != epoch
                or state.last_cursor is None
                or self._cursor_after(cursor, state.last_cursor)
            ):
                raise ValueError("source channel changed after admission")

    def build(self, *, opportunity_id: str, as_of_ms: int, policy: EntryFeaturePolicy) -> BuiltEntryObservation:
        permit, coverage = self.qualification, self.coverage
        if (
            permit is None
            or coverage is None
            or type(as_of_ms) is not int
            or not permit.review_available_at_ms <= as_of_ms < permit.expires_at_ms
            or self.coverage_received_at_ms is None
            or self.coverage_received_at_ms > as_of_ms
        ):
            raise ValueError("independent source and joint coverage unavailable")
        start = as_of_ms - policy.window_ms
        trade_state, quote_state = self.channels["trades-all"], self.channels["bbo-tbt"]
        if (
            trade_state.evicted_at_ms is not None
            and trade_state.evicted_at_ms >= start
            or quote_state.evicted_at_ms is not None
            and quote_state.evicted_at_ms >= start - policy.max_quote_age_ms
        ):
            raise ValueError("required observation history exceeded reader capacity")
        if trade_state.epoch != coverage.trade_source_epoch or quote_state.epoch != coverage.quote_source_epoch:
            raise ValueError("source epoch changed after coverage verification")
        anchors = (start, as_of_ms - policy.stabilization_window_ms, as_of_ms)
        selected = []
        for at in anchors:
            candidates = [
                quote
                for quote, visible in zip(self.quotes, self._quote_visible_at_ms)
                if quote.event_at_ms <= at and visible <= at
            ]
            if not candidates:
                raise ValueError("no causally available quote for required anchor")
            selected.append(
                max(
                    candidates,
                    key=lambda q: (q.available_at_ms, q.event_at_ms, q.sequence if q.sequence is not None else -1),
                )
            )
        trades = tuple(
            trade
            for trade, visible in zip(self.trades, self._trade_visible_at_ms)
            if start <= trade.event_at_ms < as_of_ms and visible <= as_of_ms
        )
        return build_entry_observation(
            opportunity_id=opportunity_id,
            as_of_ms=as_of_ms,
            trades=trades,
            quotes=tuple(selected),
            coverage=coverage,
            policy=policy,
        )
