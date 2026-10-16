"""Causal entry features from independently qualified trade and quote streams."""

import re
from dataclasses import dataclass
from decimal import Decimal, localcontext

from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.market_context.market_picture.microstructure import FlowWindow, flow_window
from hummingbot.strategy_v2.market_context.market_picture.top_of_book import QualifiedTopOfBook, TopOfBookObservation

from .observation_adapter import NativeTradeObservation
from .observation_entry import EntryObservation

_HASH = re.compile(r"[a-f0-9]{64}\Z")
_SOURCE_ID = re.compile(r"[a-z0-9][a-z0-9_.:-]{0,127}\Z")
_PROFILE = {"okx_eea_public_v1", "okx_global_public_v1"}


@dataclass(frozen=True)
class EntryFeaturePolicy:
    window_ms: int
    stabilization_window_ms: int
    max_quote_age_ms: int
    max_feature_age_ms: int
    max_spread_bps: Decimal
    minimum_buy_imbalance: Decimal
    minimum_adverse_drop_bps: Decimal
    maximum_late_decline_bps: Decimal

    def __post_init__(self):
        if (
            type(self.window_ms) is not int
            or not 0 < self.window_ms <= 300_000
            or type(self.stabilization_window_ms) is not int
            or not 0 < self.stabilization_window_ms < self.window_ms
            or type(self.max_quote_age_ms) is not int
            or not 0 < self.max_quote_age_ms <= 60_000
            or type(self.max_feature_age_ms) is not int
            or not 0 < self.max_feature_age_ms <= 60_000
            or any(
                not isinstance(value, Decimal) or not value.is_finite()
                for value in (
                    self.max_spread_bps,
                    self.minimum_buy_imbalance,
                    self.minimum_adverse_drop_bps,
                    self.maximum_late_decline_bps,
                )
            )
            or not 0 <= self.max_spread_bps <= 10_000
            or not -1 <= self.minimum_buy_imbalance <= 0
            or not 0 < self.minimum_adverse_drop_bps <= 10_000
            or not 0 <= self.maximum_late_decline_bps <= 10_000
        ):
            raise ValueError("explicit bounded joint entry policy is required")


@dataclass(frozen=True)
class DurableCursor:
    segment: int
    sequence: int
    hash: str

    def __post_init__(self):
        if (
            type(self.segment) is not int
            or self.segment < 0
            or type(self.sequence) is not int
            or self.sequence < 1
            or not isinstance(self.hash, str)
            or _HASH.fullmatch(self.hash) is None
        ):
            raise ValueError("valid durable observation cursor required")

    @classmethod
    def from_mapping(cls, value):
        if not isinstance(value, dict) or set(value) != {"segment", "sequence", "hash"}:
            raise ValueError("exact durable observation cursor required")
        return cls(**value)


@dataclass(frozen=True)
class JointCoverageReceipt:
    instrument_id: str
    source_id: str
    source_contract_hash: str
    source_profile: str
    metadata_revision: str
    selection_epoch: str
    trade_source_epoch: str
    quote_source_epoch: str
    trade_coverage_start_ms: int
    trade_coverage_end_ms: int
    quote_coverage_start_ms: int
    quote_coverage_end_ms: int
    available_at_ms: int
    receipt_sha256: str
    quality: str
    side_verified: bool
    trade_cursor: DurableCursor
    quote_cursor: DurableCursor
    trade_gaps: tuple[tuple[int, int], ...] = ()
    quote_gaps: tuple[tuple[int, int], ...] = ()

    def __post_init__(self):
        if (
            not isinstance(self.instrument_id, str)
            or not self.instrument_id.startswith("okx:spot:")
            or not isinstance(self.source_id, str)
            or _SOURCE_ID.fullmatch(self.source_id) is None
            or not isinstance(self.source_contract_hash, str)
            or _HASH.fullmatch(self.source_contract_hash) is None
            or self.source_profile not in _PROFILE
            or any(
                not isinstance(value, str) or not 1 <= len(value) <= 128
                for value in (
                    self.metadata_revision,
                    self.selection_epoch,
                    self.trade_source_epoch,
                    self.quote_source_epoch,
                )
            )
            or not isinstance(self.receipt_sha256, str)
            or _HASH.fullmatch(self.receipt_sha256) is None
            or self.quality not in {"qualified", "unqualified"}
            or type(self.side_verified) is not bool
            or not isinstance(self.trade_cursor, DurableCursor)
            or not isinstance(self.quote_cursor, DurableCursor)
        ):
            raise ValueError("joint source identity and durable receipt are required")
        for start, end in (
            (self.trade_coverage_start_ms, self.trade_coverage_end_ms),
            (self.quote_coverage_start_ms, self.quote_coverage_end_ms),
        ):
            if type(start) is not int or type(end) is not int or not 0 <= start < end:
                raise ValueError("valid independent channel coverage is required")
        if type(self.available_at_ms) is not int or self.available_at_ms < max(
            self.trade_coverage_end_ms, self.quote_coverage_end_ms
        ):
            raise ValueError("joint coverage cannot precede either channel")
        for gaps in (self.trade_gaps, self.quote_gaps):
            if not isinstance(gaps, tuple) or len(gaps) > 10_000:
                raise ValueError("bounded source gaps required")
            previous = -1
            for gap in gaps:
                if (
                    not isinstance(gap, tuple)
                    or len(gap) != 2
                    or any(type(value) is not int for value in gap)
                    or not 0 <= gap[0] < gap[1]
                    or gap[0] < previous
                ):
                    raise ValueError("source gaps must be ordered half-open intervals")
                previous = gap[1]


@dataclass(frozen=True)
class BuiltEntryObservation:
    observation: EntryObservation
    flow: FlowWindow
    top: QualifiedTopOfBook
    coverage_receipt_sha256: str
    selection_epoch: str
    policy: EntryFeaturePolicy
    trades: tuple[NativeTradeObservation, ...]
    quotes: tuple[TopOfBookObservation, TopOfBookObservation, TopOfBookObservation]
    coverage: JointCoverageReceipt
    as_of_ms: int
    total_response_bps: Decimal
    late_response_bps: Decimal


def _response(start: Decimal, end: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 256
        return Decimal(10_000) * (end / start - 1)


def build_entry_observation(
    *,
    opportunity_id: str,
    as_of_ms: int,
    trades: tuple[NativeTradeObservation, ...],
    quotes: tuple[TopOfBookObservation, TopOfBookObservation, TopOfBookObservation],
    coverage: JointCoverageReceipt,
    policy: EntryFeaturePolicy,
) -> BuiltEntryObservation:
    if (
        not isinstance(coverage, JointCoverageReceipt)
        or not isinstance(policy, EntryFeaturePolicy)
        or type(as_of_ms) is not int
        or as_of_ms < policy.window_ms
        or not isinstance(trades, tuple)
        or len(trades) > 100_000
        or not isinstance(quotes, tuple)
        or len(quotes) != 3
        or any(not isinstance(value, TopOfBookObservation) for value in quotes)
    ):
        raise ValueError("bounded typed joint entry inputs are required")
    if coverage.quality != "qualified" or coverage.available_at_ms > as_of_ms:
        raise ValueError("joint source coverage is unqualified or future")
    start_ms = as_of_ms - policy.window_ms
    split_ms = as_of_ms - policy.stabilization_window_ms
    if (
        coverage.trade_coverage_start_ms > start_ms
        or coverage.trade_coverage_end_ms < as_of_ms
        or coverage.quote_coverage_start_ms > start_ms
        or coverage.quote_coverage_end_ms < as_of_ms
    ):
        raise ValueError("joint source coverage does not contain the decision window")
    if any(
        left < as_of_ms and right > start_ms
        for gaps in (coverage.trade_gaps, coverage.quote_gaps)
        for left, right in gaps
    ):
        raise ValueError("joint source gap intersects the decision window")
    quote_identity = (
        coverage.instrument_id,
        coverage.source_profile,
        coverage.quote_source_epoch,
        coverage.metadata_revision,
        coverage.selection_epoch,
    )
    for quote in quotes:
        if (
            quote.instrument_id,
            quote.source_profile,
            quote.source_epoch,
            quote.metadata_revision,
            quote.selection_epoch,
        ) != quote_identity:
            raise ValueError("quote and joint coverage lineage differ")
    for item in trades:
        if (
            not isinstance(item, NativeTradeObservation)
            or not item.qualified
            or (item.instrument_id, item.source_profile, item.source_epoch, item.metadata_revision)
            != (
                coverage.instrument_id,
                coverage.source_profile,
                coverage.trade_source_epoch,
                coverage.metadata_revision,
            )
            or item.available_at_ms > as_of_ms
        ):
            raise ValueError("trade is unqualified or has different channel lineage")
    anchors = (start_ms, split_ms, as_of_ms)
    tops = tuple(quote.as_of(as_of_ms=at, max_age_ms=policy.max_quote_age_ms) for quote, at in zip(quotes, anchors))
    if any(not top.qualified or top.midpoint is None for top in tops):
        raise ValueError("missing qualified causal quote anchor")
    if tops[-1].spread_bps > policy.max_spread_bps:
        raise ValueError("entry spread exceeds configured cap")
    with localcontext() as context:
        context.prec = 256
        flow = flow_window(
            tuple(item.trade for item in trades),
            instrument_id=coverage.instrument_id,
            channel="trades-all",
            start_ms=start_ms,
            end_ms=as_of_ms,
            as_of_ms=as_of_ms,
            coverage_start_ms=coverage.trade_coverage_start_ms,
            coverage_end_ms=coverage.trade_coverage_end_ms,
            reset_id=coverage.trade_source_epoch,
            side_verified=coverage.side_verified,
            gaps=coverage.trade_gaps,
        )
        total_response = _response(tops[0].midpoint, tops[-1].midpoint)
        late_response = _response(tops[1].midpoint, tops[-1].midpoint)
    expires = min(
        quotes[-1].event_at_ms + policy.max_quote_age_ms,
        coverage.trade_coverage_end_ms + policy.max_feature_age_ms,
        coverage.quote_coverage_end_ms + policy.max_feature_age_ms,
    )
    available = max(
        [
            coverage.available_at_ms,
            *(quote.available_at_ms for quote in quotes),
            *(item.available_at_ms for item in trades),
        ]
    )
    if expires <= available:
        raise ValueError("entry features expire before they are available")
    qualified = flow.confirmation_imbalance is not None
    adverse = bool(
        qualified
        and flow.confirmation_imbalance < policy.minimum_buy_imbalance
        and total_response <= -policy.minimum_adverse_drop_bps
        and late_response < -policy.maximum_late_decline_bps
    )
    content = digest(
        {
            "opportunity": opportunity_id,
            "as_of_ms": as_of_ms,
            "trade_hashes": [item.raw_sha256 for item in trades],
            "quotes": [
                [q.instrument_id, q.source_epoch, q.sequence, str(q.bid_price), str(q.ask_price), q.available_at_ms]
                for q in quotes
            ],
            "coverage": coverage.receipt_sha256,
            "source": [
                coverage.source_id,
                coverage.source_contract_hash,
                coverage.trade_source_epoch,
                coverage.quote_source_epoch,
                coverage.selection_epoch,
            ],
            "policy": [
                policy.window_ms,
                policy.stabilization_window_ms,
                policy.max_quote_age_ms,
                policy.max_feature_age_ms,
                str(policy.max_spread_bps),
                str(policy.minimum_buy_imbalance),
                str(policy.minimum_adverse_drop_bps),
                str(policy.maximum_late_decline_bps),
            ],
            "flow": [str(flow.buy_quote), str(flow.sell_quote), str(flow.unknown_quote), flow.reasons],
            "response": [str(total_response), str(late_response)],
        }
    )
    observation = EntryObservation(
        opportunity_id=opportunity_id,
        content_sha256=content,
        instrument_id=coverage.instrument_id,
        source_profile=coverage.source_profile,
        source_epoch=coverage.selection_epoch,
        available_at_ms=available,
        expires_at_ms=expires,
        qualified=bool(qualified),
        adverse_flow=adverse,
    )
    return BuiltEntryObservation(
        observation,
        flow,
        tops[-1],
        coverage.receipt_sha256,
        coverage.selection_epoch,
        policy,
        trades,
        quotes,
        coverage,
        as_of_ms,
        total_response,
        late_response,
    )
