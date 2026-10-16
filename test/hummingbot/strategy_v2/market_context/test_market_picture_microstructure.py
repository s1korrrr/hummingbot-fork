from dataclasses import replace
from decimal import Decimal as D

import pytest

from hummingbot.strategy_v2.market_context.market_picture.microstructure import (
    BookStream,
    CumulativeDelta,
    flow_window,
    normalize_spot_trade,
    reference_percentile,
    visible_book_cost,
    visible_depth,
)


def trade(identity="1", side="buy", quantity="0.6", timestamp=10, channel="trades", count="1"):
    return normalize_spot_trade(
        {"instId": "BTC-USDC", "tradeId": identity, "px": "100", "sz": quantity,
         "side": side, "ts": str(timestamp), "count": count},
        instrument_id="okx:spot:BTC-USDC", channel=channel, available_at_ms=timestamp + 1,
    )


def window(rows, start=0, end=100, **kwargs):
    kwargs.setdefault("side_verified", True)
    return flow_window(rows, instrument_id="okx:spot:BTC-USDC", channel="trades",
                       start_ms=start, end_ms=end, as_of_ms=end + 10,
                       coverage_start_ms=start, coverage_end_ms=end, reset_id="epoch-1",
                       **kwargs)


def snapshot(channel="books", asks=None, bids=None):
    stream = BookStream("okx:spot:BTC-USDC", channel=channel)
    stream.apply("snapshot", {
        "prevSeqId": -1, "seqId": 10, "ts": "10", "checksum": 0,
        "asks": asks or [["100.1", "1", "0", "1"], ["100.2", "1", "0", "1"]],
        "bids": bids or [["99.9", "1", "0", "1"], ["99.8", "1", "0", "1"]],
    }, available_at_ms=11)
    return stream


def cost(stream, size, **kwargs):
    return visible_book_cost(stream.view, side="buy", requested_size=D(size),
                             size_unit="base", as_of_ms=11, **kwargs)


def test_aggregate_trade_size_is_not_multiplied_and_duplicates_do_not_inflate_flow():
    buys = trade("103", count="3")
    sells = trade("104", "sell", ".4")
    result = window([buys, sells, buys])
    assert result.buy_quote == D(60)
    assert result.sell_quote == D(40)
    assert result.known_imbalance == D(".2")
    assert result.confirmation_imbalance == D(".2")
    assert result.trade_records == 2
    assert result.duplicate_records == 1


def test_unknown_sides_and_gaps_keep_known_ratio_but_withhold_confirmation():
    result = window([trade(), trade("2", "sell", ".4"), trade("3", "", ".1")])
    assert result.unknown_quote == 10
    assert result.known_imbalance == D(".2")
    assert result.unknown_share == D(1) / 11
    assert result.confirmation_imbalance is None
    assert "UNKNOWN_AGGRESSOR" in result.reasons
    gap = window([trade()], gaps=((20, 30),))
    assert not gap.complete and gap.confirmation_imbalance is None


def test_flow_does_not_mix_channels_units_or_conflicting_duplicate_payloads():
    with pytest.raises(ValueError, match="scope"):
        window([trade(), trade(channel="trades-all")])
    with pytest.raises(ValueError, match="conflicting"):
        window([trade(), trade(quantity=".7")])
    with pytest.raises(ValueError, match="overlap"):
        window([trade("103", count="3"), trade("102", count="1")])
    with pytest.raises(ValueError):
        normalize_spot_trade({"instId": "BTC-USDC-SWAP"}, instrument_id="okx:swap:BTC-USDC-SWAP",
                             channel="trades", available_at_ms=11)


def test_decision_cutoff_and_half_open_flow_window():
    result = window([trade("1", timestamp=99), trade("2", timestamp=100)])
    assert result.trade_records == 1
    late = replace(trade(), available_at_ms=200)
    result = window([late])
    assert result.trade_records == 0 and not result.complete
    assert result.confirmation_imbalance is None
    assert window([]).known_imbalance is None


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1", "0", True, ""])
def test_invalid_trade_numeric_values_are_rejected(value):
    with pytest.raises(ValueError):
        trade(quantity=value)


def test_cumulative_delta_never_bridges_gap_or_implicit_reset():
    cumulative = CumulativeDelta()
    assert cumulative.append(window([trade()])) == 60
    assert cumulative.append(window([trade()])) == 60
    assert cumulative.append(window([trade("2", "sell", ".4", timestamp=101)], 100, 200)) == 20
    assert cumulative.append(window([], 200, 300, gaps=((220, 230),))) is None
    assert cumulative.append(window([trade("3", timestamp=310)], 300, 400)) is None
    next_epoch = replace(window([trade("4", timestamp=410)], 400, 500), reset_id="epoch-2")
    assert cumulative.append(next_epoch) == 60


def test_requested_size_book_walk_and_incomplete_depth_are_distinct():
    stream = snapshot()
    full = cost(stream, "1.5")
    assert full.covered_base == D("1.5") and full.uncovered_size == 0
    assert full.vwap == D("150.2") / D("1.5")
    assert float(full.cost_bps) == pytest.approx(13.3333333333)
    assert full.total_cost_bps is None and "FEE_UNAVAILABLE" in full.reasons
    partial = cost(stream, "3")
    assert partial.covered_base == 2 and partial.uncovered_size == 1
    assert partial.vwap is None and partial.cost_bps is None
    assert partial.covered_vwap == D("100.15")
    assert "BOOK_DEPTH_INSUFFICIENT" in partial.reasons


def test_quote_budget_consumes_actual_notional_and_fee_provenance_is_required():
    stream = snapshot()
    result = visible_book_cost(stream.view, side="buy", requested_size=D("150.2"),
                               size_unit="quote", as_of_ms=11,
                               fee_bps=D(10), fee_source="scenario:10bps")
    assert result.covered_base == D("1.5")
    assert result.covered_quote == D("150.2")
    assert result.uncovered_size == 0
    assert result.total_cost_bps == result.cost_bps + D(10) * result.vwap / D(100)
    with pytest.raises(ValueError, match="fee"):
        cost(stream, "1", fee_bps=D(0))


def test_sell_walk_uses_bids_and_stale_book_never_yields_current_cost():
    stream = snapshot()
    result = visible_book_cost(stream.view, side="sell", requested_size=D("1.5"),
                               size_unit="base", as_of_ms=11)
    assert result.vwap == D("149.8") / D("1.5")
    assert float(result.cost_bps) == pytest.approx(13.3333333333)
    stale = visible_book_cost(stream.view, side="sell", requested_size=D("1.5"),
                              size_unit="base", as_of_ms=10_000)
    assert stale.cost_bps is None and "STALE" in stale.reasons


def test_book_gap_requires_snapshot_not_zero_checksum_or_next_delta():
    stream = snapshot()
    stream.apply("update", {"prevSeqId": 12, "seqId": 13, "ts": "12", "asks": [], "bids": [],
                            "checksum": 0}, available_at_ms=13)
    assert stream.view is None and stream.reason == "SEQUENCE_GAP"
    stream.apply("update", {"prevSeqId": 13, "seqId": 14, "ts": "13", "asks": [], "bids": []},
                 available_at_ms=14)
    assert stream.view is None


def test_book_duplicate_keepalive_and_documented_sequence_reset():
    stream = snapshot()
    update = {"prevSeqId": 10, "seqId": 15, "ts": "12", "asks": [["100.1", "2", "0", "1"]], "bids": []}
    stream.apply("update", update, available_at_ms=13)
    stream.apply("update", update, available_at_ms=14)
    assert cost(stream, "2").covered_base == 0  # as_of=11 cannot see the future update
    assert stream.view.asks[0].quantity == 2
    stream.apply("update", {"prevSeqId": 15, "seqId": 15, "ts": "14", "asks": [], "bids": []},
                 available_at_ms=15)
    stream.apply("update", {"prevSeqId": 15, "seqId": 3, "ts": "15", "asks": [], "bids": []},
                 available_at_ms=16)
    assert stream.view.sequence == 3
    stream.apply("update", {"prevSeqId": 3, "seqId": 5, "ts": "16", "asks": [], "bids": []},
                 available_at_ms=17)
    assert stream.view.sequence == 5


def test_crossed_or_malformed_update_invalidates_depth_atomically():
    stream = snapshot()
    with pytest.raises(ValueError):
        stream.apply("update", {"prevSeqId": 10, "seqId": 11, "ts": "12",
                                "asks": [["90", "1", "0", "1"]], "bids": []}, available_at_ms=13)
    assert stream.view is None


def test_consolidated_book_organic_subset_is_not_added_to_total():
    stream = snapshot(channel="books-rpi", asks=[["100.1", "12", "7", "2"], ["100.3", "1", "0", "1"]],
                      bids=[["99.9", "12", "7", "2"], ["99.7", "1", "0", "1"]])
    result = cost(stream, "10", access_basis="non_rpi")
    assert result.covered_base == 7 and result.uncovered_size == 3
    assert result.vwap is None
    unknown = cost(stream, "1", access_basis="unknown")
    assert unknown.cost_bps is None and "ACCESS_BASIS_UNKNOWN" in unknown.reasons
    total = cost(stream, "10", access_basis="rpi_authorized")
    assert total.covered_base == 10


def test_truncated_depth_does_not_claim_full_band():
    stream = snapshot()
    result = visible_depth(stream.view, band_bps=D(25), as_of_ms=11)
    assert result.bid_covered_quote == D("199.7")
    assert result.ask_covered_quote == D("200.3")
    assert not result.complete and result.imbalance is None
    narrow = visible_depth(stream.view, band_bps=D(10), as_of_ms=11)
    assert narrow.complete and narrow.imbalance == D("-.001")


def test_resnapshot_can_recover_same_payload_after_invalid_update():
    stream = snapshot()
    with pytest.raises(ValueError):
        stream.apply("update", {"prevSeqId": 10, "seqId": 11, "ts": "12", "bids": [], "asks": "malformed"},
                     available_at_ms=13)
    stream.apply("snapshot", {
        "prevSeqId": -1, "seqId": 10, "ts": "10", "checksum": 0,
        "asks": [["100.1", "1", "0", "1"], ["100.2", "1", "0", "1"]],
        "bids": [["99.9", "1", "0", "1"], ["99.8", "1", "0", "1"]],
    }, available_at_ms=14)
    assert stream.view is not None and stream.view.available_at_ms == 14


def test_percentile_rejects_different_size_access_and_incomplete_history():
    key = ("okx:spot:BTC-USDC", "cost.v1", "buy", "base", "1.5", "non_rpi")
    points = [(minute * 60_000, key, D(minute % 10), minute * 60_000 + 1000) for minute in range(7 * 24 * 60)]
    cutoff = 7 * 24 * 60 * 60_000
    result = reference_percentile(D(5), points, key=key, as_of_ms=cutoff)
    assert D(0) < result < D(1)
    assert reference_percentile(D(5), points[:100], key=key, as_of_ms=cutoff) is None
    assert reference_percentile(D(5), points, key=(*key[:-1], "rpi_authorized"), as_of_ms=cutoff) is None
    # Current value cannot enter its own reference, and duplicates cannot fill gaps.
    assert reference_percentile(D(5), [points[0]] * 10_080, key=key, as_of_ms=cutoff) is None
    future_receipts = [(time, identity, value, cutoff + 1) for time, identity, value, _ in points]
    assert reference_percentile(D(5), future_receipts, key=key, as_of_ms=cutoff) is None
