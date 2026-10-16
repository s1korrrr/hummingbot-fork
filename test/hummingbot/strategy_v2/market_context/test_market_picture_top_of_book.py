from decimal import Decimal as D, localcontext

import pytest

from hummingbot.strategy_v2.market_context.market_picture.top_of_book import TopOfBookObservation


def observation(**changes):
    fields = dict(
        instrument_id="okx:spot:BTC-USDC",
        region="eea",
        environment="public",
        source_profile="okx_eea_public_v1",
        metadata_revision="meta-1",
        source_epoch="epoch-1",
        selection_epoch="selection-1",
        event_at_ms=100,
        available_at_ms=110,
        sequence=7,
        bid_price=D("99.9"),
        bid_quantity=D("2"),
        ask_price=D("100.1"),
        ask_quantity=D("3"),
        quality="QUALIFIED",
    )
    fields.update(changes)
    return TopOfBookObservation(**fields)


def test_qualified_bbo_keeps_source_identity_and_exact_spread():
    result = observation().as_of(as_of_ms=115, max_age_ms=20)
    assert result.qualified
    assert result.midpoint == D("100")
    assert result.spread_bps == D("20")
    assert result.bid_quantity == 2 and result.ask_quantity == 3
    assert result.region == "eea" and result.source_epoch == "epoch-1"
    assert result.depth_levels == 1


def test_future_receipt_stale_quote_and_clock_anomaly_cannot_be_used():
    quote = observation()
    assert quote.as_of(as_of_ms=109, max_age_ms=20).reason == "NOT_YET_AVAILABLE"
    assert quote.as_of(as_of_ms=131, max_age_ms=20).reason == "STALE"
    future_event = observation(event_at_ms=120, available_at_ms=110)
    assert future_event.as_of(as_of_ms=120, max_age_ms=20).reason == "CLOCK_ANOMALY"


def test_crossed_locked_and_nonpositive_quotes_remain_diagnostic():
    assert observation(bid_price=D("100.1")).as_of(as_of_ms=115, max_age_ms=20).reason == "LOCKED_OR_CROSSED"
    assert observation(bid_quantity=D("0")).as_of(as_of_ms=115, max_age_ms=20).reason == "INVALID_SIZE"
    with pytest.raises(ValueError):
        observation(bid_price=D("NaN"))


def test_bbo_requires_distinct_channel_and_bounded_freshness():
    with pytest.raises(ValueError, match="channel"):
        observation(channel="books")
    with pytest.raises(ValueError, match="age"):
        observation().as_of(as_of_ms=115, max_age_ms=0)


def test_rust_bbo_record_is_unqualified_until_capture_lineage_is_proven():
    record = dict(
        schema_version="okx.observations.v1",
        venue="okx",
        environment="public",
        region="eea",
        instrument_id="okx:spot:BTC-USDC",
        channel="bbo-tbt",
        bid_price="99.9",
        bid_quantity="2",
        ask_price="100.1",
        ask_quantity="3",
        sequence="7",
        event_at_ms=100,
        available_at_ms=110,
        metadata_revision="meta-1",
        source_epoch="epoch-1",
        snapshot=True,
        depth_scope="single_level",
        quality="unqualified",
        reason_codes=["MISSING_CAPTURE_LINEAGE"],
    )
    parsed = TopOfBookObservation.from_rust_record(
        record, source_profile="okx_eea_public_v1", selection_epoch="selection-1"
    )
    assert parsed.reason_codes == ("MISSING_CAPTURE_LINEAGE",)
    assert parsed.as_of(as_of_ms=115, max_age_ms=20).reason == "SOURCE_UNQUALIFIED"
    record["quality"] = "qualified"
    record["reason_codes"] = []
    assert (
        TopOfBookObservation.from_rust_record(record, source_profile="okx_eea_public_v1", selection_epoch="selection-1")
        .as_of(as_of_ms=115, max_age_ms=20)
        .spread_bps
        == 20
    )
    record["depth_scope"] = "full_depth"
    with pytest.raises(ValueError, match="single-level"):
        TopOfBookObservation.from_rust_record(record, source_profile="okx_eea_public_v1", selection_epoch="selection-1")


def test_global_profile_and_spread_are_independent_of_ambient_decimal_context():
    quote = observation(
        region="global",
        source_profile="okx_global_public_v1",
        bid_price=D("1.12345678901234567890"),
        ask_price=D("1.22345678901234567890"),
    )
    with localcontext() as context:
        context.prec = 5
        first = quote.as_of(as_of_ms=115, max_age_ms=20)
    with localcontext() as context:
        context.prec = 50
        second = quote.as_of(as_of_ms=115, max_age_ms=20)
    assert first.midpoint == second.midpoint == D("1.17345678901234567890")
    assert first.spread_bps == second.spread_bps
    with pytest.raises(ValueError, match="decimal"):
        observation(bid_price=D("1e101"))
