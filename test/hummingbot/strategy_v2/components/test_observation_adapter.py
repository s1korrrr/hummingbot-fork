from decimal import Decimal as D, localcontext

import pytest

from hummingbot.strategy_v2.market_context.observation_adapter import parse_rust_trade


def trade_record(**changes):
    record = dict(
        schema_version="okx.observations.v1",
        venue="okx",
        environment="public",
        region="eea",
        instrument_id="okx:spot:BTC-USDC",
        channel="trades-all",
        trade_id="123",
        price="10",
        base_quantity="4",
        quote_notional="40",
        side="sell",
        aggregate_count=1,
        event_at_ms=100,
        available_at_ms=110,
        metadata_revision="meta-1",
        source_epoch="epoch-1",
        quality="qualified",
        connection_id="c1",
        receive_sequence=1,
        raw_sha256="a" * 64,
        reason_codes=[],
    )
    record.update(changes)
    return record


def test_trade_adapter_keeps_lineage_and_native_flow_meaning():
    result = parse_rust_trade(trade_record(), source_profile="okx_eea_public_v1")
    assert result.qualified
    assert result.trade.quote_notional == D(40)
    assert result.trade.trade_id == 123 and result.trade.side == "sell"
    assert result.region == "eea" and result.source_epoch == "epoch-1"
    assert result.connection_id == "c1" and result.raw_sha256 == "a" * 64


def test_unqualified_or_future_event_never_reaches_native_flow():
    unqualified = parse_rust_trade(
        trade_record(quality="unqualified", reason_codes=["MISSING_CAPTURE_LINEAGE"]),
        source_profile="okx_eea_public_v1",
    )
    assert unqualified.trade is None and unqualified.reason == "SOURCE_UNQUALIFIED"
    future = parse_rust_trade(trade_record(event_at_ms=120, available_at_ms=110), source_profile="okx_eea_public_v1")
    assert future.trade is None and future.reason == "CLOCK_ANOMALY"


def test_trade_adapter_rejects_changed_notional_and_wrong_channel():
    with pytest.raises(ValueError, match="notional"):
        parse_rust_trade(trade_record(quote_notional="120"), source_profile="okx_eea_public_v1")
    with pytest.raises(ValueError, match="channel"):
        parse_rust_trade(trade_record(channel="books"), source_profile="okx_eea_public_v1")
    with pytest.raises(ValueError, match="profile"):
        parse_rust_trade(trade_record(region="global"), source_profile="okx_eea_public_v1")


def test_unqualified_parser_record_without_raw_hash_stays_diagnostic():
    result = parse_rust_trade(
        trade_record(quality="unqualified", raw_sha256="", reason_codes=["MISSING_CAPTURE_LINEAGE"]),
        source_profile="okx_eea_public_v1",
    )
    assert result.trade is None and result.source_reasons == ("MISSING_CAPTURE_LINEAGE",)


def test_global_profile_and_exact_notional_ignore_ambient_decimal_context():
    record = trade_record(
        region="global",
        price="1.123456789012345678901234567890",
        base_quantity="2.123456789012345678901234567890",
        quote_notional="2.385611945790275873454199055232536198787501905199875019052100",
    )
    with localcontext() as context:
        context.prec = 5
        result = parse_rust_trade(record, source_profile="okx_global_public_v1")
    assert result.qualified and result.region == "global"
    with pytest.raises(ValueError, match="notional"):
        parse_rust_trade({**record, "quote_notional": "2.3852"}, source_profile="okx_global_public_v1")
