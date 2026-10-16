from decimal import Decimal
from test.hummingbot.strategy_v2.components.test_observation_adapter import trade_record

import pytest

from hummingbot.strategy_v2.market_context.observation_adapter import parse_rust_trade
from hummingbot.strategy_v2.market_context.observation_bars import bar_membership_sha256, enrich_completed_bar


def members():
    return [
        parse_rust_trade(
            trade_record(trade_id=str(i), side=side, event_at_ms=i * 100, available_at_ms=i * 100 + 1),
            source_profile="okx_eea_public_v1",
        )
        for i, side in enumerate(("buy", "sell", "unknown"), 1)
    ]


def time_bar(**changes):
    result = dict(
        schema_version="okx.observations.time-bar.v1",
        start_ms=0,
        end_ms=60000,
        available_at_ms=60001,
        open="10",
        high="10",
        low="10",
        close="10",
        base_quantity="12",
        quote_notional="120",
        trade_count=3,
        quality="qualified",
        reason_codes=[],
    )
    result.update(changes)
    return result


def enrich(bar, values=None, **changes):
    args = dict(
        instrument_id="okx:spot:BTC-USDC",
        source_epoch="epoch-1",
        source_profile="okx_eea_public_v1",
        metadata_revision="meta-1",
        as_of_ms=60002,
        coverage_start_ms=0,
        coverage_end_ms=60000,
        side_verified=True,
    )
    values = members() if values is None else values
    args["verified_membership_sha256"] = bar_membership_sha256(
        bar,
        values,
        **{key: args[key] for key in ("instrument_id", "source_epoch", "source_profile", "metadata_revision")},
    )
    args.update(changes)
    return enrich_completed_bar(bar, values, **args)


def test_enrichment_uses_native_unknown_flow_and_exact_totals():
    result = enrich(time_bar())
    assert result.flow.buy_quote == result.flow.sell_quote == result.flow.unknown_quote == Decimal(40)
    assert result.flow.confirmation_imbalance is None
    assert result.base_by_side == (Decimal(4), Decimal(4), Decimal(4))
    assert result.count_by_side == (1, 1, 1)
    assert result.vwap == Decimal(10) and result.quote_per_second == Decimal(2)


@pytest.mark.parametrize(
    "change", [dict(trade_count=4), dict(quote_notional="121"), dict(quality="unavailable"), dict(close="11")]
)
def test_missing_members_or_changed_bar_cannot_enrich(change):
    with pytest.raises(ValueError):
        enrich(time_bar(**change))


def test_scope_availability_duplicates_and_half_open_boundary_fail_closed():
    for values in (members()[:-1], members() + members()[:1]):
        with pytest.raises(ValueError):
            enrich(time_bar(), values)
    for args in (dict(source_epoch="other"), dict(as_of_ms=60000), dict(coverage_start_ms=1)):
        with pytest.raises(ValueError):
            enrich(time_bar(), **args)
    values = members()
    values[-1] = parse_rust_trade(
        trade_record(trade_id="3", event_at_ms=60000, available_at_ms=60001), source_profile="okx_eea_public_v1"
    )
    with pytest.raises(ValueError):
        enrich(time_bar(), values)


def test_healthy_empty_has_null_vwap_and_zero_rate():
    result = enrich(
        time_bar(
            open=None,
            high=None,
            low=None,
            close=None,
            base_quantity="0",
            quote_notional="0",
            trade_count=0,
            quality="qualified_empty",
        ),
        [],
    )
    assert result.vwap is None and result.quote_per_second == 0


def test_notional_membership_digest_and_zero_duration():
    import hashlib

    value = members()[0]
    digest = hashlib.sha256(("0" * 64 + ":1").encode()).hexdigest()
    bar = dict(
        schema_version="okx.observations.notional-bar.v1",
        start_ms=100,
        end_ms=100,
        available_at_ms=101,
        open="10",
        high="10",
        low="10",
        close="10",
        quote_notional="40",
        overshoot="0",
        trade_ids=["1"],
        trade_id_count=1,
        trade_ids_truncated=False,
        trade_ids_sha256=digest,
        first_trade_id="1",
        last_trade_id="1",
        close_reason="THRESHOLD",
    )
    result = enrich(bar, [value], calibrated_quote_threshold=Decimal(40))
    assert result.duration_ms == 0 and result.quote_per_second is None
    with pytest.raises(ValueError, match="overshoot"):
        enrich({**bar, "overshoot": "999"}, [value], calibrated_quote_threshold=Decimal(40))
    with pytest.raises(ValueError, match="calibration"):
        enrich(bar, [value])
    with pytest.raises(ValueError):
        enrich({**bar, "trade_ids_sha256": "f" * 64}, [value], calibrated_quote_threshold=Decimal(40))


def test_time_contract_and_independent_membership_receipt():
    for bar in (time_bar(end_ms=30000), time_bar(start_ms=1, end_ms=60001, available_at_ms=60002)):
        with pytest.raises(ValueError, match="60-second"):
            enrich(bar)
    changed = members()
    original_receipt = receipt(time_bar(), changed)
    changed[1] = parse_rust_trade(
        trade_record(trade_id="4", event_at_ms=200, available_at_ms=201), source_profile="okx_eea_public_v1"
    )
    with pytest.raises(ValueError, match="membership"):
        enrich(time_bar(), changed, verified_membership_sha256=original_receipt)


def receipt(bar, values):
    return bar_membership_sha256(
        bar,
        values,
        instrument_id="okx:spot:BTC-USDC",
        source_epoch="epoch-1",
        source_profile="okx_eea_public_v1",
        metadata_revision="meta-1",
    )


def test_membership_receipt_binds_normalized_economics_and_empty_window():
    from dataclasses import replace

    values = members()
    original = receipt(time_bar(), values)
    values[1] = replace(values[1], trade=replace(values[1].trade, quantity=Decimal(5)))
    values[2] = replace(values[2], trade=replace(values[2].trade, quantity=Decimal(3)))
    with pytest.raises(ValueError, match="membership"):
        enrich(time_bar(), values, verified_membership_sha256=original)
    assert receipt(time_bar(), []) != receipt(time_bar(start_ms=60000, end_ms=120000), [])
