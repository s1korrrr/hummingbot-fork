from dataclasses import replace

import pandas as pd

from controllers.directional_trading.hyperliquid_portfolio_rsi_features import (
    PairFeatureConfig,
    build_pair_signal_snapshot,
)

AS_OF = 1_700_000_040.0
PAIR = "COINALPHA-USDC"


def _candles(closes, *, include_open=True):
    closed_count = len(closes)
    first_open = AS_OF - (closed_count * 60)
    rows = []
    for index, close in enumerate(closes):
        timestamp = first_open + (index * 60)
        rows.append({
            "timestamp": timestamp,
            "open": close,
            "high": close * 1.002,
            "low": close * 0.998,
            "close": close,
            "volume": 1_000 + index,
        })
    if include_open:
        rows.append({
            "timestamp": AS_OF,
            "open": closes[-1],
            "high": closes[-1] * 20,
            "low": closes[-1] * 0.5,
            "close": closes[-1] * 20,
            "volume": 1_000_000,
        })
    return pd.DataFrame(rows)


def _market(price, **overrides):
    payload = {
        "timestamp": AS_OF,
        "bid_price": price * 0.9999,
        "ask_price": price * 1.0001,
        "bid_depth_quote": "100000",
        "ask_depth_quote": "100000",
    }
    payload.update(overrides)
    return payload


def _trading_rule(**overrides):
    payload = {
        "min_notional_size": "10",
        "min_base_amount_increment": "0.01",
        "min_price_increment": "0.000001",
        "max_price_significant_digits": "5",
    }
    payload.update(overrides)
    return payload


def _build(closes, **overrides):
    payload = {
        "pair_id": PAIR,
        "candles": _candles(closes),
        "market_snapshot": _market(closes[-1]),
        "trading_rule": _trading_rule(),
        "as_of": AS_OF,
    }
    payload.update(overrides)
    return build_pair_signal_snapshot(**payload)


def test_excludes_open_candle_and_builds_rsi_buy_signal_from_closed_data():
    closes = [120 - (index * 0.2) for index in range(80)]

    snapshot = _build(closes)

    assert snapshot["schema"] == "rsibot.hyperliquid_portfolio_rsi.pair_signal.v1"
    assert snapshot["freshness_status"] == "fresh"
    assert snapshot["symbol_mapping_status"] == "resolved"
    assert snapshot["actionable"] is True
    assert snapshot["closed_candle_count"] == 80
    assert snapshot["excluded_open_candle_count"] == 1
    assert snapshot["price_quote"] != str(closes[-1] * 20)
    assert snapshot["indicators"]["rsi"] <= 31
    assert float(snapshot["buy_score"]) >= 0.5
    assert snapshot["sell_score"] == "0"


def test_builds_overbought_rollover_sell_signal():
    closes = [80 + (index * 0.5) for index in range(75)] + [118, 121, 124, 127, 126]

    snapshot = _build(closes)

    assert snapshot["actionable"] is True
    assert snapshot["indicators"]["rsi"] >= 75
    assert "rsi_overbought" in snapshot["signal_reasons"]["sell"]
    assert "rsi_rollover" in snapshot["signal_reasons"]["sell"]
    assert float(snapshot["sell_score"]) >= 0.5
    assert snapshot["buy_score"] == "0"


def test_stale_closed_candle_fails_closed_even_when_market_is_current():
    closes = [100 + (index * 0.1) for index in range(80)]
    config = replace(PairFeatureConfig(), max_candle_age_intervals=2.0)

    snapshot = _build(
        closes,
        as_of=AS_OF + 600,
        market_snapshot=_market(closes[-1], timestamp=AS_OF + 600),
        config=config,
    )

    assert snapshot["actionable"] is False
    assert snapshot["freshness_status"] == "stale"
    assert "candle_stale" in snapshot["blockers"]
    assert snapshot["buy_score"] == "0"
    assert snapshot["sell_score"] == "0"


def test_crossed_or_shallow_book_fails_closed_with_cost_evidence():
    closes = [100 - (index * 0.2) for index in range(80)]
    market = _market(
        closes[-1],
        bid_price="100",
        ask_price="99",
        bid_depth_quote="5",
        ask_depth_quote="5",
    )

    snapshot = _build(closes, market_snapshot=market)

    assert snapshot["actionable"] is False
    assert "book_crossed" in snapshot["blockers"]
    assert "bid_liquidity_below_minimum" in snapshot["blockers"]
    assert "ask_liquidity_below_minimum" in snapshot["blockers"]
    assert snapshot["buy_score"] == "0"
    assert snapshot["sell_score"] == "0"


def test_excessive_spread_and_estimated_slippage_fail_closed():
    closes = [100 - (index * 0.2) for index in range(80)]
    market = _market(
        closes[-1],
        bid_price="95",
        ask_price="105",
        bid_depth_quote="100",
        ask_depth_quote="100",
    )

    snapshot = _build(closes, market_snapshot=market)

    assert snapshot["actionable"] is False
    assert "spread_above_limit" in snapshot["blockers"]
    assert "buy_slippage_above_limit" in snapshot["blockers"]
    assert "sell_slippage_above_limit" in snapshot["blockers"]
    assert snapshot["market"]["spread_bps"] == 1000.0


def test_estimated_fee_is_included_in_total_cost_gate():
    closes = [100 - (index * 0.1) for index in range(80)]
    config = replace(
        PairFeatureConfig(),
        estimated_fee_bps_per_side=60.0,
        max_total_cost_bps_per_side=50.0,
    )

    snapshot = _build(closes, config=config)

    assert snapshot["actionable"] is False
    assert "buy_total_cost_above_limit" in snapshot["blockers"]
    assert "sell_total_cost_above_limit" in snapshot["blockers"]
    assert snapshot["market"]["estimated_fee_bps_per_side"] == 60.0
    assert snapshot["market"]["estimated_round_trip_cost_bps"] > 120.0


def test_unresolved_symbol_or_missing_minimum_notional_fails_closed():
    closes = [100 - (index * 0.2) for index in range(80)]

    snapshot = _build(
        closes,
        symbol_mapping_status="unresolved",
        trading_rule=_trading_rule(min_notional_size=None),
    )

    assert snapshot["actionable"] is False
    assert snapshot["symbol_mapping_status"] == "unresolved"
    assert "symbol_unresolved" in snapshot["blockers"]
    assert "minimum_notional_missing" in snapshot["blockers"]


def test_present_but_mismatched_advisory_evidence_fails_closed():
    closes = [100 - (index * 0.2) for index in range(80)]
    advisory = {
        "schema": "rsibot.hyperliquid_portfolio_rsi.advisory_signal.v1",
        "pair_id": "OTHER-USDC",
        "source_timestamp": AS_OF,
        "feature_cutoff": AS_OF,
        "buy_score": "1",
        "sell_score": "0",
    }

    snapshot = _build(closes, advisory=advisory)

    assert snapshot["actionable"] is False
    assert snapshot["advisory"]["status"] == "invalid"
    assert "advisory_pair_mismatch" in snapshot["blockers"]
    assert snapshot["buy_score"] == "0"


def test_insufficient_history_and_invalid_timestamp_fail_closed():
    candles = _candles([100 + index for index in range(5)])
    candles["timestamp"] = candles["timestamp"].astype(object)
    candles.loc[2, "timestamp"] = "not-a-time"

    snapshot = build_pair_signal_snapshot(
        pair_id=PAIR,
        candles=candles,
        market_snapshot=_market(104),
        trading_rule=_trading_rule(),
        as_of=AS_OF,
    )

    assert snapshot["actionable"] is False
    assert "candle_timestamp_invalid" in snapshot["blockers"]
    assert snapshot["buy_score"] == "0"
    assert snapshot["sell_score"] == "0"
