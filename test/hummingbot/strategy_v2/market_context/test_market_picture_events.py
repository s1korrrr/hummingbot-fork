from hummingbot.strategy_v2.market_context.market_picture.events import (
    ValidObservation,
    breadth_threshold_transition,
    event_occurrence_key,
    qualified_flow_spike,
    quality_transition,
    regime_transition,
    threshold_crossing,
    trend_continuation,
)


def test_threshold_crossing_never_bridges_missing_values():
    assert threshold_crossing(None, 71, 70, above=True) is None
    assert threshold_crossing(71, 69, 70, above=True) is None
    crossing = threshold_crossing(70, 71, 70, above=True)
    assert crossing.direction == "above"
    assert (crossing.prior_value, crossing.current_value) == (70, 71)
    assert threshold_crossing(30, 29, 30, above=False).direction == "below"
    assert threshold_crossing(31, 30, 30, above=False) is None


def test_regime_events_require_same_instrument_schema_and_consecutive_sequence():
    previous = ValidObservation("okx:spot:BTC-USDC", "regime.v1", 8, 60_000, None)
    current = ValidObservation("okx:spot:BTC-USDC", "regime.v1", 9, 120_000, None)
    assert regime_transition(
        previous,
        current,
        previous_label="range",
        current_label="trend",
        expected_instrument_id="okx:spot:BTC-USDC",
        expected_schema="regime.v1",
    )
    other_asset = ValidObservation("okx:spot:ETH-USDC", "regime.v1", 9, 120_000, None)
    assert not regime_transition(
        previous,
        other_asset,
        previous_label="range",
        current_label="trend",
        expected_instrument_id="okx:spot:BTC-USDC",
        expected_schema="regime.v1",
    )
    skipped = ValidObservation("okx:spot:BTC-USDC", "regime.v1", 10, 180_000, None)
    assert not regime_transition(
        previous,
        skipped,
        previous_label="range",
        current_label="trend",
        expected_instrument_id="okx:spot:BTC-USDC",
        expected_schema="regime.v1",
    )


def test_breadth_crossing_requires_same_cohort_and_whole_market_coverage():
    assert (
        breadth_threshold_transition(
            0.59, 0.61, previous_cohort_hash="a", current_cohort_hash="a", previous_coverage=0.95, current_coverage=0.95
        ).direction
        == "above_60"
    )
    assert (
        breadth_threshold_transition(
            0.59, 0.61, previous_cohort_hash="a", current_cohort_hash="b", previous_coverage=1, current_coverage=1
        )
        is None
    )
    assert (
        breadth_threshold_transition(
            0.59, 0.61, previous_cohort_hash="a", current_cohort_hash="a", previous_coverage=0.94, current_coverage=1
        )
        is None
    )


def test_trend_event_needs_three_consecutive_valid_increasing_strength_bars():
    rows = [(0, "up", 26.0, "VALID"), (60_000, "up", 27.0, "VALID"), (120_000, "up", 28.0, "VALID")]
    assert trend_continuation(rows) == "up"
    assert trend_continuation([rows[0], rows[1], (180_000, "up", 30.0, "VALID")]) is None
    assert (
        trend_continuation([(0, "up", 24.0, "VALID"), (60_000, "up", 26.0, "VALID"), (120_000, "up", 28.0, "VALID")])
        is None
    )


def test_occurrence_identity_is_stable_across_source_epoch_or_sequence_changes():
    args = dict(
        definition_id="rsi14_wilder",
        definition_version="1.0.0",
        instrument_id="okx:spot:BTC-USDC",
        universe_hash="a" * 64,
        window_start_ms=60_000,
        window_end_ms=120_000,
        transition={"from": "below", "to": "above", "threshold": "70"},
    )
    assert event_occurrence_key(**args) == event_occurrence_key(**args)


def test_flow_event_requires_complete_taker_side_source_and_real_baselines():
    baseline = [10.0] * 20
    assert (
        qualified_flow_spike(
            current_buy_quote=14,
            current_sell_quote=6,
            prior_buy_quote=baseline,
            prior_sell_quote=baseline,
            complete_window=True,
            current_total_quote=20,
        )
        is None
    )
    assert (
        qualified_flow_spike(
            current_buy_quote=30,
            current_sell_quote=10,
            prior_buy_quote=baseline,
            prior_sell_quote=baseline,
            complete_window=True,
            current_total_quote=40,
        )
        == "BUY"
    )
    assert (
        qualified_flow_spike(
            current_buy_quote=10,
            current_sell_quote=30,
            prior_buy_quote=baseline,
            prior_sell_quote=baseline,
            complete_window=True,
            current_total_quote=40,
        )
        == "SELL"
    )
    assert (
        qualified_flow_spike(
            current_buy_quote=30,
            current_sell_quote=10,
            prior_buy_quote=baseline,
            prior_sell_quote=baseline,
            complete_window=False,
            current_total_quote=40,
        )
        is None
    )
    assert (
        qualified_flow_spike(
            current_buy_quote=30,
            current_sell_quote=10,
            prior_buy_quote=[0.0] * 20,
            prior_sell_quote=baseline,
            complete_window=True,
            current_total_quote=40,
        )
        is None
    )


def test_quality_events_represent_only_state_changes():
    assert quality_transition("fresh", "stale") == ("fresh", "stale")
    assert quality_transition("stale", "stale") is None
