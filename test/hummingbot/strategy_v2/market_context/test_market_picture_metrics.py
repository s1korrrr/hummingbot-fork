import math

import numpy as np
import pytest

from hummingbot.strategy_v2.market_context.market_picture.metrics import (
    AssetBarsV1,
    breadth,
    breadth_pressure,
    breadth_pressure_decimal,
    classify_pressure,
    compute_asset_metrics,
    correlate_hourly_closes,
    correlation_30d_trend,
    count_predicate,
    distribution_summary,
    histogram,
    indicator_snapshot,
    metric_decimal,
    new_10d_hourly_close_break,
    new_24h_high,
    new_52w_completed_day_break,
    pearson_correlation,
    percentile_midranks,
    realized_volatility_24h,
    relative_volume,
    return_percent,
)


def test_six_horizon_returns_and_breadth_keep_zero_and_missing_distinct():
    assert return_percent(np.array([100.0, 101.0]), 1) == pytest.approx(1.0)
    values = {"a": 1.0, "b": -1.0, "c": 0.0, "d": None}
    result = breadth(values, expected_count=4, horizon_minutes=1, cutoff_ms=60_000)
    assert (result.advances, result.declines, result.unchanged) == (1, 1, 1)
    assert (result.valid_count, result.expected_count, result.omitted_count) == (3, 4, 1)
    assert result.advance_share == pytest.approx(1 / 3)


def test_breadth_pressure_uses_same_eligible_denominator_and_display_bands():
    result = breadth({"a": 1.0, "b": 1.0, "c": -1.0}, 3, 1, 60_000)
    assert breadth_pressure(result) == pytest.approx(1.0)
    assert breadth_pressure_decimal(result) == "1"
    assert classify_pressure(-1.5) == "bear"
    assert classify_pressure(-0.5) == "neutral"
    assert classify_pressure(0.5) == "neutral"
    assert classify_pressure(1.5) == "bull"
    assert classify_pressure(3.0) == "extreme_bull"
    assert classify_pressure(None) is None


def test_breadth_pressure_decimal_uses_frozen_precision_without_float_serialization():
    result = breadth({"a": 1.0}, expected_count=1, horizon_minutes=1, cutoff_ms=60_000)
    assert breadth_pressure_decimal(result) == "3"
    result = breadth({"a": 1.0, "b": 0.0, "c": 0.0}, 3, 1, 60_000)
    assert breadth_pressure_decimal(result) == "1"
    result = breadth({f"a{i}": (1.0 if i < 4 else -1.0) for i in range(7)}, 7, 1, 60_000)
    assert breadth_pressure_decimal(result) == "0.4285714285714285714285714286"


def test_histogram_has_symmetric_overflow_and_exact_count_conservation():
    values = [-11, -10, -5, -0.5, 0, 0.5, 5, 10, 11]
    result = histogram(values, span_percent=10, interior_bins=40)
    assert len(result.counts) == 42
    assert result.counts[0] == result.counts[-1] == 1
    assert sum(result.counts) == len(values)
    assert result.zero_count == 1


def test_participation_predicate_uses_strict_comparison_and_real_valid_denominator():
    result = count_predicate({"a": 70.0, "b": 70.01, "c": None}, lambda x: x > 70.0, 3)
    assert (result.numerator, result.denominator, result.expected_denominator) == (1, 2, 3)
    assert result.share == pytest.approx(0.5)


def test_relative_volume_excludes_current_bar_and_zero_mean_is_unavailable():
    assert relative_volume([10.0, *([2.0] * 20)], baseline_count=20) == pytest.approx(5.0)
    assert relative_volume([10.0, *([0.0] * 20)], baseline_count=20) is None
    assert relative_volume([10.0, 2.0], baseline_count=20) is None


def test_realized_volatility_uses_1440_log_returns_sample_std():
    close = np.exp(np.linspace(0.0, 0.1, 1441))
    expected = np.std(np.diff(np.log(close)), ddof=1) * math.sqrt(365 * 24 * 60)
    assert realized_volatility_24h(close) == pytest.approx(expected)
    assert realized_volatility_24h(close[:-1]) is None


def test_percentile_midranks_ties_and_correlation_requires_valid_pairs():
    assert percentile_midranks(2.0, [1.0, 2.0, 2.0, 3.0]) == pytest.approx(50.0)
    assert pearson_correlation([1, 2, 3], [2, 4, 6], min_pairs=3) == pytest.approx(1.0)
    assert pearson_correlation([1, 1, 1], [2, 4, 6], min_pairs=3) is None
    assert pearson_correlation([1, 2], [2, 4], min_pairs=3) is None


def test_hourly_correlation_never_spans_a_missing_hour_and_enforces_95_percent():
    times = [i * 3_600_000 for i in range(101)]
    left = {t: 100.0 + i for i, t in enumerate(times)}
    right = {t: 200.0 + 2 * i for i, t in enumerate(times)}
    complete = correlate_hourly_closes(left, right, expected_end_times_ms=times[1:], min_pairs=60)
    assert complete.sample_count == complete.expected_count == 100
    assert complete.value == pytest.approx(1.0)
    for index in (4, 15, 30, 45, 60, 75):
        left.pop(times[index])
    partial = correlate_hourly_closes(left, right, expected_end_times_ms=times[1:], min_pairs=60)
    assert partial.sample_count == 88  # neither adjacent return spans a missing hour
    assert partial.value is None  # below the 95% gate


def test_30d_correlation_trend_is_30_points_of_the_rolling_90d_estimator():
    interval = 3_600_000
    times = list(range(0, 121 * 24 * interval, interval))
    left = {t: 100.0 + (i % 24) + i * 0.01 for i, t in enumerate(times)}
    right = {t: 200.0 + (i % 24) * 2 + i * 0.02 for i, t in enumerate(times)}
    trend = correlation_30d_trend(left, right, cutoff_ms=times[-1])
    assert len(trend) == 30
    assert trend[0][0] == times[-1] - 29 * 24 * interval
    assert all(point.value is not None and point.expected_count == 2160 for _, point in trend)


def test_indicator_snapshot_is_causal_and_returns_null_during_warmup():
    close = np.linspace(100, 120, 50)
    high, low = close + 1, close - 1
    warm = indicator_snapshot(high[:10], low[:10], close[:10])
    assert warm.ema21 is not None and warm.rsi14 is None and warm.adx14 is None
    result = indicator_snapshot(high, low, close)
    assert result.rsi14 is not None and result.atr14 is not None and result.adx14 is not None
    assert result.plus_di14 is not None and result.minus_di14 is not None


def test_wilder_rsi_matches_existing_modular_v2_kernel_without_importing_it_in_runtime():
    from controllers.generic.modular_spot_core.market import wilder_rsi as existing_wilder_rsi

    close = np.asarray([100, 99, 101, 102, 100, 98, 99, 103, 102, 105, 107, 106, 108, 110, 109, 112, 111], dtype=float)
    expected = existing_wilder_rsi(close, 14)
    actual = __import__(
        "hummingbot.strategy_v2.market_context.market_picture.metrics", fromlist=["wilder_rsi"]
    ).wilder_rsi(close, 14)
    np.testing.assert_allclose(actual[np.isfinite(actual)], expected)


def test_breakouts_use_exact_closed_history_and_strict_boundaries():
    assert new_24h_high(11.0, [10.0] * 1440) is True
    assert new_24h_high(10.0, [10.0] * 1440) is False
    assert new_24h_high(11.0, [10.0] * 1439) is None
    assert new_10d_hourly_close_break(12.0, [11.0] * 240, [9.0] * 240) == (True, False)
    assert new_10d_hourly_close_break(11.0, [11.0] * 240, [9.0] * 240) == (False, False)
    assert new_52w_completed_day_break(12.0, 8.0, [11.0] * 364, [9.0] * 364) == (True, True)
    assert new_52w_completed_day_break(12.0, 8.0, [11.0] * 363, [9.0] * 363) is None


def test_asset_metric_builder_emits_all_frozen_asset_keys_and_feature_specific_statuses():
    n = 10_081
    cutoff = 1_800_000_000_000
    close = tuple(100.0 + i * 0.001 for i in range(n))
    bars = AssetBarsV1(
        instrument_id="okx:spot:BTC-USDC",
        close_times_ms=tuple(cutoff - (n - i - 1) * 60_000 for i in range(n)),
        high=tuple(value + 0.5 for value in close),
        low=tuple(value - 0.5 for value in close),
        close=close,
        quote_volume=tuple(10.0 if i == n - 1 else 2.0 for i in range(n)),
        cutoff_ms=cutoff,
    )
    metrics = compute_asset_metrics(bars, benchmark_returns_24h={"btc": 0.0, "eth": 1.0})
    assert set(metrics) == {
        "price",
        *(f"return_{h}m" for h in (1, 5, 15, 60, 240, 1440, 10080)),
        "ema21",
        "rsi14",
        "adx14",
        "atr14_percentile",
        "rvol20",
        "rvol_24h",
        "atr14_percent",
        "realized_volatility_24h",
        "relative_24h_btc",
        "relative_24h_eth",
        "relative_24h_bnb",
        "relative_24h_sol",
    }
    assert metrics["return_10080m"].computation_status == "VALID"
    assert metrics["rvol20"].value == pytest.approx(5.0)
    assert metrics["rvol_24h"].value == pytest.approx(5.0)
    assert metrics["atr14_percentile"].value == pytest.approx(0.0)
    assert metrics["relative_24h_btc"].value is not None
    assert metrics["relative_24h_bnb"].computation_status == "UNAVAILABLE"


def test_distribution_summary_preserves_empty_and_stable_extrema():
    result = distribution_summary({"a": -2.0, "b": 4.0, "c": 4.0, "d": 0.0, "e": None}, 5)
    assert (result.valid_count, result.expected_count) == (4, 5)
    assert (result.negative_count, result.unchanged_count, result.positive_count) == (1, 1, 2)
    assert (result.best_instrument_id, result.worst_instrument_id) == ("b", "a")
    empty = distribution_summary({"a": None}, 1)
    assert empty.mean_return_percent is None and empty.positive_count == 0


def test_numeric_wire_text_is_finite_fixed_and_bounded():
    assert metric_decimal(1.25) == "1.25"
    assert metric_decimal(1e-8) == "0.00000001"
    assert metric_decimal(-0.0) == "0"
    assert metric_decimal(None) is None
    with pytest.raises(ValueError):
        metric_decimal(float("inf"))
    with pytest.raises(ValueError):
        metric_decimal(True)
