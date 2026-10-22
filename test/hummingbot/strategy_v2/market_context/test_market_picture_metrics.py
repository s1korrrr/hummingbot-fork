import math
import warnings
from decimal import Decimal

import numpy as np
import pytest

from hummingbot.strategy_v2.market_context.market_picture import metrics as market_picture_metrics
from hummingbot.strategy_v2.market_context.market_picture.metrics import (
    AssetBarsV1,
    breadth,
    breadth_pressure,
    breadth_pressure_decimal,
    classify_pressure,
    compute_asset_metrics,
    correlate_hourly_closes,
    correlate_hourly_return_vectors,
    correlation_30d_trend,
    correlation_min_pairs,
    count_predicate,
    dense_hourly_correlation_matrix,
    dense_hourly_correlation_row,
    distribution_summary,
    histogram,
    indicator_snapshot,
    metric_decimal,
    new_10d_hourly_close_break,
    new_24h_high,
    new_52w_completed_day_break,
    pearson_correlation,
    percentile_midranks,
    prepare_hourly_log_returns,
    prepare_hourly_log_returns_from_validated_closes,
    realized_volatility_24h,
    relative_volume,
    relative_volume_hourly,
    return_percent,
    return_percent_decimal,
)


def test_six_horizon_returns_and_breadth_keep_zero_and_missing_distinct():
    assert return_percent(np.array([100.0, 101.0]), 1) == pytest.approx(1.0)
    values = {"a": 1.0, "b": -1.0, "c": 0.0, "d": None}
    result = breadth(values, expected_count=4, horizon_minutes=1, cutoff_ms=60_000)
    assert (result.advances, result.declines, result.unchanged) == (1, 1, 1)
    assert (result.valid_count, result.expected_count, result.omitted_count) == (
        3,
        4,
        1,
    )
    assert result.advance_share == pytest.approx(1 / 3)


def test_exact_decimal_return_preserves_sub_float_breadth_sign():
    closes = [
        Decimal("1000000000000000000000000"),
        Decimal("1000000000000000000000001"),
    ]
    exact = return_percent_decimal(closes, 1)
    assert exact is not None and exact > 0
    assert 100.0 * (float(closes[-1]) / float(closes[0]) - 1.0) == 0.0
    result = breadth({"tiny_advance": exact, "flat": Decimal(0)}, 2, 1, 60_000)
    assert (result.advances, result.declines, result.unchanged) == (1, 0, 1)


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
    result = breadth(
        {f"a{i}": (1.0 if i < 4 else -1.0) for i in range(7)}, 7, 1, 60_000
    )
    assert breadth_pressure_decimal(result) == "0.4285714285714285714285714286"


def test_histogram_has_symmetric_overflow_and_exact_count_conservation():
    values = [-11, -10, -5, -0.5, 0, 0.5, 5, 10, 11]
    result = histogram(values, span_percent=10, interior_bins=40)
    assert len(result.counts) == 42
    assert result.counts[0] == result.counts[-1] == 1
    assert sum(result.counts) == len(values)
    assert result.zero_count == 1


def test_prepared_correlation_preserves_pairwise_missing_and_constant_semantics():
    grid = tuple(range(3_600_000, 2161 * 3_600_000, 3_600_000))
    left = {t: 100 + math.sin(index / 10) for index, t in enumerate((0, *grid))}
    right = {t: 50 + math.sin(index / 11) for index, t in enumerate((0, *grid))}
    for t in grid[10:20]:
        right.pop(t)
    a = prepare_hourly_log_returns(left, expected_end_times_ms=grid)
    b = prepare_hourly_log_returns(right, expected_end_times_ms=grid)
    result = correlate_hourly_return_vectors(a, b)
    paired = [
        (
            math.log(left[t] / left[t - 3_600_000]),
            math.log(right[t] / right[t - 3_600_000]),
        )
        for t in grid
        if t in right and t - 3_600_000 in right
    ]
    assert result.sample_count == len(paired) == 2149
    assert result.value == pytest.approx(np.corrcoef(np.array(paired).T)[0, 1])
    assert not a.flags.writeable
    assert correlate_hourly_return_vectors(a, np.zeros_like(b)).value is None
    assert correlate_hourly_return_vectors(a, np.full_like(b, np.nan)).sample_count == 0
    with pytest.raises(ValueError):
        correlate_hourly_return_vectors(a, b[:-1])


def test_batched_dense_correlations_match_scalar_estimator_and_preserve_constant_rows():
    rng = np.random.default_rng(8217)
    returns = rng.normal(0.0, 0.01, (6, 720))
    returns[4] = 0.0
    matrix, constant_rows = dense_hourly_correlation_matrix(returns)
    assert constant_rows.tolist() == [False, False, False, False, True, False]
    for left in range(len(returns)):
        for right in range(len(returns)):
            expected = pearson_correlation(returns[left], returns[right], min_pairs=60)
            actual = matrix[left, right]
            if expected is None:
                assert np.isnan(actual)
            else:
                assert actual == pytest.approx(expected, abs=1e-12)

    row, constants = dense_hourly_correlation_row(returns, 1, 24, 600)
    assert constants.tolist() == [False, False, False, False, True, False]
    for other in range(len(returns)):
        expected = pearson_correlation(
            returns[1, 24:600], returns[other, 24:600], min_pairs=60
        )
        if expected is None:
            assert np.isnan(row[other])
        else:
            assert row[other] == pytest.approx(expected, abs=1e-12)


def test_validated_close_alignment_matches_public_helper_for_gaps_and_extreme_ratios():
    start = 10_000 * 3_600_000
    grid = tuple(start + i * 3_600_000 for i in range(8))
    prices = {
        grid[0] - 3_600_000: 1e308,
        grid[0]: 1e-308,
        grid[1]: 2e-308,
        grid[2]: 3e-308,
        grid[4] - 3_600_000: 4e-308,
        grid[4]: 8e-308,
        grid[5] - 3_600_000: 8e-308,
        grid[5]: 16e-308,
        grid[6]: 1e308,
        grid[7]: 8e307,
    }
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        actual = prepare_hourly_log_returns_from_validated_closes(prices, grid)
        expected = prepare_hourly_log_returns(prices, expected_end_times_ms=grid)
    np.testing.assert_equal(actual, expected)
    assert actual.flags.writeable is False


@pytest.mark.parametrize(
    "horizon,span", [(1, 0.5), (5, 1), (15, 2), (60, 3), (240, 5), (1440, 10)]
)
def test_declared_histogram_spans_conserve_boundary_and_overflow_values(horizon, span):
    from hummingbot.strategy_v2.market_context.market_picture.definitions import get_definition

    definition = get_definition("return_distribution_40_bins")
    parameters = dict(definition.parameters)
    assert float(parameters["spans_percent"][str(horizon)]) == span
    result = histogram(
        [-span - 0.01, -span, 0, span, span + 0.01], span, parameters["interior_bins"]
    )
    assert len(result.counts) == 42
    assert result.edges[0] == -span and result.edges[-1] == span
    assert result.counts[0] == result.counts[-1] == 1
    assert result.counts[1] == result.counts[-2] == 1
    assert sum(result.counts) == 5


def test_participation_predicate_uses_strict_comparison_and_real_valid_denominator():
    result = count_predicate({"a": 70.0, "b": 70.01, "c": None}, lambda x: x > 70.0, 3)
    assert (result.numerator, result.denominator, result.expected_denominator) == (
        1,
        2,
        3,
    )
    assert result.share == pytest.approx(0.5)


def test_relative_volume_excludes_current_bar_and_zero_mean_is_unavailable():
    assert relative_volume([10.0, *([2.0] * 20)], baseline_count=20) == pytest.approx(
        5.0
    )
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
    complete = correlate_hourly_closes(
        left, right, expected_end_times_ms=times[1:], min_pairs=60
    )
    assert complete.sample_count == complete.expected_count == 100
    assert complete.value == pytest.approx(1.0)
    for index in (4, 15, 30, 45, 60, 75):
        left.pop(times[index])
    partial = correlate_hourly_closes(
        left, right, expected_end_times_ms=times[1:], min_pairs=60
    )
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
    assert all(
        point.value is not None and point.expected_count == 2160 for _, point in trend
    )


def test_indicator_snapshot_is_causal_and_returns_null_during_warmup():
    close = np.linspace(100, 120, 50)
    high, low = close + 1, close - 1
    warm = indicator_snapshot(high[:10], low[:10], close[:10])
    assert warm.ema21 is not None and warm.rsi14 is None and warm.adx14 is None
    result = indicator_snapshot(high, low, close)
    assert (
        result.rsi14 is not None
        and result.atr14 is not None
        and result.adx14 is not None
    )
    assert result.plus_di14 is not None and result.minus_di14 is not None


def test_wilder_rsi_matches_existing_modular_v2_kernel_without_importing_it_in_runtime():
    from controllers.generic.modular_spot_core.market import wilder_rsi as existing_wilder_rsi

    close = np.asarray(
        [
            100,
            99,
            101,
            102,
            100,
            98,
            99,
            103,
            102,
            105,
            107,
            106,
            108,
            110,
            109,
            112,
            111,
        ],
        dtype=float,
    )
    expected = existing_wilder_rsi(close, 14)
    actual = __import__(
        "hummingbot.strategy_v2.market_context.market_picture.metrics",
        fromlist=["wilder_rsi"],
    ).wilder_rsi(close, 14)
    np.testing.assert_allclose(actual[np.isfinite(actual)], expected)


def test_breakouts_use_exact_closed_history_and_strict_boundaries():
    assert new_24h_high(11.0, [10.0] * 1440) is True
    assert new_24h_high(10.0, [10.0] * 1440) is False
    assert new_24h_high(11.0, [10.0] * 1439) is None
    assert new_10d_hourly_close_break(12.0, [11.0] * 240, [9.0] * 240) == (True, False)
    assert new_10d_hourly_close_break(11.0, [11.0] * 240, [9.0] * 240) == (False, False)
    assert new_52w_completed_day_break(12.0, 8.0, [11.0] * 364, [9.0] * 364) == (
        True,
        True,
    )
    assert new_52w_completed_day_break(12.0, 8.0, [11.0] * 363, [9.0] * 363) is None


def test_asset_metrics_do_not_reparse_validated_ohlc(monkeypatch):
    n = 1_441
    cutoff = 1_800_000_000_000
    source = tuple(str(100 + i / 1000) for i in range(n))
    parsed = []
    original = market_picture_metrics._price_vector

    def count_parses(values, *, name):
        parsed.append((name, len(values)))
        return original(values, name=name)

    monkeypatch.setattr(market_picture_metrics, "_price_vector", count_parses)
    bars = AssetBarsV1(
        instrument_id="okx:spot:BTC-USDC",
        close_times_ms=tuple(cutoff - (n - i - 1) * 60_000 for i in range(n)),
        high=tuple(str(Decimal(value) + Decimal("0.5")) for value in source),
        low=tuple(str(Decimal(value) - Decimal("0.5")) for value in source),
        close=source,
        quote_volume=(1.0,) * n,
        cutoff_ms=cutoff,
    )
    result = compute_asset_metrics(bars)
    assert result["return_1440m"].value == return_percent_decimal(source, 1440)
    assert parsed == [("high", n), ("low", n), ("close", n)]


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
    metrics = compute_asset_metrics(
        bars, benchmark_returns_24h={"btc": 0.0, "eth": 1.0}
    )
    assert set(metrics) == {
        "price",
        *(f"return_{h}m" for h in (1, 5, 15, 60, 240, 1440, 10080)),
        "ema21",
        "rsi14",
        "adx14",
        "atr14_percentile",
        "rvol20",
        "rvol_24h",
        "rvol_1h",
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
    # Last hour is 59*2 + 10 = 128 against a 120 baseline hour.
    assert metrics["rvol_1h"].value == pytest.approx(128.0 / 120.0)
    assert metrics["atr14_percentile"].value == pytest.approx(0.0)
    assert metrics["relative_24h_btc"].value is not None
    assert metrics["relative_24h_bnb"].computation_status == "UNAVAILABLE"


def test_distribution_summary_preserves_empty_and_stable_extrema():
    result = distribution_summary(
        {"a": -2.0, "b": 4.0, "c": 4.0, "d": 0.0, "e": None}, 5
    )
    assert (result.valid_count, result.expected_count) == (4, 5)
    assert (result.negative_count, result.unchanged_count, result.positive_count) == (
        1,
        1,
        2,
    )
    assert (result.best_instrument_id, result.worst_instrument_id) == ("b", "a")
    empty = distribution_summary({"a": None}, 1)
    assert empty.mean_return_percent is None and empty.positive_count == 0


def test_asset_metric_builder_keeps_exact_decimal_return_for_breadth_consumers():
    cutoff = 1_800_000_000_000
    first = Decimal("1000000000000000000000000")
    last = Decimal("1000000000000000000000001")
    bars = AssetBarsV1(
        instrument_id="okx:spot:BTC-USDC",
        close_times_ms=(cutoff - 60_000, cutoff),
        high=(last, last),
        low=(first, first),
        close=(first, last),
        quote_volume=(1.0, 1.0),
        cutoff_ms=cutoff,
    )
    metrics = compute_asset_metrics(bars)
    result = metrics["return_1m"]
    assert isinstance(result.value, Decimal) and result.value > 0
    assert metrics["price"].value == last
    assert metrics["price"].value != Decimal(str(float(last)))
    assert result.computation_status == "VALID"
    summary = breadth({"okx:spot:BTC-USDC": result.value}, 1, 1, cutoff)
    assert (summary.advances, summary.declines, summary.unchanged) == (1, 0, 0)


def test_distribution_dispersion_requires_two_valid_returns():
    singleton = distribution_summary({"a": 0.25, "b": None}, 2)
    assert singleton.valid_count == 1
    assert singleton.mean_return_percent == pytest.approx(0.25)
    assert singleton.dispersion_percent is None

    pair = distribution_summary({"a": 0.25, "b": 0.25}, 2)
    assert pair.dispersion_percent == pytest.approx(0.0)


def test_numeric_wire_text_is_finite_fixed_and_bounded():
    assert metric_decimal(1.25) == "1.25"
    assert metric_decimal(1e-8) == "0.00000001"
    assert metric_decimal(-0.0) == "0"
    assert metric_decimal(None) is None
    with pytest.raises(ValueError):
        metric_decimal(float("inf"))
    with pytest.raises(ValueError):
        metric_decimal(True)


def test_dense_correlation_row_preserves_large_finite_scalar_result():
    x = np.linspace(-1, 1, 2160)
    rows = np.array([x * 1e150, (x + np.sin(x * 20) * 0.1) * 1e150])
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        actual, constant = dense_hourly_correlation_row(rows, 0, 0, 2160)
    assert not constant.any()
    expected = correlate_hourly_return_vectors(rows[0], rows[1]).value
    assert actual[1] == pytest.approx(expected, abs=1e-12)


def test_hourly_correlation_exact_95_percent_pair_boundary():
    left = np.linspace(-0.1, 0.1, 2160)
    right = left.copy()
    right[2052:] = np.nan
    accepted = correlate_hourly_return_vectors(left, right)
    assert accepted.sample_count == 2052 and accepted.value == pytest.approx(1.0)
    right[2051] = np.nan
    rejected = correlate_hourly_return_vectors(left, right)
    assert rejected.sample_count == 2051 and rejected.value is None


def _zero_inflated_minutes(hours: int, *, last_minute: float = 0.0) -> list[float]:
    """One 60-unit trade per hour at a drifting minute, all other minutes zero; oldest first."""
    volumes = [0.0] * (hours * 60)
    for hour in range(hours):
        volumes[hour * 60 + (hour * 7) % 59] = 60.0
    volumes[-1] = last_minute
    return volumes


def _bars_with_volumes(volumes, cutoff=1_800_000_000_000):
    n = len(volumes)
    close = tuple(100.0 + 0.001 * (i % 50) for i in range(n))
    return AssetBarsV1(
        instrument_id="okx:spot:BNB-USDC",
        close_times_ms=tuple(cutoff - (n - i - 1) * 60_000 for i in range(n)),
        high=tuple(value + 0.5 for value in close),
        low=tuple(value - 0.5 for value in close),
        close=close,
        quote_volume=tuple(volumes),
        cutoff_ms=cutoff,
    )


def test_relative_volume_hourly_matches_independent_block_arithmetic():
    volumes = [float((i * 37) % 11) for i in range(1500)]
    current = sum(volumes[-60:])
    blocks = [sum(volumes[hour * 60 : hour * 60 + 60]) for hour in range(24)]  # noqa: E203
    assert relative_volume_hourly(volumes) == pytest.approx(current / (sum(blocks) / 24))
    # The evaluated hour is excluded from its own baseline.
    assert relative_volume_hourly([0.0] * 1440 + [1.0] * 60) is None
    assert relative_volume_hourly([1.0] * 1500) == pytest.approx(1.0)
    assert relative_volume_hourly([1.0] * 1440 + [3.0] * 60) == pytest.approx(3.0)


def test_relative_volume_hourly_rejects_wrong_length_negative_and_nonpositive_baseline():
    assert relative_volume_hourly([1.0] * 1499) is None
    assert relative_volume_hourly([1.0] * 1501) is None
    assert relative_volume_hourly([1.0] * 1499 + [-1.0]) is None
    assert relative_volume_hourly([0.0] * 1500) is None  # zero baseline is never infinite
    assert relative_volume_hourly([1.0] * 120, baseline_hours=1) == pytest.approx(1.0)
    with pytest.raises(ValueError):
        relative_volume_hourly([1.0] * 1500, baseline_hours=0)
    with pytest.raises(ValueError):
        relative_volume_hourly([1.0] * 1499 + [float("nan")])


def test_zero_inflated_thin_book_single_minute_ratio_is_below_one_but_hourly_ratio_is_about_one():
    volumes = _zero_inflated_minutes(25)
    assert sum(volumes[-60:]) == 60.0
    single_minute = relative_volume([volumes[-1], *reversed(volumes[-1441:-1])], 1440)
    assert single_minute is not None and single_minute < 0.01
    assert relative_volume_hourly(volumes) == pytest.approx(1.0)

    metrics = compute_asset_metrics(_bars_with_volumes(volumes))
    assert metrics["rvol_24h"].value < 0.01
    assert metrics["rvol_1h"].value == pytest.approx(1.0)
    observed = metrics["rvol_1h"]
    assert (observed.computation_status, observed.reason_codes) == ("VALID", ())
    assert (observed.sample_count, observed.required_samples) == (1500, 1500)
    assert observed.window_start_ms == 1_800_000_000_000 - 1499 * 60_000


def test_rvol_1h_statuses_zero_baseline_warming_and_missing_input():
    zero = compute_asset_metrics(_bars_with_volumes([0.0] * 1440 + [5.0] * 60))["rvol_1h"]
    assert (zero.value, zero.computation_status, zero.reason_codes) == (None, "UNAVAILABLE", ("ZERO_BASELINE",))
    all_zero = compute_asset_metrics(_bars_with_volumes([0.0] * 1600))["rvol_1h"]
    assert all_zero.value is None and all_zero.reason_codes == ("ZERO_BASELINE",)

    warming = compute_asset_metrics(_bars_with_volumes([1.0] * 1499))["rvol_1h"]
    assert (warming.value, warming.computation_status, warming.reason_codes) == (None, "WARMING", ("INSUFFICIENT_HISTORY",))
    assert (warming.sample_count, warming.required_samples) == (1499, 1500)

    gappy = [1.0] * 1600
    gappy[-700] = None
    missing = compute_asset_metrics(_bars_with_volumes(gappy))["rvol_1h"]
    assert (missing.value, missing.computation_status, missing.reason_codes) == (None, "MISSING", ("INPUT_MISSING",))
    # A hole older than the 1,500-minute window does not invalidate the metric.
    old_hole = [1.0] * 1600
    old_hole[3] = None
    assert compute_asset_metrics(_bars_with_volumes(old_hole))["rvol_1h"].value == pytest.approx(1.0)


def test_correlation_min_pairs_follows_the_95_percent_rule_with_the_legacy_floor():
    assert {hours: correlation_min_pairs(hours) for hours in (2160, 720, 336, 168, 72, 24)} == {
        2160: 60,
        720: 60,
        336: 60,
        168: 60,
        72: 60,
        24: 23,
    }
    with pytest.raises(ValueError):
        correlation_min_pairs(100)
