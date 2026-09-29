import time
from decimal import Decimal

import numpy as np
import pytest

from hummingbot.strategy_v2.market_context.market_picture import incremental as incremental_module
from hummingbot.strategy_v2.market_context.market_picture.contracts import MetricCellV1
from hummingbot.strategy_v2.market_context.market_picture.incremental import (
    IncrementalAssetMetricState,
    MetricStateResetRequired,
)
from hummingbot.strategy_v2.market_context.market_picture.metrics import (
    AssetBarsV1,
    compute_asset_metrics,
    ema,
    true_range,
    wilder_adx,
    wilder_atr,
    wilder_rsi,
)


def _python_ema(values, length):
    output = np.full(len(values), np.nan)
    if not len(values):
        return output
    alpha = 2.0 / (length + 1.0)
    output[0] = values[0]
    for index in range(1, len(values)):
        output[index] = alpha * values[index] + (1.0 - alpha) * output[index - 1]
    return output


def _python_smooth(values, length):
    output = np.full(len(values), np.nan)
    if len(values) < length:
        return output
    output[length - 1] = float(np.mean(values[:length]))
    for index in range(length, len(values)):
        output[index] = (output[index - 1] * (length - 1) + values[index]) / length
    return output


def _python_rsi(close, length=14):
    output = np.full(len(close), np.nan)
    changes = np.diff(close)
    if len(changes) <= length:
        return output
    gains, losses = np.maximum(changes, 0.0), np.maximum(-changes, 0.0)
    gain, loss = float(np.mean(gains[:length])), float(np.mean(losses[:length]))
    for index in range(length, len(changes)):
        gain = (gain * (length - 1) + gains[index]) / length
        loss = (loss * (length - 1) + losses[index]) / length
        output[index + 1] = 50.0 if gain == loss == 0 else 100.0 if loss == 0 else 100.0 - 100.0 / (1.0 + gain / loss)
    return output


def _python_adx(high, low, close, length=14):
    count = len(close)
    true_ranges = np.empty(count)
    if count:
        true_ranges[0] = high[0] - low[0]
    if count > 1:
        true_ranges[1:] = np.maximum(
            high[1:] - low[1:], np.maximum(np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1]))
        )
    plus_dm, minus_dm = np.zeros(count), np.zeros(count)
    if count > 1:
        up, down = np.diff(high), -np.diff(low)
        plus_dm[1:] = np.where((up > down) & (up > 0), up, 0.0)
        minus_dm[1:] = np.where((down > up) & (down > 0), down, 0.0)
    atr = _python_smooth(true_ranges, length)
    plus_smoothed, minus_smoothed = _python_smooth(plus_dm, length), _python_smooth(minus_dm, length)
    plus_di, minus_di, dx = np.full(count, np.nan), np.full(count, np.nan), np.full(count, np.nan)
    for index in range(count):
        if not np.isfinite(atr[index]) or atr[index] <= 0:
            continue
        plus_di[index], minus_di[index] = 100.0 * plus_smoothed[index] / atr[index], 100.0 * minus_smoothed[index] / atr[index]
        denominator = plus_di[index] + minus_di[index]
        if np.isfinite(denominator):
            dx[index] = 0.0 if denominator == 0.0 else 100.0 * abs(plus_di[index] - minus_di[index]) / denominator
    adx = np.full(count, np.nan)
    valid = np.flatnonzero(np.isfinite(dx))
    if len(valid) >= length:
        first = valid[length - 1]
        if np.array_equal(valid[:length], np.arange(first - length + 1, first + 1)):
            adx_start = first - length + 1
            adx[first] = float(np.mean(dx[adx_start:first + 1]))
            for index in range(first + 1, count):
                if np.isfinite(dx[index]):
                    adx[index] = (adx[index - 1] * (length - 1) + dx[index]) / length
    return adx, plus_di, minus_di


def _python_atr_percent(high, low, close, length=14):
    return 100.0 * _python_smooth(true_range(high, low, close), length) / close


def _bars(instrument_id="okx:spot:BTC-USDC", *, n=80, start=1_800_000_000_000, mode="walk"):
    index = np.arange(n, dtype=np.float64)
    if mode == "flat":
        closes = np.full(n, 100.0)
    elif mode == "extreme":
        closes = np.geomspace(1e150, 1e-150, n)
    else:
        closes = 100.0 + 0.001 * index + 0.15 * np.sin(index / 7.0)
    highs = closes * 1.001
    lows = closes * 0.999
    exact_close = tuple(Decimal(str(value)) for value in closes)
    times = tuple(start - (n - row - 1) * 60_000 for row in range(n))
    return AssetBarsV1(
        instrument_id=instrument_id,
        close_times_ms=times,
        high=tuple(Decimal(str(value)) for value in highs),
        low=tuple(Decimal(str(value)) for value in lows),
        close=exact_close,
        quote_volume=tuple(None if row % 997 == 12 else float(3 + row % 11) for row in range(n)),
        cutoff_ms=start,
    )


def _reference(state):
    return AssetBarsV1(
        instrument_id=state.instrument_id,
        close_times_ms=tuple(int(value) for value in state._times),
        high=tuple(Decimal(str(value)) for value in state._high),
        low=tuple(Decimal(str(value)) for value in state._low),
        close=tuple(state._exact_close),
        quote_volume=tuple(None if np.isnan(value) else float(value) for value in state._quote_volume),
        cutoff_ms=int(state._times[-1]),
    )


def _assert_batch_equal(actual, expected):
    assert set(actual) == set(expected)
    for metric_id, got in actual.items():
        want = expected[metric_id]
        assert (got.computation_status, got.reason_codes, got.sample_count, got.required_samples) == (
            want.computation_status,
            want.reason_codes,
            want.sample_count,
            want.required_samples,
        )
        assert (got.window_start_ms, got.window_end_ms, got.observation_interval_ms) == (
            want.window_start_ms,
            want.window_end_ms,
            want.observation_interval_ms,
        )
        if isinstance(want.value, Decimal):
            assert got.value == want.value
        elif want.value is None:
            assert got.value is None
        else:
            assert got.value == pytest.approx(want.value, rel=1e-13, abs=1e-13)


@pytest.mark.parametrize("mode", ["walk", "flat", "extreme"])
def test_incremental_snapshots_match_batch_oracle_after_each_append(mode):
    initial = _bars(n=45, mode=mode)
    state = IncrementalAssetMetricState(initial.instrument_id, capacity=64)
    state.reset(initial, benchmark_returns_24h={"btc": 0.0, "eth": 1.0})
    for offset in range(12):
        previous = state.last_close_ms
        base = float(state._close[-1])
        close = base * (1.0 + (0.0002 if offset % 2 == 0 else -0.0001))
        close_text = format(close, ".17g")
        state.upsert(
            previous + 60_000,
            high=Decimal(close_text) * Decimal("1.001"),
            low=Decimal(close_text) * Decimal("0.999"),
            close=close_text,
            quote_volume=4.0 + offset,
        )
        _assert_batch_equal(state.snapshot(), compute_asset_metrics(_reference(state), benchmark_returns_24h={"btc": 0.0, "eth": 1.0}))


def test_incremental_observations_build_strict_owner_metric_cells():
    bars = _bars(n=384)
    state = IncrementalAssetMetricState.from_history(bars)
    observations = state.snapshot()
    cells = []
    for observation in observations.values():
        assert type(observation.window_end_ms) is int
        assert observation.window_start_ms is None or type(observation.window_start_ms) is int
        assert type(observation.sample_count) is int
        assert type(observation.required_samples) is int
        cells.append(
            MetricCellV1(
                instrument_id=state.instrument_id,
                value=str(observation.value) if observation.value is not None else None,
                computation_status=observation.computation_status,
                reason_codes=observation.reason_codes,
                window_start_ms=observation.window_start_ms,
                window_end_ms=observation.window_end_ms,
                observation_interval_ms=observation.observation_interval_ms,
                sample_count=observation.sample_count,
                required_samples=observation.required_samples,
            )
        )
    assert len(cells) == len(observations)


@pytest.mark.parametrize("mode", ["walk", "flat", "extreme"])
def test_compiled_indicator_kernels_match_original_python_recurrence(mode):
    bars = _bars(n=384, mode=mode)
    close = bars._close_vector
    high, low = bars._high_vector, bars._low_vector
    fast_ema, ref_ema = ema(close, 21), _python_ema(close, 21)
    fast_rsi, ref_rsi = wilder_rsi(close, 14), _python_rsi(close, 14)
    fast_adx = wilder_adx(high, low, close, 14)
    ref_adx = _python_adx(high, low, close, 14)
    np.testing.assert_array_equal(fast_ema, ref_ema)
    np.testing.assert_array_equal(fast_rsi, ref_rsi)
    for got, expected in zip(fast_adx, ref_adx):
        np.testing.assert_array_equal(got, expected)
    assert np.array_equal(close > fast_ema, close > ref_ema)
    for threshold in (30.0, 50.0, 70.0):
        assert np.array_equal(fast_rsi > threshold, ref_rsi > threshold)
    assert np.array_equal(fast_adx[0] > 25.0, ref_adx[0] > 25.0)
    ref_atr = _python_atr_percent(high, low, close)
    # ATR percentile predicates use the current ATR against the prior 200 finite values.
    fast_atr_percent = 100.0 * wilder_atr(high, low, close, 14) / close
    np.testing.assert_array_equal(fast_atr_percent, ref_atr)
    finite = np.flatnonzero(np.isfinite(fast_atr_percent))
    if len(finite) >= 201:
        history = finite[-201:-1]
        current = finite[-1]
        fast_rank = 100 * (
            np.count_nonzero(fast_atr_percent[history] < fast_atr_percent[current])
            + 0.5 * np.count_nonzero(fast_atr_percent[history] == fast_atr_percent[current])
        ) / 200
        reference_rank = 100 * (
            np.count_nonzero(ref_atr[history] < ref_atr[current])
            + 0.5 * np.count_nonzero(ref_atr[history] == ref_atr[current])
        ) / 200
        assert (fast_rank <= 20.0) == (reference_rank <= 20.0)


def test_full_source_window_shift_retains_exact_seed_and_threshold_parity():
    initial = _bars(n=11_521)
    state = IncrementalAssetMetricState(initial.instrument_id, capacity=11_521)
    state.reset(initial)
    for offset in range(3):
        close = Decimal(str(100.0 + offset * 0.01))
        state.upsert(
            state.last_close_ms + 60_000,
            high=close + Decimal("0.1"),
            low=close - Decimal("0.1"),
            close=close,
            quote_volume=10.0,
        )
        expected = compute_asset_metrics(_reference(state))
        actual = state.snapshot()
        _assert_batch_equal(actual, expected)
        for key, threshold in (("ema21", 100.0), ("rsi14", 50.0), ("rsi14", 70.0), ("rsi14", 30.0), ("adx14", 25.0), ("atr14_percentile", 20.0)):
            if actual[key].value is not None:
                assert (actual[key].value > threshold) == (expected[key].value > threshold)


def test_correction_rebuild_gap_and_expired_correction_requirements():
    bars = _bars(n=32)
    state = IncrementalAssetMetricState(bars.instrument_id, capacity=32)
    state.reset(bars)
    corrected_at = int(state._times[11])
    correction = Decimal(str(float(state._close[11]) * 1.01))
    state.upsert(
        corrected_at,
        high=correction * Decimal("1.001"),
        low=correction * Decimal("0.999"),
        close=correction,
        quote_volume=7.0,
    )
    _assert_batch_equal(state.snapshot(), compute_asset_metrics(_reference(state)))
    before = state.snapshot()
    with pytest.raises(MetricStateResetRequired, match="gapped"):
        state.upsert(state.last_close_ms + 120_000, high=101.0, low=100.0, close=100.5, quote_volume=1.0)
    assert state.snapshot() == before
    with pytest.raises(MetricStateResetRequired, match="outside"):
        state.upsert(state.first_close_ms - 60_000, high=101.0, low=100.0, close=100.5, quote_volume=1.0)


def test_delta_rejects_prices_that_underflow_or_overflow_float64():
    bars = _bars(n=8)
    state = IncrementalAssetMetricState.from_history(bars)
    timestamp = state.last_close_ms + 60_000
    with pytest.raises(ValueError, match="finite positive prices"):
        state.upsert(timestamp, high=Decimal("1e-1000"), low=Decimal("1e-1000"), close=Decimal("1e-1000"), quote_volume=1)
    with pytest.raises(ValueError, match="finite positive prices"):
        state.upsert(timestamp, high=Decimal("1e1000"), low=Decimal("1e1000"), close=Decimal("1e1000"), quote_volume=1)
    assert state.last_close_ms == bars.cutoff_ms


def test_retain_window_and_accessors_are_bounded_and_read_only():
    bars = _bars(n=64)
    state = IncrementalAssetMetricState(bars.instrument_id, capacity=64)
    state.reset(bars)
    first, last = state.first_close_ms, state.last_close_ms
    state.retain_window(first + 5 * 60_000, last - 2 * 60_000)
    assert state.row_count == 57
    highs, lows = state.high_low_window(100)
    assert len(highs) == len(lows) == 57
    with pytest.raises(ValueError):
        highs[-1] = 0
    assert state.return_close_pair(5) == (state._exact_close[-6], state._exact_close[-1])
    assert state.nbytes >= state._times.nbytes + state._high.nbytes + state._low.nbytes + state._close.nbytes
    with pytest.raises(MetricStateResetRequired, match="extends"):
        state.retain_window(first, last)


def test_multiple_delta_upserts_compute_one_snapshot(monkeypatch):
    bars = _bars(n=32)
    state = IncrementalAssetMetricState.from_history(bars)
    original = incremental_module.compute_asset_metrics
    calls = []

    def count_compute(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(incremental_module, "compute_asset_metrics", count_compute)
    for offset in range(3):
        close = Decimal(str(101 + offset))
        state.upsert(
            state.last_close_ms + 60_000,
            high=close + Decimal("0.1"),
            low=close - Decimal("0.1"),
            close=close,
            quote_volume=10.0,
        )
    assert calls == []
    state.snapshot()
    assert len(calls) == 1


def test_gap_recovery_replaces_epoch_window_from_validated_reset():
    bars = _bars(n=32)
    state = IncrementalAssetMetricState.from_history(bars)
    next_close = state.last_close_ms + 120_000
    with pytest.raises(MetricStateResetRequired):
        state.upsert(next_close, high=102.0, low=101.0, close=101.5, quote_volume=1.0)
    replacement = _bars(n=40, start=bars.cutoff_ms + 600_000, mode="flat")
    state.reset(replacement)
    assert state.row_count == 40
    assert state.first_close_ms == replacement.close_times_ms[0]
    _assert_batch_equal(state.snapshot(), compute_asset_metrics(replacement))


def test_300_member_warm_delta_metric_refresh_under_one_second():
    bars = _bars(n=11_521)
    states = [IncrementalAssetMetricState(f"okx:spot:ASSET{index:03}-USDC") for index in range(300)]
    # A separately measured cold full-history seed runs before the minute cadence.
    for state in states:
        member_bars = AssetBarsV1(
            instrument_id=state.instrument_id,
            close_times_ms=bars.close_times_ms,
            high=bars.high,
            low=bars.low,
            close=bars.close,
            quote_volume=bars.quote_volume,
            cutoff_ms=bars.cutoff_ms,
        )
        state.reset(member_bars)
    started = time.perf_counter()
    for state in states:
        close = Decimal("111.6")
        state.upsert(
            state.last_close_ms + 60_000,
            high=close + Decimal("0.1"),
            low=close - Decimal("0.1"),
            close=close,
            quote_volume=10.0,
        )
    for state in states:
        state.snapshot()
    elapsed = time.perf_counter() - started
    assert elapsed < 1.0, f"warmed 300-member metric refresh took {elapsed:.3f}s"
