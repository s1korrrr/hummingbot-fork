from decimal import Decimal

import pytest

from hummingbot.strategy_v2.market_context.market_picture.hourly_internals import calculate_hourly_internals


def _hourly(close_values, *, first_open_ms=0, available_at_ms=None, source_epoch="epoch-1"):
    interval = 3_600_000
    count = len(close_values)
    opens = tuple(first_open_ms + index * interval for index in range(count))
    closes_at = tuple(opened + interval for opened in opens)
    return calculate_hourly_internals(
        open_times_ms=opens,
        bar_available_at_ms=closes_at,
        source_epochs=(source_epoch,) * count,
        closes=tuple(Decimal(value) for value in close_values),
        cutoff_ms=closes_at[-1] if count else 0,
        available_at_ms=available_at_ms if available_at_ms is not None else (closes_at[-1] if count else 0),
    )


def test_hourly_internals_calculate_moving_averages_trend_and_efficiency_from_closed_bars():
    result = _hourly(range(1, 61))

    sma20 = result.moving_averages[20]
    sma50 = result.moving_averages[50]
    assert sma20.sma == Decimal("50.5")
    assert sma20.above_close is True
    assert sma20.slope_percent_per_hour == Decimal(100) * (Decimal("50.5") / Decimal("49.5") - 1)
    assert sma20.cross == "none"
    assert sma50.sma == Decimal("35.5")
    assert sma50.above_close is True
    assert result.moving_averages[200].sma is None
    assert result.trend_class == "bullish"
    assert result.efficiency_ratio_20 == Decimal(1)


def test_hourly_internals_emit_strict_crosses_and_flat_efficiency_only_with_complete_path():
    crossing_up = _hourly([100] * 20 + [103])
    crossing_down = _hourly([100] * 20 + [97])
    flat = _hourly([100] * 21)
    incomplete = _hourly([100] * 20)

    assert crossing_up.moving_averages[20].cross == "cross_up"
    assert crossing_down.moving_averages[20].cross == "cross_down"
    assert flat.efficiency_ratio_20 == Decimal(0)
    assert incomplete.moving_averages[20].sma == Decimal(100)
    assert incomplete.moving_averages[20].slope_percent_per_hour is None
    assert incomplete.moving_averages[20].cross is None
    assert incomplete.efficiency_ratio_20 is None


def test_hourly_internals_reject_future_bars_gaps_and_source_epoch_bridges():
    interval = 3_600_000
    opens = tuple(index * interval for index in range(21))
    closes_at = tuple(opened + interval for opened in opens)
    values = tuple(Decimal(100 + index) for index in range(21))
    common = {
        "open_times_ms": opens,
        "bar_available_at_ms": closes_at,
        "source_epochs": ("epoch-1",) * len(opens),
        "closes": values,
        "cutoff_ms": closes_at[-1],
        "available_at_ms": closes_at[-1],
    }

    future = dict(common, available_at_ms=closes_at[-1] - 1)
    with pytest.raises(ValueError, match="confirmed"):
        calculate_hourly_internals(**future)

    gap = dict(common, open_times_ms=(*opens[:5], opens[5] + interval, *opens[6:]))
    with pytest.raises(ValueError, match="contiguous"):
        calculate_hourly_internals(**gap)

    epoch_bridge = dict(common, source_epochs=("epoch-1",) * 20 + ("epoch-2",))
    with pytest.raises(ValueError, match="one nonempty source epoch"):
        calculate_hourly_internals(**epoch_bridge)
