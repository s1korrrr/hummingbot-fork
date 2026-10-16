from __future__ import annotations

from decimal import Decimal

import pytest

from hummingbot.strategy_v2.market_context.market_picture.daily_internals import calculate_daily_internals

DAY_MS = 86_400_000


def _calculate(
    count: int,
    *,
    epochs: tuple[str, ...] | None = None,
    last_available_delta: int = 0,
    decision_delta: int = 0,
):
    cutoff = 1_800_000_000_000 // DAY_MS * DAY_MS
    first_open = cutoff - count * DAY_MS
    closes = tuple(Decimal(index + 1) for index in range(count))
    return calculate_daily_internals(
        open_times_ms=tuple(first_open + index * DAY_MS for index in range(count)),
        bar_available_at_ms=tuple(first_open + (index + 1) * DAY_MS for index in range(count - 1))
        + ((cutoff + last_available_delta),)
        if count
        else (),
        source_epochs=epochs or tuple("epoch-1" for _ in range(count)),
        highs=tuple(value + Decimal("0.5") for value in closes),
        lows=tuple(value - Decimal("0.5") for value in closes),
        closes=closes,
        cutoff_ms=cutoff,
        available_at_ms=cutoff + decision_delta,
        expected_daily_bars=401,
    )


def test_daily_internals_use_prior_20_completed_days_and_strict_close_breaks():
    result = _calculate(201)
    assert result.cutoff_ms % DAY_MS == 0
    assert result.high_break_20d is True
    assert result.low_break_20d is False
    assert result.close_break_high_20d is True
    assert result.close_break_low_20d is False


def test_daily_high_touch_is_separate_from_close_breakout():
    cutoff = 1_800_000_000_000 // DAY_MS * DAY_MS
    count = 21
    first_open = cutoff - count * DAY_MS
    closes = [Decimal("10")] * 20 + [Decimal("10.5")]
    highs = [Decimal("11")] * 20 + [Decimal("11.5")]
    lows = [Decimal("9")] * count
    result = calculate_daily_internals(
        open_times_ms=tuple(first_open + index * DAY_MS for index in range(count)),
        bar_available_at_ms=tuple(first_open + (index + 1) * DAY_MS for index in range(count)),
        source_epochs=tuple("epoch-1" for _ in range(count)),
        highs=tuple(highs),
        lows=tuple(lows),
        closes=tuple(closes),
        cutoff_ms=cutoff,
        available_at_ms=cutoff,
        expected_daily_bars=401,
    )
    assert result.high_break_20d is True
    assert result.close_break_high_20d is False


def test_t07_daily_sma200_needs_200_daily_bars_and_slope_needs_201():
    at_199 = _calculate(199).moving_averages[2]
    at_200 = _calculate(200).moving_averages[2]
    at_201 = _calculate(201).moving_averages[2]
    assert at_199.sma is None
    assert at_200.sma is not None and at_200.above_close is True
    assert at_200.slope_percent_per_day is None and at_200.cross is None
    assert at_201.sma is not None and at_201.slope_percent_per_day is not None
    assert at_201.cross == "none"


def test_each_sma_requires_one_contiguous_source_epoch_for_its_own_window():
    epochs = ["prior"] + ["current"] * 200
    result = _calculate(201, epochs=tuple(epochs))
    assert result.moving_averages[2].sma is not None
    assert result.moving_averages[2].slope_percent_per_day is None
    assert result.moving_averages[2].cross is None


@pytest.mark.parametrize(
    ("previous_close", "current_close", "expected_cross"),
    (("10", "12", "cross_up"), ("10", "8", "cross_down")),
)
def test_daily_sma_cross_uses_prior_and_current_close_relative_to_each_sma(
    previous_close: str, current_close: str, expected_cross: str
):
    cutoff = 1_800_000_000_000 // DAY_MS * DAY_MS
    count = 21
    first_open = cutoff - count * DAY_MS
    closes = [Decimal("10")] * 20 + [Decimal(current_close)]
    closes[-2] = Decimal(previous_close)
    result = calculate_daily_internals(
        open_times_ms=tuple(first_open + index * DAY_MS for index in range(count)),
        bar_available_at_ms=tuple(first_open + (index + 1) * DAY_MS for index in range(count)),
        source_epochs=tuple("epoch-1" for _ in range(count)),
        highs=tuple(value + Decimal("0.5") for value in closes),
        lows=tuple(value - Decimal("0.5") for value in closes),
        closes=tuple(closes),
        cutoff_ms=cutoff,
        available_at_ms=cutoff,
        expected_daily_bars=401,
    )
    assert result.moving_averages[0].cross == expected_cross


def test_daily_bar_availability_after_decision_time_is_rejected():
    with pytest.raises(ValueError, match="unavailable at the decision time"):
        _calculate(201, last_available_delta=1, decision_delta=0)


def test_daily_bar_gap_is_rejected_instead_of_bridged():
    count = 201
    cutoff = 1_800_000_000_000 // DAY_MS * DAY_MS
    first_open = cutoff - count * DAY_MS
    open_times = [first_open + index * DAY_MS for index in range(count)]
    open_times[-2] -= DAY_MS
    closes = [Decimal(index + 1) for index in range(count)]
    with pytest.raises(ValueError, match="contiguous"):
        calculate_daily_internals(
            open_times_ms=tuple(open_times),
            bar_available_at_ms=tuple(open_ms + DAY_MS for open_ms in open_times),
            source_epochs=tuple("epoch-1" for _ in range(count)),
            highs=tuple(value + Decimal("0.5") for value in closes),
            lows=tuple(value - Decimal("0.5") for value in closes),
            closes=tuple(closes),
            cutoff_ms=cutoff,
            available_at_ms=cutoff,
            expected_daily_bars=401,
        )
