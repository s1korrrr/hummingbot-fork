from __future__ import annotations

import math

import pytest

from hummingbot.strategy_v2.market_context.market_picture.anomalies import calculate_hourly_anomalies

HOUR_MS = 3_600_000
BASE = 1_800_000_000_000


def _inputs(count: int, *, quote_turnovers: list[str] | None = None):
    opens = tuple(BASE + index * HOUR_MS for index in range(count))
    closes = tuple("100" for _ in range(count))
    highs = tuple("101" for _ in range(count))
    lows = tuple("99" for _ in range(count))
    turnovers = tuple(quote_turnovers or ["10"] * count)
    return {
        "open_times_ms": opens,
        "bar_available_at_ms": tuple(opened + HOUR_MS for opened in opens),
        "source_epochs": tuple("epoch-a" for _ in opens),
        "highs": highs,
        "lows": lows,
        "closes": closes,
        "quote_turnovers": turnovers,
        "cutoff_ms": opens[-1] + HOUR_MS,
        "available_at_ms": opens[-1] + HOUR_MS + 1_000,
    }


def test_hourly_dictionary_rvol_uses_previous_median_not_mean_and_includes_threshold():
    turnover = ["10"] * 19 + ["100", "20"]
    result = calculate_hourly_anomalies(**_inputs(21, quote_turnovers=turnover))
    assert result.turnover_rvol_1h == 2
    assert result.high_rvol_1h is True
    assert result.turnover_rvol_reason is None


@pytest.mark.parametrize("baseline,current,expected", [
    (["1"] * 20, "1.99999999999999999", False),
    (["1"] * 20, "2", True),
    (["1"] * 10 + ["1.00000000000000000000000000001"] * 10,
     "2.000000000000000000000000000005", False),
])
def test_rvol_threshold_uses_exact_turnover_before_display_rounding(baseline, current, expected):
    result = calculate_hourly_anomalies(**_inputs(21, quote_turnovers=baseline + [current]))
    assert result.high_rvol_1h is expected


def test_hourly_dictionary_rvol_rejects_zero_baseline_and_marks_insufficient_history():
    zero = calculate_hourly_anomalies(**_inputs(21, quote_turnovers=["0"] * 21))
    assert zero.turnover_rvol_1h is None
    assert zero.high_rvol_1h is None
    assert zero.turnover_rvol_reason == "ZERO_BASELINE"

    short = calculate_hourly_anomalies(**_inputs(20))
    assert short.turnover_rvol_1h is None
    assert short.turnover_rvol_reason == "INSUFFICIENT_HISTORY"


def test_bollinger_bands_inside_ema_atr_is_strict_and_uses_complete_hourly_history():
    inside = calculate_hourly_anomalies(**_inputs(60))
    assert inside.compression_1h is True

    equal_envelopes = _inputs(60)
    equal_envelopes["highs"] = tuple("100" for _ in range(60))
    equal_envelopes["lows"] = tuple("100" for _ in range(60))
    equal = calculate_hourly_anomalies(**equal_envelopes)
    assert equal.compression_1h is False

    short = calculate_hourly_anomalies(**_inputs(59))
    assert short.compression_1h is None
    assert short.compression_reason == "INSUFFICIENT_HISTORY"


def test_extreme_realized_volatility_excludes_current_from_720_prior_observations():
    inputs = _inputs(745)
    close_values = ["100"] * 744 + [str(100 * math.exp(0.2))]
    inputs["closes"] = tuple(close_values)
    inputs["highs"] = tuple(["101"] * 744 + [close_values[-1]])
    result = calculate_hourly_anomalies(**inputs)
    assert result.realized_volatility_24h_hourly_annualized is not None
    assert result.extreme_realized_volatility_1h_95p is True
    assert result.extreme_volatility_reason is None

    short = calculate_hourly_anomalies(**_inputs(744))
    assert short.realized_volatility_24h_hourly_annualized == 0
    assert short.extreme_realized_volatility_1h_95p is None
    assert short.extreme_volatility_reason == "INSUFFICIENT_HISTORY"


@pytest.mark.parametrize(
    "mutation, message",
    [
        (lambda row: row["open_times_ms"].__setitem__(30, row["open_times_ms"][30] + 1), "UTC hour"),
        (lambda row: row["source_epochs"].__setitem__(30, "epoch-b"), "one source epoch"),
        (lambda row: row["bar_available_at_ms"].__setitem__(59, row["available_at_ms"] + 1), "decision availability"),
    ],
)
def test_gap_epoch_change_and_future_bar_are_rejected(mutation, message):
    inputs = _inputs(60)
    for key in ("open_times_ms", "source_epochs", "bar_available_at_ms"):
        inputs[key] = list(inputs[key])
    mutation(inputs)
    with pytest.raises(ValueError, match=message):
        calculate_hourly_anomalies(**inputs)


def test_zero_or_nonfinite_turnover_and_invalid_prices_are_rejected():
    inputs = _inputs(21)
    inputs["quote_turnovers"] = tuple(["10"] * 20 + ["-1"])
    with pytest.raises(ValueError, match="nonnegative"):
        calculate_hourly_anomalies(**inputs)
    inputs = _inputs(21)
    inputs["closes"] = tuple(["100"] * 20 + ["0"])
    with pytest.raises(ValueError, match="positive"):
        calculate_hourly_anomalies(**inputs)
