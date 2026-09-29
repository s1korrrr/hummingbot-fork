"""Pure Market Picture calculations over already aligned, closed observations."""

import math
import re
from dataclasses import dataclass, field
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from typing import Callable, Mapping, Sequence

import numpy as np
from numba import njit

HORIZONS_MINUTES = (1, 5, 15, 60, 240, 1440)
ANNUAL_MINUTES = 365 * 24 * 60


@dataclass(frozen=True)
class BreadthValue:
    horizon_minutes: int
    cutoff_ms: int
    advances: int
    declines: int
    unchanged: int
    valid_count: int
    expected_count: int
    omitted_count: int
    advance_share: float | None
    decline_share: float | None
    unchanged_share: float | None
    mean_return_percent: float | None
    median_return_percent: float | None
    dispersion_percent: float | None


@dataclass(frozen=True)
class PredicateCount:
    numerator: int
    denominator: int
    expected_denominator: int
    share: float | None


@dataclass(frozen=True)
class Histogram:
    edges: tuple[float, ...]
    counts: tuple[int, ...]
    underflow_count: int
    overflow_count: int
    valid_count: int
    zero_count: int


@dataclass(frozen=True)
class CorrelationValue:
    value: float | None
    sample_count: int
    expected_count: int
    coverage_fraction: float | None


@dataclass(frozen=True)
class IndicatorSnapshot:
    ema21: float | None
    rsi14: float | None
    atr14: float | None
    atr14_percent: float | None
    adx14: float | None
    plus_di14: float | None
    minus_di14: float | None


@dataclass(frozen=True)
class AssetBarsV1:
    """One exact instrument's already-qualified contiguous closed 1m bars, newest close at cutoff."""

    instrument_id: str
    close_times_ms: tuple[int, ...]
    high: tuple[Decimal | str | float | int, ...]
    low: tuple[Decimal | str | float | int, ...]
    close: tuple[Decimal | str | float | int, ...]
    quote_volume: tuple[float | None, ...]
    cutoff_ms: int
    _high_vector: np.ndarray = field(init=False, repr=False, compare=False)
    _low_vector: np.ndarray = field(init=False, repr=False, compare=False)
    _close_vector: np.ndarray = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        from .contracts import INSTRUMENT_PATTERN, MAX_TIMESTAMP_MS

        if (
            not isinstance(self.instrument_id, str)
            or re.fullmatch(INSTRUMENT_PATTERN, self.instrument_id) is None
            or type(self.cutoff_ms) is not int
            or not 0 <= self.cutoff_ms <= MAX_TIMESTAMP_MS
            or self.cutoff_ms % 60_000
        ):
            raise ValueError(
                "asset bars require exact instrument identity and a UTC minute cutoff"
            )
        n = len(self.close_times_ms)
        if not 1 <= n <= 20_161 or not (
            len(self.high)
            == len(self.low)
            == len(self.close)
            == len(self.quote_volume)
            == n
        ):
            raise ValueError("asset OHLCV columns must have one bounded common length")
        if any(type(value) is not int for value in self.close_times_ms):
            raise ValueError("bar close times must be integer UTC milliseconds")
        if (
            self.close_times_ms[0] < 0
            or self.close_times_ms[-1] != self.cutoff_ms
            or any(
                right - left != 60_000
                for left, right in zip(self.close_times_ms, self.close_times_ms[1:])
            )
        ):
            raise ValueError(
                "asset bars must be contiguous and end exactly at the frame cutoff"
            )
        highs, lows, closes = (
            _price_vector(values, name=name)
            for values, name in (
                (self.high, "high"),
                (self.low, "low"),
                (self.close, "close"),
            )
        )
        if (
            (highs <= 0).any()
            or (lows <= 0).any()
            or (closes <= 0).any()
            or (lows > closes).any()
            or (closes > highs).any()
        ):
            raise ValueError(
                "asset OHLC prices must be positive with low <= close <= high"
            )
        for value in self.quote_volume:
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(
                    "quote volume must be a finite nonnegative number or null"
                )
        for name, vector in (
            ("_high_vector", highs),
            ("_low_vector", lows),
            ("_close_vector", closes),
        ):
            vector.flags.writeable = False
            object.__setattr__(self, name, vector)


@dataclass(frozen=True)
class AssetMetricObservation:
    metric_id: str
    value: float | Decimal | None
    computation_status: str
    reason_codes: tuple[str, ...]
    sample_count: int
    required_samples: int
    window_start_ms: int | None
    window_end_ms: int
    observation_interval_ms: int


@dataclass(frozen=True)
class DistributionSummary:
    valid_count: int
    expected_count: int
    mean_return_percent: float | None
    median_return_percent: float | None
    dispersion_percent: float | None
    negative_count: int
    unchanged_count: int
    positive_count: int
    downside_magnitude_percent: float | None
    best_instrument_id: str | None
    worst_instrument_id: str | None


def _finite_vector(values: Sequence[float], *, name: str) -> np.ndarray:
    source = np.asarray(values)
    if source.dtype.kind not in "iuf":
        raise ValueError(f"{name} must contain numeric, non-boolean values")
    array = source.astype(float, copy=False)
    if array.ndim != 1 or not np.isfinite(array).all():
        raise ValueError(f"{name} must be a finite one-dimensional sequence")
    return array


def _price_vector(
    values: Sequence[Decimal | str | float | int], *, name: str
) -> np.ndarray:
    """Validate exact decimal-compatible prices, then project to float for indicator math."""
    converted = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (Decimal, str, float, int)):
            raise ValueError(
                f"{name} must contain decimal-compatible non-boolean prices"
            )
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"{name} must contain finite positive prices")
        try:
            exact = value if isinstance(value, Decimal) else Decimal(str(value))
            numeric = float(exact)
        except (ValueError, TypeError, OverflowError) as error:
            raise ValueError(f"{name} must contain finite positive prices") from error
        if not exact.is_finite() or not math.isfinite(numeric):
            raise ValueError(f"{name} must contain finite positive prices")
        converted.append(numeric)
    array = np.asarray(converted, dtype=float)
    if array.ndim != 1:
        raise ValueError(f"{name} must be a one-dimensional sequence")
    return array


def return_percent_decimal(
    closes: Sequence[Decimal | str | float | int], horizon_minutes: int
) -> Decimal | None:
    """Exact source-price return; Decimal sign survives float display underflow."""
    if type(horizon_minutes) is not int or horizon_minutes not in (
        *HORIZONS_MINUTES,
        10_080,
    ):
        raise ValueError(
            "horizon must be a registered interval or exact seven-day ranking horizon"
        )
    if len(closes) != horizon_minutes + 1:
        return None
    values = []
    for value in closes:
        if isinstance(value, bool) or not isinstance(value, (Decimal, str, float, int)):
            raise ValueError(
                "closes must contain exact decimal-compatible positive prices"
            )
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("closes must contain finite prices")
        try:
            decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
        except Exception as error:
            raise ValueError("closes must contain decimal-compatible prices") from error
        if not decimal_value.is_finite():
            raise ValueError("closes must contain finite prices")
        if decimal_value <= 0:
            return None
        values.append(decimal_value)
    with localcontext() as context:
        context.prec = 80
        return (values[-1] / values[0] - Decimal(1)) * Decimal(100)


def _return_percent_from_validated_endpoints(first, last) -> Decimal | None:
    """Exact return from endpoints already validated as positive AssetBars closes."""
    if isinstance(first, Decimal):
        first_decimal = first
    else:
        first_decimal = Decimal(str(first))
    if isinstance(last, Decimal):
        last_decimal = last
    else:
        last_decimal = Decimal(str(last))
    if (
        not first_decimal.is_finite()
        or not last_decimal.is_finite()
        or first_decimal <= 0
        or last_decimal <= 0
    ):
        return None
    with localcontext() as context:
        context.prec = 80
        return (last_decimal / first_decimal - Decimal(1)) * Decimal(100)


def return_percent(
    closes: Sequence[Decimal | str | float | int], horizon_minutes: int
) -> float | None:
    """Simple return at the exact horizon, projected to float for math/display kernels."""
    value = return_percent_decimal(closes, horizon_minutes)
    return float(value) if value is not None else None


def breadth(
    values: Mapping[str, Decimal | float | int | None],
    expected_count: int,
    horizon_minutes: int,
    cutoff_ms: int,
) -> BreadthValue:
    """Count signs before display rounding; null and absent members are omitted, not unchanged."""
    if (
        type(expected_count) is not int
        or expected_count < len(values)
        or expected_count < 0
    ):
        raise ValueError("expected_count must cover the supplied unique members")
    if type(horizon_minutes) is not int or horizon_minutes not in HORIZONS_MINUTES:
        raise ValueError("unsupported breadth horizon")
    if type(cutoff_ms) is not int or cutoff_ms < 0 or cutoff_ms % 60_000:
        raise ValueError("cutoff must be a nonnegative UTC minute boundary")
    valid = []
    signs = []
    for value in values.values():
        if value is None:
            continue
        if (
            isinstance(value, bool)
            or not isinstance(value, (Decimal, int, float))
            or not math.isfinite(value)
        ):
            raise ValueError("breadth values must be finite numbers or null")
        signs.append((value > 0, value < 0))
        valid.append(float(value))
    advances = sum(positive for positive, _ in signs)
    declines = sum(negative for _, negative in signs)
    unchanged = len(signs) - advances - declines
    n = len(valid)

    def share(count):
        return count / n if n else None

    return BreadthValue(
        horizon_minutes,
        cutoff_ms,
        advances,
        declines,
        unchanged,
        n,
        expected_count,
        expected_count - n,
        share(advances),
        share(declines),
        share(unchanged),
        float(np.mean(valid)) if n else None,
        float(np.median(valid)) if n else None,
        float(np.std(valid, ddof=0)) if n else None,
    )


def breadth_pressure(value: BreadthValue) -> float | None:
    """Descriptive net breadth pressure in [-3,3], never aggressor or order-book flow."""
    if (
        value.valid_count < 0
        or value.advances < 0
        or value.declines < 0
        or value.unchanged < 0
        or value.advances + value.declines + value.unchanged != value.valid_count
    ):
        raise ValueError("breadth counts are inconsistent")
    if value.valid_count == 0:
        return None
    return 3.0 * (value.advances - value.declines) / value.valid_count


def breadth_pressure_decimal(value: BreadthValue) -> str | None:
    """Canonical pressure wire number: Decimal precision 28, ROUND_HALF_EVEN, fixed notation."""
    if (
        value.valid_count < 0
        or value.advances < 0
        or value.declines < 0
        or value.unchanged < 0
        or value.advances + value.declines + value.unchanged != value.valid_count
    ):
        raise ValueError("breadth counts are inconsistent")
    if value.valid_count == 0:
        return None
    with localcontext() as context:
        context.prec = 28
        context.rounding = ROUND_HALF_EVEN
        pressure = Decimal(3 * (value.advances - value.declines)) / Decimal(
            value.valid_count
        )
    rendered = format(pressure, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def metric_decimal(value: float | int | Decimal | None) -> str | None:
    """Serialize one finite computed number as bounded fixed decimal text, without NaN/Infinity/exponents."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (float, int, Decimal)):
        raise ValueError("metric value must be numeric or null")
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    if not decimal_value.is_finite():
        raise ValueError("metric value must be finite")
    result = format(decimal_value, "f")
    if "." in result:
        result = result.rstrip("0").rstrip(".")
    if not result or result == "-0":
        result = "0"
    if len(result) > 128:
        raise ValueError("metric value exceeds the wire decimal bound")
    return result


def classify_pressure(value: float | None) -> str | None:
    """Named display bands from the product spec; exact boundaries are intentionally asymmetric."""
    if value is None:
        return None
    if isinstance(value, bool) or not math.isfinite(value) or not -3 <= value <= 3:
        raise ValueError("pressure must be finite and in [-3,3]")
    if value < -1.5:
        return "extreme_bear"
    if value < -0.5:
        return "bear"
    if value <= 0.5:
        return "neutral"
    if value <= 1.5:
        return "bull"
    return "extreme_bull"


def count_predicate(
    values: Mapping[str, float | None],
    predicate: Callable[[float], bool],
    expected_count: int,
) -> PredicateCount:
    if type(expected_count) is not int or expected_count < len(values):
        raise ValueError("expected_count must cover the supplied unique members")
    valid = []
    for value in values.values():
        if value is None:
            continue
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise ValueError("predicate values must be finite numbers or null")
        valid.append(float(value))
    numerator = sum(bool(predicate(value)) for value in valid)
    denominator = len(valid)
    return PredicateCount(
        numerator,
        denominator,
        expected_count,
        numerator / denominator if denominator else None,
    )


def histogram(
    values: Sequence[float], span_percent: float, interior_bins: int = 40
) -> Histogram:
    """Fixed symmetric histogram with explicit under/overflow; last upper edge is included."""
    if (
        not math.isfinite(span_percent)
        or span_percent <= 0
        or type(interior_bins) is not int
        or interior_bins < 1
    ):
        raise ValueError("histogram span and bin count must be positive")
    data = _finite_vector(values, name="values")
    interior_edges = np.linspace(
        -span_percent, span_percent, interior_bins + 1, dtype=float
    )
    interior_counts, edges = np.histogram(data, bins=interior_edges)
    underflow = int(np.count_nonzero(data < -span_percent))
    overflow = int(np.count_nonzero(data > span_percent))
    all_counts = (underflow, *map(int, interior_counts), overflow)
    if sum(all_counts) != len(data):
        raise AssertionError("histogram bins must conserve every observation")
    return Histogram(
        tuple(float(x) for x in edges),
        tuple(all_counts),
        underflow,
        overflow,
        len(data),
        int(np.count_nonzero(data == 0.0)),
    )


def relative_volume(
    current_and_previous: Sequence[float], baseline_count: int
) -> float | None:
    """Current quote volume divided by the mean of preceding same-interval bars."""
    if type(baseline_count) is not int or baseline_count < 1:
        raise ValueError("baseline_count must be a positive integer")
    values = _finite_vector(current_and_previous, name="volumes")
    if len(values) != baseline_count + 1 or (values < 0).any():
        return None
    baseline = float(np.mean(values[1:]))
    return None if baseline == 0.0 else float(values[0] / baseline)


def realized_volatility_24h(closes: Sequence[float]) -> float | None:
    """Sample stdev of 1,440 contiguous minute log returns, annualized on 365×24×60 minutes."""
    values = _finite_vector(closes, name="closes")
    if len(values) != 1441 or (values <= 0).any():
        return None
    returns = np.diff(np.log(values))
    if not np.isfinite(returns).all():
        return None
    return float(np.std(returns, ddof=1) * math.sqrt(ANNUAL_MINUTES))


def percentile_midranks(value: float, prior_values: Sequence[float]) -> float | None:
    """Percentile rank versus prior observations only; ties receive their midrank."""
    prior = _finite_vector(prior_values, name="prior_values")
    if not math.isfinite(value):
        raise ValueError("ranked value must be finite")
    if len(prior) == 0:
        return None
    less = int(np.count_nonzero(prior < value))
    equal = int(np.count_nonzero(prior == value))
    return 100.0 * (less + 0.5 * equal) / len(prior)


def pearson_correlation(
    left: Sequence[float], right: Sequence[float], min_pairs: int = 60
) -> float | None:
    """Pearson correlation over already timestamp-paired 1h log returns; constants are undefined."""
    a = _finite_vector(left, name="left returns")
    b = _finite_vector(right, name="right returns")
    if type(min_pairs) is not int or min_pairs < 2:
        raise ValueError("min_pairs must be at least two")
    if len(a) != len(b):
        raise ValueError("paired return sequences must have equal length")
    if len(a) < min_pairs or np.ptp(a) == 0.0 or np.ptp(b) == 0.0:
        return None
    result = float(np.corrcoef(a, b)[0, 1])
    return min(1.0, max(-1.0, result)) if math.isfinite(result) else None


def correlation_coverage(sample_count: int, expected_count: int) -> float | None:
    if (
        type(sample_count) is not int
        or type(expected_count) is not int
        or sample_count < 0
        or expected_count < 0
    ):
        raise ValueError("correlation counts must be nonnegative integers")
    if sample_count > expected_count:
        raise ValueError("sample count exceeds expected paired observations")
    return sample_count / expected_count if expected_count else None


def prepare_hourly_log_returns(
    prices: Mapping[int, float], *, expected_end_times_ms: Sequence[int]
) -> np.ndarray:
    """Align closed prices once; NaN explicitly represents an unavailable hourly return.

    This reusable neutral projection lets an all-pairs consumer avoid calculating
    an instrument's identical hourly log returns for every counterpart.
    """
    if any(type(t) is not int or t < 0 for t in expected_end_times_ms):
        raise ValueError(
            "expected hourly endpoints must be nonnegative integer UTC milliseconds"
        )
    expected_times = _finite_vector(
        expected_end_times_ms, name="expected hourly endpoints"
    )
    if len(expected_times) and (np.diff(expected_times) <= 0).any():
        raise ValueError("expected hourly endpoints must be strictly increasing")
    if len(expected_times) > 1 and not (np.diff(expected_times) == 3_600_000).all():
        raise ValueError("expected hourly endpoints must be a contiguous 1h grid")
    result = np.full(len(expected_times), np.nan, dtype=float)
    for index, end_ms in enumerate(expected_times.astype(np.int64)):
        end = int(end_ms)
        if end not in prices or end - 3_600_000 not in prices:
            continue
        current, prior = prices[end], prices[end - 3_600_000]
        if any(
            isinstance(x, bool)
            or not isinstance(x, (int, float))
            or not math.isfinite(x)
            or x <= 0
            for x in (current, prior)
        ):
            continue
        ratio = current / prior
        if math.isfinite(ratio) and ratio > 0:
            result[index] = math.log(ratio)
    result.flags.writeable = False
    return result


def prepare_hourly_log_returns_from_validated_closes(
    prices: Mapping[int, float], expected_end_times_ms: Sequence[int]
) -> np.ndarray:
    """Align closes already validated by an owner adapter on a contiguous hourly grid.

    This avoids repeating the per-price trust-boundary checks performed by
    ``prepare_hourly_log_returns``. Callers must validate every source timestamp
    and positive finite close before using this optimized projection.
    """
    if any(
        type(timestamp) is not int or timestamp < 0
        for timestamp in expected_end_times_ms
    ):
        raise ValueError(
            "expected hourly endpoints must be nonnegative integer UTC milliseconds"
        )
    expected = np.asarray(expected_end_times_ms, dtype=np.int64)
    if len(expected) and (np.diff(expected) <= 0).any():
        raise ValueError("expected hourly endpoints must be strictly increasing")
    if len(expected) > 1 and not (np.diff(expected) == 3_600_000).all():
        raise ValueError("expected hourly endpoints must be a contiguous 1h grid")
    times = np.fromiter(sorted(prices), dtype=np.int64, count=len(prices))
    values = np.fromiter(
        (float(prices[int(timestamp)]) for timestamp in times),
        dtype=float,
        count=len(times),
    )
    positions = np.searchsorted(times, expected)
    prior_positions = np.searchsorted(times, expected - 3_600_000)
    current_present = positions < len(times)
    prior_present = prior_positions < len(times)
    current_valid = np.zeros(len(expected), dtype=bool)
    prior_valid = np.zeros(len(expected), dtype=bool)
    current_valid[current_present] = (
        times[positions[current_present]] == expected[current_present]
    )
    prior_valid[prior_present] = (
        times[prior_positions[prior_present]] == expected[prior_present] - 3_600_000
    )
    valid = current_valid & prior_valid
    result = np.full(len(expected), np.nan, dtype=float)
    for index in np.flatnonzero(valid):
        current, prior = (
            float(values[positions[index]]),
            float(values[prior_positions[index]]),
        )
        ratio = current / prior
        if math.isfinite(ratio) and ratio > 0:
            result[index] = math.log(ratio)
    result.flags.writeable = False
    return result


def _validated_dense_return_matrix(values: np.ndarray) -> np.ndarray:
    if (
        not isinstance(values, np.ndarray)
        or values.ndim != 2
        or values.dtype != np.dtype(float)
        or values.shape[0] < 1
        or values.shape[1] < 2
        or not np.isfinite(values).all()
    ):
        raise ValueError(
            "dense correlation input must be a finite two-dimensional float matrix"
        )
    return values


def dense_hourly_correlation_matrix(
    returns: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Batch Pearson correlations for complete rows; constants remain undefined."""
    data = _validated_dense_return_matrix(returns)
    constant_rows = np.ptp(data, axis=1) == 0.0
    variable = np.flatnonzero(~constant_rows)
    result = np.full((len(data), len(data)), np.nan)
    if len(variable):
        result[np.ix_(variable, variable)] = np.corrcoef(data[variable])
    result.flags.writeable = False
    constant_rows.flags.writeable = False
    return result, constant_rows


def dense_hourly_correlation_row(
    returns: np.ndarray, reference_index: int, start: int, end: int
) -> tuple[np.ndarray, np.ndarray]:
    """Batch a reference series against all rows in one complete hourly window."""
    data = _validated_dense_return_matrix(returns)
    if (
        type(reference_index) is not int
        or not 0 <= reference_index < len(data)
        or type(start) is not int
        or type(end) is not int
        or not 0 <= start < end <= data.shape[1]
        or end - start < 2
    ):
        raise ValueError("dense correlation window or reference row is invalid")
    window = data[:, start:end]
    constant_rows = np.ptp(window, axis=1) == 0.0
    result = np.full(len(data), np.nan)
    if not constant_rows[reference_index]:
        reference = window[reference_index]
        centered_reference = reference - np.mean(reference)
        reference_ss = float(np.sum(centered_reference * centered_reference))
        centered = window - np.mean(window, axis=1, keepdims=True)
        sums_of_squares = np.sum(centered * centered, axis=1)
        covariance = centered @ centered_reference
        # Taking roots first avoids overflowing the product for finite large
        # vectors whose individual sums of squares remain representable.
        denominator = np.sqrt(sums_of_squares) * math.sqrt(reference_ss)
        valid = ~constant_rows & (denominator > 0.0)
        result[valid] = covariance[valid] / denominator[valid]
        result[valid] = np.clip(result[valid], -1.0, 1.0)
    result.flags.writeable = False
    constant_rows.flags.writeable = False
    return result, constant_rows


def correlate_hourly_return_vectors(
    left: np.ndarray,
    right: np.ndarray,
    *,
    min_coverage: float = 0.95,
    min_pairs: int = 60,
) -> CorrelationValue:
    """Same estimator on vectors prepared against the same explicit hourly grid.

    Callers must bind both vectors to one grid; vector positions are timestamps,
    never arbitrary observation offsets. Missing pairs are omitted together.
    """
    if not 0 < min_coverage <= 1 or type(min_pairs) is not int or min_pairs < 2:
        raise ValueError("invalid correlation coverage policy")
    if (
        not isinstance(left, np.ndarray)
        or not isinstance(right, np.ndarray)
        or left.ndim != 1
        or right.ndim != 1
        or left.shape != right.shape
        or left.dtype != np.dtype(float)
        or right.dtype != np.dtype(float)
    ):
        raise ValueError(
            "prepared return vectors must be matching one-dimensional float arrays"
        )
    expected = len(left)
    if not expected:
        return CorrelationValue(None, 0, 0, None)
    paired = np.isfinite(left) & np.isfinite(right)
    sample_count = int(np.count_nonzero(paired))
    coverage = sample_count / expected
    if coverage < min_coverage or sample_count < min_pairs:
        return CorrelationValue(None, sample_count, expected, coverage)
    coefficient = pearson_correlation(left[paired], right[paired], min_pairs=min_pairs)
    return CorrelationValue(coefficient, sample_count, expected, coverage)


def correlate_hourly_closes(
    left: Mapping[int, float],
    right: Mapping[int, float],
    *,
    expected_end_times_ms: Sequence[int],
    min_coverage: float = 0.95,
    min_pairs: int = 60,
) -> CorrelationValue:
    """Paired 1h log-return correlation; each return needs its prior hour on both sides."""
    return correlate_hourly_return_vectors(
        prepare_hourly_log_returns(left, expected_end_times_ms=expected_end_times_ms),
        prepare_hourly_log_returns(right, expected_end_times_ms=expected_end_times_ms),
        min_coverage=min_coverage,
        min_pairs=min_pairs,
    )


def correlate_90d_hourly_closes(
    left: Mapping[int, float], right: Mapping[int, float], *, cutoff_ms: int
) -> CorrelationValue:
    """Canonical 90-day correlation window ending at a completed UTC hour."""
    if type(cutoff_ms) is not int or cutoff_ms < 0 or cutoff_ms % 3_600_000:
        raise ValueError("correlation cutoff must be a completed UTC hour")
    first = cutoff_ms - 2160 * 3_600_000 + 3_600_000
    if first < 0:
        return CorrelationValue(None, 0, 2160, 0.0)
    expected = tuple(range(first, cutoff_ms + 1, 3_600_000))
    return correlate_hourly_closes(
        left, right, expected_end_times_ms=expected, min_coverage=0.95, min_pairs=60
    )


def correlation_30d_trend(
    left: Mapping[int, float], right: Mapping[int, float], *, cutoff_ms: int
) -> tuple[tuple[int, CorrelationValue], ...]:
    """Thirty daily points of the same rolling 90d estimator; needs roughly 120d of hourly history."""
    if type(cutoff_ms) is not int or cutoff_ms < 0 or cutoff_ms % 3_600_000:
        raise ValueError("correlation cutoff must be a completed UTC hour")
    first = cutoff_ms - 29 * 24 * 3_600_000
    return tuple(
        (point, correlate_90d_hourly_closes(left, right, cutoff_ms=point))
        for point in range(first, cutoff_ms + 1, 24 * 3_600_000)
    )


def relative_return_percent(
    asset_return: float | None, benchmark_return: float | None
) -> float | None:
    if asset_return is None or benchmark_return is None:
        return None
    if (
        isinstance(asset_return, bool)
        or isinstance(benchmark_return, bool)
        or not math.isfinite(asset_return)
        or not math.isfinite(benchmark_return)
    ):
        raise ValueError("relative returns must be finite")
    return asset_return - benchmark_return


def is_new_high(current_high: float, previous_highs: Sequence[float]) -> bool | None:
    previous = _finite_vector(previous_highs, name="previous highs")
    if (
        isinstance(current_high, bool)
        or not math.isfinite(current_high)
        or current_high <= 0
        or len(previous) == 0
        or (previous <= 0).any()
    ):
        return None
    return bool(current_high > np.max(previous))


def is_new_low(current_low: float, previous_lows: Sequence[float]) -> bool | None:
    previous = _finite_vector(previous_lows, name="previous lows")
    if (
        isinstance(current_low, bool)
        or not math.isfinite(current_low)
        or current_low <= 0
        or len(previous) == 0
        or (previous <= 0).any()
    ):
        return None
    return bool(current_low < np.min(previous))


def new_24h_high(current_high: float, previous_highs: Sequence[float]) -> bool | None:
    if len(previous_highs) != 1440:
        return None
    return is_new_high(current_high, previous_highs)


def new_24h_low(current_low: float, previous_lows: Sequence[float]) -> bool | None:
    if len(previous_lows) != 1440:
        return None
    return is_new_low(current_low, previous_lows)


def new_7d_high(current_high: float, previous_highs: Sequence[float]) -> bool | None:
    if len(previous_highs) != 10_080:
        return None
    return is_new_high(current_high, previous_highs)


def new_7d_low(current_low: float, previous_lows: Sequence[float]) -> bool | None:
    if len(previous_lows) != 10_080:
        return None
    return is_new_low(current_low, previous_lows)


def new_10d_hourly_close_break(
    current_close: float,
    previous_highs: Sequence[float],
    previous_lows: Sequence[float],
) -> tuple[bool, bool] | None:
    highs = _finite_vector(previous_highs, name="previous hourly highs")
    lows = _finite_vector(previous_lows, name="previous hourly lows")
    if (
        len(highs) != 240
        or len(lows) != 240
        or isinstance(current_close, bool)
        or not math.isfinite(current_close)
        or current_close <= 0
    ):
        return None
    if (highs <= 0).any() or (lows <= 0).any() or (lows > highs).any():
        return None
    return bool(current_close > np.max(highs)), bool(current_close < np.min(lows))


def new_52w_completed_day_break(
    event_high: float,
    event_low: float,
    previous_364_highs: Sequence[float],
    previous_364_lows: Sequence[float],
) -> tuple[bool, bool] | None:
    highs = _finite_vector(previous_364_highs, name="previous completed-day highs")
    lows = _finite_vector(previous_364_lows, name="previous completed-day lows")
    if (
        len(highs) != 364
        or len(lows) != 364
        or isinstance(event_high, bool)
        or isinstance(event_low, bool)
        or not math.isfinite(event_high)
        or not math.isfinite(event_low)
    ):
        return None
    if (
        (highs <= 0).any()
        or (lows <= 0).any()
        or (lows > highs).any()
        or event_high <= 0
        or event_low <= 0
    ):
        return None
    return bool(event_high > np.max(highs)), bool(event_low < np.min(lows))


def annualized_realized_volatility(
    closes: Sequence[float], periods_per_year: int, return_count: int
) -> float | None:
    """General contiguous log-return sample volatility. The 24h public definition calls with 365×24×60, 1440."""
    if (
        type(periods_per_year) is not int
        or periods_per_year < 1
        or type(return_count) is not int
        or return_count < 2
    ):
        raise ValueError("annualization and return window must be positive integers")
    values = _finite_vector(closes, name="closes")
    if len(values) != return_count + 1 or (values <= 0).any():
        return None
    return float(np.std(np.diff(np.log(values)), ddof=1) * math.sqrt(periods_per_year))


def distribution_summary(
    values: Mapping[str, float | None], expected_count: int
) -> DistributionSummary:
    if type(expected_count) is not int or expected_count < len(values):
        raise ValueError("expected count must cover all supplied instruments")
    valid = {}
    for instrument_id, value in values.items():
        if value is None:
            continue
        if (
            isinstance(value, bool)
            or not isinstance(value, (float, int))
            or not math.isfinite(value)
        ):
            raise ValueError("distribution values must be finite percentages or null")
        valid[instrument_id] = float(value)
    ordered = np.asarray(list(valid.values()), dtype=float)
    n = len(ordered)
    mean = float(np.mean(ordered)) if n else None
    return DistributionSummary(
        n,
        expected_count,
        mean,
        float(np.median(ordered)) if n else None,
        float(np.std(ordered, ddof=0)) if n >= 2 else None,
        sum(value < 0 for value in valid.values()),
        sum(value == 0 for value in valid.values()),
        sum(value > 0 for value in valid.values()),
        max(-mean, 0.0) if mean is not None else None,
        min(valid, key=lambda instrument: (-valid[instrument], instrument))
        if valid
        else None,
        min(valid, key=lambda instrument: (valid[instrument], instrument))
        if valid
        else None,
    )


def _observation(
    metric_id: str,
    value: float | Decimal | None,
    *,
    available: int,
    required: int,
    cutoff_ms: int,
    status: str | None = None,
    reason: str | None = None,
    window_start_ms: int | None = None,
    interval_ms: int = 60_000,
) -> AssetMetricObservation:
    # Public observations feed strict Pydantic/API contracts. Incremental windows
    # keep timestamps in NumPy arrays, so remove NumPy scalar wrappers here.
    if isinstance(value, np.generic):
        value = value.item()
    if status is None:
        status = (
            "VALID"
            if value is not None
            else {
                "INPUT_MISSING": "MISSING",
                "SOURCE_UNAVAILABLE": "UNAVAILABLE",
                "ZERO_BASELINE": "UNAVAILABLE",
            }.get(reason, "WARMING")
        )
    if reason is None and value is None:
        reason = "INSUFFICIENT_HISTORY"
    return AssetMetricObservation(
        metric_id,
        value,
        status,
        () if reason is None else (reason,),
        int(available),
        int(required),
        int(window_start_ms) if window_start_ms is not None else None,
        int(cutoff_ms),
        int(interval_ms),
    )


def compute_asset_metrics(
    bars: AssetBarsV1,
    *,
    benchmark_returns_24h: Mapping[str, float | None] | None = None,
) -> dict[str, AssetMetricObservation]:
    """Build all frozen per-instrument panel values from one already-admitted 1m history slice.

    Source gaps are rejected by AssetBarsV1 before this function. Missing quote-volume values remain
    unavailable only for the RVOL families; they never invalidate price/return calculations.
    """
    exact_close = bars.close
    close = bars._close_vector
    high = bars._high_vector
    low = bars._low_vector
    volumes = bars.quote_volume
    n = len(close)
    end = bars.cutoff_ms
    result: dict[str, AssetMetricObservation] = {}

    def put(
        metric_id, value, required, *, count=None, reason=None, start=None, status=None
    ):
        result[metric_id] = _observation(
            metric_id,
            value,
            available=n if count is None else count,
            required=required,
            cutoff_ms=end,
            status=status,
            reason=reason,
            window_start_ms=start,
        )

    latest_close = (
        exact_close[-1]
        if isinstance(exact_close[-1], Decimal)
        else Decimal(str(exact_close[-1]))
    )
    put("price", latest_close, 1, count=1, start=end)
    returns = {}
    for horizon in (*HORIZONS_MINUTES, 10_080):
        metric_id = f"return_{horizon}m"
        exact_value = (
            _return_percent_from_validated_endpoints(
                exact_close[-horizon - 1], exact_close[-1]
            )
            if n >= horizon + 1
            else None
        )
        value = float(exact_value) if exact_value is not None else None
        count = min(n, horizon + 1)
        put(
            metric_id,
            exact_value,
            horizon + 1,
            count=count,
            reason=None if value is not None else "INSUFFICIENT_HISTORY",
            start=end - horizon * 60_000 if value is not None else None,
        )
        returns[horizon] = value

    ema_values = ema(close, 21)
    put(
        "ema21",
        float(ema_values[-1]) if np.isfinite(ema_values[-1]) else None,
        21,
        count=n,
        start=bars.close_times_ms[0] if n else None,
    )
    rsi_values = wilder_rsi(close, 14)
    rsi_ready = np.flatnonzero(np.isfinite(rsi_values))
    put(
        "rsi14",
        float(rsi_values[-1]) if np.isfinite(rsi_values[-1]) else None,
        16,
        count=n,
        start=bars.close_times_ms[0] if len(rsi_ready) else None,
    )

    atr_values = atr_percent(high, low, close, 14)
    atr_valid = np.flatnonzero(np.isfinite(atr_values))
    atr_current = float(atr_values[-1]) if n and np.isfinite(atr_values[-1]) else None
    put(
        "atr14_percent",
        atr_current,
        14,
        count=n,
        start=bars.close_times_ms[0] if atr_current is not None else None,
    )
    percentile = None
    if atr_current is not None and len(atr_valid) >= 201:
        prior = atr_values[atr_valid[-201:-1]]
        if len(prior) == 200:
            percentile = percentile_midranks(atr_current, prior)
    put(
        "atr14_percentile",
        percentile,
        201,
        count=min(len(atr_valid), 201),
        reason=None if percentile is not None else "INSUFFICIENT_HISTORY",
        start=bars.close_times_ms[atr_valid[-201]] if percentile is not None else None,
    )

    adx_values, plus_di, minus_di = wilder_adx(high, low, close, 14)
    put(
        "adx14",
        float(adx_values[-1]) if n and np.isfinite(adx_values[-1]) else None,
        27,
        count=n,
        start=bars.close_times_ms[0]
        if n >= 27 and np.isfinite(adx_values[-1])
        else None,
    )

    for metric_id, baseline_count in (("rvol20", 20), ("rvol_24h", 1440)):
        required = baseline_count + 1
        value = None
        reason = "INSUFFICIENT_HISTORY"
        if n >= required:
            prior_window = volumes[-required:]
            if isinstance(prior_window, np.ndarray):
                has_missing = bool(np.isnan(prior_window).any())
            else:
                has_missing = any(item is None for item in prior_window)
            if has_missing:
                reason = "INPUT_MISSING"
            else:
                if isinstance(prior_window, np.ndarray):
                    ordered_window = [
                        float(prior_window[-1]),
                        *(float(item) for item in prior_window[-2::-1]),
                    ]
                else:
                    ordered_window = [
                        float(prior_window[-1]),
                        *(float(item) for item in reversed(prior_window[:-1])),
                    ]
                value = relative_volume(ordered_window, baseline_count)
                reason = None if value is not None else "ZERO_BASELINE"
        status = (
            "MISSING"
            if reason == "INPUT_MISSING"
            else ("UNAVAILABLE" if reason == "ZERO_BASELINE" else None)
        )
        put(
            metric_id,
            value,
            required,
            count=min(n, required),
            reason=reason,
            status=status,
            start=end - baseline_count * 60_000 if value is not None else None,
        )

    rv24h = realized_volatility_24h(close[-1441:] if n >= 1441 else close)
    put(
        "realized_volatility_24h",
        rv24h,
        1440,
        count=min(max(0, n - 1), 1440),
        start=end - 1440 * 60_000 if rv24h is not None else None,
    )

    for benchmark in ("btc", "eth", "bnb", "sol"):
        metric_id = f"relative_24h_{benchmark}"
        benchmark_return = (
            None
            if benchmark_returns_24h is None
            else benchmark_returns_24h.get(benchmark)
        )
        value = relative_return_percent(returns[1440], benchmark_return)
        reason = (
            None
            if value is not None
            else (
                "INSUFFICIENT_HISTORY"
                if returns[1440] is None
                else "SOURCE_UNAVAILABLE"
            )
        )
        put(
            metric_id,
            value,
            1441,
            count=min(n, 1441),
            reason=reason,
            start=end - 1440 * 60_000 if value is not None else None,
        )
    return result


@njit(cache=False, fastmath=False)
def _ema_kernel(data: np.ndarray, length: int) -> np.ndarray:
    output = np.full(len(data), np.nan)
    if not len(data):
        return output
    alpha = 2.0 / (length + 1.0)
    output[0] = data[0]
    for i in range(1, len(data)):
        output[i] = alpha * data[i] + (1.0 - alpha) * output[i - 1]
    return output


@njit(cache=False, fastmath=False)
def _wilder_rsi_kernel(
    gains: np.ndarray,
    losses: np.ndarray,
    length: int,
    initial_gain: float,
    initial_loss: float,
):
    output = np.full(len(gains) + 1, np.nan)
    if len(gains) <= length:
        return output
    gain, loss = initial_gain, initial_loss
    for i in range(length, len(gains)):
        gain = (gain * (length - 1) + gains[i]) / length
        loss = (loss * (length - 1) + losses[i]) / length
        output[i + 1] = (
            50.0
            if gain == loss == 0
            else 100.0
            if loss == 0
            else 100.0 - 100.0 / (1.0 + gain / loss)
        )
    return output


def ema(values: Sequence[float], length: int) -> np.ndarray:
    """EMA with first-observation seed and adjust=False recurrence, aligned to input indices."""
    data = _finite_vector(values, name="EMA values")
    if type(length) is not int or length < 1:
        raise ValueError("EMA length must be a positive integer")
    return _ema_kernel(data, length)


def wilder_rsi(close: Sequence[float], length: int = 14) -> np.ndarray:
    """RSI matching the existing modular V2 Wilder recurrence and warmup semantics.

    The source's first RSI follows the seeded average by one update; NaNs preserve
    input alignment before that first fully formed value.
    """
    prices = _finite_vector(close, name="RSI close")
    if type(length) is not int or length < 1:
        raise ValueError("RSI length must be a positive integer")
    if (prices <= 0).any():
        raise ValueError("RSI closes must be positive")
    changes = np.diff(prices)
    gains, losses = np.maximum(changes, 0.0), np.maximum(-changes, 0.0)
    if len(changes) <= length:
        return np.full(len(prices), np.nan)
    # Keep NumPy's historical pairwise SMA seed; the recurrence itself is JIT-compiled.
    return _wilder_rsi_kernel(
        gains,
        losses,
        length,
        float(np.mean(gains[:length])),
        float(np.mean(losses[:length])),
    )


def true_range(
    high: Sequence[float], low: Sequence[float], close: Sequence[float]
) -> np.ndarray:
    highs, lows, closes = (
        _finite_vector(column, name=name)
        for column, name in ((high, "high"), (low, "low"), (close, "close"))
    )
    if (
        not (len(highs) == len(lows) == len(closes))
        or (highs <= 0).any()
        or (lows <= 0).any()
        or (closes <= 0).any()
    ):
        raise ValueError("OHLC columns must have equal length and positive prices")
    if (lows > np.minimum(highs, closes)).any() or (
        np.maximum(lows, closes) > highs
    ).any():
        raise ValueError("OHLC bounds are inconsistent")
    result = np.empty(len(closes), dtype=float)
    if not len(result):
        return result
    result[0] = highs[0] - lows[0]
    if len(result) > 1:
        result[1:] = np.maximum(
            highs[1:] - lows[1:],
            np.maximum(np.abs(highs[1:] - closes[:-1]), np.abs(lows[1:] - closes[:-1])),
        )
    return result


@njit(cache=False, fastmath=False)
def _wilder_smooth_kernel(
    data: np.ndarray, length: int, initial_value: float
) -> np.ndarray:
    output = np.full(len(data), np.nan)
    if len(data) < length:
        return output
    output[length - 1] = initial_value
    for i in range(length, len(data)):
        output[i] = (output[i - 1] * (length - 1) + data[i]) / length
    return output


def wilder_smooth(values: Sequence[float], length: int) -> np.ndarray:
    """SMA-seeded Wilder smoothing, with explicit NaNs until a complete seed exists."""
    data = _finite_vector(values, name="Wilder values")
    if type(length) is not int or length < 1:
        raise ValueError("Wilder length must be a positive integer")
    if len(data) < length:
        return np.full(len(data), np.nan)
    # Match the original NumPy SMA seed exactly, including pairwise reduction order.
    return _wilder_smooth_kernel(data, length, float(np.mean(data[:length])))


def wilder_atr(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    length: int = 14,
) -> np.ndarray:
    ranges = true_range(high, low, close)
    return wilder_smooth(ranges, length)


def atr_percent(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    length: int = 14,
) -> np.ndarray:
    closes = _finite_vector(close, name="ATR close")
    atr = wilder_atr(high, low, closes, length)
    return 100.0 * atr / closes


@njit(cache=False, fastmath=False)
def _wilder_adx_components_kernel(
    tr: np.ndarray,
    plus_dm: np.ndarray,
    minus_dm: np.ndarray,
    length: int,
    atr_seed: float,
    plus_seed: float,
    minus_seed: float,
):
    atr = _wilder_smooth_kernel(tr, length, atr_seed)
    plus_smoothed = _wilder_smooth_kernel(plus_dm, length, plus_seed)
    minus_smoothed = _wilder_smooth_kernel(minus_dm, length, minus_seed)
    plus_di = np.full(len(tr), np.nan)
    minus_di = np.full(len(tr), np.nan)
    dx = np.full(len(tr), np.nan)
    for i in range(len(tr)):
        if not np.isfinite(atr[i]) or atr[i] <= 0:
            continue
        plus_di[i] = 100.0 * plus_smoothed[i] / atr[i]
        minus_di[i] = 100.0 * minus_smoothed[i] / atr[i]
        denom = plus_di[i] + minus_di[i]
        if math.isfinite(denom):
            dx[i] = (
                0.0 if denom == 0.0 else 100.0 * abs(plus_di[i] - minus_di[i]) / denom
            )
    return dx, plus_di, minus_di


@njit(cache=False, fastmath=False)
def _wilder_adx_finish_kernel(
    dx: np.ndarray, length: int, first: int, initial_value: float
):
    adx = np.full(len(dx), np.nan)
    adx[first] = initial_value
    for i in range(first + 1, len(dx)):
        if np.isfinite(dx[i]):
            adx[i] = (adx[i - 1] * (length - 1) + dx[i]) / length
    return adx


def _finish_wilder_adx(dx: np.ndarray, length: int) -> np.ndarray:
    adx = np.full(len(dx), np.nan)
    valid_dx = np.flatnonzero(np.isfinite(dx))
    if len(valid_dx) >= length:
        first = valid_dx[length - 1]
        if np.array_equal(valid_dx[:length], np.arange(first - length + 1, first + 1)):
            # Preserve NumPy's original pairwise seed before returning to the compiled recurrence.
            seed = float(np.mean(dx[first - length + 1 : first + 1]))  # noqa: E203
            return _wilder_adx_finish_kernel(dx, length, first, seed)
    return adx


def wilder_adx(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    length: int = 14,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """SMA-seeded Wilder ADX, +DI and -DI; output arrays remain aligned and warmup is NaN."""
    highs, lows, closes = (
        _finite_vector(column, name=name)
        for column, name in ((high, "high"), (low, "low"), (close, "close"))
    )
    if not (len(highs) == len(lows) == len(closes)):
        raise ValueError("OHLC columns must have equal length")
    if type(length) is not int or length < 1:
        raise ValueError("ADX length must be a positive integer")
    tr = true_range(highs, lows, closes)
    plus_dm = np.zeros(len(closes), dtype=float)
    minus_dm = np.zeros(len(closes), dtype=float)
    if len(closes) > 1:
        up, down = np.diff(highs), -np.diff(lows)
        plus_dm[1:] = np.where((up > down) & (up > 0), up, 0.0)
        minus_dm[1:] = np.where((down > up) & (down > 0), down, 0.0)
    if len(closes) < length:
        empty = np.full(len(closes), np.nan)
        return empty, empty.copy(), empty.copy()
    dx, plus_di, minus_di = _wilder_adx_components_kernel(
        tr,
        plus_dm,
        minus_dm,
        length,
        float(np.mean(tr[:length])),
        float(np.mean(plus_dm[:length])),
        float(np.mean(minus_dm[:length])),
    )
    return _finish_wilder_adx(dx, length), plus_di, minus_di


def indicator_snapshot(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    *,
    ema_length: int = 21,
    rsi_length: int = 14,
    atr_length: int = 14,
) -> IndicatorSnapshot:
    """Return latest confirmed-candle EMA/RSI/ATR/ADX/DI values or explicit None during warmup."""
    closes = _finite_vector(close, name="indicator close")
    if not len(closes):
        return IndicatorSnapshot(None, None, None, None, None, None, None)
    ema_values = ema(closes, ema_length)
    rsi_values = wilder_rsi(closes, rsi_length)
    atr_values = wilder_atr(high, low, closes, atr_length)
    adx_values, plus_di, minus_di = wilder_adx(high, low, closes, 14)
    atr = float(atr_values[-1]) if np.isfinite(atr_values[-1]) else None
    close_now = float(closes[-1])
    return IndicatorSnapshot(
        float(ema_values[-1]) if np.isfinite(ema_values[-1]) else None,
        float(rsi_values[-1]) if np.isfinite(rsi_values[-1]) else None,
        atr,
        (100.0 * atr / close_now) if atr is not None else None,
        float(adx_values[-1]) if np.isfinite(adx_values[-1]) else None,
        float(plus_di[-1]) if np.isfinite(plus_di[-1]) else None,
        float(minus_di[-1]) if np.isfinite(minus_di[-1]) else None,
    )
