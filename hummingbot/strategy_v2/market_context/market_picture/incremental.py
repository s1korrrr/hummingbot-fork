"""Bounded incremental Market Picture input state with batch-equivalent outputs.

Source deltas update compact numeric columns and exact close text. Snapshots use
the same retained-window calculations and seeds as ``compute_asset_metrics``;
the recursive kernels are compiled with Numba so a 300-instrument warm refresh
does not pay Python-loop cost. Corrections rebuild only this instrument's bounded
window. Source identity, lineage, availability and epochs remain caller-owned.
"""

from __future__ import annotations

import math
import re
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Mapping

import numpy as np

from .contracts import INSTRUMENT_PATTERN, MAX_TIMESTAMP_MS
from .metrics import AssetBarsV1, AssetMetricObservation, compute_asset_metrics

_MAX_HISTORY = 20_161
_MINUTE_MS = 60_000


class MetricStateResetRequired(ValueError):
    """The delta is outside the retained contiguous window; supply a full reset."""


class IncrementalAssetMetricState:
    """Retain a bounded OHLCV window and expose batch-equivalent metric snapshots.

    Append updates move one source row through bounded arrays. Recursive EMA and
    Wilder indicators are recalculated over the retained window, preserving the
    independent batch oracle's exact seed and warmup semantics. Corrections
    inside the window are accepted; gaps, expired corrections, and epoch changes
    require a full validated history reset.
    """

    def __init__(self, instrument_id: str, *, capacity: int = 11_521) -> None:
        if not isinstance(instrument_id, str) or re.fullmatch(INSTRUMENT_PATTERN, instrument_id) is None:
            raise ValueError("incremental metric state requires an exact instrument identity")
        if type(capacity) is not int or not 1 <= capacity <= _MAX_HISTORY:
            raise ValueError("incremental metric capacity is outside the source history bound")
        self.instrument_id = instrument_id
        self.capacity = capacity
        self._times = np.empty(0, dtype=np.int64)
        self._high = np.empty(0, dtype=np.float64)
        self._low = np.empty(0, dtype=np.float64)
        self._close = np.empty(0, dtype=np.float64)
        self._quote_volume = np.empty(0, dtype=np.float64)
        self._exact_close: list[str] = []
        self._exact_close_bytes = 0
        self._metrics = MappingProxyType({})
        self._dirty = False
        self._cutoff_ms: int | None = None
        self._benchmark_returns_24h: Mapping[str, float | None] | None = None

    @property
    def cutoff_ms(self) -> int | None:
        return self._cutoff_ms

    @property
    def first_close_ms(self) -> int | None:
        return int(self._times[0]) if self.row_count else None

    @property
    def last_close_ms(self) -> int | None:
        return int(self._times[-1]) if self.row_count else None

    @property
    def row_count(self) -> int:
        return len(self._times)

    @property
    def count(self) -> int:
        return self.row_count

    @property
    def nbytes(self) -> int:
        """O(1) retained-byte estimate, including exact close strings."""
        return sum(
            array.nbytes for array in (self._times, self._high, self._low, self._close, self._quote_volume)
        ) + sys.getsizeof(self._exact_close) + self._exact_close_bytes

    @classmethod
    def from_history(cls, bars: AssetBarsV1, *, capacity: int = 11_521) -> "IncrementalAssetMetricState":
        """Create state from a validated full-history baseline."""
        state = cls(bars.instrument_id, capacity=capacity)
        state.reset(bars)
        return state

    @property
    def metrics(self) -> Mapping[str, AssetMetricObservation]:
        return self.snapshot()

    def reset(
        self,
        bars: AssetBarsV1,
        *,
        benchmark_returns_24h: Mapping[str, float | None] | None = None,
    ) -> Mapping[str, AssetMetricObservation]:
        """Replace state from an already validated source history."""
        if bars.instrument_id != self.instrument_id:
            raise ValueError("reset history belongs to another instrument")
        start = max(0, len(bars.close_times_ms) - self.capacity)
        self._times = np.asarray(bars.close_times_ms[start:], dtype=np.int64).copy()
        self._high = np.asarray(bars._high_vector[start:], dtype=np.float64).copy()
        self._low = np.asarray(bars._low_vector[start:], dtype=np.float64).copy()
        self._close = np.asarray(bars._close_vector[start:], dtype=np.float64).copy()
        self._quote_volume = np.asarray(
            [np.nan if value is None else float(value) for value in bars.quote_volume[start:]], dtype=np.float64
        )
        self._exact_close = [str(value) for value in bars.close[start:]]
        self._exact_close_bytes = sum(sys.getsizeof(value) for value in self._exact_close)
        self._cutoff_ms = bars.cutoff_ms
        self._benchmark_returns_24h = benchmark_returns_24h
        self._dirty = True
        return self.snapshot()

    def upsert(
        self,
        close_time_ms: int,
        *,
        high: Decimal | str | float | int,
        low: Decimal | str | float | int,
        close: Decimal | str | float | int,
        quote_volume: float | int | None,
    ) -> None:
        """Append the next minute or replace an in-window bar; call snapshot once after a batch."""
        numeric = self._validated_bar(close_time_ms, high, low, close, quote_volume)
        if self._cutoff_ms is None or not self.row_count:
            raise MetricStateResetRequired("incremental metric state needs a full source history reset")
        timestamp = numeric[0]
        if timestamp == int(self._times[-1]):
            self._replace(self.row_count - 1, timestamp, numeric, close)
        elif timestamp > int(self._times[-1]):
            if timestamp != int(self._times[-1]) + _MINUTE_MS:
                raise MetricStateResetRequired("minute delta is gapped; rebuild from a full source history")
            self._append(timestamp, numeric, close)
            self._cutoff_ms = timestamp
        else:
            index = int(np.searchsorted(self._times, timestamp))
            if index >= self.row_count or int(self._times[index]) != timestamp:
                raise MetricStateResetRequired("correction is outside the retained source window")
            self._replace(index, timestamp, numeric, close)
        self._dirty = True

    def retain_window(self, first_close_ms: int, last_close_ms: int) -> None:
        """Trim to a contiguous retained subwindow; extending it requires a full reset."""
        if type(first_close_ms) is not int or type(last_close_ms) is not int or first_close_ms > last_close_ms:
            raise ValueError("retained metric interval must be an increasing UTC interval")
        if not self.row_count or first_close_ms < self.first_close_ms or last_close_ms > self.last_close_ms:
            raise MetricStateResetRequired("requested metric interval extends outside retained history")
        first = int(np.searchsorted(self._times, first_close_ms))
        last = int(np.searchsorted(self._times, last_close_ms))
        if (
            first >= self.row_count
            or last >= self.row_count
            or int(self._times[first]) != first_close_ms
            or int(self._times[last]) != last_close_ms
        ):
            raise MetricStateResetRequired("requested metric interval is not covered by retained rows")
        if first == 0 and last == self.row_count - 1:
            return
        stop = last + 1
        self._times = self._times[first:stop].copy()
        self._high = self._high[first:stop].copy()
        self._low = self._low[first:stop].copy()
        self._close = self._close[first:stop].copy()
        self._quote_volume = self._quote_volume[first:stop].copy()
        self._exact_close = self._exact_close[first:stop]
        self._exact_close_bytes = sum(sys.getsizeof(value) for value in self._exact_close)
        self._cutoff_ms = int(self._times[-1])
        self._dirty = True

    def snapshot(
        self, *, benchmark_returns_24h: Mapping[str, float | None] | None = None
    ) -> Mapping[str, AssetMetricObservation]:
        """Compute once after pending appends/corrections and return immutable metrics."""
        if benchmark_returns_24h is not None:
            self._benchmark_returns_24h = benchmark_returns_24h
            self._dirty = True
        if self._dirty:
            if not self.row_count:
                self._metrics = MappingProxyType({})
            else:
                view = _ValidatedMetricWindow(
                    self.instrument_id,
                    self._times,
                    self._high,
                    self._low,
                    tuple(self._exact_close),
                    self._quote_volume,
                    int(self._times[-1]),
                    self._high,
                    self._low,
                    self._close,
                )
                self._metrics = MappingProxyType(
                    compute_asset_metrics(view, benchmark_returns_24h=self._benchmark_returns_24h)
                )
            self._dirty = False
        return self._metrics

    def return_close_pair(self, horizon_minutes: int) -> tuple[str, str] | None:
        """Return exact retained close endpoints for a horizon, or None while warming."""
        if type(horizon_minutes) is not int or horizon_minutes < 1:
            raise ValueError("return horizon must be a positive integer")
        if self.row_count < horizon_minutes + 1:
            return None
        return self._exact_close[-horizon_minutes - 1], self._exact_close[-1]

    def high_low_window(self, row_count: int) -> tuple[np.ndarray, np.ndarray]:
        """Return read-only ephemeral views of up to the newest requested rows."""
        if type(row_count) is not int or row_count < 1:
            raise ValueError("high/low row count must be positive")
        size = min(row_count, self.row_count)
        highs, lows = self._high[-size:].view(), self._low[-size:].view()
        highs.flags.writeable = False
        lows.flags.writeable = False
        return highs, lows

    def _append(self, timestamp, numeric, exact_close) -> None:
        values = numeric[1:]
        exact_text = str(exact_close)
        if self.row_count < self.capacity:
            self._times = np.append(self._times, timestamp)
            self._high = np.append(self._high, values[0])
            self._low = np.append(self._low, values[1])
            self._close = np.append(self._close, values[2])
            self._quote_volume = np.append(self._quote_volume, values[3])
            self._exact_close.append(exact_text)
            self._exact_close_bytes += sys.getsizeof(exact_text)
            return
        np.copyto(self._times[:-1], self._times[1:])
        np.copyto(self._high[:-1], self._high[1:])
        np.copyto(self._low[:-1], self._low[1:])
        np.copyto(self._close[:-1], self._close[1:])
        np.copyto(self._quote_volume[:-1], self._quote_volume[1:])
        self._times[-1] = timestamp
        self._high[-1], self._low[-1], self._close[-1], self._quote_volume[-1] = values
        self._exact_close_bytes -= sys.getsizeof(self._exact_close[0])
        del self._exact_close[0]
        self._exact_close.append(exact_text)
        self._exact_close_bytes += sys.getsizeof(exact_text)

    def _replace(self, index, timestamp, numeric, exact_close) -> None:
        self._times[index] = timestamp
        self._high[index], self._low[index], self._close[index], self._quote_volume[index] = numeric[1:]
        exact_text = str(exact_close)
        self._exact_close_bytes += sys.getsizeof(exact_text) - sys.getsizeof(self._exact_close[index])
        self._exact_close[index] = exact_text

    @staticmethod
    def _validated_bar(close_time_ms, high, low, close, quote_volume):
        if (
            type(close_time_ms) is not int
            or close_time_ms < _MINUTE_MS
            or close_time_ms > MAX_TIMESTAMP_MS
            or close_time_ms % _MINUTE_MS
        ):
            raise ValueError("delta close time must be a positive UTC minute boundary")
        numeric = []
        for value in (high, low, close):
            if isinstance(value, bool) or not isinstance(value, (Decimal, str, float, int)):
                raise ValueError("delta OHLC values must be decimal-compatible positive prices")
            try:
                decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
                float_value = float(decimal_value)
            except (InvalidOperation, ValueError, TypeError, OverflowError) as exc:
                raise ValueError("delta OHLC values must be finite positive prices") from exc
            if (
                not decimal_value.is_finite()
                or decimal_value <= 0
                or not math.isfinite(float_value)
                or float_value <= 0
            ):
                raise ValueError("delta OHLC values must be finite positive prices")
            numeric.append(float_value)
        high_value, low_value, close_value = numeric
        if low_value > close_value or close_value > high_value:
            raise ValueError("delta OHLC values must satisfy low <= close <= high")
        if quote_volume is None:
            quote_value = np.nan
        elif (
            isinstance(quote_volume, bool)
            or not isinstance(quote_volume, (int, float))
            or not math.isfinite(float(quote_volume))
            or quote_volume < 0
        ):
            raise ValueError("delta quote volume must be finite nonnegative or null")
        else:
            quote_value = float(quote_volume)
        return close_time_ms, high_value, low_value, close_value, quote_value


@dataclass(frozen=True)
class _ValidatedMetricWindow:
    instrument_id: str
    close_times_ms: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: tuple[str, ...]
    quote_volume: np.ndarray
    cutoff_ms: int
    _high_vector: np.ndarray
    _low_vector: np.ndarray
    _close_vector: np.ndarray
