"""Exact parity with the pre-vectorization owner algorithm; no exchange I/O."""

import math
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from controllers.directional_trading.ok_rsi_core.candles import (
    _CANDLE_SUM_COLUMNS,
    _CANDLE_VALUE_COLUMNS,
    derive_higher_timeframe_candles,
)
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase


def original_aggregation(
    base_candles: pd.DataFrame,
    *,
    base_interval: str,
    target_interval: str,
    current_time: float,
    max_records: int,
) -> pd.DataFrame:
    """Aggregate complete, consecutive base candles into UTC-aligned higher-timeframe bars."""
    output_columns = [
        "timestamp",
        *[column for column in (*_CANDLE_VALUE_COLUMNS, *_CANDLE_SUM_COLUMNS) if column in base_candles.columns],
    ]
    if base_candles.empty or not {"timestamp", *_CANDLE_VALUE_COLUMNS}.issubset(base_candles.columns):
        return pd.DataFrame(columns=output_columns)

    base_seconds = CandlesBase.interval_to_seconds[base_interval]
    target_seconds = CandlesBase.interval_to_seconds[target_interval]
    if target_seconds <= base_seconds or target_seconds % base_seconds != 0:
        raise ValueError("target interval must be a larger whole multiple of the base interval")

    frame = base_candles[output_columns].copy()
    frame["timestamp"] = pd.to_numeric(frame["timestamp"], errors="coerce")
    numeric_columns = [column for column in output_columns if column != "timestamp"]
    frame[numeric_columns] = frame[numeric_columns].apply(pd.to_numeric, errors="coerce")
    frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp")
    frame["timestamp"] = frame["timestamp"].astype("int64")
    frame["bucket_timestamp"] = (frame["timestamp"] // target_seconds) * target_seconds
    frame = frame[frame["bucket_timestamp"] + target_seconds <= int(current_time)]

    expected_count = target_seconds // base_seconds
    records = []
    for bucket_timestamp, bucket in frame.groupby("bucket_timestamp", sort=True):
        expected_timestamps = list(range(int(bucket_timestamp), int(bucket_timestamp) + target_seconds, base_seconds))
        if bucket["timestamp"].tolist() != expected_timestamps or len(bucket) != expected_count:
            continue
        if bucket[numeric_columns].isna().any().any():
            continue
        if not bucket[numeric_columns].map(math.isfinite).all().all():
            continue
        record = {
            "timestamp": int(bucket_timestamp),
            "open": bucket["open"].iloc[0],
            "high": bucket["high"].max(),
            "low": bucket["low"].min(),
            "close": bucket["close"].iloc[-1],
        }
        for column in _CANDLE_SUM_COLUMNS:
            if column in bucket.columns:
                record[column] = bucket[column].sum()
        else:
            records.append(record)

    return pd.DataFrame(records, columns=output_columns).tail(max_records).reset_index(drop=True)


def candles(count=1500):
    rng = np.random.default_rng(991)
    frame = pd.DataFrame(
        {
            "timestamp": np.arange(count) * 60 + 1788825600,
            "open": rng.uniform(1, 100, count),
            "high": rng.uniform(100, 200, count),
            "low": rng.uniform(0, 1, count),
            "close": rng.uniform(1, 100, count),
        }
    )
    for column in _CANDLE_SUM_COLUMNS:
        frame[column] = rng.uniform(0, 1000, count)
    frame["n_trades"] = np.arange(count, dtype=np.int64)
    return frame


def assert_parity(frame, *, interval="5m", now=1788825600 + 1500 * 60, keep=100):
    before = frame.copy(deep=True)
    kwargs = dict(base_interval="1m", target_interval=interval, current_time=now, max_records=keep)
    expected = original_aggregation(frame, **kwargs)
    actual = derive_higher_timeframe_candles(frame, **kwargs)
    assert_frame_equal(actual, expected, check_exact=True)
    assert_frame_equal(frame, before, check_exact=True)


def test_aggregation_does_not_allocate_validation_frames_for_every_bucket():
    frame = candles()
    calls = []
    original = pd.DataFrame.isna

    def counted(value):
        calls.append(len(value))
        return original(value)

    with patch.object(pd.DataFrame, "isna", counted):
        result = derive_higher_timeframe_candles(
            frame, base_interval="1m", target_interval="5m", current_time=1788825600 + 1500 * 60, max_records=100
        )
    assert len(result) == 100
    assert len(calls) <= 2, "whole-frame validation must replace per-bucket DataFrames"


@pytest.mark.parametrize(
    "case",
    [
        "complete",
        "missing",
        "duplicate",
        "shuffled",
        "nonfinite",
        "utc",
        "fractional",
        "strings",
        "no_volume",
        "negative",
        "float32",
        "nullable",
    ],
)
@pytest.mark.parametrize("interval", ["5m", "15m"])
def test_edge_cases_match_original_exactly(case, interval):
    frame = candles(80)
    if case == "missing":
        frame = frame.drop(index=[7, 20, 49])
    elif case == "duplicate":
        frame = pd.concat([frame, frame.iloc[[7]]], ignore_index=True)
    elif case == "shuffled":
        frame = frame.sample(frac=1, random_state=3)
    elif case == "nonfinite":
        frame.loc[7, "high"] = np.inf
        frame.loc[20, "volume"] = np.nan
        frame.loc[49, "low"] = -np.inf
    elif case == "utc":
        frame["timestamp"] -= 61
    elif case == "fractional":
        frame["timestamp"] += 0.25
    elif case == "strings":
        frame = frame.astype(str)
    elif case == "no_volume":
        frame = frame.drop(columns=list(_CANDLE_SUM_COLUMNS))
    elif case == "negative":
        frame.loc[:, "open"] = -1
    elif case == "float32":
        for column in _CANDLE_SUM_COLUMNS:
            frame[column] = frame[column].astype("float32")
    elif case == "nullable":
        frame["volume"] = frame["volume"].astype("Float64")
        frame.loc[7, "volume"] = pd.NA
    assert_parity(frame, interval=interval, now=1788825600 + 71 * 60 + 17, keep=7)


@pytest.mark.parametrize("seed", range(8))
def test_randomized_corrections_and_truncation_match_original(seed):
    rng = np.random.default_rng(seed)
    frame = candles(200)
    frame = frame.drop(index=rng.choice(len(frame), size=8, replace=False))
    frame = pd.concat([frame, frame.iloc[[4, 40]]], ignore_index=True).sample(frac=1, random_state=seed)
    for keep in (0, 1, 100, -2):
        assert_parity(frame, interval="15m", now=1788825600 + 199 * 60, keep=keep)
    frame.loc[frame.index[5], "volume"] = 99999.12345
    assert_parity(frame, keep=10)


@pytest.mark.parametrize("frame", [pd.DataFrame(), pd.DataFrame({"timestamp": [0]}), candles(2)])
def test_empty_and_incomplete_match_original(frame):
    assert_parity(frame)


@pytest.mark.parametrize(
    "dtype",
    ["Int64", "UInt64", "Int32", "UInt32", "int64[pyarrow]", "uint64[pyarrow]", "int32[pyarrow]", "float32[pyarrow]"],
)
@pytest.mark.parametrize("column", ["n_trades", "open", "high", "low", "close"])
def test_nullable_integer_values_and_dtypes_survive_rejected_bucket(dtype, column):
    if "pyarrow" in dtype:
        pytest.importorskip("pyarrow")
    frame = candles(20)
    value = 2**53 + 1 if "64" in dtype else 1000001
    frame[column] = pd.Series([value] * len(frame), dtype=dtype)
    frame.loc[0, column] = pd.NA
    assert_parity(frame)
