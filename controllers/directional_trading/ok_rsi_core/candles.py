"""UTC aggregation with complete, consecutive input candle validation."""

import math

import pandas as pd

from hummingbot.data_feed.candles_feed.candles_base import CandlesBase

_CANDLE_VALUE_COLUMNS = ("open", "high", "low", "close")
_CANDLE_SUM_COLUMNS = (
    "volume",
    "quote_asset_volume",
    "n_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
)


def derive_higher_timeframe_candles(
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
        expected_timestamps = list(
            range(int(bucket_timestamp), int(bucket_timestamp) + target_seconds, base_seconds)
        )
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
