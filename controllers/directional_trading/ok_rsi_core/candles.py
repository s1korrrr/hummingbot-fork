"""UTC aggregation with complete, consecutive input candle validation."""

import numpy as np
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
    # Sorted input makes each bucket contiguous. Validate all candidate buckets
    # in arrays instead of allocating and reducing several DataFrames per bucket.
    buckets, starts, counts = np.unique(frame["bucket_timestamp"].to_numpy(), return_index=True, return_counts=True)
    complete = counts == expected_count
    buckets, starts = buckets[complete], starts[complete]
    if not len(buckets):
        return pd.DataFrame(columns=output_columns)
    indices = starts[:, None] + np.arange(expected_count)
    consecutive = (
        frame["timestamp"].to_numpy()[indices] == buckets[:, None] + np.arange(expected_count) * base_seconds
    ).all(axis=1)
    finite_rows = np.isfinite(frame[numeric_columns].to_numpy(dtype="float64", na_value=np.nan)).all(axis=1)
    valid = consecutive & finite_rows[indices].all(axis=1)
    buckets, indices = buckets[valid], indices[valid]
    columns = {}
    for column in numeric_columns:
        values = frame[column]
        if pd.api.types.is_integer_dtype(values.dtype):
            # A null in a rejected bucket must not coerce valid integer values
            # to float (and round values above 2**53). The placeholder is never
            # selected: indices include only fully finite buckets.
            dtype = getattr(values.dtype, "numpy_dtype", values.dtype)
            columns[column] = values.to_numpy(dtype=dtype, na_value=0)[indices]
        else:
            columns[column] = values.to_numpy()[indices]
    aggregated = {"timestamp": buckets}
    for column, values in columns.items():
        if isinstance(frame[column].dtype, pd.ArrowDtype):
            # Arrow returns Python scalars and has its own sum accumulator dtype.
            # Preserve that uncommon input contract through its native reducers;
            # ordinary owner NumPy candle columns keep the vectorized fast path.
            result = []
            for index in indices:
                bucket = frame[column].iloc[index]
                if column in {"open", "close"}:
                    result.append(bucket.iloc[0 if column == "open" else -1])
                else:
                    reducer = "max" if column == "high" else "min" if column == "low" else "sum"
                    result.append(getattr(bucket, reducer)())
            aggregated[column] = result
        elif column in {"open", "close"}:
            aggregated[column] = values[:, 0 if column == "open" else -1]
        elif column == "high":
            aggregated[column] = values.max(axis=1)
        elif column == "low":
            aggregated[column] = values.min(axis=1)
        else:
            aggregated[column] = values.sum(axis=1)
    # Scalar records retain pandas' original dtype inference, including empty
    # results and nullable source columns. Tail semantics (including negative
    # max_records) remain unchanged.
    records = [dict(zip(aggregated, values)) for values in zip(*aggregated.values())]

    return pd.DataFrame(records, columns=output_columns).tail(max_records).reset_index(drop=True)
