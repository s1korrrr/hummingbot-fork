"""Causal, completed-candle market context for the shared episode reducer."""

from decimal import Decimal

import numpy as np
import pandas as pd

HISTORY_RECORDS = 3660


class ContextUnavailable(ValueError):
    """Required completed market history cannot support a policy decision."""


def _atr(frame):
    previous = frame.close.shift(1)
    ranges = pd.concat([frame.high - frame.low, (frame.high - previous).abs(), (frame.low - previous).abs()], axis=1)
    return ranges.max(axis=1).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean().iloc[-1]


def _aggregate(frame, seconds):
    # The caller has already proved consecutive aligned 1m inputs. Exclude the
    # partial first/last buckets before indicators, using UTC bucket starts.
    buckets = (frame.timestamp // seconds) * seconds
    result = frame.groupby(buckets).agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"),
        close=("close", "last"), count=("timestamp", "size"))
    result = result.loc[result["count"] == seconds // 60].reset_index()
    return result


def completed_market_context(candles: pd.DataFrame, current_time: float) -> dict:
    required = ["timestamp", "open", "high", "low", "close"]
    if candles is None or not set(required).issubset(candles.columns):
        raise ContextUnavailable("missing OHLC timestamps")
    frame = candles.loc[:, required].apply(pd.to_numeric, errors="coerce")
    frame = frame.loc[frame.timestamp + 60 <= current_time].tail(HISTORY_RECORDS).copy()
    if len(frame) < 3060 or not np.isfinite(frame.to_numpy()).all():
        raise ContextUnavailable("insufficient or nonfinite completed history")
    if not frame.timestamp.diff().iloc[1:].eq(60).all() or not (frame.timestamp % 60).eq(0).all():
        raise ContextUnavailable("nonconsecutive or unaligned completed history")
    if current_time - (frame.timestamp.iloc[-1] + 60) >= 60:
        raise ContextUnavailable("stale completed history")
    if (frame[["open", "high", "low", "close"]] <= 0).any().any():
        raise ContextUnavailable("nonpositive OHLC")
    if ((frame.high < frame[["open", "close", "low"]].max(axis=1)) |
            (frame.low > frame[["open", "close", "high"]].min(axis=1))).any():
        raise ContextUnavailable("invalid OHLC envelope")
    medium = _aggregate(frame, 900)
    long = _aggregate(frame, 3600)
    if len(medium) < 51 or len(long) < 21:
        raise ContextUnavailable("insufficient complete higher timeframes")
    ema_medium20 = medium.close.ewm(span=20, adjust=False, min_periods=20).mean()
    ema_medium50 = medium.close.ewm(span=50, adjust=False, min_periods=50).mean()
    ema_long20 = long.close.ewm(span=20, adjust=False, min_periods=20).mean()
    anchor = long.high.iloc[-21:-1].max()
    atr_1m, atr_15m, atr_1h = _atr(frame), _atr(medium), _atr(long)
    if min(atr_1m, atr_15m, atr_1h) <= 0:
        raise ContextUnavailable("nonpositive ATR")
    return {
        "timestamp": int(frame.timestamp.iloc[-1] + 60),
        "htf_timestamp": int(long.timestamp.iloc[-1] + 3600),
        "low": Decimal(str(frame.low.iloc[-1])),
        "atr_1m": Decimal(str(atr_1m)), "atr_15m": Decimal(str(atr_15m)),
        "atr_1h": Decimal(str(atr_1h)), "anchor": Decimal(str(anchor)),
        "recovery": bool(medium.close.iloc[-1] > ema_medium20.iloc[-1] > ema_medium20.iloc[-2]),
        "markup": bool(long.close.iloc[-1] > anchor and ema_long20.iloc[-1] > ema_long20.iloc[-2]
                       and ema_medium20.iloc[-1] > ema_medium50.iloc[-1]),
        "reset": bool(long.close.iloc[-1] < ema_long20.iloc[-1]),
        "hostile": bool(long.close.iloc[-1] < ema_long20.iloc[-1] < ema_long20.iloc[-2]),
        "cascade": bool(frame.close.iloc[-1] < frame.close.iloc[-2] - 3 * atr_15m),
    }
