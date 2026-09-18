"""Causal, controller-local candle features; the shared feed is never mutated."""
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable

import numpy as np
import pandas as pd

from .config import SignalPolicy
from .domain import PolicyError, decimal


@dataclass(frozen=True)
class MarketFeatures:
    candle_open: int
    close: Decimal
    previous_rsi: Decimal
    rsi: Decimal
    breakout: bool
    trend_up: bool


def features(frame: pd.DataFrame, *, now: float, interval: int, max_age: int,
             policy: SignalPolicy, trend_analyzer: Callable) -> MarketFeatures:
    now = float(decimal(now, 'now'))
    if type(interval) is not int or interval <= 0 or type(max_age) is not int or max_age <= 0:
        raise PolicyError('invalid candle clock')
    names = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    if not isinstance(frame, pd.DataFrame) or not set(names).issubset(frame.columns):
        raise PolicyError('candle columns unavailable')
    try:
        data = frame[names].copy(deep=True).astype(float)
    except (TypeError, ValueError) as exc:
        raise PolicyError('non-numeric candles') from exc
    stamps = data.timestamp.to_numpy()
    if not len(stamps) or not np.isfinite(stamps).all() or (stamps > now).any():
        raise PolicyError('invalid or future candle timestamps')
    if (stamps < 0).any() or (stamps % interval != 0).any() or not (np.diff(stamps) == interval).all():
        raise PolicyError('unordered, duplicated or gapped candles')
    data = data.loc[data.timestamp + interval <= now].copy()
    required = max(60, 2 * policy.rsi_length + 2, policy.breakout_lookback + 2)
    if len(data) < required:
        raise PolicyError('candle warmup unavailable')
    if now - (data.timestamp.iloc[-1] + interval) > max_age:
        raise PolicyError('stale candles')
    if not np.isfinite(data.to_numpy()).all() or (data[['open', 'high', 'low', 'close']] <= 0).any().any():
        raise PolicyError('invalid candle values')
    if (data.volume < 0).any() or (data.low > data[['open', 'close']].min(axis=1)).any() or (
            data.high < data[['open', 'close']].max(axis=1)).any():
        raise PolicyError('invalid OHLC geometry')
    # Wilder smoothing with an explicit initial arithmetic mean, not a future fit.
    changes = np.diff(data.close.to_numpy())
    gains, losses = np.maximum(changes, 0), np.maximum(-changes, 0)
    n = policy.rsi_length
    gain, loss = gains[:n].mean(), losses[:n].mean()
    values = []
    for i in range(n, len(changes)):
        gain = (gain * (n - 1) + gains[i]) / n
        loss = (loss * (n - 1) + losses[i]) / n
        values.append(50.0 if gain == loss == 0 else 100.0 if loss == 0 else 100 - 100 / (1 + gain / loss))
    trend = trend_analyzer(data.copy(deep=True), timeframe=str(interval), adx_length=14, adx_threshold=20)
    if trend is None or trend.is_valid is not True or trend.direction.value not in {'up', 'down', 'range'}:
        raise PolicyError('native trend context unavailable')
    prior_high = data.high.iloc[-policy.breakout_lookback - 1:-1].max()
    return MarketFeatures(int(data.timestamp.iloc[-1]), Decimal(str(data.close.iloc[-1])),
                          Decimal(str(values[-2])), Decimal(str(values[-1])),
                          bool(data.close.iloc[-1] > prior_high), trend.direction.value == 'up')
