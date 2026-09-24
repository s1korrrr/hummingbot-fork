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
    turn: bool = False
    score: int = 0
    atr_fraction: Decimal | None = None


def wilder_rsi(close: np.ndarray, length: int) -> np.ndarray:
    """Wilder smoothing with an explicit initial arithmetic mean, not a future fit."""
    changes = np.diff(close)
    gains, losses = np.maximum(changes, 0), np.maximum(-changes, 0)
    gain, loss = gains[:length].mean(), losses[:length].mean()
    values = []
    for i in range(length, len(changes)):
        gain = (gain * (length - 1) + gains[i]) / length
        loss = (loss * (length - 1) + losses[i]) / length
        values.append(50.0 if gain == loss == 0 else 100.0 if loss == 0 else 100 - 100 / (1 + gain / loss))
    return np.array(values)


def reversal_turn(rsi: pd.Series, policy: SignalPolicy) -> bool:
    """OK RSI debounced buy reversal on the last closed candle."""
    window = policy.reversal_window
    threshold = float(policy.oversold)
    previous = rsi.shift(1)
    prev_was_min = previous == rsi.rolling(window=window, min_periods=2).min()
    turning_up = rsi > previous
    was_oversold = rsi.rolling(window=window, min_periods=1).min() <= threshold
    near_bottom = rsi <= threshold * float(policy.near_bottom_multiplier)
    raw = (prev_was_min & turning_up & was_oversold & near_bottom).fillna(False)
    prior_fire = raw.shift(1).rolling(window=window * 4, min_periods=1).max().fillna(0).astype(bool)
    return bool((raw & ~prior_fire).iloc[-1])


def confluence_score(data: pd.DataFrame, rsi: pd.Series, policy: SignalPolicy) -> int:
    """OK RSI entry score: oversold RSI is required, then Bollinger touch, MACD turn, RSI extremeness."""
    last_rsi = float(rsi.iloc[-1])
    if not last_rsi <= float(policy.oversold):
        return 0
    score = 1
    rolling = data.close.rolling(policy.bb_length)
    lower = (rolling.mean() - rolling.std(ddof=0) * float(policy.bb_std)).iloc[-1]
    if pd.notna(lower) and float(data.close.iloc[-1]) <= float(lower):
        score += 1
    fast = data.close.ewm(span=policy.macd_fast, adjust=False).mean()
    slow = data.close.ewm(span=policy.macd_slow, adjust=False).mean()
    macd = fast - slow
    hist = macd - macd.ewm(span=policy.macd_signal, adjust=False).mean()
    current, previous = hist.iloc[-1], hist.iloc[-2]
    if pd.notna(current) and pd.notna(previous) and current > previous and (current > 0 or previous < 0):
        score += 1
    if (abs(last_rsi - 50) / 50) ** 1.5 >= float(policy.mean_reversion_minimum):
        score += 1
    return score


def atr_fraction(data: pd.DataFrame, length: int) -> Decimal | None:
    previous_close = data.close.shift(1)
    true_range = pd.concat([data.high - data.low, (data.high - previous_close).abs(),
                            (data.low - previous_close).abs()], axis=1).max(axis=1)
    atr = true_range.ewm(alpha=1.0 / length, min_periods=length, adjust=False).mean().iloc[-1]
    if pd.isna(atr):
        return None
    return Decimal(str(float(atr) / float(data.close.iloc[-1])))


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
    if len(data) < policy.required_history:
        raise PolicyError('candle warmup unavailable')
    if now - (data.timestamp.iloc[-1] + interval) > max_age:
        raise PolicyError('stale candles')
    if not np.isfinite(data.to_numpy()).all() or (data[['open', 'high', 'low', 'close']] <= 0).any().any():
        raise PolicyError('invalid candle values')
    if (data.volume < 0).any() or (data.low > data[['open', 'close']].min(axis=1)).any() or (
            data.high < data[['open', 'close']].max(axis=1)).any():
        raise PolicyError('invalid OHLC geometry')
    values = wilder_rsi(data.close.to_numpy(), policy.rsi_length)
    trend = trend_analyzer(data.copy(deep=True), timeframe=str(interval), adx_length=14, adx_threshold=20)
    if trend is None or trend.is_valid is not True or trend.direction.value not in {'up', 'down', 'range'}:
        raise PolicyError('native trend context unavailable')
    prior_high = data.high.iloc[-policy.breakout_lookback - 1:-1].max()
    rsi = pd.Series(values)
    return MarketFeatures(int(data.timestamp.iloc[-1]), Decimal(str(data.close.iloc[-1])),
                          Decimal(str(values[-2])), Decimal(str(values[-1])),
                          bool(data.close.iloc[-1] > prior_high), trend.direction.value == 'up',
                          turn=reversal_turn(rsi, policy), score=confluence_score(data, rsi, policy),
                          atr_fraction=atr_fraction(data, policy.atr_length))
