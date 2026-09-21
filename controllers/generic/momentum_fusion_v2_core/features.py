"""Stateless candle features. Setup arming lives in setups.py, not a feature closure."""
import json
from hashlib import sha256
from pathlib import Path

import pandas as pd

from hummingbot.strategy_v2.components.domain import DataUnavailable, decimal
from hummingbot.strategy_v2.components.feature_registry import FeatureSpec

INTERVAL_SECONDS = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}


def implementation_sha256():
    return sha256(Path(__file__).read_bytes()).hexdigest()


def implementation_symbol(function):
    return function.__module__ + '.' + function.__qualname__


def is_closed_minute(open_time, now):
    return int(open_time) + INTERVAL_SECONDS['1m'] <= int(now)


def prior_channel_break(frame, dependencies, parameters):
    lookback = int(parameters['lookback'])
    close = decimal(frame['close'].iloc[-1], 'close', positive=True)
    prior_high = decimal(frame['high'].iloc[-(lookback + 1):-1].max(), 'prior high', positive=True)
    return {'break': int(close > prior_high), 'prior_high': prior_high, 'close': close}


def impulse_move(frame, dependencies, parameters):
    close = decimal(frame['close'].iloc[-1], 'close', positive=True)
    previous = decimal(frame['close'].iloc[-2], 'prior close', positive=True)
    last_return = (close / previous) - 1
    high = decimal(frame['high'].iloc[-1], 'high', positive=True)
    low = decimal(frame['low'].iloc[-1], 'low', positive=True)
    last_volume = decimal(frame['volume'].iloc[-1], 'volume')
    prior_volume = decimal(frame['volume'].iloc[-(int(parameters['lookback']) + 1):-1].mean(), 'prior volume', positive=True)
    volume_ratio = last_volume / prior_volume
    bar_range = high - low
    prior_range = decimal((frame['high'] - frame['low']).iloc[-(int(parameters['lookback']) + 1):-1].mean(), 'prior range', positive=True)
    range_ratio = bar_range / prior_range
    armed = int(
        last_return >= decimal(parameters['min_return'], 'min return', minimum=None)
        and range_ratio >= decimal(parameters['min_range_ratio'], 'min range ratio', positive=True)
        and volume_ratio >= decimal(parameters['min_volume_ratio'], 'min volume ratio', positive=True)
    )
    return {
        'armed': armed,
        'last_return': last_return,
        'range_ratio': range_ratio,
        'volume_ratio': volume_ratio,
        'high': high,
        'low': low,
        'close': close,
    }


def atr_value(frame, dependencies, parameters):
    period = int(parameters['period'])
    previous = frame['close'].shift(1)
    true_range = pd.concat([
        frame['high'] - frame['low'],
        (frame['high'] - previous).abs(),
        (frame['low'] - previous).abs(),
    ], axis=1).max(axis=1)
    value = decimal(true_range.iloc[-period:].mean(), 'atr', positive=True)
    close = decimal(frame['close'].iloc[-1], 'close', positive=True)
    return {'atr': value, 'atr_fraction': value / close, 'close': close}


def bandwidth_ratio(frame, dependencies, parameters):
    lookback = int(parameters['lookback'])
    last_range = decimal(frame['high'].iloc[-1] - frame['low'].iloc[-1], 'last range')
    prior = decimal((frame['high'] - frame['low']).iloc[-(lookback + 1):-1].mean(), 'prior range', positive=True)
    return {'bandwidth': last_range / prior, 'last_range': last_range, 'close': decimal(frame['close'].iloc[-1], 'close', positive=True)}


def htf_uptrend(frame, dependencies, parameters):
    bars = int(parameters['trend_bars'])
    closes = frame['close']
    last = decimal(closes.iloc[-1], 'close', positive=True)
    first = decimal(closes.iloc[-bars], 'htf start', positive=True)
    higher_lows = bool((frame['low'].iloc[-bars:] >= frame['low'].iloc[-bars:].shift(1).bfill()).iloc[1:].all())
    return {'uptrend': int(last > first and higher_lows), 'reference': last, 'close': last}


def higher_timeframe(frame, *, base_interval, target_interval, now, max_records):
    try:
        base = INTERVAL_SECONDS[base_interval]
        target = INTERVAL_SECONDS[target_interval]
    except KeyError as error:
        raise ValueError('unsupported candle interval') from error
    if target <= base or target % base:
        raise ValueError('target interval must be a larger whole multiple of the base interval')
    if frame.empty or not {'timestamp', 'open', 'high', 'low', 'close'}.issubset(frame.columns):
        raise DataUnavailable('higher timeframe constituents are incomplete')
    expected = target // base
    rows = frame.copy()
    rows['timestamp'] = pd.to_numeric(rows['timestamp'], errors='coerce').astype('int64')
    rows['bucket'] = (rows['timestamp'] // target) * target
    rows = rows[rows['bucket'] + target <= int(now)]
    complete = []
    for bucket, group in rows.groupby('bucket', sort=True):
        times = group['timestamp'].to_numpy()
        if len(times) != expected:
            continue
        expected_times = bucket + (pd.Series(range(expected)) * base).to_numpy()
        if not (times == expected_times).all():
            continue
        complete.append({
            'timestamp': int(bucket),
            'open': group['open'].iloc[0],
            'high': group['high'].max(),
            'low': group['low'].min(),
            'close': group['close'].iloc[-1],
            'volume': group['volume'].sum() if 'volume' in group else 0,
        })
    if not complete:
        raise DataUnavailable('higher timeframe constituents are incomplete')
    return pd.DataFrame(complete).tail(max_records).reset_index(drop=True)


def spec_for(name, function, dataset_key, output_fields, parameters, *, min_records, initialization_sha256, version=1):
    return FeatureSpec(
        name=name, version=version, dataset_key=dataset_key,
        implementation_sha256=implementation_sha256(),
        implementation_symbol=implementation_symbol(function),
        initialization_sha256=initialization_sha256,
        parameters_json=json.dumps(parameters, sort_keys=True, separators=(',', ':')),
        required_fields=('open', 'high', 'low', 'close', 'volume'),
        output_fields=output_fields, min_records=min_records,
    )
