"""The vectorized paper candle validator admits and refuses exactly what the pandas rules did."""
import numpy as np
import pandas as pd
import pytest

from hummingbot.strategy_v2.paper_session import InjectedPaperMarketData

SECONDS, AVAILABLE = 60, 600000


def reference_rules(frame, available_at=AVAILABLE, seconds=SECONDS):
    """The pandas rules in force before the vectorized rewrite (numeric and confirmed checks)."""
    numeric = frame[['timestamp', 'open', 'high', 'low', 'close', 'volume']].astype(float)
    timestamps = numeric['timestamp']
    if (not np.isfinite(numeric.to_numpy()).all() or not timestamps.is_monotonic_increasing
            or timestamps.duplicated().any() or (timestamps + seconds > available_at).any()
            or (timestamps < 0).any() or (timestamps % seconds != 0).any()
            or (timestamps.diff().dropna() != seconds).any()
            or (numeric[['open', 'high', 'low', 'close']] <= 0).any().any()
            or (numeric['volume'] < 0).any()
            or (numeric['low'] > numeric[['open', 'close']].min(axis=1)).any()
            or (numeric['high'] < numeric[['open', 'close']].max(axis=1)).any()):
        return False
    if 'confirmed' in frame and not frame['confirmed'].map(
            lambda value: isinstance(value, (bool, np.bool_)) and bool(value)).all():
        return False
    return True


def frame(rows=50):
    close = 100 + np.sin(np.arange(rows))
    return pd.DataFrame(dict(timestamp=AVAILABLE - 60 * np.arange(rows, 0, -1), open=close, high=close + 1,
                             low=close - 1, close=close, volume=np.full(rows, 5.0)))


def mutate(kind):
    f = frame()
    if kind == 'nan':
        f.loc[3, 'close'] = np.nan
    elif kind == 'inf_volume':
        f.loc[3, 'volume'] = np.inf
    elif kind == 'duplicate':
        f.loc[4, 'timestamp'] = f.loc[3, 'timestamp']
    elif kind == 'reversed':
        f = f.iloc[::-1].reset_index(drop=True)
    elif kind == 'gap':
        f.loc[10:, 'timestamp'] += 60
    elif kind == 'future':
        f['timestamp'] += 60
    elif kind == 'unaligned':
        f['timestamp'] += 1
    elif kind == 'negative_time':
        f['timestamp'] -= AVAILABLE + 60 * 100
    elif kind == 'zero_price':
        f.loc[2, 'open'] = 0
    elif kind == 'negative_volume':
        f.loc[2, 'volume'] = -1
    elif kind == 'low_above_body':
        f.loc[2, 'low'] = f.loc[2, 'close'] + .5
    elif kind == 'high_below_body':
        f.loc[2, 'high'] = f.loc[2, 'close'] - .5
    elif kind == 'confirmed_true':
        f['confirmed'] = True
    elif kind == 'confirmed_one_false':
        f['confirmed'] = True
        f.loc[5, 'confirmed'] = False
    elif kind == 'confirmed_numpy_object':
        f['confirmed'] = pd.Series([np.bool_(True)] * len(f), dtype=object)
    elif kind == 'confirmed_int':
        f['confirmed'] = 1
    elif kind == 'confirmed_string':
        f['confirmed'] = 'true'
    elif kind == 'confirmed_nullable':
        f['confirmed'] = pd.array([True] * len(f), dtype='boolean')
    elif kind == 'single_row':
        f = f.tail(1).reset_index(drop=True)
    elif kind == 'int_columns':
        f = f.astype({'timestamp': 'int64'})
    return f


KINDS = ['valid', 'nan', 'inf_volume', 'duplicate', 'reversed', 'gap', 'future', 'unaligned', 'negative_time',
         'zero_price', 'negative_volume', 'low_above_body', 'high_below_body', 'confirmed_true',
         'confirmed_one_false', 'confirmed_numpy_object', 'confirmed_int', 'confirmed_string', 'confirmed_nullable',
         'single_row', 'int_columns']


@pytest.mark.parametrize('kind', KINDS)
def test_vectorized_validator_matches_the_pandas_rules(kind):
    provider = InjectedPaperMarketData.__new__(InjectedPaperMarketData)
    provider._paper_connector = type('C', (), {'trading_pairs': ['ETH-USDC'], 'current_timestamp': AVAILABLE})()
    f = mutate(kind)
    expected = reference_rules(f)
    try:
        validated = provider.validate_candles_input('okx', 'ETH-USDC', '1m', f, available_at=AVAILABLE)
    except ValueError:
        accepted = False
    else:
        accepted = True
        pd.testing.assert_frame_equal(validated, f)
    assert accepted is expected, kind
