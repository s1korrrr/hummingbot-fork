"""OK RSI reversal port, confluence score, and ATR band as modular signal policy."""
from decimal import Decimal as D
from types import SimpleNamespace as NS

import numpy as np
import pandas as pd
import pytest

from controllers.generic.modular_spot_core.config import SignalPolicy
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.market import (
    atr_fraction,
    confluence_score,
    features,
    reversal_turn,
    wilder_rsi,
)
from controllers.generic.modular_spot_core.signals import signal_decision


def reference_reversal(rsi, threshold, length):
    """Direct transcription of hl_rsi._compute_raw_buy_reversal_components."""
    window = max(3, length // 2)
    prev = rsi.shift(1)
    raw = ((prev == rsi.rolling(window=window, min_periods=2).min()) & (rsi > prev)
           & (rsi.rolling(window=window, min_periods=1).min() <= threshold) & (rsi <= threshold * 1.3))
    prior = raw.shift(1).rolling(window=window * 4, min_periods=1).max().fillna(0).astype(bool)
    return (raw & ~prior).fillna(False)


def synthetic_frame(count=400, seed=7):
    rng = np.random.default_rng(seed)
    close = 100 + np.cumsum(rng.normal(0, .3, count)) + 3 * np.sin(np.arange(count) / 15)
    return pd.DataFrame({'timestamp': np.arange(0, count * 60, 60), 'open': close, 'high': close + .2,
                         'low': close - .2, 'close': close, 'volume': np.ones(count)})


def trend(frame, **kwargs):
    return NS(is_valid=True, direction=NS(value='range'))


def test_reversal_turn_matches_the_ok_rsi_reference_on_every_prefix():
    frame = synthetic_frame()
    policy = SignalPolicy(kind='rsi_turn', rsi_length=14, oversold=D('31'))
    rsi = pd.Series(wilder_rsi(frame.close.to_numpy(), 14))
    reference = reference_reversal(rsi, 31.0, 14)
    fired = [reversal_turn(rsi.iloc[:end], policy) for end in range(40, len(rsi) + 1)]
    assert fired == reference.iloc[39:].tolist()
    assert any(fired), 'fixture must contain at least one reversal'


def test_confluence_score_requires_oversold_and_counts_each_agreeing_indicator():
    frame = synthetic_frame()
    policy = SignalPolicy(kind='rsi_turn', rsi_length=14, oversold=D('31'), bb_length=17, bb_std=D('2.25'),
                          macd_fast=14, macd_slow=26, macd_signal=11)
    rsi = pd.Series(wilder_rsi(frame.close.to_numpy(), 14))
    scores = [confluence_score(frame.iloc[:end], rsi.iloc[:end - 15], policy) for end in range(60, len(frame) + 1)]
    oversold = [float(rsi.iloc[end - 16]) <= 31 for end in range(60, len(frame) + 1)]
    assert all(0 <= s <= 4 for s in scores)
    assert all((s >= 1) == o for s, o in zip(scores, oversold))
    assert max(scores) >= 2


def test_atr_fraction_is_wilder_smoothed_and_none_during_warmup():
    frame = synthetic_frame(30)
    assert atr_fraction(frame.iloc[:10], 14) is None
    value = atr_fraction(frame, 14)
    assert value is not None and D('0') < value < D('.05')


def test_features_carry_turn_score_and_atr_and_keep_positional_compatibility():
    frame = synthetic_frame()
    policy = SignalPolicy(kind='rsi_turn', rsi_length=14, oversold=D('31'))
    result = features(frame, now=float(frame.timestamp.iloc[-1] + 60), interval=60, max_age=120,
                      policy=policy, trend_analyzer=trend)
    assert type(result.turn) is bool and type(result.score) is int and result.atr_fraction is not None
    legacy = features(frame, now=float(frame.timestamp.iloc[-1] + 60), interval=60, max_age=120,
                      policy=SignalPolicy(), trend_analyzer=trend)
    assert (legacy.candle_open, legacy.close, legacy.previous_rsi, legacy.rsi) == (
        result.candle_open, result.close, result.previous_rsi, result.rsi)


def test_rsi_turn_decision_needs_turn_and_score():
    policy = SignalPolicy(kind='rsi_turn', minimum_score=2)
    common = dict(previous_rsi=D('20'), rsi=D('24'), breakout=False, trend_up=False)
    assert signal_decision(policy, **common, turn=True, score=2).enter
    assert signal_decision(policy, **common, turn=True, score=1).reason == 'score_below_minimum'
    assert signal_decision(policy, **common, turn=False, score=4).reason == 'no_turn'
    with pytest.raises(PolicyError):
        signal_decision(policy, **common, turn='yes', score=2)


def test_atr_band_gates_every_kind_and_fails_closed_without_atr():
    policy = SignalPolicy(min_atr_fraction=D('.0002'), max_atr_fraction=D('.008'))
    rebound = dict(previous_rsi=D('28'), rsi=D('31'), breakout=False, trend_up=False)
    assert signal_decision(policy, **rebound, atr_fraction=D('.001')).enter
    assert signal_decision(policy, **rebound, atr_fraction=D('.0001')).reason == 'low_volatility'
    assert signal_decision(policy, **rebound, atr_fraction=D('.01')).reason == 'high_volatility'
    assert signal_decision(policy, **rebound, atr_fraction=None).reason == 'atr_unavailable'
    assert signal_decision(SignalPolicy(), **rebound, atr_fraction=None).enter


def test_policy_rejects_inconsistent_bands_and_extends_required_history():
    with pytest.raises(ValueError):
        SignalPolicy(macd_fast=26, macd_slow=26)
    with pytest.raises(ValueError):
        SignalPolicy(min_atr_fraction=D('.01'), max_atr_fraction=D('.001'))
    assert SignalPolicy().required_history == 60
    assert SignalPolicy(bb_length=100).required_history == 102
    assert SignalPolicy(rsi_length=40).required_history == 40 + 5 * 20 + 2
