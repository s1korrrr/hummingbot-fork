from decimal import Decimal as D

import pandas as pd
import pytest

from controllers.generic.momentum_fusion_v2_core import features
from hummingbot.strategy_v2.components.candle_data import Candle, CandleSnapshot, Dataset
from hummingbot.strategy_v2.components.domain import DataUnavailable
from hummingbot.strategy_v2.components.feature_registry import FeatureRegistry
from hummingbot.strategy_v2.components.recipe_contract import digest


def frame(n=60, close=100.0, last_high=None, last_close=None, last_volume=1.0, start=0):
    rows = []
    for i in range(n):
        high = close + 1
        rows.append(dict(timestamp=start + i * 60, open=close, high=high, low=close - 1, close=close, volume=1.0))
    if last_high is not None:
        rows[-1]['high'] = last_high
    if last_close is not None:
        rows[-1]['close'] = last_close
        rows[-1]['open'] = last_close
    rows[-1]['volume'] = last_volume
    return pd.DataFrame(rows)


def snapshot(rows=60, now=3600):
    dataset = Dataset(connector='okx', trading_pair='BNB-USDC', history=rows)
    candles = tuple(Candle(open_time=t, open=100, high=101, low=99, close=100, volume=1)
                    for t in range(0, rows * 60, 60))
    published = candles[-1].open_time + 60
    return CandleSnapshot(dataset=dataset, epoch='e', sequence=1, published_at=published, rows=candles)


def test_open_minute_is_not_closed():
    assert features.is_closed_minute(0, 59) is False
    assert features.is_closed_minute(0, 60) is True


def test_prior_channel_excludes_the_candidate_bar():
    data = features.prior_channel_break(frame(last_high=120, last_close=110), {}, {'lookback': 20})
    assert data['break'] == 1
    assert data['prior_high'] == D('101')
    flat = features.prior_channel_break(frame(last_close=100, last_high=101), {}, {'lookback': 20})
    assert flat['break'] == 0


def test_missing_confirmed_is_not_treated_as_true():
    class Missing:
        pass

    class Denied:
        confirmed = False

    class Allowed:
        confirmed = True

    assert features.is_explicitly_confirmed(Missing()) is False
    assert features.is_explicitly_confirmed(Denied()) is False
    assert features.is_explicitly_confirmed(Allowed()) is True


def test_unconfirmed_candle_cannot_enter_a_snapshot():
    with pytest.raises(ValueError):
        Candle(open_time=0, open=1, high=1, low=1, close=1, volume=0, confirmed=False)


def test_incomplete_higher_timeframe_constituents_hold():
    short = frame(n=4, start=0)
    with pytest.raises(DataUnavailable, match='incomplete'):
        features.higher_timeframe(short, base_interval='1m', target_interval='5m', now=240, max_records=10)


def test_complete_five_minute_aggregate_uses_only_finished_buckets():
    data = frame(n=10, start=0)
    out = features.higher_timeframe(data, base_interval='1m', target_interval='5m', now=600, max_records=10)
    assert list(out['timestamp']) == [0, 300]


def test_feature_registry_pins_source_and_rejects_warmup_shortfall():
    registry = FeatureRegistry()
    spec = features.spec_for(
        'channel', features.prior_channel_break, snapshot().dataset.key,
        ('break', 'prior_high', 'close'), {'lookback': 20}, min_records=60,
        initialization_sha256=digest({'v': 1}))
    registry.register(spec, features.prior_channel_break)
    result = registry.evaluate('channel', {snapshot().dataset.key: snapshot()}, now=3601)
    assert result.as_dict()['break'] == 0
    stale = snapshot()
    stale = stale.model_copy(update={'published_at': 10_000})
    with pytest.raises(DataUnavailable):
        registry.evaluate('channel', {stale.dataset.key: stale}, now=10_001)
