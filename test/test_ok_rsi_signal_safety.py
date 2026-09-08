import math
from test.test_ok_rsi_admission_safety import controller

import numpy as np
import pandas as pd
import pytest

from hummingbot.core.data_type.common import OrderType


def candles():
    close = 100 + np.sin(np.arange(500) / 3)
    return pd.DataFrame({'timestamp': np.arange(500) * 60, 'open': close, 'close': close,
                         'high': close + 1, 'low': close - 1, 'volume': 10 + np.cos(np.arange(500) / 5)})


@pytest.mark.parametrize('fault', ['future', 'infinite_timestamp', 'gap', 'volume', 'negative_price', 'inverted_high'])
def test_invalid_market_inputs_fail_closed(fault):
    instance = controller()
    frame = candles()
    if fault == 'future':
        frame.loc[499, 'timestamp'] = 30060
    elif fault == 'infinite_timestamp':
        frame['timestamp'] = frame['timestamp'].astype(float)
        frame.loc[499, 'timestamp'] = float('inf')
    elif fault == 'gap':
        frame = frame.drop(index=200)
    elif fault == 'volume':
        frame.loc[400:, 'volume'] = float('nan')
    elif fault == 'negative_price':
        frame.loc[499, 'close'] = -1
    else:
        frame.loc[499, 'high'] = 1
    assert not instance._build_data_health(frame, 30000)['ready']


@pytest.mark.asyncio
async def test_live_bollinger_bands_honor_nondefault_width():
    instance = controller()
    instance.config = type(instance.config).model_validate(dict(instance.config.model_dump(), bb_length=17, bb_std=2.25))
    frame = candles()
    instance.market_data_provider.get_candles_df.return_value = frame
    instance.market_data_provider.time.return_value = 30000
    await instance.update_processed_data()
    features = instance.processed_data['features']
    key = 'BBL_17_2.25'
    assert key in features.columns
    values = frame.close.iloc[-17:].to_numpy()
    expected = values.mean() - 2.25 * math.sqrt(sum((values - values.mean())**2) / 17)
    assert features[key].iloc[-1] == pytest.approx(expected)


@pytest.mark.asyncio
async def test_forming_candle_is_excluded_from_live_signal_features():
    instance = controller()
    instance.market_data_provider.get_candles_df.return_value = candles()
    instance.market_data_provider.time.return_value = 29950
    await instance.update_processed_data()
    assert instance.processed_data['candle_timestamp'] == 29880
    assert instance.processed_data['features']['timestamp'].iloc[-1] == 29880


def test_coupled_config_update_is_atomic_and_updates_new_executor_settings():
    instance = controller()
    values = dict(instance.config.model_dump(), trailing_stop_order_type=OrderType.LIMIT, trailing_stop_limit_max_age_seconds=3)
    instance.config = type(instance.config).model_validate(values)
    requested = type(instance.config).model_validate(dict(values, trailing_stop_order_type=OrderType.MARKET,
                                                          trailing_stop_limit_max_age_seconds=0,
                                                          trailing_activation_pct=.02, entry_limit_max_age_seconds=15))
    instance.update_config(requested)
    assert instance.config.trailing_stop_order_type == OrderType.MARKET
    assert instance.config.trailing_stop_limit_max_age_seconds == 0
    assert instance.config.trailing_activation_pct == .02
    assert instance.config.entry_limit_max_age_seconds == 15


def test_restart_required_change_leaves_current_config_intact():
    instance = controller()
    previous = instance.config.model_dump()
    requested = type(instance.config).model_validate(dict(previous, bb_length=17, bb_std=2.25))
    with pytest.raises(ValueError, match='restart'):
        instance.update_config(requested)
    assert instance.config.model_dump() == previous


def test_regime_preparation_rejects_missing_recent_volume():
    from hummingbot.strategy_v2.utils.market_analysis import MarketRegimeDetector

    detector = MarketRegimeDetector(use_hmm=False)
    frame = candles()
    frame.loc[400:, 'volume'] = float('nan')
    prepared, _ = detector._prepare(frame, fit_scaler=False)
    assert prepared is None


def test_regime_smoothing_counts_each_candle_once():
    from hummingbot.strategy_v2.utils.market_analysis import MarketRegimeDetector

    detector = MarketRegimeDetector(use_hmm=False)
    frame = candles()
    detector.detect(frame)
    detector.detect(frame)
    assert len(detector._hist) == 1


def test_smoothed_regime_confidence_matches_the_returned_label():
    from collections import deque
    from unittest.mock import MagicMock

    from hummingbot.strategy_v2.utils.market_analysis import MarketRegime, MarketRegimeDetector

    detector = MarketRegimeDetector(use_hmm=False)
    detector._is_fit = True
    detector._scaler_mu = np.zeros(len(detector.features))
    detector._scaler_sigma = np.ones(len(detector.features))
    detector.model = MagicMock()
    detector.model.predict_proba.return_value = np.array([[.9, .1]])
    detector._state2label = {0: 'HV_Trend_Up', 1: 'LV_Range'}
    detector._hist = deque(['LV_Range'] * 6, maxlen=7)
    detector._last = MarketRegime(timestamp=1, regime_label='LV_Range', confidence=.8,
                                  volatility_level='low', is_trending=False, trend_direction=0, trend_strength=0)
    regime = detector.detect(candles())
    assert regime.regime_label == 'LV_Range'
    assert regime.confidence == pytest.approx(.1)


def test_fresh_file_reload_normalizes_automatic_candle_configuration():
    from pathlib import Path

    import yaml

    from controllers.directional_trading.ok_rsi import OKRSIControllerConfig
    payload = yaml.safe_load(Path('test/fixtures/ok_rsi/controllers/ok_rsi_eth.yml').read_text())
    original = OKRSIControllerConfig(**payload)
    instance = controller()
    instance.config = original
    instance.config.candles_config = instance._build_default_candles_config()
    payload['entry_limit_max_age_seconds'] = 42
    instance.update_config(OKRSIControllerConfig(**payload))
    assert instance.config.entry_limit_max_age_seconds == 42
