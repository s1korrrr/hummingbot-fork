"""Genuine V5 profile publishes charts through the native runner and SQLite recorder."""
import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest
from test_native_composition import provider
from test_native_lifecycle import recorder as _recorder_fixture

from controllers.generic.rsi_modular import get_config_class
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.model.chart_snapshot import ChartSnapshot
from hummingbot.strategy.strategy_v2_base import StrategyV2Base

recorder = _recorder_fixture


def chart_owner():
    config = get_config_class('rsi_v5')(
        profile='rsi_v5', id='modular_spot_v5_charts', trading_pair='BTC-USDC',
        strict_hmm_mode=False, trend_confirmation_enabled=True,
        trend_confirmation_medium_interval='5m', trend_confirmation_long_interval='15m')
    market = provider()
    frame = pd.DataFrame([dict(timestamp=timestamp, open=99, high=102, low=98, close=100,
                               volume=3, RSI_14=42, signal=0) for timestamp in (120, 180)])
    market.get_candles_df.return_value = frame.copy()
    owner = config.get_controller_class()(config, market, asyncio.Queue())
    owner.processed_data = {'features': frame, 'signal': 0}
    return owner


def runner(owner):
    return SimpleNamespace(_chart_snapshots_enabled=lambda: True, ready_to_trade=True,
                           _runtime_status_snapshot_bot_name=lambda: 'rsi-modular-v2', current_timestamp=200,
                           _last_chart_snapshot_prune_ts=200, _chart_snapshot_prune_interval=lambda: 3600,
                           controllers={owner.config.id: owner})


def stored(recorder):
    with recorder._sql_manager.get_new_session() as session:
        return [(r.controller_id, r.pair, r.interval, r.candle_timestamp, r.close_price)
                for r in session.query(ChartSnapshot).all()]


def test_v5_public_profile_chart_bootstrap_increment_and_restart_recording(recorder):
    owner = chart_owner()
    assert callable(getattr(owner, 'build_chart_snapshot_rows', None))
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        StrategyV2Base._write_chart_snapshots_if_due(runner(owner))
        assert len(stored(recorder)) == 6
        assert {r[2] for r in stored(recorder)} == {'1m', '5m', '15m'}
        assert owner.build_chart_snapshot_rows('rsi-modular-v2') == []
        owner.processed_data['features'] = pd.concat([
            owner.processed_data['features'],
            pd.DataFrame([dict(timestamp=240, open=100, high=103, low=99, close=101, volume=2)])], ignore_index=True)
        StrategyV2Base._write_chart_snapshots_if_due(runner(owner))
        assert len(stored(recorder)) == 7
        assert any(r[2:] == ('1m', 240000, 101) for r in stored(recorder))
        StrategyV2Base._write_chart_snapshots_if_due(runner(chart_owner()))
        assert len(stored(recorder)) == 7
    assert all(r[:2] == (owner.config.id, 'BTC-USDC') for r in stored(recorder))


def test_v5_chart_failure_retries_identical_rows_until_recorder_commit(recorder):
    owner = chart_owner()
    assert callable(getattr(owner, 'build_chart_snapshot_rows', None))
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with patch.object(recorder, 'store_or_update_chart_snapshots', side_effect=RuntimeError('disk unavailable')):
            with pytest.raises(RuntimeError, match='disk unavailable'):
                StrategyV2Base._write_chart_snapshots_if_due(runner(owner))
        assert stored(recorder) == []
        StrategyV2Base._write_chart_snapshots_if_due(runner(owner))
        assert len(stored(recorder)) == 6
        assert owner.build_chart_snapshot_rows('rsi-modular-v2') == []
