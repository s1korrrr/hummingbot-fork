"""Genuine V5 profile publishes charts through the native runner and SQLite recorder."""
import asyncio
from test.test_ok_rsi_chart_snapshots import bind_chart_snapshot_methods
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
    value = SimpleNamespace(_chart_snapshots_enabled=lambda: True, ready_to_trade=True,
                            _runtime_status_snapshot_bot_name=lambda: 'rsi-modular-v2', current_timestamp=200,
                            _last_chart_snapshot_prune_ts=200, _chart_snapshot_prune_interval=lambda: 3600,
                            _is_stop_triggered=False, controllers={owner.config.id: owner})
    return bind_chart_snapshot_methods(value)


async def wait_for_chart_writer(strategy, *, timeout=2):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while strategy._chart_snapshot_jobs or strategy._chart_snapshot_prune_retries:
        if loop.time() >= deadline:
            pytest.fail('chart snapshot writer did not finish before the test deadline')
        await asyncio.sleep(0.01)


async def close_chart_writer(strategy):
    if strategy._chart_snapshot_writer is not None:
        await strategy._shutdown_chart_snapshot_writer()


def stored(recorder):
    with recorder._sql_manager.get_new_session() as session:
        return [(r.controller_id, r.pair, r.interval, r.candle_timestamp, r.close_price)
                for r in session.query(ChartSnapshot).all()]


def test_v5_public_profile_chart_bootstrap_increment_and_restart_recording(recorder):
    async def scenario():
        owner = chart_owner()
        assert callable(getattr(owner, 'build_chart_snapshot_rows', None))
        strategy = runner(owner)
        restarted_strategy = None
        try:
            with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
                StrategyV2Base._write_chart_snapshots_if_due(strategy)
                await wait_for_chart_writer(strategy)
                assert len(stored(recorder)) == 6
                assert {r[2] for r in stored(recorder)} == {'1m', '5m', '15m'}
                assert owner.build_chart_snapshot_rows('rsi-modular-v2') == []
                owner.processed_data['features'] = pd.concat([
                    owner.processed_data['features'],
                    pd.DataFrame([dict(timestamp=240, open=100, high=103, low=99, close=101, volume=2)])],
                    ignore_index=True)
                strategy.current_timestamp = 260
                StrategyV2Base._write_chart_snapshots_if_due(strategy)
                await wait_for_chart_writer(strategy)
                assert len(stored(recorder)) == 7
                assert any(r[2:] == ('1m', 240000, 101) for r in stored(recorder))

                restarted_strategy = runner(chart_owner())
                StrategyV2Base._write_chart_snapshots_if_due(restarted_strategy)
                await wait_for_chart_writer(restarted_strategy)
                assert len(stored(recorder)) == 7
            assert all(r[:2] == (owner.config.id, 'BTC-USDC') for r in stored(recorder))
        finally:
            await close_chart_writer(strategy)
            if restarted_strategy is not None:
                await close_chart_writer(restarted_strategy)

    asyncio.run(scenario())


def test_v5_chart_failure_retries_identical_rows_until_recorder_commit(recorder):
    async def scenario():
        owner = chart_owner()
        assert callable(getattr(owner, 'build_chart_snapshot_rows', None))
        strategy = runner(owner)
        original_write = recorder.store_or_update_chart_snapshots
        attempts = []

        def fail_once(rows):
            attempts.append(rows)
            if len(attempts) == 1:
                raise RuntimeError('disk unavailable')
            return original_write(rows)

        try:
            with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
                with patch.object(recorder, 'store_or_update_chart_snapshots', side_effect=fail_once):
                    StrategyV2Base._write_chart_snapshots_if_due(strategy)
                    deadline = asyncio.get_running_loop().time() + 2
                    while not any(job.last_error is not None for job in strategy._chart_snapshot_jobs.values()):
                        if asyncio.get_running_loop().time() >= deadline:
                            pytest.fail('asynchronous recorder failure was not reported to the owner loop')
                        await asyncio.sleep(0.01)
                    assert stored(recorder) == []
                    assert len(attempts) == 1
                    assert owner.build_chart_snapshot_rows('rsi-modular-v2') == attempts[0]

                    StrategyV2Base._write_chart_snapshots_if_due(strategy)
                    await wait_for_chart_writer(strategy)
                assert len(stored(recorder)) == 6
                assert owner.build_chart_snapshot_rows('rsi-modular-v2') == []
                assert len(attempts) == 2
                assert attempts[1] is attempts[0], 'retry must retain the exact built batch'
        finally:
            await close_chart_writer(strategy)

    asyncio.run(scenario())


def test_v5_chart_history_never_borrows_current_signal_or_rsi(recorder):
    owner = chart_owner()
    frame = pd.DataFrame([
        {'timestamp': 120, 'close': 100, 'signal': 0, f'RSI_{owner.config.rsi_length}': None},
        {'timestamp': 180, 'close': 101, 'signal': None, f'RSI_{owner.config.rsi_length}': 0},
        {'timestamp': 240, 'close': 102, 'signal': -1, f'RSI_{owner.config.rsi_length}': 42},
    ])
    owner.processed_data = {'features': frame, 'signal': 1, 'indicators': {'rsi': 99}}

    async def scenario():
        strategy = runner(owner)
        try:
            with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
                StrategyV2Base._write_chart_snapshots_if_due(strategy)
                await wait_for_chart_writer(strategy)
            with recorder._sql_manager.get_new_session() as session:
                rows = session.query(ChartSnapshot).filter_by(interval='1m').order_by(
                    ChartSnapshot.candle_timestamp).all()
                assert [(row.signal, row.rsi) for row in rows] == [(0, None), (None, 0), (-1, 42)]
            assert owner.processed_data['signal'] == 1
            assert owner.processed_data['indicators']['rsi'] == 99
        finally:
            await close_chart_writer(strategy)

    asyncio.run(scenario())
