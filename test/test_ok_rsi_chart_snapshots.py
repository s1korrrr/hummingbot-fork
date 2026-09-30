from types import SimpleNamespace

import pandas as pd

from controllers.directional_trading.ok_rsi_core.chart_snapshots import build_chart_rows


def config():
    return SimpleNamespace(id='ok-rsi', trading_pair='BTC-USDC', interval='1m',
                           rsi_length=10, atr_length=14, ema_fast_length=9, ema_slow_length=21,
                           bb_length=20, bb_std=2.0, macd_fast=12, macd_slow=26, macd_signal=9)


def test_closed_bars_use_own_indicators_and_never_backfill_current_decisions():
    frame = pd.DataFrame([{'timestamp': t, 'open': 10, 'high': 12, 'low': 9, 'close': 11,
                           'volume': 0, 'RSI_10': rsi, 'signal': 1,
                           'signal_state': {'buy_decision': 'unsafe-historical'}}
                          for t, rsi in [(120, 0), (180, 45), (240, 99)]])
    processed = {'data_health': {'ready': True}, 'timestamp': 245, 'candle_timestamp': 180, 'signal': 0,
                 'signal_state': {'buy_decision': 'hold'}, 'indicators': {'rsi': 88, 'cost_basis': 9}}
    rows = build_chart_rows(config(), 'main', '1m', frame, processed, now=245, interval_seconds=60)
    assert [r['candle_timestamp'] for r in rows] == [120000, 180000]
    assert rows[0]['rsi'] == 0
    assert rows[1]['rsi'] == 45
    assert rows[0].get('buy_decision') is None
    assert rows[0].get('signal') is None
    assert rows[0].get('cost_basis') is None
    assert rows[1]['buy_decision'] == 'hold'
    assert rows[1]['signal'] == 0
    assert rows[1]['cost_basis'] == 9
    assert rows[0]['volume'] == 0
    assert rows[0].get('atr') is None
    assert frame.iloc[0]['signal_state']['buy_decision'] == 'unsafe-historical'


def test_invalid_ohlc_nonfinite_and_stale_observation_are_not_fabricated():
    frame = pd.DataFrame([{'timestamp': 120, 'open': 10, 'high': 12, 'low': 9, 'close': 11},
                          {'timestamp': 180, 'open': float('nan'), 'high': 12, 'low': 9, 'close': 11}])
    rows = build_chart_rows(config(), 'main', '1m', frame, {'timestamp': 181, 'candle_timestamp': 120, 'signal': 1},
                            now=400, interval_seconds=60)
    assert len(rows) == 1
    assert rows[0].get('signal') is None
    assert rows[0].get('volume') is None


def test_owner_hook_retries_until_durable_ack_and_emits_new_closed_candles():
    from controllers.directional_trading.ok_rsi import OKRSIController
    owner = object.__new__(OKRSIController)
    owner.config = config()
    owner.config.trend_confirmation_enabled = False
    owner.market_data_provider = SimpleNamespace(time=lambda: 245)
    owner.processed_data = {'features': pd.DataFrame([
        {'timestamp': 120, 'open': 10, 'high': 12, 'low': 9, 'close': 11},
        {'timestamp': 180, 'open': 10, 'high': 12, 'low': 9, 'close': 11}])}
    first = owner.build_chart_snapshot_rows('main')
    assert len(first) == 2
    assert owner.build_chart_snapshot_rows('main') == first
    owner.chart_snapshot_rows_persisted(first)
    assert owner.build_chart_snapshot_rows('main') == []


def test_recorder_failure_does_not_acknowledge_uncommitted_batch():
    from unittest.mock import Mock, patch

    import pytest

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    controller = SimpleNamespace(build_chart_snapshot_rows=Mock(return_value=[{'id': 1}, {'id': 2}]),
                                 chart_snapshot_rows_persisted=Mock())
    owner = SimpleNamespace(_chart_snapshots_enabled=lambda: True, ready_to_trade=True,
                            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=100,
                            _last_chart_snapshot_prune_ts=100, _chart_snapshot_prune_interval=lambda: 60,
                            controllers={'a': controller})
    recorder = Mock()
    recorder.store_or_update_chart_snapshots.side_effect = RuntimeError('disk failure')
    with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='disk failure'):
            StrategyV2Base._write_chart_snapshots_if_due(owner)
        controller.chart_snapshot_rows_persisted.assert_not_called()
        recorder.store_or_update_chart_snapshots.side_effect = None
        StrategyV2Base._write_chart_snapshots_if_due(owner)
    controller.chart_snapshot_rows_persisted.assert_called_once_with([{'id': 1}, {'id': 2}])


def test_tick_continues_executor_decisions_when_chart_persistence_fails():
    from unittest.mock import Mock

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    owner = SimpleNamespace(
        controllers={'a': object()}, update_executors_info=Mock(),
        _unattended_restart_hold=False,
        _sync_runtime_state_to_db_if_due=Mock(), _write_chart_snapshots_if_due=Mock(side_effect=RuntimeError('disk full')),
        _write_runtime_status_snapshot_if_due=Mock(), update_controllers_configs=Mock(),
        market_data_provider=SimpleNamespace(ready=True), _is_stop_triggered=False,
        determine_executor_actions=Mock(return_value=['risk-action']),
        executor_orchestrator=SimpleNamespace(resume_planned_positions_if_ready=Mock(return_value=True),
                                              execute_action=Mock()), logger=Mock())
    StrategyV2Base.on_tick(owner)
    owner.executor_orchestrator.execute_action.assert_called_once_with('risk-action')
    owner.logger().exception.assert_called_once()


def chart_owner(timestamps):
    from controllers.directional_trading.ok_rsi import OKRSIController
    owner = object.__new__(OKRSIController)
    owner.config = config()
    owner.config.trend_confirmation_enabled = False
    owner.market_data_provider = SimpleNamespace(time=lambda: 1000000)
    owner.processed_data = {'features': candle_frame(timestamps)}
    return owner


def candle_frame(timestamps):
    return pd.DataFrame([{'timestamp': t, 'open': 10, 'high': 12, 'low': 9, 'close': 11}
                         for t in timestamps])


def test_older_history_arriving_after_first_page_is_backfilled():
    owner = chart_owner([600, 660])
    owner.chart_snapshot_rows_persisted(owner.build_chart_snapshot_rows('main'))
    owner.processed_data['features'] = candle_frame([120, 180, 600, 660])
    rows = owner.build_chart_snapshot_rows('main')
    assert [row['candle_timestamp'] for row in rows] == [120000, 180000]
    owner.chart_snapshot_rows_persisted(rows)
    assert owner.build_chart_snapshot_rows('main') == []


def test_raw_row_is_enriched_once_at_same_timestamp_without_downgrade():
    owner = chart_owner([120])
    owner.chart_snapshot_rows_persisted(owner.build_chart_snapshot_rows('main'))
    owner.processed_data['features']['RSI_10'] = 47.5
    rows = owner.build_chart_snapshot_rows('main')
    assert len(rows) == 1 and rows[0]['rsi'] == 47.5
    assert owner.build_chart_snapshot_rows('main') == rows
    owner.chart_snapshot_rows_persisted(rows)
    assert owner.build_chart_snapshot_rows('main') == []
    owner.processed_data['features'] = candle_frame([120])
    assert owner.build_chart_snapshot_rows('main') == []


def test_export_cache_and_batches_are_bounded_and_duplicate_rows_are_idempotent():
    from controllers.directional_trading.ok_rsi_core.chart_snapshots import MAX_BATCH_ROWS, MAX_CACHED_CANDLES
    owner = chart_owner(range(0, (MAX_CACHED_CANDLES + 200) * 60, 60))
    owner.processed_data['features'] = pd.concat([owner.processed_data['features']] * 2, ignore_index=True)
    total = 0
    for _ in range(MAX_CACHED_CANDLES // MAX_BATCH_ROWS + 2):
        rows = owner.build_chart_snapshot_rows('main')
        assert len(rows) <= MAX_BATCH_ROWS
        assert len({row['candle_timestamp'] for row in rows}) == len(rows)
        total += len(rows)
        owner.chart_snapshot_rows_persisted(rows)
    assert total == MAX_CACHED_CANDLES
    assert owner.build_chart_snapshot_rows('main') == []
    assert len(owner._chart_snapshot_cache['1m']) <= MAX_CACHED_CANDLES


def test_shorter_feature_window_does_not_hide_older_raw_owner_candles():
    owner = chart_owner([600, 660])
    owner.processed_data['features']['RSI_10'] = [45, 48]
    owner._get_interval_candles_df = lambda interval: candle_frame([120, 180, 600, 660])
    rows = owner.build_chart_snapshot_rows('main')
    assert [row['candle_timestamp'] for row in rows] == [120000, 180000, 600000, 660000]
    assert rows[0].get('rsi') is None
    assert rows[-1]['rsi'] == 48


def test_real_sqlite_upsert_enriches_one_row_and_restart_raw_preserves_it(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.model.chart_snapshot import ChartSnapshot
    engine = create_engine(f'sqlite:///{tmp_path / "charts.sqlite"}')
    ChartSnapshot.__table__.create(engine)
    recorder = object.__new__(MarketsRecorder)
    recorder._sql_manager = SimpleNamespace(get_new_session=lambda: Session(engine))
    owner = chart_owner([120])
    for row in owner.build_chart_snapshot_rows('main'):
        recorder.store_or_update_chart_snapshot(row)
        owner.chart_snapshot_rows_persisted([row])
    owner.processed_data['features']['RSI_10'] = 47.5
    for row in owner.build_chart_snapshot_rows('main'):
        recorder.store_or_update_chart_snapshot(row)
        owner.chart_snapshot_rows_persisted([row])
    restarted = chart_owner([120])
    for row in restarted.build_chart_snapshot_rows('main'):
        recorder.store_or_update_chart_snapshot(row)
    with Session(engine) as session:
        records = session.query(ChartSnapshot).all()
        assert len(records) == 1
        assert records[0].rsi == 47.5
        assert records[0].signal is None
    engine.dispose()


def test_cache_eviction_retains_latest_candles_and_higher_frames_stay_raw():
    from controllers.directional_trading.ok_rsi_core.chart_snapshots import MAX_CACHED_CANDLES, acknowledge_chart_rows
    cache = {}
    acknowledge_chart_rows(cache, [{'interval': '1m', 'candle_timestamp': t}
                                   for t in range(MAX_CACHED_CANDLES + 10)])
    assert len(cache['1m']) == MAX_CACHED_CANDLES
    assert min(cache['1m']) == 10
    owner = chart_owner(range(0, 900, 60))
    owner.config.derived_higher_timeframe_max_records = 100
    owner.config.trend_confirmation_enabled = True
    owner.config.trend_confirmation_medium_interval = '5m'
    owner.config.trend_confirmation_long_interval = '15m'
    owner.processed_data['features']['RSI_10'] = 48
    owner._get_interval_candles_df = lambda interval: candle_frame(range(0, 900, 60))
    rows = owner.build_chart_snapshot_rows('main')
    assert next(row for row in rows if row['interval'] == '1m')['rsi'] == 48
    assert {row['interval'] for row in rows} == {'1m', '5m', '15m'}
    assert all(row.get('rsi') is None for row in rows if row['interval'] != '1m')


def realistic_chart_owner(rows=1500):
    owner = chart_owner(range(0, rows * 60, 60))
    owner.max_records = 500
    for key, value in dict(trend_confirmation_enabled=True, trend_confirmation_medium_interval='5m',
                           trend_confirmation_long_interval='15m', derived_higher_timeframe_max_records=100,
                           episode_enabled=False, candles_connector=None, connector_name='okx',
                           candles_trading_pair=None).items():
        setattr(owner.config, key, value)
    raw = owner.processed_data['features'].copy()
    owner.market_data_provider.get_candles_df = lambda **_: raw.copy()
    owner.processed_data['features'] = raw.tail(500).copy()
    return owner, raw


def test_unchanged_projection_reuses_aggregation_while_uncommitted_batches_continue():
    from unittest.mock import patch

    from controllers.directional_trading.ok_rsi_core.candles import derive_higher_timeframe_candles
    owner, _ = realistic_chart_owner()
    with patch('controllers.directional_trading.ok_rsi.derive_higher_timeframe_candles',
               wraps=derive_higher_timeframe_candles) as derive:
        first = owner.build_chart_snapshot_rows('main')
        owner.chart_snapshot_rows_persisted(first)
        second = owner.build_chart_snapshot_rows('main')
        assert second and first != second
        assert derive.call_count == 2
        owner.chart_snapshot_rows_persisted(second)
        for _ in range(3):
            owner.chart_snapshot_rows_persisted(owner.build_chart_snapshot_rows('main'))
        assert owner.build_chart_snapshot_rows('main') == []
        assert derive.call_count == 2
        owner.processed_data['features']['RSI_10'] = 45
        enriched = owner.build_chart_snapshot_rows('main')
        assert enriched and all(row['rsi'] == 45 for row in enriched)
        assert derive.call_count == 2


def test_open_candle_updates_do_not_rederive_closed_projections():
    from unittest.mock import patch

    from controllers.directional_trading.ok_rsi_core.candles import derive_higher_timeframe_candles
    owner, raw = realistic_chart_owner(20)
    owner.market_data_provider.time = lambda: 1200
    owner.processed_data['features'] = pd.DataFrame()
    raw.loc[len(raw)] = {'timestamp': 1200, 'open': 10, 'high': 12, 'low': 9, 'close': 11}
    with patch('controllers.directional_trading.ok_rsi.derive_higher_timeframe_candles',
               wraps=derive_higher_timeframe_candles) as derive:
        owner.chart_snapshot_rows_persisted(owner.build_chart_snapshot_rows('main'))
        raw.loc[raw.timestamp == 1200, 'close'] = 10
        assert owner.build_chart_snapshot_rows('main') == []
        assert derive.call_count == 2


def test_batch_sqlite_rollback_preserves_existing_row_and_does_not_acknowledge(tmp_path):
    from unittest.mock import Mock, patch

    import pytest
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.model.chart_snapshot import ChartSnapshot
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    engine = create_engine(f'sqlite:///{tmp_path / "batch.sqlite"}')
    ChartSnapshot.__table__.create(engine)
    recorder = object.__new__(MarketsRecorder)
    recorder._sql_manager = SimpleNamespace(get_new_session=lambda: Session(engine))
    row = chart_owner([120]).build_chart_snapshot_rows('main')[0]
    recorder.store_or_update_chart_snapshot({**row, 'rsi': 45})
    controller = SimpleNamespace(build_chart_snapshot_rows=Mock(return_value=[{**row, 'rsi': 46}, {'bad': 'row'}]),
                                 chart_snapshot_rows_persisted=Mock())
    strategy = SimpleNamespace(_chart_snapshots_enabled=lambda: True, ready_to_trade=True,
                               _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=100,
                               _last_chart_snapshot_prune_ts=100, _chart_snapshot_prune_interval=lambda: 60,
                               controllers={'a': controller})
    with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
        with pytest.raises(KeyError):
            StrategyV2Base._write_chart_snapshots_if_due(strategy)
    controller.chart_snapshot_rows_persisted.assert_not_called()
    with Session(engine) as session:
        record = session.query(ChartSnapshot).one()
        assert record.rsi == 45
        original_id = record.id
    recorder.store_or_update_chart_snapshots([{**row, 'rsi': 46}, row])
    recorder.store_or_update_chart_snapshots([{**row, 'rsi': 46}, row])
    with Session(engine) as session:
        record = session.query(ChartSnapshot).one()
        assert record.id == original_id
        assert record.rsi == 46
        assert record.signal is None
    engine.dispose()
