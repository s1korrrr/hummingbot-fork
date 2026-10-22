from types import SimpleNamespace

import pandas as pd

from controllers.directional_trading.ok_rsi_core.chart_snapshots import build_chart_rows


def config():
    return SimpleNamespace(id='ok-rsi', trading_pair='BTC-USDC', interval='1m',
                           rsi_length=10, atr_length=14, ema_fast_length=9, ema_slow_length=21,
                           bb_length=20, bb_std=2.0, macd_fast=12, macd_slow=26, macd_signal=9)


def bind_chart_snapshot_methods(owner):
    import logging
    from types import MethodType

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    for name in (
        '_schedule_chart_snapshot_jobs', '_enqueue_chart_snapshot_job',
        '_chart_snapshot_write_completed', '_complete_chart_snapshot_job',
        '_complete_chart_snapshot_prune_retry', '_enqueue_chart_snapshot_prune_retry',
        '_pending_chart_snapshot_batch_count', '_report_chart_snapshot_backpressure',
        '_capture_chart_snapshot_successor',
        '_refresh_chart_snapshot_prune_requests', '_finish_chart_snapshot_prune_cycle_if_ready',
        '_shutdown_chart_snapshot_writer',
    ):
        setattr(owner, name, MethodType(getattr(StrategyV2Base, name), owner))
    owner._chart_snapshot_jobs = {}
    owner._chart_snapshot_prune_retries = {}
    owner._chart_snapshot_active_job = None
    owner._chart_snapshot_writer = None
    owner._chart_snapshot_writer_loop = None
    owner._chart_snapshot_activity = None
    owner._chart_snapshot_writer_draining = False
    owner._chart_snapshot_scheduler_cursor = 0
    owner._chart_snapshot_prune_cycle = None
    owner._chart_snapshot_saturation_reported = False
    if not hasattr(owner, 'logger'):
        owner.logger = lambda: logging.getLogger('test.chart_snapshot')
    return owner


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


def test_recorder_failure_retries_identical_batch_before_building_or_acknowledging_again():
    import asyncio
    from unittest.mock import Mock, patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        callback_seen = asyncio.Event()
        acknowledged = asyncio.Event()
        rows = [{'id': 1}, {'id': 2}]
        controller = SimpleNamespace(
            build_chart_snapshot_rows=Mock(return_value=rows),
            chart_snapshot_rows_persisted=Mock(side_effect=lambda _rows: acknowledged.set()),
        )
        logger = SimpleNamespace(error=lambda *_args: callback_seen.set(), exception=Mock(), info=Mock())
        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=100,
            _last_chart_snapshot_prune_ts=100, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={'a': controller}, _is_stop_triggered=False,
            logger=lambda: logger,
        )
        bind_chart_snapshot_methods(owner)
        recorder = Mock()
        recorder.store_or_update_chart_snapshots.side_effect = [RuntimeError('disk failure'), None]
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            await asyncio.wait_for(callback_seen.wait(), timeout=2)
            assert not acknowledged.is_set()
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            await asyncio.wait_for(acknowledged.wait(), timeout=2)
            await owner._shutdown_chart_snapshot_writer()

        controller.build_chart_snapshot_rows.assert_called_once_with(bot_name='main')
        controller.chart_snapshot_rows_persisted.assert_called_once_with(rows)
        assert recorder.store_or_update_chart_snapshots.call_count == 2
        first_batch = recorder.store_or_update_chart_snapshots.call_args_list[0].args[0]
        retry_batch = recorder.store_or_update_chart_snapshots.call_args_list[1].args[0]
        assert first_batch is retry_batch

    asyncio.run(scenario())


def test_blocked_chart_commit_does_not_block_strategy_loop_and_ack_waits_for_commit():
    import asyncio
    import threading
    from types import MethodType
    from unittest.mock import patch

    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, name='chart-test-loop')
    commit_entered = threading.Event()
    release_commit = threading.Event()
    commit_finished = threading.Event()
    heartbeat_ran = threading.Event()
    acknowledged = threading.Event()
    tick_finished = threading.Event()
    events = []
    events_lock = threading.Lock()
    rows = [{'controller_id': 'test', 'pair': 'BTC-USDC', 'interval': '1m',
             'candle_timestamp': 60000, 'close': 1.0}]

    def record(event):
        with events_lock:
            events.append(event)

    class Controller:
        def build_chart_snapshot_rows(self, *, bot_name):
            assert bot_name == 'test-bot'
            return rows

        def chart_snapshot_rows_persisted(self, persisted_rows):
            assert persisted_rows == rows
            record('acknowledged')
            acknowledged.set()

        def chart_snapshot_prune_requests(self):
            return []

    def commit(_rows):
        record('commit_started')
        commit_entered.set()
        if not release_commit.wait(timeout=5):
            raise TimeoutError('test did not release chart commit')
        record('commit_finished')
        commit_finished.set()

    controller = Controller()
    owner = SimpleNamespace(
        config=SimpleNamespace(chart_snapshots_enabled=True),
        ready_to_trade=True,
        _chart_snapshots_enabled=lambda: True,
        current_timestamp=100,
        _last_chart_snapshot_prune_ts=100,
        _chart_snapshot_prune_interval=lambda: 3600,
        _runtime_status_snapshot_bot_name=lambda: 'test-bot',
        controllers={'test': controller},
        executor_orchestrator=SimpleNamespace(resume_planned_positions_if_ready=lambda: True),
        _unattended_restart_hold=False,
        _persist_controller_configs_if_safe=lambda: None,
        update_executors_info=lambda: None,
        _start_controllers_if_safe=lambda: None,
        _sync_runtime_state_to_db_if_due=lambda: None,
        _write_runtime_status_snapshot_if_due=lambda: None,
        update_controllers_configs=lambda: None,
        market_data_provider=SimpleNamespace(ready=True),
        _is_stop_triggered=False,
        determine_executor_actions=lambda: [],
    )
    # Bind the production method to the synthetic strategy owner.
    owner._write_chart_snapshots_if_due = MethodType(StrategyV2Base._write_chart_snapshots_if_due, owner)
    bind_chart_snapshot_methods(owner)
    recorder = SimpleNamespace(store_or_update_chart_snapshots=commit, prune_chart_snapshots=lambda **_kwargs: None)

    def same_loop_heartbeat():
        record('heartbeat')
        heartbeat_ran.set()

    def tick():
        try:
            StrategyV2Base.on_tick(owner)
        finally:
            tick_finished.set()

    loop_thread.start()
    try:
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
            loop.call_soon_threadsafe(tick)
            assert commit_entered.wait(timeout=2), 'chart persistence did not reach the blocked commit'
            loop.call_soon_threadsafe(same_loop_heartbeat)
            heartbeat_before_commit_release = heartbeat_ran.wait(timeout=0.25)
            ack_before_commit = acknowledged.is_set()
            release_commit.set()
            assert commit_finished.wait(timeout=2), 'chart commit did not finish after release'
            assert acknowledged.wait(timeout=2), 'completed commit was not acknowledged'
            assert tick_finished.wait(timeout=2), 'strategy tick did not complete'
            assert not ack_before_commit, 'controller cache was acknowledged before durable commit'
            assert heartbeat_before_commit_release, 'same-loop heartbeat stalled behind chart commit'
            assert events.index('heartbeat') < events.index('commit_finished')
            assert events.index('commit_finished') < events.index('acknowledged')
    finally:
        release_commit.set()
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=2)
        loop.close()


def test_writer_capacity_is_global_bounded_and_rotates_admission_fairly():
    import asyncio
    import threading
    from unittest.mock import patch

    from hummingbot.strategy.strategy_v2_base import CHART_SNAPSHOT_WRITER_MAX_PENDING, StrategyV2Base

    async def scenario():
        names = [f'controller-{index}' for index in range(CHART_SNAPSHOT_WRITER_MAX_PENDING + 1)]
        started = {name: threading.Event() for name in names}
        releases = {name: threading.Event() for name in names}
        acknowledged = {name: asyncio.Event() for name in names}
        builders = {name: 0 for name in names}
        start_order = []

        class Controller:
            def __init__(self, name):
                self.name = name

            def build_chart_snapshot_rows(self, *, bot_name):
                builders[self.name] += 1
                if builders[self.name] == 1:
                    return [{'controller_id': self.name, 'timestamp': 1}]
                return []

            def chart_snapshot_rows_persisted(self, _rows):
                acknowledged[self.name].set()

        controllers = {name: Controller(name) for name in names}

        def commit(rows):
            name = rows[0]['controller_id']
            start_order.append(name)
            started[name].set()
            if not releases[name].wait(timeout=4):
                raise TimeoutError(f'test did not release {name}')

        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=100,
            _last_chart_snapshot_prune_ts=100, _chart_snapshot_prune_interval=lambda: 3600,
            controllers=controllers, _is_stop_triggered=False,
        )
        bind_chart_snapshot_methods(owner)
        recorder = SimpleNamespace(store_or_update_chart_snapshots=commit, prune_chart_snapshots=lambda **_kwargs: None)
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            assert started[names[0]].wait(timeout=2)
            assert sum(builders.values()) == CHART_SNAPSHOT_WRITER_MAX_PENDING
            assert builders[names[-1]] == 0
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            assert sum(builders.values()) == CHART_SNAPSHOT_WRITER_MAX_PENDING
            releases[names[0]].set()
            await asyncio.wait_for(acknowledged[names[0]].wait(), timeout=2)
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            assert builders[names[-1]] == 1, 'next controller was not admitted when a slot opened'
            for release in releases.values():
                release.set()
            await owner._shutdown_chart_snapshot_writer()

        assert start_order[0] == names[0]
        assert len(owner._chart_snapshot_jobs) == 0

    asyncio.run(scenario())


def test_writer_scheduler_remains_fair_with_five_continuously_ready_controllers():
    import asyncio
    from collections import Counter
    from unittest.mock import patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        committed = Counter()
        built = Counter()

        class Controller:
            def __init__(self, controller_id):
                self.controller_id = controller_id

            def build_chart_snapshot_rows(self, **_kwargs):
                built[self.controller_id] += 1
                return [{'controller_id': self.controller_id, 'timestamp': built[self.controller_id]}]

            def chart_snapshot_rows_persisted(self, _rows):
                pass

        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True, _is_stop_triggered=False,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=100,
            _last_chart_snapshot_prune_ts=100, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={name: Controller(name) for name in 'abcde'},
        )
        bind_chart_snapshot_methods(owner)

        def commit(rows):
            committed[rows[0]['controller_id']] += 1

        recorder = SimpleNamespace(store_or_update_chart_snapshots=commit,
                                   prune_chart_snapshots=lambda **_kwargs: None)
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            while sum(committed.values()) < 25:
                StrategyV2Base._write_chart_snapshots_if_due(owner)
                await asyncio.sleep(0.001)
            await asyncio.wait_for(_wait_for_chart_counts(committed, 5, minimum=3), timeout=3)
            await owner._shutdown_chart_snapshot_writer()

        assert set(committed) == set('abcde')
        assert min(committed.values()) >= 3
        assert max(committed.values()) - min(committed.values()) <= 1

    asyncio.run(scenario())


async def _wait_for_chart_counts(counts, expected, minimum):
    import asyncio

    while len(counts) < expected or min(counts.values(), default=0) < minimum:
        await asyncio.sleep(0)


def test_failed_pruning_does_not_block_later_chart_rows_for_same_controller():
    import asyncio
    from unittest.mock import Mock, patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        write_finished = asyncio.Event()
        first_acknowledged = asyncio.Event()
        second_acknowledged = asyncio.Event()
        attempts = {'build': 0, 'prune': 0}

        class Controller:
            def build_chart_snapshot_rows(self, **_kwargs):
                attempts['build'] += 1
                return [{'controller_id': 'a', 'timestamp': attempts['build']}]

            def chart_snapshot_rows_persisted(self, rows):
                if rows[0]['timestamp'] == 1:
                    first_acknowledged.set()
                elif rows[0]['timestamp'] == 2:
                    second_acknowledged.set()

            def chart_snapshot_prune_requests(self):
                return [{'controller_id': 'a', 'pair': 'BTC-USDC', 'interval': '1m',
                         'keep_after_timestamp': 1}]

        controller = Controller()
        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True, _is_stop_triggered=False,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=3600,
            _last_chart_snapshot_prune_ts=0, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={'a': controller}, logger=lambda: SimpleNamespace(
                error=Mock(), exception=Mock(), info=Mock()),
        )
        bind_chart_snapshot_methods(owner)
        writes = []

        def write(rows):
            writes.append(rows[0]['timestamp'])
            write_finished.set()

        def prune(**_request):
            attempts['prune'] += 1
            raise RuntimeError('pruning remains unavailable')

        recorder = SimpleNamespace(store_or_update_chart_snapshots=write, prune_chart_snapshots=prune)
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            await asyncio.wait_for(write_finished.wait(), timeout=2)
            await asyncio.wait_for(first_acknowledged.wait(), timeout=2)
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            for _ in range(100):
                if second_acknowledged.is_set():
                    break
                StrategyV2Base._write_chart_snapshots_if_due(owner)
                await asyncio.sleep(0.001)
            assert second_acknowledged.is_set(), writes
            await owner._shutdown_chart_snapshot_writer()

        assert writes[:2] == [1, 2]
        assert attempts['build'] >= 2
        assert owner._last_chart_snapshot_prune_ts == 0

    asyncio.run(scenario())


def test_prune_request_error_during_shutdown_still_closes_writer():
    import asyncio
    from unittest.mock import Mock, patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        acknowledged = asyncio.Event()
        logger = SimpleNamespace(error=Mock(), exception=Mock(), info=Mock())
        controller = SimpleNamespace(
            build_chart_snapshot_rows=lambda **_kwargs: [{'controller_id': 'a', 'timestamp': 1}],
            chart_snapshot_rows_persisted=lambda _rows: acknowledged.set(),
            chart_snapshot_prune_requests=Mock(side_effect=RuntimeError('prune request failed')),
        )
        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True, _is_stop_triggered=False,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=3600,
            _last_chart_snapshot_prune_ts=0, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={'a': controller}, logger=lambda: logger,
        )
        bind_chart_snapshot_methods(owner)
        recorder = SimpleNamespace(store_or_update_chart_snapshots=lambda _rows: None,
                                   prune_chart_snapshots=lambda **_kwargs: None)
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            await asyncio.wait_for(acknowledged.wait(), timeout=2)
            await owner._shutdown_chart_snapshot_writer()
            assert owner._chart_snapshot_writer.is_alive is False
            assert not owner._chart_snapshot_jobs
            assert owner._chart_snapshot_prune_retries
            logger.error.assert_called()

    asyncio.run(scenario())


def test_ok_rsi_successor_preserves_same_candle_decision_during_blocked_commit():
    import asyncio
    import copy
    import threading
    from unittest.mock import patch

    from controllers.directional_trading.ok_rsi import OKRSIController
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        controller = chart_owner([120, 180])
        controller.market_data_provider.time = lambda: 245
        controller.processed_data.update({
            'timestamp': 245,
            'candle_timestamp': 180,
            'data_health': {'ready': True},
            'signal': 1,
            'indicators': {'cost_basis': 1},
            'signal_state': {'buy_decision': 'hold'},
        })
        assert isinstance(controller, OKRSIController)
        first_started = threading.Event()
        release_first = threading.Event()
        committed = []
        acknowledgements = []
        third_acknowledged = asyncio.Event()
        build_rows = controller.build_chart_snapshot_rows
        build_calls = 0

        def count_build(**kwargs):
            nonlocal build_calls
            build_calls += 1
            return build_rows(**kwargs)

        controller.build_chart_snapshot_rows = count_build

        def ack(rows):
            acknowledgements.append(copy.deepcopy(rows))
            if len(acknowledgements) == 3:
                third_acknowledged.set()

        controller.chart_snapshot_rows_persisted = ack
        strategy = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True, _is_stop_triggered=False,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=245,
            _last_chart_snapshot_prune_ts=1000, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={'ok-rsi': controller},
        )
        bind_chart_snapshot_methods(strategy)

        def write(rows):
            committed.append(copy.deepcopy(rows))
            if len(committed) == 1:
                first_started.set()
                if not release_first.wait(timeout=3):
                    raise TimeoutError('test did not release first chart commit')

        recorder = SimpleNamespace(store_or_update_chart_snapshots=write,
                                   prune_chart_snapshots=lambda **_kwargs: None)
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            StrategyV2Base._write_chart_snapshots_if_due(strategy)
            assert await asyncio.to_thread(first_started.wait, 2)
            first = next(row for row in committed[0] if row['candle_timestamp'] == 180000)
            assert first.get('buy_decision') == 'hold', first
            controller.processed_data['signal_state']['buy_decision'] = 'buy'
            controller.market_data_provider.time = lambda: 250
            StrategyV2Base._write_chart_snapshots_if_due(strategy)
            controller.processed_data['signal_state']['buy_decision'] = 'sell'
            controller.processed_data['signal'] = 0
            controller.processed_data['indicators']['cost_basis'] = 0
            controller.market_data_provider.time = lambda: 255
            StrategyV2Base._write_chart_snapshots_if_due(strategy)
            controller.market_data_provider.time = lambda: 310
            controller.processed_data['signal_state']['buy_decision'] = 'hold'
            StrategyV2Base._write_chart_snapshots_if_due(strategy)
            assert build_calls == 3, 'saturation must defer before calling the stateful native builder'
            release_first.set()
            while not third_acknowledged.is_set():
                StrategyV2Base._write_chart_snapshots_if_due(strategy)
                await asyncio.sleep(0.001)
            await asyncio.wait_for(third_acknowledged.wait(), timeout=3)
            await strategy._shutdown_chart_snapshot_writer()

        second = next(row for row in committed[1] if row['candle_timestamp'] == 180000)
        third = next(row for row in committed[2] if row['candle_timestamp'] == 180000)
        assert second['buy_decision'] == 'buy'
        assert third['buy_decision'] == 'sell'
        assert third['signal'] == 0
        assert third['cost_basis'] == 0
        first_ack = next(row for row in acknowledgements[0] if row['candle_timestamp'] == 180000)
        second_ack = next(row for row in acknowledgements[1] if row['candle_timestamp'] == 180000)
        third_ack = next(row for row in acknowledgements[2] if row['candle_timestamp'] == 180000)
        assert first_ack['buy_decision'] == 'hold'
        assert second_ack['buy_decision'] == 'buy'
        assert third_ack['buy_decision'] == 'sell'

    asyncio.run(scenario())


def test_ok_rsi_outstanding_successors_deduplicate_adjacent_payloads_only():
    import asyncio
    import copy
    import threading
    from unittest.mock import patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        controller = chart_owner([120, 180])
        controller.market_data_provider.time = lambda: 245
        controller.processed_data.update({
            'timestamp': 245,
            'candle_timestamp': 180,
            'data_health': {'ready': True},
            'signal': 1,
            'indicators': {'cost_basis': 1},
            'signal_state': {'buy_decision': 'hold'},
        })
        first_started = threading.Event()
        release_first = threading.Event()
        first_failure_reported = asyncio.Event()
        attempt_rows = []
        committed = []
        acknowledgements = []
        final_acknowledged = asyncio.Event()

        logger = SimpleNamespace(
            error=lambda *_args: first_failure_reported.set(),
            exception=lambda *_args: None,
            info=lambda *_args: None,
            warning=lambda *_args: None,
        )

        def ack(rows):
            acknowledgements.append(copy.deepcopy(rows))
            if len(acknowledgements) == 3:
                final_acknowledged.set()

        controller.chart_snapshot_rows_persisted = ack
        strategy = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True, _is_stop_triggered=False,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=245,
            _last_chart_snapshot_prune_ts=1000, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={'ok-rsi': controller}, logger=lambda: logger,
        )
        bind_chart_snapshot_methods(strategy)

        def write(rows):
            attempt_rows.append(copy.deepcopy(rows))
            if len(attempt_rows) == 1:
                first_started.set()
                if not release_first.wait(timeout=3):
                    raise TimeoutError('test did not release first chart commit')
                raise RuntimeError('injected first chart write failure')
            committed.append(copy.deepcopy(rows))

        recorder = SimpleNamespace(store_or_update_chart_snapshots=write,
                                   prune_chart_snapshots=lambda **_kwargs: None)
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            try:
                StrategyV2Base._write_chart_snapshots_if_due(strategy)
                assert await asyncio.to_thread(first_started.wait, 2)
                first = copy.deepcopy(attempt_rows[0])

                # The native builder reports the same still-unacknowledged state on
                # repeated ticks. Repeated payloads must not consume successor slots.
                StrategyV2Base._write_chart_snapshots_if_due(strategy)
                StrategyV2Base._write_chart_snapshots_if_due(strategy)

                controller.processed_data['signal_state']['buy_decision'] = 'buy'
                StrategyV2Base._write_chart_snapshots_if_due(strategy)
                controller.processed_data['signal_state']['buy_decision'] = 'hold'
                StrategyV2Base._write_chart_snapshots_if_due(strategy)
                StrategyV2Base._write_chart_snapshots_if_due(strategy)

                release_first.set()
                await asyncio.wait_for(first_failure_reported.wait(), timeout=2)
                assert not acknowledgements, 'failed write must not acknowledge a controller batch'

                async def pump_until_final_ack():
                    while not final_acknowledged.is_set():
                        StrategyV2Base._write_chart_snapshots_if_due(strategy)
                        await asyncio.sleep(0.001)

                await asyncio.wait_for(pump_until_final_ack(), timeout=3)
            finally:
                release_first.set()
                await strategy._shutdown_chart_snapshot_writer()

        assert committed == acknowledgements
        assert attempt_rows[0] == committed[0], 'retry must preserve the exact failed batch'
        assert len(committed) == 3
        assert committed[0] == first
        assert committed[0] != committed[1]
        assert committed[1] != committed[2]
        assert next(row for row in committed[0] if row['candle_timestamp'] == 180000)['buy_decision'] == 'hold'
        assert next(row for row in committed[1] if row['candle_timestamp'] == 180000)['buy_decision'] == 'buy'
        assert next(row for row in committed[2] if row['candle_timestamp'] == 180000)['buy_decision'] == 'hold'

    asyncio.run(scenario())


def test_same_payload_can_be_built_again_after_prior_batch_is_acknowledged():
    import asyncio
    import copy
    from unittest.mock import patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        rows = [{'controller_id': 'a', 'timestamp': 1}]
        committed = []
        acknowledged = []
        acknowledgements_seen = asyncio.Event()

        def ack(persisted_rows):
            acknowledged.append(copy.deepcopy(persisted_rows))
            if len(acknowledged) == 2:
                acknowledgements_seen.set()

        controller = SimpleNamespace(
            build_chart_snapshot_rows=lambda **_kwargs: copy.deepcopy(rows),
            chart_snapshot_rows_persisted=ack,
        )
        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True, _is_stop_triggered=False,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=100,
            _last_chart_snapshot_prune_ts=100, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={'a': controller},
        )
        bind_chart_snapshot_methods(owner)
        recorder = SimpleNamespace(
            store_or_update_chart_snapshots=lambda batch: committed.append(copy.deepcopy(batch)),
            prune_chart_snapshots=lambda **_kwargs: None,
        )
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            try:
                StrategyV2Base._write_chart_snapshots_if_due(owner)

                async def wait_for_first_ack():
                    while len(acknowledged) < 1:
                        await asyncio.sleep(0.001)

                await asyncio.wait_for(wait_for_first_ack(), timeout=2)
                StrategyV2Base._write_chart_snapshots_if_due(owner)
                await asyncio.wait_for(acknowledgements_seen.wait(), timeout=2)
            finally:
                await owner._shutdown_chart_snapshot_writer()

        assert committed == [rows, rows]
        assert acknowledged == committed

    asyncio.run(scenario())


def test_prune_failure_retries_prune_without_rewriting_or_reacknowledging_rows():
    import asyncio
    from unittest.mock import Mock, patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        prune_failed = asyncio.Event()
        prune_retried = asyncio.Event()
        acknowledged = asyncio.Event()
        logger = SimpleNamespace(error=lambda *_args: prune_failed.set(), exception=Mock(), info=Mock())

        def build_once(**_kwargs):
            if acknowledged.is_set():
                return []
            return [{'controller_id': 'a', 'timestamp': 1}]

        controller = SimpleNamespace(
            build_chart_snapshot_rows=build_once,
            chart_snapshot_rows_persisted=lambda _rows: acknowledged.set(),
            chart_snapshot_prune_requests=lambda: [{'controller_id': 'a', 'pair': 'BTC-USDC',
                                                    'interval': '1m', 'keep_after_timestamp': 1}],
        )
        writes = []
        prunes = []

        def write(rows):
            writes.append(rows)

        def prune(**request):
            prunes.append(request)
            if len(prunes) == 1:
                raise RuntimeError('temporary prune failure')
            prune_retried.set()

        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=3600,
            _last_chart_snapshot_prune_ts=0, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={'a': controller}, _is_stop_triggered=False,
            logger=lambda: logger,
        )
        bind_chart_snapshot_methods(owner)
        recorder = SimpleNamespace(store_or_update_chart_snapshots=write, prune_chart_snapshots=prune)
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            await asyncio.wait_for(acknowledged.wait(), timeout=2)
            await asyncio.wait_for(prune_failed.wait(), timeout=2)
            assert owner._last_chart_snapshot_prune_ts == 0
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            for _ in range(100):
                if prune_retried.is_set():
                    break
                StrategyV2Base._write_chart_snapshots_if_due(owner)
                await asyncio.sleep(0.001)
            assert prune_retried.is_set(), len(prunes)
            await owner._shutdown_chart_snapshot_writer()

        assert len(writes) == 1
        assert len(prunes) == 2
        assert owner._last_chart_snapshot_prune_ts == 3600

    asyncio.run(scenario())


def test_prune_cycle_timestamp_waits_for_every_controller_prune():
    import asyncio
    import threading
    from unittest.mock import patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        commit_started = {name: threading.Event() for name in ('a', 'b')}
        release_commit = {name: threading.Event() for name in ('a', 'b')}
        acknowledgements = {name: asyncio.Event() for name in ('a', 'b')}
        prune_finished = {name: threading.Event() for name in ('a', 'b')}
        pruned = []

        class Controller:
            def __init__(self, name):
                self.name = name
                self.built = False

            def build_chart_snapshot_rows(self, **_kwargs):
                if self.built:
                    return []
                self.built = True
                return [{'controller_id': self.name, 'timestamp': 1}]

            def chart_snapshot_rows_persisted(self, _rows):
                acknowledgements[self.name].set()

            def chart_snapshot_prune_requests(self):
                return [{'controller_id': self.name, 'pair': 'BTC-USDC',
                         'interval': '1m', 'keep_after_timestamp': 1}]

        def write(rows):
            name = rows[0]['controller_id']
            commit_started[name].set()
            if not release_commit[name].wait(timeout=4):
                raise TimeoutError(f'test did not release {name}')

        def prune(**request):
            pruned.append(request['controller_id'])
            prune_finished[request['controller_id']].set()

        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=3600,
            _last_chart_snapshot_prune_ts=0, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={name: Controller(name) for name in ('a', 'b')}, _is_stop_triggered=False,
        )
        bind_chart_snapshot_methods(owner)
        recorder = SimpleNamespace(store_or_update_chart_snapshots=write, prune_chart_snapshots=prune)
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            assert commit_started['a'].wait(timeout=2)
            release_commit['a'].set()
            await asyncio.wait_for(acknowledgements['a'].wait(), timeout=2)
            assert prune_finished['a'].wait(timeout=2)
            await asyncio.sleep(0)
            assert owner._last_chart_snapshot_prune_ts == 0

            StrategyV2Base._write_chart_snapshots_if_due(owner)
            assert commit_started['b'].wait(timeout=2)
            release_commit['b'].set()
            await asyncio.wait_for(acknowledgements['b'].wait(), timeout=2)
            for _ in range(100):
                if prune_finished['b'].is_set():
                    break
                StrategyV2Base._write_chart_snapshots_if_due(owner)
                await asyncio.sleep(0.001)
            assert prune_finished['b'].is_set(), pruned
            await asyncio.sleep(0)
            assert owner._last_chart_snapshot_prune_ts == 3600
            await owner._shutdown_chart_snapshot_writer()

    asyncio.run(scenario())


def test_outstanding_commit_acknowledges_original_controller_when_disabled_and_replaced():
    import asyncio
    import threading
    from unittest.mock import Mock, patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        commit_started = threading.Event()
        release_commit = threading.Event()
        original_ack = asyncio.Event()
        enabled = {'value': True}
        original = SimpleNamespace(
            build_chart_snapshot_rows=Mock(return_value=[{'controller_id': 'a', 'timestamp': 1}]),
            chart_snapshot_rows_persisted=Mock(side_effect=lambda _rows: original_ack.set()),
        )
        replacement = SimpleNamespace(
            build_chart_snapshot_rows=Mock(return_value=[{'controller_id': 'a', 'timestamp': 2}]),
            chart_snapshot_rows_persisted=Mock(),
        )

        def commit(_rows):
            commit_started.set()
            if not release_commit.wait(timeout=4):
                raise TimeoutError('test did not release chart commit')

        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: enabled['value'], ready_to_trade=True,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=100,
            _last_chart_snapshot_prune_ts=100, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={'a': original}, _is_stop_triggered=False,
        )
        bind_chart_snapshot_methods(owner)
        recorder = SimpleNamespace(store_or_update_chart_snapshots=commit, prune_chart_snapshots=lambda **_kwargs: None)
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            assert commit_started.wait(timeout=2)
            owner.controllers['a'] = replacement
            enabled['value'] = False
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            release_commit.set()
            await asyncio.wait_for(original_ack.wait(), timeout=2)
            await owner._shutdown_chart_snapshot_writer()

        original.build_chart_snapshot_rows.assert_called_once_with(bot_name='main')
        original.chart_snapshot_rows_persisted.assert_called_once()
        replacement.build_chart_snapshot_rows.assert_not_called()
        replacement.chart_snapshot_rows_persisted.assert_not_called()

    asyncio.run(scenario())


def test_replaced_controller_is_not_called_for_successor_while_old_commit_is_outstanding():
    import asyncio
    import copy
    import threading
    from unittest.mock import Mock, patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        first_commit_started = threading.Event()
        release_first_commit = threading.Event()
        replacement_commit_started = threading.Event()
        old_acknowledged = asyncio.Event()
        replacement_acknowledged = asyncio.Event()
        original_build = Mock(return_value=[{'controller_id': 'a', 'timestamp': 1}])
        original = SimpleNamespace(
            build_chart_snapshot_rows=original_build,
            chart_snapshot_rows_persisted=Mock(side_effect=lambda _rows: old_acknowledged.set()),
            stop=Mock(),
        )
        replacement = SimpleNamespace(
            build_chart_snapshot_rows=Mock(return_value=[{'controller_id': 'a', 'timestamp': 2}]),
            chart_snapshot_rows_persisted=Mock(side_effect=lambda _rows: replacement_acknowledged.set()),
        )
        committed = []

        def commit(rows):
            committed.append(copy.deepcopy(rows))
            if len(committed) == 1:
                first_commit_started.set()
                if not release_first_commit.wait(timeout=4):
                    raise TimeoutError('test did not release original chart commit')
            else:
                replacement_commit_started.set()

        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True, _is_stop_triggered=False,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=100,
            _last_chart_snapshot_prune_ts=100, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={'a': original},
        )
        bind_chart_snapshot_methods(owner)
        recorder = SimpleNamespace(store_or_update_chart_snapshots=commit,
                                   prune_chart_snapshots=lambda **_kwargs: None)
        try:
            with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
                StrategyV2Base._write_chart_snapshots_if_due(owner)
                assert first_commit_started.wait(timeout=2)
                original.stop()
                owner.controllers['a'] = replacement
                StrategyV2Base._write_chart_snapshots_if_due(owner)
                old_builder_calls_after_stop = original_build.call_count
                release_first_commit.set()
                await asyncio.wait_for(old_acknowledged.wait(), timeout=2)
            assert old_builder_calls_after_stop == 1, 'a stopped controller must not build a successor batch'
            assert original_build.call_count == 1
            assert len(committed) == 1, committed
            assert committed[0] == [{'controller_id': 'a', 'timestamp': 1}]
            assert original.chart_snapshot_rows_persisted.call_count == 1
            replacement.chart_snapshot_rows_persisted.assert_not_called()

            with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
                StrategyV2Base._write_chart_snapshots_if_due(owner)
                assert replacement_commit_started.wait(timeout=2)
                await asyncio.wait_for(replacement_acknowledged.wait(), timeout=2)
        finally:
            release_first_commit.set()
            await owner._shutdown_chart_snapshot_writer()

        assert committed[1] == [{'controller_id': 'a', 'timestamp': 2}]
        original_build.assert_called_once_with(bot_name='main')
        replacement.build_chart_snapshot_rows.assert_called_once_with(bot_name='main')
        replacement.chart_snapshot_rows_persisted.assert_called_once()

    asyncio.run(scenario())


def test_stop_latch_blocks_new_chart_builder_calls():
    from unittest.mock import Mock, patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    controller = SimpleNamespace(build_chart_snapshot_rows=Mock())
    owner = SimpleNamespace(
        _chart_snapshots_enabled=lambda: True, ready_to_trade=True, _is_stop_triggered=True,
        controllers={'a': controller},
    )
    with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance') as get_recorder:
        StrategyV2Base._write_chart_snapshots_if_due(owner)
    get_recorder.assert_not_called()
    controller.build_chart_snapshot_rows.assert_not_called()


def test_chart_writer_shutdown_runs_after_strict_stop_failure_or_exception():
    import asyncio
    from unittest.mock import AsyncMock, Mock

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        logger = SimpleNamespace(info=Mock(), exception=Mock(), error=Mock())
        shutdown = AsyncMock()
        failed_result = SimpleNamespace(reconciliation_complete=False)
        orchestrator = SimpleNamespace(stop=AsyncMock(return_value=failed_result))
        owner = SimpleNamespace(
            _is_stop_triggered=False, controllers={}, _controllers_started=True,
            _controller_start_requested=True, listen_to_executor_actions_task=None,
            _shutdown_keep_position_preferences=lambda: {},
            executor_orchestrator=orchestrator, max_executors_close_attempts=1,
            _shutdown_chart_snapshot_writer=shutdown, logger=lambda: logger,
        )
        result = await StrategyV2Base.on_stop(owner, strict=True)
        assert result is failed_result
        shutdown.assert_awaited_once_with()

        shutdown.reset_mock()
        owner._is_stop_triggered = False
        owner.executor_orchestrator.stop = AsyncMock(side_effect=RuntimeError('strict stop failed'))
        try:
            await StrategyV2Base.on_stop(owner, strict=True)
        except RuntimeError as exc:
            assert str(exc) == 'strict stop failed'
        else:
            raise AssertionError('strict executor stop failure was hidden')
        shutdown.assert_awaited_once_with()

    asyncio.run(scenario())


def test_chart_writer_shutdown_deadline_does_not_wait_for_default_executor_capacity():
    import asyncio
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario():
        loop = asyncio.get_running_loop()
        default_pool = ThreadPoolExecutor(max_workers=1)
        loop.set_default_executor(default_pool)
        default_started = threading.Event()
        release_default = threading.Event()
        release_commit = threading.Event()
        commit_started = threading.Event()

        def occupy_default_pool():
            default_started.set()
            release_default.wait(timeout=3)

        default_work = loop.run_in_executor(None, occupy_default_pool)
        while not default_started.is_set():
            await asyncio.sleep(0)

        controller = SimpleNamespace(
            build_chart_snapshot_rows=lambda **_kwargs: [{'controller_id': 'a', 'timestamp': 1}],
            chart_snapshot_rows_persisted=lambda _rows: None,
        )
        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True, _is_stop_triggered=False,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=100,
            _last_chart_snapshot_prune_ts=100, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={'a': controller}, logger=lambda: __import__('logging').getLogger('test.chart_snapshot'),
        )
        bind_chart_snapshot_methods(owner)

        def write(_rows):
            commit_started.set()
            release_commit.wait(timeout=3)

        recorder = SimpleNamespace(store_or_update_chart_snapshots=write,
                                   prune_chart_snapshots=lambda **_kwargs: None)
        try:
            with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder), \
                    patch('hummingbot.strategy.strategy_v2_base.CHART_SNAPSHOT_WRITER_SHUTDOWN_TIMEOUT_SECONDS', 0.05):
                StrategyV2Base._write_chart_snapshots_if_due(owner)
                for _ in range(200):
                    if commit_started.is_set():
                        break
                    await asyncio.sleep(0.01)
                assert commit_started.is_set()
                started = time.monotonic()
                await asyncio.wait_for(owner._shutdown_chart_snapshot_writer(), timeout=0.5)
                elapsed = time.monotonic() - started
                assert elapsed < 0.5
                assert owner._chart_snapshot_writer._closing.is_set()
                assert owner._chart_snapshot_writer.is_alive
        finally:
            release_commit.set()
            release_default.set()
            owner._chart_snapshot_writer.close()
            await asyncio.wait_for(default_work, timeout=2)
            for _ in range(100):
                if not owner._chart_snapshot_writer.is_alive:
                    break
                await asyncio.sleep(0.01)
            assert not owner._chart_snapshot_writer.is_alive
            default_pool.shutdown(wait=True)

    asyncio.run(scenario())


def test_strict_stop_drains_admitted_chart_write_after_executor_cleanup():
    import asyncio
    import threading
    from unittest.mock import patch

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    async def scenario(executor_error=False):
        commit_started = threading.Event()
        release_commit = threading.Event()
        acknowledged = asyncio.Event()

        class Controller:
            def build_chart_snapshot_rows(self, **_kwargs):
                return [{'controller_id': 'a', 'timestamp': 1}]

            def chart_snapshot_rows_persisted(self, _rows):
                acknowledged.set()

            def stop(self):
                pass

        def write(_rows):
            commit_started.set()
            if not release_commit.wait(timeout=4):
                raise TimeoutError('test did not release chart write')

        async def stop_executors(*_args, **_kwargs):
            release_commit.set()
            if executor_error:
                raise RuntimeError('executor cleanup failed')
            return SimpleNamespace(reconciliation_complete=False)

        owner = SimpleNamespace(
            _chart_snapshots_enabled=lambda: True, ready_to_trade=True,
            _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=100,
            _last_chart_snapshot_prune_ts=100, _chart_snapshot_prune_interval=lambda: 3600,
            controllers={'a': Controller()}, _is_stop_triggered=False,
            _controllers_started=True, _controller_start_requested=True,
            listen_to_executor_actions_task=None, _shutdown_keep_position_preferences=lambda: {},
            executor_orchestrator=SimpleNamespace(stop=stop_executors),
            max_executors_close_attempts=1, logger=lambda: __import__('logging').getLogger('test.chart_snapshot'),
        )
        bind_chart_snapshot_methods(owner)
        recorder = SimpleNamespace(store_or_update_chart_snapshots=write,
                                   prune_chart_snapshots=lambda **_kwargs: None)
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            StrategyV2Base._write_chart_snapshots_if_due(owner)
            assert await asyncio.to_thread(commit_started.wait, 2)
            if executor_error:
                try:
                    await StrategyV2Base.on_stop(owner, strict=True)
                except RuntimeError as exc:
                    assert str(exc) == 'executor cleanup failed'
                else:
                    raise AssertionError('executor cleanup failure was hidden')
            else:
                result = await StrategyV2Base.on_stop(owner, strict=True)
                assert result.reconciliation_complete is False
            await asyncio.wait_for(acknowledged.wait(), timeout=2)

        assert owner._chart_snapshot_writer.is_alive is False
        assert not owner._chart_snapshot_jobs

    asyncio.run(scenario())
    asyncio.run(scenario(executor_error=True))


def test_tick_continues_executor_decisions_when_chart_persistence_fails():
    from unittest.mock import Mock

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    owner = SimpleNamespace(
        controllers={'a': object()}, update_executors_info=Mock(),
        _unattended_restart_hold=False,
        _persist_controller_configs_if_safe=Mock(),
        _start_controllers_if_safe=Mock(),
        _sync_runtime_state_to_db_if_due=Mock(), _write_chart_snapshots_if_due=Mock(side_effect=RuntimeError('disk full')),
        _write_runtime_status_snapshot_if_due=Mock(), update_controllers_configs=Mock(),
        market_data_provider=SimpleNamespace(ready=True), _is_stop_triggered=False,
        determine_executor_actions=Mock(return_value=['risk-action']),
        executor_orchestrator=SimpleNamespace(resume_planned_positions_if_ready=Mock(return_value=True),
                                              execute_action=Mock()), logger=Mock())
    StrategyV2Base.on_tick(owner)
    owner._start_controllers_if_safe.assert_called_once_with()
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
    import asyncio
    from unittest.mock import Mock, patch

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
    failed = asyncio.Event()
    logger = SimpleNamespace(error=lambda *_args: failed.set(), exception=Mock(), info=Mock())
    strategy = SimpleNamespace(_chart_snapshots_enabled=lambda: True, ready_to_trade=True,
                               _runtime_status_snapshot_bot_name=lambda: 'main', current_timestamp=100,
                               _last_chart_snapshot_prune_ts=100, _chart_snapshot_prune_interval=lambda: 60,
                               controllers={'a': controller}, _is_stop_triggered=False,
                               logger=lambda: logger)
    bind_chart_snapshot_methods(strategy)

    async def run_failed_batch():
        with patch('hummingbot.strategy.strategy_v2_base.MarketsRecorder.get_instance', return_value=recorder):
            StrategyV2Base._write_chart_snapshots_if_due(strategy)
            await asyncio.wait_for(failed.wait(), timeout=2)
            await strategy._shutdown_chart_snapshot_writer()

    asyncio.run(run_failed_batch())
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
