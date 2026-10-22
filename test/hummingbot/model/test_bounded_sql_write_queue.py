import asyncio
import tempfile
import threading
from pathlib import Path

import pytest
from sqlalchemy.exc import IntegrityError

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.model.metadata import Metadata
from hummingbot.model.sql_connection_manager import (
    SQLConnectionManager,
    SQLConnectionType,
    SQLWriteCancelledAfterCommit,
    SQLWriteQueueFull,
)


def test_bounded_sql_write_queue_orders_acknowledged_commits_and_drains_on_close():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        writer = manager.write_queue
        first_started = threading.Event()
        release_first = threading.Event()
        committed_order = []
        writer_threads = []

        def persist(key, wait_for_release=False):
            writer_threads.append(threading.current_thread().name)
            if wait_for_release:
                first_started.set()
                if not release_first.wait(timeout=8):
                    raise TimeoutError('test did not release first SQL write')
            with manager.get_new_session() as session:
                with session.begin():
                    session.add(Metadata(key=key, value=key))
            committed_order.append(key)
            return key

        first = asyncio.create_task(writer.write(lambda: persist('first', True)))
        assert await asyncio.wait_for(asyncio.to_thread(first_started.wait), timeout=4)
        second = asyncio.create_task(writer.write(lambda: persist('second')))
        await asyncio.sleep(0)
        third = asyncio.create_task(writer.write(lambda: persist('third')))
        await asyncio.sleep(0)
        assert writer._queue.full()
        assert not third.done()
        with pytest.raises(SQLWriteQueueFull, match='queue and bounded admission'):
            await writer.write(lambda: persist('overflow'))

        try:
            close_one = asyncio.create_task(writer.close())
            close_two = asyncio.create_task(writer.close())
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(writer.close(), timeout=0.01)
            assert not first.done()
            release_first.set()
            assert await asyncio.gather(first, second, third) == ['first', 'second', 'third']
            await asyncio.gather(close_one, close_two)
        finally:
            release_first.set()
            await manager.close_write_queue()

        assert committed_order == ['first', 'second', 'third']
        assert writer_threads and all(name.startswith('hummingbot-sql-write') for name in writer_threads)
        with manager.get_new_session() as session:
            rows = session.query(Metadata).filter(Metadata.key.in_(['first', 'second', 'third'])).all()
        assert {row.key for row in rows} == {'first', 'second', 'third'}
        with pytest.raises(RuntimeError, match='closing or closed'):
            await writer.write(lambda: None)
        manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-bounded-writer-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'writer.sqlite'))


def test_bounded_sql_write_queue_propagates_commit_failure_and_remains_usable():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        writer = manager.write_queue

        with manager.get_new_session() as session:
            session.add(Metadata(key='duplicate', value='existing'))
            session.commit()

        def fail():
            with manager.get_new_session() as session:
                with session.begin():
                    session.add(Metadata(key='duplicate', value='duplicate'))

        try:
            with pytest.raises(IntegrityError):
                await writer.write(fail)

            def persist_after_failure():
                with manager.get_new_session() as session:
                    with session.begin():
                        session.add(Metadata(key='after-failure', value='committed'))
                return 'committed'

            assert await writer.write(persist_after_failure) == 'committed'
            with manager.get_new_session() as session:
                row = session.get(Metadata, 'after-failure')
            assert row is not None and row.value == 'committed'
        finally:
            await manager.close_write_queue()
            manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-bounded-writer-failure-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'writer.sqlite'))


def test_bounded_sql_write_queue_repeated_cancellation_waits_for_durable_outcome():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        writer = manager.write_queue
        operation_started = threading.Event()
        release_operation = threading.Event()

        def persist():
            operation_started.set()
            if not release_operation.wait(timeout=8):
                raise TimeoutError('test did not release cancelled SQL write')
            with manager.get_new_session() as session:
                with session.begin():
                    session.add(Metadata(key='cancelled-write', value='committed'))
            return 'committed'

        task = asyncio.create_task(writer.write(persist))
        assert await asyncio.wait_for(asyncio.to_thread(operation_started.wait), timeout=4)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release_operation.set()
        with pytest.raises(SQLWriteCancelledAfterCommit) as cancelled:
            await task
        assert cancelled.value.result == 'committed'

        with manager.get_new_session() as session:
            row = session.get(Metadata, 'cancelled-write')
        assert row is not None and row.value == 'committed'
        await manager.close_write_queue()
        manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-bounded-writer-cancel-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'writer.sqlite'))


def test_sql_queue_cancellation_after_completion_still_reports_cancelled_commit():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        writer = manager.write_queue
        loop = asyncio.get_running_loop()
        original_create_future = loop.create_future
        completions = []
        loop.create_future = lambda: completions.append(original_create_future()) or completions[-1]
        try:
            task = asyncio.create_task(writer.write(lambda: 'committed'))
            while not completions:
                await asyncio.sleep(0)
            completions[0].add_done_callback(lambda _: task.cancel())
            with pytest.raises(SQLWriteCancelledAfterCommit) as cancelled:
                await task
            assert cancelled.value.result == 'committed'
        finally:
            loop.create_future = original_create_future
            await manager.close_write_queue()
            manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-bounded-writer-race-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'writer.sqlite'))


def test_sql_queue_cancellation_after_failed_completion_preserves_write_error():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        writer = manager.write_queue
        loop = asyncio.get_running_loop()
        original_create_future = loop.create_future
        completions = []
        loop.create_future = lambda: completions.append(original_create_future()) or completions[-1]
        try:
            task = asyncio.create_task(writer.write(lambda: (_ for _ in ()).throw(ValueError('write failed'))))
            while not completions:
                await asyncio.sleep(0)
            completions[0].add_done_callback(lambda _: task.cancel())
            with pytest.raises(ValueError, match='write failed'):
                await task
        finally:
            loop.create_future = original_create_future
            await manager.close_write_queue()
            manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-bounded-writer-failure-race-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'writer.sqlite'))


def test_async_risk_state_write_acknowledges_atomic_risk_and_operator_receipt_commit():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        recorder = MarketsRecorder(
            sql=manager,
            markets=[],
            config_file_path='async-risk-config',
            strategy_name='test_strategy',
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )
        state = {'version': 2, 'stopped': False, 'sequence': 1}
        receipt = {'controller_id': 'controller-1', 'command_id': 'command-1', 'requested_at': 12.5}
        try:
            await recorder.store_strategy_risk_state_async(state, operator_receipts=(receipt,))
            assert recorder.load_strategy_risk_state() == state
            assert recorder.has_operator_command('controller-1', 'command-1')

            with pytest.raises(ValueError, match='already applied'):
                await recorder.store_strategy_risk_state_async(
                    {'version': 2, 'stopped': True, 'sequence': 2}, operator_receipts=(receipt,)
                )
            assert recorder.load_strategy_risk_state() == state
        finally:
            await manager.close_write_queue()
            manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-async-risk-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'risk.sqlite'))
