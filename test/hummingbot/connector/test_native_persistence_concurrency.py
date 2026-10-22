import asyncio
import logging
import tempfile
import threading
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import event

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.model.market_state import MarketState
from hummingbot.model.metadata import Metadata
from hummingbot.model.position import Position
from hummingbot.model.sql_connection_manager import (
    SQLConnectionManager,
    SQLConnectionType,
    SQLWriteCancelledAfterCommit,
)
from hummingbot.strategy.chart_snapshot_writer import BoundedChartSnapshotWriter
from hummingbot.strategy_v2.models.base import RunnableStatus


@pytest.fixture(autouse=True)
def isolate_markets_recorder_singleton():
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    try:
        yield
    finally:
        MarketsRecorder._shared_instance = previous


def test_chart_position_and_risk_writes_share_bounded_lane_and_preserve_loop_progress():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        recorder = MarketsRecorder(
            sql=manager,
            markets=[],
            config_file_path="test_config",
            strategy_name="test_strategy",
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )
        loop = asyncio.get_running_loop()
        chart_inserted = threading.Event()
        release_chart = threading.Event()
        chart_ack = asyncio.Event()
        chart_errors = []

        def after_cursor_execute(_conn, _cursor, statement, _parameters, _context, _many):
            if statement.lstrip().upper().startswith('INSERT INTO "CHARTSNAPSHOT"'):
                chart_inserted.set()
                if not release_chart.wait(timeout=8):
                    raise TimeoutError('test failed to release chart transaction')

        event.listen(manager.engine, 'after_cursor_execute', after_cursor_execute)

        chart_job = SimpleNamespace(recorder=recorder)

        def on_complete(_job, _phase, error):
            if error is not None:
                chart_errors.append(error)
            chart_ack.set()

        writer = BoundedChartSnapshotWriter(loop, logging.getLogger('test.persistence'), on_complete)
        assert writer.submit(chart_job, 'write', [{
            'bot_name': 'test_bot',
            'controller_id': 'test_controller',
            'pair': 'BTC-USDC',
            'interval': '1m',
            'candle_timestamp': 1_800_000_000_000,
            'open_price': 100.0,
            'high_price': 101.0,
            'low_price': 99.0,
            'close_price': 100.5,
        }])
        assert await asyncio.wait_for(asyncio.to_thread(chart_inserted.wait), timeout=4)

        heartbeat = asyncio.Event()
        loop.call_soon(heartbeat.set)
        position = Position(
            id='position-1', controller_id='test_controller', connector_name='okx',
            trading_pair='BTC-USDC', side=TradeType.BUY.name, timestamp=1,
            volume_traded_quote=Decimal('100'), amount=Decimal('1'),
            breakeven_price=Decimal('100'), unrealized_pnl_quote=Decimal('0'),
            realized_pnl_quote=Decimal('0'), cum_fees_quote=Decimal('0'),
        )
        position_task = asyncio.create_task(recorder.store_live_position_snapshot_async(
            [{
                'id': position.id,
                'controller_id': position.controller_id,
                'connector_name': position.connector_name,
                'trading_pair': position.trading_pair,
                'side': position.side,
                'timestamp': position.timestamp,
                'volume_traded_quote': position.volume_traded_quote,
                'amount': position.amount,
                'breakeven_price': position.breakeven_price,
                'unrealized_pnl_quote': position.unrealized_pnl_quote,
                'realized_pnl_quote': position.realized_pnl_quote,
                'cum_fees_quote': position.cum_fees_quote,
            }], ['test_controller'], 1
        ))
        risk_task = asyncio.create_task(recorder.store_strategy_risk_state_async(
            {'version': 1, 'stopped': False, 'sequence': 1}
        ))
        await asyncio.sleep(0)
        heartbeat_ran_before_chart_release = heartbeat.is_set()
        assert not position_task.done() and not risk_task.done()

        release_chart.set()
        await asyncio.wait_for(chart_ack.wait(), timeout=4)
        await asyncio.wait_for(asyncio.gather(position_task, risk_task), timeout=4)
        assert not chart_errors, chart_errors
        writer.close()
        assert await asyncio.to_thread(writer.join, 4)

        with manager.get_new_session() as session:
            saved_position = session.get(Position, 'position-1')
            saved_risk = recorder.load_strategy_risk_state()
            chart_rows = session.execute(__import__('sqlalchemy').text('SELECT COUNT(*) FROM ChartSnapshot')).scalar_one()

        event.remove(manager.engine, 'after_cursor_execute', after_cursor_execute)
        manager.engine.dispose()

        assert heartbeat_ran_before_chart_release
        assert saved_position is not None and saved_position.amount == Decimal('1.000000')
        assert saved_risk == {'sequence': 1, 'stopped': False, 'version': 1}
        assert chart_rows == 1

    with tempfile.TemporaryDirectory(prefix='hummingbot-native-persistence-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'runtime.sqlite'))


def test_old_runtime_position_capture_cannot_reopen_newer_terminal_snapshot():
    with tempfile.TemporaryDirectory(prefix='hummingbot-position-sequence-') as temp_dir:
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
            db_path=str(Path(temp_dir) / 'runtime.sqlite'),
        )
        recorder = _recorder(manager, MagicMock())
        recorder.enable_async_financial_events()
        base = {
            'id': 'position-1', 'controller_id': 'controller-1', 'connector_name': 'okx',
            'trading_pair': 'BTC-USDC', 'side': TradeType.BUY.name, 'timestamp': 100,
            'volume_traded_quote': Decimal('100'), 'amount': Decimal('1'),
            'breakeven_price': Decimal('100'), 'unrealized_pnl_quote': Decimal('0'),
            'realized_pnl_quote': Decimal('0'), 'cum_fees_quote': Decimal('0'),
        }
        recorder.store_live_position_snapshot([base], ['controller-1'], 100, {'controller-1': 9})
        old_capture = {**base, 'amount': Decimal('1.25'), 'timestamp': 110}
        terminal_capture = {**base, 'amount': Decimal('0'), 'timestamp': 120}
        recorder.store_live_position_snapshot(
            [terminal_capture], ['controller-1'], 120, {'controller-1': 11})
        recorder.store_live_position_snapshot([old_capture], ['controller-1'], 110, {'controller-1': 10})
        with manager.get_new_session() as session:
            assert session.query(Position).filter_by(id='position-1').one_or_none() is None
            marker = session.get(Metadata, 'native-recorder-sequence:test_config:position:controller-1')
            assert marker.value == '11'
        manager.engine.dispose()


def test_native_fill_callback_uses_bounded_off_loop_writer_behind_chart_transaction():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        market = MagicMock()
        market.display_name = 'test_market'
        market.name = 'test_market'
        market.tracking_states = {}
        recorder = MarketsRecorder(
            sql=manager,
            markets=[market],
            config_file_path='test_config',
            strategy_name='test_strategy',
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )
        loop = asyncio.get_running_loop()
        chart_inserted = threading.Event()
        release_chart = threading.Event()
        chart_ack = asyncio.Event()
        chart_errors = []

        def after_cursor_execute(_conn, _cursor, statement, _parameters, _context, _many):
            if statement.lstrip().upper().startswith('INSERT INTO "CHARTSNAPSHOT"'):
                chart_inserted.set()
                if not release_chart.wait(timeout=8):
                    raise TimeoutError('test failed to release chart transaction')

        event.listen(manager.engine, 'after_cursor_execute', after_cursor_execute)
        recorder.enable_async_financial_events()
        recorder.start()
        writer = BoundedChartSnapshotWriter(
            loop, logging.getLogger('test.persistence'),
            lambda _job, _phase, error: (chart_errors.append(error) if error else None, chart_ack.set()),
        )
        assert writer.submit(SimpleNamespace(recorder=recorder), 'write', [{
            'bot_name': 'test_bot', 'controller_id': 'test_controller', 'pair': 'BTC-USDC',
            'interval': '1m', 'candle_timestamp': 1_800_000_000_001,
            'open_price': 100.0, 'high_price': 101.0, 'low_price': 99.0, 'close_price': 100.5,
        }])
        assert await asyncio.wait_for(asyncio.to_thread(chart_inserted.wait), timeout=4)
        heartbeat = asyncio.Event()
        loop.call_soon(heartbeat.set)
        fill = OrderFilledEvent(
            timestamp=1_800_000_000, order_id='order-1', trading_pair='BTC-USDC',
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal('100'),
            amount=Decimal('1'), trade_fee=AddedToCostTradeFee(), exchange_trade_id='trade-1',
        )
        start = time.monotonic()
        recorder._did_fill_order(MarketEvent.OrderFilled.value, market, fill)
        fill_ack = asyncio.create_task(recorder.flush_financial_events())
        await asyncio.sleep(0)
        blocked_seconds = time.monotonic() - start
        heartbeat_ran = heartbeat.is_set()
        release_chart.set()
        await asyncio.wait_for(chart_ack.wait(), timeout=4)
        await asyncio.wait_for(fill_ack, timeout=4)
        writer.close()
        assert await asyncio.to_thread(writer.join, 4)
        with manager.get_new_session() as session:
            from hummingbot.model.trade_fill import TradeFill
            saved_fill = session.query(TradeFill).filter_by(order_id='order-1', exchange_trade_id='trade-1').one()
        event.remove(manager.engine, 'after_cursor_execute', after_cursor_execute)
        await manager.write_queue.close()
        manager.engine.dispose()
        assert not chart_errors, chart_errors
        assert heartbeat_ran and blocked_seconds < 0.5, (heartbeat_ran, blocked_seconds)
        assert saved_fill.amount_decimal == Decimal('1.000000')

    with tempfile.TemporaryDirectory(prefix='hummingbot-fill-callback-lock-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'runtime.sqlite'))


def test_delayed_runtime_executor_snapshot_cannot_reopen_terminal_executor():
    async def scenario(db_path: Path):
        from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor

        from hummingbot.model.executors import Executors

        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        recorder = MarketsRecorder(
            sql=manager, markets=[], config_file_path='test_config', strategy_name='test_strategy',
            market_data_collection=MarketDataCollectionConfigMap(market_data_collection_enabled=False),
        )
        recorder.enable_async_financial_events()
        executor = make_executor()
        info = executor.executor_info
        old_capture = recorder.capture_executor_info_payload(info.model_dump_json())
        terminal = info.model_copy(update={
            'status': RunnableStatus.TERMINATED,
            'is_active': False, 'is_trading': False,
        })
        new_capture = recorder.capture_executor_info_payload(terminal.model_dump_json())
        try:
            await manager.write_queue.write(lambda: recorder._store_executor_info_payload(new_capture))
            # A periodic snapshot captured before closure reaches SQL afterwards.
            await manager.write_queue.write(lambda: recorder._store_executor_info_payload(old_capture))
            with manager.get_new_session() as session:
                stored = session.get(Executors, info.id)
            assert stored.status == terminal.status.value
            assert stored.is_active is False
        finally:
            await manager.close_write_queue()
            manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-executor-snapshot-order-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'runtime.sqlite'))


def _recorder(manager, market):
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    if isinstance(market, MagicMock):
        market.name = market.display_name = 'test_market'
    return MarketsRecorder(
        sql=manager, markets=[market], config_file_path='test_config', strategy_name='test_strategy',
        market_data_collection=MarketDataCollectionConfigMap(
            market_data_collection_enabled=False, market_data_collection_interval=60,
            market_data_collection_depth=20,
        ),
    )


def _fill_event(trade_id, timestamp):
    return OrderFilledEvent(
        timestamp=timestamp, order_id='order-1', trading_pair='BTC-USDC',
        trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal('100'),
        amount=Decimal('1'), trade_fee=AddedToCostTradeFee(), exchange_trade_id=trade_id,
    )


def test_bounded_financial_overflow_replays_exact_tracker_receipt_once():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        tracker_order = SimpleNamespace(
            client_order_id='order-1', trading_pair='BTC-USDC', trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT, leverage=1, position=PositionAction.NIL,
            order_fills={}, recorder_pending_events={},
        )
        tracker = SimpleNamespace(all_fillable_orders={'order-1': tracker_order})
        market = MagicMock()
        market.display_name = market.name = 'test_market'
        market.tracking_states = {}
        market._order_tracker = tracker
        market.add_trade_fills_from_market_recorder = MagicMock()
        recorder = _recorder(manager, market)
        recorder._financial_event_queue = asyncio.Queue(maxsize=1)
        recorder.enable_async_financial_events()
        recorder.start()
        first = TradeUpdate(
            trade_id='trade-1', client_order_id='order-1', exchange_order_id='exchange-1',
            trading_pair='BTC-USDC', fill_timestamp=100.0, fill_price=Decimal('100'),
            fill_base_amount=Decimal('1'), fill_quote_amount=Decimal('100'),
            fee=AddedToCostTradeFee(), receipt_timestamp=101.0,
        )
        second = first._replace(trade_id='trade-2', fill_timestamp=102.0, receipt_timestamp=103.0)
        tracker_order.order_fills.update({'trade-1': first, 'trade-2': second})
        recorder._did_fill_order(MarketEvent.OrderFilled.value, market, _fill_event('trade-1', 101.0))
        recorder._did_fill_order(MarketEvent.OrderFilled.value, market, _fill_event('trade-2', 103.0))
        assert recorder._financial_event_reconciliation_needed
        await asyncio.wait_for(recorder.flush_financial_events(), timeout=4)
        await asyncio.wait_for(recorder.flush_financial_events(), timeout=4)
        with manager.get_new_session() as session:
            from hummingbot.model.trade_fill import TradeFill
            rows = session.query(TradeFill).filter_by(order_id='order-1').order_by(TradeFill.exchange_trade_id).all()
            assert [(row.exchange_trade_id, row.timestamp) for row in rows] == [
                ('trade-1', 101000), ('trade-2', 103000),
            ]
        await manager.write_queue.close()
        manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-financial-overflow-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'runtime.sqlite'))


def test_failed_financial_commit_retains_receipt_until_retry_ack():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        market = MagicMock()
        market.display_name = market.name = 'test_market'
        market.tracking_states = {}
        market.add_trade_fills_from_market_recorder = MagicMock()
        recorder = _recorder(manager, market)
        recorder.enable_async_financial_events()
        recorder.start()
        event = _fill_event('trade-retry', 104.0)
        recorder._did_fill_order(MarketEvent.OrderFilled.value, market, event)
        original_write = manager.write_queue.write
        attempts = 0

        async def fail_once(operation):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError('injected commit failure')
            return await original_write(operation)

        manager.write_queue.write = fail_once
        with pytest.raises(RuntimeError, match='not been durably acknowledged'):
            await recorder.flush_financial_events()
        assert recorder._financial_event_failed
        assert ('test_market', 'order-1', 'trade-retry') in recorder._financial_event_pending
        await asyncio.wait_for(recorder.flush_financial_events(), timeout=4)
        with manager.get_new_session() as session:
            from hummingbot.model.trade_fill import TradeFill
            rows = session.query(TradeFill).filter_by(order_id='order-1', exchange_trade_id='trade-retry').all()
            assert len(rows) == 1 and rows[0].timestamp == 104000
        await manager.write_queue.close()
        manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-financial-retry-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'runtime.sqlite'))


def test_late_fill_replay_cannot_replace_newer_order_market_snapshot():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        market = MagicMock()
        market.display_name = market.name = 'test_market'
        market.tracking_states = {'snapshot': {'version': 'fill'}}
        recorder = _recorder(manager, market)
        recorder.enable_async_financial_events()

        # Freeze the fill first, then an order callback, and persist in reverse
        # order. The later capture must remain the canonical MarketState even
        # when the older financial receipt is replayed afterwards.
        _, captured_fill = recorder._capture_financial_fill(
            MarketEvent.OrderFilled.value, market, _fill_event('trade-sequence', 105.0)
        )
        fill_payload = captured_fill['payload']
        market.tracking_states = {'snapshot': {'version': 'order'}}
        status_payload = {
            'order_id': 'order-1', 'config_file_path': 'test_config', 'market': 'test_market',
            'timestamp': 106000, 'event_status': 'OPEN', 'creation_if_missing': None,
            **recorder._capture_market_state_payload(market),
        }
        assert fill_payload['recorder_sequence'] < status_payload['recorder_sequence']

        recorder._persist_order_status_payload(status_payload)
        recorder._persist_financial_fill(fill_payload)

        with manager.get_new_session() as session:
            saved = session.query(MarketState).filter_by(
                config_file_path='test_config', market='test_market'
            ).one()
            assert saved.saved_state == {'snapshot': {'version': 'order'}}
            marker = session.get(Metadata, 'native-recorder-sequence:test_config:market:test_market')
            assert marker.value == str(status_payload['recorder_sequence'])
        await manager.write_queue.close()
        manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-fill-order-sequence-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'runtime.sqlite'))


@pytest.mark.parametrize('state_name', ['pending', 'failed'])
def test_duplicate_financial_identity_requires_same_immutable_receipt(state_name):
    with tempfile.TemporaryDirectory(prefix='hummingbot-financial-identity-') as temp_dir:
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
            db_path=str(Path(temp_dir) / 'runtime.sqlite'),
        )
        recorder = _recorder(manager, MagicMock())
        identity = ('test_market', 'order-1', 'trade-identity')
        original_source = {'identity': identity, 'payload': {
            'order_id': 'order-1', 'exchange_trade_id': 'trade-identity', 'amount': '1.0',
            'price': '100.0', 'trade_fee_json': '{"flat_fees":[]}', 'market_state_json': '{"a":1}',
            'market_state_timestamp': 100, 'recorder_sequence': 4,
        }}
        existing = {'identity': identity, 'source': original_source}
        getattr(recorder, f'_financial_event_{state_name}')[identity] = existing

        same_receipt_new_projection = {'identity': identity, 'payload': {
            **original_source['payload'], 'market_state_json': '{"a":2}',
            'market_state_timestamp': 101, 'recorder_sequence': 5,
        }}
        assert recorder.enqueue_financial_write(
            identity, lambda: None, kind='fill', source=same_receipt_new_projection,
        ) is True

        conflicting_receipt = {'identity': identity, 'payload': {
            **original_source['payload'], 'amount': '2.0',
        }}
        assert recorder.enqueue_financial_write(
            identity, lambda: None, kind='fill', source=conflicting_receipt,
        ) is False
        assert recorder._financial_event_error is not None
        assert recorder._financial_event_reconciliation_needed
        assert identity in getattr(recorder, f'_financial_event_{state_name}')
        manager.engine.dispose()


def test_replay_only_financial_receipt_is_visible_as_unacknowledged():
    with tempfile.TemporaryDirectory(prefix='hummingbot-financial-replay-status-') as temp_dir:
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
            db_path=str(Path(temp_dir) / 'runtime.sqlite'),
        )
        recorder = _recorder(manager, MagicMock())
        recorder._async_financial_events = True
        identity = ('test_market', 'order-1', 'replay-only')
        recorder.register_financial_replay(identity, lambda: False)
        status = recorder.financial_persistence_status()
        assert status['pending_count'] == 1
        assert status['reconciliation_needed'] is False
        assert status['acknowledged'] is False
        manager.engine.dispose()


def test_financial_commit_cancellation_acknowledges_receipt_before_flush_returns():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        market = MagicMock()
        market.display_name = market.name = 'test_market'
        market.tracking_states = {}
        market.add_trade_fills_from_market_recorder = MagicMock()
        recorder = _recorder(manager, market)
        recorder.enable_async_financial_events()
        recorder._did_fill_order(MarketEvent.OrderFilled.value, market, _fill_event('trade-cancelled', 107.0))

        original_write = manager.write_queue.write

        async def commit_then_cancel(operation):
            result = await original_write(operation)
            raise SQLWriteCancelledAfterCommit(result)

        manager.write_queue.write = commit_then_cancel
        await asyncio.wait_for(recorder.flush_financial_events(), timeout=4)
        identity = ('test_market', 'order-1', 'trade-cancelled')
        assert identity not in recorder._financial_event_pending
        assert identity not in recorder._financial_event_failed
        calls = market.add_trade_fills_from_market_recorder.call_args_list
        # Recorder startup restores an empty set before the committed fill is
        # acknowledged; assert the actual receipt is applied exactly once.
        assert sum(any(getattr(item, 'exchange_trade_id', None) == 'trade-cancelled'
                       for item in call.args[0]) for call in calls) == 1
        with manager.get_new_session() as session:
            from hummingbot.model.trade_fill import TradeFill
            assert session.query(TradeFill).filter_by(
                order_id='order-1', exchange_trade_id='trade-cancelled'
            ).count() == 1
        await manager.write_queue.close()
        manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-financial-cancel-ack-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'runtime.sqlite'))


def test_tracker_ingress_ack_precedes_concurrent_terminal_cleanup():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        market = MagicMock()
        market.display_name = market.name = 'test_market'
        market.tracking_states = {}
        market.add_trade_fills_from_market_recorder = MagicMock()
        tracker = None
        recorder = _recorder(manager, market)

        def dispatch(event_tag, event):
            if event_tag == MarketEvent.OrderFilled:
                recorder._did_fill_order(event_tag.value, market, event)

        connector = SimpleNamespace(current_timestamp=108.0, trigger_event=dispatch)
        tracker = ClientOrderTracker(connector)
        market._order_tracker = tracker
        order = InFlightOrder(
            client_order_id='order-race', exchange_order_id='exchange-race', trading_pair='BTC-USDC',
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal('1'),
            creation_timestamp=100.0, price=Decimal('100'),
        )
        tracker.start_tracking_order(order)
        recorder.enable_async_financial_events()
        recorder.start()
        fill = TradeUpdate(
            trade_id='trade-race', client_order_id='order-race', exchange_order_id='exchange-race',
            trading_pair='BTC-USDC', fill_timestamp=107.0, fill_price=Decimal('100'),
            fill_base_amount=Decimal('1'), fill_quote_amount=Decimal('100'), fee=AddedToCostTradeFee(),
        )
        assert tracker.process_trade_update(fill)
        terminal = OrderUpdate(
            trading_pair='BTC-USDC', update_timestamp=109.0, new_state=OrderState.FILLED,
            client_order_id='order-race', exchange_order_id='exchange-race',
        )
        await asyncio.wait_for(tracker._process_order_update(terminal), timeout=4)
        assert tracker.fetch_cached_order('order-race') is not None
        with manager.get_new_session() as session:
            from hummingbot.model.trade_fill import TradeFill
            row = session.query(TradeFill).filter_by(order_id='order-race',
                                                     exchange_trade_id='trade-race').one()
            assert row.timestamp == 108000
        await manager.write_queue.close()
        manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-terminal-ack-race-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'runtime.sqlite'))


def test_duplicate_fill_redelivery_retries_failed_durable_ack():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        market = MagicMock()
        market.display_name = market.name = 'test_market'
        market.tracking_states = {}
        market.add_trade_fills_from_market_recorder = MagicMock()
        recorder = _recorder(manager, market)

        def dispatch(event_tag, event):
            if event_tag == MarketEvent.OrderFilled:
                recorder._did_fill_order(event_tag.value, market, event)

        connector = SimpleNamespace(current_timestamp=109.0, trigger_event=dispatch)
        tracker = ClientOrderTracker(connector)
        market._order_tracker = tracker
        order = InFlightOrder(
            client_order_id='order-redelivery', exchange_order_id='exchange-redelivery', trading_pair='BTC-USDC',
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal('1'),
            creation_timestamp=100.0, price=Decimal('100'),
        )
        tracker.start_tracking_order(order)
        recorder.enable_async_financial_events()
        recorder.start()
        fill = TradeUpdate(
            trade_id='trade-redelivery', client_order_id='order-redelivery', exchange_order_id='exchange-redelivery',
            trading_pair='BTC-USDC', fill_timestamp=108.0, fill_price=Decimal('100'),
            fill_base_amount=Decimal('1'), fill_quote_amount=Decimal('100'), fee=AddedToCostTradeFee(),
            receipt_timestamp=109.0,
        )
        original_write = manager.write_queue.write
        attempts = 0

        async def fail_once(operation):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError('injected commit failure')
            return await original_write(operation)

        manager.write_queue.write = fail_once
        with pytest.raises(RuntimeError, match='not been durably acknowledged'):
            await tracker.process_trade_update_and_wait(fill)
        assert await tracker.process_trade_update_and_wait(fill) is False
        with manager.get_new_session() as session:
            from hummingbot.model.trade_fill import TradeFill
            rows = session.query(TradeFill).filter_by(
                order_id='order-redelivery', exchange_trade_id='trade-redelivery'
            ).all()
            assert len(rows) == 1 and rows[0].timestamp == 109000
        await manager.write_queue.close()
        manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-fill-redelivery-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'runtime.sqlite'))


def test_terminal_tracker_receipt_restores_and_reconciles_startup_ack():
    async def scenario(db_path: Path):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_path=str(db_path)
        )
        connector = SimpleNamespace(current_timestamp=105.0, trigger_event=MagicMock())
        order = InFlightOrder(
            client_order_id='order-terminal', exchange_order_id='exchange-terminal', trading_pair='BTC-USDC',
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal('1'),
            creation_timestamp=99.0, price=Decimal('100'),
        )
        update = TradeUpdate(
            trade_id='trade-terminal', client_order_id='order-terminal', exchange_order_id='exchange-terminal',
            trading_pair='BTC-USDC', fill_timestamp=105.0, fill_price=Decimal('100'),
            fill_base_amount=Decimal('1'), fill_quote_amount=Decimal('100'), fee=AddedToCostTradeFee(),
            receipt_timestamp=106.0,
        )
        order.update_with_trade_update(update)
        order.update_with_order_update(OrderUpdate(
            trading_pair='BTC-USDC', update_timestamp=107.0, new_state=OrderState.FILLED,
            client_order_id='order-terminal', exchange_order_id='exchange-terminal',
        ))
        tracking_state = {'order-terminal': order.to_json()}
        restored = ClientOrderTracker(connector)

        def restore(states):
            restored.restore_tracking_states(states)

        market = MagicMock()
        market.display_name = market.name = 'test_market'
        market.tracking_states = {}
        market._order_tracker = restored
        market.restore_tracking_states = restore
        market.add_trade_fills_from_market_recorder = MagicMock()
        recorder = _recorder(manager, market)
        with manager.get_new_session() as session:
            session.add(MarketState(config_file_path='test_config', market='test_market',
                                    timestamp=105000, saved_state=tracking_state))
            session.commit()

        recorder.enable_async_financial_events()
        recorder.start()
        recorder.restore_market_states('test_config', market)
        cached = restored.fetch_cached_order('order-terminal')
        assert cached is not None
        assert cached.order_fills['trade-terminal'].receipt_timestamp == 106.0
        await asyncio.wait_for(recorder.flush_financial_events(), timeout=4)
        with manager.get_new_session() as session:
            from hummingbot.model.trade_fill import TradeFill
            row = session.query(TradeFill).filter_by(order_id='order-terminal',
                                                     exchange_trade_id='trade-terminal').one()
            assert row.timestamp == 106000
        await manager.write_queue.close()
        manager.engine.dispose()

    with tempfile.TemporaryDirectory(prefix='hummingbot-terminal-recovery-') as temp_dir:
        asyncio.run(scenario(Path(temp_dir) / 'runtime.sqlite'))
