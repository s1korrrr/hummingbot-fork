"""Real-file order receipts preserve loop progress, replay and commit ordering."""
import asyncio
import json
import threading
from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import BuyOrderCreatedEvent, MarketEvent, OrderCancelledEvent, OrderFilledEvent
from hummingbot.model.exact_order_values import OrderExact
from hummingbot.model.market_state import MarketState
from hummingbot.model.metadata import Metadata
from hummingbot.model.order import Order
from hummingbot.model.order_status import OrderStatus
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.model.trade_fill import TradeFill


class Market:
    display_name = name = 'test_market'
    current_timestamp = 100.0

    def __init__(self):
        self._order_tracker = ClientOrderTracker(connector=self)
        self.add_exchange_order_ids_from_market_recorder = MagicMock()
        self.add_trade_fills_from_market_recorder = MagicMock()
        self.add_listener = MagicMock()
        self.remove_listener = MagicMock()

    @property
    def tracking_states(self):
        return {key: order.to_json() for key, order in self._order_tracker.all_fillable_orders.items()}


@asynccontextmanager
async def runtime(path):
    previous = MarketsRecorder._shared_instance
    manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                   db_path=str(path))
    market = Market()
    order = InFlightOrder(client_order_id='order-1', exchange_order_id='exchange-1',
                          trading_pair='BTC-USDC', order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
                          amount=Decimal('1'), price=Decimal('100'), creation_timestamp=100,
                          initial_state=OrderState.OPEN)
    market._order_tracker.start_tracking_order(order)
    recorder = MarketsRecorder(manager, [market], 'test_config', 'test_strategy',
                               MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    recorder.enable_async_financial_events()
    try:
        yield manager, recorder, market, order
    finally:
        await recorder.stop_async()
        await manager.close_write_queue()
        manager.engine.dispose()
        MarketsRecorder._shared_instance = previous


def created_event():
    return BuyOrderCreatedEvent(timestamp=101, type=OrderType.LIMIT, trading_pair='BTC-USDC',
                                amount=Decimal('1'), price=Decimal('100'), order_id='order-1',
                                creation_timestamp=100, exchange_order_id='exchange-1')


def test_order_creation_during_real_write_lock_is_async_and_indexes_follow_commit(tmp_path):
    async def scenario():
        async with runtime(tmp_path / 'orders.sqlite') as (manager, recorder, market, order):
            acquired, release = threading.Event(), threading.Event()

            def hold_transaction():
                with manager.engine.begin() as connection:
                    connection.exec_driver_sql("INSERT INTO Metadata(key,value) VALUES ('held-lock','1')")
                    acquired.set()
                    if not release.wait(5):
                        raise TimeoutError('test lock not released')
            holder = asyncio.create_task(manager.write_queue.write(hold_transaction))
            assert await asyncio.wait_for(asyncio.to_thread(acquired.wait), 2)
            heartbeat = asyncio.Event()
            asyncio.get_running_loop().call_soon(heartbeat.set)
            market.add_exchange_order_ids_from_market_recorder.reset_mock()
            try:
                recorder._did_create_order(MarketEvent.BuyOrderCreated.value, market, created_event())
                await asyncio.sleep(0)
                assert heartbeat.is_set()
                market.add_exchange_order_ids_from_market_recorder.assert_not_called()
                assert order.recorder_pending_events
            finally:
                release.set()
            await asyncio.wait_for(holder, 3)
            await asyncio.wait_for(recorder.flush_financial_events(), 3)
            market.add_exchange_order_ids_from_market_recorder.assert_called_once_with({'exchange-1': 'order-1'})
            assert not order.recorder_pending_events
            with manager.get_new_session() as session:
                assert session.get(Order, 'order-1').amount == Decimal('1')
                assert session.query(OrderStatus).filter_by(order_id='order-1').count() == 1
                state = session.query(MarketState).one().saved_state
                assert not state['order-1']['recorder_pending_events']
    asyncio.run(scenario())


def test_capacity_rejection_retains_cancel_metadata_and_replays_without_duplicate_receipts(tmp_path):
    async def scenario():
        async with runtime(tmp_path / 'orders.sqlite') as (manager, recorder, market, order):
            recorder._financial_event_queue = asyncio.Queue(maxsize=1)
            recorder._did_create_order(MarketEvent.BuyOrderCreated.value, market, created_event())
            order.current_state = OrderState.CANCELED
            recorder._did_cancel_order(MarketEvent.OrderCancelled.value, market,
                                       OrderCancelledEvent(102, 'order-1', 'exchange-1'))
            assert len(order.recorder_pending_events) == 2
            restored = InFlightOrder.from_json(json.loads(json.dumps(order.to_json())))
            assert restored.recorder_pending_events == order.recorder_pending_events
            await asyncio.wait_for(recorder.flush_financial_events(), 4)
            await asyncio.wait_for(recorder.flush_financial_events(), 4)
            assert not order.recorder_pending_events
            with manager.get_new_session() as session:
                row = session.get(Order, 'order-1')
                assert row.last_status == 'OrderCancelled'
                statuses = session.query(OrderStatus).filter_by(order_id='order-1').all()
                assert sorted(item.status for item in statuses) == ['BuyOrderCreated', 'OrderCancelled']
    asyncio.run(scenario())


def test_older_same_millisecond_replay_cannot_rewind_newer_order_or_market_state(tmp_path):
    async def scenario():
        async with runtime(tmp_path / 'orders.sqlite') as (manager, recorder, market, order):
            creation = recorder._capture_order_creation(MarketEvent.BuyOrderCreated.value, market, created_event())
            creation.update(recorder._capture_market_state_payload(market))
            recorder._persist_order_creation_payload(creation)
            old = {'order_id': 'order-1', 'config_file_path': 'test_config', 'market': market.display_name,
                   'timestamp': 103000, 'event_status': 'OrderFailure', 'creation_if_missing': None,
                   'cancel_event': False, 'recorder_sequence': 10, 'market_state_timestamp': 103000,
                   'market_state_json': json.dumps({'projection': {'version': 'old'}})}
            new = {**old, 'event_status': 'OrderCancelled', 'recorder_sequence': 11,
                   'market_state_json': json.dumps({'projection': {'version': 'new'}})}
            recorder._persist_order_status_payload(new)
            recorder._persist_order_status_payload(old)
            recorder._persist_order_status_payload(old)
            with manager.get_new_session() as session:
                assert session.get(Order, 'order-1').last_status == 'OrderCancelled'
                assert session.query(MarketState).one().saved_state['projection']['version'] == 'new'
                assert session.query(OrderStatus).filter_by(order_id='order-1', status='OrderFailure').count() == 1
    asyncio.run(scenario())


def test_late_captured_older_fill_does_not_rewind_terminal_order_projection(tmp_path):
    async def scenario():
        async with runtime(tmp_path / 'orders.sqlite') as (manager, recorder, market, order):
            creation = recorder._capture_order_creation(MarketEvent.BuyOrderCreated.value, market, created_event())
            creation.update(recorder._capture_market_state_payload(market))
            recorder._persist_order_creation_payload(creation)
            terminal = {
                'order_id': 'order-1', 'config_file_path': 'test_config', 'market': market.display_name,
                'timestamp': 105000, 'event_status': 'BuyOrderCompleted', 'creation_if_missing': None,
                'cancel_event': False, 'recorder_sequence': 10,
                'market_state_timestamp': 105000, 'market_state_json': '{}',
            }
            # The fill happened before completion, but a delayed callback or
            # replay can capture it later. Keep the fill receipt while
            # retaining the chronologically newer terminal projection.
            late_fill = {**terminal, 'timestamp': 104000, 'event_status': 'OrderFilled', 'recorder_sequence': 11}
            recorder._persist_order_status_payload(terminal)
            recorder._persist_order_status_payload(late_fill)
            with manager.get_new_session() as session:
                row = session.get(Order, 'order-1')
                assert (row.last_status, row.last_update_timestamp) == ('BuyOrderCompleted', 105000)
                marker = session.get(
                    Metadata,
                    'native-recorder-sequence:test_config:order:order-1',
                )
                assert marker.value == '11'
                assert session.query(OrderStatus).filter_by(order_id='order-1', status='BuyOrderCompleted').count() == 1
                assert session.query(OrderStatus).filter_by(order_id='order-1', status='OrderFilled').count() == 1
    asyncio.run(scenario())


@pytest.mark.parametrize('terminal_status,fill_timestamp', [
    ('BuyOrderCompleted', 105),  # Same-millisecond nonterminal replay.
    ('OrderCancelled', 106),     # Later-timestamp partial fill callback.
])
def test_late_financial_fill_keeps_terminal_projection_and_persists_receipt(
        tmp_path, terminal_status, fill_timestamp):
    async def scenario():
        async with runtime(tmp_path / 'orders.sqlite') as (manager, recorder, market, order):
            creation = recorder._capture_order_creation(MarketEvent.BuyOrderCreated.value, market, created_event())
            creation.update(recorder._capture_market_state_payload(market))
            recorder._persist_order_creation_payload(creation)
            terminal_state = recorder._capture_market_state_payload(market)
            terminal = {
                'order_id': 'order-1', 'config_file_path': 'test_config', 'market': market.display_name,
                'timestamp': 105000, 'event_status': terminal_status, 'creation_if_missing': None,
                'cancel_event': terminal_status == 'OrderCancelled', **terminal_state,
            }
            recorder._persist_order_status_payload(terminal)
            # A delayed fill callback can carry an equal or later timestamp.
            # Its financial receipt must commit without reopening the terminal.
            _, captured = recorder._capture_financial_fill(
                MarketEvent.OrderFilled.value, market,
                OrderFilledEvent(timestamp=fill_timestamp, order_id='order-1', trading_pair='BTC-USDC',
                                 trade_type=TradeType.BUY, order_type=OrderType.LIMIT,
                                 price=Decimal('100'), amount=Decimal('0.25'),
                                 trade_fee=AddedToCostTradeFee(), exchange_trade_id='trade-late'),
            )
            fill_payload = captured['payload']
            recorder._persist_financial_fill(fill_payload)
            with manager.get_new_session() as session:
                row = session.get(Order, 'order-1')
                assert (row.last_status, row.last_update_timestamp) == (terminal_status, 105000)
                assert session.query(OrderStatus).filter_by(order_id='order-1', status='OrderFilled').count() == 1
                fill = session.query(TradeFill).filter_by(order_id='order-1', exchange_trade_id='trade-late').one()
                assert (fill.timestamp, fill.amount_decimal) == (fill_timestamp * 1000, Decimal('0.25'))
                marker = session.get(Metadata, 'native-recorder-sequence:test_config:order:order-1')
                assert marker.value == str(fill_payload['recorder_sequence'])
    asyncio.run(scenario())


def test_conflicting_terminal_receipts_remain_visible_without_capture_order_winner(tmp_path):
    async def scenario():
        async with runtime(tmp_path / 'orders.sqlite') as (manager, recorder, market, order):
            creation = recorder._capture_order_creation(MarketEvent.BuyOrderCreated.value, market, created_event())
            creation.update(recorder._capture_market_state_payload(market))
            recorder._persist_order_creation_payload(creation)
            base = {
                'order_id': 'order-1', 'config_file_path': 'test_config', 'market': market.display_name,
                'creation_if_missing': None, 'cancel_event': False,
                'market_state_timestamp': 105000, 'market_state_json': '{}',
            }
            cancelled = {**base, 'timestamp': 105000, 'event_status': 'OrderCancelled', 'recorder_sequence': 10}
            completed = {**base, 'timestamp': 106000, 'event_status': 'BuyOrderCompleted', 'recorder_sequence': 11}
            recorder._persist_order_status_payload(cancelled)
            recorder._persist_order_status_payload(completed)
            with manager.get_new_session() as session:
                assert session.get(Order, 'order-1').last_status == 'OrderCancelled'
                statuses = {row.status for row in session.query(OrderStatus).filter_by(order_id='order-1')}
                assert {'OrderCancelled', 'BuyOrderCompleted'} <= statuses
            assert recorder.get_order_status_history('order-1') == [
                'BuyOrderCreated', 'OrderCancelled', 'BuyOrderCompleted',
            ]
    asyncio.run(scenario())


def test_replayed_creation_does_not_reopen_terminal_order_projection(tmp_path):
    async def scenario():
        async with runtime(tmp_path / 'orders.sqlite') as (manager, recorder, market, order):
            creation = recorder._capture_order_creation(MarketEvent.BuyOrderCreated.value, market, created_event())
            creation.update(recorder._capture_market_state_payload(market))
            recorder._persist_order_creation_payload(creation)
            terminal = {
                'order_id': 'order-1', 'config_file_path': 'test_config', 'market': market.display_name,
                'timestamp': 105000, 'event_status': 'OrderCancelled', 'creation_if_missing': None,
                'cancel_event': True, **recorder._capture_market_state_payload(market),
            }
            recorder._persist_order_status_payload(terminal)
            replayed_creation = recorder._capture_order_creation(
                MarketEvent.BuyOrderCreated.value, market, created_event())
            replayed_creation.update(recorder._capture_market_state_payload(market))
            recorder._persist_order_creation_payload(replayed_creation)
            with manager.get_new_session() as session:
                assert session.get(Order, 'order-1').last_status == 'OrderCancelled'
                assert session.query(OrderStatus).filter_by(
                    order_id='order-1', status='BuyOrderCreated').count() == 1
    asyncio.run(scenario())


def test_validated_legacy_creation_enrichment_clears_persisted_pending_marker(tmp_path):
    async def scenario():
        async with runtime(tmp_path / 'orders.sqlite') as (manager, recorder, market, order):
            payload = recorder._capture_order_creation(MarketEvent.BuyOrderCreated.value, market, created_event())
            payload.update(recorder._capture_market_state_payload(market))
            recorder._persist_order_creation_payload(payload)
            with manager.get_new_session() as session:
                with session.begin():
                    session.delete(session.get(OrderExact, 'order-1'))  # Offline legacy-row fixture only.
            recorder._did_create_order(MarketEvent.BuyOrderCreated.value, market, created_event())
            await asyncio.wait_for(recorder.flush_financial_events(), 3)
            assert not order.recorder_pending_events
            with manager.get_new_session() as session:
                exact = session.get(OrderExact, 'order-1')
                assert exact.amount == '1' and exact.price == '100'
                assert session.query(OrderStatus).filter_by(order_id='order-1').count() == 1
                assert not session.query(MarketState).one().saved_state['order-1']['recorder_pending_events']
    asyncio.run(scenario())


def test_same_terminal_replay_keeps_monotone_projection_timestamp(tmp_path):
    async def scenario():
        async with runtime(tmp_path / 'orders.sqlite') as (manager, recorder, market, order):
            creation = recorder._capture_order_creation(MarketEvent.BuyOrderCreated.value, market, created_event())
            creation.update(recorder._capture_market_state_payload(market))
            recorder._persist_order_creation_payload(creation)
            terminal = {'order_id': 'order-1', 'config_file_path': 'test_config',
                        'market': market.display_name, 'creation_if_missing': None, 'cancel_event': False,
                        'market_state_timestamp': 105000, 'market_state_json': '{}',
                        'timestamp': 105000, 'event_status': 'BuyOrderCompleted', 'recorder_sequence': 10}
            recorder._persist_order_status_payload(terminal)
            recorder._persist_order_status_payload({**terminal, 'timestamp': 104000, 'recorder_sequence': 11})
            with manager.get_new_session() as session:
                row = session.get(Order, 'order-1')
                assert (row.last_status, row.last_update_timestamp) == ('BuyOrderCompleted', 105000)
                assert session.get(Metadata, 'native-recorder-sequence:test_config:order:order-1').value == '11'
                assert session.query(OrderStatus).filter_by(order_id='order-1', status='BuyOrderCompleted').count() == 2
    asyncio.run(scenario())
