"""A venue-confirmed cancellation must not lose its native order receipt."""
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent, OrderCancelledEvent
from hummingbot.model.exact_order_values import OrderExact
from hummingbot.model.order import Order
from hummingbot.model.order_status import OrderStatus
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.model.trade_fill import TradeFill


class ReceiptExchange(ExchangeBase):
    @property
    def name(self):
        return "okx"

    @property
    def tracking_states(self):
        return {}


class CancelBeforeAckRecorderTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.previous_recorder = MarketsRecorder._shared_instance
        self.directory = tempfile.TemporaryDirectory()
        self.manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
            db_path=str(Path(self.directory.name) / "orders.sqlite"))
        self.market = ReceiptExchange()
        self.market._set_current_timestamp(1792272545.227)
        self.tracker = ClientOrderTracker(self.market)
        self.market._order_tracker = self.tracker
        self.recorder = MarketsRecorder(
            self.manager, [self.market], "rsi_modular_v2.yml", "v2_with_controllers",
            MarketDataCollectionConfigMap(market_data_collection_enabled=False))
        self.recorder.start()
        self.events = EventLogger()
        for event in (MarketEvent.BuyOrderCreated, MarketEvent.SellOrderCreated, MarketEvent.OrderCancelled):
            self.market.add_listener(event, self.events)

    async def asyncTearDown(self):
        self.recorder.stop()
        self.manager.engine.dispose()
        self.directory.cleanup()
        MarketsRecorder._shared_instance = self.previous_recorder

    def order(self, side=TradeType.BUY, exchange_id=None):
        return InFlightOrder(
            client_order_id="cancel-before-ack", exchange_order_id=exchange_id,
            trading_pair="ETH-USDC", order_type=OrderType.LIMIT_MAKER,
            trade_type=side, amount=Decimal("0.016620"), price=Decimal("2402.15"),
            creation_timestamp=1792272542.0)

    async def test_confirmed_cancel_before_ack_persists_order_then_terminal_once(self):
        for side in (TradeType.BUY, TradeType.SELL):
            with self.subTest(side=side):
                order = self.order(side)
                order.client_order_id = "cancel-before-ack-" + side.name
                self.tracker.start_tracking_order(order)
                update = OrderUpdate(
                    trading_pair=order.trading_pair, update_timestamp=1792272545.227,
                    new_state=OrderState.CANCELED, client_order_id=order.client_order_id,
                    exchange_order_id="venue-" + side.name)
                await self.tracker._process_order_update(update)
                await self.tracker._process_order_update(update)
                with self.manager.get_new_session() as session:
                    record = session.get(Order, order.client_order_id)
                    self.assertIsNotNone(record, "confirmed venue cancellation lost its native order receipt")
                    self.assertEqual(record.exchange_order_id, update.exchange_order_id)
                    self.assertEqual(record.symbol, "ETH-USDC")
                    self.assertEqual(record.last_status, "OrderCancelled")
                    exact = session.get(OrderExact, order.client_order_id)
                    self.assertEqual(Decimal(exact.amount), order.amount)
                    self.assertEqual(Decimal(exact.price), order.price)
                    states = session.query(OrderStatus).filter_by(order_id=order.client_order_id).order_by(OrderStatus.id).all()
                    self.assertEqual([row.status for row in states], [side.name.title() + "OrderCreated", "OrderCancelled"])
                self.assertNotIn(order.client_order_id, self.tracker.active_orders)
        self.assertEqual(len(self.events.event_log), 2)

    async def test_pending_cancel_before_venue_ack_retains_the_terminal_receipt(self):
        order = self.order()
        self.tracker.start_tracking_order(order)
        await self.tracker._process_order_update(OrderUpdate(
            trading_pair=order.trading_pair, update_timestamp=1792272543,
            new_state=OrderState.PENDING_CANCEL, client_order_id=order.client_order_id))
        await self.tracker._process_order_update(OrderUpdate(
            trading_pair=order.trading_pair, update_timestamp=1792272545.227,
            new_state=OrderState.CANCELED, client_order_id=order.client_order_id,
            exchange_order_id="venue-id"))
        with self.manager.get_new_session() as session:
            self.assertEqual(session.get(Order, order.client_order_id).last_status, "OrderCancelled")
            self.assertEqual(session.query(OrderStatus).filter_by(order_id=order.client_order_id).count(), 2)

    async def test_late_venue_identity_recovers_cancel_without_duplicating_order(self):
        order = self.order()
        self.tracker.start_tracking_order(order)
        for exchange_id in (None, "venue-id", "venue-id"):
            await self.tracker._process_order_update(OrderUpdate(
                trading_pair=order.trading_pair, update_timestamp=1792272545.227,
                new_state=OrderState.CANCELED, client_order_id=order.client_order_id,
                exchange_order_id=exchange_id))
        with self.manager.get_new_session() as session:
            self.assertEqual(session.get(Order, order.client_order_id).exchange_order_id, "venue-id")
            self.assertEqual(session.query(OrderStatus).filter_by(order_id=order.client_order_id).count(), 2)

    async def test_cancel_without_venue_identity_never_fabricates_creation(self):
        order = self.order()
        self.tracker.start_tracking_order(order)
        await self.tracker._process_order_update(OrderUpdate(
            trading_pair=order.trading_pair, update_timestamp=1792272545.227,
            new_state=OrderState.CANCELED, client_order_id=order.client_order_id))
        with self.manager.get_new_session() as session:
            self.assertIsNone(session.get(Order, order.client_order_id))

    async def test_submission_failure_never_fabricates_creation(self):
        order = self.order(exchange_id="known-venue-id")
        self.tracker.start_tracking_order(order)
        await self.tracker._process_order_update(OrderUpdate(
            trading_pair=order.trading_pair, update_timestamp=1792272545.227,
            new_state=OrderState.FAILED, client_order_id=order.client_order_id))
        with self.manager.get_new_session() as session:
            self.assertIsNone(session.get(Order, order.client_order_id))

    async def test_mismatched_cancel_identity_cannot_create_a_receipt(self):
        order = self.order(exchange_id="real-venue-id")
        self.tracker.start_tracking_order(order)
        order.current_state = OrderState.CANCELED
        self.recorder._did_cancel_order(MarketEvent.OrderCancelled.value, self.market,
                                        OrderCancelledEvent(1792272545.227, order.client_order_id, "wrong-id"))
        with self.manager.get_new_session() as session:
            self.assertIsNone(session.get(Order, order.client_order_id))

    async def test_partial_fill_before_creation_then_cancel_preserves_native_fill_once(self):
        order = self.order()
        self.tracker.start_tracking_order(order)
        fill = TradeUpdate(
            trade_id="partial-fill", client_order_id=order.client_order_id,
            exchange_order_id="venue-id", trading_pair=order.trading_pair,
            fill_timestamp=1792272543, fill_price=order.price, fill_base_amount=Decimal("0.005"),
            fill_quote_amount=Decimal("0.005") * order.price, fee=AddedToCostTradeFee())
        with patch.object(self.recorder, 'append_to_csv'):
            self.tracker.process_trade_update(fill)
        for _ in range(2):
            await self.tracker._process_order_update(OrderUpdate(
                trading_pair=order.trading_pair, update_timestamp=1792272545.227,
                new_state=OrderState.CANCELED, client_order_id=order.client_order_id,
                exchange_order_id="venue-id"))
        with self.manager.get_new_session() as session:
            self.assertEqual(session.get(Order, order.client_order_id).last_status, "OrderCancelled")
            saved, = session.query(TradeFill).filter_by(order_id=order.client_order_id).all()
            self.assertEqual(saved.amount_decimal, Decimal("0.005"))
            self.assertEqual(saved.exchange_trade_id, "partial-fill")
