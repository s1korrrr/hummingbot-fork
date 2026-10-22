import copy
import tempfile
from pathlib import Path
from test.hummingbot.connector.exchange.paper_trade.test_native_paper_trade_exchange import PAIR, D, paper
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.model.order import Order
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.model.trade_fill import TradeFill


class NativePaperTerminalRecorderTests(IsolatedAsyncioTestCase):
    def setUp(self):
        self.previous = MarketsRecorder._shared_instance
        self.directory = tempfile.TemporaryDirectory()
        self.manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                            db_path=str(Path(self.directory.name) / 'paper.sqlite'))
        self.market = paper()
        self.recorder = MarketsRecorder(self.manager, [self.market], 'terminal-fixture', 'v2_with_controllers',
                                        MarketDataCollectionConfigMap(market_data_collection_enabled=False))
        self.recorder.start()
        self.csv = patch.object(self.recorder, 'append_to_csv')
        self.csv.start()

    def tearDown(self):
        self.recorder.stop()
        self.csv.stop()
        self.manager.engine.dispose()
        self.directory.cleanup()
        MarketsRecorder._shared_instance = self.previous

    async def cancel_order(self, fill=False):
        oid = self.market.buy(PAIR, D('1'), OrderType.LIMIT, D('99'))
        await self.market.drain()
        order = self.market._order_tracker.fetch_order(oid)
        if fill:
            await self.market.process_trade(PAIR, 1, 1000, TradeType.SELL, D('98'), D('.25'))
        self.market.cancel(PAIR, oid)
        await self.market.drain()
        return copy.deepcopy(order)

    async def test_durable_terminal_lookup_after_compaction_and_restart(self):
        order = await self.cancel_order(fill=True)
        self.assertEqual(self.market._terminal_orders, {})
        self.market._order_tracker._cached_orders.clear()
        self.assertEqual(self.market.tracking_states['orders'], {})
        update = await self.market._request_order_status(order)
        self.assertEqual(update.new_state, OrderState.CANCELED)
        self.assertEqual(update.misc_updates['terminal_reconciliation']['filled_base'], '0.25')
        with self.manager.get_new_session() as session, session.begin():
            self.recorder.save_market_states('terminal-fixture', self.market, session)
        self.recorder.stop()
        restored = paper()
        self.recorder = MarketsRecorder(self.manager, [restored], 'terminal-fixture', 'v2_with_controllers',
                                        MarketDataCollectionConfigMap(market_data_collection_enabled=False))
        self.recorder.restore_market_states('terminal-fixture', restored)
        update = await restored._request_order_status(order)
        self.assertEqual(update.misc_updates['terminal_reconciliation']['filled_base'], '0.25')
        self.assertEqual(restored._terminal_orders, {})

    async def test_many_resolved_terminals_do_not_grow_snapshots(self):
        for _ in range(80):
            await self.cancel_order()
        self.market._order_tracker._cached_orders.clear()
        self.assertEqual(self.market._terminal_orders, {})
        self.assertEqual(self.market.tracking_states['orders'], {})
        with self.manager.get_new_session() as session:
            self.assertEqual(session.query(Order).count(), 80)

    async def test_missing_fill_or_wrong_scope_never_becomes_terminal_proof(self):
        order = await self.cancel_order(fill=True)
        self.market._order_tracker._cached_orders.clear()
        with self.manager.get_new_session() as session, session.begin():
            session.query(TradeFill).delete()
        with self.assertRaises(ValueError):
            await self.market._request_order_status(order)
        order = await self.cancel_order()
        self.market._order_tracker._cached_orders.clear()
        with self.manager.get_new_session() as session, session.begin():
            session.get(Order, order.client_order_id).config_file_path = 'other-owner'
        with self.assertRaises(ValueError):
            await self.market._request_order_status(order)

    async def test_uncommitted_or_unavailable_proof_is_retained_and_backpressures(self):
        self.recorder.stop()
        self.market.max_unresolved_orders = 2
        for _ in range(2):
            await self.cancel_order()
        self.assertEqual(len(self.market._terminal_orders), 2)
        self.assertEqual(len(self.market.tracking_states['orders']), 2)
        with self.assertRaisesRegex(ValueError, 'terminal evidence capacity'):
            self.market.buy(PAIR, D('1'), OrderType.LIMIT, D('99'))

    async def test_query_failure_retains_proof_and_sell_exit_is_not_blocked(self):
        self.market.max_unresolved_orders = 1
        self.market.set_balance('BTC', D('1'))
        oid = self.market.buy(PAIR, D('1'), OrderType.LIMIT, D('99'))
        await self.market.drain()
        self.market.cancel(PAIR, oid)
        with patch.object(self.market, '_recorded_terminal', side_effect=RuntimeError('recorder unavailable')):
            with self.assertRaisesRegex(RuntimeError, 'recorder unavailable'):
                await self.market.drain()
        self.assertIn(oid, self.market._terminal_orders)
        exit_id = self.market.sell(PAIR, D('.5'), OrderType.LIMIT, D('102'))
        self.assertTrue(exit_id)
        await self.market.drain()
        self.market.cancel(PAIR, exit_id)
        await self.market.drain()
        self.assertEqual(self.market._terminal_orders, {})

    async def test_crash_after_commit_before_compaction_retains_recorder_authority(self):
        with patch.object(self.market, '_compact_terminal_orders'):
            order = await self.cancel_order(fill=True)
        self.assertIn(order.client_order_id, self.market._terminal_orders)
        # Reconstruct a connector as after a crash. No in-memory terminal map is copied.
        self.recorder.stop()
        restored = paper()
        self.recorder = MarketsRecorder(self.manager, [restored], 'terminal-fixture', 'v2_with_controllers',
                                        MarketDataCollectionConfigMap(market_data_collection_enabled=False))
        self.recorder.restore_market_states('terminal-fixture', restored)
        update = await restored._request_order_status(order)
        self.assertEqual(update.misc_updates['terminal_reconciliation']['filled_base'], '0.25')
