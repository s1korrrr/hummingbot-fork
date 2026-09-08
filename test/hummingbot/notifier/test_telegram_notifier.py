import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from types import SimpleNamespace
from unittest.mock import Mock

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event import events
from hummingbot.notifier.telegram_notifier import TelegramMarketEventForwarder, TelegramNotifier


class TelegramNotifierTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        client_config = ClientConfigAdapter(ClientConfigMap())
        client_config.telegram_notifications.telegram_bot_token = "token"
        client_config.telegram_notifications.telegram_chat_id = "chat-id"
        client_config.telegram_notifications.telegram_enabled = True

        self.hb_app = Mock()
        self.hb_app.client_config_map = client_config
        self.hb_app.ev_loop = self.local_event_loop
        self.hb_app.strategy_name = "rsi_v5"
        self.hb_app.strategy_file_name = "rsi_v5.yml"
        self.hb_app.instance_id = "instance-1"
        self.hb_app.save_client_config = Mock()
        self.hb_app.markets = {}
        self.hb_app.trading_core = SimpleNamespace(strategy=None, _strategy_running=False)

    def test_format_order_filled_message_includes_core_stats(self):
        notifier = TelegramNotifier(self.hb_app)
        event = events.OrderFilledEvent(
            timestamp=1712745600,
            order_id="OID-1",
            trading_pair="ETH-USDC",
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("2144.82"),
            amount=Decimal("0.0342"),
            trade_fee=AddedToCostTradeFee(flat_fees=[TokenAmount("USDC", Decimal("0.21"))]),
            exchange_trade_id="TID-1",
            exchange_order_id="EX-1",
        )

        message = notifier.format_market_event(events.MarketEvent.OrderFilled.value, event)

        self.assertIn("🟢 BUY FILLED", message)
        self.assertIn("📍 Pair: 🟣 ETH-USDC", message)
        self.assertIn("💵 Price: 2144.82 USDC", message)
        self.assertIn("📦 Amount: 0.0342 ETH", message)
        self.assertIn("💰 Notional: 73.352844 USDC", message)
        self.assertIn("🧾 Fee: 0.21 USDC", message)
        self.assertIn("✅ Status: filled", message)
        self.assertNotIn("🤖 Bot:", message)
        self.assertNotIn("📁 Config:", message)
        self.assertNotIn("🆔 Instance:", message)
        self.assertNotIn("🧷 Order ID:", message)
        self.assertNotIn("🏦 Exchange Order ID:", message)
        self.assertNotIn("🔁 Exchange Trade ID:", message)

    def test_format_sell_filled_message_includes_trade_realized_pnl_context(self):
        notifier = TelegramNotifier(self.hb_app)
        notifier._sell_fill_realized_pnl_quote = Mock(return_value=Decimal("8.91"))
        event = events.OrderFilledEvent(
            timestamp=1712745600,
            order_id="OID-SELL-FILL-1",
            trading_pair="ETH-USDC",
            trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal("2144.82"),
            amount=Decimal("0.0342"),
            trade_fee=AddedToCostTradeFee(flat_fees=[TokenAmount("USDC", Decimal("0.21"))]),
            exchange_trade_id="TID-SELL-1",
            exchange_order_id="EX-SELL-1",
        )

        message = notifier.format_market_event(events.MarketEvent.OrderFilled.value, event)

        notifier._sell_fill_realized_pnl_quote.assert_called_once_with(event)
        self.assertIn("🔴 SELL FILLED", message)
        self.assertIn("📈 Trade Realized PnL: $8.91", message)
        self.assertNotIn("Pair Realized PnL", message)
        self.assertNotIn("Pair Global PnL", message)
        self.assertIn("✅ Status: filled", message)

    def test_format_sell_completed_message_includes_fee_and_trade_realized_pnl_context(self):
        notifier = TelegramNotifier(self.hb_app)
        notifier._order_completion_context = Mock(return_value={
            "trading_pair": "ETH-USDC",
            "fee_quote": Decimal("0.21"),
            "trade_realized_pnl_quote": Decimal("12.34"),
        })
        event = events.SellOrderCompletedEvent(
            timestamp=1712745600,
            order_id="OID-SELL-1",
            base_asset="ETH",
            quote_asset="USDC",
            base_asset_amount=Decimal("0.0342"),
            quote_asset_amount=Decimal("73.352844"),
            order_type=OrderType.LIMIT,
            exchange_order_id="EX-SELL-1",
        )

        message = notifier.format_market_event(events.MarketEvent.SellOrderCompleted.value, event)

        self.assertIn("🔴 SELL COMPLETED", message)
        self.assertIn("📍 Pair: 🟣 ETH-USDC", message)
        self.assertIn("💵 Average Price: 2144.82 USDC", message)
        self.assertIn("💰 Gross Quote: 73.352844 USDC", message)
        self.assertIn("🧾 Fee: 0.21 USDC", message)
        self.assertIn("💵 Net Quote: 73.142844 USDC", message)
        self.assertIn("📈 Trade Realized PnL: $12.34", message)
        self.assertNotIn("Pair Realized PnL", message)
        self.assertNotIn("Pair Global PnL", message)
        self.assertIn("✅ Status: completed", message)

    def test_trade_realized_pnl_uses_fifo_basis_for_current_sell_order_only(self):
        notifier = TelegramNotifier(self.hb_app)
        fills = [
            SimpleNamespace(order_id="BUY-1", trade_type="BUY", price=Decimal("100"), amount=Decimal("1"), trade_fee_in_quote=Decimal("0.10"), timestamp=1),
            SimpleNamespace(order_id="BUY-2", trade_type="BUY", price=Decimal("110"), amount=Decimal("1"), trade_fee_in_quote=Decimal("0.20"), timestamp=2),
            SimpleNamespace(order_id="SELL-PRIOR", trade_type="SELL", price=Decimal("120"), amount=Decimal("0.5"), trade_fee_in_quote=Decimal("0.05"), timestamp=3),
            SimpleNamespace(order_id="SELL-CURRENT", trade_type="SELL", price=Decimal("130"), amount=Decimal("0.75"), trade_fee_in_quote=Decimal("0.075"), timestamp=4),
        ]

        realized_pnl = notifier._trade_realized_pnl_from_sell_fills(
            sell_fills=[fills[-1]],
            inventory_fills=fills,
            current_order_id="SELL-CURRENT",
        )

        self.assertEqual(Decimal("19.825"), realized_pnl)

    def test_sell_fill_realized_pnl_requires_exchange_trade_id_for_partial_fill_safety(self):
        notifier = TelegramNotifier(self.hb_app)
        notifier._order_record = Mock(return_value=SimpleNamespace(market="binance_paper_trade"))
        notifier._fills_for_pair = Mock()
        event = events.OrderFilledEvent(
            timestamp=1712745600,
            order_id="OID-SELL-FILL-NO-TRADE-ID",
            trading_pair="ETH-USDC",
            trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal("2200"),
            amount=Decimal("0.01"),
            trade_fee=AddedToCostTradeFee(flat_fees=[TokenAmount("USDC", Decimal("0.1"))]),
            exchange_trade_id="",
        )

        self.assertIsNone(notifier._sell_fill_realized_pnl_quote(event))
        notifier._fills_for_pair.assert_not_called()

    def test_sell_order_realized_pnl_scopes_inventory_by_market_and_sell_timestamp(self):
        notifier = TelegramNotifier(self.hb_app)
        sell_fill = SimpleNamespace(order_id="SELL-CURRENT", trade_type="SELL", price=Decimal("130"), amount=Decimal("0.75"), trade_fee_in_quote=Decimal("0.075"), timestamp=4_000_000_000_000)
        notifier._fills_for_order = Mock(return_value=[sell_fill])
        notifier._fills_for_pair = Mock(return_value=[
            SimpleNamespace(order_id="BUY-1", trade_type="BUY", price=Decimal("100"), amount=Decimal("1"), trade_fee_in_quote=Decimal("0.10"), timestamp=1_000_000_000_000),
        ])

        realized_pnl = notifier._sell_order_realized_pnl_quote("SELL-CURRENT", "ETH-USDC", market="binance_paper_trade")

        notifier._fills_for_pair.assert_called_once_with(
            "ETH-USDC",
            market="binance_paper_trade",
            end_timestamp=Decimal("4000000000000"),
        )
        self.assertEqual(Decimal("22.35"), realized_pnl)

    def test_sell_order_realized_pnl_omits_authoritative_value_without_market_scope(self):
        notifier = TelegramNotifier(self.hb_app)
        notifier._fills_for_order = Mock(return_value=[
            SimpleNamespace(order_id="SELL-CURRENT", trade_type="SELL", price=Decimal("130"), amount=Decimal("0.75"), trade_fee_in_quote=Decimal("0.075"), timestamp=4_000),
        ])
        notifier._fills_for_pair = Mock()

        self.assertIsNone(notifier._sell_order_realized_pnl_quote("SELL-CURRENT", "ETH-USDC", market=None))
        notifier._fills_for_pair.assert_not_called()

    def test_trade_realized_helpers_cover_fail_closed_edges(self):
        notifier = TelegramNotifier(self.hb_app)

        self.assertEqual([], notifier._trade_realized_pnl_lines(None))
        self.assertEqual(Decimal("1712745600000"), notifier._normalized_fill_timestamp(1712745600))
        self.assertEqual(Decimal("0"), notifier._normalized_fill_timestamp(object()))
        self.assertEqual("SELL", notifier._trade_fill_side({"trade_type": TradeType.SELL}))
        self.assertIsNone(notifier._trade_realized_pnl_from_sell_fills(
            sell_fills=[{"trade_type": "SELL", "price": Decimal("130"), "amount": Decimal("2"), "timestamp": 2}],
            inventory_fills=[{"trade_type": "BUY", "price": Decimal("100"), "amount": Decimal("1"), "timestamp": 1}],
        ))
        self.assertIsNone(notifier._trade_realized_pnl_from_sell_fills(
            sell_fills=[{"trade_type": "SELL", "price": Decimal("0"), "amount": Decimal("1"), "timestamp": 2}],
            inventory_fills=[{"trade_type": "BUY", "price": Decimal("100"), "amount": Decimal("1"), "timestamp": 1}],
        ))
        self.assertIsNone(notifier._trade_realized_pnl_from_sell_fills(
            sell_fills=[{"trade_type": "SELL", "price": Decimal("130"), "amount": Decimal("1"), "timestamp": 2}],
            inventory_fills=[{"trade_type": "SELL", "price": Decimal("120"), "amount": Decimal("1"), "timestamp": 1}],
        ))

    def test_fills_for_pair_scopes_query_and_fails_closed_without_scope(self):
        notifier = TelegramNotifier(self.hb_app)
        self.assertEqual([], notifier._fills_for_pair("ETH-USDC", market=None))

        notifier._markets_recorder = Mock(return_value=None)
        self.assertEqual([], notifier._fills_for_pair("ETH-USDC", market="binance"))

        recorder_without_config = SimpleNamespace(config_file_path="")
        notifier._markets_recorder = Mock(return_value=recorder_without_config)
        self.assertEqual([], notifier._fills_for_pair("ETH-USDC", market="binance"))

        query = Mock()
        query.filter.return_value = query
        query.order_by.return_value = query
        query.all.return_value = ["fill"]
        session = Mock()
        session.query.return_value = query
        session_scope = Mock()
        session_scope.__enter__ = Mock(return_value=session)
        session_scope.__exit__ = Mock(return_value=None)
        recorder = SimpleNamespace(
            config_file_path="conf.yml",
            sql_manager=SimpleNamespace(get_new_session=Mock(return_value=session_scope)),
        )
        notifier._markets_recorder = Mock(return_value=recorder)

        fills = notifier._fills_for_pair("ETH-USDC", market="binance", end_timestamp=Decimal("123"))

        self.assertEqual(["fill"], fills)
        session.query.assert_called_once()
        query.filter.assert_called_once()
        query.order_by.assert_called_once()
        query.all.assert_called_once()

    def test_sell_fill_realized_pnl_uses_scoped_inventory_and_quote_fee(self):
        notifier = TelegramNotifier(self.hb_app)
        notifier._order_record = Mock(return_value=SimpleNamespace(market="binance_paper_trade"))
        notifier._fills_for_pair = Mock(return_value=[
            {"order_id": "BUY-1", "exchange_trade_id": "B1", "trade_type": "BUY", "price": Decimal("100"), "amount": Decimal("1"), "trade_fee_in_quote": Decimal("0.10"), "timestamp": Decimal("1712745599000")},
        ])
        event = events.OrderFilledEvent(
            timestamp=1712745600,
            order_id="OID-SELL-FILL-1",
            trading_pair="ETH-USDC",
            trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal("130"),
            amount=Decimal("0.5"),
            trade_fee=AddedToCostTradeFee(flat_fees=[TokenAmount("USDC", Decimal("0.05"))]),
            exchange_trade_id="TID-SELL-1",
        )

        realized_pnl = notifier._sell_fill_realized_pnl_quote(event)

        notifier._fills_for_pair.assert_called_once_with(
            "ETH-USDC",
            market="binance_paper_trade",
            end_timestamp=Decimal("1712745600000"),
        )
        self.assertEqual(Decimal("14.900"), realized_pnl)

    def test_order_completion_context_uses_order_market_for_trade_pnl(self):
        notifier = TelegramNotifier(self.hb_app)
        notifier._order_record = Mock(return_value=SimpleNamespace(symbol="ETH-USDC", market="binance_paper_trade"))
        notifier._fills_for_order = Mock(return_value=[SimpleNamespace(trade_fee_in_quote=Decimal("0.21"))])
        notifier._sell_order_realized_pnl_quote = Mock(return_value=Decimal("12.34"))

        context = notifier._order_completion_context("OID-SELL-1", "ETH", "USDC", include_trade_realized_pnl=True)

        self.assertEqual("ETH-USDC", context["trading_pair"])
        self.assertEqual(Decimal("0.21"), context["fee_quote"])
        self.assertEqual(Decimal("12.34"), context["trade_realized_pnl_quote"])
        notifier._sell_order_realized_pnl_quote.assert_called_once_with(
            "OID-SELL-1",
            "ETH-USDC",
            market="binance_paper_trade",
        )

    def test_market_event_forwarder_registers_selected_listeners(self):
        market = Mock()
        self.hb_app.markets = {"binance": market}
        notifier = TelegramNotifier(self.hb_app)

        forwarder = TelegramMarketEventForwarder(self.hb_app, notifier)

        self.assertEqual(2, market.add_listener.call_count)
        listener_tags = [call.args[0] for call in market.add_listener.call_args_list]
        self.assertIn(events.MarketEvent.OrderFilled, listener_tags)
        self.assertIn(events.MarketEvent.OrderFailure, listener_tags)

        forwarder.stop()
        self.assertEqual(2, market.remove_listener.call_count)

    def test_bind_or_validate_chat_sets_chat_id_and_persists(self):
        notifier = TelegramNotifier(self.hb_app)
        self.hb_app.client_config_map.telegram_notifications.telegram_chat_id = ""

        is_valid = notifier._bind_or_validate_chat("123456")

        self.assertTrue(is_valid)
        self.assertEqual("123456", self.hb_app.client_config_map.telegram_notifications.telegram_chat_id)
        self.hb_app.save_client_config.assert_called_once()

    async def test_status_command_uses_runtime_snapshot(self):
        notifier = TelegramNotifier(self.hb_app)
        fake_strategy = Mock()
        fake_strategy._build_runtime_status_snapshot.return_value = {
            "bot_name": "rsi_v5",
            "updated_at": "2026-04-10T10:00:00+00:00",
            "active_orders_count": 2,
            "focus_queue": [{"pair": "BTC-USDC", "focus": "2 buy active"}],
            "summary": {
                "balance_value_quote": 1555.39,
                "net_pnl_quote": 17.59,
                "realized_pnl_quote": 22.24,
                "unrealized_pnl_quote": -2.94,
                "fees_quote": 0.68,
                "active_executor_count": 6,
                "positions_held_count": 3,
                "controller_count": 5,
            },
        }
        self.hb_app.trading_core.strategy = fake_strategy
        self.hb_app.trading_core._strategy_running = True

        message = await notifier._dispatch_command("status", [])

        self.assertIn("📊 STATUS", message)
        self.assertIn("🤖 Bot: rsi_v5", message)
        self.assertIn("💹 Reporting Net: $17.59", message)
        self.assertIn("📬 Active Orders: 2", message)
        self.assertIn("🎯 Focus Queue", message)
        self.assertIn("• BTC-USDC: 2 buy active", message)

    async def test_run_command_loop_retries_after_transient_failure(self):
        notifier = TelegramNotifier(self.hb_app)
        attempts = []
        sleep_delays = []

        async def fake_cycle():
            attempts.append("cycle")
            if len(attempts) == 1:
                raise RuntimeError("temporary telegram failure")
            raise asyncio.CancelledError()

        async def fake_sleep(delay: float):
            sleep_delays.append(delay)

        notifier._run_command_cycle = fake_cycle
        notifier._sleep = fake_sleep

        with self.assertRaises(asyncio.CancelledError):
            await notifier._run_command_loop()

        self.assertEqual(2, len(attempts))
        self.assertEqual([notifier.COMMAND_LOOP_RETRY_DELAY], sleep_delays)

    async def test_conflict_suspends_polling_without_stopping_notifications(self):
        from unittest.mock import AsyncMock

        from hummingbot.notifier.telegram_polling import TelegramPollingConflict
        notifier = TelegramNotifier(self.hb_app)
        notifier._run_command_cycle = AsyncMock(side_effect=TelegramPollingConflict('conflict'))
        notifier._send_telegram_text = AsyncMock()
        task = asyncio.create_task(notifier._run_owned_command_loop())
        try:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            notifier._run_command_cycle.assert_awaited_once()
            await notifier._send_message('notification')
            notifier._send_telegram_text.assert_awaited_once_with('notification')
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def test_http_conflict_becomes_typed_error_without_token(self):
        import io
        from unittest.mock import patch
        from urllib.error import HTTPError

        from hummingbot.notifier.telegram_polling import TelegramPollingConflict
        notifier = TelegramNotifier(self.hb_app)
        error = HTTPError('https://example.invalid/secret-token', 409, 'Conflict', {}, io.BytesIO(b'conflict'))
        with patch('hummingbot.notifier.telegram_notifier.request.urlopen', side_effect=error):
            with self.assertRaises(TelegramPollingConflict) as raised:
                notifier._fetch_updates()
        self.assertNotIn('secret-token', str(raised.exception))
