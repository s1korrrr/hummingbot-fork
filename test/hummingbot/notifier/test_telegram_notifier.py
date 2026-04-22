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

    def test_format_sell_completed_message_includes_fee_and_pair_pnl_context(self):
        notifier = TelegramNotifier(self.hb_app)
        notifier._order_completion_context = Mock(return_value={
            "trading_pair": "ETH-USDC",
            "fee_quote": Decimal("0.21"),
            "pair_performance": {
                "realized_pnl_quote": Decimal("12.34"),
                "global_pnl_quote": Decimal("15.67"),
            },
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
        self.assertIn("📈 Pair Realized PnL: $12.34", message)
        self.assertIn("💹 Pair Global PnL: $15.67", message)
        self.assertIn("✅ Status: completed", message)

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
