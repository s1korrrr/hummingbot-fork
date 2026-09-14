import asyncio
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from types import SimpleNamespace
from unittest.mock import Mock, patch

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event import events
from hummingbot.model.order_status import OrderStatus  # noqa: F401 - register ORM relationship target
from hummingbot.model.trade_fill import TradeFill
from hummingbot.notifier.telegram_notifier import TelegramMarketEventForwarder, TelegramNotifier


class TelegramNotifierTest(IsolatedAsyncioWrapperTestCase):
    @staticmethod
    def status_snapshot(
        bot_name,
        *,
        controllers,
        orders,
        executors,
        positions,
        equity,
        net,
        pnl_available=True,
        lifecycle_executors=None,
        active_orders=None,
    ):
        active_orders = active_orders or []
        snapshot = {
            "schema_version": 1,
            "bot_name": bot_name,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "active_orders_count": orders,
            "active_orders": active_orders,
            "active_orders_status": {"complete": True, "source": "connector_in_flight_orders", "errors": []},
            "controller_reports_valid": True,
            "controllers": [{"controller_id": controller} for controller in controllers],
            "focus_queue": [],
            "summary": {
                "balance_value_quote": equity,
                "pnl_available": pnl_available,
                "net_pnl_quote": net,
                "realized_pnl_quote": 0,
                "unrealized_pnl_quote": net,
                "fees_quote": 0,
                "active_executor_count": executors,
                "positions_held_count": positions,
                "controller_count": len(controllers),
            },
        }
        if lifecycle_executors is not None:
            snapshot["active_executors"] = [
                row
                for row in lifecycle_executors
                if row.get("status") not in {"shutting_down", "shutting-down", "terminated"}
            ]
            snapshot["lifecycle_executors"] = lifecycle_executors
            snapshot["summary"]["lifecycle_executor_count"] = len(lifecycle_executors)
        return snapshot

    @staticmethod
    def write_status_snapshot(root, lane, snapshot):
        path = root / f"{lane}.status.json"
        path.write_text(json.dumps(snapshot))
        return str(path)

    @staticmethod
    def exact_fill(side, amount, price, fee, *, base_fee="0", order_id="order", timestamp=1):
        return TradeFill(market="okx", order_id=order_id, exchange_trade_id=order_id, symbol="BTC-USDC",
                         base_asset="BTC", quote_asset="USDC", trade_type=side, timestamp=timestamp,
                         amount=Decimal(amount).quantize(Decimal("0.000001")), price=Decimal(price),
                         trade_fee_in_quote=Decimal("0"), exact_amount=amount, exact_price=price,
                         exact_trade_fee_in_quote=fee, exact_receipt_source="order_filled_event_v1",
                         trade_fee={"fee_type": "DeductedFromReturns", "percent": "0", "percent_token": None,
                                    "flat_fees": [{"token": "BTC", "amount": base_fee}]})

    def test_unavailable_fee_conversion_never_formats_as_zero_or_nan(self):
        for result in [ValueError("missing conversion"), Decimal("NaN"), Decimal("Infinity")]:
            with self.subTest(result=result):
                fee = AddedToCostTradeFee()
                fee.fee_amount_in_token = Mock(side_effect=result) if isinstance(result, Exception) else Mock(return_value=result)
                self.assertEqual("UNAVAILABLE", TelegramNotifier._format_fee(
                    fee, "BTC-USDC", Decimal("100"), Decimal("1"), "USDC"))
        fee = AddedToCostTradeFee(flat_fees=[TokenAmount("BTC", Decimal("0.01"))])
        fee.fee_amount_in_token = Mock(side_effect=ValueError("missing conversion"))
        self.assertEqual("0.01 BTC", TelegramNotifier._format_fee(
            fee, "BTC-USDC", Decimal("100"), Decimal("1"), "USDC"))

    def test_unknown_event_fee_does_not_create_realized_profit(self):
        notifier = TelegramNotifier(self.hb_app)
        notifier._order_record = Mock(return_value=SimpleNamespace(market="okx"))
        notifier._fills_for_pair = Mock(return_value=[self.exact_fill("BUY", "1", "100", "0")])
        fee = AddedToCostTradeFee()
        fee.fee_amount_in_token = Mock(side_effect=ValueError("conversion unavailable"))
        event = events.OrderFilledEvent(timestamp=2, order_id="sell", trading_pair="BTC-USDC",
                                        trade_type=TradeType.SELL, order_type=OrderType.LIMIT,
                                        price=Decimal("110"), amount=Decimal("1"), trade_fee=fee,
                                        exchange_trade_id="sell")
        self.assertIsNone(notifier._sell_fill_realized_pnl_quote(event))

    def test_invalid_accounting_timestamp_does_not_invent_fifo_order(self):
        notifier = TelegramNotifier(self.hb_app)
        sell = self.exact_fill("SELL", "1", "110", "0", order_id="sell", timestamp=2)
        for timestamp in ["bad", None, 0, -1, "NaN", "Infinity"]:
            with self.subTest(timestamp=timestamp):
                buy = self.exact_fill("BUY", "1", "100", "0", timestamp=timestamp)
                self.assertIsNone(notifier._trade_realized_pnl_from_sell_fills(
                    sell_fills=[sell], inventory_fills=[buy]))

    def test_tied_target_and_historical_partials_cannot_assign_fifo_basis(self):
        notifier = TelegramNotifier(self.hb_app)
        buys = [self.exact_fill("BUY", "1", "100", "0", order_id="buy1", timestamp=1),
                self.exact_fill("BUY", "1", "200", "0", order_id="buy2", timestamp=2)]
        prior = self.exact_fill("SELL", "1", "150", "0", order_id="sell", timestamp=3)
        prior.exchange_trade_id = "prior"
        target = self.exact_fill("SELL", "1", "150", "0", order_id="sell", timestamp=3)
        self.assertIsNone(notifier._trade_realized_pnl_from_sell_fills(
            sell_fills=[target], inventory_fills=[*buys, prior]))
        self.assertEqual(Decimal("0"), notifier._trade_realized_pnl_from_sell_fills(
            sell_fills=[target, prior], inventory_fills=buys))

    def test_sell_pnl_uses_exact_net_inventory_and_does_not_double_charge_base_fee(self):
        notifier = TelegramNotifier(self.hb_app)
        buy = self.exact_fill("BUY", "1", "100", "1", base_fee="0.01", order_id="buy")
        sell = self.exact_fill("SELL", "0.99", "110", "0.1089", order_id="sell", timestamp=2)
        self.assertEqual(Decimal("8.7911"), notifier._trade_realized_pnl_from_sell_fills(
            sell_fills=[sell], inventory_fills=[buy]))

    def test_btc_incident_receipts_preserve_sub_micro_fill_and_dust_cost_basis(self):
        notifier = TelegramNotifier(self.hb_app)
        buy = self.exact_fill("BUY", "0.00013824", "78207.5", "0.00864912384",
                              base_fee="0.000000110592", order_id="buy")
        sell = self.exact_fill("SELL", "0.00013812", "78187.8", "0.010799298936", order_id="sell", timestamp=2)
        pnl = notifier._trade_realized_pnl_from_sell_fills(sell_fills=[sell], inventory_fills=[buy])
        self.assertEqual(Decimal("-0.02216879768"), pnl.quantize(Decimal("0.00000000001")))

    def test_split_exits_conserve_full_lot_cost(self):
        notifier = TelegramNotifier(self.hb_app)
        buy = self.exact_fill("BUY", "1", "100", "1", base_fee="0.01", order_id="buy")
        sells = [self.exact_fill("SELL", "0.4", "110", "0.044", order_id="sell1", timestamp=2),
                 self.exact_fill("SELL", "0.59", "110", "0.0649", order_id="sell2", timestamp=3)]
        self.assertEqual(Decimal("8.7911"), notifier._trade_realized_pnl_from_sell_fills(
            sell_fills=sells, inventory_fills=[buy]))

    def test_interleaved_target_partials_receive_chronological_fifo_basis(self):
        notifier = TelegramNotifier(self.hb_app)
        buy1 = self.exact_fill("BUY", "1", "100", "0", order_id="buy1", timestamp=1)
        first = self.exact_fill("SELL", "0.5", "110", "0", order_id="target", timestamp=2)
        buy2 = self.exact_fill("BUY", "1", "200", "0", order_id="buy2", timestamp=3)
        other = self.exact_fill("SELL", "0.75", "210", "0", order_id="other", timestamp=4)
        last = self.exact_fill("SELL", "0.5", "220", "0", order_id="target", timestamp=5)
        first.exchange_trade_id = "partial1"
        last.exchange_trade_id = "partial2"
        pnl = notifier._trade_realized_pnl_from_sell_fills(sell_fills=[first, last],
                                                           inventory_fills=[buy1, first, buy2, other, last], current_order_id="target")
        self.assertEqual(Decimal("15"), pnl)

    def test_legacy_or_missing_fee_basis_is_unavailable_for_trade_pnl(self):
        notifier = TelegramNotifier(self.hb_app)
        legacy = TradeFill(amount=Decimal("1"), price=Decimal("100"), trade_fee_in_quote=Decimal("0"),
                           trade_type="BUY", timestamp=1)
        sell = self.exact_fill("SELL", "1", "110", "0", order_id="sell", timestamp=2)
        self.assertIsNone(notifier._trade_realized_pnl_from_sell_fills(sell_fills=[sell], inventory_fills=[legacy]))
        self.assertIsNone(notifier._trade_realized_pnl_from_sell_fills(sell_fills=[sell], inventory_fills=[
            {"trade_type": "BUY", "amount": "1", "price": "100", "timestamp": 1}]))

    def test_ambiguous_same_timestamp_fifo_order_does_not_invent_basis(self):
        notifier = TelegramNotifier(self.hb_app)
        buys = [self.exact_fill("BUY", "1", "100", "0", order_id="buy1", timestamp=1),
                self.exact_fill("BUY", "1", "200", "0", order_id="buy2", timestamp=1)]
        sell = self.exact_fill("SELL", "1", "150", "0", order_id="sell", timestamp=2)
        self.assertIsNone(notifier._trade_realized_pnl_from_sell_fills(sell_fills=[sell], inventory_fills=buys))

    def test_exact_fill_missing_asset_fee_receipt_cannot_claim_pnl(self):
        notifier = TelegramNotifier(self.hb_app)
        buy = self.exact_fill("BUY", "1", "100", "1", order_id="buy")
        buy.trade_fee = None
        sell = self.exact_fill("SELL", "1", "110", "0", order_id="sell", timestamp=2)
        self.assertIsNone(notifier._trade_realized_pnl_from_sell_fills(sell_fills=[sell], inventory_fills=[buy]))

    def test_reused_trade_id_on_other_order_retains_its_inventory_consumption(self):
        notifier = TelegramNotifier(self.hb_app)
        history = [self.exact_fill("BUY", "1", "100", "0", order_id="buy1", timestamp=1),
                   self.exact_fill("BUY", "1", "200", "0", order_id="buy2", timestamp=2),
                   self.exact_fill("SELL", "1.5", "210", "0", order_id="other", timestamp=3)]
        target = self.exact_fill("SELL", "0.5", "220", "0", order_id="target", timestamp=4)
        history[-1].exchange_trade_id = target.exchange_trade_id = "reused"
        self.assertEqual(Decimal("10"), notifier._trade_realized_pnl_from_sell_fills(
            sell_fills=[target], inventory_fills=history, current_trade_id="reused"))

    def test_duplicate_target_receipt_is_counted_once_and_conflicts_are_unavailable(self):
        notifier = TelegramNotifier(self.hb_app)
        history = [self.exact_fill("BUY", "1", "100", "0", order_id="buy1", timestamp=1),
                   self.exact_fill("BUY", "1", "200", "0", order_id="buy2", timestamp=2)]
        target = self.exact_fill("SELL", "1", "220", "0", order_id="target", timestamp=3)
        self.assertEqual(Decimal("120"), notifier._trade_realized_pnl_from_sell_fills(
            sell_fills=[target, target], inventory_fills=history))
        conflict = self.exact_fill("SELL", "0.5", "220", "0", order_id="target", timestamp=3)
        self.assertIsNone(notifier._trade_realized_pnl_from_sell_fills(
            sell_fills=[target, conflict], inventory_fills=history))

    def test_missing_exact_fee_prevents_trade_pnl_and_completion_fee_claim(self):
        notifier = TelegramNotifier(self.hb_app)
        buy = self.exact_fill("BUY", "1", "100", None, order_id="buy")
        sell = self.exact_fill("SELL", "1", "110", "0.1", order_id="sell", timestamp=2)
        self.assertIsNone(notifier._trade_realized_pnl_from_sell_fills(sell_fills=[sell], inventory_fills=[buy]))
        notifier._order_record = lambda order_id: None
        notifier._fills_for_order = lambda order_id: [buy]
        self.assertIsNone(notifier._order_completion_context("buy", "BTC", "USDC")["fee_quote"])

    async def test_recent_fills_preserve_sub_micro_amount_and_show_receipt_precision(self):
        notifier = TelegramNotifier(self.hb_app)
        fill = self.exact_fill("SELL", "0.00013812", "78187.8", "0.010799298936")
        notifier._recent_fills = lambda limit: [fill]
        message = await notifier._command_fills([])
        self.assertIn("0.00013812", message)
        self.assertIn("exact receipt", message)

    def test_order_message_preserves_integer_magnitude(self):
        for value, expected in [("100", "100"), ("10.50", "10.5"), ("0.0000100", "0.00001"), ("-20", "-20")]:
            with self.subTest(value=value):
                self.assertEqual(expected, TelegramNotifier._format_decimal(Decimal(value)))

    async def test_command_poll_reports_recovery_only_after_success(self):
        notifier = TelegramNotifier(self.hb_app)
        attempts = 0

        async def cycle():
            nonlocal attempts
            attempts += 1
            if attempts <= 2:
                raise ConnectionResetError("connection reset")
            if attempts == 4:
                raise asyncio.CancelledError()

        async def sleep(delay):
            pass

        notifier._run_command_cycle = cycle
        notifier._sleep = sleep
        with self.assertLogs(notifier.logger(), level="INFO") as logs:
            with self.assertRaises(asyncio.CancelledError):
                await notifier._run_command_loop()
        recovery = [record for record in logs.records if "polling recovered" in record.getMessage()]
        self.assertEqual(1, len(recovery))
        self.assertEqual("INFO", recovery[0].levelname)
        self.assertIn("2 failed attempts", recovery[0].getMessage())

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

    async def test_status_without_scope_combines_main_and_sui_without_summing_shared_equity(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {
                "main": self.write_status_snapshot(root, "main", self.status_snapshot(
                    "ok_rsi", controllers=["btc", "eth", "sol", "xrp", "bnb"],
                    orders=2, executors=1, positions=5, equity=15000, net=12.5,
                )),
                "sui": self.write_status_snapshot(root, "sui", self.status_snapshot(
                    "ok_rsi_sui_sell_only", controllers=["sui-usdc"],
                    orders=0, executors=0, positions=1, equity=15000, net=-0.5,
                )),
            }
            with patch.dict(os.environ, {"HB_TELEGRAM_STATUS_SNAPSHOTS": json.dumps(paths)}):
                message = await TelegramNotifier(self.hb_app)._dispatch_command("status", [])

        self.assertIn("📊 STATUS • MAIN + SUI", message)
        self.assertIn("🤖 MAIN: ok_rsi", message)
        self.assertIn("🤖 SUI: ok_rsi_sui_sell_only", message)
        self.assertIn("📬 Orders: 2", message)
        self.assertIn("🧠 Controllers: 5", message)
        self.assertIn("🧠 Controllers: 1", message)
        self.assertNotIn("Total Equity", message)
        self.assertLess(message.index("🤖 MAIN"), message.index("🤖 SUI"))

    async def test_status_scope_selects_configured_main_or_sui_snapshot(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {
                "main": self.write_status_snapshot(root, "main", self.status_snapshot(
                    "ok_rsi", controllers=["btc", "eth", "sol", "xrp", "bnb"],
                    orders=2, executors=1, positions=5, equity=15000, net=12.5,
                )),
                "sui": self.write_status_snapshot(root, "sui", self.status_snapshot(
                    "ok_rsi_sui_sell_only", controllers=["sui-usdc"],
                    orders=0, executors=0, positions=1, equity=15000, net=-0.5,
                )),
            }
            with patch.dict(os.environ, {"HB_TELEGRAM_STATUS_SNAPSHOTS": json.dumps(paths)}):
                notifier = TelegramNotifier(self.hb_app)
                main = await notifier._dispatch_command("status", ["main"])
                sui = await notifier._dispatch_command("status", ["sui"])

        self.assertIn("📊 STATUS • MAIN", main)
        self.assertIn("🤖 Bot: ok_rsi", main)
        self.assertIn("🧠 Controllers: 5", main)
        self.assertNotIn("ok_rsi_sui_sell_only", main)
        self.assertIn("📊 STATUS • SUI", sui)
        self.assertIn("🤖 Bot: ok_rsi_sui_sell_only", sui)
        self.assertIn("🧠 Controllers: 1", sui)

    async def test_combined_status_keeps_healthy_lane_when_other_snapshot_is_missing(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {
                "main": self.write_status_snapshot(root, "main", self.status_snapshot(
                    "ok_rsi", controllers=["btc", "eth", "sol", "xrp", "bnb"],
                    orders=0, executors=1, positions=5, equity=15000, net=12.5,
                )),
                "sui": str(root / "missing.status.json"),
            }
            with patch.dict(os.environ, {"HB_TELEGRAM_STATUS_SNAPSHOTS": json.dumps(paths)}):
                message = await TelegramNotifier(self.hb_app)._dispatch_command("status", [])

        self.assertIn("🤖 MAIN: ok_rsi", message)
        self.assertIn("⚠️ SUI: UNAVAILABLE", message)

    async def test_sui_status_preserves_explicitly_unavailable_pnl_instead_of_zero(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            main = self.status_snapshot(
                "ok_rsi", controllers=["btc", "eth", "sol", "xrp", "bnb"],
                orders=0, executors=1, positions=5, equity=15000, net=12.5,
            )
            sui = self.status_snapshot(
                "ok_rsi_sui_sell_only", controllers=["sui-usdc"],
                orders=0, executors=0, positions=0, equity=571.18, net=None, pnl_available=False,
            )
            sui["summary"]["realized_pnl_quote"] = None
            paths = {
                "main": self.write_status_snapshot(root, "main", main),
                "sui": self.write_status_snapshot(root, "sui", sui),
            }
            with patch.dict(os.environ, {"HB_TELEGRAM_STATUS_SNAPSHOTS": json.dumps(paths)}):
                message = await TelegramNotifier(self.hb_app)._dispatch_command("status", ["sui"])

        self.assertIn("📊 STATUS • SUI", message)
        self.assertIn("💹 Reporting Net: UNAVAILABLE", message)
        self.assertIn("✅ Realized: UNAVAILABLE", message)
        self.assertIn("📈 Unrealized: UNAVAILABLE", message)
        self.assertNotIn("Reporting Net: $0", message)

    async def test_status_rejects_unknown_or_extra_scope_arguments(self):
        notifier = TelegramNotifier(self.hb_app)

        self.assertEqual("Usage: /status [main|sui]", await notifier._dispatch_command("status", ["other"]))
        self.assertEqual("Usage: /status [main|sui]", await notifier._dispatch_command("status", ["main", "sui"]))

    async def test_status_distinguishes_active_from_closing_executor_count(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            lifecycle_executors = [
                {"status": "running"},
                {"status": "shutting-down"},
            ]
            paths = {
                "main": self.write_status_snapshot(root, "main", self.status_snapshot(
                    "ok_rsi", controllers=["btc", "eth", "sol", "xrp", "bnb"],
                    orders=0, executors=1, positions=5, equity=15000, net=12.5,
                    lifecycle_executors=lifecycle_executors,
                )),
                "sui": self.write_status_snapshot(root, "sui", self.status_snapshot(
                    "ok_rsi_sui_sell_only", controllers=["sui-usdc"],
                    orders=0, executors=0, positions=0, equity=500, net=None,
                )),
            }
            with patch.dict(os.environ, {"HB_TELEGRAM_STATUS_SNAPSHOTS": json.dumps(paths)}):
                message = await TelegramNotifier(self.hb_app)._dispatch_command("status", ["main"])

        self.assertIn("⚙️ Executors: 1 active / 2 in flight", message)

    async def test_executors_combines_lifecycle_and_order_transitions_from_both_owners(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            main_executors = [
                {
                    "executor_id": "exec-create-1234567890",
                    "controller_id": "ok-rsi-btc",
                    "connector_name": "okx",
                    "pair": "BTC-USDC",
                    "side": "buy",
                    "executor_type": "position",
                    "status": "not_started",
                    "age": "2s",
                    "amount_base": "0.001",
                    "filled_amount_quote": 0,
                    "net_pnl_quote": 0,
                    "net_pnl_pct": 0,
                    "entry_lifecycle": {"policy": "single_attempt", "state": "awaiting_submission"},
                },
                {
                    "executor_id": "exec-close-1234567890",
                    "controller_id": "ok-rsi-eth",
                    "connector_name": "okx",
                    "pair": "ETH-USDC",
                    "side": "buy",
                    "executor_type": "position",
                    "status": "shutting-down",
                    "close_type": "trailing-stop",
                    "age": "3h4m",
                    "amount_base": "0.004929",
                    "remaining_position_amount_base": "0.004925",
                    "filled_amount_quote": 12.31,
                    "net_pnl_quote": 0.08,
                    "net_pnl_pct": 0.7,
                    "trailing_state": "armed",
                    "trailing_current_price": "2519.85",
                    "trailing_trigger_price": "2510.25",
                    "distance_to_trailing_trigger_pct": "0.0038",
                    "entry_lifecycle": {"policy": "down_only_maker_v1", "state": "managing_fill"},
                    "entry_acquisition": {
                        "policy": "down_only_maker_v1",
                        "state": "cancel_pending_reprice",
                        "terminal_reason": None,
                        "current_order_price": "99.90",
                        "maximum_acceptable_price": "100",
                        "authorized_base": "1",
                        "cumulative_filled_base": "0.2",
                        "remaining_base": "0.8",
                        "reprice_count": 1,
                        "rest_seconds": 30,
                        "total_max_age_seconds": 300,
                        "queue_fill_status": "UNAVAILABLE",
                        "fee_economics_status": "AVAILABLE",
                    },
                },
            ]
            active_orders = [
                {
                    "order_id": "order-create-1234567890",
                    "connector_name": "okx",
                    "pair": "BTC-USDC",
                    "controller_id": "ok-rsi-btc",
                    "side": "buy",
                    "status": "pending_create",
                    "amount_base": "0.001",
                    "filled_amount_base": "0",
                    "remaining_amount_base": "0.001",
                    "price_quote": "98000",
                },
                {
                    "order_id": "order-cancel-1234567890",
                    "connector_name": "okx",
                    "pair": "ETH-USDC",
                    "controller_id": "ok-rsi-eth",
                    "side": "buy",
                    "status": "pending_cancel",
                    "amount_base": "0.004929",
                    "filled_amount_base": "0.001",
                    "remaining_amount_base": "0.003929",
                    "price_quote": "2500",
                },
            ]
            paths = {
                "main": self.write_status_snapshot(root, "main", self.status_snapshot(
                    "ok_rsi", controllers=["btc", "eth", "sol", "xrp", "bnb"],
                    orders=2, executors=1, positions=5, equity=15000, net=12.5,
                    lifecycle_executors=main_executors, active_orders=active_orders,
                )),
                "sui": self.write_status_snapshot(root, "sui", self.status_snapshot(
                    "ok_rsi_sui_sell_only", controllers=["sui-usdc"],
                    orders=0, executors=0, positions=0, equity=500, net=None,
                )),
            }
            with patch.dict(os.environ, {"HB_TELEGRAM_STATUS_SNAPSHOTS": json.dumps(paths)}):
                message = await TelegramNotifier(self.hb_app)._dispatch_command("executors", [])

        self.assertIn("⚙️ EXECUTORS • MAIN + SUI", message)
        self.assertIn("🤖 MAIN: ok_rsi", message)
        self.assertIn("CREATING", message)
        self.assertIn("CLOSING • trailing stop", message)
        self.assertIn("entry down_only_maker_v1:managing fill", message)
        self.assertIn("acquisition down_only_maker_v1:cancel pending reprice", message)
        self.assertIn("reprices 1", message)
        self.assertIn("entry price 99.9 / max 100", message)
        self.assertIn("filled 0.2 / authorized 1", message)
        self.assertIn("timing rest 30s / max age 300s", message)
        self.assertIn("trail armed", message)
        self.assertIn("trigger 2510.25", message)
        self.assertIn("📬 CREATING", message)
        self.assertIn("📬 CANCELING", message)
        self.assertIn("🤖 SUI: ok_rsi_sui_sell_only", message)
        self.assertIn("No in-flight executors or orders.", message)

    async def test_executors_scope_selects_snapshot_and_rejects_unknown_scope(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {
                "main": self.write_status_snapshot(root, "main", self.status_snapshot(
                    "ok_rsi", controllers=["btc", "eth", "sol", "xrp", "bnb"],
                    orders=0, executors=0, positions=5, equity=15000, net=12.5,
                )),
                "sui": self.write_status_snapshot(root, "sui", self.status_snapshot(
                    "ok_rsi_sui_sell_only", controllers=["sui-usdc"],
                    orders=0, executors=0, positions=0, equity=500, net=None,
                )),
            }
            with patch.dict(os.environ, {"HB_TELEGRAM_STATUS_SNAPSHOTS": json.dumps(paths)}):
                notifier = TelegramNotifier(self.hb_app)
                message = await notifier._dispatch_command("executors", ["sui"])
                usage = await notifier._dispatch_command("executors", ["other"])

        self.assertIn("⚙️ EXECUTORS • SUI", message)
        self.assertIn("🤖 SUI: ok_rsi_sui_sell_only", message)
        self.assertNotIn("ok_rsi\n", message)
        self.assertEqual("Usage: /executors [main|sui]", usage)

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
