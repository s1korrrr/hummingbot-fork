import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple
from urllib import parse, request
from urllib.error import HTTPError, URLError

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.event import events
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.pubsub import PubSub
from hummingbot.model.order import Order
from hummingbot.model.trade_fill import TradeFill
from hummingbot.notifier.notifier_base import NotifierBase

if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication
    from hummingbot.core.event.event_listener import EventListener
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base


telegram_logger = None


class TelegramNotifier(NotifierBase):
    TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"
    TELEGRAM_UPDATES_URL = "https://api.telegram.org/bot{token}/getUpdates"
    TELEGRAM_SET_COMMANDS_URL = "https://api.telegram.org/bot{token}/setMyCommands"
    REQUEST_TIMEOUT = 10.0
    LONG_POLL_TIMEOUT = 30
    COMMAND_LOOP_RETRY_DELAY = 5.0
    MAX_MESSAGE_LEN = 3900
    COMMAND_SPECS = [
        ("start", "bind chat and show command list"),
        ("help", "show all trading desk commands"),
        ("ping", "basic bot health check"),
        ("status", "portfolio and bot overview"),
        ("balances", "current balances and USD values"),
        ("controllers", "controller lanes summary"),
        ("focus", "attention queue from controllers"),
        ("executors", "active executors, newest first"),
        ("positions", "held positions summary"),
        ("performance", "realized, unrealized, fees, volume"),
        ("orders", "active orders and latest stored orders"),
        ("fills", "latest fills from SQLite"),
    ]
    BULLET = "•"

    @classmethod
    def logger(cls):
        global telegram_logger
        if telegram_logger is None:
            telegram_logger = logging.getLogger(__name__)
        return telegram_logger

    def __init__(self, hb_app: "HummingbotApplication"):
        super().__init__()
        self._hb_app = hb_app
        self._ev_loop: asyncio.AbstractEventLoop = self._hb_app.ev_loop
        self._config = self._hb_app.client_config_map.telegram_notifications
        self._market_events: Optional["TelegramMarketEventForwarder"] = None
        self._command_task: Optional[asyncio.Task] = None
        self._last_update_id: Optional[int] = None
        self._command_menu_published = False

    def start_market_events_fw(self):
        if not self._hb_app.markets:
            return
        if self._market_events is not None:
            self._market_events.stop()
        self._market_events = TelegramMarketEventForwarder(self._hb_app, self)

    def start(self):
        super().start()
        if self._config.telegram_commands_enabled and self._command_task is None:
            self._command_task = asyncio.create_task(self._run_command_loop())

    def stop_market_events_fw(self):
        if self._market_events is not None:
            self._market_events.stop()
            self._market_events = None

    def stop(self):
        self.stop_market_events_fw()
        if self._command_task is not None:
            self._command_task.cancel()
            self._command_task = None
        super().stop()

    def enqueue_event_message(self, message: str):
        super().add_message_to_queue(message)

    async def _send_message(self, message: str):
        if not self._config.telegram_chat_id:
            self.logger().warning("Skipping Telegram notification because telegram_chat_id is not bound yet.")
            return
        await self._send_telegram_text(message)

    async def _send_telegram_text(self, message: str):
        for chunk in self._chunk_message(message):
            await self._ev_loop.run_in_executor(
                None,
                self._telegram_request,
                "sendMessage",
                {
                    "chat_id": self._config.telegram_chat_id,
                    "text": chunk,
                    "disable_web_page_preview": True,
                },
            )

    def format_market_event(self, event_tag: int, event) -> Optional[str]:
        if event_tag == events.MarketEvent.OrderFilled.value and isinstance(event, events.OrderFilledEvent):
            return self._format_order_filled_message(event)
        if event_tag == events.MarketEvent.BuyOrderCompleted.value and isinstance(event, events.BuyOrderCompletedEvent):
            return self._format_order_completed_message("BUY", event)
        if event_tag == events.MarketEvent.SellOrderCompleted.value and isinstance(event, events.SellOrderCompletedEvent):
            return self._format_order_completed_message("SELL", event)
        if event_tag == events.MarketEvent.OrderFailure.value and isinstance(event, events.MarketOrderFailureEvent):
            return self._format_order_failed_message(event)
        return None

    def _format_order_filled_message(self, event: events.OrderFilledEvent) -> str:
        base_asset, quote_asset = split_hb_trading_pair(event.trading_pair)
        side = event.trade_type.name if isinstance(event.trade_type, TradeType) else str(event.trade_type)
        notional = event.price * event.amount
        fee_text = self._format_fee(event.trade_fee, event.trading_pair, event.price, event.amount, quote_asset)
        pair_label = f"{self._pair_marker(event.trading_pair)} {event.trading_pair}"
        lines = [
            self._title("🟢" if side == "BUY" else "🔴", f"{side} FILLED"),
            self._kv("Pair", pair_label, "📍"),
            self._kv("Time", self._format_timestamp(event.timestamp), "🕒"),
            self._kv("Price", f"{self._format_decimal(event.price)} {quote_asset}", "💵"),
            self._kv("Amount", f"{self._format_decimal(event.amount)} {base_asset}", "📦"),
            self._kv("Notional", f"{self._format_decimal(notional)} {quote_asset}", "💰"),
            self._kv("Fee", fee_text, "🧾"),
            self._kv("Status", "filled", "✅"),
        ]
        return "\n".join(lines)

    def _format_order_completed_message(self, side: str, event) -> str:
        average_price = Decimal("0")
        if event.base_asset_amount != Decimal("0"):
            average_price = event.quote_asset_amount / event.base_asset_amount
        context = self._order_completion_context(event.order_id, event.base_asset, event.quote_asset)
        trading_pair = context["trading_pair"]
        fee_quote = context["fee_quote"]
        net_quote = event.quote_asset_amount - fee_quote if fee_quote is not None else None
        pair_performance = context["pair_performance"]
        lines = [
            self._title("🟢" if side == "BUY" else "🔴", f"{side} COMPLETED"),
            self._kv("Pair", f"{self._pair_marker(trading_pair)} {trading_pair}", "📍"),
            self._kv("Time", self._format_timestamp(event.timestamp), "🕒"),
            self._kv("Average Price", f"{self._format_decimal(average_price)} {event.quote_asset}", "💵"),
            self._kv("Filled Base", f"{self._format_decimal(event.base_asset_amount)} {event.base_asset}", "📦"),
            self._kv("Gross Quote", f"{self._format_decimal(event.quote_asset_amount)} {event.quote_asset}", "💰"),
            self._kv("Status", "completed", "✅"),
        ]
        if fee_quote is not None:
            lines.insert(-1, self._kv("Fee", f"{self._format_decimal(fee_quote)} {event.quote_asset}", "🧾"))
        if net_quote is not None:
            lines.insert(-1, self._kv("Net Quote", f"{self._format_decimal(net_quote)} {event.quote_asset}", "💵"))
        if side == "SELL" and pair_performance is not None:
            lines.insert(
                -1,
                self._kv("Pair Realized PnL", f"${self._format_number(pair_performance['realized_pnl_quote'], 2)}", "📈"),
            )
            lines.insert(
                -1,
                self._kv("Pair Global PnL", f"${self._format_number(pair_performance['global_pnl_quote'], 2)}", "💹"),
            )
        return "\n".join(lines)

    def _format_order_failed_message(self, event: events.MarketOrderFailureEvent) -> str:
        lines = [
            self._title("🚨", "ORDER FAILED"),
            self._kv("Time", self._format_timestamp(event.timestamp), "🕒"),
            self._kv("Status", "failed", "❌"),
        ]
        if event.error_type:
            lines.append(self._kv("Error Type", event.error_type, "⚠️"))
        if event.error_message:
            lines.append(self._kv("Error", event.error_message, "🧯"))
        return "\n".join(lines)

    @classmethod
    def _format_fee(
        cls,
        trade_fee: TradeFeeBase,
        trading_pair: str,
        price: Decimal,
        amount: Decimal,
        quote_asset: str,
    ) -> str:
        try:
            fee_value = trade_fee.fee_amount_in_token(
                trading_pair=trading_pair,
                price=price,
                order_amount=amount,
                token=quote_asset,
            )
            if fee_value != Decimal("0"):
                return f"{cls._format_decimal(fee_value)} {quote_asset}"
        except Exception:
            pass

        flat_fees = ", ".join(f"{cls._format_decimal(fee.amount)} {fee.token}" for fee in trade_fee.flat_fees)
        if flat_fees:
            return flat_fees
        if trade_fee.percent != Decimal("0"):
            percent_value = trade_fee.percent * Decimal("100")
            return f"{cls._format_decimal(percent_value)}%"
        return "0"

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        normalized = value.normalize() if isinstance(value, Decimal) else Decimal(str(value)).normalize()
        return format(normalized, "f").rstrip("0").rstrip(".") or "0"

    @staticmethod
    def _format_timestamp(timestamp: float) -> str:
        return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")

    async def _run_command_loop(self):
        while True:
            try:
                await self._run_command_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().warning(
                    f"Telegram command loop iteration failed; retrying in {self.COMMAND_LOOP_RETRY_DELAY:.0f}s: {e}",
                    exc_info=True,
                )
                await self._sleep(self.COMMAND_LOOP_RETRY_DELAY)

    async def _run_command_cycle(self):
        if self._last_update_id is None:
            await self._initialize_command_state()
        if self._config.telegram_set_my_commands and not self._command_menu_published:
            self._command_menu_published = await self._publish_command_menu()
        updates = await self._ev_loop.run_in_executor(None, self._fetch_updates)
        for update in updates:
            await self._handle_update(update)

    async def _initialize_command_state(self):
        if self._last_update_id is not None:
            return
        updates = await self._ev_loop.run_in_executor(None, self._fetch_updates_once)
        if updates:
            self._last_update_id = max(int(update["update_id"]) for update in updates) + 1
        else:
            self._last_update_id = 0

    async def _publish_command_menu(self) -> bool:
        try:
            await self._ev_loop.run_in_executor(
                None,
                self._telegram_request,
                "setMyCommands",
                {"commands": json.dumps([{"command": c, "description": d} for c, d in self.COMMAND_SPECS])},
            )
            return True
        except Exception as e:
            self.logger().warning(f"Failed to publish Telegram command menu: {e}")
            return False

    def _fetch_updates_once(self) -> List[Dict[str, Any]]:
        return self._telegram_request(
            "getUpdates",
            {
                "timeout": 0,
            },
        )

    def _fetch_updates(self) -> List[Dict[str, Any]]:
        params = {
            "timeout": self.LONG_POLL_TIMEOUT,
            "allowed_updates": json.dumps(["message"]),
        }
        if self._last_update_id is not None:
            params["offset"] = self._last_update_id
        updates = self._telegram_request("getUpdates", params)
        if updates:
            self._last_update_id = max(int(update["update_id"]) for update in updates) + 1
        return updates

    async def _handle_update(self, update: Dict[str, Any]):
        message = update.get("message") or {}
        text = str(message.get("text") or "").strip()
        if not text.startswith("/"):
            return

        chat = message.get("chat") or {}
        chat_id = str(chat.get("id") or "")
        if chat_id == "":
            return
        if not self._bind_or_validate_chat(chat_id):
            return

        command_name = text.split()[0].split("@")[0].lstrip("/").lower()
        args = text.split()[1:]
        response = await self._dispatch_command(command_name, args)
        if response:
            await self._send_telegram_text(response)

    def _bind_or_validate_chat(self, chat_id: str) -> bool:
        if not self._config.telegram_chat_id:
            self._config.telegram_chat_id = chat_id
            save_fn = getattr(self._hb_app, "save_client_config", None)
            if callable(save_fn):
                try:
                    save_fn()
                except Exception as e:
                    self.logger().warning(f"Failed to persist telegram_chat_id binding: {e}")
            self.logger().info(f"Bound Telegram notifications to chat_id={chat_id}")
            return True
        return self._config.telegram_chat_id == chat_id

    async def _dispatch_command(self, command_name: str, args: List[str]) -> str:
        handlers = {
            "start": self._command_start,
            "help": self._command_help,
            "ping": self._command_ping,
            "status": self._command_status,
            "balances": self._command_balances,
            "controllers": self._command_controllers,
            "focus": self._command_focus,
            "executors": self._command_executors,
            "positions": self._command_positions,
            "performance": self._command_performance,
            "pnl": self._command_performance,
            "orders": self._command_orders,
            "fills": self._command_fills,
        }
        handler = handlers.get(command_name)
        if handler is None:
            return "Unknown command. Use /help."
        try:
            return await handler(args)
        except Exception as e:
            self.logger().error(f"Telegram command failed | command={command_name} error={e}", exc_info=True)
            return f"Command /{command_name} failed: {e}"

    async def _command_start(self, _: List[str]) -> str:
        return (
            "🛰️ RSI Capital Telegram control is online.\n"
            f"🔗 Chat bound: {self._config.telegram_chat_id}\n\n"
            f"{await self._command_help([])}"
        )

    async def _command_help(self, _: List[str]) -> str:
        lines = [self._title("🤖", "Available Commands")]
        for command, description in self.COMMAND_SPECS:
            lines.append(f"{self.BULLET} /{command} - {description}")
        return "\n".join(lines)

    async def _command_ping(self, _: List[str]) -> str:
        strategy = self._current_strategy()
        is_running = strategy is not None and getattr(self._hb_app.trading_core, "_strategy_running", False)
        return (
            f"{self._title('🏓', 'pong')}\n"
            f"{self._kv('Instance', self._hb_app.instance_id, '🆔')}\n"
            f"{self._kv('Strategy', self._hb_app.strategy_name or 'n/a', '🧠')}\n"
            f"{self._kv('Running', 'yes' if is_running else 'no', '▶️' if is_running else '⏸️')}\n"
            f"{self._kv('Time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '🕒')}"
        )

    async def _command_status(self, _: List[str]) -> str:
        snapshot = self._runtime_snapshot()
        if snapshot is None:
            return self._fallback_status_message()
        summary = snapshot["summary"]
        focus_queue = snapshot.get("focus_queue", [])
        lines = [
            self._title("📊", "STATUS"),
            self._kv("Bot", snapshot["bot_name"], "🤖"),
            self._kv("Updated", snapshot["updated_at"], "🕒"),
            self._kv("Total Equity", f"${self._format_number(summary.get('balance_value_quote', 0), 2)}", "🏦"),
            self._kv("Reporting Net", f"${self._format_number(summary.get('net_pnl_quote', 0), 2)}", "💹"),
            self._kv("Realized", f"${self._format_number(summary.get('realized_pnl_quote', 0), 2)}", "✅"),
            self._kv("Unrealized", f"${self._format_number(summary.get('unrealized_pnl_quote', 0), 2)}", "📈"),
            self._kv("Fees", f"${self._format_number(summary.get('fees_quote', 0), 2)}", "🧾"),
            self._kv("Active Orders", snapshot.get('active_orders_count', 0), "📬"),
            self._kv("Active Executors", summary.get('active_executor_count', 0), "⚙️"),
            self._kv("Held Positions", summary.get('positions_held_count', 0), "👜"),
            self._kv("Controllers", summary.get('controller_count', 0), "🧠"),
        ]
        if focus_queue:
            lines.append(self._title("🎯", "Focus Queue"))
            for item in focus_queue[:5]:
                lines.append(f"{self.BULLET} {item.get('pair', 'n/a')}: {item.get('focus', '')}")
        return "\n".join(lines)

    async def _command_balances(self, _: List[str]) -> str:
        snapshot = self._runtime_snapshot()
        if snapshot is None:
            return "Balances are unavailable because no V2 strategy is running."
        balances = sorted(snapshot.get("balances", []), key=lambda row: row.get("value_quote", 0), reverse=True)
        if not balances:
            return "No balances available."
        lines = [self._title("💼", "BALANCES")]
        for row in balances:
            lines.append(
                f"{self.BULLET} {row['exchange']} {row['asset']} | total {self._format_number(row['total_balance'], 6)} | "
                f"avail {self._format_number(row['available_balance'], 6)} | "
                f"value ${self._format_number(row['value_quote'], 2)}"
            )
        return "\n".join(lines)

    async def _command_controllers(self, _: List[str]) -> str:
        snapshot = self._runtime_snapshot()
        if snapshot is None:
            return "Controller summary is unavailable because no V2 strategy is running."
        controllers = snapshot.get("controllers", [])
        if not controllers:
            return "No controllers are active."
        lines = [self._title("🧠", "CONTROLLERS")]
        for row in controllers:
            price = "n/a" if row.get("price_quote") is None else self._format_number(row["price_quote"], 4)
            breakeven = "n/a" if row.get("breakeven_price") is None else self._format_number(row["breakeven_price"], 4)
            upnl_pct = "n/a" if row.get("unrealized_pnl_pct") is None else f"{self._format_number(row['unrealized_pnl_pct'], 2)}%"
            lines.append(
                f"{self.BULLET} {row['pair']} | {row['state']} | signal {row['signal']} | price {price} | "
                f"breakeven {breakeven} | u% {upnl_pct} | {row['execs']} | {row['exposure_text']} | gate {row['gate']}"
            )
        return "\n".join(lines)

    async def _command_focus(self, _: List[str]) -> str:
        snapshot = self._runtime_snapshot()
        if snapshot is None:
            return "Focus queue is unavailable because no V2 strategy is running."
        focus_queue = snapshot.get("focus_queue", [])
        if not focus_queue:
            return "Focus queue is empty."
        lines = [self._title("🎯", "FOCUS QUEUE")]
        for item in focus_queue[:10]:
            lines.append(f"{self.BULLET} {item.get('pair', 'n/a')} | {item.get('focus', '')}")
        return "\n".join(lines)

    async def _command_executors(self, _: List[str]) -> str:
        strategy = self._current_strategy()
        if strategy is None or not hasattr(strategy, "_build_controller_status_entries"):
            return "Executors are unavailable because no V2 strategy is running."
        rows: List[Dict[str, Any]] = []
        for entry in strategy._build_controller_status_entries():
            pair = entry["summary"].get("pair", entry["controller_id"])
            for executor in entry["executors"]:
                if not executor.is_active:
                    continue
                side = strategy._compact_side_label(executor.side or getattr(executor.config, "side", None)) or "n/a"
                rows.append(
                    {
                        "timestamp": executor.timestamp,
                        "pair": pair,
                        "side": side,
                        "type": strategy._compact_executor_type(executor.type),
                        "status": strategy._compact_enum_label(executor.status) or "unknown",
                        "age": strategy._format_executor_age_short(executor.timestamp),
                        "pnl_quote": executor.net_pnl_quote,
                        "pnl_pct": strategy._safe_decimal(executor.net_pnl_pct) * Decimal("100"),
                        "fill_quote": executor.filled_amount_quote,
                    }
                )
        rows.sort(key=lambda row: row["timestamp"], reverse=True)
        if not rows:
            return "No active executors."
        lines = [self._title("⚙️", "ACTIVE EXECUTORS")]
        for row in rows[:12]:
            lines.append(
                f"{self.BULLET} {row['pair']} | {row['side']} | {row['type']} | {row['status']} | {row['age']} | "
                f"PnL ${self._format_number(row['pnl_quote'], 2)} / {self._format_number(row['pnl_pct'], 2)}% | "
                f"Fill ${self._format_number(row['fill_quote'], 2)}"
            )
        return "\n".join(lines)

    async def _command_positions(self, _: List[str]) -> str:
        snapshot = self._runtime_snapshot()
        if snapshot is None:
            return "Positions are unavailable because no V2 strategy is running."
        positions = sorted(snapshot.get("positions_held", []), key=lambda row: row.get("value_quote", 0), reverse=True)
        if not positions:
            return "No held positions."
        lines = [self._title("👜", "POSITIONS HELD")]
        for row in positions:
            lines.append(
                f"{self.BULLET} {row['pair']} | {row['side']} | amt {self._format_number(row['amount_base'], 6)} | "
                f"value ${self._format_number(row['value_quote'], 2)} | "
                f"breakeven {self._format_number(row['breakeven_price'], 6)} | "
                f"uPnL ${self._format_number(row['unrealized_pnl_quote'], 2)} | "
                f"rPnL ${self._format_number(row['realized_pnl_quote'], 2)} | "
                f"fees ${self._format_number(row['fees_quote'], 2)}"
            )
        return "\n".join(lines)

    async def _command_performance(self, _: List[str]) -> str:
        strategy = self._current_strategy()
        if strategy is None or not hasattr(strategy, "_build_controller_status_entries"):
            return "Performance is unavailable because no V2 strategy is running."
        snapshot = self._runtime_snapshot()
        if snapshot is None:
            return self._fallback_status_message()
        lines = [
            self._title("📈", "PERFORMANCE"),
            self._kv("Net", f"${self._format_number(snapshot['summary'].get('net_pnl_quote', 0), 2)}", "💹"),
            self._kv("Realized", f"${self._format_number(snapshot['summary'].get('realized_pnl_quote', 0), 2)}", "✅"),
            self._kv("Unrealized", f"${self._format_number(snapshot['summary'].get('unrealized_pnl_quote', 0), 2)}", "📊"),
            self._kv("Fees", f"${self._format_number(snapshot['summary'].get('fees_quote', 0), 2)}", "🧾"),
        ]
        for entry in strategy._build_controller_status_entries():
            performance = entry["performance"]
            if performance is None:
                continue
            pair = entry["summary"].get("pair", entry["controller_id"])
            lines.append(
                f"{self.BULLET} {pair} | realized ${self._format_number(performance.realized_pnl_quote, 2)} | "
                f"unrealized ${self._format_number(performance.unrealized_pnl_quote, 2)} | "
                f"global ${self._format_number(performance.global_pnl_quote, 2)} | "
                f"volume ${self._format_number(performance.volume_traded, 2)}"
            )
        return "\n".join(lines)

    async def _command_orders(self, _: List[str]) -> str:
        strategy = self._current_strategy()
        lines = [self._title("📬", "ORDERS")]
        if strategy is not None:
            try:
                active_df = strategy.active_orders_df()
                if len(active_df) > 0:
                    lines.append("🟢 Active")
                    for row in active_df.head(10).to_dict("records"):
                        lines.append(
                            f"{self.BULLET} {row['Exchange']} {row['Market']} | {row['Side']} | "
                            f"price {self._format_number(row['Price'], 6)} | "
                            f"amount {self._format_number(row['Amount'], 6)} | age {row['Age']}"
                        )
            except Exception:
                lines.append("🟢 Active: none")
        recent_orders = self._recent_orders(limit=8)
        if recent_orders:
            lines.append("🗃️ Latest Stored")
            for order in recent_orders:
                lines.append(
                    f"{self.BULLET} {self._format_db_timestamp(order.last_update_timestamp)} | {order.symbol} | "
                    f"price {self._format_number(order.price, 6)} | amount {self._format_number(order.amount, 6)} | "
                    f"status {order.last_status}"
                )
        if len(lines) == 1:
            return "No active or stored orders."
        return "\n".join(lines)

    async def _command_fills(self, _: List[str]) -> str:
        fills = self._recent_fills(limit=10)
        if not fills:
            return "No fills found in SQLite for the current config."
        lines = [self._title("🧾", "RECENT FILLS")]
        for fill in fills:
            notional = Decimal(str(fill.price)) * Decimal(str(fill.amount))
            fee_quote = Decimal(str(fill.trade_fee_in_quote or 0))
            lines.append(
                f"{self.BULLET} {self._format_db_timestamp(fill.timestamp)} | {fill.symbol} | {fill.trade_type} | "
                f"price {self._format_number(fill.price, 6)} | amount {self._format_number(fill.amount, 6)} | "
                f"notional ${self._format_number(notional, 2)} | fee ${self._format_number(fee_quote, 2)}"
            )
        return "\n".join(lines)

    def _fallback_status_message(self) -> str:
        return (
            "⚠️ No active V2 strategy snapshot is available.\n"
            f"{self._kv('Strategy', self._hb_app.strategy_name or 'n/a', '🧠')}\n"
            f"{self._kv('Running', 'yes' if getattr(self._hb_app.trading_core, '_strategy_running', False) else 'no', '▶️' if getattr(self._hb_app.trading_core, '_strategy_running', False) else '⏸️')}"
        )

    def _current_strategy(self) -> Optional["StrategyV2Base"]:
        strategy = getattr(self._hb_app.trading_core, "strategy", None)
        if strategy is None or not hasattr(strategy, "_build_runtime_status_snapshot"):
            return None
        return strategy

    def _runtime_snapshot(self) -> Optional[Dict[str, Any]]:
        strategy = self._current_strategy()
        if strategy is None:
            return None
        return strategy._build_runtime_status_snapshot()

    @staticmethod
    def _pair_marker(trading_pair: Optional[str]) -> str:
        palette = ["🔵", "🟢", "🟡", "🟠", "🔴", "🟣", "🟤", "⚪"]
        base_asset = str(trading_pair or "").split("-")[0].upper()
        pinned = {
            "BTC": "🟠",
            "ETH": "🟣",
            "BNB": "🟡",
            "SOL": "🟢",
            "XRP": "🔵",
            "DOGE": "🟤",
        }
        if base_asset in pinned:
            return pinned[base_asset]
        if not base_asset:
            return "⚪"
        return palette[sum(ord(char) for char in base_asset) % len(palette)]

    def _order_record(self, order_id: str) -> Optional[Order]:
        recorder = self._markets_recorder()
        if recorder is None:
            return None
        config_file_path = getattr(recorder, "config_file_path", None)
        if not config_file_path:
            return None
        with recorder.sql_manager.get_new_session() as session:
            return (
                session.query(Order)
                .filter(Order.config_file_path == config_file_path, Order.id == order_id)
                .one_or_none()
            )

    def _fills_for_order(self, order_id: str) -> List[TradeFill]:
        recorder = self._markets_recorder()
        if recorder is None:
            return []
        config_file_path = getattr(recorder, "config_file_path", None)
        if not config_file_path:
            return []
        with recorder.sql_manager.get_new_session() as session:
            return (
                session.query(TradeFill)
                .filter(TradeFill.config_file_path == config_file_path, TradeFill.order_id == order_id)
                .order_by(TradeFill.timestamp.asc())
                .all()
            )

    def _pair_performance_context(self, trading_pair: str) -> Optional[Dict[str, Decimal]]:
        strategy = self._current_strategy()
        if strategy is None or not hasattr(strategy, "_build_controller_status_entries"):
            return None
        try:
            entries = strategy._build_controller_status_entries()
        except Exception:
            return None
        for entry in entries:
            summary = entry.get("summary", {})
            pair = summary.get("pair", entry.get("controller_id"))
            if pair != trading_pair:
                continue
            performance = entry.get("performance")
            if performance is None:
                return None
            return {
                "realized_pnl_quote": Decimal(str(getattr(performance, "realized_pnl_quote", 0))),
                "unrealized_pnl_quote": Decimal(str(getattr(performance, "unrealized_pnl_quote", 0))),
                "global_pnl_quote": Decimal(str(getattr(performance, "global_pnl_quote", 0))),
            }
        return None

    def _order_completion_context(self, order_id: str, base_asset: str, quote_asset: str) -> Dict[str, Any]:
        trading_pair = f"{base_asset}-{quote_asset}"
        order = self._order_record(order_id)
        if order is not None and getattr(order, "symbol", None):
            trading_pair = order.symbol
        fills = self._fills_for_order(order_id)
        fee_quote = sum((Decimal(str(fill.trade_fee_in_quote or 0)) for fill in fills), Decimal("0")) if fills else None
        return {
            "trading_pair": trading_pair,
            "fee_quote": fee_quote,
            "pair_performance": self._pair_performance_context(trading_pair),
        }

    def _recent_orders(self, limit: int) -> List[Order]:
        recorder = self._markets_recorder()
        if recorder is None:
            return []
        config_file_path = getattr(recorder, "config_file_path", None)
        if not config_file_path:
            return []
        with recorder.sql_manager.get_new_session() as session:
            return (
                session.query(Order)
                .filter(Order.config_file_path == config_file_path)
                .order_by(Order.last_update_timestamp.desc())
                .limit(limit)
                .all()
            )

    def _recent_fills(self, limit: int) -> List[TradeFill]:
        recorder = self._markets_recorder()
        if recorder is None:
            return []
        config_file_path = getattr(recorder, "config_file_path", None)
        if not config_file_path:
            return []
        return recorder.get_trades_for_config(config_file_path, number_of_rows=limit)

    def _markets_recorder(self):
        try:
            from hummingbot.connector.markets_recorder import MarketsRecorder
            return MarketsRecorder.get_instance()
        except Exception:
            return None

    @classmethod
    def _format_number(cls, value: Any, decimals: int) -> str:
        try:
            decimal_value = Decimal(str(value))
        except Exception:
            decimal_value = Decimal("0")
        return f"{decimal_value:.{decimals}f}".rstrip("0").rstrip(".") if "." in f"{decimal_value:.{decimals}f}" else f"{decimal_value:.{decimals}f}"

    @staticmethod
    def _format_db_timestamp(timestamp_ms: Any) -> str:
        try:
            ts = float(timestamp_ms) / 1e3
        except Exception:
            return "n/a"
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _chunk_message(message: str) -> List[str]:
        if len(message) <= TelegramNotifier.MAX_MESSAGE_LEN:
            return [message]
        chunks: List[str] = []
        current_lines: List[str] = []
        current_len = 0
        for line in message.splitlines():
            extra = len(line) + (1 if current_lines else 0)
            if current_len + extra > TelegramNotifier.MAX_MESSAGE_LEN and current_lines:
                chunks.append("\n".join(current_lines))
                current_lines = [line]
                current_len = len(line)
            else:
                current_lines.append(line)
                current_len += extra
        if current_lines:
            chunks.append("\n".join(current_lines))
        return chunks

    @classmethod
    def _title(cls, icon: str, text: str) -> str:
        return f"{icon} {text}"

    @classmethod
    def _kv(cls, label: str, value: Any, icon: str = BULLET) -> str:
        return f"{icon} {label}: {value}"

    def _telegram_request(self, method: str, params: Dict[str, Any]):
        token = self._config.telegram_bot_token.get_secret_value()
        base_urls = {
            "sendMessage": self.TELEGRAM_API_URL,
            "getUpdates": self.TELEGRAM_UPDATES_URL,
            "setMyCommands": self.TELEGRAM_SET_COMMANDS_URL,
        }
        url = base_urls[method].format(token=token)
        encoded = parse.urlencode(
            {
                key: value if isinstance(value, str) else json.dumps(value) if isinstance(value, (dict, list, bool)) else value
                for key, value in params.items()
            }
        ).encode()
        req = request.Request(
            url=url,
            data=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self.REQUEST_TIMEOUT + self.LONG_POLL_TIMEOUT) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Telegram API {method} failed: {e.code} {body}")
        except URLError as e:
            raise RuntimeError(f"Telegram API {method} failed: {e}")

        if not payload.get("ok"):
            raise RuntimeError(f"Telegram API {method} failed: {payload}")
        return payload.get("result", [])


class TelegramMarketEventForwarder:
    def __init__(self, hb_app: "HummingbotApplication", notifier: TelegramNotifier):
        self._hb_app = hb_app
        self._notifier = notifier
        self._markets: List[ConnectorBase] = list(self._hb_app.markets.values())
        self._forwarder = SourceInfoEventForwarder(self._queue_event_message)
        self._market_event_pairs: List[Tuple[int, "EventListener"]] = []

        config = self._hb_app.client_config_map.telegram_notifications
        if config.telegram_notify_order_filled:
            self._market_event_pairs.append((events.MarketEvent.OrderFilled, self._forwarder))
        if config.telegram_notify_order_completed:
            self._market_event_pairs.extend(
                [
                    (events.MarketEvent.BuyOrderCompleted, self._forwarder),
                    (events.MarketEvent.SellOrderCompleted, self._forwarder),
                ]
            )
        if config.telegram_notify_order_failed:
            self._market_event_pairs.append((events.MarketEvent.OrderFailure, self._forwarder))

        self._start_event_listeners()

    def _queue_event_message(self, event_tag: int, pubsub: PubSub, event):
        message = self._notifier.format_market_event(event_tag, event)
        if message is None:
            return
        self._notifier.enqueue_event_message(message)

    def _start_event_listeners(self):
        for market in self._markets:
            for event_pair in self._market_event_pairs:
                market.add_listener(event_pair[0], event_pair[1])

    def stop(self):
        for market in self._markets:
            for event_pair in self._market_event_pairs:
                market.remove_listener(event_pair[0], event_pair[1])
