import asyncio
import json
import logging
import os
import re
import stat
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
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
from hummingbot.notifier.telegram_polling import (
    TelegramPollingAdmissionDenied,
    TelegramPollingConflict,
    TelegramPollingLease,
    polling_admitted,
    require_polling_admission,
    wait_for_poll,
)
from hummingbot.strategy_v2.executors.order_accounting import base_fee_amount

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
    STATUS_SNAPSHOT_ENV = "HB_TELEGRAM_STATUS_SNAPSHOTS"
    STATUS_SNAPSHOT_MAX_BYTES = 2 * 1024 * 1024
    STATUS_SNAPSHOT_MAX_AGE_SECONDS = 60
    STATUS_LANES = {
        "main": "ok_rsi",
        "sui": "ok_rsi_sui_sell_only",
    }
    EXECUTOR_LIFECYCLE_LABELS = {
        "not_started": "CREATING",
        "running": "RUNNING",
        "shutting_down": "CLOSING",
        "terminated": "CLOSED",
    }
    ORDER_LIFECYCLE_LABELS = {
        "pending_create": "CREATING",
        "pending_cancel": "CANCELING",
        "partially_filled": "PARTIALLY FILLED",
        "open": "OPEN",
        "created": "CREATED",
    }
    COMMAND_SPECS = [
        ("start", "bind chat and show command list"),
        ("help", "show all trading desk commands"),
        ("ping", "basic bot health check"),
        ("status", "portfolio and bot overview"),
        ("balances", "current balances and USD values"),
        ("controllers", "controller lanes summary"),
        ("focus", "attention queue from controllers"),
        ("executors", "executor and order lifecycle (main|sui)"),
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
        if os.environ.get("HB_TELEGRAM_OWNER_LABEL_ENABLED") == "1":
            owner = self._hb_app.instance_id
            if not isinstance(owner, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", owner) is None:
                raise ValueError("Outgoing Telegram owner label requires a valid native instance ID")
            message = f"[{owner}]\n{message}"
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
        if side == "SELL":
            lines[-1:-1] = self._trade_realized_pnl_lines(self._sell_fill_realized_pnl_quote(event))
        return "\n".join(lines)

    def _format_order_completed_message(self, side: str, event) -> str:
        average_price = Decimal("0")
        if event.base_asset_amount != Decimal("0"):
            average_price = event.quote_asset_amount / event.base_asset_amount
        context = self._order_completion_context(
            event.order_id,
            event.base_asset,
            event.quote_asset,
            include_trade_realized_pnl=side == "SELL",
        )
        trading_pair = context["trading_pair"]
        fee_quote = context["fee_quote"]
        net_quote = event.quote_asset_amount - fee_quote if fee_quote is not None else None
        trade_realized_pnl_quote = context.get("trade_realized_pnl_quote")
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
        if side == "SELL":
            lines[-1:-1] = self._trade_realized_pnl_lines(trade_realized_pnl_quote)
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
        fee_value = cls._fee_quote_from_trade_fee(trade_fee, trading_pair, price, amount)
        if fee_value is not None:
            return f"{cls._format_decimal(fee_value)} {quote_asset}" if fee_value else "0"
        try:
            raw_fees = [(Decimal(str(fee.amount)), fee.token) for fee in trade_fee.flat_fees]
            if any(not fee.is_finite() for fee, _ in raw_fees):
                return "UNAVAILABLE"
            if raw_fees:
                return ", ".join(f"{cls._format_decimal(fee)} {token}" for fee, token in raw_fees)
            percent = Decimal(str(trade_fee.percent))
            if percent.is_finite() and percent:
                return f"{cls._format_decimal(percent * Decimal('100'))}%"
        except (ValueError, TypeError, ArithmeticError):
            pass
        return "UNAVAILABLE"

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        normalized = value.normalize() if isinstance(value, Decimal) else Decimal(str(value)).normalize()
        formatted = format(normalized, "f")
        return formatted.rstrip("0").rstrip(".") if "." in formatted else formatted

    @staticmethod
    def _format_timestamp(timestamp: float) -> str:
        return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")

    async def _run_command_loop(self):
        lease = TelegramPollingLease(self._config.telegram_bot_token.get_secret_value())
        try:
            waiting_logged = False
            while True:
                if not polling_admitted():
                    lease.close()
                    if not waiting_logged:
                        self.logger().info('Telegram commands await operator admission; notifications remain enabled.')
                        waiting_logged = True
                    await self._sleep(self.COMMAND_LOOP_RETRY_DELAY)
                    continue
                if not lease.acquire():
                    if not waiting_logged:
                        self.logger().info('Another local bot owns Telegram commands; notifications remain enabled.')
                        waiting_logged = True
                    await self._sleep(self.COMMAND_LOOP_RETRY_DELAY)
                    continue
                waiting_logged = False
                await self._run_owned_command_loop()
                lease.close()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception('Telegram command ownership failed; polling disabled, notifications remain enabled.')
        finally:
            lease.close()

    async def _run_owned_command_loop(self):
        outage_started = None
        failed_attempts = 0
        while True:
            try:
                require_polling_admission()
                await self._run_command_cycle()
                if outage_started is not None:
                    self.logger().info(
                        "Telegram command polling recovered after %.1fs and %d failed attempts.",
                        self._ev_loop.time() - outage_started,
                        failed_attempts,
                    )
                    outage_started = None
                    failed_attempts = 0
            except asyncio.CancelledError:
                raise
            except TelegramPollingAdmissionDenied:
                return
            except TelegramPollingConflict:
                self.logger().warning(
                    'Telegram command polling suspended: another consumer or webhook uses this token. '
                    'Notifications remain enabled. Stop the competing poller before restarting this notifier.'
                )
                # Keep the local lease so standby bots do not repeat the same conflict.
                while polling_admitted():
                    await self._sleep(self.COMMAND_LOOP_RETRY_DELAY)
                return
            except Exception as e:
                if outage_started is None:
                    outage_started = self._ev_loop.time()
                failed_attempts += 1
                self.logger().warning(
                    f"Telegram command loop iteration failed; retrying in {self.COMMAND_LOOP_RETRY_DELAY:.0f}s: {e}",
                    exc_info=True,
                )
                await self._sleep(self.COMMAND_LOOP_RETRY_DELAY)

    async def _run_command_cycle(self):
        require_polling_admission()
        if self._last_update_id is None:
            await self._initialize_command_state()
        if self._config.telegram_set_my_commands and not self._command_menu_published:
            require_polling_admission()
            self._command_menu_published = await self._publish_command_menu()
        require_polling_admission()
        updates = await wait_for_poll(self._ev_loop, self._fetch_updates)
        for update in updates:
            require_polling_admission()
            await self._handle_update(update)

    async def _initialize_command_state(self):
        if self._last_update_id is not None:
            return
        require_polling_admission()
        updates = await wait_for_poll(self._ev_loop, self._fetch_updates_once)
        require_polling_admission()
        if updates:
            self._last_update_id = max(int(update["update_id"]) for update in updates) + 1
        else:
            self._last_update_id = 0

    async def _publish_command_menu(self) -> bool:
        try:
            require_polling_admission()
            await wait_for_poll(self._ev_loop, lambda: self._telegram_request(
                "setMyCommands", {"commands": json.dumps([
                    {"command": c, "description": d} for c, d in self.COMMAND_SPECS])}))
            require_polling_admission()
            return True
        except TelegramPollingAdmissionDenied:
            raise
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
        require_polling_admission()
        if updates:
            self._last_update_id = max(int(update["update_id"]) for update in updates) + 1
        return updates

    async def _handle_update(self, update: Dict[str, Any]):
        require_polling_admission()
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
            require_polling_admission()
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

    async def _command_status(self, args: List[str]) -> str:
        if len(args) > 1 or (args and args[0].lower() not in self.STATUS_LANES):
            return "Usage: /status [main|sui]"
        scope = args[0].lower() if args else None
        configured = self._configured_status_snapshots()
        if configured is None:
            snapshot = self._runtime_snapshot()
            if scope is not None and self._status_lane(snapshot) != scope:
                return f"⚠️ {scope.upper()} status unavailable from this Telegram owner."
            if snapshot is None:
                return self._fallback_status_message()
            return self._format_status_snapshot(snapshot, "STATUS" if scope is None else f"STATUS • {scope.upper()}")
        if scope is not None:
            snapshot = configured[scope]
            if snapshot is None:
                return f"⚠️ {scope.upper()} status unavailable."
            return self._format_status_snapshot(snapshot, f"STATUS • {scope.upper()}")

        lines = [self._title("📊", "STATUS • MAIN + SUI")]
        for lane in self.STATUS_LANES:
            snapshot = configured[lane]
            if snapshot is None:
                lines.append(f"⚠️ {lane.upper()}: UNAVAILABLE")
            else:
                lines.extend(self._format_status_overview(lane, snapshot))
        lines.append("Use /status main or /status sui for full details; /executors for lifecycle details.")
        return "\n".join(lines)

    def _format_status_snapshot(self, snapshot: Dict[str, Any], title: str) -> str:
        if snapshot is None:
            return self._fallback_status_message()
        summary = snapshot["summary"]
        focus_queue = snapshot.get("focus_queue", [])
        lines = [
            self._title("📊", title),
            self._kv("Bot", snapshot["bot_name"], "🤖"),
            self._kv("Updated", snapshot["updated_at"], "🕒"),
            self._kv("Total Equity", self._format_status_money(summary.get("balance_value_quote")), "🏦"),
            self._kv("Reporting Net", self._format_status_money(summary.get("net_pnl_quote")), "💹"),
            self._kv("Realized", self._format_status_money(summary.get("realized_pnl_quote")), "✅"),
            self._kv("Unrealized", self._format_status_money(summary.get("unrealized_pnl_quote")), "📈"),
            self._kv("Fees", self._format_status_money(summary.get("fees_quote")), "🧾"),
            self._kv("Active Orders", snapshot.get('active_orders_count', 0), "📬"),
            self._kv("Executors", self._format_executor_counts(summary), "⚙️"),
            self._kv("Held Positions", summary.get('positions_held_count', 0), "👜"),
            self._kv("Controllers", summary.get('controller_count', 0), "🧠"),
        ]
        if focus_queue:
            lines.append(self._title("🎯", "Focus Queue"))
            for item in focus_queue[:5]:
                lines.append(f"{self.BULLET} {item.get('pair', 'n/a')}: {item.get('focus', '')}")
        return "\n".join(lines)

    def _format_status_overview(self, lane: str, snapshot: Dict[str, Any]) -> List[str]:
        summary = snapshot["summary"]
        return [
            self._kv(lane.upper(), snapshot["bot_name"], "🤖"),
            self._kv("Updated", snapshot["updated_at"], "🕒"),
            self._kv("Reporting Net", self._format_status_money(summary.get("net_pnl_quote")), "💹"),
            self._kv("Orders", snapshot["active_orders_count"], "📬"),
            self._kv("Executors", self._format_executor_counts(summary), "⚙️"),
            self._kv("Held Positions", summary["positions_held_count"], "👜"),
            self._kv("Controllers", summary["controller_count"], "🧠"),
        ]

    @staticmethod
    def _format_executor_counts(summary: Dict[str, Any]) -> str:
        active = summary.get("active_executor_count", 0)
        in_flight = summary.get("lifecycle_executor_count", active)
        return f"{active} active / {in_flight} in flight"

    def _configured_status_snapshots(self) -> Optional[Dict[str, Optional[Dict[str, Any]]]]:
        raw = os.environ.get(self.STATUS_SNAPSHOT_ENV)
        if raw is None:
            return None
        try:
            paths = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError("Invalid Telegram status snapshot configuration") from exc
        if not isinstance(paths, dict) or set(paths) != set(self.STATUS_LANES):
            raise ValueError("Telegram status snapshots must configure main and sui")

        snapshots = {}
        for lane, expected_bot in self.STATUS_LANES.items():
            try:
                snapshots[lane] = self._read_status_snapshot(paths[lane], expected_bot)
            except (OSError, ValueError, TypeError, UnicodeError):
                self.logger().warning(
                    "Telegram %s runtime status snapshot is unavailable or invalid.",
                    lane,
                )
                snapshots[lane] = None
        return snapshots

    def _read_status_snapshot(self, raw_path: Any, expected_bot: str) -> Dict[str, Any]:
        if not isinstance(raw_path, str):
            raise ValueError("Status snapshot path must be text")
        path = Path(raw_path)
        if not path.is_absolute() or ".." in path.parts:
            raise ValueError("Status snapshot path must be absolute")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > self.STATUS_SNAPSHOT_MAX_BYTES:
                raise ValueError("Status snapshot must be a bounded regular file")
            payload = stream.read(self.STATUS_SNAPSHOT_MAX_BYTES + 1)
        if len(payload) > self.STATUS_SNAPSHOT_MAX_BYTES:
            raise ValueError("Status snapshot exceeds size limit")
        snapshot = json.loads(payload)
        if not isinstance(snapshot, dict) or snapshot.get("schema_version") != 1:
            raise ValueError("Unsupported status snapshot schema")
        if snapshot.get("bot_name") != expected_bot:
            raise ValueError("Status snapshot owner mismatch")
        if not isinstance(snapshot.get("summary"), dict) or not isinstance(snapshot.get("controllers"), list):
            raise ValueError("Incomplete status snapshot")
        for key in ("active_executors", "lifecycle_executors", "active_orders"):
            value = snapshot.get(key)
            if value is not None and (not isinstance(value, list) or any(not isinstance(row, dict) for row in value)):
                raise ValueError(f"Status snapshot {key} are invalid")
        updated_at = snapshot.get("updated_at")
        if not isinstance(updated_at, str):
            raise ValueError("Status snapshot timestamp is unavailable")
        updated = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        if updated.tzinfo is None:
            raise ValueError("Status snapshot timestamp requires a timezone")
        age = (datetime.now(timezone.utc) - updated.astimezone(timezone.utc)).total_seconds()
        if age < -5 or age > self.STATUS_SNAPSHOT_MAX_AGE_SECONDS:
            raise ValueError("Status snapshot is stale")
        counts = {
            "active_orders_count": snapshot.get("active_orders_count"),
            "active_executor_count": snapshot["summary"].get("active_executor_count"),
            "positions_held_count": snapshot["summary"].get("positions_held_count"),
            "controller_count": snapshot["summary"].get("controller_count"),
        }
        if any(type(value) is not int or value < 0 for value in counts.values()):
            raise ValueError("Status snapshot counts are invalid")
        if counts["controller_count"] != len(snapshot["controllers"]):
            raise ValueError("Status snapshot controller count mismatch")
        lifecycle_count = snapshot["summary"].get("lifecycle_executor_count")
        lifecycle_executors = snapshot.get("lifecycle_executors")
        if lifecycle_count is not None:
            if type(lifecycle_count) is not int or lifecycle_count < 0:
                raise ValueError("Status snapshot lifecycle executor count is invalid")
            if lifecycle_executors is None or lifecycle_count != len(lifecycle_executors):
                raise ValueError("Status snapshot lifecycle executor count mismatch")
        for key in ("balance_value_quote", "net_pnl_quote", "realized_pnl_quote", "unrealized_pnl_quote", "fees_quote"):
            raw_value = snapshot["summary"].get(key)
            if raw_value is None:
                continue
            try:
                value = Decimal(str(raw_value))
            except ArithmeticError as exc:
                raise ValueError("Status snapshot economics are invalid") from exc
            if not value.is_finite():
                raise ValueError("Status snapshot economics are invalid")
        return snapshot

    def _status_lane(self, snapshot: Optional[Dict[str, Any]]) -> Optional[str]:
        if snapshot is None:
            return None
        bot_name = snapshot.get("bot_name")
        return next((lane for lane, expected in self.STATUS_LANES.items() if bot_name == expected), None)

    @classmethod
    def _format_status_money(cls, value: Any) -> str:
        try:
            decimal_value = Decimal(str(value))
        except ArithmeticError:
            return "UNAVAILABLE"
        if not decimal_value.is_finite():
            return "UNAVAILABLE"
        return f"${cls._format_number(decimal_value, 2)}"

    @staticmethod
    def _status_text(value: Any, unavailable: str = "UNAVAILABLE") -> str:
        if value is None:
            return unavailable
        text = " ".join(str(value).split())
        return text[:200] if text else unavailable

    @classmethod
    def _format_status_number(cls, value: Any, decimals: int) -> str:
        try:
            decimal_value = Decimal(str(value))
        except ArithmeticError:
            return "UNAVAILABLE"
        if not decimal_value.is_finite():
            return "UNAVAILABLE"
        return cls._format_number(decimal_value, decimals)

    @classmethod
    def _format_status_percent(cls, value: Any) -> str:
        number = cls._format_status_number(value, 4)
        return number if number == "UNAVAILABLE" else f"{number}%"

    @classmethod
    def _format_status_fraction_percent(cls, value: Any) -> str:
        try:
            decimal_value = Decimal(str(value)) * Decimal("100")
        except ArithmeticError:
            return "UNAVAILABLE"
        if not decimal_value.is_finite():
            return "UNAVAILABLE"
        return f"{cls._format_number(decimal_value, 4)}%"

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

    async def _command_executors(self, args: List[str]) -> str:
        if len(args) > 1 or (args and args[0].lower() not in self.STATUS_LANES):
            return "Usage: /executors [main|sui]"
        scope = args[0].lower() if args else None
        configured = self._configured_status_snapshots()
        if configured is None:
            return await self._command_local_executors()

        if scope is not None:
            snapshot = configured[scope]
            if snapshot is None:
                return f"⚠️ {scope.upper()} executor status unavailable."
            lines = [self._title("⚙️", f"EXECUTORS • {scope.upper()}")]
            lines.extend(self._format_executor_snapshot(scope, snapshot))
            return "\n".join(lines)

        lines = [self._title("⚙️", "EXECUTORS • MAIN + SUI")]
        for lane in self.STATUS_LANES:
            snapshot = configured[lane]
            if snapshot is None:
                lines.append(f"⚠️ {lane.upper()}: UNAVAILABLE")
            else:
                lines.extend(self._format_executor_snapshot(lane, snapshot))
        lines.append("Use /executors main or /executors sui to isolate one owner.")
        return "\n".join(lines)

    def _format_executor_snapshot(self, lane: str, snapshot: Dict[str, Any]) -> List[str]:
        executors = snapshot.get("lifecycle_executors")
        if executors is None:
            executors = snapshot.get("active_executors", [])
        orders = snapshot.get("active_orders", [])
        lines = [
            self._kv(lane.upper(), snapshot["bot_name"], "🤖"),
            self._kv("Updated", snapshot["updated_at"], "🕒"),
            self._kv("In-flight Executors", len(executors), "⚙️"),
            self._kv("Active Orders", snapshot.get("active_orders_count", len(orders)), "📬"),
        ]
        if not executors and not orders:
            lines.append("No in-flight executors or orders.")
            return lines

        for row in executors:
            lines.extend(self._format_executor_lifecycle(row))
        for row in orders:
            lines.append(self._format_order_lifecycle(row))
        order_status = snapshot.get("active_orders_status")
        if isinstance(order_status, dict) and not order_status.get("complete", False):
            lines.append("⚠️ Active-order coverage is partial.")
        return lines

    def _format_executor_lifecycle(self, row: Dict[str, Any]) -> List[str]:
        raw_status = self._status_text(row.get("status"), "unknown").lower()
        status_key = raw_status.replace("-", "_")
        lifecycle = self.EXECUTOR_LIFECYCLE_LABELS.get(status_key, status_key.replace("_", " ").upper())
        close_type = self._status_text(row.get("close_type"), "").replace("_", " ").replace("-", " ").lower()
        if close_type:
            lifecycle = f"{lifecycle} • {close_type}"
        pair = self._status_text(row.get("pair"))
        side = self._status_text(row.get("side")).upper()
        executor_type = self._status_text(row.get("executor_type")).replace("_", " ")
        lines = [
            f"⚙️ {lifecycle} | {pair} {side} {executor_type} | age {self._status_text(row.get('age'))}",
            f"{self.BULLET} id {self._status_text(row.get('executor_id'))} | "
            f"controller {self._status_text(row.get('controller_id'))} | "
            f"venue {self._status_text(row.get('connector_name'))}",
            f"{self.BULLET} amount {self._format_status_number(row.get('amount_base'), 8)} | "
            f"remaining {self._format_status_number(row.get('remaining_position_amount_base'), 8)} | "
            f"fill {self._format_status_money(row.get('filled_amount_quote'))} | "
            f"PnL {self._format_status_money(row.get('net_pnl_quote'))} / "
            f"{self._format_status_percent(row.get('net_pnl_pct'))}",
        ]
        entry = row.get("entry_lifecycle")
        if isinstance(entry, dict):
            policy = self._status_text(entry.get("policy"), "unknown")
            state = self._status_text(entry.get("state"), "unknown").replace("_", " ")
            reason = self._status_text(entry.get("stop_reason"), "").replace("_", " ")
            detail = f"{self.BULLET} entry {policy}:{state}"
            if reason:
                detail += f" | reason {reason}"
            lines.append(detail)
        acquisition = row.get("entry_acquisition")
        if isinstance(acquisition, dict):
            policy = self._status_text(acquisition.get("policy"), "unknown")
            state = self._status_text(acquisition.get("state"), "unknown").replace("_", " ").replace("-", " ")
            reason = self._status_text(acquisition.get("terminal_reason"), "").replace("_", " ").replace("-", " ")
            acquisition_line = f"{self.BULLET} acquisition {policy}:{state}"
            if acquisition.get("reprice_count") is not None:
                acquisition_line += f" | reprices {self._format_status_number(acquisition['reprice_count'], 0)}"
            if reason:
                acquisition_line += f" | terminal {reason}"
            lines.append(acquisition_line)
            if any(acquisition.get(key) is not None for key in ("current_order_price", "maximum_acceptable_price")):
                lines.append(
                    f"{self.BULLET} entry price "
                    f"{self._format_status_number(acquisition.get('current_order_price'), 8)} / max "
                    f"{self._format_status_number(acquisition.get('maximum_acceptable_price'), 8)}"
                )
            if any(acquisition.get(key) is not None for key in
                   ("cumulative_filled_base", "authorized_base", "remaining_base")):
                lines.append(
                    f"{self.BULLET} filled "
                    f"{self._format_status_number(acquisition.get('cumulative_filled_base'), 8)} / authorized "
                    f"{self._format_status_number(acquisition.get('authorized_base'), 8)} | remaining "
                    f"{self._format_status_number(acquisition.get('remaining_base'), 8)}"
                )
            if any(acquisition.get(key) is not None for key in ("rest_seconds", "total_max_age_seconds")):
                lines.append(
                    f"{self.BULLET} timing rest "
                    f"{self._format_status_number(acquisition.get('rest_seconds'), 2)}s / max age "
                    f"{self._format_status_number(acquisition.get('total_max_age_seconds'), 2)}s | "
                    f"queue {self._status_text(acquisition.get('queue_fill_status'))} | "
                    f"fees {self._status_text(acquisition.get('fee_economics_status'))}"
                )
        trailing_state = self._status_text(row.get("trailing_state"), "")
        if trailing_state:
            trail = f"{self.BULLET} trail {trailing_state.replace('_', ' ')}"
            for label, key in (
                ("current", "trailing_current_price"),
                ("activation", "trailing_activation_price"),
                ("trigger", "trailing_trigger_price"),
            ):
                if row.get(key) is not None:
                    trail += f" | {label} {self._format_status_number(row[key], 8)}"
            gap = row.get("distance_to_trailing_trigger_pct")
            if gap is None:
                gap = row.get("distance_to_trailing_activation_pct")
            if gap is not None:
                trail += f" | gap {self._format_status_fraction_percent(gap)}"
            lines.append(trail)
        return lines

    def _format_order_lifecycle(self, row: Dict[str, Any]) -> str:
        raw_status = self._status_text(row.get("status"), "unknown").lower()
        lifecycle = self.ORDER_LIFECYCLE_LABELS.get(raw_status, raw_status.replace("_", " ").upper())
        return (
            f"📬 {lifecycle} | {self._status_text(row.get('pair'))} "
            f"{self._status_text(row.get('side')).upper()} | "
            f"price {self._format_status_number(row.get('price_quote'), 8)} | "
            f"amount {self._format_status_number(row.get('amount_base'), 8)} | "
            f"filled {self._format_status_number(row.get('filled_amount_base'), 8)} | "
            f"remaining {self._format_status_number(row.get('remaining_amount_base'), 8)} | "
            f"id {self._status_text(row.get('order_id'))} | "
            f"controller {self._status_text(row.get('controller_id'))}"
        )

    async def _command_local_executors(self) -> str:
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
            try:
                price = self._fill_decimal(fill, "price")
                amount = self._fill_decimal(fill, "amount")
                fee_quote = self._fill_decimal(fill, "trade_fee_in_quote")
                notional = price * amount
            except (ValueError, TypeError, ArithmeticError):
                lines.append(f"{self.BULLET} {fill.symbol} | receipt unavailable: invalid stored values")
                continue
            precision = "exact receipt" if isinstance(fill, TradeFill) and fill.has_exact_receipt else "legacy receipt (6 decimals)"
            fee_text = self._format_decimal(fee_quote) if fee_quote is not None else "UNAVAILABLE"
            lines.append(
                f"{self.BULLET} {self._format_db_timestamp(fill.timestamp)} | {fill.symbol} | {fill.trade_type} | "
                f"price {self._format_decimal(price)} | amount {self._format_decimal(amount)} | "
                f"notional ${self._format_number(notional, 2)} | fee {fee_text} {fill.quote_asset} | {precision}"
            )
        return "\n".join(lines)

    @staticmethod
    def _fill_decimal(fill: Any, field: str) -> Optional[Decimal]:
        if isinstance(fill, TradeFill):
            return getattr(fill, f"{field}_decimal")
        value = fill.get(field) if isinstance(fill, dict) else getattr(fill, field, None)
        if value is None:
            return None
        result = Decimal(str(value))
        if not result.is_finite():
            raise ValueError("Non-finite fill value")
        return result

    def _accounting_fill(self, fill: Any) -> Dict[str, Any]:
        def value(name, default=None):
            return fill.get(name, default) if isinstance(fill, dict) else getattr(fill, name, default)

        if isinstance(fill, TradeFill) and not fill.has_exact_receipt:
            raise ValueError("Legacy fill precision cannot establish exact inventory basis")
        amount = self._fill_decimal(fill, "amount")
        price = self._fill_decimal(fill, "price")
        fee_quote = self._fill_decimal(fill, "trade_fee_in_quote")
        if amount is None or price is None or fee_quote is None or amount <= 0 or price <= 0:
            raise ValueError("Incomplete fill economics")
        side = self._trade_fill_side(fill)
        if side not in {"BUY", "SELL"}:
            raise ValueError("Unknown fill side")
        base_asset = value("base_asset") or str(value("symbol", "")).split("-")[0]
        raw_fee = value("trade_fee")
        if isinstance(fill, TradeFill) and raw_fee is None:
            raise ValueError("Missing asset-denominated fee receipt")
        fee = raw_fee if raw_fee is not None else {}
        if not isinstance(fee, dict):
            raise ValueError("Invalid stored fill fee")
        base_fee = base_fee_amount({"trade_type": side, "order_fills": {
            "fill": {"fill_base_amount": str(amount), "fee": fee}}}, base_asset)
        inventory_amount = amount - base_fee if side == "BUY" else amount + base_fee
        if inventory_amount <= 0:
            raise ValueError("Invalid fee-adjusted fill quantity")
        return {"order_id": value("order_id"), "exchange_trade_id": value("exchange_trade_id"),
                "timestamp": self._accounting_fill_timestamp(value("timestamp")),
                "market": value("market"), "symbol": value("symbol"),
                "trade_type": side, "amount": inventory_amount,
                "quote_amount": price * amount, "trade_fee_in_quote": fee_quote - base_fee * price}

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

    def _fills_for_pair(
        self,
        trading_pair: str,
        *,
        market: Optional[str],
        end_timestamp: Optional[Decimal] = None,
    ) -> List[TradeFill]:
        if not market:
            return []
        recorder = self._markets_recorder()
        if recorder is None:
            return []
        config_file_path = getattr(recorder, "config_file_path", None)
        if not config_file_path:
            return []
        filters = [
            TradeFill.config_file_path == config_file_path,
            TradeFill.market == market,
            TradeFill.symbol == trading_pair,
        ]
        if end_timestamp is not None:
            filters.append(TradeFill.timestamp <= end_timestamp)
        with recorder.sql_manager.get_new_session() as session:
            return (
                session.query(TradeFill)
                .filter(*filters)
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

    def _pair_pnl_lines(self, pair_performance: Optional[Dict[str, Decimal]]) -> List[str]:
        if pair_performance is None:
            return []
        return [
            self._kv("Pair Realized PnL", f"${self._format_number(pair_performance['realized_pnl_quote'], 2)}", "📈"),
            self._kv("Pair Global PnL", f"${self._format_number(pair_performance['global_pnl_quote'], 2)}", "💹"),
        ]

    def _trade_realized_pnl_lines(self, realized_pnl_quote: Optional[Decimal]) -> List[str]:
        if realized_pnl_quote is None:
            return []
        return [
            self._kv("Trade Realized PnL", f"${self._format_number(realized_pnl_quote, 2)}", "📈"),
        ]

    @staticmethod
    def _normalized_fill_timestamp(timestamp: Any) -> Decimal:
        try:
            value = Decimal(str(timestamp or 0))
        except Exception:
            return Decimal("0")
        return value * Decimal("1000") if value and value < Decimal("100000000000") else value

    @classmethod
    def _accounting_fill_timestamp(cls, timestamp: Any) -> Decimal:
        value = Decimal(str(timestamp))
        if not value.is_finite() or value <= 0:
            raise ValueError("Invalid accounting timestamp")
        return cls._normalized_fill_timestamp(value)

    def _sell_fill_realized_pnl_quote(self, event: events.OrderFilledEvent) -> Optional[Decimal]:
        order = self._order_record(event.order_id)
        market = getattr(order, "market", None)
        if not market or not event.exchange_trade_id:
            return None
        try:
            event_timestamp = self._accounting_fill_timestamp(event.timestamp)
        except (ValueError, TypeError, ArithmeticError):
            return None
        fee_quote = self._fee_quote_from_trade_fee(
            event.trade_fee,
            event.trading_pair,
            event.price,
            event.amount,
        )
        current_fill = {
            "market": market,
            "symbol": event.trading_pair,
            "order_id": event.order_id,
            "exchange_trade_id": event.exchange_trade_id,
            "trade_type": "SELL",
            "price": event.price,
            "amount": event.amount,
            "base_asset": event.trading_pair.split("-")[0],
            "trade_fee": event.trade_fee.to_json(),
            "trade_fee_in_quote": fee_quote,
            "timestamp": event_timestamp,
        }
        return self._trade_realized_pnl_from_sell_fills(
            sell_fills=[current_fill],
            inventory_fills=self._fills_for_pair(event.trading_pair, market=market, end_timestamp=event_timestamp),
            current_order_id=None,
            current_trade_id=event.exchange_trade_id,
        )

    def _sell_order_realized_pnl_quote(self, order_id: str, trading_pair: str, *, market: Optional[str]) -> Optional[Decimal]:
        sell_fills = [
            fill for fill in self._fills_for_order(order_id)
            if self._trade_fill_side(fill) == "SELL"
        ]
        if not sell_fills or not market:
            return None
        try:
            end_timestamp = max(self._accounting_fill_timestamp(getattr(fill, "timestamp", None)) for fill in sell_fills)
        except (ValueError, TypeError, ArithmeticError):
            return None
        return self._trade_realized_pnl_from_sell_fills(
            sell_fills=sell_fills,
            inventory_fills=self._fills_for_pair(trading_pair, market=market, end_timestamp=end_timestamp),
            current_order_id=order_id,
        )

    @staticmethod
    def _trade_fill_side(fill: Any) -> str:
        value = fill.get("trade_type") if isinstance(fill, dict) else getattr(fill, "trade_type", None)
        if hasattr(value, "name"):
            return str(value.name).upper()
        return str(value or "").upper()

    def _trade_realized_pnl_from_sell_fills(
        self,
        *,
        sell_fills: List[Any],
        inventory_fills: List[Any],
        current_order_id: Optional[str] = None,
        current_trade_id: Optional[str] = None,
    ) -> Optional[Decimal]:
        lots: List[Dict[str, Decimal]] = []
        try:
            sell_fills = [self._accounting_fill(fill) for fill in sell_fills]
            inventory_fills = [self._accounting_fill(fill) for fill in inventory_fills]
            if any(fill["trade_type"] != "SELL" for fill in sell_fills):
                return None
        except (ValueError, TypeError, KeyError, ArithmeticError):
            return None

        def consume(amount: Decimal) -> Optional[Decimal]:
            remaining = amount
            cost = Decimal("0")
            while remaining > 0 and lots:
                lot = lots[0]
                matched = min(remaining, lot["amount"])
                matched_cost = lot["cost_quote"] if matched == lot["amount"] else lot["cost_quote"] * matched / lot["amount"]
                cost += matched_cost
                lot["cost_quote"] -= matched_cost
                lot["amount"] -= matched
                remaining -= matched
                if lot["amount"] <= 0:
                    lots.pop(0)
            if remaining > 0:
                return None
            return cost

        events = []
        keyed_events = {}
        for fill, is_target in [(fill, True) for fill in sell_fills] + [(fill, False) for fill in inventory_fills]:
            key = (fill["market"], fill["order_id"], fill["exchange_trade_id"])
            if all(key):
                previous = keyed_events.get(key)
                if previous is not None:
                    if previous[0] != fill:
                        return None
                    continue
                keyed_events[key] = (fill, is_target)
            elif not is_target and current_order_id is not None and fill["order_id"] == current_order_id:
                # Compatibility for in-memory callers without a persisted composite key.
                continue
            events.append((fill, is_target))
        scopes = {(fill["market"], fill["symbol"]) for fill, _ in events if fill["market"] and fill["symbol"]}
        if len(scopes) > 1:
            return None
        timestamp_owners = {}
        for fill, is_target in events:
            owners = timestamp_owners.setdefault(fill["timestamp"], set())
            owners.add((fill["trade_type"], fill["order_id"], is_target))
            if len(owners) > 1:
                # The event clock cannot order different orders or assign tied partials to a target.
                return None
        realized_pnl = Decimal("0")
        matched_any = False
        # Target partial fills must consume basis at their own event time, alongside other orders.
        for fill, is_target in sorted(events, key=lambda item: item[0]["timestamp"]):
            amount = fill["amount"]
            quote_amount = fill["quote_amount"]
            fee_quote = fill["trade_fee_in_quote"]
            if fill["trade_type"] == "BUY":
                lots.append({"amount": amount, "cost_quote": quote_amount + fee_quote})
                continue
            cost = consume(amount)
            if cost is None:
                return None
            if is_target:
                realized_pnl += quote_amount - cost - fee_quote
                matched_any = True

        return realized_pnl if matched_any else None

    def _order_completion_context(
        self,
        order_id: str,
        base_asset: str,
        quote_asset: str,
        *,
        include_trade_realized_pnl: bool = False,
    ) -> Dict[str, Any]:
        trading_pair = f"{base_asset}-{quote_asset}"
        order = self._order_record(order_id)
        if order is not None and getattr(order, "symbol", None):
            trading_pair = str(order.symbol)
        fills = self._fills_for_order(order_id)
        try:
            fees = [self._fill_decimal(fill, "trade_fee_in_quote") for fill in fills]
            fee_quote = sum(fees, Decimal("0")) if fees and all(fee is not None for fee in fees) else None
        except (ValueError, TypeError, ArithmeticError):
            fee_quote = None
        context = {
            "trading_pair": trading_pair,
            "fee_quote": fee_quote,
        }
        if include_trade_realized_pnl:
            context["trade_realized_pnl_quote"] = self._sell_order_realized_pnl_quote(
                order_id,
                trading_pair,
                market=getattr(order, "market", None),
            )
        return context

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
    def _fee_quote_from_trade_fee(
        cls,
        trade_fee: TradeFeeBase,
        trading_pair: str,
        price: Decimal,
        amount: Decimal,
    ) -> Optional[Decimal]:
        try:
            _, quote_asset = split_hb_trading_pair(trading_pair)
            fee = Decimal(str(trade_fee.fee_amount_in_token(
                trading_pair=trading_pair,
                price=price,
                order_amount=amount,
                token=quote_asset,
            )))
            return fee if fee.is_finite() else None
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
            if method in {'getUpdates', 'setMyCommands'}:
                require_polling_admission()
            with request.urlopen(req, timeout=self.REQUEST_TIMEOUT + self.LONG_POLL_TIMEOUT) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            if method == 'getUpdates' and e.code == 409:
                e.close()
                raise TelegramPollingConflict('Telegram updates already have a consumer') from None
            body = e.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Telegram API {method} failed: {e.code} {body}")
        except URLError as e:
            raise RuntimeError(f"Telegram API {method} failed: {e}")

        if not payload.get("ok"):
            if method == "getUpdates" and payload.get("error_code") == 409:
                raise TelegramPollingConflict("Telegram updates already have a consumer")
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
