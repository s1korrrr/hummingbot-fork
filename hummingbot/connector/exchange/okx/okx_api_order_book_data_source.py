import asyncio
import math
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.okx import okx_constants as CONSTANTS, okx_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange


class OkxAPIOrderBookDataSource(OrderBookTrackerDataSource):

    requires_order_book_stream_validation = True
    _BOOK_RECOVERY_TIMEOUT = 10.0
    _logger: Optional[HummingbotLogger] = None
    _DYNAMIC_SUBSCRIBE_ID_START = 100
    _next_subscribe_id: int = _DYNAMIC_SUBSCRIBE_ID_START

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'OkxExchange',
                 api_factory: WebAssistantsFactory):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        # Start invalid. REST initializes levels, but only a WS snapshot establishes continuity.
        self._stream_epochs: Dict[str, int] = {}
        self._stream_sequences: Dict[str, int] = {}
        self._stream_symbols: Dict[str, str] = {}
        self._ingress_epochs: Dict[str, int] = {}
        self._valid_streams = set()
        self._recovering_streams = set()
        self._book_snapshot_deadlines: Dict[str, asyncio.Task] = {}
        self._connection_generation = 0
        self._connection_invalidated = False

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_data: Dict[str, Any] = snapshot_response['data'][0]
        snapshot_timestamp: float = int(snapshot_data["ts"]) * 1e-3
        update_id: int = int(snapshot_data["ts"])

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [(bid[0], bid[1]) for bid in snapshot_data["bids"]],
            "asks": [(ask[0], ask[1]) for ask in snapshot_data["asks"]],
        }
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            snapshot_timestamp)

        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "instId": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "sz": "400"
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(
                path_url=CONSTANTS.OKX_ORDER_BOOK_PATH,
                domain=self._connector.domain
            ),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.OKX_ORDER_BOOK_PATH,
        )

        return data

    def order_book_stream_valid(self, trading_pair: str, epoch: Optional[int]) -> bool:
        """Validity belongs to the epoch actually applied by the tracker, not parser progress."""
        return (type(epoch) is int and trading_pair in self._valid_streams
                and epoch == self._stream_epochs.get(trading_pair, 0))

    def _invalidate_book_stream(self, trading_pair: str):
        self._stream_epochs[trading_pair] = self._stream_epochs.get(trading_pair, 0) + 1
        self._valid_streams.discard(trading_pair)
        self._stream_sequences.pop(trading_pair, None)
        symbol = self._stream_symbols.get(trading_pair)
        if symbol is not None:
            self._ingress_epochs[symbol] = self._stream_epochs[trading_pair]

    async def request_order_book_resync(self, trading_pair: str):
        """The tracker could not apply a complete baseline for this stream."""
        await self._recover_book_stream(trading_pair)

    def _cancel_snapshot_deadline(self, trading_pair: str):
        task = self._book_snapshot_deadlines.pop(trading_pair, None)
        if task is not None:
            task.cancel()

    def _require_snapshot_before_deadline(self, trading_pair: str, ws: WSAssistant):
        if self._ws_assistant is not ws or self._connection_invalidated:
            return  # A completed send from an old connection cannot replace its successor's timer.
        self._cancel_snapshot_deadline(trading_pair)
        if trading_pair in self._valid_streams:
            return
        epoch = self._stream_epochs.get(trading_pair, 0)
        generation = self._connection_generation

        async def deadline():
            try:
                await asyncio.sleep(self._BOOK_RECOVERY_TIMEOUT)
                if (self._ws_assistant is not ws or generation != self._connection_generation
                        or epoch != self._stream_epochs.get(trading_pair, 0)
                        or trading_pair in self._valid_streams):
                    return
                # Do not cancel this task when invalidating the other pair timers.
                self._book_snapshot_deadlines.pop(trading_pair, None)
                self._invalidate_connection()
                self.logger().warning(f"OKX snapshot deadline expired for {trading_pair}; reconnect required")
                await ws.disconnect()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(f"OKX snapshot deadline recovery failed for {trading_pair}")
            finally:
                if self._book_snapshot_deadlines.get(trading_pair) is asyncio.current_task():
                    self._book_snapshot_deadlines.pop(trading_pair, None)

        self._book_snapshot_deadlines[trading_pair] = asyncio.create_task(deadline())

    async def _recover_book_stream(self, trading_pair: str):
        if trading_pair in self._recovering_streams:
            return
        self._recovering_streams.add(trading_pair)
        self._invalidate_book_stream(trading_pair)
        self.logger().warning(f"OKX order book continuity lost for {trading_pair}; awaiting recovery snapshot")
        ws = self._ws_assistant
        if ws is None:
            return
        generation = self._connection_generation
        epoch = self._stream_epochs[trading_pair]

        async def resubscribe():
            symbol = self._stream_symbols[trading_pair]
            for operation in ("unsubscribe", "subscribe"):
                request = WSJSONRequest(payload={
                    "op": operation, "args": [{"channel": "books", "instId": symbol}]})
                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
                    await ws.send(request)

        try:
            # One bounded attempt per invalid epoch; subsequent diffs cannot cause a request storm.
            await asyncio.wait_for(resubscribe(), timeout=self._BOOK_RECOVERY_TIMEOUT)
            if generation == self._connection_generation and epoch == self._stream_epochs[trading_pair]:
                self._require_snapshot_before_deadline(trading_pair, ws)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"OKX order book recovery failed for {trading_pair}; reconnect required")
            if self._ws_assistant is ws and generation == self._connection_generation:
                self._invalidate_connection()
                await ws.disconnect()

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        await self._parse_book_message(raw_message, message_queue, snapshot=True)

    async def _parse_book_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue, *, snapshot: bool):
        symbol = raw_message["arg"]["instId"]
        generation = raw_message.get("_connection_generation", self._connection_generation)
        ingress_epoch = raw_message.get("_stream_epoch", self._ingress_epochs.get(symbol, 0))
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
        self._stream_symbols[trading_pair] = symbol
        if (self._connection_invalidated or generation != self._connection_generation
                or ingress_epoch != self._stream_epochs.get(trading_pair, 0)):
            return  # Raw messages queued before a gap/disconnect cannot recover the new epoch.
        try:
            if raw_message.get("action") not in ("snapshot", "update"):
                raise ValueError("Invalid OKX book action")
            updates = raw_message["data"]
            if not updates or (snapshot and len(updates) != 1):
                raise ValueError("Invalid OKX book payload")
            for data in updates:
                sequence, previous = data["seqId"], data["prevSeqId"]
                if type(sequence) is not int or type(previous) is not int or sequence < 0:
                    raise ValueError("Invalid OKX book sequence")
                bids = [(bid[0], bid[1]) for bid in data["bids"]]
                asks = [(ask[0], ask[1]) for ask in data["asks"]]
                update_id = int(data["ts"])
                if snapshot:
                    if previous != -1:
                        raise ValueError("Invalid OKX snapshot sequence")
                else:
                    if trading_pair not in self._valid_streams:
                        await self._recover_book_stream(trading_pair)
                        return
                    if previous != self._stream_sequences[trading_pair]:
                        raise ValueError("OKX book sequence gap")
                    # Maintenance can reset seqId below prevSeqId. An unchanged sequence is
                    # valid only for OKX's empty keepalive (both sides empty).
                    if sequence == previous and (bids or asks):
                        raise ValueError("Nonempty OKX book heartbeat")
                message = OrderBookMessage(
                    OrderBookMessageType.SNAPSHOT if snapshot else OrderBookMessageType.DIFF,
                    {"trading_pair": trading_pair, "update_id": update_id,
                     "bids": bids, "asks": asks,
                     "stream_epoch": self._stream_epochs.get(trading_pair, 0)},
                    update_id * 1e-3)
                # Validate level conversion before advancing continuity or publishing validity.
                levels = [*message.bids, *message.asks]
                if any(not math.isfinite(row.price) or row.price <= 0
                       or not math.isfinite(row.amount) or row.amount < 0 for row in levels):
                    raise ValueError('Invalid OKX book price or quantity')
                self._stream_sequences[trading_pair] = sequence
                if snapshot:
                    self._connection_invalidated = False
                    self._valid_streams.add(trading_pair)
                    self._recovering_streams.discard(trading_pair)
                    self._cancel_snapshot_deadline(trading_pair)
                message_queue.put_nowait(message)
        except (KeyError, TypeError, ValueError, IndexError, OverflowError):
            await self._recover_book_stream(trading_pair)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trade_updates = raw_message["data"]

        for trade_data in trade_updates:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=trade_data["instId"])
            message_content = {
                "trade_id": trade_data["tradeId"],
                "trading_pair": trading_pair,
                "trade_type": float(TradeType.BUY.value) if trade_data["side"] == "buy" else float(
                    TradeType.SELL.value),
                "amount": trade_data["sz"],
                "price": trade_data["px"]
            }
            trade_message: Optional[OrderBookMessage] = OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content=message_content,
                timestamp=(int(trade_data["ts"]) * 1e-3))

            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # All WS books messages share one queue, preserving snapshot/diff sequence order.
        await self._parse_book_message(raw_message, message_queue, snapshot=raw_message.get("action") == "snapshot")

    def _invalidate_connection(self):
        for trading_pair in tuple(self._book_snapshot_deadlines):
            self._cancel_snapshot_deadline(trading_pair)
        if self._connection_invalidated:
            return
        self._connection_invalidated = True
        self._connection_generation += 1
        for trading_pair in set(self._trading_pairs) | set(self._stream_epochs) | self._valid_streams.copy():
            self._invalidate_book_stream(trading_pair)
        self._recovering_streams.clear()

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        self._invalidate_connection()
        await super()._on_order_stream_interruption(websocket_assistant)

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                self._stream_symbols[trading_pair] = symbol
                self._ingress_epochs[symbol] = self._stream_epochs.get(trading_pair, 0)

                payload = {
                    "op": "subscribe",
                    "args": [
                        {
                            "channel": "trades",
                            "instId": symbol,
                        }
                    ]
                }
                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=payload)

                payload = {
                    "op": "subscribe",
                    "args": [
                        {
                            "channel": "books",
                            "instId": symbol,
                        }]
                }
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=payload)

                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
                    await ws.send(subscribe_trade_request)
                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
                    await ws.send(subscribe_orderbook_request)
                self._require_snapshot_before_deadline(trading_pair, ws)

            self.logger().info("Sent public order book and trade subscription requests; awaiting snapshots.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "data" in event_message:
            event_channel = event_message["arg"]["channel"]
            if event_channel == CONSTANTS.OKX_WS_PUBLIC_TRADES_CHANNEL:
                channel = self._trade_messages_queue_key
            elif event_channel == CONSTANTS.OKX_WS_PUBLIC_BOOKS_CHANNEL:
                event_message["_connection_generation"] = self._connection_generation
                event_message["_stream_epoch"] = self._ingress_epochs.get(event_message["arg"]["instId"], 0)
                channel = self._diff_messages_queue_key

        return channel

    async def _process_message_for_unknown_channel(
        self, event_message: Dict[str, Any], websocket_assistant: WSAssistant
    ):
        if isinstance(event_message, Mapping) and event_message.get("event") == "error":
            # Control errors may omit arg. Exit through native invalidation and reconnect;
            # never include the server's echoed request or raw payload in the exception.
            raise ConnectionError("OKX public subscription rejected")
        await super()._process_message_for_unknown_channel(event_message, websocket_assistant)

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        try:
            while True:
                try:
                    await super()._process_websocket_messages(websocket_assistant=websocket_assistant)
                    return
                except asyncio.TimeoutError:
                    ping_request = WSPlainTextRequest(payload="ping")
                    await websocket_assistant.send(request=ping_request)
        finally:
            # Invalidate before the base subscription loop's retry delay, including cancellation.
            self._invalidate_connection()

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECTION_LIMIT_ID):
            await ws.connect(
                ws_url=CONSTANTS.get_okx_ws_uri_public(sub_domain=self._connector.okx_registration_sub_domain),
                message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE)
        self._connection_invalidated = False
        return ws

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        """
        Subscribes to order book and trade channels for a single trading pair
        on the existing WebSocket connection.

        :param trading_pair: the trading pair to subscribe to
        :return: True if subscription requests were sent, False otherwise.
                 Readiness still requires a fresh WebSocket snapshot.
        """
        if self._ws_assistant is None:
            self.logger().warning(
                f"Cannot subscribe to {trading_pair}: WebSocket not connected"
            )
            return False

        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            self._stream_symbols[trading_pair] = symbol
            self._ingress_epochs[symbol] = self._stream_epochs.get(trading_pair, 0)

            trade_payload = {
                "op": "subscribe",
                "args": [{"channel": "trades", "instId": symbol}]
            }
            subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trade_payload)

            orderbook_payload = {
                "op": "subscribe",
                "args": [{"channel": "books", "instId": symbol}]
            }
            subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=orderbook_payload)

            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
                await self._ws_assistant.send(subscribe_trade_request)
            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
                await self._ws_assistant.send(subscribe_orderbook_request)

            self.add_trading_pair(trading_pair)
            self._require_snapshot_before_deadline(trading_pair, self._ws_assistant)
            self.logger().info(f"Sent {trading_pair} order book and trade subscription requests; awaiting snapshot.")
            return True

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Error subscribing to {trading_pair}")
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        """
        Unsubscribes from order book and trade channels for a single trading pair
        on the existing WebSocket connection.

        :param trading_pair: the trading pair to unsubscribe from
        :return: True if unsubscription was successful, False otherwise
        """
        if self._ws_assistant is None:
            self.logger().warning(
                f"Cannot unsubscribe from {trading_pair}: WebSocket not connected"
            )
            return False

        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

            unsubscribe_payload = {
                "op": "unsubscribe",
                "args": [
                    {"channel": "trades", "instId": symbol},
                    {"channel": "books", "instId": symbol}
                ]
            }
            unsubscribe_request: WSJSONRequest = WSJSONRequest(payload=unsubscribe_payload)

            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
                await self._ws_assistant.send(unsubscribe_request)

            self._invalidate_book_stream(trading_pair)
            self._recovering_streams.discard(trading_pair)
            self._cancel_snapshot_deadline(trading_pair)
            self.remove_trading_pair(trading_pair)
            self.logger().info(f"Unsubscribed from {trading_pair} order book and trade channels")
            return True

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Error unsubscribing from {trading_pair}")
            return False

    @classmethod
    def _get_next_subscribe_id(cls) -> int:
        """Returns the next subscription ID and increments the counter."""
        current_id = cls._next_subscribe_id
        cls._next_subscribe_id += 1
        return current_id
