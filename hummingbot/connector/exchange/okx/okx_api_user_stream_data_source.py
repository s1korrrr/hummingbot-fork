import asyncio
from typing import TYPE_CHECKING, Any, Dict, Optional

from hummingbot.connector.exchange.okx import okx_constants as CONSTANTS
from hummingbot.connector.exchange.okx.okx_auth import OkxAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSPlainTextRequest, WSResponse
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange


class OkxAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            auth: OkxAuth,
            connector: 'OkxExchange',
            api_factory: WebAssistantsFactory):
        super().__init__()
        self._auth: OkxAuth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._private_channels_ready = False
        self._output_queue: Optional[asyncio.Queue] = None

    @property
    def last_recv_time(self) -> float:
        # A login reply, pong, or socket heartbeat does not prove that either
        # private channel is subscribed. Exchange health must wait for both ACKs.
        if self._private_channels_ready:
            return super().last_recv_time
        return 0

    def _reset_subscription_state(self):
        self._private_channels_ready = False

    @staticmethod
    def _is_control_message(message: Any) -> bool:
        # WSConnection decodes OKX's text heartbeat response as the plain string "pong".
        return message in ("pong", "")

    @staticmethod
    def _is_success_code(code: Any) -> bool:
        return (type(code) is str and code == "0") or (type(code) is int and code == 0)

    @staticmethod
    def _safe_error_code(code: Any) -> Optional[str]:
        if type(code) is str and len(code) <= 8 and code.isascii() and code.isdigit():
            return code
        if type(code) is int and 0 <= code <= 99_999_999:
            return str(code)
        return None

    @staticmethod
    def _subscription_failure(message: Dict[str, Any]) -> Optional[str]:
        event = message.get("event")
        if event == "error":
            safe_code = OkxAPIUserStreamDataSource._safe_error_code(message.get("code"))
            if safe_code:
                return f"OKX private websocket error (code {safe_code})"
            return "OKX private websocket error"
        if event == "channel-conn-count-error":
            return "OKX private websocket channel connection limit reached"
        code = message.get("code")
        if event == "subscribe" and code is not None and not OkxAPIUserStreamDataSource._is_success_code(code):
            safe_code = OkxAPIUserStreamDataSource._safe_error_code(code)
            if safe_code:
                return f"OKX private websocket subscription rejected (code {safe_code})"
            return "OKX private websocket subscription rejected"
        return None

    @staticmethod
    def _acknowledged_private_channel(message: Dict[str, Any]) -> Optional[str]:
        if message.get("event") != "subscribe":
            return None
        argument = message.get("arg")
        if argument == {"channel": "account"}:
            return "account"
        if argument == {"channel": "orders", "instType": "SPOT"}:
            return "orders"
        return None

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """

        self._reset_subscription_state()
        ws: WSAssistant = await self._get_ws_assistant()
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECTION_LIMIT_ID):
            await ws.connect(
                ws_url=CONSTANTS.get_okx_ws_uri_private(self._connector.okx_registration_sub_domain),
                message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE)

        payload = {
            "op": "login",
            "args": [self._auth.websocket_login_parameters()]
        }

        login_request: WSJSONRequest = WSJSONRequest(payload=payload)

        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_LOGIN_LIMIT_ID):
            await ws.send(login_request)

        response: WSResponse = await asyncio.wait_for(
            ws.receive(), timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE
        )
        message = response.data
        if (not isinstance(message, dict) or message.get("event") != "login"
                or not self._is_success_code(message.get("code"))):
            self.logger().error("Error authenticating the private websocket connection")
            self._reset_subscription_state()
            raise IOError("Private websocket connection authentication failed")

        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        self._reset_subscription_state()
        try:
            payload = {
                "op": "subscribe",
                "args": [{"channel": "account"}],
            }
            subscribe_account_request: WSJSONRequest = WSJSONRequest(payload=payload)

            payload = {
                "op": "subscribe",
                "args": [
                    {
                        "channel": "orders",
                        "instType": "SPOT",
                    }
                ]
            }
            subscribe_orders_request: WSJSONRequest = WSJSONRequest(payload=payload)

            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
                await websocket_assistant.send(subscribe_account_request)
            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIPTION_LIMIT_ID):
                await websocket_assistant.send(subscribe_orders_request)

            pending_channels = {"account", "orders"}
            loop = asyncio.get_running_loop()
            deadline = loop.time() + CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE
            while pending_channels:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise asyncio.TimeoutError("Timed out waiting for OKX private channel acknowledgements")
                response = await asyncio.wait_for(websocket_assistant.receive(), timeout=remaining)
                message = response.data
                if self._is_control_message(message):
                    continue
                if not isinstance(message, dict):
                    raise IOError("Invalid OKX private websocket response during subscription")
                failure = self._subscription_failure(message)
                if failure:
                    raise IOError(failure)
                if message.get("event") == "subscribe":
                    channel = self._acknowledged_private_channel(message)
                    if not channel:
                        raise IOError("Unexpected OKX private websocket subscription acknowledgement")
                    pending_channels.discard(channel)
                elif "data" in message:
                    # Deliver fills immediately even while the other channel is
                    # still awaiting its ACK; readiness remains closed meanwhile.
                    if self._output_queue is None:
                        raise IOError("OKX private websocket received data without an active listener queue")
                    await self._process_event_message(message, self._output_queue)

            self._private_channels_ready = True
            self.logger().info("Subscribed to private account and orders channels...")
        except asyncio.CancelledError:
            self._reset_subscription_state()
            raise
        except Exception:
            self._reset_subscription_state()
            self.logger().exception("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        if not self._private_channels_ready:
            raise IOError("OKX private websocket is not subscribed to both required channels")
        while True:
            try:
                await super()._process_websocket_messages(
                    websocket_assistant=websocket_assistant,
                    queue=queue)
            except asyncio.TimeoutError:
                ping_request = WSPlainTextRequest(payload="ping")
                await websocket_assistant.send(request=ping_request)

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        if self._is_control_message(event_message):
            return
        if not isinstance(event_message, dict):
            self._reset_subscription_state()
            raise IOError("Invalid OKX private websocket event")
        failure = self._subscription_failure(event_message)
        if failure:
            self._reset_subscription_state()
            raise IOError(failure)
        if event_message.get("event") == "subscribe":
            if not self._acknowledged_private_channel(event_message):
                self._reset_subscription_state()
                raise IOError("Unexpected OKX private websocket subscription acknowledgement")
        if len(event_message) > 0 and "data" in event_message:
            queue.put_nowait(event_message)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        self._reset_subscription_state()
        await super()._on_user_stream_interruption(websocket_assistant)

    async def listen_for_user_stream(self, output: asyncio.Queue):
        if self._output_queue is not None:
            raise RuntimeError("OKX private websocket listener is already active")
        self._output_queue = output
        try:
            await super().listen_for_user_stream(output)
        finally:
            self._output_queue = None

    async def stop(self):
        self._reset_subscription_state()
        await super().stop()
        self._reset_subscription_state()

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant
