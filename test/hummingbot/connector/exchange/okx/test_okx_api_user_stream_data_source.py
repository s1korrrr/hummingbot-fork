import asyncio
import time
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.okx.okx_api_user_stream_data_source import OkxAPIUserStreamDataSource
from hummingbot.core.web_assistant.connections.data_types import WSResponse


class OkxPrivateSubscriptionAcknowledgementTests(IsolatedAsyncioTestCase):
    def setUp(self):
        factory = MagicMock()
        factory.throttler.execute_task.return_value = AsyncMock()
        self.data_source = OkxAPIUserStreamDataSource(
            auth=MagicMock(), connector=MagicMock(), api_factory=factory
        )
        self.websocket = AsyncMock()

    async def test_both_exact_private_channel_acknowledgements_are_required(self):
        self.websocket.receive.side_effect = [
            WSResponse({"event": "subscribe", "arg": {"channel": "account"}}),
            WSResponse({"event": "subscribe", "arg": {"channel": "orders", "instType": "SPOT"}}),
        ]

        await self.data_source._subscribe_channels(self.websocket)

        self.assertEqual(2, self.websocket.receive.await_count)
        self.assertTrue(self.data_source._private_channels_ready)

    async def test_reverse_ack_order_and_interleaved_private_data_are_preserved(self):
        account_update = {"arg": {"channel": "account"}, "data": [{"seqId": 1}]}
        orders_update = {"arg": {"channel": "orders", "instType": "SPOT"}, "data": [{"ordId": "x"}]}
        self.websocket.receive.side_effect = [
            WSResponse(account_update),
            WSResponse({"event": "subscribe", "arg": {"channel": "orders", "instType": "SPOT"}}),
            WSResponse(orders_update),
            WSResponse({"event": "subscribe", "arg": {"channel": "account"}}),
        ]
        queue = asyncio.Queue()
        self.data_source._output_queue = queue
        try:
            await self.data_source._subscribe_channels(self.websocket)
        finally:
            self.data_source._output_queue = None
        self.assertEqual([account_update, orders_update], [queue.get_nowait(), queue.get_nowait()])

    async def test_handshake_uses_one_aggregate_receive_deadline(self):
        calls = 0

        async def receive():
            nonlocal calls
            calls += 1
            if calls == 1:
                await asyncio.sleep(0.14)
                return WSResponse({"event": "subscribe", "arg": {"channel": "account"}})
            await asyncio.sleep(1)

        self.websocket.receive.side_effect = receive
        started = time.monotonic()
        with patch("hummingbot.connector.exchange.okx.okx_api_user_stream_data_source.CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE", 0.2):
            with self.assertRaises(asyncio.TimeoutError):
                await self.data_source._subscribe_channels(self.websocket)
        elapsed = time.monotonic() - started

        self.assertGreaterEqual(self.websocket.receive.await_count, 2)
        self.assertLess(elapsed, 0.32)
        self.assertFalse(self.data_source._private_channels_ready)

    async def test_login_requires_success_code_zero(self):
        ws = AsyncMock()
        self.data_source._connector.okx_registration_sub_domain = "www"
        self.data_source._get_ws_assistant = AsyncMock(return_value=ws)
        ws.receive.return_value = WSResponse({"event": "login", "code": "60009", "msg": "Login failed"})

        with self.assertRaises(IOError):
            await self.data_source._connected_websocket_assistant()

        self.assertFalse(self.data_source._private_channels_ready)

    async def test_text_pong_and_empty_control_do_not_acknowledge_channels(self):
        self.websocket.receive.side_effect = [
            WSResponse("pong"),
            WSResponse(""),
            WSResponse({"event": "subscribe", "arg": {"channel": "account"}}),
            WSResponse({"event": "subscribe", "arg": {"channel": "orders", "instType": "SPOT"}}),
        ]

        await self.data_source._subscribe_channels(self.websocket)

        self.assertTrue(self.data_source._private_channels_ready)
        self.assertEqual(4, self.websocket.receive.await_count)

    async def test_text_pong_without_channel_ack_still_times_out(self):
        self.websocket.receive.side_effect = [WSResponse("pong"), asyncio.TimeoutError("No ACK")]

        with self.assertRaises(asyncio.TimeoutError):
            await self.data_source._subscribe_channels(self.websocket)

        self.assertFalse(self.data_source._private_channels_ready)

    async def test_post_handshake_text_pong_keeps_readiness(self):
        self.data_source._private_channels_ready = True

        await self.data_source._process_event_message("pong", asyncio.Queue())

        self.assertTrue(self.data_source._private_channels_ready)

    async def test_channel_count_error_after_ack_revokes_readiness(self):
        self.data_source._private_channels_ready = True
        error = {"event": "channel-conn-count-error", "channel": "orders", "connCount": 3}

        with self.assertRaises(IOError):
            await self.data_source._process_event_message(error, asyncio.Queue())

        self.assertFalse(self.data_source._private_channels_ready)

    async def test_handshake_cancellation_resets_readiness(self):
        entered_receive = asyncio.Event()

        async def blocked_receive():
            entered_receive.set()
            await asyncio.Future()

        self.websocket.receive.side_effect = blocked_receive
        task = asyncio.create_task(self.data_source._subscribe_channels(self.websocket))
        await entered_receive.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertFalse(self.data_source._private_channels_ready)

    async def test_interruption_resets_readiness_for_next_connection(self):
        self.data_source._private_channels_ready = True
        ws = AsyncMock()

        await self.data_source._on_user_stream_interruption(ws)

        self.assertFalse(self.data_source._private_channels_ready)

    async def test_stop_resets_readiness(self):
        self.data_source._private_channels_ready = True
        self.data_source._ws_assistant = self.websocket

        await self.data_source.stop()

        self.assertFalse(self.data_source._private_channels_ready)
        self.websocket.disconnect.assert_awaited_once()

    def test_login_or_transport_activity_alone_does_not_mark_private_stream_ready(self):
        self.websocket.last_recv_time = 123.0
        self.data_source._ws_assistant = self.websocket

        self.assertEqual(0, self.data_source.last_recv_time)

    async def test_orders_subscription_error_fails_instead_of_reporting_success(self):
        self.websocket.receive.side_effect = [
            WSResponse({"event": "subscribe", "arg": {"channel": "account"}}),
            WSResponse({"event": "error", "code": "60012", "msg": "Invalid request",
                        "arg": {"channel": "orders", "instType": "SPOT"}}),
        ]

        with self.assertRaises(IOError):
            await self.data_source._subscribe_channels(self.websocket)

    async def test_wrong_instrument_type_does_not_acknowledge_spot_orders(self):
        self.websocket.receive.side_effect = [
            WSResponse({"event": "subscribe", "arg": {"channel": "account"}}),
            WSResponse({"event": "subscribe", "arg": {"channel": "orders", "instType": "SWAP"}}),
        ]

        with self.assertRaises(IOError):
            await self.data_source._subscribe_channels(self.websocket)

    async def test_missing_orders_acknowledgement_propagates_receive_timeout(self):
        self.websocket.receive.side_effect = [
            WSResponse({"event": "subscribe", "arg": {"channel": "account"}}),
            asyncio.TimeoutError("No orders acknowledgement"),
        ]

        with self.assertRaises(asyncio.TimeoutError):
            await self.data_source._subscribe_channels(self.websocket)

    async def test_post_subscription_error_is_not_discarded(self):
        queue = asyncio.Queue()
        error = {"event": "error", "code": "60012", "msg": "Invalid request",
                 "arg": {"channel": "orders", "instType": "SPOT"}}

        with self.assertRaises(IOError):
            await self.data_source._process_event_message(error, queue)

        self.assertTrue(queue.empty())

    async def test_nonzero_subscription_ack_is_rejected(self):
        self.websocket.receive.side_effect = [
            WSResponse({"event": "subscribe", "code": "60012", "msg": "Invalid request",
                        "arg": {"channel": "account"}}),
        ]

        with self.assertRaises(IOError):
            await self.data_source._subscribe_channels(self.websocket)

        self.assertFalse(self.data_source._private_channels_ready)

    async def test_channel_count_error_during_handshake_is_rejected(self):
        self.websocket.receive.side_effect = [
            WSResponse({"event": "channel-conn-count-error", "channel": "orders", "connCount": 3}),
        ]

        with self.assertRaises(IOError):
            await self.data_source._subscribe_channels(self.websocket)

        self.assertFalse(self.data_source._private_channels_ready)

    async def test_account_ack_with_unexpected_arguments_is_rejected(self):
        self.websocket.receive.side_effect = [
            WSResponse({"event": "subscribe", "arg": {"channel": "account", "ccy": "USDC"}}),
        ]

        with self.assertRaises(IOError):
            await self.data_source._subscribe_channels(self.websocket)

        self.assertFalse(self.data_source._private_channels_ready)

    async def test_server_error_payload_is_not_echoed_in_exception(self):
        marker = "private-marker-do-not-log"
        self.websocket.receive.side_effect = [
            WSResponse({"event": "error", "code": marker, "msg": marker, "arg": {"channel": marker}}),
        ]

        with self.assertLogs(self.data_source.logger(), level="ERROR") as captured:
            with self.assertRaises(IOError) as raised:
                await self.data_source._subscribe_channels(self.websocket)

        self.assertNotIn(marker, str(raised.exception))
        self.assertNotIn(marker, "\n".join(captured.output))

    async def test_data_without_listener_queue_fails_instead_of_dropping(self):
        self.websocket.receive.side_effect = [
            WSResponse({"arg": {"channel": "orders", "instType": "SPOT"}, "data": [{"ordId": "secret"}]}),
        ]

        with self.assertRaisesRegex(IOError, "without an active listener queue"):
            await self.data_source._subscribe_channels(self.websocket)

        self.assertFalse(self.data_source._private_channels_ready)

    async def test_wrong_post_handshake_channel_ack_revokes_readiness(self):
        self.data_source._private_channels_ready = True

        with self.assertRaises(IOError):
            await self.data_source._process_event_message(
                {"event": "subscribe", "arg": {"channel": "orders", "instType": "SWAP"}},
                asyncio.Queue(),
            )

        self.assertFalse(self.data_source._private_channels_ready)
