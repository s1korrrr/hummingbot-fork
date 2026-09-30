import asyncio
import json
import re
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses.core import aioresponses
from bidict import bidict

import hummingbot.connector.exchange.okx.okx_constants as CONSTANTS
import hummingbot.connector.exchange.okx.okx_web_utils as web_utils
from hummingbot.connector.exchange.okx.okx_api_order_book_data_source import OkxAPIOrderBookDataSource
from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class OkxAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)

        self.connector = OkxExchange(
            okx_api_key="",
            okx_secret_key="",
            okx_passphrase="",
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )
        self.data_source = OkxAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory)

        self._original_full_order_book_reset_time = self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = -1

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map(
            bidict({f"{self.base_asset}-{self.quote_asset}": self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = self._original_full_order_book_reset_time
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _book_event(self, sequence=10, previous=-1, action="snapshot", bids=None, pair=None):
        return {"arg": {"channel": "books", "instId": pair or self.trading_pair}, "action": action,
                "data": [{"ts": "1800000000100", "seqId": sequence, "prevSeqId": previous,
                          "bids": [["99", "1"]] if bids is None else bids, "asks": []}]}

    async def _seed_stream(self):
        output = asyncio.Queue()
        await self.data_source._parse_order_book_diff_message(self._book_event(), output)
        return output.get_nowait()

    async def test_recovery_snapshot_deadline_disconnects_after_successful_sends(self):
        await self._seed_stream()
        ws = AsyncMock()
        disconnected = asyncio.Event()
        ws.disconnect.side_effect = disconnected.set
        self.data_source._ws_assistant = ws
        self.data_source._BOOK_RECOVERY_TIMEOUT = .01
        await self.data_source.request_order_book_resync(self.trading_pair)
        # Traffic on other channels cannot keep an invalid book latched forever.
        await asyncio.wait_for(disconnected.wait(), timeout=.5)
        self.assertEqual(2, ws.send.await_count)
        self.assertNotIn(self.trading_pair, self.data_source._valid_streams)
        self.assertFalse(self.data_source._recovering_streams)

    async def test_snapshot_cancels_recovery_deadline(self):
        await self._seed_stream()
        ws = AsyncMock()
        self.data_source._ws_assistant = ws
        await self.data_source.request_order_book_resync(self.trading_pair)
        task = self.data_source._book_snapshot_deadlines[self.trading_pair]
        recovered = await self._seed_stream()
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.cancelled())
        self.assertNotIn(self.trading_pair, self.data_source._book_snapshot_deadlines)
        self.assertTrue(self.data_source.order_book_stream_valid(self.trading_pair, recovered.content['stream_epoch']))
        ws.disconnect.assert_not_awaited()

    async def test_old_snapshot_deadline_does_not_disconnect_replacement_socket(self):
        await self._seed_stream()
        old, replacement = AsyncMock(), AsyncMock()
        self.data_source._ws_assistant = old
        self.data_source._BOOK_RECOVERY_TIMEOUT = .01
        await self.data_source.request_order_book_resync(self.trading_pair)
        task = self.data_source._book_snapshot_deadlines[self.trading_pair]
        self.data_source._ws_assistant = replacement
        await task
        old.disconnect.assert_not_awaited()
        replacement.disconnect.assert_not_awaited()

    async def test_old_socket_cannot_replace_current_snapshot_deadline(self):
        old, current = AsyncMock(), AsyncMock()
        self.data_source._ws_assistant = current
        self.data_source._require_snapshot_before_deadline(self.trading_pair, current)
        task = self.data_source._book_snapshot_deadlines[self.trading_pair]
        self.data_source._require_snapshot_before_deadline(self.trading_pair, old)
        assert self.data_source._book_snapshot_deadlines[self.trading_pair] is task
        self.data_source._invalidate_connection()
        await asyncio.gather(task, return_exceptions=True)

    async def test_old_snapshot_deadline_does_not_disconnect_new_epoch(self):
        await self._seed_stream()
        ws = AsyncMock()
        self.data_source._ws_assistant = ws
        self.data_source._BOOK_RECOVERY_TIMEOUT = .01
        await self.data_source.request_order_book_resync(self.trading_pair)
        task = self.data_source._book_snapshot_deadlines[self.trading_pair]
        self.data_source._invalidate_book_stream(self.trading_pair)
        await task
        ws.disconnect.assert_not_awaited()

    async def test_initial_subscription_requires_snapshot_before_deadline(self):
        ws = AsyncMock()
        disconnected = asyncio.Event()
        ws.disconnect.side_effect = disconnected.set
        self.data_source._ws_assistant = ws
        self.data_source._BOOK_RECOVERY_TIMEOUT = .01
        await self.data_source._subscribe_channels(ws)
        await asyncio.wait_for(disconnected.wait(), timeout=.5)
        self.assertFalse(self.data_source._book_snapshot_deadlines)

    async def test_interruption_cancels_deadline_and_late_snapshot_cannot_revive_socket(self):
        await self._seed_stream()
        ws = AsyncMock()
        self.data_source._ws_assistant = ws
        await self.data_source.request_order_book_resync(self.trading_pair)
        task = self.data_source._book_snapshot_deadlines[self.trading_pair]
        await self.data_source._on_order_stream_interruption(ws)
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.cancelled())
        queue = asyncio.Queue()
        await self.data_source._parse_order_book_diff_message(self._book_event(), queue)
        self.assertTrue(queue.empty())
        self.assertFalse(self.data_source._valid_streams)

    async def test_unlinked_diff_is_not_published(self):
        await self._seed_stream()
        output = asyncio.Queue()
        await self.data_source._parse_order_book_diff_message(self._book_event(12, 11, "update"), output)
        self.assertTrue(output.empty())

    async def test_invalid_numeric_level_revokes_the_stream_before_publishing(self):
        for price, amount in [('NaN', '1'), ('Infinity', '1'), ('0', '1'), ('-1', '1'),
                              ('100', 'NaN'), ('100', '-1'), ('100', 'Infinity')]:
            with self.subTest(price=price, amount=amount):
                queue = asyncio.Queue()
                await self.data_source._parse_order_book_diff_message(self._book_event(), queue)
                epoch = queue.get_nowait().content['stream_epoch']
                await self.data_source._parse_order_book_diff_message(
                    self._book_event(11, 10, 'update', bids=[[price, amount]]), queue)
                self.assertTrue(queue.empty())
                self.assertFalse(self.data_source.order_book_stream_valid(self.trading_pair, epoch))

    async def test_gap_is_isolated_to_one_pair(self):
        other_pair = "OTHER-HBOT"
        self.connector._set_trading_pair_symbol_map(bidict({self.trading_pair: self.trading_pair, other_pair: other_pair}))
        await self._seed_stream()
        output = asyncio.Queue()
        await self.data_source._parse_order_book_diff_message(self._book_event(pair=other_pair), output)
        other = output.get_nowait()
        await self.data_source._parse_order_book_diff_message(self._book_event(12, 11, "update"), output)
        self.assertTrue(self.data_source.order_book_stream_valid(other_pair, other.content["stream_epoch"]))
        self.assertTrue(output.empty())

    async def test_gap_invalidates_old_epoch_and_recovers_once_with_snapshot(self):
        snapshot = await self._seed_stream()
        old_epoch = snapshot.content["stream_epoch"]
        self.assertTrue(self.data_source.order_book_stream_valid(self.trading_pair, old_epoch))
        self.data_source._ws_assistant = AsyncMock()
        output = asyncio.Queue()
        queued_snapshot = self._book_event()
        self.data_source._channel_originating_message(queued_snapshot)
        await self.data_source._parse_order_book_diff_message(self._book_event(12, 11, "update"), output)
        self.assertTrue(output.empty())
        self.assertFalse(self.data_source.order_book_stream_valid(self.trading_pair, old_epoch))
        self.assertEqual(2, self.data_source._ws_assistant.send.await_count)
        requests = [call.args[0].payload for call in self.data_source._ws_assistant.send.call_args_list]
        self.assertEqual(["unsubscribe", "subscribe"], [request["op"] for request in requests])
        self.assertTrue(all(request["args"] == [{"channel": "books", "instId": self.trading_pair}]
                            for request in requests))
        await self.data_source._parse_order_book_diff_message(self._book_event(13, 12, "update"), output)
        await self.data_source._parse_order_book_diff_message(queued_snapshot, output)
        self.assertTrue(output.empty())
        self.assertEqual(2, self.data_source._ws_assistant.send.await_count)
        await self.data_source._parse_order_book_diff_message(self._book_event(20), output)
        recovered = output.get_nowait()
        self.assertGreater(recovered.content["stream_epoch"], old_epoch)
        self.assertTrue(self.data_source.order_book_stream_valid(self.trading_pair, recovered.content["stream_epoch"]))
        self.assertFalse(self.data_source.order_book_stream_valid(self.trading_pair, old_epoch))

    async def test_start_and_rest_snapshot_do_not_establish_ws_continuity(self):
        self.assertFalse(self.data_source.order_book_stream_valid(self.trading_pair, 0))
        response = {"data": self._book_event()["data"]}
        with patch.object(self.data_source, "_request_order_book_snapshot", new=AsyncMock(return_value=response)):
            snapshot = await self.data_source._order_book_snapshot(self.trading_pair)
        self.assertNotIn("stream_epoch", snapshot.content)
        self.assertFalse(self.data_source.order_book_stream_valid(self.trading_pair, 0))
        output = asyncio.Queue()
        await self.data_source._parse_order_book_diff_message(self._book_event(11, 10, "update"), output)
        self.assertTrue(output.empty())

    async def test_empty_same_sequence_heartbeat_and_maintenance_reset(self):
        await self._seed_stream()
        output = asyncio.Queue()
        await self.data_source._parse_order_book_diff_message(self._book_event(10, 10, "update", bids=[]), output)
        self.assertEqual(OrderBookMessageType.DIFF, output.get_nowait().type)
        await self.data_source._parse_order_book_diff_message(self._book_event(2, 10, "update"), output)
        self.assertEqual(OrderBookMessageType.DIFF, output.get_nowait().type)
        await self.data_source._parse_order_book_diff_message(self._book_event(3, 2, "update"), output)
        self.assertTrue(self.data_source.order_book_stream_valid(self.trading_pair, output.get_nowait().content["stream_epoch"]))

    async def test_nonempty_same_sequence_and_malformed_messages_invalidate(self):
        malformed = self._book_event(11, 10, "update")
        del malformed["data"][0]["seqId"]
        for raw in (self._book_event(10, 10, "update"), malformed):
            await self._seed_stream()
            output = asyncio.Queue()
            await self.data_source._parse_order_book_diff_message(raw, output)
            self.assertTrue(output.empty())
            self.assertFalse(self.data_source.order_book_stream_valid(self.trading_pair, 0))

    async def test_disconnect_rejects_queued_old_connection_snapshot(self):
        old = await self._seed_stream()
        queued = self._book_event()
        self.data_source._channel_originating_message(queued)
        await self.data_source._on_order_stream_interruption(AsyncMock())
        self.assertFalse(self.data_source.order_book_stream_valid(self.trading_pair, old.content["stream_epoch"]))
        output = asyncio.Queue()
        await self.data_source._parse_order_book_diff_message(queued, output)
        self.assertTrue(output.empty())
        with patch.object(self.data_source._api_factory, 'get_ws_assistant',
                          new=AsyncMock(return_value=AsyncMock())):
            await self.data_source._connected_websocket_assistant()
        await self.data_source._parse_order_book_diff_message(self._book_event(), output)
        self.assertTrue(self.data_source.order_book_stream_valid(self.trading_pair, output.get_nowait().content["stream_epoch"]))

    async def test_processing_failure_invalidates_before_outer_retry_delay(self):
        old = await self._seed_stream()
        with patch("hummingbot.core.data_type.order_book_tracker_data_source."
                   "OrderBookTrackerDataSource._process_websocket_messages", new=AsyncMock(side_effect=ConnectionError)):
            with self.assertRaises(ConnectionError):
                await self.data_source._process_websocket_messages(AsyncMock())
        self.assertFalse(self.data_source.order_book_stream_valid(self.trading_pair, old.content["stream_epoch"]))
        epoch = self.data_source._stream_epochs[self.trading_pair]
        await self.data_source._on_order_stream_interruption(AsyncMock())
        self.assertEqual(epoch, self.data_source._stream_epochs[self.trading_pair])

    async def test_gap_recovery_failure_remains_invalid_and_disconnects(self):
        await self._seed_stream()
        self.data_source._ws_assistant = AsyncMock()
        self.data_source._ws_assistant.send.side_effect = asyncio.TimeoutError
        await self.data_source._parse_order_book_diff_message(self._book_event(12, 11, "update"), asyncio.Queue())
        self.data_source._ws_assistant.disconnect.assert_awaited_once()
        self.assertFalse(self.data_source.order_book_stream_valid(self.trading_pair, 0))

    async def test_same_second_later_diff_survives_rest_snapshot_restore(self):
        snapshot = {'data': [{'ts': '1800000000100', 'seqId': 10,
                             'bids': [['99', '1']], 'asks': [['101', '1']]}]}
        with patch.object(self.data_source, '_request_order_book_snapshot', new=AsyncMock(return_value=snapshot)):
            snapshot_message = await self.data_source._order_book_snapshot(self.trading_pair)
        await self._seed_stream()
        output = asyncio.Queue()
        raw_diff = {'arg': {'channel': 'books', 'instId': self.trading_pair}, 'action': 'update',
                    'data': [{'ts': '1800000000900', 'seqId': 11, 'prevSeqId': 10,
                              'bids': [['99', '2']], 'asks': []}]}
        await self.data_source._parse_order_book_diff_message(raw_diff, output)
        diff = output.get_nowait()
        book = OrderBook()
        book.restore_from_snapshot_and_diffs(snapshot_message, [diff])
        self.assertEqual(2, list(book.bid_entries())[0].amount)
        self.assertEqual(1800000000100, snapshot_message.update_id)
        self.assertEqual(1800000000900, diff.update_id)
        self.assertAlmostEqual(1800000000.9, diff.timestamp)

    async def test_same_second_later_diff_survives_websocket_snapshot_restore(self):
        output = asyncio.Queue()
        raw = {'arg': {'channel': 'books', 'instId': self.trading_pair}, 'action': 'snapshot',
               'data': [{'ts': '1800000000100', 'seqId': 10, 'prevSeqId': -1,
                         'bids': [['99', '1']], 'asks': [['101', '1']]}]}
        await self.data_source._parse_order_book_snapshot_message(raw, output)
        snapshot = output.get_nowait()
        raw['action'] = 'update'
        raw['data'] = [{'ts': '1800000000900', 'seqId': 11, 'prevSeqId': 10,
                        'bids': [['99', '3']], 'asks': []}]
        await self.data_source._parse_order_book_diff_message(raw, output)
        diff = output.get_nowait()
        book = OrderBook()
        book.restore_from_snapshot_and_diffs(snapshot, [diff])
        self.assertEqual(3, list(book.bid_entries())[0].amount)
        self.assertEqual(1800000000100, snapshot.update_id)

    @aioresponses()
    async def test_get_new_order_book_successful(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.OKX_ORDER_BOOK_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        resp = {
            "code": "0",
            "msg": "",
            "data": [
                {
                    "asks": [
                        [
                            "41006.8",
                            "0.60038921",
                            "0",
                            "1"
                        ]
                    ],
                    "bids": [
                        [
                            "41006.3",
                            "0.30178218",
                            "0",
                            "2"
                        ]
                    ],
                    "ts": "1629966436396"
                }
            ]
        }

        mock_api.get(regex_url, body=json.dumps(resp))

        order_book: OrderBook = await self.data_source.get_new_order_book(self.trading_pair)

        expected_update_id = int(resp["data"][0]["ts"])

        self.assertEqual(expected_update_id, order_book.snapshot_uid)
        bids = list(order_book.bid_entries())
        asks = list(order_book.ask_entries())
        self.assertEqual(1, len(bids))
        self.assertEqual(41006.3, bids[0].price)
        self.assertEqual(0.30178218, bids[0].amount)
        self.assertEqual(expected_update_id, bids[0].update_id)
        self.assertEqual(1, len(asks))
        self.assertEqual(41006.8, asks[0].price)
        self.assertEqual(0.60038921, asks[0].amount)
        self.assertEqual(expected_update_id, asks[0].update_id)

    @aioresponses()
    async def test_get_new_order_book_raises_exception(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.OKX_ORDER_BOOK_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, status=400)
        with self.assertRaises(IOError):
            await self.data_source.get_new_order_book(self.trading_pair)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_subscribes_to_trades_and_order_diffs(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        result_subscribe_trades = {
            "event": "subscribe",
            "args": {
                "channel": "trades",
                "instId": self.trading_pair
            }
        }
        result_subscribe_diffs = {
            "event": "subscribe",
            "arg": {
                "channel": "books",
                "instId": self.trading_pair
            }
        }

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe_trades))
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe_diffs))

        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_subscriptions())

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        self.assertEqual(2, len(sent_subscription_messages))
        expected_trade_subscription = {
            "op": "subscribe",
            "args": [
                {
                    "channel": "trades",
                    "instId": self.trading_pair
                }
            ]
        }
        self.assertEqual(expected_trade_subscription, sent_subscription_messages[0])
        expected_diff_subscription = {
            "op": "subscribe",
            "args": [
                {
                    "channel": "books",
                    "instId": self.trading_pair
                }
            ]
        }
        self.assertEqual(expected_diff_subscription, sent_subscription_messages[1])

        self.assertTrue(self._is_logged(
            "INFO",
            "Subscribed to public order book and trade channels..."
        ))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_sends_ping_message_before_ping_interval_finishes(self, ws_connect_mock):

        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()
        ws_connect_mock.return_value.receive.side_effect = [asyncio.TimeoutError("Test timeiout"),
                                                            asyncio.CancelledError]

        try:
            await self.data_source.listen_for_subscriptions()
        except asyncio.CancelledError:
            pass

        sent_messages = self.mocking_assistant.text_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        expected_ping_message = "ping"
        self.assertEqual(expected_ping_message, sent_messages[0])

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_listen_for_subscriptions_raises_cancel_exception(self, mock_ws, _: AsyncMock):
        mock_ws.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_subscriptions()

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_logs_exception_details(self, mock_ws, sleep_mock):
        mock_ws.side_effect = Exception("TEST ERROR.")
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(asyncio.CancelledError())

        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_subscriptions())

        await self.resume_test_event.wait()

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds..."))

    async def test_subscribe_channels_raises_cancel_exception(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(mock_ws)

    async def test_subscribe_channels_raises_exception_and_logs_error(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = Exception("Test Error")

        with self.assertRaises(Exception):
            await self.data_source._subscribe_channels(mock_ws)

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error occurred subscribing to order book trading and delta streams...")
        )

    async def test_listen_for_trades_cancelled_when_listening(self):
        mock_queue = MagicMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_trades(self.local_event_loop, msg_queue)

    async def test_listen_for_trades_logs_exception(self):
        incomplete_resp = {
            "arg": {
                "channel": "trades",
                "instId": "BTC-USDT"
            },
            "data": [
                {
                    "instId": "BTC-USDT",
                }
            ]
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            await self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public trade updates from exchange"))

    async def test_listen_for_trades_successful(self):
        mock_queue = AsyncMock()
        trade_event = {
            "arg": {
                "channel": "trades",
                "instId": self.trading_pair
            },
            "data": [
                {
                    "instId": self.trading_pair,
                    "tradeId": "130639474",
                    "px": "42219.9",
                    "sz": "0.12060306",
                    "side": "buy",
                    "ts": "1630048897897"
                }
            ]
        }
        mock_queue.get.side_effect = [trade_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_trades(self.local_event_loop, msg_queue))

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(trade_event["data"][0]["tradeId"], msg.trade_id)
        self.assertEqual(int(trade_event["data"][0]["ts"]) * 1e-3, msg.timestamp)

    async def test_listen_for_order_book_diffs_cancelled(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_order_book_diffs(self.local_event_loop, msg_queue)

    async def test_listen_for_order_book_diffs_logs_exception(self):
        incomplete_resp = {
            "arg": {
                "channel": "books",
                "instId": self.trading_pair
            },
            "action": "update",
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            await self.data_source.listen_for_order_book_diffs(self.local_event_loop, msg_queue)
        except asyncio.CancelledError:
            pass

        self.assertTrue(msg_queue.empty())
        self.assertTrue(self._is_logged(
            "WARNING", f"OKX order book continuity lost for {self.trading_pair}; awaiting recovery snapshot"))

    async def test_listen_for_order_book_diffs_successful(self):
        await self._seed_stream()
        mock_queue = AsyncMock()
        diff_event = {
            "arg": {
                "channel": "books",
                "instId": self.trading_pair
            },
            "action": "update",
            "data": [
                {
                    "asks": [
                        ["8476.98", "415", "0", "13"],
                        ["8477", "7", "0", "2"],
                        ["8477.34", "85", "0", "1"],
                    ],
                    "bids": [
                        ["8476.97", "256", "0", "12"],
                        ["8475.55", "101", "0", "1"],
                    ],
                    "ts": "1597026383085",
                    "checksum": 0, "seqId": 11, "prevSeqId": 10
                }
            ]
        }
        mock_queue.get.side_effect = [diff_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.local_event_loop, msg_queue))

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(OrderBookMessageType.DIFF, msg.type)
        self.assertEqual(-1, msg.trade_id)
        self.assertEqual(int(diff_event["data"][0]["ts"]) * 1e-3, msg.timestamp)
        expected_update_id = int(diff_event["data"][0]["ts"])
        self.assertEqual(expected_update_id, msg.update_id)

        bids = msg.bids
        asks = msg.asks
        self.assertEqual(2, len(bids))
        self.assertEqual(8476.97, bids[0].price)
        self.assertEqual(256, bids[0].amount)
        self.assertEqual(expected_update_id, bids[0].update_id)
        self.assertEqual(3, len(asks))
        self.assertEqual(8476.98, asks[0].price)
        self.assertEqual(415, asks[0].amount)
        self.assertEqual(expected_update_id, asks[0].update_id)

    async def test_listen_for_order_book_snapshots_websocket_successful(self):
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 1
        mock_queue = AsyncMock()
        snapshot_event = {
            "arg": {
                "channel": "books",
                "instId": self.trading_pair
            },
            "action": "snapshot",
            "data": [
                {
                    "asks": [
                        ["8476.98", "415", "0", "13"],
                        ["8477", "7", "0", "2"],
                        ["8477.34", "85", "0", "1"],
                    ],
                    "bids": [
                        ["8476.97", "256", "0", "12"],
                        ["8475.55", "101", "0", "1"],
                    ],
                    "ts": "1597026383085",
                    "checksum": 0, "seqId": 10, "prevSeqId": -1
                }
            ]
        }
        mock_queue.get.side_effect = [snapshot_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.local_event_loop, msg_queue))

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(-1, msg.trade_id)
        self.assertEqual(int(snapshot_event["data"][0]["ts"]) * 1e-3, msg.timestamp)
        expected_update_id = int(snapshot_event["data"][0]["ts"])
        self.assertEqual(expected_update_id, msg.update_id)

        bids = msg.bids
        asks = msg.asks
        self.assertEqual(2, len(bids))
        self.assertEqual(8476.97, bids[0].price)
        self.assertEqual(256, bids[0].amount)
        self.assertEqual(expected_update_id, bids[0].update_id)
        self.assertEqual(3, len(asks))
        self.assertEqual(8476.98, asks[0].price)
        self.assertEqual(415, asks[0].amount)
        self.assertEqual(expected_update_id, asks[0].update_id)

    @aioresponses()
    async def test_listen_for_order_book_snapshots_cancelled_when_fetching_snapshot(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.OKX_ORDER_BOOK_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, exception=asyncio.CancelledError)

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_order_book_snapshots(self.local_event_loop, asyncio.Queue())

    @aioresponses()
    @patch("hummingbot.connector.exchange.okx.okx_api_order_book_data_source"
           ".OkxAPIOrderBookDataSource._sleep")
    async def test_listen_for_order_book_snapshots_log_exception(self, mock_api, sleep_mock):
        msg_queue: asyncio.Queue = asyncio.Queue()
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(asyncio.CancelledError())

        url = web_utils.public_rest_url(path_url=CONSTANTS.OKX_ORDER_BOOK_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, exception=Exception)

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.local_event_loop, msg_queue)
        )
        await self.resume_test_event.wait()

        self.assertTrue(
            self._is_logged("ERROR", f"Unexpected error fetching order book snapshot for {self.trading_pair}."))

    @aioresponses()
    async def test_listen_for_order_book_snapshots_api_successful(self, mock_api, ):
        msg_queue: asyncio.Queue = asyncio.Queue()
        url = web_utils.public_rest_url(path_url=CONSTANTS.OKX_ORDER_BOOK_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        resp = {
            "code": "0",
            "msg": "",
            "data": [
                {
                    "asks": [
                        [
                            "41006.8",
                            "0.60038921",
                            "0",
                            "1"
                        ]
                    ],
                    "bids": [
                        [
                            "41006.3",
                            "0.30178218",
                            "0",
                            "2"
                        ]
                    ],
                    "ts": "1629966436396"
                }
            ]
        }

        mock_api.get(regex_url, body=json.dumps(resp))

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.local_event_loop, msg_queue)
        )

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(-1, msg.trade_id)
        self.assertEqual(int(resp["data"][0]["ts"]) * 1e-3, msg.timestamp)
        expected_update_id = int(resp["data"][0]["ts"])
        self.assertEqual(expected_update_id, msg.update_id)

        bids = msg.bids
        asks = msg.asks
        self.assertEqual(1, len(bids))
        self.assertEqual(41006.3, bids[0].price)
        self.assertEqual(0.30178218, bids[0].amount)
        self.assertEqual(expected_update_id, bids[0].update_id)
        self.assertEqual(1, len(asks))
        self.assertEqual(41006.8, asks[0].price)
        self.assertEqual(0.60038921, asks[0].amount)
        self.assertEqual(expected_update_id, asks[0].update_id)

    async def test_channel_originating_message_snapshot_uses_serial_book_queue(self):
        event_message = {
            "arg": {
                "channel": "books",
                "instId": self.trading_pair
            },
            "action": "snapshot",
            "data": [
                {
                    "asks": [
                        ["8476.98", "415", "0", "13"],
                    ],
                    "bids": [
                        ["8476.97", "256", "0", "12"],
                    ],
                    "ts": "1597026383085",
                    "checksum": -855196043
                }
            ]
        }
        channel_result = self.data_source._channel_originating_message(event_message)
        self.assertEqual(channel_result, self.data_source._diff_messages_queue_key)

    async def test_channel_originating_message_diff_queue(self):
        event_message = {
            "arg": {
                "channel": "books",
                "instId": self.trading_pair
            },
            "action": "update",
            "data": [
                {
                    "asks": [
                        ["8476.98", "415", "0", "13"],
                    ],
                    "bids": [
                        ["8476.97", "256", "0", "12"],
                    ],
                    "ts": "1597026383085",
                    "checksum": -855196043
                }
            ]
        }
        channel_result = self.data_source._channel_originating_message(event_message)
        self.assertEqual(channel_result, self.data_source._diff_messages_queue_key)

    # Dynamic subscription tests for subscribe_to_trading_pair and unsubscribe_from_trading_pair

    async def test_subscribe_to_trading_pair_successful(self):
        """Test successful subscription to a new trading pair."""
        new_pair = "ETH-USDT"

        # Set up the symbol map for the new pair
        self.connector._set_trading_pair_symbol_map(
            bidict({f"{self.base_asset}-{self.quote_asset}": self.trading_pair, "ETH-USDT": new_pair})
        )

        # Create a mock WebSocket assistant
        mock_ws = AsyncMock()
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.subscribe_to_trading_pair(new_pair)

        self.assertTrue(result)
        # OKX sends 2 messages: trades and books
        self.assertEqual(2, mock_ws.send.call_count)

        # Verify pair was added to trading pairs
        self.assertIn(new_pair, self.data_source._trading_pairs)

        self.assertTrue(
            self._is_logged("INFO", f"Subscribed to {new_pair} order book and trade channels")
        )

    async def test_subscribe_to_trading_pair_websocket_not_connected(self):
        """Test subscription fails when WebSocket is not connected."""
        new_pair = "ETH-USDT"

        # Ensure ws_assistant is None
        self.data_source._ws_assistant = None

        result = await self.data_source.subscribe_to_trading_pair(new_pair)

        self.assertFalse(result)
        self.assertTrue(
            self._is_logged("WARNING", f"Cannot subscribe to {new_pair}: WebSocket not connected")
        )

    async def test_subscribe_to_trading_pair_raises_cancel_exception(self):
        """Test that CancelledError is properly raised during subscription."""
        new_pair = "ETH-USDT"

        self.connector._set_trading_pair_symbol_map(
            bidict({f"{self.base_asset}-{self.quote_asset}": self.trading_pair, "ETH-USDT": new_pair})
        )

        mock_ws = AsyncMock()
        mock_ws.send.side_effect = asyncio.CancelledError
        self.data_source._ws_assistant = mock_ws

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.subscribe_to_trading_pair(new_pair)

    async def test_subscribe_to_trading_pair_raises_exception_and_logs_error(self):
        """Test that exceptions during subscription are logged and return False."""
        new_pair = "ETH-USDT"

        self.connector._set_trading_pair_symbol_map(
            bidict({f"{self.base_asset}-{self.quote_asset}": self.trading_pair, "ETH-USDT": new_pair})
        )

        mock_ws = AsyncMock()
        mock_ws.send.side_effect = Exception("Test Error")
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.subscribe_to_trading_pair(new_pair)

        self.assertFalse(result)
        self.assertTrue(
            self._is_logged("ERROR", f"Error subscribing to {new_pair}")
        )

    async def test_unsubscribe_from_trading_pair_successful(self):
        """Test successful unsubscription from a trading pair."""
        # The trading pair is already added in setup
        self.assertIn(self.trading_pair, self.data_source._trading_pairs)

        mock_ws = AsyncMock()
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

        self.assertTrue(result)
        # OKX sends 1 message for unsubscribe (both channels in one request)
        self.assertEqual(1, mock_ws.send.call_count)

        # Verify pair was removed from trading pairs
        self.assertNotIn(self.trading_pair, self.data_source._trading_pairs)

        self.assertTrue(
            self._is_logged("INFO", f"Unsubscribed from {self.trading_pair} order book and trade channels")
        )

    async def test_unsubscribe_from_trading_pair_websocket_not_connected(self):
        """Test unsubscription fails when WebSocket is not connected."""
        self.data_source._ws_assistant = None

        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

        self.assertFalse(result)
        self.assertTrue(
            self._is_logged("WARNING", f"Cannot unsubscribe from {self.trading_pair}: WebSocket not connected")
        )

    async def test_unsubscribe_from_trading_pair_raises_cancel_exception(self):
        """Test that CancelledError is properly raised during unsubscription."""
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = asyncio.CancelledError
        self.data_source._ws_assistant = mock_ws

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

    async def test_unsubscribe_from_trading_pair_raises_exception_and_logs_error(self):
        """Test that exceptions during unsubscription are logged and return False."""
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = Exception("Test Error")
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

        self.assertFalse(result)
        self.assertTrue(
            self._is_logged("ERROR", f"Error unsubscribing from {self.trading_pair}")
        )
