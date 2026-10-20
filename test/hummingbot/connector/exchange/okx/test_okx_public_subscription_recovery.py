"""Explicit public rejection must invalidate books and use native reconnect."""

import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from bidict import bidict

from hummingbot.connector.exchange.okx.okx_api_order_book_data_source import OkxAPIOrderBookDataSource
from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.core.web_assistant.connections.data_types import WSResponse


class PublicSubscriptionRecoveryTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pair = "COINALPHA-HBOT"
        connector = OkxExchange(
            okx_api_key="", okx_secret_key="", okx_passphrase="",
            trading_pairs=[self.pair], trading_required=False,
        )
        connector._set_trading_pair_symbol_map(bidict({self.pair: self.pair}))
        self.source = OkxAPIOrderBookDataSource(
            trading_pairs=[self.pair], connector=connector,
            api_factory=connector._web_assistants_factory,
        )

    def snapshot(self):
        return {
            "arg": {"channel": "books", "instId": self.pair}, "action": "snapshot",
            "data": [{"ts": "1800000000100", "seqId": 10, "prevSeqId": -1,
                      "bids": [["99", "1"]], "asks": [["100", "1"]]}],
        }

    async def seed(self):
        books = asyncio.Queue()
        await self.source._parse_order_book_diff_message(self.snapshot(), books)
        epoch = books.get_nowait().content["stream_epoch"]
        self.assertTrue(self.source.order_book_stream_valid(self.pair, epoch))
        return epoch

    def websocket(self, messages):
        ws = AsyncMock()
        closed = asyncio.Event()
        ws.disconnect.side_effect = closed.set

        async def frames():
            for message in messages:
                yield WSResponse(message)
            await closed.wait()

        ws.iter_messages = MagicMock(side_effect=frames)
        return ws

    async def assert_rejection(self, frame):
        epoch = await self.seed()
        ws = self.websocket([frame])
        task = asyncio.create_task(self.source._process_websocket_messages(ws))
        try:
            done, _ = await asyncio.wait({task}, timeout=0.1)
            self.assertIn(task, done, "rejection left the public socket and epoch active")
            with self.assertRaisesRegex(ConnectionError, "public subscription rejected") as error:
                await task
            self.assertNotIn("private-marker", str(error.exception))
            self.assertFalse(self.source.order_book_stream_valid(self.pair, epoch))
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_scoped_trades_error_invalidates_valid_book(self):
        await self.assert_rejection({
            "event": "error", "code": "60012", "msg": "private-marker",
            "arg": {"channel": "trades", "instId": self.pair},
        })

    async def test_unscoped_error_invalidates_valid_book(self):
        await self.assert_rejection({"event": "error", "msg": "private-marker"})

    async def test_books_error_invalidates_valid_book(self):
        await self.assert_rejection({
            "event": "error", "arg": {"channel": "books", "instId": self.pair},
        })

    async def test_benign_controls_preserve_book_and_data_routing(self):
        epoch = await self.seed()
        trade = {"arg": {"channel": "trades", "instId": self.pair}, "data": []}
        ws = self.websocket([
            {"event": "subscribe", "arg": {"channel": "trades", "instId": self.pair}},
            "pong", {"event": "notice", "msg": "benign"}, self.snapshot(), trade,
        ])
        task = asyncio.create_task(self.source._process_websocket_messages(ws))
        try:
            queued_book = await asyncio.wait_for(
                self.source._message_queue[self.source._diff_messages_queue_key].get(), 0.5,
            )
            queued_trade = await asyncio.wait_for(
                self.source._message_queue[self.source._trade_messages_queue_key].get(), 0.5,
            )
            self.assertEqual("snapshot", queued_book["action"])
            self.assertIs(trade, queued_trade)
            self.assertFalse(task.done())
            self.assertTrue(self.source.order_book_stream_valid(self.pair, epoch))
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_native_listener_disconnects_and_resubscribes_after_error(self):
        old_epoch = await self.seed()
        first = self.websocket([{"event": "error", "msg": "private-marker"}])
        second = self.websocket([self.snapshot()])
        reconnected = asyncio.Event()
        subscribed = asyncio.Event()
        connections = iter([first, second])

        async def sent(_request):
            if second.send.await_count == 2:
                subscribed.set()

        second.send.side_effect = sent

        async def connect():
            ws = next(connections)
            if ws is second:
                self.assertFalse(self.source.order_book_stream_valid(self.pair, old_epoch))
                first.disconnect.assert_awaited_once()
                reconnected.set()
            return ws

        # Keep the native connect/reset transition; replace only its transport factory.
        self.source._api_factory.get_ws_assistant = AsyncMock(side_effect=connect)
        logger = MagicMock()
        diff_task = None
        with patch.object(self.source, "logger", return_value=logger):
            task = asyncio.create_task(self.source.listen_for_subscriptions())
            try:
                await asyncio.wait_for(reconnected.wait(), 0.5)
                self.assertEqual(2, first.send.await_count)
                # Sent requests cannot make the replacement connection ready.
                self.assertFalse(self.source.order_book_stream_valid(self.pair, old_epoch))
                await asyncio.wait_for(subscribed.wait(), 0.5)
                self.assertEqual(2, second.send.await_count)
                self.assertIn("public subscription rejected", str(logger.warning.call_args_list))
                self.assertNotIn("private-marker", str(logger.warning.call_args_list))
                output = asyncio.Queue()
                diff_task = asyncio.create_task(self.source.listen_for_order_book_diffs(
                    asyncio.get_running_loop(), output,
                ))
                replacement = await asyncio.wait_for(output.get(), 0.5)
                new_epoch = replacement.content["stream_epoch"]
                self.assertTrue(self.source.order_book_stream_valid(self.pair, new_epoch))
                self.assertGreater(new_epoch, old_epoch)
                self.assertFalse(self.source.order_book_stream_valid(self.pair, old_epoch))
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                if diff_task is not None:
                    diff_task.cancel()
                    await asyncio.gather(diff_task, return_exceptions=True)
        second.disconnect.assert_awaited_once()
