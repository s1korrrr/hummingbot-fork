import asyncio
import logging
import threading
import time
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase

from hummingbot.remote_iface.mqtt import MQTTGateway, MQTTLogHandler


class MQTTLogIsolationTest(IsolatedAsyncioTestCase):
    async def test_close_during_formatting_cannot_orphan_a_queued_record(self):
        formatting = threading.Event()
        release = threading.Event()
        app = SimpleNamespace(ev_loop=asyncio.get_running_loop(), instance_id="test")
        node = SimpleNamespace(namespace="test", create_publisher=lambda **kwargs: None)
        handler = MQTTLogHandler(app, node)

        def format_record(record):
            formatting.set()
            release.wait(1)
            return "formatted"

        handler.format = format_record
        record = logging.LogRecord("test", logging.WARNING, "", 0, "example", (), None)
        producer = threading.Thread(target=handler.emit, args=(record,))
        producer.start()
        try:
            self.assertTrue(await asyncio.to_thread(formatting.wait, 1))
            handler.close()
            await asyncio.to_thread(handler._worker.join, 0.5)
            release.set()
            await asyncio.to_thread(producer.join, 1)
            await asyncio.to_thread(handler._worker.join, 1)
            self.assertEqual(0, handler._pending.qsize())
            self.assertEqual(1, handler.dropped_log_count)
        finally:
            release.set()
            handler.close()

    async def test_retirement_removes_root_and_named_handlers(self):
        app = SimpleNamespace(ev_loop=asyncio.get_running_loop(), instance_id="test")
        node = SimpleNamespace(namespace="test", create_publisher=lambda **kwargs: None)
        handler = MQTTLogHandler(app, node)
        root = logging.getLogger()
        named = logging.getLogger("hummingbot.test.retirement")
        root.addHandler(handler)
        named.addHandler(handler)
        gateway = object.__new__(MQTTGateway)
        gateway._logh = handler
        gateway._safe_get_log_handlers = lambda: [named]
        gateway.stop = lambda *args: None
        try:
            gateway._remove_log_handlers()
            self.assertNotIn(handler, root.handlers)
            self.assertNotIn(handler, named.handlers)
            await asyncio.to_thread(handler._worker.join, 1)
            self.assertFalse(handler._worker.is_alive())
        finally:
            root.removeHandler(handler)
            named.removeHandler(handler)
            handler.close()

    async def test_slow_transport_does_not_block_ticks_and_queue_is_bounded(self):
        release = threading.Event()
        entered = threading.Event()
        sent = []

        class Publisher:
            def publish(self, message):
                entered.set()
                release.wait(1)
                sent.append(message)

        app = SimpleNamespace(ev_loop=asyncio.get_running_loop(), instance_id="test")
        node = SimpleNamespace(namespace="test", create_publisher=lambda **kwargs: Publisher())
        handler = MQTTLogHandler(app, node)
        record = logging.LogRecord("test", logging.WARNING, "", 0, "example", (), None)
        try:
            start = time.monotonic()
            handler.emit(record)
            self.assertLess(time.monotonic() - start, 0.1)
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            for _ in range(300):
                handler.emit(record)
            self.assertGreater(handler.dropped_log_count, 0)
            self.assertLessEqual(handler._pending.qsize(), 256)
            handler.close()
            release.set()
            await asyncio.to_thread(handler._worker.join, 1)
            self.assertFalse(handler._worker.is_alive())
            self.assertEqual(1, len(sent))
        finally:
            release.set()
            handler.close()
