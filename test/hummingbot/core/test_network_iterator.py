import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

import pandas as pd

from hummingbot.core.clock import Clock, ClockMode
from hummingbot.core.network_iterator import NetworkIterator, NetworkStatus


class MockNetworkIterator(NetworkIterator):

    def __init__(self):
        super().__init__()
        self._start_network_event = asyncio.Event()
        self._stop_network_event = asyncio.Event()

    async def start_network(self):
        self._start_network_event.set()
        self._stop_network_event = asyncio.Event()

    async def stop_network(self):
        self._stop_network_event.set()

        self._network_status = NetworkStatus.STOPPED
        self._start_network_event = asyncio.Event()

    async def check_network(self):
        if self.network_status != NetworkStatus.CONNECTED:
            self.last_connected_timestamp = self.current_timestamp
            return NetworkStatus.CONNECTED
        else:
            return NetworkStatus.NOT_CONNECTED


class ScriptedNetworkIterator(NetworkIterator):
    """Answers each network check from a script: up, down, or timeout (slower than the check timeout)."""

    def __init__(self, script):
        super().__init__()
        self.script = list(script)
        self.starts = 0
        self.stops = 0
        self.check_network_interval = 0.001
        self.check_network_timeout = 0.02

    async def check_network(self):
        if not self.script:
            raise asyncio.CancelledError
        step = self.script.pop(0)
        if step == "timeout":
            await asyncio.sleep(1)
        return NetworkStatus.CONNECTED if step == "up" else NetworkStatus.NOT_CONNECTED

    async def start_network(self):
        self.starts += 1

    async def stop_network(self):
        self.stops += 1


class NetworkFailureThresholdTest(IsolatedAsyncioWrapperTestCase):
    """A connected connector is torn down (and later rebuilt by start_network) only after the failure persists."""

    async def run_script(self, script):
        iterator = ScriptedNetworkIterator(script)
        clock = Clock(ClockMode.BACKTEST, 1, 0, 10)
        iterator.start(clock, 0)
        with self.assertRaises(asyncio.CancelledError):
            await iterator.check_network_task
        return iterator

    async def test_transient_failures_keep_the_connection(self):
        iterator = await self.run_script(["up", "timeout", "down", "up", "timeout", "timeout", "up"])
        self.assertEqual((NetworkStatus.CONNECTED, 1, 0), (iterator.network_status, iterator.starts, iterator.stops))

    async def test_a_persistent_failure_stops_networking_once_and_recovery_restarts_it(self):
        iterator = await self.run_script(["up", "timeout", "down", "timeout", "timeout", "up"])
        self.assertEqual((NetworkStatus.CONNECTED, 2, 1), (iterator.network_status, iterator.starts, iterator.stops))
        iterator = await self.run_script(["up", "timeout", "timeout", "timeout"])
        self.assertEqual((NetworkStatus.NOT_CONNECTED, 1, 1), (iterator.network_status, iterator.starts, iterator.stops))

    async def test_failures_before_the_first_connection_do_not_start_or_stop(self):
        iterator = await self.run_script(["timeout", "down", "timeout"])
        self.assertEqual((NetworkStatus.NOT_CONNECTED, 0, 0), (iterator.network_status, iterator.starts, iterator.stops))


class NetworkIteratorUnitTest(IsolatedAsyncioWrapperTestCase):

    start: pd.Timestamp = pd.Timestamp("2021-01-01", tz="UTC")
    end: pd.Timestamp = pd.Timestamp("2022-01-01 01:00:00", tz="UTC")
    start_timestamp: float = start.timestamp()
    end_timestamp: float = end.timestamp()
    clock_tick_size = 10

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        self.network_iterator = MockNetworkIterator()
        self.clock: Clock = Clock(ClockMode.BACKTEST, self.clock_tick_size, self.start_timestamp, self.end_timestamp)
        self.clock.add_iterator(self.network_iterator)
        super().setUp()

    async def asyncSetUp(self):
        await super().asyncSetUp()

    async def test_network_status(self):
        # This test technically tests the _check_network_loop() and all its paths.
        self.assertEqual(NetworkStatus.STOPPED, self.network_iterator.network_status)

        self.network_iterator.check_network_interval = 0.5

        self.clock.backtest_til(self.start_timestamp)
        await asyncio.sleep(0.5)
        self.assertEqual(NetworkStatus.CONNECTED, self.network_iterator.network_status)
        self.assertTrue(self.network_iterator._start_network_event.is_set())

        # The mock alternates connected / not connected: a single failed check never tears the connection down.
        await asyncio.sleep(0.5)
        self.assertEqual(NetworkStatus.CONNECTED, self.network_iterator.network_status)
        self.assertFalse(self.network_iterator._stop_network_event.is_set())

    def test_last_connected_timestamp(self):
        self.clock.backtest_til(self.start_timestamp)
        self.local_event_loop.run_until_complete(asyncio.sleep(0.5))
        self.assertEqual(self.start_timestamp, self.network_iterator.last_connected_timestamp)

    def test_check_network_task(self):
        self.clock.backtest_til(self.start_timestamp)
        self.assertIsNotNone(self.network_iterator.check_network_task)

    def test_check_network_interval(self):
        # Default interval
        self.assertEqual(10.0, self.network_iterator.check_network_interval)

    def test_network_error_wait_time(self):
        # Default wait time
        self.assertEqual(60.0, self.network_iterator.network_error_wait_time)

    def test_check_network_timeout(self):
        # Default timeout
        self.assertEqual(5.0, self.network_iterator.check_network_timeout)

    def test_start_network(self):
        self.assertFalse(self.network_iterator._start_network_event.is_set())
        self.assertFalse(self.network_iterator._stop_network_event.is_set())

        self.local_event_loop.run_until_complete(self.network_iterator.start_network())
        self.assertTrue(self.network_iterator._start_network_event.is_set())
        self.assertFalse(self.network_iterator._stop_network_event.is_set())

    def test_stop_network(self):
        self.assertFalse(self.network_iterator._start_network_event.is_set())
        self.assertFalse(self.network_iterator._stop_network_event.is_set())

        self.local_event_loop.run_until_complete(self.network_iterator.stop_network())
        self.assertFalse(self.network_iterator._start_network_event.is_set())
        self.assertTrue(self.network_iterator._stop_network_event.is_set())
        self.assertEqual(NetworkStatus.STOPPED, self.network_iterator.network_status)

    def test_start(self):
        self.assertEqual(NetworkStatus.STOPPED, self.network_iterator.network_status)

        self.network_iterator.start(self.clock, self.clock.current_timestamp)

        self.assertIsNotNone(self.network_iterator.check_network_task)
        self.assertEqual(NetworkStatus.NOT_CONNECTED, self.network_iterator.network_status)

    def test_stop(self):
        self.assertEqual(NetworkStatus.STOPPED, self.network_iterator.network_status)

        self.network_iterator.start(self.clock, self.clock.current_timestamp)
        self.assertEqual(NetworkStatus.NOT_CONNECTED, self.network_iterator.network_status)

        self.network_iterator.stop(self.clock)

        self.assertEqual(NetworkStatus.STOPPED, self.network_iterator.network_status)
        self.assertIsNone(self.network_iterator.check_network_task)
