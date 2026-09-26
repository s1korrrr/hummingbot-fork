"""The compiled network loop can report NOT_CONNECTED without stopping networking (a check call that outlived
check_network_timeout), then calls start_network() on the next good check. The OKX connector rebuilds only when
networking was stopped or the previous good check is too old to trust the running streams."""
import unittest
from unittest.mock import AsyncMock, patch

from hummingbot.connector.exchange.okx import okx_exchange
from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.network_iterator import NetworkStatus


class OkxNetworkRebuildTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.clock = [1000.0]
        self.monotonic = patch.object(okx_exchange.time, "monotonic", side_effect=lambda: self.clock[0])
        self.monotonic.start()
        self.addCleanup(self.monotonic.stop)
        self.base_start = patch.object(ExchangePyBase, "start_network", new_callable=AsyncMock)
        self.base_stop = patch.object(ExchangePyBase, "stop_network", new_callable=AsyncMock)
        self.start = self.base_start.start()
        self.stop = self.base_stop.start()
        self.addCleanup(self.base_start.stop)
        self.addCleanup(self.base_stop.stop)
        self.exchange = OkxExchange(okx_api_key="k", okx_secret_key="s", okx_passphrase="p",
                                    trading_pairs=["BTC-USDC"])
        self.exchange._make_network_check_request = AsyncMock()

    async def good_check_after(self, seconds):
        self.clock[0] += seconds
        self.assertEqual(NetworkStatus.CONNECTED, await self.exchange.check_network())

    async def test_first_connection_starts_networking(self):
        await self.good_check_after(0)
        await self.exchange.start_network()
        self.assertEqual(1, self.start.await_count)

    async def test_recovery_after_a_short_silent_spell_keeps_the_running_streams(self):
        await self.good_check_after(0)
        await self.exchange.start_network()
        await self.good_check_after(10)
        await self.good_check_after(70)  # one check timed out in between: NOT_CONNECTED without stop_network()
        with self.assertLogs(okx_exchange.OkxExchange.logger().name, level="INFO") as logs:
            await self.exchange.start_network()
        self.assertEqual((1, 0), (self.start.await_count, self.stop.await_count))
        self.assertIn("keeping the running order books and streams", "\n".join(logs.output))

    async def test_a_long_silent_spell_rebuilds(self):
        await self.good_check_after(0)
        await self.exchange.start_network()
        await self.good_check_after(121)
        await self.exchange.start_network()
        self.assertEqual(2, self.start.await_count)

    async def test_a_logged_stop_always_rebuilds(self):
        await self.good_check_after(0)
        await self.exchange.start_network()
        await self.exchange.stop_network()
        await self.good_check_after(10)
        await self.exchange.start_network()
        self.assertEqual((2, 1), (self.start.await_count, self.stop.await_count))

    async def test_a_failed_start_is_retried_in_full(self):
        self.start.side_effect = [RuntimeError("boom"), None]
        await self.good_check_after(0)
        with self.assertRaises(RuntimeError):
            await self.exchange.start_network()
        await self.good_check_after(10)
        await self.exchange.start_network()
        self.assertEqual(2, self.start.await_count)

    async def test_failed_or_slow_checks_do_not_refresh_the_last_good_check(self):
        await self.good_check_after(0)
        await self.exchange.start_network()
        self.exchange._make_network_check_request = AsyncMock(side_effect=IOError("down"))
        self.clock[0] += 60
        self.assertEqual(NetworkStatus.NOT_CONNECTED, await self.exchange.check_network())
        self.exchange._make_network_check_request = AsyncMock()
        await self.good_check_after(61)
        await self.exchange.start_network()
        self.assertEqual(2, self.start.await_count)


if __name__ == "__main__":
    unittest.main()
