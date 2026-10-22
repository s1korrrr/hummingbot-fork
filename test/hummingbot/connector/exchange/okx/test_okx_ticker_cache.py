import asyncio
import types
import unittest
from unittest.mock import patch

from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange


class CompletedResponseTickerCacheTests(unittest.IsolatedAsyncioTestCase):
    def make_connector(self):
        clock = [100.0]
        calls = []

        async def get(**kwargs):
            calls.append(kwargs)
            await asyncio.sleep(0)
            clock[0] += 3.0
            return {'data': [
                {'instId': pair, 'last': str(index + 1), 'ts': '1720000000123'}
                for index, pair in enumerate(['BTC-USDC', 'ETH-USDC', 'SOL-USDC'])
            ]}

        async def symbol(trading_pair):
            return trading_pair

        connector = types.SimpleNamespace(
            _spot_ticker_lock=asyncio.Lock(), _spot_ticker_cache=None,
            _api_get=get, exchange_symbol_associated_to_pair=symbol,
            _last_traded_price_source_timestamps={},
        )
        connector._spot_ticker_snapshot = types.MethodType(OkxExchange._spot_ticker_snapshot, connector)
        connector._get_last_traded_price = types.MethodType(OkxExchange._get_last_traded_price, connector)
        return connector, clock, calls

    async def test_slow_response_is_reused_by_concurrent_pairs(self):
        connector, clock, calls = self.make_connector()
        with patch('hummingbot.connector.exchange.okx.okx_exchange.time.monotonic', side_effect=lambda: clock[0]):
            prices = await asyncio.gather(*[
                connector._get_last_traded_price(pair)
                for pair in ['BTC-USDC', 'ETH-USDC', 'SOL-USDC']
            ])
        self.assertEqual(prices, [1.0, 2.0, 3.0])
        self.assertEqual(len(calls), 1)
        self.assertEqual(connector._spot_ticker_cache[0], 103.0)
        self.assertEqual(set(connector._last_traded_price_source_timestamps.values()), {1720000000.123})

    async def test_cache_expires_from_completed_response_and_refreshes(self):
        connector, clock, calls = self.make_connector()
        with patch('hummingbot.connector.exchange.okx.okx_exchange.time.monotonic', side_effect=lambda: clock[0]):
            await connector._spot_ticker_snapshot()
            clock[0] += 1.9
            await connector._spot_ticker_snapshot()
            self.assertEqual(len(calls), 1)
            clock[0] += .2
            await connector._spot_ticker_snapshot()
            self.assertEqual(len(calls), 2)
            self.assertEqual(connector._spot_ticker_cache[0], clock[0])

    async def test_failed_response_does_not_create_cache(self):
        connector, clock, calls = self.make_connector()

        async def fail(**kwargs):
            clock[0] += 3
            raise TimeoutError('offline fixture')

        connector._api_get = fail
        with self.assertRaises(TimeoutError):
            await connector._spot_ticker_snapshot()
        self.assertIsNone(connector._spot_ticker_cache)
