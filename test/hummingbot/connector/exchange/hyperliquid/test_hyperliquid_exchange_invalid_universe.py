import asyncio
import unittest

from hummingbot.connector.exchange.hyperliquid.hyperliquid_exchange import HyperliquidExchange
from hummingbot.connector.utils import combine_to_hb_trading_pair


class HyperliquidExchangeInvalidUniverseEntryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "USDC"
        cls.trading_pair = combine_to_hb_trading_pair(cls.base_asset, cls.quote_asset)

    def setUp(self) -> None:
        super().setUp()
        self.exchange = HyperliquidExchange(
            hyperliquid_secret_key="13e56ca9cceebf1f33065c2c5376ab38570a114bc1b003b60d838f92be9d7930",  # noqa: mock
            hyperliquid_mode="arb_wallet",
            hyperliquid_address="test-address",
            use_vault=False,
            trading_pairs=[self.trading_pair],
        )

    def async_run_with_timeout(self, coroutine, timeout: int = 1):
        return asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))

    def test_update_trading_rules_ignores_invalid_universe_entries(self):
        mocked_response = [
            {
                "tokens": [
                    {"name": self.quote_asset, "szDecimals": 8, "index": 0},
                    {"name": self.base_asset, "szDecimals": 0, "index": 1},
                ],
                "universe": [
                    {"name": "COINALPHA/USDC", "tokens": [1, 0], "index": 0, "isCanonical": True},
                    {"name": "BROKEN/USDC", "tokens": [99, 0], "index": 1, "isCanonical": True},
                ],
            },
            [
                {"coin": "COINALPHA/USDC", "markPx": "0.22923"},
                {"coin": "BROKEN/USDC", "markPx": "1.0"},
            ],
        ]

        self.exchange._initialize_trading_pair_symbols_from_exchange_info(mocked_response)
        trading_rules = self.async_run_with_timeout(self.exchange._format_trading_rules(mocked_response))
        trading_pair_symbol_map = self.async_run_with_timeout(self.exchange.trading_pair_symbol_map())

        self.assertIn(self.trading_pair, trading_pair_symbol_map.inverse)
        self.assertTrue(any(rule.trading_pair == self.trading_pair for rule in trading_rules))
        self.assertTrue(all(rule.trading_pair != "BROKEN-USDC" for rule in trading_rules))
