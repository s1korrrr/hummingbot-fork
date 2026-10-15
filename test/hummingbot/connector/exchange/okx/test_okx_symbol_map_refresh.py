"""A newly listed instrument must not log a trading-rule error on the first refresh after it goes live.

Live evidence (2026-10-14/15, every OKX bot): `Error parsing the trading pair rule {... 'baseCcy': 'XTSLL' ...}`
with `KeyError: 'XTSLL-USDT'`. ExchangePyBase._update_trading_rules formatted the rules from a fresh instruments
payload before refreshing the symbol map from that same payload, so a pair that became live since the last
refresh was not yet in the map."""
import logging
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange


def instrument(inst, base, quote, state="live"):
    return {"instType": "SPOT", "instId": inst, "baseCcy": base, "quoteCcy": quote, "state": state,
            "minSz": "0.001", "tickSz": "0.01", "lotSz": "0.001"}


class OkxSymbolMapRefreshTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange = OkxExchange(okx_api_key="k", okx_secret_key="s", okx_passphrase="p",
                                    trading_pairs=["BTC-USDC"], trading_required=False)
        self.records = []
        handler = logging.Handler()
        handler.emit = self.records.append
        self.exchange.logger().addHandler(handler)
        self.addCleanup(self.exchange.logger().removeHandler, handler)

    async def test_instrument_listed_since_the_last_refresh_gets_a_rule_without_an_error(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(
            {"data": [instrument("BTC-USDC", "BTC", "USDC"), instrument("XTSLL-USDT", "XTSLL", "USDT", "preopen")]})
        payload = {"data": [instrument("BTC-USDC", "BTC", "USDC"), instrument("XTSLL-USDT", "XTSLL", "USDT")]}
        self.exchange._api_get = AsyncMock(return_value=payload)  # OKX's own rules request; no network
        await self.exchange._update_trading_rules()
        self.exchange._api_get.assert_awaited_once()
        self.assertEqual([r for r in self.records if r.levelno >= logging.ERROR], [])
        self.assertEqual(set(self.exchange._trading_rules), {"BTC-USDC", "XTSLL-USDT"})
        self.assertEqual(await self.exchange.trading_pair_associated_to_exchange_symbol("XTSLL-USDT"), "XTSLL-USDT")
