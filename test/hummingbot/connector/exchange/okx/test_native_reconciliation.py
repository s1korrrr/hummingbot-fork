from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.okx import okx_constants as CONSTANTS
from hummingbot.connector.exchange.okx.native_reconciliation import verify_no_open_orders


class NativeReconciliationTest(IsolatedAsyncioTestCase):

    @staticmethod
    def connector(trading_pairs):
        connector = MagicMock()
        connector.trading_pairs = trading_pairs
        connector.exchange_symbol_associated_to_pair = AsyncMock(return_value="BTC-USDT")
        connector.logger.return_value = MagicMock()
        return connector

    async def test_rejects_open_spot_order_outside_configured_pairs(self):
        connector = self.connector(["BTC-USDT"])

        async def api_get(path_url, params, is_auth_required):
            if path_url == CONSTANTS.OKX_PENDING_ORDERS_PATH:
                data = [] if "instId" in params else [{"instId": "ETH-USDT", "ordId": "101"}]
                return {"code": "0", "data": data}
            return {"code": "0", "data": []}

        connector._api_get = AsyncMock(side_effect=api_get)

        self.assertFalse(await verify_no_open_orders(connector))
        pending_calls = [call.kwargs["params"] for call in connector._api_get.await_args_list
                         if call.kwargs["path_url"] == CONSTANTS.OKX_PENDING_ORDERS_PATH]
        self.assertTrue(any("instId" not in params for params in pending_calls))

    async def test_rejects_open_spot_algo_order_outside_configured_pairs(self):
        connector = self.connector(["BTC-USDT"])

        async def api_get(path_url, params, is_auth_required):
            if path_url == CONSTANTS.OKX_PENDING_ORDERS_PATH:
                return {"code": "0", "data": []}
            if path_url == CONSTANTS.OKX_PENDING_ALGO_ORDERS_PATH and "instId" not in params:
                if params.get("ordType") == "conditional":
                    return {"code": "0", "data": [{"instId": "ETH-USDT", "algoId": "202"}]}
            return {"code": "0", "data": []}

        connector._api_get = AsyncMock(side_effect=api_get)

        self.assertFalse(await verify_no_open_orders(connector))

    async def test_empty_account_wide_spot_state_is_clean_without_configured_pairs(self):
        connector = self.connector([])
        connector._api_get = AsyncMock(return_value={"code": "0", "data": []})

        self.assertTrue(await verify_no_open_orders(connector))
        requests = [(call.kwargs["path_url"], call.kwargs["params"])
                    for call in connector._api_get.await_args_list]
        self.assertEqual(len(requests), 8)
        self.assertEqual(requests[0][0], CONSTANTS.OKX_PENDING_ORDERS_PATH)
        self.assertEqual({params["ordType"] for path, params in requests[1:]
                          if path == CONSTANTS.OKX_PENDING_ALGO_ORDERS_PATH}, {
            "conditional", "oco", "trigger", "move_order_stop", "iceberg", "twap", "smart_iceberg",
        })
        for _, params in requests:
            self.assertEqual(params["instType"], "SPOT")
            self.assertNotIn("instId", params)
        connector.exchange_symbol_associated_to_pair.assert_not_awaited()
