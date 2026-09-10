from decimal import Decimal
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

import pandas as pd

from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.data_feed.candles_feed.okx_spot_candles.okx_spot_candles import OKXSpotCandles
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy.strategy_v2_base import StrategyV2Base


class NativeMarketDataHealthTest(IsolatedAsyncioTestCase):
    def setUp(self):
        self.provider = MarketDataProvider({})
        self.connector = SimpleNamespace(
            name="okx", _get_last_traded_price=AsyncMock(return_value=100),
            last_traded_price_source_timestamp=lambda pair: 120.0,
        )

    def health(self, now=125):
        return self.provider.get_market_data_health("okx", "BTC-USDC", "1m", observed_at=now)

    async def test_receipt_is_captured_at_request_completion_and_never_refreshed_by_snapshot(self):
        with patch("hummingbot.data_feed.market_data_provider.time.time", return_value=123), \
                patch("hummingbot.data_feed.market_data_provider.time.monotonic", return_value=30):
            self.assertEqual(Decimal("100"), await self.provider._safe_get_last_traded_price(self.connector, "BTC-USDC"))
        with patch("hummingbot.data_feed.market_data_provider.time.monotonic", return_value=31):
            price = self.health()["price"]
        self.assertEqual("1970-01-01T00:02:00+00:00", price["source_at"])
        self.assertEqual("last_traded_ticker", price["source"])
        self.assertEqual("1970-01-01T00:02:03+00:00", price["received_at"])
        with patch("hummingbot.data_feed.market_data_provider.time.monotonic", return_value=200):
            stale = self.health(now=300)["price"]
        self.assertEqual("stale", stale["availability"])
        self.assertEqual(price["received_at"], stale["received_at"])
        self.assertEqual(price["source_at"], stale["source_at"])

    async def test_failed_or_invalid_quote_clears_previous_receipt(self):
        for bad in [ValueError("offline"), float("nan"), 0, -1]:
            with self.subTest(bad=bad):
                self.connector._get_last_traded_price = AsyncMock(return_value=100)
                await self.provider._safe_get_last_traded_price(self.connector, "BTC-USDC")
                self.connector._get_last_traded_price = (AsyncMock(side_effect=bad) if isinstance(bad, Exception)
                                                         else AsyncMock(return_value=bad))
                await self.provider._safe_get_last_traded_price(self.connector, "BTC-USDC")
                price = self.health()["price"]
                self.assertEqual("unavailable", price["availability"])
                self.assertIsNone(price["received_at"])
                self.assertIsNone(price["source_at"])

    def test_missing_price_and_candles_remain_unavailable(self):
        value = self.health()
        self.assertIsNone(value["price"]["received_at"])
        self.assertIsNone(value["price"]["source_at"])
        self.assertIsNone(value["candle"]["source_at"])
        self.assertIsNone(value["candle"]["interval_seconds"])

    async def test_absent_source_timestamp_does_not_reuse_host_receipt(self):
        self.connector.last_traded_price_source_timestamp = lambda pair: None
        with patch("hummingbot.data_feed.market_data_provider.time.time", return_value=123):
            await self.provider._safe_get_last_traded_price(self.connector, "BTC-USDC")
        self.assertIsNone(self.health()["price"]["source_at"])
        self.assertEqual("1970-01-01T00:02:03+00:00", self.health()["price"]["received_at"])

    async def test_okx_keeps_validated_source_timestamp_without_changing_price(self):
        connector = OkxExchange.__new__(OkxExchange)
        connector.exchange_symbol_associated_to_pair = AsyncMock(return_value="BTC-USDC")
        connector._api_request = AsyncMock(return_value={"data": [{"last": "100", "ts": "120000"}]})
        self.assertEqual(100.0, await connector._get_last_traded_price("BTC-USDC"))
        self.assertEqual(120.0, connector.last_traded_price_source_timestamp("BTC-USDC"))
        for timestamp in [None, "bad", "NaN", "Infinity", "-1"]:
            connector._api_request.return_value = {"data": [{"last": "100", "ts": timestamp}]}
            self.assertEqual(100.0, await connector._get_last_traded_price("BTC-USDC"))
            self.assertIsNone(connector.last_traded_price_source_timestamp("BTC-USDC"))

    def test_confirmed_candle_uses_source_open_time_and_actual_interval(self):
        feed = OKXSpotCandles("BTC-USDC", interval="1m")
        feed._candles.extend([[60, 1, 1, 1, 1, 1, 1, 0, 0, 0], [120, 1, 1, 1, 1, 1, 1, 0, 0, 0]])
        self.provider.candles_feeds["okx_BTC-USDC_1m"] = feed
        candle = self.health(now=180)["candle"]
        self.assertEqual("1970-01-01T00:02:00+00:00", candle["source_at"])
        self.assertEqual(60, candle["interval_seconds"])
        self.assertEqual("okx", candle["connector"])
        self.assertEqual("BTC-USDC", candle["pair"])
        self.assertEqual("open", candle["timestamp_semantics"])
        self.assertTrue(candle["closed"])
        self.assertIsNone(candle["received_at"])
        self.assertEqual(candle["source_at"], candle["last_expected_closed_at"])

    def test_unproven_candle_semantics_cannot_be_declared_confirmed(self):
        self.provider.candles_feeds["okx_BTC-USDC_1m"] = SimpleNamespace(
            interval="1m", interval_in_seconds=60,
            candles_df=pd.DataFrame({"timestamp": [120]}))
        self.assertIsNone(self.health(now=180)["candle"]["closed"])
        self.assertIsNone(self.health(now=180)["candle"]["source_at"])

    def test_runtime_operations_uses_controller_source_namespace_without_starting_feeds(self):
        provider = self.provider
        config = SimpleNamespace(connector_name="okx", trading_pair="BTC-USDC", interval="1m",
                                 candles_connector="okx", candles_trading_pair="BTC-USDC")
        strategy = SimpleNamespace(current_timestamp=180, market_data_provider=provider,
                                   controllers={"btc": SimpleNamespace(config=config)})
        strategy._runtime_reconciliation_observation = lambda: StrategyV2Base._runtime_reconciliation_observation(strategy)
        with patch.object(provider, "get_candles_feed", side_effect=AssertionError("no new feeds")):
            operations = StrategyV2Base._build_runtime_operations_snapshot(strategy)
        self.assertEqual(1, operations["schema_version"])
        self.assertEqual("BTC-USDC", operations["pairs"][0]["pair"])
        self.assertIsNone(operations["pairs"][0]["price"]["source_at"])
        self.assertEqual("1970-01-01T00:03:00+00:00", operations["observed_at"])

    def test_reconciliation_reports_concrete_records_without_claiming_global_clearance(self):
        strategy = SimpleNamespace(
            executor_orchestrator=SimpleNamespace(recovery_required={"btc": ["entry"]}, active_executors={}),
            controllers={"btc": SimpleNamespace(_unresolved_executor_result_ids={"entry", "receipt"})})
        value = StrategyV2Base._runtime_reconciliation_observation(strategy)
        self.assertEqual("pending", value["state"])
        self.assertEqual(2, value["pending_count"])
        self.assertEqual("partial", value["coverage"])
        self.assertEqual(["startup_recovery", "unresolved_result_receipts"], value["pending"][0]["reasons"])
        strategy.executor_orchestrator.recovery_required = {}
        strategy.controllers["btc"]._unresolved_executor_result_ids.clear()
        value = StrategyV2Base._runtime_reconciliation_observation(strategy)
        self.assertEqual("unavailable", value["state"])
        self.assertEqual(0, value["pending_count"])

    def test_expired_empty_entry_proof_is_pending_but_normal_filled_owner_is_not(self):
        executor = SimpleNamespace(config=SimpleNamespace(id="entry"), _open_order_entry_expired=True,
                                   _open_order_expiry_reconciled=False, open_filled_amount=Decimal("0"))
        strategy = SimpleNamespace(executor_orchestrator=SimpleNamespace(
            recovery_required={}, active_executors={"btc": [executor]}), controllers={})
        self.assertEqual(1, StrategyV2Base._runtime_reconciliation_observation(strategy)["pending_count"])
        executor.open_filled_amount = Decimal("0.2")
        self.assertEqual(0, StrategyV2Base._runtime_reconciliation_observation(strategy)["pending_count"])
        strategy.executor_orchestrator.recovery_required = None
        self.assertIsNone(StrategyV2Base._runtime_reconciliation_observation(strategy)["pending_count"])
