"""OK RSI opt-in to the shared candle service: config contract, routing, and identical controller input."""
import asyncio
import importlib
import random
import sys
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import yaml

from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
from hummingbot.data_feed.candles_feed.okx_service_candles import OKXServiceCandles
from hummingbot.data_feed.candles_feed.okx_service_candles.service_hub import reset_service_candles
from hummingbot.data_feed.candles_feed.okx_spot_candles import OKXSpotCandles
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.components.candle_stream import CandleStreamClient

ROOT_PATH = Path(__file__).resolve().parent.parent
FIXTURES_PATH = ROOT_PATH / "test" / "fixtures" / "ok_rsi" / "controllers"
PAIR = "BNB-USDC"
DATA = {"mode": "shared", "transport": "stream", "origin": "http://127.0.0.1:8093", "token_file": "/run/candles/token"}


def ensure_local_controller_imports():
    while str(ROOT_PATH) in sys.path:
        sys.path.remove(str(ROOT_PATH))
    sys.path.insert(0, str(ROOT_PATH))
    for module_name in list(sys.modules):
        if module_name == "controllers" or module_name.startswith("controllers."):
            sys.modules.pop(module_name, None)
    importlib.invalidate_caches()


def raw_yaml(name="ok_rsi_bnb.yml"):
    return yaml.safe_load((FIXTURES_PATH / name).read_text())


def okx_rows(count, *, last_open, seed=7):
    """Deterministic confirmed OKX history-candles arrays (oldest first), including zero-volume minutes."""
    rng = random.Random(seed)
    price, rows = 775.0, []
    for index in range(count):
        open_ = price
        close = round(max(1.0, open_ + rng.uniform(-0.6, 0.6)), 1)
        high = round(max(open_, close) + rng.choice([0, 0, 0.1, 0.3]), 1)
        low = round(min(open_, close) - rng.choice([0, 0, 0.1, 0.3]), 1)
        volume = 0 if rng.random() < 0.4 else round(rng.uniform(0.01, 30), 8)
        rows.append([str((last_open - (count - 1 - index) * 60) * 1000), f"{open_:.1f}", f"{high:.1f}",
                     f"{low:.1f}", f"{close:.1f}", f"{volume}", f"{volume}", f"{volume * close:.8f}", "1"])
        price = close
    return rows


def native_frame(rows, max_records):
    feed = OKXSpotCandles(PAIR, "1m", max_records)
    for row in feed._parse_rest_candles({"data": list(reversed(rows))}):
        feed._candles.append(np.array(row).astype(float))
    return feed.candles_df


def native_provider(rows, max_records):
    """Real provider holding a native OKX feed preloaded with `rows`, as the live feed holds them (never started)."""
    provider = MarketDataProvider({})
    feed = OKXSpotCandles(PAIR, "1m", max_records)
    for row in feed._parse_rest_candles({"data": list(reversed(rows))}):
        feed._candles.append(np.array(row).astype(float))
    provider.candles_feeds[f"okx_{PAIR}_1m"] = feed
    return provider


class FakeClient(CandleStreamClient):
    def start(self):
        self.begin_session()

    async def close(self):
        self.replica.disconnect()


class ConfigContract(unittest.TestCase):
    def setUp(self):
        ensure_local_controller_imports()

    def config(self, **changes):
        from controllers.directional_trading.ok_rsi import OKRSIControllerConfig
        return OKRSIControllerConfig(**{**raw_yaml(), **changes})

    def test_shipped_files_are_unchanged_native(self):
        for name in ("ok_rsi_bnb.yml", "ok_rsi_btc.yml", "ok_rsi_eth.yml", "ok_rsi_sol.yml", "ok_rsi_xrp.yml"):
            config = self.config(**{k: v for k, v in raw_yaml(name).items()})
            self.assertIsNone(config.data)
            self.assertFalse(config.uses_candle_service)
            self.assertEqual(config.candles_connector, "okx")

    def test_selector_alias_and_data_block_enable_the_service_and_keep_the_okx_namespace(self):
        config = self.config(candles_connector="okx_service", data=DATA)
        self.assertTrue(config.uses_candle_service)
        self.assertEqual(config.candles_connector, "okx")
        self.assertEqual(config.data.origin, DATA["origin"])
        # The normalized dump is itself a valid selection (re-load, update_config copies).
        again = self.config(**{k: v for k, v in config.model_dump(mode="json").items() if k in raw_yaml()
                               or k in ("data", "candles_connector")})
        self.assertTrue(again.uses_candle_service)
        self.assertEqual(again.candles_connector, "okx")

    def test_data_block_alone_selects_the_service(self):
        self.assertTrue(self.config(data=DATA).uses_candle_service)

    def test_alias_without_data_is_refused(self):
        with self.assertRaises(ValueError):
            self.config(candles_connector="okx_service")

    def test_data_must_be_a_shared_native_stream_without_symbols(self):
        for bad in ({**DATA, "mode": "native", "origin": None, "token_file": None},
                    {**DATA, "transport": "snapshot"}, {**DATA, "symbols": ["BNB-USDC"]},
                    {**DATA, "source_profile": "custom"}):
            with self.assertRaises(ValueError):
                self.config(data=bad)

    def test_data_origin_and_token_file_are_validated_like_v2(self):
        for bad in ({**DATA, "origin": "http://example.com:8093"}, {**DATA, "token_file": "relative/token"},
                    {**DATA, "token_file": None}):
            with self.assertRaises(ValueError):
                self.config(data=bad)

    def test_other_candle_connectors_stay_refused(self):
        with self.assertRaises(ValueError):
            self.config(candles_connector="binance")

    def test_data_is_not_updatable_at_runtime(self):
        field = type(self.config()).model_fields["data"]
        self.assertEqual(field.json_schema_extra, {"is_updatable": False})


class ControllerRouting(unittest.IsolatedAsyncioTestCase):
    NOW = 1_790_933_600
    LAST_OPEN = 1_790_933_520

    def setUp(self):
        ensure_local_controller_imports()
        reset_service_candles()
        self.addCleanup(reset_service_candles)

    def controller(self, provider, **changes):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
        provider.initialize_rate_sources = lambda *args, **kwargs: None
        provider.time = lambda: float(self.NOW)
        return OKRSIController(config=OKRSIControllerConfig(**{**raw_yaml(), **changes}),
                               market_data_provider=provider, actions_queue=asyncio.Queue())

    def service_provider(self, rows):
        """Controller on the real provider, routed to a service stream carrying `rows`."""
        from hummingbot.data_feed.candles_feed.okx_service_candles import service_hub
        original = service_hub.ServiceCandleHub.__init__

        def init(hub, policy, **kwargs):
            original(hub, policy, **{**kwargs, "clock": lambda: float(self.NOW), "client_factory": FakeClient,
                                     "coalesce_seconds": 0})
        patcher = patch.object(service_hub.ServiceCandleHub, "__init__", init)
        patcher.start()
        self.addCleanup(patcher.stop)
        provider = MarketDataProvider({})
        self.addCleanup(provider.stop)
        controller = self.controller(provider, candles_connector="okx_service", data=DATA)
        return controller, provider

    async def deliver(self, rows):
        from hummingbot.data_feed.candles_feed.okx_service_candles.service_hub import get_service_hub
        for _ in range(10):
            await asyncio.sleep(0)
        hub = get_service_hub()
        for pair in hub.client.by_pair:
            hub.client.apply(dict(v=1, type="snapshot", epoch="e", published_at=float(self.NOW), pair=pair,
                                  sequence=1, received_at=float(self.NOW),
                                  rows=[dict(open_time=int(r[0]) // 1000, open=r[1], high=r[2], low=r[3], close=r[4],
                                             volume=r[5], quote_volume=r[7], confirmed=True) for r in rows]))

    async def test_default_off_controller_builds_the_native_feed(self):
        provider = MarketDataProvider({})
        self.addCleanup(provider.stop)
        controller = self.controller(provider)
        self.assertEqual(CandlesFactory._routes, {})
        with patch.object(OKXSpotCandles, "start", lambda self: None):
            controller.initialize_candles()
        self.assertEqual(list(provider.candles_feeds), [f"okx_{PAIR}_1m"])
        self.assertIsInstance(provider.candles_feeds[f"okx_{PAIR}_1m"], OKXSpotCandles)

    async def test_opted_in_controller_reads_the_service_under_the_okx_identity(self):
        rows = okx_rows(3660 + 40, last_open=self.LAST_OPEN)
        controller, provider = self.service_provider(rows)
        with patch.object(OKXSpotCandles, "start", side_effect=AssertionError("native OKX feed started")):
            controller.initialize_candles()
        key = f"okx_{PAIR}_1m"
        self.assertEqual(list(provider.candles_feeds), [key])
        feed = provider.candles_feeds[key]
        self.assertIsInstance(feed, OKXServiceCandles)
        self.assertEqual(feed.max_records, controller._derived_base_max_records())
        await self.deliver(rows[-3660:])
        self.assertTrue(feed.ready)
        self.assertTrue(provider.ready)
        frame = provider.get_candles_df(connector_name=controller.config.candles_connector,
                                        trading_pair=controller.config.trading_pair,
                                        interval=controller.config.interval, max_records=controller.max_records)
        pd.testing.assert_frame_equal(frame, native_frame(rows[-3660:], feed.max_records).iloc[-controller.max_records:],
                                      check_exact=True)

    async def test_derived_timeframes_and_owner_reconstruction_read_identical_inputs(self):
        rows = okx_rows(3660 + 40, last_open=self.LAST_OPEN)
        controller, provider = self.service_provider(rows)
        controller.initialize_candles()
        await self.deliver(rows[-3660:])
        native = self.controller(native_provider(rows[-3660:], controller._derived_base_max_records()))
        for interval in (controller.config.interval, "5m", "15m"):
            service_frame = controller._get_interval_candles_df(interval)
            self.assertIsNotNone(service_frame)
            pd.testing.assert_frame_equal(service_frame, native._get_interval_candles_df(interval), check_exact=True)
        # The longest read V1 ever makes (owner reconstruction) fits the subscription.
        self.assertEqual(len(provider.get_candles_df("okx", PAIR, "1m", 3660)), 3660)

    async def test_signal_computation_is_identical_on_service_and_native_frames(self):
        rows = okx_rows(3660 + 40, last_open=self.LAST_OPEN)
        service_controller, _ = self.service_provider(rows)
        service_controller.initialize_candles()
        await self.deliver(rows[-3660:])
        native_controller = self.controller(
            native_provider(rows[-3660:], service_controller._derived_base_max_records()))
        await service_controller.update_processed_data()
        await native_controller.update_processed_data()
        a, b = service_controller.processed_data, native_controller.processed_data
        self.assertEqual(set(a), set(b))
        for key in a:
            if isinstance(a[key], pd.DataFrame):
                pd.testing.assert_frame_equal(a[key], b[key], check_exact=True)
            else:
                self.assertEqual(deepcopy(a[key]), deepcopy(b[key]), key)
        self.assertTrue(a["data_health"]["ready"], a["data_health"])


if __name__ == "__main__":
    unittest.main()
