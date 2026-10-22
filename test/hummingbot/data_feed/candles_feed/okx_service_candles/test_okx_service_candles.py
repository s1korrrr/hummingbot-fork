import asyncio
import json
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.data_feed.candles_feed.okx_service_candles import OKXServiceCandles, constants as CONSTANTS
from hummingbot.data_feed.candles_feed.okx_service_candles.service_hub import (
    ServiceCandleHub,
    configure_service_candles,
    get_service_hub,
    reset_service_candles,
)
from hummingbot.data_feed.candles_feed.okx_spot_candles import OKXSpotCandles
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.components.candle_stream import CandleStreamClient
from hummingbot.strategy_v2.components.data_reader import DataPolicy
from hummingbot.strategy_v2.components.domain import PolicyError

FIXTURE = Path(__file__).resolve().parents[5] / "test" / "fixtures" / "okx_service_candles" / "bnb_usdc_1m_okx_rows.json"
PAIR = "BNB-USDC"
HISTORY = 60


def policy():
    return DataPolicy(mode="shared", transport="stream", origin="http://127.0.0.1:8093",
                      token_file="/run/candles/token")


class FakeClient(CandleStreamClient):
    """The real wire-v1 state machine without a socket: tests deliver messages through `apply`."""

    def start(self):
        self.begin_session()

    async def close(self):
        self.replica.disconnect()


class Wire:
    """Builds wire-v1 messages from recorded OKX rows and delivers them to the hub's client."""

    def __init__(self, hub, now):
        self.hub, self.now, self.sequence = hub, now, {}

    @staticmethod
    def stream_row(okx_row, confirmed=True):
        return dict(open_time=int(okx_row[0]) // 1000, open=okx_row[1], high=okx_row[2], low=okx_row[3],
                    close=okx_row[4], volume=okx_row[5], quote_volume=okx_row[7], confirmed=confirmed)

    def envelope(self, kind, **fields):
        return dict(v=1, type=kind, epoch="test-epoch", published_at=self.now[0], **fields)

    def snapshot(self, pair, okx_rows_oldest_first):
        self.sequence[pair] = 1
        self.hub.client.apply(self.envelope(
            "snapshot", pair=pair, sequence=1, received_at=self.now[0],
            rows=[self.stream_row(row) for row in okx_rows_oldest_first]))

    def candle(self, pair, okx_row, confirmed=True):
        self.sequence[pair] += 1
        self.hub.client.apply(self.envelope(
            "candle", pair=pair, sequence=self.sequence[pair], received_at=self.now[0],
            row=self.stream_row(okx_row, confirmed)))

    def status_down(self, pair, reason="upstream disconnected"):
        self.sequence[pair] += 1
        self.hub.client.apply(self.envelope("status", pair=pair, sequence=self.sequence[pair], ready=False,
                                            reason=reason))

    def heartbeat(self):
        client = self.hub.client
        self.hub.client.apply(self.envelope(
            "heartbeat", states={pair: dict(ready=client.replica.snapshots.get(spec.key) is not None,
                                            sequence=client.sequences.get(pair))
                                 for pair, spec in client.by_pair.items()}))


def recorded_rows():
    """Recorded OKX history-candles arrays, oldest first."""
    return list(reversed(json.loads(FIXTURE.read_text())["rows"]))


def native_frame(okx_rows_oldest_first, max_records):
    """What the native OKXSpotCandles serves for the same confirmed rows (REST parser + deque + candles_df)."""
    feed = OKXSpotCandles(PAIR, "1m", max_records)
    parsed = feed._parse_rest_candles({"data": list(reversed(okx_rows_oldest_first))})
    for row in parsed:
        feed._candles.append(np.array(row).astype(float))
    return feed.candles_df


class ServiceFeedCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        reset_service_candles()
        rows = recorded_rows()
        self.rows = rows
        self.now = [int(rows[-1][0]) // 1000 + 61]
        self.hub = configure_service_candles(policy(), history=HISTORY, clock=lambda: self.now[0],
                                             client_factory=FakeClient, coalesce_seconds=0,
                                             idle_close_seconds=0.05)
        self.wire = Wire(self.hub, self.now)
        self.addCleanup(reset_service_candles)

    async def settle(self):
        for _ in range(10):
            await asyncio.sleep(0)

    async def feed(self, max_records=50, pair=PAIR):
        feed = OKXServiceCandles(pair, "1m", max_records)
        feed.start()
        self.addCleanup(feed.stop)
        await self.settle()
        return feed

    async def serve(self, feed, count=HISTORY, pair=PAIR):
        self.wire.snapshot(pair, self.rows[-count:])
        return feed


class TestFrameParityWithNativeFeed(ServiceFeedCase):
    async def test_frame_equals_the_native_feed_frame_exactly(self):
        feed = await self.feed(max_records=50)
        await self.serve(feed)
        expected = native_frame(self.rows[-HISTORY:], 50)
        actual = feed.candles_df
        self.assertEqual(list(actual.columns), CandlesBase.columns)
        self.assertEqual(list(actual.columns), list(expected.columns))
        self.assertTrue((actual.dtypes == np.float64).all())
        pd.testing.assert_frame_equal(actual, expected, check_exact=True)

    async def test_provider_slice_matches_native_for_the_controller_read(self):
        provider = MarketDataProvider({})
        feed = provider.get_candles_feed(CandlesConfig(connector="okx", trading_pair=PAIR, interval="1m",
                                                       max_records=HISTORY))
        self.assertIsInstance(feed, OKXServiceCandles)
        await self.settle()
        self.wire.snapshot(PAIR, self.rows[-HISTORY:])
        expected = native_frame(self.rows[-HISTORY:], HISTORY).iloc[-40:]
        actual = provider.get_candles_df("okx", PAIR, "1m", 40)
        pd.testing.assert_frame_equal(actual, expected, check_exact=True)
        self.assertTrue(provider.ready)
        provider.stop()

    async def test_ordering_zero_volume_bars_and_unused_columns(self):
        feed = await self.feed()
        await self.serve(feed)
        frame = feed.candles_df
        self.assertTrue(frame["timestamp"].is_monotonic_increasing)
        self.assertEqual(set(np.diff(frame["timestamp"])), {60.0})
        self.assertEqual(frame["timestamp"].iloc[-1], float(int(self.rows[-1][0]) // 1000))
        self.assertTrue((frame["volume"] == 0).any(), "fixture must keep OKX's zero-volume bars")
        self.assertTrue((frame[["n_trades", "taker_buy_base_volume", "taker_buy_quote_volume"]] == 0).all().all())

    async def test_frames_are_copies_the_caller_may_mutate(self):
        feed = await self.feed()
        await self.serve(feed)
        first = feed.candles_df
        first.loc[:, "close"] = -1.0
        pd.testing.assert_frame_equal(feed.candles_df, native_frame(self.rows[-HISTORY:], 50), check_exact=True)

    async def test_max_records_bounds_the_frame(self):
        feed = await self.feed(max_records=10)
        await self.serve(feed)
        self.assertEqual(len(feed.candles_df), 10)
        pd.testing.assert_frame_equal(feed.candles_df, native_frame(self.rows[-HISTORY:], 10), check_exact=True)


class TestClosedBarSemantics(ServiceFeedCase):
    async def test_new_confirmed_bar_is_served_and_a_correction_replaces_it(self):
        feed = await self.feed()
        history = self.rows[-HISTORY - 2:-2]
        self.wire.snapshot(PAIR, history)
        self.now[0] += 60
        self.wire.candle(PAIR, self.rows[-2])
        self.assertEqual(feed.candles_df["timestamp"].iloc[-1], float(int(self.rows[-2][0]) // 1000))
        corrected = list(self.rows[-2])
        corrected[4] = corrected[2]
        self.wire.candle(PAIR, corrected)
        frame = feed.candles_df
        self.assertEqual(len(frame), 50)
        self.assertEqual(frame["close"].iloc[-1], float(corrected[2]))
        pd.testing.assert_frame_equal(frame, native_frame(history[1:] + [corrected], 50), check_exact=True)

    async def test_a_forming_bar_cannot_enter_the_feed(self):
        feed = await self.feed()
        self.wire.snapshot(PAIR, self.rows[-HISTORY - 1:-1])
        self.assertTrue(feed.ready)
        with self.assertRaises(ValueError):
            self.wire.candle(PAIR, self.rows[-1], confirmed=False)
        self.assertFalse(feed.ready)
        self.assertTrue(feed.candles_df.empty)
        self.assertIn("disconnected", feed.unavailable_reason)

    async def test_no_bar_newer_than_the_last_confirmed_one(self):
        feed = await self.feed()
        await self.serve(feed)
        self.assertTrue(feed.closed_candles_only)
        last_open = float(feed.candles_df["timestamp"].iloc[-1])
        self.assertLessEqual(last_open + 60, self.now[0])

    async def test_stale_candles_are_served_for_the_controller_to_judge(self):
        # The native feed has no age check; the controller's data_health owns the age veto. Transport staleness is
        # the feed's concern and is covered below.
        feed = await self.feed()
        await self.serve(feed)
        self.now[0] += 5
        self.assertTrue(feed.ready)
        self.assertEqual(len(feed.candles_df), 50)


class TestUnavailableAndReconnect(ServiceFeedCase):
    async def test_not_ready_with_empty_native_shaped_frame_before_the_first_snapshot(self):
        feed = await self.feed()
        self.assertFalse(feed.ready)
        self.assertIn("not yet received", feed.unavailable_reason)
        empty = feed.candles_df
        self.assertTrue(empty.empty)
        pd.testing.assert_frame_equal(empty, OKXSpotCandles(PAIR, "1m", 50).candles_df)
        self.assertEqual(await feed.check_network(), NetworkStatus.NOT_CONNECTED)

    async def test_stream_loss_clears_the_feed_and_reconnect_restores_it(self):
        feed = await self.feed()
        await self.serve(feed)
        self.assertTrue(feed.ready)
        self.assertEqual(await feed.check_network(), NetworkStatus.CONNECTED)
        self.hub.client.replica.disconnect()
        self.assertFalse(feed.ready)
        self.assertTrue(feed.candles_df.empty)
        self.assertIn("service candle stream disconnected", feed.unavailable_reason)
        self.hub.client.begin_session()
        self.assertFalse(feed.ready)
        self.wire.sequence.clear()
        self.wire.snapshot(PAIR, self.rows[-HISTORY:])
        self.assertTrue(feed.ready)
        pd.testing.assert_frame_equal(feed.candles_df, native_frame(self.rows[-HISTORY:], 50), check_exact=True)

    async def test_service_reported_outage_is_surfaced_not_papered_over(self):
        feed = await self.feed()
        await self.serve(feed)
        self.wire.status_down(PAIR, "upstream disconnected")
        self.assertFalse(feed.ready)
        self.assertEqual(feed.unavailable_reason, "service reports candles unavailable: upstream disconnected")
        self.assertTrue(feed.candles_df.empty)

    async def test_transport_staleness_withholds_rows(self):
        feed = await self.feed()
        await self.serve(feed)
        self.now[0] += 11
        self.assertFalse(feed.ready)
        self.assertEqual(feed.unavailable_reason, "service candle transport is stale")
        self.wire.heartbeat()
        self.assertTrue(feed.ready)

    async def test_unavailable_reason_is_logged_once_per_change(self):
        feed = await self.feed()
        with self.assertLogs(feed.logger(), level="INFO") as logs:
            for _ in range(3):
                feed.candles_df
            await self.serve(feed)
            feed.candles_df
            feed.candles_df
        messages = [record.getMessage() for record in logs.records]
        self.assertEqual(sum("unavailable" in message for message in messages), 1)
        self.assertEqual(sum("available | feed" in message and "unavailable" not in message
                             for message in messages), 1)

    async def test_client_construction_failure_is_reported(self):
        reset_service_candles()
        bad = DataPolicy(mode="shared", transport="stream", origin="http://127.0.0.1:8093",
                         token_file="/run/candles/token")

        def broken(*args, **kwargs):
            raise ValueError("stream requires unique bounded OKX spot 1m datasets")
        hub = configure_service_candles(bad, history=HISTORY, clock=lambda: self.now[0], client_factory=broken,
                                        coalesce_seconds=0)
        feed = OKXServiceCandles(PAIR, "1m", 50)
        feed.start()
        self.addCleanup(feed.stop)
        await self.settle()
        self.assertFalse(feed.ready)
        self.assertIn("service candle client unavailable", feed.unavailable_reason)
        self.assertIsNotNone(hub.error)

    async def test_supervisor_survives_an_unexpected_rebuild_failure(self):
        reset_service_candles()
        calls = []

        def flaky(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("unexpected")
            return FakeClient(*args, **kwargs)
        hub = configure_service_candles(policy(), history=HISTORY, clock=lambda: self.now[0], client_factory=flaky,
                                        coalesce_seconds=0)
        first = OKXServiceCandles(PAIR, "1m", 50)
        first.start()
        self.addCleanup(first.stop)
        await self.settle()
        self.assertIn("RuntimeError: unexpected", first.unavailable_reason)
        second = OKXServiceCandles("BTC-USDC", "1m", 50)
        second.start()
        self.addCleanup(second.stop)
        await self.settle()
        self.assertIsNone(hub.error)
        self.assertEqual(sorted(hub.client.by_pair), [PAIR, "BTC-USDC"])

    async def test_status_reports_the_shared_stream(self):
        feed = await self.feed()
        await self.serve(feed)
        status = feed.service_status()
        self.assertTrue(status["ready"])
        self.assertEqual(status["last_open_time"], int(self.rows[-1][0]) // 1000)
        self.assertEqual(status["hub"]["pairs"], [PAIR])
        self.assertEqual(status["hub"]["builds"], 1)


class TestNoOkxContact(ServiceFeedCase):
    async def test_start_network_opens_no_connection_and_no_listener(self):
        feed = await self.feed()
        with patch("hummingbot.core.web_assistant.web_assistants_factory.WebAssistantsFactory.get_ws_assistant",
                   side_effect=AssertionError("public websocket requested")), \
                patch("hummingbot.core.web_assistant.web_assistants_factory.WebAssistantsFactory.get_rest_assistant",
                      side_effect=AssertionError("public REST requested")):
            await feed.start_network()
            await self.settle()
            self.assertIsNone(feed._listen_candles_task)
            await feed.stop_network()

    async def test_every_exchange_path_is_refused(self):
        feed = await self.feed()
        for call in (feed.fetch_candles(start_time=1), feed.get_historical_candles(None),
                     feed.fill_historical_candles(), feed.listen_for_subscriptions()):
            with self.assertRaises(PolicyError):
                await call
        with self.assertRaises(PolicyError):
            feed.load_candles_from_csv("/nonexistent")

    async def test_unconfigured_process_cannot_build_a_service_feed(self):
        reset_service_candles()
        with self.assertRaises(PolicyError):
            OKXServiceCandles(PAIR, "1m", 50)
        with self.assertRaises(PolicyError):
            CandlesFactory.get_candle(CandlesConfig(connector="okx_service", trading_pair=PAIR, interval="1m"))
        with self.assertRaises(PolicyError):
            get_service_hub()

    async def test_oversized_request_raises_instead_of_falling_back_to_okx(self):
        provider = MarketDataProvider({})
        with self.assertRaises(PolicyError):
            provider.get_candles_df("okx", PAIR, "1m", HISTORY + 1)
        self.assertEqual(provider.candles_feeds, {})


class TestSharedStream(ServiceFeedCase):
    PAIRS = ("BNB-USDC", "BTC-USDC", "ETH-USDC", "SOL-USDC", "XRP-USDC")

    async def test_five_controllers_share_one_client_one_subscription(self):
        built = []

        def factory(*args, **kwargs):
            client = FakeClient(*args, **kwargs)
            built.append(client)
            return client
        reset_service_candles()
        self.hub = configure_service_candles(policy(), history=HISTORY, clock=lambda: self.now[0],
                                             client_factory=factory, coalesce_seconds=0)
        self.wire = Wire(self.hub, self.now)
        feeds = [OKXServiceCandles(pair, "1m", 50) for pair in self.PAIRS]
        for feed in feeds:
            feed.start()
            self.addCleanup(feed.stop)
        await self.settle()
        self.assertEqual(len(built), 1)
        self.assertEqual(sorted(built[0].by_pair), sorted(self.PAIRS))
        self.assertEqual(self.hub.builds, 1)
        for pair in self.PAIRS:
            self.wire.snapshot(pair, self.rows[-HISTORY:])
        self.assertTrue(all(feed.ready for feed in feeds))

    async def test_a_new_pair_rebuilds_once_and_other_feeds_recover_with_the_new_stream(self):
        first = await self.feed(pair="BNB-USDC")
        await self.serve(first)
        second = OKXServiceCandles("BTC-USDC", "1m", 50)
        second.start()
        self.addCleanup(second.stop)
        await self.settle()
        self.assertEqual(self.hub.builds, 2)
        self.assertFalse(first.ready)
        self.assertIn("not yet received", first.unavailable_reason)
        self.wire.sequence.clear()
        for pair in ("BNB-USDC", "BTC-USDC"):
            self.wire.snapshot(pair, self.rows[-HISTORY:])
        self.assertTrue(first.ready and second.ready)

    async def test_feed_replacement_does_not_tear_the_stream_down(self):
        old = await self.feed(max_records=20)
        await self.serve(old)
        old.stop()
        replacement = OKXServiceCandles(PAIR, "1m", 40)
        replacement.start()
        self.addCleanup(replacement.stop)
        await self.settle()
        self.assertEqual(self.hub.builds, 1)
        self.assertTrue(replacement.ready)
        self.assertEqual(len(replacement.candles_df), 40)

    async def test_stream_closes_after_the_last_feed_stops(self):
        feed = await self.feed()
        await self.serve(feed)
        client = self.hub.client
        feed.stop()
        await asyncio.sleep(0.15)
        self.assertIsNone(self.hub.client)
        self.assertFalse(client.replica.connected)

    async def test_start_and_stop_are_idempotent(self):
        feed = await self.feed()
        feed.start()
        await self.settle()
        feed.stop()
        feed.stop()
        self.assertFalse(feed.ready)


class TestBounds(unittest.TestCase):
    def setUp(self):
        reset_service_candles()
        self.addCleanup(reset_service_candles)

    def test_history_constant_covers_v1_reconstruction(self):
        from controllers.directional_trading.ok_rsi_core.context import HISTORY_RECORDS
        self.assertEqual(CONSTANTS.SERVICE_HISTORY, HISTORY_RECORDS)

    def test_feed_construction_bounds(self):
        configure_service_candles(policy(), history=HISTORY)
        with self.assertRaises(PolicyError):
            OKXServiceCandles(PAIR, "5m", 50)
        with self.assertRaises(PolicyError):
            OKXServiceCandles(PAIR, "1m", HISTORY + 1)
        with self.assertRaises(PolicyError):
            OKXServiceCandles(PAIR, "1m", 0)
        with self.assertRaises(PolicyError):
            OKXServiceCandles("BNB-USDT", "1m", 50)

    def test_hub_rejects_unbounded_or_native_configuration(self):
        for kwargs in ({"history": 59}, {"history": CONSTANTS.SERVICE_HISTORY + 1}, {"history": True}):
            with self.assertRaises(PolicyError):
                ServiceCandleHub(policy(), **kwargs)
        for bad in (DataPolicy(), DataPolicy(mode="shared", transport="snapshot", origin="http://127.0.0.1:8093",
                                             token_file="/run/candles/token")):
            with self.assertRaises(PolicyError):
                ServiceCandleHub(bad)

    def test_enabling_is_logged_once(self):
        with self.assertLogs(ServiceCandleHub.logger(), level="INFO") as logs:
            configure_service_candles(policy(), history=HISTORY)
            configure_service_candles(policy(), history=HISTORY)
        self.assertEqual(sum("Service candles enabled" in record.getMessage() for record in logs.records), 1)

    def test_one_process_one_stream_configuration(self):
        hub = configure_service_candles(policy(), history=HISTORY)
        self.assertIs(configure_service_candles(policy(), history=HISTORY), hub)
        other = DataPolicy(mode="shared", transport="stream", origin="http://127.0.0.1:8094",
                           token_file="/run/candles/token")
        with self.assertRaises(PolicyError):
            configure_service_candles(other, history=HISTORY)
        with self.assertRaises(PolicyError):
            configure_service_candles(policy(), history=HISTORY + 1)


class TestFactoryRegistration(unittest.TestCase):
    def setUp(self):
        reset_service_candles()
        self.addCleanup(reset_service_candles)

    def test_default_off_okx_is_the_native_feed(self):
        feed = CandlesFactory.get_candle(CandlesConfig(connector="okx", trading_pair=PAIR, interval="1m"))
        self.assertIsInstance(feed, OKXSpotCandles)

    def test_service_connector_is_registered(self):
        self.assertIs(CandlesFactory._candles_map[CONSTANTS.SERVICE_CONNECTOR], OKXServiceCandles)
        self.assertEqual(CONSTANTS.SERVICE_CONNECTOR, "okx_service")

    def test_opt_in_routes_okx_without_renaming_it(self):
        configure_service_candles(policy(), history=HISTORY)
        config = CandlesConfig(connector="okx", trading_pair=PAIR, interval="1m", max_records=50)
        self.assertEqual(config.connector, "okx")
        self.assertIsInstance(CandlesFactory.get_candle(config), OKXServiceCandles)
        explicit = CandlesConfig(connector="okx_service", trading_pair=PAIR, interval="1m", max_records=50)
        self.assertIsInstance(CandlesFactory.get_candle(explicit), OKXServiceCandles)
        perpetual = CandlesFactory.get_candle(CandlesConfig(connector="okx_perpetual", trading_pair="BTC-USDT",
                                                            interval="1m"))
        self.assertNotIsInstance(perpetual, OKXServiceCandles)
        reset_service_candles()
        self.assertIsInstance(CandlesFactory.get_candle(config), OKXSpotCandles)

    def test_other_connectors_are_never_routed(self):
        configure_service_candles(policy(), history=HISTORY)
        self.assertEqual(CandlesFactory._routes, {"okx": "okx_service"})
        with self.assertRaises(ValueError):
            CandlesFactory.route("okx", "binance")
        with self.assertRaises(Exception):
            CandlesFactory.route("okx", "unknown_connector")


if __name__ == "__main__":
    unittest.main()
