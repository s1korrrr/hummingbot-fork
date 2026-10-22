"""Replay memoization keeps decisions identical while removing per-second recomputation.

Every tick of a one-second replay used to re-hash the whole source closure and recompute
candle features that only change once per candle. These tests pin that the reuse is keyed
on real change signals: a new candle publication, an interval boundary, staleness, or a
configuration mutation still invalidate it; an unchanged second does not recompute.
"""
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.connector.exchange.paper_trade.test_native_paper_trade_exchange import paper
from test.hummingbot.strategy_v2.paper import test_paper_session
from test.hummingbot.strategy_v2.paper.test_modular_spot_research_paper import (
    BOOK,
    OWNER,
    candle,
    rebound_frames,
    research_config,
)
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from controllers.generic import modular_spot_research
from controllers.generic.modular_spot_core import runtime as runtime_module
from hummingbot.strategy_v2.paper_session import NativePaperSession


class Counting:
    def __init__(self, target):
        self.target, self.calls = target, 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        return self.target(*args, **kwargs)


class ModularSpotReplayMemoizationTests(IsolatedAsyncioTestCase):
    setUp = test_paper_session.NativePaperSessionTests.setUp

    def test_source_manifest_is_hashed_once_but_configuration_changes_still_fingerprint(self):
        counting = Counting(modular_spot_research.source_records)
        with patch.object(modular_spot_research, 'source_records', counting):
            config = research_config()
            first = config.fingerprint()
            self.assertEqual(config.fingerprint(), first)
            self.assertEqual(config.fingerprint(), first)
            self.assertEqual(counting.calls, 1)
            config.total_amount_quote = D('50')
            self.assertNotEqual(config.fingerprint(), first)
            self.assertEqual(counting.calls, 1)
            self.assertEqual(research_config().fingerprint(), first)
            self.assertEqual(counting.calls, 2)  # a distinct instance hashes its own closure once

    async def test_unchanged_seconds_reuse_the_published_observation(self):
        counting = Counting(runtime_module.features)
        frames = rebound_frames()
        with tempfile.TemporaryDirectory() as directory, patch.object(runtime_module, 'features', counting):
            session = NativePaperSession(paper(), [research_config()], Path(directory) / 'paper.sqlite')
            try:
                await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)], candles=[candle(frames[0], 600000)])
                controller = session.controllers[OWNER]
                self.assertTrue(controller.processed_data['data_health']['ready'], controller.runtime.reason)
                computed = counting.calls
                self.assertEqual(computed, 1)  # update_processed_data and actions share one computation
                first = (controller.runtime.market, dict(controller.runtime.closed_rows), controller.runtime.input_identity)
                for second in (600001, 600002, 600059):
                    await session.step(second, books=[dict(**BOOK, sequence=second - 599998, timestamp=second)])
                    self.assertTrue(controller.processed_data['data_health']['ready'], controller.runtime.reason)
                    self.assertEqual((controller.runtime.market, controller.runtime.closed_rows, controller.runtime.input_identity), first)
                self.assertEqual(counting.calls, computed)
                # A new publication of the next candle recomputes exactly once.
                receipt = await session.step(600060, books=[dict(**BOOK, sequence=62, timestamp=600060)], candles=[candle(frames[1], 600060)])
                self.assertEqual(counting.calls, computed + 1)
                self.assertEqual(len(receipt['actions']), 1, controller.runtime.reason)
                self.assertNotEqual(controller.runtime.input_identity, first[2])
            finally:
                await session.close()

    async def test_interval_boundary_without_publication_recomputes_the_same_observation(self):
        counting = Counting(runtime_module.features)
        frames = rebound_frames()
        with tempfile.TemporaryDirectory() as directory, patch.object(runtime_module, 'features', counting):
            session = NativePaperSession(paper(), [research_config()], Path(directory) / 'paper.sqlite')
            try:
                await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)], candles=[candle(frames[0], 600000)])
                controller = session.controllers[OWNER]
                before = counting.calls
                first = (controller.runtime.market, dict(controller.runtime.closed_rows), controller.runtime.input_identity)
                await session.step(600059, books=[dict(**BOOK, sequence=3, timestamp=600059)])
                self.assertEqual(counting.calls, before)
                # The interval boundary is a conservative invalidation: the published frame is
                # unchanged, so the recomputed observation must equal the reused one.
                await session.step(600060, books=[dict(**BOOK, sequence=4, timestamp=600060)])
                self.assertEqual(counting.calls, before + 1)
                self.assertEqual((controller.runtime.market, controller.runtime.closed_rows, controller.runtime.input_identity), first)
            finally:
                await session.close()

    async def test_stale_publication_is_not_reused_inside_its_interval(self):
        counting = Counting(runtime_module.features)
        frames = rebound_frames()
        with tempfile.TemporaryDirectory() as directory, patch.object(runtime_module, 'features', counting):
            session = NativePaperSession(paper(), [research_config(max_candle_age_seconds=30)], Path(directory) / 'paper.sqlite')
            try:
                await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)], candles=[candle(frames[0], 600000)])
                controller = session.controllers[OWNER]
                self.assertTrue(controller.processed_data['data_health']['ready'], controller.runtime.reason)
                await session.step(600030, books=[dict(**BOOK, sequence=3, timestamp=600030)])
                self.assertTrue(controller.processed_data['data_health']['ready'], controller.runtime.reason)
                reused = counting.calls
                await session.step(600031, books=[dict(**BOOK, sequence=4, timestamp=600031)])
                self.assertFalse(controller.processed_data['data_health']['ready'])
                self.assertIn('stale candles', controller.runtime.reason)
                self.assertGreater(counting.calls, reused)
            finally:
                await session.close()

    async def test_revised_publication_with_identical_timestamps_recomputes(self):
        counting = Counting(runtime_module.features)
        frames = rebound_frames()
        with tempfile.TemporaryDirectory() as directory, patch.object(runtime_module, 'features', counting):
            session = NativePaperSession(paper(), [research_config()], Path(directory) / 'paper.sqlite')
            try:
                await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)], candles=[candle(frames[0], 600000)])
                controller = session.controllers[OWNER]
                identity = controller.runtime.input_identity
                before = counting.calls
                revised = frames[0].copy()
                revised.loc[revised.index[-1], 'volume'] = 11.0  # same timestamps, different closed bar
                await session.step(600001, books=[dict(**BOOK, sequence=3, timestamp=600001)], candles=[candle(revised, 600001)])
                self.assertEqual(counting.calls, before + 1)
                self.assertNotEqual(controller.runtime.input_identity, identity)
            finally:
                await session.close()

    def test_provider_without_publication_identity_recomputes_every_observation(self):
        import asyncio
        from test.modular_spot.test_native_lifecycle import config as deployment_config
        from types import SimpleNamespace

        from controllers.generic.modular_spot import ModularSpotController
        counting = Counting(runtime_module.features)
        frame = rebound_frames()[0]
        provider = SimpleNamespace(ready=True, time=lambda: 600000, connectors={},
                                   get_candles_df=lambda *args, **kwargs: frame.copy())
        with patch.object(runtime_module, 'features', counting):
            controller = ModularSpotController(deployment_config(execution_enabled=True), provider, asyncio.Queue())
            self.assertIsNotNone(controller.runtime.observe())
            self.assertIsNotNone(controller.runtime.observe())
            self.assertEqual(counting.calls, 2)

    async def test_recorder_reads_are_shared_inside_one_decision_only(self):
        frames = rebound_frames()
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [research_config()], Path(directory) / 'paper.sqlite')
            try:
                await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)], candles=[candle(frames[0], 600000)])
                runtime = session.controllers[OWNER].runtime
                self.assertIsNone(runtime._decision_scope)
                executors = Counting(session.recorder.get_all_executors)
                by_controller = Counting(session.recorder.get_executors_by_controller)
                with patch.object(session.recorder, 'get_all_executors', executors), \
                        patch.object(session.recorder, 'get_executors_by_controller', by_controller):
                    runtime._decision_scope = {}
                    try:
                        self.assertIs(runtime._native_executors(), runtime._native_executors())
                        self.assertIs(runtime.inventory(), runtime.inventory())
                        self.assertEqual((executors.calls, by_controller.calls), (1, 1))
                    finally:
                        runtime._decision_scope = None
                    runtime._native_executors()
                    runtime.inventory()
                    self.assertEqual((executors.calls, by_controller.calls), (2, 2))
                receipt = await session.step(600060, books=[dict(**BOOK, sequence=3, timestamp=600060)], candles=[candle(frames[1], 600060)])
                self.assertEqual(len(receipt['actions']), 1, runtime.reason)
                self.assertIsNone(runtime._decision_scope)
            finally:
                await session.close()
