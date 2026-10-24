"""Synthetic native paper checks that D2h's depth ladder sizes real paper orders (not economics)."""
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v4.test_meridian_v4_d2h_contract import config
from test.meridian_v4.test_meridian_v4_native_paper import BOOK, PAIR, candles, frames, paper
from unittest import IsolatedAsyncioTestCase

from controllers.generic.modular_spot_core.bag import depth_slice_quote
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.paper_session import NativePaperSession

OWNER = 'modular_spot_meridian_v4_d2h_paper_bnb'


def with_high(frame, high, minutes_before_end):
    """One earlier closed candle reaching ``high`` inside the 24 h lookback; ATR(14) is unchanged."""
    frame = frame.copy()
    frame.loc[len(frame) - 1 - minutes_before_end, 'high'] = high
    return frame


class MeridianV4D2hNativePaperTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    async def test_restore_exposes_native_reconciliation_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'd2h.sqlite'
            session = NativePaperSession(paper(cash='200000'), [config(execution_enabled=True)], db)
            await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)],
                               candles=[candles(frames(.05)[0], 600000)])
            receipt = await session.step(600060, books=[dict(**BOOK, sequence=3, timestamp=600060)],
                                         candles=[candles(frames(.05)[1], 600060)])
            self.assertEqual(len(receipt['actions']), 1)
            amount = session.executor_orchestrator.active_executors[OWNER][0]._open_order.order.amount
            await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                    side=TradeType.SELL, price=D('99.49'), amount=amount)])
            await session.close()

            restored = NativePaperSession(paper(cash='200000'), [config(execution_enabled=True)], db, restore=True)
            try:
                evidence = restored.reconciliation_status()
                self.assertEqual(evidence['status'], 'verified')
                self.assertTrue(evidence['session_identity']['marker_matches'])
                self.assertFalse(evidence['interrupted_step'])
                self.assertEqual(evidence['unresolved_executors'], [])
                self.assertEqual(evidence['unresolved_orders'], [])
                self.assertEqual(evidence['inventory_consistency']['status'], 'verified')
                await restored.step(600062, books=[dict(**BOOK, sequence=4, timestamp=600062)],
                                    candles=[candles(frames(.05)[1], 600062)])
                self.assertEqual(restored.reconciliation_status()['status'], 'verified')
            finally:
                await restored.close()

    async def entry_quote(self, high=None):
        before, rebound = frames(.05)
        if high is not None:
            before, rebound = with_high(before, high, 600), with_high(rebound, high, 601)
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(cash='200000'), [config(execution_enabled=True)],
                                         Path(directory) / 'd2h.sqlite')
            try:
                await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)],
                                   candles=[candles(before, 600000)])
                receipt = await session.step(600060, books=[dict(**BOOK, sequence=3, timestamp=600060)],
                                             candles=[candles(rebound, 600060)])
                controller = session.controllers[OWNER]
                self.assertEqual(len(receipt['actions']), 1, controller.runtime.reason)
                order = session.executor_orchestrator.active_executors[OWNER][0]._open_order.order
                self.assertIsNone(session.executor_orchestrator.active_executors[OWNER][0]
                                  .config.triple_barrier_config.stop_loss)
                self.assertEqual(controller.get_custom_info()['strategy_name'], 'V4 D2h')
                return order.price * order.amount
            finally:
                await session.close()

    async def test_shallow_rebound_plans_about_the_50_quote_base_slice(self):
        quote = await self.entry_quote()
        self.assertGreater(quote, D('49'))
        self.assertLess(quote, D('55'))

    async def test_rebound_17_percent_below_the_24h_high_plans_the_depth_scaled_slice(self):
        quote = await self.entry_quote(high=120)
        depth = 1 - D('99.5') / 120
        expected = depth_slice_quote(config().depth_budget, D('50'), depth)
        self.assertGreater(expected, D('180'))
        self.assertLessEqual(quote * D('1.003'), expected * D('1.01'))
        self.assertGreater(quote, expected * D('.97'))
