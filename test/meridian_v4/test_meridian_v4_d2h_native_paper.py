"""Synthetic native paper checks that D2h's depth ladder sizes real paper orders (not economics)."""
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v4.test_meridian_v4_d2h_contract import config
from test.meridian_v4.test_meridian_v4_native_paper import BOOK, candles, frames, paper
from unittest import IsolatedAsyncioTestCase

from controllers.generic.modular_spot_core.bag import depth_slice_quote
from hummingbot.strategy_v2.paper_session import NativePaperSession

OWNER = 'modular_spot_meridian_v4_d2h_paper_bnb'


def with_high(frame, high, minutes_before_end):
    """One earlier closed candle reaching ``high`` inside the 24 h lookback; ATR(14) is unchanged."""
    frame = frame.copy()
    frame.loc[len(frame) - 1 - minutes_before_end, 'high'] = high
    return frame


class MeridianV4D2hNativePaperTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

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
