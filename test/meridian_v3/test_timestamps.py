"""F14: each retained timestamp carries its real meaning; decisions are unchanged."""
import tempfile
from dataclasses import replace
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.lifecycle_harness import Scenario, creates, rebound
from test.portfolio_v3.test_meridian_composition import observation
from test.portfolio_v3.test_portfolio import proposal
from unittest import IsolatedAsyncioTestCase

import pytest

from controllers.generic.meridian_v3_portfolio import BNBMRObservation

BTC = 'BTC-USDC'


@pytest.mark.parametrize('changes', [{'candle_open_ms': 1000}, {'candle_open_ms': 1001},
                                     {'feature_closed_at_ms': 1001}, {'feature_closed_at_ms': 899}])
def test_observation_requires_open_before_close_and_close_inside_event_and_availability(changes):
    base = observation(proposal(producer='bnb_p23_mr').opportunity)  # event 900, available 1000
    with pytest.raises(ValueError, match='observation binding'):
        BNBMRObservation(**{**base.model_dump(mode='json'), **changes})


class TimestampTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    async def asyncSetUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.s = Scenario(Path(self._directory.name))

    async def asyncTearDown(self):
        if not self.s.session._closed:
            await self.s.close()
        self._directory.cleanup()

    async def test_closed_candle_open_close_and_owner_read_are_distinct_and_named(self):
        s = self.s
        await s.step()
        receipt = await s.step(rsi={BTC: rebound()})
        self.assertEqual(len(creates(receipt, BTC)), 1)  # the decision itself is unchanged
        portfolio = s.controller.portfolio
        now_ms = s.now * 1000
        open_ms = s.features[BTC].candle_open * 1000
        self.assertEqual(portfolio.clock['decision_cutoff_ms'], now_ms)
        self.assertEqual(portfolio.clock['candles'][BTC],
                         {'open_ms': open_ms, 'closed_at_ms': open_ms + 60000, 'read_at_ms': now_ms})
        self.assertEqual(open_ms + 60000, now_ms)  # the harness candle closed exactly at the owner clock
        market = s.features[BTC]
        built = portfolio.observation(BTC, market, s.controller.contexts[BTC].runtime, now_ms + 5000)
        self.assertEqual((built.candle_open_ms, built.feature_closed_at_ms), (open_ms, open_ms + 60000))
        self.assertEqual(built.opportunity.primary.event_time_ms, open_ms + 60000)
        self.assertEqual(built.opportunity.primary.available_time_ms, now_ms + 5000)
        info = s.controller.get_custom_info()['clock']
        self.assertEqual(info['native_book_read_ms'], now_ms)
        self.assertIsNone(info['venue_book_event_ms'])
        self.assertEqual(info['venue_book_event'], 'UNAVAILABLE_FROM_NATIVE_ADAPTER')

    async def test_snapshot_marks_are_the_native_book_read_time(self):
        s = self.s
        await s.step()
        now_ms = s.now * 1000
        snapshot = s.controller.portfolio.snapshot(now_ms)
        self.assertEqual({(a.mark_event_ms, a.available_at_ms) for a in snapshot.assets}, {(now_ms, now_ms)})
        self.assertEqual(snapshot.native_available_at_ms, s.controller.portfolio.clock['native_book_read_ms'])

    async def test_a_candle_that_has_not_closed_is_not_an_observation(self):
        s = self.s
        await s.step()
        market = replace(s.features[BTC], candle_open=s.now)  # closes a minute after the owner clock
        with self.assertRaises(ValueError):
            s.controller.portfolio.observation(BTC, market, s.controller.contexts[BTC].runtime, s.now * 1000)
