"""Synthetic native paper lifecycle checks for V4, not historical economics or live receipts."""
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v4.test_meridian_v4_contract import config
from unittest import IsolatedAsyncioTestCase

import numpy as np
import pandas as pd

from controllers.generic.meridian_v4_recipe import PAIRS
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.strategy_v2.paper_session import NativePaperSession

PAIR = 'BNB-USDC'
OWNER = 'modular_spot_meridian_v4_paper_bnb'
BOOK = dict(trading_pair=PAIR, bids=[(99.5, 10)], asks=[(99.51, 10)])


def paper(pairs=(PAIR,), cash='5000'):
    rules = {pair: TradingRule(pair, min_order_size=D('.001'), min_price_increment=D('.01'),
                               min_base_amount_increment=D('.001'), min_notional_size=D('1')) for pair in pairs}
    market = NativePaperTradeExchange(rules, fee_schema=TradeFeeSchema(
        maker_percent_fee_decimal=D('.001'), taker_percent_fee_decimal=D('.002')), trade_participation=D('1'))
    for pair in pairs:
        market.set_balance(pair.split('-')[0], D('0'))
    market.set_balance('USDC', D(cash))
    market.tick(1000)
    for pair in pairs:
        market.set_order_book(pair, 1, 1000, [(99.5, 10)], [(99.51, 10)])
    return market


def frames(half_range):
    """Slow decline, then RSI(17) crosses 29 (Meridian V3's synthetic tape). Each candle spans
    close +/- half_range: 0.05 keeps ATR(14)/close near 0.001 (inside V4's 0.0012 ceiling), 0.1 near 0.002."""
    close = np.r_[100 + .05 * np.sin(np.arange(1400) / 10), np.linspace(100, 99.5, 100)]
    frame = pd.DataFrame(dict(timestamp=np.arange(510000, 600000, 60), open=close, high=close + half_range,
                              low=close - half_range, close=close, volume=np.ones(1500) * 10))
    rebound = dict(timestamp=600000, open=99.5, high=99.56 + half_range, low=99.5 - half_range, close=99.56, volume=10)
    return frame, pd.concat([frame, pd.DataFrame([rebound])], ignore_index=True)


def candles(frame, now, pair=PAIR):
    return dict(connector_name='okx', trading_pair=pair, interval='1m', frame=frame, available_at=now)


class MeridianV4NativePaperTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    async def step_rebound(self, session, half_range):
        before, rebound = frames(half_range)
        receipt = await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)],
                                     candles=[candles(before, 600000)])
        controller = session.controllers[OWNER]
        self.assertEqual(receipt['actions'], [], controller.runtime.reason)
        self.assertEqual(controller.runtime.reason, 'seeded')
        receipt = await session.step(600060, books=[dict(**BOOK, sequence=3, timestamp=600060)],
                                     candles=[candles(rebound, 600060)])
        return controller, receipt

    async def test_rebound_inside_the_atr_ceiling_rests_one_100_quote_entry_and_fills(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [config(execution_enabled=True)], Path(directory) / 'v4.sqlite')
            try:
                controller, receipt = await self.step_rebound(session, .05)
                self.assertEqual(len(receipt['actions']), 1, controller.runtime.reason)
                executor = session.executor_orchestrator.active_executors[OWNER][0]
                self.assertIs(type(executor), ModularPositionExecutor)
                order = executor._open_order.order
                self.assertLessEqual(order.price * order.amount * D('1.003'), D('100'))
                self.assertGreater(order.price * order.amount, D('99'))
                barrier = executor.config.triple_barrier_config
                self.assertIsNone(barrier.stop_loss)
                self.assertIsNotNone(barrier.trailing_stop)
                await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                        side=TradeType.SELL, price=D('99.49'), amount=D('.5'))])
                self.assertEqual(executor.open_filled_amount, D('.5'))
                self.assertEqual(session._connector.get_balance('BNB'), D('.5'))
                self.assertEqual(len(session.recorder.get_trades_for_config('native-paper-session')), 1)
                self.assertEqual(controller.get_custom_info()['strategy_name'], 'V4')
            finally:
                await session.close()

    async def test_the_same_rebound_above_the_atr_ceiling_places_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [config(execution_enabled=True)], Path(directory) / 'v4.sqlite')
            try:
                controller, receipt = await self.step_rebound(session, .1)
                self.assertEqual(receipt['actions'], [])
                self.assertEqual(controller.runtime.reason, 'acquisition_not_eligible')
                self.assertEqual(session._connector.in_flight_orders, {})
            finally:
                await session.close()

    async def test_five_stand_alone_owners_share_one_program_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            configs = [config(pair, execution_enabled=True) for pair in PAIRS]
            session = NativePaperSession(paper(PAIRS), configs, Path(directory) / 'v4.sqlite')
            try:
                for step, frame in enumerate(frames(.05)):
                    now = 600000 + 60 * step
                    receipt = await session.step(
                        now, books=[dict(**{**BOOK, 'trading_pair': pair}, sequence=step + 2, timestamp=now) for pair in PAIRS],
                        candles=[candles(frame, now, pair) for pair in PAIRS])
                self.assertEqual({a.controller_id for a in receipt['actions']}, {c.id for c in configs})
                self.assertEqual(len(session._connector.in_flight_orders), 5)
                budgets = {session.controllers[c.id].runtime.budget.key for c in configs}
                self.assertEqual(len(budgets), 1)
                for c in configs:
                    executor = session.executor_orchestrator.active_executors[c.id][0]
                    self.assertEqual((executor.config.controller_id, executor.config.trading_pair), (c.id, c.trading_pair))
            finally:
                await session.close()
