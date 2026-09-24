"""Component composition drives the native paper executor/recorder lifecycle."""
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.connector.exchange.paper_trade.test_native_paper_trade_exchange import PAIR, paper
from test.hummingbot.strategy_v2.paper.test_paper_session import NativePaperSessionTests
from unittest import IsolatedAsyncioTestCase

import numpy as np
import pandas as pd

from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
from controllers.generic.modular_spot_core.program_budget import BudgetPolicy
from controllers.generic.modular_spot_core.universe import UniversePolicy
from controllers.generic.modular_spot_research import ModularSpotResearchConfig
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.paper_session import NativePaperSession

OWNER = 'modular_spot_research_fixture'
BOOK = dict(trading_pair=PAIR, bids=[(95, 10)], asks=[(95.1, 10)])


def research_config(**overrides):
    values = dict(id=OWNER, trading_pair=PAIR, execution_enabled=True, total_amount_quote=D('100'),
                  budget=BudgetPolicy(group='research', max_quote=D('100'), max_assets=1), account='research')
    return ModularSpotResearchConfig(**{**values, **overrides})


def rebound_frames(now=600000):
    """Synthetic causal RSI(14) rebound across 30 on the final closed bar; not an economic dataset."""
    closes = np.r_[100 + np.sin(np.arange(575) / 10), np.linspace(100 + np.sin(57.4), 95, 25)]
    count = len(closes)
    frame = pd.DataFrame({'timestamp': np.arange(now - count * 60, now, 60), 'open': closes, 'high': closes + .1,
                          'low': closes - .1, 'close': closes, 'volume': np.ones(count) * 10})
    row = dict(timestamp=now, open=95.0, high=96.1, low=94.9, close=96.0, volume=10.0)
    return frame, pd.concat([frame, pd.DataFrame([row])], ignore_index=True)


def candle(frame, available_at):
    return dict(connector_name='okx', trading_pair=PAIR, interval='1m', frame=frame, available_at=available_at)


class ModularSpotResearchPaperTests(IsolatedAsyncioTestCase):
    setUp = NativePaperSessionTests.setUp

    def test_binding_admits_only_native_fixed_funded_paper_compositions(self):
        research_config()
        for overrides in (dict(data={'mode': 'shared'}), dict(universe=UniversePolicy(mode='sell_only'), acquisition_enabled=False),
                          dict(budget=None), dict(account='unassigned'), dict(connector_name='okx'),
                          dict(candles_connector='okx_paper_trade'), dict(execution_mode='live'), dict(research_only=False)):
            with self.subTest(overrides=str(overrides)), self.assertRaises(ValueError):
                research_config(**overrides)
        base = ModularSpotResearchConfig.__mro__[1](id='modular_spot_deploy', connector_name='okx', trading_pair=PAIR)
        self.assertEqual(len(research_config().source_manifest()), len(base.source_manifest()) + 1)

    async def test_session_rejects_deployment_config_and_foreign_connector(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'paper.sqlite'
            deployment = ModularSpotResearchConfig.__mro__[1](id='modular_spot_deploy', connector_name='okx', trading_pair=PAIR)
            with self.assertRaisesRegex(ValueError, 'paper-bound'):
                NativePaperSession(paper(), [deployment], db)
            with self.assertRaisesRegex(ValueError, 'native paper connector'):
                research_config().get_controller_class()(research_config(), type('P', (), {'connectors': {'okx_paper_trade': object()}})(), None)

    async def test_rebound_entry_partial_fill_native_fees_and_restart_without_duplicate_buy(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'paper.sqlite'
            session = NativePaperSession(paper(), [research_config()], db)
            frames = rebound_frames()
            try:
                seeded = await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)], candles=[candle(frames[0], 600000)])
                controller = session.controllers[OWNER]
                self.assertEqual(seeded['actions'], [])
                self.assertTrue(controller.processed_data['data_health']['ready'], controller.runtime.reason)
                self.assertEqual(controller.runtime.reason, 'seeded')
                receipt = await session.step(600060, books=[dict(**BOOK, sequence=3, timestamp=600060)], candles=[candle(frames[1], 600060)])
                self.assertEqual(len(receipt['actions']), 1, controller.runtime.reason)
                executor = session.executor_orchestrator.active_executors[OWNER][0]
                self.assertIs(type(executor), ModularPositionExecutor)
                order = executor._open_order.order
                self.assertIsNotNone(order)
                self.assertEqual(order.price, D('95'))
                # Single default stage: the full 100 quote allocation net of the fee reserve, floored to the amount step at the bid.
                self.assertEqual(order.amount, D('1.04'))
                fill = await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                               side=TradeType.SELL, price=D('94.99'), amount=D('.1'))])
                self.assertEqual(session._connector.get_balance('BTC'), D('.1'))
                self.assertEqual(executor.open_filled_amount, D('.1'))
                self.assertEqual(session._connector.get_balance('USDC'), D('1000') - D('.1') * D('95') * D('1.001'))
                self.assertEqual(len(session.recorder.get_trades_for_config('native-paper-session')), 1)
                self.assertGreater(fill['reports'][OWNER]['executors'][0].filled_amount_quote, 0)
                same_candle = await session.step(600062, books=[dict(**BOOK, sequence=4, timestamp=600062)])
                self.assertEqual(same_candle['actions'], [])
                expected_balances = session._connector.get_all_balances()
            finally:
                await session.close()
            restored = NativePaperSession(paper(), [research_config()], db, restore=True)
            try:
                self.assertEqual(restored._connector.get_all_balances(), expected_balances)
                receipt = await restored.step(600063, books=[dict(**BOOK, sequence=5, timestamp=600063)], candles=[candle(frames[1], 600063)])
                self.assertEqual(receipt['actions'], [])
                self.assertEqual(restored._connector.get_balance('BTC'), D('.1'))
                self.assertNotIn('duplicate', restored.controllers[OWNER].runtime.reason)
            finally:
                await restored.close()
