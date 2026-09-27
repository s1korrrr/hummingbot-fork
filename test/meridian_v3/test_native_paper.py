"""Synthetic native lifecycle checks, not historical economics or live receipts."""
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.test_contract import config
from unittest import IsolatedAsyncioTestCase

import numpy as np
import pandas as pd

from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.strategy_v2.paper_session import NativePaperSession

PAIR = 'BNB-USDC'
OWNER = 'modular_spot_meridian_v3_paper_bnb'
BOOK = dict(trading_pair=PAIR, bids=[(99.5, 10)], asks=[(99.51, 10)])


def paper(pairs=(PAIR,), cash='100'):
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


def frames():
    # Slow decline followed by RSI(17) crossing 29. No flash-crash threshold breach.
    close = np.r_[100 + .05 * np.sin(np.arange(1400) / 10), np.linspace(100, 99.5, 100)]
    frame = pd.DataFrame(dict(timestamp=np.arange(510000, 600000, 60), open=close, high=close + .1,
                              low=close - .1, close=close, volume=np.ones(1500) * 10))
    rebound = dict(timestamp=600000, open=99.5, high=99.66, low=99.4, close=99.56, volume=10)
    return frame, pd.concat([frame, pd.DataFrame([rebound])], ignore_index=True)


def candles(frame, now):
    return dict(connector_name='okx', trading_pair=PAIR, interval='1m', frame=frame, available_at=now)


class MeridianNativePaperTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    async def rest_entry(self, session):
        before, rebound = frames()
        receipt = await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)],
                                     candles=[candles(before, 600000)])
        controller = session.controllers[OWNER]
        self.assertEqual(receipt['actions'], [], controller.runtime.reason)
        self.assertEqual(controller.runtime.reason, 'seeded')
        receipt = await session.step(600060, books=[dict(**BOOK, sequence=3, timestamp=600060)],
                                     candles=[candles(rebound, 600060)])
        self.assertEqual(len(receipt['actions']), 1, controller.runtime.reason)
        executor = session.executor_orchestrator.active_executors[OWNER][0]
        self.assertIs(type(executor), ModularPositionExecutor)
        self.assertIsNotNone(executor._open_order.order)
        return controller, executor

    async def test_five_owners_share_one_funded_paper_program_with_separate_attribution(self):
        import json

        with tempfile.TemporaryDirectory() as directory:
            configs = [config(pair, execution_enabled=True) for pair in PAIRS]
            session = NativePaperSession(paper(PAIRS, '500'), configs, Path(directory) / 'portfolio.sqlite')
            try:
                for step, frame in enumerate(frames()):
                    now = 600000 + 60 * step
                    receipt = await session.step(now,
                                                 books=[dict(**{**BOOK, 'trading_pair': pair}, sequence=step + 2, timestamp=now) for pair in PAIRS],
                                                 candles=[{**candles(frame, now), 'trading_pair': pair} for pair in PAIRS])
                self.assertEqual({a.controller_id for a in receipt['actions']}, {c.id for c in configs})
                self.assertEqual(len(session._connector.in_flight_orders), 5)
                budget_keys = set()
                for c in configs:
                    controller = session.controllers[c.id]
                    budget_keys.add(controller.runtime.budget.key)
                    executor = session.executor_orchestrator.active_executors[c.id][0]
                    self.assertEqual(executor.config.controller_id, c.id)
                    self.assertEqual(executor.config.trading_pair, c.trading_pair)
                    self.assertLessEqual(executor.config.amount * executor.config.entry_price * D('1.003'), D('20'))
                self.assertEqual(len(budget_keys), 1)
                claims = json.loads(controller.runtime.budget._load())['reservations']
                self.assertEqual({r['controller_id'] for r in claims}, {c.id for c in configs})
                self.assertLessEqual(sum(D(r['reserved_quote']) for r in claims), D('100'))
                self.assertEqual(session._connector.get_balance('USDC'), D('500'))
                self.assertGreaterEqual(session._connector.get_available_balance('USDC'), D('400'))
                filled = await session.step(600061, trades=[dict(trading_pair=pair, sequence=1, timestamp=600061,
                                                                 side=TradeType.SELL, price=D('99.49'), amount=D('.05')) for pair in PAIRS])
                fills = session.recorder.get_trades_for_config('native-paper-session')
                self.assertEqual(len(fills), 5)
                for pair in PAIRS:
                    self.assertEqual(session._connector.get_balance(pair.split('-')[0]), D('.05'))
                for c in configs:
                    self.assertGreater(filled['reports'][c.id]['executors'][0].filled_amount_quote, 0)
            finally:
                await session.close()

    async def test_native_entry_partial_fill_fees_reservations_and_exact_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'meridian.sqlite'
            session = NativePaperSession(paper(), [config(execution_enabled=True)], db)
            try:
                controller, executor = await self.rest_entry(session)
                order = executor._open_order.order
                self.assertLessEqual(order.price * order.amount * D('1.003'), D('20'))
                self.assertGreater(order.price * order.amount, D('19'))
                receipt = await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1,
                                                                  timestamp=600061, side=TradeType.SELL, price=D('99.49'), amount=D('.05'))])
                self.assertEqual(executor.open_filled_amount, D('.05'))
                self.assertEqual(session._connector.get_balance('USDC'), D('100') - D('.05') * order.price * D('1.001'))
                self.assertLess(session._connector.get_available_balance('USDC'), session._connector.get_balance('USDC'))
                self.assertEqual(len(session.recorder.get_trades_for_config('native-paper-session')), 1)
                self.assertGreater(receipt['reports'][OWNER]['executors'][0].filled_amount_quote, 0)
                duplicate = await session.step(600062, books=[dict(**BOOK, sequence=4, timestamp=600062)])
                self.assertEqual(duplicate['actions'], [])
                self.assertEqual(controller.get_custom_info()['strategy_name'], 'Meridian v3')
                expected = session._connector.get_all_balances()
            finally:
                await session.close()
            restored = NativePaperSession(paper(), [config(execution_enabled=True)], db, restore=True)
            try:
                self.assertEqual(restored._connector.get_all_balances(), expected)
                receipt = await restored.step(600063, books=[dict(**BOOK, sequence=5, timestamp=600063)],
                                              candles=[candles(frames()[1], 600063)])
                self.assertEqual(receipt['actions'], [])
                self.assertEqual(restored._connector.get_balance('BNB'), D('.05'))
                self.assertEqual(len(restored.recorder.get_trades_for_config('native-paper-session')), 1)
            finally:
                await restored.close()

    async def test_entry_pause_preserves_filled_native_executor_and_stale_data_blocks_new_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [config(execution_enabled=True)], Path(directory) / 'meridian.sqlite')
            try:
                controller, executor = await self.rest_entry(session)
                amount = executor._open_order.order.amount
                await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                        side=TradeType.SELL, price=D('99.49'), amount=D('1'))])
                self.assertEqual(executor.open_filled_amount, amount)
                controller.config.acquisition_enabled = False
                receipt = await session.step(600062, books=[dict(**BOOK, sequence=4, timestamp=600062)])
                self.assertEqual(receipt['actions'], [])
                self.assertTrue(executor.is_active)
                self.assertEqual(session._connector.get_balance('BNB'), amount)
                controller.config.acquisition_enabled = True
                receipt = await session.step(601000, books=[dict(**BOOK, sequence=5, timestamp=601000)])
                self.assertEqual(receipt['actions'], [])
                self.assertTrue(executor.is_active)
                self.assertFalse(controller.processed_data['data_health']['ready'])
            finally:
                await session.close()

    async def test_restore_rejects_changed_risk_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'meridian.sqlite'
            session = NativePaperSession(paper(), [config()], db)
            await session.close()
            changed = config().risk.model_dump(mode='json')
            changed['max_spread_fraction'] = '.002'
            with self.assertRaisesRegex(ValueError, 'identity'):
                NativePaperSession(paper(), [config(risk=changed)], db, restore=True)

    async def test_mutable_execution_switch_cannot_bypass_mode_or_recipe_seal(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [config()], Path(directory) / 'meridian.sqlite')
            try:
                controller = session.controllers[OWNER]
                with self.assertRaisesRegex(ValueError, 'sealed recipe'):
                    controller.config.execution_enabled = True
                self.assertEqual(controller.determine_executor_actions(), [])
                self.assertEqual(controller.runtime.reason, 'meridian_recipe_binding_required')
                # Pydantic rejects after-validation but may retain the assigned
                # value. The dispatch boundary must still be closed.
                with self.assertRaises(ValueError):
                    controller.config.execution_mode = 'shadow'
                self.assertEqual(controller.determine_executor_actions(), [])
                self.assertEqual(controller.runtime.reason, 'meridian_shadow_execution_not_supported')
                self.assertFalse(controller.validate_executor_config(None))
                self.assertFalse(controller.entry_permission(None))
                with self.assertRaisesRegex(ValueError, 'sealed Meridian paper'):
                    controller.get_executor_class_for_config(None)
            finally:
                await session.close()

    async def test_drawdown_retains_inventory_and_restore_never_adopts_wallet_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'meridian.sqlite'
            session = NativePaperSession(paper(), [config(execution_enabled=True)], db)
            try:
                controller, executor = await self.rest_entry(session)
                amount = executor._open_order.order.amount
                await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                        side=TradeType.SELL, price=D('99.49'), amount=D('1'))])
                receipt = await session.step(600062, books=[dict(trading_pair=PAIR, sequence=4,
                                                                 timestamp=600062, bids=[(96, 10)], asks=[(96.01, 10)])])
                self.assertTrue(any(a.keep_position for a in receipt['actions']))
                self.assertEqual(session._connector.get_balance('BNB'), amount)
                self.assertFalse(any(o.trade_type == TradeType.SELL for o in session._connector.in_flight_orders.values()))
                self.assertTrue(controller.positions_held)
                held_amount = sum(p.amount for p in controller.positions_held)
                self.assertEqual(held_amount, amount)
            finally:
                await session.close()
            restored = NativePaperSession(paper(), [config(execution_enabled=True)], db, restore=True)
            try:
                await restored.step(600063, books=[dict(trading_pair=PAIR, sequence=5,
                                                        timestamp=600063, bids=[(96, 10)], asks=[(96.01, 10)])], candles=[candles(frames()[1], 600063)])
                self.assertEqual(restored._connector.get_balance('BNB'), amount)
                self.assertEqual(sum(p.amount for p in restored.controllers[OWNER].positions_held), amount)
            finally:
                await restored.close()
            foreign = paper()
            foreign.set_balance('BNB', D('1'))
            with self.assertRaisesRegex(ValueError, 'identity'):
                NativePaperSession(foreign, [config(execution_enabled=True)], db, restore=True)
