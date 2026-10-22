"""Synthetic native paper lifecycle checks for V4, not historical economics or live receipts."""
import copy
import json
import sqlite3
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


class MeridianV4RestingEntryTests(IsolatedAsyncioTestCase):
    """A submitted entry rests until it fills or times out, as in the backtests (paper A/B, 2026-10-14).

    Live paper evidence: D2h's ETH entry was cancelled after 28 s with `modular_entry_permission_revoked`
    because a quiet BNB book made the shared paper connector not ready (`data_hold`) for one tick."""
    setUp = session_tests.NativePaperSessionTests.setUp

    async def test_restore_exposes_native_reconciliation_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'v4.sqlite'
            session = NativePaperSession(paper(), [config(execution_enabled=True)], db)
            before, rebound = frames(.05)
            await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)],
                               candles=[candles(before, 600000)])
            receipt = await session.step(600060, books=[dict(**BOOK, sequence=3, timestamp=600060)],
                                         candles=[candles(rebound, 600060)])
            controller = session.controllers[OWNER]
            self.assertEqual(len(receipt['actions']), 1, controller.runtime.reason)
            amount = session.executor_orchestrator.active_executors[OWNER][0]._open_order.order.amount
            await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                    side=TradeType.SELL, price=D('99.49'), amount=amount)])
            await session.close()

            restored = NativePaperSession(paper(), [config(execution_enabled=True)], db, restore=True)
            try:
                evidence = restored.reconciliation_status()
                self.assertEqual(evidence['status'], 'verified')
                self.assertTrue(evidence['session_identity']['marker_matches'])
                self.assertFalse(evidence['interrupted_step'])
                self.assertEqual(evidence['unresolved_executors'], [])
                self.assertEqual(evidence['unresolved_orders'], [])
                self.assertEqual(evidence['inventory_consistency']['status'], 'verified')
                restored.recorder.financial_persistence_status = lambda: {
                    'mode': 'legacy_sync', 'pending_count': 0, 'failed_count': 0,
                    'reconciliation_needed': True, 'error_type': None,
                }
                self.assertEqual(restored.reconciliation_status()['status'], 'verified')
                restored.recorder.financial_persistence_status = lambda: {
                    'mode': 'legacy_sync', 'pending_count': 1, 'failed_count': 0,
                    'reconciliation_needed': True, 'error_type': None,
                }
                self.assertEqual(restored.reconciliation_status()['status'], 'unresolved')
                del restored.recorder.financial_persistence_status
                restored.executor_orchestrator.recovery_required[OWNER] = ['old-executor']
                unresolved = restored.reconciliation_status()
                self.assertEqual(unresolved['status'], 'unresolved')
                self.assertEqual(unresolved['unresolved_executors'], [f'{OWNER}:old-executor'])
                restored.executor_orchestrator.recovery_required.clear()
                restored.executor_orchestrator.recovery_required['unowned_orders'] = ['old-order']
                unresolved_order = restored.reconciliation_status()
                self.assertEqual(unresolved_order['status'], 'unresolved')
                self.assertEqual(unresolved_order['unresolved_orders'], ['old-order'])
                restored.executor_orchestrator.recovery_required.clear()
                restored._failure_path.write_text('{"status":"in_progress"}')
                interrupted = restored.reconciliation_status()
                self.assertEqual(interrupted['status'], 'unresolved')
                self.assertTrue(interrupted['interrupted_step'])
                restored._failure_path.unlink()
                restored._market_state_restore_verified = False
                unverified_inventory = restored.reconciliation_status()
                self.assertEqual(unverified_inventory['status'], 'unresolved')
                self.assertEqual(unverified_inventory['inventory_consistency']['status'], 'unverified')
                restored._market_state_restore_verified = True
                controller = restored.controllers[OWNER]
                controller.recovery_hold = 'inventory_reconciliation_required'
                held_inventory = restored.reconciliation_status()
                self.assertEqual(held_inventory['status'], 'unresolved')
                self.assertEqual(held_inventory['unresolved_owners'], [f'{OWNER}:inventory_reconciliation_required'])
                controller.recovery_hold = ''
                await restored.step(600062, books=[dict(**BOOK, sequence=4, timestamp=600062)],
                                    candles=[candles(frames(.05)[1], 600062)])
                self.assertEqual(restored.reconciliation_status()['status'], 'verified')
            finally:
                await restored.close()

    async def test_restore_readback_requires_matching_persisted_exact_fill(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'v4-corrupt-fill.sqlite'
            session = NativePaperSession(paper(), [config(execution_enabled=True)], db)
            before, rebound = frames(.05)
            await session.step(600000, books=[dict(**BOOK, sequence=2, timestamp=600000)],
                               candles=[candles(before, 600000)])
            receipt = await session.step(600060, books=[dict(**BOOK, sequence=3, timestamp=600060)],
                                         candles=[candles(rebound, 600060)])
            self.assertEqual(len(receipt['actions']), 1)
            await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                    side=TradeType.SELL, price=D('99.49'), amount=D('.5'))])
            partial_state = copy.deepcopy(session._connector.tracking_states)
            self.assertEqual(len(session.recorder.get_trades_for_config('native-paper-session')), 1)
            self.assertEqual(sum(len(order['order_fills']) for order in partial_state['orders'].values()), 1)
            market_name = session._connector.display_name
            await session.close()

            with sqlite3.connect(db) as connection:
                connection.execute('UPDATE MarketState SET saved_state = ? WHERE config_file_path = ? AND market = ?',
                                   (json.dumps(partial_state), 'native-paper-session', market_name))

            restored = NativePaperSession(paper(), [config(execution_enabled=True)], db, restore=True)
            try:
                financial = restored.reconciliation_status()['inventory_consistency']['financial_persistence']
                self.assertEqual(financial['status'], 'verified')
                self.assertEqual(financial['verified_fill_count'], 1)
            finally:
                await restored.close()

            pending_state = copy.deepcopy(partial_state)
            next(iter(pending_state['orders'].values()))['recorder_pending_events'] = {
                'fixture-pending': {
                    'kind': 'order-status',
                    'identity': ['fixture-pending', market_name, 'paper'],
                    'payload': {},
                }
            }
            with sqlite3.connect(db) as connection:
                connection.execute('UPDATE MarketState SET saved_state = ? WHERE config_file_path = ? AND market = ?',
                                   (json.dumps(pending_state), 'native-paper-session', market_name))
            restored = NativePaperSession(paper(), [config(execution_enabled=True)], db, restore=True)
            try:
                financial = restored.reconciliation_status()['inventory_consistency']['financial_persistence']
                self.assertEqual(financial['status'], 'unverified')
                self.assertIn('pending recorder events', financial['reason'])
            finally:
                await restored.close()

            with sqlite3.connect(db) as connection:
                connection.execute('UPDATE MarketState SET saved_state = ? WHERE config_file_path = ? AND market = ?',
                                   (json.dumps(partial_state), 'native-paper-session', market_name))
                cursor = connection.execute('DELETE FROM TradeFillExact')
                self.assertGreater(cursor.rowcount, 0)

            restored = NativePaperSession(paper(), [config(execution_enabled=True)], db, restore=True)
            try:
                financial = restored.reconciliation_status()['inventory_consistency']['financial_persistence']
                self.assertEqual(financial['status'], 'unverified')
                self.assertIn('lacks matching exact rows', financial['reason'])
            finally:
                await restored.close()

    async def rest_entry(self, session):
        before, rebound = frames(.05)
        books = [dict(**{**BOOK, 'trading_pair': p}, sequence=2, timestamp=600000) for p in ('BNB-USDC', 'ETH-USDC')]
        await session.step(600000, books=books, candles=[candles(before, 600000)])
        books = [dict(**{**BOOK, 'trading_pair': p}, sequence=3, timestamp=600060) for p in ('BNB-USDC', 'ETH-USDC')]
        receipt = await session.step(600060, books=books, candles=[candles(rebound, 600060)])
        self.assertEqual(len(receipt['actions']), 1, session.controllers[OWNER].runtime.reason)
        return session.executor_orchestrator.active_executors[OWNER][0]

    async def test_another_pairs_quiet_book_never_holds_or_cancels_a_resting_entry(self):
        """Root cause of the 2026-10-14 revocation: aggregate connector readiness. Readiness is now per pair."""
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(('BNB-USDC', 'ETH-USDC')), [config(execution_enabled=True)],
                                         Path(directory) / 'v4.sqlite')
            try:
                executor = await self.rest_entry(session)
                session._connector.invalidate_order_book('ETH-USDC')  # another pair's book goes quiet
                await session.step(600062, books=[dict(**BOOK, sequence=4, timestamp=600062)])
                self.assertFalse(str(session.controllers[OWNER].runtime.reason).startswith('data_hold'))
                self.assertFalse(executor.is_closed)
                self.assertIsNotNone(executor._open_order)
            finally:
                await session.close()

    async def test_own_pair_stale_book_pauses_the_owner_then_native_checks_resume(self):
        """The session steps an owner only while its own book is fresh (native per-owner freshness): a stale own book
        neither drives nor revokes the entry; once the book is fresh every native check applies again."""
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(('BNB-USDC', 'ETH-USDC')), [config(execution_enabled=True)],
                                         Path(directory) / 'v4.sqlite')
            try:
                executor = await self.rest_entry(session)
                session._connector.invalidate_order_book(PAIR)
                await session.step(600062, books=[dict(**{**BOOK, 'trading_pair': 'ETH-USDC'}, sequence=4,
                                                       timestamp=600062)])
                self.assertIsNone(executor._entry_lifecycle.get('stop_reason'))
                self.assertIsNotNone(executor._open_order)
                session.controllers[OWNER].config.manual_kill_switch = True
                await session.step(600064, books=[dict(**BOOK, sequence=5, timestamp=600064)])
                self.assertEqual(executor._entry_lifecycle.get('stop_reason'), 'modular_entry_permission_revoked')
            finally:
                await session.close()

    async def test_native_denials_revoke_a_resting_entry(self):
        """Reviews (GPT-6.1-Sol rounds 2-3): every native denial revokes, also while another pair's book is quiet."""
        cases = ('wide_spread', 'risk_latched', 'risk_latched_during_hold', 'claim_removed')
        for case in cases:
            with self.subTest(case), tempfile.TemporaryDirectory() as directory:
                session = NativePaperSession(paper(('BNB-USDC', 'ETH-USDC')), [config(execution_enabled=True)],
                                             Path(directory) / 'v4.sqlite')
                try:
                    executor = await self.rest_entry(session)
                    controller = session.controllers[OWNER]
                    store = controller.runtime.ensure_store()
                    state, raw = store.load()
                    book = dict(**BOOK, sequence=4, timestamp=600062)
                    if case == 'wide_spread':
                        book = dict(trading_pair=PAIR, bids=[(98.0, 10)], asks=[(99.51, 10)], sequence=4,
                                    timestamp=600062)  # spread > 0.3%
                    elif case.startswith('risk_latched'):
                        self.assertTrue(store.compare_and_swap(raw, state.model_copy(update={'risk_paused': True})))
                        if case == 'risk_latched_during_hold':
                            session._connector.invalidate_order_book('ETH-USDC')
                    else:
                        controller.runtime.pending.pop(executor.config.id)
                    if case != 'wide_spread':  # the wide book only exists once the step applies it
                        self.assertFalse(controller.entry_permission(executor.config, check_funds=False), case)
                    await session.step(600062, books=[book])
                    self.assertEqual(executor._entry_lifecycle.get('stop_reason'), 'modular_entry_permission_revoked',
                                     case)
                finally:
                    await session.close()

    async def test_definitive_revocations_still_end_a_resting_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(('BNB-USDC', 'ETH-USDC')), [config(execution_enabled=True)],
                                         Path(directory) / 'v4.sqlite')
            try:
                executor = await self.rest_entry(session)
                session.controllers[OWNER].config.manual_kill_switch = True
                await session.step(600062, books=[dict(**BOOK, sequence=4, timestamp=600062)])
                self.assertEqual(executor._entry_lifecycle['stop_reason'], 'modular_entry_permission_revoked')
            finally:
                await session.close()


class MeridianV4RestingEntryRevocationTests(IsolatedAsyncioTestCase):
    """Review (GPT-6.1-Sol, 2026-10-15): the resting-entry fallback must never outlive a definitive revocation.
    Before the fix a stopped controller (NOT_STARTED / SHUTTING_DOWN / TERMINATED) or an operator entry pause
    kept an unfilled buy alive."""
    setUp = session_tests.NativePaperSessionTests.setUp

    async def resting(self, directory):
        session = NativePaperSession(paper(('BNB-USDC', 'ETH-USDC')), [config(execution_enabled=True)],
                                     Path(directory) / 'v4.sqlite')
        executor = await MeridianV4RestingEntryTests.rest_entry(self, session)
        return session, executor, session.controllers[OWNER]

    async def test_operator_entry_pause_ends_a_resting_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            session, executor, controller = await self.resting(directory)
            try:
                controller._operator_entry_paused = True
                self.assertFalse(controller.entry_permission(executor.config, check_funds=False))
                await session.step(600062, books=[dict(**BOOK, sequence=4, timestamp=600062)])
                self.assertEqual(executor._entry_lifecycle['stop_reason'], 'modular_entry_permission_revoked')
            finally:
                await session.close()

    async def test_a_controller_that_is_not_running_never_retains_a_resting_entry(self):
        from hummingbot.strategy_v2.models.base import RunnableStatus
        with tempfile.TemporaryDirectory() as directory:
            session, executor, controller = await self.resting(directory)
            try:
                for status in (RunnableStatus.NOT_STARTED, RunnableStatus.SHUTTING_DOWN, RunnableStatus.TERMINATED):
                    controller._status = status
                    self.assertFalse(controller.entry_permission(executor.config, check_funds=False), status)
            finally:
                await session.close()
