"""Actual native action -> recorder -> funding -> partial fill/cancel/restart."""
import asyncio
import tempfile
from pathlib import Path
from test.hummingbot.strategy_v2.paper.test_paper_session import (
    PAIR,
    D,
    candles as synthetic_candles,
    causal_reversal_frames,
    config,
    paper,
)
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import numpy as np
import pandas as pd

from controllers.generic.modular_spot_core.domain import identity
from controllers.generic.modular_spot_core.native_funding import LEGACY_NEVER_STARTED
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.paper_session import NativePaperSession


def funded_config(allocation='50', family=None):
    args = () if family is None else (family,)
    return config(*args, execution_enabled=True, min_signal_score=1, dynamic_position_sizing=False,
                  funding={'allocation_quote': allocation, 'fee_reserve_fraction': '.003'})


def candles_for_second_reversal(now=602400):
    """A second fresh synthetic oversold reversal after the first exit."""
    frame = synthetic_candles(now)
    closes = np.r_[100 + np.sin(np.arange(550) / 10), np.linspace(100, 90, 50)]
    frame['open'] = frame['close'] = closes
    frame['high'], frame['low'] = closes + .1, closes - .1
    row = frame.iloc[-1].copy()
    row['timestamp'], row['open'], row['close'], row['high'], row['low'] = now, 90, 90.02, 90.12, 89.9
    return pd.concat([frame, pd.DataFrame([row])], ignore_index=True)


def add_legacy_uncertain_reserve(store, quote=D('3.453838524'), price=D('89.99')):
    """Seed only a historical never-started receipt, without native orders."""
    raw, state = store._load()
    executor_id = 'legacy-uncertain-roundtrip'
    amount = quote / (price * (1 + store.policy.fee_reserve_fraction))
    state['claims'][executor_id] = {
        'config': identity(['old-controller-config', executor_id]),
        'amount': str(amount), 'price': str(price), 'fee_fraction': str(store.policy.fee_reserve_fraction),
    }
    state['releases'][executor_id] = dict(LEGACY_NEVER_STARTED)
    store._write(raw, state)
    return quote


class NativePaperFundingTests(IsolatedAsyncioTestCase):
    def setUp(self):
        # Native module-isolation tests reload controller modules; use current
        # genuine exception/store classes, not collection-time stale identities.
        global PolicyError, NativeFundingStore
        from controllers.generic.modular_spot_core.domain import PolicyError
        from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
        prior = MarketsRecorder._shared_instance
        MarketsRecorder._shared_instance = None
        self.addCleanup(setattr, MarketsRecorder, '_shared_instance', prior)
        for target in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.socket.sendto', 'socket.getaddrinfo'):
            guard = patch(target, side_effect=AssertionError('network forbidden'))
            guard.start()
            self.addCleanup(guard.stop)

    async def seed(self, session, ask=90.1):
        for index, frame in enumerate(causal_reversal_frames()):
            now = 600000 + index * 60
            result = await session.step(now, books=[dict(trading_pair=PAIR, sequence=2 + index, timestamp=now,
                                                         bids=[(90, 10)], asks=[(ask, 10)])], candles=[dict(
                                                             connector_name='okx', trading_pair=PAIR, interval='1m', frame=frame, available_at=now)])
        return result

    async def test_actual_native_dispatch_rejects_unaffordable_entry_before_order(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [funded_config('10')], Path(directory) / 'paper.sqlite')
            try:
                await self.seed(session)
                controller = next(iter(session.controllers.values()))
                self.assertIn('allocation exhausted', controller._funding_hold)
                self.assertEqual(session._connector.in_flight_orders, {})
                self.assertEqual(session.recorder.get_all_executors(), [])
                self.assertEqual(session._connector.get_balance('USDC'), D('1000'))
            finally:
                await session.close()

    async def test_partial_fill_restart_releases_after_native_shutdown_reconciliation(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'paper.sqlite'
            session = NativePaperSession(paper(), [funded_config()], db)
            await self.seed(session)
            controller = next(iter(session.controllers.values()))
            executors = session.executor_orchestrator.active_executors[controller.config.id]
            self.assertEqual(len(executors), 1, controller._funding_hold)
            executor = executors[0]
            store = controller._funding_store
            _, checkpoint = store._load()
            self.assertIn(executor.config.id, checkpoint['claims'])
            price = executor.config.entry_price
            await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                    side=TradeType.SELL, price=price - D('.01'), amount=D('.1'))])
            self.assertEqual(session._connector.get_balance('BTC'), D('.1'))
            next_config = controller.get_executor_config(TradeType.BUY, D('90'), D('.3'))
            with self.assertRaisesRegex(PolicyError, 'allocation exhausted'):
                store.admit(next_config)
            await session.close()
            restored = NativePaperSession(paper(), [funded_config()], db, restore=True)
            try:
                owner = next(iter(restored.controllers.values()))
                restored._connector.set_order_book(PAIR, 4, 600061, [(90, 10)], [(90.1, 10)])
                owner.executors_info = restored.recorder.get_executors_by_controller(owner.config.id)
                restored_store = NativeFundingStore(owner, restored.recorder)
                self.assertTrue(restored_store.admit(next_config))
                self.assertEqual(restored_store.status['released_intents'], 1)
                self.assertEqual(restored._connector.get_balance('BTC'), D('.1'))
                with restored.recorder.sql_manager.get_new_session() as sql, sql.begin():
                    sql.delete(sql.get(Metadata, restored_store.key))
                with self.assertRaisesRegex(PolicyError, 'checkpoint missing|without funding checkpoint'):
                    restored_store.admit(next_config)
            finally:
                await restored.close()

    async def test_admitted_intent_write_failure_and_unknown_fee_do_not_place_orders(self):
        from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
        for failure in ('checkpoint', 'fees', 'maker_fees'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                session = NativePaperSession(paper(), [funded_config()], Path(directory) / 'paper.sqlite')
                try:
                    target = (patch.object(NativeFundingStore, '_write', side_effect=OSError('funding disk unavailable'))
                              if failure == 'checkpoint' else patch.object(session._connector, 'get_fee',
                                                                           side_effect=lambda **kwargs: AddedToCostTradeFee(percent=D('.2')
                                                                                                                            if failure == 'fees' or kwargs['is_maker'] else D('.001'))))
                    with target:
                        await self.seed(session)
                    owner = next(iter(session.controllers.values()))
                    self.assertTrue(owner._funding_hold)
                    self.assertEqual(session._connector.in_flight_orders, {})
                    self.assertEqual(session.recorder.get_all_executors(), [])
                finally:
                    await session.close()

    async def test_canceled_entry_late_native_fill_consumes_retained_capacity(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [funded_config()], Path(directory) / 'paper.sqlite')
            try:
                await self.seed(session)
                owner = next(iter(session.controllers.values()))
                executor = session.executor_orchestrator.active_executors[owner.config.id][0]
                order = executor._open_order.order
                price = order.price
                await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                        side=TradeType.SELL, price=price - D('.01'), amount=D('.1'))])
                session._connector.cancel(PAIR, order.client_order_id)
                await session._connector.drain()
                self.assertTrue(order.is_cancelled)
                store = owner._funding_store
                raw, state = store._load()
                before, pending_before, _ = store._evidence(state, raw)
                self.assertGreater(pending_before, 0)
                # Inject a native exchange-confirmed late fill, distinct from a
                # public print (which correctly cannot fill after cancel ack).
                await session._connector._fill(order, D('.1'), price, True, 'late-confirmed-fill')
                owner.executors_info = [executor.executor_info]
                after, pending_after, _ = store._evidence(state, raw)
                self.assertEqual(session._connector.get_balance('BTC'), D('.2'))
                self.assertGreater(after['cash_used'], before['cash_used'])
                self.assertLess(pending_after, pending_before)
                self.assertLessEqual(after['used'] + pending_after, before['used'] + pending_before)
                with self.assertRaisesRegex(PolicyError, 'allocation exhausted'):
                    store.admit(owner.get_executor_config(TradeType.BUY, D('90'), D('.3')))
                # Every native fill remains recorder truth; the checkpoint is not
                # a second ledger and cannot erase those receipts.
                self.assertEqual(len(session.recorder.get_trades_for_config('native-paper-session')), 2)
            finally:
                await session.close()

    async def test_conflicting_saved_current_identity_and_missing_audit_row_hold(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [funded_config()], Path(directory) / 'paper.sqlite')
            try:
                await self.seed(session)
                owner = next(iter(session.controllers.values()))
                executor = session.executor_orchestrator.active_executors[owner.config.id][0]
                store = owner._funding_store
                next_config = owner.get_executor_config(TradeType.BUY, D('90'), D('.3'))
                mutated = executor.executor_info.model_copy(deep=True)
                mutated.config.amount *= 2
                owner.executors_info = [mutated]
                with self.assertRaisesRegex(PolicyError, 'configuration conflict'):
                    store.admit(next_config)
                owner.executors_info = [executor.executor_info]
                with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
                    row = sql.query(Metadata).filter(Metadata.key.like(store.key + ':claim:%')).one()
                    sql.delete(row)
                with self.assertRaisesRegex(PolicyError, 'disappeared or changed'):
                    store.admit(next_config)
            finally:
                await session.close()

    async def test_metadata_audit_has_no_lifetime_1024_intent_4096_fill_stop(self):
        from controllers.generic.modular_spot_core.domain import identity
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [funded_config()], Path(directory) / 'paper.sqlite')
            try:
                owner = next(iter(session.controllers.values()))
                store = NativeFundingStore(owner, session.recorder)
                raw, state = store._load()
                # Storage scaling regression only: hashes/admissions are synthetic;
                # no native fills, executor or economic result is fabricated.
                state['claims'] = {str(i): {'config': identity(i), 'amount': '.01', 'price': '1', 'fee_fraction': '.003'}
                                   for i in range(1025)}
                state['fills'] = {identity(i): identity(['fill', i]) for i in range(4097)}
                self.assertTrue(store._write(raw, state))
                saved, restored = store._load()
                self.assertLess(len(saved), 512)
                self.assertEqual(len(restored['claims']), 1025)
                self.assertEqual(len(restored['fills']), 4097)
                self.assertFalse(store._write(raw, state))  # Stale writer cannot replace that revision.
            finally:
                await session.close()

    async def test_funding_exhaustion_preserves_real_native_trailing_sell_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [funded_config()], Path(directory) / 'paper.sqlite')
            try:
                await self.seed(session)
                owner = next(iter(session.controllers.values()))
                executor = session.executor_orchestrator.active_executors[owner.config.id][0]
                await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                        side=TradeType.SELL, price=executor.config.entry_price - D('.01'), amount=D('1'))])
                self.assertEqual(executor.open_filled_amount, executor.config.amount)
                with self.assertRaisesRegex(PolicyError, 'allocation exhausted'):
                    owner._funding_store.admit(owner.get_executor_config(TradeType.BUY, D('90'), D('.3')))
                owner._funding_hold = 'Candidate funding allocation exhausted'
                # Observed synthetic book movement arms and then triggers the
                # unchanged native trailing executor; no policy/exit monkeypatch.
                for timestamp, sequence, bid in ((600062, 4, 100), (600063, 5, 91), (600064, 6, 91)):
                    await session.step(timestamp, books=[dict(trading_pair=PAIR, sequence=sequence,
                                                              timestamp=timestamp, bids=[(bid, 10)], asks=[(bid + .1, 10)])])
                fills = session.recorder.get_trades_for_config('native-paper-session')
                self.assertTrue(any(fill.trade_type == 'SELL' for fill in fills))
                self.assertEqual(session._connector.get_balance('BTC'), D('0'))
                self.assertEqual(len(session.recorder.get_all_executors()), 1)
            finally:
                await session.close()

    async def test_public_ok_rsi_buy_trailing_sell_then_second_causal_buy_survives_restart(self):
        await self._run_ok_rsi_roundtrip(down_only=False)

    async def test_down_only_buy_trailing_sell_then_second_causal_buy_survives_restart(self):
        await self._run_ok_rsi_roundtrip(down_only=True)

    async def _run_ok_rsi_roundtrip(self, down_only):
        """Exercise the public OK-RSI profile through two native entry lifecycles.

        This is a synthetic causal execution fixture. It proves that a later
        qualifying candle can create another native order after the first
        position has been sold; it does not measure strategy performance.
        """
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'paper.sqlite'
            from controllers.generic.rsi_modular import get_config_class
            profile_config = get_config_class('ok_rsi')(
                id='modular_spot_public_roundtrip', profile='ok_rsi', execution_mode='paper',
                entry_acquisition_policy='down_only_maker_v1' if down_only else 'legacy_single_attempt',
                connector_name='okx_paper_trade', candles_connector='okx', execution_enabled=True,
                trading_pair=PAIR, strict_hmm_mode=False, trend_confirmation_enabled=False,
                min_signal_score=1, dynamic_position_sizing=False, usd_per_entry=100,
                funding={'allocation_quote': '101', 'max_position_quote': '101',
                         'fee_reserve_fraction': '.003', 'resize_to_available_quote': True})
            session = NativePaperSession(paper(), [profile_config], db)
            try:
                owner = next(iter(session.controllers.values()))
                store = NativeFundingStore(owner, session.recorder)
                old_reserve = add_legacy_uncertain_reserve(store)
                await self.seed(session, ask=90.01 if down_only else 90.1)
                if down_only:
                    await session.step(600061)
                owner = next(iter(session.controllers.values()))
                self.assertEqual(owner.config.profile, 'ok_rsi')
                executor = session.executor_orchestrator.active_executors[owner.config.id][0]
                self.assertEqual(owner._get_entry_decision_gate().store.load()['consumed_candle'], 600000)
                buy = executor._open_order.order
                sizing = owner.get_custom_info()['modular_composition']['funding_sizing']
                self.assertEqual(D(sizing['requested_quote']), D('100'))
                self.assertLessEqual(D(sizing['effective_quote']), D('97.255'))
                self.assertLess(executor.config.amount * executor.config.entry_price, D('97.254'))
                self.assertGreater(executor.config.amount * executor.config.entry_price, D('97.18'))
                self.assertGreater(old_reserve, D('3.45'))
                await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                        side=TradeType.SELL, price=buy.price, amount=buy.amount)])
                self.assertEqual(executor.open_filled_amount, executor.config.amount)
                self.assertEqual(session._connector.get_balance('BTC'), executor.config.amount)
                self.assertEqual(len(session.recorder.get_trades_for_config('native-paper-session')), 1)

                # The unchanged native trailing executor arms at the higher
                # bid and exits on retracement, including a terminal sell fill.
                for timestamp, sequence, bid in ((600062, 4, 100), (600063, 5, 91), (600064, 6, 91)):
                    await session.step(timestamp, books=[dict(trading_pair=PAIR, sequence=sequence,
                                                              timestamp=timestamp, bids=[(bid, 10)], asks=[(bid + .1, 10)])])
                await session.step(600065, books=[dict(trading_pair=PAIR, sequence=7, timestamp=600065,
                                                       bids=[(91, 10)], asks=[(91.1, 10)])])
                self.assertEqual(session._connector.get_balance('BTC'), D('0'))
                fills = session.recorder.get_trades_for_config('native-paper-session')
                self.assertEqual(len(fills), 2)
                self.assertEqual(executor._entry_orders()[0].order.trade_type, TradeType.BUY)
                self.assertEqual(executor._exit_orders()[0].order.trade_type, TradeType.SELL)
                self.assertTrue(executor.is_closed, executor.get_custom_info())

                # A distinct later closed reversal candle should pass the real
                # controller and dispatch path after the prior executor ends.
                clear_frame = synthetic_candles(600120)
                await session.step(600180, books=[dict(trading_pair=PAIR, sequence=8, timestamp=600180,
                                                       bids=[(90, 10)], asks=[(90.01 if down_only else 90.1, 10)])], candles=[dict(
                                                           connector_name='okx', trading_pair=PAIR, interval='1m',
                                                           frame=clear_frame, available_at=600180)])
                second_candle = 602400  # Outside the native RSI reversal debounce window.
                second_time = second_candle + 60
                frame = candles_for_second_reversal(second_candle)
                receipt = await session.step(second_time, books=[dict(trading_pair=PAIR, sequence=9,
                                                                      timestamp=second_time, bids=[(90, 10)], asks=[(90.01 if down_only else 90.1, 10)])], candles=[dict(
                                                                          connector_name='okx', trading_pair=PAIR, interval='1m',
                                                                          frame=frame, available_at=second_time)])
                self.assertTrue(receipt['actions'], {
                    'controller': owner.get_custom_info(),
                    'entry_decision': owner._get_entry_decision_gate().store.load(),
                })
                second = next((item for item in session.executor_orchestrator.active_executors[owner.config.id]
                               if item.config.id != executor.config.id), None)
                self.assertIsNotNone(second, owner.get_custom_info())
                self.assertEqual(second.config.side, TradeType.BUY)
                self.assertNotEqual(second.config.id, executor.config.id)
                self.assertIn(second._open_order.order.client_order_id, session._connector.in_flight_orders)
                self.assertEqual(len(session.recorder.get_all_executors()), 2)
            finally:
                await session.close()

            # Native executor/funding state remains reconcilable after a clean
            # shutdown while the second entry is resting and unfilled.
            restored = NativePaperSession(paper(), [profile_config], db, restore=True)
            try:
                owner = next(iter(restored.controllers.values()))
                owner.executors_info = restored.recorder.get_executors_by_controller(owner.config.id)
                result = NativeFundingStore(owner, restored.recorder).reconcile()
                self.assertEqual(len(restored.recorder.get_all_executors()), 2)
                self.assertGreater(D(result['pending_quote']), D('0'))
                self.assertEqual(restored._connector.get_balance('BTC'), D('0'))
                self.assertEqual(len(restored.recorder.get_trades_for_config('native-paper-session')), 2)
            finally:
                await restored.close()

    async def test_resize_below_native_minimum_keeps_candidate_without_order(self):
        """Funding fit cannot round a sub-minimum residual into a native buy."""
        from controllers.generic.rsi_modular import get_config_class
        with tempfile.TemporaryDirectory() as directory:
            profile_config = get_config_class('ok_rsi')(
                id='modular_spot_subminimum_roundtrip', profile='ok_rsi', execution_mode='paper',
                connector_name='okx_paper_trade', candles_connector='okx', execution_enabled=True,
                trading_pair=PAIR, strict_hmm_mode=False, trend_confirmation_enabled=False,
                min_signal_score=1, dynamic_position_sizing=False, usd_per_entry=100,
                funding={'allocation_quote': '101', 'max_position_quote': '101',
                         'fee_reserve_fraction': '.003', 'resize_to_available_quote': True})
            session = NativePaperSession(paper(), [profile_config], Path(directory) / 'paper.sqlite')
            try:
                owner = next(iter(session.controllers.values()))
                store = NativeFundingStore(owner, session.recorder)
                add_legacy_uncertain_reserve(store, quote=D('100.5'))
                await self.seed(session)
                sizing = owner.get_custom_info()['modular_composition']['funding_sizing']
                self.assertLess(D(sizing['effective_quote']), D('1'))
                self.assertEqual(session._connector.in_flight_orders, {})
                self.assertEqual(session.recorder.get_all_executors(), [])
                self.assertEqual(session._connector.get_balance('BTC'), D('0'))
            finally:
                await session.close()

    async def test_terminal_cancel_releases_only_unused_capacity_and_survives_restart(self):
        for partial in (False, True):
            with self.subTest(partial=partial), tempfile.TemporaryDirectory() as directory:
                db = Path(directory) / 'paper.sqlite'
                session = NativePaperSession(paper(), [funded_config()], db)
                try:
                    await self.seed(session)
                    owner = next(iter(session.controllers.values()))
                    executor = session.executor_orchestrator.active_executors[owner.config.id][0]
                    order = executor._open_order.order
                    if partial:
                        await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                                side=TradeType.SELL, price=order.price, amount=D('.1'))])
                    session._connector.cancel(PAIR, order.client_order_id)
                    await session._connector.drain()
                    owner.executors_info = [executor.executor_info]
                    store = owner._funding_store
                    before = store.reconcile()
                    self.assertEqual(before['released_intents'], 0)  # Local cancel alone cannot release.
                    self.assertGreater(D(before['pending_quote']), 0)
                    await executor._request_expired_entry_reconciliation(executor._entry_orders())
                    owner.executors_info = [executor.executor_info]
                    after = store.reconcile()
                    self.assertEqual(after['released_intents'], 1)
                    self.assertEqual(D(after['pending_quote']), 0)
                    self.assertGreater(D(after['available_quote']), D(before['available_quote']))
                    raw, _ = store._load()
                    self.assertEqual(store.reconcile(), after)
                    self.assertEqual(store._load()[0], raw)  # Idempotent without new metadata revision.
                    session.recorder.store_or_update_executor(executor)
                finally:
                    await session.close()
                restored = NativePaperSession(paper(), [funded_config()], db, restore=True)
                try:
                    owner = next(iter(restored.controllers.values()))
                    restored._connector.set_order_book(PAIR, 4, restored._connector.current_timestamp, [(90, 10)], [(90.1, 10)])
                    owner.executors_info = restored.recorder.get_executors_by_controller(owner.config.id)
                    result = NativeFundingStore(owner, restored.recorder).reconcile()
                    self.assertEqual(result['released_intents'], 1)
                    self.assertEqual(D(result['pending_quote']), 0)
                    self.assertEqual(restored._connector.get_balance('BTC'), D('.1') if partial else D('0'))
                finally:
                    await restored.close()

    async def test_terminal_receipt_rejects_scope_coverage_and_authority_changes(self):
        import copy
        for field, value in [('source', 'okx_terminal_status_v1'), ('order_id', 'foreign'),
                             ('exchange_order_id', 'foreign'), ('filled_base', '.1'),
                             ('trade_ids', ['missing']), ('side', 'SELL'), ('schema', True)]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                session = NativePaperSession(paper(), [funded_config()], Path(directory) / 'paper.sqlite')
                try:
                    await self.seed(session)
                    owner = next(iter(session.controllers.values()))
                    executor = session.executor_orchestrator.active_executors[owner.config.id][0]
                    session._connector.cancel(PAIR, executor._open_order.order_id)
                    await session._connector.drain()
                    await executor._request_expired_entry_reconciliation(executor._entry_orders())
                    executor._entry_reconciliation = copy.deepcopy(executor._entry_reconciliation)
                    executor._entry_reconciliation[field] = value
                    owner.executors_info = [executor.executor_info]
                    with self.assertRaises(PolicyError):
                        owner._funding_store.reconcile()
                    self.assertEqual(owner._funding_store._load()[1]['releases'], {})
                finally:
                    await session.close()

    async def test_late_fill_after_terminal_proof_blocks_readmission_without_disabling_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [funded_config()], Path(directory) / 'paper.sqlite')
            try:
                await self.seed(session)
                owner = next(iter(session.controllers.values()))
                executor = session.executor_orchestrator.active_executors[owner.config.id][0]
                order = executor._open_order.order
                await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                        side=TradeType.SELL, price=order.price, amount=D('.1'))])
                session._connector.cancel(PAIR, order.client_order_id)
                await session._connector.drain()
                await executor._request_expired_entry_reconciliation(executor._entry_orders())
                owner.executors_info = [executor.executor_info]
                store = owner._funding_store
                self.assertEqual(store.reconcile()['released_intents'], 1)
                await session._connector._fill(order, D('.1'), order.price, True, 'contradictory-late-fill')
                owner.executors_info = [executor.executor_info]
                with self.assertRaisesRegex(PolicyError, 'reconciliation.*(scope|coverage)'):
                    store.admit(owner.get_executor_config(TradeType.BUY, D('90'), D('.3')))
                self.assertFalse(executor.is_closed)
                self.assertEqual(executor.open_filled_amount, D('.2'))
                self.assertEqual(len(session.recorder.get_trades_for_config('native-paper-session')), 2)
                owner._funding_hold = 'Contradictory terminal receipt'
                for offset in range(10):
                    timestamp, bid = 600062 + offset, 100 if offset < 6 else 91
                    await session.step(timestamp, books=[dict(trading_pair=PAIR, sequence=4 + offset,
                                                              timestamp=timestamp, bids=[(bid, 10)], asks=[(bid + .1, 10)])])
                    await asyncio.sleep(0)
                    await asyncio.sleep(0)
                self.assertEqual(session._connector.get_balance('BTC'), D('0'), executor.get_custom_info())
                self.assertTrue(any(fill.trade_type == 'SELL' for fill in session.recorder.get_trades_for_config('native-paper-session')))
            finally:
                await session.close()

    async def test_persisted_release_cannot_outlive_missing_native_receipt_or_audit_row(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [funded_config()], Path(directory) / 'paper.sqlite')
            try:
                await self.seed(session)
                owner = next(iter(session.controllers.values()))
                executor = session.executor_orchestrator.active_executors[owner.config.id][0]
                session._connector.cancel(PAIR, executor._open_order.order_id)
                await session._connector.drain()
                await executor._request_expired_entry_reconciliation(executor._entry_orders())
                owner.executors_info = [executor.executor_info]
                store = owner._funding_store
                self.assertEqual(store.reconcile()['released_intents'], 1)
                proof = executor._entry_reconciliation
                executor._entry_reconciliation = None
                owner.executors_info = [executor.executor_info]
                with self.assertRaisesRegex(PolicyError, 'reconciliation scope'):
                    store.reconcile()
                executor._entry_reconciliation = proof
                owner.executors_info = [executor.executor_info]
                with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
                    row = sql.query(Metadata).filter(Metadata.key.like(store.key + ':release:%')).one()
                    sql.delete(row)
                with self.assertRaisesRegex(PolicyError, 'disappeared or changed'):
                    store.reconcile()
            finally:
                await session.close()

    async def test_rsi_v5_native_retry_entries_release_complete_terminal_set(self):
        for partial in (False, True):
            with self.subTest(partial=partial):
                await self._rsi_v5_retry_terminal_case(partial)

    async def _rsi_v5_retry_terminal_case(self, partial):
        from controllers.generic.modular_rsi_v5 import ModularRSIV5ControllerConfig
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'paper.sqlite'
            session = NativePaperSession(paper(), [funded_config(family=ModularRSIV5ControllerConfig)], db)
            try:
                await self.seed(session)
                owner = next(iter(session.controllers.values()))
                executors = session.executor_orchestrator.active_executors[owner.config.id]
                self.assertEqual(len(executors), 1, owner.get_custom_info())
                executor = executors[0]
                self.assertEqual(executor.config.entry_order_policy, 'retry')
                self.assertTrue(executor.config.require_terminal_entry_reconciliation)
                first = executor._open_order.order
                if partial:
                    await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                            side=TradeType.SELL, price=first.price, amount=D('.1'))])
                session._connector.cancel(PAIR, first.client_order_id)
                await session._connector.drain()
                await session.step(600061, books=[dict(trading_pair=PAIR, sequence=4, timestamp=600061,
                                                       bids=[(90, 10)], asks=[(90.1, 10)])])
                second = executor._open_order.order
                self.assertNotEqual(first.client_order_id, second.client_order_id)
                self.assertEqual(len(executor._entry_orders()), 2)
                # Native stop ends retry authority. Each canceled attempt must
                # independently pass terminal connector reconciliation.
                await session.close()
            finally:
                await session.close()
            restored = NativePaperSession(paper(), [funded_config(family=ModularRSIV5ControllerConfig)], db, restore=True)
            try:
                owner = next(iter(restored.controllers.values()))
                owner.executors_info = restored.recorder.get_executors_by_controller(owner.config.id)
                info = owner.executors_info[0]
                self.assertFalse(info.is_active)
                self.assertEqual(len(info.custom_info['entry_reconciliation']['orders']), 2)
                restored._connector.set_order_book(PAIR, 5, restored._connector.current_timestamp, [(90, 10)], [(90.1, 10)])
                store = NativeFundingStore(owner, restored.recorder)
                proof = info.custom_info['entry_reconciliation']
                info.custom_info['entry_reconciliation'] = {**proof, 'orders': proof['orders'][:1]}
                with self.assertRaisesRegex(PolicyError, 'trade coverage'):
                    store.reconcile()
                self.assertEqual(store._load()[1]['releases'], {})
                info.custom_info['entry_reconciliation'] = proof
                result = store.reconcile()
                self.assertEqual(result['released_intents'], 1)
                self.assertEqual(D(result['pending_quote']), D('0'))
                self.assertEqual(restored._connector.get_balance('BTC'), D('.1') if partial else D('0'))
                if not partial:
                    self.assertEqual(D(result['available_quote']), D('50'))
                else:
                    self.assertGreater(D(result['available_quote']), D('40'))
                    self.assertLess(D(result['available_quote']), D('41'))
            finally:
                await restored.close()

    async def test_rsi_v5_protective_native_sell_continues_while_terminal_get_is_pending(self):
        from controllers.generic.modular_rsi_v5 import ModularRSIV5ControllerConfig
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [funded_config(family=ModularRSIV5ControllerConfig)], Path(directory) / 'paper.sqlite')
            gate = asyncio.Event()
            status_started = asyncio.Event()
            original = session._connector._request_order_status

            async def delayed_status(tracked_order):
                if tracked_order.trade_type == TradeType.BUY:
                    status_started.set()
                    await gate.wait()
                return await original(tracked_order)

            try:
                await self.seed(session)
                owner = next(iter(session.controllers.values()))
                executor = session.executor_orchestrator.active_executors[owner.config.id][0]
                order = executor._open_order.order
                await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                        side=TradeType.SELL, price=order.price, amount=D('.1'))])
                session._connector.cancel(PAIR, order.client_order_id)
                await session._connector.drain()
                session._connector._request_order_status = delayed_status
                executor.early_stop(keep_position=False)
                step = asyncio.create_task(session.step(600062, books=[dict(trading_pair=PAIR, sequence=4, timestamp=600062,
                                                                            bids=[(90, 10)], asks=[(90.1, 10)])]))
                await asyncio.wait_for(status_started.wait(), timeout=1)
                # Let the executor resume past its background GET and place the
                # protective native sell while the GET remains deliberately held.
                for _ in range(50):
                    if session._connector.get_balance('BTC') == D('0'):
                        break
                    await asyncio.sleep(.01)
                self.assertEqual(session._connector.get_balance('BTC'), D('0'))
                self.assertFalse(executor.is_closed)  # Proof pending; exit already executed.
                gate.set()
                await step
                if executor._funding_entry_reconciliation_task is not None:
                    await executor._funding_entry_reconciliation_task
                await session.step(600063, books=[dict(trading_pair=PAIR, sequence=5, timestamp=600063,
                                                       bids=[(90, 10)], asks=[(90.1, 10)])])
                self.assertTrue(executor.is_closed)
                owner.executors_info = [executor.executor_info]
                self.assertEqual(owner._funding_store.reconcile()['released_intents'], 1)
            finally:
                gate.set()
                session._connector._request_order_status = original
                await session.close()
