"""Tidewater signal census (review K2): complete population, durable ordered outcomes, inert census-only arm.

Synthetic books and candles on the native paper session (the Meridian V3 harness), not economics.
"""
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.test_native_paper import frames, paper
from test.tidewater_v4.test_tidewater_candidates import crash_frames
from test.tidewater_v4.test_tidewater_runtime import OWNER, PAIRS, TIGHT, all_candles, books, second_rebound, tidewater
from unittest import IsolatedAsyncioTestCase

import pandas as pd

from controllers.generic.tidewater_v4_core.receipts import CENSUS, FINAL, OUTCOME, read_receipts
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.paper_session import NativePaperSession

CENSUS_KINDS = {CENSUS, OUTCOME, FINAL}


class FailingCensusStore:
    """The real recorder, except that census outcome and final appends fail while `on` is set."""

    def __init__(self, inner):
        self.inner, self.on = inner, True

    def append_rsi_v5_runtime_events(self, events):
        if self.on and any(e['event_type'] in (OUTCOME, FINAL) for e in events):
            raise RuntimeError('injected census store failure')
        return self.inner.append_rsi_v5_runtime_events(events)

    def get_rsi_v5_runtime_events(self, controller_id):
        return self.inner.get_rsi_v5_runtime_events(controller_id)


class FailingHistory:
    """The real recorder, except that reading the receipt history fails while `on` is set."""

    def __init__(self, inner):
        self.inner, self.on = inner, True

    def append_rsi_v5_runtime_events(self, events):
        return self.inner.append_rsi_v5_runtime_events(events)

    def get_rsi_v5_runtime_events(self, controller_id):
        if self.on:
            raise RuntimeError('injected history read failure')
        return self.inner.get_rsi_v5_runtime_events(controller_id)


def one_more_candle(frame):
    """The frame plus one flat closed candle: the signal's cut passes."""
    last = frame.iloc[-1]
    row = dict(timestamp=int(last.timestamp) + 60, open=float(last.close), high=float(last.close) + .05,
               low=float(last.close) - .05, close=float(last.close), volume=10)
    return pd.concat([frame, pd.DataFrame([row])], ignore_index=True)


class TidewaterCensusTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    def config(self, **overrides):
        overrides.setdefault('census', True)
        return tidewater(execution_enabled=True, **overrides)

    def session(self, db, config=None, restore=False):
        return NativePaperSession(paper(PAIRS, '1000'), [config or self.config()], db, restore=restore)

    async def enter(self, session, pair_frames=None, price=None):
        before, rebound = frames() if pair_frames is None else pair_frames
        await session.step(600000, books=books(0, 600000, price), candles=all_candles(before, 600000))
        return await session.step(600060, books=books(1, 600060, price), candles=all_candles(rebound, 600060))

    def receipts(self, session, kind=None):
        return [r for r in read_receipts(session.recorder, OWNER) if kind is None or r['kind'] == kind]

    def transitions(self, session, cut):
        result = {}
        for r in sorted(self.receipts(session, OUTCOME), key=lambda r: (r['pair'], r['seq'])):
            if r['cut'] == cut:
                result.setdefault(r['pair'], []).append(r['code'])
        return result

    def finals(self, session, cut):
        return {r['pair']: r for r in self.receipts(session, FINAL) if r['cut'] == cut}

    # ------------------------------------------------------------------ population (K2.1)
    async def test_an_entry_disabled_pair_is_censused_with_an_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            symbols = [{'trading_pair': p, 'entry_enabled': p != 'BTC-USDC'} for p in PAIRS]
            session = self.session(Path(directory) / 't.sqlite', self.config(symbols=symbols))
            try:
                receipt = await self.enter(session)
                self.assertEqual({a.executor_config.trading_pair for a in receipt['actions']}, set(PAIRS) - {'BTC-USDC'})
                self.assertEqual({r['pair'] for r in self.receipts(session, CENSUS)}, set(PAIRS))
                self.assertEqual(self.transitions(session, 600000),
                                 {p: ['entry_disabled' if p == 'BTC-USDC' else 'admitted'] for p in PAIRS})
                # Admission is terminal at once; the disabled signal is finalized when its cut passes.
                self.assertEqual({p: r['code'] for p, r in self.finals(session, 600000).items()},
                                 {p: 'admitted' for p in PAIRS if p != 'BTC-USDC'})
                _, rebound = frames()
                await session.step(600120, books=books(2, 600120), candles=all_candles(one_more_candle(rebound), 600120))
                self.assertEqual(self.finals(session, 600000)['BTC-USDC']['code'], 'entry_disabled')
            finally:
                await session.close()

    async def test_identity_held_signals_are_censused_with_an_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(Path(directory) / 't.sqlite')
            try:
                portfolio = session.controllers[OWNER].portfolio
                before, rebound = frames()
                await session.step(600000, books=books(0, 600000), candles=all_candles(before, 600000))

                def held():
                    portfolio.identity_hold = 'identity_unverified:injected'
                    return portfolio.identity_hold
                portfolio.check_identity = held
                receipt = await session.step(600060, books=books(1, 600060), candles=all_candles(rebound, 600060))
                self.assertEqual(receipt['actions'], [])
                self.assertEqual({r['pair'] for r in self.receipts(session, CENSUS)}, set(PAIRS))
                self.assertEqual(self.transitions(session, 600000), {p: ['identity_held'] for p in PAIRS})
            finally:
                await session.close()

    async def test_kill_switch_held_signals_are_censused_with_an_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(Path(directory) / 't.sqlite', self.config(manual_kill_switch=True))
            try:
                receipt = await self.enter(session)
                self.assertEqual(receipt['actions'], [])
                self.assertEqual({r['pair'] for r in self.receipts(session, CENSUS)}, set(PAIRS))
                self.assertEqual(self.transitions(session, 600000), {p: ['kill_switch_held'] for p in PAIRS})
            finally:
                await session.close()

    # ------------------------------------------------------------------ durable ordered outcomes (K2.2)
    async def test_transitions_are_ordered_never_suppressed_and_finalized_once_with_durable_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(Path(directory) / 't.sqlite')
            try:
                controller = session.controllers[OWNER]
                before, rebound, price = crash_frames()
                await self.enter(session, (before, rebound), price)  # first entries keep the native pause
                controller.set_operator_entry_pause(True, reason='test')
                await session.step(600061, books=books(2, 600061, price))
                controller.set_operator_entry_pause(False, reason='test')
                await session.step(600062, books=books(3, 600062, price))
                self.assertEqual(self.transitions(session, 600000),
                                 {p: ['native_paused', 'preview_held', 'native_paused'] for p in PAIRS})
                self.assertFalse(self.finals(session, 600000))  # the cut is still current
                await session.step(600120, books=books(4, 600120, price),
                                   candles=all_candles(one_more_candle(rebound), 600120))
                finals = self.finals(session, 600000)
                self.assertEqual({p: (r['code'], r['transitions']) for p, r in finals.items()},
                                 {p: ('native_paused', 3) for p in PAIRS})
                await session.step(600121, books=books(5, 600121, price))
                self.assertEqual(len(self.finals(session, 600000)), 5)
                self.assertEqual(len([r for r in self.receipts(session, FINAL) if r['cut'] == 600000]), 5)
                # Persistence keys are the durable census identity, not a hash of the payload.
                rows = session.recorder.get_rsi_v5_runtime_events(OWNER)
                census_ids = {r['pair']: r['census_id'] for r in self.receipts(session, CENSUS)}
                ids = {row.event_id for row in rows}
                for cid in census_ids.values():
                    self.assertIn(f'tidewater:census:{cid}', ids)
                    self.assertIn(f'tidewater:final:{cid}', ids)
                    self.assertTrue({f'tidewater:outcome:{cid}:{k}' for k in (1, 2, 3)} <= ids)
            finally:
                await session.close()

    async def test_a_failed_outcome_after_admission_is_retried_on_the_next_tick(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(Path(directory) / 't.sqlite')
            try:
                controller = session.controllers[OWNER]
                store = FailingCensusStore(session.recorder)
                controller.portfolio.receipts._recorder = store
                receipt = await self.enter(session)
                self.assertEqual(len(receipt['actions']), 5)
                self.assertFalse(self.receipts(session, OUTCOME))
                self.assertIn('injected census store failure', controller.portfolio.receipts.fault['error'])
                store.on = False
                await session.step(600061, books=books(2, 600061))
                self.assertEqual(self.transitions(session, 600000), {p: ['admitted'] for p in PAIRS})
                self.assertEqual({p: r['code'] for p, r in self.finals(session, 600000).items()},
                                 {p: 'admitted' for p in PAIRS})
            finally:
                await session.close()

    async def test_a_restart_on_the_same_database_reconciles_a_lost_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 't.sqlite'
            config = self.config()
            session = self.session(db, config)
            try:
                store = FailingCensusStore(session.recorder)
                session.controllers[OWNER].portfolio.receipts._recorder = store
                receipt = await self.enter(session)
                self.assertEqual(len(receipt['actions']), 5)
                self.assertFalse(self.receipts(session, OUTCOME))  # lost in this process
                census_before = {r['pair']: r['census_id'] for r in self.receipts(session, CENSUS)}
            finally:
                await session.close()
            restored = self.session(db, config, restore=True)
            try:
                _, rebound = frames()
                await restored.step(600061, books=books(2, 600061), candles=all_candles(rebound, 600061))
                finals = self.finals(restored, 600000)
                self.assertEqual({p: (r['code'], r['reason']) for p, r in finals.items()},
                                 {p: ('admitted', 'reconciled_from_admission_receipt') for p in PAIRS})
                census = self.receipts(restored, CENSUS)
                self.assertEqual(len(census), 5)  # the restart re-observed the same signals, not new ones
                self.assertEqual({r['pair']: r['census_id'] for r in census}, census_before)
            finally:
                await restored.close()

    # ------------------------------------------------------------------ history read failure (K3)
    async def test_signals_seen_while_the_history_read_fails_are_recorded_once_it_recovers(self):
        # K3 repro: kill switch on; the signals fire on cut 600000 while the history read fails;
        # reads recover at the non-signalling cut 600060.
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(Path(directory) / 't.sqlite', self.config(manual_kill_switch=True))
            try:
                portfolio = session.controllers[OWNER].portfolio
                history = FailingHistory(session.recorder)
                portfolio.receipts._recorder = history
                receipt = await self.enter(session)
                self.assertEqual(receipt['actions'], [])
                self.assertFalse(self.receipts(session, CENSUS))
                self.assertFalse(portfolio.census.loaded)
                self.assertIn('injected history read failure', portfolio.census.fault)
                history.on = False
                _, rebound = frames()
                await session.step(600120, books=books(2, 600120), candles=all_candles(one_more_candle(rebound), 600120))
                self.assertEqual({r['pair'] for r in self.receipts(session, CENSUS) if r['cut'] == 600000}, set(PAIRS))
                self.assertFalse([r for r in self.receipts(session, CENSUS) if r['cut'] == 600060])  # no signal there
                self.assertEqual(self.transitions(session, 600000), {p: ['kill_switch_held'] for p in PAIRS})
                self.assertEqual({p: (r['code'], r['reason']) for p, r in self.finals(session, 600000).items()},
                                 {p: ('kill_switch_held', 'cut_passed') for p in PAIRS})
                report = portfolio.census.report()
                self.assertEqual((report['loaded'], report['queued'], report['pending_history']), (True, 0, 0))
            finally:
                await session.close()

    async def test_signals_seen_across_several_failed_history_reads_are_reconciled_completely(self):
        with tempfile.TemporaryDirectory() as directory:
            symbols = [{'trading_pair': p, 'entry_enabled': p != 'BTC-USDC'} for p in PAIRS]
            session = self.session(Path(directory) / 't.sqlite', self.config(symbols=symbols))
            try:
                portfolio = session.controllers[OWNER].portfolio
                history = FailingHistory(session.recorder)
                portfolio.receipts._recorder = history
                receipt = await self.enter(session)  # admitted while the history read fails
                self.assertEqual(len(receipt['actions']), 4)
                _, rebound = frames()
                frame = rebound
                for k in range(3):  # three more cuts pass with the read still failing
                    frame = one_more_candle(frame)
                    now = 600120 + 60 * k
                    await session.step(now, books=books(2 + k, now), candles=all_candles(frame, now))
                self.assertFalse(self.receipts(session, CENSUS))
                history.on = False
                frame = one_more_candle(frame)
                await session.step(600300, books=books(9, 600300), candles=all_candles(frame, 600300))
                census = self.receipts(session, CENSUS)
                self.assertEqual(sorted((r['pair'], r['cut']) for r in census), sorted((p, 600000) for p in PAIRS))
                self.assertEqual(self.transitions(session, 600000),
                                 {p: ['entry_disabled' if p == 'BTC-USDC' else 'admitted'] for p in PAIRS})
                self.assertEqual({p: (r['code'], r['reason'], r['transitions'])
                                  for p, r in self.finals(session, 600000).items()},
                                 {p: ('entry_disabled', 'cut_passed', 1) if p == 'BTC-USDC'
                                  else ('admitted', 'admission_settled', 1) for p in PAIRS})
                self.assertEqual(sorted(r['seq'] for r in self.receipts(session, OUTCOME)), [1] * 5)
                await session.step(600301, books=books(10, 600301))  # nothing is written twice
                self.assertEqual(len(self.receipts(session, CENSUS)), 5)
                self.assertEqual(len(self.receipts(session, FINAL)), 5)
                self.assertEqual(portfolio.census.report()['pending_history'], 0)
            finally:
                await session.close()

    # ------------------------------------------------------------------ inert census-only arm (K2.3)
    async def scenario(self, directory, census):
        """B0 on identical frames: first entries, fills, a refused shallow add, then a deeper one."""
        session = self.session(Path(directory) / f'census_{census}.sqlite', self.config(census=census, tide_gate=TIGHT))
        steps = []
        try:
            steps.append(await self.enter(session))
            trades = [dict(trading_pair=p, sequence=10 + i, timestamp=600061, side=TradeType.SELL, price=D('99.49'),
                           amount=D('10')) for i, p in enumerate(PAIRS)]
            steps.append(await session.step(600061, trades=trades))
            before, rebound, cut, low = second_rebound(D('.006'))
            price = D(str(round(low, 2)))
            steps.append(await session.step(cut, books=books(20, cut, price), candles=all_candles(before, cut)))
            steps.append(await session.step(cut + 60, books=books(21, cut + 60, price),
                                            candles=all_candles(rebound, cut + 60)))
            steps.append(await session.step(cut + 61, books=books(22, cut + 61, price * D('.985'))))
            actions = [[(type(a).__name__, getattr(a, 'executor_id', None),
                         None if getattr(a, 'executor_config', None) is None else
                         a.executor_config.model_dump(mode='json')) for a in s['actions']] for s in steps]
            rows = [(r.event_type, r.episode_id, r.payload_json) for r in session.recorder.get_rsi_v5_runtime_events(OWNER)]
            executors = sorted((e.config.id, str(e.open_filled_amount))
                               for e in session.executor_orchestrator.active_executors[OWNER])
            return actions, rows, executors
        finally:
            await session.close()

    async def test_the_census_only_option_changes_no_decision_order_or_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            off_actions, off_rows, off_executors = await self.scenario(directory, False)
            on_actions, on_rows, on_executors = await self.scenario(directory, True)
        self.assertTrue(any(off_actions) and off_executors)
        self.assertEqual(on_actions, off_actions)
        self.assertEqual(on_executors, off_executors)
        self.assertFalse([r for r in off_rows if r[0] in CENSUS_KINDS])
        self.assertTrue([r for r in on_rows if r[0] == CENSUS])
        self.assertEqual([r for r in on_rows if r[0] not in CENSUS_KINDS], off_rows)
