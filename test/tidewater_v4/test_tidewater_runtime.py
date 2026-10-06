"""Tidewater on the native paper session: identity, pacing, receipts, resting entries.

Synthetic books and candles (the Meridian V3 harness), not economics.
"""
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.test_native_paper import BOOK, candles, frames, paper
from test.meridian_v3.test_unified_runtime import unified
from unittest import IsolatedAsyncioTestCase

import numpy as np
import pandas as pd
import pytest
from sqlalchemy.exc import OperationalError

from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig
from controllers.generic.tidewater_v4 import TidewaterController
from controllers.generic.tidewater_v4_core.config import TIDE_GATE_V1, TidewaterConfig
from controllers.generic.tidewater_v4_core.receipts import ADMITTED, REFUSED, REVOKED, read_receipts
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.paper_session import NativePaperSession

RISK = dict(max_spread_fraction='.003', fee_reserve_fraction='.003', cash_reserve_quote='0')
PRIORITY = ('BTC-USDC', 'ETH-USDC', 'BNB-USDC', 'SOL-USDC', 'XRP-USDC')
OWNER = 'modular_spot_tidewater_v4_paper'
# A tight gate so a five-tranche book (u ~ 0.25) already widens spacing: m(0.25) = 3.4.
TIGHT = dict(low_occupancy='0.05', high_occupancy='0.30', max_multiplier='4', upward_max_occupancy='0.20')


def tidewater(mode='paper', execution_enabled=False, **overrides):
    values = dict(id=f'modular_spot_tidewater_v4_{mode}', execution_mode=mode,
                  connector_name='okx_paper_trade' if mode == 'paper' else 'okx', account_id=f'tidewater_{mode}',
                  symbols=[{'trading_pair': p} for p in PAIRS], priority=PRIORITY, risk=RISK,
                  capital_stage='tidewater_1000')
    if mode != 'paper':
        values['data'] = {'mode': 'shared', 'transport': 'stream', 'origin': 'http://127.0.0.1:8092',
                          'token_file': '/run/candles/token'}
    values.update(overrides)
    if execution_enabled:
        values['recipe_binding'] = TidewaterConfig(**values).seal()
        values['execution_enabled'] = True
    return TidewaterConfig(**values)


def books(step, now, price=None, spread=D('.01'), pairs=PAIRS):
    result = []
    for pair in pairs:
        if price is None:
            result.append(dict(**{**BOOK, 'trading_pair': pair}, sequence=step + 2, timestamp=now))
        else:
            result.append(dict(trading_pair=pair, bids=[(float(price), 10)], asks=[(float(price + spread), 10)],
                               sequence=step + 2, timestamp=now))
    return result


def all_candles(frame, now, pairs=PAIRS):
    return [{**candles(frame, now), 'trading_pair': pair} for pair in pairs]


def second_rebound(depth):
    """After the first rebound: a slow decline to `depth` below 99.56, then a new RSI(17) cross of 29."""
    _, first = frames()
    start = float(first['close'].iloc[-1])
    low = start * (1 - float(depth))
    close = np.linspace(start, low, 90)
    t0 = int(first['timestamp'].iloc[-1]) + 60
    decline = pd.DataFrame(dict(timestamp=np.arange(t0, t0 + 90 * 60, 60), open=close, high=close + .05,
                                low=close - .05, close=close, volume=np.ones(90) * 10))
    before = pd.concat([first, decline], ignore_index=True)
    cut = t0 + 90 * 60
    rebound = dict(timestamp=cut, open=low, high=low + .2, low=low - .05, close=low + .15, volume=10)
    return before, pd.concat([before, pd.DataFrame([rebound])], ignore_index=True), cut, low


# ---------------------------------------------------------------------- contract
def test_tidewater_binds_its_own_identity_and_paced_stage():
    c = tidewater()
    assert c.get_controller_class() is TidewaterController
    assert c.tide_gate == TIDE_GATE_V1 and c.resting_entry == 'strict'
    m = c.mandate()
    # P0 per-asset stays at the envelope: one pair above a smaller mandate limit would hold every pair.
    assert (m.portfolio_id, m.funding_envelope_quote, m.per_asset_quote, m.order_quote) == ('tidewater_v4', 1000, 1000, 50)
    for symbol in c.symbol_configs().values():
        assert (symbol.capital.position_cap_quote, symbol.capital.funded_cap_quote, symbol.capital.max_active_tranches,
                symbol.depth_budget.budget_quote, symbol.budget.group) == (350, 350, 6, 350, 'tidewater_v4')
    assert c.seal().recipe_id == 'tidewater_v4_runtime_v1'


def test_seal_binds_tide_gate_and_resting_entry():
    sealed = tidewater(execution_enabled=True)
    for change in ({'tide_gate': None}, {'resting_entry': 'stable'}, {'tide_gate': TIGHT}):
        with pytest.raises(ValueError):
            TidewaterConfig(**{**sealed.model_dump(mode='json'), **change})
    assert tidewater(execution_enabled=True, tide_gate=None).seal() != sealed.seal()


@pytest.mark.parametrize('change', [
    {'id': 'modular_spot_meridian_v3_unified_paper'},
    {'capital_stage': 'full_1000'},
    {'total_amount_quote': '500'},
    {'priority': PRIORITY[:4]},
    {'resting_entry': 'loose'},
])
def test_invalid_tidewater_configurations_are_rejected(change):
    with pytest.raises(ValueError):
        tidewater(**change)


def test_meridian_and_tidewater_configurations_do_not_cross():
    with pytest.raises(ValueError):
        TidewaterController(unified(), None, None)
    with pytest.raises(ValueError):
        MeridianUnifiedConfig(**{**tidewater().model_dump(mode='json')})


def test_meridian_seal_is_unchanged_by_tidewater_sources():
    from controllers.generic.meridian_v3_unified_core.config import source_files as meridian_files
    assert not [p for p in meridian_files() if 'tidewater' in p.name or 'tidewater' in p.parent.name]


# ---------------------------------------------------------------------- native paper session
class TidewaterPaperTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    def session(self, directory, cash='1000', **overrides):
        return NativePaperSession(paper(PAIRS, cash), [tidewater(execution_enabled=True, **overrides)],
                                  Path(directory) / 'tidewater.sqlite')

    async def enter(self, session):
        before, rebound = frames()
        first = await session.step(600000, books=books(0, 600000), candles=all_candles(before, 600000))
        self.assertEqual(first['actions'], [])
        return await session.step(600060, books=books(1, 600060), candles=all_candles(rebound, 600060))

    async def fill_all(self, session, step, now):
        trades = [dict(trading_pair=p, sequence=step + i, timestamp=now, side=TradeType.SELL, price=D('99.49'),
                       amount=D('10')) for i, p in enumerate(PAIRS)]
        await session.step(now, trades=trades)
        for executor in session.executor_orchestrator.active_executors[OWNER]:
            self.assertEqual(executor.open_filled_amount, executor._open_order.order.amount)

    def receipts(self, session):
        return read_receipts(session.recorder, OWNER)

    async def test_first_tranches_are_admitted_on_all_pairs_with_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory)
            try:
                receipt = await self.enter(session)
                controller = session.controllers[OWNER]
                self.assertEqual(len(receipt['actions']), 5, controller.to_format_status())
                admitted = [r for r in self.receipts(session) if r['kind'] == ADMITTED]
                self.assertEqual({r['pair'] for r in admitted}, set(PAIRS))
                self.assertTrue(all(r['pacing']['outcome'] == 'paced_ok' for r in admitted))
                self.assertTrue(all(r['pacing']['reference'] is None for r in admitted))
                info = controller.get_custom_info()['tidewater']
                self.assertEqual(info['pair_limit_quote'], '350')
                self.assertIsNone(info['receipts']['fault'])
                self.assertEqual(controller.portfolio.last_target.status, 'PROPOSAL_ONLY')
                self.assertTrue(controller.to_format_status()[0].startswith('Tidewater v4 | tidewater_v4_runtime_v1'))
            finally:
                await session.close()

    async def test_gate_refuses_a_shallow_add_without_consuming_the_cut_then_admits_deeper(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, tide_gate=TIGHT)
            try:
                await self.enter(session)
                await self.fill_all(session, 10, 600061)
                controller = session.controllers[OWNER]
                before, rebound, cut, low = second_rebound(D('.006'))
                price = D(str(round(low, 2)))
                await session.step(cut, books=books(20, cut, price), candles=all_candles(before, cut))
                shallow = await session.step(cut + 60, books=books(21, cut + 60, price),
                                             candles=all_candles(rebound, cut + 60))
                refused = [r for r in self.receipts(session) if r['kind'] == REFUSED]
                self.assertEqual(shallow['actions'], [], controller.to_format_status())
                self.assertEqual({r['code'] for r in refused}, {'tide_gate_down'}, refused)
                self.assertEqual(controller.portfolio.status['reason'], 'PACED')
                # Same cut, a deeper book: the refusal did not consume the cut, so the pair is admitted.
                deep = price * D('.985')
                deeper = await session.step(cut + 61, books=books(22, cut + 61, deep))
                pairs = {a.executor_config.trading_pair for a in deeper['actions']}
                self.assertTrue(pairs, controller.to_format_status())
                admitted = [r for r in self.receipts(session) if r['kind'] == ADMITTED and r['cut'] == cut]
                self.assertEqual({r['pair'] for r in admitted}, pairs)
            finally:
                await session.close()

    async def test_without_the_gate_the_shallow_add_is_admitted(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, tide_gate=None)
            try:
                await self.enter(session)
                await self.fill_all(session, 10, 600061)
                before, rebound, cut, low = second_rebound(D('.006'))
                price = D(str(round(low, 2)))
                await session.step(cut, books=books(20, cut, price), candles=all_candles(before, cut))
                shallow = await session.step(cut + 60, books=books(21, cut + 60, price),
                                             candles=all_candles(rebound, cut + 60))
                self.assertTrue(shallow['actions'], session.controllers[OWNER].to_format_status())
                self.assertFalse([r for r in self.receipts(session) if r['kind'] == REFUSED])
            finally:
                await session.close()

    async def test_strict_resting_entry_is_revoked_by_a_spread_spike_with_a_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory)
            try:
                await self.enter(session)
                executors = list(session.executor_orchestrator.active_executors[OWNER])
                wide = books(3, 600062, D('99.5'), spread=D('1'))
                await session.step(600062, books=wide)
                revoked = [r for r in self.receipts(session) if r['kind'] == REVOKED]
                self.assertEqual(len(revoked), 5, self.receipts(session))
                self.assertEqual({r['phase'] for r in revoked}, {'submitted'})
                self.assertEqual({r['predicate'] for r in revoked}, {'strict:spread_limit'})
                await session.step(600063, books=books(4, 600063))  # the venue cancel completes on the next tick
                self.assertFalse([e for e in executors if e.is_active], [e.status for e in executors])
            finally:
                await session.close()

    async def test_stable_resting_entry_survives_a_spread_spike_and_ends_on_pause(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, resting_entry='stable')
            try:
                await self.enter(session)
                controller = session.controllers[OWNER]
                executors = list(session.executor_orchestrator.active_executors[OWNER])
                await session.step(600062, books=books(3, 600062, D('99.5'), spread=D('1')))
                await session.step(600063, books=books(4, 600063, D('99.5'), spread=D('1')))
                self.assertFalse([r for r in self.receipts(session) if r['kind'] == REVOKED])
                self.assertTrue(all(e.is_active for e in executors))
                controller.set_operator_entry_pause(True, reason='test')
                await session.step(600064, books=books(5, 600064))
                revoked = [r for r in self.receipts(session) if r['kind'] == REVOKED]
                self.assertEqual({(r['phase'], r['predicate']) for r in revoked}, {('submitted', 'operator_entry_pause')})
            finally:
                await session.close()

    async def test_receipt_failure_is_reported_and_never_blocks_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory)
            try:
                controller = session.controllers[OWNER]

                class Broken:
                    def append_rsi_v5_runtime_events(self, events):
                        raise RuntimeError('injected receipt store failure')
                controller.portfolio.receipts._recorder = Broken()
                receipt = await self.enter(session)
                self.assertEqual(len(receipt['actions']), 5)
                fault = controller.get_custom_info()['tidewater']['receipts']['fault']
                self.assertIn('injected receipt store failure', fault['error'])
            finally:
                await session.close()

    async def test_a_pair_that_cannot_be_paced_waits_while_the_others_enter(self):
        for error in (PolicyError('injected unreadable checkpoint'), RuntimeError('injected recorder failure'),
                      OperationalError('SELECT', {}, Exception('injected database failure'))):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as directory:
                session = self.session(directory)
                try:
                    controller = session.controllers[OWNER]
                    original = controller.portfolio._reference

                    def reference(pair, error=error, original=original):
                        if pair == 'BTC-USDC':
                            raise error
                        return original(pair)
                    controller.portfolio._reference = reference
                    receipt = await self.enter(session)
                    pairs = {a.executor_config.trading_pair for a in receipt['actions']}
                    self.assertEqual(pairs, set(PAIRS) - {'BTC-USDC'}, controller.to_format_status())
                    refused = [r for r in self.receipts(session) if r['kind'] == REFUSED]
                    self.assertEqual([(r['pair'], r['code']) for r in refused], [('BTC-USDC', 'pacing_unavailable')])
                    self.assertIsNone(controller.fault)
                finally:
                    await session.close()

    async def test_stable_entry_ends_on_a_portfolio_fault_with_a_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, resting_entry='stable')
            try:
                await self.enter(session)
                controller = session.controllers[OWNER]
                executor = session.executor_orchestrator.active_executors[OWNER][0]
                self.assertTrue(controller.entry_permission(executor.config, check_funds=False))
                controller.fault = 'injected portfolio evaluation failure'
                self.assertFalse(controller.entry_permission(executor.config, check_funds=False))
                revoked = [r for r in self.receipts(session) if r['kind'] == REVOKED]
                self.assertEqual([r['predicate'] for r in revoked], ['portfolio_fault'])
            finally:
                await session.close()

    async def test_a_stale_book_after_the_window_is_attributed_where_meridian_denied(self):
        # Meridian reads the book before the entry window: a stale book denies at the snapshot.
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory)
            try:
                await self.enter(session)
                controller = session.controllers[OWNER]
                executor = session.executor_orchestrator.active_executors[OWNER][0]
                await session.step(600060 + 400)  # no new books: every book is now stale
                self.assertFalse(controller.entry_permission(executor.config, check_funds=False))
                predicates = [r['predicate'] for r in self.receipts(session)
                              if r['kind'] == REVOKED and r['executor_id'] == executor.config.id]
                self.assertTrue(predicates and all(p.startswith('strict:data_hold') for p in predicates), predicates)
            finally:
                await session.close()

    async def test_attribution_failure_never_changes_or_delays_a_denial(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory)
            try:
                await self.enter(session)
                controller = session.controllers[OWNER]
                executors = list(session.executor_orchestrator.active_executors[OWNER])

                def broken(*args, **kwargs):
                    raise OperationalError('SELECT', {}, Exception('injected attribution failure'))
                controller._attribute = broken
                await session.step(600062, books=books(3, 600062, D('99.5'), spread=D('1')))
                await session.step(600063, books=books(4, 600063))
                self.assertFalse([e for e in executors if e.is_active])
                revoked = [r for r in self.receipts(session) if r['kind'] == REVOKED]
                self.assertEqual(len(revoked), 5)
                self.assertTrue(all(r['predicate'].startswith('strict:unattributed:OperationalError') for r in revoked))
            finally:
                await session.close()

    async def test_a_failing_stable_check_ends_the_entry_and_never_raises(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, resting_entry='stable')
            try:
                await self.enter(session)
                controller = session.controllers[OWNER]
                executor = session.executor_orchestrator.active_executors[OWNER][0]

                def broken(*args, **kwargs):
                    raise OperationalError('SELECT', {}, Exception('injected grant read failure'))
                controller.portfolio.grant_bound = broken
                self.assertFalse(controller.entry_permission(executor.config, check_funds=False))
                revoked = [r for r in self.receipts(session) if r['kind'] == REVOKED]
                self.assertTrue(revoked and revoked[0]['predicate'].startswith('stable_check_error:OperationalError'))
            finally:
                await session.close()

    async def test_admission_survives_a_clock_that_moves_between_pacing_and_evaluation(self):
        # Live clocks move between pace() and Meridian's evaluate(); paper steps hide it.
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory)
            try:
                controller = session.controllers[OWNER]
                original = type(controller.portfolio).now_ms
                calls = {'n': 0}

                def moving(portfolio):
                    calls['n'] += 1
                    return original(portfolio) + calls['n']
                controller.portfolio.now_ms = moving.__get__(controller.portfolio)
                receipt = await self.enter(session)
                self.assertEqual(len(receipt['actions']), 5, controller.get_custom_info()['target'])
            finally:
                await session.close()

    async def test_a_repeated_stale_book_is_attributed_to_the_snapshot_every_time(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory)
            try:
                await self.enter(session)
                controller = session.controllers[OWNER]
                executor = session.executor_orchestrator.active_executors[OWNER][0]
                await session.step(600060 + 120)  # no new books: stale
                controller.portfolio.receipts._seen.clear()
                predicates = []
                for _ in range(2):
                    self.assertFalse(controller.entry_permission(executor.config, check_funds=False))
                    predicates.append(controller._last_denial)
                self.assertTrue(all(p.startswith('strict:data_hold') for p in predicates), predicates)
            finally:
                await session.close()

    async def test_an_uncomputable_identity_holds_entries_instead_of_stopping_the_controller(self):
        from controllers.generic.tidewater_v4_core import portfolio as tidewater_portfolio
        with tempfile.TemporaryDirectory() as directory:
            original = tidewater_portfolio.owner_identity

            def broken(config, recipe):
                raise PolicyError('injected identity drift')
            tidewater_portfolio.owner_identity = broken
            try:
                session = self.session(directory)  # the controller still starts
                try:
                    receipt = await self.enter(session)
                    controller = session.controllers[OWNER]
                    self.assertEqual(receipt['actions'], [])
                    self.assertTrue(controller.portfolio.identity_hold.startswith('IDENTITY_UNVERIFIED'))
                finally:
                    await session.close()
            finally:
                tidewater_portfolio.owner_identity = original

    async def test_admission_receipts_are_per_pair_when_cash_admits_only_some(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, cash='120')  # two 50.15 increments, by priority
            try:
                receipt = await self.enter(session)
                pairs = {a.executor_config.trading_pair for a in receipt['actions']}
                self.assertEqual(pairs, {'BTC-USDC', 'ETH-USDC'})
                codes = {r['pair']: r['code'] for r in self.receipts(session) if r['kind'] == ADMITTED}
                self.assertEqual(codes, {'BTC-USDC': 'admitted', 'ETH-USDC': 'admitted', 'BNB-USDC': 'not_admitted',
                                         'SOL-USDC': 'not_admitted', 'XRP-USDC': 'not_admitted'})
            finally:
                await session.close()

    async def test_a_filled_entry_past_its_window_is_not_recorded_as_revoked(self):
        # The executor keeps asking permission after a full fill; the denial cancels nothing.
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory)
            try:
                await self.enter(session)
                await self.fill_all(session, 10, 600061)
                await session.step(600060 + 400, books=books(4, 600060 + 400))
                self.assertFalse([r for r in self.receipts(session) if r['kind'] == REVOKED])
                self.assertEqual(sum(e.is_active for e in session.executor_orchestrator.active_executors[OWNER]), 5)
            finally:
                await session.close()

    async def test_strict_denial_after_the_entry_window_names_the_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory)
            try:
                await self.enter(session)
                controller = session.controllers[OWNER]
                executor = session.executor_orchestrator.active_executors[OWNER][0]
                await session.step(600060 + 400, books=books(5, 600060 + 400))
                self.assertFalse(controller.entry_permission(executor.config, check_funds=False))
                predicates = {r['predicate'] for r in self.receipts(session)
                              if r['kind'] == REVOKED and r['executor_id'] == executor.config.id}
                self.assertTrue(predicates and all('entry_timeout' in p for p in predicates), predicates)
            finally:
                await session.close()


class Collecting:
    def __init__(self):
        self.events = []

    def append_rsi_v5_runtime_events(self, events):
        self.events.extend(events)
        return events


def test_changed_denial_predicates_are_never_deduplicated_away():
    from controllers.generic.tidewater_v4_core.receipts import ReceiptWriter
    recorder = Collecting()
    writer = ReceiptWriter('modular_spot_tidewater_v4_paper', recorder=recorder)
    for predicate in ('strict:data_hold:stale_book', 'strict:operator_entry_pause', 'strict:operator_entry_pause'):
        writer.record(REVOKED, pair='BTC-USDC', cut='executor-1', code='submitted', dedupe=predicate,
                      payload={'executor_id': 'executor-1', 'predicate': predicate}, now=1)
    assert [json_predicate(e) for e in recorder.events] == ['strict:data_hold:stale_book', 'strict:operator_entry_pause']


def json_predicate(event):
    import json
    return json.loads(event['payload_json'])['predicate']


def test_receipt_faults_keep_their_interval():
    from controllers.generic.tidewater_v4_core.receipts import ReceiptWriter

    class Broken:
        def append_rsi_v5_runtime_events(self, events):
            raise RuntimeError('down')
    writer = ReceiptWriter('modular_spot_tidewater_v4_paper', recorder=Broken())
    for now in (10, 20):
        writer.record(REVOKED, pair='BTC-USDC', cut=f'e{now}', code='submitted', payload={}, now=now)
    assert (writer.fault['first_at'], writer.fault['last_at'], writer.fault['count']) == (10, 20, 2)
