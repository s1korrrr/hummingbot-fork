"""Tidewater V4.1: spacing (M2/M4) in pace(), no time pause and fixed trailing as recipe options."""
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.test_native_paper import frames, paper
from test.tidewater_v4 import test_tidewater_runtime as runtime_tests  # a module import: no re-collection
from test.tidewater_v4.test_pace import ONLY_BNB, markets, portfolio, snapshot
from test.tidewater_v4.test_tidewater_runtime import OWNER, PAIRS, all_candles, books, second_rebound, tidewater
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase

import pytest

from controllers.generic.modular_spot_core.domain import DataUnavailable
from controllers.generic.tidewater_v4_core.config import SPACING_V41, TidewaterConfig
from controllers.generic.tidewater_v4_core.portfolio import TidewaterPortfolio
from controllers.generic.tidewater_v4_core.receipts import ADMITTED, REFUSED
from controllers.generic.tidewater_v4_core.spacing import hourly_atr_fraction
from hummingbot.strategy_v2.paper_session import NativePaperSession


def v41(**overrides):
    values = dict(capital_stage='tidewater_bnb_uncapped', symbols=ONLY_BNB, tide_gate=None, spacing=SPACING_V41,
                  flash_pause='off', exit_trailing='fixed_v41')
    return tidewater(**{**values, **overrides})


def spaced(config, *, tranches=(), bag=None, atr=D('0.005')):
    p = portfolio(config, {})
    p._zones = lambda pair, bid: (tuple(D(x) for x in tranches), None if bag is None else D(bag))
    p._hourly_atr = lambda pair, market: atr
    return p


# ---------------------------------------------------------------------------- pace()
def test_spacing_paces_without_the_tide_gate_and_keeps_a_gap_from_every_zone():
    p = spaced(v41(), tranches=('100', '95'))
    # gap = 2 x 0.5% = 1%; bid 95.5 is 0.53% above the older 95 zone.
    assert p.pace(('BNB-USDC',), markets(), snapshot(bid=D('95.5')), 1) == ()
    decision = p.pacing['BNB-USDC']
    spacing = decision['spacing']
    assert decision['outcome'] == 'spacing_gap' and D(spacing['gap']) == D('0.010')
    assert D(spacing['nearest']) == 95 and D(spacing['atr_1h']) == D('0.005')
    assert spacing['zones'] == ['100', '95']  # the zone snapshot the decision was checked against
    assert p.pace(('BNB-USDC',), markets(), snapshot(bid=D('97.5')), 1) == ('BNB-USDC',)
    assert p.pacing['BNB-USDC']['outcome'] == 'paced_ok'


def test_with_the_tide_gate_on_both_rules_keep_their_own_receipt_fields():
    # Astra S4-3: the tide gate's native gap must not overwrite the spacing gap in the pacing snapshot.
    from controllers.generic.tidewater_v4_core.config import TIDE_GATE_V1
    p = spaced(v41(tide_gate=TIDE_GATE_V1), tranches=('100',))
    p._reference = lambda pair, bid: D('100')
    assert p.pace(('BNB-USDC',), markets(), snapshot(bid=D('98')), 1) == ('BNB-USDC',)
    decision = p.pacing['BNB-USDC']
    assert D(decision['spacing']['gap']) == D('0.010')  # 2 x ATR_1h, applied by spacing
    assert D(decision['gap']) == D('0.008')  # the tide gate's native base gap


def test_latest_zone_spacing_is_the_m2_rung():
    from controllers.generic.tidewater_v4_core.spacing import SpacingConfig
    p = spaced(v41(spacing=SpacingConfig(atr_multiplier='2', zones='latest')), tranches=('100', '95'))
    assert p.pace(('BNB-USDC',), markets(), snapshot(bid=D('100.6')), 1) == ('BNB-USDC',)  # 5.9% from 95


def test_the_first_tranche_is_not_spaced():
    p = spaced(v41())
    assert p.pace(('BNB-USDC',), markets(), snapshot(bid=D('100')), 1) == ('BNB-USDC',)


def test_missing_hourly_history_makes_the_pair_wait():
    p = spaced(v41(), tranches=('100',))

    def unavailable(pair, market):
        raise DataUnavailable('spacing needs 15 completed hours, has 3')
    p._hourly_atr = unavailable
    assert p.pace(('BNB-USDC',), markets(), snapshot(bid=D('90')), 1) == ()
    assert p.pacing['BNB-USDC']['outcome'] == 'pacing_unavailable'
    assert 'completed hours' in p.pacing['BNB-USDC']['error']


def test_spacing_refusals_are_receipted():
    p = spaced(v41(), tranches=('100',))
    p.pace(('BNB-USDC',), markets(), snapshot(bid=D('99.5')), 1)
    events = p.receipts.recorder().events
    assert [e['event_type'] for e in events] == ['TIDEWATER_GATE_REFUSED'] and '"code":"spacing_gap"' in events[0]['payload_json']


# ---------------------------------------------------------------------------- zones
def _zone_portfolio(*, claims, inventory, held_amount='0', breakeven='100'):
    config = v41()
    store = SimpleNamespace(load=lambda: (SimpleNamespace(claims=claims), None))
    runtime = SimpleNamespace(ensure_store=lambda: store, inventory=lambda: inventory,
                              snapshot=lambda: {'amount_step': D('0.001'), 'minimum_amount': D('0.01'),
                                                'minimum_notional': D('5')})
    position = SimpleNamespace(amount=D(held_amount), side='BUY', connector_name='okx', trading_pair='BNB-USDC',
                               breakeven_price=D(breakeven), remaining_fees_quote=D('0'))
    contexts = {'BNB-USDC': SimpleNamespace(runtime=runtime, positions_held=[position])}
    owner = SimpleNamespace(config=config, contexts=contexts, symbol_config=lambda pair: SimpleNamespace(connector_name='okx'))
    p = TidewaterPortfolio.__new__(TidewaterPortfolio)
    p.owner = owner
    return p


def tranche(price, quote='50', terminal=False):
    return SimpleNamespace(entry_price=D(price), entry_quote=D(quote), terminal=terminal)


def test_zones_are_active_acquired_tranches_in_claim_order_and_the_non_dust_bag():
    inventory = {'a': tranche('100'), 'b': tranche('95'), 'c': tranche('90', terminal=True), 'd': tranche('93', '0')}
    p = _zone_portfolio(claims=['a', 'b', 'c', 'd'], inventory=inventory, held_amount='1', breakeven='120')
    assert p._zones('BNB-USDC', D('94')) == ((D('100'), D('95')), D('120'))
    assert p._reference('BNB-USDC', D('94')) == D('95')  # the tide gate's reference is the latest zone
    dust = _zone_portfolio(claims=['a'], inventory=inventory, held_amount='0.001')
    assert dust._zones('BNB-USDC', D('94')) == ((D('100'),), None)


# ---------------------------------------------------------------------------- recipe options
def test_v41_turns_the_native_pause_off_and_uses_fixed_trailing():
    bnb = v41().symbol_configs()['BNB-USDC']
    assert bnb.flash_crash is None and bnb.adaptive_trailing is None
    assert bnb.trailing.activation == D('.012') and bnb.trailing.delta == D('.005')
    native = tidewater(capital_stage='tidewater_bnb_uncapped', symbols=ONLY_BNB).symbol_configs()['BNB-USDC']
    assert native.flash_crash is not None and native.adaptive_trailing is not None  # defaults keep V3 modules


def test_default_options_keep_the_existing_recipe_digest_and_v41_options_change_it():
    from controllers.generic.tidewater_v4_core.config import TIDE_GATE_V1
    base = tidewater(capital_stage='tidewater_bnb_uncapped', symbols=ONLY_BNB)
    # The digest of a configuration without V4.1 options is computed exactly as before (no new keys).
    assert 'v41' not in base._digest_payload()
    assert base.tide_gate == TIDE_GATE_V1
    digests = {v41().recipe_digest(), v41(flash_pause='native').recipe_digest(),
               v41(exit_trailing='adaptive').recipe_digest(), v41(spacing=None).recipe_digest(),
               base.recipe_digest()}
    assert len(digests) == 5


def test_the_seal_binds_every_v41_option():
    sealed = v41(execution_enabled=True)
    assert sealed.recipe_binding.recipe_sha256 == sealed.recipe_digest()
    TidewaterConfig(**sealed.model_dump(mode='json'))  # the sealed configuration reloads
    for change in ({'flash_pause': 'native'}, {'exit_trailing': 'adaptive'}, {'spacing': None}):
        with pytest.raises(ValueError):
            TidewaterConfig(**{**sealed.model_dump(mode='json'), **change})


@pytest.mark.parametrize('change', [{'flash_pause': 'short'}, {'exit_trailing': 'atr'}])
def test_invalid_v41_options_are_rejected(change):
    with pytest.raises(ValueError):
        v41(**change)


# ---------------------------------------------------------------------------- native paper session
V41 = dict(tide_gate=None, spacing=SPACING_V41, flash_pause='off', exit_trailing='fixed_v41')


class V41PaperTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections
    enter = runtime_tests.TidewaterPaperTests.enter
    fill_all = runtime_tests.TidewaterPaperTests.fill_all
    receipts = runtime_tests.TidewaterPaperTests.receipts

    def session(self, directory):
        return NativePaperSession(paper(PAIRS, '1000'), [tidewater(execution_enabled=True, **V41)],
                                  Path(directory) / 'tidewater.sqlite')

    async def test_spacing_reads_the_native_frame_and_the_first_tranches_are_admitted(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory)
            try:
                receipt = await self.enter(session)
                controller = session.controllers[OWNER]
                self.assertEqual(len(receipt['actions']), 5, controller.to_format_status())
                _, rebound = frames()
                expected = hourly_atr_fraction(rebound.timestamp, rebound.high, rebound.low, rebound.close,
                                               cutoff_open=600000)
                admitted = [r for r in self.receipts(session) if r['kind'] == ADMITTED]
                self.assertEqual({r['pair'] for r in admitted}, set(PAIRS))
                for r in admitted:
                    self.assertEqual(r['pacing']['outcome'], 'paced_ok')
                    self.assertEqual(D(r['pacing']['spacing']['atr_1h']), expected)
                    self.assertIsNone(r['pacing']['spacing']['nearest'])  # first tranche: no zone yet
                for executor in session.executor_orchestrator.active_executors[OWNER]:
                    stop = executor.config.triple_barrier_config.trailing_stop
                    self.assertEqual((stop.activation_price, stop.trailing_delta), (D('.012'), D('.005')))
            finally:
                await session.close()

    async def test_spacing_refuses_a_shallow_add_that_v3_admits_and_admits_a_deeper_book(self):
        # The same 0.6% second rebound is admitted without spacing (test_without_the_gate_the_shallow_add_is_admitted).
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory)
            try:
                await self.enter(session)
                await self.fill_all(session, 10, 600061)
                controller = session.controllers[OWNER]
                before, rebound, cut, low = second_rebound(D('.006'))
                gap = 2 * hourly_atr_fraction(rebound.timestamp, rebound.high, rebound.low, rebound.close,
                                              cutoff_open=cut)
                price = D(str(round(low, 2)))
                await session.step(cut, books=books(20, cut, price), candles=all_candles(before, cut))
                shallow = await session.step(cut + 60, books=books(21, cut + 60, price),
                                             candles=all_candles(rebound, cut + 60))
                self.assertEqual(shallow['actions'], [], controller.to_format_status())
                refused = [r for r in self.receipts(session) if r['kind'] == REFUSED]
                self.assertEqual({r['code'] for r in refused}, {'spacing_gap'}, refused)
                for r in refused:
                    self.assertEqual(D(r['spacing']['gap']), gap)
                    self.assertLess(D(r['spacing']['distance']), gap)
                deeper = await session.step(cut + 61, books=books(22, cut + 61, price * D('.985')))
                self.assertTrue(deeper['actions'], controller.to_format_status())
            finally:
                await session.close()
