"""Tidewater R20 (`recovery_min_gap`): a smaller minimum buy gap while the recovery regime holds.

The reference is quants-lab `core/backtesting/spot_modular_minute.py` `regime_buy_gap` (V4 iteration 3, R):
the native `live_buy_gap` on the recipe BuyGapConfig with only `min_gap` replaced while the held bag is empty or
dust at the bid or the bid is at or above its breakeven; one gap for the last-buy gap and the bag gap. The
native sessions use the scripted lifecycle harness (native paper engine, scripted closed-candle features).
"""
from decimal import Decimal as D
from test.meridian_v3.lifecycle_harness import creates, rebound
from test.tidewater_v4 import test_runner_lifecycle as lifecycle
from test.tidewater_v4.test_tidewater_candidates import CONFIGS, OWNER_0E4811F71
from test.tidewater_v4.test_tidewater_runtime import tidewater
from types import SimpleNamespace as NS
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import pytest

from controllers.generic import meridian_v3_recipe
from controllers.generic.modular_spot_core.bag import BuyGapConfig, live_buy_gap
from controllers.generic.tidewater_v4_core.config import R20_RECOVERY_MIN_GAP, RUNNER_ITER5_B, TidewaterConfig
from controllers.generic.tidewater_v4_core.receipts import ADMITTED, read_receipts
from controllers.generic.tidewater_v4_core.recovery_gap import RecoveryGapView, recovery_buy_gap, recovery_regime
from controllers.generic.tidewater_v4_core.runtime import TidewaterSymbolRuntime

BTC, OWNER = lifecycle.BTC, lifecycle.OWNER
RECIPE_GAP = meridian_v3_recipe.strategy_parameters()['buy_gap']
RULES = dict(amount_step=D('.001'), minimum_amount=D('.001'), minimum_notional=D('1'))


def research_gap(atr, recovery):
    """quants-lab regime_buy_gap with recovery_min_gap: the native live_buy_gap on the recipe fields, only min_gap
    replaced (to 0.0020) while the regime holds."""
    fields = RECIPE_GAP.model_dump()
    if recovery:
        fields['min_gap'] = D('0.0020')
    return live_buy_gap(BuyGapConfig(**fields), atr)


# ---------------------------------------------------------------------- configuration and identity
def test_r20_is_the_frozen_recovery_floor_and_replaces_only_min_gap():
    assert R20_RECOVERY_MIN_GAP == D('0.0020')
    gap = recovery_buy_gap(RECIPE_GAP, R20_RECOVERY_MIN_GAP)
    assert gap.model_dump() == {**RECIPE_GAP.model_dump(), 'min_gap': D('0.0020')}


@pytest.mark.parametrize('name', sorted(CONFIGS))
def test_default_configurations_keep_their_digests_and_read_no_regime(name):
    overrides = dict(CONFIGS[name])
    config = tidewater(**overrides)
    assert config.recovery_min_gap is None and 'recovery_min_gap' not in config.sealed_payload()
    assert (config.recipe_digest(), config.seal().configuration_sha256) == OWNER_0E4811F71[name]


def test_the_recovery_gap_is_sealed_only_when_set():
    base, r20 = tidewater(), tidewater(recovery_min_gap=R20_RECOVERY_MIN_GAP)
    assert r20.sealed_payload()['recovery_min_gap'] == '0.0020'
    assert r20.recipe_digest() != base.recipe_digest()
    assert r20.seal().configuration_sha256 != base.seal().configuration_sha256
    assert tidewater(recovery_min_gap='0.003').recipe_digest() != r20.recipe_digest()
    both = tidewater(recovery_min_gap=R20_RECOVERY_MIN_GAP, exit_runner=RUNNER_ITER5_B)
    assert {both.recipe_digest()}.isdisjoint({r20.recipe_digest(), tidewater(exit_runner=RUNNER_ITER5_B).recipe_digest()})
    sealed = tidewater(execution_enabled=True, recovery_min_gap=R20_RECOVERY_MIN_GAP)
    for change in ({'recovery_min_gap': None}, {'recovery_min_gap': '0.003'}):
        with pytest.raises(ValueError):
            TidewaterConfig(**{**sealed.model_dump(mode='json'), **change})
    unsealed = tidewater(execution_enabled=True)
    with pytest.raises(ValueError):
        TidewaterConfig(**{**unsealed.model_dump(mode='json'), 'recovery_min_gap': '0.0020'})


def test_the_recovery_gap_is_a_research_option():
    with pytest.raises(ValueError, match='research option'):
        tidewater(mode='live', recovery_min_gap=R20_RECOVERY_MIN_GAP)
    assert tidewater(mode='observation', recovery_min_gap=R20_RECOVERY_MIN_GAP).recovery_min_gap == D('0.0020')


@pytest.mark.parametrize('bad', ['-0.001', '1', '0.031'])  # 0.031 exceeds the recipe's max_gap of 0.03
def test_invalid_recovery_gaps_are_rejected(bad):
    with pytest.raises(ValueError):
        tidewater(recovery_min_gap=bad)


# ---------------------------------------------------------------------- regime and effective gap
def bag(amount, breakeven='100'):
    return NS(amount=D(amount), breakeven_price=D(breakeven))


@pytest.mark.parametrize('held, bid, expected', [
    (bag('0', '0'), '99', (True, 'bag_empty')),
    (bag('0.0005'), '99', (True, 'bag_empty')),  # below the lot
    (bag('0.009'), '99', (True, 'bag_empty')),  # 0.891 quote: below the minimum notional at the bid
    (bag('0.0102'), '98', (True, 'bag_empty')),  # 0.9996 < 1 at this bid ...
    (bag('0.0102'), '98.04', (False, 'below_breakeven')),  # ... 1.000008 >= 1 at this one: a bag below breakeven
    (bag('0.5'), '100', (True, 'at_or_above_breakeven')),  # exactly at the breakeven
    (bag('0.5'), '99.99', (False, 'below_breakeven')),
    (bag('0.5'), '100.01', (True, 'at_or_above_breakeven')),
])
def test_the_regime_boundaries_at_dust_and_breakeven(held, bid, expected):
    assert recovery_regime(held, bid=D(bid), **RULES) == expected


@pytest.mark.parametrize('atr', [None, D('0'), D('0.0004'), D('0.0008'), D('0.0012'), D('0.002'), D('0.004'),
                                 D('0.02')])
@pytest.mark.parametrize('recovery', [True, False])
def test_the_effective_gap_is_native_live_buy_gap_on_the_modified_config(atr, recovery):
    source = NS(buy_gap=RECIPE_GAP, fingerprint=lambda: 'recipe-fingerprint', connector_name='okx_paper_trade')
    seen = RecoveryGapView(source, recovery_buy_gap(RECIPE_GAP, R20_RECOVERY_MIN_GAP)) if recovery else source
    assert live_buy_gap(seen.buy_gap, atr) == research_gap(atr, recovery)
    assert seen.fingerprint() == 'recipe-fingerprint' and seen.connector_name == 'okx_paper_trade'


# ---------------------------------------------------------------------- native paper sessions
class RecoveryGapSessions(IsolatedAsyncioTestCase):
    # The lifecycle harness's setup and entry helper (its tests are not inherited).
    setUp, asyncSetUp, asyncTearDown = (lifecycle.RunnerLifecycle.setUp, lifecycle.RunnerLifecycle.asyncSetUp,
                                        lifecycle.RunnerLifecycle.asyncTearDown)
    scenario, entered = lifecycle.RunnerLifecycle.scenario, lifecycle.RunnerLifecycle.entered
    ATR = D('.001')  # recipe gap: clamp(2.5 x 0.1%, 0.5%, 3%) = 0.5%; R20: clamp(0.25%, 0.2%, 3%) = 0.25%

    def session(self, **options):
        s = self.scenario(runner=options.pop('runner', None), seed_days=options.pop('seed_days', 0), **options)
        s.atr = self.ATR
        return s

    async def add(self, s, below):
        """A new RSI rebound with the bid `below` the only tranche's entry price."""
        [executor] = s.executors(BTC)
        entry = executor.config.entry_price
        mid = entry * (1 - D(below)) + D('0.01')  # the scripted bid is the mid less one tick
        receipt = await s.step(mids={BTC: str(mid)}, rsi={BTC: rebound()})
        return entry, creates(receipt, BTC)

    async def test_without_r20_the_recipe_gap_refuses_a_three_tenths_percent_add(self):
        self.assertEqual(research_gap(self.ATR, False), D('0.005'))
        s = self.session()
        await self.entered(s)
        _, created = await self.add(s, '0.003')
        self.assertEqual(created, [])
        self.assertEqual(s.controller.portfolio.status['candidate_holds'], {BTC: 'acquisition_not_eligible'})
        self.assertNotIn('recovery_min_gap', s.controller.get_custom_info()['tidewater'])

    async def test_r20_admits_the_three_tenths_percent_add_in_the_recovery_regime(self):
        gap = research_gap(self.ATR, True)
        self.assertEqual(gap, D('0.0025'))
        s = self.session(recovery_min_gap=R20_RECOVERY_MIN_GAP)
        await self.entered(s)
        _, created = await self.add(s, '0.003')
        self.assertEqual(len(created), 1, s.controller.to_format_status())
        regime = s.controller.get_custom_info()['tidewater']['recovery_min_gap']['regime'][BTC]
        self.assertEqual((regime['regime'], regime['reason']), (True, 'bag_empty'))
        [receipt] = [r for r in read_receipts(s.session.recorder, OWNER)
                     if r['kind'] == ADMITTED and r['pair'] == BTC and r['pacing'].get('reference') is not None]
        self.assertEqual(receipt['code'], 'admitted')
        self.assertEqual(D(receipt['pacing']['gap']), gap)  # the tide gate reads the gap the planner applies
        self.assertIsNone(s.controller.contexts[BTC].view)  # presented for its tick only

    async def test_r20_still_refuses_an_add_inside_its_own_gap(self):
        s = self.session(recovery_min_gap=R20_RECOVERY_MIN_GAP)
        await self.entered(s)
        _, created = await self.add(s, '0.0015')
        self.assertEqual(created, [])

    async def test_without_the_option_no_regime_is_read_and_the_context_is_the_recipes(self):
        s = self.session()
        with patch.object(TidewaterSymbolRuntime, 'recovery_view', side_effect=AssertionError('read')):
            await self.entered(s)
        context = s.controller.contexts[BTC]
        self.assertIs(context.config, s.controller.symbol_config(BTC))
        self.assertIsNone(context.runtime.recovery)

    async def held_bag(self, s):
        """Enter, then let Meridian's bag line demote the tranche (3.5% under its entry) to a held bag."""
        executor = await self.entered(s)
        for mid in ('98', '97', '96.5'):
            await s.step(mids={BTC: mid})
        [held] = [p for p in s.controller.positions_held if p.trading_pair == BTC]
        self.assertEqual((held.amount, held.breakeven_price), (executor.amount_to_close, executor.config.entry_price))
        await s.step()
        return held.breakeven_price

    async def signal_at(self, s, bid):
        receipt = await s.step(mids={BTC: str(bid + D('0.01'))}, rsi={BTC: rebound()})
        return creates(receipt, BTC)

    async def test_the_bag_gap_uses_the_r20_gap_at_or_above_breakeven_and_the_recipe_gap_below(self):
        s = self.session(recovery_min_gap=R20_RECOVERY_MIN_GAP)
        breakeven = await self.held_bag(s)
        below = (breakeven * D('0.997')).quantize(D('.01'))  # 0.3% under the breakeven: not the recovery regime
        self.assertEqual(await self.signal_at(s, below), [])
        self.assertEqual(s.controller.contexts[BTC].runtime.recovery['reason'], 'below_breakeven')
        self.assertEqual(s.controller.portfolio.status['candidate_holds'], {BTC: 'bag_gap_not_met'})
        for _ in range(3):
            await s.step()
        above = (breakeven * D('1.003')).quantize(D('.01'))  # 0.3% over it: the regime, the bag gap is 0.25%
        self.assertEqual(len(await self.signal_at(s, above)), 1, s.controller.to_format_status())
        self.assertEqual(s.controller.contexts[BTC].runtime.recovery['reason'], 'at_or_above_breakeven')

    async def test_without_r20_the_bag_gap_refuses_three_tenths_percent_above_breakeven(self):
        s = self.session()
        breakeven = await self.held_bag(s)
        self.assertEqual(await self.signal_at(s, (breakeven * D('1.003')).quantize(D('.01'))), [])
        self.assertEqual(s.controller.portfolio.status['candidate_holds'], {BTC: 'bag_gap_not_met'})

    async def test_r20_with_the_protected_runner_admits_the_add_and_both_tranches_keep_runners(self):
        s = self.session(recovery_min_gap=R20_RECOVERY_MIN_GAP, runner=RUNNER_ITER5_B, seed_days=14)
        await self.entered(s)
        _, created = await self.add(s, '0.003')
        self.assertEqual(len(created), 1)
        await s.fill(BTC, '1')
        self.assertEqual(len(s.executors(BTC)), 2)
        for mid in ('104.5', '105.5', '104.9'):  # both trailing stops arm and fire: each harvest keeps a runner
            await s.step(mids={BTC: mid})
        await s.step(minutes=0, seconds=1)
        lots = s.controller.runners.lots
        self.assertEqual({e.config.id for e in s.executors(BTC)}, set(lots))
        for executor in s.executors(BTC):
            self.assertEqual((executor.amount_to_close, lots[executor.config.id]['phase']),
                             (lots[executor.config.id]['runner_base'], 'open'))
