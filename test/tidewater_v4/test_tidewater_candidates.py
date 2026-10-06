"""Tidewater research candidates E0 (first-entry volatility veto), X0 (its exposure-control placebo
veto) and CP (capitulation override).

Synthetic books and candles on the native paper session (the Meridian V3 harness), not economics.
The frozen parameters come from the root evidence `docs/evidence/2026-10-06-tidewater-candidates/`.
"""
import dataclasses
import hashlib
import math
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.test_native_paper import frames, paper
from test.tidewater_v4.test_tidewater_runtime import OWNER, PAIRS, all_candles, books, second_rebound, tidewater
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from controllers.generic.modular_spot_core.domain import DataUnavailable
from controllers.generic.tidewater_v4_core import entry_veto
from controllers.generic.tidewater_v4_core.capitulation import CP_FROZEN, CapitulationOverride
from controllers.generic.tidewater_v4_core.config import SPACING_V41, TidewaterConfig
from controllers.generic.tidewater_v4_core.entry_veto import E0_FROZEN, X0_FROZEN, X0Model, decision_window, e0_features
from controllers.generic.tidewater_v4_core.portfolio import TidewaterPortfolio
from controllers.generic.tidewater_v4_core.receipts import (
    ADMITTED,
    CAPITULATION,
    CENSUS,
    OUTCOME,
    REFUSED,
    SHADOW_TIDE_GATE,
    read_receipts,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.paper_session import NativePaperSession

# recipe_digest() and seal().configuration_sha256 at the owner's 0e4811f71 (V4.1 options, merged into
# this branch), computed from a git archive of 0e4811f71 alone. The recipe digests equal 693fa9fda's
# (the V4.1 digest block is absent at the defaults); the configuration digests moved because the
# sealed payload now carries the V4.1 fields at their defaults. seal().source_sha256 binds the
# Tidewater source files and changes with any source edit by design.
OWNER_0E4811F71 = {
    'paper_default': ('3d4f9624dce28896e7b0d042136b6e3c0da9e5c134ebf0b4c5a497b17fae7c2b',  # noqa: documentation
                      'a8ebec895f94f4451a20e325be65acc32f099e86874f6fb1ab75f74db2debeca'),  # noqa: documentation
    'paper_bnb_1000': ('effbf284895ee040b9b06979f033031f4a5dd45893c8b0efe2082cbbff117afc',  # noqa: documentation
                       'a3d7e227e5b7287fe1afc702674e8531dbaf9d02c11a00e00ae02b5eb1588317'),  # noqa: documentation
    'paper_bnb_uncapped': ('60354719928944ed7f84d1b3bc0401af28862a1e7a97affe8dc634b5d1021d9e',  # noqa: documentation
                           '5125d6a3d097b9c6da47f3c6890f33426e7874635704945518fa512811f08b2e'),  # noqa: documentation
    'observation_default': ('91de9ebd6de85d008a6fa73d643a76384f1b3c150d6028abc28efc26b6a8d33e',  # noqa: documentation
                            '542af6d757a4e5675bd76413ceec2aac52b0e3c967aa9edd291a1d02db9974c1'),  # noqa: documentation
    'paper_no_gate_stable': ('21f3f68f6d6a4a2f0b998a8ef666dd4b8c57250ffd10a1d6ba6e5536fb03b371',  # noqa: documentation
                             'fcd314922dc96250b24ab6b9e43743a6d7bcb401667a5152b8c0de8417e353ab'),  # noqa: documentation
}
CONFIGS = {
    'paper_default': {},
    'paper_bnb_1000': dict(capital_stage='tidewater_bnb_1000',
                           symbols=[{'trading_pair': p, 'entry_enabled': p == 'BNB-USDC'} for p in PAIRS]),
    'paper_bnb_uncapped': dict(capital_stage='tidewater_bnb_uncapped',
                               symbols=[{'trading_pair': p, 'entry_enabled': p == 'BNB-USDC'} for p in PAIRS]),
    'observation_default': dict(mode='observation'),
    'paper_no_gate_stable': dict(tide_gate=None, resting_entry='stable'),
}
ALLOW_ALL = E0_FROZEN.model_copy(update={'threshold': D('0.999999999')})
VETO_ALL = E0_FROZEN.model_copy(update={'threshold': D('0.000000001')})
X0_NONE = X0_FROZEN.model_copy(update={'rate': D('0')})
X0_ALL = X0_FROZEN.model_copy(update={'rate': D('1')})
# The frozen X0 verdicts at the harness cut 600000 (seed 0, rate 0.2002), from the formula below.
X0_VETOED_AT_600000 = {'ETH-USDC', 'BNB-USDC', 'XRP-USDC'}


def x0_formula(seed, pair, cut, rate):
    """The coordinator's specification, written out independently of the controller."""
    return int(hashlib.sha256(f'x0:{seed}:{pair}:{cut}'.encode()).hexdigest()[:16], 16) / 2 ** 64 < rate


def reference_e0(closes):
    """Independent pandas definition (the offline freeze script's reference implementation)."""
    r2 = pd.Series(np.log(np.asarray(closes, dtype=float))).diff() ** 2
    return math.log(r2.ewm(halflife=60, adjust=False).mean().iloc[-1]), math.log(r2.iloc[-1440:].sum())


def crash_frames(drop_to=0.935, crash=0.02):
    """Flat, a 10-minute `crash` (native flash pause), a slow decline to `drop_to`, then an RSI(17) cross of 29."""
    flat = 100 + .05 * np.sin(np.arange(1330) / 10)
    sharp = np.linspace(100, 100 * (1 - crash), 11)[1:]
    decline = np.linspace(100 * (1 - crash), 100 * drop_to, 160)
    close = np.r_[flat, sharp, decline]
    frame = pd.DataFrame(dict(timestamp=np.arange(510000, 600000, 60), open=close, high=close + .05,
                              low=close - .05, close=close, volume=np.ones(1500) * 10))
    last = float(close[-1])
    rebound = dict(timestamp=600000, open=last, high=last + .6, low=last - .05, close=last + .5, volume=10)
    return frame, pd.concat([frame, pd.DataFrame([rebound])], ignore_index=True), D(str(round(last + .5, 2)))


def crash_continuation(drop_to, crash=0.02):
    """After the first rebound (inventory filled): a 10-minute `crash`, a decline to `drop_to`, then a new RSI cross."""
    _, first = frames()
    start = float(first['close'].iloc[-1])
    close = np.r_[np.linspace(start, start * (1 - crash), 11)[1:], np.linspace(start * (1 - crash), 100 * drop_to, 90)]
    t0 = int(first['timestamp'].iloc[-1]) + 60
    extension = pd.DataFrame(dict(timestamp=np.arange(t0, t0 + 100 * 60, 60), open=close, high=close + .05,
                                  low=close - .05, close=close, volume=np.ones(100) * 10))
    before = pd.concat([first, extension], ignore_index=True)
    cut = t0 + 100 * 60
    last = float(close[-1])
    rebound = dict(timestamp=cut, open=last, high=last + .6, low=last - .05, close=last + .5, volume=10)
    return before, pd.concat([before, pd.DataFrame([rebound])], ignore_index=True), cut, D(str(round(last + .5, 2)))


# ---------------------------------------------------------------------- seal and identity
@pytest.mark.parametrize('name', sorted(CONFIGS))
def test_default_configurations_keep_their_owner_recipe_and_configuration_digests(name):
    c = tidewater(**CONFIGS[name])
    assert (c.entry_veto, c.capitulation_override) == (None, None)
    seal = c.seal()
    assert (c.recipe_digest(), seal.configuration_sha256) == OWNER_0E4811F71[name]
    assert seal.recipe_sha256 == OWNER_0E4811F71[name][0]
    assert c.census is False
    assert not {'entry_veto', 'capitulation_override', 'census'} & set(c.sealed_payload())


def test_candidates_enter_the_seal_and_the_recipe_digest():
    base = tidewater()
    census = tidewater(census=True)
    e0, x0 = tidewater(census=True, entry_veto='e0'), tidewater(census=True, entry_veto='x0')
    cp = tidewater(census=True, capitulation_override=CP_FROZEN)
    deeper = tidewater(census=True, capitulation_override=CapitulationOverride(depth='0.10'))
    digests = {c.recipe_digest() for c in (base, census, e0, x0, cp, deeper)}
    configurations = {c.seal().configuration_sha256 for c in (base, census, e0, x0, cp, deeper)}
    assert len(digests) == len(configurations) == 6
    sealed = tidewater(execution_enabled=True, census=True, entry_veto='e0', capitulation_override=CP_FROZEN)
    for change in ({'entry_veto': None}, {'entry_veto': 'x0'}, {'capitulation_override': None}, {'census': False},
                   {'capitulation_override': {'depth': '0.10'}}):
        with pytest.raises(ValueError):
            TidewaterConfig(**{**sealed.model_dump(mode='json'), **change})
    TidewaterConfig(**sealed.model_dump(mode='json'))  # the unchanged sealed configuration still validates


def test_the_recipe_digest_binds_the_frozen_e0_constants():
    before = tidewater(census=True, entry_veto='e0').recipe_digest()
    with patch.dict(entry_veto.ENTRY_VETOES, {'e0': E0_FROZEN.model_copy(update={'threshold': D('0.7')})}):
        assert tidewater(census=True, entry_veto='e0').recipe_digest() != before
    assert tidewater(census=True, entry_veto='e0').recipe_digest() == before


@pytest.mark.parametrize('candidate', [{'entry_veto': 'e0'}, {'entry_veto': 'x0'}, {'capitulation_override': CP_FROZEN}])
def test_candidates_and_the_census_seal_on_the_uncapped_stage(candidate):
    uncapped = CONFIGS['paper_bnb_uncapped']
    sealed = tidewater(execution_enabled=True, census=True, **uncapped, **candidate)
    assert sealed.capital_stage == 'tidewater_bnb_uncapped' and sealed.pacing_reference_quote() == 1000
    assert sealed.seal().recipe_sha256 == sealed.recipe_digest() != OWNER_0E4811F71['paper_bnb_uncapped'][0]
    TidewaterConfig(**sealed.model_dump(mode='json'))  # the sealed configuration validates as written
    with pytest.raises(ValueError):  # and its candidate and census are bound by the seal
        TidewaterConfig(**{**sealed.model_dump(mode='json'), 'census': False})
    census_only = tidewater(execution_enabled=True, census=True, **uncapped)
    assert census_only.recipe_digest() not in {sealed.recipe_digest(), OWNER_0E4811F71['paper_bnb_uncapped'][0]}


# The owner's V4.1 options (0e4811f71) on the uncapped BNB stage, as its own test_v41 arms.
V41 = dict(spacing=SPACING_V41, flash_pause='off', exit_trailing='fixed_v41')


def test_v41_options_and_candidates_are_bound_together_by_the_seal():
    uncapped = CONFIGS['paper_bnb_uncapped']
    both = tidewater(execution_enabled=True, census=True, entry_veto='e0', **uncapped, **V41)
    payload = both._digest_payload()
    assert payload['v41'] == both.v41_options() and payload['entry_veto']['id'] == 'e0' and payload['census'] is True
    v41_only = tidewater(**uncapped, **V41)
    candidate_only = tidewater(census=True, entry_veto='e0', **uncapped)
    assert len({both.recipe_digest(), v41_only.recipe_digest(), candidate_only.recipe_digest()}) == 3
    assert both.seal().recipe_sha256 == both.recipe_digest()
    TidewaterConfig(**both.model_dump(mode='json'))  # the sealed configuration reloads
    for change in ({'spacing': None}, {'flash_pause': 'native'}, {'exit_trailing': 'adaptive'},
                   {'entry_veto': None}, {'census': False}):
        with pytest.raises(ValueError):
            TidewaterConfig(**{**both.model_dump(mode='json'), **change})


@pytest.mark.parametrize('change', [{'entry_veto': 'e0'}, {'entry_veto': 'x0'}, {'capitulation_override': CP_FROZEN}])
def test_every_candidate_requires_the_census(change):
    with pytest.raises(ValueError):
        tidewater(**change)
    assert tidewater(census=True, **change).census is True


@pytest.mark.parametrize('change', [{'rate': D('0.2003')}, {'seed': 1}])
def test_the_recipe_digest_binds_the_frozen_x0_rate_and_seed(change):
    before = tidewater(census=True, entry_veto='x0').recipe_digest()
    with patch.dict(entry_veto.ENTRY_VETOES, {'x0': X0_FROZEN.model_copy(update=change)}):
        assert tidewater(census=True, entry_veto='x0').recipe_digest() != before
    assert tidewater(census=True, entry_veto='x0').recipe_digest() == before


def test_x0_frozen_values_and_deterministic_verdicts():
    assert (str(X0_FROZEN.rate), X0_FROZEN.seed) == ('0.2002', 0)
    again = X0Model.model_validate(X0_FROZEN.model_dump(mode='json'))  # a new instance, as after a restart
    for k in range(500):
        for pair in PAIRS:
            cut = 600000 + 60 * k
            verdict = X0_FROZEN.vetoes(pair, cut)
            assert verdict == again.vetoes(pair, cut) == x0_formula(0, pair, cut, 0.2002)
    assert {p for p in PAIRS if X0_FROZEN.vetoes(p, 600000)} == X0_VETOED_AT_600000


@pytest.mark.parametrize('pair', ['BNB-USDC', 'BTC-USDC'])
def test_x0_empirical_rate_over_ten_thousand_cuts(pair):
    rate = sum(X0_FROZEN.vetoes(pair, 600000 + 60 * k) for k in range(10000)) / 10000
    assert abs(rate - 0.2002) <= 0.01


def test_x0_reads_no_candles():
    class Unreadable:
        def __getattr__(self, name):
            raise AssertionError(f'X0 read runtime.{name}')
    market = type('Market', (), {'candle_open': 600000})()
    verdict = entry_veto.evaluate(X0_FROZEN, Unreadable(), market, pair='BNB-USDC')
    assert verdict['vetoed'] is True and verdict['model'] == 'x0_hash_placebo_v1'


@pytest.mark.parametrize('change', [{'entry_veto': 'e1'}, {'entry_veto': 'x1'}, {'capitulation_override': {'depth': '0'}},
                                    {'capitulation_override': {'depth': '1'}},
                                    {'capitulation_override': {'depth': '0.05', 'lookback_minutes': 720}}])
def test_invalid_candidate_configurations_are_rejected(change):
    with pytest.raises(ValueError):
        tidewater(**change)


def test_frozen_values_are_the_evidence_values():
    assert (str(E0_FROZEN.intercept), str(E0_FROZEN.coef_x1), str(E0_FROZEN.coef_x2), str(E0_FROZEN.threshold)) == (
        '0.3090052320373231', '-0.09801053710977638', '0.7088562914237231', '0.681859486807908')
    assert str(CP_FROZEN.depth) == '0.05618828176431856' and CP_FROZEN.lookback_minutes == 1440
    # At the fit means the model returns sigmoid(intercept).
    p = E0_FROZEN.probability(float(E0_FROZEN.mean_x1), float(E0_FROZEN.mean_x2))
    assert abs(p - 1 / (1 + math.exp(-float(E0_FROZEN.intercept)))) < 1e-15


# ---------------------------------------------------------------------- a dust held bag (Astra P5)
def held_bag_portfolio(amount, runtime_bid='100'):
    """BNB with no tranche or claim and one held position; venue rules: step 0.001, minimum 0.01, notional 5."""
    config = tidewater(census=True, capitulation_override=CP_FROZEN, **CONFIGS['paper_bnb_uncapped'])
    position = SimpleNamespace(amount=D(amount), side='BUY', connector_name='okx', trading_pair='BNB-USDC',
                               breakeven_price=D('100'), remaining_fees_quote=D('0'))
    store = SimpleNamespace(load=lambda: (None, None))
    runtime = SimpleNamespace(ensure_store=lambda: store, inventory=lambda: {}, pending={},
                              snapshot=lambda: {'bid': D(runtime_bid), 'amount_step': D('0.001'),
                                                'minimum_amount': D('0.01'), 'minimum_notional': D('5')})
    contexts = {'BNB-USDC': SimpleNamespace(runtime=runtime, positions_held=[position])}
    owner = SimpleNamespace(config=config, contexts=contexts, symbol_config=lambda pair: SimpleNamespace(connector_name='okx'),
                            market_data_provider=SimpleNamespace(time=lambda: 600060.0))
    p = TidewaterPortfolio.__new__(TidewaterPortfolio)
    p.owner = owner
    return p


def test_a_dust_held_bag_is_no_inventory_for_the_first_entry_predicate():
    # Consistent with the native planner's bag_empty: venue dust is no bag, so the pair is a first entry.
    state, evidence, reference = held_bag_portfolio('0.001').entry_state('BNB-USDC')  # 0.001 < minimum 0.01
    assert (state, reference, evidence['reference']) == ('first', None, None)
    assert held_bag_portfolio('1').entry_state('BNB-USDC')[:3:2] == ('holds', D('100'))


def test_dust_is_judged_at_the_bid_the_caller_paces_on_else_the_runtime_book():
    assert held_bag_portfolio('0.04', runtime_bid='100').entry_state('BNB-USDC')[0] == 'first'  # 4 < 5 quote
    assert held_bag_portfolio('0.04', runtime_bid='200').entry_state('BNB-USDC')[0] == 'holds'  # 8 quote
    assert held_bag_portfolio('0.04', runtime_bid='100').entry_state('BNB-USDC', bid=D('200'))[0] == 'holds'


def test_cp_treats_a_dust_bag_as_a_first_entry_and_keeps_the_pause():
    market = SimpleNamespace(flash_pause_until=600999.0, candle_open=600000, close=D('90'),
                             bag=SimpleNamespace(recent_high=D('100')))
    dust = held_bag_portfolio('0.001').capitulation('BNB-USDC', market, CP_FROZEN)
    assert (dust['applied'], dust['reason'], dust['entry_state']) == (False, 'first_entry_keeps_pause', 'first')
    real = held_bag_portfolio('1').capitulation('BNB-USDC', market, CP_FROZEN)
    assert (real['applied'], real['reason'], real['entry_state']) == (True, 'capitulation', 'holds')


# ---------------------------------------------------------------------- E0 features (pure)
@pytest.mark.parametrize('seed', range(6))
def test_controller_e0_features_reproduce_the_offline_definition(seed):
    rng = np.random.default_rng(seed)
    steps = rng.standard_t(3, 1500) * 10 ** rng.uniform(-4, -2.5)
    closes = 600 * np.exp(np.cumsum(steps))
    got, want = e0_features(closes), reference_e0(closes)
    assert max(abs(a - b) for a, b in zip(got, want)) <= 1e-12


def test_e0_features_need_exactly_the_window_of_valid_closes():
    with pytest.raises(DataUnavailable):
        e0_features(np.ones(1499) * 100)
    with pytest.raises(DataUnavailable):
        e0_features(np.ones(1500) * 100)  # zero realized variance: no fabricated value
    bad = np.linspace(100, 101, 1500)
    bad[3] = 0
    with pytest.raises(Exception):
        e0_features(bad)


def test_e0_reads_only_candles_closed_by_the_decision_candle():
    frame, rebound = frames()
    extended = pd.concat([rebound, pd.DataFrame([dict(timestamp=600060 + 60 * k, open=90.0, high=90.1, low=89.9,
                                                      close=90.0, volume=10) for k in range(5)])], ignore_index=True)
    closes, digests = decision_window(extended, candle_open=600000, interval=60, now=600060 + 400)
    assert len(closes) == 1500 and max(digests) == 600000
    changed = extended.copy()
    changed.loc[changed.timestamp > 600000, 'close'] = 50.0  # later candles cannot move the decision
    assert np.array_equal(decision_window(changed, candle_open=600000, interval=60, now=600460)[0], closes)
    earlier = extended.copy()
    earlier.loc[earlier.timestamp == 599940, 'close'] = 99.0  # an input candle does
    assert not np.array_equal(decision_window(earlier, candle_open=600000, interval=60, now=600460)[0], closes)
    with pytest.raises(DataUnavailable):  # the decision candle has not closed yet
        decision_window(rebound, candle_open=600000, interval=60, now=600059)
    with pytest.raises(DataUnavailable):  # fewer than 1,500 closed candles
        decision_window(frame, candle_open=599940, interval=60, now=600000, size=1501)


def test_capitulation_comparison_is_exact_at_the_boundary():
    cp = CapitulationOverride(depth='0.05')
    assert cp.holds(close=D('95'), recent_high=D('100'))
    assert not cp.holds(close=D('95.0000001'), recent_high=D('100'))


# ---------------------------------------------------------------------- native paper session
class TidewaterCandidateTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    def session(self, directory, cash='1000', **overrides):
        overrides.setdefault('census', True)  # every campaign arm records the signal census
        return NativePaperSession(paper(PAIRS, cash), [tidewater(execution_enabled=True, **overrides)],
                                  Path(directory) / 'tidewater.sqlite')

    async def enter(self, session, pair_frames=None, price=None):
        before, rebound = frames() if pair_frames is None else pair_frames
        first = await session.step(600000, books=books(0, 600000, price), candles=all_candles(before, 600000))
        self.assertEqual(first['actions'], [])
        return await session.step(600060, books=books(1, 600060, price), candles=all_candles(rebound, 600060))

    def receipts(self, session, kind=None):
        return [r for r in read_receipts(session.recorder, OWNER) if kind is None or r['kind'] == kind]

    def outcomes(self, session, cut):
        """Each pair's outcome transitions for one signal cut, in their recorded order."""
        result = {}
        for r in sorted(self.receipts(session, OUTCOME), key=lambda r: r['seq']):
            if r['cut'] == cut:
                result.setdefault(r['pair'], []).append(r['code'])
        return result

    async def fill_all(self, session, now, step=10):
        trades = [dict(trading_pair=p, sequence=step + i, timestamp=now, side=TradeType.SELL, price=D('99.49'),
                       amount=D('10')) for i, p in enumerate(PAIRS)]
        await session.step(now, trades=trades)

    async def test_e0_census_precedes_a_veto_that_does_not_consume_the_cut(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, entry_veto='e0')
            try:
                controller = session.controllers[OWNER]
                with patch.dict(entry_veto.ENTRY_VETOES, {'e0': VETO_ALL}):
                    vetoed = await self.enter(session)
                self.assertEqual(vetoed['actions'], [], controller.to_format_status())
                census, refused = self.receipts(session, CENSUS), self.receipts(session, REFUSED)
                self.assertEqual({r['pair'] for r in census}, set(PAIRS))
                self.assertEqual({(r['pair'], r['code']) for r in refused}, {(p, 'entry_veto_e0') for p in PAIRS})
                for pair in PAIRS:  # the census is written before any treatment of its candidate
                    c = next(r for r in census if r['pair'] == pair)
                    self.assertLess(c['sequence'], next(r['sequence'] for r in refused if r['pair'] == pair))
                    self.assertEqual((c['entry_state'], c['native_flash_pause_until']), ('first', None))
                    self.assertEqual(c['evidence'], {'reference': None, 'unresolved_claims': 0, 'runtime_pending': 0,
                                                     'commitments_quote': None})
                self.assertEqual(len({r['census_id'] for r in census}), 5)
                # The decision read only the 1,500 closed candles ending at the cut.
                _, rebound = frames()
                x1, x2 = e0_features(rebound.close.to_numpy()[-1500:])
                self.assertEqual({(r['entry_veto']['x1'], r['entry_veto']['x2']) for r in refused}, {(repr(x1), repr(x2))})
                shadow = self.receipts(session, SHADOW_TIDE_GATE)
                self.assertEqual({(r['pair'], r['would_allow'], r['reason']) for r in shadow},
                                 {(p, True, 'tide_gate_first_tranche') for p in PAIRS})
                # Same cut, the veto now allows: the refusal did not consume the cut.
                with patch.dict(entry_veto.ENTRY_VETOES, {'e0': ALLOW_ALL}):
                    allowed = await session.step(600061, books=books(2, 600061))
                self.assertEqual({a.executor_config.trading_pair for a in allowed['actions']}, set(PAIRS))
                admitted = self.receipts(session, ADMITTED)
                self.assertEqual({(r['pair'], r['code']) for r in admitted}, {(p, 'admitted') for p in PAIRS})
                self.assertTrue(all(r['pacing']['entry_veto']['vetoed'] is False for r in admitted))
                # Each census record ends in exactly one outcome per (pair, cut): refused, then admitted here.
                for pair in PAIRS:
                    outcomes = [r['kind'] for r in self.receipts(session) if r['episode'] == f'{pair}:600000'
                                and r['kind'] in (REFUSED, ADMITTED)]
                    self.assertEqual(outcomes, [REFUSED, ADMITTED])
                # The census outcome trail: vetoed on the first tick, admitted on the re-evaluation.
                self.assertEqual(self.outcomes(session, 600000), {p: ['entry_veto_e0', 'admitted'] for p in PAIRS})
                self.assertTrue(all(r['cp_overridden'] is False for r in self.receipts(session, OUTCOME)))
                self.assertEqual(len(self.receipts(session, CENSUS)), 5)  # same candidates, same facts
                self.assertEqual(controller.get_custom_info()['tidewater']['variants']['entry_veto'], 'e0')
            finally:
                await session.close()

    async def test_a_pending_acquisition_is_not_a_first_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, entry_veto='e0')
            try:
                controller = session.controllers[OWNER]
                portfolio = controller.portfolio
                self.assertEqual({portfolio.entry_state(p)[0] for p in PAIRS}, {'first'})
                with patch.dict(entry_veto.ENTRY_VETOES, {'e0': ALLOW_ALL}):
                    receipt = await self.enter(session)
                self.assertEqual(len(receipt['actions']), 5)
                # Entries submitted, nothing filled: no inventory, but an acquisition is under way.
                snapshot = portfolio.snapshot(portfolio.now_ms())
                for pair in PAIRS:
                    state, evidence, reference = portfolio.entry_state(pair, snapshot)
                    self.assertEqual((state, reference), ('pending', None), evidence)
                    self.assertGreater(evidence['unresolved_claims'] + evidence['runtime_pending'], 0)
                # Pacing the same candidates again writes no census and runs no veto.
                portfolio.receipts._seen.clear()
                markets = {p: c.runtime.market for p, c in controller.contexts.items()}
                with patch.dict(entry_veto.ENTRY_VETOES, {'e0': VETO_ALL}):
                    paced = portfolio.pace(tuple(PAIRS), markets, snapshot, portfolio.now_ms())
                self.assertEqual(set(paced), set(PAIRS))
                self.assertEqual(len(self.receipts(session, CENSUS)), 5)
                self.assertEqual({portfolio.pacing[p]['entry_state'] for p in PAIRS}, {'pending'})
                self.assertFalse(any('entry_veto' in portfolio.pacing[p] for p in PAIRS))
            finally:
                await session.close()

    async def test_x0_refuses_by_hash_with_census_and_shadow_and_never_consumes_the_cut(self):
        outcomes = []
        for run in range(2):  # a second, fresh session (a restart) gives the same verdicts
            with tempfile.TemporaryDirectory() as directory:
                session = self.session(directory, entry_veto='x0')
                try:
                    controller = session.controllers[OWNER]
                    with patch.object(entry_veto, 'read_frame', side_effect=DataUnavailable('no candles')):
                        receipt = await self.enter(session)  # X0 never needs candle history
                    entered = {a.executor_config.trading_pair for a in receipt['actions']}
                    refused = {r['pair']: r for r in self.receipts(session, REFUSED)}
                    self.assertEqual(entered, set(PAIRS) - X0_VETOED_AT_600000, controller.to_format_status())
                    self.assertEqual({p: r['code'] for p, r in refused.items()},
                                     {p: 'entry_veto_x0' for p in X0_VETOED_AT_600000})
                    census = {r['pair']: r for r in self.receipts(session, CENSUS)}
                    self.assertEqual(set(census), set(PAIRS))
                    for pair, r in refused.items():
                        self.assertLess(census[pair]['sequence'], r['sequence'])
                        self.assertEqual(r['entry_veto']['model'], 'x0_hash_placebo_v1')
                    shadow = self.receipts(session, SHADOW_TIDE_GATE)
                    self.assertEqual({(r['pair'], r['refused_by'], r['would_allow']) for r in shadow},
                                     {(p, 'entry_veto_x0', True) for p in X0_VETOED_AT_600000})
                    self.assertEqual({p: codes[0] for p, codes in self.outcomes(session, 600000).items()},
                                     {p: 'entry_veto_x0' if p in X0_VETOED_AT_600000 else 'admitted' for p in PAIRS})
                    outcomes.append((entered, sorted(refused), sorted(r['census_id'] for r in census.values())))
                    if run == 0:  # same cut with the veto lifted: the refusal did not consume it
                        with patch.dict(entry_veto.ENTRY_VETOES, {'x0': X0_NONE}):
                            later = await session.step(600061, books=books(2, 600061))
                        self.assertEqual({a.executor_config.trading_pair for a in later['actions']},
                                         X0_VETOED_AT_600000)
                finally:
                    await session.close()
        self.assertEqual(outcomes[0], outcomes[1])

    async def test_x0_never_vetoes_a_pair_that_is_not_a_first_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, entry_veto='x0')
            try:
                controller = session.controllers[OWNER]
                portfolio = controller.portfolio
                with patch.dict(entry_veto.ENTRY_VETOES, {'x0': X0_NONE}):
                    receipt = await self.enter(session)
                self.assertEqual(len(receipt['actions']), 5)
                markets = {p: c.runtime.market for p, c in controller.contexts.items()}
                for expected in ('pending', 'holds'):
                    if expected == 'holds':
                        trades = [dict(trading_pair=p, sequence=10 + i, timestamp=600061, side=TradeType.SELL,
                                       price=D('99.49'), amount=D('10')) for i, p in enumerate(PAIRS)]
                        await session.step(600061, trades=trades)
                    portfolio.receipts._seen.clear()
                    snapshot = portfolio.snapshot(portfolio.now_ms())
                    with patch.dict(entry_veto.ENTRY_VETOES, {'x0': X0_ALL}):
                        portfolio.pace(tuple(PAIRS), markets, snapshot, portfolio.now_ms())
                    for pair in PAIRS:
                        pacing = portfolio.pacing[pair]
                        self.assertEqual(pacing['entry_state'], expected, pacing)
                        self.assertNotIn('entry_veto', pacing)
                        self.assertFalse(pacing['outcome'].startswith('entry_veto'), pacing)
                self.assertFalse([r for r in self.receipts(session, REFUSED) if r['code'].startswith('entry_veto')])
            finally:
                await session.close()

    async def test_without_cp_the_native_flash_pause_holds_a_capitulation_signal(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, census=False)
            try:
                before, rebound, price = crash_frames()
                receipt = await self.enter(session, (before, rebound), price)
                controller = session.controllers[OWNER]
                self.assertEqual(receipt['actions'], [])
                self.assertEqual(set(controller.portfolio.status['candidate_holds'].values()), {'flash_crash_pause'})
                self.assertIsNotNone(controller.contexts['BNB-USDC'].runtime.market.flash_pause_until)
                self.assertFalse(self.receipts(session, CENSUS))
            finally:
                await session.close()

    async def test_cp_never_lifts_the_pause_for_first_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, capitulation_override=CP_FROZEN)
            try:
                before, rebound, price = crash_frames()  # deep enough, but nothing is held yet
                receipt = await self.enter(session, (before, rebound), price)
                controller = session.controllers[OWNER]
                self.assertEqual(receipt['actions'], [], controller.to_format_status())
                self.assertEqual(set(controller.portfolio.status['candidate_holds'].values()), {'flash_crash_pause'})
                for pair in PAIRS:
                    runtime = controller.contexts[pair].runtime
                    self.assertEqual(runtime.capitulation['reason'], 'first_entry_keeps_pause')
                    self.assertGreaterEqual(D(runtime.capitulation['drawdown']), CP_FROZEN.depth)
                    self.assertIsNotNone(runtime.market.flash_pause_until)
                self.assertFalse(self.receipts(session, CAPITULATION))
                census = {r['pair']: r for r in self.receipts(session, CENSUS)}
                self.assertEqual({p: r['entry_state'] for p, r in census.items()}, {p: 'first' for p in PAIRS})
                self.assertTrue(all(r['native_flash_pause_until'] > 600060 for r in census.values()))
                self.assertEqual(self.outcomes(session, 600000), {p: ['native_paused'] for p in PAIRS})
            finally:
                await session.close()

    async def test_cp_lets_an_inventory_backed_add_through_the_pause_and_only_at_depth(self):
        for drop_to, deep in ((0.935, True), (0.96, False)):
            with self.subTest(drop_to=drop_to), tempfile.TemporaryDirectory() as directory:
                session = self.session(directory, capitulation_override=CP_FROZEN)
                try:
                    controller = session.controllers[OWNER]
                    self.assertEqual(len((await self.enter(session))['actions']), 5)
                    await self.fill_all(session, 600061)
                    self.assertEqual({controller.portfolio.entry_state(p)[0] for p in PAIRS}, {'holds'})
                    before, rebound, cut, price = crash_continuation(drop_to)
                    await session.step(cut, books=books(20, cut, price), candles=all_candles(before, cut))
                    receipt = await session.step(cut + 60, books=books(21, cut + 60, price),
                                                 candles=all_candles(rebound, cut + 60))
                    census = {r['pair']: r for r in self.receipts(session, CENSUS) if r['cut'] == cut}
                    self.assertEqual({p: r['entry_state'] for p, r in census.items()}, {p: 'holds' for p in PAIRS})
                    self.assertTrue(all(r['native_flash_pause_until'] > cut + 60 for r in census.values()))
                    outcomes = self.outcomes(session, cut)
                    if deep:
                        added = {a.executor_config.trading_pair for a in receipt['actions']}
                        self.assertEqual(added, set(PAIRS), controller.to_format_status())
                        self.assertTrue(all(a.executor_config.side.name == 'BUY' for a in receipt['actions']))
                        lifted = [r for r in self.receipts(session, CAPITULATION) if r['cut'] == cut]
                        self.assertEqual({(r['pair'], r['entry_state']) for r in lifted}, {(p, 'holds') for p in PAIRS})
                        self.assertTrue(all(r['shadow_native_flash_pause']['would_refuse'] for r in lifted))
                        self.assertEqual(outcomes, {p: ['admitted'] for p in PAIRS})
                        overridden = [r for r in self.receipts(session, OUTCOME) if r['cut'] == cut]
                        self.assertTrue(all(r['cp_overridden'] for r in overridden))
                        # The boundary, on a held pair during the pause: exactly at depth X passes, just above it does not.
                        runtime = controller.contexts['BNB-USDC'].runtime
                        native = runtime._observation[1]
                        self.assertIsNotNone(native.flash_pause_until)
                        at = (1 - CP_FROZEN.depth) * native.bag.recent_high
                        portfolio = controller.portfolio
                        on = portfolio.capitulation('BNB-USDC', dataclasses.replace(native, close=at), CP_FROZEN)
                        above = portfolio.capitulation('BNB-USDC', dataclasses.replace(native, close=at + D('1e-12')),
                                                       CP_FROZEN)
                        self.assertEqual((on['applied'], on['reason'], on['entry_state']), (True, 'capitulation', 'holds'))
                        self.assertEqual((above['applied'], above['reason']), (False, 'not_deep_enough'))
                        # Only the entry pause differs, on a copy; the cached native market is untouched.
                        runtime.observe()
                        self.assertEqual(runtime.capitulation['reason'], 'capitulation')
                        self.assertEqual(runtime.market, dataclasses.replace(native, flash_pause_until=None))
                        self.assertIs(runtime._observation[1], native)
                    else:
                        self.assertEqual(receipt['actions'], [])
                        self.assertFalse([r for r in self.receipts(session, CAPITULATION) if r['cut'] == cut])
                        self.assertEqual(outcomes, {p: ['native_paused'] for p in PAIRS})
                finally:
                    await session.close()

    async def test_the_veto_population_includes_first_entries_the_native_pause_held(self):
        for veto in ('e0', 'x0'):
            with self.subTest(veto=veto), tempfile.TemporaryDirectory() as directory:
                session = self.session(directory, entry_veto=veto)
                try:
                    before, rebound, price = crash_frames()
                    receipt = await self.enter(session, (before, rebound), price)
                    self.assertEqual(receipt['actions'], [])
                    census = {r['pair']: r for r in self.receipts(session, CENSUS)}
                    self.assertEqual({p: r['entry_state'] for p, r in census.items()}, {p: 'first' for p in PAIRS})
                    self.assertEqual(self.outcomes(session, 600000), {p: ['native_paused'] for p in PAIRS})
                    self.assertFalse(self.receipts(session, REFUSED))  # the veto never saw them
                finally:
                    await session.close()

    async def test_cp_keeps_the_pause_when_the_drop_is_not_deep_enough(self):
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, capitulation_override=CP_FROZEN)
            try:
                before, rebound, price = crash_frames(drop_to=0.96)
                receipt = await self.enter(session, (before, rebound), price)
                controller = session.controllers[OWNER]
                self.assertEqual(receipt['actions'], [])
                runtime = controller.contexts['BNB-USDC'].runtime
                self.assertEqual(runtime.capitulation['reason'], 'not_deep_enough')
                self.assertLess(D(runtime.capitulation['drawdown']), CP_FROZEN.depth)
                self.assertIsNotNone(runtime.market.flash_pause_until)
                self.assertFalse(self.receipts(session, CAPITULATION))
                # Every otherwise-eligible signal has a census row, and its outcome is the native pause.
                census = {r['pair']: r for r in self.receipts(session, CENSUS)}
                self.assertEqual(set(census), set(PAIRS))
                self.assertTrue(all(r['native_flash_pause_until'] is not None for r in census.values()))
                self.assertEqual(len({r['census_id'] for r in census.values()}), 5)
                self.assertEqual(self.outcomes(session, 600000), {p: ['native_paused'] for p in PAIRS})
            finally:
                await session.close()

    # ------------------------------------------------------------------ with the owner's V4.1 options
    async def test_with_v41_spacing_the_veto_refuses_first_and_its_refusal_does_not_consume_the_cut(self):
        # Pacing order pair_limit -> entry veto -> V4.1 spacing -> tide gate. A first entry has no open
        # zone, so spacing would allow it; the veto is evaluated before spacing reads ATR_1h, so a spacing
        # data failure cannot replace the veto's refusal code or its shadow receipt.
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, entry_veto='e0', **V41)
            try:
                controller = session.controllers[OWNER]
                unavailable = DataUnavailable('spacing needs 15 completed hours, has 3')
                with patch.dict(entry_veto.ENTRY_VETOES, {'e0': VETO_ALL}), \
                        patch.object(TidewaterPortfolio, '_hourly_atr', side_effect=unavailable):
                    vetoed = await self.enter(session)
                self.assertEqual(vetoed['actions'], [], controller.to_format_status())
                refused = self.receipts(session, REFUSED)
                self.assertEqual({(r['pair'], r['code']) for r in refused}, {(p, 'entry_veto_e0') for p in PAIRS})
                self.assertFalse([r for r in refused if 'spacing' in r or 'error' in r], refused)
                shadow = self.receipts(session, SHADOW_TIDE_GATE)
                self.assertEqual({(r['pair'], r['refused_by'], r['would_allow']) for r in shadow},
                                 {(p, 'entry_veto_e0', True) for p in PAIRS})
                # Same cut, the veto now allows: spacing paces the first tranche (no zone) and it is admitted.
                with patch.dict(entry_veto.ENTRY_VETOES, {'e0': ALLOW_ALL}):
                    allowed = await session.step(600061, books=books(2, 600061))
                self.assertEqual({a.executor_config.trading_pair for a in allowed['actions']}, set(PAIRS))
                admitted = self.receipts(session, ADMITTED)
                self.assertEqual({(r['pair'], r['code']) for r in admitted}, {(p, 'admitted') for p in PAIRS})
                for r in admitted:
                    self.assertEqual(r['pacing']['outcome'], 'paced_ok')
                    self.assertIsNone(r['pacing']['spacing']['nearest'])
                    self.assertIs(r['pacing']['entry_veto']['vetoed'], False)
                self.assertEqual(self.outcomes(session, 600000), {p: ['entry_veto_e0', 'admitted'] for p in PAIRS})
            finally:
                await session.close()

    async def test_the_census_records_a_spacing_refusal_and_the_later_admission_of_the_same_signal(self):
        # The owner's shallow 0.6% second rebound (test_v41): spacing refuses the add; the census row of
        # that signal records the refusal and, the cut not consumed, the admission on a deeper book.
        with tempfile.TemporaryDirectory() as directory:
            session = self.session(directory, tide_gate=None, **V41)
            try:
                controller = session.controllers[OWNER]
                self.assertEqual(len((await self.enter(session))['actions']), 5)
                await self.fill_all(session, 600061)
                before, rebound, cut, low = second_rebound(D('.006'))
                price = D(str(round(low, 2)))
                await session.step(cut, books=books(20, cut, price), candles=all_candles(before, cut))
                shallow = await session.step(cut + 60, books=books(21, cut + 60, price),
                                             candles=all_candles(rebound, cut + 60))
                self.assertEqual(shallow['actions'], [], controller.to_format_status())
                refused = [r for r in self.receipts(session, REFUSED) if r['cut'] == cut]
                self.assertEqual({r['code'] for r in refused}, {'spacing_gap'}, refused)
                census = {r['pair']: r for r in self.receipts(session, CENSUS) if r['cut'] == cut}
                self.assertEqual(set(census), {r['pair'] for r in refused})
                self.assertEqual({r['entry_state'] for r in census.values()}, {'holds'})
                self.assertEqual(self.outcomes(session, cut), {r['pair']: ['spacing_gap'] for r in refused})
                deeper = await session.step(cut + 61, books=books(22, cut + 61, price * D('.985')))
                self.assertEqual({a.executor_config.trading_pair for a in deeper['actions']}, set(census))
                self.assertEqual(self.outcomes(session, cut), {p: ['spacing_gap', 'admitted'] for p in census})
            finally:
                await session.close()
