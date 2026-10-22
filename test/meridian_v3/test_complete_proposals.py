"""Synthetic two-tick controller decisions; no session, connector, fills or market replay."""
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig
from controllers.generic.meridian_v3_unified_core.migration import grant_policy
from controllers.generic.meridian_v3_unified_core.portfolio import MeridianPortfolio
from controllers.generic.modular_spot_core.bag import BagFeatures
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.inventory import InventoryView
from controllers.generic.modular_spot_core.market import MarketFeatures
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
from controllers.generic.modular_spot_core.runtime import ModularRuntime
from controllers.generic.modular_spot_core.state import DecisionState, encode
from hummingbot.model import get_declarative_base
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.controllers.native_portfolio_bridge_v3 import PortfolioGrantStore
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.portfolio_v3.contracts import AssetState, PortfolioSnapshot

BTC, ETH = 'BTC-USDC', 'ETH-USDC'


@pytest.fixture
def native(tmp_path):
    get_declarative_base()
    engine = create_engine('sqlite:///' + str(tmp_path / 'native.sqlite'))
    Metadata.__table__.create(engine)
    yield sessionmaker(bind=engine)
    engine.dispose()


class Checkpoint:
    def __init__(self, state, recorder):
        self.state, self.recorder, self.writes = state, recorder, 0

    def load(self):
        return self.state, None if self.state is None else encode(self.state)

    def compare_and_swap(self, raw, revised):
        assert raw == (None if self.state is None else encode(self.state))
        self.state, self.writes = revised, self.writes + 1
        return True


class Runtime(ModularRuntime):
    """Real planner and action construction; native reads and dispatch effects are stubs."""

    def __init__(self, owner, pair, recorder):
        self.controller = NS(config=owner.symbol_config(pair), positions_held=[], executors_info=[],
                             market_data_provider=NS(time=lambda: 120), logger=lambda: Mock(),
                             _operator_entry_paused=False)
        self.fingerprint = self.controller.config.fingerprint()
        self.store = Checkpoint(DecisionState(fingerprint=self.fingerprint, candle=0), recorder)
        self.views = {}
        self.book = dict(bid=D('99.99'), ask=D('100.01'), available_quote=D('25'), pnl=D('0'),
                         amount_step=D('.001'), minimum_amount=D('.001'), minimum_notional=D('1'))
        self.market = MarketFeatures(60, D('100'), D('28'), D('31'), False, False, atr_fraction=D('.005'),
                                     bag=BagFeatures(D('28'), D('100'), D('0'), D('0'), D('.002'),
                                                     'range', D('0'), D('100')))
        self.input_identity = 'c' * 64
        self.closed_rows = {60: 'synthetic'}
        self.pending, self.pending_inputs, self.dispatched = {}, {}, set()
        self._decision_scope = None
        self.fault, self.reason = None, 'before_preview'
        self.budget = NS(occupied_pairs=lambda **kw: (), reserve=lambda claim, **kw:
                         owner.portfolio.bind_grant(pair, claim))
        self.selection_store = NS(freeze=Mock())
        self.bindings = NS(config=NativeBindings.config, action=NativeBindings.action,
                           fees_permit=lambda *args: True)
        self.portfolio, self.pair = owner.portfolio, pair

    def ensure_store(self):
        return self.store

    def inventory(self):
        return self.views

    def snapshot(self):
        return dict(self.book)

    def observe(self):
        return self.market

    def _managed_pairs(self):
        return ()

    def acquisition_permitted(self):
        return self.portfolio.acquisition_open(self.pair)

    def _bag_actions(self, *args):
        return []

    def _pending_recovery_ids(self):
        return set()


@pytest.fixture
def fixture(native):
    config = MeridianUnifiedConfig(
        id='modular_spot_meridian_v3_unified_paper', execution_mode='paper', capital_stage='full',
        connector_name='okx_paper_trade', account_id='meridian_paper',
        symbols=[dict(trading_pair=p) for p in PAIRS], priority=PAIRS,
        risk=dict(max_spread_fraction='.003', fee_reserve_fraction='.003', cash_reserve_quote=0))
    configs = {p: c.model_copy(update={'execution_enabled': True}) for p, c in config.symbol_configs().items()}
    owner = NS(config=config, symbol_config=configs.__getitem__, contexts={}, executors_info=[],
               market_data_provider=NS(time=lambda: 120), operator_entry_paused=False,
               entries_held=lambda: None, logger=lambda: Mock())
    portfolio = owner.portfolio = MeridianPortfolio(owner)
    recorder = NS(config_file_path='synthetic.yml', sql_manager=NS(get_new_session=native),
                  get_all_executors=lambda: [])
    portfolio.recorder = lambda: recorder
    portfolio._store = PortfolioGrantStore(recorder, grant_policy(config, portfolio.recipe))
    portfolio._targets = NS(head=lambda: None, publish=Mock())
    portfolio.execution_capable = lambda: (True, None)
    for pair in PAIRS:
        owner.contexts[pair] = NS(runtime=Runtime(owner, pair, recorder))
    assets = tuple(AssetState(symbol=p, quantity=0, basis_quote=0, net_cash_consumed_quote=0,
                              mark=100, bid='99.99', ask='100.01', lot='.001', minimum_quantity='.001',
                              minimum_notional=1, mark_event_ms=120000, available_at_ms=120000,
                              revision=1, sellable_quantity=0, minimum_sell_price=100,
                              acquisition_allowed=True, reduction_allowed=False) for p in PAIRS)
    snapshot = PortfolioSnapshot(portfolio_id='meridian_v3', account_id=config.account_id,
                                 mandate_sha256=portfolio.recipe.mandate.identity, source_sha256='a' * 64,
                                 sequence=1, cutoff_ms=120000, native_available_at_ms=120000,
                                 cash_total_quote=25, cash_available_quote=25, attributed_nav_quote=25,
                                 assets=assets, commitments=(), entry_universe=PAIRS,
                                 complete=True, reconciled=True, all_writers_accounted=True, risk_paused=False)
    portfolio.snapshot = lambda now: snapshot
    portfolio._seen = {p: 0 for p in PAIRS}
    markets = {p: c.runtime.market if p in (BTC, ETH) else None for p, c in owner.contexts.items()}
    return owner, portfolio, markets


def block(runtime, kind):
    if kind == 'flash':
        runtime.market = replace(runtime.market, flash_pause_until=D('1000'))
    elif kind == 'spread':
        runtime.book['ask'] = D('101')
    elif kind == 'cooldown':
        runtime.store.state = runtime.store.state.model_copy(update={'last_entry_time': D('120')})
    elif kind == 'missing_receipt':
        runtime.store.state = runtime.store.state.model_copy(update={'claims': ('old',), 'committed_quote': D('20')})
    elif kind in ('gap', 'depth'):
        runtime.store.state = runtime.store.state.model_copy(update={'claims': ('old',), 'committed_quote': D('60')})
        price = D('100') if kind == 'gap' else D('105')
        runtime.views['old'] = InventoryView(D('0.6'), D('60'), price, D('0'), False, True, False, D('60'))
    elif kind == 'fee':
        runtime.bindings.fees_permit = lambda *args: False
    elif kind == 'fault':
        runtime.fault = 'existing_fault'


@pytest.mark.parametrize('kind', ['flash', 'spread', 'cooldown', 'missing_receipt', 'gap', 'depth', 'fee', 'fault'])
def test_ineligible_priority_symbol_does_not_consume_scarce_grant(fixture, kind):
    owner, portfolio, markets = fixture
    btc, eth = owner.contexts[BTC].runtime, owner.contexts[ETH].runtime
    block(btc, kind)
    markets[BTC] = btc.market
    before = {p: c.runtime.store.load()[1] for p, c in owner.contexts.items()}
    assert portfolio.candidates(markets) == (ETH,)
    expected = {'flash': 'flash_crash_pause', 'spread': 'spread_limit', 'cooldown': 'cooldown',
                'missing_receipt': 'execution_receipt_missing', 'gap': 'acquisition_not_eligible',
                'depth': 'depth_budget', 'fee': 'fee_reserve_exceeded', 'fault': 'symbol_fault'}
    assert portfolio.status['candidate_holds'][BTC] == expected[kind]
    assert {p: c.runtime.store.load()[1] for p, c in owner.contexts.items()} == before
    assert all(c.runtime.store.writes == 0 for c in owner.contexts.values())
    assert portfolio.store().grants(now_ms=120000, native_executors=()) == ()
    assert all(not c.runtime.pending and not c.runtime.dispatched for c in owner.contexts.values())
    portfolio.evaluate(markets)
    actions = btc.actions() + eth.actions()
    portfolio.withdraw_unused()
    assert len(actions) == 1 and isinstance(actions[0], CreateExecutorAction)
    config = actions[0].executor_config
    assert config.trading_pair == ETH
    assert config.amount % D('.001') == 0
    assert config.amount * config.entry_price * D('1.003') <= 20
    assert portfolio.evaluate(markets) is None
    assert btc.actions() + eth.actions() == []
    grants = portfolio.store().grants(now_ms=120000, native_executors=())
    assert len(grants) == 1 and grants[0].symbol == ETH


def test_preview_does_not_persist_valid_proposal_or_consume_candle(fixture):
    owner, portfolio, markets = fixture
    before = {p: c.runtime.store.load()[1] for p, c in owner.contexts.items()}

    def durable_rows():
        with portfolio.recorder().sql_manager.get_new_session() as session:
            return sorted((r.key, r.value) for r in session.query(Metadata).all())
    persisted = durable_rows()
    assert portfolio.candidates(markets) == (BTC, ETH)
    assert portfolio.candidates(markets) == (BTC, ETH)
    assert portfolio._evaluated == {}
    assert durable_rows() == persisted
    assert all(c.runtime.reason == 'before_preview' for c in owner.contexts.values())
    assert {p: c.runtime.store.load()[1] for p, c in owner.contexts.items()} == before
    assert portfolio.store().grants(now_ms=120000, native_executors=()) == ()


def test_runtime_rechecks_changed_entry_gate_after_preview(fixture):
    owner, portfolio, markets = fixture
    btc = owner.contexts[BTC].runtime
    portfolio.evaluate(markets)
    assert portfolio.acquisition_open(BTC)
    block(btc, 'flash')  # change after allocation; normal runtime must still refuse it
    assert btc.actions() == []
    portfolio.withdraw_unused()
    assert portfolio.store().grants(now_ms=120000, native_executors=()) == ()
    assert not btc.pending and not btc.store.state.claims


def test_reserve_guard_excludes_proposals_that_cannot_preserve_cash(fixture):
    owner, portfolio, markets = fixture
    for context in owner.contexts.values():
        runtime = context.runtime
        c = runtime.controller.config
        runtime.controller.config = c.model_copy(update={
            'risk': c.risk.model_copy(update={'cash_reserve_quote': D('25')})})
        runtime.fingerprint = runtime.controller.config.fingerprint()
        runtime.store.state = DecisionState(fingerprint=runtime.fingerprint, candle=0)
    assert portfolio.candidates(markets) == ()
    assert portfolio.store().grants(now_ms=120000, native_executors=()) == ()


def test_admission_failure_does_not_consume_candidate_cut(fixture, monkeypatch):
    owner, portfolio, markets = fixture
    real_admit = portfolio.store().admit
    monkeypatch.setattr(portfolio.store(), 'admit', Mock(side_effect=PolicyError('temporary admission hold')))
    portfolio.evaluate(markets)
    assert portfolio._evaluated == {}
    monkeypatch.setattr(portfolio.store(), 'admit', real_admit)
    portfolio.evaluate(markets)
    assert portfolio._evaluated == {BTC: 60, ETH: 60}
    assert len(portfolio.store().grants(now_ms=120000, native_executors=())) == 1


def test_observation_seed_is_non_executing_and_cannot_seed_real_execution(fixture):
    owner, portfolio, markets = fixture
    for context in owner.contexts.values():
        runtime = context.runtime
        runtime.controller.config = runtime.controller.config.model_copy(update={'execution_enabled': False})
        runtime.fingerprint = runtime.controller.config.fingerprint()
        runtime.store.state = None
    portfolio.execution_capable = lambda: (False, 'EXECUTION_CAPABILITY_UNBOUND')
    target = portfolio.evaluate(markets)
    assert any(q.change > 0 for q in target.quantities)
    assert portfolio.status['candidate_previews'] == {BTC: 'observational_entry_proposed',
                                                      ETH: 'observational_entry_proposed'}
    assert portfolio.store().grants(now_ms=120000, native_executors=()) == ()
    assert all(c.runtime.store.state is None and not c.runtime.pending for c in owner.contexts.values())
    btc = owner.contexts[BTC].runtime
    btc.controller.config = btc.controller.config.model_copy(update={'execution_enabled': True})
    btc.fingerprint = btc.controller.config.fingerprint()
    assert btc.actions() == []  # real native seed is still required, and creates no executor
    assert btc.store.state.candle == 60 and not btc.store.state.claims


@pytest.mark.parametrize('history', ['inventory', 'held', 'pending'])
def test_observation_never_synthesizes_checkpoint_over_native_obligations(fixture, history):
    owner, portfolio, markets = fixture
    btc = owner.contexts[BTC].runtime
    btc.controller.config = btc.controller.config.model_copy(update={'execution_enabled': False})
    btc.fingerprint = btc.controller.config.fingerprint()
    btc.store.state = None
    if history == 'inventory':
        btc.views['old'] = InventoryView(D('0'), D('20'), D('100'), D('.2'), True, True, False)
    elif history == 'pending':
        btc.pending['old'] = NS(trading_pair=BTC)
    else:
        btc.controller.positions_held = [NS(connector_name='okx_paper_trade', trading_pair=BTC, side='BUY',
                                            amount=D('.1'), breakeven_price=D('100'), remaining_fees_quote=D('0'))]
    assert BTC not in portfolio.candidates(markets)
    assert portfolio.status['candidate_holds'][BTC] == 'observation_checkpoint_required'
    assert btc.store.state is None and btc.store.writes == 0
