"""Native portfolio grant admission (N05-N12). Synthetic receipts; no orders or economics."""
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal as D
from types import SimpleNamespace as NS

import pytest

from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.controllers.native_portfolio_bridge_v3 import PortfolioGrantStore
from hummingbot.strategy_v2.portfolio_v3.contracts import TargetPortfolio, TargetQuantity
from hummingbot.strategy_v2.portfolio_v3.runtime_contracts_v1 import GrantPolicy

H = 'a' * 64
OWNER = 'modular_spot_meridian_v3_unified_paper'
PAIRS = ('BNB-USDC', 'BTC-USDC', 'ETH-USDC', 'SOL-USDC', 'XRP-USDC')
PRIORITY = ('BTC-USDC', 'ETH-USDC', 'BNB-USDC', 'SOL-USDC', 'XRP-USDC')


def policy(**changes):
    return GrantPolicy(**{**dict(portfolio_id='meridian_v3', account_id='paper_only', controller_id=OWNER,
                                 connector='okx_paper_trade', mandate_sha256=H, policy_sha256=H, pairs=PAIRS,
                                 envelope_quote=500, per_asset_quote=100, order_quote=20), **changes})


def recorder(native):
    return NS(config_file_path='meridian.yml', sql_manager=NS(get_new_session=native))


def store(native, **changes):
    return PortfolioGrantStore(recorder(native), policy(**changes))


def target(*pairs, status='PROPOSAL_ONLY', decision=1000, expires=61000, sha=None):
    quantities = tuple(TargetQuantity(symbol=p, quantity=D('.1') if p in pairs else 0,
                                      change=D('.1') if p in pairs else 0, reference_weight=0) for p in PAIRS)
    return TargetPortfolio(portfolio_id='meridian_v3', recipe_sha256=H, mandate_sha256=H, policy_sha256=H,
                           snapshot_sha256=sha or H, forecast_sha256=H, sequence=1, epoch='e1', fence=1,
                           decision_time_ms=decision, expires_at_ms=expires, quantities=quantities,
                           requested_acquisition_quote=D(20) * len(pairs), status=status, reasons=(), producer_reasons=())


def admit(s, *pairs, now=1000, setup='cut1', capable=True, evidence=(), **kwargs):
    return s.admit(target(*pairs, **kwargs), priority=PRIORITY, setup_ids={p: setup for p in PAIRS}, now_ms=now,
                   submit_deadline_ms=now + 30000, native_executors=evidence, execution_capable=capable)


def executor(identifier, pair='BTC-USDC', *, done=False, remaining='0', owner='executor', controller=OWNER,
             side='BUY', level='modular_spot:v1:entry', min_sell=None, connector='okx_paper_trade'):
    config = NS(controller_id=controller, connector_name=connector, trading_pair=pair, type='position_executor',
                side=NS(name=side), position_action=NS(name='OPEN'), level_id=level, min_sell_price=min_sell)
    return NS(id=identifier, controller_id=controller, is_done=done, is_active=not done, config=config,
              custom_info={'execution_receipt': {'remaining_owned_base': remaining, 'accounting_owner': owner}})


def test_priority_and_one_increment_per_symbol_within_envelope(native):
    s = store(native, envelope_quote=40, per_asset_quote=20)
    outcome = admit(s, *PAIRS)
    live = s.grants(now_ms=1000, native_executors=())
    assert {g.symbol for g in live} == {'BTC-USDC', 'ETH-USDC'}
    assert len(outcome.admitted) == 2 and set(outcome.denied) == {'BNB-USDC', 'SOL-USDC', 'XRP-USDC'}
    assert outcome.reasons == ('ACQUISITION_FULL_INCREMENT_UNAVAILABLE',)
    assert all(g.ceiling_quote == 20 and g.status == 'ADMITTED' for g in live)


def test_concurrent_admissions_cannot_spend_one_remaining_increment_twice(native):
    s = store(native, envelope_quote=20, per_asset_quote=20)
    with ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(lambda pair: admit(store(native, envelope_quote=20, per_asset_quote=20), pair,
                                         setup=f'cut-{pair}'), PAIRS))
    live = s.grants(now_ms=1000, native_executors=())
    assert len(live) == 1 and sum(g.reserved_quote for g in live) == 20


def test_admitted_lapses_at_deadline_but_bound_authority_never_expires(native):
    s = store(native)
    admit(s, 'BTC-USDC', 'ETH-USDC')
    assert s.bind(executor_id='x1', symbol='BTC-USDC', reserved_quote='19.9', now_ms=2000, native_executors=())
    later = s.grants(now_ms=10 ** 9, native_executors=())
    assert [(g.symbol, g.status, g.executor_id) for g in later] == [('BTC-USDC', 'BOUND', 'x1')]
    assert not s.bind(executor_id='x2', symbol='ETH-USDC', reserved_quote=1, now_ms=10 ** 9, native_executors=())


def test_bind_requires_matching_admitted_grant_and_is_idempotent(native):
    s = store(native)
    assert not s.bind(executor_id='x1', symbol='BTC-USDC', reserved_quote=10, now_ms=1000, native_executors=())
    admit(s, 'BTC-USDC')
    assert not s.bind(executor_id='x1', symbol='BTC-USDC', reserved_quote='20.01', now_ms=1000, native_executors=())
    assert not s.bind(executor_id='x1', symbol='ETH-USDC', reserved_quote=10, now_ms=1000, native_executors=())
    assert s.bind(executor_id='x1', symbol='BTC-USDC', reserved_quote=10, now_ms=1000, native_executors=())
    assert s.bind(executor_id='x1', symbol='BTC-USDC', reserved_quote=10, now_ms=1001, native_executors=())
    assert not s.bind(executor_id='x2', symbol='BTC-USDC', reserved_quote=10, now_ms=1001, native_executors=())
    assert s.contains(executor_id='x1', symbol='BTC-USDC', reserved_quote=10, native_executors=(), now_ms=1002)
    assert not s.contains(executor_id='x1', symbol='BTC-USDC', reserved_quote=11, native_executors=(), now_ms=1002)


def test_cancel_with_late_fill_retains_and_terminal_flat_releases(native):
    s = store(native, envelope_quote=20, per_asset_quote=20)
    admit(s, 'BTC-USDC')
    assert s.bind(executor_id='x1', symbol='BTC-USDC', reserved_quote=20, now_ms=1000, native_executors=())
    cancelling = executor('x1', done=False, remaining='0')
    late = executor('x1', done=True, remaining='.01', owner='held_position')
    for evidence in ((cancelling,), (late,)):
        outcome = admit(s, 'ETH-USDC', setup='cut2', now=2000, decision=2000, expires=62000, evidence=evidence)
        assert outcome.admitted == () and 'ACQUISITION_FULL_INCREMENT_UNAVAILABLE' in outcome.reasons
    flat = executor('x1', done=True, remaining='0')
    assert admit(s, 'ETH-USDC', setup='cut3', now=3000, decision=3000, expires=63000,
                 evidence=(flat,)).admitted


def test_unexplained_or_foreign_spenders_hold_admission(native):
    s = store(native)
    with pytest.raises(PolicyError, match='unreserved or reappearing'):
        admit(s, 'BTC-USDC', evidence=(executor('ghost', remaining='.1'),))
    with pytest.raises(PolicyError, match='foreign spender'):
        admit(s, 'BTC-USDC', evidence=(executor('other', controller='modular_spot_v2_btc'),))
    assert admit(s, 'BTC-USDC', evidence=(executor('old', controller='modular_spot_v2_btc', done=True),)).admitted
    with pytest.raises(PolicyError, match='ownership differs'):
        admit(s, 'BTC-USDC', setup='c9', evidence=(executor('doge', pair='DOGE-USDC', done=True),))


def test_restart_reloads_grants_and_policy_change_holds(native):
    admit(store(native), 'BTC-USDC')
    assert store(native).bind(executor_id='x1', symbol='BTC-USDC', reserved_quote=5, now_ms=1000, native_executors=())
    assert [g.executor_id for g in store(native).grants(now_ms=5000, native_executors=())] == ['x1']
    with pytest.raises(PolicyError, match='policy changed'):
        store(native, per_asset_quote=80).grants(now_ms=5000, native_executors=())


def test_setup_replay_is_idempotent_and_consumed_setups_never_rearm(native):
    s = store(native)
    first = admit(s, 'BTC-USDC')
    assert admit(s, 'BTC-USDC').admitted == first.admitted
    s.bind(executor_id='x1', symbol='BTC-USDC', reserved_quote=5, now_ms=1000, native_executors=())
    assert admit(s, 'BTC-USDC').reasons == ('SETUP_ALREADY_CONSUMED',)
    flat = executor('x1', done=True)
    assert admit(s, 'BTC-USDC', evidence=(flat,)).reasons == ('SETUP_ALREADY_CONSUMED',)
    assert admit(s, 'BTC-USDC', setup='cut2', evidence=(flat,)).admitted


def test_withdraw_returns_only_unbound_grants(native):
    s = store(native)
    outcome = admit(s, 'BTC-USDC', 'ETH-USDC')
    btc = next(g for g in s.grants(now_ms=1000, native_executors=()) if g.symbol == 'BTC-USDC')
    s.bind(executor_id='x1', symbol='BTC-USDC', reserved_quote=5, now_ms=1000, native_executors=())
    removed = s.withdraw(grant_ids=outcome.admitted, now_ms=1000, native_executors=())
    assert btc.grant_id not in removed and len(removed) == 1
    assert [g.symbol for g in s.grants(now_ms=1000, native_executors=())] == ['BTC-USDC']


def test_held_release_is_symbol_scoped_and_proven(native):
    s = store(native)
    admit(s, 'BTC-USDC')
    s.bind(executor_id='x1', symbol='BTC-USDC', reserved_quote=20, now_ms=1000, native_executors=())
    held = executor('x1', done=True, remaining='.2', owner='held_position')
    with pytest.raises(PolicyError, match='terminal executor of this symbol'):
        s.release_held(symbol='ETH-USDC', executor_ids=('x1',), native_executors=(held,), now_ms=2000)
    with pytest.raises(PolicyError, match='only held'):
        s.release_held(symbol='BTC-USDC', executor_ids=('x1',),
                       native_executors=(executor('x1', done=True, remaining='.2'),), now_ms=2000)
    s.release_held(symbol='BTC-USDC', executor_ids=('x1',), native_executors=(held,), now_ms=2000)
    assert s.grants(now_ms=2000, native_executors=(held,)) == ()
    assert 'x1' in s.released()


def test_unexecuted_release_needs_absent_executor_and_matching_symbol(native):
    s = store(native)
    admit(s, 'BTC-USDC', 'ETH-USDC')
    assert s.bind(executor_id='x1', symbol='BTC-USDC', reserved_quote=20, now_ms=1000, native_executors=())
    assert s.bind(executor_id='x2', symbol='ETH-USDC', reserved_quote=20, now_ms=1000, native_executors=())
    with pytest.raises(PolicyError, match='executor exists'):  # a real executor's receipt decides its release
        s.release_unexecuted(executor_id='x1', symbol='BTC-USDC', native_executors=(executor('x1'),), now_ms=2000)
    with pytest.raises(PolicyError, match='another symbol'):
        s.release_unexecuted(executor_id='x1', symbol='ETH-USDC', native_executors=(), now_ms=2000)
    assert len(s.release_unexecuted(executor_id='x1', symbol='BTC-USDC', native_executors=(), now_ms=2000)) == 1
    assert [(g.symbol, g.executor_id) for g in s.grants(now_ms=2000, native_executors=())] == [('ETH-USDC', 'x2')]
    assert s.release_unexecuted(executor_id='x1', symbol='BTC-USDC', native_executors=(), now_ms=2001) == ()
    with pytest.raises(PolicyError, match='unreserved or reappearing'):  # an executor appearing later holds admission
        s.grants(now_ms=2002, native_executors=(executor('x1'),))


def test_holds_and_missing_capability_create_no_grants(native):
    s = store(native)
    assert admit(s, 'BTC-USDC', capable=False).reasons == ('EXECUTION_CAPABILITY_UNBOUND',)
    assert admit(s, status='ENTRY_HOLD').admitted == ()
    assert s.grants(now_ms=1000, native_executors=()) == ()
    with pytest.raises(PolicyError, match='expired'):
        admit(s, 'BTC-USDC', now=70000)
    with pytest.raises(PolicyError, match='scope differs'):
        s.admit(target('BTC-USDC').model_copy(update={'portfolio_id': 'other'}), priority=PRIORITY,
                setup_ids={'BTC-USDC': 'c'}, now_ms=1000, submit_deadline_ms=2000, native_executors=(),
                execution_capable=True)
