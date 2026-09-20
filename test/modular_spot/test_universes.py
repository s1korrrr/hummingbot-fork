from decimal import Decimal as D

import pytest

from controllers.generic.modular_spot_core.universe import UniversePolicy, select_universe

BASE = dict(available=('A-USDC', 'B-USDC', 'C-USDC', 'BTC-USDC'),
            tradable=('A-USDC', 'B-USDC', 'C-USDC'), scores={'A-USDC': D('2'), 'B-USDC': D('3'), 'C-USDC': D('1')},
            managed=(), pending=(), blocked=(), previous=())


def test_fixed_selection_is_not_a_position_target():
    policy = UniversePolicy(symbols=('A-USDC', 'B-USDC'), benchmarks=('BTC-USDC',), max_candidates=2)
    result = select_universe(policy, **BASE)
    assert set(result.entries) == {'A-USDC', 'B-USDC'}
    assert 'BTC-USDC' in result.observation
    assert result.management == ()


def test_sell_only_has_zero_acquisition_and_keeps_assigned_symbols():
    policy = UniversePolicy(mode='sell_only', symbols=('A-USDC', 'B-USDC'))
    result = select_universe(policy, **{**BASE, 'managed': ('C-USDC',)})
    assert not result.entries and 'C-USDC' in result.observation and result.management == ('C-USDC',)


def test_all_universe_does_not_mean_all_trade_permissions():
    policy = UniversePolicy(mode='impulse', observe_all=True, max_candidates=2)
    result = select_universe(policy, **{**BASE, 'tradable': ('C-USDC',)})
    assert set(result.observation) == set(BASE['available'])
    assert result.entries == ('C-USDC',)


def test_removed_and_blocked_symbols_still_have_management_data():
    policy = UniversePolicy(symbols=('A-USDC',))
    result = select_universe(policy, **{**BASE, 'managed': ('B-USDC',), 'pending': ('C-USDC',), 'blocked': ('B-USDC',)})
    assert result.entries == ('A-USDC',)
    assert set(result.management) == {'B-USDC', 'C-USDC'}
    assert set(result.observation) == {'A-USDC', 'B-USDC', 'C-USDC'}


def test_hysteresis_stabilizes_ranking_but_cannot_override_hard_exclusion():
    policy = UniversePolicy(mode='impulse', observe_all=True, max_candidates=1, replacement_margin=D('.5'))
    base = {**BASE, 'scores': {'A-USDC': D('2'), 'B-USDC': D('2.2')}, 'previous': ('A-USDC',)}
    assert select_universe(policy, **base).entries == ('A-USDC',)
    assert select_universe(policy, **{**base, 'blocked': ('A-USDC',)}).entries == ('B-USDC',)


def test_rank_ties_are_deterministic_and_missing_scores_do_not_become_zero():
    policy = UniversePolicy(mode='impulse', observe_all=True, max_candidates=3)
    result = select_universe(policy, **{**BASE, 'scores': {'B-USDC': D('2'), 'A-USDC': D('2')}})
    assert result.entries == ('A-USDC', 'B-USDC')


@pytest.mark.parametrize('kwargs', [dict(symbols=('A-USDC', 'A-USDC')), dict(symbols=('bad',)),
                                    dict(max_candidates=0), dict(mode='sell_only', observe_all=True)])
def test_bad_universe_config_rejected(kwargs):
    with pytest.raises(ValueError):
        UniversePolicy(**kwargs)


def test_missing_required_context_holds_candidates_but_keeps_subscriptions():
    policy = UniversePolicy(symbols=('A-USDC',), benchmarks=('ETH-USDT',), max_candidates=2)
    result = select_universe(policy, **{**BASE, 'available': ('A-USDC', 'B-USDC', 'C-USDC')})
    assert result.entries == ()
    assert 'ETH-USDT' in result.observation


def test_pending_and_managed_inventory_consume_slots():
    policy = UniversePolicy(symbols=('A-USDC', 'B-USDC', 'C-USDC'), max_candidates=2)
    result = select_universe(policy, **{**BASE, 'managed': ('X-USDC',), 'pending': ('Y-USDC',)})
    assert result.entries == ()
    assert set(result.management) == {'X-USDC', 'Y-USDC'}


def test_alias_pairs_count_as_one_asset_for_slots():
    policy = UniversePolicy(symbols=('BTC-USDC', 'ETH-USDC'), max_candidates=2)
    result = select_universe(policy, available=('BTC-USDC', 'ETH-USDC', 'BTC-USDT'),
                             tradable=('BTC-USDC', 'ETH-USDC'), scores={}, managed=('BTC-USDT',),
                             pending=(), blocked=(), previous=())
    assert 'BTC-USDC' not in result.entries
    assert result.entries == ('ETH-USDC',)
    assert 'BTC-USDT' in result.management


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '-Infinity'])
def test_nonfinite_score_cannot_enter_ranking(value):
    policy = UniversePolicy(mode='impulse', observe_all=True, max_candidates=1)
    with pytest.raises(ValueError):
        select_universe(policy, **{**BASE, 'scores': {'A-USDC': D(value)}})
