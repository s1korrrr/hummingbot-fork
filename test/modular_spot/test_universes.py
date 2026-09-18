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
