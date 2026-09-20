from decimal import Decimal as D

import pytest
from pydantic import ValidationError

from controllers.generic.modular_spot_core.acquisition import next_stage
from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy, TrailingPolicy
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.portfolio import allocate
from controllers.generic.modular_spot_core.risk import risk_update
from controllers.generic.modular_spot_core.signals import signal_decision
from controllers.generic.modular_spot_core.trailing import protected_sell_price


def test_rsi_cross_requires_a_fresh_rebound():
    p = SignalPolicy()
    assert signal_decision(p, previous_rsi=D('28'), rsi=D('31'), breakout=False, trend_up=False).enter
    assert not signal_decision(p, previous_rsi=D('31'), rsi=D('32'), breakout=False, trend_up=False).enter
    assert not signal_decision(p, previous_rsi=D('28'), rsi=D('25'), breakout=False, trend_up=False).enter


def test_momentum_does_not_reuse_rsi_decision():
    p = SignalPolicy(kind='breakout')
    assert signal_decision(p, previous_rsi=D('75'), rsi=D('76'), breakout=True, trend_up=True).enter
    assert not signal_decision(p, previous_rsi=D('75'), rsi=D('76'), breakout=True, trend_up=False).enter


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '-Infinity', '-1', '101'])
def test_bad_rsi_does_not_become_a_signal(value):
    with pytest.raises(PolicyError):
        signal_decision(SignalPolicy(), previous_rsi=D(value), rsi=D('31'), breakout=False, trend_up=False)


@pytest.mark.parametrize('values', [[], ['0'], ['.3', '.3'], ['-.1', '1.1'], ['1'] * 6])
def test_acquisition_fractions_are_bounded(values):
    with pytest.raises(ValidationError):
        AcquisitionPolicy(fractions=values)


def test_dca_is_fresh_signal_and_price_gated():
    p = AcquisitionPolicy(fractions=['.4', '.6'], minimum_drop=D('.02'))
    assert next_stage(p, stage=1, bid=D('98'), last_fill_price=D('100'), fresh_signal=True,
                      entry_terminal=True, distribution_started=False) == D('.6')
    assert next_stage(p, stage=1, bid=D('99'), last_fill_price=D('100'), fresh_signal=True,
                      entry_terminal=True, distribution_started=False) is None
    for flag in ('fresh_signal', 'entry_terminal'):
        args = dict(stage=1, bid=D('97'), last_fill_price=D('100'), fresh_signal=True,
                    entry_terminal=True, distribution_started=False)
        args[flag] = False
        assert next_stage(p, **args) is None
    assert next_stage(p, stage=1, bid=D('97'), last_fill_price=D('100'), fresh_signal=True,
                      entry_terminal=True, distribution_started=True) is None


def test_allocation_includes_fees_and_never_rounds_quantity_up():
    result = allocate(stage_quote=D('50'), remaining_quote=D('70'), available_quote=D('100'),
                      price=D('30'), amount_step=D('.1'), minimum_amount=D('.1'),
                      minimum_notional=D('1'), fee_buffer=D('.01'), cash_reserve=D('0'))
    assert result.amount == D('1.6')
    assert result.reserved_quote == D('48.480')
    assert result.reserved_quote <= D('50')


def test_allocation_respects_actual_cash_and_campaign_commitment():
    result = allocate(stage_quote=D('100'), remaining_quote=D('30'), available_quote=D('20'),
                      price=D('10'), amount_step=D('.1'), minimum_amount=D('.1'),
                      minimum_notional=D('1'), fee_buffer=D('.01'), cash_reserve=D('5'))
    assert result.reserved_quote <= D('15')
    assert result.reserved_quote <= D('30')


def test_dust_returns_no_allocation():
    assert allocate(stage_quote=D('.2'), remaining_quote=D('1'), available_quote=D('1'),
                    price=D('10'), amount_step=D('.1'), minimum_amount=D('.1'),
                    minimum_notional=D('1'), fee_buffer=D('.01'), cash_reserve=D('0')) is None


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '-1', '0'])
def test_invalid_execution_price_is_rejected(value):
    with pytest.raises(PolicyError):
        allocate(stage_quote=D('50'), remaining_quote=D('70'), available_quote=D('100'),
                 price=D(value), amount_step=D('.1'), minimum_amount=D('.1'),
                 minimum_notional=D('1'), fee_buffer=D('.01'), cash_reserve=D('0'))


def test_floor_is_above_native_after_cost_floor_and_tick_aligned():
    assert protected_sell_price(D('100.003'), TrailingPolicy(activation=D('.015'), delta=D('.005')),
                                D('.01')) == D('101.01')


@pytest.mark.parametrize('kwargs', [{'activation': '.01', 'delta': '.02'}, {'delta': '0'},
                                    {'activation': 'NaN'}, {'extra_policy': 1}])
def test_trailing_rejects_incompatible_or_unknown_config(kwargs):
    with pytest.raises(ValidationError):
        TrailingPolicy(**kwargs)


def test_risk_uses_marked_pnl_and_latches_across_day_boundary():
    policy = RiskPolicy(max_drawdown_fraction=D('.1'), max_daily_loss_fraction=D('.05'))
    first = risk_update(policy, budget=D('100'), pnl=D('2'), day=1, peak=None,
                        previous_day=None, day_start=None, paused=False)
    assert not first.paused
    loss = risk_update(policy, budget=D('100'), pnl=D('-4'), day=1, peak=first.peak,
                       previous_day=first.day, day_start=first.day_start, paused=False)
    assert loss.paused and loss.reason == 'daily_loss_limit'
    assert risk_update(policy, budget=D('100'), pnl=D('0'), day=2, peak=loss.peak,
                       previous_day=loss.day, day_start=loss.day_start, paused=True).paused


def test_drawdown_includes_giveback_of_prior_profit():
    result = risk_update(RiskPolicy(), budget=D('100'), pnl=D('9'), day=1, peak=D('20'),
                         previous_day=1, day_start=D('9'), paused=False)
    assert result.paused and result.reason == 'drawdown_limit'
