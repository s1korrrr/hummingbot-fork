from decimal import Decimal as D
from dataclasses import replace

import pytest

from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.inventory import InventoryView
from controllers.generic.modular_spot_core.market import MarketFeatures
from controllers.generic.modular_spot_core.planner import plan
from controllers.generic.modular_spot_core.state import DecisionState


def inputs(**changes):
    values = dict(fingerprint='a' * 64, budget=D('100'), now=1200,
                  market=MarketFeatures(1140, D('100'), D('29'), D('31'), False, False),
                  bid=D('100'), ask=D('100.1'), available_quote=D('100'),
                  amount_step=D('.001'), minimum_amount=D('.001'), minimum_notional=D('1'),
                  pnl=D('0'), signal=SignalPolicy(), acquisition=AcquisitionPolicy(), risk=RiskPolicy())
    return {**values, **changes}


def test_new_owner_seeds_without_trading_and_same_bar_is_consumed():
    state, proposal, reason = plan(None, {}, **inputs())
    assert reason == 'seeded' and proposal is None
    assert plan(state, {}, **inputs())[1] is None


def test_entry_commits_exact_quantity_budget_and_identity_before_dispatch():
    state = DecisionState(fingerprint='a' * 64, candle=1080)
    revised, proposal, reason = plan(state, {}, **inputs())
    assert reason == 'entry_proposed'
    assert revised.claims == (proposal.identifier,)
    assert revised.committed_quote == proposal.amount * proposal.price * D('1.005')
    assert revised.committed_quote <= D('100')
    assert plan(revised, {}, **inputs())[2] == 'execution_receipt_missing'
    assert plan(state, {}, **inputs())[1] == proposal


def test_missing_checkpoint_with_native_history_never_adopts_it():
    view = {'x': InventoryView(D('0'), D('100'), D('100'), D('1'), True, True, False)}
    with pytest.raises(PolicyError):
        plan(None, view, **inputs())


def test_unknown_active_owner_claim_blocks_instead_of_ignoring():
    state = DecisionState(fingerprint='a' * 64, candle=1080)
    view = {'x': InventoryView(D('1'), D('100'), D('100'), D('0'), False, True, False)}
    with pytest.raises(PolicyError):
        plan(state, view, **inputs())


def test_risk_latch_updates_on_same_candle_without_new_orders():
    state = DecisionState(fingerprint='a' * 64, candle=1140)
    revised, proposal, reason = plan(state, {}, **inputs(pnl=D('-11')))
    assert revised.risk_paused and proposal is None and reason == 'drawdown_limit'
    assert plan(revised, {}, **inputs(pnl=D('20')))[0].risk_paused


@pytest.mark.parametrize('change', [dict(pnl=None), dict(ask=D('99')), dict(available_quote=D('NaN'))])
def test_invalid_current_snapshot_is_not_tradeable(change):
    state = DecisionState(fingerprint='a' * 64, candle=1080)
    with pytest.raises(PolicyError):
        plan(state, {}, **inputs(**change))


def test_no_scale_in_after_distribution_begins():
    state = DecisionState(fingerprint='a' * 64, candle=1080, claims=('one',), committed_quote=D('50'))
    view = {'one': InventoryView(D('.1'), D('50'), D('110'), D('.1'), False, True, False)}
    revised, proposal, reason = plan(state, view, **inputs(acquisition=AcquisitionPolicy(fractions=(D('.5'), D('.5')))))
    assert proposal is None and revised.claims == ('one',)


def test_scale_in_uses_last_actual_fill_and_never_increases_campaign_budget():
    state = DecisionState(fingerprint='a' * 64, candle=1080, claims=('one',), committed_quote=D('50'))
    view = {'one': InventoryView(D('.45'), D('50'), D('110'), D('0'), False, True, False)}
    revised, proposal, reason = plan(state, view, **inputs(acquisition=AcquisitionPolicy(fractions=(D('.5'), D('.5')))))
    assert proposal is not None and revised.committed_quote <= D('100')
    view['one'] = replace(view['one'], entry_terminal=False)
    assert plan(state, view, **inputs(acquisition=AcquisitionPolicy(fractions=(D('.5'), D('.5')))))[1] is None


def test_late_inventory_in_an_already_closed_campaign_cannot_disappear():
    state = DecisionState(fingerprint='a' * 64, candle=1080, cycle=1)
    view = {'late': InventoryView(D('.01'), D('100'), D('100'), D('.99'), True, True, True)}
    with pytest.raises(PolicyError):
        plan(state, view, **inputs())


def test_unfilled_stage_is_not_a_new_fill_reference():
    state = DecisionState(fingerprint='a' * 64, candle=1080, claims=('one', 'two'), committed_quote=D('60'))
    view = {'one': InventoryView(D('.3'), D('30'), D('110'), D('0'), False, True, False),
            'two': InventoryView(D('0'), D('0'), D('90'), D('0'), True, True, False)}
    revised, proposal, reason = plan(state, view, **inputs(
        acquisition=AcquisitionPolicy(fractions=(D('.3'), D('.3'), D('.4')))))
    assert proposal is not None and revised.committed_quote <= D('100')
