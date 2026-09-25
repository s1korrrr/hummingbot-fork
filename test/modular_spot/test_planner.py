from dataclasses import replace
from decimal import Decimal as D

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


def _view(remaining, spent, price, terminal, held=False, planned=None):
    return InventoryView(D(remaining), D(spent), D(price), D('0'), terminal, True, held,
                         D(planned if planned is not None else spent))


def test_bag_mode_counts_active_tranches_and_frees_slots_as_tranches_close_or_are_held():
    from controllers.generic.modular_spot_core.bag import HeldBag

    acquisition = AcquisitionPolicy(fractions=(D('.5'), D('.5')))
    state = DecisionState(fingerprint='a' * 64, candle=1080, claims=('closed', 'held', 'live'),
                          committed_quote=D('150'))
    view = {'closed': _view('0', '50', '101', True), 'held': _view('.5', '50', '100', True, held=True),
            'live': _view('.5', '50', '102', False)}
    bag = HeldBag(D('.5'), D('105'), D('.1'))  # bid 99 is >= 2% (minimum_drop) away from the bag
    market = MarketFeatures(1140, D('99'), D('29'), D('31'), False, False)
    revised, proposal, reason = plan(state, view, **inputs(bid=D('99'), ask=D('99.1'), acquisition=acquisition,
                                                           market=market, budget=D('200'),
                                                           available_quote=D('200')), bag=bag)
    # One active tranche: stage 1 of 2, dip from its 102 fill, capital = live tranche + bag cost.
    assert reason == 'entry_proposed' and proposal.stage == 1
    assert revised.claims[:3] == state.claims and revised.claims[-1] == proposal.identifier
    tied = D('50') * D('1.005') + D('.5') * D('105') + D('.1')
    assert proposal.amount * proposal.price * D('1.005') <= D('200') - tied


def test_bag_mode_vetoes_buys_too_close_to_bag_breakeven_and_exhausted_capital():
    from controllers.generic.modular_spot_core.bag import HeldBag

    state = DecisionState(fingerprint='a' * 64, candle=1080)
    near = plan(state, {}, **inputs(), bag=HeldBag(D('1'), D('100.2'), D('0')))
    assert near[1] is None and near[2] == 'bag_gap_not_met'
    broke = plan(state, {}, **inputs(), bag=HeldBag(D('1'), D('90'), D('11')))
    assert broke[1] is None and broke[2] == 'bag_capital_exhausted'


def test_bag_mode_campaign_resets_only_when_every_claim_is_terminal_and_the_bag_is_empty():
    from controllers.generic.modular_spot_core.bag import HeldBag

    state = DecisionState(fingerprint='a' * 64, candle=1080, claims=('held',), committed_quote=D('50'))
    view = {'held': _view('.5', '50', '100', True, held=True)}
    kept = plan(state, view, **inputs(), bag=HeldBag(D('.5'), D('100'), D('0')))
    assert kept[0].claims == ('held',) and kept[2] != 'campaign_flat'
    flat = plan(state, view, **inputs(), bag=HeldBag(D('0'), D('0'), D('0')))
    assert flat[2] == 'campaign_flat' and flat[0].claims == () and flat[0].cycle == 1
    # The next candle: the retired held tranche's receipt still reports its base (the bag, not
    # the tranche, sold it). It must not fault as unclaimed execution; a new cycle can enter.
    next_candle = MarketFeatures(1200, D('100'), D('29'), D('31'), False, False)
    later = plan(flat[0], view, **inputs(now=1260, market=next_candle), bag=HeldBag(D('0'), D('0'), D('0')))
    assert later[2] == 'entry_proposed' and later[0].claims == (later[1].identifier,) and later[0].cycle == 1
    # Legacy mode still refuses the same unclaimed receipt.
    with pytest.raises(PolicyError, match='unclaimed'):
        plan(flat[0], view, **inputs(now=1260, market=next_candle))


def test_bag_mode_off_grid_dust_counts_as_an_empty_bag():
    from controllers.generic.modular_spot_core.bag import HeldBag

    state = DecisionState(fingerprint='a' * 64, candle=1080, claims=('held',), committed_quote=D('50'))
    view = {'held': _view('.5', '50', '100', True, held=True)}
    dust = HeldBag(D('.0004'), D('100'), D('0'))  # a sweep rounded to the .001 lot leaves this behind
    assert plan(state, view, **inputs(), bag=dust)[2] == 'campaign_flat'
    notional = HeldBag(D('.009'), D('100'), D('0'))  # .9 USDC is under the 1 USDC minimum notional
    assert plan(state, view, **inputs(), bag=notional)[2] == 'campaign_flat'
    # A dust bag does not block a buy near its breakeven either.
    fresh = DecisionState(fingerprint='a' * 64, candle=1080)
    assert plan(fresh, {}, **inputs(), bag=dust)[2] == 'entry_proposed'


def test_legacy_mode_still_holds_on_held_inventory():
    state = DecisionState(fingerprint='a' * 64, candle=1080, claims=('held',), committed_quote=D('50'))
    view = {'held': _view('.5', '50', '100', True, held=True)}
    assert plan(state, view, **inputs())[2] == 'held_inventory_requires_reconciliation'
