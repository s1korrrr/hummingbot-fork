"""PR294 numerical/causal target fixtures. No economic or execution claims."""
from decimal import Decimal as D

import pytest

from hummingbot.strategy_v2.components.decision_protocol import DecisionScope, ObservationRef, Opportunity
from hummingbot.strategy_v2.portfolio_v3.allocation import propose_target
from hummingbot.strategy_v2.portfolio_v3.capital import admissible_cash, rebase_weights, weights
from hummingbot.strategy_v2.portfolio_v3.contracts import (
    AlphaProposal,
    AssetState,
    Commitment,
    P0Policy,
    PortfolioMandate,
    PortfolioSnapshot,
)

H = 'a' * 64
PAIRS = ('BNB-USDC', 'BTC-USDC')


def mandate(**changes):
    return PortfolioMandate(**{**dict(portfolio_id='meridian_v3', account_id='paper_only', version='test1',
                                      reference_quote=100, funding_envelope_quote=100, per_asset_quote=100, order_quote=20,
                                      gross_reference_ratio=1, gross_nav_ratio=1, cash_reserve_quote=0, fee_reserve_fraction='.003',
                                      pairs=PAIRS, loss_policy_sha256=H, max_age_ms=250, target_lifetime_ms=250), **changes})


def snapshot(m=None, **changes):
    m = m or mandate()
    assets = tuple(AssetState(symbol=s, quantity=0, basis_quote=0, net_cash_consumed_quote=0,
                              mark=10, bid=10, ask='10.01', lot='.001', minimum_quantity='.001', minimum_notional=1,
                              available_at_ms=1000, mark_event_ms=1000, revision=1, sellable_quantity=0,
                              minimum_sell_price=10, acquisition_allowed=True, reduction_allowed=False) for s in PAIRS)
    return PortfolioSnapshot(**{**dict(portfolio_id=m.portfolio_id, account_id=m.account_id,
                                       mandate_sha256=m.identity, sequence=1, cutoff_ms=1000, native_available_at_ms=1000,
                                       source_sha256=H, cash_total_quote=100, cash_available_quote=100, attributed_nav_quote=100,
                                       account_equity_quote=100, assets=assets, commitments=(), entry_universe=PAIRS,
                                       complete=True, reconciled=True, all_writers_accounted=True, risk_paused=False), **changes})


def proposal(symbol='BNB-USDC', action='acquire', producer='mr', **changes):
    scope = DecisionScope(program_id='meridian_v3', owner_id='meridian_v3', connector='okx',
                          trading_pair=symbol, recipe_sha256=H)
    ref = ObservationRef(dataset_sha256=H, epoch='e1', revision=1, content_sha256=H,
                         event_time_ms=900, available_time_ms=1000)
    opp = Opportunity(scope=scope, setup=producer, action=action, primary=ref, created_at_ms=1000, lifetime_ms=250)
    return AlphaProposal(**{**dict(opportunity=opp, producer=producer, producer_version='v1', reason='synthetic_eligibility',
                                   horizon_ms=60000, feature_closed_at_ms=1000, kind='ordinal', ordinal=1,
                                   calibration_sha256=None, training_cutoff_ms=None, latest_label_matured_ms=None), **changes})


def policy(**changes):
    return P0Policy(**{**dict(version='synthetic_priority_only', horizon_ms=60000,
                              pair_priority=PAIRS, producers=('mr', 'mom')), **changes})


def test_nav_is_not_reference_capital_and_rebase_preserves_inventory():
    m = mandate()
    s = snapshot(m)
    a = s.assets[0].model_copy(update={'quantity': D(2), 'basis_quote': D(17), 'sellable_quantity': D(2)})
    s = s.model_copy(update={'assets': (a, s.assets[1]), 'attributed_nav_quote': D(120)})
    assert weights(m, s)[0].reference_weight == D('.2')
    assert weights(m, s)[0].nav_weight == D(20) / D(120)
    out = rebase_weights(m, s, mandate(reference_quote=200, version='test2'), approval_sha256=H)
    assert out[0].reference_weight == D('.1')
    assert a.quantity == 2 and a.basis_quote == 17
    with pytest.raises(ValueError):
        rebase_weights(m, s, m, approval_sha256=H)


def test_canonical_hashes_and_unsupported_authority():
    m = mandate()
    assert m.identity == mandate(pairs=tuple(reversed(PAIRS))).identity
    assert snapshot(m).identity == snapshot(m, assets=tuple(reversed(snapshot(m).assets))).identity
    for changes in ({'venue': 'okx_perpetual'}, {'execution_enabled': True}, {'reference_quote': 'NaN'}, {'gross_nav_ratio': '1.1'}):
        with pytest.raises(ValueError):
            mandate(**changes)


def test_duplicate_obligations_rejected_and_hold_subtracted_once():
    hold = Commitment(native_id='o1', symbol='BNB-USDC', side='buy', remaining_quantity=3,
                      quote_authority=30, venue_encumbered_quote=30)
    s = snapshot(commitments=(hold,), cash_available_quote=70)
    assert admissible_cash(s) == 70
    with pytest.raises(ValueError):
        snapshot(commitments=(hold, hold))
    unreflected = hold.model_copy(update={'venue_encumbered_quote': D(0)})
    assert admissible_cash(snapshot(commitments=(unreflected,))) == 70


def test_future_stale_or_unreconciled_inputs_cannot_allocate():
    m = mandate()
    s = snapshot(m)
    for changed in (s.model_copy(update={'native_available_at_ms': 1001}),
                    s.model_copy(update={'cutoff_ms': 1251}), s.model_copy(update={'reconciled': False}),
                    s.model_copy(update={'all_writers_accounted': False})):
        t = propose_target(m, changed, (proposal(),), policy(), recipe_sha256=H, epoch='e1', fence=1)
        assert t.status == 'ENTRY_HOLD' and t.requested_acquisition_quote == 0
    with pytest.raises(ValueError):
        proposal(feature_closed_at_ms=1001)


def test_joint_initial_and_dca_compete_for_one_cash_pool_not_alpha_wallets():
    m = mandate()
    s = snapshot(m, cash_total_quote=25, cash_available_quote=25, attributed_nav_quote=25)
    a = s.assets[0].model_copy(update={'quantity': D(5), 'basis_quote': D(50), 'net_cash_consumed_quote': D(50)})
    s = s.model_copy(update={'assets': (a, s.assets[1]), 'attributed_nav_quote': D(75)})
    proposals = (proposal(), proposal('BTC-USDC'))
    t = propose_target(m, s, proposals, policy(), recipe_sha256=H, epoch='e1', fence=1)
    assert 19 < t.requested_acquisition_quote <= 20
    assert not t.execution_authorized
    assert len([x for x in t.quantities if x.change > 0]) == 1
    assert 'ACQUISITION_FULL_INCREMENT_UNAVAILABLE' in t.reasons
    reverse = propose_target(m, s, tuple(reversed(proposals)), policy(), recipe_sha256=H, epoch='e1', fence=1)
    assert reverse == t


def test_duplicate_alpha_does_not_double_spend_and_conflicts_hold_quantity():
    m = mandate()
    s = snapshot(m)
    one = propose_target(m, s, (proposal(),), policy(), recipe_sha256=H, epoch='e1', fence=1)
    two = propose_target(m, s, (proposal(), proposal(producer='mom')), policy(), recipe_sha256=H, epoch='e1', fence=1)
    assert one.quantities == two.quantities and one.requested_acquisition_quote == two.requested_acquisition_quote
    conflict = propose_target(m, s, (proposal(), proposal(action='reduce', producer='mom')), policy(), recipe_sha256=H, epoch='e1', fence=1)
    assert all(x.change == 0 for x in conflict.quantities)
    assert 'FORECAST_CONFLICT' in conflict.reasons


def test_underwater_reduction_cannot_finance_buy_or_orphan_management():
    m = mandate()
    s = snapshot(m, cash_total_quote=0, cash_available_quote=0, attributed_nav_quote=0, entry_universe=('BTC-USDC',))
    a = s.assets[0].model_copy(update={'quantity': D(5), 'basis_quote': D(60), 'net_cash_consumed_quote': D(60),
                                       'sellable_quantity': D(5), 'reduction_allowed': True, 'minimum_sell_price': D(12)})
    s = s.model_copy(update={'assets': (a, s.assets[1]), 'attributed_nav_quote': D(50)})
    t = propose_target(m, s, (proposal(action='reduce'), proposal('BTC-USDC')), policy(), recipe_sha256=H, epoch='e1', fence=1)
    assert t.requested_acquisition_quote == 0
    assert t.quantities[0].quantity == 5 and 'TARGET_BLOCKED_PROFIT_FLOOR' in t.reasons


def test_infeasible_book_and_realized_loss_do_not_invent_liquidation_or_capacity():
    m = mandate()
    s = snapshot(m)
    a = s.assets[0].model_copy(update={'quantity': D(11), 'basis_quote': D(100)})
    t = propose_target(m, s.model_copy(update={'assets': (a, s.assets[1]), 'attributed_nav_quote': D(210)}), (proposal(),), policy(), recipe_sha256=H, epoch='e1', fence=1)
    assert t.status == 'CURRENT_BOOK_INFEASIBLE' and t.quantities[0].quantity == 11
    a = s.assets[0].model_copy(update={'net_cash_consumed_quote': D(100)})
    t = propose_target(m, s.model_copy(update={'assets': (a, s.assets[1])}), (proposal(),), policy(), recipe_sha256=H, epoch='e1', fence=1)
    assert t.requested_acquisition_quote == 0


def test_minimum_is_never_rounded_up_and_ordinal_is_not_return_forecast():
    m = mandate()
    s = snapshot(m, cash_total_quote=1, cash_available_quote=1, attributed_nav_quote=1)
    t = propose_target(m, s, (proposal(),), policy(), recipe_sha256=H, epoch='e1', fence=1)
    assert t.requested_acquisition_quote == 0
    with pytest.raises(ValueError):
        proposal(kind='gross_simple_return', ordinal=None, gross_mean='.01')
    with pytest.raises(ValueError):
        propose_target(m, s, (proposal(horizon_ms=1),), policy(), recipe_sha256=H, epoch='e1', fence=1)


@pytest.mark.parametrize('changes', [{'complete': False}, {'all_writers_accounted': False},
                                     {'account_id': 'other'}, {'portfolio_id': 'other'}, {'native_available_at_ms': 1001}, {'cutoff_ms': 2000}])
def test_rebase_rejects_incomplete_foreign_or_stale_native_state(changes):
    m = mandate()
    s = snapshot(m).model_copy(update=changes)
    with pytest.raises(ValueError):
        rebase_weights(m, s, mandate(reference_quote=200, version='test2'), approval_sha256=H)


def test_inconsistent_valuation_cannot_create_gross_headroom():
    s = snapshot()
    with pytest.raises(ValueError):
        AssetState.model_validate(s.assets[0].model_dump() | {'mark': '.01'})
    with pytest.raises(ValueError):
        snapshot(attributed_nav_quote=400)


def test_entry_pause_keeps_profitable_reduction_and_ignores_pending_sale_proceeds():
    m = mandate()
    s = snapshot(m)
    a = s.assets[0].model_copy(update={'quantity': D(5), 'basis_quote': D(40), 'sellable_quantity': D(5), 'reduction_allowed': True})
    s = s.model_copy(update={'assets': (a, s.assets[1]), 'cash_total_quote': D(0), 'cash_available_quote': D(0),
                             'attributed_nav_quote': D(50), 'risk_paused': True})
    t = propose_target(m, s, (proposal(action='reduce'), proposal('BTC-USDC')), policy(), recipe_sha256=H, epoch='e1', fence=1)
    assert t.quantities[0].change == -2 and t.requested_acquisition_quote == 0
    assert t.status == 'ENTRY_HOLD'
    hold = Commitment(native_id='sell1', symbol='BNB-USDC', side='sell', remaining_quantity=2,
                      quote_authority=0, venue_encumbered_quote=0)
    t = propose_target(m, s.model_copy(update={'commitments': (hold,), 'risk_paused': False}),
                       (proposal(action='reduce'), proposal('BTC-USDC')), policy(), recipe_sha256=H, epoch='e1', fence=1)
    assert all(q.change == 0 for q in t.quantities)
    assert 'NATIVE_AUTHORITY_PENDING' in t.reasons
