from decimal import Decimal as D
from test.portfolio_v3.test_portfolio import mandate, proposal, snapshot

import pytest

from controllers.generic.meridian_v3_portfolio import (
    BNBMRFeatures,
    BNBMRObservation,
    MeridianPortfolioRecipe,
    bnb_mr_proposal,
)
from controllers.generic.meridian_v3_recipe import PAIRS
from hummingbot.strategy_v2.controllers.portfolio_target_store_v3 import NativeTargetStore, ProposalWriter
from hummingbot.strategy_v2.portfolio_v3.contracts import P0Policy


def observation(o, previous_rsi=28, rsi=30, atr_fraction='.005'):
    features = BNBMRFeatures(previous_rsi=previous_rsi, rsi=rsi, atr_fraction=atr_fraction)
    o = o.model_copy(update={'primary': o.primary.model_copy(update={'content_sha256': features.identity})})
    return BNBMRObservation(opportunity=o, features=features, candle_open_ms=940, feature_closed_at_ms=1000)


def composition():
    m = mandate(reference_quote=500, funding_envelope_quote=500, pairs=PAIRS)
    p = P0Policy(version='synthetic_priority_only', horizon_ms=60000, pair_priority=PAIRS, producers=('bnb_p23_mr',))
    return MeridianPortfolioRecipe(m, p)


def test_bnb_mr_through_five_asset_joint_target_to_native_recorder(native):
    recipe = composition()
    template = snapshot().assets[0]
    s = snapshot().model_copy(update={'mandate_sha256': recipe.mandate.identity,
                                      'assets': tuple(template.model_copy(update={'symbol': p}) for p in PAIRS),
                                      'entry_universe': PAIRS, 'cash_total_quote': D(500), 'cash_available_quote': D(500), 'attributed_nav_quote': D(500)})
    inputs = []
    for pair in PAIRS:
        o = proposal(symbol=pair, producer='bnb_p23_mr').opportunity
        o = o.model_copy(update={'scope': o.scope.model_copy(update={'recipe_sha256': recipe.recipe_sha256})})
        inputs.append(observation(o))
    t = recipe.propose(s, tuple(inputs), epoch='e1', fence=1)
    assert len(t.quantities) == 5 and all(q.change > 0 for q in t.quantities)
    assert 99 < t.requested_acquisition_quote <= 100
    assert sum(q.reference_weight for q in t.quantities) < D('.2')
    store = NativeTargetStore(native, ProposalWriter(portfolio_id='meridian_v3', account_id='paper_only', writer_id='test',
                                                     epoch='e1', fence=1, mandate_sha256=recipe.mandate.identity, recipe_sha256=recipe.recipe_sha256))
    receipt = store.publish(t, previous_sha256=None, now_ms=1000)
    assert store.read() == receipt and receipt['scope'] == 'RECORDER_LOCAL_PROPOSAL_ONLY'
    assert not receipt['execution_authorized']
    with pytest.raises(ValueError):
        recipe.propose(s, (proposal(),), epoch='e1', fence=1)


def test_mr_eligibility_is_not_an_invented_momentum_or_return_model():
    o = proposal(producer='bnb_p23_mr').opportunity
    rejected = bnb_mr_proposal(observation(o, previous_rsi=31), horizon_ms=60000)
    assert rejected.reason == 'no_rebound'
    assert bnb_mr_proposal(observation(o, atr_fraction=None), horizon_ms=60000).reason == 'atr_unavailable'
    assert bnb_mr_proposal(observation(o, atr_fraction='.00001'), horizon_ms=60000).reason == 'low_volatility'
    assert rejected.ordinal == 0 and rejected.gross_mean is None and rejected.opportunity.expected_return is None
    r = composition()
    with pytest.raises(ValueError):
        MeridianPortfolioRecipe(r.mandate, r.policy.model_copy(update={'producers': ('bnb_p23_mr', 'mom')}))
    with pytest.raises(ValueError):
        MeridianPortfolioRecipe(r.mandate.model_copy(update={'reference_quote': D(100)}), r.policy)
