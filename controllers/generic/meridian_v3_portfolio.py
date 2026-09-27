"""Meridian unified P0 composition, available for explicit observation replay.

Not an executable ControllerBase adapter. A production native snapshot/funding
adapter and qualified MR/MOM models are still required. BNB p23 contributes
binary MR eligibility only; no RSI-to-return conversion or hidden MOM model.
"""
from decimal import Decimal
from pathlib import Path

from pydantic import Field, model_validator

from controllers.generic import meridian_v3_recipe, modular_spot
from controllers.generic.modular_spot_core.signals import signal_decision
from hummingbot.strategy_v2.components.decision_protocol import Opportunity
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.portfolio_v3 import allocation, capital, contracts
from hummingbot.strategy_v2.portfolio_v3.allocation import propose_target
from hummingbot.strategy_v2.portfolio_v3.contracts import AlphaProposal, Identified, P0Policy, PortfolioMandate

RECIPE_ID = 'meridian_v3_unified_p0_v1'


def source_identity():
    """Additive closure; never changes legacy component source selection."""
    import hashlib

    from hummingbot.strategy_v2 import portfolio_v3
    from hummingbot.strategy_v2.components.source_identity import checked_source, component_source_files
    from hummingbot.strategy_v2.controllers import portfolio_target_store_v3
    root = Path(__file__).resolve().parents[2]
    paths = {Path(__file__).resolve(), Path(meridian_v3_recipe.__file__).resolve(),
             Path(allocation.__file__).resolve(), Path(capital.__file__).resolve(), Path(contracts.__file__).resolve(),
             Path(portfolio_v3.__file__).resolve(), Path(portfolio_target_store_v3.__file__).resolve(),
             *[p.resolve() for p in modular_spot.source_files()],
             *[p.resolve() for p in component_source_files()]}
    return digest({str(p.relative_to(root)): hashlib.sha256(checked_source(p)).hexdigest() for p in sorted(paths)})


class BNBMRFeatures(Identified):
    previous_rsi: Decimal = Field(ge=0, le=100)
    rsi: Decimal = Field(ge=0, le=100)
    atr_fraction: Decimal | None = Field(ge=0)


class BNBMRObservation(Identified):
    opportunity: Opportunity
    features: BNBMRFeatures
    feature_closed_at_ms: int = Field(ge=0, strict=True)

    @model_validator(mode='after')
    def feature_binding(self):
        o = self.opportunity
        if (o.action != 'acquire' or o.setup != 'bnb_p23_mr' or o.primary.content_sha256 != self.features.identity
                or not o.primary.event_time_ms <= self.feature_closed_at_ms <= o.primary.available_time_ms):
            raise ValueError('confirmed MR feature content and observation binding required')
        return self


def bnb_mr_proposal(observation: BNBMRObservation, *, horizon_ms: int):
    """Typed feature receipt, not a raw score; no inferred return forecast."""
    observation = BNBMRObservation.model_validate(observation.model_dump(mode='json'))
    result = signal_decision(meridian_v3_recipe.strategy_parameters()['signal'],
                             previous_rsi=observation.features.previous_rsi, rsi=observation.features.rsi,
                             atr_fraction=observation.features.atr_fraction, breakout=False, trend_up=False)
    return AlphaProposal(opportunity=observation.opportunity, producer='bnb_p23_mr', producer_version='p23_v1',
                         reason=result.reason, horizon_ms=horizon_ms, feature_closed_at_ms=observation.feature_closed_at_ms,
                         kind='ordinal', ordinal=1 if result.enter else 0)


class MeridianPortfolioRecipe:
    """One versioned graph for all five pairs, with explicit frozen P0 policy."""

    def __init__(self, mandate: PortfolioMandate, policy: P0Policy):
        self.mandate = PortfolioMandate.model_validate(mandate.model_dump(mode='json'))
        self.policy = P0Policy.model_validate(policy.model_dump(mode='json'))
        if (mandate.portfolio_id != 'meridian_v3' or set(mandate.pairs) != set(meridian_v3_recipe.PAIRS)
                or mandate.order_quote != 20 or mandate.per_asset_quote != 100
                or mandate.funding_envelope_quote != 500 or mandate.reference_quote != 500):
            raise ValueError('Meridian requires its explicit five pairs and 20/100/500 mandate')
        if set(policy.pair_priority) != set(mandate.pairs) or policy.producers != ('bnb_p23_mr',):
            raise ValueError('explicit five-pair priority and supported BNB MR producer required; MOM is not qualified')
        self.recipe_sha256 = digest(dict(recipe_id=RECIPE_ID, source_sha256=source_identity(),
                                         mandate_sha256=self.mandate.identity, policy_sha256=self.policy.identity,
                                         bnb_candidate_sha256=meridian_v3_recipe.CANDIDATE_SHA256))

    def propose(self, snapshot, observations, *, epoch, fence):
        if len(observations) > 5 or any(type(o) is not BNBMRObservation for o in observations):
            raise ValueError('Meridian requires bounded typed BNB MR observations')
        if any(o.opportunity.scope.recipe_sha256 != self.recipe_sha256 for o in observations):
            raise ValueError('forecast composition differs from the frozen Meridian recipe')
        proposals = tuple(bnb_mr_proposal(o, horizon_ms=self.policy.horizon_ms) for o in observations)
        return propose_target(self.mandate, snapshot, proposals, self.policy,
                              recipe_sha256=self.recipe_sha256, epoch=epoch, fence=fence)
