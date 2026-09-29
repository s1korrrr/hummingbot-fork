"""Meridian unified P0 composition, available for explicit observation replay.

Not an executable ControllerBase adapter. A production native snapshot/funding
adapter and qualified MR/MOM models are still required. BNB p23 contributes
binary MR eligibility only; no RSI-to-return conversion or hidden MOM model.
"""
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import NamedTuple

from pydantic import Field, model_validator

from controllers.generic import meridian_v3_recipe, modular_spot
from controllers.generic.modular_spot_core.signals import signal_decision
from hummingbot.strategy_v2.components.decision_protocol import Opportunity
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.portfolio_v3 import allocation, capital, contracts
from hummingbot.strategy_v2.portfolio_v3.allocation import propose_target
from hummingbot.strategy_v2.portfolio_v3.contracts import AlphaProposal, Identified, P0Policy, PortfolioMandate

RECIPE_ID = 'meridian_v3_unified_p0_v1'


class CapitalStage(NamedTuple):
    envelope_quote: Decimal
    per_asset_quote: Decimal
    order_quote: Decimal


# The only admitted capital scales. Canary keeps the full per-order and per-asset sizes,
# so fills and one asset's DCA stages behave identically; only the shared envelope, for
# which the five assets compete in P0 priority, is smaller. No other numbers exist.
CAPITAL_STAGES = MappingProxyType({
    'canary': CapitalStage(Decimal('100'), Decimal('100'), Decimal('20')),
    'full': CapitalStage(Decimal('500'), Decimal('100'), Decimal('20')),
})


def portfolio_source_files():
    """The explicit source closure of the Meridian portfolio decision and its native stores.

    Every module whose code decides, proposes, admits or persists a Meridian portfolio
    target or grant: this module, the frozen p23 recipe, each portfolio_v3 contract and
    allocation module, the native grant bridge, the native target store and the p23
    symbol runtime. Additive: it never changes the legacy component source selection.
    """
    from hummingbot.strategy_v2 import portfolio_v3
    from hummingbot.strategy_v2.controllers import native_portfolio_bridge_v3, portfolio_target_store_v3
    from hummingbot.strategy_v2.portfolio_v3 import runtime_contracts_v1
    modules = (portfolio_v3, allocation, capital, contracts, runtime_contracts_v1,
               native_portfolio_bridge_v3, portfolio_target_store_v3)
    return [Path(__file__), Path(meridian_v3_recipe.__file__), *[Path(m.__file__) for m in modules],
            *modular_spot.source_files()]


def source_identity():
    """Digest of the portfolio closure plus the neutral component sources it imports."""
    import hashlib

    from hummingbot.strategy_v2.components.source_identity import checked_source, component_source_files
    root = Path(__file__).resolve().parents[2]
    paths = {*[p.resolve() for p in portfolio_source_files()], *[p.resolve() for p in component_source_files()]}
    return digest({str(p.relative_to(root)): hashlib.sha256(checked_source(p)).hexdigest() for p in sorted(paths)})


class BNBMRFeatures(Identified):
    previous_rsi: Decimal = Field(ge=0, le=100)
    rsi: Decimal = Field(ge=0, le=100)
    atr_fraction: Decimal | None = Field(ge=0)


class BNBMRObservation(Identified):
    """Features of one closed candle. `candle_open_ms` is the candle's open time and
    `feature_closed_at_ms` its close, when the features became final; the primary
    observation's event is that close and its availability is when the owner read it."""
    opportunity: Opportunity
    features: BNBMRFeatures
    candle_open_ms: int = Field(ge=0, strict=True)
    feature_closed_at_ms: int = Field(ge=0, strict=True)

    @model_validator(mode='after')
    def feature_binding(self):
        o = self.opportunity
        if (o.action != 'acquire' or o.setup != 'bnb_p23_mr' or o.primary.content_sha256 != self.features.identity
                or not self.candle_open_ms < self.feature_closed_at_ms
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
        stage = (mandate.funding_envelope_quote, mandate.per_asset_quote, mandate.order_quote)
        if (mandate.portfolio_id != 'meridian_v3' or set(mandate.pairs) != set(meridian_v3_recipe.PAIRS)
                or stage not in {tuple(s) for s in CAPITAL_STAGES.values()}
                or mandate.reference_quote != mandate.funding_envelope_quote):
            raise ValueError('Meridian requires its explicit five pairs and a canary 20/100/100 '
                             'or full 20/100/500 mandate')
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
