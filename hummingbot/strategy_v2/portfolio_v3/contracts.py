"""PR294 r2 proposal contracts; values never grant execution or account authority."""
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import Field, StrictBool, field_validator, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import HASH, NAME, Opportunity
from hummingbot.strategy_v2.components.recipe_contract import digest


class Identified(PolicyModel):
    @property
    def identity(self):
        return digest(self.model_dump(mode='json'))


def unique_symbols(values):
    if not values or len(values) > 128 or len(set(values)) != len(values):
        raise ValueError('bounded nonempty unique spot symbols required')
    if any(not s.endswith('-USDC') or not s[:-5].isalnum() or not s.isupper() for s in values):
        raise ValueError('only explicit USDC spot symbols are supported')
    return tuple(sorted(values))


class PortfolioMandate(Identified):
    schema_version: Literal['portfolio_mandate_v3_1'] = 'portfolio_mandate_v3_1'
    portfolio_id: str = Field(pattern=NAME)
    account_id: str = Field(pattern=NAME)
    version: str = Field(pattern=NAME)
    venue: Literal['okx_eu_eea_spot'] = 'okx_eu_eea_spot'
    quote: Literal['USDC'] = 'USDC'
    mode: Literal['observation', 'shadow'] = 'observation'
    execution_enabled: Literal[False] = False
    capital_mode: Literal['fixed_envelope'] = 'fixed_envelope'
    reference_quote: Decimal = Field(gt=0)
    funding_envelope_quote: Decimal = Field(gt=0)
    per_asset_quote: Decimal = Field(gt=0)
    order_quote: Decimal = Field(gt=0)
    gross_reference_ratio: Decimal = Field(gt=0)
    gross_nav_ratio: Decimal = Field(gt=0, le=1)
    cash_reserve_quote: Decimal = Field(ge=0)
    fee_reserve_fraction: Decimal = Field(ge=0, lt=1)
    pairs: tuple[str, ...]
    loss_policy_sha256: str = Field(pattern=HASH)
    max_age_ms: int = Field(gt=0, le=86400000, strict=True)
    target_lifetime_ms: int = Field(gt=0, le=86400000, strict=True)

    @field_validator('pairs')
    @classmethod
    def pairs_valid(cls, values):
        return unique_symbols(values)

    @field_validator('execution_enabled', mode='before')
    @classmethod
    def no_grants(cls, value):
        if value is not False:
            raise ValueError('a portfolio proposal has no execution authority')
        return value

    @model_validator(mode='after')
    def bounded(self):
        if self.order_quote > self.per_asset_quote or self.per_asset_quote > self.funding_envelope_quote:
            raise ValueError('order/pair/funding limits are inconsistent')
        return self


class AssetState(Identified):
    symbol: str
    quantity: Decimal = Field(ge=0)
    basis_quote: Decimal = Field(ge=0)
    net_cash_consumed_quote: Decimal
    mark: Decimal = Field(gt=0)
    bid: Decimal = Field(gt=0)
    ask: Decimal = Field(gt=0)
    lot: Decimal = Field(gt=0)
    minimum_quantity: Decimal = Field(gt=0)
    minimum_notional: Decimal = Field(ge=0)
    fee_currency: Literal['USDC'] = 'USDC'
    mark_event_ms: int = Field(ge=0, strict=True)
    available_at_ms: int = Field(ge=0, strict=True)
    revision: int = Field(ge=1, strict=True)
    sellable_quantity: Decimal = Field(ge=0)
    minimum_sell_price: Decimal = Field(gt=0)
    acquisition_allowed: StrictBool
    reduction_allowed: StrictBool

    @model_validator(mode='after')
    def native_constraints(self):
        unique_symbols((self.symbol,))
        if not self.bid <= self.mark <= self.ask or self.sellable_quantity > self.quantity:
            raise ValueError('invalid native prices or sellable ownership')
        if self.available_at_ms < self.mark_event_ms:
            raise ValueError('mark availability precedes its event')
        return self


class Commitment(Identified):
    """Residual worst-case native authority, including children/replacements/fees.

    venue_encumbered_quote is the portion already deducted from cash_available.
    Cancellation requests and timeouts never remove these input obligations.
    """
    native_id: str = Field(pattern=NAME)
    symbol: str
    side: Literal['buy', 'sell']
    remaining_quantity: Decimal = Field(ge=0)
    quote_authority: Decimal = Field(ge=0)
    venue_encumbered_quote: Decimal = Field(ge=0)

    @model_validator(mode='after')
    def valid_hold(self):
        unique_symbols((self.symbol,))
        if self.venue_encumbered_quote > self.quote_authority:
            raise ValueError('venue hold exceeds residual native authority')
        if self.side == 'sell' and (self.quote_authority or self.venue_encumbered_quote):
            raise ValueError('future sale proceeds are not quote commitments or cash')
        return self


class PortfolioSnapshot(Identified):
    portfolio_id: str = Field(pattern=NAME)
    account_id: str = Field(pattern=NAME)
    mandate_sha256: str = Field(pattern=HASH)
    source_sha256: str = Field(pattern=HASH)
    sequence: int = Field(ge=0, strict=True)
    cutoff_ms: int = Field(ge=0, strict=True)
    native_available_at_ms: int = Field(ge=0, strict=True)
    cash_total_quote: Decimal = Field(ge=0)
    cash_available_quote: Decimal = Field(ge=0)
    attributed_nav_quote: Decimal = Field(ge=0)
    account_equity_quote: Decimal | None = Field(default=None, ge=0)
    soft_plans_quote: Decimal = Field(default=0, ge=0)
    assets: tuple[AssetState, ...] = Field(min_length=1, max_length=128)
    commitments: tuple[Commitment, ...] = Field(max_length=4096)
    entry_universe: tuple[str, ...] = Field(max_length=128)
    complete: StrictBool
    reconciled: StrictBool
    all_writers_accounted: StrictBool
    risk_paused: StrictBool

    @field_validator('assets')
    @classmethod
    def ordered_assets(cls, values):
        if len({x.symbol for x in values}) != len(values):
            raise ValueError('duplicate instrument state')
        return tuple(sorted(values, key=lambda a: a.symbol))

    @field_validator('commitments')
    @classmethod
    def ordered_commitments(cls, values):
        if len({x.native_id for x in values}) != len(values):
            raise ValueError('duplicate native obligations')
        return tuple(sorted(values, key=lambda a: a.native_id))

    @field_validator('entry_universe')
    @classmethod
    def ordered_universe(cls, values):
        return unique_symbols(values) if values else ()

    @model_validator(mode='after')
    def cash_and_management(self):
        if self.cash_available_quote > self.cash_total_quote:
            raise ValueError('available cash exceeds total cash')
        if self.attributed_nav_quote != self.cash_total_quote + sum(a.quantity * a.mark for a in self.assets):
            raise ValueError('complete spot NAV must equal attributed total cash plus marked owned inventory')
        symbols = {x.symbol for x in self.assets}
        if not set(self.entry_universe) <= symbols or any(c.symbol not in symbols for c in self.commitments):
            raise ValueError('universe or obligation lacks managed native state')
        if sum(c.venue_encumbered_quote for c in self.commitments) > self.cash_total_quote - self.cash_available_quote:
            raise ValueError('venue holds are not reflected in the declared balance convention')
        return self


class AlphaProposal(Identified):
    opportunity: Opportunity
    producer: str = Field(pattern=NAME)
    producer_version: str = Field(pattern=NAME)
    reason: str = Field(pattern=NAME)
    horizon_ms: int = Field(gt=0, le=86400000, strict=True)
    feature_closed_at_ms: int = Field(ge=0, strict=True)
    kind: Literal['ordinal', 'gross_simple_return']
    ordinal: Decimal | None = Field(default=None, ge=0, le=1)
    gross_mean: Decimal | None = None
    mean_uncertainty: Decimal | None = Field(default=None, ge=0)
    calibration_sha256: str | None = Field(default=None, pattern=HASH)
    training_cutoff_ms: int | None = Field(default=None, ge=0, strict=True)
    latest_label_matured_ms: int | None = Field(default=None, ge=0, strict=True)

    @model_validator(mode='after')
    def causal_units(self):
        primary = self.opportunity.primary
        if not primary.event_time_ms <= self.feature_closed_at_ms <= primary.available_time_ms:
            raise ValueError('unconfirmed or unavailable feature close')
        if self.kind == 'ordinal':
            if self.ordinal is None or self.gross_mean is not None or self.opportunity.expected_return is not None:
                raise ValueError('ordinal eligibility is not a return forecast')
        else:
            if (self.ordinal is not None or self.gross_mean is None or self.mean_uncertainty is None
                    or self.calibration_sha256 is None or self.training_cutoff_ms is None
                    or self.latest_label_matured_ms is None):
                raise ValueError('return forecast requires calibrated common-horizon lineage')
            if not self.latest_label_matured_ms <= self.training_cutoff_ms <= primary.available_time_ms:
                raise ValueError('training labels or cutoff were not available')
            if self.opportunity.expected_return != self.gross_mean:
                raise ValueError('forecast and opportunity return disagree')
        return self


class P0Policy(Identified):
    """Explicit ordinal comparator policy, never an economically learned ranking.

    Conflicting producer directions retain inventory; magnitudes are not added.
    Positive same-direction suggestions authorize at most one proposed increment.
    The caller must freeze priority; no instrument ranking is a hidden default.
    """
    version: str = Field(pattern=NAME)
    horizon_ms: int = Field(gt=0, le=86400000, strict=True)
    pair_priority: tuple[str, ...]
    producers: tuple[str, ...] = Field(min_length=1, max_length=32)
    conflict_policy: Literal['retain_on_disagreement'] = 'retain_on_disagreement'
    ranking: Literal['explicit_pair_priority'] = 'explicit_pair_priority'

    @model_validator(mode='after')
    def ordered_policy(self):
        unique_symbols(self.pair_priority)  # Validate without sorting an intentional priority.
        if len(set(self.producers)) != len(self.producers):
            raise ValueError('duplicate producer identities')
        return self


class TargetQuantity(Identified):
    symbol: str = Field(pattern=r'^[A-Z0-9]+-USDC$', max_length=80)
    quantity: Decimal = Field(ge=0)
    change: Decimal
    reference_weight: Decimal = Field(ge=0)
    nav_weight: Decimal | None = Field(default=None, ge=0)


class TargetPortfolio(Identified):
    schema_version: Literal['target_portfolio_v3_1'] = 'target_portfolio_v3_1'
    portfolio_id: str = Field(pattern=NAME)
    recipe_sha256: str = Field(pattern=HASH)
    mandate_sha256: str = Field(pattern=HASH)
    policy_sha256: str = Field(pattern=HASH)
    snapshot_sha256: str = Field(pattern=HASH)
    forecast_sha256: str = Field(pattern=HASH)
    sequence: int = Field(ge=0, strict=True)
    epoch: str = Field(pattern=NAME)
    fence: int = Field(ge=1, strict=True)
    decision_time_ms: int = Field(ge=0, strict=True)
    expires_at_ms: int = Field(ge=0, strict=True)
    quantities: tuple[TargetQuantity, ...] = Field(min_length=1, max_length=128)
    requested_acquisition_quote: Decimal = Field(ge=0, description='Requested fee-inclusive ceiling, not a grant')
    status: Literal['PROPOSAL_ONLY', 'ENTRY_HOLD', 'CURRENT_BOOK_INFEASIBLE']
    reasons: tuple[Annotated[str, Field(pattern=NAME)], ...] = Field(max_length=128)
    producer_reasons: tuple[Annotated[str, Field(max_length=400)], ...] = Field(max_length=4096)
    execution_authorized: Literal[False] = False

    @field_validator('execution_authorized', mode='before')
    @classmethod
    def no_grants(cls, value):
        if value is not False:
            raise ValueError('targets never grant execution authority')
        return value

    @model_validator(mode='after')
    def bounded_lifetime(self):
        if self.expires_at_ms <= self.decision_time_ms:
            raise ValueError('target expiry must follow its decision cut')
        if len({q.symbol for q in self.quantities}) != len(self.quantities):
            raise ValueError('duplicate target instrument')
        if any(q.quantity - q.change < 0 for q in self.quantities):
            raise ValueError('target implies negative pre-decision spot inventory')
        if any(q.change > 0 for q in self.quantities) != (self.requested_acquisition_quote > 0):
            raise ValueError('positive acquisitions require a positive requested quote ceiling')
        return self
