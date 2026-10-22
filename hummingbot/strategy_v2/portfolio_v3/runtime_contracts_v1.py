"""Strategy-neutral runtime contracts for one portfolio owner. No I/O or orders.

A grant is native-owned admission for one bounded acquisition increment. It is
separate from the non-authorizing target: the target proposes, the native grant
store admits, the native executor spends. None of these types moves real cash.
"""
from decimal import Decimal
from typing import Literal

from pydantic import Field, StrictBool, field_validator, model_validator

from hummingbot.strategy_v2.components.decision_protocol import HASH, NAME

from .contracts import Identified, unique_symbols

# Existing P0 reasons are reused unchanged; these are the runtime adapter reasons.
FEATURE_GAP_REBUILD_REQUIRED = 'FEATURE_GAP_REBUILD_REQUIRED'
SETUP_ALREADY_CONSUMED = 'SETUP_ALREADY_CONSUMED'
MANAGEMENT_OBLIGATION_RETAINED = 'MANAGEMENT_OBLIGATION_RETAINED'
CAPITAL_DOMAIN_UNPROVEN = 'CAPITAL_DOMAIN_UNPROVEN'
NATIVE_GRANT_REQUIRED = 'NATIVE_GRANT_REQUIRED'
EXECUTION_CAPABILITY_UNBOUND = 'EXECUTION_CAPABILITY_UNBOUND'
NATIVE_SNAPSHOT_UNAVAILABLE = 'NATIVE_SNAPSHOT_UNAVAILABLE'
NATIVE_INVENTORY_MISMATCH = 'NATIVE_INVENTORY_MISMATCH'
GRANT_STORE_HOLD = 'GRANT_STORE_HOLD'


class LiveCapitalBinding(Identified):
    """Operator-approved bounded live spending on a wallet other bots also use.

    The portfolio spends at most `envelope_quote`, admits at most its per-asset and
    per-order limits, and sells only inventory its own executors bought. It cannot
    stop another process from spending the same balance first; its own buys then
    fail at the native funds check instead of overspending.
    """
    mode: Literal['shared_wallet_bounded']
    account_id: str = Field(pattern=NAME)
    connector: Literal['okx']
    envelope_quote: Decimal = Field(gt=0)
    shared_wallet_acknowledged: Literal[True]
    approved_by: str = Field(pattern=NAME)


class GrantPolicy(Identified):
    """The durable admission scope. Source/code upgrades do not change it."""
    schema_version: Literal['portfolio_grant_policy_v1'] = 'portfolio_grant_policy_v1'
    portfolio_id: str = Field(pattern=NAME)
    account_id: str = Field(pattern=NAME)
    controller_id: str = Field(pattern=NAME)
    connector: str = Field(pattern=NAME)
    quote: Literal['USDC'] = 'USDC'
    mandate_sha256: str = Field(pattern=HASH)
    policy_sha256: str = Field(pattern=HASH)
    pairs: tuple[str, ...]
    envelope_quote: Decimal = Field(gt=0)
    per_asset_quote: Decimal = Field(gt=0)
    order_quote: Decimal = Field(gt=0)

    @field_validator('pairs')
    @classmethod
    def pairs_valid(cls, values):
        return unique_symbols(values)

    @model_validator(mode='after')
    def bounded(self):
        if self.order_quote > self.per_asset_quote or self.per_asset_quote > self.envelope_quote:
            raise ValueError('grant order/asset/envelope limits are inconsistent')
        return self


class PortfolioGrant(Identified):
    """Lifecycle: ADMITTED (reserved, unsent) -> BOUND (owned by one native executor).

    An ADMITTED grant with no executor may lapse at its submission deadline because
    nothing can have been sent. A BOUND grant is released only by native terminal
    evidence or an explicit held-inventory release; never by time or acknowledgement.
    """
    grant_id: str = Field(pattern=HASH)
    symbol: str
    setup_id: str = Field(pattern=NAME)
    target_sha256: str = Field(pattern=HASH)
    epoch: str = Field(pattern=NAME)
    fence: int = Field(ge=1, strict=True)
    ceiling_quote: Decimal = Field(gt=0)
    admitted_at_ms: int = Field(ge=0, strict=True)
    submit_deadline_ms: int = Field(ge=0, strict=True)
    status: Literal['ADMITTED', 'BOUND']
    executor_id: str | None = Field(default=None, min_length=1, max_length=128)
    bound_quote: Decimal | None = Field(default=None, gt=0)

    @model_validator(mode='after')
    def lifecycle(self):
        unique_symbols((self.symbol,))
        if self.submit_deadline_ms <= self.admitted_at_ms:
            raise ValueError('grant submission deadline must follow admission')
        bound = self.executor_id is not None, self.bound_quote is not None
        if (self.status == 'BOUND') != all(bound) or any(bound) != all(bound):
            raise ValueError('only a bound grant names its native executor and quote')
        if self.bound_quote is not None and self.bound_quote > self.ceiling_quote:
            raise ValueError('bound quote exceeds the admitted ceiling')
        return self

    @property
    def reserved_quote(self):
        return self.bound_quote if self.status == 'BOUND' else self.ceiling_quote


class AdmissionOutcome(Identified):
    target_sha256: str = Field(pattern=HASH)
    admitted: tuple[str, ...] = Field(max_length=128)
    denied: tuple[str, ...] = Field(max_length=128)
    reasons: tuple[str, ...] = Field(max_length=128)
    execution_capable: StrictBool
