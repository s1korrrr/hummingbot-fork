"""Recheck an admitted claim against current native evidence before any order effect.

This module never imports modular_spot_core or compiled network iterators. Peak
price and cost basis are caller-owned and are returned unchanged.
"""
from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.exit_policy import (
    ExitProposal,
    NativeExitView,
    NativeProfitFloor,
    arbitrate_exit,
)

from .admission import ExitAAdmissionStore, ExitAClaimV2, request_cancel
from .recipe import FORBIDDEN


class RestoreIdentity(PolicyModel):
    owner_id: str = Field(pattern=r'^[A-Za-z0-9_.:-]{1,128}$')
    recipe_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    source_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    model_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    full_configuration_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    @model_validator(mode='after')
    def isolated(self):
        if self.owner_id == 'r11' or any(token in self.owner_id for token in FORBIDDEN):
            raise ValueError('restore cannot inherit a legacy or compatibility identity')
        return self


class DispatchRequest(PolicyModel):
    claim_id: str
    connector: str
    trading_pair: str
    side: Literal['SELL'] = 'SELL'
    order_type: Literal['LIMIT'] = 'LIMIT'
    amount_base: Decimal = Field(gt=0)
    limit_price: Decimal = Field(gt=0)
    receipt_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    inventory_revision: int = Field(ge=1, strict=True)
    execution_authorized: Literal[False] = False
    market_fallback: Literal[False] = False


class DispatchOutcome(PolicyModel):
    status: Literal['HOLD', 'DISPATCHED', 'CANCEL_RACE']
    reason: str
    claim: ExitAClaimV2
    request: DispatchRequest | None = None
    peak_price: Decimal = Field(gt=0)
    cost_basis: Decimal = Field(gt=0)
    inventory_released: Literal[False] = False

    @model_validator(mode='after')
    def coherent(self):
        if self.claim.inventory_released:
            raise ValueError('optional dispatch cannot release inventory')
        if self.status == 'DISPATCHED' and (self.request is None or self.reason != 'placed'):
            raise ValueError('dispatch requires a native limit request')
        if self.status != 'DISPATCHED' and self.reason == 'placed':
            raise ValueError('placed reason requires DISPATCHED')
        return self


class RestoreResult(PolicyModel):
    claims: tuple[ExitAClaimV2, ...]
    peak_price: Decimal = Field(gt=0)
    cost_basis: Decimal = Field(gt=0)
    identity: RestoreIdentity


def restore(store: ExitAAdmissionStore, identity: RestoreIdentity, *,
            peak_price: Decimal, cost_basis: Decimal) -> RestoreResult:
    identity = RestoreIdentity.model_validate(identity.model_dump(mode='json'))
    if store.owner_id != identity.owner_id:
        raise PolicyError('restore identity mismatch')
    loaded = store._parse(store._load_raw())
    claims = tuple(loaded[key] for key in sorted(loaded))
    return RestoreResult(claims=claims, peak_price=peak_price, cost_basis=cost_basis, identity=identity)


def dispatch(claim: ExitAClaimV2, proposal: ExitProposal, view: NativeExitView,
             floor: NativeProfitFloor | None, store: ExitAAdmissionStore, *,
             now_ms: int, max_age_ms: int, inventory_revision: int,
             peak_price: Decimal, cost_basis: Decimal,
             native_close_pending: bool = False, entry_paused: bool = False,
             place=None) -> DispatchOutcome:
    del entry_paused
    current = store.load(claim.claim_id)
    if current is None or current.claim_id != claim.claim_id:
        raise PolicyError('dispatch requires the persisted claim')
    proposal = ExitProposal.model_validate(proposal.model_dump(mode='json'))
    view = NativeExitView.model_validate(view.model_dump(mode='json'))
    if proposal.receipt_sha256 != current.receipt_sha256:
        raise PolicyError('proposal receipt does not match the admitted claim')

    def done(status, reason, request=None, persisted=None):
        return DispatchOutcome(
            status=status, reason=reason, claim=persisted or current, request=request,
            peak_price=peak_price, cost_basis=cost_basis)

    if current.state != 'ADMITTED':
        return done('HOLD', 'claim_not_admitted')
    if native_close_pending or view.receipt_sha256 != current.receipt_sha256:
        return done('CANCEL_RACE', 'protective_close_or_receipt_changed',
                    persisted=request_cancel(current, store))
    assessment = arbitrate_exit(view, (proposal,), floor, now_ms=now_ms, max_age_ms=max_age_ms)
    if assessment.status != 'PROPOSED_REDUCTION':
        return done('HOLD', assessment.reason)
    request = DispatchRequest(
        claim_id=current.claim_id, connector=view.connector, trading_pair=view.trading_pair,
        amount_base=assessment.amount_base, limit_price=assessment.limit_price,
        receipt_sha256=view.receipt_sha256, inventory_revision=inventory_revision)
    if place is None:
        return done('HOLD', 'place_callback_required', request=None)
    try:
        place(request)
    except Exception:
        return done('HOLD', 'submission_uncertain')
    if store.load(current.claim_id) != current:
        raise PolicyError('dispatch mutated the admitted claim')
    return done('DISPATCHED', 'placed', request=request)
