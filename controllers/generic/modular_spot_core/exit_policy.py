"""Pure exit arbitration over native evidence; not an exit engine or a cost ledger."""
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
from typing import Literal

from pydantic import Field, StrictBool

from .config import PolicyModel
from .decision_protocol import HASH, NAME
from .domain import PolicyError
from .recipe_contract import digest


class NativeExitView(PolicyModel):
    owner_id: str = Field(pattern=NAME)
    executor_id: str = Field(pattern=NAME)
    connector: str = Field(pattern=r'^[a-z0-9_]{1,80}$')
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$', max_length=80)
    receipt_sha256: str = Field(pattern=HASH)
    observed_at_ms: int = Field(ge=0, strict=True)
    reconciled: StrictBool
    fees_complete: StrictBool
    remaining_base: Decimal = Field(ge=0)
    reserved_base: Decimal = Field(ge=0)
    base_fee_fraction: Decimal = Field(ge=0, lt=1)
    amount_step: Decimal = Field(gt=0)
    price_step: Decimal = Field(gt=0)
    minimum_amount: Decimal = Field(gt=0)
    minimum_notional: Decimal = Field(ge=0)
    bid: Decimal = Field(gt=0)


class ExitProposal(PolicyModel):
    policy_id: str = Field(pattern=NAME)
    owner_id: str = Field(pattern=NAME)
    executor_id: str = Field(pattern=NAME)
    receipt_sha256: str = Field(pattern=HASH)
    requested_base: Decimal = Field(gt=0)
    minimum_price: Decimal = Field(ge=0)
    expires_at_ms: int = Field(ge=0, strict=True)


class NativeProfitFloor(PolicyModel):
    owner_id: str = Field(pattern=NAME)
    executor_id: str = Field(pattern=NAME)
    receipt_sha256: str = Field(pattern=HASH)
    quantity_base: Decimal = Field(gt=0)
    price: Decimal = Field(gt=0)
    observed_at_ms: int = Field(ge=0, strict=True)


class ExitAssessment(PolicyModel):
    status: Literal['HOLD', 'PROPOSED_REDUCTION']
    reason: str
    input_sha256: str = Field(pattern=HASH)
    amount_base: Decimal | None = Field(default=None, gt=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    policy_ids: tuple[str, ...] = ()
    market_fallback: Literal[False] = False
    execution_authorized: Literal[False] = False


def arbitrate_exit(view: NativeExitView, proposals: tuple[ExitProposal, ...], floor: NativeProfitFloor | None,
                   *, now_ms: int, max_age_ms: int) -> ExitAssessment:
    view = NativeExitView.model_validate(view.model_dump(mode='json'))
    if (type(now_ms) is not int or now_ms < 0 or type(max_age_ms) is not int
            or not 1 <= max_age_ms <= 60000 or len(proposals) > 64):
        raise PolicyError('invalid exit clock or bounded proposal capacity')
    proposals = tuple(ExitProposal.model_validate(item.model_dump(mode='json')) for item in proposals)
    if len({item.policy_id for item in proposals}) != len(proposals):
        raise PolicyError('duplicate exit policy identity')
    proposals = tuple(sorted(proposals, key=lambda item: item.policy_id))
    if floor is not None:
        floor = NativeProfitFloor.model_validate(floor.model_dump(mode='json'))
    input_hash = digest([view, proposals, floor, now_ms, max_age_ms])
    def hold(reason):
        return ExitAssessment(status='HOLD', reason=reason, input_sha256=input_hash)
    if not 0 <= now_ms - view.observed_at_ms <= max_age_ms:
        return hold('native_snapshot_stale')
    if not view.reconciled:
        return hold('native_reconciliation_required')
    if not view.fees_complete:
        return hold('native_fees_incomplete')
    if view.reserved_base > view.remaining_base:
        return hold('native_quantity_conflict')
    uncommitted = view.remaining_base - view.reserved_base
    if uncommitted == 0:
        return hold('no_uncommitted_inventory')
    scope = (view.owner_id, view.executor_id, view.receipt_sha256)
    if any((item.owner_id, item.executor_id, item.receipt_sha256) != scope for item in proposals):
        return hold('exit_scope_conflict')
    current = tuple(item for item in proposals if item.expires_at_ms > now_ms)
    if not current:
        return hold('no_current_exit_proposal')
    with localcontext() as context:
        context.prec = 60
        # Requests overlap on the SAME attributed lot: choose their maximum, never their sum.
        requested = min(max(item.requested_base for item in current), uncommitted / (1 + view.base_fee_fraction))
        quantity = (requested / view.amount_step).to_integral_value(rounding=ROUND_FLOOR) * view.amount_step
    if quantity <= 0 or quantity < view.minimum_amount:
        return hold('native_quantity_below_minimum')
    if floor is None:
        return hold('native_floor_unavailable')
    if (floor.owner_id, floor.executor_id, floor.receipt_sha256) != scope:
        return hold('native_floor_scope_conflict')
    if not 0 <= now_ms - floor.observed_at_ms <= max_age_ms:
        return hold('native_floor_stale')
    # Flat fees and quantity-sensitive native costs cannot be inferred for a smaller sale.
    if floor.quantity_base != quantity:
        return hold('native_floor_quantity_mismatch')
    with localcontext() as context:
        context.prec = 60
        protected = max(floor.price, *(item.minimum_price for item in current))
        price = (protected / view.price_step).to_integral_value(rounding=ROUND_CEILING) * view.price_step
        notional = quantity * price
    if notional < view.minimum_notional:
        return hold('native_notional_below_minimum')
    if view.bid < price:
        return hold('bid_below_profit_floor')
    return ExitAssessment(status='PROPOSED_REDUCTION', reason='native_price_bounded_reduction',
        input_sha256=input_hash, amount_base=quantity, limit_price=price,
        policy_ids=tuple(item.policy_id for item in current))
