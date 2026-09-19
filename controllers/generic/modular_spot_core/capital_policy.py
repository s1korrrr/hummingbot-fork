"""Read-only capital proposals. Atomic spending permission remains a native owner concern."""
from decimal import ROUND_FLOOR, Decimal, localcontext
from typing import Literal

from pydantic import Field, StrictBool, model_validator

from .config import PolicyModel
from .decision_protocol import HASH, NAME
from .domain import PolicyError, decimal
from .portfolio import allocate
from .recipe_contract import digest


class NativeExposure(PolicyModel):
    executor_id: str = Field(pattern=NAME)
    program_id: str = Field(pattern=NAME)
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$', max_length=80)
    marked_inventory_quote: Decimal | None = Field(ge=0)
    reserved_acquisition_quote: Decimal | None = Field(ge=0)


class CapitalSnapshot(PolicyModel):
    account_id: str = Field(pattern=NAME)
    valuation_currency: str = Field(pattern=r'^[A-Z0-9]{1,16}$')
    observed_at_ms: int = Field(ge=0, strict=True)
    source_sha256: str = Field(pattern=HASH)
    valuation_sha256: str = Field(pattern=HASH)
    complete: StrictBool
    reconciled: StrictBool
    risk_paused: StrictBool
    available_quote: Decimal | None = Field(ge=0)
    exposures: tuple[NativeExposure, ...] = Field(max_length=4096)

    @model_validator(mode='after')
    def unique_native_obligations(self):
        if len({item.executor_id for item in self.exposures}) != len(self.exposures):
            raise ValueError('duplicate native execution obligations')
        return self


class CapitalRequest(PolicyModel):
    account_id: str = Field(pattern=NAME)
    program_id: str = Field(pattern=NAME)
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$', max_length=80)
    valuation_currency: str = Field(pattern=r'^[A-Z0-9]{1,16}$')
    requested_quote: Decimal = Field(gt=0)


class CapitalLimits(PolicyModel):
    account_quote: Decimal = Field(gt=0)
    program_quote: Decimal = Field(gt=0)
    base_quote: Decimal = Field(gt=0)
    order_quote: Decimal = Field(gt=0)
    max_age_ms: int = Field(ge=1, le=60000, strict=True)


class CapitalAssessment(PolicyModel):
    status: Literal['HOLD', 'ELIGIBLE_FOR_NATIVE_ADMISSION']
    reason: str
    input_sha256: str = Field(pattern=HASH)
    projected_account_quote: Decimal | None = Field(default=None, ge=0)
    projected_program_quote: Decimal | None = Field(default=None, ge=0)
    projected_base_quote: Decimal | None = Field(default=None, ge=0)
    execution_authorized: Literal[False] = False


def assess_acquisition(request: CapitalRequest, snapshot: CapitalSnapshot, limits: CapitalLimits, *, now_ms: int):
    request = CapitalRequest.model_validate(request.model_dump(mode='json'))
    snapshot = CapitalSnapshot.model_validate(snapshot.model_dump(mode='json'))
    limits = CapitalLimits.model_validate(limits.model_dump(mode='json'))
    if type(now_ms) is not int or now_ms < 0:
        raise PolicyError('invalid observation clock')
    if request.account_id != snapshot.account_id or request.valuation_currency != snapshot.valuation_currency:
        raise PolicyError('native account/valuation scope mismatch')
    stamp = digest([request, snapshot, limits, now_ms])

    def hold(reason):
        return CapitalAssessment(status='HOLD', reason=reason, input_sha256=stamp)
    if not 0 <= now_ms - snapshot.observed_at_ms <= limits.max_age_ms:
        return hold('native_snapshot_stale')
    if not snapshot.reconciled:
        return hold('native_reconciliation_required')
    if snapshot.risk_paused:
        return hold('native_risk_paused')
    if (not snapshot.complete or snapshot.available_quote is None
            or any(item.marked_inventory_quote is None or item.reserved_acquisition_quote is None
                   for item in snapshot.exposures)):
        return hold('native_snapshot_incomplete')
    base = request.trading_pair.split('-')[0]
    # Native marks include ALL retained inventory; reservations contain only still-unfilled acquisition commitments.
    # All values are already denominated in the native snapshot's valuation currency. No synthetic FX conversion.
    with localcontext() as context:
        context.prec = 60
        account = program = asset = request.requested_quote
        for item in snapshot.exposures:
            value = item.marked_inventory_quote + item.reserved_acquisition_quote
            account += value
            if item.program_id == request.program_id:
                program += value
            if item.trading_pair.split('-')[0] == base:
                asset += value
    checks = ((request.requested_quote, limits.order_quote, 'order_limit'),
              (account, limits.account_quote, 'account_limit'), (program, limits.program_quote, 'program_limit'),
              (asset, limits.base_quote, 'base_limit'), (request.requested_quote, snapshot.available_quote, 'available_quote_limit'))
    reason = next((name for value, ceiling, name in checks if value > ceiling), 'eligible')
    return CapitalAssessment(status='ELIGIBLE_FOR_NATIVE_ADMISSION' if reason == 'eligible' else 'HOLD',
                             reason=reason, input_sha256=stamp, projected_account_quote=account, projected_program_quote=program,
                             projected_base_quote=asset)


class AllocationTargets(PolicyModel):
    targets: tuple[tuple[str, Decimal], ...]
    unallocated_quote: Decimal = Field(ge=0)
    execution_authorized: Literal[False] = False


def fixed_targets(capital_quote, weights, *, quote_step) -> AllocationTargets:
    capital = decimal(capital_quote, 'assigned capital')
    step = decimal(quote_step, 'quote precision', positive=True)
    weights = tuple(weights)
    if not 1 <= len(weights) <= 128:
        raise PolicyError('bounded nonempty allocation weights required')
    parsed = []
    import re
    for name, weight in weights:
        if not isinstance(name, str) or re.fullmatch(NAME, name) is None:
            raise PolicyError('invalid allocation program')
        parsed.append((name, decimal(weight, 'allocation weight')))
    if len({name for name, _ in parsed}) != len(parsed):
        raise PolicyError('duplicate allocation program')
    with localcontext() as context:
        context.prec = 60
        if sum(weight for _, weight in parsed) > 1:
            raise PolicyError('weights exceed assigned capital; no implicit normalization')
        targets = tuple((name, (capital * weight / step).to_integral_value(rounding=ROUND_FLOOR) * step)
                        for name, weight in sorted(parsed))
        idle = capital - sum(value for _, value in targets)
    return AllocationTargets(targets=targets, unallocated_quote=idle)


def bounded_size(*, stage_quote, liquidity_cap_quote, volatility_cap_quote=None, **native_inputs):
    stage = decimal(stage_quote, 'stage quote')
    liquidity = decimal(liquidity_cap_quote, 'liquidity cap')
    ceiling = min(stage, liquidity)
    if volatility_cap_quote is not None:
        ceiling = min(ceiling, decimal(volatility_cap_quote, 'volatility cap'))
    return allocate(stage_quote=ceiling, **native_inputs)
