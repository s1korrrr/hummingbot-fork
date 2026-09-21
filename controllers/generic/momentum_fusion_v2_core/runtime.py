"""Native mapping for momentum v2. Recorder-local claims are not account-wide locks."""
import json
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, localcontext
from pathlib import Path
from typing import Literal

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.components.capital_policy import (
    CapitalAssessment,
    CapitalLimits,
    CapitalRequest,
    CapitalSnapshot,
    NativeExposure,
    assess_acquisition,
)
from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.domain import PolicyError, decimal, identity
from hummingbot.strategy_v2.components.exit_policy import (
    ExitAssessment,
    ExitProposal,
    NativeExitView,
    NativeProfitFloor,
    arbitrate_exit,
)
from hummingbot.strategy_v2.components.portfolio import allocate
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from pydantic import Field

ClaimStatus = Literal['RESERVED', 'SUBMITTED_UNACKNOWLEDGED', 'PARTIAL', 'CANCEL_PENDING', 'RECONCILED_TERMINAL']


class Claim(PolicyModel):
    claim_id: str = Field(min_length=8, max_length=128)
    opportunity_id: str = Field(min_length=8, max_length=128)
    controller_id: str = Field(min_length=1, max_length=128)
    trading_pair: str
    status: ClaimStatus
    reserved_quote: Decimal = Field(ge=0)
    reserved_base: Decimal = Field(ge=0)
    executor_identity: str | None = None
    topology: Literal['single_recorder', 'shared_multi_process'] = 'single_recorder'


@dataclass(frozen=True)
class ActionPlan:
    actions: tuple
    reason: str

    def __post_init__(self):
        if not self.reason:
            raise PolicyError('action plan requires an explicit reason')


CLAIM_RANK = {
    'RESERVED': 0,
    'SUBMITTED_UNACKNOWLEDGED': 1,
    'PARTIAL': 2,
    'CANCEL_PENDING': 2,
    'RECONCILED_TERMINAL': 3,
}


class ClaimStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._claims: dict[str, Claim] = {}
        if self.path.exists():
            raw = json.loads(self.path.read_text())
            self._claims = {row['claim_id']: Claim.model_validate(row) for row in raw}

    def _persist(self):
        payload = [item.model_dump(mode='json') for item in self._claims.values()]
        tmp = self.path.with_suffix('.tmp')
        tmp.write_text(json.dumps(payload, sort_keys=True))
        tmp.replace(self.path)

    def put(self, claim: Claim) -> Claim:
        current = self._claims.get(claim.claim_id)
        if current is None:
            self._claims[claim.claim_id] = claim
            self._persist()
            return claim
        if current.opportunity_id != claim.opportunity_id:
            raise PolicyError('claim id already bound')
        if current.status == 'RECONCILED_TERMINAL' or CLAIM_RANK[claim.status] < CLAIM_RANK[current.status]:
            return current
        if claim.status == current.status:
            return current
        self._claims[claim.claim_id] = claim
        self._persist()
        return claim

    def get(self, claim_id: str) -> Claim | None:
        return self._claims.get(claim_id)

    def open_claims(self) -> tuple[Claim, ...]:
        return tuple(item for item in self._claims.values() if item.status != 'RECONCILED_TERMINAL')


def refuse_unsupported_topology(topology: str):
    if topology != 'single_recorder':
        raise PolicyError('unsupported shared-account multi-process topology')


def admit_capital(request: CapitalRequest, snapshot: CapitalSnapshot, limits: CapitalLimits, *, now_ms: int,
                  extra_reserved: tuple[Claim, ...] = ()) -> CapitalAssessment:
    rows = list(snapshot.exposures)
    for claim in extra_reserved:
        if claim.status == 'RECONCILED_TERMINAL':
            continue
        rows.append(NativeExposure(
            executor_id='claim-' + claim.claim_id[:16],
            program_id=request.program_id,
            trading_pair=claim.trading_pair,
            marked_inventory_quote=Decimal('0'),
            reserved_acquisition_quote=claim.reserved_quote,
        ))
    combined = snapshot.model_copy(update={'exposures': tuple(rows)})
    return assess_acquisition(request, combined, limits, now_ms=now_ms)


def size_entry(*, stage_quote, remaining_quote, available_quote, price, amount_step, minimum_amount,
               minimum_notional, fee_buffer, cash_reserve=0):
    return allocate(
        stage_quote=stage_quote, remaining_quote=remaining_quote, available_quote=available_quote,
        price=price, amount_step=amount_step, minimum_amount=minimum_amount,
        minimum_notional=minimum_notional, fee_buffer=fee_buffer, cash_reserve=cash_reserve,
    )


def split_or_single(*, total_base, harvest_base, amount_step, minimum_amount):
    step = decimal(amount_step, 'amount step', positive=True)
    total = decimal(total_base, 'total base', positive=True)
    harvest = decimal(harvest_base, 'harvest base')
    minimum = decimal(minimum_amount, 'minimum amount', positive=True)
    harvest = (harvest / step).to_integral_value(rounding=ROUND_FLOOR) * step
    runner = total - harvest
    if harvest < minimum or runner < minimum:
        return {'mode': 'single', 'amount': total, 'reason': 'sub_minimum_leg'}
    return {'mode': 'split', 'harvest': harvest, 'runner': runner}


def profit_floor(*, cost_quote, quantity, fee_fraction, margin, buffer, price_step):
    with localcontext() as context:
        context.prec = 60
        cost = decimal(cost_quote, 'cost quote', positive=True)
        qty = decimal(quantity, 'quantity', positive=True)
        fee = decimal(fee_fraction, 'fee fraction')
        if fee >= 1:
            raise PolicyError('fee fraction must be below one')
        needed = (cost * (1 + decimal(margin, 'margin')) + decimal(buffer, 'buffer')) / (qty * (1 - fee))
        tick = decimal(price_step, 'price increment', positive=True)
        return (needed / tick).to_integral_value(rounding=ROUND_CEILING) * tick


def remaining_after_partial(requested, filled):
    total = decimal(requested, 'requested', positive=True)
    done = decimal(filled, 'filled')
    if done < 0 or done > total:
        raise PolicyError('fill exceeds request')
    return total - done


def net_uncommitted(remaining_base, reserved_base):
    remaining = decimal(remaining_base, 'remaining')
    reserved = decimal(reserved_base, 'reserved')
    if reserved > remaining:
        raise PolicyError('native quantity conflict')
    return remaining - reserved


def pending_sale_base(executors):
    total = Decimal('0')
    for item in executors:
        if getattr(item, 'is_done', False):
            continue
        side = getattr(item, 'side', None)
        if side is None:
            side = getattr(getattr(item, 'config', None), 'side', None)
        name = getattr(side, 'name', side)
        if name != 'SELL':
            continue
        info = getattr(item, 'custom_info', None) or {}
        amount = info.get('remaining_position_amount_base')
        if amount is None:
            amount = getattr(getattr(item, 'config', None), 'amount', None)
        if amount is None:
            continue
        total += decimal(amount, 'pending sale')
    return total


def one_exit(view: NativeExitView, proposals: tuple[ExitProposal, ...], floor: NativeProfitFloor | None,
             *, now_ms: int, max_age_ms: int = 100) -> ExitAssessment:
    return arbitrate_exit(view, proposals, floor, now_ms=now_ms, max_age_ms=max_age_ms)


def maximum_marked_drawdown(marks):
    peak = None
    worst = Decimal('0')
    for value in marks:
        mark = decimal(value, 'marked equity', positive=True)
        if peak is None or mark > peak:
            peak = mark
        drawdown = (mark - peak) / peak
        if drawdown < worst:
            worst = drawdown
    return worst


def position_config(*, now, controller_id, connector_name, trading_pair, amount, price, stop, take_profit,
                    time_limit, ttl_seconds, side=None):
    # ponytail: compiled executor DTOs; this pin has no Cython objects. Upgrade: import after compile.
    from hummingbot.core.data_type.common import OrderType, TradeType
    from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig

    return PositionExecutorConfig(
        timestamp=now, controller_id=controller_id, connector_name=connector_name, trading_pair=trading_pair,
        side=TradeType.BUY if side is None else side, amount=amount, entry_price=price, leverage=1,
        entry_order_policy='single_attempt',
        triple_barrier_config=TripleBarrierConfig(
            stop_loss=stop, take_profit=take_profit, time_limit=time_limit,
            open_order_type=OrderType.LIMIT, open_order_limit_max_age_seconds=ttl_seconds,
            take_profit_order_type=OrderType.LIMIT, stop_loss_order_type=OrderType.LIMIT,
        ),
    )


def claim_id_for(opportunity_id: str, controller_id: str) -> str:
    return identity([opportunity_id, controller_id])


def _skip(reason: str) -> ActionPlan:
    return ActionPlan((), reason)


def plan_acquisition(*, claims, execution_enabled, acquisition_allowed, recovery_hold, data_ready,
                     signal, last_decision, decision_time, now, max_candle_age_seconds,
                     pending_inflight, controller_id, connector_name, trading_pair, available_quote,
                     total_amount_quote, trade_quote, risk_fraction, price, atr, atr_stop_multiple,
                     maximum_stop_fraction, take_profit, time_limit, fee_buffer, amount_step,
                     minimum_amount, minimum_notional) -> ActionPlan:
    if not execution_enabled:
        return _skip('execution_disabled')
    if claims is None:
        return _skip('claim_store_required')
    if not acquisition_allowed:
        return _skip('acquisition_paused')
    if recovery_hold:
        return _skip('recovery_hold')
    if not data_ready:
        return _skip('data_unready')
    if signal != 1:
        return _skip('no_setup')
    if pending_inflight:
        return _skip('pending_inflight')
    if not 0 <= now - decision_time <= max_candle_age_seconds:
        return _skip('stale_decision')
    if last_decision is not None and decision_time <= last_decision:
        return _skip('already_decided')
    opportunity_id = str(int(decision_time))
    claim_key = claim_id_for(opportunity_id, controller_id)
    existing = next((item for item in claims.open_claims()
                     if item.opportunity_id == opportunity_id or item.claim_id == claim_key), None)
    if existing is not None:
        return _skip('claim_already_open')
    try:
        price_d = decimal(price, 'price', positive=True)
        atr_d = decimal(atr, 'atr', positive=True)
        stop = atr_d * decimal(atr_stop_multiple, 'atr stop multiple', positive=True) / price_d
        max_stop = decimal(maximum_stop_fraction, 'maximum stop', positive=True)
        if not Decimal('0') < stop <= max_stop:
            return _skip('invalid_stop')
        total = decimal(total_amount_quote, 'total quote', positive=True)
        stage = min(decimal(trade_quote, 'trade quote', positive=True),
                    total * decimal(risk_fraction, 'risk fraction', positive=True) / stop)
        now_ms = int(Decimal(str(now)) * 1000)
        digest = identity({'account': controller_id, 'available': str(available_quote), 'now': now_ms})
        snapshot = CapitalSnapshot(
            account_id=controller_id, valuation_currency='USDC', observed_at_ms=now_ms,
            source_sha256=digest, valuation_sha256=digest, complete=True, reconciled=True,
            risk_paused=False, available_quote=str(available_quote), exposures=())
        assessment = admit_capital(
            CapitalRequest(account_id=controller_id, program_id='okx_spot_momentum_v2',
                           trading_pair=trading_pair, valuation_currency='USDC', requested_quote=stage),
            snapshot, CapitalLimits(account_quote=total, program_quote=total, base_quote=total,
                                    order_quote=total, max_age_ms=100),
            now_ms=now_ms, extra_reserved=claims.open_claims())
        if assessment.status != 'ELIGIBLE_FOR_NATIVE_ADMISSION':
            return _skip(assessment.reason)
        allocation = size_entry(
            stage_quote=stage, remaining_quote=total, available_quote=available_quote,
            price=price_d, amount_step=amount_step, minimum_amount=minimum_amount,
            minimum_notional=minimum_notional, fee_buffer=fee_buffer, cash_reserve=0)
    except (PolicyError, ValueError, KeyError, TypeError) as error:
        return _skip(str(error) or 'admission_failed')
    if allocation is None:
        return _skip('size_rejected')
    executor = position_config(
        now=now, controller_id=controller_id, connector_name=connector_name,
        trading_pair=trading_pair, amount=allocation.amount, price=price_d, stop=stop,
        take_profit=take_profit, time_limit=time_limit, ttl_seconds=60)
    claim = Claim(
        claim_id=claim_key, opportunity_id=opportunity_id, controller_id=controller_id,
        trading_pair=trading_pair, status='RESERVED', reserved_quote=allocation.reserved_quote,
        reserved_base=allocation.reserved_base)
    stored = claims.put(claim)
    if stored.status != 'RESERVED' or stored.claim_id != claim.claim_id:
        return _skip('claim_mismatch')
    pending_identity = identity(executor.model_dump(mode='json'))
    submitted = claims.put(claim.model_copy(update={
        'status': 'SUBMITTED_UNACKNOWLEDGED', 'executor_identity': pending_identity,
    }))
    if submitted.status != 'SUBMITTED_UNACKNOWLEDGED':
        return _skip('claim_mismatch')
    return ActionPlan((CreateExecutorAction(controller_id=controller_id, executor_config=executor),),
                      'submitted')


def plan_management(*, claims, acquisition_allowed, view, proposals, cost_quote, harvest_base,
                    fee_fraction, margin, buffer, now_ms, controller_id, connector_name,
                    trading_pair, time_limit) -> ActionPlan:
    if type(acquisition_allowed) is not bool:
        return _skip('invalid_acquisition_flag')
    if claims is None:
        return _skip('claim_store_required')
    try:
        split = split_or_single(
            total_base=view.remaining_base, harvest_base=harvest_base,
            amount_step=view.amount_step, minimum_amount=view.minimum_amount)
        qty = split['harvest'] if split['mode'] == 'split' else split['amount']
        uncommitted = net_uncommitted(view.remaining_base, view.reserved_base)
        qty = min(qty, uncommitted)
        qty = (qty / view.amount_step).to_integral_value(rounding=ROUND_FLOOR) * view.amount_step
        if qty < view.minimum_amount:
            return _skip('native_quantity_below_minimum')
        floor_price = profit_floor(
            cost_quote=cost_quote, quantity=qty, fee_fraction=fee_fraction, margin=margin,
            buffer=buffer, price_step=view.price_step)
        floor = NativeProfitFloor(
            owner_id=view.owner_id, executor_id=view.executor_id, receipt_sha256=view.receipt_sha256,
            quantity_base=qty, price=floor_price, observed_at_ms=view.observed_at_ms)
        result = one_exit(view, proposals, floor, now_ms=now_ms)
    except (PolicyError, ValueError, TypeError, KeyError) as error:
        return _skip(str(error) or 'management_failed')
    if result.status != 'PROPOSED_REDUCTION':
        return _skip(result.reason)
    executor = position_config(
        now=now_ms / 1000, controller_id=controller_id, connector_name=connector_name,
        trading_pair=trading_pair, amount=result.amount_base, price=result.limit_price,
        stop=None, take_profit=None, time_limit=time_limit, ttl_seconds=60, side=TradeType.SELL)
    return ActionPlan((CreateExecutorAction(controller_id=controller_id, executor_config=executor),),
                      'proposed_reduction')
