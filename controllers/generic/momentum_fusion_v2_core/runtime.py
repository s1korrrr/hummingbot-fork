"""Native mapping for momentum v2. Recorder-local claims are not account-wide locks."""
import json
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, localcontext
from pathlib import Path
from typing import Literal

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
                    time_limit, ttl_seconds):
    # ponytail: compiled executor DTOs; this pin has no Cython objects. Upgrade: import after compile.
    from hummingbot.core.data_type.common import OrderType, TradeType
    from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig

    return PositionExecutorConfig(
        timestamp=now, controller_id=controller_id, connector_name=connector_name, trading_pair=trading_pair,
        side=TradeType.BUY, amount=amount, entry_price=price, leverage=1, entry_order_policy='single_attempt',
        triple_barrier_config=TripleBarrierConfig(
            stop_loss=stop, take_profit=take_profit, time_limit=time_limit,
            open_order_type=OrderType.LIMIT, open_order_limit_max_age_seconds=ttl_seconds,
            take_profit_order_type=OrderType.LIMIT, stop_loss_order_type=OrderType.LIMIT,
        ),
    )


def claim_id_for(opportunity_id: str, controller_id: str) -> str:
    return identity([opportunity_id, controller_id])
