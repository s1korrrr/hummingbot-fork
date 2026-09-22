"""Recorder-scoped acquisition sizing and admission proposals. A proposal is never a grant."""
from decimal import Decimal, localcontext
from typing import Literal

from pydantic import Field, StrictBool, model_validator

from controllers.generic.spot_recipe_v2_core.spot_lifecycle_v2 import NativeSnapshotV2, checked_snapshot
from hummingbot.strategy_v2.components.acquisition import next_stage
from hummingbot.strategy_v2.components.capital_policy import (
    CapitalAssessment,
    CapitalLimits,
    CapitalRequest,
    CapitalSnapshot,
    assess_acquisition,
    bounded_size,
)
from hummingbot.strategy_v2.components.config import AcquisitionPolicy, PolicyModel, RiskPolicy
from hummingbot.strategy_v2.components.decision_protocol import HASH, NAME
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.program_budget import BudgetPolicy
from hummingbot.strategy_v2.components.recipe_contract import digest

from .acquisition import AcquisitionAssessmentV2

Capability = Literal['RECORDER_SCOPED', 'ACCOUNT_ATOMIC']
# One ceiling for one concept: the ledger's capacity and the stage index it can report.
MAX_STAGES = 8


class CapitalCapability(PolicyModel):
    name: Capability
    supported: StrictBool
    guarantee: str = Field(max_length=400)


CAPABILITIES = (
    CapitalCapability(
        name='RECORDER_SCOPED', supported=True,
        guarantee='commitments recorded by one native recorder and strategy configuration are counted once; '
                  'spending by any other recorder, process or manual actor on the same exchange cash is not coordinated'),
    CapitalCapability(
        name='ACCOUNT_ATOMIC', supported=False,
        guarantee='unsupported; no atomic account-wide check-and-reserve over shared exchange cash exists here, '
                  'so no account-wide safety may be claimed or admitted'),
)


def declared_capability(name) -> CapitalCapability:
    found = next((item for item in CAPABILITIES if item.name == name), None)
    if found is None:
        raise PolicyError('unknown capital capability')
    return found


class RecorderOwner(PolicyModel):
    owner_id: str = Field(pattern=NAME)
    recorder_sha256: str = Field(pattern=HASH)
    represented: StrictBool


class RecorderScopeEvidence(PolicyModel):
    account_id: str = Field(pattern=NAME)
    recorder_sha256: str = Field(pattern=HASH)
    budget_policy: BudgetPolicy
    owners: tuple[RecorderOwner, ...] = Field(min_length=1, max_length=64)
    unrepresented_commitments: StrictBool

    @model_validator(mode='after')
    def unique_owners(self):
        if len({item.owner_id for item in self.owners}) != len(self.owners):
            raise ValueError('duplicate recorder owner identity')
        return self


class AdmittedStage(PolicyModel):
    request_id: str = Field(pattern=NAME)
    admitted_quote: Decimal = Field(gt=0)
    filled_quote: Decimal = Field(ge=0)
    filled_fees_quote: Decimal = Field(ge=0)
    filled_base: Decimal = Field(ge=0)
    terminal: StrictBool

    @model_validator(mode='after')
    def costed_fill(self):
        if self.filled_quote + self.filled_fees_quote > self.admitted_quote:
            raise ValueError('stage fills and fees exceed the admitted grant')
        if (self.filled_base > 0) != (self.filled_quote > 0):
            raise ValueError('native fill requires both base and quote evidence')
        if self.terminal and self.filled_quote > 0 and self.filled_fees_quote <= 0:
            raise ValueError('a terminal fill requires actual fee evidence')
        return self

    @property
    def committed_quote(self) -> Decimal:
        # An unresolved request keeps its whole admitted capacity; expiry, cancellation and
        # missing acknowledgements cannot release capacity the exchange may still fill.
        return self.filled_quote + self.filled_fees_quote if self.terminal else self.admitted_quote


class AcquisitionLedgerV2(PolicyModel):
    owner_id: str = Field(pattern=NAME)
    executor_id: str = Field(pattern=NAME)
    grant_quote: Decimal = Field(gt=0)
    stages: tuple[AdmittedStage, ...] = Field(default=(), max_length=MAX_STAGES)
    # Recorded by reconciled_ledger. On its own this is only a claim; `size_stage` is what makes it
    # mean anything, by checking it against the predecessor object the caller has to produce.
    lineage_sha256: str | None = Field(default=None, pattern=HASH)

    @property
    def ledger_sha256(self) -> str:
        return digest(self.model_dump(mode='json'))

    @model_validator(mode='after')
    def one_unresolved_commitment(self):
        if len({item.request_id for item in self.stages}) != len(self.stages):
            raise ValueError('duplicate admitted request identity')
        if sum(item.terminal is False for item in self.stages) > 1:
            raise ValueError('concurrent unresolved acquisitions in one attributed scope')
        if self.committed_quote > self.grant_quote:
            raise ValueError('admitted stages exceed the program grant')
        return self

    @property
    def committed_quote(self) -> Decimal:
        return sum((item.committed_quote for item in self.stages), Decimal('0'))

    @property
    def filled_quote(self) -> Decimal:
        return sum((item.filled_quote for item in self.stages), Decimal('0'))

    @property
    def residual_quote(self) -> Decimal:
        return self.grant_quote - self.committed_quote

    @property
    def settled(self) -> bool:
        return all(item.terminal for item in self.stages)

    @property
    def stage_index(self) -> int:
        return sum(item.terminal and item.filled_base > 0 for item in self.stages)

    @property
    def last_fill_price(self) -> Decimal:
        filled = [item for item in self.stages if item.filled_base > 0]
        if not filled:
            return Decimal('0')
        with localcontext() as context:
            context.prec = 60
            return filled[-1].filled_quote / filled[-1].filled_base


def reconciled_ledger(previous: AcquisitionLedgerV2, current: AcquisitionLedgerV2) -> AcquisitionLedgerV2:
    if type(previous) is not AcquisitionLedgerV2 or type(current) is not AcquisitionLedgerV2:
        raise PolicyError('canonical admitted stage ledgers required')
    previous = AcquisitionLedgerV2.model_validate(previous.model_dump(mode='python'))
    current = AcquisitionLedgerV2.model_validate(current.model_dump(mode='python'))
    if (previous.owner_id, previous.executor_id) != (current.owner_id, current.executor_id):
        raise PolicyError('admitted ledger scope changed; a shadow or foreign scope needs its own state')
    if previous.grant_quote != current.grant_quote:
        raise PolicyError('admitted program grant changed without a new admission')
    revised = {item.request_id: item for item in current.stages}
    for before in previous.stages:
        after = revised.get(before.request_id)
        if after is None:
            raise PolicyError('an admitted request needs a terminal receipt before it leaves the ledger')
        if after.admitted_quote != before.admitted_quote:
            raise PolicyError('admitted grant for an existing request cannot be revised')
        if (after.filled_quote < before.filled_quote or after.filled_fees_quote < before.filled_fees_quote
                or after.filled_base < before.filled_base or (before.terminal and not after.terminal)):
            raise PolicyError('native fill or terminal evidence regressed')
    return AcquisitionLedgerV2.model_validate(
        current.model_dump(mode='python') | {'lineage_sha256': previous.ledger_sha256})


class StageInputs(PolicyModel):
    bid: Decimal = Field(gt=0)
    available_quote: Decimal = Field(ge=0)
    liquidity_cap_quote: Decimal = Field(gt=0)
    volatility_cap_quote: Decimal | None = Field(default=None, gt=0)
    amount_step: Decimal = Field(gt=0)
    minimum_amount: Decimal = Field(gt=0)
    minimum_notional: Decimal = Field(ge=0)


class StageQuoteV2(PolicyModel):
    status: Literal['HOLD', 'SIZED']
    reason: str = Field(pattern=NAME)
    input_sha256: str = Field(pattern=HASH)
    program_id: str = Field(pattern=NAME)
    owner_id: str = Field(pattern=NAME)
    executor_id: str = Field(pattern=NAME)
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$', max_length=80)
    stage_index: int = Field(ge=0, le=MAX_STAGES, strict=True)
    residual_quote: Decimal = Field(ge=0)
    entry_basis_quote: Decimal = Field(ge=0)
    quote_amount: Decimal | None = Field(default=None, gt=0)
    base_amount: Decimal | None = Field(default=None, gt=0)
    execution_authorized: Literal[False] = False
    economic_backtest: Literal[False] = False

    @model_validator(mode='after')
    def sized_or_held(self):
        if (self.status == 'SIZED') != (self.quote_amount is not None and self.base_amount is not None):
            raise ValueError('a sized stage requires both amounts and a hold carries none')
        if self.status == 'SIZED' and self.quote_amount > self.residual_quote:
            raise ValueError('sized stage exceeds the residual admitted budget')
        return self


def size_stage(assessment: AcquisitionAssessmentV2, snapshot: NativeSnapshotV2, ledger: AcquisitionLedgerV2,
               policy: AcquisitionPolicy, risk: RiskPolicy, inputs: StageInputs, *,
               predecessor: AcquisitionLedgerV2 | None) -> StageQuoteV2:
    """Size one bounded stage from native fills and an admitted residual budget.

    `predecessor` has no default. A ledger that claims lineage is checked against the actual
    predecessor object, because a caller can mint a digest but cannot mint a ledger whose
    unresolved stages it then has to account for.

    Maximum commitment (`ledger.grant_quote`), stage allocation (`policy.fractions`),
    eligibility (the canonical acquisition assessment) and the sizing price stay separate.
    """
    if type(assessment) is not AcquisitionAssessmentV2 or type(inputs) is not StageInputs:
        raise PolicyError('canonical acquisition assessment and stage inputs required')
    if type(policy) is not AcquisitionPolicy or type(risk) is not RiskPolicy:
        raise PolicyError('canonical acquisition and risk policies required')
    if type(ledger) is not AcquisitionLedgerV2:
        raise PolicyError('canonical admitted stage ledger required')
    snapshot = checked_snapshot(snapshot)
    ledger = AcquisitionLedgerV2.model_validate(ledger.model_dump(mode='python'))
    inputs = StageInputs.model_validate(inputs.model_dump(mode='python'))
    if assessment.cursor.action != 'acquire':
        raise PolicyError('short or reduction sizing is unsupported in this spot scope')
    if assessment.snapshot_sha256 != snapshot.snapshot_sha256:
        raise PolicyError('acquisition assessment and native snapshot differ')
    if (ledger.owner_id, ledger.executor_id) != (snapshot.scope.owner_id, snapshot.scope.executor_id):
        raise PolicyError('admitted ledger is attributed to another native scope')
    # The ledger records fee-exclusive fills so it compares directly with the native gross entry total.
    if ledger.filled_quote != snapshot.entry_gross_quote:
        raise PolicyError('admitted stage ledger and authoritative native fills disagree')
    # A ledger holding fills is the recipe's memory of capacity it already submitted. Rebuilding it
    # from the snapshot silently forgets unresolved capacity, so it must descend from its predecessor.
    if ledger.lineage_sha256 is None:
        if ledger.filled_quote > 0:
            raise PolicyError('an admitted ledger carrying native fills must descend from its predecessor')
        if predecessor is not None:
            raise PolicyError('a predecessor was supplied for a ledger that claims no lineage')
    else:
        if type(predecessor) is not AcquisitionLedgerV2:
            raise PolicyError('a ledger claiming lineage must be checked against its predecessor')
        if predecessor.ledger_sha256 != ledger.lineage_sha256:
            raise PolicyError('admitted ledger lineage does not match the supplied predecessor')
        # Catches the rebuild the native commitment check cannot see: a submitted request whose
        # acknowledgement never arrived is absent from `snapshot.commitments` but present here.
        if {item.request_id for item in predecessor.stages if not item.terminal} - {
                item.request_id for item in ledger.stages}:
            raise PolicyError('admitted ledger drops an unresolved predecessor acquisition')
    # Resolution is native evidence, not a caller assertion: a rebuilt ledger must not drop or
    # resolve capacity the exchange still has outstanding.
    live = {item.request_id for item in snapshot.commitments if item.action == 'acquire'}
    unresolved = {item.request_id for item in ledger.stages if not item.terminal}
    if live - unresolved:
        raise PolicyError('admitted ledger omits an outstanding native acquisition request')
    stamp = digest([assessment, snapshot, ledger, policy, risk, inputs])
    scope = snapshot.scope

    def result(status, reason, allocation=None):
        return StageQuoteV2(
            status=status, reason=reason, input_sha256=stamp, program_id=scope.program_id, owner_id=scope.owner_id,
            executor_id=scope.executor_id, trading_pair=scope.trading_pair, stage_index=ledger.stage_index,
            residual_quote=ledger.residual_quote, entry_basis_quote=snapshot.entry_gross_quote,
            quote_amount=None if allocation is None else allocation.reserved_quote,
            base_amount=None if allocation is None else allocation.amount)

    if assessment.native_reason != 'eligible':
        return result('HOLD', assessment.native_reason)
    if assessment.trace.status != 'ELIGIBLE_FOR_NATIVE_ADMISSION':
        return result('HOLD', assessment.trace.reason)
    fraction = next_stage(policy, stage=ledger.stage_index, bid=inputs.bid,
                          last_fill_price=ledger.last_fill_price, fresh_signal=True,
                          entry_terminal=ledger.settled, distribution_started=snapshot.exit_gross_base > 0)
    if fraction is None:
        if not ledger.settled:
            return result('HOLD', 'unresolved_acquisition_outstanding')
        if ledger.stage_index >= len(policy.fractions):
            return result('HOLD', 'acquisition_schedule_exhausted')
        if snapshot.exit_gross_base > 0:
            return result('HOLD', 'distribution_started')
        return result('HOLD', 'minimum_stage_drop_unmet')
    with localcontext() as context:
        context.prec = 60
        stage_quote = min(ledger.grant_quote * fraction, ledger.residual_quote)
        if inputs.available_quote < stage_quote * (1 + risk.fee_reserve_fraction) + risk.cash_reserve_quote:
            return result('HOLD', 'insufficient_fee_reserve')
    sizing = dict(stage_quote=stage_quote, liquidity_cap_quote=inputs.liquidity_cap_quote,
                  volatility_cap_quote=inputs.volatility_cap_quote, remaining_quote=ledger.residual_quote,
                  available_quote=inputs.available_quote, price=inputs.bid, amount_step=inputs.amount_step,
                  fee_buffer=risk.fee_reserve_fraction, cash_reserve=risk.cash_reserve_quote)
    allocation = bounded_size(minimum_amount=inputs.minimum_amount, minimum_notional=inputs.minimum_notional, **sizing)
    if allocation is not None:
        return result('SIZED', 'eligible', allocation)
    # The canonical allocator returns nothing for every rejection; requote at the precision floor
    # to name which bound actually failed instead of reimplementing its quantization.
    probe = bounded_size(minimum_amount=inputs.amount_step, minimum_notional=Decimal('0'), **sizing)
    if probe is None:
        return result('HOLD', 'stage_quote_below_precision')
    reason = 'below_minimum_amount' if probe.amount < inputs.minimum_amount else 'below_minimum_notional'
    return result('HOLD', reason)


class CapitalAdmissionV2(PolicyModel):
    status: Literal['HOLD', 'ELIGIBLE_FOR_NATIVE_ADMISSION']
    reason: str = Field(pattern=NAME)
    capability: Capability
    input_sha256: str = Field(pattern=HASH)
    stage: StageQuoteV2
    request: CapitalRequest | None = None
    assessment: CapitalAssessment | None = None
    admitted_quote: Decimal | None = Field(default=None, gt=0)
    execution_authorized: Literal[False] = False
    economic_backtest: Literal[False] = False

    @model_validator(mode='after')
    def admitted_only_with_canonical_evidence(self):
        if self.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION' and (
                self.capability != 'RECORDER_SCOPED' or self.assessment is None or self.request is None
                or self.admitted_quote != self.request.requested_quote
                or self.assessment.status != 'ELIGIBLE_FOR_NATIVE_ADMISSION'):
            raise ValueError('admission requires a recorder-scoped canonical capital assessment')
        return self


def admit_acquisition(stage: StageQuoteV2, evidence: RecorderScopeEvidence, snapshot: CapitalSnapshot,
                      limits: CapitalLimits, risk: RiskPolicy, *, capability: Capability = 'RECORDER_SCOPED',
                      now_ms: int) -> CapitalAdmissionV2:
    """Guard recorder scope and commitment identity, then delegate to the canonical assessor.

    The only added vetoes are the recorder-scope guard, commitment de-duplication and the
    marked-loss envelope. Every account, program, base-asset, order and freshness check
    stays in `assess_acquisition`.
    """
    if type(stage) is not StageQuoteV2 or type(evidence) is not RecorderScopeEvidence:
        raise PolicyError('canonical sized stage and recorder scope evidence required')
    if type(snapshot) is not CapitalSnapshot or type(limits) is not CapitalLimits or type(risk) is not RiskPolicy:
        raise PolicyError('canonical capital snapshot, limits and risk policy required')
    stage = StageQuoteV2.model_validate(stage.model_dump(mode='python'))
    evidence = RecorderScopeEvidence.model_validate(evidence.model_dump(mode='python'))
    snapshot = CapitalSnapshot.model_validate(snapshot.model_dump(mode='python'))
    if evidence.account_id != snapshot.account_id:
        raise PolicyError('recorder evidence and capital snapshot describe different accounts')
    stamp = digest([stage, evidence, snapshot, limits, risk, capability, now_ms])

    def hold(reason):
        return CapitalAdmissionV2(status='HOLD', reason=reason, capability=capability, input_sha256=stamp, stage=stage)

    if not declared_capability(capability).supported:
        return hold(f'{capability.lower()}_unsupported')
    if any(owner.recorder_sha256 != evidence.recorder_sha256 for owner in evidence.owners):
        return hold('native_account_segregation_unproven')
    if evidence.unrepresented_commitments or any(not owner.represented for owner in evidence.owners):
        return hold('native_commitments_unrepresented')
    if stage.status != 'SIZED':
        return hold(stage.reason)
    mine = [item for item in snapshot.exposures if item.executor_id == stage.executor_id]
    if any(item.reserved_acquisition_quote is None or item.reserved_acquisition_quote > 0 for item in mine):
        return hold('unresolved_acquisition_outstanding')
    if stage.entry_basis_quote > 0:
        if not mine or mine[0].marked_inventory_quote is None:
            return hold('native_marked_inventory_missing')
        with localcontext() as context:
            context.prec = 60
            loss = (stage.entry_basis_quote - mine[0].marked_inventory_quote) / stage.entry_basis_quote
        if loss > risk.max_drawdown_fraction:
            return hold('marked_loss_limit')
    requested = stage.quote_amount
    request = CapitalRequest(account_id=snapshot.account_id, program_id=stage.program_id,
                             trading_pair=stage.trading_pair, valuation_currency=snapshot.valuation_currency,
                             requested_quote=requested)
    assessment = assess_acquisition(request, snapshot, limits, now_ms=now_ms)
    admitted = assessment.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    return CapitalAdmissionV2(status=assessment.status, reason=assessment.reason, capability=capability,
                              input_sha256=stamp, stage=stage, request=request, assessment=assessment,
                              admitted_quote=requested if admitted else None)
