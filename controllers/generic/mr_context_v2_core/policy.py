"""Unconnected initial-entry assessments. No orders, reservations, native state writes or I/O."""
from decimal import Decimal, ROUND_FLOOR, localcontext
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from hummingbot.strategy_v2.components.decision_protocol import (
    DecisionCursor, DecisionScope, DecisionTrace, Opportunity, Prerequisite, decide,
)
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, digest

from .recipe import ContextRecipeV2, verify_policy_binding
from .snapshot import (
    ContextModel, ContextSnapshotV2, Fraction, Hash, Millis, Name, Nonnegative, Number,
    check_clock, checked, context_blockers,
)

Positive = Annotated[Number, Field(gt=0)]


class NativeEvidenceV2(ContextModel):
    scope: DecisionScope
    account_id: Name
    state_sha256: Hash
    state_version: Annotated[int, Field(strict=True, ge=0)]
    observed_at_ms: Millis
    expires_at_ms: Millis

    @model_validator(mode='after')
    def supported_scope(self):
        if self.scope.connector != 'okx' or not self.scope.trading_pair.endswith('-USDC'):
            raise ValueError('this view supports OKX USDC spot only')
        if not 0 < self.expires_at_ms - self.observed_at_ms <= 86400000:
            raise ValueError('native evidence needs bounded expiry')
        return self


class AcquisitionProposalV2(NativeEvidenceV2):
    opportunity_id: Hash
    baseline_recipe_sha256: Hash
    price: Positive
    quantity: Positive
    amount_step: Positive
    minimum_quantity: Nonnegative
    minimum_notional: Nonnegative
    fee_currency: Literal['USDC']
    fee_fraction_bound: Annotated[Fraction, Field(lt=1)]
    max_quote_commitment: Positive

    @model_validator(mode='after')
    def baseline_is_bounded(self):
        with localcontext() as ctx:
            ctx.prec = 80
            if self.quantity % self.amount_step:
                raise ValueError('baseline quantity must be native-step aligned')
            if (self.quantity < self.minimum_quantity or
                    self.quantity * self.price < self.minimum_notional or
                    self.quantity * self.price * (1 + self.fee_fraction_bound) > self.max_quote_commitment):
                raise ValueError('baseline proposal violates its native amount/fee bounds')
        return self


class CapacityEvidenceV2(NativeEvidenceV2):
    """Supplied read-only caps; this record does not establish account-wide authority."""
    remaining_quote: Nonnegative
    liquidity_quote_cap: Nonnegative


class ContextAssessmentV2(ContextModel):
    schema_version: Literal['mr_context.assessment.v2'] = 'mr_context.assessment.v2'
    opportunity_id: Hash
    recipe_sha256: Hash
    snapshot_sha256: Hash | None
    input_sha256: Hash
    source_sha256: Hash
    state_sha256: Hash
    state_version: Annotated[int, Field(strict=True, ge=0)]
    baseline_proposal_sha256: Hash
    mode: Literal['shadow', 'restrict']
    status: Literal['PASS', 'HOLD', 'STALE', 'UNAVAILABLE']
    multiplier: Fraction
    quantity: Nonnegative
    quote_commitment: Nonnegative
    evaluated_at_ms: Millis
    expires_at_ms: Millis
    reasons: tuple[Name, ...] = Field(min_length=1, max_length=64)
    execution_authorized: Literal[False] = False

    @field_validator('execution_authorized', mode='before')
    @classmethod
    def never_authorize(cls, value):
        if value is not False:
            raise ValueError('assessment cannot authorize execution')
        return value

    @model_validator(mode='after')
    def coherent_result(self):
        if self.status == 'PASS':
            if min(self.multiplier, self.quantity, self.quote_commitment) <= 0:
                raise ValueError('PASS needs a positive bounded proposal')
            if self.evaluated_at_ms >= self.expires_at_ms:
                raise ValueError('expired evidence cannot PASS')
        elif self.quantity != 0 or self.quote_commitment != 0:
            raise ValueError('withheld assessments must have zero proposed exposure')
        return self

    @property
    def assessment_sha256(self) -> str:
        return digest(self.model_dump(mode='json'))


def evaluate_context(*, recipe: ContextRecipeV2 | None, binding: RecipeBinding | None,
                     source_sha256: str | None, opportunity: Opportunity,
                     proposal: AcquisitionProposalV2 | None, capacity: CapacityEvidenceV2 | None,
                     snapshot: ContextSnapshotV2 | None, now_ms: int) -> ContextAssessmentV2 | None:
    """Return a diagnostic cap. Off/reduce bypass context; no native actions are emitted."""
    check_clock(now_ms)
    opportunity = checked(opportunity, Opportunity)
    if now_ms < opportunity.created_at_ms:
        raise ValueError('decision clock precedes setup')
    if opportunity.action == 'reduce':
        return None
    recipe = checked(recipe, ContextRecipeV2)
    verify_policy_binding(recipe, binding, source_sha256=source_sha256)
    if opportunity.scope.recipe_sha256 != recipe.recipe_sha256:
        raise ValueError('opportunity belongs to another recipe')
    if recipe.mode == 'off':
        return None
    proposal = checked(proposal, AcquisitionProposalV2)
    capacity = checked(capacity, CapacityEvidenceV2)
    if proposal.opportunity_id != opportunity.opportunity_id or proposal.baseline_recipe_sha256 != recipe.baseline_recipe_sha256:
        raise ValueError('baseline proposal is not bound to this opportunity/recipe')
    if proposal.scope != opportunity.scope or capacity.scope != opportunity.scope:
        raise ValueError('native scope does not match the opportunity')
    if (proposal.account_id, proposal.state_version, proposal.state_sha256) != (
            capacity.account_id, capacity.state_version, capacity.state_sha256):
        raise ValueError('native capacity/proposal identity or state differs')
    snapshot = None if snapshot is None else checked(snapshot, ContextSnapshotV2)
    with localcontext() as ctx:
        ctx.prec = 80
        return _assess(recipe, checked(binding, RecipeBinding), opportunity, proposal, capacity, snapshot, now_ms)


def _assess(recipe, binding, opportunity, proposal, capacity, snapshot, now_ms):
    blockers = list(context_blockers(snapshot, recipe.requirements, now_ms=now_ms))
    if not opportunity.scope.acquisition_allowed:
        blockers.append('acquisition_forbidden')
    for name, evidence in (('proposal', proposal), ('capacity', capacity)):
        if evidence.observed_at_ms > now_ms:
            blockers.append(name + '_not_available')
        if now_ms >= evidence.expires_at_ms:
            blockers.append(name + '_expired')
    if now_ms >= opportunity.expires_at_ms:
        blockers.append('opportunity_expired')
    expiry = min(opportunity.expires_at_ms, proposal.expires_at_ms, capacity.expires_at_ms)
    if snapshot is not None:
        expiry = min(expiry, snapshot.expires_at_ms)
        if snapshot.observation not in opportunity.dependencies:
            blockers.append('context_not_bound')
        if snapshot.received_at_ms > opportunity.created_at_ms:
            blockers.append('context_unavailable_at_setup')
        if any(getattr(snapshot.components, field) is None for rule in recipe.rules
               for field, threshold, _ in rule.predicates if threshold is not None):
            blockers.append('component_unavailable')
    multiplier, quantity, commitment = Decimal('1'), Decimal('0'), Decimal('0')
    reasons = []
    if blockers:
        status = ('STALE' if any(item.endswith(('_expired', '_stale')) for item in blockers) else
                  'HOLD' if any(item in {'context_not_bound', 'acquisition_forbidden', 'membership_changed',
                                        'source_mismatch', 'dataset_mismatch', 'implementation_mismatch'}
                                for item in blockers) else 'UNAVAILABLE')
        multiplier = Decimal('0')
        reasons = blockers
    else:
        for rule in recipe.rules:
            matches = all((getattr(snapshot.components, field) <= threshold if comparison == 'le'
                           else getattr(snapshot.components, field) >= threshold)
                          for field, threshold, comparison in rule.predicates if threshold is not None)
            if matches:
                multiplier = min(multiplier, rule.multiplier)
                reasons.append('rule:' + rule.name)
        cap = min(multiplier * proposal.max_quote_commitment, capacity.remaining_quote, capacity.liquidity_quote_cap)
        unit_commitment = proposal.price * (1 + proposal.fee_fraction_bound)
        raw_quantity = min(multiplier * proposal.quantity, cap / unit_commitment)
        quantity = (raw_quantity / proposal.amount_step).to_integral_value(rounding=ROUND_FLOOR) * proposal.amount_step
        commitment = quantity * unit_commitment
        status = 'PASS'
        if quantity <= 0 or quantity < proposal.minimum_quantity or quantity * proposal.price < proposal.minimum_notional:
            status, quantity, commitment = 'HOLD', Decimal('0'), Decimal('0')
            reasons.append('context_veto' if multiplier == 0 else 'minimum_order')
    inputs = dict(binding=binding.model_dump(mode='json'), opportunity=opportunity.model_dump(mode='json'), proposal=proposal.model_dump(mode='json'),
                  capacity=capacity.model_dump(mode='json'),
                  snapshot=None if snapshot is None else snapshot.model_dump(mode='json'))
    return ContextAssessmentV2(opportunity_id=opportunity.opportunity_id, recipe_sha256=recipe.recipe_sha256,
                               snapshot_sha256=None if snapshot is None else snapshot.content_sha256,
                               input_sha256=digest(inputs), source_sha256=binding.source_sha256,
                               state_sha256=capacity.state_sha256, state_version=capacity.state_version,
                               baseline_proposal_sha256=digest(proposal.model_dump(mode='json')),
                               mode=recipe.mode, status=status, multiplier=multiplier,
                               quantity=quantity, quote_commitment=commitment, evaluated_at_ms=now_ms,
                               expires_at_ms=expiry, reasons=tuple(sorted(set(reasons))) or ('context_clear',))


def _prerequisite(assessment: ContextAssessmentV2) -> Prerequisite:
    return Prerequisite(name='mr_context', status=assessment.status, evidence_sha256=assessment.assessment_sha256)


def admission_prerequisite(assessment: ContextAssessmentV2, *, opportunity: Opportunity,
                           now_ms: int) -> Prerequisite:
    """An eligibility prerequisite, never order or capital authorization."""
    assessment = checked(assessment, ContextAssessmentV2)
    check_clock(now_ms)
    opportunity = checked(opportunity, Opportunity)
    if assessment.mode != 'restrict':
        raise ValueError('shadow assessment must not enter baseline admission')
    if (opportunity.action != 'acquire' or assessment.opportunity_id != opportunity.opportunity_id or
            assessment.recipe_sha256 != opportunity.scope.recipe_sha256):
        raise ValueError('assessment is not bound to this acquisition opportunity')
    if now_ms < assessment.evaluated_at_ms:
        raise ValueError('admission clock precedes the assessment')
    if now_ms >= min(assessment.expires_at_ms, opportunity.expires_at_ms):
        return Prerequisite(name='mr_context', status='STALE', evidence_sha256=assessment.assessment_sha256)
    return _prerequisite(assessment)


def project_shadow(previous: DecisionCursor | None, opportunity: Opportunity,
                   assessment: ContextAssessmentV2, *,
                   prerequisites: tuple[Prerequisite, ...]) -> tuple[DecisionCursor, DecisionTrace]:
    """Pure observer transition; callers must never persist it as a native decision cursor."""
    assessment = checked(assessment, ContextAssessmentV2)
    opportunity = checked(opportunity, Opportunity)
    if (assessment.mode != 'shadow' or opportunity.action != 'acquire' or
            assessment.opportunity_id != opportunity.opportunity_id or
            assessment.recipe_sha256 != opportunity.scope.recipe_sha256):
        raise ValueError('shadow evidence does not bind this acquisition opportunity')
    previous = None if previous is None else checked(previous, DecisionCursor)
    return decide(previous, opportunity, now_ms=assessment.evaluated_at_ms,
                  prerequisites=(*prerequisites, _prerequisite(assessment)))
