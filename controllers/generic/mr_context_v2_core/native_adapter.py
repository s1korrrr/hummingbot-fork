"""Context assessment helpers; restrict mode must bind before native reservation."""
from decimal import Decimal

from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction

from .policy import AcquisitionProposalV2, CapacityEvidenceV2, ContextAssessmentV2, evaluate_context
from .recipe import ContextRecipeV2
from .snapshot import ContextSnapshotV2, checked


def apply_to_actions(actions, *, recipe: ContextRecipeV2, binding, source_sha256: str,
                     opportunity, proposal: AcquisitionProposalV2 | None,
                     capacity: CapacityEvidenceV2 | None, snapshot: ContextSnapshotV2 | None,
                     now_ms: int) -> tuple[tuple, ContextAssessmentV2 | None]:
    if type(actions) not in (tuple, list):
        raise ValueError('native actions must be a sequence')
    recipe = checked(recipe, ContextRecipeV2)
    if recipe.mode == 'restrict':
        raise ValueError('restrict requires pre-reservation plan integration')
    assessment = evaluate_context(
        recipe=recipe, binding=binding, source_sha256=source_sha256, opportunity=opportunity,
        proposal=proposal, capacity=capacity, snapshot=snapshot, now_ms=now_ms,
    )
    for action in actions:
        if not isinstance(action, (CreateExecutorAction, StopExecutorAction)):
            raise ValueError(f'unsupported native action {type(action).__name__}')
    return tuple(actions), assessment


def apply_restrict_to_plan(*, previous, revised, proposal, reason, assessment, mode, fee_fraction):
    """Rewrite a planned entry before reserve/CAS. Shadow/off leave the plan unchanged."""
    if mode != 'restrict' or proposal is None:
        return revised, proposal, reason
    if getattr(proposal, 'stage', 0) != 0:
        return revised, proposal, reason
    if (previous is None or type(fee_fraction) is not Decimal or not fee_fraction.is_finite()
            or not Decimal('0') <= fee_fraction < Decimal('1')):
        raise ValueError('invalid native fee fraction for reservation')
    if (revised.claims != (*previous.claims, proposal.identifier)
            or type(proposal.amount) is not Decimal or type(proposal.price) is not Decimal
            or proposal.amount <= 0 or proposal.price <= 0):
        raise ValueError('unsupported_adapter')
    old_reserved = proposal.amount * proposal.price * (1 + fee_fraction)
    if revised.committed_quote - previous.committed_quote != old_reserved:
        raise ValueError('native reservation differs from proposed amount and fee')
    if assessment is None or assessment.status != 'PASS' or assessment.quantity <= 0:
        held = revised.model_copy(update={'claims': previous.claims,
                                          'committed_quote': previous.committed_quote,
                                          'last_entry_time': previous.last_entry_time})
        return held, None, 'acquisition_disabled'
    if assessment.quantity > proposal.amount:
        raise ValueError('context cannot increase baseline quantity')
    if assessment.quantity == proposal.amount:
        return revised, proposal, reason
    new_reserved = assessment.quantity * proposal.price * (1 + fee_fraction)
    reduced = type(proposal)(proposal.identifier, proposal.price, assessment.quantity,
                             proposal.timestamp, proposal.cycle, proposal.stage)
    return (revised.model_copy(update={'committed_quote': revised.committed_quote - old_reserved + new_reserved}),
            reduced, reason)
