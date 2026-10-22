"""Optional reduction proposals only; caller owns durable claims and native admission."""
from decimal import Decimal

from hummingbot.strategy_v2.components.decision_protocol import (
    DecisionCursor, DecisionScope, Opportunity, Prerequisite, decide,
)
from hummingbot.strategy_v2.components.exit_policy import ExitProposal

from .contracts import ContinuationEstimateV2, ExitAContextV2, ExitAParametersV2, ExitAResultV2

REQUIRED_PREREQUISITES = frozenset({'v2_recipe', 'native_inventory', 'native_protection'})


def choose_action(estimate_bps: Decimal, margin_bps: Decimal) -> str:
    if any(type(v) is not Decimal or not v.is_finite() for v in (estimate_bps, margin_bps)) or margin_bps < 0:
        raise ValueError('finite Decimal estimate and nonnegative margin required')
    return 'PROPOSE_REDUCTION' if estimate_bps < -margin_bps else 'NO_PROPOSAL'


def evaluate_exit_a(context: ExitAContextV2, estimate: ContinuationEstimateV2 | None,
                    parameters: ExitAParametersV2, *, previous: DecisionCursor | None = None,
                    prerequisites: tuple[Prerequisite, ...] = ()) -> ExitAResultV2:
    # Revalidate at the boundary: model_copy/model_construct do not establish validity.
    try:
        context = ExitAContextV2.model_validate(context.model_dump(mode='json'))
        parameters = ExitAParametersV2.model_validate(parameters.model_dump(mode='json'))
        if estimate is not None:
            estimate = ContinuationEstimateV2.model_validate(estimate.model_dump(mode='json'))
        if previous is not None:
            previous = DecisionCursor.model_validate(previous.model_dump(mode='json'))
    except (ValueError, TypeError, AttributeError):
        return ExitAResultV2(reason='invalid_input')

    def abstain(reason):
        return ExitAResultV2(reason=reason, cursor=previous)

    if not parameters.enabled:
        return abstain('disabled')
    if context.recipe != parameters.recipe or context.full_configuration_sha256 != parameters.full_configuration_sha256:
        return abstain('binding_mismatch')
    if not context.activated:
        return abstain('not_activated')
    if context.native_close_pending:
        return abstain('native_close_pending')
    now = context.decision_time_ms
    if now - context.observation.available_time_ms > parameters.max_feature_age_ms:
        return abstain('feature_stale')
    if now >= context.observation.available_time_ms + parameters.lifetime_ms:
        return abstain('opportunity_expired')
    if now - context.native.observed_at_ms > parameters.max_native_age_ms:
        return abstain('native_snapshot_stale')
    if not context.native.reconciled or not context.native.fees_complete:
        return abstain('native_evidence_incomplete')
    uncommitted = context.native.remaining_base - context.native.reserved_base
    if uncommitted <= 0:
        return abstain('no_uncommitted_inventory')
    if estimate is None or not estimate.available:
        return abstain('estimate_unavailable')
    if estimate.context_sha256 != context.context_sha256 or estimate.feature_sha256 != context.feature_sha256:
        return abstain('estimate_context_mismatch')
    if estimate.model_sha256 != parameters.model_sha256 or estimate.training_manifest_sha256 != parameters.training_manifest_sha256:
        return abstain('model_binding_mismatch')
    if not estimate.label_cutoff_ms <= estimate.trained_at_ms <= now < estimate.expires_at_ms:
        return abstain('model_not_available')
    try:
        if not isinstance(prerequisites, tuple) or len(prerequisites) > 64:
            raise ValueError('bounded prerequisite tuple required')
        checks = tuple(Prerequisite.model_validate(p.model_dump(mode='json')) for p in prerequisites)
        if not REQUIRED_PREREQUISITES <= {p.name for p in checks}:
            return abstain('missing_prerequisites')
        # Acquisition pause must not change the identity of this reduce-only cursor.
        reduction_scope = DecisionScope.model_validate(context.scope.model_dump() | {'acquisition_allowed': False})
        opportunity = Opportunity(scope=reduction_scope, setup=context.setup_key, action='reduce',
            primary=context.observation, created_at_ms=now, lifetime_ms=parameters.lifetime_ms)
        cursor, trace = decide(previous, opportunity, now_ms=now, prerequisites=checks)
    except (ValueError, TypeError, AttributeError):
        return abstain('cursor_scope_conflict')
    if trace.status != 'ELIGIBLE_FOR_NATIVE_ADMISSION':
        return ExitAResultV2(reason=trace.reason, cursor=cursor, trace=trace)
    if choose_action(estimate.value_bps, parameters.margin_bps) != 'PROPOSE_REDUCTION':
        return ExitAResultV2(reason='continuation_not_negative', cursor=cursor, trace=trace)
    proposal = ExitProposal(policy_id='exit_a_v2', owner_id=context.native.owner_id,
        executor_id=context.native.executor_id, receipt_sha256=context.native.receipt_sha256,
        requested_base=uncommitted, minimum_price=Decimal('0'),
        expires_at_ms=min(opportunity.expires_at_ms, estimate.expires_at_ms))
    return ExitAResultV2(status='PROPOSE_REDUCTION', reason='negative_continuation_value',
                         cursor=cursor, trace=trace, proposal=proposal)
