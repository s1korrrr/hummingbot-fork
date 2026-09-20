from decimal import Decimal as D

import pytest

from hummingbot.strategy_v2.components.decision_protocol import DecisionScope, ObservationRef, Opportunity, Prerequisite
from controllers.generic.spot_recipe_v2_core.acquisition import evaluate_acquisition
from .test_lifecycle import pending, scope, snapshot
from .test_native_views import assess, exit_inputs


def opportunity(event=1000, **updates):
    binding = dict(program_id='fixture_program_v2', owner_id='spot_v2_owner', connector='okx_paper_trade',
                   trading_pair='BTC-USDC', recipe_sha256='a' * 64, acquisition_allowed=True)
    binding.update(updates)
    return Opportunity(scope=DecisionScope(**binding), setup='fixture_setup', action='acquire',
        primary=ObservationRef(dataset_sha256='1' * 64, epoch='fixture', revision=1,
            content_sha256='2' * 64, event_time_ms=event, available_time_ms=event),
        created_at_ms=event, lifetime_ms=100)


def ready(**updates):
    return snapshot(remaining_base=D('0'), entry_gross_quote=D('0'), entry_terminal=False, **updates)


def seed(view=None, purpose='initial_entry'):
    view = ready() if view is None else view
    return evaluate_acquisition(view, None, opportunity(999), purpose=purpose,
                                now_ms=1002, max_age_ms=10, prerequisites=()).cursor


def evaluate(view, **updates):
    args = dict(purpose='initial_entry', now_ms=1002, max_age_ms=10, prerequisites=()) | updates
    return evaluate_acquisition(view, seed(), opportunity(), **args)


def test_real_canonical_cursor_and_trace_are_reused():
    first = evaluate_acquisition(ready(), None, opportunity(999), purpose='initial_entry',
                                now_ms=1002, max_age_ms=10, prerequisites=())
    assert first.trace.reason == 'seeded'
    next_result = evaluate(ready())
    assert next_result.trace.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert next_result.trace.execution_authorized is False
    assert next_result.economic_backtest is False
    assert next_result.native_reason == 'eligible'


@pytest.mark.parametrize('view,purpose,reason', [
    (snapshot(), 'initial_entry', 'initial_entry_requires_ready_scope'),
    (ready(), 'dca', 'dca_requires_managing_inventory'),
    (snapshot(exit_gross_base=D('.1')), 'dca', 'acquisition_after_liquidation_unsupported'),
    (snapshot(terminal=True), 'dca', 'dca_requires_managing_inventory'),
    (snapshot(commitments=(pending(),)), 'dca', 'native_commitment_uncertain'),
    (snapshot(commitments=(pending(status='PENDING'),)), 'dca', 'native_request_pending'),
    (snapshot(commitments=(pending('reduce', status='PENDING'),)), 'dca', 'native_request_pending'),
    (snapshot(fees_complete=False), 'dca', 'native_fees_incomplete'),
    (snapshot(reconciled=False), 'dca', 'native_reconciliation_required'),
])
def test_lifecycle_vetoes_do_not_create_or_expand_an_acquisition(view, purpose, reason):
    result = evaluate(view, purpose=purpose)
    assert result.native_reason == reason
    assert result.trace.status == 'HOLD'
    assert result.trace.execution_authorized is False


def test_dca_only_qualifies_inventory_before_liquidation():
    result = evaluate(snapshot(), purpose='dca')
    assert result.trace.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert result.native_reason == 'eligible'


def test_external_native_risk_veto_wins_over_lifecycle_pass():
    result = evaluate(ready(), prerequisites=(Prerequisite(name='capital', status='HOLD'),))
    assert result.trace.status == 'HOLD'
    assert 'capital:HOLD' in result.trace.blockers


def test_same_event_after_hold_or_restart_cannot_be_rearmed():
    blocked = evaluate(ready(reconciled=False))
    restored = type(blocked.cursor).model_validate_json(blocked.cursor.model_dump_json())
    repeated = evaluate_acquisition(ready(), restored, opportunity(), purpose='initial_entry',
                                   now_ms=1003, max_age_ms=10, prerequisites=())
    assert repeated.trace.status == 'HOLD'
    assert repeated.trace.reason == 'already_observed'


def test_initial_entry_pause_does_not_block_native_exit_assessment():
    paused = evaluate_acquisition(snapshot(), None, opportunity(acquisition_allowed=False), purpose='initial_entry',
                                 now_ms=1002, max_age_ms=10, prerequisites=())
    assert paused.trace.reason == 'acquisition_forbidden'
    assert assess(exit_inputs()).status == 'PROPOSED_REDUCTION'


@pytest.mark.parametrize('updates', [
    {'program_id': 'other'}, {'owner_id': 'foreign'}, {'connector': 'okx'},
    {'recipe_sha256': 'f' * 64}, {'trading_pair': 'ETH-USDC'},
])
def test_opportunity_must_match_native_program_recipe_and_scope(updates):
    with pytest.raises(ValueError, match='scope'):
        evaluate_acquisition(ready(), seed(), opportunity(**updates), purpose='initial_entry',
                             now_ms=1002, max_age_ms=10, prerequisites=())


def test_caller_cannot_override_the_reserved_native_prerequisite():
    with pytest.raises(ValueError):
        evaluate(ready(), prerequisites=(Prerequisite(name='spot_lifecycle_v2', status='PASS', evidence_sha256='f' * 64),))


def test_reduction_cannot_be_sent_through_the_entry_gate():
    with pytest.raises(ValueError):
        evaluate_acquisition(ready(), seed(), opportunity().model_copy(update={'action': 'reduce'}),
                             purpose='initial_entry', now_ms=1002, max_age_ms=10, prerequisites=())


def test_unknown_pyramiding_policy_is_not_silently_treated_as_dca():
    with pytest.raises(ValueError):
        evaluate(ready(), purpose='pyramiding')


def test_shadow_cursor_is_explicit_and_does_not_mutate_the_baseline_cursor():
    baseline = seed()
    encoded = baseline.model_dump_json()
    evaluate_acquisition(ready(), baseline, opportunity(), purpose='initial_entry',
                         now_ms=1002, max_age_ms=10, prerequisites=())
    assert baseline.model_dump_json() == encoded


def test_permission_change_cannot_silently_rebind_an_existing_cursor():
    with pytest.raises(ValueError, match='cursor scope'):
        evaluate_acquisition(snapshot(), seed(), opportunity(acquisition_allowed=False),
            purpose='initial_entry', now_ms=1002, max_age_ms=10, prerequisites=())
    assert assess(exit_inputs()).status == 'PROPOSED_REDUCTION'


def test_invalid_cursor_has_a_typed_failure():
    with pytest.raises(ValueError):
        evaluate_acquisition(ready(), object(), opportunity(), purpose='initial_entry',
            now_ms=1002, max_age_ms=10, prerequisites=())
