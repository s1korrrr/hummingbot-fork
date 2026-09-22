import json
from decimal import Decimal as D

import pytest

from controllers.generic.spot_recipe_v2_core.acquisition import evaluate_acquisition
from controllers.generic.spot_recipe_v2_core.lifecycle import (
    LifecycleCheckpointV2,
    NativeTrailingViewV2,
    bind_trailing_view,
    capture_checkpoint,
    decode_checkpoint,
    encode_checkpoint,
    protective_exit_price,
    resolve_exit,
    restore_checkpoint,
)
from controllers.generic.spot_recipe_v2_core.spot_lifecycle_v2 import (
    NativeTerminalV2,
    management_pairs,
    project_lifecycle,
)
from hummingbot.strategy_v2.components.config import TrailingPolicy
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.trailing import protected_sell_price

from .test_acquisition import opportunity
from .test_lifecycle import pending, scope, snapshot
from .test_native_views import exit_inputs


def trailing(snap=None, **updates):
    snap = snapshot() if snap is None else snap
    values = dict(owner_id=snap.scope.owner_id, executor_id=snap.scope.executor_id,
                  receipt_sha256=snap.receipt_sha256, observed_at_ms=snap.observed_at_ms,
                  activated=True, peak_price=D('110'), threshold_price=D('108'),
                  exit_owner='native_executor')
    return NativeTrailingViewV2(**(values | updates))


def resolve(inputs, views=None, **kwargs):
    snap, view, proposals, floor = inputs
    views = (trailing(snap),) if views is None else views
    kwargs.setdefault('policy', TrailingPolicy())
    kwargs.setdefault('previous_trailing', None)
    return resolve_exit(snap, views, view, proposals, floor, now_ms=1002, max_age_ms=10, **kwargs)


def clearing(snap=None, **updates):
    """Priced inside the ratchet-to-bid window, so quantity arbitration is what the assertion tests."""
    return exit_inputs(snap, minimum_price=D('109'), **updates)


def checkpoint(snap=None, **kwargs):
    snap = snapshot() if snap is None else snap
    kwargs.setdefault('previous', None)
    return capture_checkpoint(snap, (trailing(snap),), now_ms=1002, max_age_ms=10, **kwargs)


def test_rising_volatility_cannot_lower_the_ratcheted_threshold():
    snap = snapshot()
    previous = trailing(snap)
    loosened = trailing(snap, threshold_price=D('104'))
    with pytest.raises(ValueError, match='ratchet'):
        bind_trailing_view(snap, (loosened,), now_ms=1002, max_age_ms=10, previous=previous)
    widened_peak = trailing(snap, peak_price=D('118'), threshold_price=D('108'))
    result = bind_trailing_view(snap, (widened_peak,), now_ms=1002, max_age_ms=10, previous=previous)
    assert result.threshold_price == D('108')
    assert result.peak_price == D('118')


def test_peak_high_water_and_activation_cannot_be_withdrawn():
    snap = snapshot()
    previous = trailing(snap)
    with pytest.raises(ValueError, match='ratchet'):
        bind_trailing_view(snap, (trailing(snap, peak_price=D('109')),), now_ms=1002, max_age_ms=10,
                           previous=previous)
    with pytest.raises(ValueError, match='activation'):
        bind_trailing_view(snap, (trailing(snap, activated=False),), now_ms=1002, max_age_ms=10,
                           previous=previous)


def test_protective_price_only_tightens_and_reuses_the_canonical_calculation():
    policy = TrailingPolicy()
    view = trailing()
    canonical = protected_sell_price(D('100'), policy, D('.01'))
    assert protective_exit_price(view, D('100'), policy=policy, price_step=D('.01')) == view.threshold_price
    loose = view.model_copy(update={'threshold_price': D('1')})
    assert protective_exit_price(loose, D('100'), policy=policy, price_step=D('.01')) == canonical
    assert canonical > D('100')


def test_threshold_above_observed_peak_is_not_a_valid_trailing_record():
    with pytest.raises(ValueError):
        trailing(threshold_price=D('111'))


def test_trailing_record_is_frozen_and_never_authorizes_execution():
    view = trailing()
    with pytest.raises(ValueError):
        view.threshold_price = D('1')
    assert resolve(exit_inputs()).execution_authorized is False


@pytest.mark.parametrize('updates,message', [
    ({'executor_id': 'lot_2'}, 'another lot'),
    ({'owner_id': 'foreign'}, 'another lot'),
    ({'receipt_sha256': 'f' * 64}, 'another lot'),
    ({'observed_at_ms': 999}, 'differs'),
])
def test_another_lots_trailing_state_is_never_silently_reused(updates, message):
    with pytest.raises(ValueError, match=message):
        bind_trailing_view(snapshot(), (trailing(**updates),), now_ms=1002, max_age_ms=10, previous=None)


def test_stale_trailing_observation_rejected():
    snap = snapshot(observed_at_ms=1, available_at_ms=1)
    with pytest.raises(ValueError, match='stale'):
        bind_trailing_view(snap, (trailing(snap),), now_ms=1002, max_age_ms=10, previous=None)


def test_new_acquisition_cannot_inherit_another_lots_trailing_lineage():
    other = snapshot(scope=scope(executor_id='lot_2'))
    with pytest.raises(ValueError, match='lineage'):
        bind_trailing_view(other, (trailing(other),), now_ms=1002, max_age_ms=10,
                           previous=trailing())


def test_overlapping_exit_requests_produce_one_bounded_maximum_request():
    result = resolve(clearing())
    assert result.status == 'PROPOSED_REDUCTION'
    assert result.amount_base == D('1.2')
    assert result.policy_ids == ('exhaustion', 'trail')
    assert result.market_fallback is False


def test_a_checkpointed_request_cannot_rebind_to_a_different_action_under_the_same_id():
    """An id is not an identity. A persisted reduction must not come back as a live acquisition."""
    snap = snapshot(commitments=(pending('reduce', status='PENDING'),))
    raw = encode_checkpoint(checkpoint(snap))
    assert restore_checkpoint(raw, snap).status == 'BOUND_REQUEST'
    swapped = snap.model_copy(update={'commitments': (pending('acquire', status='PENDING'),)})
    with pytest.raises(PolicyError, match='action changed since the checkpoint'):
        restore_checkpoint(raw, swapped)


def test_a_reduction_priced_below_the_ratchet_is_held_not_silently_loosened():
    """The bound threshold is a price floor. Selling under it is the loosening F07 forbids."""
    held = resolve(exit_inputs())
    assert held.status == 'HOLD'
    assert held.reason == 'native_trailing_floor_not_met'
    assert resolve(clearing()).status == 'PROPOSED_REDUCTION'


def test_the_ratchet_floor_tightens_with_the_peak_and_never_with_the_proposal():
    snap = snapshot()
    inputs = clearing(snap)
    ratcheted = trailing(snap, peak_price=D('112'), threshold_price=D('110'))
    assert resolve(inputs, views=(ratcheted,)).status == 'HOLD'
    assert resolve(inputs, views=(trailing(snap),)).status == 'PROPOSED_REDUCTION'


def test_duplicate_trailing_owner_holds_before_arbitration():
    snap = snapshot()
    views = (trailing(snap), trailing(snap, exit_owner='recipe_proposal'))
    result = resolve(exit_inputs(snap), views)
    assert result.status == 'HOLD'
    assert result.reason == 'duplicate_native_trailing_owner'
    assert result.amount_base is None


def test_a_proposal_layer_may_not_own_submission():
    snap = snapshot()
    result = resolve(exit_inputs(snap), (trailing(snap, exit_owner='recipe_proposal'),))
    assert result.status == 'HOLD'
    assert result.reason == 'native_trailing_submission_not_owned'


def test_ownership_guard_runs_before_evidence_binding():
    snap = snapshot()
    foreign = trailing(snap, executor_id='lot_2', exit_owner='recipe_proposal')
    assert resolve(exit_inputs(snap), (foreign,)).reason == 'native_trailing_submission_not_owned'


def test_mismatched_trailing_evidence_raises_rather_than_quietly_holding():
    snap = snapshot()
    with pytest.raises(ValueError, match='another lot'):
        resolve(exit_inputs(snap), (trailing(snap, receipt_sha256='f' * 64),))


def test_resolution_carries_the_ratchet_across_a_restart():
    snap, view, proposals, floor = exit_inputs()
    loosened = trailing(snap, threshold_price=D('104'))
    with pytest.raises(ValueError, match='ratchet'):
        resolve_exit(snap, (loosened,), view, proposals, floor, now_ms=1002, max_age_ms=10,
                     policy=TrailingPolicy(),
                     previous_trailing=trailing(snap))
    assert resolve_exit(snap, (trailing(snap),), view, proposals, None, now_ms=1002, policy=TrailingPolicy(),
                        previous_trailing=None, max_age_ms=10).reason == 'native_floor_unavailable'


def test_held_inventory_outside_the_entry_universe_is_still_managed():
    snap = snapshot()
    assert management_pairs((snap,), now_ms=1002, max_age_ms=10) == ('BTC-USDC',)
    assert project_lifecycle(snap, now_ms=1002, max_age_ms=10).management_required is True
    assert resolve(clearing(snap)).status == 'PROPOSED_REDUCTION'


def test_entry_pause_or_stale_alpha_does_not_suspend_a_pending_native_exit():
    snap = snapshot(commitments=(pending('reduce', status='PENDING'),))
    paused = evaluate_acquisition(snap, None, opportunity(acquisition_allowed=False), purpose='dca',
                                  now_ms=1002, max_age_ms=10, prerequisites=())
    assert paused.trace.status == 'HOLD'
    assert paused.native_reason == 'native_request_pending'
    stale = evaluate_acquisition(snap, None, opportunity(600), purpose='dca', now_ms=1002,
                                 max_age_ms=10, prerequisites=())
    assert stale.trace.reason == 'opportunity_expired'
    projection = project_lifecycle(snap, now_ms=1002, max_age_ms=10)
    assert projection.phase == 'EXIT_PENDING'
    assert projection.management_required is True
    assert management_pairs((snap,), now_ms=1002, max_age_ms=10) == ('BTC-USDC',)
    assert resolve(exit_inputs(snap)).reason == 'native_request_pending'
    assert capture_checkpoint(snap, (trailing(snap),), now_ms=1002, max_age_ms=10,
                              previous=None).outstanding_request_ids == ('request_1',)


def test_dust_or_uncertain_fees_without_an_active_executor_is_not_closed():
    dust = snapshot(remaining_base=D('.00000001'), terminal=True)
    assert project_lifecycle(dust, now_ms=1002, max_age_ms=10).phase == 'RETAINED'
    unclear = snapshot(remaining_base=D('0'), terminal=True, fees_complete=False)
    projection = project_lifecycle(unclear, now_ms=1002, max_age_ms=10)
    assert projection.phase == 'RECONCILIATION_REQUIRED'
    assert projection.management_required is True
    assert management_pairs((dust, unclear.model_copy(update={'scope': scope(executor_id='lot_2')}),),
                            now_ms=1002, max_age_ms=10) == ('BTC-USDC',)


def test_checkpoint_binds_recipe_scope_cursor_requests_and_trailing_lineage():
    snap = snapshot(commitments=(pending('reduce', status='PENDING'),))
    record = checkpoint(snap, cursor_sha256='1' * 64)
    assert record.scope.recipe_binding == snap.scope.recipe_binding
    assert record.outstanding_request_ids == ('request_1',)
    assert record.cursor_sha256 == '1' * 64
    assert record.execution_authorized is False
    restored = decode_checkpoint(encode_checkpoint(record))
    assert restored == record
    assert restored.trailing.peak_price == D('110')
    assert restored.trailing.threshold_price == D('108')


def test_trigger_partial_fill_cancel_and_late_fill_keep_one_bound_request():
    snap = snapshot(remaining_base=D('2'), commitments=(pending('reduce', status='PENDING'),))
    raw = encode_checkpoint(checkpoint(snap))
    later = snapshot(sequence=2, remaining_base=D('1.5'), exit_gross_base=D('.5'),
                     commitments=(pending('reduce', status='PENDING', remaining_base=D('.2')),))
    decision = restore_checkpoint(raw, later, previous=snap)
    assert decision.status == 'BOUND_REQUEST'
    assert decision.reason == 'native_request_rebound'
    assert decision.request_ids == ('request_1',)
    assert decision.execution_authorized is False


def test_crash_before_persistence_never_adopts_an_unbound_native_request():
    raw = encode_checkpoint(checkpoint())
    after = snapshot(sequence=2, commitments=(pending('reduce', status='PENDING'),))
    decision = restore_checkpoint(raw, after)
    assert decision.status == 'UNCERTAINTY_HOLD'
    assert decision.reason == 'unbound_native_request_present'
    assert decision.request_ids == ()


def test_crash_after_persistence_before_acknowledgement_holds_on_uncertainty():
    snap = snapshot(commitments=(pending('reduce', status='PENDING'),))
    raw = encode_checkpoint(checkpoint(snap))
    vanished = snapshot(sequence=2)
    decision = restore_checkpoint(raw, vanished)
    assert decision.status == 'UNCERTAINTY_HOLD'
    assert decision.reason == 'bound_native_request_unresolved'
    assert decision.request_ids == ('request_1',)
    unknown = snapshot(sequence=2, commitments=(pending('reduce'),))
    assert restore_checkpoint(raw, unknown).reason == 'native_commitment_uncertain'


def test_a_terminal_receipt_resolves_the_bound_request_without_rebinding_it():
    snap = snapshot(commitments=(pending('reduce', status='PENDING'),))
    raw = encode_checkpoint(checkpoint(snap))
    ended = snapshot(sequence=2, remaining_base=D('1.5'), exit_gross_base=D('.5'),
                     terminal_requests=(NativeTerminalV2(request_id='request_1', receipt_sha256='e' * 64),))
    decision = restore_checkpoint(raw, ended, previous=snap)
    assert decision.status == 'UNCERTAINTY_HOLD'
    assert decision.reason == 'no_bound_native_request'


def test_restore_reuses_continuity_when_a_previous_snapshot_is_available():
    snap = snapshot(commitments=(pending('reduce', status='PENDING'),))
    raw = encode_checkpoint(checkpoint(snap))
    erased = snapshot(sequence=2, receipt_sha256='e' * 64)
    with pytest.raises(ValueError, match='terminal'):
        restore_checkpoint(raw, erased, previous=snap)
    with pytest.raises(ValueError, match='checkpoint'):
        restore_checkpoint(raw, erased, previous=snapshot(sequence=1, receipt_sha256='e' * 64))


@pytest.mark.parametrize('updates,message', [
    ({'scope': scope(executor_id='lot_2')}, 'scope or recipe changed'),
    ({'scope': scope(recipe_binding=scope().recipe_binding.model_copy(
        update={'recipe_sha256': 'f' * 64}))}, 'scope or recipe changed'),
    ({'scope': scope(recipe_binding=scope().recipe_binding.model_copy(
        update={'source_sha256': 'f' * 64}))}, 'scope or recipe changed'),
    ({'sequence': 1, 'observed_at_ms': 1000}, 'sequence regressed'),
])
def test_restore_rejects_changed_recipe_source_scope_or_regressed_sequence(updates, message):
    raw = encode_checkpoint(checkpoint(snapshot(sequence=2)))
    with pytest.raises(ValueError, match=message):
        restore_checkpoint(raw, snapshot(**(dict(sequence=3) | updates)))


def test_the_persisted_ratchet_cannot_be_loosened_by_the_writer_or_lost_on_restore():
    snap = snapshot(commitments=(pending('reduce', status='PENDING'),))
    previous = trailing(snap)
    with pytest.raises(ValueError, match='ratchet'):
        capture_checkpoint(snap, (trailing(snap, threshold_price=D('104')),), now_ms=1002,
                           max_age_ms=10, previous=previous)
    raw = encode_checkpoint(capture_checkpoint(snap, (trailing(snap),), now_ms=1002, max_age_ms=10,
                                               previous=previous))
    later = snapshot(sequence=2, receipt_sha256='e' * 64,
                     commitments=(pending('reduce', status='PENDING'),))
    decision = restore_checkpoint(raw, later)
    assert decision.trailing.threshold_price == D('108')
    assert decision.trailing.peak_price == D('110')
    with pytest.raises(ValueError, match='ratchet'):
        bind_trailing_view(later, (trailing(later, threshold_price=D('104')),), now_ms=1002,
                           max_age_ms=10, previous=decision.trailing)


def test_restore_rejects_a_changed_receipt_at_the_checkpoint_sequence():
    raw = encode_checkpoint(checkpoint())
    with pytest.raises(ValueError, match='receipt changed'):
        restore_checkpoint(raw, snapshot(receipt_sha256='e' * 64))


def test_checkpoint_decoding_is_bounded_and_unambiguous():
    with pytest.raises(ValueError, match='bounded'):
        decode_checkpoint('x' * 131073)
    with pytest.raises(ValueError, match='duplicate'):
        decode_checkpoint('{"sequence":1,"sequence":2}')
    with pytest.raises(ValueError):
        decode_checkpoint('{"value":NaN}')
    with pytest.raises(ValueError):
        restore_checkpoint('{}', snapshot())


@pytest.mark.parametrize('field', ['peak_price', 'threshold_price'])
def test_persisted_trailing_prices_reject_inexact_json_numbers(field):
    raw = json.loads(encode_checkpoint(checkpoint()))
    raw['trailing'][field] = 108.5
    with pytest.raises(ValueError, match='amounts'):
        decode_checkpoint(json.dumps(raw))


def test_checkpoint_rejects_trailing_state_from_another_receipt_or_lot():
    snap = snapshot()
    record = checkpoint(snap)
    with pytest.raises(ValueError):
        LifecycleCheckpointV2(**(record.model_dump(mode='python')
                                 | {'receipt_sha256': 'e' * 64}))
    with pytest.raises(ValueError):
        LifecycleCheckpointV2(**(record.model_dump(mode='python')
                                 | {'scope': scope(executor_id='lot_2')}))


def test_checkpoint_capture_requires_a_single_bound_trailing_owner():
    snap = snapshot()
    with pytest.raises(ValueError, match='one native trailing owner'):
        capture_checkpoint(snap, (trailing(snap), trailing(snap)), now_ms=1002, max_age_ms=10, previous=None)
    with pytest.raises(ValueError, match='one native trailing owner'):
        capture_checkpoint(snap, (), now_ms=1002, max_age_ms=10, previous=None)


@pytest.mark.parametrize('now,age', [(True, 10), (1002, 0), (1002, 60001), (-1, 10)])
def test_trailing_clock_and_bounds_are_strict(now, age):
    snap = snapshot()
    with pytest.raises(ValueError):
        bind_trailing_view(snap, (trailing(snap),), now_ms=now, max_age_ms=age, previous=None)


@pytest.mark.parametrize('views', ['not-a-tuple', (object(),), (trailing(),) * 5, ()])
def test_only_bounded_canonical_trailing_evidence_is_accepted(views):
    with pytest.raises(ValueError):
        resolve_exit(snapshot(), views, *exit_inputs()[1:], now_ms=1002, max_age_ms=10,
                     policy=TrailingPolicy(), previous_trailing=None)


@pytest.mark.parametrize('proposals', ['not-a-tuple', (object(),)])
def test_uncanonical_exit_proposals_fail_before_hashing(proposals):
    snap, view, _, floor = exit_inputs()
    with pytest.raises(ValueError, match='canonical native exit evidence'):
        resolve_exit(snap, (trailing(snap),), view, proposals, floor, now_ms=1002, max_age_ms=10,
                     policy=TrailingPolicy(), previous_trailing=None)


@pytest.mark.parametrize('call', [
    lambda snap: bind_trailing_view(snap, (trailing(snap),), now_ms=1002, max_age_ms=10),
    lambda snap: capture_checkpoint(snap, (trailing(snap),), now_ms=1002, max_age_ms=10),
    lambda snap: resolve_exit(snap, (trailing(snap),), *exit_inputs(snap)[1:], now_ms=1002,
                              max_age_ms=10, policy=TrailingPolicy()),
])
def test_a_caller_cannot_omit_the_prior_ratchet_and_get_an_unguarded_bind(call):
    """An opt-in ratchet is not a ratchet: no lineage argument means no call, not a silent reset."""
    with pytest.raises(TypeError, match='previous'):
        call(snapshot())
