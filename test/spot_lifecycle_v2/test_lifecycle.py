from decimal import Decimal as D

import pytest
from pydantic import ValidationError

from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding
from hummingbot.strategy_v2.components.spot_lifecycle_v2 import (
    NativeCommitmentV2,
    NativeScopeV2,
    NativeSnapshotV2,
    NativeTerminalV2,
    decode_snapshot,
    encode_snapshot,
    management_pairs,
    project_lifecycle,
    validate_continuity,
)


def scope(**updates):
    values = dict(program_id='fixture_program_v2', owner_id='spot_v2_owner', executor_id='lot_1', connector='okx_paper_trade',
                  trading_pair='BTC-USDC', execution_mode='paper',
                  recipe_binding=RecipeBinding(recipe_id='fixture_v2', recipe_sha256='a' * 64,
                                               source_sha256='b' * 64, configuration_sha256='c' * 64))
    return NativeScopeV2(**(values | updates))


def snapshot(**updates):
    values = dict(scope=scope(), receipt_sha256='d' * 64, sequence=1, observed_at_ms=1000,
                  available_at_ms=1001, remaining_base=D('2'), entry_gross_quote=D('200'),
                  exit_gross_base=D('0'), entry_terminal=True, terminal=False,
                  reconciled=True, fees_complete=True, accounting_owner='executor',
                  commitments=(), terminal_requests=())
    return NativeSnapshotV2(**(values | updates))


def pending(action='acquire', **updates):
    values = dict(request_id='request_1', action=action, status='UNCERTAIN',
                  remaining_quote=D('25') if action == 'acquire' else D('0'),
                  remaining_base=D('0') if action == 'acquire' else D('.5'))
    return NativeCommitmentV2(**(values | updates))


def test_default_projection_uses_native_inventory_not_an_entry_signal():
    view = snapshot()
    result = project_lifecycle(view, now_ms=1002, max_age_ms=10)
    assert result.phase == 'MANAGING'
    assert result.management_required is True
    assert result.execution_authorized is False
    assert result.snapshot_sha256 == view.snapshot_sha256
    assert result.blockers == ()


@pytest.mark.parametrize('updates,phase', [
    ({'commitments': (pending(),)}, 'ACQUISITION_UNCERTAIN'),
    ({'commitments': (pending(status='PENDING'),)}, 'ACQUIRING'),
    ({'commitments': (pending('reduce'),)}, 'EXIT_UNCERTAIN'),
    ({'commitments': (pending('reduce', status='PENDING'),)}, 'EXIT_PENDING'),
    ({'terminal': True}, 'RETAINED'),
    ({'accounting_owner': 'held_position'}, 'RETAINED'),
    ({'remaining_base': D('.00000001'), 'terminal': True}, 'RETAINED'),
    ({'remaining_base': D('0'), 'terminal': True}, 'CLOSED'),
    ({'remaining_base': D('0'), 'entry_gross_quote': D('0'), 'entry_terminal': False}, 'READY'),
])
def test_native_phase_projection(updates, phase):
    result = project_lifecycle(snapshot(**updates), now_ms=1002, max_age_ms=10)
    assert result.phase == phase
    assert result.execution_authorized is False


@pytest.mark.parametrize('updates,reason', [
    ({'reconciled': False}, 'native_reconciliation_required'),
    ({'fees_complete': False}, 'native_fees_incomplete'),
    ({'available_at_ms': 1010}, 'native_snapshot_not_available'),
    ({'observed_at_ms': 900}, 'native_snapshot_stale'),
])
def test_unknown_or_old_flat_state_never_closes_obligations(updates, reason):
    view = snapshot(remaining_base=D('0'), terminal=True, **updates)
    result = project_lifecycle(view, now_ms=1002, max_age_ms=10)
    assert result.phase == 'RECONCILIATION_REQUIRED'
    assert result.management_required is True
    assert reason in result.blockers


def test_publication_does_not_refresh_old_observation():
    assert 'native_snapshot_stale' in project_lifecycle(
        snapshot(observed_at_ms=1), now_ms=1002, max_age_ms=10).blockers


def test_uncertain_commitment_survives_terminal_executor_and_large_clock_advance():
    view = snapshot(terminal=True, commitments=(pending(),))
    result = project_lifecycle(view, now_ms=1000000, max_age_ms=10)
    assert result.management_required
    assert view.commitments[0].remaining_quote == D('25')
    assert 'native_commitment_uncertain' in result.blockers


def test_conflicting_acquire_and_reduce_are_held():
    result = project_lifecycle(snapshot(commitments=(pending(), pending('reduce', request_id='r2'))),
                               now_ms=1002, max_age_ms=10)
    assert 'conflicting_native_commitments' in result.blockers
    assert result.management_required


@pytest.mark.parametrize('field,value', [
    ('sequence', True), ('sequence', '1'), ('observed_at_ms', 1.2),
    ('available_at_ms', -1), ('available_at_ms', 999), ('fees_complete', 'true'),
    ('reconciled', 1), ('terminal', 1), ('remaining_base', True),
    ('remaining_base', 1.5), ('remaining_base', 'NaN'), ('remaining_base', 'Infinity'),
    ('remaining_base', '-.1'), ('remaining_base', '1e999999'),
    ('entry_gross_quote', '-1'), ('entry_gross_quote', '0'),
])
def test_invalid_snapshot_fields_rejected(field, value):
    with pytest.raises((ValueError, TypeError)):
        snapshot(**{field: value})


@pytest.mark.parametrize('updates', [
    {'connector': 'binance'}, {'execution_mode': 'live'},
    {'trading_pair': 'BTC-BTC'}, {'owner_id': ''}, {'capital_model': 'PERPETUAL_MARGIN'},
])
def test_scope_is_bounded_okx_spot_not_a_venue_or_inventory_adapter(updates):
    with pytest.raises((ValueError, TypeError)):
        scope(**updates)


def test_nested_binding_is_revalidated_not_trusted_because_frozen():
    binding = scope().recipe_binding.model_copy(update={'source_sha256': 'not-a-hash'})
    with pytest.raises(ValueError):
        scope(recipe_binding=binding)


def test_duplicate_requests_and_open_terminal_overlap_rejected():
    with pytest.raises(ValueError):
        snapshot(commitments=(pending(), pending()))
    terminal = NativeTerminalV2(request_id='request_1', receipt_sha256='e' * 64)
    with pytest.raises(ValueError):
        snapshot(commitments=(pending(),), terminal_requests=(terminal,))


@pytest.mark.parametrize('updates', [
    {'remaining_quote': '0'}, {'remaining_quote': '-1'}, {'expires_at_ms': 1},
    {'status': 'CANCEL_REQUESTED'}, {'request_id': ''}, {'action': 'short'},
])
def test_invalid_or_expiring_commitment_contract_rejected(updates):
    with pytest.raises((ValueError, TypeError)):
        pending(**updates)


def test_decision_boundaries_revalidate_copied_models():
    with pytest.raises(ValueError):
        project_lifecycle(snapshot().model_copy(update={'remaining_base': D('-1')}), now_ms=1002, max_age_ms=10)


@pytest.mark.parametrize('now,age', [(True, 10), (1002, True), (1002, 0), (1002, 60001), (-1, 10)])
def test_explicit_clock_and_bounds_are_strict(now, age):
    with pytest.raises(ValueError):
        project_lifecycle(snapshot(), now_ms=now, max_age_ms=age)


def test_roundtrip_and_duplicate_json_denial():
    view = snapshot(commitments=(pending(),))
    assert decode_snapshot(encode_snapshot(view)) == view
    with pytest.raises(ValueError, match='duplicate'):
        decode_snapshot('{"sequence":1,"sequence":2}')
    with pytest.raises(ValueError):
        decode_snapshot(' ' * 131073)
    with pytest.raises(ValueError):
        decode_snapshot('{"value":NaN}')


def test_continuity_cannot_erase_an_uncertain_request_without_native_terminal_receipt():
    old = snapshot(commitments=(pending(),))
    new = snapshot(sequence=2, receipt_sha256='e' * 64)
    with pytest.raises(ValueError, match='terminal'):
        validate_continuity(old, new)
    terminal = NativeTerminalV2(request_id='request_1', receipt_sha256='f' * 64)
    closed_request = snapshot(sequence=2, receipt_sha256='e' * 64, terminal_requests=(terminal,))
    validate_continuity(old, closed_request)


def test_identical_redelivery_is_idempotent_but_revision_at_same_sequence_is_not():
    old = snapshot()
    validate_continuity(old, old)
    with pytest.raises(ValueError, match='sequence'):
        validate_continuity(old, snapshot(remaining_base=D('1')))


@pytest.mark.parametrize('updates', [
    {'sequence': 1}, {'observed_at_ms': 999}, {'available_at_ms': 1000},
    {'scope': scope(executor_id='another_lot')},
])
def test_restore_scope_sequence_and_clock_cannot_regress(updates):
    old = snapshot(sequence=2)
    values = dict(sequence=3, observed_at_ms=1000, available_at_ms=1001) | updates
    with pytest.raises(ValueError):
        validate_continuity(old, snapshot(**values))


def test_management_universe_retains_inventory_after_entry_deselection():
    assert management_pairs((snapshot(),), now_ms=1002, max_age_ms=10) == ('BTC-USDC',)
    flat = snapshot(remaining_base=D('0'), terminal=True)
    assert management_pairs((flat,), now_ms=1002, max_age_ms=10) == ()
    uncertain = snapshot(remaining_base=D('0'), terminal=True, fees_complete=False)
    assert management_pairs((uncertain,), now_ms=1002, max_age_ms=10) == ('BTC-USDC',)
    with pytest.raises(ValueError):
        management_pairs((flat, flat), now_ms=1002, max_age_ms=10)


def test_frozen_snapshot_rejects_mutation():
    with pytest.raises(ValidationError):
        snapshot().remaining_base = D('0')


def test_stale_flat_snapshot_stays_in_management():
    view = snapshot(remaining_base=D('0'), terminal=True)
    assert management_pairs((view,), now_ms=2000, max_age_ms=10) == ('BTC-USDC',)


def test_invalid_decimal_text_has_a_typed_validation_failure():
    with pytest.raises(ValueError):
        snapshot(remaining_base='not-money')


def test_sold_inventory_without_native_acquisition_provenance_rejects():
    with pytest.raises(ValueError):
        snapshot(remaining_base=D('0'), entry_gross_quote=D('0'), exit_gross_base=D('1'), terminal=True)


@pytest.mark.parametrize('field', ['sequence', 'observed_at_ms', 'available_at_ms'])
def test_native_counters_and_clocks_are_int64_bounded(field):
    with pytest.raises(ValueError):
        snapshot(**{field: 2 ** 63})


def test_same_request_cannot_change_action_on_replay():
    old = snapshot(commitments=(pending(),))
    new = snapshot(sequence=2, commitments=(pending('reduce'),))
    with pytest.raises(ValueError, match='action'):
        validate_continuity(old, new)
