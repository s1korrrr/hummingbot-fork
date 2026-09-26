import json
from decimal import Decimal as D

import pytest

from controllers.generic.spot_recipe_v2_core.lifecycle import (
    NativeTrailingViewV2,
    bind_trailing_view,
    capture_checkpoint,
    decode_checkpoint,
    encode_checkpoint,
)
from controllers.generic.spot_recipe_v2_core.native_trailing import capture_native_trailing_view
from controllers.generic.spot_recipe_v2_core.recipe import source_files
from hummingbot.strategy_v2.components.domain import PolicyError

from .test_native_views import capture, native_info


def trailing_info(**updates):
    info = native_info()
    info.custom_info.update({
        'entry_price': D('100'),
        'current_position_average_price': D('100'),
        'trailing_state': 'armed',
        'trailing_delta_pct': D('.02'),
        'trailing_stop_trigger_pct': D('.08'),
        'trailing_trigger_price': D('109'),
        'trailing_current_price': D('112'),
        'trailing_move_count': 0,
        'trailing_armed_timestamp': 1000.0,
        'trailing_last_update_timestamp': 1001.0,
    })
    info.custom_info.update(updates)
    return info


def unarmed_info(**updates):
    values = dict(trailing_state='waiting', trailing_stop_trigger_pct=None,
                  trailing_trigger_price=None, trailing_move_count=0,
                  trailing_armed_timestamp=None, trailing_last_update_timestamp=None)
    return trailing_info(**(values | updates))


def test_native_executor_projection_produces_bound_threshold_without_fabricating_peak():
    info = trailing_info()
    snap = capture(info)
    view = capture_native_trailing_view(info, snap)
    assert view.owner_id == snap.scope.owner_id
    assert view.executor_id == snap.scope.executor_id
    assert view.receipt_sha256 == snap.receipt_sha256
    assert view.observed_at_ms == snap.observed_at_ms
    assert view.activated is True
    assert view.threshold_price == D('109')
    assert view.peak_price is None
    assert view.exit_owner == 'native_executor'


def test_unarmed_native_trailing_state_is_not_fabricated_into_a_ratchet():
    info = unarmed_info()
    assert capture_native_trailing_view(info, capture(info)) is None


@pytest.mark.parametrize('updates', [
    {'trailing_stop_trigger_pct': D('.01')},
    {'trailing_trigger_price': D('101')},
    {'trailing_move_count': 1},
    {'trailing_armed_timestamp': 1000.0},
    {'trailing_last_update_timestamp': 1000.0},
])
def test_unarmed_projection_rejects_stale_native_ratchet_state(updates):
    info = unarmed_info(**updates)
    with pytest.raises(PolicyError, match='unarmed'):
        capture_native_trailing_view(info, capture(info))


@pytest.mark.parametrize('updates', [
    {'trailing_state': 'unknown'},
    {'trailing_delta_pct': D('0')},
    {'trailing_delta_pct': float('nan')},
    {'trailing_stop_trigger_pct': None},
    {'trailing_trigger_price': None},
    {'trailing_trigger_price': D('-1')},
    {'trailing_move_count': -1},
    {'trailing_move_count': True},
])
def test_armed_projection_rejects_missing_or_invalid_native_trailing_evidence(updates):
    info = trailing_info(**updates)
    with pytest.raises((PolicyError, ValueError, TypeError)):
        capture_native_trailing_view(info, capture(info))


def test_trailing_projection_is_bound_to_the_same_native_execution_receipt():
    info = trailing_info()
    snap = capture(info)
    info.custom_info['execution_receipt']['remaining_owned_base'] = D('1.5')
    with pytest.raises(PolicyError, match='snapshot'):
        capture_native_trailing_view(info, snap)


def test_trailing_projection_rejects_state_changed_after_snapshot_capture():
    info = trailing_info()
    snap = capture(info)
    info.custom_info.update({
        'trailing_stop_trigger_pct': D('.09'),
        'trailing_trigger_price': D('110'),
        'trailing_move_count': 1,
        'trailing_last_update_timestamp': 1002.0,
    })
    with pytest.raises(PolicyError, match='snapshot'):
        capture_native_trailing_view(info, snap)


def test_native_projection_and_lifecycle_share_the_same_threshold_ratchet_contract():
    first_info = trailing_info()
    first_snapshot = capture(first_info)
    first = capture_native_trailing_view(first_info, first_snapshot)
    assert bind_trailing_view(first_snapshot, (first,), now_ms=1002, max_age_ms=10, previous=None) == first

    second_info = trailing_info(trailing_stop_trigger_pct=D('.09'), trailing_trigger_price=D('110'),
                                trailing_current_price=D('113'), trailing_move_count=1,
                                trailing_last_update_timestamp=1002.0)
    second_snapshot = capture(second_info, sequence=2, observed_at_ms=1001, available_at_ms=1002)
    second = capture_native_trailing_view(second_info, second_snapshot)
    assert bind_trailing_view(second_snapshot, (second,), now_ms=1003, max_age_ms=10, previous=first) == second

    loosened_info = trailing_info(trailing_stop_trigger_pct=D('.07'), trailing_trigger_price=D('108'),
                                  trailing_move_count=1, trailing_last_update_timestamp=1002.0)
    loosened_snapshot = capture(loosened_info, sequence=2, observed_at_ms=1001, available_at_ms=1002)
    loosened = capture_native_trailing_view(loosened_info, loosened_snapshot)
    with pytest.raises(PolicyError, match='ratchet loosened'):
        bind_trailing_view(loosened_snapshot, (loosened,), now_ms=1003, max_age_ms=10, previous=first)


@pytest.mark.parametrize('pct,moves,threshold', [
    (D('.08'), 0, D('107.91')),  # same high-water, lower fill basis
    (D('.09'), 1, D('108.90')),  # tighter native ratchet, lower fill basis
])
def test_native_pct_ratchet_accepts_cost_basis_repricing(pct, moves, threshold):
    first_info = trailing_info()
    first_snapshot = capture(first_info)
    first = capture_native_trailing_view(first_info, first_snapshot)
    second_info = trailing_info(entry_price=D('99'), current_position_average_price=D('99'),
                                trailing_stop_trigger_pct=pct, trailing_trigger_price=threshold,
                                trailing_move_count=moves,
                                trailing_last_update_timestamp=1001.0 if moves == 0 else 1002.0)
    second_snapshot = capture(second_info, sequence=2, observed_at_ms=1001, available_at_ms=1002)
    second = capture_native_trailing_view(second_info, second_snapshot)
    assert second.threshold_price < first.threshold_price
    assert bind_trailing_view(second_snapshot, (second,), now_ms=1003, max_age_ms=10, previous=first) == second


@pytest.mark.parametrize('updates', [
    {'trailing_stop_trigger_pct': D('.07'), 'trailing_trigger_price': D('112')},
    {'trailing_move_count': 0},
    {'trailing_armed_timestamp': 1002.0},
    {'trailing_last_update_timestamp': 1001.0},
])
def test_native_pct_ratchet_rejects_regression_or_changed_lineage(updates):
    first_info = trailing_info(trailing_stop_trigger_pct=D('.08'), trailing_move_count=1,
                               trailing_last_update_timestamp=1002.0)
    first_snapshot = capture(first_info)
    first = capture_native_trailing_view(first_info, first_snapshot)
    second_fields = dict(trailing_stop_trigger_pct=D('.08'), trailing_move_count=1,
                         trailing_last_update_timestamp=1002.0)
    second_fields.update(updates)
    second_info = trailing_info(**second_fields)
    second_snapshot = capture(second_info, sequence=2, observed_at_ms=1001, available_at_ms=1002)
    second = capture_native_trailing_view(second_info, second_snapshot)
    with pytest.raises(PolicyError, match='ratchet|lineage|regressed'):
        bind_trailing_view(second_snapshot, (second,), now_ms=1003, max_age_ms=10, previous=first)


def test_native_evidence_cannot_disappear_or_be_claimed_by_recipe_view():
    info = trailing_info()
    first_snapshot = capture(info)
    first = capture_native_trailing_view(info, first_snapshot)
    second_snapshot = capture(info, sequence=2, observed_at_ms=1001, available_at_ms=1002)
    missing = first.model_copy(update={'receipt_sha256': second_snapshot.receipt_sha256,
                                       'observed_at_ms': second_snapshot.observed_at_ms,
                                       'native_trigger_pct': None, 'native_move_count': None,
                                       'native_armed_timestamp': None, 'native_last_update_timestamp': None})
    with pytest.raises(PolicyError, match='evidence disappeared'):
        bind_trailing_view(second_snapshot, (missing,), now_ms=1003, max_age_ms=10, previous=first)
    with pytest.raises(ValueError):
        NativeTrailingViewV2(**(first.model_dump(mode='python') | {'exit_owner': 'recipe_proposal'}))


def test_native_ratchet_rejects_wrong_owner_receipt_and_activation_withdrawal():
    info = trailing_info()
    first_snapshot = capture(info)
    first = capture_native_trailing_view(info, first_snapshot)
    second_snapshot = capture(info, sequence=2, observed_at_ms=1001, available_at_ms=1002)
    second = capture_native_trailing_view(info, second_snapshot)
    for changes, expected in [
        ({'owner_id': 'another-owner'}, 'another lot'),
        ({'receipt_sha256': 'f' * 64}, 'another lot'),
    ]:
        with pytest.raises(PolicyError, match=expected):
            bind_trailing_view(second_snapshot, (second.model_copy(update=changes),),
                               now_ms=1003, max_age_ms=10, previous=first)
    with pytest.raises(ValueError, match='activated native trailing evidence'):
        bind_trailing_view(second_snapshot, (second.model_copy(update={'activated': False}),),
                           now_ms=1003, max_age_ms=10, previous=first)


def test_native_ratchet_evidence_survives_checkpoint_json_roundtrip():
    info = trailing_info()
    snapshot = capture(info)
    view = capture_native_trailing_view(info, snapshot)
    checkpoint = capture_checkpoint(snapshot, (view,), now_ms=1002, max_age_ms=10, previous=None)
    decoded = decode_checkpoint(encode_checkpoint(checkpoint))
    assert decoded.trailing == view


def test_legacy_checkpoint_without_native_fields_still_decodes_with_legacy_guard():
    info = trailing_info()
    snapshot = capture(info)
    legacy = NativeTrailingViewV2(owner_id=snapshot.scope.owner_id, executor_id=snapshot.scope.executor_id,
                                  receipt_sha256=snapshot.receipt_sha256, observed_at_ms=snapshot.observed_at_ms,
                                  activated=True, threshold_price=D('109'), exit_owner='native_executor')
    checkpoint = capture_checkpoint(snapshot, (legacy,), now_ms=1002, max_age_ms=10, previous=None)
    raw = checkpoint.model_dump(mode='json')
    for key in ('native_trigger_pct', 'native_move_count', 'native_armed_timestamp',
                'native_last_update_timestamp'):
        raw['trailing'].pop(key)
    decoded = decode_checkpoint(json.dumps(raw))
    assert decoded.trailing.native_trigger_pct is None
    assert decoded.trailing.threshold_price == D('109')


def test_legacy_view_keeps_quote_price_ratchet_and_owner_guards():
    info = trailing_info()
    first_snapshot = capture(info)
    first = NativeTrailingViewV2(owner_id=first_snapshot.scope.owner_id,
                                 executor_id=first_snapshot.scope.executor_id,
                                 receipt_sha256=first_snapshot.receipt_sha256,
                                 observed_at_ms=first_snapshot.observed_at_ms, activated=True,
                                 threshold_price=D('109'), exit_owner='native_executor')
    second_snapshot = capture(info, sequence=2, observed_at_ms=1001, available_at_ms=1002)
    lower = first.model_copy(update={'receipt_sha256': second_snapshot.receipt_sha256,
                                     'observed_at_ms': second_snapshot.observed_at_ms,
                                     'threshold_price': D('108')})
    with pytest.raises(PolicyError, match='ratchet loosened'):
        bind_trailing_view(second_snapshot, (lower,), now_ms=1003, max_age_ms=10, previous=first)
    wrong_owner = lower.model_copy(update={'owner_id': 'another-owner', 'threshold_price': D('110')})
    with pytest.raises(PolicyError, match='another lot'):
        bind_trailing_view(second_snapshot, (wrong_owner,), now_ms=1003, max_age_ms=10, previous=first)
    inactive = lower.model_copy(update={'threshold_price': D('110'), 'activated': False})
    with pytest.raises(PolicyError, match='activation withdrawn'):
        bind_trailing_view(second_snapshot, (inactive,), now_ms=1003, max_age_ms=10, previous=first)
    recipe_previous = first.model_copy(update={'exit_owner': 'recipe_proposal'})
    recipe_lower = lower.model_copy(update={'exit_owner': 'recipe_proposal'})
    with pytest.raises(PolicyError, match='ratchet loosened'):
        bind_trailing_view(second_snapshot, (recipe_lower,), now_ms=1003, max_age_ms=10,
                           previous=recipe_previous)
    # An old checkpoint cannot gain a weaker quote floor merely by adding new native fields.
    projected = trailing_info(trailing_trigger_price=D('108'))
    native_snapshot = capture(projected, sequence=2, observed_at_ms=1001, available_at_ms=1002)
    native = capture_native_trailing_view(projected, native_snapshot)
    with pytest.raises(PolicyError, match='ratchet loosened'):
        bind_trailing_view(native_snapshot, (native,), now_ms=1003, max_age_ms=10, previous=first)


def test_native_trailing_adapter_is_inside_the_isolated_v2_source_seal():
    assert any(path.name == 'native_trailing.py' for path in source_files())
