from decimal import Decimal as D

import pytest

from controllers.generic.spot_recipe_v2_core.lifecycle import bind_trailing_view
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


def test_native_trailing_adapter_is_inside_the_isolated_v2_source_seal():
    assert any(path.name == 'native_trailing.py' for path in source_files())
