from decimal import Decimal
from test.mean_reversion_v2.test_exit_a_binding_and_admission import H, metadata_store, reduction

import pytest

from controllers.generic.mean_reversion_v2_core.exit_a.admission import admit
from controllers.generic.mean_reversion_v2_core.exit_a.lifecycle import (
    DispatchRequest,
    RestoreIdentity,
    dispatch,
    restore,
)
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.exit_policy import NativeExitView, NativeProfitFloor


def _identity(**changes):
    values = dict(owner_id='mean_reversion_v2_exit_a', recipe_sha256=H, source_sha256=H,
                  model_sha256=H, full_configuration_sha256=H)
    values.update(changes)
    return RestoreIdentity(**values)


def test_restore_keeps_peak_and_cost_and_rejects_legacy(tmp_path):
    engine, store = metadata_store(tmp_path)
    result, current, floor = reduction()
    claim = admit(result, current.native, floor, store, now_ms=101, max_age_ms=50)
    peak, cost = Decimal('120'), Decimal('100')
    restored = restore(store, _identity(), peak_price=peak, cost_basis=cost)
    engine.dispose()
    assert restored.claims == (claim,)
    assert restored.peak_price == peak and restored.cost_basis == cost
    with pytest.raises(ValueError):
        _identity(owner_id='ok_rsi')
    with pytest.raises(PolicyError, match='restore identity'):
        restore(store, _identity(owner_id='mean_reversion_v2_other'), peak_price=peak, cost_basis=cost)


def test_entry_pause_still_dispatches_and_does_not_reset_peak(tmp_path):
    engine, store = metadata_store(tmp_path)
    result, current, floor = reduction()
    claim = admit(result, current.native, floor, store, now_ms=101, max_age_ms=50)
    seen = []
    outcome = dispatch(claim, result.proposal, current.native, floor, store, now_ms=101, max_age_ms=50,
                       inventory_revision=current.inventory_revision, peak_price=Decimal('120'),
                       cost_basis=Decimal('100'), entry_paused=True, place=seen.append)
    engine.dispose()
    assert outcome.status == 'DISPATCHED' and outcome.reason == 'placed'
    assert outcome.peak_price == Decimal('120') and outcome.cost_basis == Decimal('100')
    assert outcome.inventory_released is False
    assert seen == [outcome.request]
    assert isinstance(outcome.request, DispatchRequest)
    assert outcome.request.side == 'SELL' and outcome.request.order_type == 'LIMIT'
    assert outcome.request.execution_authorized is False
    assert outcome.request.limit_price == floor.price


def test_protective_close_after_proposal_cancels_without_place(tmp_path):
    engine, store = metadata_store(tmp_path)
    result, current, floor = reduction()
    claim = admit(result, current.native, floor, store, now_ms=101, max_age_ms=50)
    seen = []
    changed = NativeExitView.model_validate(current.native.model_dump(mode='json') | {'receipt_sha256': 'b' * 64})
    outcome = dispatch(claim, result.proposal, changed, floor, store, now_ms=101, max_age_ms=50,
                       inventory_revision=1, peak_price=Decimal('120'), cost_basis=Decimal('100'),
                       place=seen.append)
    engine.dispose()
    assert outcome.status == 'CANCEL_RACE'
    assert seen == []
    assert store.load(claim.claim_id).state == 'CANCEL_PENDING'
    assert store.load(claim.claim_id).inventory_released is False


def test_native_close_pending_cancels_before_place(tmp_path):
    engine, store = metadata_store(tmp_path)
    result, current, floor = reduction()
    claim = admit(result, current.native, floor, store, now_ms=101, max_age_ms=50)
    seen = []
    outcome = dispatch(claim, result.proposal, current.native, floor, store, now_ms=101, max_age_ms=50,
                       inventory_revision=1, peak_price=Decimal('120'), cost_basis=Decimal('100'),
                       native_close_pending=True, place=seen.append)
    engine.dispose()
    assert outcome.status == 'CANCEL_RACE' and seen == []
    assert store.load(claim.claim_id).state == 'CANCEL_PENDING'


def test_dust_and_incomplete_fees_hold(tmp_path):
    engine, store = metadata_store(tmp_path)
    result, current, floor = reduction()
    claim = admit(result, current.native, floor, store, now_ms=101, max_age_ms=50)
    dust = NativeExitView.model_validate(current.native.model_dump(mode='json') | {
        'remaining_base': '0.001', 'minimum_amount': '0.01'})
    dusty_floor = NativeProfitFloor.model_validate(floor.model_dump(mode='json') | {'quantity_base': '0.001'})
    fees = NativeExitView.model_validate(current.native.model_dump(mode='json') | {'fees_complete': False})
    seen = []
    held_dust = dispatch(claim, result.proposal, dust, dusty_floor, store, now_ms=101, max_age_ms=50,
                         inventory_revision=1, peak_price=Decimal('120'), cost_basis=Decimal('100'),
                         place=seen.append)
    held_fees = dispatch(claim, result.proposal, fees, floor, store, now_ms=101, max_age_ms=50,
                         inventory_revision=1, peak_price=Decimal('120'), cost_basis=Decimal('100'),
                         place=seen.append)
    engine.dispose()
    assert held_dust.status == held_fees.status == 'HOLD'
    assert held_dust.reason == 'native_quantity_below_minimum'
    assert held_fees.reason == 'native_fees_incomplete'
    assert seen == []
    assert store.load(claim.claim_id).state == 'ADMITTED'


def test_fee_correction_and_floor_mismatch_hold(tmp_path):
    engine, store = metadata_store(tmp_path)
    result, current, floor = reduction()
    claim = admit(result, current.native, floor, store, now_ms=101, max_age_ms=50)
    taxed = NativeExitView.model_validate(current.native.model_dump(mode='json') | {
        'base_fee_fraction': '0.1', 'remaining_base': '1'})
    seen = []
    outcome = dispatch(claim, result.proposal, taxed, floor, store, now_ms=101, max_age_ms=50,
                       inventory_revision=1, peak_price=Decimal('120'), cost_basis=Decimal('100'),
                       place=seen.append)
    engine.dispose()
    assert outcome.status == 'HOLD' and outcome.reason == 'native_floor_quantity_mismatch'
    assert seen == []
    assert outcome.peak_price == Decimal('120')


def test_uncertain_submission_keeps_claim(tmp_path):
    engine, store = metadata_store(tmp_path)
    result, current, floor = reduction()
    claim = admit(result, current.native, floor, store, now_ms=101, max_age_ms=50)

    def boom(_request):
        raise RuntimeError('exchange timeout')

    outcome = dispatch(claim, result.proposal, current.native, floor, store, now_ms=101, max_age_ms=50,
                       inventory_revision=1, peak_price=Decimal('120'), cost_basis=Decimal('100'),
                       place=boom)
    engine.dispose()
    assert outcome.status == 'HOLD' and outcome.reason == 'submission_uncertain'
    loaded = store.load(claim.claim_id)
    assert loaded.state == 'ADMITTED' and loaded.inventory_released is False


def test_lifecycle_module_does_not_import_native_composition():
    import sys

    import controllers.generic.mean_reversion_v2_core.exit_a.lifecycle as lifecycle
    assert 'controllers.generic.modular_spot_core.native_composition' not in sys.modules
    assert lifecycle.dispatch.__name__ == 'dispatch'
