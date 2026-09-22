from decimal import Decimal as D
from types import SimpleNamespace as NS

import pytest

from controllers.generic.spot_recipe_v2_core.native_views import assess_exit, capture_native_snapshot
from hummingbot.strategy_v2.components.exit_policy import ExitProposal, NativeExitView, NativeProfitFloor

from .test_lifecycle import pending, scope, snapshot


def native_info():
    return NS(id='lot_1', controller_id='spot_v2_owner', is_done=False, is_active=True,
              config=NS(controller_id='spot_v2_owner', connector_name='okx_paper_trade',
                        trading_pair='BTC-USDC', side=NS(name='BUY')),
              custom_info={'execution_receipt': {'accounting_owner': 'executor',
                                                 'remaining_owned_base': D('2'), 'entry_gross_quote': D('200'), 'exit_gross_base': D('0')},
                           'entry_lifecycle': {'state': 'managing_fill'}})


def capture(info=None, **changes):
    args = dict(scope=scope(), sequence=1, observed_at_ms=1000, available_at_ms=1001,
                reconciled=True, fees_complete=True, commitments=(), terminal_requests=()) | changes
    return capture_native_snapshot(native_info() if info is None else info, **args)


def exit_inputs(snap=None, minimum_price=D('100'), **updates):
    snap = snapshot() if snap is None else snap
    common = dict(owner_id=snap.scope.owner_id, executor_id=snap.scope.executor_id,
                  receipt_sha256=snap.receipt_sha256)
    values = dict(**common, connector=snap.scope.connector, trading_pair=snap.scope.trading_pair,
                  observed_at_ms=1000, reconciled=True, fees_complete=True, remaining_base=D('2'), reserved_base=D('0'),
                  base_fee_fraction=D('0'), amount_step=D('.1'), price_step=D('.01'), minimum_amount=D('.1'),
                  minimum_notional=D('10'), bid=D('110')) | updates
    view = NativeExitView(**values)
    proposals = tuple(ExitProposal(**common, policy_id=policy, requested_base=D(q),
                                   minimum_price=minimum_price, expires_at_ms=1010)
                      for policy, q in [('trail', '1.2'), ('exhaustion', '.8')])
    floor = NativeProfitFloor(**common, quantity_base=D('1.2'), price=D('100'), observed_at_ms=1000)
    return snap, view, proposals, floor


def assess(inputs):
    return assess_exit(*inputs, now_ms=1002, max_age_ms=10)


def test_capture_reads_existing_native_receipt_shape_without_a_second_ledger():
    info = native_info()
    result = capture(info)
    assert result.remaining_base == D('2')
    assert result.entry_terminal is True
    assert result.accounting_owner == 'executor'
    assert result.receipt_sha256 == capture(info).receipt_sha256
    assert info.custom_info['execution_receipt']['remaining_owned_base'] == D('2')


@pytest.mark.parametrize('change', [
    lambda i: setattr(i, 'id', 'wrong'),
    lambda i: setattr(i, 'controller_id', 'foreign'),
    lambda i: setattr(i.config, 'controller_id', 'foreign'),
    lambda i: setattr(i.config, 'connector_name', 'okx'),
    lambda i: setattr(i.config, 'trading_pair', 'ETH-USDC'),
    lambda i: setattr(i.config, 'side', NS(name='SELL')),
    lambda i: setattr(i, 'is_done', 'false'),
    lambda i: setattr(i, 'is_active', False),
    lambda i: i.custom_info.pop('execution_receipt'),
    lambda i: i.custom_info['execution_receipt'].pop('entry_gross_quote'),
    lambda i: i.custom_info['execution_receipt'].update(remaining_owned_base=float('nan')),
    lambda i: i.custom_info['entry_lifecycle'].update(state='unrecognized'),
])
def test_native_capture_rejects_missing_wrong_or_unsupported_evidence(change):
    info = native_info()
    change(info)
    with pytest.raises((ValueError, TypeError)):
        capture(info)


def test_fee_correction_changes_receipt_identity_not_previous_evidence():
    info = native_info()
    old = capture(info)
    info.custom_info['execution_receipt']['entry_fee_quote'] = D('.5')
    changed = capture(info, sequence=2)
    assert changed.receipt_sha256 != old.receipt_sha256
    assert changed.remaining_base == old.remaining_base


def test_maximum_exit_request_not_sum_uses_canonical_arbitration():
    result = assess(exit_inputs())
    assert result.status == 'PROPOSED_REDUCTION'
    assert result.amount_base == D('1.2')
    assert result.execution_authorized is False
    assert result.market_fallback is False
    assert result.policy_ids == ('exhaustion', 'trail')


def test_native_capture_to_exit_boundary_smoke():
    captured = capture()
    result = assess(exit_inputs(captured))
    assert result.amount_base == D('1.2')
    assert result.limit_price == D('100')


@pytest.mark.parametrize('changes,reason', [
    ({'remaining_base': D('3')}, 'native_exit_snapshot_mismatch'),
    ({'receipt_sha256': 'f' * 64}, 'native_exit_snapshot_mismatch'),
    ({'connector': 'okx'}, 'native_exit_snapshot_mismatch'),
    ({'observed_at_ms': 1001}, 'native_exit_snapshot_mismatch'),
    ({'owner_id': 'foreign'}, 'native_exit_snapshot_mismatch'),
    ({'fees_complete': False}, 'native_exit_snapshot_mismatch'),
    ({'reserved_base': D('.1')}, 'native_commitment_projection_mismatch'),
    ({'bid': D('90')}, 'bid_below_profit_floor'),
])
def test_exit_evidence_cannot_be_mixed_or_reanchored(changes, reason):
    result = assess(exit_inputs(**changes))
    assert result.status == 'HOLD'
    assert result.reason == reason


@pytest.mark.parametrize('updates', [
    {'commitments': (pending(),)}, {'commitments': (pending('reduce'),)},
    {'fees_complete': False}, {'reconciled': False}, {'available_at_ms': 1010},
    {'entry_terminal': False}, {'accounting_owner': 'held_position'}, {'terminal': True},
])
def test_no_new_seller_for_unresolved_native_scope(updates):
    result = assess(exit_inputs(snapshot(**updates)))
    assert result.status == 'HOLD'
    assert result.execution_authorized is False


def test_native_floor_is_required_for_exact_final_quantity():
    snap, view, proposals, floor = exit_inputs()
    assert assess((snap, view, proposals, None)).reason == 'native_floor_unavailable'
    wrong = floor.model_copy(update={'quantity_base': D('1.1')})
    assert assess((snap, view, proposals, wrong)).reason == 'native_floor_quantity_mismatch'


def test_base_fee_quantization_requests_a_new_quantity_specific_floor():
    snap, view, proposals, floor = exit_inputs(remaining_base=D('2'), base_fee_fraction=D('.01'))
    proposals = tuple(p.model_copy(update={'requested_base': D('2')}) for p in proposals)
    assert assess((snap, view, proposals, floor)).reason == 'native_floor_quantity_mismatch'
    bound_floor = floor.model_copy(update={'quantity_base': D('1.9')})
    result = assess((snap, view, proposals, bound_floor))
    assert result.amount_base == D('1.9')
    assert result.amount_base * D('1.01') <= D('2')


def test_exit_assessment_hash_binds_recipe_not_just_quantities():
    original = exit_inputs()
    result = assess(original)
    new_scope = scope(recipe_binding=scope().recipe_binding.model_copy(update={'recipe_sha256': 'f' * 64}))
    changed = (snapshot(scope=new_scope), *original[1:])
    assert assess(changed).input_sha256 != result.input_sha256


def test_expired_proposals_do_not_submit_or_reset_native_inventory():
    snap, view, proposals, floor = exit_inputs()
    expired = tuple(p.model_copy(update={'expires_at_ms': 1002}) for p in proposals)
    assert assess((snap, view, expired, floor)).reason == 'no_current_exit_proposal'
    assert snap.remaining_base == D('2')
