import importlib
from decimal import Decimal as D

import pytest


def module():
    try:
        return importlib.import_module('controllers.generic.modular_spot_core.exit_policy')
    except ModuleNotFoundError:
        pytest.fail('exit arbitration is not implemented')


def view(m, **changes):
    value = dict(owner_id='modular_spot_rebound', executor_id='e1', connector='okx', trading_pair='BTC-USDC',
        receipt_sha256='a' * 64, observed_at_ms=1000, reconciled=True, fees_complete=True,
        remaining_base='10', reserved_base='2', base_fee_fraction='0', amount_step='.1',
        price_step='.01', minimum_amount='.1', minimum_notional='1', bid='12')
    value.update(changes)
    return m.NativeExitView.model_validate(value)


def request(m, quantity='5', **changes):
    value = dict(policy_id='native_trailing', owner_id='modular_spot_rebound', executor_id='e1',
        receipt_sha256='a' * 64, requested_base=quantity, minimum_price='0', expires_at_ms=2000)
    value.update(changes)
    return m.ExitProposal.model_validate(value)


def floor(m, quantity='5', **changes):
    value = dict(owner_id='modular_spot_rebound', executor_id='e1', receipt_sha256='a' * 64,
        quantity_base=quantity, price='11.005', observed_at_ms=1000)
    value.update(changes)
    return m.NativeProfitFloor.model_validate(value)


def test_overlapping_exit_requests_are_not_added_together():
    m = module()
    result = m.arbitrate_exit(view(m), (request(m, '3'), request(m, '5', policy_id='exhaustion')),
        floor(m), now_ms=1050, max_age_ms=100)
    assert result.status == 'PROPOSED_REDUCTION'
    assert result.amount_base == D('5') and result.limit_price == D('11.01')
    assert result.execution_authorized is False and result.market_fallback is False
    assert result.policy_ids == ('exhaustion', 'native_trailing')


def test_native_fee_and_reserved_quantity_are_respected():
    m = module()
    result = m.arbitrate_exit(view(m, base_fee_fraction='.01'), (request(m, '10'),),
        floor(m, '7.9'), now_ms=1050, max_age_ms=100)
    assert result.amount_base == D('7.9')
    assert result.amount_base * D('1.01') + D('2') <= D('10')


def test_floor_for_different_quantity_requires_native_recalculation():
    m = module()
    result = m.arbitrate_exit(view(m), (request(m, '10'),), floor(m, '10'), now_ms=1050, max_age_ms=100)
    assert result.reason == 'native_floor_quantity_mismatch'
    assert result.amount_base is None


@pytest.mark.parametrize('changes,reason', [({'reconciled': False}, 'native_reconciliation_required'),
    ({'fees_complete': False}, 'native_fees_incomplete'), ({'observed_at_ms': 100}, 'native_snapshot_stale'),
    ({'observed_at_ms': 2000}, 'native_snapshot_stale'), ({'reserved_base': '10'}, 'no_uncommitted_inventory'),
    ({'reserved_base': '11'}, 'native_quantity_conflict'), ({'bid': '10'}, 'bid_below_profit_floor')])
def test_unsafe_exit_conditions_hold(changes, reason):
    m = module()
    result = m.arbitrate_exit(view(m, **changes), (request(m),), floor(m), now_ms=1050, max_age_ms=100)
    assert result.reason == reason and result.status == 'HOLD'


def test_unknown_cost_and_stale_floor_are_not_invented():
    m = module()
    assert m.arbitrate_exit(view(m), (request(m),), None, now_ms=1050, max_age_ms=100).reason == 'native_floor_unavailable'
    result = m.arbitrate_exit(view(m), (request(m),), floor(m, observed_at_ms=100), now_ms=1050, max_age_ms=100)
    assert result.reason == 'native_floor_stale'


def test_foreign_owner_or_receipt_holds_instead_of_selling_other_lot():
    m = module()
    result = m.arbitrate_exit(view(m), (request(m, owner_id='different'),), floor(m), now_ms=1050, max_age_ms=100)
    assert result.reason == 'exit_scope_conflict'
    result = m.arbitrate_exit(view(m), (request(m),), floor(m, receipt_sha256='b' * 64), now_ms=1050, max_age_ms=100)
    assert result.reason == 'native_floor_scope_conflict'


def test_expired_policies_and_dust_do_not_create_orders():
    m = module()
    result = m.arbitrate_exit(view(m), (request(m, expires_at_ms=1050),), floor(m), now_ms=1050, max_age_ms=100)
    assert result.reason == 'no_current_exit_proposal'
    result = m.arbitrate_exit(view(m), (request(m, '.05'),), floor(m, '.05'), now_ms=1050, max_age_ms=100)
    assert result.reason == 'native_quantity_below_minimum'


def test_policy_can_tighten_but_never_lower_the_native_floor():
    m = module()
    result = m.arbitrate_exit(view(m, bid='14'), (request(m, minimum_price='13'),), floor(m), now_ms=1050, max_age_ms=100)
    assert result.limit_price == D('13')
    result = m.arbitrate_exit(view(m), (request(m, minimum_price='1'),), floor(m), now_ms=1050, max_age_ms=100)
    assert result.limit_price == D('11.01')


def test_duplicate_policy_identity_and_boolean_quantities_rejected():
    m = module()
    with pytest.raises(ValueError, match='duplicate'):
        m.arbitrate_exit(view(m), (request(m), request(m, '7')), floor(m), now_ms=1050, max_age_ms=100)
    with pytest.raises(ValueError):
        request(m, True)
    with pytest.raises(ValueError):
        view(m, remaining_base='NaN')
