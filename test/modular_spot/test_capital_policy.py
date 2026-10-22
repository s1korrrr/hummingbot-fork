import importlib
from decimal import Decimal as D

import pytest


def module():
    try:
        return importlib.import_module('controllers.generic.modular_spot_core.capital_policy')
    except ModuleNotFoundError:
        pytest.fail('capital policy is not implemented')


def snapshot(m, **changes):
    value = dict(account_id='segregated-research', valuation_currency='USDC', observed_at_ms=1000,
                 source_sha256='a' * 64, valuation_sha256='b' * 64, complete=True, reconciled=True,
                 risk_paused=False, available_quote='500', exposures=(dict(executor_id='e1', program_id='rebound',
                                                                           trading_pair='BTC-USDC', marked_inventory_quote='40', reserved_acquisition_quote='10'),))
    value.update(changes)
    return m.CapitalSnapshot.model_validate(value)


def assess(m, snap=None, **changes):
    values = dict(account_id='segregated-research', program_id='rebound', trading_pair='BTC-USDC',
                  valuation_currency='USDC', requested_quote='20')
    values.update(changes)
    limits = m.CapitalLimits(account_quote='200', program_quote='100', base_quote='90', order_quote='30', max_age_ms=100)
    return m.assess_acquisition(m.CapitalRequest(**values), snapshot(m) if snap is None else snap, limits, now_ms=1050)


def test_all_four_limits_reported_and_not_an_authorization():
    m = module()
    result = assess(m)
    assert result.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert result.execution_authorized is False
    assert result.projected_account_quote == D('70')
    assert result.projected_program_quote == D('70')
    assert result.projected_base_quote == D('70')


def test_same_base_across_quote_pairs_counts_together_in_native_valuation():
    m = module()
    snap = snapshot(m, exposures=(
        dict(executor_id='e1', program_id='other', trading_pair='BTC-USDT', marked_inventory_quote='80', reserved_acquisition_quote='0'),))
    result = assess(m, snap)
    assert result.reason == 'base_limit'


def test_reserved_and_retained_inventory_are_not_free_capital():
    m = module()
    snap = snapshot(m, exposures=(
        dict(executor_id='e1', program_id='rebound', trading_pair='BTC-USDC', marked_inventory_quote='70', reserved_acquisition_quote='15'),))
    assert assess(m, snap).reason == 'program_limit'


@pytest.mark.parametrize('change,reason', [({'available_quote': None}, 'native_snapshot_incomplete'),
                                           ({'complete': False}, 'native_snapshot_incomplete'), ({'reconciled': False}, 'native_reconciliation_required'),
                                           ({'risk_paused': True}, 'native_risk_paused'), ({'observed_at_ms': 1}, 'native_snapshot_stale'),
                                           ({'observed_at_ms': 1100}, 'native_snapshot_stale'), ({'available_quote': '10'}, 'available_quote_limit')])
def test_missing_stale_and_paused_never_pass(change, reason):
    m = module()
    assert assess(m, snapshot(m, **change)).reason == reason


def test_unknown_mark_and_duplicate_receipts_are_rejected_or_held():
    m = module()
    row = dict(executor_id='e', program_id='rebound', trading_pair='BTC-USDC',
               marked_inventory_quote=None, reserved_acquisition_quote='0')
    assert assess(m, snapshot(m, exposures=(row,))).reason == 'native_snapshot_incomplete'
    with pytest.raises(ValueError, match='duplicate'):
        snapshot(m, exposures=(row, row))


@pytest.mark.parametrize('change', [dict(requested_quote=True), dict(requested_quote='NaN'), dict(requested_quote='-1')])
def test_invalid_amounts_rejected(change):
    m = module()
    with pytest.raises(ValueError):
        assess(m, **change)


def test_foreign_account_or_valuation_cannot_be_combined():
    m = module()
    with pytest.raises(ValueError, match='scope'):
        assess(m, account_id='another')
    with pytest.raises(ValueError, match='scope'):
        assess(m, valuation_currency='USD')


def test_fixed_allocations_leave_explicit_cash_and_do_not_claim_wallet():
    m = module()
    result = m.fixed_targets('100', (('rebound', '.3'), ('impulse', '.4')), quote_step='.01')
    assert result.targets == (('impulse', D('40')), ('rebound', D('30')))
    assert result.unallocated_quote == D('30')
    assert result.execution_authorized is False
    rounded = m.fixed_targets('1', (('a', '.333333'), ('b', '.333333'), ('c', '.333334')), quote_step='.01')
    assert sum(v for _, v in rounded.targets) + rounded.unallocated_quote == D('1')


@pytest.mark.parametrize('weights', [(('a', '.6'), ('b', '.6')), (('a', '.5'), ('a', '.5')),
                                     (('a', '-.1'),), (('a', True),)])
def test_invalid_allocation_is_not_normalized_silently(weights):
    m = module()
    with pytest.raises(ValueError):
        m.fixed_targets('100', weights, quote_step='.01')


def test_sizing_reuses_existing_fee_reserved_native_policy():
    m = module()
    from controllers.generic.modular_spot_core.portfolio import allocate
    kwargs = dict(remaining_quote='100', available_quote='100', price='10', amount_step='.1',
                  minimum_amount='.1', minimum_notional='1', fee_buffer='.01', cash_reserve='0')
    result = m.bounded_size(stage_quote='100', liquidity_cap_quote='40', volatility_cap_quote='30', **kwargs)
    assert result == allocate(stage_quote='30', **kwargs)
    assert result.reserved_quote <= D('30')
    assert m.bounded_size(stage_quote='100', liquidity_cap_quote='0', volatility_cap_quote='30', **kwargs) is None
