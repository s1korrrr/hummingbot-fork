from decimal import Decimal as D
from pathlib import Path

import pytest

from controllers.generic.momentum_fusion_v2_core import runtime
from hummingbot.strategy_v2.components.capital_policy import CapitalLimits, CapitalRequest, CapitalSnapshot
from hummingbot.strategy_v2.components.domain import PolicyError


def snapshot(**changes):
    value = dict(account_id='mom-v2', valuation_currency='USDC', observed_at_ms=1000,
                 source_sha256='a' * 64, valuation_sha256='b' * 64, complete=True, reconciled=True,
                 risk_paused=False, available_quote='100', exposures=())
    value.update(changes)
    return CapitalSnapshot.model_validate(value)


def request(quote='60'):
    return CapitalRequest(account_id='mom-v2', program_id='okx_spot_momentum_v2', trading_pair='BNB-USDC',
                          valuation_currency='USDC', requested_quote=quote)


def limits():
    return CapitalLimits(account_quote='100', program_quote='100', base_quote='100', order_quote='70', max_age_ms=100)


def test_two_sixty_unit_plans_cannot_exceed_one_hundred():
    first = runtime.admit_capital(request(), snapshot(), limits(), now_ms=1050)
    assert first.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    claim = runtime.Claim(claim_id='c' * 64, opportunity_id='o' * 64, controller_id='mom',
                          trading_pair='BNB-USDC', status='RESERVED', reserved_quote='60', reserved_base='0.07')
    second = runtime.admit_capital(request(), snapshot(), limits(), now_ms=1050, extra_reserved=(claim,))
    assert second.status == 'HOLD'
    assert second.reason in {'account_limit', 'program_limit', 'available_quote_limit'}
    assert second.execution_authorized is False


def test_quantity_below_minimum_is_rejected_never_uplifted():
    assert runtime.size_entry(
        stage_quote='5', remaining_quote='100', available_quote='100', price='100',
        amount_step='1', minimum_amount='1', minimum_notional='50', fee_buffer='0.003',
    ) is None


def test_shared_multi_process_topology_is_unsupported():
    with pytest.raises(PolicyError, match='multi-process'):
        runtime.refuse_unsupported_topology('shared_multi_process')


def test_durable_claim_survives_restart_without_duplicate(tmp_path):
    store = runtime.ClaimStore(tmp_path / 'claims.json')
    claim = runtime.Claim(claim_id='c' * 64, opportunity_id='o' * 64, controller_id='mom',
                          trading_pair='BNB-USDC', status='RESERVED', reserved_quote='60', reserved_base='0.07')
    first = store.put(claim)
    second = store.put(claim.model_copy(update={'reserved_quote': '80'}))
    assert first.reserved_quote == second.reserved_quote == D('60')
    restored = runtime.ClaimStore(tmp_path / 'claims.json')
    assert restored.get('c' * 64).status == 'RESERVED'
