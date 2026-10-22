from decimal import Decimal as D

import pytest

from controllers.generic.momentum_fusion_v2_runtime import runtime
from hummingbot.core.data_type.common import OrderType
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
    with pytest.raises(PolicyError, match='reserved capacity'):
        store.put(claim.model_copy(update={'reserved_quote': '80'}))
    assert first.reserved_quote == D('60')
    restored = runtime.ClaimStore(tmp_path / 'claims.json')
    assert restored.get('c' * 64).status == 'RESERVED'


def test_claim_status_advances_reserved_to_submitted(tmp_path):
    store = runtime.ClaimStore(tmp_path / 'claims.json')
    reserved = runtime.Claim(claim_id='c' * 64, opportunity_id='o' * 64, controller_id='mom',
                             trading_pair='BNB-USDC', status='RESERVED', reserved_quote='60',
                             reserved_base='0.07')
    store.put(reserved)
    submitted = store.put(reserved.model_copy(update={'status': 'SUBMITTED_UNACKNOWLEDGED',
                                                      'executor_identity': 'e' * 64}))
    assert submitted.status == 'SUBMITTED_UNACKNOWLEDGED'
    assert runtime.ClaimStore(tmp_path / 'claims.json').get('c' * 64).status == 'SUBMITTED_UNACKNOWLEDGED'


def _acquire(tmp_path, **over):
    store = over.pop('claims', runtime.ClaimStore(tmp_path / 'claims.json'))
    values = dict(
        claims=store, execution_enabled=True, acquisition_allowed=True, recovery_hold='',
        data_ready=True, signal=1, last_decision=None, decision_time=1_700_000_000,
        now=1_700_000_005, max_candle_age_seconds=10, pending_inflight=False,
        controller_id='mom-v2', connector_name='okx_paper_trade', trading_pair='BNB-USDC',
        available_quote='100', total_amount_quote='100', trade_quote='60', risk_fraction='0.05',
        price='100', atr='1', atr_stop_multiple='2', maximum_stop_fraction='0.10',
        take_profit='0.04', fee_buffer='0.003', amount_step='0.01',
        minimum_amount='0.01', minimum_notional='1',
    )
    values.update(over)
    return runtime.plan_acquisition(**values)


def test_emit_without_claim_store_is_rejected_with_reason(tmp_path):
    plan = _acquire(tmp_path, claims=None)
    assert plan.actions == ()
    assert plan.reason == 'claim_store_required'


def test_rejected_entry_never_omits_reason(tmp_path):
    stopped = _acquire(tmp_path, atr='0')
    assert stopped.actions == ()
    assert stopped.reason
    sized = _acquire(tmp_path, minimum_notional='10000')
    assert sized.actions == ()
    assert sized.reason


@pytest.mark.asyncio
async def test_open_claim_binds_the_second_sixty_unit_plan(tmp_path, native_evidence):
    first = _acquire(tmp_path, native_evidence=native_evidence)
    assert first.actions
    assert first.reason == 'submitted'
    second = _acquire(tmp_path, decision_time=1_700_000_001, now=1_700_000_005, native_evidence=native_evidence)
    assert second.actions == ()
    assert second.reason in {'account_limit', 'program_limit', 'available_quote_limit'}


@pytest.mark.asyncio
async def test_restart_does_not_reemit_submitted_claim(tmp_path, native_evidence):
    first = _acquire(tmp_path, native_evidence=native_evidence)
    assert first.actions
    restored = runtime.ClaimStore(tmp_path / 'claims.json')
    again = _acquire(tmp_path, claims=restored, last_decision=None, native_evidence=native_evidence)
    assert again.actions == ()
    assert again.reason == 'claim_already_open'


@pytest.mark.asyncio
async def test_buy_stop_loss_order_type_is_market_for_frozen_position_executor_init(tmp_path, native_evidence):
    plan = _acquire(tmp_path, native_evidence=native_evidence)
    assert plan.actions
    barriers = plan.actions[0].executor_config.triple_barrier_config
    assert barriers.stop_loss_order_type == OrderType.MARKET
    assert barriers.time_limit_order_type == OrderType.MARKET
    assert barriers.time_limit is None


@pytest.mark.asyncio
async def test_full_account_inventory_and_unvalued_assets_are_not_empty_exposures(tmp_path, native_evidence):
    market = native_evidence.session._connector
    market.set_balance('BNB', D('1'))
    snapshot = native_evidence.snapshot()
    assert sum(row.marked_inventory_quote for row in snapshot.exposures) == 99
    assert _acquire(tmp_path, native_evidence=native_evidence).actions == ()
    market.set_balance('UNKNOWN', D('1'))
    plan = _acquire(tmp_path, native_evidence=native_evidence)
    assert plan.actions == ()
    assert 'valuation unavailable' in plan.reason


def test_free_quote_alone_cannot_fabricate_native_account_admission(tmp_path):
    plan = _acquire(tmp_path, available_quote='1000000000')
    assert plan.actions == ()
    assert plan.reason == 'native_paper_account_evidence_required'
