from decimal import Decimal as D

from controllers.generic.momentum_fusion_v2_core import runtime
from hummingbot.strategy_v2.components.exit_policy import ExitProposal, NativeExitView, NativeProfitFloor


def view(**changes):
    value = dict(owner_id='mom_v2', executor_id='e1', connector='okx', trading_pair='BNB-USDC',
                 receipt_sha256='a' * 64, observed_at_ms=1000, reconciled=True, fees_complete=True,
                 remaining_base='10', reserved_base='3', base_fee_fraction='0', amount_step='.1',
                 price_step='.01', minimum_amount='.1', minimum_notional='1', bid='12')
    value.update(changes)
    return NativeExitView.model_validate(value)


def proposal(policy, quantity, price='0'):
    return ExitProposal(policy_id=policy, owner_id='mom_v2', executor_id='e1', receipt_sha256='a' * 64,
                        requested_base=quantity, minimum_price=price, expires_at_ms=2000)


def floor(quantity='7'):
    return NativeProfitFloor(owner_id='mom_v2', executor_id='e1', receipt_sha256='a' * 64,
                             quantity_base=quantity, price='11.00', observed_at_ms=1000)


def test_trail_plus_exhaustion_cannot_exceed_uncommitted():
    result = runtime.one_exit(view(), (proposal('trail', '10'), proposal('exhaustion', '4')),
                              floor('7'), now_ms=1050)
    assert result.status == 'PROPOSED_REDUCTION'
    assert result.amount_base == D('7')
    assert result.execution_authorized is False
    assert result.market_fallback is False


def test_bid_below_floor_does_not_market_out():
    result = runtime.one_exit(view(bid='10'), (proposal('trail', '7'),), floor('7'), now_ms=1050)
    assert result.status == 'HOLD'
    assert result.reason == 'bid_below_profit_floor'
    assert result.market_fallback is False


def test_missing_fees_block_the_sale():
    result = runtime.one_exit(view(fees_complete=False), (proposal('trail', '7'),), floor('7'), now_ms=1050)
    assert result.reason == 'native_fees_incomplete'


def test_management_emits_while_acquisition_is_paused(tmp_path):
    store = runtime.ClaimStore(tmp_path / 'claims.json')
    plan = runtime.plan_management(
        claims=store, acquisition_allowed=False, view=view(),
        proposals=(proposal('trail', '7'),), cost_quote='70', harvest_base='7',
        fee_fraction='0', margin='0', buffer='0', now_ms=1050, controller_id='mom-v2',
        connector_name='okx', trading_pair='BNB-USDC', time_limit=3600)
    assert plan.actions
    assert plan.reason == 'proposed_reduction'
    assert plan.actions[0].executor_config.side.name == 'SELL'


def test_bid_below_floor_management_does_not_emit(tmp_path):
    store = runtime.ClaimStore(tmp_path / 'claims.json')
    plan = runtime.plan_management(
        claims=store, acquisition_allowed=True, view=view(bid='10'),
        proposals=(proposal('trail', '7'),), cost_quote='77', harvest_base='7',
        fee_fraction='0', margin='0', buffer='0', now_ms=1050, controller_id='mom-v2',
        connector_name='okx', trading_pair='BNB-USDC', time_limit=3600)
    assert plan.actions == ()
    assert plan.reason == 'bid_below_profit_floor'


def test_management_without_claim_store_is_rejected_with_reason():
    plan = runtime.plan_management(
        claims=None, acquisition_allowed=False, view=view(),
        proposals=(proposal('trail', '7'),), cost_quote='70', harvest_base='7',
        fee_fraction='0', margin='0', buffer='0', now_ms=1050, controller_id='mom-v2',
        connector_name='okx', trading_pair='BNB-USDC', time_limit=3600)
    assert plan.actions == ()
    assert plan.reason == 'claim_store_required'
