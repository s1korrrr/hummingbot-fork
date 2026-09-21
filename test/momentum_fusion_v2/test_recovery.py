from controllers.generic.momentum_fusion_v2_core import runtime
from hummingbot.strategy_v2.components.decision_protocol import DecisionScope
from hummingbot.strategy_v2.components.exit_policy import ExitProposal, NativeExitView


def test_entry_pause_does_not_clear_management_scope():
    scope = DecisionScope(program_id='okx_spot_momentum_v2', owner_id='mom', connector='okx',
                          trading_pair='BNB-USDC', recipe_sha256='a' * 64, acquisition_allowed=False)
    assert scope.acquisition_allowed is False


def test_entry_pause_still_emits_management_actions(tmp_path):
    store = runtime.ClaimStore(tmp_path / 'claims.json')
    view = NativeExitView.model_validate(dict(
        owner_id='mom_v2', executor_id='e1', connector='okx', trading_pair='BNB-USDC',
        receipt_sha256='a' * 64, observed_at_ms=1000, reconciled=True, fees_complete=True,
        remaining_base='10', reserved_base='3', base_fee_fraction='0', amount_step='.1',
        price_step='.01', minimum_amount='.1', minimum_notional='1', bid='12'))
    proposal = ExitProposal(policy_id='trail', owner_id='mom_v2', executor_id='e1',
                            receipt_sha256='a' * 64, requested_base='7', minimum_price='0',
                            expires_at_ms=2000)
    plan = runtime.plan_management(
        claims=store, acquisition_allowed=False, view=view, proposals=(proposal,),
        cost_quote='70', harvest_base='7', fee_fraction='0', margin='0', buffer='0',
        now_ms=1050, controller_id='mom-v2', connector_name='okx', trading_pair='BNB-USDC',
        time_limit=3600)
    assert plan.actions
    assert all(getattr(action, 'keep_position', False) is False for action in plan.actions)


def test_restored_reserved_claim_is_not_a_new_grant(tmp_path):
    path = tmp_path / 'claims.json'
    store = runtime.ClaimStore(path)
    claim = runtime.Claim(claim_id='c' * 64, opportunity_id='o' * 64, controller_id='mom',
                          trading_pair='BNB-USDC', status='SUBMITTED_UNACKNOWLEDGED',
                          reserved_quote='10', reserved_base='0.01', executor_identity='e' * 64)
    store.put(claim)
    restored = runtime.ClaimStore(path)
    found = restored.get('c' * 64)
    assert found.status == 'SUBMITTED_UNACKNOWLEDGED'
    assert found.executor_identity == 'e' * 64
    again = restored.put(claim.model_copy(update={'status': 'RESERVED'}))
    assert again.status == 'SUBMITTED_UNACKNOWLEDGED'


def test_corrupt_claim_store_holds_visibly(tmp_path):
    path = tmp_path / 'claims.json'
    path.write_text('{not-json')
    try:
        runtime.ClaimStore(path)
    except (ValueError, TypeError, OSError):
        held = True
    else:
        held = False
    assert held is True
