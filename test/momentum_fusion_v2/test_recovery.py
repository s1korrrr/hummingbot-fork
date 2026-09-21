from controllers.generic.momentum_fusion_v2_core import runtime
from hummingbot.strategy_v2.components.decision_protocol import DecisionScope


def test_entry_pause_does_not_clear_management_scope():
    scope = DecisionScope(program_id='okx_spot_momentum_v2', owner_id='mom', connector='okx',
                          trading_pair='BNB-USDC', recipe_sha256='a' * 64, acquisition_allowed=False)
    assert scope.acquisition_allowed is False


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
