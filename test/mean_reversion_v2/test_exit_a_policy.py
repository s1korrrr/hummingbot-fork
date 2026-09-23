import hashlib
import json
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.components.decision_protocol import Prerequisite
from hummingbot.strategy_v2.components.exit_policy import NativeProfitFloor, arbitrate_exit
from hummingbot.strategy_v2.components.recipe_contract import digest
from controllers.generic.mean_reversion_v2_core.exit_a.contracts import ExitAContextV2, ExitAParametersV2
from controllers.generic.mean_reversion_v2_core.exit_a.inference import load_model, estimate
from controllers.generic.mean_reversion_v2_core.exit_a.policy import choose_action, evaluate_exit_a

H = 'a' * 64

def model_payload():
    return dict(schema='exit_a_linear_v2', training_manifest_sha256=H, feature_names=['return_1'],
                means=[0.0], scales=[1.0], horizon_bins=2, hazard_weights=None,
                payoff_weights=[0.0], payoff_intercept=-3.0, label_cutoff_ms=50,
                trained_at_ms=60, valid_until_ms=10000)

def pack(payload):
    raw=json.dumps(payload,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    return raw,hashlib.sha256(raw).hexdigest()

def context(t=100, executor='position_1', revision=1):
    binding=dict(recipe_id='exit_a_v2',recipe_sha256=H,source_sha256=H,configuration_sha256=H)
    return ExitAContextV2(recipe=binding,scope=dict(program_id='test_v2',owner_id='v2_owner',connector='okx',
        trading_pair='BNB-USDC',recipe_sha256=H,acquisition_allowed=False),
        observation=dict(dataset_sha256=H,epoch='cache_v1',revision=1,content_sha256=H,
                         event_time_ms=t-1,available_time_ms=t),
        native=dict(owner_id='v2_owner',executor_id=executor,connector='okx',trading_pair='BNB-USDC',
                    receipt_sha256=H,observed_at_ms=t,reconciled=True,fees_complete=True,remaining_base='1',
                    reserved_base='0',base_fee_fraction='0',amount_step='0.01',price_step='0.01',
                    minimum_amount='0.01',minimum_notional='1',bid='101'),
        inventory_revision=revision,full_configuration_sha256=H,decision_time_ms=t,activated=True,
        native_close_pending=False,activation_cost_quote='100',feature_names=('return_1',),feature_values=(0.0,))

def params(sha, **changes):
    values=dict(enabled=True,recipe=context().recipe,full_configuration_sha256=H,model_sha256=sha,
                training_manifest_sha256=H,margin_bps='2',max_feature_age_ms=50,max_native_age_ms=50,
                lifetime_ms=100)
    values.update(changes)
    return ExitAParametersV2(**values)

def prerequisites():
    return tuple(Prerequisite(name=n,status='PASS',evidence_sha256=H)
                 for n in ('v2_recipe','native_inventory','native_protection'))

def evaluate(c, previous=None, **changes):
    raw,sha=pack(model_payload()); m=load_model(raw,expected_sha256=sha)
    return evaluate_exit_a(c,estimate(c,m),params(sha,**changes),previous=previous,prerequisites=prerequisites())

@pytest.mark.parametrize('value,margin,expected',[('-3','2','PROPOSE_REDUCTION'),('-2','2','NO_PROPOSAL'),('0','0','NO_PROPOSAL')])
def test_strict_margin(value,margin,expected):
    assert choose_action(Decimal(value),Decimal(margin))==expected

@pytest.mark.parametrize('value,margin',[(Decimal('NaN'),Decimal(0)),(Decimal('Infinity'),Decimal(0)),(Decimal(0),Decimal(-1)),(True,Decimal(0)),(1.0,Decimal(0))])
def test_invalid_numeric_types(value,margin):
    with pytest.raises(ValueError): choose_action(value,margin)

def test_seed_then_reduce_during_entry_pause_and_replay_hold():
    first=evaluate(context()); assert first.reason=='seeded' and first.proposal is None
    c=context(101); second=evaluate(c,first.cursor)
    assert second.status=='PROPOSE_REDUCTION' and second.proposal.requested_base==Decimal(1)
    assert second.trace.execution_authorized is False and second.execution_authorized is False
    repeat=evaluate(c,second.cursor); assert repeat.reason=='already_observed' and repeat.proposal is None
    assert evaluate(c,first.cursor)==second

def test_same_event_revision_does_not_rearm():
    first=evaluate(context()); c=context(101); second=evaluate(c,first.cursor)
    data=c.model_dump(); data['observation']['revision']=2; data['observation']['content_sha256']='b'*64
    revised=evaluate(ExitAContextV2.model_validate(data),second.cursor)
    assert revised.reason=='revised_observation_requires_new_event' and revised.proposal is None

def test_two_executors_and_inventory_revisions_have_different_keys():
    one=evaluate(context()); two=evaluate(context(executor='position_2'))
    three=evaluate(context(revision=2))
    assert len({x.trace.consumption_key for x in (one,two,three)})==3
    assert evaluate(context(101,executor='position_2'),one.cursor).reason=='cursor_scope_conflict'

@pytest.mark.parametrize('field,value,reason',[('activated',False,'not_activated'),('native_close_pending',True,'native_close_pending'),('full_configuration_sha256','b'*64,'binding_mismatch')])
def test_context_gates(field,value,reason):
    c=context().model_copy(update={field:value})
    assert evaluate(c).reason==reason

def test_optional_missing_model_never_emits_a_native_stop():
    _,sha=pack(model_payload()); c=context()
    result=evaluate_exit_a(c,None,params(sha),prerequisites=prerequisites())
    assert result.proposal is None and result.reason=='estimate_unavailable'
    assert not hasattr(result,'executor_action')

def test_disabled_is_default_and_has_no_proposal():
    _,sha=pack(model_payload()); p=params(sha).model_dump(); p.pop('enabled')
    assert ExitAParametersV2(**p).enabled is False
    assert evaluate(context(),enabled=False).reason=='disabled'

@pytest.mark.parametrize('field,value',[('margin_bps',True),('max_feature_age_ms',True),('enabled','true')])
def test_no_boolean_or_string_coercion(field,value):
    _,sha=pack(model_payload())
    with pytest.raises(ValueError): params(sha,**{field:value})

def test_future_and_forged_context_reject():
    c=context().model_dump(); c['observation']['available_time_ms']=101
    with pytest.raises(ValueError): ExitAContextV2(**c)
    c=context().model_copy(update={'activation_cost_quote':Decimal('NaN')})
    assert evaluate_exit_a(c,None,params('a'*64)).reason=='invalid_input'

def test_wrong_estimate_and_controls_are_not_accepted():
    c=context(); raw,sha=pack(model_payload()); e=estimate(c,load_model(raw,expected_sha256=sha))
    assert evaluate_exit_a(context(101),e,params(sha),prerequisites=prerequisites()).reason=='estimate_context_mismatch'
    changed=e.model_copy(update={'model_sha256':'b'*64})
    assert evaluate_exit_a(c,changed,params(sha),prerequisites=prerequisites()).reason=='model_binding_mismatch'

def test_expiry_and_native_staleness():
    c=context().model_copy(update={'decision_time_ms':200})
    raw,sha=pack(model_payload()); e=estimate(c,load_model(raw,expected_sha256=sha))
    assert evaluate_exit_a(c,e,params(sha),prerequisites=prerequisites()).reason=='feature_stale'
    c=context().model_dump(); c['native']['observed_at_ms']=0
    assert evaluate(ExitAContextV2(**c)).reason=='native_snapshot_stale'

def test_missing_prerequisites_hold():
    c=context(); raw,sha=pack(model_payload()); e=estimate(c,load_model(raw,expected_sha256=sha))
    assert evaluate_exit_a(c,e,params(sha)).reason=='missing_prerequisites'

def test_native_arbitration_exact_floor_and_overlap_without_dispatch():
    first=evaluate(context()); c=context(101); r=evaluate(c,first.cursor)
    floor=NativeProfitFloor(owner_id='v2_owner',executor_id='position_1',receipt_sha256=H,
                           quantity_base='1',price='100',observed_at_ms=101)
    other=r.proposal.model_copy(update={'policy_id':'other','requested_base':Decimal('.5')})
    a=arbitrate_exit(c.native,(r.proposal,other),floor,now_ms=101,max_age_ms=50)
    assert a.amount_base==Decimal('1') and a.execution_authorized is False and a.market_fallback is False
    bad=floor.model_copy(update={'quantity_base':Decimal('.5')})
    assert arbitrate_exit(c.native,(r.proposal,),bad,now_ms=101,max_age_ms=50).reason=='native_floor_quantity_mismatch'
    assert arbitrate_exit(c.native,(r.proposal,),None,now_ms=101,max_age_ms=50).reason=='native_floor_unavailable'

def test_reserved_inventory_and_base_fee_are_not_double_spent():
    first=evaluate(context()); data=context(101).model_dump(); data['native'].update(reserved_base='0.4',base_fee_fraction='0.1')
    c=ExitAContextV2(**data); r=evaluate(c,first.cursor)
    floor=NativeProfitFloor(owner_id='v2_owner',executor_id='position_1',receipt_sha256=H,
                           quantity_base='.54',price='100',observed_at_ms=101)
    a=arbitrate_exit(c.native,(r.proposal,),floor,now_ms=101,max_age_ms=50)
    assert a.amount_base==Decimal('.54') and a.amount_base*Decimal('1.1')<=Decimal('.6')

def test_entry_pause_transition_does_not_disable_reduction_cursor():
    data=context().model_dump(); data['scope']['acquisition_allowed']=True
    first=evaluate(ExitAContextV2(**data))
    second=evaluate(context(101),first.cursor)
    assert second.status=='PROPOSE_REDUCTION'

def test_model_availability_bounds_and_nonnegative_estimate():
    c=context(); p=model_payload(); p['trained_at_ms']=101; raw,sha=pack(p)
    e=estimate(c,load_model(raw,expected_sha256=sha))
    assert not e.available and evaluate_exit_a(c,e,params(sha)).proposal is None
    p=model_payload(); p['valid_until_ms']=100; raw,sha=pack(p)
    assert estimate(c,load_model(raw,expected_sha256=sha)).available is False
    p=model_payload(); p['payoff_intercept']=4.0; raw,sha=pack(p); m=load_model(raw,expected_sha256=sha)
    first=evaluate_exit_a(c,estimate(c,m),params(sha),prerequisites=prerequisites())
    c=context(101); result=evaluate_exit_a(c,estimate(c,m),params(sha),previous=first.cursor,prerequisites=prerequisites())
    assert result.reason=='continuation_not_negative' and result.proposal is None

def test_expiry_tied_to_observation_and_not_submission_time():
    c=context().model_copy(update={'decision_time_ms':150}); raw,sha=pack(model_payload())
    e=estimate(c,load_model(raw,expected_sha256=sha))
    r=evaluate_exit_a(c,e,params(sha,lifetime_ms=40,max_feature_age_ms=500),prerequisites=prerequisites())
    assert r.reason=='opportunity_expired'

def test_incomplete_native_evidence_and_prerequisite_hold():
    for field,value in [('fees_complete',False),('reconciled',False)]:
        c=context().model_dump(); c['native'][field]=value
        assert evaluate(ExitAContextV2(**c)).reason=='native_evidence_incomplete'
    first=evaluate(context()); c=context(101); raw,sha=pack(model_payload()); m=load_model(raw,expected_sha256=sha)
    checks=tuple(p.model_copy(update={'status':'HOLD'}) if p.name=='native_inventory' else p for p in prerequisites())
    r=evaluate_exit_a(c,estimate(c,m),params(sha),previous=first.cursor,prerequisites=checks)
    assert r.proposal is None and r.reason=='prerequisite_hold'
