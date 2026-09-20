from hummingbot.strategy_v2.components.decision_protocol import Prerequisite
from controllers.generic.momentum_fusion_v2_core.setups import step
from .fixtures import observed, passing, recipe, scope
from .test_setups import advance
import pytest


def test_risk_veto_and_context_refresh_cannot_revive_event():
    r=recipe(); state,_=advance(r,None,0)
    checks=tuple(x.model_copy(update={'status':'HOLD','evidence_sha256':None}) if x.name=='risk' else x for x in passing())
    f=observed(r,1,close=103,high=103.2,low=99.9,volume=400)
    state,result=step(r,state,f,scope=scope(r),now_ms=f.primary.available_time_ms,prerequisites=checks)
    assert result.trace.status=='HOLD'
    state,result=step(r,state,f,scope=scope(r),now_ms=f.primary.available_time_ms+1,prerequisites=passing())
    assert result.trace.status=='HOLD' and result.trace.reason=='already_observed'


def test_absent_required_prerequisites_cannot_authorize():
    r=recipe(); state,_=advance(r,None,0)
    state,result=advance(r,state,1,checks=(),close=103,high=103.2,low=99.9,volume=400)
    assert result.trace.status=='HOLD'
    assert 'capital:UNAVAILABLE' in result.trace.blockers


def test_blocked_owner_still_advances_observation_watermark():
    r=recipe(); state,_=advance(r,None,0)
    checks=tuple(x.model_copy(update={'status':'HOLD','evidence_sha256':None}) if x.name=='owner_ready' else x for x in passing())
    for i in range(1,6): state,result=advance(r,state,i,checks=checks)
    state,result=advance(r,state,6,close=103,high=103.2,low=99.9,volume=400)
    assert result.trace.status=='ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert state.cursor.last_event_ms==observed(r,6).primary.event_time_ms


def test_real_gap_and_epoch_change_hold_and_clear_shelf():
    r=recipe('C1'); state,_=advance(r,None,0)
    state,_=advance(r,state,1,close=103,high=103.2,low=99.9,volume=400)
    state,result=advance(r,state,3,close=103.5,high=103.6,low=99.9,volume=400)
    assert result.reason=='stream_reseed' and state.candidate is None
    f=observed(r,4)
    f=f.model_copy(update={'primary':f.primary.model_copy(update={'epoch':'reconnected','revision':1})})
    state,result=step(r,state,f,scope=scope(r),now_ms=f.primary.available_time_ms,prerequisites=passing())
    assert result.reason=='stream_reseed'


def test_reserved_prerequisite_cannot_be_injected():
    r=recipe(); state,_=advance(r,None,0)
    with pytest.raises(ValueError):
        advance(r,state,1,checks=(*passing(),Prerequisite(name='setup',status='PASS',evidence_sha256='a'*64)))


def test_shadow_cannot_use_baseline_cursor():
    r=recipe(); state,_=advance(r,None,0); f=observed(r,1)
    with pytest.raises(ValueError):
        step(r,state,f,scope=scope(r,'shadow'),now_ms=f.primary.available_time_ms,prerequisites=passing())


def test_heartbeat_does_not_destroy_or_extend_shelf():
    r=recipe('C1'); state,_=advance(r,None,0)
    f=observed(r,1,close=103,high=103.2,low=99.9,volume=400)
    state,_=step(r,state,f,scope=scope(r),now_ms=f.primary.available_time_ms,prerequisites=passing())
    original=state.candidate
    heartbeat=f.model_copy(update={'primary':f.primary.model_copy(update={'available_time_ms':f.primary.available_time_ms+1})})
    state,result=step(r,state,heartbeat,scope=scope(r),now_ms=f.primary.available_time_ms+1,prerequisites=passing())
    assert result.trace.status=='HOLD'
    assert state.candidate == original


def test_direct_feature_adapter_rejects_wrong_higher_timeframe_boundary():
    r=recipe(); state,_=advance(r,None,0); f=observed(r,1)
    bad=f.dependencies[0].model_copy(update={'event_time_ms':f.dependencies[0].event_time_ms+1})
    f=f.model_copy(update={'dependencies':(bad, f.dependencies[1])})
    with pytest.raises(ValueError):
        step(r,state,f,scope=scope(r),now_ms=f.primary.available_time_ms,prerequisites=passing())


def test_expired_opportunity_at_exact_boundary_stays_held():
    r=recipe(opportunity_lifetime_ms=1000); state,_=advance(r,None,0)
    f=observed(r,1,close=103,high=103.2,low=99.9,volume=400)
    _,result=step(r,state,f,scope=scope(r),now_ms=f.primary.available_time_ms+1000,prerequisites=passing())
    assert result.trace.reason == 'opportunity_expired'
    assert result.trace.execution_authorized is False


def test_revised_primary_bar_cannot_reuse_armed_shelf():
    r=recipe('C1'); state,_=advance(r,None,0)
    f=observed(r,1,close=103,high=103.2,low=99.9,volume=400)
    state,_=step(r,state,f,scope=scope(r),now_ms=f.primary.available_time_ms,prerequisites=passing())
    revised=f.model_copy(update={'primary':f.primary.model_copy(update={'content_sha256':'0'*64,'revision':3})})
    state,result=step(r,state,revised,scope=scope(r),now_ms=f.primary.available_time_ms,prerequisites=passing())
    assert state.candidate is None and result.trace.status=='HOLD'


@pytest.mark.parametrize('name,status', [(n,s) for n in ('owner_ready','capital','risk') for s in ('HOLD','STALE','UNAVAILABLE')])
def test_each_required_veto_wins(name,status):
    r=recipe(); state,_=advance(r,None,0)
    checks=tuple(x.model_copy(update={'status':status,'evidence_sha256':None}) if x.name==name else x for x in passing())
    state,result=advance(r,state,1,checks=checks,close=103,high=103.2,low=99.9,volume=400)
    assert result.trace.status=='HOLD' and f'{name}:{status}' in result.trace.blockers
    assert not state.issued
