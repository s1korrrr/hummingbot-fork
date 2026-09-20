import pytest
from controllers.generic.momentum_fusion_v2_core.setups import PolicyState, step
from .fixtures import recipe, scope, observed, passing


def advance(r, state, i, checks=None, **values):
    f=observed(r,i,**values)
    return step(r,state,f,scope=scope(r),now_ms=f.primary.available_time_ms,
                prerequisites=passing() if checks is None else checks)


def test_baseline_seeds_then_proposes_but_never_authorizes():
    r=recipe(); state, out=advance(r,None,0)
    assert out.reason == 'seeded'
    state, out=advance(r,state,1,close=103,high=103.2,low=99.9,volume=400)
    assert out.trace.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert out.trace.execution_authorized is False
    state, later=advance(r,state,2,close=104,high=104.2,low=103.5,open=104,volume=400)
    assert later.reason == 'episode_consumed'


def test_shelf_waits_for_two_bars_then_breaks_with_replayable_state():
    r=recipe('C1'); state,_=advance(r,None,0)
    state,out=advance(r,state,1,close=103,high=103.2,low=99.9,volume=400)
    assert out.reason=='shelf_armed'
    for i in (2,3):
        state,out=advance(r,state,i,open=102.9,close=103,high=103.1,low=102.7,volume=120)
        assert out.reason=='shelf_building'
    restored=PolicyState.model_validate_json(state.model_dump_json())
    args=dict(open=103,close=103.5,high=103.6,low=102.9,volume=400)
    final, result=advance(r,restored,4,**args)
    assert result.trace.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert (final,result)==advance(r,state,4,**args)
    assert state.candidate.bars == 2


def test_expiry_and_failed_structure_do_not_qualify():
    r=recipe('C1',shelf={'ttl_ms':120000}); state,_=advance(r,None,0)
    state,_=advance(r,state,1,close=103,high=103.2,low=99.9,volume=400)
    state,result=advance(r,state,2,open=100,close=100,high=100.3,low=99.9,volume=100)
    assert result.reason=='shelf_invalidated' and state.candidate is None
    state,_=advance(r,None,0)
    state,_=advance(r,state,1,close=103,high=103.2,low=99.9,volume=400)
    state,_=advance(r,state,2,open=103,close=103,high=103.1,low=102.9,volume=120)
    state,result=advance(r,state,3,open=103,close=103,high=103.1,low=102.9,volume=120)
    assert result.reason=='shelf_expired'


def test_state_is_scoped_and_clocks_cannot_regress():
    r=recipe(); state,_=advance(r,None,1)
    f=observed(r,2)
    with pytest.raises(ValueError):
        step(r,state,f,scope=scope(r,owner='momentum_v2_other'),now_ms=f.primary.available_time_ms,prerequisites=passing())
    with pytest.raises(ValueError): advance(r,state,0)
    with pytest.raises(ValueError): advance(recipe('C1'),state,2)


def test_failed_setup_requires_rearm_and_a_new_breakout():
    r=recipe('C1'); state,_=advance(r,None,0)
    state,_=advance(r,state,1,close=103,high=103.2,low=99.9,volume=400)
    state,failed=advance(r,state,2,close=100,high=100.2,low=99.8,volume=100)
    assert failed.reason=='shelf_invalidated'
    state,reset=advance(r,state,3)
    assert reset.reason=='waiting_rearm' and not state.needs_reset
    state,armed=advance(r,state,4,close=103,high=103.2,low=99.9,volume=400)
    assert armed.reason=='shelf_armed'


def test_premature_continuation_is_not_chased():
    r=recipe('C1'); state,_=advance(r,None,0)
    state,_=advance(r,state,1,close=103,high=103.2,low=99.9,volume=400)
    state,out=advance(r,state,2,open=103,close=104,high=104.1,low=102.9,volume=400)
    assert out.reason=='premature_breakout' and out.trace.status=='HOLD'


def test_excessive_shelf_length_and_extension_reject():
    r=recipe('C1',shelf={'min_bars':1,'max_bars':1}); state,_=advance(r,None,0)
    state,_=advance(r,state,1,close=103,high=103.2,low=99.9,volume=400)
    state,_=advance(r,state,2,open=103,close=103,high=103.1,low=102.9,volume=100)
    state,out=advance(r,state,3,open=103,close=103,high=103.1,low=102.9,volume=100)
    assert out.reason=='shelf_invalidated'
    r=recipe('C1',shelf={'max_avwap_extension':'.001'}); state,_=advance(r,None,0)
    state,_=advance(r,state,1,close=103,high=103.2,low=99.9,volume=400)
    for i in (2,3): state,_=advance(r,state,i,open=103,close=103,high=103.1,low=102.9,volume=100)
    state,out=advance(r,state,4,open=103,close=104,high=104.1,low=102.9,volume=400)
    assert out.reason=='avwap_extension' and out.trace.status=='HOLD'


def test_corrupt_or_changed_state_does_not_silently_restore():
    r=recipe(); state,_=advance(r,None,0)
    for change in [{'schema_version':True}, {'issued':'false'}, {'revision':0},
                   {'feature_contract_sha256':'b'*64}, {'scope':scope(recipe('C1'))}]:
        with pytest.raises(ValueError): advance(r,state.model_copy(update=change),1)


@pytest.mark.parametrize('change', [{'bars':10}, {'expires_at_ms':999999999}, {'armed_event_ms':1}])
def test_restored_shelf_cannot_expand_recipe_window(change):
    r=recipe('C1'); state,_=advance(r,None,0)
    state,_=advance(r,state,1,close=103,high=103.2,low=99.9,volume=400)
    bad=state.model_copy(update={'candidate':state.candidate.model_copy(update=change)})
    with pytest.raises(ValueError):
        advance(r,bad,2,open=103,close=104,high=104.1,low=102.9,volume=400)
