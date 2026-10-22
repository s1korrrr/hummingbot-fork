import hashlib
import json
import math

import pytest

from controllers.generic.mean_reversion_v2_core.exit_a.inference import load_model, predict_values

H='a'*64

def payload():
    return dict(schema='exit_a_linear_v2',training_manifest_sha256=H,feature_names=['x'],means=[0.0],scales=[1.0],
                horizon_bins=2,hazard_weights=[[0.0,0.0,0.0]]*3,payoff_weights=[0.0,1.0,2.0,3.0],
                payoff_intercept=0.0,label_cutoff_ms=0,trained_at_ms=1,valid_until_ms=100)

def load(p):
    b=json.dumps(p,separators=(',',':')).encode(); return load_model(b,expected_sha256=hashlib.sha256(b).hexdigest())

def test_hazard_math_and_export_prediction():
    m=load(payload()); value,incidence=predict_values(m,('x',),(0.0,))
    assert incidence==pytest.approx((4/9,4/9,1/9))
    assert value==pytest.approx(5/3)
    assert load(payload())==m

@pytest.mark.parametrize('key,value',[('scales',[0]),('means',[True]),('horizon_bins',65),('feature_names',['x','x']),('payoff_weights',[0]),('trained_at_ms',-1),('schema','other'),('valid_until_ms',0)])
def test_bad_artifact(key,value):
    p=payload(); p[key]=value
    with pytest.raises(ValueError): load(p)

@pytest.mark.parametrize('raw',[b'{"schema":1,"schema":2}',b'{"x":NaN}',b'[]',b'['*20+b'0'+b']'*20,b'x'*1048577])
def test_bounded_strict_json(raw):
    with pytest.raises(ValueError): load_model(raw,expected_sha256=hashlib.sha256(raw).hexdigest())

def test_pin_required_and_mismatch_rejects():
    raw=json.dumps(payload()).encode()
    with pytest.raises(ValueError): load_model(raw,expected_sha256='b'*64)

def test_bad_features_reject_not_reorder():
    m=load(payload())
    for names,values in [(('other',),(0.0,)),(('x',),(True,)),(('x',),(math.inf,)),(('x',),('0',))]:
        with pytest.raises(ValueError): predict_values(m,names,values)

def test_extreme_logits_stable_and_nonfinite_output_rejects():
    p=payload(); p['hazard_weights']=[[1000.0,0.0,0.0],[-1000.0,0.0,0.0],[0.0,0.0,0.0]]
    value,probs=predict_values(load(p),('x',),(0.0,)); assert probs==pytest.approx((1,0,0))
    p=payload(); p['payoff_weights'][0]=1e308
    with pytest.raises(ValueError): predict_values(load(p),('x',),(1e308,))

def test_oversized_integer_input_is_a_controlled_validation_error():
    with pytest.raises(ValueError): predict_values(load(payload()),('x',),(10**400,))
