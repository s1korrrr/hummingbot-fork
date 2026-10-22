from decimal import Decimal as D

import pytest
from hummingbot.strategy_v2.components.feature_registry import FeatureRegistry
from controllers.generic.momentum_fusion_v2_core.features import FeaturePipeline, millis
from .fixtures import T, recipe, frames, observed


def test_actual_registry_prior_only_features_and_identity(r):
    pipeline = FeaturePipeline(r)
    assert isinstance(pipeline.registry, FeatureRegistry)
    data = frames(r, close=103, high=103.2, low=99.9, volume=400)
    frozen = {k:v.model_dump_json() for k,v in data.items()}
    value = pipeline.evaluate(data, now_ms=(T+1)*1000)
    assert value.values.prior_high == D('100.2')
    assert abs(value.values.atr-D('.4')) < D('1e-10')
    assert value.values.relative_volume == D('4')
    assert value.values.trend_5m == value.values.trend_15m == 1
    assert value.primary.event_time_ms == T*1000
    assert value.primary.available_time_ms == (T+1)*1000
    assert len(value.dependencies) == 2
    assert frozen == {k:v.model_dump_json() for k,v in data.items()}
    assert value == pipeline.evaluate(data, now_ms=(T+2)*1000)
    changed = observed(r, close=1000, high=1001, low=99.9, volume=400)
    assert changed.values.atr == value.values.atr
    assert changed.values.prior_high == value.values.prior_high


def test_missing_and_stale_inputs_do_not_fallback(r):
    pipeline = FeaturePipeline(r); data = frames(r)
    with pytest.raises(ValueError):
        pipeline.evaluate({r.datasets[0].key:data[r.datasets[0].key]}, now_ms=(T+1)*1000)
    with pytest.raises(ValueError):
        pipeline.evaluate(data, now_ms=(T+20)*1000)
    with pytest.raises(ValueError):
        pipeline.evaluate(data, now_ms=T*1000)


def test_revalidated_copies_reject_gap_finality_wrong_scope(r):
    pipeline = FeaturePipeline(r)
    for mode in ('gap','unconfirmed','scope','future'):
        data = frames(r); old = data[r.datasets[0].key]
        if mode=='gap': new = old.model_copy(update={'rows':old.rows[:-2]+old.rows[-1:]})
        elif mode=='unconfirmed':
            new = old.model_copy(update={'rows':old.rows[:-1]+(old.rows[-1].model_copy(update={'confirmed':False}),)})
        elif mode=='scope': new = old.model_copy(update={'dataset':r.datasets[1]})
        else: new = old.model_copy(update={'published_at':T+20})
        data[r.datasets[0].key] = new
        with pytest.raises(ValueError): pipeline.evaluate(data, now_ms=(T+1)*1000)


def test_feature_identity_changes_with_parameters_or_initialization():
    a=FeaturePipeline(recipe()); b=FeaturePipeline(recipe(features={'lookback':21}))
    c=FeaturePipeline(recipe(initialization_sha256='2'*64))
    assert len({a.contract_sha256,b.contract_sha256,c.contract_sha256}) == 3
    assert a.contract_sha256 == FeaturePipeline(recipe('C1')).contract_sha256


@pytest.mark.parametrize('value', [True, -1, float('nan'), float('inf'), '0.0001', '9007199254740.992'])
def test_time_conversion_never_truncates(value):
    with pytest.raises(ValueError): millis(value)


def test_millisecond_precision():
    assert millis('180001.123') == 180001123


def test_flat_zero_reference_volume_is_unavailable(r):
    data=frames(r); old=data[r.datasets[0].key]
    data[r.datasets[0].key]=old.model_copy(update={'rows':tuple(x.model_copy(update={'volume':D(0)}) for x in old.rows)})
    with pytest.raises(ValueError): FeaturePipeline(r).evaluate(data, now_ms=(T+1)*1000)


def test_cached_output_is_defensive_and_source_identity_is_checked(r, monkeypatch):
    pipeline=FeaturePipeline(r); data=frames(r)
    before=pipeline.evaluate(data,now_ms=(T+1)*1000)
    # Canonical frozen result copies must not poison the cache through model_copy.
    altered=before.values.model_copy(update={'close':D(999)})
    assert altered.close != pipeline.evaluate(data,now_ms=(T+1)*1000).values.close
    from hummingbot.strategy_v2.components import feature_registry
    original=feature_registry._callable_identity
    monkeypatch.setattr(feature_registry,'_callable_identity',lambda f:('f'*64,original(f)[1]))
    with pytest.raises(ValueError): pipeline.evaluate(data,now_ms=(T+1)*1000)


def test_duplicate_same_sequence_different_candle_is_not_cached_as_same_input(r):
    pipeline=FeaturePipeline(r)
    a=pipeline.evaluate(frames(r),now_ms=(T+1)*1000)
    b=pipeline.evaluate(frames(r,high=104,close=103,volume=400),now_ms=(T+1)*1000)
    assert a.primary.content_sha256 != b.primary.content_sha256
    assert a.result_sha256 != b.result_sha256


def test_stale_higher_bucket_not_substituted(r):
    data=frames(r,5); higher=data[r.datasets[1].key]
    data[r.datasets[1].key]=higher.model_copy(update={'rows':tuple(row.model_copy(update={'open_time':row.open_time-300}) for row in higher.rows)})
    with pytest.raises(ValueError): FeaturePipeline(r).evaluate(data,now_ms=(T+301)*1000)


@pytest.mark.parametrize('flag', [True, -1, 1.5, 2**53])
def test_invalid_decision_clock(r,flag):
    with pytest.raises(ValueError): FeaturePipeline(r).evaluate(frames(r),now_ms=flag)
