import importlib
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from controllers.generic.modular_spot_core.candle_data import Candle, CandleSnapshot, Dataset


def api():
    try:
        return importlib.import_module('controllers.generic.modular_spot_core.feature_registry')
    except ModuleNotFoundError:
        pytest.fail('The provenance-aware feature registry is not implemented')


def snapshot(**changes):
    d = Dataset(connector='okx', trading_pair='BTC-USDC', history=60)
    values = dict(dataset=d, epoch='epoch1', sequence=1, published_at=3600,
                  rows=tuple(Candle(open_time=t, open=100, high=101, low=99, close=100, volume=1)
                             for t in range(0, 3600, 60)))
    values.update(changes)
    return CandleSnapshot(**values)


def close_value(frame, dependencies, parameters):
    return {'value': frame['close'].iloc[-1] * parameters.get('scale', 1)}


def doubled(frame, dependencies, parameters):
    return {'value': dependencies['close'].as_dict()['value'] * 2}


def spec(name='close', function=close_value, **changes):
    values = dict(name=name, version=1, dataset_key=snapshot().dataset.key,
                  implementation_sha256=sha256(Path(__file__).read_bytes()).hexdigest(),
                  implementation_symbol=function.__module__ + '.' + function.__qualname__,
                  initialization_sha256=sha256(b'full-window-v1').hexdigest(), required_fields=('close',),
                  min_records=60)
    values.update(changes)
    return api().FeatureSpec(**values)


def test_registry_reuses_existing_cache_and_returns_immutable_results():
    m = api()
    from controllers.generic.modular_spot_core.feature_cache import FeatureCache
    registry = m.FeatureRegistry(capacity=2)
    registry.register(spec(), close_value)
    value = registry.evaluate('close', {snapshot().dataset.key: snapshot()}, now=3601)
    assert isinstance(registry.cache, FeatureCache)
    assert value.as_dict() == {'value': Decimal('100.0')}
    copied = value.as_dict()
    copied['value'] = 0
    assert value.as_dict()['value'] == 100
    with pytest.raises(ValueError):
        value.values = ()


def test_dependencies_use_the_same_decision_time_and_have_bound_provenance():
    registry = api().FeatureRegistry()
    registry.register(spec(), close_value)
    registry.register(spec('double', doubled, dependencies=('close',)), doubled)
    result = registry.evaluate('double', {snapshot().dataset.key: snapshot()}, now=3601)
    assert result.as_dict()['value'] == 200
    assert len(result.dependency_sha256) == 1
    assert result.available_at <= 3601


def test_cache_hit_still_checks_transport_and_market_freshness():
    registry = api().FeatureRegistry()
    registry.register(spec(), close_value)
    registry.evaluate('close', {snapshot().dataset.key: snapshot()}, now=3601)
    with pytest.raises(ValueError):
        registry.evaluate('close', {snapshot().dataset.key: snapshot()}, now=3616)
    refreshed = snapshot(published_at=3800)
    with pytest.raises(ValueError):
        registry.evaluate('close', {refreshed.dataset.key: refreshed}, now=3801)


def test_changed_epoch_or_revision_changes_result_identity_even_when_values_match():
    registry = api().FeatureRegistry()
    registry.register(spec(), close_value)
    original = snapshot()
    a = registry.evaluate('close', {original.dataset.key: original}, now=3601)
    changed = snapshot(epoch='epoch2')
    b = registry.evaluate('close', {changed.dataset.key: changed}, now=3601)
    corrected = snapshot(sequence=2)
    c = registry.evaluate('close', {corrected.dataset.key: corrected}, now=3601)
    assert a.as_dict() == b.as_dict() == c.as_dict()
    assert len({a.result_sha256, b.result_sha256, c.result_sha256}) == 3


def test_snapshot_correction_recomputes_and_does_not_mutate_native_rows():
    registry = api().FeatureRegistry()
    registry.register(spec(), close_value)
    original = snapshot()
    a = registry.evaluate('close', {original.dataset.key: original}, now=3601)
    rows = (*original.rows[:-1], Candle(open_time=3540, open=100, high=102, low=99, close=102, volume=1))
    corrected = snapshot(sequence=2, rows=rows)
    b = registry.evaluate('close', {corrected.dataset.key: corrected}, now=3601)
    assert a.as_dict()['value'] == 100
    assert b.as_dict()['value'] == 102
    assert original.rows[-1].close == 100


def test_missing_fields_are_not_zero():
    registry = api().FeatureRegistry()
    registry.register(spec(required_fields=('quote_asset_volume',)), close_value)
    with pytest.raises(ValueError):
        registry.evaluate('close', {snapshot().dataset.key: snapshot()}, now=3601)


def test_cycles_and_missing_dependencies_fail_before_computation():
    registry = api().FeatureRegistry()
    registry.register(spec(dependencies=('double',)), close_value)
    registry.register(spec('double', doubled, dependencies=('close',)), doubled)
    with pytest.raises(ValueError):
        registry.evaluate('close', {snapshot().dataset.key: snapshot()}, now=3601)
    registry = api().FeatureRegistry()
    registry.register(spec(dependencies=('missing',)), close_value)
    with pytest.raises(ValueError):
        registry.evaluate('close', {snapshot().dataset.key: snapshot()}, now=3601)


def test_source_symbol_mismatch_cannot_register():
    registry = api().FeatureRegistry()
    with pytest.raises(ValueError):
        registry.register(spec(implementation_sha256='0' * 64), close_value)
    with pytest.raises(ValueError):
        registry.register(spec(implementation_symbol='foreign.symbol'), close_value)


def test_registration_freezes_after_first_evaluation():
    registry = api().FeatureRegistry()
    registry.register(spec(), close_value)
    registry.evaluate('close', {snapshot().dataset.key: snapshot()}, now=3601)
    with pytest.raises(ValueError):
        registry.register(spec('double', doubled), doubled)


@pytest.mark.parametrize('parameters', ['{"x":1,"x":2}', '{"x":NaN}', '[]'])
def test_ambiguous_or_nonobject_parameters_are_rejected(parameters):
    with pytest.raises(ValueError):
        spec(parameters_json=parameters)


def test_incomplete_or_future_learned_artifacts_fail_closed():
    with pytest.raises(ValueError):
        spec(model_sha256='a' * 64)
    registry = api().FeatureRegistry()
    registry.register(spec(model_sha256='a' * 64, training_cutoff=3600, artifact_available_at=3600), close_value)
    with pytest.raises(ValueError):
        registry.evaluate('close', {snapshot().dataset.key: snapshot()}, now=3601)


def test_cross_timeframe_dependency_cannot_use_future_publication():
    hourly = Dataset(connector='okx', trading_pair='BTC-USDC', interval='1h', history=60)
    registry = api().FeatureRegistry()
    registry.register(spec('hourly', dataset_key=hourly.key), close_value)
    registry.register(spec(dependencies=('hourly',)), close_value)
    with pytest.raises(ValueError):
        registry.evaluate('close', {snapshot().dataset.key: snapshot()}, now=3601)


def invalid_result(frame, dependencies, parameters):
    return {'value': float('nan')}


def test_nonfinite_feature_output_is_never_cached():
    registry = api().FeatureRegistry()
    registry.register(spec(function=invalid_result), invalid_result)
    with pytest.raises(ValueError):
        registry.evaluate('close', {snapshot().dataset.key: snapshot()}, now=3601)
    assert len(registry.cache.values) == 0


def test_callable_closure_cannot_hide_unversioned_state():
    captured = 4

    def hidden_state(frame, dependencies, parameters):
        return {'value': captured}
    registry = api().FeatureRegistry()
    with pytest.raises(ValueError, match='stateless'):
        registry.register(spec(function=hidden_state), hidden_state)
