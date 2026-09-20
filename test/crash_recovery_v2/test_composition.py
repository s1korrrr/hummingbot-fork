import importlib
from hashlib import sha256

import pytest

from hummingbot.strategy_v2.components.config import AcquisitionPolicy
from hummingbot.strategy_v2.components.decision_protocol import Opportunity, Prerequisite

from .test_evidence import DATASET, packet, policies

SOURCE = 'a' * 64


def api():
    prefix = 'controllers.generic.crash_recovery_v2_core.'
    for name in ('recipe', 'runtime'):
        if importlib.util.find_spec(prefix + name) is None:
            pytest.fail('missing specified v2 composition: ' + name)
    return importlib.import_module(prefix + 'recipe'), importlib.import_module(prefix + 'runtime')


def recipe(**changes):
    r, _ = api()
    sp, rp = policies()
    values = dict(schema_version=1, recipe_id='crash_recovery_reference', program_id='test-v2',
                  owner_id='test-owner', dataset=DATASET, baseline_recipe_sha256='b' * 64,
                  exit_policy_sha256='c' * 64, capital_model='spot_cash_inventory_1x', quote_budget='50',
                  mode='recovery', stress=sp, recovery=rp, acquisition=AcquisitionPolicy(),
                  lifetime_ms=5000, execution_enabled=False)
    values.update(changes)
    return r.CrashRecoveryRecipe(**values)


def checks(*, omit=(), hold=None):
    return tuple(Prerequisite(name=name, status='HOLD' if name == hold else 'PASS',
                             evidence_sha256='d' * 64)
                 for name in ('data', 'risk', 'funding', 'ownership', 'native_admission') if name not in omit)


def evaluate(state=None, prices=(), *, config=None, present=True, acquire=True, action='acquire',
             prerequisites=None, has_obligations=False, **changes):
    r, rt = api()
    config = config or recipe()
    observed = packet(prices)
    opportunity = Opportunity(scope=config.scope, setup='baseline', action=action,
                              primary=observed.observation, created_at_ms=observed.received_at_ms,
                              lifetime_ms=config.lifetime_ms) if acquire else None
    values = dict(state=state, snapshot=observed if present else None, recipe=config,
                  binding=r.bind_recipe(config, SOURCE), expected_source_sha256=SOURCE,
                  now_ms=observed.received_at_ms, opportunity=opportunity,
                  prerequisites=checks() if prerequisites is None else prerequisites,
                  has_obligations=has_obligations)
    values.update(changes)
    return rt.evaluate(**values)


def recover(config=None):
    state = None
    for prices in ((), (90,), (90, 90), (90, 90, 91), (90, 90, 91, 94)):
        result = evaluate(state, prices, config=config)
        state = result.state
    return result


def test_recipe_and_trace_seed_are_non_executing():
    x = evaluate()
    assert x.trace.status == 'HOLD' and not x.execution_authorized
    assert not x.trace.execution_authorized


def test_recovered_opportunity_needs_every_native_prerequisite():
    x = recover()
    assert x.trace.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert not x.trace.execution_authorized
    x = evaluate(x.state, (90, 90, 91, 94, 94), prerequisites=checks(hold='risk'))
    assert x.trace.status == 'HOLD' and 'risk:HOLD' in x.trace.blockers


@pytest.mark.parametrize('missing', ['data', 'risk', 'funding', 'ownership', 'native_admission'])
def test_missing_prerequisite_does_not_mean_pass(missing):
    x = recover()
    x = evaluate(x.state, (90, 90, 91, 94, 94), prerequisites=checks(omit=(missing,)))
    assert x.trace.status == 'HOLD' and missing + ':UNAVAILABLE' in x.trace.blockers


def test_protection_updates_without_entry_signal():
    x = evaluate(acquire=False)
    x = evaluate(x.state, (90,), acquire=False)
    assert x.trace is None and x.recovery.state.phase == 'SHOCK_HOLD'
    assert x.state.acquire_cursor is None


def test_same_event_cannot_rearm_after_a_risk_hold():
    x = recover()
    x = evaluate(x.state, (90, 90, 91, 94, 94), prerequisites=checks(hold='risk'))
    y = evaluate(x.state, (90, 90, 91, 94, 94))
    assert y.trace.status == 'HOLD' and y.trace.reason == 'already_observed'


def test_reduce_does_not_inherit_buy_feature_or_funding_checks():
    x = evaluate(action='reduce', prerequisites=())
    x = evaluate(x.state, (90,), action='reduce', present=False, prerequisites=())
    assert x.trace.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert not x.trace.execution_authorized
    assert x.state.acquire_cursor is None


def test_stress_only_and_off_have_distinct_sealed_behavior():
    off, veto = recipe(mode='off'), recipe(mode='stress_only')
    a = evaluate(config=off)
    a = evaluate(a.state, (90,), config=off)
    b = evaluate(config=veto)
    b = evaluate(b.state, (90,), config=veto)
    assert a.trace.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert b.trace.status == 'HOLD'
    b = evaluate(b.state, (90, 90), config=veto)
    assert b.trace.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert b.recovery.state.phase == 'STABILIZING'
    assert off.fingerprint != veto.fingerprint


def test_external_prerequisite_order_is_deterministic():
    x = recover()
    a = evaluate(x.state, (90, 90, 91, 94, 94), prerequisites=checks())
    b = evaluate(x.state, (90, 90, 91, 94, 94), prerequisites=tuple(reversed(checks())))
    assert a == b


def test_caller_cannot_override_crash_prerequisites_or_duplicate_names():
    x = recover()
    with pytest.raises(ValueError):
        evaluate(x.state, (90, 90, 91, 94, 94), prerequisites=checks() + (checks()[0],))
    with pytest.raises(ValueError):
        evaluate(x.state, (90, 90, 91, 94, 94), prerequisites=(
            Prerequisite(name='crash.stress', status='PASS', evidence_sha256='f' * 64),))


@pytest.mark.parametrize('field,value', [('execution_enabled', True), ('execution_enabled', 0),
    ('schema_version', True), ('schema_version', 2), ('mode', 'live'), ('quote_budget', True),
    ('quote_budget', 'Infinity'), ('capital_model', 'perpetual_margin'), ('lifetime_ms', True)])
def test_unsupported_recipe_combinations_reject(field, value):
    with pytest.raises(ValueError):
        recipe(**{field: value})


def test_binding_source_recipe_and_configuration_must_match():
    r, rt = api()
    config = recipe()
    binding = r.bind_recipe(config, SOURCE)
    for field in ('source_sha256', 'recipe_sha256', 'configuration_sha256', 'recipe_id'):
        bad = binding.model_copy(update={field: 'wrong' if field == 'recipe_id' else 'f' * 64})
        with pytest.raises(ValueError):
            evaluate(config=config, binding=bad)
    with pytest.raises(ValueError):
        evaluate(config=config, expected_source_sha256='0' * 64)


def test_changing_recipe_with_existing_state_does_not_reset_hold():
    x = recover()
    with pytest.raises(ValueError):
        evaluate(x.state, (90, 90, 91, 94, 94), config=recipe(mode='off'))


def test_missing_checkpoint_with_native_obligations_rejects():
    with pytest.raises(ValueError):
        evaluate(has_obligations=True)
    with pytest.raises(ValueError):
        evaluate(has_obligations=1)


def test_opportunity_is_bound_to_scope_and_current_observation():
    x = recover()
    config = recipe()
    old = packet(())
    op = Opportunity(scope=config.scope, setup='baseline', action='acquire', primary=old.observation,
                     created_at_ms=old.received_at_ms, lifetime_ms=config.lifetime_ms)
    with pytest.raises(ValueError, match='regressed'):
        evaluate(x.state, (90, 90, 91, 94, 94), opportunity=op)
    new = packet((90, 90, 91, 94, 94))
    bad = Opportunity(scope=config.scope.model_copy(update={'owner_id': 'someone-else'}), setup='baseline',
                      action='acquire', primary=new.observation, created_at_ms=new.received_at_ms,
                      lifetime_ms=config.lifetime_ms)
    with pytest.raises(ValueError):
        evaluate(x.state, (90, 90, 91, 94, 94), opportunity=bad)


def test_checkpoint_codec_requires_external_digest_and_binding():
    r, rt = api()
    x = recover()
    raw = rt.encode_checkpoint(x.state)
    binding = r.bind_recipe(recipe(), SOURCE)
    restored = rt.decode_checkpoint(raw, expected_sha256=sha256(raw.encode()).hexdigest(), binding=binding)
    assert restored == x.state
    assert evaluate(restored, (90, 90, 91, 94, 94)) == evaluate(x.state, (90, 90, 91, 94, 94))
    with pytest.raises(ValueError):
        rt.decode_checkpoint(raw, expected_sha256='0' * 64, binding=binding)
    with pytest.raises(ValueError):
        rt.decode_checkpoint(raw, expected_sha256=sha256(raw.encode()).hexdigest(),
                             binding=r.bind_recipe(recipe(mode='off'), SOURCE))


@pytest.mark.parametrize('raw', ['{"schema_version":1,"schema_version":1}', '{"x":NaN}', 'x' * 2_000_001])
def test_checkpoint_rejects_ambiguous_or_oversized_json(raw):
    r, rt = api()
    with pytest.raises(ValueError):
        rt.decode_checkpoint(raw, expected_sha256=sha256(raw.encode()).hexdigest(),
                             binding=r.bind_recipe(recipe(), SOURCE))


def test_checkpoint_has_no_orders_balances_or_mutable_inventory():
    _, rt = api()
    raw = rt.encode_checkpoint(recover().state)
    assert not any('"' + name + '"' in raw for name in ('orders', 'fills', 'balance', 'budget', 'inventory'))
