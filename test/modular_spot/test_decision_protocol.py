import importlib

import pytest


def module():
    try:
        return importlib.import_module('controllers.generic.modular_spot_core.decision_protocol')
    except ModuleNotFoundError:
        pytest.fail('decision protocol is not implemented')


def scope(m, **change):
    return m.DecisionScope.model_validate(dict(program_id='rebound', owner_id='modular_spot_rebound_BTC_USDC',
        connector='okx', trading_pair='BTC-USDC', recipe_sha256='a' * 64, **change))


def opportunity(m, stamp=1000, revision=1, **change):
    values = dict(scope=scope(m), setup='rsi_rebound', action='acquire',
                  primary=m.ObservationRef(dataset_sha256='b' * 64, epoch='one', revision=revision,
                      content_sha256=str(revision) * 64, event_time_ms=stamp, available_time_ms=stamp + 10),
                  dependencies=(), created_at_ms=stamp + 20, lifetime_ms=100, expected_return=None)
    values.update(change)
    return m.Opportunity.model_validate(values)


def test_opportunity_delivery_time_does_not_mint_new_identity():
    m = module()
    first = opportunity(m)
    later = opportunity(m, created_at_ms=1030)
    assert first.opportunity_id == later.opportunity_id
    assert first.expires_at_ms == later.expires_at_ms == 1110
    assert first.expected_return is None


def test_revision_changes_evidence_but_not_consumption_key():
    m = module()
    first = opportunity(m)
    corrected = opportunity(m, revision=2)
    assert first.opportunity_id != corrected.opportunity_id
    assert first.consumption_key == corrected.consumption_key


def test_replay_seeds_then_emits_only_one_eligible_decision():
    m = module()
    state, first = m.decide(None, opportunity(m), now_ms=1020, prerequisites=())
    assert first.status == 'HOLD' and first.reason == 'seeded'
    state2, second = m.decide(state, opportunity(m, stamp=2000), now_ms=2020, prerequisites=())
    assert second.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert second.execution_authorized is False
    _, repeated = m.decide(state2, opportunity(m, stamp=2000, created_at_ms=2030), now_ms=2030, prerequisites=())
    assert repeated.reason == 'already_observed'
    _, corrected = m.decide(state2, opportunity(m, stamp=2000, revision=2), now_ms=2040, prerequisites=())
    assert corrected.reason == 'revised_observation_requires_new_event'


def test_failed_prerequisite_consumes_event_without_rearming():
    m = module()
    state, _ = m.decide(None, opportunity(m), now_ms=1020, prerequisites=())
    blocked = m.Prerequisite(name='native_account_reconciliation', status='UNAVAILABLE', evidence_sha256=None)
    state, trace = m.decide(state, opportunity(m, 2000), now_ms=2020, prerequisites=(blocked,))
    assert trace.reason == 'prerequisite_hold'
    assert trace.blockers == ('native_account_reconciliation:UNAVAILABLE',)
    _, trace = m.decide(state, opportunity(m, 2000), now_ms=2030, prerequisites=())
    assert trace.status == 'HOLD'


def test_expired_signal_is_not_a_reservation_release():
    m = module()
    state, _ = m.decide(None, opportunity(m), now_ms=1020, prerequisites=())
    _, trace = m.decide(state, opportunity(m, 2000), now_ms=2110, prerequisites=())
    assert trace.reason == 'opportunity_expired'
    assert 'release' not in trace.model_dump()


def test_sell_only_scope_rejects_buy_proposals():
    m = module()
    buy = opportunity(m, scope=scope(m, acquisition_allowed=False))
    _, result = m.decide(None, buy, now_ms=1020, prerequisites=())
    assert result.reason == 'acquisition_forbidden'


@pytest.mark.parametrize('change', [dict(created_at_ms=True), dict(created_at_ms=1005),
    dict(created_at_ms=1120), dict(lifetime_ms=0), dict(expected_return='NaN')])
def test_invalid_time_or_estimate_rejected(change):
    m = module()
    with pytest.raises(ValueError):
        opportunity(m, **change)


def test_dependencies_must_have_been_available_at_creation():
    m = module()
    dep = opportunity(m, 2000).primary.model_copy(update={'dataset_sha256': 'c' * 64})
    with pytest.raises(ValueError, match='available'):
        opportunity(m, dependencies=(dep,))


def test_owner_recipe_clock_regression_and_tampered_state_rejected():
    m = module()
    state, _ = m.decide(None, opportunity(m), now_ms=1020, prerequisites=())
    other = state.model_dump(mode='json')
    other['scope']['owner_id'] = 'different'
    with pytest.raises(ValueError, match='scope'):
        m.decide(m.DecisionCursor.model_validate(other), opportunity(m, 2000), now_ms=2020, prerequisites=())
    with pytest.raises(ValueError, match='clock'):
        m.decide(state, opportunity(m), now_ms=1019, prerequisites=())
    with pytest.raises(ValueError):
        m.decide(state.model_copy(update={'last_event_ms': -1}), opportunity(m, 2000), now_ms=2020, prerequisites=())


def test_replay_is_byte_deterministic_and_bounded():
    m = module()
    events = tuple((opportunity(m, n), n + 20, ()) for n in (1000, 2000, 3000))
    a = m.replay(events)
    b = m.replay(events)
    assert a == b and a['economic_backtest'] is False
    assert a['eligible_count'] == 2
    with pytest.raises(ValueError, match='bounded'):
        m.replay(events, max_events=2)


def test_pass_prerequisite_needs_source_identity():
    m = module()
    with pytest.raises(ValueError, match='evidence'):
        m.Prerequisite(name='data', status='PASS', evidence_sha256=None)
