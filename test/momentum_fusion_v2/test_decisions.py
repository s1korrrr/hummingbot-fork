from hummingbot.strategy_v2.components.decision_protocol import (
    DecisionScope,
    ObservationRef,
    Opportunity,
    Prerequisite,
    decide,
)


def scope(**change):
    return DecisionScope(program_id='okx_spot_momentum_v2', owner_id='mom_bnb',
                         connector='okx', trading_pair='BNB-USDC', recipe_sha256='a' * 64, **change)


def event(t, revision=1, setup='b0_breakout'):
    ref = ObservationRef(dataset_sha256='b' * 64, epoch='fixture', revision=revision,
                         content_sha256=str(revision) * 64, event_time_ms=t, available_time_ms=t)
    return Opportunity(scope=scope(), setup=setup, action='acquire', primary=ref,
                       created_at_ms=t, lifetime_ms=10_000)


def test_seed_replay_revision_and_no_authority():
    checks = (Prerequisite(name='data', status='PASS', evidence_sha256='d' * 64),)
    cursor, seed = decide(None, event(1000), now_ms=1000, prerequisites=checks)
    cursor, eligible = decide(cursor, event(2000), now_ms=2000, prerequisites=checks)
    _, repeat = decide(cursor, event(2000), now_ms=2001, prerequisites=checks)
    _, revised = decide(cursor, event(2000, revision=2), now_ms=2002, prerequisites=checks)
    assert seed.reason == 'seeded' and seed.status == 'HOLD'
    assert eligible.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert eligible.execution_authorized is False
    assert repeat.reason == 'already_observed'
    assert revised.reason == 'revised_observation_requires_new_event'


def test_shadow_cursor_cannot_consume_baseline():
    checks = (Prerequisite(name='data', status='PASS', evidence_sha256='d' * 64),)
    live, _ = decide(None, event(1000), now_ms=1000, prerequisites=checks)
    shadow, _ = decide(None, event(1000), now_ms=1000, prerequisites=checks)
    live, eligible = decide(live, event(2000), now_ms=2000, prerequisites=checks)
    shadow, _ = decide(shadow, event(2000), now_ms=2000, prerequisites=checks)
    assert eligible.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    _, again = decide(live, event(2000), now_ms=2001, prerequisites=checks)
    assert again.reason == 'already_observed'
    assert shadow.last_opportunity_id == live.last_opportunity_id


def test_expired_and_acquisition_pause():
    checks = (Prerequisite(name='data', status='PASS', evidence_sha256='d' * 64),)
    paused = Opportunity(scope=scope(acquisition_allowed=False), setup='b0_breakout', action='acquire',
                         primary=event(1000).primary, created_at_ms=1000, lifetime_ms=10_000)
    _, hold = decide(None, paused, now_ms=1000, prerequisites=checks)
    assert hold.reason == 'acquisition_forbidden'
    cursor, _ = decide(None, event(1000), now_ms=1000, prerequisites=checks)
    late = event(2000)
    _, expired = decide(cursor, late, now_ms=late.expires_at_ms, prerequisites=checks)
    assert expired.reason == 'opportunity_expired'
