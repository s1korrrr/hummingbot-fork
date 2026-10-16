import pytest

from hummingbot.strategy_v2.components.decision_protocol import (
    DecisionScope,
    DecisionTrace,
    ObservationRef,
    Opportunity,
)
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.market_context.observation_entry import EntryObservation, EntryPolicy, evaluate_entry


def opportunity():
    h = "a" * 64
    return Opportunity(
        scope=DecisionScope(
            program_id="v2", owner_id="owner", connector="okx", trading_pair="BTC-USDC", recipe_sha256=h
        ),
        setup="dip",
        action="acquire",
        primary=ObservationRef(
            dataset_sha256=h, epoch="e1", revision=1, content_sha256=h, event_time_ms=100, available_time_ms=101
        ),
        created_at_ms=101,
        lifetime_ms=100,
    )


def baseline(opp):
    return DecisionTrace(
        opportunity_id=opp.opportunity_id,
        consumption_key=opp.consumption_key,
        decision_time_ms=110,
        status="ELIGIBLE_FOR_NATIVE_ADMISSION",
        reason="eligible",
        prerequisite_sha256=digest([]),
    )


def evidence(opp, **changes):
    fields = dict(
        opportunity_id=opp.opportunity_id,
        content_sha256="b" * 64,
        instrument_id="okx:spot:BTC-USDC",
        source_profile="okx_eea_public_v1",
        source_epoch="e1",
        available_at_ms=108,
        expires_at_ms=150,
        qualified=True,
        adverse_flow=True,
    )
    fields.update(changes)
    return EntryObservation(**fields)


def test_disabled_passive_consumer_preserves_native_trace_exactly():
    opp = opportunity()
    original = baseline(opp)
    result = evaluate_entry(
        None,
        opp,
        original,
        now_ms=110,
        policy=EntryPolicy(enabled=False),
        observation=None,
        native_recheck=lambda: (_ for _ in ()).throw(AssertionError()),
    )
    assert result.trace is original and result.state is None


def test_adverse_flow_holds_then_releases_once_after_native_recheck():
    opp = opportunity()
    checks = []
    policy = EntryPolicy(enabled=True, max_wait_ms=40)
    first = evaluate_entry(
        None,
        opp,
        baseline(opp),
        now_ms=110,
        policy=policy,
        observation=evidence(opp),
        native_recheck=lambda: checks.append(1) or True,
    )
    assert first.trace.status == "HOLD" and first.trace.reason == "adverse_flow_veto"
    assert not checks
    second = evaluate_entry(
        first.state,
        opp,
        baseline(opp),
        now_ms=120,
        policy=policy,
        observation=evidence(opp, adverse_flow=False),
        native_recheck=lambda: checks.append(1) or True,
    )
    assert second.trace.status == "ELIGIBLE_FOR_NATIVE_ADMISSION"
    assert second.state.phase == "RELEASED" and len(checks) == 1
    third = evaluate_entry(
        second.state,
        opp,
        baseline(opp),
        now_ms=121,
        policy=policy,
        observation=evidence(opp, adverse_flow=False),
        native_recheck=lambda: checks.append(1) or True,
    )
    assert third.trace.status == "HOLD" and third.trace.reason == "already_released"
    assert len(checks) == 1


def test_missing_observation_does_not_disarm_exit_or_baseline_hold():
    opp = opportunity()
    held = baseline(opp).model_copy(update={"status": "HOLD", "reason": "native_risk_hold"})
    result = evaluate_entry(
        None, opp, held, now_ms=110, policy=EntryPolicy(enabled=True), observation=None, native_recheck=lambda: True
    )
    assert result.trace is held
    exit_opp = opp.model_copy(update={"action": "reduce"})
    exit_trace = baseline(exit_opp)
    exit_result = evaluate_entry(
        None,
        exit_opp,
        exit_trace,
        now_ms=110,
        policy=EntryPolicy(enabled=True),
        observation=None,
        native_recheck=lambda: True,
    )
    assert exit_result.trace is exit_trace


def test_expiry_never_releases_pending_commitment_and_recheck_failure_holds():
    opp = opportunity()
    policy = EntryPolicy(enabled=True, max_wait_ms=20)
    first = evaluate_entry(
        None, opp, baseline(opp), now_ms=110, policy=policy, observation=evidence(opp), native_recheck=lambda: True
    )
    expired = evaluate_entry(
        first.state,
        opp,
        baseline(opp),
        now_ms=131,
        policy=policy,
        observation=evidence(opp, adverse_flow=False),
        native_recheck=lambda: True,
        pending_commitment=True,
    )
    assert expired.trace.reason == "pending_commitment"
    assert expired.trace.status == "HOLD"
    assert expired.state.phase == "EXPIRED"
    declined = evaluate_entry(
        None,
        opp,
        baseline(opp),
        now_ms=110,
        policy=policy,
        observation=evidence(opp, adverse_flow=False),
        native_recheck=lambda: False,
    )
    assert declined.trace.reason == "native_recheck_hold"


def test_wrong_snapshot_or_regressed_clock_rejects():
    opp = opportunity()
    policy = EntryPolicy(enabled=True)
    with pytest.raises(ValueError):
        evaluate_entry(
            None,
            opp,
            baseline(opp),
            now_ms=110,
            policy=policy,
            observation=evidence(opp, opportunity_id="f" * 64),
            native_recheck=lambda: True,
        )
    with pytest.raises(ValueError):
        evaluate_entry(
            None,
            opp,
            baseline(opp),
            now_ms=110,
            policy=policy,
            observation=evidence(opp, instrument_id="okx:spot:ETH-USDC"),
            native_recheck=lambda: True,
        )
    first = evaluate_entry(
        None, opp, baseline(opp), now_ms=110, policy=policy, observation=evidence(opp), native_recheck=lambda: True
    )
    with pytest.raises(ValueError):
        evaluate_entry(
            first.state,
            opp,
            baseline(opp),
            now_ms=109,
            policy=policy,
            observation=evidence(opp),
            native_recheck=lambda: True,
        )
