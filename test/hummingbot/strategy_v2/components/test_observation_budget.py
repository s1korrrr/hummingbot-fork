from types import SimpleNamespace as NS

from hummingbot.strategy_v2.market_context.observation_budget import NativeEntryGate, ObservationBudgetProxy
from hummingbot.strategy_v2.market_context.observation_entry import EntryObservation


def claim():
    return NS(executor_id="a" * 64, trading_pair="BTC-USDC", expires_at=1)


def evidence(**changes):
    fields = dict(
        opportunity_id="a" * 64,
        content_sha256="b" * 64,
        instrument_id="okx:spot:BTC-USDC",
        source_profile="okx_eea_public_v1",
        source_epoch="e1",
        available_at_ms=100,
        expires_at_ms=500,
        qualified=True,
        adverse_flow=True,
    )
    fields.update(changes)
    return EntryObservation(**fields)


def test_proxy_hold_skips_native_reserve_but_delegates_other_budget_methods():
    called = []
    native = NS(
        reserve=lambda row, *, native_executors: called.append(row) or True,
        contains=lambda *args, **kwargs: "native-contains",
    )
    gate = NativeEntryGate(max_wait_ms=200)
    proxy = ObservationBudgetProxy(native, lambda row: gate.evaluate(row, evidence(), now_ms=110))
    assert not proxy.reserve(claim(), native_executors=[])
    assert called == []
    assert proxy.contains(None) == "native-contains"
    assert gate.state[claim().executor_id].phase == "WAIT"


def test_release_needs_actual_native_create_action_to_consume():
    called = []
    native = NS(reserve=lambda row, *, native_executors: called.append(row) or True)
    gate = NativeEntryGate(max_wait_ms=200)
    proxy = ObservationBudgetProxy(native, lambda row: gate.evaluate(row, evidence(adverse_flow=False), now_ms=120))
    assert proxy.reserve(claim(), native_executors=[])
    assert len(called) == 1
    assert gate.state[claim().executor_id].phase == "WAIT"
    assert gate.mark_queued(claim().executor_id, now_ms=121)
    assert gate.state[claim().executor_id].phase == "QUEUED"
    assert gate.dispatch_permitted(claim().executor_id, evidence(adverse_flow=False), pair="BTC-USDC", now_ms=122)
    assert gate.mark_dispatched(claim().executor_id, now_ms=123)
    assert gate.state[claim().executor_id].phase == "DISPATCHED"
    assert not proxy.reserve(claim(), native_executors=[])
    assert len(called) == 1


def test_expired_native_proposal_and_wrong_symbol_do_not_delegate():
    native = NS(reserve=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))
    gate = NativeEntryGate(max_wait_ms=20)
    proxy = ObservationBudgetProxy(native, lambda row: gate.evaluate(row, evidence(), now_ms=110))
    assert not proxy.reserve(claim(), native_executors=[])
    later = ObservationBudgetProxy(native, lambda row: gate.evaluate(row, evidence(adverse_flow=False), now_ms=131))
    assert not later.reserve(claim(), native_executors=[])
    assert gate.state[claim().executor_id].phase == "EXPIRED"
    fresh = NativeEntryGate(max_wait_ms=200)
    wrong = ObservationBudgetProxy(
        native,
        lambda row: fresh.evaluate(row, evidence(instrument_id="okx:spot:ETH-USDC", adverse_flow=False), now_ms=110),
    )
    assert not wrong.reserve(claim(), native_executors=[])
    assert fresh.reason == "observation_scope_mismatch"
