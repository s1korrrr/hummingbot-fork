"""Durable entry decisions and queue replay; no exchange or market I/O."""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from controllers.directional_trading.ok_rsi_core.entry_decisions import EntryDecisionGate, MetadataEntryDecisionStore
from hummingbot.model.metadata import Metadata


def observation(candle, candidate, signal=0, **extra):
    return {"candle_timestamp": candle, "signal": signal,
            "data_health": {"ready": True},
            "signal_state": {"raw_buy_candidate": candidate, "condition_ok": True}, **extra}


@pytest.fixture
def store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'decisions.sqlite'}")
    Metadata.__table__.create(engine)
    manager = SimpleNamespace(get_new_session=sessionmaker(bind=engine))
    return MetadataEntryDecisionStore(manager, "test-script", "ok-rsi-btc", "okx", "BTC-USDC", "1m")


def test_requires_clear_then_new_qualified_candle_and_persists_before_dispatch(store):
    gate = EntryDecisionGate(store)
    assert not gate.observe(observation(60, True, 1))
    assert not gate.observe(observation(120, False))
    assert gate.observe(observation(180, True, 1))
    assert gate.reserve("executor-1", "fingerprint-1", 180)
    assert store.load()["consumed_candle"] == 0
    assert not EntryDecisionGate(store).observe(observation(180, True, 1))
    assert gate.dispatch("executor-1", "fingerprint-1", 180)
    assert not gate.dispatch("executor-1", "fingerprint-1", 180)
    assert gate.commit("executor-1", "fingerprint-1", 180)
    assert store.load()["consumed_candle"] == 180
    assert not EntryDecisionGate(store).observe(observation(180, True, 1))


def test_second_cleared_buy_can_dispatch_and_commit_after_first_buy(store):
    rows = set()
    gate = EntryDecisionGate(store, executor_exists=lambda executor_id: executor_id in rows)
    assert not gate.observe(observation(60, False))
    assert gate.observe(observation(120, True, 1))
    assert gate.reserve("first", "first-fingerprint", 120)
    assert gate.dispatch("first", "first-fingerprint", 120)
    rows.add("first")
    assert gate.commit("first", "first-fingerprint", 120)

    assert not gate.observe(observation(180, False))
    assert gate.observe(observation(240, True, 1))
    assert gate.reserve("second", "second-fingerprint", 240)
    assert gate.dispatch("second", "second-fingerprint", 240)
    rows.add("second")
    assert gate.commit("second", "second-fingerprint", 240)
    assert not EntryDecisionGate(store, executor_exists=lambda executor_id: executor_id in rows).observe(
        observation(240, True, 1)
    )


def test_new_candle_cannot_replace_dispatched_entry_awaiting_commit(store):
    gate = EntryDecisionGate(store)
    gate.observe(observation(60, False))
    assert gate.observe(observation(120, True, 1))
    assert gate.reserve("pending", "fp", 120)
    assert gate.dispatch("pending", "fp", 120)
    assert not gate.observe(observation(180, False))
    assert not gate.observe(observation(240, True, 1))
    assert gate.commit("pending", "fp", 120)
    assert not EntryDecisionGate(store).observe(observation(120, True, 1))


def test_float_timestamp_at_dispatch_commits_canonical_integer_candle(store):
    gate = EntryDecisionGate(store)
    gate.observe(observation(60, False))
    assert gate.observe(observation(120, True, 1))
    assert gate.reserve("entry", "fp", 120)
    assert gate.dispatch("entry", "fp", 120.0)
    assert gate.commit_dispatched("entry", "fp")
    assert type(store.load()["consumed_candle"]) is int
    assert store.load()["consumed_candle"] == 120
    assert not gate.observe(observation(120, True, 1))


def test_pulse_zero_invalid_data_and_persistent_candidate_do_not_rearm(store):
    gate = EntryDecisionGate(store)
    gate.observe(observation(60, False))
    gate.observe(observation(120, True, 1))
    gate.reserve("first", "same", 120)
    gate.dispatch("first", "same", 120)
    assert not gate.observe(observation(120, True, signal_suppression_reason="same_candle"))
    assert not gate.observe(observation(180, False, data_health={"ready": False}))
    assert not gate.observe(observation(180, True, 1))
    assert not gate.observe(observation(240, False, signal_suppression_reason="non_advancing_candle"))
    assert not gate.observe(observation(300, True, 1))
    # A dispatched attempt must receive an explicit failed-persistence verdict.
    assert gate.release()
    gate.observe(observation(360, False))
    assert gate.observe(observation(420, True, 1))
    assert gate.reserve("second", "next", 420)


def test_restart_cannot_dispatch_unsubmitted_pre_restart_authorization(store):
    gate = EntryDecisionGate(store)
    gate.observe(observation(60, False))
    gate.observe(observation(120, True, 1))
    assert gate.reserve("first", "same", 120)
    restarted = EntryDecisionGate(store)
    assert not restarted.dispatch("first", "same", 120)
    assert not restarted.observe(observation(180, True, 1))
    assert not restarted.observe(observation(240, False))
    reconciled = EntryDecisionGate(store, executor_exists=lambda _: False)
    assert not reconciled.observe(observation(240, False))
    assert reconciled.observe(observation(300, True, 1))


def test_reserve_then_missing_executor_releases_candle_for_later_dispatch(store):
    rows = set()
    gate = EntryDecisionGate(store, executor_exists=lambda executor_id: executor_id in rows)
    gate.observe(observation(60, False))
    assert gate.observe(observation(120, True, 1))
    assert gate.reserve("ghost", "fp", 120)
    assert gate.dispatch("ghost", "fp", 120)
    assert store.load()["consumed_candle"] == 0
    later = EntryDecisionGate(store, executor_exists=lambda executor_id: executor_id in rows)
    assert later.observe(observation(120, True, 1))
    assert store.load()["authorization"] is None
    assert later.reserve("retry", "fp2", 120)
    assert later.dispatch("retry", "fp2", 120)
    rows.add("retry")
    assert later.commit("retry", "fp2", 120)
    assert store.load()["consumed_candle"] == 120
    assert not EntryDecisionGate(store, executor_exists=lambda executor_id: executor_id in rows).observe(
        observation(120, True, 1)
    )


def test_durable_executor_keeps_candle_consumed(store):
    rows = {"live"}
    gate = EntryDecisionGate(store, executor_exists=lambda executor_id: executor_id in rows)
    gate.observe(observation(60, False))
    assert gate.observe(observation(120, True, 1))
    assert gate.reserve("live", "fp", 120)
    assert gate.dispatch("live", "fp", 120)
    assert gate.commit("live", "fp", 120)
    restarted = EntryDecisionGate(store, executor_exists=lambda executor_id: executor_id in rows)
    assert not restarted.observe(observation(120, True, 1))
    assert not restarted.release()
    assert store.load()["consumed_candle"] == 120


def test_queued_identity_payload_candle_and_once_only_are_checked(store):
    gate = EntryDecisionGate(store)
    gate.observe(observation(60, False))
    gate.observe(observation(120, True, 1))
    gate.reserve("first", "same", 120)
    assert not gate.dispatch("other", "same", 120)
    assert not gate.dispatch("first", "mutated", 120)
    assert not gate.dispatch("first", "same", 180)
    assert gate.dispatch("first", "same", 120)


def test_store_compare_and_swap_rejects_stale_writer(store):
    gate = EntryDecisionGate(store)
    gate.observe(observation(60, False))
    prior = store.load()
    newer = dict(prior, candle=120)
    store.save(prior, newer)
    with pytest.raises(RuntimeError, match="changed"):
        store.save(prior, dict(prior, candle=180))


def test_invalid_persisted_state_and_write_failure_raise(store):
    with store.manager.get_new_session() as session:
        session.add(Metadata(key=store.key, value=json.dumps({"version": 99})))
        session.commit()
    with pytest.raises(ValueError):
        EntryDecisionGate(store).observe(observation(60, False))
    gate = EntryDecisionGate(store)
    with patch.object(store, "load", return_value=None), patch.object(store, "save", side_effect=OSError("disk")):
        with pytest.raises(OSError):
            gate.observe(observation(60, False))
    assert gate.last_observation_storage_ok is False


def test_no_clear_on_missing_candidate_bad_timestamp_or_unhealthy_condition(store):
    gate = EntryDecisionGate(store)
    gate.observe(observation(60, True, 1))
    for data in (observation(120, None), observation(float("nan"), False),
                 observation(120, False, signal_state={"raw_buy_candidate": False, "condition_ok": False})):
        assert not gate.observe(data)
    assert not gate.observe(observation(180, True, 1))


def controller_stub(store):
    from controllers.directional_trading.ok_rsi import OKRSIController
    controller = OKRSIController.__new__(OKRSIController)
    controller.config = SimpleNamespace(durable_candle_watermark_enabled=False, id="test", connector_name="okx",
                                        trading_pair="BTC-USDC", interval="1m")
    controller._episode_runtime = None
    controller._entry_decision_store = store
    controller._entry_decision_gate = None
    controller._entry_decision_error = None
    controller._entry_decision_error_stage = None
    controller.processed_data = observation(60, False)
    controller._last_evaluated_candle_timestamp = 60
    controller.market_data_provider = SimpleNamespace(time=lambda: 600)
    return controller


def config_stub(executor_id="first"):
    from hummingbot.core.data_type.common import TradeType
    return SimpleNamespace(id=executor_id, side=TradeType.BUY,
                           model_dump=lambda **kwargs: {"id": executor_id, "amount": "1"})


def test_controller_reserves_without_consuming_until_commit(store):
    from controllers.directional_trading.hl_rsi import HLRSIController
    controller = controller_stub(store)
    action = SimpleNamespace(executor_config=config_stub())
    with patch.object(HLRSIController, "create_actions_proposal", return_value=[action]):
        assert controller.create_actions_proposal() == []
        controller.processed_data = observation(120, True, 1)
        controller._last_evaluated_candle_timestamp = 120
        assert controller.create_actions_proposal() == [action]
        assert store.load()["authorization"]["executor_id"] == "first"
        assert store.load()["consumed_candle"] == 0
        assert controller.create_actions_proposal() == []
    with patch.object(HLRSIController, "validate_executor_config", return_value=True) as existing:
        assert controller.validate_executor_config(action.executor_config)
        assert not controller.validate_executor_config(action.executor_config)
        assert existing.call_count == 2
        assert store.load()["consumed_candle"] == 0
        controller.processed_data = observation(180, False)
        controller.commit_entry_executor(action.executor_config)
        assert store.load()["consumed_candle"] == 120
        assert controller.create_actions_proposal() == []


def test_controller_validate_failure_releases_candle(store):
    from controllers.directional_trading.hl_rsi import HLRSIController
    controller = controller_stub(store)
    action = SimpleNamespace(executor_config=config_stub())
    with patch.object(HLRSIController, "create_actions_proposal", return_value=[action]):
        controller.processed_data = observation(60, False)
        controller.create_actions_proposal()
        controller.processed_data = observation(120, True, 1)
        controller._last_evaluated_candle_timestamp = 120
        assert controller.create_actions_proposal() == [action]
    with patch.object(HLRSIController, "validate_executor_config", return_value=False):
        assert not controller.validate_executor_config(action.executor_config)
    assert store.load()["consumed_candle"] == 0
    assert store.load()["authorization"] is None
    with patch.object(HLRSIController, "create_actions_proposal", return_value=[action]):
        later = controller.create_actions_proposal()
        assert later == [action]
        assert store.load()["authorization"]["executor_id"] == "first"


@pytest.mark.parametrize("failing_operation", ["load", "save"])
def test_controller_failure_blocks_buy_but_does_not_swallow_existing_gate_rejection(store, failing_operation):
    from controllers.directional_trading.hl_rsi import HLRSIController
    controller = controller_stub(store)
    with patch.object(store, failing_operation, side_effect=OSError("disk unavailable")):
        assert controller.create_actions_proposal() == []
        assert "disk unavailable" in controller._entry_decision_error
        assert controller._entry_decision_error_stage == "observation"
    with patch.object(HLRSIController, "validate_executor_config", return_value=False):
        assert not controller.validate_executor_config(config_stub())


@pytest.mark.parametrize("signal", [0, -1], ids=["neutral", "sell"])
def test_successful_ineligible_observation_clears_previous_admission_error(store, signal):
    from controllers.directional_trading.hl_rsi import HLRSIController

    controller = controller_stub(store)
    controller._entry_decision_error = "OperationalError: database is locked"
    controller._entry_decision_error_stage = "observation"
    controller.processed_data = observation(120, False, signal)
    controller._last_evaluated_candle_timestamp = 120
    if signal < 0:
        with patch.object(HLRSIController, "create_actions_proposal", return_value=["sell"]):
            assert controller.create_actions_proposal() == ["sell"]
    else:
        assert controller.create_actions_proposal() == []

    assert controller._entry_decision_error is None
    assert store.load()["candle"] == 120


def test_successful_neutral_observation_preserves_later_proposal_error_until_eligible_proposal(store):
    from controllers.directional_trading.hl_rsi import HLRSIController

    controller = controller_stub(store)
    controller._entry_decision_error = "ValueError: executor fingerprint failed"
    controller._entry_decision_error_stage = "proposal"
    controller.processed_data = observation(60, False)
    controller._last_evaluated_candle_timestamp = 60
    assert controller.create_actions_proposal() == []
    assert controller._entry_decision_error == "ValueError: executor fingerprint failed"

    action = SimpleNamespace(executor_config=config_stub())
    controller.processed_data = observation(120, True, 1)
    controller._last_evaluated_candle_timestamp = 120
    with patch.object(HLRSIController, "create_actions_proposal", return_value=[action]):
        assert controller.create_actions_proposal() == [action]
    assert controller._entry_decision_error is None
    assert controller._entry_decision_error_stage is None


@pytest.mark.parametrize(
    "observation_overrides",
    [
        {"data_health": {"ready": False}},
        {"signal_suppression_reason": "stale_candles"},
    ],
    ids=["unhealthy", "suppressed"],
)
def test_unstored_observation_does_not_clear_previous_admission_error(store, observation_overrides):
    controller = controller_stub(store)
    controller._entry_decision_error = "OperationalError: database is locked"
    controller._entry_decision_error_stage = "observation"
    controller.processed_data = observation(120, False, **observation_overrides)
    controller._last_evaluated_candle_timestamp = 120

    assert controller.create_actions_proposal() == []
    assert controller._entry_decision_error == "OperationalError: database is locked"
    assert controller._entry_decision_error_stage == "observation"
    assert store.load() is None


def test_episode_reconciliation_stops_and_sell_actions_continue_but_buy_is_blocked(store):
    from decimal import Decimal
    from unittest.mock import Mock

    from hummingbot.core.data_type.common import TradeType
    from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
    from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
    from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction

    controller = controller_stub(store)
    controller.config.manual_kill_switch = False
    buy = CreateExecutorAction(executor_config=OrderExecutorConfig(
        timestamp=600, connector_name="okx", trading_pair="BTC-USDC", side=TradeType.BUY, amount=Decimal("1"),
        execution_strategy=ExecutionStrategy.LIMIT_MAKER, price=Decimal("10")))
    sell = CreateExecutorAction(executor_config=OrderExecutorConfig(
        timestamp=600, connector_name="okx", trading_pair="BTC-USDC", side=TradeType.SELL, amount=Decimal("1"),
        execution_strategy=ExecutionStrategy.LIMIT_MAKER, price=Decimal("10")))
    stop = StopExecutorAction(executor_id="existing", keep_position=True)
    runtime = SimpleNamespace(determine_actions=Mock(return_value=[stop, buy, sell]),
                              hold_reason=None, state=SimpleNamespace(fault=None))
    controller._episode_runtime = runtime
    assert controller.determine_executor_actions() == [stop, sell]
    runtime.determine_actions.assert_called_once()
    assert "episode" in controller._entry_decision_error.lower()
    with patch.object(ControllerBase, "validate_executor_config", return_value=True):
        assert not controller.validate_executor_config(buy.executor_config)
        assert controller.validate_executor_config(sell.executor_config)
        runtime.hold_reason = "reconciliation"
        assert not controller.validate_executor_config(sell.executor_config)
        runtime.hold_reason = None
        runtime.state.fault = "fault"
        assert not controller.validate_executor_config(sell.executor_config)
    with patch.object(ControllerBase, "validate_executor_config", return_value=False):
        assert not controller.validate_executor_config(sell.executor_config)


def test_missing_production_recorder_does_not_enable_fallback(store):
    from hummingbot.connector.markets_recorder import MarketsRecorder
    controller = controller_stub(None)
    with patch.object(MarketsRecorder, "get_existing_instance", return_value=None):
        assert controller.create_actions_proposal() == []
        assert "recorder" in controller._entry_decision_error.lower()


def test_confirmation_signal_is_a_decision_but_does_not_itself_clear_setup(store):
    gate = EntryDecisionGate(store)
    assert not gate.observe(observation(60, False, 1))
    gate.observe(observation(120, False))
    assert gate.observe(observation(180, False, 1))
    gate.reserve("confirmed", "one", 180)
    assert not gate.observe(observation(240, False, 1))


def test_controller_rejects_uncompleted_or_not_owner_evaluated_candle(store):
    controller = controller_stub(store)
    controller._last_evaluated_candle_timestamp = None
    assert controller.create_actions_proposal() == []
    assert store.load() is None
    controller._last_evaluated_candle_timestamp = 60
    controller.market_data_provider = SimpleNamespace(time=lambda: 90)
    assert controller.create_actions_proposal() == []
    assert store.load() is None


def test_sell_candle_can_clear_buy_setup_without_blocking_sell_on_storage_failure(store):
    from controllers.directional_trading.hl_rsi import HLRSIController
    controller = controller_stub(store)
    controller.processed_data = observation(60, False, -1)
    with patch.object(HLRSIController, "create_actions_proposal", return_value=["sell"]):
        assert controller.create_actions_proposal() == ["sell"]
        assert store.load()["armed"] is True
        with patch.object(store, "load", side_effect=OSError("disk")):
            controller.processed_data = observation(120, False, -1)
            controller._last_evaluated_candle_timestamp = 120
            assert controller.create_actions_proposal() == ["sell"]


@pytest.fixture
def complete_empty_native_history():
    """Exposure fails closed without native history; this test covers sizing and queueing."""
    from hummingbot.connector.markets_recorder import MarketsRecorder
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = []
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        yield recorder


def test_real_controller_constructor_sizing_queue_validation_and_restart(store, complete_empty_native_history):
    import asyncio
    from decimal import Decimal
    from test.test_ok_rsi_controller import OKRSIControllerTest

    from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
    from hummingbot.strategy_v2.models.base import RunnableStatus

    provider = OKRSIControllerTest._provider(base_balance=Decimal("0"), price=Decimal("50000"))
    provider.time.return_value = 600.0
    config = OKRSIControllerConfig(id="ok-rsi-btc", connector_name="okx", trading_pair="BTC-USDC",
                                   trade_mode="buy_sell", strict_hmm_mode=False, use_adaptive_trailing=False,
                                   use_adaptive_filters=False, dynamic_position_sizing=False)
    controller = OKRSIController(config, provider, asyncio.Queue(), entry_decision_store=store)
    controller._status = RunnableStatus.RUNNING
    controller.processed_data = observation(480, False)
    controller._last_evaluated_candle_timestamp = 480
    assert controller.create_actions_proposal() == []
    controller.processed_data = observation(540, True, 1)
    controller._last_evaluated_candle_timestamp = 540
    actions = controller.create_actions_proposal()
    assert len(actions) == 1
    executor = actions[0].executor_config
    assert executor.entry_order_policy == "single_attempt"
    assert executor.amount > 0
    assert store.load()["authorization"]["executor_id"] == executor.id
    asyncio.run(controller.send_actions(actions))
    queued = controller.actions_queue.get_nowait()
    assert queued[0] is actions[0]
    assert controller.validate_executor_config(executor)
    assert not controller.validate_executor_config(executor)
    assert store.load()["consumed_candle"] == 0
    controller.commit_entry_executor(executor)
    assert store.load()["consumed_candle"] == 540
    restarted = OKRSIController(config, provider, asyncio.Queue(), entry_decision_store=store)
    restarted._status = RunnableStatus.RUNNING
    restarted.processed_data = observation(540, True, 1)
    restarted._last_evaluated_candle_timestamp = 540
    assert restarted.create_actions_proposal() == []
    assert not restarted.validate_executor_config(executor)
    controller.config.manual_kill_switch = True
    assert not controller.validate_executor_config(executor)
