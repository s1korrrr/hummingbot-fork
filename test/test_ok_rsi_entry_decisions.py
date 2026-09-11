"""Durable entry decisions and queue replay; no exchange or market I/O."""
import json
from types import SimpleNamespace
from unittest.mock import patch

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
    assert not EntryDecisionGate(store).observe(observation(180, True, 1))
    assert gate.dispatch("executor-1", "fingerprint-1", 180)
    assert not gate.dispatch("executor-1", "fingerprint-1", 180)


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
    restarted.observe(observation(240, False))
    assert restarted.observe(observation(300, True, 1))


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
    with patch.object(store, "load", return_value=None), patch.object(store, "save", side_effect=OSError("disk")):
        with pytest.raises(OSError):
            EntryDecisionGate(store).observe(observation(60, False))


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
    controller.processed_data = observation(60, False)
    controller._last_evaluated_candle_timestamp = 60
    controller.market_data_provider = SimpleNamespace(time=lambda: 600)
    return controller


def config_stub(executor_id="first"):
    from hummingbot.core.data_type.common import TradeType
    return SimpleNamespace(id=executor_id, side=TradeType.BUY,
                           model_dump=lambda **kwargs: {"id": executor_id, "amount": "1"})


def test_controller_consumes_before_queue_and_original_passes_existing_validation(store):
    from controllers.directional_trading.hl_rsi import HLRSIController
    controller = controller_stub(store)
    action = SimpleNamespace(executor_config=config_stub())
    with patch.object(HLRSIController, "create_actions_proposal", return_value=[action]):
        assert controller.create_actions_proposal() == []
        controller.processed_data = observation(120, True, 1)
        controller._last_evaluated_candle_timestamp = 120
        assert controller.create_actions_proposal() == [action]
        assert store.load()["authorization"]["executor_id"] == "first"
        assert controller.create_actions_proposal() == []
    with patch.object(HLRSIController, "validate_executor_config", return_value=True) as existing:
        assert controller.validate_executor_config(action.executor_config)
        assert not controller.validate_executor_config(action.executor_config)
        assert existing.call_count == 2


def test_controller_failure_blocks_buy_but_does_not_swallow_existing_gate_rejection(store):
    from controllers.directional_trading.hl_rsi import HLRSIController
    controller = controller_stub(store)
    with patch.object(store, "load", side_effect=OSError("disk unavailable")):
        assert controller.create_actions_proposal() == []
        assert "disk unavailable" in controller._entry_decision_error
    with patch.object(HLRSIController, "validate_executor_config", return_value=False):
        assert not controller.validate_executor_config(config_stub())


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


def test_real_controller_constructor_sizing_queue_validation_and_restart(store):
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
    restarted = OKRSIController(config, provider, asyncio.Queue(), entry_decision_store=store)
    restarted._status = RunnableStatus.RUNNING
    restarted.processed_data = observation(540, True, 1)
    restarted._last_evaluated_candle_timestamp = 540
    assert restarted.create_actions_proposal() == []
    assert not restarted.validate_executor_config(executor)
    controller.config.manual_kill_switch = True
    assert not controller.validate_executor_config(executor)
