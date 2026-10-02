"""The native runner must persist Meridian entry holds before another boot admits buys."""

import asyncio
from collections import deque
from contextlib import nullcontext
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor
from test.meridian_v3.test_unified_runtime import unified
from test.test_ok_rsi_restart_safety import recorder as _recorder_fixture
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.controllers.native_policy_identity import is_meridian_operator_policy, is_modular_policy
from hummingbot.strategy_v2.models.base import RunnableStatus
from scripts.v2_with_controllers import V2WithControllers, V2WithControllersConfig

recorder = _recorder_fixture
OWNER = "modular_spot_meridian_v3_unified_live"


def approved_test_risk_state():
    # Offline fixture only. A live owner's peaks and pause require native reconciliation.
    return {
        "version": 1,
        "controllers": [OWNER],
        "max_global_pnl": "0",
        "max_pnl_by_controller": {OWNER: "0"},
        "drawdown_exited_controllers": [],
        "stopped": False,
        "operator_paused": {OWNER: False},
    }


def test_meridian_operator_identity_is_exact_and_does_not_widen_modular_accounting():
    config = unified("live")
    assert is_meridian_operator_policy(config)
    assert not is_meridian_operator_policy(SimpleNamespace(controller_name="meridian_v3_unified"))
    assert not is_modular_policy(config)


class OperatorController:
    """Exact native config with only the public operator state shape; no legacy private bit."""

    def __init__(self):
        self.config = unified("live")
        self.paused = False
        self.status = RunnableStatus.RUNNING

    def get_operator_state(self):
        return {"entry_paused": self.paused}

    def set_operator_entry_pause(self, enabled, **_kwargs):
        self.paused = enabled

    def stop(self):
        self.status = RunnableStatus.TERMINATED


def runner(recorder):
    connector = OkxExchange(okx_api_key="", okx_secret_key="", okx_passphrase="", trading_pairs=["BTC-USDC"])

    def initialize(strategy):
        strategy.controllers = {OWNER: OperatorController()}

    with patch.object(MarketsRecorder, "get_instance", return_value=recorder), patch.object(
        StrategyV2Base, "initialize_controllers", initialize
    ), patch.object(StrategyV2Base, "initialize_candles"), patch(
        "hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources"
    ), patch(
        "scripts.v2_with_controllers.ExternalEventFactory.create_queue", return_value=deque()
    ):
        return V2WithControllers(
            {"okx": connector},
            V2WithControllersConfig(
                runtime_db_sync_enabled=False, runtime_status_snapshot_enabled=False, chart_snapshots_enabled=False
            ),
        )


async def close_runner(strategy):
    strategy.listen_to_executor_actions_task.cancel()
    await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


def command(action, command_id, requested_at, bot_name="candidate"):
    return (
        "rsibot.operator",
        SimpleNamespace(
            data={
                "action": action,
                "bot_name": bot_name,
                "controller_id": OWNER,
                "requested_at": requested_at,
                "command_id": command_id,
            }
        ),
    )


def dispatch(strategy, recorder, event):
    with patch.object(MarketsRecorder, "get_instance", return_value=recorder), patch(
        "scripts.v2_with_controllers.time.time", return_value=1000.5
    ), patch(
        "scripts.v2_with_controllers.HummingbotApplication.main_application",
        return_value=SimpleNamespace(instance_id="candidate"),
    ):
        strategy._operator_event_queue.append(event)
        strategy.process_operator_events()


@pytest.mark.asyncio
async def test_meridian_pause_and_resume_roundtrip_through_native_risk_and_command_receipts(recorder):
    assert not is_modular_policy(unified("live"))  # operational admission must not widen accounting identity
    recorder.store_strategy_risk_state(approved_test_risk_state())
    first = runner(recorder)
    try:
        assert first._risk_persistence_enabled
        assert recorder.load_strategy_risk_state()["operator_paused"] == {OWNER: False}
        first._operator_started_at = 999
        dispatch(first, recorder, command("bot_pause_entries", "pause-1", 1000.1))
        assert first.controllers[OWNER].paused
        assert recorder.load_strategy_risk_state()["operator_paused"] == {OWNER: True}
        assert recorder.has_operator_command(OWNER, "pause-1")
    finally:
        await close_runner(first)

    second = runner(recorder)
    try:
        assert second.controllers[OWNER].paused
        second._operator_started_at = 999
        dispatch(second, recorder, command("bot_resume_entries", "resume-1", 1000.2))
        assert not second.controllers[OWNER].paused
        assert recorder.load_strategy_risk_state()["operator_paused"] == {OWNER: False}
        assert recorder.has_operator_command(OWNER, "resume-1")
    finally:
        await close_runner(second)

    third = runner(recorder)
    try:
        assert not third.controllers[OWNER].paused
    finally:
        await close_runner(third)


@pytest.mark.asyncio
async def test_meridian_operator_rejects_wrong_bot_stale_and_duplicate_commands(recorder):
    recorder.store_strategy_risk_state(approved_test_risk_state())
    strategy = runner(recorder)
    try:
        strategy._operator_started_at = 999
        dispatch(strategy, recorder, command("bot_pause_entries", "wrong-bot", 1000.1, "other"))
        dispatch(strategy, recorder, command("bot_pause_entries", "stale", 998))
        assert not strategy.controllers[OWNER].paused
        assert recorder.load_strategy_risk_state()["operator_paused"] == {OWNER: False}
        dispatch(strategy, recorder, command("bot_pause_entries", "pause-once", 1000.2))
        assert strategy.controllers[OWNER].paused
        dispatch(strategy, recorder, command("bot_resume_entries", "pause-once", 1000.3))
        assert strategy.controllers[OWNER].paused
        assert not recorder.has_operator_command(OWNER, "wrong-bot")
        assert not recorder.has_operator_command(OWNER, "stale")
    finally:
        await close_runner(strategy)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["missing_empty", "missing_with_history", "malformed", "load_failed", "store_failed"]
)
async def test_meridian_missing_or_failed_durable_state_blocks_new_execution(recorder, failure):
    if failure == "missing_with_history":
        executor = make_executor()
        executor.config.controller_id = OWNER
        executor._status = RunnableStatus.TERMINATED
        recorder.store_or_update_executor(executor)
    elif failure == "malformed":
        state = approved_test_risk_state()
        state["operator_paused"] = {OWNER: "yes"}
        recorder.store_strategy_risk_state(state)
    elif failure in {"load_failed", "store_failed"}:
        recorder.store_strategy_risk_state(approved_test_risk_state())
    method = "load_strategy_risk_state" if failure == "load_failed" else "store_strategy_risk_state"
    context = (
        patch.object(recorder, method, side_effect=OSError("native DB unavailable"))
        if failure.endswith("failed")
        else nullcontext()
    )
    with context:
        strategy = runner(recorder)
    try:
        assert strategy._risk_persistence_enabled
        assert strategy._is_stop_triggered and strategy._risk_reconciliation_error
        assert not strategy.validate_create_action(SimpleNamespace(controller_id=OWNER))
        if failure.startswith("missing"):
            assert recorder.load_strategy_risk_state() is None
    finally:
        await close_runner(strategy)


@pytest.mark.asyncio
async def test_meridian_malformed_public_pause_or_failed_command_write_holds_execution(recorder):
    recorder.store_strategy_risk_state(approved_test_risk_state())
    strategy = runner(recorder)
    try:
        controller = strategy.controllers[OWNER]
        controller.get_operator_state = lambda: {"entry_paused": "yes"}
        with pytest.raises(ValueError, match="operator pause state unavailable"):
            strategy._persist_risk_state()
        controller.get_operator_state = lambda: {"entry_paused": controller.paused}
        strategy._operator_started_at = 999
        with patch.object(
            recorder, "store_strategy_risk_state", side_effect=OSError("native DB unavailable")
        ), pytest.raises(RuntimeError, match="Operator command persistence failed"):
            dispatch(strategy, recorder, command("bot_pause_entries", "pause-unwritten", 1000.1))
        assert strategy._is_stop_triggered and strategy._risk_reconciliation_error
        assert not strategy.validate_create_action(SimpleNamespace(controller_id=OWNER))
        assert not recorder.has_operator_command(OWNER, "pause-unwritten")
    finally:
        await close_runner(strategy)


@pytest.mark.asyncio
async def test_daily_entry_pause_reads_meridian_public_operator_state(recorder):
    recorder.store_strategy_risk_state(approved_test_risk_state())
    strategy = runner(recorder)
    try:
        controller = strategy.controllers[OWNER]
        strategy._daily_entry_risk = SimpleNamespace(paused=True)
        controller.paused = True
        with patch.object(controller, "set_operator_entry_pause") as pause:
            strategy._apply_daily_entry_pause()
            pause.assert_not_called()
            controller.paused = False
            strategy._apply_daily_entry_pause()
            pause.assert_called_once_with(True, reason="daily-loss-limit", scope="global")
    finally:
        await close_runner(strategy)


@pytest.mark.asyncio
async def test_meridian_buy_admission_reads_typed_operator_state_and_fails_closed(recorder):
    recorder.store_strategy_risk_state(approved_test_risk_state())
    strategy = runner(recorder)
    try:
        strategy._controller_reports_valid = True
        buy = SimpleNamespace(controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.BUY))
        sell = SimpleNamespace(controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.SELL))
        controller = strategy.controllers[OWNER]
        with patch.object(strategy, "check_manual_kill_switch"), patch.object(strategy, "control_max_drawdown"):
            controller.paused = True
            assert not strategy.validate_create_action(buy)
            assert strategy.validate_create_action(sell)
            controller.get_operator_state = lambda: {"entry_paused": "yes"}
            assert not strategy.validate_create_action(buy)
            assert strategy._is_stop_triggered
    finally:
        await close_runner(strategy)
