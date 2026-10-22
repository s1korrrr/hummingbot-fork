"""Legacy OK RSI operator commands use the native authenticated receipt path."""

import asyncio
from collections import deque
from test.test_ok_rsi_restart_safety import recorder as _recorder_fixture
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.controllers.native_policy_identity import (
    is_legacy_ok_rsi_operator_policy,
    is_modular_policy,
)
from scripts.v2_with_controllers import V2WithControllers, V2WithControllersConfig

recorder = _recorder_fixture
OWNER = "controller"


def _strategy(recorder):
    connector = OkxExchange(okx_api_key="", okx_secret_key="", okx_passphrase="", trading_pairs=["ETH-USDT"])

    def initialize(strategy):
        config = OKRSIControllerConfig(id=OWNER, connector_name="okx", trading_pair="ETH-USDT")
        strategy.controllers = {OWNER: OKRSIController(config, strategy.market_data_provider, strategy.actions_queue)}

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


async def _close(strategy):
    strategy.listen_to_executor_actions_task.cancel()
    await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


def _dispatch(strategy, recorder, action, command_id, requested_at, bot_name="candidate"):
    payload = {
        "action": action,
        "controller_id": OWNER,
        "bot_name": bot_name,
        "requested_at": requested_at,
        "command_id": command_id,
    }
    with patch.object(MarketsRecorder, "get_instance", return_value=recorder), patch(
        "scripts.v2_with_controllers.time.time", return_value=1000.5
    ), patch(
        "scripts.v2_with_controllers.HummingbotApplication.main_application",
        return_value=SimpleNamespace(instance_id="candidate"),
    ):
        strategy._operator_event_queue.append(("rsibot.operator", SimpleNamespace(data=payload)))
        strategy.process_operator_events()


def test_legacy_operator_identity_requires_exact_config_type():
    config = OKRSIControllerConfig(id=OWNER, connector_name="okx", trading_pair="ETH-USDT")
    assert is_legacy_ok_rsi_operator_policy(config)
    assert V2WithControllers._durable_operator_policy(config)
    assert not is_modular_policy(config)
    assert not is_legacy_ok_rsi_operator_policy(SimpleNamespace(controller_name="ok_rsi"))
    assert not V2WithControllers._durable_operator_policy(SimpleNamespace(controller_name="ok_rsi"))


@pytest.mark.asyncio
async def test_legacy_pause_resume_have_atomic_native_receipts_and_survive_restart(recorder):
    first = _strategy(recorder)
    try:
        first._operator_started_at = 999
        _dispatch(first, recorder, "bot_pause_entries", "pause-1", 1000.1)
        assert first.controllers[OWNER]._operator_entry_paused
        assert recorder.load_strategy_risk_state()["operator_paused"] == {OWNER: True}
        assert recorder.has_operator_command(OWNER, "pause-1")
    finally:
        await _close(first)

    second = _strategy(recorder)
    try:
        assert second.controllers[OWNER]._operator_entry_paused
        second._operator_started_at = 999
        _dispatch(second, recorder, "bot_resume_entries", "resume-1", 1000.2)
        assert not second.controllers[OWNER]._operator_entry_paused
        assert recorder.load_strategy_risk_state()["operator_paused"] == {OWNER: False}
        assert recorder.has_operator_command(OWNER, "resume-1")
    finally:
        await _close(second)

    third = _strategy(recorder)
    try:
        assert not third.controllers[OWNER]._operator_entry_paused
    finally:
        await _close(third)


@pytest.mark.asyncio
async def test_legacy_refuses_unauthenticated_wrong_bot_stale_and_replayed_commands(recorder):
    strategy = _strategy(recorder)
    try:
        strategy._operator_started_at = 999
        for command_id, requested_at, bot_name in (
            ("missing-bot", 1000.1, None),
            ("wrong-bot", 1000.1, "other"),
            ("stale", 998, "candidate"),
        ):
            _dispatch(strategy, recorder, "bot_pause_entries", command_id, requested_at, bot_name)
            assert not strategy.controllers[OWNER]._operator_entry_paused
            assert not recorder.has_operator_command(OWNER, command_id)
        _dispatch(strategy, recorder, "bot_pause_entries", "pause-once", 1000.2)
        assert strategy.controllers[OWNER]._operator_entry_paused
        _dispatch(strategy, recorder, "bot_resume_entries", "pause-once", 1000.3)
        _dispatch(strategy, recorder, "bot_resume_entries", "older", 1000.1)
        assert strategy.controllers[OWNER]._operator_entry_paused
        assert not recorder.has_operator_command(OWNER, "older")
    finally:
        await _close(strategy)


@pytest.mark.asyncio
async def test_legacy_command_write_failure_latches_entry_block_without_receipt(recorder):
    strategy = _strategy(recorder)
    try:
        strategy._operator_started_at = 999
        with patch.object(recorder, "store_strategy_risk_state", side_effect=OSError("native DB unavailable")):
            with pytest.raises(RuntimeError, match="Operator command persistence failed"):
                _dispatch(strategy, recorder, "bot_pause_entries", "unwritten", 1000.1)
        assert strategy._is_stop_triggered and strategy._risk_reconciliation_error
        assert not recorder.has_operator_command(OWNER, "unwritten")
        assert not strategy.validate_create_action(SimpleNamespace(controller_id=OWNER))
    finally:
        await _close(strategy)


@pytest.mark.asyncio
async def test_queued_legacy_buy_rechecks_fresh_pause_before_creation_but_sell_can_pass(recorder):
    strategy = _strategy(recorder)
    try:
        strategy._operator_started_at = 999
        strategy._controller_reports_valid = True
        buy = SimpleNamespace(controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.BUY))
        sell = SimpleNamespace(controller_id=OWNER, executor_config=SimpleNamespace(side=TradeType.SELL))
        payload = {
            "action": "bot_pause_entries",
            "controller_id": OWNER,
            "bot_name": "candidate",
            "requested_at": 1000.1,
            "command_id": "queued-pause",
        }
        strategy._operator_event_queue.append(("rsibot.operator", SimpleNamespace(data=payload)))
        with patch.object(MarketsRecorder, "get_instance", return_value=recorder), patch(
            "scripts.v2_with_controllers.time.time", return_value=1000.5
        ), patch(
            "scripts.v2_with_controllers.HummingbotApplication.main_application",
            return_value=SimpleNamespace(instance_id="candidate"),
        ), patch.object(
            strategy, "check_manual_kill_switch"
        ), patch.object(
            strategy, "control_max_drawdown"
        ):
            assert not strategy.validate_create_action(buy)
            assert strategy.controllers[OWNER]._operator_entry_paused
            assert recorder.has_operator_command(OWNER, "queued-pause")
            assert strategy.validate_create_action(sell)
            assert not strategy._is_stop_triggered
    finally:
        await _close(strategy)
