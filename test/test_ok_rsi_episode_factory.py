from decimal import Decimal
from test.test_ok_rsi_episode_runtime import actions, setup_controller
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.core.data_type.common import PositionAction, TradeType
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, LimitChaserConfig


def pending_request():
    controller, _ = setup_controller()
    config = actions(controller)[0].executor_config
    config.controller_id = controller.config.id  # Native orchestrator does this before resolution.
    return controller, config


@pytest.mark.parametrize("field,value", [
    ("execution_strategy", ExecutionStrategy.MARKET),
    ("execution_strategy", ExecutionStrategy.LIMIT),
    ("chaser_config", LimitChaserConfig(distance=Decimal(".01"), refresh_threshold=Decimal(".01"))),
    ("type", "position_executor"), ("id", "unknown"), ("side", TradeType.SELL),
    ("position_action", PositionAction.CLOSE), ("leverage", 3),
    ("amount", Decimal("100")), ("price", Decimal("1000")),
    ("timestamp", 10021), ("controller_id", "different-controller"),
    ("connector_name", "binance"), ("trading_pair", "BTC-USDT"),
    ("episode_id", "different-episode"), ("episode_level", 10),
    ("episode_level", False), ("episode_revision", True),
    ("episode_revision", 1000), ("episode_expires_at", 20000), ("level_id", "legacy"),
])
def test_factory_rejects_every_changed_authorization_field(field, value):
    controller, config = pending_request()
    with pytest.raises(ValueError, match="episode"):
        controller.get_executor_class_for_config(config.model_copy(update={field: value}))


@pytest.mark.parametrize("progress_field", ["done", "seen"])
def test_factory_rejects_terminal_or_already_observed_requests(progress_field):
    controller, config = pending_request()
    setattr(controller._episode_runtime.state.orders[config.id], progress_field, True)
    with pytest.raises(ValueError, match="episode"):
        controller.get_executor_class_for_config(config)


def test_factory_rejects_unknown_sell_only_buy_without_native_fallback():
    _, buy = pending_request()
    controller, _ = setup_controller(mode="sell_only", episode_assignment_id="assignment",
                                     episode_initial_base=Decimal("2"), sell_only_if_profitable=False)
    with pytest.raises(ValueError, match="episode"):
        controller.get_executor_class_for_config(buy)


def test_factory_claims_authorized_request_only_once():
    from controllers.directional_trading.ok_rsi_core.execution import EpisodeOrderExecutor
    controller, config = pending_request()
    assert controller.get_executor_class_for_config(config) is EpisodeOrderExecutor
    with pytest.raises(ValueError, match="episode"):
        controller.get_executor_class_for_config(config)


def test_disabled_controller_retains_default_executor_mapping():
    controller, config = pending_request()
    controller._episode_runtime = None
    assert controller.get_executor_class_for_config(config) is None


def test_native_orchestrator_constructs_once_and_rejects_duplicate_before_start():
    from controllers.directional_trading.ok_rsi_core.execution import EpisodeOrderExecutor
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
    controller, _ = setup_controller()
    action = actions(controller)[0]
    orchestrator = object.__new__(ExecutorOrchestrator)
    orchestrator.strategy = SimpleNamespace(
        controllers={controller.config.id: controller}, connectors={"okx": MagicMock()}, current_timestamp=10020)
    orchestrator.active_executors = {controller.config.id: []}
    orchestrator.executors_update_interval = 1
    orchestrator.executors_max_retries = 0
    with patch.object(EpisodeOrderExecutor, "start", autospec=True) as start:
        orchestrator.create_executor(action)
        with pytest.raises(ValueError, match="episode"):
            orchestrator.create_executor(action)
    assert len(orchestrator.active_executors[controller.config.id]) == 1
    assert isinstance(orchestrator.active_executors[controller.config.id][0], EpisodeOrderExecutor)
    assert start.call_count == 1
