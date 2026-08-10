import asyncio
import importlib
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from hummingbot.core.data_type.common import PositionMode, TradeType
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction

ROOT_PATH = Path(__file__).resolve().parent.parent
ROOT_PATH_STR = str(ROOT_PATH)


def ensure_local_controller_imports():
    while ROOT_PATH_STR in sys.path:
        sys.path.remove(ROOT_PATH_STR)
    sys.path.insert(0, ROOT_PATH_STR)
    for module_name in list(sys.modules):
        if module_name == "controllers" or module_name.startswith("controllers."):
            sys.modules.pop(module_name, None)
    importlib.invalidate_caches()


def make_provider(mark: Decimal = Decimal("100")):
    provider = MagicMock()
    provider.initialize_rate_sources.return_value = None
    provider.time.return_value = 1_700_000_000.0
    provider.get_price_by_type.return_value = mark
    provider.get_available_balance.return_value = Decimal("10000")
    provider.get_balance.return_value = Decimal("10000")
    provider.get_trading_rules.return_value = SimpleNamespace(
        min_order_size=Decimal("0.0001"),
        min_base_amount_increment=Decimal("0.0001"),
        min_price_increment=Decimal("0.1"),
        min_notional_size=Decimal("5"),
        min_order_value=Decimal("5"),
    )
    provider.quantize_order_amount.side_effect = (
        lambda connector, pair, amount: amount
    )
    return provider


def buy_executor(
    executor_id: str,
    *,
    configured_base: str = "1",
    filled_base: str = "0",
    filled_quote: str = "0",
    entry_price: str = "100",
    executor_type: str = "position_executor",
    is_active: bool = True,
    status: RunnableStatus = RunnableStatus.RUNNING,
):
    return SimpleNamespace(
        id=executor_id,
        is_active=is_active,
        is_done=status == RunnableStatus.TERMINATED,
        is_trading=Decimal(filled_base) > 0,
        status=status,
        type=executor_type,
        connector_name="hyperliquid_perpetual",
        trading_pair="BTC-USD",
        side=TradeType.BUY,
        timestamp=1_699_999_900.0,
        close_timestamp=None,
        close_type=None,
        net_pnl_pct=Decimal("0"),
        net_pnl_quote=Decimal("0"),
        cum_fees_quote=Decimal("0"),
        filled_amount_quote=Decimal(filled_quote),
        config=SimpleNamespace(
            amount=Decimal(configured_base),
            entry_price=Decimal(entry_price),
            level_id=None,
        ),
        custom_info={
            "side": TradeType.BUY,
            "entry_price": Decimal(entry_price),
            "open_order_filled_amount_base": Decimal(filled_base),
            "open_order_filled_amount_quote": Decimal(filled_quote),
        },
    )


class TestRSIv5HyperliquidController:
    def setup_method(self):
        ensure_local_controller_imports()

    def build_controller(self, provider=None, **overrides):
        from controllers.directional_trading.rsi_v5_hyperliquid import (
            RSIv5HyperliquidController,
            RSIv5HyperliquidControllerConfig,
        )

        controller = RSIv5HyperliquidController(
            config=RSIv5HyperliquidControllerConfig(
                id="btc-rsi-v5-hyperliquid",
                **overrides,
            ),
            market_data_provider=provider or make_provider(),
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = []
        controller.executors_info = []
        controller.processed_data = {
            "signal": 0,
            "signal_state": {
                "condition_ok": True,
                "sell_reason": "rsi-reversal",
            },
        }
        return controller

    def test_defaults_are_hyperliquid_oneway_and_no_position_hold(self):
        from controllers.directional_trading.rsi_v5_hyperliquid import RSIv5HyperliquidControllerConfig

        config = RSIv5HyperliquidControllerConfig(id="defaults")

        assert config.connector_name == "hyperliquid_perpetual"
        assert config.trading_pair == "BTC-USD"
        assert config.position_mode == PositionMode.ONEWAY
        assert config.leverage == 1
        assert config.perpetual_max_leverage == 2
        assert config.perpetual_allow_position_hold is False
        assert config.use_bag_freeze is False
        assert config.early_stop_keep_position is False

    @pytest.mark.parametrize(
        "overrides",
        [
            {"leverage": 3, "perpetual_max_leverage": 2},
            {"position_mode": "HEDGE"},
            {"use_bag_freeze": True},
            {"early_stop_keep_position": True},
        ],
    )
    def test_unsafe_perpetual_configuration_is_rejected(self, overrides):
        from controllers.directional_trading.rsi_v5_hyperliquid import RSIv5HyperliquidControllerConfig

        with pytest.raises(Exception):
            RSIv5HyperliquidControllerConfig(id="invalid", **overrides)

    def test_active_partial_fill_counts_as_inventory_and_reserves_pending_notional(self):
        controller = self.build_controller(
            provider=make_provider(Decimal("110"))
        )
        controller.executors_info = [
            buy_executor(
                "partial",
                configured_base="1",
                filled_base="0.4",
                filled_quote="40",
                entry_price="100",
            )
        ]

        assert controller._has_buy_inventory() is True
        assert controller._get_position_cost_basis() == Decimal("100")
        assert (
            controller._get_active_buy_executor_position_value_usd()
            == Decimal("44")
        )
        assert (
            controller._get_pending_buy_executor_notional_usd()
            == Decimal("66")
        )
        assert controller._get_total_position_value_usd() == Decimal("110")

    def test_executor_exposure_recovers_fill_amount_when_custom_base_field_is_missing(self):
        controller = self.build_controller(
            provider=make_provider(Decimal("110"))
        )
        executor = buy_executor(
            "fallback",
            configured_base="1",
            filled_base="0",
            filled_quote="40",
            entry_price="100",
        )
        executor.custom_info.pop("open_order_filled_amount_base")
        executor.custom_info.pop("open_order_filled_amount_quote")
        controller.executors_info = [executor]

        exposure = controller._executor_exposure(executor)

        assert exposure.filled_base == Decimal("0.4")
        assert exposure.pending_base == Decimal("0.6")
        assert controller._get_total_position_value_usd() == Decimal("110")

    def test_shutting_down_executor_keeps_filled_exposure_but_releases_pending_reservation(self):
        controller = self.build_controller(
            provider=make_provider(Decimal("110"))
        )
        controller.executors_info = [
            buy_executor(
                "closing",
                configured_base="1",
                filled_base="0.4",
                filled_quote="40",
                entry_price="100",
                is_active=False,
                status=RunnableStatus.SHUTTING_DOWN,
            )
        ]

        exposure = controller._exposure_snapshot()

        assert exposure.filled_notional == Decimal("44")
        assert exposure.pending_notional == Decimal("0")
        assert exposure.total_notional == Decimal("44")
        assert controller._has_buy_inventory() is True

    def test_shutting_down_executor_is_not_sent_a_duplicate_stop_action(self):
        controller = self.build_controller()
        controller.executors_info = [
            buy_executor(
                "closing",
                configured_base="1",
                filled_base="0.4",
                filled_quote="40",
                is_active=False,
                status=RunnableStatus.SHUTTING_DOWN,
            )
        ]
        controller.processed_data["signal"] = -1

        actions = controller.stop_actions_proposal()

        assert [
            action
            for action in actions
            if isinstance(action, StopExecutorAction)
        ] == []
        assert controller._has_buy_inventory() is True

    def test_rsi_exit_stops_every_owning_buy_executor_without_creating_sell_executor(self):
        controller = self.build_controller()
        controller.executors_info = [
            buy_executor("unfilled", filled_base="0", filled_quote="0"),
            buy_executor("partial", filled_base="0.4", filled_quote="40"),
            buy_executor("filled", filled_base="1", filled_quote="100"),
        ]
        controller.processed_data["signal"] = -1

        actions = controller.stop_actions_proposal()

        exit_actions = [
            action
            for action in actions
            if isinstance(action, StopExecutorAction)
        ]
        assert {action.executor_id for action in exit_actions} == {
            "unfilled",
            "partial",
            "filled",
        }
        assert all(action.keep_position is False for action in exit_actions)
        assert controller.can_create_executor(-1) is False
        with pytest.raises(RuntimeError, match="owning PositionExecutor"):
            controller.get_executor_config(
                TradeType.SELL,
                Decimal("100"),
                Decimal("1"),
            )

    def test_exit_request_is_idempotent_until_retry_interval(self):
        provider = make_provider()
        controller = self.build_controller(
            provider=provider,
            executor_exit_retry_seconds=5,
        )
        controller.executors_info = [
            buy_executor("filled", filled_base="1", filled_quote="100")
        ]
        controller.processed_data["signal"] = -1

        first = controller.stop_actions_proposal()
        second = controller.stop_actions_proposal()
        provider.time.return_value += 5
        retry = controller.stop_actions_proposal()

        assert [
            action.executor_id
            for action in first
            if isinstance(action, StopExecutorAction)
        ] == ["filled"]
        assert [
            action.executor_id
            for action in second
            if isinstance(action, StopExecutorAction)
        ] == []
        assert [
            action.executor_id
            for action in retry
            if isinstance(action, StopExecutorAction)
        ] == ["filled"]

    def test_buy_executor_uses_canonical_type_partial_fill_safe_runtime_and_hard_stop(self):
        from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
        from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
        from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
        from hummingbot.strategy_v2.executors.rsi_position_executor import RSIPositionExecutor

        controller = self.build_controller(
            perpetual_hard_stop_loss_pct=Decimal("0.08")
        )
        config = controller.get_executor_config(
            TradeType.BUY,
            Decimal("100"),
            Decimal("0.3"),
        )

        assert isinstance(config, PositionExecutorConfig)
        assert config.type == "position_executor"
        assert config.connector_name == "hyperliquid_perpetual"
        assert config.leverage == 1
        assert config.triple_barrier_config.stop_loss == Decimal("0.08")
        assert ExecutorOrchestrator._executor_mapping["position_executor"] is PositionExecutor
        assert controller.get_executor_class_for_config(config) is RSIPositionExecutor
        assert "rsi_position_executor" not in ExecutorOrchestrator._executor_mapping

    def test_perpetual_held_position_uses_position_amount_not_spot_wallet_balance(self):
        provider = make_provider()
        provider.get_available_balance.return_value = Decimal("0")
        provider.get_balance.return_value = Decimal("0")
        controller = self.build_controller(
            provider=provider,
            perpetual_allow_position_hold=True,
        )
        controller.positions_held = [
            SimpleNamespace(
                connector_name="hyperliquid_perpetual",
                trading_pair="BTC-USD",
                side=TradeType.BUY,
                amount=Decimal("0.25"),
                breakeven_price=Decimal("100"),
                cum_fees_quote=Decimal("0"),
            )
        ]

        assert controller._get_base_balance() == Decimal("0.25")
        assert controller._get_sellable_inventory_amount() == Decimal("0.25")

    def test_unexpected_short_position_blocks_new_entries(self):
        controller = self.build_controller()
        controller.positions_held = [
            SimpleNamespace(
                connector_name="hyperliquid_perpetual",
                trading_pair="BTC-USD",
                side=TradeType.SELL,
                amount=Decimal("0.25"),
                breakeven_price=Decimal("100"),
                cum_fees_quote=Decimal("0"),
            )
        ]

        assert controller.can_create_executor(1) is False
        assert "unexpected_short" in controller.get_custom_info()["runtime_hold"]["reasons"]

    def test_unowned_held_long_sets_orphan_hold_without_opening_empty_episode(self):
        controller = self.build_controller()
        controller.positions_held = [
            SimpleNamespace(
                connector_name="hyperliquid_perpetual",
                trading_pair="BTC-USD",
                side=TradeType.BUY,
                amount=Decimal("0.25"),
                breakeven_price=Decimal("100"),
                cum_fees_quote=Decimal("0"),
            )
        ]

        controller._sync_episode_tracker()

        assert controller._episode_tracker.is_open is False
        assert controller.can_create_executor(1) is False
        assert "orphan_exposure" in controller.get_custom_info()["runtime_hold"]["reasons"]

    def test_shutting_down_pnl_is_provisional_until_terminal_settlement(self):
        controller = self.build_controller()
        controller._episode_tracker.open(timestamp=100)
        controller._episode_tracker.register_executor("entry", Decimal("100"))
        shutting_down = buy_executor(
            "entry",
            filled_base="1",
            filled_quote="100",
            is_active=False,
            status=RunnableStatus.SHUTTING_DOWN,
        )
        shutting_down.net_pnl_quote = Decimal("-2")

        controller._track_executor_result(shutting_down)
        assert controller._episode_tracker.close_if_flat(is_flat=True, timestamp=110) is None

        terminal = buy_executor(
            "entry",
            filled_base="1",
            filled_quote="100",
            is_active=False,
            status=RunnableStatus.TERMINATED,
        )
        terminal.net_pnl_quote = Decimal("999")
        controller._track_executor_result(terminal)

        outcome = controller._episode_tracker.close_if_flat(is_flat=True, timestamp=120)
        assert outcome is not None
        assert outcome.net_pnl_quote == Decimal("999")

    def test_changed_final_settlement_sets_fail_closed_runtime_hold(self):
        controller = self.build_controller()
        controller._episode_tracker.open(timestamp=100)
        controller._episode_tracker.register_executor("entry", Decimal("100"))
        terminal = buy_executor(
            "entry",
            filled_base="1",
            filled_quote="100",
            is_active=False,
            status=RunnableStatus.TERMINATED,
        )
        terminal.net_pnl_quote = Decimal("1")
        controller._track_executor_result(terminal)
        terminal.net_pnl_quote = Decimal("2")

        controller._track_executor_result(terminal)

        assert controller.can_create_executor(1) is False
        assert "settlement_conflict" in controller.get_custom_info()["runtime_hold"]["reasons"]

    def test_loss_streak_changes_once_for_aggregate_episode(self):
        controller = self.build_controller()
        controller._episode_tracker.open(timestamp=100)
        controller._episode_tracker.register_executor(
            "scout",
            Decimal("30"),
        )
        controller._episode_tracker.register_executor(
            "runner",
            Decimal("70"),
        )
        controller._consecutive_losses = 2

        controller._track_executor_result(
            SimpleNamespace(
                id="scout",
                is_done=True,
                status=RunnableStatus.TERMINATED,
                net_pnl_quote=Decimal("2"),
            )
        )
        controller._track_executor_result(
            SimpleNamespace(
                id="runner",
                is_done=True,
                status=RunnableStatus.TERMINATED,
                net_pnl_quote=Decimal("-5"),
            )
        )
        controller.executors_info = []
        controller.positions_held = []
        controller._finalize_episode_if_flat(timestamp=130)

        assert controller._consecutive_losses == 3
        assert controller._last_loss_timestamp == 130
