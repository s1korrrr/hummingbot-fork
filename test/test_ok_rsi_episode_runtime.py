import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction


def setup_controller(*, mode="buy_sell", store=None, **config_values):
    from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
    from controllers.directional_trading.rsi_v5_runtime_store import RSIV5RuntimeStore
    config = OKRSIControllerConfig(
        id="episode-test", episode_enabled=True, strict_hmm_mode=False,
        connector_name="okx", trading_pair="ETH-USDT", trade_mode=mode,
        max_total_position_usd=Decimal("500"), **config_values)
    provider = MagicMock()
    provider.time.return_value = 10020
    provider.get_available_balance.return_value = Decimal("1000")
    provider.get_price_by_type.return_value = Decimal("94")
    provider.get_trading_rules.return_value = SimpleNamespace(
        min_price_increment=Decimal(".01"), min_order_size=Decimal(".001"),
        min_base_amount_increment=Decimal(".001"), min_notional_size=Decimal("1"), min_order_value=Decimal("1"))
    store = store or RSIV5RuntimeStore.in_memory()
    controller = OKRSIController(config, provider, asyncio.Queue(), episode_store=store)
    controller.processed_data = {"signal": 1, "signal_state": {"condition_ok": True}}
    return controller, store


def context():
    return dict(timestamp=10020, htf_timestamp=7200, low=Decimal("93"),
                atr_1m=Decimal("1"), atr_15m=Decimal("2"), atr_1h=Decimal("2"),
                anchor=Decimal("100"), recovery=False, markup=False, reset=False,
                hostile=False, cascade=False)


def actions(controller):
    with patch("controllers.directional_trading.ok_rsi_core.runtime.completed_market_context", return_value=context()):
        return controller.determine_executor_actions()


def test_durable_reservation_precedes_action_and_restart_never_redispatches():
    controller, store = setup_controller()
    created = actions(controller)
    assert len(created) == 1 and isinstance(created[0], CreateExecutorAction)
    request_id = created[0].executor_config.id
    assert request_id in store.load(controller.config.id)[-1].payload["state"]["orders"]
    restarted, _ = setup_controller(store=store)
    assert actions(restarted) == []
    assert request_id in restarted._episode_runtime.state.orders


def test_failed_checkpoint_produces_no_executor():
    controller, store = setup_controller()
    store.append_many = MagicMock(side_effect=RuntimeError("disk-full"))
    assert actions(controller) == []
    assert "persistence" in controller.get_custom_info()["episode"]["hold_reason"]


def test_untagged_inventory_is_not_adopted():
    controller, _ = setup_controller()
    controller.positions_held = [SimpleNamespace(connector_name="okx", trading_pair="ETH-USDT",
                                                 side=TradeType.BUY, amount=Decimal("1"))]
    assert actions(controller) == []
    assert controller.get_custom_info()["episode"]["hold_reason"] == "unassigned_held_inventory"


def test_kill_and_operator_pause_cannot_reserve_buy():
    for attr in ("manual_kill_switch", "_operator_entry_paused"):
        controller, store = setup_controller()
        setattr(controller.config if attr == "manual_kill_switch" else controller, attr, True)
        assert actions(controller) == []
        assert controller._episode_runtime.state.orders == {}


def test_sell_only_assignment_does_not_adopt_wallet_surplus_or_buy():
    controller, _ = setup_controller(mode="sell_only", episode_assignment_id="operator-assignment",
                                     episode_initial_base=Decimal("2"), sell_only_if_profitable=False)
    assert actions(controller) == []
    assert controller._episode_runtime.state.base == Decimal("2")
    assert all(order.request.side != "BUY" for order in controller._episode_runtime.state.orders.values())


def test_hot_reload_cannot_disable_unresolved_episode():
    from controllers.directional_trading.ok_rsi import OKRSIController
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    controller, _ = setup_controller()
    actions(controller)
    disabled = controller.config.model_copy(update={"episode_enabled": False})
    replacement = OKRSIController(disabled, controller.market_data_provider, asyncio.Queue())
    with pytest.raises(ValueError, match="episode"):
        StrategyV2Base._copy_controller_runtime_state(controller, replacement)


def test_restart_cannot_disable_durable_unresolved_episode():
    from controllers.directional_trading.ok_rsi import OKRSIController
    controller, store = setup_controller()
    actions(controller)
    disabled = controller.config.model_copy(update={"episode_enabled": False})
    with pytest.raises(ValueError, match="episode"):
        OKRSIController(disabled, controller.market_data_provider, asyncio.Queue(), episode_store=store)


def test_native_partial_receipt_reconciles_cash_fee_and_net_inventory_before_deciding():
    controller, store = setup_controller()
    created = actions(controller)[0]
    controller.executors_info = [SimpleNamespace(
        id=created.executor_config.id, connector_name="okx", trading_pair="ETH-USDT", is_active=False,
        is_done=True, custom_info={"episode_receipt": dict(gross_base=".4", gross_quote="37.6",
                                                           base_fee=".0004", quote_fee="0", final=True, fees_known=True)})]
    assert actions(controller) == []
    assert controller._episode_runtime.state.base == Decimal(".3996")
    assert controller._episode_runtime.state.gross_spent == Decimal("37.6")
    restarted, _ = setup_controller(store=store)
    restarted.executors_info = controller.executors_info
    actions(restarted)
    assert restarted._episode_runtime.state.base == Decimal(".3996")


def test_direct_factory_cannot_change_an_authorized_quantity():
    controller, _ = setup_controller()
    created = actions(controller)[0]
    with pytest.raises(ValueError, match="authorization"):
        controller.get_executor_class_for_config(created.executor_config.model_copy(update={"amount": Decimal("100")}))


@pytest.mark.parametrize("enabled", [False, True])
def test_framework_config_loader_resolves_public_controller_after_config_extraction(enabled):
    from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
    config = OKRSIControllerConfig(id="loader-regression", strict_hmm_mode=False, episode_enabled=enabled)
    assert config.get_controller_class() is OKRSIController
