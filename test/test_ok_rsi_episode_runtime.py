import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


def setup_controller(*, mode="buy_sell", store=None, **config_values):
    from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
    from controllers.directional_trading.rsi_v5_runtime_store import RSIV5RuntimeStore
    maximum_position = config_values.pop("max_total_position_usd", Decimal("500"))
    config = OKRSIControllerConfig(
        id="episode-test", episode_enabled=True, strict_hmm_mode=False,
        connector_name="okx", trading_pair="ETH-USDT", trade_mode=mode,
        max_total_position_usd=maximum_position, **config_values)
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
    assert created[0].executor_config.episode_intent == "SCOUT_BUY"
    assert request_id in store.load(controller.config.id)[-1].payload["state"]["orders"]
    restarted, _ = setup_controller(store=store)
    assert actions(restarted) == []
    assert request_id in restarted._episode_runtime.state.orders


def test_lifecycle_v2_configuration_maps_without_changing_hard_cap():
    controller, _ = setup_controller(
        max_total_position_usd=Decimal("1500"),
        episode_budget_quote=Decimal("500"),
        episode_sequential_scale_ins=True,
        episode_require_scale_in_signal=True,
        episode_scale_in_setup_bars=30,
        episode_scale_in_stabilization_bars=15,
        episode_level_fractions=[Decimal("0.10"), Decimal("0.15"), Decimal("0.20"), Decimal("0.25"), Decimal("0.30")],
        episode_dip_atr_levels=[Decimal("2"), Decimal("4"), Decimal("6"), Decimal("9"), Decimal("13")],
        episode_basis_improvement_atr=Decimal("0.50"),
        episode_last_fill_gap_atr=Decimal("1.00"),
        episode_distribution_atr_levels=[Decimal("6"), Decimal("9"), Decimal("12")],
        episode_remaining_fractions=[Decimal("0.80"), Decimal("0.60"), Decimal("0.40")],
        episode_trail_atr=Decimal("3"),
        episode_trail_min=Decimal("0.01"),
        episode_trail_max=Decimal("0.05"),
        episode_hostile_risk_latch=True,
        episode_recovery_exit_enabled=True,
        episode_scout_rebound_confirmation=True,
        episode_structure_held_runner=True,
        episode_markup_release_runner=True,
    )

    policy = controller._episode_runtime.config
    assert policy.budget_quote == Decimal("500")
    assert policy.mark_cap_quote == Decimal("1500")
    assert policy.sequential_scale_ins is True
    assert policy.require_scale_in_signal is True
    assert policy.setup_bars == 30
    assert policy.stabilization_bars == 15
    assert policy.dip_atr_levels == tuple(map(Decimal, ("2", "4", "6", "9", "13")))
    assert policy.basis_improvement_atr == Decimal("0.50")
    assert policy.last_fill_gap_atr == Decimal("1.00")
    assert policy.distribution_atr_levels == tuple(map(Decimal, ("6", "9", "12")))
    assert policy.remaining_fractions == tuple(map(Decimal, ("0.80", "0.60", "0.40")))
    assert policy.trail_atr == Decimal("3")
    assert policy.trail_min == Decimal("0.01")
    assert policy.trail_max == Decimal("0.05")
    assert policy.hostile_risk_latch is True
    assert policy.recovery_exit_enabled is True
    assert policy.adaptive_entry_pricing is False
    assert policy.scout_rebound_confirmation is True
    assert policy.structure_held_runner is True
    assert policy.markup_release_runner is True
    assert policy.rebound_atr == Decimal("0.25")


def test_public_config_rejects_markup_release_without_structure_runner():
    with pytest.raises(ValueError, match="markup release"):
        setup_controller(episode_markup_release_runner=True)


def test_markup_release_receipt_maps_to_executor_and_status():
    from controllers.directional_trading.ok_rsi_core.episode import (
        EpisodeState,
        OrderProgress,
        OrderRequest,
        Transition,
    )

    controller, _ = setup_controller(
        episode_structure_held_runner=True,
        episode_markup_release_runner=True,
    )
    runtime = controller._episode_runtime
    request = OrderRequest(
        id="markup-release-sell-1",
        episode_id="episode-1",
        level=-1,
        side="SELL",
        amount=Decimal("0.4"),
        price=Decimal("103.2"),
        reserved_quote=Decimal("0"),
        expires_at=10620,
        revision=1,
        intent="TRAILING_PROFIT_EXIT",
        exit_policy="MARKUP_RELEASED_RUNNER_V1",
        exit_hourly_recovered=True,
        exit_risk_clear=True,
        exit_trail_breach="MARKUP_LOSS",
        exit_target_base=Decimal("0"),
        exit_target_revision=2,
    )
    state = EpisodeState(
        config_hash=runtime.config.identity,
        phase="EXIT",
        episode_number=1,
        episode_id="episode-1",
        timestamp=10020,
        levels=["AUTHORIZED"] * len(runtime.config.level_fractions),
        orders={request.id: OrderProgress(request)},
        base=Decimal("0.4"),
        cost=Decimal("40"),
        gross_spent=Decimal("100"),
        distribution_start_base=Decimal("1"),
        target_base=Decimal("0"),
        command_revision=1,
        exit_target_revision=2,
        last_exit_trail_breach="MARKUP_LOSS",
        last_exit_hourly_recovered=True,
        last_exit_risk_clear=True,
    )
    result = Transition(state=state, requests=(request,), reason="trailing_profit_exit")

    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=context(),
    ), patch("controllers.directional_trading.ok_rsi_core.runtime.transition", return_value=result):
        created = controller.determine_executor_actions()[0]

    executor = created.executor_config.model_copy(update={"controller_id": controller.config.id})
    assert executor.episode_exit_policy == "MARKUP_RELEASED_RUNNER_V1"
    assert executor.episode_exit_trail_breach == "MARKUP_LOSS"
    assert executor.episode_exit_target_base == Decimal("0")
    assert executor.episode_exit_target_revision == 2
    assert controller.get_executor_class_for_config(executor).__name__ == "EpisodeOrderExecutor"
    runtime.last_market = SimpleNamespace(
        hourly_recovered=True,
        markup=False,
        cascade=False,
        hostile=False,
        reset=False,
    )
    status = runtime.status()
    assert status["exit_policy"] == "MARKUP_RELEASED_RUNNER_V1"
    assert status["exit_markup"] is False


def test_structure_exit_receipt_maps_to_executor_and_factory_rejects_mutation():
    from controllers.directional_trading.ok_rsi_core.episode import (
        EpisodeState,
        OrderProgress,
        OrderRequest,
        Transition,
    )

    controller, _ = setup_controller(episode_structure_held_runner=True)
    runtime = controller._episode_runtime
    request = OrderRequest(
        id="structure-sell-1",
        episode_id="episode-1",
        level=-1,
        side="SELL",
        amount=Decimal("0.6"),
        price=Decimal("102.4"),
        reserved_quote=Decimal("0"),
        expires_at=10620,
        revision=1,
        intent="DISTRIBUTION_SELL",
        exit_policy="STRUCTURE_HELD_RUNNER_V1",
        exit_hourly_recovered=True,
        exit_risk_clear=True,
        exit_trail_breach="PRICE_AND_EQUITY",
        exit_target_base=Decimal("0.4"),
        exit_target_revision=1,
    )
    state = EpisodeState(
        config_hash=runtime.config.identity,
        phase="DISTRIBUTE",
        episode_number=1,
        episode_id="episode-1",
        timestamp=10020,
        levels=["AUTHORIZED"] * len(runtime.config.level_fractions),
        orders={request.id: OrderProgress(request)},
        base=Decimal("1"),
        cost=Decimal("100"),
        gross_spent=Decimal("100"),
        distribution_start_base=Decimal("1"),
        target_base=Decimal("0.4"),
        command_revision=1,
        structure_runner_active=True,
        exit_target_revision=1,
        last_exit_trail_breach="PRICE_AND_EQUITY",
    )
    result = Transition(state=state, requests=(request,), reason="structure_runner_distribution")

    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=context(),
    ), patch("controllers.directional_trading.ok_rsi_core.runtime.transition", return_value=result):
        created = controller.determine_executor_actions()[0]

    executor = created.executor_config.model_copy(update={"controller_id": controller.config.id})
    assert executor.episode_exit_policy == "STRUCTURE_HELD_RUNNER_V1"
    assert executor.episode_exit_hourly_recovered is True
    assert executor.episode_exit_risk_clear is True
    assert executor.episode_exit_trail_breach == "PRICE_AND_EQUITY"
    assert executor.episode_exit_target_base == Decimal("0.4")
    assert executor.episode_exit_target_revision == 1
    with pytest.raises(ValueError, match="authorization"):
        controller.get_executor_class_for_config(
            executor.model_copy(update={"episode_exit_target_revision": 2})
        )
    assert controller.get_executor_class_for_config(executor).__name__ == "EpisodeOrderExecutor"


def test_status_exposes_persisted_structure_runner_state():
    controller, _ = setup_controller(episode_structure_held_runner=True)
    runtime = controller._episode_runtime
    runtime.state.distribution_start_base = Decimal("1")
    runtime.state.target_base = Decimal("0.4")
    runtime.state.structure_runner_active = True
    runtime.state.exit_target_revision = 3
    runtime.state.last_exit_trail_breach = "EQUITY"
    runtime.state.last_exit_hourly_recovered = True
    runtime.state.last_exit_risk_clear = True
    runtime.last_market = SimpleNamespace(
        hourly_recovered=True,
        cascade=False,
        hostile=False,
        reset=False,
    )

    status = runtime.status()

    assert status["exit_policy"] == "STRUCTURE_HELD_RUNNER_V1"
    assert status["exit_hourly_recovered"] is True
    assert status["exit_risk_clear"] is True
    assert status["structure_runner_active"] is True
    assert status["structure_runner_target_base"] == "0.4"
    assert status["last_exit_trail_breach"] == "EQUITY"
    assert status["last_exit_hourly_recovered"] is True
    assert status["last_exit_risk_clear"] is True
    assert status["exit_target_revision"] == 3


def test_recovery_aware_scout_maps_context_and_persists_route_receipt():
    controller, store = setup_controller(episode_recovery_aware_scout=True)

    recovery_context = context() | {"recovery": True}
    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=recovery_context,
    ):
        assert controller.determine_executor_actions() == []
    assert controller._episode_runtime.reason == "recovery_scout_deferred"
    assert controller._episode_runtime.state.episode_id is None

    restarted, _ = setup_controller(store=store, episode_recovery_aware_scout=True)
    restarted.market_data_provider.time.return_value = 10080
    next_context = context() | {"timestamp": 10080, "recovery": False}
    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=next_context,
    ):
        created = restarted.determine_executor_actions()

    assert len(created) == 1
    executor = created[0].executor_config
    assert executor.episode_setup_policy == "RECOVERY_AWARE_SCOUT_V1"
    assert executor.episode_setup_started_at == 0
    status = restarted.get_custom_info()["episode"]
    assert status["last_entry_setup_policy"] == "RECOVERY_AWARE_SCOUT_V1"


def test_rebound_scout_persists_setup_then_emits_exact_owner_receipt():
    controller, store = setup_controller(episode_scout_rebound_confirmation=True)

    assert actions(controller) == []
    armed = controller._episode_runtime.status()
    assert armed["setup_policy"] == "SCOUT_REBOUND_V1"
    assert armed["setup_started_at"] == 10020
    assert armed["setup_low"] == "93"
    restarted, _ = setup_controller(
        store=store,
        episode_scout_rebound_confirmation=True,
    )
    restarted.processed_data = {"signal": 0, "signal_state": {"condition_ok": True}}
    restarted.market_data_provider.time.return_value = 10080
    first_context = context() | {"timestamp": 10080, "low": Decimal("93.1")}
    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=first_context,
    ):
        assert restarted.determine_executor_actions() == []
    restarted.market_data_provider.time.return_value = 10140
    second_context = context() | {"timestamp": 10140, "low": Decimal("93.1")}
    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=second_context,
    ):
        created = restarted.determine_executor_actions()

    assert len(created) == 1
    executor = created[0].executor_config
    assert executor.episode_setup_policy == "SCOUT_REBOUND_V1"
    assert executor.episode_setup_started_at == 10020
    assert executor.episode_setup_low == Decimal("93")
    assert executor.episode_setup_rebound == Decimal("0.25")
    assert executor.episode_setup_no_new_low == 2


def test_recovery_rebound_setup_survives_restart_and_emits_route_receipt():
    controller, store = setup_controller(episode_recovery_rebound_scout=True)
    recovery_context = context() | {"recovery": True}
    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=recovery_context,
    ):
        assert controller.determine_executor_actions() == []
    armed = controller._episode_runtime.status()
    assert armed["setup_policy"] == "RECOVERY_REBOUND_SCOUT_V1"
    assert armed["setup_started_at"] == 10020
    assert armed["setup_confirmation_started_at"] is None

    restarted, _ = setup_controller(
        store=store,
        episode_recovery_rebound_scout=True,
    )
    restarted.processed_data = {"signal": 0, "signal_state": {"condition_ok": True}}
    restarted.market_data_provider.time.return_value = 10080
    cleared_context = context() | {
        "timestamp": 10080,
        "recovery": False,
        "low": Decimal("93.1"),
    }
    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=cleared_context,
    ):
        assert restarted.determine_executor_actions() == []
    assert restarted._episode_runtime.status()["setup_confirmation_started_at"] == 10080

    for timestamp in (10140, 10200):
        restarted.market_data_provider.time.return_value = timestamp
        followup = context() | {
            "timestamp": timestamp,
            "recovery": False,
            "low": Decimal("93.1"),
        }
        with patch(
            "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
            return_value=followup,
        ):
            created = restarted.determine_executor_actions()

    assert len(created) == 1
    executor = created[0].executor_config
    assert executor.episode_setup_policy == "RECOVERY_REBOUND_SCOUT_V1"
    assert executor.episode_setup_started_at == 10020
    assert executor.episode_setup_confirmation_started_at == 10080
    assert executor.episode_setup_low == Decimal("93")
    assert executor.episode_setup_no_new_low == 2


def test_adaptive_entry_pricing_maps_owner_signal_context_and_exposes_receipt():
    controller, _ = setup_controller(episode_adaptive_entry_pricing=True)
    controller.processed_data = {
        "signal": 1,
        "signal_state": {"condition_ok": True, "signal_strength_score": 0.8},
        "trend_confirmation": {"alignment_score": -0.2},
    }

    created = actions(controller)

    assert len(created) == 1
    assert created[0].executor_config.price == Decimal("93.80")
    status = controller.get_custom_info()["episode"]
    assert status["last_entry_pricing_policy"] == "ADAPTIVE_MAKER_V1"
    assert status["last_entry_reference_price"] == "94"
    assert status["last_entry_selected_price"] == "93.8"
    assert status["last_entry_discount"] == "0.2"
    assert status["last_entry_discount_atr"] == "0.2"
    assert status["last_entry_confidence"] == "0.8"
    assert status["last_entry_trend_alignment"] == "-0.2"


def test_reserved_adaptive_entry_maps_hard_reservation_rail_and_receipt():
    controller, _ = setup_controller(
        episode_adaptive_entry_pricing=True,
        episode_adaptive_reserved_entry=True,
    )
    controller.processed_data = {
        "signal": 1,
        "signal_state": {"condition_ok": True, "signal_strength_score": 0.8},
        "trend_confirmation": {"alignment_score": -0.2},
    }

    created = actions(controller)

    assert len(created) == 1
    status = controller.get_custom_info()["episode"]
    assert status["last_entry_pricing_policy"] == "ADAPTIVE_RESERVED_ENTRY_V2"
    assert status["entry_reservation_allowed"] is True
    assert status["last_entry_expires_at"] == created[0].executor_config.episode_expires_at


def test_reserved_adaptive_entry_persists_then_operator_pause_stops_native_order():
    controller, _ = setup_controller(
        episode_adaptive_entry_pricing=True,
        episode_adaptive_reserved_entry=True,
    )
    controller.processed_data = {
        "signal": 1,
        "signal_state": {"condition_ok": True, "signal_strength_score": 0.8},
        "trend_confirmation": {"alignment_score": -0.2},
    }
    created = actions(controller)[0]
    controller.executors_info = [SimpleNamespace(
        id=created.executor_config.id,
        connector_name="okx",
        trading_pair="ETH-USDT",
        is_active=True,
        is_done=False,
        custom_info={"episode_receipt": {
            "gross_base": "0",
            "gross_quote": "0",
            "base_fee": "0",
            "quote_fee": "0",
            "final": False,
            "fees_known": True,
            "errors": [],
        }},
    )]
    controller.processed_data["signal"] = 0
    controller.processed_data["signal_state"]["condition_ok"] = False
    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=context() | {
            "timestamp": 10080,
            "hostile": True,
            "recovery": True,
            "markup": True,
        },
    ):
        assert controller.determine_executor_actions() == []

    controller._operator_entry_paused = True
    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=context() | {"timestamp": 10140},
    ):
        stopped = controller.determine_executor_actions()

    assert len(stopped) == 1
    assert isinstance(stopped[0], StopExecutorAction)
    assert stopped[0].executor_id == created.executor_config.id


def test_manual_kill_returns_only_stop_for_active_episode_order():
    controller, _ = setup_controller()
    created = actions(controller)[0]
    controller.executors_info = [SimpleNamespace(
        id=created.executor_config.id,
        connector_name="okx",
        trading_pair="ETH-USDT",
        is_active=True,
        is_done=False,
        custom_info={"episode_receipt": {
            "gross_base": "0",
            "gross_quote": "0",
            "base_fee": "0",
            "quote_fee": "0",
            "final": False,
            "fees_known": True,
            "errors": [],
        }},
    )]
    controller.config.manual_kill_switch = True
    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=context() | {"timestamp": 10080},
    ):
        stopped = controller.determine_executor_actions()

    assert len(stopped) == 1
    assert isinstance(stopped[0], StopExecutorAction)
    assert stopped[0].executor_id == created.executor_config.id


def test_adaptive_entry_pricing_fails_closed_when_owner_inputs_are_missing():
    controller, _ = setup_controller(episode_adaptive_entry_pricing=True)

    assert actions(controller) == []
    assert controller._episode_runtime.reason.startswith("context_unavailable:")
    assert controller._episode_runtime.state.orders == {}


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


def test_status_exposes_episode_basis_profit_floor_and_adverse_latch():
    from controllers.directional_trading.ok_rsi_core.episode import minimum_profit_price

    controller, _ = setup_controller()
    runtime = controller._episode_runtime
    runtime.state.base = Decimal("2")
    runtime.state.cost = Decimal("188")
    runtime.state.gross_spent = Decimal("188")
    runtime.state.sell_proceeds = Decimal("0")
    runtime.state.cost_known = True
    runtime.state.adverse_entry_latched = True

    status = runtime.status()

    assert status["cost"] == "188"
    assert status["cost_known"] is True
    assert status["sell_proceeds"] == "0"
    assert status["minimum_profit_price"] == str(minimum_profit_price(runtime.config, runtime.state))
    assert status["adverse_entry_latched"] is True


def test_status_selects_latest_entry_across_episode_revision_resets():
    from controllers.directional_trading.ok_rsi_core.episode import OrderProgress, OrderRequest

    controller, _ = setup_controller()
    runtime = controller._episode_runtime

    def request(identifier, episode, revision, expires_at, price):
        return OrderRequest(
            id=identifier,
            episode_id=episode,
            level=0,
            side="BUY",
            amount=Decimal("1"),
            price=price,
            reserved_quote=price,
            expires_at=expires_at,
            revision=revision,
            intent="SCOUT_BUY",
            pricing_policy="TOP_OF_BOOK",
            reference_price=price,
        )

    old = request("old", "episode-1", 4, 10_000, Decimal("100"))
    newest = request("new", "episode-2", 1, 20_000, Decimal("90"))
    runtime.state.orders = {old.id: OrderProgress(old), newest.id: OrderProgress(newest)}

    status = runtime.status()

    assert status["last_entry_selected_price"] == "90"


def test_direct_factory_cannot_change_an_authorized_quantity():
    controller, _ = setup_controller()
    created = actions(controller)[0]
    with pytest.raises(ValueError, match="authorization"):
        controller.get_executor_class_for_config(created.executor_config.model_copy(update={"amount": Decimal("100")}))


def test_direct_factory_rejects_changed_execution_intent():
    controller, _ = setup_controller()
    created = actions(controller)[0]
    with pytest.raises(ValueError, match="authorization"):
        controller.get_executor_class_for_config(
            created.executor_config.model_copy(update={"episode_intent": "TERMINAL_PROFIT_EXIT"})
        )


def test_direct_factory_rejects_changed_rebound_setup_receipt():
    controller, _ = setup_controller(episode_scout_rebound_confirmation=True)
    assert actions(controller) == []
    controller.processed_data = {"signal": 0, "signal_state": {"condition_ok": True}}
    controller.market_data_provider.time.return_value = 10080
    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=context() | {"timestamp": 10080, "low": Decimal("93.1")},
    ):
        assert controller.determine_executor_actions() == []
    controller.market_data_provider.time.return_value = 10140
    with patch(
        "controllers.directional_trading.ok_rsi_core.runtime.completed_market_context",
        return_value=context() | {"timestamp": 10140, "low": Decimal("93.1")},
    ):
        created = controller.determine_executor_actions()[0]

    with pytest.raises(ValueError, match="authorization"):
        controller.get_executor_class_for_config(
            created.executor_config.model_copy(update={"episode_setup_low": Decimal("1")})
        )


@pytest.mark.parametrize("enabled", [False, True])
def test_framework_config_loader_resolves_public_controller_after_config_extraction(enabled):
    from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
    config = OKRSIControllerConfig(id="loader-regression", strict_hmm_mode=False, episode_enabled=enabled)
    assert config.get_controller_class() is OKRSIController


@pytest.mark.parametrize("error", ["unresolved_native_order:order-1", "incomplete_fill_records"])
def test_transient_native_receipt_recovers_without_restart_and_retains_reservation(error):
    controller, _ = setup_controller()
    created = actions(controller)[0]
    native = SimpleNamespace(
        id=created.executor_config.id, connector_name="okx", trading_pair="ETH-USDT",
        is_active=True, is_done=False, custom_info={"episode_receipt": dict(
            gross_base="0", gross_quote="0", base_fee="0", quote_fee="0", final=False,
            fees_known=False, errors=[error])})
    controller.executors_info = [native]
    assert actions(controller) == []
    runtime = controller._episode_runtime
    assert runtime.hold_reason is None
    assert runtime.reason.startswith("reconciliation_pending:")
    assert runtime.state.orders[native.id].reserved_quote > 0
    native.is_active, native.is_done = False, True
    native.custom_info["episode_receipt"] = dict(gross_base=".4", gross_quote="37.6", base_fee=".0004",
                                                 quote_fee="0", final=True, fees_known=True, errors=[])
    assert actions(controller) == []
    assert runtime.state.base == Decimal(".3996")
    assert runtime.state.orders[native.id].done
