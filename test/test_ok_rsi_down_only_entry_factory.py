import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from controllers.directional_trading import hl_rsi
from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import (
    DownOnlyMakerPositionExecutor,
    DownOnlyMakerPositionExecutorConfig,
)
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy_v2.executors.order_executor.data_types import OrderExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction


def make_controller(*, policy="down_only_maker_v1"):
    provider = MagicMock()
    provider.initialize_rate_sources.return_value = None
    provider.time.return_value = 1_000.0
    prices = {
        PriceType.MidPrice: Decimal("100.005"),
        PriceType.BestBid: Decimal("100"),
        PriceType.BestAsk: Decimal("100.01"),
    }
    provider.get_price_by_type.side_effect = lambda connector, pair, price_type: prices[price_type]
    provider.get_available_balance.return_value = Decimal("1000")
    provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
    provider.get_trading_rules.return_value = TradingRule(
        "ETH-USDC",
        min_order_size=Decimal("0.01"),
        min_price_increment=Decimal("0.01"),
        min_base_amount_increment=Decimal("0.01"),
        min_notional_size=Decimal("1"),
    )
    config_values = {
        "id": "ok-rsi-active",
        "connector_name": "okx",
        "trading_pair": "ETH-USDC",
        "trade_mode": "buy_sell",
        "strict_hmm_mode": False,
        "use_adaptive_trailing": False,
        "sell_only_if_profitable": False,
    }
    if policy == "shadow":
        config_values["entry_acquisition_shadow_policy"] = "down_only_maker_v1"
    else:
        config_values["entry_acquisition_policy"] = policy
    controller = OKRSIController(
        config=OKRSIControllerConfig(**config_values),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    controller.positions_held = []
    controller.executors_info = []
    controller.processed_data = {"signal": 1, "signal_state": {"condition_ok": True}}
    controller._latest_atr_pct = Decimal("0.01")
    controller._resolve_buy_entry_plan = MagicMock(return_value=(
        Decimal("30"), Decimal("30"), Decimal("99.99"), Decimal("0.3"), False,
    ))
    return controller, provider, prices


def buy_config(controller):
    return controller.get_executor_config(TradeType.BUY, Decimal("100"), Decimal("0.3"))


def test_active_buy_freezes_exact_down_only_envelope() -> None:
    controller, _, _ = make_controller()
    config = buy_config(controller)
    assert isinstance(config, DownOnlyMakerPositionExecutorConfig)
    assert isinstance(config.id, str) and config.id
    assert config.side is TradeType.BUY
    assert config.leverage == 1
    assert config.triple_barrier_config.open_order_type is OrderType.LIMIT_MAKER
    assert config.entry_order_policy == "single_attempt"
    assert config.activation_bounds == [Decimal("0.0005")]
    assert config.entry_activation_bound_up_pct == Decimal("0.0005")
    assert config.entry_upward_bound_hysteresis_seconds == 0
    assert config.triple_barrier_config.open_order_limit_max_age_seconds == 0
    assert config.entry_total_max_age_seconds == 300
    assert config.entry_max_downward_reprices == 3
    assert config.decision_atr_1m == config.decision_best_bid * controller._latest_atr_pct


def test_eth_yaml_factory_freezes_split_up_wait_and_two_candle_hysteresis() -> None:
    controller, _, _ = make_controller()
    controller.config.entry_activation_bound_up_pct = Decimal("0.001")
    controller.config.entry_upward_bound_hysteresis_candles = 2
    config = buy_config(controller)
    assert config.activation_bounds == [Decimal("0.0005")]
    assert config.entry_activation_bound_up_pct == Decimal("0.001")
    assert config.entry_upward_bound_hysteresis_seconds == 120


def test_entry_validity_is_independent_of_adaptive_entry_spacing() -> None:
    controller, _, _ = make_controller()
    controller.config.use_adaptive_filters = True
    controller.config.min_executor_price_gap_threshold = Decimal("0.005")
    controller.config.max_executor_price_gap_threshold = Decimal("0.018")
    controller.config.price_gap_atr_multiplier = 3.0
    controller._latest_atr_pct = Decimal("0.000283")

    config = buy_config(controller)

    assert config.activation_bounds == [Decimal("0.0005")]


@pytest.mark.parametrize("policy", ["legacy_single_attempt", "shadow"])
def test_legacy_and_shadow_buy_keep_native_position_executor_config(policy) -> None:
    controller, _, _ = make_controller(policy=policy)
    config = buy_config(controller)
    assert type(config) is PositionExecutorConfig
    assert config.entry_order_policy == "single_attempt"
    assert config.triple_barrier_config.open_order_limit_max_age_seconds == 30


def test_active_policy_does_not_change_sell_executor() -> None:
    controller, _, _ = make_controller()
    controller._get_sellable_inventory_amount = MagicMock(return_value=Decimal("1"))
    controller._resolve_recovery_sell_amount = MagicMock(return_value=Decimal("0.2"))
    controller._shade_limit_maker_price = MagicMock(return_value=Decimal("101"))
    config = controller.get_executor_config(TradeType.SELL, Decimal("100"), Decimal("0.2"))
    assert type(config) is OrderExecutorConfig


@pytest.mark.parametrize(
    "field,value",
    [
        ("controller_id", "other"),
        ("connector_name", "binance"),
        ("trading_pair", "BTC-USDC"),
        ("side", TradeType.SELL),
        ("leverage", 2),
        ("amount", Decimal("0.4")),
        ("entry_price", Decimal("99.98")),
        ("id", "other-intent"),
        ("decision_best_bid", Decimal("99")),
        ("decision_best_ask", Decimal("101")),
        ("decision_atr_1m", Decimal("2")),
        ("decision_price_step", Decimal("0.1")),
        ("maximum_acceptable_price", Decimal("99.98")),
        ("entry_acquisition_policy", "legacy_single_attempt"),
        ("entry_total_max_age_seconds", 301),
        ("entry_reprice_cooldown_seconds", 6),
        ("entry_max_downward_reprices", 4),
        ("entry_reprice_min_ticks", 4),
        ("entry_reprice_atr_fraction", Decimal("0.06")),
        ("activation_bounds", [Decimal("0.01")]),
        ("entry_activation_bound_up_pct", Decimal("0.01")),
        ("entry_upward_bound_hysteresis_seconds", 60),
    ],
)
def test_factory_rejects_tampered_authorization(field, value) -> None:
    controller, _, _ = make_controller()
    config = buy_config(controller)
    tampered = config.model_copy(deep=True, update={field: value})
    with pytest.raises(ValueError, match="authorization"):
        controller.get_executor_class_for_config(tampered)


def test_factory_rejects_tampered_order_type() -> None:
    controller, _, _ = make_controller()
    config = buy_config(controller)
    tampered = config.model_copy(deep=True)
    tampered.triple_barrier_config.open_order_type = OrderType.LIMIT
    with pytest.raises(ValueError, match="authorization"):
        controller.get_executor_class_for_config(tampered)


def test_factory_claims_authorized_config_once() -> None:
    controller, _, _ = make_controller()
    config = buy_config(controller)
    assert controller.get_executor_class_for_config(config) is DownOnlyMakerPositionExecutor
    with pytest.raises(ValueError, match="already claimed"):
        controller.get_executor_class_for_config(config)


def active_info(config):
    return SimpleNamespace(
        id=config.id,
        config=config,
        is_active=True,
        is_done=False,
        side=TradeType.BUY,
        connector_name=config.connector_name,
        trading_pair=config.trading_pair,
    )


KEEP_POSITION_STOP_CONDITIONS = ("opposing", "kill", "unavailable")


@pytest.mark.parametrize("condition", [*KEEP_POSITION_STOP_CONDITIONS, "pause"])
def test_active_entry_invalidation_emits_acquisition_only_stop(condition) -> None:
    """Kill, opposing signal, and unusable book stop only the entry acquisition.

    The filled part keeps its trailing exit (never POSITION_HOLD). Operator entry
    pause emits nothing; unfilled cancel is the executor's job.
    """
    controller, _, prices = make_controller()
    config = buy_config(controller)
    controller.executors_info = [active_info(config)]
    if condition == "opposing":
        controller.processed_data["signal"] = -1
    elif condition == "kill":
        controller.config.manual_kill_switch = True
    elif condition == "pause":
        controller._operator_entry_paused = True
    else:
        prices[PriceType.BestAsk] = None
    with patch.object(hl_rsi.HLRSIController, "stop_actions_proposal", return_value=[]):
        actions = controller.stop_actions_proposal()
    if condition == "pause":
        assert actions == []
        return
    assert actions == [StopExecutorAction(
        controller_id=controller.config.id,
        executor_id=config.id,
        keep_position=True,
        acquisition_only=True,
    )]


@pytest.mark.parametrize("state", ["managing_fill", "ended_no_fill", "held_residual"])
def test_invalidation_leaves_finished_acquisitions_alone(state) -> None:
    controller, _, _ = make_controller()
    config = buy_config(controller)
    info = active_info(config)
    info.custom_info = {"entry_acquisition": {"state": state, "terminal_reason": None}}
    controller.executors_info = [info]
    controller.processed_data["signal"] = -1
    with patch.object(hl_rsi.HLRSIController, "stop_actions_proposal", return_value=[]):
        assert controller.stop_actions_proposal() == []


def test_operator_pause_does_not_stop_a_filled_trailing_owner_and_refuses_new_buy() -> None:
    controller, _, _ = make_controller()
    config = buy_config(controller)
    filled = active_info(config)
    filled.filled_amount_quote = Decimal("100")
    filled.custom_info = {"open_order_filled_amount_base": Decimal("1"), "trailing_state": "armed"}
    controller.executors_info = [filled]
    controller._operator_entry_paused = True
    with patch.object(hl_rsi.HLRSIController, "stop_actions_proposal", return_value=[]):
        assert controller.stop_actions_proposal() == []
    paused, _, _ = make_controller()
    paused._operator_entry_paused = True
    assert paused.can_create_executor(1) is False

    controller._operator_entry_paused = False
    controller.config.manual_kill_switch = True
    with patch.object(hl_rsi.HLRSIController, "stop_actions_proposal", return_value=[]):
        kill_actions = controller.stop_actions_proposal()
    assert kill_actions == [StopExecutorAction(
        controller_id=controller.config.id,
        executor_id=config.id,
        keep_position=True,
        acquisition_only=True,
    )]


def test_neutral_signal_does_not_stop_active_entry() -> None:
    controller, _, _ = make_controller()
    config = buy_config(controller)
    controller.executors_info = [active_info(config)]
    controller.processed_data["signal"] = 0
    with patch.object(hl_rsi.HLRSIController, "stop_actions_proposal", return_value=[]):
        assert controller.stop_actions_proposal() == []
