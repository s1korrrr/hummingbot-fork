from __future__ import annotations

import asyncio
import copy
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping
from unittest.mock import MagicMock, patch

from controllers.directional_trading.rsi_v5 import RecoveryTrailState, RSIv5Controller, RSIv5ControllerConfig
from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import TrackedOrder

SCENARIO_STEPS = {
    "trailing_full_close": [
        "activation_equal",
        "activation_cross",
        "new_peak",
        "trigger_equal",
        "trigger_cross",
    ],
    "hard_stop_no_preserve": ["above_stop", "at_stop"],
    "signal_recovery_full": ["sell_signal"],
    "signal_recovery_capped": ["sell_signal"],
    "recovery_partial": ["pullback"],
    "signal_quote_slice": ["sell_signal"],
}


class ExitLifecycleMarketDataProvider:
    def __init__(self, market_price: Decimal, available_base: Decimal) -> None:
        self.market_price = market_price
        self.available_base = available_base
        self.now = 1_700_000_000.0
        self.trading_rule = SimpleNamespace(
            min_price_increment=None,
            min_order_size=Decimal("0.00000001"),
            min_base_amount_increment=Decimal("0.00000001"),
            min_notional_size=Decimal("0"),
            min_order_value=Decimal("0"),
        )

    def initialize_rate_sources(self, _pairs) -> None:
        return None

    def time(self) -> float:
        return self.now

    def get_price_by_type(self, _connector: str, _pair: str, _price_type: PriceType) -> Decimal:
        return self.market_price

    def get_balance(self, _connector: str, asset: str) -> Decimal:
        return self.available_base if asset == "BNB" else Decimal("1000")

    def get_available_balance(self, _connector: str, asset: str) -> Decimal:
        return self.get_balance(_connector, asset)

    def get_trading_rules(self, _connector: str, _pair: str):
        return self.trading_rule

    def quantize_order_amount(self, _connector: str, _pair: str, amount: Decimal) -> Decimal:
        return amount.quantize(self.trading_rule.min_base_amount_increment)


def observe_q2_exit_lifecycle_scenarios(request: Mapping[str, Any] | str | Path) -> list[dict[str, Any]]:
    """Return raw, unhashed Q2 exit-lifecycle action/state observations."""

    request_value = _load_request(request)
    _validate_request_shape(request_value)
    profile = request_value["profile"]
    observed = []
    for scenario in request_value["scenarios"]:
        scenario_profile = copy.deepcopy(profile)
        scenario_profile.update(scenario.get("profile_overrides", {}))
        scenario_id = scenario["scenario_id"]
        if scenario_id == "trailing_full_close":
            steps = _observe_trailing(scenario_profile, scenario["steps"])
        elif scenario_id == "hard_stop_no_preserve":
            steps = _observe_hard_stop(scenario_profile, scenario["steps"])
        else:
            steps = _observe_recovery(scenario_profile, scenario["steps"])
        observed.append({"scenario_id": scenario_id, "steps": steps})
    return observed


def _load_request(request: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(request, Mapping):
        return copy.deepcopy(dict(request))
    return json.loads(Path(request).read_text(encoding="utf-8"))


def _validate_request_shape(request: Mapping[str, Any]) -> None:
    profile = request.get("profile")
    if not isinstance(profile, Mapping):
        raise ValueError("Q2 exit lifecycle request must contain a profile")
    required_profile = {
        "connector",
        "trading_pair",
        "entry_price",
        "position_amount",
        "trailing_activation",
        "trailing_delta",
        "trailing_order_type",
        "hard_stop_fraction",
        "early_stop_keep_position",
        "recovery_partial_fraction",
        "signal_sell_inventory_slice_multiplier",
        "usd_per_entry",
        "sell_price",
    }
    if not required_profile.issubset(profile):
        raise ValueError("Q2 exit lifecycle profile is incomplete")
    scenarios = request.get("scenarios")
    if not isinstance(scenarios, list) or [item.get("scenario_id") for item in scenarios] != list(SCENARIO_STEPS):
        raise ValueError(f"Q2 exit lifecycle request requires ordered scenarios: {list(SCENARIO_STEPS)}")
    for scenario in scenarios:
        steps = scenario.get("steps")
        expected_steps = SCENARIO_STEPS[scenario["scenario_id"]]
        if not isinstance(steps, list) or [step.get("step_id") for step in steps] != expected_steps:
            raise ValueError(f"Q2 exit lifecycle scenario {scenario['scenario_id']} has invalid ordered steps")
        if any(not isinstance(step.get("input"), Mapping) for step in steps):
            raise ValueError(f"Q2 exit lifecycle scenario {scenario['scenario_id']} requires primitive step inputs")


def _controller_config(profile: Mapping[str, Any], **overrides: Any) -> RSIv5ControllerConfig:
    values = {
        "id": "rsi-v5-q2-exit-lifecycle",
        "connector_name": profile["connector"],
        "trading_pair": profile["trading_pair"],
        "usd_per_entry": Decimal(profile["usd_per_entry"]),
        "use_bag_freeze": False,
        "use_adaptive_filters": False,
        "trend_confirmation_enabled": False,
        "executor_price_gap_threshold": Decimal("0"),
        "recovery_partial_exit_fraction": float(profile["recovery_partial_fraction"]),
        "signal_sell_inventory_slice_multiplier": Decimal(profile["signal_sell_inventory_slice_multiplier"]),
    }
    values.update(overrides)
    return RSIv5ControllerConfig(**values)


def _observe_trailing(profile: Mapping[str, Any], request_steps: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    entry_price = Decimal(profile["entry_price"])
    remaining_amount = Decimal(profile["position_amount"])
    provider = ExitLifecycleMarketDataProvider(entry_price, remaining_amount)
    controller = RSIv5Controller(
        config=_controller_config(
            profile,
            use_trailing_exit=True,
            use_adaptive_trailing=False,
            trailing_activation_pct=float(profile["trailing_activation"]),
            trailing_delta_pct=float(profile["trailing_delta"]),
            trailing_stop_order_type=profile["trailing_order_type"],
        ),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    executor_config = controller.get_executor_config(TradeType.BUY, entry_price, remaining_amount)
    executor_config.amount = remaining_amount

    strategy = MagicMock(spec=StrategyV2Base)
    connector = MagicMock()
    connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
    strategy.connectors = {profile["connector"]: connector}
    strategy.current_timestamp = provider.now
    strategy.sell.return_value = "q2-trailing-close-intent"
    trading_rule = SimpleNamespace(
        min_price_increment=Decimal("0.01"),
        min_order_size=Decimal("0.00000001"),
        min_notional_size=Decimal("0"),
    )
    with patch.object(PositionExecutor, "get_trading_rules", return_value=trading_rule):
        executor = PositionExecutor(strategy=strategy, config=executor_config)
    executor._status = RunnableStatus.RUNNING
    executor.get_price = MagicMock(return_value=entry_price)
    open_order = MagicMock()
    open_order.order_fills = {}
    open_order.executed_amount_base = remaining_amount
    open_order.average_executed_price = entry_price
    open_order.price = entry_price
    open_order.cumulative_fee_paid.return_value = Decimal("0")
    open_order.is_done = True
    open_order.is_open = False
    executor._open_order = TrackedOrder(order_id="q2-entry-filled")
    executor._open_order.order = open_order

    observed = []
    for request_step in request_steps:
        before_trigger = executor._trailing_stop_trigger_pct
        before_moves = executor._trailing_stop_move_count
        before_close_calls = strategy.sell.call_count
        net_pnl_pct = Decimal(request_step["input"]["net_pnl_pct"])
        executor.get_net_pnl_pct = MagicMock(return_value=net_pnl_pct)
        executor.control_trailing_stop()
        after_trigger = executor._trailing_stop_trigger_pct
        if strategy.sell.call_count > before_close_calls:
            reason = "trailing_close"
        elif before_trigger is None and after_trigger is None:
            reason = "activation_not_exceeded"
        elif before_trigger is None and after_trigger is not None:
            reason = "trailing_armed"
        elif executor._trailing_stop_move_count > before_moves:
            reason = "new_peak"
        else:
            reason = "trigger_not_crossed"

        actions = []
        if strategy.sell.call_count > before_close_calls:
            call = strategy.sell.call_args
            actions.append(
                _action(
                    kind="CLOSE_POSITION",
                    side="SELL",
                    amount=call.args[2],
                    order_type=call.args[3].name,
                )
            )
        peak_pct = None
        if after_trigger is not None:
            peak_pct = after_trigger + executor_config.triple_barrier_config.trailing_stop.trailing_delta
        observed.append(
            _step(
                request_step,
                owner="POSITION_EXECUTOR",
                reason=reason,
                actions=actions,
                trailing_armed=after_trigger is not None,
                peak_net_pnl_pct=peak_pct,
                trigger_net_pnl_pct=after_trigger,
                remaining_amount=remaining_amount,
            )
        )
        strategy.current_timestamp += 1
    return observed


def _observe_hard_stop(profile: Mapping[str, Any], request_steps: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    entry_price = Decimal(profile["entry_price"])
    position_amount = Decimal(profile["position_amount"])
    provider = ExitLifecycleMarketDataProvider(entry_price, position_amount)
    controller = RSIv5Controller(
        config=_controller_config(
            profile,
            early_stop_drawdown_pct=Decimal(profile["hard_stop_fraction"]),
            early_stop_keep_position=bool(profile["early_stop_keep_position"]),
        ),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    controller.executors_info = [
        SimpleNamespace(
            id="q2-hard-stop-active-buy",
            is_active=True,
            connector_name=profile["connector"],
            trading_pair=profile["trading_pair"],
            side=TradeType.BUY,
            timestamp=provider.now - 60,
            config=SimpleNamespace(entry_price=entry_price),
            custom_info={},
        )
    ]
    observed = []
    for request_step in request_steps:
        market_price = Decimal(request_step["input"]["market_price"])
        actions = controller._check_active_executor_drawdown_stop(market_price)
        normalized_actions = [
            _action(kind="STOP_EXECUTOR", keep_position=action.keep_position)
            for action in actions
            if isinstance(action, StopExecutorAction)
        ]
        observed.append(
            _step(
                request_step,
                owner="RSI_V5_CONTROLLER",
                reason="hard_stop" if normalized_actions else "threshold_not_reached",
                actions=normalized_actions,
                remaining_amount=position_amount,
            )
        )
    return observed


def _observe_recovery(profile: Mapping[str, Any], request_steps: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    request_step = request_steps[0]
    event = request_step["input"]
    held_amount = Decimal(event["held_base"])
    available_base = Decimal(event["available_base"])
    current_signal = int(event["signal"])
    sell_price = Decimal(profile["sell_price"])
    provider = ExitLifecycleMarketDataProvider(sell_price, available_base)
    controller = RSIv5Controller(
        config=_controller_config(profile, early_stop_drawdown_pct=Decimal("0")),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    position = SimpleNamespace(
        id="q2-held-position",
        connector_name=profile["connector"],
        trading_pair=profile["trading_pair"],
        side=TradeType.BUY,
        amount=held_amount,
        breakeven_price=Decimal(profile["entry_price"]),
        cum_fees_quote=Decimal("0"),
    )
    position_key = controller._recovery_position_key(position)
    peak_price = sell_price
    if current_signal == 0:
        pullback = Decimal(str(controller.config.recovery_trail_pullback_pct))
        peak_price = sell_price / (Decimal("1") - pullback)
    controller.positions_held = [position]
    controller.executors_info = []
    controller._recovery_trails[position_key] = RecoveryTrailState(
        position_key=position_key,
        armed_timestamp=provider.now - 60,
        arm_price=sell_price,
        peak_price=peak_price,
        tracked_amount=held_amount,
        peak_rsi=82.0,
        target_profit_pct=Decimal(str(controller.config.bag_recovery_target_pct)),
        last_reason="armed",
    )
    controller.processed_data = {
        "signal": current_signal,
        "signal_state": {"rsi": 82.0, "rsi_prev": 82.0, "ema_fast": float(sell_price - Decimal("1"))},
        "trend_confirmation": {
            "direction": "neutral",
            "confidence": 0.0,
            "alignment_score": 0.0,
            "states": {},
        },
    }

    if current_signal < 0:
        direct_actions = controller.create_actions_proposal()
        if direct_actions or controller._last_gate_reason[TradeType.SELL] != "held_bag_recovery":
            raise AssertionError("held-bag recovery must remain the sole accepted SELL owner")
    recovery_actions = controller.stop_actions_proposal()
    normalized_actions = [
        _action(
            kind="CREATE_RECOVERY_EXIT",
            side=action.executor_config.side.name,
            amount=action.executor_config.amount,
            order_type=action.executor_config.execution_strategy.name,
        )
        for action in recovery_actions
        if isinstance(action, CreateExecutorAction)
    ]
    if current_signal == 0:
        reason = "recovery_partial_exit"
    elif Decimal(profile["signal_sell_inventory_slice_multiplier"]) > 0:
        reason = "signal_quote_slice"
    elif available_base < held_amount:
        reason = "signal_capped_exit"
    else:
        reason = "signal_full_exit"
    return [
        _step(
            request_step,
            owner="HELD_BAG_RECOVERY",
            reason=reason,
            actions=normalized_actions,
            remaining_amount=held_amount,
        )
    ]


def _action(
    *,
    kind: str,
    side: str | None = None,
    amount: Decimal | None = None,
    order_type: str | None = None,
    keep_position: bool | None = None,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "side": side,
        "amount": _decimal(amount),
        "order_type": order_type,
        "keep_position": keep_position,
    }


def _step(
    request_step: Mapping[str, Any],
    *,
    owner: str,
    reason: str,
    actions: list[dict[str, Any]],
    trailing_armed: bool = False,
    peak_net_pnl_pct: Decimal | None = None,
    trigger_net_pnl_pct: Decimal | None = None,
    remaining_amount: Decimal,
) -> dict[str, Any]:
    return {
        "step_id": request_step["step_id"],
        "decision": {"owner": owner, "reason": reason},
        "actions": actions,
        "post_state": {
            "trailing_armed": trailing_armed,
            "peak_net_pnl_pct": _decimal(peak_net_pnl_pct),
            "trigger_net_pnl_pct": _decimal(trigger_net_pnl_pct),
            "remaining_amount": _decimal(remaining_amount),
        },
    }


def _decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(Decimal(value).normalize(), "f")
