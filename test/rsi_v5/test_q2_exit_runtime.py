from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from controllers.directional_trading.rsi_v5 import RecoveryTrailState, RSIv5Controller, RSIv5ControllerConfig
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class ExitMarketDataProvider:
    def __init__(self, mid_price: Decimal, base_available: Decimal = Decimal("1")) -> None:
        self.mid_price = mid_price
        self.base_available = base_available
        self.now = 1_700_000_000.0
        self.trading_rule = SimpleNamespace(
            min_price_increment=Decimal("0.01"),
            min_order_size=Decimal("0.00000001"),
            min_base_amount_increment=Decimal("0.00000001"),
            min_notional_size=Decimal("0"),
            min_order_value=Decimal("0"),
        )

    def initialize_rate_sources(self, _pairs) -> None:
        return None

    def time(self) -> float:
        return self.now

    def get_price_by_type(self, _connector: str, _pair: str, price_type: PriceType) -> Decimal:
        if price_type == PriceType.BestBid:
            return self.mid_price - Decimal("0.01")
        if price_type == PriceType.BestAsk:
            return self.mid_price + Decimal("0.01")
        return self.mid_price

    def get_balance(self, _connector: str, asset: str) -> Decimal:
        return self.base_available if asset == "BNB" else Decimal("1000")

    def get_available_balance(self, _connector: str, asset: str) -> Decimal:
        return self.get_balance(_connector, asset)

    def get_trading_rules(self, _connector: str, _pair: str):
        return self.trading_rule

    def quantize_order_amount(self, _connector: str, _pair: str, amount: Decimal) -> Decimal:
        return amount.quantize(self.trading_rule.min_base_amount_increment)


def _recovery_controller(
    *,
    current_price: Decimal,
    peak_price: Decimal,
    current_signal: int,
    base_available: Decimal = Decimal("1"),
    signal_sell_inventory_slice_multiplier: Decimal = Decimal("0"),
    usd_per_entry: Decimal = Decimal("30"),
    recovery_sell_execution_strategy: str = "LIMIT_CHASER",
) -> RSIv5Controller:
    provider = ExitMarketDataProvider(current_price, base_available=base_available)
    config = RSIv5ControllerConfig(
        id="rsi-v5-q2-exit-runtime",
        connector_name="hyperliquid_perpetual",
        trading_pair="BNB-USDC",
        use_bag_freeze=False,
        early_stop_drawdown_pct=Decimal("0"),
        bag_recovery_target_pct=Decimal("0.05"),
        recovery_partial_exit_fraction=0.5,
        recovery_trail_pullback_pct=0.015,
        recovery_rsi_rollover_delta=2.0,
        executor_price_gap_threshold=Decimal("0"),
        use_adaptive_filters=False,
        trend_confirmation_enabled=False,
        signal_sell_inventory_slice_multiplier=signal_sell_inventory_slice_multiplier,
        usd_per_entry=usd_per_entry,
        recovery_sell_execution_strategy=recovery_sell_execution_strategy,
    )
    controller = RSIv5Controller(
        config=config,
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    position = SimpleNamespace(
        id="q2-held-bag",
        connector_name=config.connector_name,
        trading_pair=config.trading_pair,
        side=TradeType.BUY,
        amount=Decimal("1"),
        breakeven_price=Decimal("100"),
        cum_fees_quote=Decimal("0"),
    )
    position_key = controller._recovery_position_key(position)
    controller.positions_held = [position]
    controller.executors_info = []
    controller._recovery_trails[position_key] = RecoveryTrailState(
        position_key=position_key,
        armed_timestamp=provider.now - 60,
        arm_price=Decimal("105"),
        peak_price=peak_price,
        tracked_amount=position.amount,
        peak_rsi=82.0,
        target_profit_pct=Decimal("0.05"),
        last_reason="armed",
    )
    controller.processed_data = {
        "signal": current_signal,
        "signal_state": {
            "rsi": 82.0,
            "rsi_prev": 82.0,
            "ema_fast": float(current_price - Decimal("1")),
        },
        "trend_confirmation": {
            "direction": "neutral",
            "confidence": 0.0,
            "alignment_score": 0.0,
            "states": {},
        },
    }
    return controller


def test_q2_accepted_sell_signal_uses_recovery_owner_for_full_position_exit() -> None:
    controller = _recovery_controller(
        current_price=Decimal("106"),
        peak_price=Decimal("106"),
        current_signal=-1,
    )

    direct_actions = controller.create_actions_proposal()
    combined_actions = controller.determine_executor_actions()

    assert controller.config.signal_sell_inventory_slice_multiplier == Decimal("0")
    assert direct_actions == []
    assert controller._last_gate_reason[TradeType.SELL] == "held_bag_recovery"
    assert len(combined_actions) == 1
    assert isinstance(combined_actions[0], CreateExecutorAction)
    assert combined_actions[0].executor_config.level_id.startswith("recovery_exit:")
    assert combined_actions[0].executor_config.amount == Decimal("1.00000000")


def test_q2_sell_signal_recovery_exit_caps_full_position_to_available_base() -> None:
    controller = _recovery_controller(
        current_price=Decimal("106"),
        peak_price=Decimal("106"),
        current_signal=-1,
        base_available=Decimal("0.75"),
    )

    direct_actions = controller.create_actions_proposal()
    actions = controller.stop_actions_proposal()

    assert direct_actions == []
    assert controller._last_gate_reason[TradeType.SELL] == "held_bag_recovery"
    assert len(actions) == 1
    assert actions[0].executor_config.amount == Decimal("0.75000000")


def test_nonzero_signal_sell_slice_multiplier_remains_effective_for_recovery_owned_exit() -> None:
    controller = _recovery_controller(
        current_price=Decimal("106"),
        peak_price=Decimal("106"),
        current_signal=-1,
        signal_sell_inventory_slice_multiplier=Decimal("1"),
        usd_per_entry=Decimal("53"),
        recovery_sell_execution_strategy="MARKET",
    )

    actions = controller.stop_actions_proposal()

    assert controller.config.signal_sell_inventory_slice_multiplier == Decimal("1")
    assert len(actions) == 1
    assert actions[0].executor_config.amount == Decimal("0.50000000")


def test_q2_pullback_without_sell_signal_keeps_configured_partial_recovery_slice() -> None:
    controller = _recovery_controller(
        current_price=Decimal("105"),
        peak_price=Decimal("107"),
        current_signal=0,
    )

    actions = controller.determine_executor_actions()

    assert len(actions) == 1
    assert isinstance(actions[0], CreateExecutorAction)
    assert actions[0].executor_config.level_id.startswith("recovery_exit:")
    assert actions[0].executor_config.amount == Decimal("0.50000000")


def test_q2_hard_stop_requests_non_preserving_stop_at_exact_eight_percent_drawdown() -> None:
    provider = ExitMarketDataProvider(Decimal("92"))
    controller = RSIv5Controller(
        config=RSIv5ControllerConfig(
            id="rsi-v5-q2-hard-stop",
            connector_name="hyperliquid_perpetual",
            trading_pair="BNB-USDC",
            use_bag_freeze=False,
            early_stop_drawdown_pct=Decimal("0.08"),
            early_stop_keep_position=False,
        ),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    controller.executors_info = [
        SimpleNamespace(
            id="q2-active-buy",
            is_active=True,
            connector_name=controller.config.connector_name,
            trading_pair=controller.config.trading_pair,
            side=TradeType.BUY,
            timestamp=provider.now - 60,
            config=SimpleNamespace(entry_price=Decimal("100")),
            custom_info={},
        )
    ]

    before = controller._check_active_executor_drawdown_stop(Decimal("92.00000001"))
    at_boundary = controller._check_active_executor_drawdown_stop(Decimal("92"))

    assert before == []
    assert at_boundary == [
        StopExecutorAction(
            executor_id="q2-active-buy",
            controller_id=controller.config.id,
            keep_position=False,
        )
    ]


def test_q2_position_executor_trailing_boundaries_close_full_remaining_amount() -> None:
    provider = ExitMarketDataProvider(Decimal("100"))
    controller = RSIv5Controller(
        config=RSIv5ControllerConfig(
            id="rsi-v5-q2-trailing",
            connector_name="hyperliquid_perpetual",
            trading_pair="BNB-USDC",
            usd_per_entry=Decimal("100"),
            executor_price_gap_threshold=Decimal("0"),
            use_adaptive_filters=False,
            use_adaptive_trailing=False,
            use_trailing_exit=True,
            trailing_activation_pct=0.047,
            trailing_delta_pct=0.034,
            trailing_stop_order_type=OrderType.LIMIT,
            trailing_stop_limit_max_age_seconds=3,
        ),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    config = controller.get_executor_config(
        trade_type=TradeType.BUY,
        price=Decimal("100"),
        amount=Decimal("1"),
    )
    assert config.connector_name == "hyperliquid_perpetual"
    assert config.triple_barrier_config.trailing_stop.activation_price == Decimal("0.047")
    assert config.triple_barrier_config.trailing_stop.trailing_delta == Decimal("0.034")
    assert config.triple_barrier_config.trailing_stop_order_type == OrderType.LIMIT
    assert config.triple_barrier_config.trailing_stop_limit_max_age_seconds == 3

    strategy = MagicMock(spec=StrategyV2Base)
    connector = MagicMock()
    connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
    strategy.connectors = {"hyperliquid_perpetual": connector}
    strategy.current_timestamp = 1_700_000_000.0
    strategy.sell.return_value = "q2-trailing-close"
    trading_rule = SimpleNamespace(
        min_order_size=Decimal("0.00000001"),
        min_notional_size=Decimal("0"),
    )
    with patch.object(PositionExecutor, "get_trading_rules", return_value=trading_rule):
        executor = PositionExecutor(strategy=strategy, config=config)
    executor._status = RunnableStatus.RUNNING
    executor.get_price = MagicMock(return_value=Decimal("100"))
    open_order = MagicMock()
    open_order.order_fills = {}
    open_order.executed_amount_base = config.amount
    open_order.average_executed_price = config.entry_price
    open_order.price = config.entry_price
    open_order.cumulative_fee_paid.return_value = Decimal("0")
    open_order.is_done = True
    open_order.is_open = False
    executor._open_order = TrackedOrder(order_id="q2-entry-filled")
    executor._open_order.order = open_order

    executor.get_net_pnl_pct = MagicMock(return_value=Decimal("0.047"))
    executor.control_trailing_stop()
    assert executor._trailing_stop_trigger_pct is None

    strategy.current_timestamp += 1
    executor.get_net_pnl_pct.return_value = Decimal("0.047001")
    executor.control_trailing_stop()
    assert executor._trailing_stop_trigger_pct == Decimal("0.013001")
    assert executor._trailing_stop_move_count == 0

    strategy.current_timestamp += 1
    executor.get_net_pnl_pct.return_value = Decimal("0.08")
    executor.control_trailing_stop()
    assert executor._trailing_stop_trigger_pct == Decimal("0.046")
    assert executor._trailing_stop_move_count == 1

    strategy.current_timestamp += 1
    executor.get_net_pnl_pct.return_value = Decimal("0.046")
    executor.control_trailing_stop()
    strategy.sell.assert_not_called()

    strategy.current_timestamp += 1
    executor.get_net_pnl_pct.return_value = Decimal("0.045999")
    executor.control_trailing_stop()

    strategy.sell.assert_called_once()
    assert strategy.sell.call_args.args[2] == config.amount
    assert strategy.sell.call_args.args[3] == OrderType.LIMIT
    assert executor.close_type == CloseType.TRAILING_STOP
    assert executor.status == RunnableStatus.SHUTTING_DOWN
