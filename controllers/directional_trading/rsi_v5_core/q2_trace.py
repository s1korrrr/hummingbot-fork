from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig
from controllers.directional_trading.rsi_v5_core.canonical import stable_hash
from hummingbot.core.data_type.common import PriceType, TradeType


class _TraceMarketDataProvider:
    def __init__(self, *, now: float = 1000.0, price: Decimal = Decimal("100")) -> None:
        self.now = now
        self.price = price

    def initialize_rate_sources(self, _config) -> None:
        return None

    def time(self) -> float:
        return self.now

    def get_price_by_type(self, _connector, _pair, price_type) -> Decimal:
        if price_type == PriceType.BestAsk:
            return self.price + Decimal("0.50")
        return self.price

    def get_balance(self, _connector, _asset) -> Decimal:
        return Decimal("1000")

    def get_available_balance(self, _connector, _asset) -> Decimal:
        return Decimal("1000")

    def get_trading_rules(self, _connector, _pair):
        return SimpleNamespace(
            min_price_increment=Decimal("0.01"),
            min_order_size=Decimal("0"),
            min_base_amount_increment=Decimal("0.00000001"),
            min_notional_size=Decimal("5"),
            min_order_value=Decimal("5"),
        )

    def quantize_order_amount(self, _connector, _pair, amount: Decimal) -> Decimal:
        return amount.quantize(Decimal("0.00000001"))


def _controller(provider: _TraceMarketDataProvider, **overrides) -> RSIv5Controller:
    config = {
        "id": "rsi-v5-q2-oracle",
        "connector_name": "hyperliquid_perpetual",
        "trading_pair": "BNB-USDC",
        "interval": "1m",
        "rsi_length": 10,
        "rsi_buy_threshold": 31.0,
        "rsi_sell_threshold": 76.0,
        "min_signal_score": 2,
        "usd_per_entry": Decimal("25"),
        "max_total_position_usd": Decimal("500"),
        "cooldown_time": 90,
        "min_cooldown_time": 45,
        "executor_price_gap_threshold": Decimal("0.01"),
        "use_adaptive_filters": False,
        "dynamic_position_sizing": False,
        "buy_confirmation_mode": "rebound_confirm",
        "use_split_entries": True,
        "scout_entry_fraction": 0.35,
        "trailing_activation_pct": 0.047,
        "trailing_delta_pct": 0.034,
        "sell_only_if_profitable": False,
        "trend_confirmation_enabled": False,
        "use_bag_freeze": False,
        "early_stop_drawdown_pct": Decimal("0"),
        "recovery_partial_exit_fraction": 0.5,
    }
    config.update(overrides)
    return RSIv5Controller(
        config=RSIv5ControllerConfig(**config),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )


def _event(sequence: int, action: str, inputs: dict, result: dict, methods: list[str]) -> dict:
    return {
        "sequence": sequence,
        "action": action,
        "inputs": inputs,
        "result": result,
        "source_methods": methods,
    }


def capture_q2_lifecycle_trace() -> dict:
    events: list[dict] = []

    entry_provider = _TraceMarketDataProvider(price=Decimal("100"))
    entry = _controller(entry_provider)
    entry_signal = entry._determine_signal(
        last_rsi=30.0,
        signal_score=3,
        rsi_buy_eff=31.0,
        rsi_sell_eff=74.0,
        last_close=Decimal("100"),
        raw_buy_reversal=True,
        signal_timestamp=1000.0,
        candle_timestamp=1000.0,
    )
    events.append(
        _event(
            1,
            "entry",
            {"candle_time": 1000, "rsi": "30", "score": 3, "raw_reversal": True},
            {"signal": entry_signal, "role": entry._last_buy_signal_role},
            ["RSIv5Controller._determine_signal"],
        )
    )

    dca_provider = _TraceMarketDataProvider(price=Decimal("98"))
    dca = _controller(dca_provider)
    dca.processed_data = {
        "signal": 1,
        "signal_state": {
            "condition_ok": True,
            "buy_entry_role": "full",
            "size_multiplier": 1.0,
            "signal_score": 3,
        },
    }
    dca.positions_held = [
        SimpleNamespace(
            connector_name="hyperliquid_perpetual",
            trading_pair="BNB-USDC",
            side=TradeType.BUY,
            amount=Decimal("0.25"),
            breakeven_price=Decimal("100"),
        )
    ]
    dca_allowed = dca.can_create_executor(1)
    events.append(
        _event(
            2,
            "dca",
            {"time": 1000, "mark": "98", "held_base": "0.25", "basis": "100"},
            {"allowed": dca_allowed, "gate_reason": dca._last_gate_reason[TradeType.BUY]},
            ["RSIv5Controller.can_create_executor"],
        )
    )

    cooldown_provider = _TraceMarketDataProvider(price=Decimal("98"))
    cooldown = _controller(cooldown_provider)
    cooldown.processed_data = {
        "signal": 1,
        "signal_state": {
            "condition_ok": True,
            "buy_entry_role": "full",
            "size_multiplier": 1.0,
            "signal_score": 3,
        },
    }
    cooldown.executors_info = [
        SimpleNamespace(
            is_active=True,
            connector_name="hyperliquid_perpetual",
            trading_pair="BNB-USDC",
            side=TradeType.BUY,
            timestamp=990.0,
            filled_amount_quote=Decimal("25"),
            net_pnl_pct=-0.02,
            custom_info={"current_position_average_price": Decimal("100")},
        )
    ]
    cooldown_allowed = cooldown.can_create_executor(1)
    events.append(
        _event(
            3,
            "cooldown",
            {"time": 1000, "last_entry_time": 990, "cooldown_seconds": 90},
            {
                "allowed": cooldown_allowed,
                "gate_reason": cooldown._last_gate_reason[TradeType.BUY],
            },
            ["RSIv5Controller.can_create_executor", "RSIv5Controller._effective_cooldown_time"],
        )
    )

    recovery_provider = _TraceMarketDataProvider(price=Decimal("101"))
    recovery = _controller(recovery_provider)
    recovery.processed_data = {
        "signal": 0,
        "trend_confirmation": {
            "direction": "up",
            "confidence": 0.74,
            "alignment_score": 0.74,
            "states": {},
        },
        "signal_state": {"rsi": 82.0, "rsi_prev": 82.4, "ema_fast": 100.6},
    }
    recovery.positions_held = [
        SimpleNamespace(
            connector_name="hyperliquid_perpetual",
            trading_pair="BNB-USDC",
            side=TradeType.BUY,
            amount=Decimal("1"),
            breakeven_price=Decimal("100"),
        )
    ]
    recovery.stop_actions_proposal()
    recovery_provider.price = Decimal("102")
    recovery.processed_data["signal_state"]["rsi"] = 84.0
    recovery.stop_actions_proposal()
    recovery_provider.price = Decimal("101.60")
    recovery.processed_data["signal_state"].update(rsi=81.5, rsi_prev=83.0, ema_fast=101.9)
    harvest_actions = recovery.stop_actions_proposal()
    harvest_config = harvest_actions[0].executor_config
    events.append(
        _event(
            4,
            "partial_harvest",
            {"basis": "100", "peak": "102", "pullback_mark": "101.60", "held_base": "1"},
            {
                "actions": len(harvest_actions),
                "side": harvest_config.side.name,
                "amount_base": format(harvest_config.amount, "f"),
                "execution_strategy": harvest_config.execution_strategy.name,
            },
            ["RSIv5Controller.stop_actions_proposal", "RSIv5Controller._check_held_bag_recovery"],
        )
    )

    runner_provider = _TraceMarketDataProvider(now=1000.0, price=Decimal("100"))
    runner = _controller(runner_provider, scout_entry_fraction=0.4)
    runner.processed_data = {
        "signal": 1,
        "timestamp": 1000.0,
        "signal_state": {
            "timestamp": 1000.0,
            "condition_ok": True,
            "buy_entry_role": "runner",
            "buy_entry_fraction": 0.6,
            "buy_context_bias": "neutral",
            "size_multiplier": 1.0,
            "signal_score": 3,
        },
    }
    runner.executors_info = [
        SimpleNamespace(
            is_active=True,
            connector_name="hyperliquid_perpetual",
            trading_pair="BNB-USDC",
            side=TradeType.BUY,
            timestamp=990.0,
            filled_amount_quote=Decimal("10"),
            net_pnl_pct=0.0,
            custom_info={
                "level_id": "scout",
                "role": "scout",
                "current_position_average_price": Decimal("100"),
            },
        )
    ]
    runner._has_sufficient_price_gap = lambda *_args, **_kwargs: False
    runner_allowed = runner.can_create_executor(1)
    runner_config = runner.get_executor_config(TradeType.BUY, Decimal("100"), Decimal("1"))
    events.append(
        _event(
            5,
            "runner",
            {"time": 1000, "scout_time": 990, "scout_quote": "10", "fraction": "0.6"},
            {
                "allowed": runner_allowed,
                "gate_reason": runner._last_gate_reason[TradeType.BUY],
                "level_id": runner_config.level_id,
                "amount_base": format(runner_config.amount, "f"),
            },
            ["RSIv5Controller.can_create_executor", "RSIv5Controller.get_executor_config"],
        )
    )

    trailing = runner_config.triple_barrier_config.trailing_stop
    events.append(
        _event(
            6,
            "trailing",
            {"entry_role": "runner", "reference_price": "100"},
            {
                "activation_fraction": format(trailing.activation_price, "f"),
                "distance_fraction": format(trailing.trailing_delta, "f"),
                "order_type": runner_config.triple_barrier_config.trailing_stop_order_type.name,
            },
            ["RSIv5Controller.get_executor_config"],
        )
    )

    exit_provider = _TraceMarketDataProvider(price=Decimal("101.50"))
    exit_controller = _controller(exit_provider)
    exit_controller.positions_held = [
        SimpleNamespace(
            connector_name="hyperliquid_perpetual",
            trading_pair="BNB-USDC",
            side=TradeType.BUY,
            amount=Decimal("0.5"),
            breakeven_price=Decimal("100"),
        )
    ]
    exit_controller._determine_signal(
        last_rsi=76.0,
        signal_score=0,
        rsi_buy_eff=31.0,
        rsi_sell_eff=74.0,
        last_close=Decimal("101.50"),
        rsi_prev=75.7,
        ema_fast=101.0,
        macd_hist=0.8,
        macd_hist_prev=0.6,
        raw_buy_reversal=False,
        signal_timestamp=1060.0,
        candle_timestamp=1060.0,
    )
    exit_signal = exit_controller._determine_signal(
        last_rsi=75.2,
        signal_score=0,
        rsi_buy_eff=31.0,
        rsi_sell_eff=74.0,
        last_close=Decimal("100.80"),
        rsi_prev=76.1,
        ema_fast=101.0,
        macd_hist=0.4,
        macd_hist_prev=0.7,
        raw_buy_reversal=False,
        signal_timestamp=1120.0,
        candle_timestamp=1120.0,
    )
    events.append(
        _event(
            7,
            "exit",
            {"candle_time": 1120, "rsi": "75.2", "held_base": "0.5", "basis": "100"},
            {"signal": exit_signal, "ownership": "direct_signal_exit"},
            ["RSIv5Controller._determine_signal"],
        )
    )

    payload = {
        "schema_version": "rsi-v5-q2-lifecycle-trace/1.0",
        "authority": {
            "paper_shadow_only": True,
            "live_orders_allowed": False,
            "execution_authority": False,
        },
        "controller": {
            "connector": "hyperliquid_perpetual",
            "trading_pair": "BNB-USDC",
            "interval": "1m",
            "controller_id": "rsi-v5-q2-oracle",
        },
        "events": events,
    }
    return {**payload, "trace_hash": stable_hash(payload)}
