from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import ConfigDict, field_validator, model_validator

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig


class AbsoluteBarrierConfig(TripleBarrierConfig):
    stop_loss: None = None
    take_profit: None = None
    trailing_stop: None = None
    stop_loss_price: Decimal
    take_profit_price: Decimal
    open_order_type: OrderType = OrderType.LIMIT
    take_profit_order_type: OrderType = OrderType.MARKET
    stop_loss_order_type: OrderType = OrderType.MARKET
    time_limit_order_type: OrderType = OrderType.MARKET
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    @field_validator("stop_loss_price", "take_profit_price")
    @classmethod
    def validate_absolute_price(cls, value: Decimal) -> Decimal:
        parsed = Decimal(str(value))
        if not parsed.is_finite() or parsed <= Decimal("0"):
            raise ValueError("absolute barrier prices must be finite and positive")
        return parsed

    @field_validator("time_limit")
    @classmethod
    def validate_time_limit(cls, value: int | None) -> int:
        if value is None or value <= 0 or value > 604800:
            raise ValueError("absolute barrier executor requires a time limit between 1 and 604800 seconds")
        return value

    @field_validator("open_order_limit_max_age_seconds")
    @classmethod
    def validate_entry_order_age(cls, value: int) -> int:
        if value <= 0 or value > 300:
            raise ValueError("absolute barrier executor requires entry max age between 1 and 300 seconds")
        return value

    @model_validator(mode="after")
    def validate_supported_orders(self) -> "AbsoluteBarrierConfig":
        if self.open_order_type != OrderType.LIMIT:
            raise ValueError("absolute barrier executor requires LIMIT entry orders")
        if self.take_profit_order_type != OrderType.MARKET:
            raise ValueError("absolute barrier executor requires MARKET take-profit exits")
        if self.stop_loss_order_type != OrderType.MARKET:
            raise ValueError("absolute barrier executor requires MARKET stop-loss exits")
        if self.time_limit_order_type != OrderType.MARKET:
            raise ValueError("absolute barrier executor requires MARKET time-limit exits")
        if self.trailing_stop is not None:
            raise ValueError("trailing stop is not supported by absolute barrier executor v1")
        return self

    def new_instance_with_adjusted_volatility(self, volatility_factor: float) -> "AbsoluteBarrierConfig":
        factor = Decimal(str(volatility_factor))
        if not factor.is_finite() or factor <= Decimal("0"):
            raise ValueError("volatility factor must be finite and positive")
        return self.model_copy(deep=True)


class AbsoluteBarrierPositionExecutorConfig(PositionExecutorConfig):
    type: Literal["absolute_barrier_position_executor"] = "absolute_barrier_position_executor"
    entry_price: Decimal
    triple_barrier_config: AbsoluteBarrierConfig
    leverage: Literal[1] = 1
    activation_bounds: None = None
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    @field_validator("entry_price", "amount")
    @classmethod
    def validate_positive_finite_decimal(cls, value: Decimal) -> Decimal:
        parsed = Decimal(str(value))
        if not parsed.is_finite() or parsed <= Decimal("0"):
            raise ValueError("entry price and amount must be finite and positive")
        return parsed

    @model_validator(mode="after")
    def validate_absolute_barrier_geometry(self) -> "AbsoluteBarrierPositionExecutorConfig":
        stop = self.triple_barrier_config.stop_loss_price
        take_profit = self.triple_barrier_config.take_profit_price
        if self.side == TradeType.BUY:
            valid = stop < self.entry_price < take_profit
        else:
            valid = stop > self.entry_price > take_profit
        if not valid:
            raise ValueError("absolute stop/entry/take-profit geometry is invalid for the configured side")
        return self
