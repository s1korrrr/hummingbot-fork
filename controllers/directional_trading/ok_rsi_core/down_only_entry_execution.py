from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig


class DownOnlyMakerPositionExecutorConfig(PositionExecutorConfig):
    entry_acquisition_policy: Literal["down_only_maker_v1"] = "down_only_maker_v1"
    decision_best_bid: Decimal
    decision_best_ask: Decimal
    decision_atr_1m: Decimal
    decision_price_step: Decimal
    maximum_acceptable_price: Decimal
    entry_rest_seconds: int = Field(ge=1, le=300)
    entry_total_max_age_seconds: int = Field(ge=30, le=900)
    entry_max_downward_reprices: int = Field(ge=1, le=10)
    entry_reprice_cooldown_seconds: int = Field(ge=1, le=30)
    entry_reprice_min_ticks: int = Field(ge=1, le=100)
    entry_reprice_atr_fraction: Decimal = Field(gt=0, le=1)
    entry_max_downward_atr: Decimal = Field(gt=0, le=10)

    @model_validator(mode="after")
    def validate_down_only_maker_contract(self):
        if self.side is not TradeType.BUY:
            raise ValueError("down-only maker entry requires BUY side")
        if self.leverage != 1:
            raise ValueError("down-only maker entry requires leverage=1")
        if self.triple_barrier_config.open_order_type is not OrderType.LIMIT_MAKER:
            raise ValueError("down-only maker entry requires LIMIT_MAKER orders")
        if self.entry_order_policy != "single_attempt":
            raise ValueError("down-only maker entry requires single_attempt reconciliation")
        if self.entry_price is None:
            raise ValueError("down-only maker entry requires an entry_price")

        positive_decimals = {
            "entry_price": self.entry_price,
            "amount": self.amount,
            "decision_best_bid": self.decision_best_bid,
            "decision_best_ask": self.decision_best_ask,
            "decision_atr_1m": self.decision_atr_1m,
            "decision_price_step": self.decision_price_step,
            "maximum_acceptable_price": self.maximum_acceptable_price,
            "entry_reprice_atr_fraction": self.entry_reprice_atr_fraction,
            "entry_max_downward_atr": self.entry_max_downward_atr,
        }
        for name, value in positive_decimals.items():
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        if self.decision_best_bid > self.decision_best_ask:
            raise ValueError("down-only maker entry rejects a crossed decision book")
        if self.entry_price > self.maximum_acceptable_price:
            raise ValueError("entry_price exceeds the maximum acceptable price")
        if self.entry_total_max_age_seconds < self.entry_rest_seconds:
            raise ValueError("entry total age must not be shorter than the rest lease")
        if self.triple_barrier_config.open_order_limit_max_age_seconds != self.entry_rest_seconds:
            raise ValueError("entry rest lease must match the native open-order lease")
        return self
