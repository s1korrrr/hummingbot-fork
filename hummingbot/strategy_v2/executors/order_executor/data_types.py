from decimal import Decimal
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_core.core_schema import ValidationInfo

from hummingbot.core.data_type.common import PositionAction, TradeType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase


class ExecutionStrategy(Enum):
    LIMIT = "LIMIT"
    LIMIT_MAKER = "LIMIT_MAKER"
    MARKET = "MARKET"
    LIMIT_CHASER = "LIMIT_CHASER"


class LimitChaserConfig(BaseModel):
    distance: Decimal
    refresh_threshold: Decimal


class OrderExecutorConfig(ExecutorConfigBase):
    type: Literal["order_executor"] = "order_executor"
    trading_pair: str
    connector_name: str
    side: TradeType
    amount: Decimal
    position_action: PositionAction = PositionAction.OPEN
    price: Optional[Decimal] = None  # Required for LIMIT and LIMIT_MAKER
    chaser_config: Optional[LimitChaserConfig] = None  # Required for LIMIT_CHASER
    execution_strategy: ExecutionStrategy
    leverage: int = 1
    level_id: Optional[str] = None
    min_sell_price: Optional[Decimal] = Field(default=None, gt=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_price_floor(self):
        if self.min_sell_price is not None and (self.side != TradeType.SELL or self.execution_strategy == ExecutionStrategy.MARKET):
            raise ValueError("A minimum sell price requires a limit sell order")
        return self

    @field_validator("execution_strategy", mode="before")
    @classmethod
    def validate_execution_strategy(cls, value, validation_info: ValidationInfo):
        if value in [ExecutionStrategy.LIMIT, ExecutionStrategy.LIMIT_MAKER]:
            if validation_info.data.get('price') is None:
                raise ValueError("Price is required for LIMIT and LIMIT_MAKER execution strategies")
        elif value == ExecutionStrategy.LIMIT_CHASER:
            if validation_info.data.get('chaser_config') is None:
                raise ValueError("Chaser config is required for LIMIT_CHASER execution strategy")
        return value
