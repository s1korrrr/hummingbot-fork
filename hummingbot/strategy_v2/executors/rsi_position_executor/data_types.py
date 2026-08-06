from typing import Literal

from pydantic import field_validator

from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig


class RSIPositionExecutorConfig(PositionExecutorConfig):
    """Compatibility wrapper that keeps the standard persisted executor type.

    Earlier development builds emitted ``rsi_position_executor`` as a new
    discriminator. Hummingbot's durable ``ExecutorInfo`` union only supports the
    standard ``position_executor`` tag. Normalize the development-only tag at
    validation time so runtime status, database persistence, and restart restore
    all use the canonical Hummingbot contract.
    """

    type: Literal["position_executor"] = "position_executor"

    @field_validator("type", mode="before")
    @classmethod
    def normalize_executor_type(cls, value):
        if value in (None, "", "rsi_position_executor"):
            return "position_executor"
        return value
