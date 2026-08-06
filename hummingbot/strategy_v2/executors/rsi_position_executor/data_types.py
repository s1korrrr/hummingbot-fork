from typing import Literal

from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig


class RSIPositionExecutorConfig(PositionExecutorConfig):
    """Position executor config with an isolated orchestrator type."""

    type: Literal["rsi_position_executor"] = "rsi_position_executor"
