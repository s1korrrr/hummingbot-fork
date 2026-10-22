from typing import Optional, TypeVar

from pydantic import BaseModel

from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase

ExecutorConfigType = TypeVar("ExecutorConfigType", bound=ExecutorConfigBase)


class ExecutorAction(BaseModel):
    """
    Base class for bot actions.
    """
    controller_id: Optional[str] = "main"


class CreateExecutorAction(ExecutorAction):
    """
    Action to create an executor.
    """
    executor_config: ExecutorConfigType


class StopExecutorAction(ExecutorAction):
    """
    Action to stop an executor.
    """
    executor_id: str
    keep_position: Optional[bool] = False
    # Stop only the entry acquisition (cancel the resting remainder); a filled
    # position keeps its exit barriers. Used for signal invalidation/kill switch.
    acquisition_only: bool = False


class StoreExecutorAction(ExecutorAction):
    """
    Action to store an executor.
    """
    executor_id: str
