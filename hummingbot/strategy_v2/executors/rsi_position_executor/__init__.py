from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.executors.rsi_position_executor.data_types import RSIPositionExecutorConfig
from hummingbot.strategy_v2.executors.rsi_position_executor.rsi_position_executor import RSIPositionExecutor


def register_rsi_position_executor() -> None:
    """Install the partial-fill-safe implementation under Hummingbot's standard type.

    The configuration discriminator remains ``position_executor`` so executor
    status, persistence, and restart restoration continue to use Hummingbot's
    existing typed contract. Registration is idempotent and refuses to replace
    an unrelated extension.
    """
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator

    existing = ExecutorOrchestrator._executor_mapping.get("position_executor")
    if existing not in (PositionExecutor, RSIPositionExecutor):
        raise RuntimeError(
            "executor type 'position_executor' is already registered "
            f"to an incompatible implementation {existing!r}"
        )
    ExecutorOrchestrator._executor_mapping["position_executor"] = RSIPositionExecutor


__all__ = [
    "RSIPositionExecutor",
    "RSIPositionExecutorConfig",
    "register_rsi_position_executor",
]
