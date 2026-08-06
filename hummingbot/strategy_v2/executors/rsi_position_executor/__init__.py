from hummingbot.strategy_v2.executors.rsi_position_executor.data_types import RSIPositionExecutorConfig
from hummingbot.strategy_v2.executors.rsi_position_executor.rsi_position_executor import RSIPositionExecutor


def register_rsi_position_executor() -> None:
    """Register the RSI-specific executor without overriding Hummingbot defaults."""
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator

    existing = ExecutorOrchestrator._executor_mapping.get("rsi_position_executor")
    if existing is not None and existing is not RSIPositionExecutor:
        raise RuntimeError(
            "executor type 'rsi_position_executor' is already registered "
            f"to {existing!r}"
        )
    ExecutorOrchestrator._executor_mapping["rsi_position_executor"] = RSIPositionExecutor


__all__ = [
    "RSIPositionExecutor",
    "RSIPositionExecutorConfig",
    "register_rsi_position_executor",
]
