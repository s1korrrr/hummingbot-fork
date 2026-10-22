from __future__ import annotations

from collections.abc import Iterable

from controllers.directional_trading.rsi_v5_core.canonical import stable_hash
from controllers.directional_trading.rsi_v5_core.domain import (
    ReplayTrace,
    StrategyInput,
    StrategyState,
)
from controllers.directional_trading.rsi_v5_core.state_machine import step


def replay(initial_state: StrategyState, events: Iterable[StrategyInput]) -> ReplayTrace:
    state = initial_state
    results = []
    for event in events:
        result = step(state, event)
        results.append(result)
        state = result.state
    result_tuple = tuple(results)
    return ReplayTrace(
        initial_state=initial_state,
        results=result_tuple,
        trace_hash=stable_hash(
            {
                "initial_state": initial_state,
                "results": result_tuple,
            }
        ),
    )
