from __future__ import annotations

from decimal import Decimal
from typing import Optional

from controllers.directional_trading.rsi_v5_core.canonical import stable_hash
from controllers.directional_trading.rsi_v5_core.domain import (
    ExecutorPhase,
    ExitIntent,
    ExitReason,
    Side,
    StrategyInput,
    StrategyState,
)

_ZERO = Decimal("0")


def _aggregate_basis(strategy_input: StrategyInput) -> Optional[Decimal]:
    total_base = _ZERO
    total_quote = _ZERO
    for position in strategy_input.positions:
        if position.side == Side.BUY:
            total_base += position.amount
            total_quote += position.amount * position.breakeven_price
    for executor in strategy_input.executors:
        if executor.side == Side.BUY and executor.owns_exposure and executor.filled_base > _ZERO:
            total_base += executor.filled_base
            total_quote += executor.filled_base * executor.reference_price
    return total_quote / total_base if total_base > _ZERO else None


def evaluate_exit(state: StrategyState, strategy_input: StrategyInput) -> tuple[Optional[ExitIntent], tuple[str, ...]]:
    market = strategy_input.market
    config = strategy_input.config
    owners = tuple(
        sorted(
            executor.executor_id
            for executor in strategy_input.executors
            if executor.side == Side.BUY
            and executor.phase in (ExecutorPhase.PENDING, ExecutorPhase.OPEN)
        )
    )
    held_ids = tuple(
        sorted(position.position_id for position in strategy_input.positions if position.side == Side.BUY)
    )
    if not owners and not held_ids:
        return None, ()

    basis = _aggregate_basis(strategy_input)
    hard_stop = bool(
        basis is not None
        and market.mark_price <= basis * (Decimal("1") - config.hard_stop_loss_pct)
    )
    signal_exit = bool(
        market.rsi is not None
        and market.rsi >= config.rsi_sell_threshold
        and market.sell_reversal_confirmed
    )
    profit_ok = bool(
        basis is not None
        and market.mark_price >= basis * (Decimal("1") + config.min_profit_pct_for_sell)
    )

    reason: Optional[ExitReason] = None
    reason_codes = []
    if hard_stop:
        reason = ExitReason.HARD_STOP
        reason_codes.append("hard_stop")
    elif signal_exit:
        if config.sell_only_if_profitable and not profit_ok:
            return None, ("need_profit",)
        reason = ExitReason.SIGNAL
        reason_codes.append("sell_signal")
    else:
        return None, ()

    snapshot_hash = stable_hash(strategy_input)
    unsigned = {
        "controller_id": state.controller_id,
        "event_id": market.event_id,
        "sequence": market.sequence,
        "config_hash": config.identity_hash,
        "snapshot_hash": snapshot_hash,
        "reason": reason,
        "executor_ids": owners,
        "held_position_ids": held_ids,
    }
    return ExitIntent(
        intent_id=stable_hash({"kind": "exit", **unsigned}),
        **unsigned,
    ), tuple(reason_codes)
