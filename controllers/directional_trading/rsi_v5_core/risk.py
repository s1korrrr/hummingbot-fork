from __future__ import annotations

from decimal import Decimal

from controllers.directional_trading.rsi_v5_core.domain import (
    ExecutorPhase,
    ExposureSnapshot,
    RiskEvaluation,
    Side,
    StrategyInput,
)

_ZERO = Decimal("0")


def calculate_exposure(strategy_input: StrategyInput, proposed_notional: Decimal = _ZERO) -> ExposureSnapshot:
    mark = strategy_input.market.mark_price
    held = sum(
        (position.amount * mark for position in strategy_input.positions if position.side == Side.BUY),
        _ZERO,
    )
    filled = _ZERO
    pending = _ZERO
    for executor in strategy_input.executors:
        if executor.side != Side.BUY or not executor.owns_exposure:
            continue
        if executor.filled_base > _ZERO:
            filled += executor.filled_base * mark
        elif executor.filled_quote > _ZERO:
            filled += executor.filled_quote
        if executor.phase in (ExecutorPhase.PENDING, ExecutorPhase.OPEN):
            pending += executor.pending_base * max(mark, executor.reference_price)
    return ExposureSnapshot(
        held_notional=max(held, _ZERO),
        filled_notional=max(filled, _ZERO),
        pending_notional=max(pending, _ZERO),
        proposed_notional=max(Decimal(str(proposed_notional)), _ZERO),
    )


def evaluate_entry_risk(strategy_input: StrategyInput, proposed_notional: Decimal) -> RiskEvaluation:
    reasons = []
    exposure = calculate_exposure(strategy_input, proposed_notional)
    config = strategy_input.config
    account = strategy_input.account

    if any(
        executor.side == Side.BUY
        and executor.phase == ExecutorPhase.SHUTTING_DOWN
        and executor.owns_exposure
        for executor in strategy_input.executors
    ):
        reasons.append("exit_pending")
    if exposure.total_notional > config.max_total_position_usd:
        reasons.append("max_position")
    if account.available_collateral < proposed_notional:
        reasons.append("balance_low")
    if account.daily_pnl <= -config.max_daily_loss_quote:
        reasons.append("daily_loss_limit")
    if account.margin_utilization >= config.max_margin_utilization:
        reasons.append("margin_utilization")
    if config.leverage > config.max_leverage:
        reasons.append("leverage_limit")

    return RiskEvaluation(
        allowed=not reasons,
        reason_codes=tuple(dict.fromkeys(reasons)),
        exposure=exposure,
    )
