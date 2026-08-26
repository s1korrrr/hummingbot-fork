from __future__ import annotations

from controllers.directional_trading.rsi_v5_core.canonical import stable_hash
from controllers.directional_trading.rsi_v5_core.domain import (
    DecisionKind,
    DecisionRecord,
    ExecutorPhase,
    Lifecycle,
    Side,
    StrategyInput,
    StrategyState,
    TransitionResult,
)
from controllers.directional_trading.rsi_v5_core.entry import evaluate_entry
from controllers.directional_trading.rsi_v5_core.errors import (
    ConfigMismatchError,
    DuplicateEventError,
    OutOfOrderEventError,
)
from controllers.directional_trading.rsi_v5_core.exit import evaluate_exit
from controllers.directional_trading.rsi_v5_core.risk import calculate_exposure


def _validate_event(state: StrategyState, strategy_input: StrategyInput) -> None:
    market = strategy_input.market
    if state.last_event_id == market.event_id or state.last_sequence == market.sequence:
        raise DuplicateEventError(f"duplicate event {market.event_id}")
    if market.sequence < state.last_sequence:
        raise OutOfOrderEventError(
            f"sequence {market.sequence} is older than {state.last_sequence}"
        )
    if market.event_time_ms < state.last_event_time_ms:
        raise OutOfOrderEventError(
            f"event time {market.event_time_ms} is older than {state.last_event_time_ms}"
        )
    config_hash = strategy_input.config.identity_hash
    if state.config_hash is not None and state.config_hash != config_hash:
        raise ConfigMismatchError("configuration changed without an explicit state migration")


def _observed_lifecycle(strategy_input: StrategyInput) -> Lifecycle:
    if any(
        executor.side == Side.BUY and executor.phase == ExecutorPhase.SHUTTING_DOWN
        for executor in strategy_input.executors
    ):
        return Lifecycle.EXITING
    if any(
        executor.side == Side.BUY and executor.phase in (ExecutorPhase.PENDING, ExecutorPhase.OPEN)
        for executor in strategy_input.executors
    ):
        return Lifecycle.OPEN
    if any(position.side == Side.BUY for position in strategy_input.positions):
        return Lifecycle.OPEN
    return Lifecycle.FLAT


def step(state: StrategyState, strategy_input: StrategyInput) -> TransitionResult:
    _validate_event(state, strategy_input)
    market = strategy_input.market
    config_hash = strategy_input.config.identity_hash
    observed = _observed_lifecycle(strategy_input)

    exit_intent, exit_reasons = evaluate_exit(state, strategy_input)
    entry_intent = None
    reason_codes = list(exit_reasons)
    kind = DecisionKind.NOOP
    next_lifecycle = observed

    if exit_intent is not None:
        kind = DecisionKind.EXIT
        next_lifecycle = Lifecycle.EXITING
    else:
        entry_intent, risk, entry_reasons = evaluate_entry(
            state.model_copy(update={"lifecycle": observed}),
            strategy_input,
        )
        reason_codes.extend(entry_reasons)
        if entry_intent is not None:
            kind = DecisionKind.ENTRY
            next_lifecycle = Lifecycle.ENTERING
        elif state.entry_paused:
            kind = DecisionKind.PAUSE
            next_lifecycle = Lifecycle.PAUSED
        exposure = risk.exposure

    if exit_intent is not None:
        exposure = calculate_exposure(strategy_input)

    reason_codes = tuple(dict.fromkeys(reason_codes))
    decision_payload = {
        "kind": kind,
        "event_id": market.event_id,
        "sequence": market.sequence,
        "reason_codes": reason_codes,
        "entry_intent_id": entry_intent.intent_id if entry_intent else None,
        "exit_intent_id": exit_intent.intent_id if exit_intent else None,
    }
    decision = DecisionRecord(
        decision_id=stable_hash(decision_payload),
        kind=kind,
        event_id=market.event_id,
        sequence=market.sequence,
        reason_codes=reason_codes,
    )
    next_state = state.model_copy(
        update={
            "state_version": state.state_version + 1,
            "lifecycle": next_lifecycle,
            "last_event_id": market.event_id,
            "last_sequence": market.sequence,
            "last_event_time_ms": market.event_time_ms,
            "config_hash": config_hash,
        }
    )
    return TransitionResult(
        state=next_state,
        decision=decision,
        exposure=exposure,
        entry_intent=entry_intent,
        exit_intent=exit_intent,
    )
