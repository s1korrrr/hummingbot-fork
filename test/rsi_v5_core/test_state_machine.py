from decimal import Decimal

import pytest

from controllers.directional_trading.rsi_v5_core.domain import (
    AccountSnapshot,
    DataHealth,
    DataHealthStatus,
    DecisionKind,
    ExecutorPhase,
    ExecutorSnapshot,
    Lifecycle,
    MarketSnapshot,
    Side,
    StrategyConfig,
    StrategyInput,
    StrategyState,
)
from controllers.directional_trading.rsi_v5_core.errors import DuplicateEventError, OutOfOrderEventError
from controllers.directional_trading.rsi_v5_core.replay import replay
from controllers.directional_trading.rsi_v5_core.state_machine import step


def input_at(sequence: int, **market_overrides):
    market = dict(
        event_id=f"evt-{sequence}",
        sequence=sequence,
        event_time_ms=sequence * 60_000,
        received_time_ms=sequence * 60_000 + 25,
        connector="hyperliquid_perpetual",
        trading_pair="BTC-USD",
        mark_price=Decimal("100"),
        best_bid=Decimal("99.9"),
        best_ask=Decimal("100.1"),
        candle_time_ms=sequence * 60_000 - 10,
        rsi=Decimal("29"),
        signal_score=3,
        raw_buy_reversal=True,
        buy_confirmation_ready=True,
        sell_reversal_confirmed=False,
        condition_ok=True,
        condition_reason="ok",
        data_health=DataHealth(status=DataHealthStatus.READY, age_ms=10, max_age_ms=120_000),
    )
    market.update(market_overrides)
    return StrategyInput(
        market=MarketSnapshot(**market),
        account=AccountSnapshot(
            collateral_asset="USDC",
            available_collateral=Decimal("1000"),
            equity=Decimal("1000"),
            daily_pnl=Decimal("0"),
            margin_utilization=Decimal("0"),
        ),
        executors=(),
        positions=(),
        config=StrategyConfig(
            usd_per_entry=Decimal("30"),
            max_total_position_usd=Decimal("100"),
        ),
    )


def test_step_is_deterministic_and_replay_uses_same_transition_function():
    initial = StrategyState.initial("btc-rsi-v5")
    events = [input_at(1), input_at(2, raw_buy_reversal=False)]

    direct_first = step(initial, events[0])
    direct_second = step(direct_first.state, events[1])
    trace = replay(initial, events)

    assert trace.results == (direct_first, direct_second)
    assert trace.trace_hash == replay(initial, events).trace_hash


def test_duplicate_and_out_of_order_events_fail_closed():
    initial = StrategyState.initial("btc-rsi-v5")
    first = step(initial, input_at(2))

    with pytest.raises(DuplicateEventError):
        step(first.state, input_at(2))
    with pytest.raises(OutOfOrderEventError):
        step(first.state, input_at(1))


def test_healthy_buy_signal_emits_deterministic_entry_intent():
    result = step(StrategyState.initial("btc-rsi-v5"), input_at(1))

    assert result.decision.kind == DecisionKind.ENTRY
    assert result.entry_intent is not None
    assert result.entry_intent.side == Side.BUY
    assert result.entry_intent.notional == Decimal("30")
    assert result.state.lifecycle == Lifecycle.ENTERING
    assert result.entry_intent.intent_id == step(
        StrategyState.initial("btc-rsi-v5"), input_at(1)
    ).entry_intent.intent_id


def test_stale_data_blocks_entry_without_blocking_existing_exit():
    stale = DataHealth(status=DataHealthStatus.STALE, age_ms=130_000, max_age_ms=120_000, reason="stale_candle")
    no_position = step(
        StrategyState.initial("btc-rsi-v5"),
        input_at(1, data_health=stale),
    )
    assert no_position.decision.kind == DecisionKind.NOOP
    assert no_position.entry_intent is None

    closing = ExecutorSnapshot(
        executor_id="owner",
        side=Side.BUY,
        phase=ExecutorPhase.OPEN,
        configured_base=Decimal("1"),
        filled_base=Decimal("1"),
        filled_quote=Decimal("100"),
        reference_price=Decimal("100"),
        net_pnl_quote=Decimal("1"),
    )
    exit_input = input_at(
        1,
        data_health=stale,
        mark_price=Decimal("101"),
        best_bid=Decimal("100.9"),
        best_ask=Decimal("101.1"),
        rsi=Decimal("80"),
        raw_buy_reversal=False,
        sell_reversal_confirmed=True,
    ).model_copy(update={"executors": (closing,)})
    exit_result = step(StrategyState.initial("btc-rsi-v5"), exit_input)

    assert exit_result.decision.kind == DecisionKind.EXIT
    assert exit_result.exit_intent.executor_ids == ("owner",)


def test_pending_and_filled_exposure_enforce_cap():
    executor = ExecutorSnapshot(
        executor_id="existing",
        side=Side.BUY,
        phase=ExecutorPhase.OPEN,
        configured_base=Decimal("0.8"),
        filled_base=Decimal("0.4"),
        filled_quote=Decimal("40"),
        reference_price=Decimal("100"),
    )
    strategy_input = input_at(1).model_copy(update={"executors": (executor,)})

    result = step(StrategyState.initial("btc-rsi-v5"), strategy_input)

    assert result.entry_intent is None
    assert result.decision.kind == DecisionKind.NOOP
    assert "max_position" in result.decision.reason_codes


def test_shutting_down_exposure_blocks_new_entry_and_gets_no_duplicate_exit():
    closing = ExecutorSnapshot(
        executor_id="closing",
        side=Side.BUY,
        phase=ExecutorPhase.SHUTTING_DOWN,
        configured_base=Decimal("1"),
        filled_base=Decimal("0.4"),
        filled_quote=Decimal("40"),
        reference_price=Decimal("100"),
    )
    strategy_input = input_at(1).model_copy(update={"executors": (closing,)})

    result = step(StrategyState.initial("btc-rsi-v5"), strategy_input)

    assert result.entry_intent is None
    assert result.exit_intent is None
    assert "exit_pending" in result.decision.reason_codes
