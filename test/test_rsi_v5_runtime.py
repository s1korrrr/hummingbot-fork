from decimal import Decimal

import pytest

from controllers.directional_trading.rsi_v5_runtime import (
    EpisodeTracker,
    ExecutorExposure,
    ExecutorSettlement,
    LifecycleEvent,
    LifecycleState,
    SettlementConflict,
    SettlementMutation,
    SettlementStatus,
    build_exit_plan,
    calculate_exposure,
    reduce_lifecycle_event,
    update_loss_streak,
)


def ex(
    executor_id: str,
    *,
    side: str = "BUY",
    executor_type: str = "position_executor",
    active: bool = True,
    configured_base: str = "1",
    filled_base: str = "0",
    filled_quote: str = "0",
    reference_price: str = "100",
    net_pnl_quote: str = "0",
    done: bool = False,
) -> ExecutorExposure:
    return ExecutorExposure(
        executor_id=executor_id,
        side=side,
        executor_type=executor_type,
        active=active,
        configured_base=Decimal(configured_base),
        filled_base=Decimal(filled_base),
        filled_quote=Decimal(filled_quote),
        reference_price=Decimal(reference_price),
        net_pnl_quote=Decimal(net_pnl_quote),
        done=done,
    )


def test_exit_plan_stops_every_active_buy_position_executor():
    executors = [
        ex("unfilled", filled_base="0"),
        ex("partial", filled_base="0.4", filled_quote="40"),
        ex("filled", filled_base="1", filled_quote="100"),
        ex("sell", side="SELL"),
        ex("order", executor_type="order_executor"),
        ex("done", active=False, done=True),
    ]

    plan = build_exit_plan(executors, reason="rsi_reversal")

    assert plan.reason == "rsi_reversal"
    assert plan.executor_ids == ("unfilled", "partial", "filled")


def test_exposure_reserves_unfilled_notional_and_proposed_intent():
    executors = [
        ex("a", configured_base="1", filled_base="0.4", filled_quote="40", reference_price="100"),
        ex("b", configured_base="2", filled_base="0", filled_quote="0", reference_price="90"),
        ex("inactive", active=False, configured_base="10", reference_price="100"),
    ]

    exposure = calculate_exposure(
        executors=executors,
        mark_price=Decimal("110"),
        held_notional=Decimal("55"),
        proposed_notional=Decimal("30"),
    )

    assert exposure.held_notional == Decimal("55")
    assert exposure.filled_notional == Decimal("44")
    assert exposure.pending_notional == Decimal("286")
    assert exposure.proposed_notional == Decimal("30")
    assert exposure.total_notional == Decimal("415")


def test_episode_aggregates_executor_results_before_updating_loss_streak():
    tracker = EpisodeTracker()
    episode_id = tracker.open(timestamp=100.0)
    tracker.register_executor("scout", planned_notional=Decimal("30"))
    tracker.register_executor("runner", planned_notional=Decimal("70"))

    tracker.record_executor_result("scout", net_pnl_quote=Decimal("2"))
    assert tracker.close_if_flat(is_flat=False, timestamp=120.0) is None

    tracker.record_executor_result("runner", net_pnl_quote=Decimal("-5"))
    outcome = tracker.close_if_flat(is_flat=True, timestamp=130.0)

    assert outcome is not None
    assert outcome.episode_id == episode_id
    assert outcome.net_pnl_quote == Decimal("-3")
    assert outcome.executor_ids == ("runner", "scout")
    assert outcome.total_planned_notional == Decimal("100")
    assert update_loss_streak(current_streak=2, outcome=outcome) == 3


def test_episode_waits_for_every_registered_executor_result_even_when_flat():
    tracker = EpisodeTracker()
    tracker.open(timestamp=100.0)
    tracker.register_executor("scout", planned_notional=Decimal("30"))
    tracker.register_executor("runner", planned_notional=Decimal("70"))
    tracker.record_executor_result("scout", net_pnl_quote=Decimal("2"))

    assert tracker.close_if_flat(is_flat=True, timestamp=130.0) is None

    tracker.record_executor_result("runner", net_pnl_quote=Decimal("0"))
    assert tracker.close_if_flat(is_flat=True, timestamp=131.0) is not None


def test_unregistered_historical_executor_result_is_ignored():
    tracker = EpisodeTracker()
    tracker.open(timestamp=100.0)
    tracker.register_executor("current", planned_notional=Decimal("30"))

    assert tracker.record_executor_result("historical", net_pnl_quote=Decimal("-999")) is False
    assert tracker.record_executor_result("current", net_pnl_quote=Decimal("1")) is True

    outcome = tracker.close_if_flat(is_flat=True, timestamp=130.0)

    assert outcome is not None
    assert outcome.executor_ids == ("current",)
    assert outcome.net_pnl_quote == Decimal("1")


def test_profitable_episode_resets_loss_streak_but_zero_episode_does_not():
    profitable = EpisodeTracker()
    profitable.open(timestamp=1)
    profitable.register_executor("a", planned_notional=Decimal("10"))
    profitable.record_executor_result("a", net_pnl_quote=Decimal("1"))
    profit_outcome = profitable.close_if_flat(is_flat=True, timestamp=2)
    assert update_loss_streak(current_streak=4, outcome=profit_outcome) == 0

    flat = EpisodeTracker()
    flat.open(timestamp=1)
    flat.register_executor("a", planned_notional=Decimal("10"))
    flat.record_executor_result("a", net_pnl_quote=Decimal("0"))
    flat_outcome = flat.close_if_flat(is_flat=True, timestamp=2)
    assert update_loss_streak(current_streak=4, outcome=flat_outcome) == 4


def test_provisional_executor_result_is_replaced_by_final_result():
    tracker = EpisodeTracker()
    tracker.open(timestamp=1)
    tracker.register_executor("a", planned_notional=Decimal("10"))

    assert tracker.record_executor_settlement(
        ExecutorSettlement(
            executor_id="a",
            status=SettlementStatus.PROVISIONAL,
            net_pnl_quote=Decimal("-2"),
            revision=1,
        )
    ) is SettlementMutation.INSERTED
    assert tracker.close_if_flat(is_flat=True, timestamp=1.5) is None
    assert tracker.record_executor_settlement(
        ExecutorSettlement(
            executor_id="a",
            status=SettlementStatus.FINAL,
            net_pnl_quote=Decimal("999"),
            revision=2,
        )
    ) is SettlementMutation.REPLACED

    outcome = tracker.close_if_flat(is_flat=True, timestamp=2)

    assert outcome is not None
    assert outcome.net_pnl_quote == Decimal("999")


def test_identical_final_settlement_is_idempotent_but_changed_final_conflicts():
    tracker = EpisodeTracker()
    tracker.open(timestamp=1)
    tracker.register_executor("a", planned_notional=Decimal("10"))
    final = ExecutorSettlement(
        executor_id="a",
        status=SettlementStatus.FINAL,
        net_pnl_quote=Decimal("1"),
        revision=2,
    )

    assert tracker.record_executor_settlement(final) is SettlementMutation.INSERTED
    assert tracker.record_executor_settlement(final) is SettlementMutation.IDEMPOTENT
    with pytest.raises(SettlementConflict):
        tracker.record_executor_settlement(
            ExecutorSettlement(
                executor_id="a",
                status=SettlementStatus.FINAL,
                net_pnl_quote=Decimal("2"),
                revision=3,
            )
        )


def test_provisional_settlement_revision_must_advance():
    tracker = EpisodeTracker()
    tracker.open(timestamp=1)
    tracker.register_executor("a", planned_notional=Decimal("10"))
    tracker.record_executor_settlement(
        ExecutorSettlement(
            executor_id="a",
            status=SettlementStatus.PROVISIONAL,
            net_pnl_quote=Decimal("-2"),
            revision=2,
        )
    )

    with pytest.raises(SettlementConflict):
        tracker.record_executor_settlement(
            ExecutorSettlement(
                executor_id="a",
                status=SettlementStatus.PROVISIONAL,
                net_pnl_quote=Decimal("-3"),
                revision=1,
            )
        )


@pytest.mark.parametrize(
    "events",
    [
        [
            LifecycleEvent("cancel-1", "CANCEL_ACK"),
            LifecycleEvent("fill-1", "ENTRY_FILL", Decimal("0.4")),
        ],
        [
            LifecycleEvent("fill-1", "ENTRY_FILL", Decimal("0.4")),
            LifecycleEvent("cancel-1", "CANCEL_ACK"),
        ],
    ],
)
def test_cancel_and_partial_fill_event_order_converges(events):
    state = LifecycleState.initial("executor-1")

    for event in events:
        state = reduce_lifecycle_event(state, event)

    assert state.filled_base == Decimal("0.4")
    assert state.entry_cancelled is True
    assert state.reconciliation_required is False


def test_lifecycle_reducer_deduplicates_callbacks_and_applies_late_fees():
    state = LifecycleState.initial("executor-1")
    fill = LifecycleEvent("fill-1", "ENTRY_FILL", Decimal("0.4"))
    fee = LifecycleEvent("fee-1", "FEE_ADJUSTMENT", Decimal("0.01"))

    state = reduce_lifecycle_event(state, fill)
    state = reduce_lifecycle_event(state, fill)
    state = reduce_lifecycle_event(state, fee)

    assert state.filled_base == Decimal("0.4")
    assert state.fee_adjustment_quote == Decimal("0.01")
    assert len(state.events) == 2


def test_close_fill_terminal_and_late_fee_event_order_converges():
    events = [
        LifecycleEvent("entry-1", "ENTRY_FILL", Decimal("0.4")),
        LifecycleEvent("close-1", "CLOSE_FILL", Decimal("0.4")),
        LifecycleEvent("terminal-1", "TERMINAL_CONFIRMATION"),
        LifecycleEvent("fee-1", "FEE_ADJUSTMENT", Decimal("0.02")),
    ]

    forward = LifecycleState.initial("executor-1")
    reverse = LifecycleState.initial("executor-1")
    for event in events:
        forward = reduce_lifecycle_event(forward, event)
    for event in reversed(events):
        reverse = reduce_lifecycle_event(reverse, event)

    assert forward == reverse
    assert forward.close_filled_base == Decimal("0.4")
    assert forward.fee_adjustment_quote == Decimal("0.02")
    assert forward.terminal is True
    assert forward.reconciliation_required is False


def test_lifecycle_timeout_and_conflicting_duplicate_fail_closed():
    state = LifecycleState.initial("executor-1")
    state = reduce_lifecycle_event(
        state,
        LifecycleEvent("timeout-1", "TIMEOUT"),
    )
    state = reduce_lifecycle_event(
        state,
        LifecycleEvent("timeout-1", "ENTRY_FILL", Decimal("1")),
    )

    assert state.reconciliation_required is True
    assert state.filled_base == Decimal("0")
