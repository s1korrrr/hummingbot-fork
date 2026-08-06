from decimal import Decimal

from controllers.directional_trading.rsi_v5_runtime import (
    EpisodeTracker,
    ExecutorExposure,
    build_exit_plan,
    calculate_exposure,
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
    assert outcome.max_planned_notional == Decimal("100")
    assert update_loss_streak(current_streak=2, outcome=outcome) == 3


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


def test_episode_result_recording_is_idempotent():
    tracker = EpisodeTracker()
    tracker.open(timestamp=1)
    tracker.register_executor("a", planned_notional=Decimal("10"))
    tracker.record_executor_result("a", net_pnl_quote=Decimal("-2"))
    tracker.record_executor_result("a", net_pnl_quote=Decimal("999"))

    outcome = tracker.close_if_flat(is_flat=True, timestamp=2)

    assert outcome is not None
    assert outcome.net_pnl_quote == Decimal("-2")
