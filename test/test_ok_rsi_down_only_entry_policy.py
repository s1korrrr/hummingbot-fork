from dataclasses import replace
from decimal import Decimal

import pytest

from controllers.directional_trading.ok_rsi_core.down_only_entry import (
    DownOnlyMakerAction,
    DownOnlyMakerDecision,
    DownOnlyMakerInputError,
    DownOnlyMakerParameters,
    DownOnlyMakerReason,
    DownOnlyMakerSnapshot,
    decide_down_only_maker,
)


def parameters(**changes) -> DownOnlyMakerParameters:
    values = {
        "total_max_age_seconds": 300,
        "max_reprices": 3,
        "cooldown_seconds": 5,
        "min_ticks": 3,
        "atr_fraction": Decimal("0.05"),
        "activation_bound_pct": Decimal("0.005"),
    }
    values.update(changes)
    return DownOnlyMakerParameters(**values)


def snapshot(**changes) -> DownOnlyMakerSnapshot:
    values = {
        "now": Decimal("10"),
        "intent_created_at": Decimal("0"),
        "order_created_at": Decimal("0"),
        "last_reprice_at": None,
        "initial_best_bid": Decimal("100.00"),
        "initial_best_ask": Decimal("100.01"),
        "best_bid": Decimal("100.00"),
        "best_ask": Decimal("100.01"),
        "current_order_price": Decimal("100.00"),
        "decision_atr_1m": Decimal("1.00"),
        "price_step": Decimal("0.01"),
        "maximum_acceptable_price": Decimal("100.00"),
        "authorized_base": Decimal("1.00"),
        "cumulative_filled_base": Decimal("0"),
        "reprice_count": 0,
        "opposing_signal": False,
        "risk_allowed": True,
    }
    values.update({key: Decimal(value) if isinstance(value, str) else value for key, value in changes.items()})
    return DownOnlyMakerSnapshot(**values)


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("total_max_age_seconds", 0, "total_max_age_seconds"),
        ("max_reprices", -1, "max_reprices"),
        ("cooldown_seconds", -1, "cooldown_seconds"),
        ("min_ticks", 0, "min_ticks"),
        ("atr_fraction", Decimal("0"), "atr_fraction"),
        ("activation_bound_pct", Decimal("NaN"), "activation_bound_pct"),
        ("activation_bound_pct", Decimal("1"), "activation_bound_pct"),
    ],
)
def test_rejects_invalid_parameters(field, value, message) -> None:
    with pytest.raises(DownOnlyMakerInputError, match=message):
        decide_down_only_maker(parameters(**{field: value}), snapshot())


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("now", Decimal("NaN"), "now"),
        ("best_bid", Decimal("0"), "best_bid"),
        ("best_ask", Decimal("Infinity"), "best_ask"),
        ("decision_atr_1m", Decimal("0"), "decision_atr_1m"),
        ("price_step", Decimal("-0.01"), "price_step"),
        ("intent_created_at", Decimal("11"), "intent age"),
        ("order_created_at", Decimal("11"), "order age"),
        ("reprice_count", -1, "reprice_count"),
        ("cumulative_filled_base", Decimal("1.01"), "authorized_base"),
        ("current_order_price", Decimal("100.01"), "maximum acceptable"),
    ],
)
def test_rejects_invalid_snapshot(field, value, message) -> None:
    with pytest.raises(DownOnlyMakerInputError, match=message):
        decide_down_only_maker(parameters(), snapshot(**{field: value}))


def test_rejects_crossed_book() -> None:
    with pytest.raises(DownOnlyMakerInputError, match="crossed book"):
        decide_down_only_maker(parameters(), snapshot(best_bid="100.01", best_ask="100.00"))


def test_rejects_locked_book_that_cannot_produce_a_maker_safe_bid() -> None:
    with pytest.raises(DownOnlyMakerInputError, match="crossed or locked"):
        decide_down_only_maker(parameters(), snapshot(best_bid="99.90", best_ask="99.90"))


def test_risk_block_has_highest_terminal_precedence() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(now="301", best_bid="98.00", best_ask="98.01", risk_allowed=False),
    )
    assert result == DownOnlyMakerDecision(
        action=DownOnlyMakerAction.CANCEL_TERMINAL,
        reason=DownOnlyMakerReason.RISK_BLOCKED,
    )


def test_opposing_signal_cancels_terminally() -> None:
    result = decide_down_only_maker(parameters(), snapshot(opposing_signal=True))
    assert result.reason is DownOnlyMakerReason.OPPOSING_SIGNAL
    assert result.action is DownOnlyMakerAction.CANCEL_TERMINAL


def test_total_age_expiry_is_measured_from_original_intent() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(now="300", order_created_at="290", last_reprice_at="290", reprice_count=2),
    )
    assert result.reason is DownOnlyMakerReason.TOTAL_AGE_EXPIRED
    assert result.action is DownOnlyMakerAction.CANCEL_TERMINAL


def test_downward_bound_breach_cancels_terminally() -> None:
    result = decide_down_only_maker(parameters(), snapshot(best_bid="99.49", best_ask="99.50"))
    assert result.reason is DownOnlyMakerReason.DOWNWARD_BOUND_BREACHED
    assert result.action is DownOnlyMakerAction.CANCEL_TERMINAL


def test_exact_downward_activation_boundary_remains_valid() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(best_bid="99.49", best_ask="99.51", current_order_price="99.40"),
    )
    assert result.action is DownOnlyMakerAction.HOLD
    assert result.reason is DownOnlyMakerReason.RESTING


def test_upward_activation_bound_breach_cancels_terminally() -> None:
    result = decide_down_only_maker(parameters(), snapshot(best_bid="100.50", best_ask="100.51"))
    assert result.reason is DownOnlyMakerReason.UPWARD_BOUND_BREACHED
    assert result.action is DownOnlyMakerAction.CANCEL_TERMINAL


def test_partial_fill_disables_reprice_and_holds_inside_activation_bound() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(best_bid="99.90", best_ask="99.91", cumulative_filled_base="0.25"),
    )
    assert result.reason is DownOnlyMakerReason.PARTIAL_FILL_RESTING
    assert result.action is DownOnlyMakerAction.HOLD


def test_partial_fill_does_not_expire_after_thirty_seconds() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(now="30", best_bid="99.90", best_ask="99.91", cumulative_filled_base="0.25"),
    )
    assert result.reason is DownOnlyMakerReason.PARTIAL_FILL_RESTING
    assert result.action is DownOnlyMakerAction.HOLD


def test_partial_fill_outside_activation_bound_cancels_only_remaining_acquisition() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(now="31", best_bid="99.49", best_ask="99.50", cumulative_filled_base="0.25"),
    )
    assert result.reason is DownOnlyMakerReason.DOWNWARD_BOUND_BREACHED
    assert result.action is DownOnlyMakerAction.CANCEL_TERMINAL


def test_marketable_resting_buy_holds_for_fill_reconciliation() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(
            now="31",
            initial_best_bid="722.7",
            initial_best_ask="723.0",
            best_bid="722.4",
            best_ask="722.5",
            current_order_price="722.7",
            maximum_acceptable_price="722.7",
            decision_atr_1m="0.204858129220430181860",
            price_step="0.1",
            authorized_base="0.024292",
        ),
    )
    assert result.reason is DownOnlyMakerReason.TOUCH_PENDING
    assert result.action is DownOnlyMakerAction.HOLD


def test_qualifying_decline_selects_strictly_lower_quantized_bid() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(
            now="12",
            best_bid="99.905",
            best_ask="100.01",
            current_order_price="100.00",
            decision_atr_1m="1.00",
            price_step="0.01",
        ),
    )
    assert result == DownOnlyMakerDecision(
        action=DownOnlyMakerAction.CANCEL_FOR_REPRICE,
        reason=DownOnlyMakerReason.DOWNWARD_REPRICE,
        target_price=Decimal("99.90"),
    )


def test_wider_current_spread_does_not_deadlock_a_no_touch_reprice() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(now="12", best_bid="99.90", best_ask="100.02"),
    )
    assert result.action is DownOnlyMakerAction.CANCEL_FOR_REPRICE
    assert result.target_price == Decimal("99.90")


def test_rising_price_never_produces_an_upward_target() -> None:
    result = decide_down_only_maker(parameters(), snapshot(now="20", best_bid="100.20", best_ask="100.21"))
    assert result.action is DownOnlyMakerAction.HOLD
    assert result.target_price is None


def test_cooldown_prevents_rapid_reprice() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(
            now="12",
            last_reprice_at="10",
            best_bid="99.70",
            best_ask="99.91",
            current_order_price="99.80",
            reprice_count=1,
        ),
    )
    assert result.reason is DownOnlyMakerReason.RESTING
    assert result.action is DownOnlyMakerAction.HOLD


def test_reprice_budget_exhaustion_holds_valid_resting_order() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(best_bid="99.70", best_ask="99.91", current_order_price="99.80", reprice_count=3),
    )
    assert result.reason is DownOnlyMakerReason.REPRICE_BUDGET_EXHAUSTED
    assert result.action is DownOnlyMakerAction.HOLD


def test_unqualified_decline_remains_live_after_thirty_seconds() -> None:
    result = decide_down_only_maker(parameters(), snapshot(now="30", best_bid="99.98", best_ask="100.01"))
    assert result.reason is DownOnlyMakerReason.RESTING
    assert result.action is DownOnlyMakerAction.HOLD


def test_dataclasses_are_immutable() -> None:
    with pytest.raises(AttributeError):
        parameters().rest_seconds = 31
    with pytest.raises(AttributeError):
        replace(snapshot(), best_bid=Decimal("99.99")).best_bid = Decimal("99.98")


@pytest.mark.parametrize("bid,ask,expected", [
    ("722.9", "723.1", DownOnlyMakerReason.RESTING),
    ("722.4", "722.5", DownOnlyMakerReason.TOUCH_PENDING),
    ("723.1", "723.2", DownOnlyMakerReason.UPWARD_BOUND_BREACHED),
    ("722.2", "722.3", DownOnlyMakerReason.DOWNWARD_BOUND_BREACHED),
])
def test_bnb_point_zero_five_percent_midpoint_scenarios(bid, ask, expected):
    result = decide_down_only_maker(parameters(activation_bound_pct=Decimal("0.0005")), snapshot(
        now="31", initial_best_bid="722.8", initial_best_ask="723.1",
        best_bid=bid, best_ask=ask, current_order_price="722.7",
        maximum_acceptable_price="722.7", price_step="0.1",
        decision_atr_1m="0.204858129220430181860",
    ))
    assert result.reason is expected
