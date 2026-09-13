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
        "rest_seconds": 30,
        "total_max_age_seconds": 300,
        "max_reprices": 3,
        "cooldown_seconds": 5,
        "min_ticks": 3,
        "atr_fraction": Decimal("0.05"),
        "max_downward_atr": Decimal("1.0"),
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
        ("rest_seconds", 0, "rest_seconds"),
        ("total_max_age_seconds", 0, "total_max_age_seconds"),
        ("max_reprices", -1, "max_reprices"),
        ("cooldown_seconds", -1, "cooldown_seconds"),
        ("min_ticks", 0, "min_ticks"),
        ("atr_fraction", Decimal("0"), "atr_fraction"),
        ("max_downward_atr", Decimal("NaN"), "max_downward_atr"),
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
    result = decide_down_only_maker(parameters(), snapshot(best_bid="98.99", best_ask="99.00"))
    assert result.reason is DownOnlyMakerReason.DOWNWARD_BOUND_BREACHED
    assert result.action is DownOnlyMakerAction.CANCEL_TERMINAL


def test_partial_fill_disables_reprice_and_holds_during_lease() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(best_bid="99.90", best_ask="99.91", cumulative_filled_base="0.25"),
    )
    assert result.reason is DownOnlyMakerReason.PARTIAL_FILL_RESTING
    assert result.action is DownOnlyMakerAction.HOLD


def test_partial_fill_cancels_only_remainder_after_lease() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(now="30", best_bid="99.90", best_ask="99.91", cumulative_filled_base="0.25"),
    )
    assert result.reason is DownOnlyMakerReason.REST_LEASE_EXPIRED
    assert result.action is DownOnlyMakerAction.CANCEL_TERMINAL


def test_qualifying_decline_selects_strictly_lower_quantized_bid() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(
            now="12",
            best_bid="99.905",
            best_ask="99.915",
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


def test_rising_price_never_produces_an_upward_target() -> None:
    result = decide_down_only_maker(parameters(), snapshot(now="20", best_bid="100.20", best_ask="100.21"))
    assert result.action is DownOnlyMakerAction.HOLD
    assert result.target_price is None


def test_cooldown_prevents_rapid_reprice() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(now="12", last_reprice_at="10", best_bid="99.90", best_ask="99.91", reprice_count=1),
    )
    assert result.reason is DownOnlyMakerReason.RESTING
    assert result.action is DownOnlyMakerAction.HOLD


def test_reprice_budget_exhaustion_is_terminal() -> None:
    result = decide_down_only_maker(
        parameters(),
        snapshot(best_bid="99.90", best_ask="99.91", reprice_count=3),
    )
    assert result.reason is DownOnlyMakerReason.REPRICE_BUDGET_EXHAUSTED
    assert result.action is DownOnlyMakerAction.CANCEL_TERMINAL


def test_unqualified_decline_expires_at_local_lease() -> None:
    result = decide_down_only_maker(parameters(), snapshot(now="30", best_bid="99.98", best_ask="99.99"))
    assert result.reason is DownOnlyMakerReason.REST_LEASE_EXPIRED
    assert result.action is DownOnlyMakerAction.CANCEL_TERMINAL


def test_dataclasses_are_immutable() -> None:
    with pytest.raises(AttributeError):
        parameters().rest_seconds = 31
    with pytest.raises(AttributeError):
        replace(snapshot(), best_bid=Decimal("99.99")).best_bid = Decimal("99.98")
