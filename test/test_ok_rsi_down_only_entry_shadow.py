import json
import logging
from decimal import Decimal

import pytest

from controllers.directional_trading.ok_rsi_core.down_only_entry import DownOnlyMakerParameters
from controllers.directional_trading.ok_rsi_core.down_only_entry_shadow import DownOnlyMakerShadowTracker


def parameters() -> DownOnlyMakerParameters:
    return DownOnlyMakerParameters(
        total_max_age_seconds=300,
        max_reprices=3,
        cooldown_seconds=5,
        min_ticks=3,
        atr_fraction=Decimal("0.05"),
        activation_bound_pct=Decimal("0.005"),
    )


def started_tracker() -> DownOnlyMakerShadowTracker:
    tracker = DownOnlyMakerShadowTracker(parameters())
    tracker.start(
        intent_id="shadow:executor-1",
        now=Decimal("0"),
        initial_price=Decimal("100"),
        best_bid=Decimal("100"),
        best_ask=Decimal("100.01"),
        atr_1m=Decimal("1"),
        price_step=Decimal("0.01"),
        authorized_base=Decimal("1"),
    )
    return tracker


def test_shadow_reprice_mutates_only_virtual_state() -> None:
    tracker = started_tracker()
    events = tracker.advance(
        now=Decimal("10"),
        best_bid=Decimal("99.90"),
        best_ask=Decimal("100.01"),
        opposing_signal=False,
        risk_allowed=True,
    )
    assert events[-1]["action"] == "replace_lower"
    assert tracker.snapshot()["current_order_price"] == "99.90"
    assert tracker.snapshot()["reprice_count"] == 1


def test_start_rejects_second_active_intent() -> None:
    tracker = started_tracker()
    with pytest.raises(ValueError, match="active shadow intent"):
        tracker.start(
            intent_id="shadow:executor-2",
            now=Decimal("1"),
            initial_price=Decimal("100"),
            best_bid=Decimal("100"),
            best_ask=Decimal("100.01"),
            atr_1m=Decimal("1"),
            price_step=Decimal("0.01"),
            authorized_base=Decimal("1"),
        )


def test_rising_price_inside_bound_holds_without_upward_reprice() -> None:
    tracker = started_tracker()
    events = tracker.advance(
        now=Decimal("30"),
        best_bid=Decimal("100.20"),
        best_ask=Decimal("100.21"),
        opposing_signal=False,
        risk_allowed=True,
    )
    assert events[-1]["action"] == "hold"
    assert events[-1]["reason"] == "resting"
    assert tracker.snapshot()["current_order_price"] == "100"


def test_three_replacements_then_budget_exhaustion_holds() -> None:
    tracker = started_tracker()
    for now, bid, ask in (
        ("10", "99.90", "100.01"),
        ("20", "99.80", "99.91"),
        ("30", "99.70", "99.81"),
    ):
        tracker.advance(
            now=Decimal(now),
            best_bid=Decimal(bid),
            best_ask=Decimal(ask),
            opposing_signal=False,
            risk_allowed=True,
        )
    events = tracker.advance(
        now=Decimal("40"),
        best_bid=Decimal("99.60"),
        best_ask=Decimal("99.71"),
        opposing_signal=False,
        risk_allowed=True,
    )
    assert tracker.snapshot()["reprice_count"] == 3
    assert events[-1]["reason"] == "reprice_budget_exhausted"
    assert events[-1]["action"] == "hold"
    assert tracker.snapshot()["terminal_reason"] is None


def test_total_age_expiry_survives_lease_renewal() -> None:
    tracker = started_tracker()
    tracker.advance(
        now=Decimal("290"),
        best_bid=Decimal("99.90"),
        best_ask=Decimal("100.01"),
        opposing_signal=False,
        risk_allowed=True,
    )
    events = tracker.advance(
        now=Decimal("300"),
        best_bid=Decimal("99.90"),
        best_ask=Decimal("99.91"),
        opposing_signal=False,
        risk_allowed=True,
    )
    assert events[-1]["reason"] == "total_age_expired"


def test_downward_bound_is_terminal() -> None:
    tracker = started_tracker()
    events = tracker.advance(
        now=Decimal("10"),
        best_bid=Decimal("99.49"),
        best_ask=Decimal("100.01"),
        opposing_signal=False,
        risk_allowed=True,
    )
    assert events[-1]["reason"] == "downward_bound_breached"


def test_touch_proxy_is_terminal_but_not_claimed_as_fill() -> None:
    tracker = started_tracker()
    events = tracker.advance(
        now=Decimal("2"),
        best_bid=Decimal("99.99"),
        best_ask=Decimal("100"),
        opposing_signal=False,
        risk_allowed=True,
    )
    assert events[-1]["reason"] == "touch_proxy"
    assert events[-1]["queue_fill_status"] == "UNAVAILABLE"
    assert tracker.snapshot()["cumulative_filled_base"] == "UNAVAILABLE"


def test_restart_abandonment_is_explicit_and_terminal() -> None:
    tracker = started_tracker()
    event = tracker.abandon_for_restart(now=Decimal("15"))
    assert event["reason"] == "restart_abandoned"
    assert tracker.snapshot()["terminal_reason"] == "restart_abandoned"


def test_all_emitted_decimals_are_strings() -> None:
    tracker = started_tracker()
    event = tracker.snapshot()["last_event"]
    assert event["at"] == "0"
    assert event["current_order_price"] == "100"
    assert event["queue_fill_status"] == "UNAVAILABLE"


def test_shadow_log_suffix_is_parseable_json_and_not_repeated_for_same_transition(caplog) -> None:
    with caplog.at_level(logging.INFO):
        tracker = started_tracker()
        tracker.advance(
            now=Decimal("10"),
            best_bid=Decimal("99.90"),
            best_ask=Decimal("100.01"),
            opposing_signal=False,
            risk_allowed=True,
        )
    messages = [record.message for record in caplog.records if "ENTRY_ACQUISITION_SHADOW" in record.message]
    assert len(messages) == 2
    payload = json.loads(messages[-1].split("ENTRY_ACQUISITION_SHADOW ", 1)[1])
    assert payload["action"] == "replace_lower"
    assert payload["current_order_price"] == "99.90"
    assert payload["queue_fill_status"] == "UNAVAILABLE"
