"""Native fixture regressions for the follow-up review of consolidated PR #52."""
from decimal import Decimal

import pytest

from hummingbot.core.data_type.common import PriceType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.event.events import OrderCancelledEvent
from hummingbot.strategy_v2.models.base import RunnableStatus
from test.test_ok_rsi_down_only_entry_execution import (
    bind_controller,
    finish_reconciliation,
    install_terminal_reconciliation,
    make_executor,
    request_reprice,
    tracked_order,
)


def test_missing_receipt_deadline_keeps_submitted_identity():
    executor, _, _ = make_executor()
    executor.control_open_order()
    original = executor._open_order
    assert original is not None and original.order is None
    executor._strategy.current_timestamp = 900

    executor.control_open_order()

    assert executor._entry_stop_reason == "total_age_expired"
    assert executor._open_order is original
    assert executor.status is RunnableStatus.RUNNING
    assert executor.place_order.call_count == 1


def test_missing_receipt_pause_keeps_submitted_identity():
    executor, _, _ = make_executor()
    executor.control_open_order()
    bind_controller(executor, pause=True)

    executor.control_open_order()

    assert executor._entry_stop_reason == "risk_blocked"
    assert executor._open_order is not None
    assert executor.status is RunnableStatus.RUNNING
    assert executor.place_order.call_count == 1


def test_missing_local_handle_is_not_a_no_fill_outcome():
    executor, _, _ = make_executor(strategy_timestamp=900)
    executor._entry_submitted = True
    executor._open_order = None

    executor.control_open_order()

    assert executor._entry_stop_reason == "total_age_expired"
    assert executor._entry_lifecycle["state"] == "reconciliation_required"
    assert executor.status is RunnableStatus.RUNNING
    executor.place_order.assert_not_called()


def test_quote_lookup_error_does_not_skip_pending_deadline():
    executor, _, _ = make_executor(strategy_timestamp=900)
    executor._entry_submitted = True
    executor._open_order = tracked_order(state=OrderState.PENDING_CREATE)
    executor.get_price.side_effect = KeyError("book unavailable")

    executor.control_open_order()

    assert executor._entry_stop_reason == "total_age_expired"
    assert executor._open_order is not None
    assert executor.status is RunnableStatus.RUNNING
    executor.place_order.assert_not_called()


def test_initial_missing_book_does_not_hide_entry_pause():
    executor, _, prices = make_executor()
    bind_controller(executor, pause=True)
    prices[PriceType.BestAsk] = None

    executor.control_open_order()

    assert executor._entry_stop_reason == "risk_blocked"
    executor.place_order.assert_not_called()


def test_book_disappearance_during_control_does_not_report_resting():
    executor, _, prices = make_executor()
    executor._entry_submitted = True
    executor._open_order = tracked_order()
    reads = 0

    def quote(connector, pair, price_type):
        nonlocal reads
        reads += 1
        return prices[price_type] if reads <= 2 else None

    executor.get_price.side_effect = quote
    executor.control_open_order()

    assert executor._entry_lifecycle["state"] == "awaiting_order_update"
    assert executor._entry_stop_reason is None
    executor.place_order.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_source", ["0", "3", "27", "999"])
async def test_explicit_venue_cancel_veto_precedes_local_reprice(cancel_source):
    executor, connector, prices = make_executor()
    request_reprice(executor, prices)
    parent = executor._open_order
    parent.order.current_state = OrderState.CANCELED
    parent.order.cancel_source = cancel_source
    install_terminal_reconciliation(connector, parent)

    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    await finish_reconciliation(executor)
    executor.control_open_order()

    assert executor._entry_stop_reason is not None
    assert executor._entry_replacement_purpose is None
    executor.place_order.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("ask_after", [Decimal("99.90"), None])
async def test_final_child_book_check_defers_without_consuming_intent(ask_after):
    executor, connector, prices = make_executor(bid=Decimal("99.90"))
    executor._open_order = tracked_order(state=OrderState.CANCELED)
    executor._open_order.order.cancel_source = "31"
    executor._entry_submitted = True
    executor.place_order.side_effect = ["child-after-repair"]
    install_terminal_reconciliation(connector, executor._open_order)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    await finish_reconciliation(executor)
    reads = 0

    def quote(connector_name, pair, price_type):
        nonlocal reads
        reads += 1
        if reads <= 4:
            return prices[price_type]
        return Decimal("99.80") if price_type is PriceType.BestBid else ask_after

    executor.get_price.side_effect = quote
    executor.control_open_order()

    executor.place_order.assert_not_called()
    assert executor._entry_stop_reason is None
    assert executor._entry_child_submission_authorized

    executor.get_price.side_effect = lambda connector_name, pair, price_type: prices[price_type]
    executor.control_open_order()

    assert executor.place_order.call_count == 1
    assert executor.place_order.call_args.kwargs["price"] == Decimal("99.90")
    assert executor._open_order.order_id == "child-after-repair"
