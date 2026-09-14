import asyncio
import json
import logging
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import (
    DownOnlyMakerPositionExecutor,
    DownOnlyMakerPositionExecutorConfig,
)
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import OrderCancelledEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import TrackedOrder


def make_config(**changes) -> DownOnlyMakerPositionExecutorConfig:
    values = {
        "id": "entry-intent",
        "timestamp": 0.0,
        "controller_id": "ok-rsi",
        "connector_name": "okx",
        "trading_pair": "ETH-USDC",
        "side": TradeType.BUY,
        "entry_price": Decimal("100"),
        "amount": Decimal("1"),
        "leverage": 1,
        "triple_barrier_config": TripleBarrierConfig(
            open_order_type=OrderType.LIMIT_MAKER,
            open_order_limit_max_age_seconds=0,
        ),
        "activation_bounds": [Decimal("0.005")],
        "entry_order_policy": "single_attempt",
        "decision_best_bid": Decimal("100"),
        "decision_best_ask": Decimal("100.01"),
        "decision_atr_1m": Decimal("1"),
        "decision_price_step": Decimal("0.01"),
        "maximum_acceptable_price": Decimal("100"),
        "entry_total_max_age_seconds": 300,
        "entry_max_downward_reprices": 3,
        "entry_reprice_cooldown_seconds": 5,
        "entry_reprice_min_ticks": 3,
        "entry_reprice_atr_fraction": Decimal("0.05"),
    }
    values.update(changes)
    return DownOnlyMakerPositionExecutorConfig(**values)


def make_executor(
    *, bid=Decimal("100"), ask=Decimal("100.01"), config=None, strategy_timestamp=0.0
):
    strategy = MagicMock(spec=StrategyV2Base)
    strategy.current_timestamp = strategy_timestamp
    connector = MagicMock()
    connector.supports_terminal_fill_reconciliation = True
    connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
    strategy.connectors = {"okx": connector}
    rule = TradingRule(
        "ETH-USDC",
        min_order_size=Decimal("0.01"),
        min_price_increment=Decimal("0.01"),
        min_base_amount_increment=Decimal("0.01"),
        min_notional_size=Decimal("1"),
    )
    prices = {PriceType.BestBid: bid, PriceType.BestAsk: ask, PriceType.MidPrice: (bid + ask) / 2}
    with patch.object(DownOnlyMakerPositionExecutor, "get_trading_rules", return_value=rule):
        executor = DownOnlyMakerPositionExecutor(strategy, config or make_config())
    executor._status = RunnableStatus.RUNNING
    executor.get_price = MagicMock(side_effect=lambda connector_name, trading_pair, price_type: prices[price_type])
    executor.get_in_flight_order = MagicMock(return_value=None)
    executor.place_order = MagicMock(side_effect=["parent", "child-1", "child-2", "child-3"])
    return executor, connector, prices


def tracked_order(
    order_id="parent",
    *,
    price="100",
    fill="0",
    state=OrderState.OPEN,
    amount="1",
    fee=None,
    creation_timestamp=0,
):
    order = InFlightOrder(
        client_order_id=order_id,
        exchange_order_id=f"exchange-{order_id}",
        trading_pair="ETH-USDC",
        order_type=OrderType.LIMIT_MAKER,
        trade_type=TradeType.BUY,
        amount=Decimal(amount),
        price=Decimal(price),
        creation_timestamp=creation_timestamp,
        initial_state=state,
    )
    if Decimal(fill) > 0:
        order.update_with_trade_update(TradeUpdate(
            trade_id=f"fill-{order_id}",
            client_order_id=order_id,
            exchange_order_id=f"exchange-{order_id}",
            trading_pair="ETH-USDC",
            fill_price=Decimal(price),
            fill_base_amount=Decimal(fill),
            fill_quote_amount=Decimal(fill) * Decimal(price),
            fee=fee or AddedToCostTradeFee(),
            fill_timestamp=1,
        ))
    tracked = TrackedOrder(order_id=order_id)
    tracked.order = order
    return tracked


def install_terminal_reconciliation(connector, tracked):
    async def terminal_status(tracked_order):
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=12,
            new_state=OrderState.CANCELED,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
        )

    connector._request_order_status = AsyncMock(side_effect=terminal_status)


async def finish_reconciliation(executor):
    await executor._reconcile_expired_entry()
    await asyncio.sleep(0)
    await executor._reconcile_expired_entry()


def request_reprice(executor, prices):
    executor._open_order = tracked_order()
    executor._entry_submitted = True
    executor._strategy.current_timestamp = 10
    prices[PriceType.BestBid] = Decimal("99.90")
    prices[PriceType.BestAsk] = Decimal("100.01")
    executor.control_open_order()


def test_initial_submission_is_one_maker_buy() -> None:
    executor, _, _ = make_executor()
    executor.control_open_order()
    executor.control_open_order()
    executor.place_order.assert_called_once_with(
        connector_name="okx",
        trading_pair="ETH-USDC",
        order_type=OrderType.LIMIT_MAKER,
        amount=Decimal("1"),
        price=Decimal("100"),
        side=TradeType.BUY,
        position_action=PositionAction.OPEN,
    )


def test_entry_acquisition_snapshot_is_stable_and_json_native() -> None:
    executor, _, _ = make_executor()
    executor._open_order = tracked_order()
    executor._entry_submitted = True
    executor.control_open_order()
    snapshot = executor.get_custom_info()["entry_acquisition"]
    assert {
        key: snapshot[key]
        for key in (
            "policy",
            "intent_id",
            "state",
            "terminal_reason",
            "initial_price",
            "current_order_price",
            "maximum_acceptable_price",
            "decision_atr_1m",
            "authorized_base",
            "cumulative_filled_base",
            "remaining_base",
            "reprice_count",
            "activation_bound_pct",
            "total_max_age_seconds",
            "order_ids",
            "queue_fill_status",
        )
    } == {
        "policy": "down_only_maker_v1",
        "intent_id": "entry-intent",
        "state": "resting",
        "terminal_reason": None,
        "initial_price": "100",
        "current_order_price": "100",
        "maximum_acceptable_price": "100",
        "decision_atr_1m": "1",
        "authorized_base": "1",
        "cumulative_filled_base": "0",
        "remaining_base": "1",
        "reprice_count": 0,
        "activation_bound_pct": "0.005",
        "total_max_age_seconds": 300,
        "order_ids": ["parent"],
        "queue_fill_status": "UNAVAILABLE",
    }
    json.dumps(snapshot)


def test_fractional_config_timestamp_with_whole_second_order_clock_keeps_entry_resting() -> None:
    config = make_config(timestamp=1789357441.4710643)
    executor, _, _ = make_executor(config=config, strategy_timestamp=1789357441)
    executor._open_order = tracked_order(creation_timestamp=1789357441)
    executor._entry_submitted = True
    executor._strategy.current_timestamp = 1789357442

    executor.control_open_order()

    executor._strategy.cancel.assert_not_called()
    acquisition = executor.get_custom_info()["entry_acquisition"]
    assert acquisition["state"] == "resting"
    assert acquisition["terminal_reason"] is None


def test_valid_untouched_entry_remains_live_after_thirty_seconds() -> None:
    executor, _, prices = make_executor()
    executor._open_order = tracked_order()
    executor._entry_submitted = True
    executor._strategy.current_timestamp = 31
    prices[PriceType.BestBid] = Decimal("100.20")
    prices[PriceType.BestAsk] = Decimal("100.21")

    executor.control_open_order()

    executor._strategy.cancel.assert_not_called()
    acquisition = executor.get_custom_info()["entry_acquisition"]
    assert acquisition["state"] == "resting"
    assert acquisition["terminal_reason"] is None


def test_bnb_touch_after_thirty_seconds_waits_for_exchange_fill_update() -> None:
    config = make_config(
        trading_pair="BNB-USDC",
        entry_price=Decimal("722.7"),
        maximum_acceptable_price=Decimal("722.7"),
        decision_best_bid=Decimal("722.7"),
        decision_best_ask=Decimal("723.0"),
        decision_atr_1m=Decimal("0.204858129220430181860"),
        decision_price_step=Decimal("0.1"),
        amount=Decimal("0.024292"),
    )
    executor, _, prices = make_executor(config=config)
    executor._open_order = tracked_order(
        price="722.7",
        amount="0.024292",
        creation_timestamp=0,
    )
    executor._entry_submitted = True
    executor._strategy.current_timestamp = 31
    prices[PriceType.BestBid] = Decimal("722.4")
    prices[PriceType.BestAsk] = Decimal("722.5")

    executor.control_open_order()

    executor._strategy.cancel.assert_not_called()
    acquisition = executor.get_custom_info()["entry_acquisition"]
    assert acquisition["state"] == "touch_pending"
    assert acquisition["terminal_reason"] is None


def test_cumulative_child_fills_are_monotonic_and_duplicate_trade_ids_are_idempotent() -> None:
    executor, _, _ = make_executor()
    parent = tracked_order(fill="0.4", state=OrderState.CANCELED)
    child = tracked_order("child-1", price="99.90", fill="0.3", state=OrderState.PARTIALLY_FILLED, amount="0.6")
    executor._completed_open_orders = [parent]
    executor._open_order = child

    first = executor.get_custom_info()["entry_acquisition"]
    assert first["cumulative_filled_base"] == "0.7"
    assert first["remaining_base"] == "0.3"
    duplicate = next(iter(child.order.order_fills.values()))
    assert child.order.update_with_trade_update(duplicate) is False
    assert executor.get_custom_info()["entry_acquisition"]["cumulative_filled_base"] == "0.7"

    child.order.update_with_trade_update(TradeUpdate(
        trade_id="fill-child-1-later",
        client_order_id="child-1",
        exchange_order_id="exchange-child-1",
        trading_pair="ETH-USDC",
        fill_price=Decimal("99.90"),
        fill_base_amount=Decimal("0.2"),
        fill_quote_amount=Decimal("19.98"),
        fee=AddedToCostTradeFee(),
        fill_timestamp=2,
    ))
    second = executor.get_custom_info()["entry_acquisition"]
    assert second["cumulative_filled_base"] == "0.9"
    assert second["remaining_base"] == "0.1"


def test_base_fee_reduces_managed_inventory_once_and_unknown_fee_marks_economics_unavailable() -> None:
    executor, _, _ = make_executor()
    executor._open_order = tracked_order(
        fill="0.4",
        fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="ETH", amount=Decimal("0.001"))]),
    )
    first = executor.get_custom_info()["entry_acquisition"]
    second = executor.get_custom_info()["entry_acquisition"]
    assert executor.open_filled_amount == Decimal("0.399")
    assert first["cumulative_filled_base"] == second["cumulative_filled_base"] == "0.4"
    assert first["fee_economics_status"] == "AVAILABLE"

    executor._open_order = tracked_order(
        fill="0.4",
        fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="BNB", amount=Decimal("0.001"))]),
    )
    unavailable = executor.get_custom_info()["entry_acquisition"]
    assert unavailable["fee_economics_status"] == "UNAVAILABLE"
    assert unavailable["fee_errors"] == ["unsupported_fee_asset:BNB"]


def test_active_decision_log_has_parseable_bounded_schema(caplog) -> None:
    executor, _, prices = make_executor()
    with caplog.at_level(logging.INFO):
        request_reprice(executor, prices)
    messages = [record.message for record in caplog.records if "ENTRY_ACQUISITION_DECISION" in record.message]
    assert len(messages) == 1
    payload = json.loads(messages[0].split("ENTRY_ACQUISITION_DECISION ", 1)[1])
    assert payload["action"] == "cancel_for_reprice"
    assert payload["candidate_price"] == "99.90"
    assert payload["activation_bound_pct"] == "0.005"
    assert payload["activation_lower_price"] == "99.500"
    assert payload["activation_upper_price"] == "100.500"
    assert payload["queue_fill_status"] == "UNAVAILABLE"
    assert "connectors" not in payload


def test_rising_price_never_requests_cancel_or_upward_reprice() -> None:
    executor, _, prices = make_executor()
    executor._open_order = tracked_order()
    executor._entry_submitted = True
    executor._strategy.current_timestamp = 10
    prices[PriceType.BestBid] = Decimal("100.20")
    prices[PriceType.BestAsk] = Decimal("100.21")
    executor.control_open_order()
    executor._strategy.cancel.assert_not_called()
    assert executor._entry_pending_target is None


def test_qualified_lower_target_requests_cancel_exactly_once() -> None:
    executor, _, prices = make_executor()
    request_reprice(executor, prices)
    executor.control_open_order()
    executor._strategy.cancel.assert_called_once_with(
        connector_name="okx", trading_pair="ETH-USDC", order_id="parent"
    )
    assert executor._entry_pending_target == Decimal("99.90")


@pytest.mark.asyncio
async def test_confirmed_zero_fill_cancel_waits_for_reconciliation_then_places_one_child() -> None:
    executor, connector, prices = make_executor()
    request_reprice(executor, prices)
    parent = executor._open_order
    parent.order.current_state = OrderState.CANCELED
    install_terminal_reconciliation(connector, parent)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    executor.control_open_order()
    executor.place_order.assert_not_called()
    await finish_reconciliation(executor)
    executor.control_open_order()
    executor.control_open_order()
    executor.place_order.assert_called_once()
    assert executor.place_order.call_args.kwargs["price"] == Decimal("99.90")
    assert executor.place_order.call_args.kwargs["amount"] == Decimal("1")


@pytest.mark.asyncio
async def test_reprice_child_uses_the_quantized_remaining_authorization() -> None:
    executor, connector, prices = make_executor(config=make_config(amount=Decimal("1.003")))
    request_reprice(executor, prices)
    parent = executor._open_order
    parent.order.current_state = OrderState.CANCELED
    executor._open_order = parent
    connector.quantize_order_amount.side_effect = (
        lambda trading_pair, amount: (amount / Decimal("0.01")).to_integral_value(rounding="ROUND_FLOOR")
        * Decimal("0.01")
    )
    install_terminal_reconciliation(connector, parent)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    await finish_reconciliation(executor)
    executor.control_open_order()

    assert executor.place_order.call_args.kwargs["amount"] == Decimal("1.00")


@pytest.mark.asyncio
async def test_fill_during_cancel_suppresses_child_and_retains_inventory() -> None:
    executor, connector, prices = make_executor()
    request_reprice(executor, prices)
    parent = tracked_order(fill="0.4", state=OrderState.CANCELED)
    executor._open_order = parent
    install_terminal_reconciliation(connector, parent)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    await finish_reconciliation(executor)
    executor.control_open_order()
    executor.place_order.assert_not_called()
    assert executor.open_filled_amount == Decimal("0.4")
    assert executor.status is RunnableStatus.RUNNING
    assert executor.get_custom_info()["entry_acquisition"]["terminal_reason"] == "partial_fill"


@pytest.mark.asyncio
async def test_any_gross_fill_suppresses_child_even_if_base_fee_consumes_the_net_inventory() -> None:
    executor, connector, prices = make_executor()
    request_reprice(executor, prices)
    parent = tracked_order(
        fill="0.001",
        state=OrderState.CANCELED,
        fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="ETH", amount=Decimal("0.001"))]),
    )
    executor._open_order = parent
    install_terminal_reconciliation(connector, parent)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    await finish_reconciliation(executor)
    executor.control_open_order()

    executor.place_order.assert_not_called()
    assert executor.open_filled_amount == Decimal("0")
    assert executor.get_custom_info()["entry_acquisition"]["terminal_reason"] == "partial_fill"


def test_cancel_uncertainty_blocks_replacement() -> None:
    executor, _, prices = make_executor()
    request_reprice(executor, prices)
    executor.process_order_canceled_event(0, executor.connectors["okx"], OrderCancelledEvent(12, "unknown"))
    executor.control_open_order()
    executor.place_order.assert_not_called()
    assert executor._open_order.order_id == "parent"


@pytest.mark.parametrize(
    "timestamp,bid,expected",
    [
        (300, Decimal("99.90"), "total_age_expired"),
        (10, Decimal("98.9"), "downward_bound_breached"),
    ],
)
def test_hard_rails_cancel_terminally(timestamp, bid, expected) -> None:
    executor, _, prices = make_executor()
    executor._open_order = tracked_order()
    executor._entry_submitted = True
    executor._strategy.current_timestamp = timestamp
    prices[PriceType.BestBid] = bid
    prices[PriceType.BestAsk] = Decimal("100.01")
    executor.control_open_order()
    executor._strategy.cancel.assert_called_once()
    state = executor.get_custom_info()["entry_acquisition"]
    assert state["terminal_reason"] == expected
    assert executor._entry_pending_target is None


def test_reprice_budget_exhaustion_holds_valid_order() -> None:
    executor, _, prices = make_executor()
    executor._entry_reprice_count = 3
    request_reprice(executor, prices)
    acquisition = executor.get_custom_info()["entry_acquisition"]
    assert acquisition["state"] == "reprice_budget_exhausted"
    assert acquisition["terminal_reason"] is None
    executor._strategy.cancel.assert_not_called()


@pytest.mark.asyncio
async def test_below_minimum_amount_ends_without_child() -> None:
    executor, connector, prices = make_executor(config=make_config(amount=Decimal("0.005")))
    request_reprice(executor, prices)
    parent = executor._open_order
    parent.order.amount = Decimal("0.005")
    parent.order.current_state = OrderState.CANCELED
    install_terminal_reconciliation(connector, parent)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    await finish_reconciliation(executor)
    executor.control_open_order()
    executor.place_order.assert_not_called()
    assert executor.get_custom_info()["entry_acquisition"]["terminal_reason"] == "remaining_below_minimum"


@pytest.mark.asyncio
async def test_duplicate_terminal_events_never_dispatch_second_child_or_sell() -> None:
    executor, connector, prices = make_executor()
    request_reprice(executor, prices)
    parent = executor._open_order
    parent.order.current_state = OrderState.CANCELED
    install_terminal_reconciliation(connector, parent)
    event = OrderCancelledEvent(12, "parent")
    executor.process_order_canceled_event(0, connector, event)
    await finish_reconciliation(executor)
    executor.control_open_order()
    executor.process_order_canceled_event(0, connector, event)
    executor.control_open_order()
    assert executor.place_order.call_count == 1
    executor._strategy.sell.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("time,bid,ask,reason", [
    (300, "99.9", "100.01", "total_age_expired"),
    (12, "101", "101.01", "upward_bound_breached"),
    (12, "99", "99.01", "downward_bound_breached"),
    (12, "99.8", "99.9", "submission_would_cross"),
])
async def test_replacement_rechecks_book_and_deadline_after_reconciliation(time, bid, ask, reason):
    executor, connector, prices = make_executor()
    request_reprice(executor, prices)
    parent = executor._open_order
    parent.order.current_state = OrderState.CANCELED
    install_terminal_reconciliation(connector, parent)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    await finish_reconciliation(executor)
    executor._strategy.current_timestamp = time
    prices[PriceType.BestBid], prices[PriceType.BestAsk] = Decimal(bid), Decimal(ask)
    executor.control_open_order()
    executor.place_order.assert_not_called()
    assert executor.get_custom_info()["entry_acquisition"]["terminal_reason"] == reason


def test_initial_submission_outside_bound_is_not_sent():
    executor, _, _ = make_executor(bid=Decimal("101"), ask=Decimal("101.01"))
    executor.control_open_order()
    executor.place_order.assert_not_called()
    assert executor.get_custom_info()["entry_acquisition"]["terminal_reason"] == "upward_bound_breached"


def test_fully_filled_entry_does_not_expire_at_three_hundred_seconds():
    executor, _, _ = make_executor()
    executor._open_order = tracked_order(fill="1", state=OrderState.FILLED)
    executor._strategy.current_timestamp = 301
    executor.control_open_order()
    executor._strategy.cancel.assert_not_called()
    assert executor.get_custom_info()["entry_acquisition"]["state"] == "managing_fill"


def test_filled_order_with_missing_trade_quantities_requires_reconciliation():
    executor, _, _ = make_executor()
    executor._open_order = tracked_order(fill="0.4", state=OrderState.FILLED)
    executor.control_open_order()
    assert executor.get_custom_info()["entry_acquisition"]["state"] == "reconciliation_required"


@pytest.mark.asyncio
async def test_fill_after_child_authorization_suppresses_replacement():
    executor, connector, prices = make_executor()
    request_reprice(executor, prices)
    parent = executor._open_order
    parent.order.current_state = OrderState.CANCELED
    install_terminal_reconciliation(connector, parent)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    await finish_reconciliation(executor)
    parent.order = tracked_order(fill="0.4", state=OrderState.CANCELED).order
    executor.control_open_order()
    executor.place_order.assert_not_called()
    assert executor.open_filled_amount == Decimal("0.4")
    assert executor.get_custom_info()["entry_acquisition"]["state"] == "managing_fill"
    executor.control_open_order()
    assert executor._entry_terminal_confirmed()
    assert executor.get_custom_info()["entry_acquisition"]["state"] == "managing_fill"
    assert executor.open_filled_amount == Decimal("0.4")


@pytest.mark.asyncio
async def test_full_fill_after_cancel_ack_supersedes_pending_reprice():
    executor, connector, prices = make_executor()
    request_reprice(executor, prices)
    executor._open_order.order.current_state = OrderState.CANCELED
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    executor._open_order.order = tracked_order(fill="1", state=OrderState.FILLED).order
    await executor._reconcile_expired_entry()
    executor.control_open_order()
    assert executor._entry_pending_target is None
    assert executor._entry_terminal_confirmed()
    assert executor.get_custom_info()["entry_acquisition"]["state"] == "managing_fill"
    executor.place_order.assert_not_called()
