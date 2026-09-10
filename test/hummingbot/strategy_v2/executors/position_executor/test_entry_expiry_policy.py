"""Native entry events must never turn cancellation into a position liquidation."""

from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import (
    cancel,
    make_executor,
    tracked,
)
from unittest.mock import MagicMock

import pytest

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import MarketOrderFailureEvent
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


def single_attempt():
    executor = make_executor()
    executor.config = PositionExecutorConfig.model_validate({
        **executor.config.model_dump(), "entry_order_policy": "single_attempt",
    })
    executor.config.triple_barrier_config.open_order_limit_max_age_seconds = 30
    executor._strategy.current_timestamp = 31
    executor.place_order = MagicMock(return_value="unexpected-order")
    return executor


@pytest.mark.asyncio
async def test_expired_partial_entry_keeps_fill_under_management_without_market_sale():
    executor = single_attempt()
    entry = tracked("entry", TradeType.BUY, ".4", OrderState.PARTIALLY_FILLED)
    executor._open_order = entry

    await executor.control_task()
    assert executor.status == RunnableStatus.RUNNING
    assert executor.close_type is None
    cancel(executor, entry)
    await executor.control_task()
    await executor.control_task()

    executor.place_order.assert_not_called()
    assert executor.open_filled_amount == D(".4")
    assert executor.is_trading
    assert executor.get_custom_info()["entry_lifecycle"]["stop_reason"] == "entry_deadline"


@pytest.mark.asyncio
async def test_fill_arriving_during_expiry_is_managed_instead_of_liquidated():
    executor = single_attempt()
    entry = tracked("entry", TradeType.BUY, "0", OrderState.OPEN)
    executor._open_order = entry
    await executor.control_task()
    entry.order.update_with_trade_update(TradeUpdate(
        trade_id="late", client_order_id="entry", exchange_order_id="entry",
        trading_pair="ETH-USDT", fill_price=D("100"), fill_base_amount=D("1"),
        fill_quote_amount=D("100"), fee=AddedToCostTradeFee(), fill_timestamp=32,
    ))
    entry.order.current_state = OrderState.FILLED
    executor._strategy.current_timestamp = 42
    await executor.control_task()
    await executor.control_task()

    executor.place_order.assert_not_called()
    assert executor.open_filled_amount == D("1")
    assert executor.status == RunnableStatus.RUNNING
    assert executor.close_type is None


@pytest.mark.asyncio
async def test_exchange_cancellation_is_not_replaced_even_when_price_changes():
    executor = single_attempt()
    entry = tracked("entry", TradeType.BUY, "0", OrderState.OPEN)
    executor._open_order = entry
    executor._strategy.current_timestamp = 5
    cancel(executor, entry)
    executor.get_price.return_value = D("99")
    await executor.control_task()
    executor.control_open_order()
    executor.place_open_order()

    executor.place_order.assert_not_called()
    assert executor.is_closed
    assert executor.get_custom_info()["entry_lifecycle"]["stop_reason"] == "exchange_canceled"


@pytest.mark.asyncio
async def test_partial_exchange_cancellation_never_tops_up_entry():
    executor = single_attempt()
    entry = tracked("entry", TradeType.BUY, ".4", OrderState.PARTIALLY_FILLED)
    executor._open_order = entry
    executor._strategy.current_timestamp = 5
    cancel(executor, entry)
    await executor.control_task()
    executor.place_open_order()

    executor.place_order.assert_not_called()
    assert executor.status == RunnableStatus.RUNNING
    assert executor.open_filled_amount == D(".4")


@pytest.mark.asyncio
async def test_expiry_keeps_unknown_order_active_and_reserved_until_terminal_event():
    executor = single_attempt()
    executor._open_order = tracked("entry", TradeType.BUY, "0", OrderState.OPEN)
    await executor.control_task()
    executor._strategy.current_timestamp = 300
    await executor.control_task()

    assert executor.is_active
    assert not executor.is_closed
    assert executor.config.amount == D("1")
    executor.place_order.assert_not_called()
    assert executor.get_custom_info()["entry_lifecycle"]["state"] == "cancel_pending"


@pytest.mark.asyncio
async def test_zero_fill_expiry_finishes_only_after_cancel_acknowledgement():
    executor = single_attempt()
    entry = tracked("entry", TradeType.BUY, "0", OrderState.OPEN)
    executor._open_order = entry
    await executor.control_task()
    assert not executor.is_closed
    cancel(executor, entry)
    await executor.control_task()
    assert executor.is_closed
    assert executor.close_type == CloseType.EXPIRED
    executor.place_order.assert_not_called()


@pytest.mark.asyncio
async def test_entry_failure_does_not_reissue_an_order():
    executor = single_attempt()
    entry = tracked("entry", TradeType.BUY, "0", OrderState.FAILED)
    executor._open_order = entry
    executor.process_order_failed_event(0, None, MarketOrderFailureEvent(4, "entry", OrderType.LIMIT_MAKER))
    await executor.control_task()
    executor.place_open_order()
    executor.place_order.assert_not_called()


@pytest.mark.asyncio
async def test_uncertain_failure_with_fill_does_not_exit_before_reconciliation():
    executor = single_attempt()
    executor._open_order = tracked("entry", TradeType.BUY, ".4", OrderState.FAILED)
    executor.process_order_failed_event(0, None, MarketOrderFailureEvent(4, "entry", OrderType.LIMIT_MAKER))
    executor.get_price.return_value = D("105")
    await executor.control_task()
    executor.get_price.return_value = D("103")
    await executor.control_task()
    await executor.control_task()
    executor.place_order.assert_not_called()
    assert executor.is_active
    assert executor.get_custom_info()["entry_lifecycle"]["state"] == "reconciliation_required"


@pytest.mark.asyncio
async def test_partial_near_full_is_not_terminal_until_exchange_confirms_remainder():
    executor = single_attempt()
    # InFlightOrder.is_done uses float isclose, which can consider this remaining
    # quantity done even though the exchange still reports PARTIALLY_FILLED.
    executor._open_order = tracked("entry", TradeType.BUY, ".9999999999", OrderState.PARTIALLY_FILLED)
    await executor.control_task()
    assert executor.get_custom_info()["entry_lifecycle"]["state"] == "cancel_pending"
    executor.place_order.assert_not_called()


@pytest.mark.asyncio
async def test_fill_receipt_survives_handoff_without_double_counting_scalar_volume():
    executor = single_attempt()
    executor._open_order = tracked("entry", TradeType.BUY, ".4", OrderState.CANCELED)
    executor.early_stop(keep_position=True)
    await executor.control_task()
    info = executor.executor_info
    assert info.custom_info["execution_receipt"]["entry_gross_quote"] == D("40")
    assert info.custom_info["execution_receipt"]["remaining_owned_base"] == D(".4")
    assert info.custom_info["execution_receipt"]["accounting_owner"] == "held_position"
    assert info.filled_amount_quote == D("0")


@pytest.mark.asyncio
async def test_retained_partial_fill_still_obeys_a_separate_trailing_exit():
    executor = single_attempt()
    entry = tracked("entry", TradeType.BUY, ".4", OrderState.PARTIALLY_FILLED)
    executor._open_order = entry
    await executor.control_task()
    cancel(executor, entry)
    await executor.control_task()
    executor.get_price.return_value = D("105")
    await executor.control_task()
    executor.get_price.return_value = D("103")
    await executor.control_task()
    await executor.control_task()

    assert executor.close_type == CloseType.TRAILING_STOP
    assert executor.place_order.call_args.kwargs["side"] == TradeType.SELL
    assert executor.place_order.call_args.kwargs["amount"] == D(".4")
    assert executor.place_order.call_args.kwargs["order_type"] == OrderType.LIMIT
    assert executor.get_custom_info()["entry_lifecycle"]["stop_reason"] == "entry_deadline"


@pytest.mark.asyncio
async def test_sell_uses_largest_owned_lot_not_whole_dollars_or_wallet_inventory():
    executor = single_attempt()
    executor._open_order = tracked("entry", TradeType.BUY, ".419", OrderState.CANCELED)
    # The fixture wallet contains 10 ETH, but this position owns only .419 ETH.
    executor.connectors["okx"].quantize_order_amount.side_effect = lambda trading_pair, amount: amount
    executor.place_close_order_and_cancel_open_orders(CloseType.TRAILING_STOP)
    assert executor.place_order.call_args.kwargs["amount"] == D(".41")
    assert executor.get_custom_info()["close_quantity"]["residual_base"] == D(".009")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unavailable_balance", "quantize_error", "quantize_rounds_up"])
async def test_sell_blocks_on_uncertain_balance_or_invalid_quantization(failure):
    executor = single_attempt()
    executor._open_order = tracked("entry", TradeType.BUY, ".419", OrderState.CANCELED)
    if failure == "unavailable_balance":
        executor.connectors["okx"].get_available_balance.return_value = D("NaN")
    elif failure == "quantize_error":
        executor.connectors["okx"].quantize_order_amount.side_effect = ValueError("rules unavailable")
    else:
        executor.connectors["okx"].quantize_order_amount.side_effect = lambda trading_pair, amount: D(".42")
    executor.place_close_order_and_cancel_open_orders(CloseType.TRAILING_STOP)
    executor.place_order.assert_not_called()
    assert executor.get_custom_info()["close_quantity"]["blocked_reason"]
    assert executor.amount_to_close == D(".419")


@pytest.mark.asyncio
async def test_locked_balance_does_not_classify_tradeable_owned_inventory_as_dust():
    executor = single_attempt()
    executor._open_order = tracked("entry", TradeType.BUY, ".419", OrderState.CANCELED)
    executor.connectors["okx"].get_available_balance.return_value = D("0")
    executor.close_type = CloseType.TRAILING_STOP
    executor._status = RunnableStatus.SHUTTING_DOWN
    await executor.control_task()
    executor.place_order.assert_not_called()
    assert executor.close_type == CloseType.TRAILING_STOP
    assert not executor.is_closed
    assert executor.get_custom_info()["close_quantity"]["blocked_reason"] == "available_balance_below_minimum"


@pytest.mark.asyncio
async def test_quantized_owned_remainder_below_min_notional_transfers_exact_dust():
    executor = single_attempt()
    executor._open_order = tracked("entry", TradeType.BUY, ".019", OrderState.CANCELED)
    executor.trading_rules.min_notional_size = D("1.5")
    executor.close_type = CloseType.TRAILING_STOP
    executor._status = RunnableStatus.SHUTTING_DOWN
    await executor.control_task()
    await executor.control_task()
    executor.place_order.assert_not_called()
    assert executor.close_type == CloseType.POSITION_HOLD
    assert executor.is_closed
    assert D(executor._held_position_orders[0]["executed_amount_base"]) == D(".019")
    assert executor.get_custom_info()["close_quantity"]["residual_base"] == D(".019")


@pytest.mark.asyncio
async def test_terminal_partial_entry_below_exchange_minimum_hands_off_without_sale():
    executor = single_attempt()
    entry = tracked("entry", TradeType.BUY, ".001", OrderState.CANCELED)
    executor._open_order = entry
    await executor.control_task()
    await executor.control_task()
    assert executor.is_closed
    assert executor.close_type == CloseType.POSITION_HOLD
    assert D(executor._held_position_orders[0]["executed_amount_base"]) == D(".001")
    executor.place_order.assert_not_called()


@pytest.mark.asyncio
async def test_filled_status_with_missing_fill_receipts_keeps_reconciliation_blocked():
    executor = single_attempt()
    executor._open_order = tracked("entry", TradeType.BUY, ".4", OrderState.FILLED)
    executor.get_price.return_value = D("105")
    await executor.control_task()
    executor.get_price.return_value = D("103")
    await executor.control_task()
    await executor.control_task()
    executor.place_order.assert_not_called()
    assert executor.is_active
    assert executor.get_custom_info()["entry_lifecycle"]["state"] == "reconciliation_required"


@pytest.mark.asyncio
async def test_terminal_receipt_is_frozen_before_late_connector_mutation():
    executor = single_attempt()
    entry = tracked("entry", TradeType.BUY, "0", OrderState.CANCELED)
    executor._open_order = entry
    connector_order = entry.order
    await executor.control_task()
    assert executor.is_closed
    connector_order.update_with_trade_update(TradeUpdate(
        trade_id="after-stop", client_order_id="entry", exchange_order_id="entry",
        trading_pair="ETH-USDT", fill_price=D("100"), fill_base_amount=D(".4"),
        fill_quote_amount=D("40"), fee=AddedToCostTradeFee(), fill_timestamp=33,
    ))
    # The late receipt belongs to held-inventory reconciliation, not to newly
    # mutated scalar performance of a stopped executor.
    assert executor.executor_info.filled_amount_quote == D("0")
    assert executor.get_custom_info()["execution_receipt"]["entry_gross_base"] == D("0")
