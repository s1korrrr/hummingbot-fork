from decimal import Decimal as D
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import OrderCancelledEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


def make_executor():
    strategy = MagicMock(spec=StrategyV2Base)
    strategy.current_timestamp = 1000
    connector = MagicMock()
    connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
    connector.get_available_balance.return_value = D('10')
    strategy.connectors = {'okx': connector}
    config = PositionExecutorConfig(
        id='lifecycle', timestamp=1, connector_name='okx', trading_pair='ETH-USDT',
        side=TradeType.BUY, entry_price=D('100'), amount=D('1'),
        triple_barrier_config=TripleBarrierConfig(
            open_order_type=OrderType.LIMIT_MAKER, open_order_limit_max_age_seconds=180,
            trailing_stop=TrailingStop(activation_price=D('.01'), trailing_delta=D('.005')),
            trailing_stop_order_type=OrderType.LIMIT, trailing_stop_limit_max_age_seconds=3))
    rule = TradingRule('ETH-USDT', min_order_size=D('.01'), min_price_increment=D('.01'),
                       min_base_amount_increment=D('.01'), min_notional_size=D('1'))
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=rule):
        executor = PositionExecutor(strategy, config)
    executor._status = RunnableStatus.RUNNING
    executor._sleep = AsyncMock()
    executor.get_price = MagicMock(return_value=D('100'))
    # A missing lookup must not erase a previously observed order.
    executor.get_in_flight_order = MagicMock(return_value=None)
    return executor


def tracked(oid, side, fill, state):
    order = InFlightOrder(client_order_id=oid, exchange_order_id=oid, trading_pair='ETH-USDT',
                          order_type=OrderType.LIMIT, trade_type=side, amount=D('1'), price=D('100'),
                          creation_timestamp=1, initial_state=state)
    if D(fill):
        order.update_with_trade_update(TradeUpdate(
            trade_id=oid, client_order_id=oid, exchange_order_id=oid, trading_pair='ETH-USDT',
            fill_price=D('100'), fill_base_amount=D(fill), fill_quote_amount=D(fill) * 100,
            fee=AddedToCostTradeFee(), fill_timestamp=2))
    result = TrackedOrder(order_id=oid)
    result.order = order
    return result


def cancel(executor, order):
    order.order.current_state = OrderState.CANCELED
    executor.process_order_canceled_event(0, None, OrderCancelledEvent(1000, order.order_id))


def test_cancelled_partial_entry_retains_exposure_and_only_retries_remaining():
    executor = make_executor()
    executor._open_order = tracked('entry', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
    cancel(executor, executor._open_order)
    assert executor.open_filled_amount == D('.4')
    assert not executor.open_and_close_volume_match()
    executor.place_order = MagicMock(return_value='replacement')
    executor.place_open_order()
    assert executor.place_order.call_args.kwargs['amount'] == D('.6')
    assert executor.open_filled_amount == D('.4')


def test_cancelled_partial_exit_is_counted_once_and_marked_to_market():
    executor = make_executor()
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    close = tracked('close', TradeType.SELL, '.4', OrderState.PARTIALLY_FILLED)
    executor._close_order = close
    executor.get_price.return_value = D('80')
    assert executor.trade_pnl_quote == D('-12')
    cancel(executor, close)
    cancel(executor, close)
    assert executor.amount_to_close == D('.6')
    assert executor.trade_pnl_quote == D('-12')


def test_partial_entry_has_timeout_and_trailing_protection():
    executor = make_executor()
    executor._open_order = tracked('entry', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
    assert executor._should_cancel_stale_unfilled_open_limit_order()
    executor.get_price.return_value = D('105')
    executor.control_barriers()
    assert executor._trailing_stop_trigger_pct is not None


@pytest.mark.asyncio
async def test_shutdown_drives_stale_limit_cancellation_then_remaining_market_close():
    executor = make_executor()
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    close = tracked('close', TradeType.SELL, '.4', OrderState.PARTIALLY_FILLED)
    executor._close_order = close
    executor._close_order_type = OrderType.LIMIT
    executor.close_type = CloseType.TRAILING_STOP
    executor._status = RunnableStatus.SHUTTING_DOWN
    executor.close_timestamp = 3
    executor.get_price.return_value = D('101')
    executor.place_order = MagicMock(return_value='market-close')
    await executor.control_task()
    executor._strategy.cancel.assert_called_once()
    executor.place_order.assert_not_called()
    cancel(executor, close)
    await executor.control_task()
    assert executor.place_order.call_args.kwargs['amount'] == D('.6')
    assert executor.place_order.call_args.kwargs['order_type'] == OrderType.MARKET


def test_close_waits_for_partial_entry_cancel_acknowledgement():
    executor = make_executor()
    entry = tracked('entry', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
    executor._open_order = entry
    executor.place_order = MagicMock(return_value='close')
    executor.place_close_order_and_cancel_open_orders(CloseType.TRAILING_STOP)
    executor.place_order.assert_not_called()
    assert executor.status == RunnableStatus.SHUTTING_DOWN


def test_non_finite_active_mark_cannot_be_reported_as_zero_risk():
    executor = make_executor()
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    executor.get_price.return_value = D('NaN')
    with pytest.raises(ValueError, match='finite'):
        _ = executor.executor_info


@pytest.mark.asyncio
async def test_dust_is_handed_to_inventory_and_never_disappears():
    executor = make_executor()
    executor._open_order = tracked('entry', TradeType.BUY, '.004', OrderState.CANCELED)
    executor._status = RunnableStatus.SHUTTING_DOWN
    executor.close_type = CloseType.EARLY_STOP
    await executor.control_shutdown_process()
    assert executor.close_type == CloseType.POSITION_HOLD
    await executor.control_shutdown_process()
    assert executor.is_closed
    assert executor._held_position_orders[0]['executed_amount_base'] == '0.004'
