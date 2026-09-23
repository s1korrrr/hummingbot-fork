from decimal import Decimal as D
from unittest.mock import MagicMock, patch

from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

ENTRY = D("100")
BUY_FEE_QUOTE = D("0.08")
SELL_FEE_RATE = D("0.001")
SELL_FLAT_QUOTE = D("0.05")
NET_ENTRY = ENTRY + BUY_FEE_QUOTE
MIN_SELL = NET_ENTRY / (D("1") - SELL_FEE_RATE)
MIN_SELL_WITH_FLAT = NET_ENTRY / (D("1") - (SELL_FEE_RATE + SELL_FLAT_QUOTE / ENTRY))


def _executor(price: D, *, max_age=3, close_order_type=OrderType.LIMIT, close_fee=None,
              available_base=D("10")):
    strategy = MagicMock(spec=StrategyV2Base)
    strategy.current_timestamp = 1000
    connector = MagicMock()
    connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
    connector.get_available_balance.return_value = available_base
    if close_fee is None:
        close_fee = AddedToCostTradeFee(percent=SELL_FEE_RATE)
    connector.get_fee.return_value = close_fee
    strategy.connectors = {"okx": connector}
    strategy.sell.return_value = "OID-SELL-1"
    config = PositionExecutorConfig(
        id="floor", timestamp=1, connector_name="okx", trading_pair="ETH-USDT",
        side=TradeType.BUY, entry_price=ENTRY, amount=D("1"),
        triple_barrier_config=TripleBarrierConfig(
            open_order_type=OrderType.LIMIT_MAKER,
            trailing_stop=TrailingStop(activation_price=D("0.01"), trailing_delta=D("0.005")),
            trailing_stop_order_type=close_order_type,
            trailing_stop_limit_max_age_seconds=max_age,
        ),
    )
    rule = TradingRule(
        "ETH-USDT", min_order_size=D("0.01"), min_price_increment=D("0.01"),
        min_base_amount_increment=D("0.01"), min_notional_size=D("1"),
    )
    with patch.object(PositionExecutor, "get_trading_rules", return_value=rule):
        executor = PositionExecutor(strategy, config)
    executor._status = RunnableStatus.RUNNING
    executor.get_price = MagicMock(return_value=price)
    executor.get_in_flight_order = MagicMock(return_value=None)
    order = InFlightOrder(
        client_order_id="entry", exchange_order_id="entry", trading_pair="ETH-USDT",
        order_type=OrderType.LIMIT_MAKER, trade_type=TradeType.BUY, amount=D("1"),
        price=ENTRY, creation_timestamp=1, initial_state=OrderState.FILLED,
    )
    order.update_with_trade_update(TradeUpdate(
        trade_id="entry", client_order_id="entry", exchange_order_id="entry",
        trading_pair="ETH-USDT", fill_price=ENTRY, fill_base_amount=D("1"),
        fill_quote_amount=ENTRY, fill_timestamp=2,
        fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=BUY_FEE_QUOTE)]),
    ))
    tracked = TrackedOrder(order_id="entry")
    tracked.order = order
    executor._open_order = tracked
    return executor


def _arm(executor, peak: D):
    executor.get_price.return_value = peak
    with patch.object(MarketsRecorder, "get_instance", return_value=MagicMock()):
        executor.control_trailing_stop()
    assert executor._trailing_stop_trigger_pct is not None
    executor._strategy.sell.reset_mock()


def _completed_close(amount: D) -> TrackedOrder:
    order = InFlightOrder(
        client_order_id="close", exchange_order_id="close", trading_pair="ETH-USDT",
        order_type=OrderType.LIMIT, trade_type=TradeType.SELL, amount=amount,
        price=ENTRY, creation_timestamp=2, initial_state=OrderState.FILLED,
    )
    order.update_with_trade_update(TradeUpdate(
        trade_id="close", client_order_id="close", exchange_order_id="close",
        trading_pair="ETH-USDT", fill_price=ENTRY, fill_base_amount=amount,
        fill_quote_amount=ENTRY * amount, fill_timestamp=3,
        fee=AddedToCostTradeFee(percent=D("0")),
    ))
    tracked = TrackedOrder(order_id="close")
    tracked.order = order
    return tracked


def _flat_close_fee():
    return AddedToCostTradeFee(
        percent=SELL_FEE_RATE,
        flat_fees=[TokenAmount(token="USDT", amount=SELL_FLAT_QUOTE)],
    )


def _min_sell_for_close_qty(close_qty: D) -> D:
    return NET_ENTRY / (D("1") - (SELL_FEE_RATE + SELL_FLAT_QUOTE / (close_qty * ENTRY)))


def test_trailing_stop_that_would_sell_below_breakeven_after_fees_holds():
    executor = _executor(D("101.20"))
    _arm(executor, D("101.20"))
    below_floor = D("100.12")
    assert below_floor > ENTRY
    assert below_floor < MIN_SELL
    executor.get_price.return_value = below_floor
    assert executor.get_net_pnl_pct() > 0
    assert executor.get_net_pnl_pct() < executor._trailing_stop_trigger_pct

    executor.control_trailing_stop()

    executor._strategy.sell.assert_not_called()
    assert executor.status == RunnableStatus.RUNNING
    assert executor.close_type is None


def test_trailing_stop_still_sells_when_close_stays_profitable_after_fees():
    executor = _executor(D("102"))
    _arm(executor, D("102"))
    profitable = D("101.40")
    assert profitable > MIN_SELL

    executor.get_price.return_value = profitable
    assert executor.get_net_pnl_pct() < executor._trailing_stop_trigger_pct

    executor.control_trailing_stop()

    executor._strategy.sell.assert_called_once()
    args = executor._strategy.sell.call_args.args
    assert args[3] == OrderType.LIMIT
    assert args[4] >= MIN_SELL
    assert args[5] == PositionAction.CLOSE
    assert executor.close_type == CloseType.TRAILING_STOP


def test_ordinary_limit_close_fee_is_not_maker_for_age_zero_and_three():
    for max_age in (0, 3):
        executor = _executor(D("102"), max_age=max_age)
        rate = executor._expected_close_fee_rate()
        assert rate == SELL_FEE_RATE
        kwargs = executor.connectors["okx"].get_fee.call_args.kwargs
        assert kwargs["is_maker"] is False
        assert kwargs["order_type"] == OrderType.LIMIT
        assert kwargs["order_side"] == TradeType.SELL


def test_limit_maker_close_fee_is_maker_regardless_of_max_age():
    for max_age in (0, 3):
        executor = _executor(D("102"), max_age=max_age, close_order_type=OrderType.LIMIT_MAKER)
        rate = executor._expected_close_fee_rate()
        assert rate == SELL_FEE_RATE
        kwargs = executor.connectors["okx"].get_fee.call_args.kwargs
        assert kwargs["is_maker"] is True
        assert kwargs["order_type"] == OrderType.LIMIT_MAKER


def test_marketable_limit_flat_fee_raises_no_loss_floor_for_age_zero_and_three():
    between = D("100.20")
    assert MIN_SELL < between < MIN_SELL_WITH_FLAT
    close_fee = AddedToCostTradeFee(
        percent=SELL_FEE_RATE,
        flat_fees=[TokenAmount(token="USDT", amount=SELL_FLAT_QUOTE)],
    )
    for max_age in (0, 3):
        executor = _executor(D("102"), max_age=max_age, close_fee=close_fee)
        _arm(executor, D("102"))
        executor.get_price.return_value = between
        assert executor.get_net_pnl_pct() > 0
        assert executor.get_net_pnl_pct() < executor._trailing_stop_trigger_pct

        executor.control_trailing_stop()

        executor._strategy.sell.assert_not_called()
        assert executor.status == RunnableStatus.RUNNING
        assert executor.close_type is None
        assert executor._min_no_loss_sell_price() == MIN_SELL_WITH_FLAT


def test_residual_quantized_close_amortizes_flat_fee_over_next_sell_quantity():
    close_qty = D("0.01")
    min_sell_on_close = _min_sell_for_close_qty(close_qty)
    between = D("102")
    assert MIN_SELL_WITH_FLAT < between < min_sell_on_close
    executor = _executor(D("110"), close_fee=_flat_close_fee(), available_base=close_qty)
    assert executor.open_filled_amount == D("1")
    assert executor._close_order_amount() == close_qty
    expected_rate = SELL_FEE_RATE + SELL_FLAT_QUOTE / (close_qty * ENTRY)
    assert executor._expected_close_fee_rate() == expected_rate
    assert executor.connectors["okx"].get_fee.call_args.kwargs["amount"] == close_qty
    assert executor.connectors["okx"].get_fee.call_args.kwargs["is_maker"] is False
    assert executor._min_no_loss_sell_price() == min_sell_on_close

    _arm(executor, D("110"))
    executor.get_price.return_value = between
    assert executor.get_net_pnl_pct() > 0
    assert executor.get_net_pnl_pct() < executor._trailing_stop_trigger_pct

    executor.control_trailing_stop()

    executor._strategy.sell.assert_not_called()
    assert executor.status == RunnableStatus.RUNNING
    assert executor.close_type is None


def test_flat_fee_floor_uses_remainder_after_prior_close():
    remainder = D("0.1")
    min_sell_on_remainder = _min_sell_for_close_qty(remainder)
    between = D("100.40")
    assert MIN_SELL_WITH_FLAT < between < min_sell_on_remainder
    executor = _executor(between, close_fee=_flat_close_fee())
    executor._completed_close_orders = [_completed_close(D("0.9"))]
    assert executor.open_filled_amount == D("1")
    assert executor.amount_to_close == remainder
    assert executor._close_order_amount() == remainder
    assert executor._expected_close_fee_rate() == SELL_FEE_RATE + SELL_FLAT_QUOTE / (remainder * ENTRY)
    assert executor.connectors["okx"].get_fee.call_args.kwargs["amount"] == remainder
    assert executor._min_no_loss_sell_price() == min_sell_on_remainder
    assert executor._should_hold_trailing_close() is True


def test_blocked_close_quantity_holds_without_inventing_a_fee_rate():
    executor = _executor(D("110"), close_fee=_flat_close_fee(), available_base=D("0"))
    executor.connectors["okx"].get_fee.reset_mock()
    assert executor._close_order_amount() == D("0")
    assert executor._expected_close_amount() is None
    assert executor._expected_close_fee_rate() is None
    executor.connectors["okx"].get_fee.assert_not_called()
    assert executor._min_no_loss_sell_price() is None
    assert executor._should_hold_trailing_close() is True


def test_oversized_or_failed_native_close_does_not_invent_a_fee_rate():
    executor = _executor(D("110"), close_fee=_flat_close_fee())
    executor.connectors["okx"].get_fee.reset_mock()
    executor._close_order_amount = lambda: executor.amount_to_close + D("1")
    assert executor._expected_close_amount() is None
    assert executor._expected_close_fee_rate() is None
    executor.connectors["okx"].get_fee.assert_not_called()

    executor._close_order_amount = MagicMock(side_effect=RuntimeError("qty"))
    assert executor._expected_close_amount() is None
    assert executor._expected_close_fee_rate() is None
    executor.connectors["okx"].get_fee.assert_not_called()
