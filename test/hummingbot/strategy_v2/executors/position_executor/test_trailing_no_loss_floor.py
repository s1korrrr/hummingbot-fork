from decimal import Decimal as D
from unittest.mock import MagicMock, patch

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
NET_ENTRY = ENTRY + BUY_FEE_QUOTE
MIN_SELL = NET_ENTRY / (D("1") - SELL_FEE_RATE)


def _executor(price: D):
    strategy = MagicMock(spec=StrategyV2Base)
    strategy.current_timestamp = 1000
    connector = MagicMock()
    connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
    connector.get_available_balance.return_value = D("10")
    connector.get_fee.return_value = AddedToCostTradeFee(percent=SELL_FEE_RATE)
    strategy.connectors = {"okx": connector}
    strategy.sell.return_value = "OID-SELL-1"
    config = PositionExecutorConfig(
        id="floor", timestamp=1, connector_name="okx", trading_pair="ETH-USDT",
        side=TradeType.BUY, entry_price=ENTRY, amount=D("1"),
        triple_barrier_config=TripleBarrierConfig(
            open_order_type=OrderType.LIMIT_MAKER,
            trailing_stop=TrailingStop(activation_price=D("0.01"), trailing_delta=D("0.005")),
            trailing_stop_order_type=OrderType.LIMIT,
            trailing_stop_limit_max_age_seconds=3,
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
    executor.control_trailing_stop()
    assert executor._trailing_stop_trigger_pct is not None
    executor._strategy.sell.reset_mock()


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
