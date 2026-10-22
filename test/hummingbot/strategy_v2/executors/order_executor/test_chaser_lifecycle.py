from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor, tracked
from unittest.mock import MagicMock

from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.event.events import OrderCancelledEvent
from hummingbot.strategy_v2.executors.order_executor.data_types import (
    ExecutionStrategy,
    LimitChaserConfig,
    OrderExecutorConfig,
)
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor

RULE = TradingRule('ETH-USDT', min_order_size=D('.01'), min_price_increment=D('.01'),
                   min_base_amount_increment=D('.001'), min_notional_size=D('1'))


def test_chaser_waits_for_ack_preserves_partial_fill_and_price_floor():
    from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
    strategy = make_executor()._strategy
    strategy.connectors['okx'].get_fee.return_value = AddedToCostTradeFee(percent=D('.001'))
    config = OrderExecutorConfig(timestamp=1, connector_name='okx', trading_pair='ETH-USDT',
                                 side=TradeType.SELL, amount=D('1'), price=D('100'),
                                 min_sell_price=D('101'), execution_strategy=ExecutionStrategy.LIMIT_CHASER,
                                 chaser_config=LimitChaserConfig(distance=D('.001'), refresh_threshold=D('.001')))
    executor = OrderExecutor(strategy, config)
    executor.get_trading_rules = MagicMock(return_value=RULE)
    old = tracked('old', TradeType.SELL, '.4', OrderState.PARTIALLY_FILLED)
    executor._order = old
    executor.place_order = MagicMock(return_value='replacement')
    executor.get_price = MagicMock(return_value=D('80'))
    executor.renew_order()
    executor.place_order.assert_not_called()
    assert executor._order.order_id == 'old'
    old.order.current_state = OrderState.CANCELED
    executor.process_order_canceled_event(0, None, OrderCancelledEvent(1000, 'old'))
    executor.control_order()
    assert executor.place_order.call_args.kwargs['amount'] == D('.6')
    assert executor.place_order.call_args.kwargs['price'] * D('.999') >= D('101')


def test_unplaceable_remainder_after_partial_fill_finishes_as_dust():
    from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
    from hummingbot.strategy_v2.models.executors import CloseType
    strategy = make_executor()._strategy
    strategy.connectors['okx'].get_fee.return_value = AddedToCostTradeFee(percent=D('.001'))
    config = OrderExecutorConfig(timestamp=1, connector_name='okx', trading_pair='ETH-USDT',
                                 side=TradeType.SELL, amount=D('1'), price=D('100'),
                                 min_sell_price=D('101'), execution_strategy=ExecutionStrategy.LIMIT_CHASER,
                                 chaser_config=LimitChaserConfig(distance=D('.001'), refresh_threshold=D('.001')))
    executor = OrderExecutor(strategy, config)
    executor.get_trading_rules = MagicMock(return_value=RULE)
    executor.get_price = MagicMock(return_value=D('100'))
    executor.place_order = MagicMock(return_value='replacement')
    executor.stop = MagicMock()
    old = tracked('old', TradeType.SELL, '.996', OrderState.PARTIALLY_FILLED)
    executor._order = old
    executor.renew_order()
    old.order.current_state = OrderState.CANCELED
    executor.process_order_canceled_event(0, None, OrderCancelledEvent(1000, 'old'))
    executor.control_order()  # 0.004 ETH (~0.40 quote) is below both venue minimums
    executor.place_order.assert_not_called()
    assert executor.close_type == CloseType.POSITION_HOLD
    executor.stop.assert_called_once()


def test_first_order_below_minimum_is_still_submitted_for_the_venue_to_judge():
    strategy = make_executor()._strategy
    config = OrderExecutorConfig(timestamp=1, connector_name='okx', trading_pair='ETH-USDT', side=TradeType.SELL,
                                 amount=D('.004'), price=D('100'), execution_strategy=ExecutionStrategy.LIMIT)
    executor = OrderExecutor(strategy, config)
    executor.get_trading_rules = MagicMock(return_value=RULE)
    executor.place_order = MagicMock(return_value='first')
    executor.control_order()
    executor.place_order.assert_called_once()
