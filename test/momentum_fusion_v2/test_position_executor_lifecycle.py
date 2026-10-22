from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import cancel, tracked
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

try:
    from hummingbot.connector.connector_base import ConnectorBase  # noqa: F401
except ModuleNotFoundError:
    pytest.skip('compiled hummingbot native package required', allow_module_level=True)

from controllers.generic.momentum_fusion_v2_runtime import runtime
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus


def _momentum_buy():
    return runtime.position_config(
        now=1000, controller_id='mom-v2', connector_name='okx', trading_pair='BNB-USDC',
        amount=D('1'), price=D('12'), stop=D('0.05'), take_profit=D('0.04'), ttl_seconds=60)


def _momentum_sell():
    return runtime.position_config(
        now=1000, controller_id='mom-v2', connector_name='okx', trading_pair='BNB-USDC',
        amount=D('1'), price=D('12'), stop=None, take_profit=None, ttl_seconds=60,
        side=TradeType.SELL)


def _running(config):
    strategy = MagicMock(spec=StrategyV2Base)
    strategy.current_timestamp = 1000
    connector = MagicMock()
    connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
    connector.get_available_balance.return_value = D('10')
    strategy.connectors = {config.connector_name: connector}
    rule = TradingRule(
        config.trading_pair, min_order_size=D('0.01'), min_price_increment=D('0.01'),
        min_base_amount_increment=D('0.01'), min_notional_size=D('1'))
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=rule):
        executor = PositionExecutor(strategy, config)
    executor._status = RunnableStatus.RUNNING
    executor._sleep = AsyncMock()
    executor.get_price = MagicMock(return_value=config.entry_price)
    executor.get_in_flight_order = MagicMock(return_value=None)
    return executor


def _open(config, fill, state, created=1000):
    row = tracked('entry', config.side, fill, state)
    row.order.trading_pair = config.trading_pair
    row.order.creation_timestamp = created
    return row


@pytest.mark.parametrize('config_fn', [_momentum_buy, _momentum_sell])
def test_momentum_limit_ttl_cancels_unfilled_open(config_fn):
    config = config_fn()
    assert config.triple_barrier_config.open_order_type == OrderType.LIMIT
    assert config.triple_barrier_config.open_order_limit_max_age_seconds == 60
    assert config.triple_barrier_config.time_limit is None
    executor = _running(config)
    executor._open_order = _open(config, '0', OrderState.OPEN)
    executor._strategy.current_timestamp = 1060
    executor.control_open_order()
    executor._strategy.cancel.assert_called_with(
        connector_name='okx', trading_pair='BNB-USDC', order_id='entry')
    assert executor.close_type is None
    assert executor._entry_stop_reason == 'entry_deadline'


@pytest.mark.parametrize('config_fn', [_momentum_buy, _momentum_sell])
def test_momentum_partial_then_cancel_keeps_fill(config_fn):
    config = config_fn()
    executor = _running(config)
    executor._open_order = _open(config, '.4', OrderState.PARTIALLY_FILLED)
    cancel(executor, executor._open_order)
    assert executor.open_filled_amount == D('.4')
    executor.place_order = MagicMock(return_value='replacement')
    executor.place_open_order()
    executor.place_order.assert_not_called()
    assert executor.open_filled_amount == D('.4')
