import asyncio
from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor, tracked
from types import SimpleNamespace
from unittest.mock import MagicMock

from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


def controller():
    provider = MagicMock()
    provider.time.return_value = 1000
    provider.get_price_by_type.return_value = D('100')
    provider.get_available_balance.return_value = D('1000')
    provider.get_trading_rules.return_value = SimpleNamespace(
        min_price_increment=D('.01'), min_order_size=D('.001'),
        min_base_amount_increment=D('.001'), min_notional_size=D('1'), min_order_value=D('1'))
    provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
    config = OKRSIControllerConfig(id='controller', connector_name='okx', trading_pair='ETH-USDT',
                                   use_adaptive_trailing=False, strict_hmm_mode=False,
                                   usd_per_entry=D('25'), max_total_position_usd=D('100'))
    result = OKRSIController(config, provider, asyncio.Queue())
    result.processed_data = {'timestamp': 1000, 'signal': 1, 'data_health': {'ready': True},
                             'signal_state': {'condition_ok': True, 'size_multiplier': 1}}
    result._status = RunnableStatus.RUNNING
    return result


def test_pending_entry_reserves_its_full_commitment_including_during_shutdown():
    instance = controller()
    instance.positions_held = [SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT', side=TradeType.BUY,
                                               amount=D('.74'), breakeven_price=D('100'), cum_fees_quote=D('0'))]
    executor = make_executor()
    executor.config.amount = D('.25')
    executor._open_order = tracked('pending', TradeType.BUY, '.01', OrderState.PARTIALLY_FILLED)
    instance.executors_info = [executor.executor_info]
    assert instance._get_total_position_value_usd() == D('99')
    assert not instance.can_create_executor(1)
    executor._status = RunnableStatus.SHUTTING_DOWN
    instance.executors_info = [executor.executor_info]
    assert instance._get_total_position_value_usd() == D('99')


def test_invalid_inventory_cannot_be_valued_as_zero_exposure():
    instance = controller()
    instance.positions_held = [SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT', side=TradeType.BUY,
                                               amount='bad', breakeven_price=D('100'), cum_fees_quote=D('0'))]
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)


def test_completed_loss_is_counted_before_any_entry_proposal():
    instance = controller()
    instance.config.max_consecutive_losses = 1
    executor = make_executor()
    executor._status = RunnableStatus.TERMINATED
    executor.close_type = CloseType.STOP_LOSS
    info = executor.executor_info.model_copy(update={'net_pnl_quote': D('-1'), 'close_timestamp': 999})
    instance.executors_info = [info]
    instance.create_actions_proposal = MagicMock(side_effect=lambda: ['unsafe'] if not instance._check_consecutive_loss_pause() else [])
    instance.stop_actions_proposal = MagicMock(return_value=[])
    assert instance.determine_executor_actions() == []
