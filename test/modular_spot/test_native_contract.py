"""Run in the owning Hummingbot environment; never substitute a fake engine."""
import asyncio
import importlib.util
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import patch

import pytest

if importlib.util.find_spec('hummingbot') is None:
    pytest.skip('native Hummingbot package/compiled runtime is unavailable', allow_module_level=True)

from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig  # noqa: E402
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor, native_config  # noqa: E402
from hummingbot.core.data_type.common import OrderType, TradeType  # noqa: E402
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase  # noqa: E402
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor  # noqa: E402
from hummingbot.strategy_v2.models.base import RunnableStatus  # noqa: E402
from hummingbot.strategy_v2.models.executors import CloseType  # noqa: E402


def config(**overrides):
    return ModularSpotControllerConfig(id='modular_spot_test', connector_name='okx', trading_pair='BTC-USDC', **overrides)


def test_native_discovery_and_default_disabled():
    value = config()
    assert value.get_controller_class() is ModularSpotController
    assert ModularSpotController.__bases__ == (ControllerBase,)
    assert not value.execution_enabled
    controller = ModularSpotController(value, NS(ready=False), asyncio.Queue())
    assert controller.determine_executor_actions() == []
    assert not controller.validate_executor_config(None)


@pytest.mark.parametrize('overrides', [dict(initial_positions=[{}]), dict(execution_enabled='yes'),
                                     dict(total_amount_quote='NaN'), dict(id='ok_rsi_existing'),
                                     dict(trading_pair='BTC-USDC-X'),
                                     dict(signal={'kind': 'breakout'}, acquisition={'fractions': ['.5', '.5']})])
def test_invalid_controller_configs_rejected(overrides):
    with pytest.raises((ValueError, TypeError)):
        value = {'id': 'modular_spot_test', 'connector_name': 'okx', 'trading_pair': 'BTC-USDC', **overrides}
        ModularSpotControllerConfig(**value)


def test_native_config_roundtrip_retains_entire_execution_policy():
    value = config()
    native = native_config(value, identifier='test', timestamp=120, price=D('100'), amount=D('1'), cycle=0, stage=0)
    restored = type(native).model_validate_json(native.model_dump_json())
    assert restored == native and native.side == TradeType.BUY and native.leverage == 1
    assert native.entry_order_policy == 'single_attempt'
    barrier = native.triple_barrier_config
    assert barrier.stop_loss is barrier.time_limit is barrier.take_profit is None
    assert barrier.trailing_stop_order_type == OrderType.LIMIT
    assert barrier.trailing_stop_limit_max_age_seconds == 0


def test_executor_is_local_extension_and_does_not_replace_native_callbacks():
    assert ModularPositionExecutor.__bases__ == (PositionExecutor,)
    for method in ('process_order_filled_event', 'process_order_canceled_event',
                   'process_order_failed_event', '_control_single_attempt_entry'):
        assert getattr(ModularPositionExecutor, method) is getattr(PositionExecutor, method)


def test_local_profit_floor_and_no_market_fallback():
    executor = object.__new__(ModularPositionExecutor)
    executor.config = native_config(config(), identifier='one', timestamp=120, price=D('100'), amount=D('1'), cycle=0, stage=0)
    executor.trading_rules = NS(min_price_increment=D('.01'))
    with patch.object(PositionExecutor, '_min_no_loss_sell_price', return_value=D('100.123')):
        assert executor._min_no_loss_sell_price() == D('101.13')
    assert executor._close_order_type_for(CloseType.TRAILING_STOP) == OrderType.LIMIT
    assert executor._should_fallback_trailing_limit_close() is False
    with patch.object(PositionExecutor, 'early_stop') as stop:
        executor.early_stop(keep_position=False)
        stop.assert_called_once_with(keep_position=True)


def test_ungranted_configs_never_fall_back_to_unprotected_native_executor():
    controller = ModularSpotController(config(), NS(ready=True), asyncio.Queue())
    controller._status = RunnableStatus.RUNNING
    native = native_config(config(), identifier='unknown', timestamp=120, price=D('100'), amount=D('1'), cycle=0, stage=0)
    assert not controller.validate_executor_config(native)
    with pytest.raises(ValueError):
        controller.get_executor_class_for_config(native)
