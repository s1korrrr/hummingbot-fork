import asyncio
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

try:
    from hummingbot.connector.connector_base import ConnectorBase  # noqa: F401
except ModuleNotFoundError:
    pytest.skip('compiled hummingbot native package required', allow_module_level=True)

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.components.domain import identity
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

from controllers.generic.momentum_fusion_v2 import MomentumFusionV2Config, MomentumFusionV2Controller


def test_source_identity_hold_is_visible_on_the_controller():
    assert MomentumFusionV2Controller.allow_implementation_hot_reload is False
    config = MomentumFusionV2Config(id='mom_fixture')
    assert config.execution_mode == 'observation'
    assert config.execution_enabled is False


def _host(tmp_path, *, kill_switch=False):
    claims = tmp_path / 'claims.json'
    config = MomentumFusionV2Config(
        id='mom-v2', execution_enabled=True, execution_mode='paper',
        claim_store_path=str(claims), manual_kill_switch=kill_switch)
    provider = MagicMock()
    provider.initialize_rate_sources.return_value = None
    provider.time.return_value = 1_700_000_180
    provider.get_trading_rules.return_value = SimpleNamespace(
        min_base_amount_increment=D('0.1'), min_order_size=D('0.1'),
        min_notional_size=D('1'), min_price_increment=D('0.01'))
    provider.get_price_by_type.return_value = D('12')
    provider.get_available_balance.return_value = D('100')
    controller = MomentumFusionV2Controller(
        config=config, market_data_provider=provider, actions_queue=asyncio.Queue())
    controller._status = RunnableStatus.RUNNING
    controller.processed_data = {
        'signal': 0, 'data_health': {'ready': True}, 'decision_time': 1_700_000_120, 'close': D('12')}
    return controller


def _buy_held():
    buy = PositionExecutorConfig(
        timestamp=1, controller_id='mom-v2', connector_name='okx', trading_pair='BNB-USDC',
        side=TradeType.BUY, amount=D('10'), entry_price=D('10'),
        triple_barrier_config=TripleBarrierConfig(time_limit=None))
    return ExecutorInfo(
        id='long-1', timestamp=1, type='position_executor', status=RunnableStatus.RUNNING,
        config=buy, net_pnl_pct=D('0'), net_pnl_quote=D('0'), cum_fees_quote=D('0'),
        filled_amount_quote=D('100'), is_active=True, is_trading=True,
        custom_info={'side': TradeType.BUY, 'remaining_position_amount_base': D('10'),
                     'entry_price': D('10'), 'cum_fees_quote': D('0')})


def _sell_pending():
    sell = PositionExecutorConfig(
        timestamp=1, controller_id='mom-v2', connector_name='okx', trading_pair='BNB-USDC',
        side=TradeType.SELL, amount=D('3'), entry_price=D('12'),
        triple_barrier_config=TripleBarrierConfig(time_limit=None))
    return ExecutorInfo(
        id='sell-1', timestamp=1, type='position_executor', status=RunnableStatus.RUNNING,
        config=sell, net_pnl_pct=D('0'), net_pnl_quote=D('0'), cum_fees_quote=D('0'),
        filled_amount_quote=D('36'), is_active=True, is_trading=True,
        custom_info={'side': TradeType.SELL, 'remaining_position_amount_base': D('3')})


def test_host_maps_reserved_base_from_pending_sales(tmp_path):
    controller = _host(tmp_path)
    controller.executors_info = [_buy_held(), _sell_pending()]
    view, proposals, extras = controller._management_inputs(controller.market_data_provider.time())
    assert view.remaining_base == D('10')
    assert view.reserved_base == D('3')
    assert extras['harvest_base'] == '7'
    assert proposals[0].requested_base == D('7')


def test_management_sell_passes_controller_validator_while_entry_paused(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    actions = controller.determine_executor_actions()
    assert actions
    sell = actions[0].executor_config
    assert sell.side == TradeType.SELL
    assert controller.validate_executor_config(sell) is True
    assert identity(sell.model_dump(mode='json')) == controller._pending_identity


def test_management_does_not_double_emit_sell_while_pending(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    first = controller.determine_executor_actions()
    assert first and first[0].executor_config.side == TradeType.SELL
    second = controller.determine_executor_actions()
    assert second == []
    assert controller.processed_data['action_reason'] == 'pending_inflight'
