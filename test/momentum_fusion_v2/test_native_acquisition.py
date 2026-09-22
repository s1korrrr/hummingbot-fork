import pytest

try:
    from hummingbot.connector.connector_base import ConnectorBase  # noqa: F401
except ModuleNotFoundError:
    pytest.skip('compiled hummingbot native package required', allow_module_level=True)

from decimal import Decimal as D

from controllers.generic.momentum_fusion_v2 import MomentumFusionV2Config
from controllers.generic.momentum_fusion_v2_runtime import runtime
from hummingbot.core.data_type.common import OrderType


def test_paper_and_live_identities_stay_distinct():
    from .fixtures import recipe
    paper = MomentumFusionV2Config(id='momentum_v2_paper', trading_pair='BNB-USDC', recipe=recipe(), execution_mode='paper', connector_name='okx_paper_trade')
    live = MomentumFusionV2Config(id='momentum_v2_observe', trading_pair='BNB-USDC', recipe=recipe(), execution_mode='observation')
    assert paper.execution_mode != live.execution_mode
    assert paper.execution_enabled is False and live.execution_enabled is False
    shadow = MomentumFusionV2Config(id='momentum_v2_shadow', trading_pair='BNB-USDC', recipe=recipe(), execution_mode='shadow')
    assert {paper.execution_mode, live.execution_mode, shadow.execution_mode} == {'paper', 'observation', 'shadow'}


def test_native_mapping_is_capped_limit_single_attempt():
    config = runtime.position_config(
        now=1.0, controller_id='mom', connector_name='okx', trading_pair='BNB-USDC',
        amount='0.1', price='100', stop='0.02', take_profit='0.04', ttl_seconds=60)
    assert config.entry_order_policy == 'single_attempt'
    assert config.triple_barrier_config.open_order_type == OrderType.LIMIT
    assert config.triple_barrier_config.open_order_limit_max_age_seconds == 60
    assert config.triple_barrier_config.stop_loss_order_type == OrderType.MARKET
    assert config.triple_barrier_config.time_limit is None
    assert config.leverage == 1


def test_partial_fill_cancels_remainder_not_the_filled_qty():
    assert runtime.remaining_after_partial('10', '3') == D('7')
