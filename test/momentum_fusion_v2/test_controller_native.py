import pytest

try:
    from hummingbot.connector.connector_base import ConnectorBase  # noqa: F401
except ModuleNotFoundError:
    pytest.skip('compiled hummingbot native package required', allow_module_level=True)

from controllers.generic.momentum_fusion_v2 import MomentumFusionV2Config, MomentumFusionV2Controller


def test_source_identity_hold_is_visible_on_the_controller():
    assert MomentumFusionV2Controller.allow_implementation_hot_reload is False
    config = MomentumFusionV2Config(id='mom_fixture')
    assert config.execution_mode == 'observation'
    assert config.execution_enabled is False
