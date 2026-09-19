import asyncio
from types import SimpleNamespace

import pytest

from controllers.generic.modular_ok_rsi import ModularOKRSIControllerConfig
from controllers.generic.modular_rsi_v5 import ModularRSIV5ControllerConfig


@pytest.mark.parametrize('model', [ModularOKRSIControllerConfig, ModularRSIV5ControllerConfig])
def test_paper_binding_requires_explicit_execution_and_candle_owners(model):
    values = dict(id='modular_spot_paper', trading_pair='BTC-USDC')
    with pytest.raises(ValueError):
        model(**values, execution_mode='paper')
    with pytest.raises(ValueError):
        model(**values, connector_name='okx_paper_trade')
    config = model(**values, execution_mode='paper', connector_name='okx_paper_trade', candles_connector='okx')
    assert all(feed.connector == 'okx' for feed in config.candles_config)
    with pytest.raises(ValueError, match='native paper connector'):
        config.get_controller_class()(config, SimpleNamespace(connectors={'okx_paper_trade': object()}), asyncio.Queue())
