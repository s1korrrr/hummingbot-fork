"""One config resolution contract for native runtime, CLI and backtesting."""
import importlib
import inspect

from hummingbot.exceptions import InvalidController
from hummingbot.strategy_v2.controllers.controller_base import ControllerConfigBase
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.controllers.market_making_controller_base import MarketMakingControllerConfigBase


def resolve_controller_config_class(module, config_data=None):
    """Resolve an explicit profile before touching a profile's native defaults."""
    data = {} if config_data is None else config_data
    bases = (ControllerConfigBase, DirectionalTradingControllerConfigBase, MarketMakingControllerConfigBase)
    if getattr(module, 'CONFIG_PROFILES', None) is not None:
        model = module.get_config_class(data.get('profile'))
    elif isinstance(getattr(module, 'CONFIG_CLASS', None), str):
        model = getattr(module, module.CONFIG_CLASS)
    else:
        # Retain existing discovery for controllers without an explicit contract.
        model = next((member for _, member in inspect.getmembers(module)
                      if inspect.isclass(member) and member not in bases and issubclass(member, ControllerConfigBase)), None)
    if not inspect.isclass(model) or model in bases or not issubclass(model, ControllerConfigBase):
        raise InvalidController(f'No valid native configuration class in {module.__name__}')
    return model


def load_controller_config(config_data, controllers_module='controllers', module=None):
    """Load and strictly validate one controller configuration, never activate it."""
    if not isinstance(config_data, dict) or not config_data.get('controller_type') or not config_data.get('controller_name'):
        raise ValueError('Missing controller_type or controller_name in the configuration')
    path = f"{controllers_module}.{config_data['controller_type']}.{config_data['controller_name']}"
    if module is None:
        module = importlib.import_module(path)
    elif module.__name__ != path:
        raise ValueError('Loaded controller module differs from configuration identity')
    return resolve_controller_config_class(module, config_data)(**config_data)
