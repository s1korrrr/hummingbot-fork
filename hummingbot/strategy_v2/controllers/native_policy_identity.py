"""Narrow native policy identity shared by the strategy runner and accounting gates."""


def is_ok_rsi_policy(config) -> bool:
    """Preserve legacy OK RSI admission; recognize only the genuine modular config."""
    name = getattr(config, 'controller_name', None)
    if name == 'ok_rsi':
        return True
    if name == 'rsi_modular':
        from controllers.generic.rsi_modular import get_config_class
        return type(config) is get_config_class('ok_rsi') and config.profile == 'ok_rsi'
    if name != 'modular_ok_rsi':
        return False
    # Loading controllers from the orchestrator's module scope would introduce a
    # circular dependency. An opt-in name alone never grants the policy exception.
    from controllers.generic.modular_ok_rsi import ModularOKRSIControllerConfig
    return type(config) is ModularOKRSIControllerConfig


def is_modular_policy(config) -> bool:
    """Exact modular model identity for operational rails, never accounting exceptions."""
    if getattr(config, 'controller_name', None) == 'rsi_modular':
        from controllers.generic.rsi_modular import CONFIG_PROFILES
        return any(type(config) is cls and config.profile == profile for profile, cls in CONFIG_PROFILES.items())
    if getattr(config, 'controller_name', None) not in {'modular_spot', 'modular_ok_rsi', 'modular_rsi_v5'}:
        return False
    from controllers.generic.modular_ok_rsi import ModularOKRSIControllerConfig
    from controllers.generic.modular_rsi_v5 import ModularRSIV5ControllerConfig
    from controllers.generic.modular_spot import ModularSpotControllerConfig
    return type(config) in {ModularOKRSIControllerConfig, ModularRSIV5ControllerConfig, ModularSpotControllerConfig}


def is_legacy_ok_rsi_operator_policy(config) -> bool:
    """Exact legacy directional owner for authenticated operator commands only."""
    if getattr(config, 'controller_name', None) != 'ok_rsi':
        return False
    from controllers.directional_trading.ok_rsi_core.config import OKRSIControllerConfig
    return type(config) is OKRSIControllerConfig


def is_meridian_operator_policy(config) -> bool:
    """Exact unified Meridian owner for durable operator controls only.

    This is deliberately separate from modular accounting and funding identities.
    A controller-name string or a symbol's modular config cannot opt into it.
    """
    if getattr(config, 'controller_name', None) != 'meridian_v3_unified':
        return False
    from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig
    return type(config) is MeridianUnifiedConfig
