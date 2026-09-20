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
