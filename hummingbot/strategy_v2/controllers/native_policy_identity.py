"""Narrow native policy identity shared by the strategy runner and accounting gates."""


def is_ok_rsi_policy(config) -> bool:
    """Preserve legacy OK RSI admission; recognize only the genuine modular config."""
    name = getattr(config, 'controller_name', None)
    if name == 'ok_rsi':
        return True
    if name != 'modular_ok_rsi':
        return False
    # Loading controllers from the orchestrator's module scope would introduce a
    # circular dependency. An opt-in name alone never grants the policy exception.
    from controllers.generic.modular_ok_rsi import ModularOKRSIControllerConfig
    return type(config) is ModularOKRSIControllerConfig
