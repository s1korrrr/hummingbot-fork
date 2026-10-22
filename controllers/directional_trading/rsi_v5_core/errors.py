class StrategyCoreError(Exception):
    """Base error for deterministic strategy-core contract violations."""


class DuplicateEventError(StrategyCoreError):
    pass


class OutOfOrderEventError(StrategyCoreError):
    pass


class ConfigMismatchError(StrategyCoreError):
    pass
