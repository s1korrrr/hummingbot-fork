"""Compatibility import for the canonical strategy-neutral component."""
import sys

from hummingbot.strategy_v2.components import universe as _implementation

sys.modules[__name__] = _implementation
