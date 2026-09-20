"""Compatibility import for the canonical strategy-neutral component."""
import sys

from hummingbot.strategy_v2.components import capital_policy as _implementation

sys.modules[__name__] = _implementation
