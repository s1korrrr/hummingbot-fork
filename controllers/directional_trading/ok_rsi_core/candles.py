"""Compatibility import for canonical UTC candle aggregation."""
import sys

from hummingbot.strategy_v2.components import candle_aggregation as _implementation

sys.modules[__name__] = _implementation
