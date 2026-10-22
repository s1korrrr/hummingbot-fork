"""Compatibility import for the canonical strategy-neutral component."""
import sys

from hummingbot.strategy_v2.components import candle_transport as _implementation

if __name__ == '__main__':
    _implementation.main()
else:
    sys.modules[__name__] = _implementation
