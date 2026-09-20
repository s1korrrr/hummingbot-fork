"""Bounded reuse of immutable results with explicit implementation/input identity."""
from collections import OrderedDict
from copy import deepcopy
from typing import Callable, TypeVar

from .candle_data import CandleSnapshot
from .domain import DataUnavailable, identity

T = TypeVar('T')


class FeatureCache:
    def __init__(self, capacity: int = 2048):
        if type(capacity) is not int or not 1 <= capacity <= 10000:
            raise ValueError('invalid feature-cache capacity')
        self.capacity = capacity
        self.values = OrderedDict()

    def get(self, snapshot: CandleSnapshot, *, spec: dict, implementation: str,
            now: float, max_age: float, compute: Callable[..., T],
            initialization: str = 'closed-bar-complete-window',
            required_fields: tuple[str, ...] = ('open', 'high', 'low', 'close', 'volume'),
            max_transport_age: float = 15, force_recompute: bool = False) -> T:
        if not implementation or not initialization:
            raise ValueError('feature implementation identity is required')
        missing = [name for name in required_fields if name not in type(snapshot.rows[0]).model_fields]
        if missing:
            raise ValueError('required feature input is unavailable')
        if any(getattr(row, name) is None for row in snapshot.rows for name in required_fields):
            raise ValueError('required feature input is unavailable')
        frame = snapshot.frame(now=now, max_age=max_age)
        if not 0 <= now - snapshot.published_at <= max_transport_age:
            raise DataUnavailable('candle transport is stale')
        key = identity([snapshot.digest, snapshot.dataset.key, snapshot.epoch, spec, implementation,
                        initialization, list(required_fields)])
        if force_recompute:
            self.values.pop(key, None)
        if key not in self.values:
            try:
                self.values[key] = deepcopy(compute(frame))
            except Exception:
                self.values.pop(key, None)
                raise
            if len(self.values) > self.capacity:
                self.values.popitem(last=False)
        self.values.move_to_end(key)
        return deepcopy(self.values[key])
