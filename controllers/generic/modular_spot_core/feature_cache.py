"""Bounded reuse of immutable results with explicit implementation/input identity."""
from collections import OrderedDict
from copy import deepcopy
from typing import Callable, TypeVar

from .candle_data import CandleSnapshot
from .domain import identity

T = TypeVar('T')


class FeatureCache:
    def __init__(self, capacity: int = 2048):
        if type(capacity) is not int or not 1 <= capacity <= 10000:
            raise ValueError('invalid feature-cache capacity')
        self.capacity = capacity
        self.values = OrderedDict()

    def get(self, snapshot: CandleSnapshot, *, spec: dict, implementation: str,
            now: float, max_age: float, compute: Callable[..., T]) -> T:
        if not implementation:
            raise ValueError('feature implementation identity is required')
        frame = snapshot.frame(now=now, max_age=max_age)
        key = identity([snapshot.digest, spec, implementation])
        if key not in self.values:
            self.values[key] = deepcopy(compute(frame))
            if len(self.values) > self.capacity:
                self.values.popitem(last=False)
        self.values.move_to_end(key)
        return deepcopy(self.values[key])
