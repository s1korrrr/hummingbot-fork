"""Observation, acquisition eligibility and management are separate universes."""
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from pydantic import Field, StrictBool, model_validator

from .candle_data import PAIR_PATTERN
from .config import PolicyModel
from .domain import PolicyError, decimal


def pairs(values):
    if isinstance(values, str):
        raise PolicyError('symbol collection cannot be a string')
    result = tuple(values)
    if len(result) > 1000 or len(set(result)) != len(result):
        raise PolicyError('duplicate or unbounded universe')
    if any(not isinstance(x, str) or re.fullmatch(PAIR_PATTERN, x) is None for x in result):
        raise PolicyError('invalid instrument identity')
    return result


class UniversePolicy(PolicyModel):
    mode: Literal['fixed', 'impulse', 'sell_only'] = 'fixed'
    symbols: tuple[str, ...] = ()
    benchmarks: tuple[str, ...] = ()
    observe_all: StrictBool = False
    max_candidates: int = Field(default=5, ge=1, le=1000, strict=True)
    replacement_margin: Decimal = Field(default=Decimal('0'), ge=0)

    @model_validator(mode='after')
    def valid_scope(self):
        pairs(self.symbols)
        pairs(self.benchmarks)
        if self.mode == 'sell_only' and self.observe_all:
            raise ValueError('sell-only scope must be explicit')
        if self.mode != 'impulse' and self.observe_all:
            raise ValueError('only the screener profile observes the full admitted universe')
        return self


@dataclass(frozen=True)
class UniverseSelection:
    observation: tuple[str, ...]
    entries: tuple[str, ...]
    management: tuple[str, ...]


def select_universe(policy: UniversePolicy, *, available, tradable, scores,
                    managed, pending, blocked, previous) -> UniverseSelection:
    available, tradable = set(pairs(available)), set(pairs(tradable))
    management = set(pairs(managed)) | set(pairs(pending))
    blocked, previous = set(pairs(blocked)), pairs(previous)
    requested = available if policy.observe_all else set(policy.symbols)
    observation = requested | set(policy.benchmarks) | management
    if policy.mode == 'sell_only':
        return UniverseSelection(tuple(sorted(observation)), (), tuple(sorted(management)))
    permitted = requested & available & tradable - blocked
    if policy.mode == 'fixed':
        chosen = [x for x in policy.symbols if x in permitted][:policy.max_candidates]
    else:
        ranked = {symbol: decimal(value, f'{symbol} candidate score', minimum=None)
                  for symbol, value in scores.items() if symbol in permitted and value is not None}
        chosen = [x for x in previous if x in ranked][:policy.max_candidates]
        for symbol in sorted(ranked, key=lambda x: (-ranked[x], x)):
            if symbol in chosen:
                continue
            if len(chosen) < policy.max_candidates:
                chosen.append(symbol)
            else:
                weakest = sorted(chosen, key=lambda x: (ranked[x], x), reverse=False)[0]
                if ranked[symbol] > ranked[weakest] + policy.replacement_margin:
                    chosen[chosen.index(weakest)] = symbol
        chosen.sort(key=lambda x: (-ranked[x], x))
    return UniverseSelection(tuple(sorted(observation)), tuple(chosen), tuple(sorted(management)))
