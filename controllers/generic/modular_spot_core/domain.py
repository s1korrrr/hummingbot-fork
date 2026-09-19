import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256


class PolicyError(ValueError):
    """Required evidence is invalid or unavailable; do not authorize new risk."""


class DataUnavailable(PolicyError):
    """A transient native observation is missing; remain idle until it returns."""


def decimal(value, name, *, minimum=Decimal('0'), positive=False):
    try:
        if isinstance(value, bool):
            raise ValueError('boolean is not an amount')
        result = Decimal(str(value))
        if not result.is_finite() or (minimum is not None and result < minimum) or (positive and result <= 0):
            raise ValueError('out of range')
        return result
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PolicyError(f'invalid {name}') from exc


def identity(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class SignalDecision:
    enter: bool
    reason: str


@dataclass(frozen=True)
class Allocation:
    amount: Decimal
    reserved_quote: Decimal
    reserved_base: Decimal
