"""Tide gate: occupancy-paced spacing between buys of one pair.

Meridian V3 adds a tranche whenever price moves its ATR-scaled gap from the last buy,
in either direction, until the shared envelope is spent. Live it filled 85% of the
envelope in five flat days. The tide gate widens that spacing as portfolio occupancy
rises, so cash stays available for deeper moves, and stops upward adds once half the
envelope is in use.

It is an entry rule like the buy gap. It never cancels an order, blocks an exit or a
recovery sell, or demotes a position.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from pydantic import Field, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel

ONE = Decimal('1')


class TideGateConfig(PolicyModel):
    """`multiplier(u)` is 1 up to `low_occupancy`, rises linearly to `max_multiplier` at
    `high_occupancy`, and stays there. Upward adds need `u < upward_max_occupancy`."""
    low_occupancy: Decimal = Field(ge=0, lt=1)
    high_occupancy: Decimal = Field(gt=0, le=1)
    max_multiplier: Decimal = Field(ge=1)
    upward_max_occupancy: Decimal = Field(gt=0, le=1)

    @model_validator(mode='after')
    def ordered(self):
        if self.low_occupancy >= self.high_occupancy:
            raise ValueError('tide gate low occupancy must be below high occupancy')
        return self


@dataclass(frozen=True)
class TideDecision:
    allowed: bool
    reason: str
    multiplier: Decimal
    required_price: Optional[Decimal]


def occupancy(*, inventory_cost, commitments, envelope) -> Decimal:
    """Share of the envelope tied up in inventory at cost plus open buy commitments."""
    inventory_cost, commitments, envelope = Decimal(inventory_cost), Decimal(commitments), Decimal(envelope)
    if envelope <= 0 or inventory_cost < 0 or commitments < 0:
        raise ValueError('occupancy needs a positive envelope and nonnegative usage')
    return (inventory_cost + commitments) / envelope


def multiplier(cfg: TideGateConfig, u) -> Decimal:
    u = Decimal(u)
    if u <= cfg.low_occupancy:
        return ONE
    if u >= cfg.high_occupancy:
        return cfg.max_multiplier
    share = (u - cfg.low_occupancy) / (cfg.high_occupancy - cfg.low_occupancy)
    return ONE + share * (cfg.max_multiplier - ONE)


def tide_gate(cfg: TideGateConfig, *, gap, occupancy, reference, bid) -> TideDecision:
    """Whether a new tranche at `bid` keeps the occupancy-scaled distance from `reference`.

    `reference` is the pair's last active buy, else its held-bag breakeven, else None
    (no inventory: the first tranche is never gated). `gap` is the pair's own buy gap.
    """
    gap, u, bid = Decimal(gap), Decimal(occupancy), Decimal(bid)
    if gap <= 0 or bid <= 0 or u < 0:
        raise ValueError('tide gate needs a positive gap and bid and nonnegative occupancy')
    m = multiplier(cfg, u)
    if reference is None:
        return TideDecision(True, 'tide_gate_first_tranche', m, None)
    reference = Decimal(reference)
    if reference <= 0:
        raise ValueError('tide gate reference price must be positive')
    distance = gap * m
    if bid < reference:
        required = reference * (ONE - distance)
        if bid <= required:
            return TideDecision(True, 'tide_gate_down_ok', m, required)
        return TideDecision(False, 'tide_gate_down', m, required)
    required = reference * (ONE + distance)
    if u >= cfg.upward_max_occupancy:
        return TideDecision(False, 'tide_gate_up_occupancy', m, required)
    if bid >= required:
        return TideDecision(True, 'tide_gate_up_ok', m, required)
    return TideDecision(False, 'tide_gate_up_gap', m, required)
