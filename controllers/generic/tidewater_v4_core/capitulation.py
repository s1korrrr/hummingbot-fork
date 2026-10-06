"""CP capitulation override of the native flash-crash entry pause (research candidate, opt-in).

The native pause (`modular_spot_core.bag.flash_pause_until`, enforced by the shared planner as
`flash_crash_pause`) stops new buys for about 8.3 h after a 17-minute drop of 1.06%. CP lets a
DCA buy through that pause only in a deep capitulation:

    close_t <= (1 - depth) * max(high over the trailing 1,440 closed minutes)

and only when the pair holds inventory or is a first entry (not while an acquisition is
pending). Everything else (buy gap, depth budget, pair limit, tide gate, P0 admission) still
applies. The 1,440-minute high is the engine's own `BagFeatures.recent_high` (the depth-budget
lookback), read from the same closed candles as the pause.

`depth` was frozen OFFLINE from May-Jul 2026 BNB candidate signals the native pause blocked; see
the root evidence `docs/evidence/2026-10-06-tidewater-candidates/` (`cp_frozen.json`).

CP only allows or refuses BUY admissions. It never cancels an order, blocks an exit or a
recovery sell, or demotes a position: the pause it lifts is read by the entry planner only.
"""
from decimal import Decimal
from typing import Literal

from pydantic import Field

from hummingbot.strategy_v2.components.config import PolicyModel

LOOKBACK_MINUTES = 1440
ONE = Decimal('1')


class CapitulationOverride(PolicyModel):
    depth: Decimal = Field(gt=0, lt=1)
    lookback_minutes: Literal[1440] = LOOKBACK_MINUTES

    def holds(self, *, close, recent_high):
        """Whether the close is at least `depth` below the trailing high (exact decimal comparison)."""
        close, recent_high = Decimal(close), Decimal(recent_high)
        if close <= 0 or recent_high <= 0:
            raise ValueError('capitulation prices must be positive')
        return close <= (ONE - self.depth) * recent_high


def drawdown(*, close, recent_high):
    """1 - close / recent_high, for receipts."""
    return ONE - Decimal(close) / Decimal(recent_high)


# Frozen 2026-10-06 by docs/evidence/2026-10-06-tidewater-candidates/freeze_candidates.py (root repo):
# 75th percentile over 237 natively paused BNB candidate signals of May-Jul 2026 (cp_frozen.json `depth`).
CP_FROZEN = CapitulationOverride(depth='0.05618828176431856')
