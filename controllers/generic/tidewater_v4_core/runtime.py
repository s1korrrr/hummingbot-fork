"""Tidewater symbol runtime: Meridian V3's `SymbolRuntime` plus the opt-in capitulation override and
the opt-in R20 recovery-gap view (`recovery_gap.py`).

The native flash pause is a computed market feature (`MarketFeatures.flash_pause_until`) that only
the shared entry planner reads (`planner.plan`: `flash_crash_pause`). The runtime's market feeds
both the portfolio's entry preview and the symbol's own dispatch planning, and dispatch observes
again before planning. Applying the override here, where the market is observed, is therefore the
one place that keeps preview and dispatch consistent, without editing the sealed planner.

With the override disabled (the default) `observe` returns Meridian's market unchanged. With it
enabled and its condition met, the planner reads a copy of the market whose pause is cleared;
the cached market and the shared feature cache are never mutated. Exits, recovery sells and held
bags read no pause, so they are unaffected.

R20 (`recovery_min_gap`): `recovery_view` reads the pair's recovery regime from the native snapshot and
held bag for one decision tick; `TidewaterSymbolContext` presents the resulting view to every reader of
the symbol configuration during that tick (the entry preview and the dispatch planner). Without the
option no view is read and the context returns the recipe configuration, as Meridian's does.
"""
import dataclasses

from controllers.generic.meridian_v3_unified_core.portfolio import ERRORS
from controllers.generic.meridian_v3_unified_core.symbols import SymbolContext, SymbolRuntime
from controllers.generic.modular_spot_core.bag import HeldBag

from .recovery_gap import RecoveryGapView, recovery_buy_gap, recovery_regime

# Reading the regime fails closed (no view: the recipe gap) and is reported; the planner meets the same failure.
REGIME_ERRORS = (RuntimeError, *ERRORS)


class TidewaterSymbolContext(SymbolContext):
    """Meridian's symbol context; during a decision tick with R20 in its regime it presents the view."""

    def __init__(self, owner, pair):
        super().__init__(owner, pair)
        self.view = None

    @property
    def config(self):
        return self.view if self.view is not None else self.owner.symbol_config(self.pair)


class TidewaterSymbolRuntime(SymbolRuntime):
    def __init__(self, context, portfolio, *, paper):
        super().__init__(context, portfolio, paper=paper)
        self.capitulation = None  # the last observation's override decision, None when no pause was active
        self.recovery = None  # the last R20 regime reading, for reporting

    def observe(self):
        market = super().observe()
        self.capitulation = None
        override = self.portfolio.owner.config.capitulation_override
        if market is None or override is None:
            return market
        decision = self.portfolio.capitulation(self.controller.pair, market, override)
        self.capitulation = decision
        if decision is not None and decision['applied']:
            self.market = dataclasses.replace(market, flash_pause_until=None)
        return self.market

    def recovery_view(self, min_gap):
        """The R20 view of this symbol's configuration for one decision tick, or None (the recipe's)."""
        config = self.controller.config
        try:
            snapshot = self.snapshot()
            bid = snapshot['bid']
            held = HeldBag.from_positions(self.controller.positions_held, connector=config.connector_name,
                                          pair=config.trading_pair)
            holds, reason = recovery_regime(held, bid=bid, amount_step=snapshot['amount_step'],
                                            minimum_amount=snapshot['minimum_amount'],
                                            minimum_notional=snapshot['minimum_notional'])
        except REGIME_ERRORS as exc:
            self.recovery = {'regime': None, 'reason': f'unavailable:{type(exc).__name__}:{exc}'[:200]}
            return None
        self.recovery = {'regime': holds, 'reason': reason, 'bid': str(bid), 'held_amount': str(held.amount),
                         'breakeven': str(held.breakeven_price)}
        return RecoveryGapView(config, recovery_buy_gap(config.buy_gap, min_gap)) if holds else None
