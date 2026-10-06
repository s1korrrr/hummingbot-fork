"""Tidewater symbol runtime: Meridian V3's `SymbolRuntime` plus the opt-in capitulation override.

The native flash pause is a computed market feature (`MarketFeatures.flash_pause_until`) that only
the shared entry planner reads (`planner.plan`: `flash_crash_pause`). The runtime's market feeds
both the portfolio's entry preview and the symbol's own dispatch planning, and dispatch observes
again before planning. Applying the override here, where the market is observed, is therefore the
one place that keeps preview and dispatch consistent, without editing the sealed planner.

With the override disabled (the default) `observe` returns Meridian's market unchanged. With it
enabled and its condition met, the planner reads a copy of the market whose pause is cleared;
the cached market and the shared feature cache are never mutated. Exits, recovery sells and held
bags read no pause, so they are unaffected.
"""
import dataclasses

from controllers.generic.meridian_v3_unified_core.symbols import SymbolRuntime


class TidewaterSymbolRuntime(SymbolRuntime):
    def __init__(self, context, portfolio, *, paper):
        super().__init__(context, portfolio, paper=paper)
        self.capitulation = None  # the last observation's override decision, None when no pause was active

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
