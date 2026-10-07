"""Tidewater R20 (`recovery_min_gap`, opt-in research option): a smaller minimum buy gap in the recovery regime.

Reference semantics: quants-lab `core/backtesting/spot_modular_minute.py` `regime_buy_gap` (V4 iteration 3, R).
While the regime holds, the gap is the native `live_buy_gap` on the pair's recipe `BuyGapConfig` with only
`min_gap` replaced; otherwise the native gap is unchanged. The same gap applies to the gap from the last active
buy and to the bag gap, as in the native planner (`planner._live_capital_entry`).

Regime: the pair's held bag is empty or venue dust at the bid (the planner's own `bag_empty`: native
`held_is_dust` at max(amount step, minimum amount) and the minimum notional) or the bid is at or above the
bag's breakeven.

Mechanism. The sealed planner reads the gap from the symbol configuration (`buy_gap=c.buy_gap`), in the
portfolio's entry preview and in the symbol's dispatch. Tidewater's pacing can only refuse, so a smaller gap
must reach the planner. At the start of each decision tick the controller reads each pair's regime from a
native snapshot (bid and lot rules) and the held bag, as the planner reads them later in the same synchronous
tick. While the regime holds, the pair's symbol context presents `RecoveryGapView` for that tick: the recipe
configuration with only `buy_gap` replaced. The view keeps the recipe configuration's fingerprint (the decision
checkpoint identity). The option itself is bound by Tidewater's seal and owner identity (sealed payload and
recipe digest), like every other Tidewater option. Outside the regime, or when the regime cannot be read (fail
closed: no smaller gap without its evidence), the planner reads the recipe configuration object itself.
Exits, recovery sells and held bags read no buy gap, so they are unaffected.
"""
from controllers.generic.modular_spot_core.bag import BuyGapConfig
from hummingbot.strategy_v2.components.held_inventory import held_is_dust


def recovery_buy_gap(buy_gap, min_gap):
    """The recipe's BuyGapConfig with only `min_gap` replaced (validated: min_gap <= max_gap)."""
    return BuyGapConfig(**{**buy_gap.model_dump(), 'min_gap': min_gap})


def recovery_regime(held, *, bid, amount_step, minimum_amount, minimum_notional):
    """(holds, reason): the bag is empty or dust at the bid, or the bid is at or above its breakeven."""
    if held_is_dust(held.amount, bid, min_amount=max(amount_step, minimum_amount), min_notional=minimum_notional):
        return True, 'bag_empty'
    if bid >= held.breakeven_price:
        return True, 'at_or_above_breakeven'
    return False, 'below_breakeven'


class RecoveryGapView:
    """A symbol configuration whose only difference is `buy_gap`; every other read is the recipe's."""

    def __init__(self, config, buy_gap):
        self._source = config
        self.buy_gap = buy_gap

    def __getattr__(self, name):
        return getattr(self._source, name)

    def fingerprint(self):
        return self._source.fingerprint()
