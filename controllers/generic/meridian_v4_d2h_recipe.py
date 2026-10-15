"""Versioned D2h parameters: V4 plus native depth-scaled buy sizing, for the forward paper A/B.

D2h is the frozen finalist F1 of the 2026-10-14 edge search (rsibot
docs/evidence/2026-10-14-edge-search/REPORT.md). It differs from V4 only in its declared values:

- every buy plans a 50 quote base slice, scaled by depth: 50 x (1 + 4 x min(1, depth / 0.25)),
  depth = 1 - price / max(24 h closed-candle high, held bag breakeven), so 50 to 250 quote;
- the bag sells in 100 quote slices with a 2.5 quote remainder sweep (V4's slice-to-buy ratio);
- the depth budget (native ``DepthBudgetConfig``) caps capital at cost at the per-pair budget
  (base fraction 1: the cap never tightens with depth, it only bounds the ladder).

Its pre-registered Stage 3 acceptance FAILED (label NOT CONFIRMED): it is a paper candidate only.
V4's module values are read from ``meridian_v4_recipe`` and the declared differences are applied
here, so a V4 recipe change is a D2h change too and both need a new version.
"""
from decimal import Decimal

from controllers.generic import meridian_v4_recipe as v4
from controllers.generic.modular_spot_core.bag import DepthBudgetConfig, LiveCapitalConfig

RECIPE_ID = 'meridian_v4_d2h_v1'
# The frozen F1 run: D2h, BNB-USDC, continuous Jan-Aug 2025 (Stage 3 trial D2h_bnb_janaug25).
CANDIDATE_SHA256 = '77270fb6da0e9b1fb1cd1f31082a33c0d3195c0086ac2019a1a11f9ba2ffb530'  # noqa: documentation
PREPARED_SHA256 = 'ed2684c745bfc097a77d6a73691141d3b468d510245f95c0382db7100708535a'  # noqa: documentation
ECONOMIC_VALIDATION = 'CANDLE_MODEL_STAGE3_NOT_CONFIRMED_PAPER_CANDIDATE'
PAIRS = v4.PAIRS
ORDER_QUOTE = Decimal('50')
MAX_SLICE_MULTIPLIER = Decimal('5')
FULL_DEPTH = Decimal('.25')
DEPTH_LOOKBACK_MINUTES = 1440
MAX_ACTIVE_TRANCHES = v4.MAX_ACTIVE_TRANCHES


def strategy_parameters():
    """V4's module configs with D2h's declared bag slice and remainder sweep."""
    values = v4.strategy_parameters()
    values['bag'] = values['bag'].model_copy(update={'slice_quote': 2 * ORDER_QUOTE,
                                                     'remainder_sweep_quote': Decimal('2.5')})
    return values


def depth_budget(pair_quote):
    """The depth ladder; its budget is the per-pair budget, so only the ladder binds."""
    return DepthBudgetConfig(budget_quote=Decimal(pair_quote), lookback_minutes=DEPTH_LOOKBACK_MINUTES,
                             full_depth=FULL_DEPTH, base_fraction=Decimal('1'),
                             max_slice_multiplier=MAX_SLICE_MULTIPLIER)


def capital_policy(fee_reserve, pair_quote):
    """V4's live capital model on the 50 quote base slice (the depth ladder resizes each buy)."""
    pair_quote = Decimal(pair_quote)
    if pair_quote < ORDER_QUOTE * MAX_SLICE_MULTIPLIER:
        raise ValueError(f'the D2h per-pair budget must cover one {ORDER_QUOTE * MAX_SLICE_MULTIPLIER} quote buy')
    return LiveCapitalConfig(slice_quote=ORDER_QUOTE, position_cap_quote=pair_quote,
                             max_active_tranches=MAX_ACTIVE_TRANCHES, funded_cap_quote=pair_quote,
                             funding_fee_reserve=fee_reserve)
