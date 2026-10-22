"""Versioned BNB p23 parameters and explicit Meridian sizing adaptations.

This is a native recipe binding, not a new policy implementation. The historical
5000-quote candle result does not validate the 100-quote native configuration.
"""
from decimal import Decimal

from controllers.generic.modular_spot_core.bag import (
    AdaptiveTrailingConfig,
    BagConfig,
    BuyGapConfig,
    DepthBudgetConfig,
    FlashCrashConfig,
    LiveCapitalConfig,
)
from controllers.generic.modular_spot_core.config import AcquisitionPolicy, SignalPolicy, TrailingPolicy

RECIPE_ID = 'meridian_v3_bnb_p23_v1'
CANDIDATE_SHA256 = '28e41498968eef239d60c0af6d28120e66037758e582ef9d9f49997f2d911f3d'  # noqa: documentation
BASELINE_SHA256 = '730e5009ca59b21b68f9bf801d9a465e3be8b0caca154b3a1f217f8600a152a2'  # noqa: documentation
RESULT_SHA256 = '6d2c34ea33026d3f974725abd6df5694e0fbe9441420ca8e276847ab660dfec0'  # noqa: documentation
PAIRS = ('BTC-USDC', 'ETH-USDC', 'BNB-USDC', 'SOL-USDC', 'XRP-USDC')


# The frozen p23 sizing basis: 20 per buy and 100 per asset (the canary and full stages).
# The recipe's two capital-scaled module values derive from a stage's numbers so that
# another stage changes only sizing, never the signal, ladder, trailing or bag rules.
ORDER_QUOTE, PER_ASSET_QUOTE = Decimal('20'), Decimal('100')


def strategy_parameters(order_quote=ORDER_QUOTE, per_asset_quote=PER_ASSET_QUOTE):
    """Fresh immutable module configs; pair-specific market rules remain connector-owned.

    `bag.slice_quote` is two buys (the live sell-slice multiplier of 2) and
    `depth_budget.budget_quote` is the per-asset limit; at the frozen 20/100 basis these are
    the sealed 40 and 100 exactly.
    """
    order_quote, per_asset_quote = Decimal(order_quote), Decimal(per_asset_quote)
    if order_quote <= 0 or per_asset_quote < order_quote:
        raise ValueError('recipe sizing requires 0 < order_quote <= per_asset_quote')
    return dict(
        signal=SignalPolicy(kind='rsi_rebound', rsi_length=17, oversold='29',
                            breakout_lookback=20, require_uptrend=True, atr_length=14,
                            min_atr_fraction='.0002', max_atr_fraction='.01'),
        acquisition=AcquisitionPolicy(fractions=(Decimal('.2'),) * 5, minimum_drop='.008',
                                      cooldown_seconds=1, entry_timeout_seconds=300),
        trailing=TrailingPolicy(activation='.04', delta='.005'),
        adaptive_trailing=AdaptiveTrailingConfig(
            baseline_atr_pct='.002', activation_min='.003', activation_max='.05',
            delta_min='.001', delta_max='.02', min_locked_profit='.013',
            activation_exponent='.70', delta_exponent='.35', min_multiplier='.5', max_multiplier='2.5'),
        bag=BagConfig(
            early_stop_drawdown_pct='.03', recovery_target_pct='.02', min_profit_pct_for_sell='.02',
            recovery_arm_rsi='65', recovery_trail_pullback_pct='.009000000000000001',
            recovery_rsi_rollover_delta='2', sell_price_gap_pct='.01', slice_quote=2 * order_quote,
            remainder_sweep_quote='5', chaser_distance_pct='.002', chaser_refresh_threshold_pct='.003',
            stale_cancel_pct='.005', sell_rsi_rollover_delta='.5', trend_sell_threshold_boost='4',
            sell_trend_hold_min_confidence='.2', trend_adx_threshold='18',
            trend_short_records=500, trend_higher_records=85),
        buy_gap=BuyGapConfig(base_gap='.008', atr_multiplier='2.5', min_gap='.005', max_gap='.03'),
        depth_budget=DepthBudgetConfig(budget_quote=per_asset_quote, lookback_minutes=1440,
                                       full_depth='.175', base_fraction='.75', max_slice_multiplier='1'),
        flash_crash=FlashCrashConfig(lookback=17, drop_pct='.01060362829599566', cooldown_seconds=29993),
    )


def capital_policy(fee_reserve, order_quote=ORDER_QUOTE, per_asset_quote=PER_ASSET_QUOTE, max_active_tranches=8):
    """Live OK RSI capital model at a stage's numbers: every buy plans `order_quote`; the
    per-asset position cap and funded cap are `per_asset_quote` (equal to the envelope in a
    stage with no per-pair cap, so only the shared envelope binds); `max_active_tranches`
    concurrent buys per symbol. The frozen 20/100/8 basis is the sealed canary/full policy."""
    return LiveCapitalConfig(slice_quote=Decimal(order_quote), position_cap_quote=Decimal(per_asset_quote),
                             max_active_tranches=max_active_tranches, funded_cap_quote=Decimal(per_asset_quote),
                             funding_fee_reserve=fee_reserve)
