"""Versioned V4 parameters: the backtested v3c candidate on the generic native modules.

V4 is a stand-alone composition (one `modular_spot` controller per pair, no shared portfolio
writer). Its module values are the candle-model candidate v3c exactly (rsibot
docs/evidence/2026-10-13-bnb-v1-v4-sep-sep/v4-analysis/V4.md): the Meridian V3 p23 modules
with an entry ATR ceiling of 0.0012, trailing activation 0.025 and adaptive minimum lock 0.010,
the flash-crash pause the backtests used (5 bars / 4% / 300 s) and no depth budget. Values are
written out here, not derived from `meridian_v3_recipe`, so V3's recipe can never move V4.

Sizing: every buy plans 100 quote and a bag sell slice is two buys, as in the backtests. The
backtests had no position cap; the per-pair budget is an explicit configuration value, never a
default. The result (an in-sample BNB year and an untuned four-pair transfer) does not
validate any live sizing.
"""
from decimal import Decimal

from controllers.generic.modular_spot_core.bag import (
    AdaptiveTrailingConfig,
    BagConfig,
    BuyGapConfig,
    FlashCrashConfig,
    LiveCapitalConfig,
)
from controllers.generic.modular_spot_core.config import AcquisitionPolicy, SignalPolicy, TrailingPolicy

RECIPE_ID = 'meridian_v4_v3c_v1'
# Candidate and the owner's sealed prepared configuration of the BNB run (bundle summary
# candidate_sha256 / config_sha256; test fixtures carry the bytes).
CANDIDATE_SHA256 = '7936a884257dd53937b489c564b7becb9533eb14a6576ac82e1465fb47e7bbc0'  # noqa: documentation
PREPARED_SHA256 = '9c6ceec48cc99132aae399cdc8ffc76dd46bd4e8e82eb1a449581f47d9be483f'  # noqa: documentation
PAIRS = ('BTC-USDC', 'ETH-USDC', 'BNB-USDC', 'SOL-USDC', 'XRP-USDC')
ORDER_QUOTE = Decimal('100')
# The native live-capital model's tranche ceiling (`LiveCapitalConfig.max_active_tranches`), as in
# the backtests; the per-pair budget (position and funded cap) is the binding limit.
MAX_ACTIVE_TRANCHES = 64
ECONOMIC_VALIDATION = 'CANDLE_MODEL_SEP25_SEP26_BNB_IN_SAMPLE_UNCAPPED'


def strategy_parameters():
    """Fresh immutable module configs; pair-specific market rules remain connector-owned."""
    return dict(
        signal=SignalPolicy(kind='rsi_rebound', rsi_length=17, oversold='29',
                            breakout_lookback=20, require_uptrend=True, atr_length=14,
                            min_atr_fraction='.0002', max_atr_fraction='.0012'),
        acquisition=AcquisitionPolicy(fractions=(Decimal('.2'),) * 5, minimum_drop='.008',
                                      cooldown_seconds=1, entry_timeout_seconds=300),
        trailing=TrailingPolicy(activation='.025', delta='.005'),
        adaptive_trailing=AdaptiveTrailingConfig(
            baseline_atr_pct='.002', activation_min='.003', activation_max='.05',
            delta_min='.001', delta_max='.02', min_locked_profit='.010',
            activation_exponent='.70', delta_exponent='.35', min_multiplier='.5', max_multiplier='2.5'),
        bag=BagConfig(
            early_stop_drawdown_pct='.03', recovery_target_pct='.02', min_profit_pct_for_sell='.02',
            recovery_arm_rsi='65', recovery_trail_pullback_pct='.009000000000000001',
            recovery_rsi_rollover_delta='2', sell_price_gap_pct='.01', slice_quote=2 * ORDER_QUOTE,
            remainder_sweep_quote='5', chaser_distance_pct='.002', chaser_refresh_threshold_pct='.003',
            stale_cancel_pct='.005', sell_rsi_rollover_delta='.5', trend_sell_threshold_boost='4',
            sell_trend_hold_min_confidence='.2', trend_adx_threshold='18',
            trend_short_records=500, trend_higher_records=85),
        buy_gap=BuyGapConfig(base_gap='.008', atr_multiplier='2.5', min_gap='.005', max_gap='.03'),
        flash_crash=FlashCrashConfig(lookback=5, drop_pct='.04', cooldown_seconds=300),
    )


def capital_policy(fee_reserve, pair_quote):
    """Live OK RSI capital model: every buy plans ORDER_QUOTE; the per-pair budget is both the
    position cap and the funded cap (fee-inclusive), and at most MAX_ACTIVE_TRANCHES buys are active."""
    pair_quote = Decimal(pair_quote)
    if pair_quote < ORDER_QUOTE:
        raise ValueError(f'the V4 per-pair budget must cover one {ORDER_QUOTE} quote buy')
    return LiveCapitalConfig(slice_quote=ORDER_QUOTE, position_cap_quote=pair_quote,
                             max_active_tranches=MAX_ACTIVE_TRANCHES, funded_cap_quote=pair_quote,
                             funding_fee_reserve=fee_reserve)
