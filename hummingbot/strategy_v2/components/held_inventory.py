"""Strategy-neutral held-inventory ("bag") policy.

Pure functions for the OK RSI bag lifecycle so live controllers, native modular
controllers and research simulators share one definition:

1. An active long tranche whose price falls ``early_stop_drawdown_pct`` below its
   position basis is stopped with ``keep_position=True``: nothing is sold, the
   executor slot is freed and its inventory joins the held position.
2. Held inventory is one blended long position per market (``HeldInventory``,
   mirroring live ``PositionHold`` sequential accounting): running-average
   breakeven, remaining fees scaled down by sells, reset when flat.
3. Recovery: once net PnL reaches ``max(recovery_target_pct, min_profit)`` the
   bag waits for RSI >= ``recovery_arm_rsi`` to arm a trail, tracks new peaks,
   and sells a slice on an ``recovery_trail_pullback_pct`` pullback or on an RSI
   rollover of ``recovery_rsi_rollover_delta`` from its peak while price is below
   the fast EMA. A drop below target disarms the trail.

Mirrors ``HLRSIController._check_active_executor_drawdown_stop``,
``_position_net_pnl_pct``, ``_recovery_reversal_details``,
``_check_held_bag_recovery``, ``_has_sufficient_price_gap`` (SELL) and the
OK RSI recovery slice. Parity is pinned by tests against those methods.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import ROUND_CEILING, Decimal
from typing import Optional, Tuple

ZERO = Decimal("0")


def _finite(value: Decimal, name: str) -> Decimal:
    value = Decimal(str(value))
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


@dataclass(frozen=True)
class BagRecoveryPolicy:
    early_stop_drawdown_pct: Decimal = Decimal("0.03")
    recovery_target_pct: Decimal = Decimal("0.04")
    min_profit_pct_for_sell: Decimal = Decimal("0.02")
    sell_only_if_profitable: bool = True
    recovery_arm_rsi: float = 78.0
    recovery_trail_pullback_pct: Decimal = Decimal("0.008")
    recovery_rsi_rollover_delta: float = 2.0
    sell_price_gap_pct: Decimal = Decimal("0.01")
    slice_quote: Decimal = Decimal("200")

    def __post_init__(self):
        for name in ("early_stop_drawdown_pct", "recovery_target_pct", "min_profit_pct_for_sell",
                     "recovery_trail_pullback_pct", "sell_price_gap_pct", "slice_quote"):
            value = _finite(getattr(self, name), name)
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
            object.__setattr__(self, name, value)
        if not 0 < self.recovery_arm_rsi <= 100 or self.recovery_rsi_rollover_delta < 0:
            raise ValueError("recovery RSI settings are invalid")
        if type(self.sell_only_if_profitable) is not bool:
            raise ValueError("sell_only_if_profitable must be boolean")

    @property
    def effective_target(self) -> Decimal:
        if self.sell_only_if_profitable:
            return max(self.recovery_target_pct, self.min_profit_pct_for_sell)
        return self.recovery_target_pct


def should_hold_as_bag(basis_price: Decimal, price: Decimal, policy: BagRecoveryPolicy) -> bool:
    """True when a long tranche's gross drawdown from its basis reaches the early-stop threshold."""
    threshold = policy.early_stop_drawdown_pct
    if threshold <= 0 or price <= 0 or basis_price is None or basis_price <= 0:
        return False
    return (price - basis_price) / basis_price <= -threshold


def held_is_dust(amount: Decimal, price: Decimal, *, min_amount: Decimal, min_notional: Decimal) -> bool:
    """Live ``_inventory_is_dust``: empty, below the venue minimum size, or below minimum notional.

    A dust bag is not a bag: it is never sold, never counts as held inventory and never
    blocks a campaign from being flat."""
    if amount <= 0:
        return True
    return amount < min_amount or (min_notional > 0 and price > 0 and amount * price < min_notional)


@dataclass(frozen=True)
class HeldInventory:
    """Blended long held position with the sequential semantics of live ``PositionHold``.

    Mirrors ``PositionHold.get_position_summary`` when order records exist: buys
    move a running-average breakeven and add their fees to remaining fees; sells
    realize ``(price - breakeven) * amount``, scale remaining fees by the unsold
    fraction and reset breakeven and remaining fees when the position is flat.
    ``fees_paid_quote`` keeps every fee for total PnL.
    """
    amount: Decimal = ZERO
    breakeven_price: Decimal = ZERO
    remaining_fees_quote: Decimal = ZERO
    realized_pnl_quote: Decimal = ZERO
    fees_paid_quote: Decimal = ZERO

    def add_buy(self, base: Decimal, quote: Decimal, fee_quote: Decimal = ZERO) -> "HeldInventory":
        if base <= 0 or quote <= 0 or fee_quote < 0:
            raise ValueError("held buy requires positive base/quote and nonnegative fee")
        amount = self.amount + base
        return replace(self, amount=amount, breakeven_price=(self.amount * self.breakeven_price + quote) / amount,
                       remaining_fees_quote=self.remaining_fees_quote + fee_quote,
                       fees_paid_quote=self.fees_paid_quote + fee_quote)

    def add_sell(self, base: Decimal, quote: Decimal, fee_quote: Decimal = ZERO) -> "HeldInventory":
        if base <= 0 or quote <= 0 or fee_quote < 0 or base > self.amount:
            raise ValueError("held sell must be positive and within held amount")
        amount = self.amount - base
        realized = self.realized_pnl_quote + (quote / base - self.breakeven_price) * base
        remaining_fees = self.remaining_fees_quote * amount / self.amount
        return replace(self, amount=amount, breakeven_price=self.breakeven_price if amount else ZERO,
                       remaining_fees_quote=remaining_fees if amount else ZERO, realized_pnl_quote=realized,
                       fees_paid_quote=self.fees_paid_quote + fee_quote)

    def pnl_quote(self, price: Decimal) -> Decimal:
        """Total held PnL: realized minus every fee paid plus mark of the remaining inventory."""
        return self.realized_pnl_quote - self.fees_paid_quote + (price - self.breakeven_price) * self.amount


def net_pnl_pct(amount: Decimal, breakeven_price: Decimal, remaining_fees_quote: Decimal,
                price: Decimal) -> Optional[Decimal]:
    """Mirror of ``HLRSIController._position_net_pnl_pct``."""
    if price <= 0 or amount is None or amount <= 0 or breakeven_price is None or breakeven_price <= 0:
        return None
    cost_quote = amount * breakeven_price
    gross = (price - breakeven_price) * amount
    return (gross - (remaining_fees_quote or ZERO)) / cost_quote


def recovery_min_sell_price(breakeven_price: Decimal, amount: Decimal, remaining_fees_quote: Decimal,
                            policy: BagRecoveryPolicy) -> Optional[Decimal]:
    """Limit floor for a recovery sell (None when not profit-constrained)."""
    if not policy.sell_only_if_profitable or amount <= 0:
        return None
    return breakeven_price * (1 + policy.min_profit_pct_for_sell) + remaining_fees_quote / amount


def maker_sell_price(mid_price: Decimal, best_ask: Decimal, tick: Decimal) -> Decimal:
    """Mirror of ``_shade_limit_maker_price`` for SELL: ceil(max(mid, ask)) to tick, one tick above the touch."""
    if tick is None or tick <= 0:
        return best_ask
    aligned = (max(mid_price, best_ask) / tick).to_integral_value(rounding=ROUND_CEILING) * tick
    price = aligned + tick if aligned <= best_ask else aligned
    return price if price > 0 else best_ask


def sell_trend_hold(strong_bullish_trend: bool, price: Decimal, rsi: Optional[float], rsi_prev: Optional[float],
                    ema_fast: Optional[float], sell_rsi_rollover_delta: float, *,
                    trail_reversal_ready: bool = False) -> bool:
    """Mirror of ``_sell_trend_hold_active``: hold only in a strong bullish trend when no recovery
    trail (including one frozen behind a live sell) is reversal-ready and there is no RSI rollover
    below the fast EMA."""
    if not strong_bullish_trend or trail_reversal_ready:
        return False
    rollover = rsi is not None and rsi_prev is not None and (rsi_prev - rsi) >= sell_rsi_rollover_delta
    below_ema = ema_fast is not None and price < Decimal(str(ema_fast))
    return not (rollover and below_ema)


def sell_signal_threshold(base_sell: float, *, regime_label: Optional[str] = None, regime_confidence: float = 0.0,
                          trend_direction: Optional[str] = None, trend_confidence: float = 0.0,
                          trend_sell_threshold_boost: float = 4.0, trend_confirmation_enabled: bool = True) -> float:
    """Mirror of ``_get_dynamic_rsi_thresholds`` (sell side) then ``_apply_trend_confirmation_to_thresholds``."""
    sell = float(base_sell)
    if regime_label is not None:
        adjust = 3.0 * max(0.0, min(1.0, float(regime_confidence)))
        if "Range" in regime_label:
            sell += adjust
        elif "Trend_Up" in regime_label:
            sell += adjust * 0.7
        elif "Trend_Down" in regime_label:
            sell -= adjust
        if any(label in regime_label for label in ("Range", "Trend_Up", "Trend_Down")):
            sell = max(60.0, min(80.0, sell))
    if trend_confirmation_enabled and trend_direction is not None:
        boost = float(trend_sell_threshold_boost) * max(0.0, min(1.0, float(trend_confidence or 0.0)))
        if trend_direction == "up":
            sell = min(88.0, sell + boost)
        elif trend_direction == "down":
            sell = max(60.0, sell - boost * 0.5)
    return float(sell)


def bag_sell_signal(*, rsi: Optional[float], rsi_prev: Optional[float], sell_threshold: float, price: Decimal,
                    ema_fast: Optional[float], macd_hist: Optional[float], macd_hist_prev: Optional[float],
                    inventory_net_pnl: Optional[Decimal], strong_bullish_trend: bool, trail_reversal_ready: bool,
                    policy: BagRecoveryPolicy, sell_rsi_rollover_delta: float = 0.5) -> bool:
    """Mirror of the SELL (-1) branch of ``_determine_signal`` for a non-dust held bag.

    RSI at/above the dynamic threshold, a reversal (RSI rollover below the fast EMA,
    or a falling MACD histogram), no trend hold, and (if profit-constrained) net
    inventory PnL >= min profit. Live emits it only on the first tick after a
    candle closes.
    """
    if rsi is None or rsi < sell_threshold or price is None or price <= 0:
        return False
    rollover = rsi_prev is not None and (rsi_prev - rsi) >= sell_rsi_rollover_delta
    below_ema = ema_fast is not None and price < Decimal(str(ema_fast))
    macd_rollover = macd_hist is not None and macd_hist_prev is not None and macd_hist < macd_hist_prev
    if not ((rollover and below_ema) or macd_rollover):
        return False
    if strong_bullish_trend and not (trail_reversal_ready or (rollover and below_ema)):
        return False
    if policy.sell_only_if_profitable and (inventory_net_pnl is None or inventory_net_pnl < policy.min_profit_pct_for_sell):
        return False
    return True


def chaser_sell_price(market_price: Decimal, *, distance: Decimal, min_sell_price: Optional[Decimal],
                      fee_rate: Decimal, tick: Optional[Decimal]) -> Decimal:
    """Mirror of ``OrderExecutor.get_order_price`` for a LIMIT_CHASER SELL with a percentage fee.

    Target = market * (1 + distance); with a floor it is at least floor / (1 - fee rate),
    rounded up to the price tick.
    """
    price = market_price * (1 + distance)
    if min_sell_price is None:
        return price
    if not 0 <= fee_rate < 1:
        raise ValueError("fee rate must be in [0, 1)")
    price = max(price, min_sell_price / (1 - fee_rate))
    if tick is not None and tick > 0:
        price = (price / tick).to_integral_value(rounding=ROUND_CEILING) * tick
    return price


def chaser_needs_refresh(order_price: Decimal, target_price: Decimal, market_price: Decimal,
                         refresh_threshold: Decimal) -> bool:
    """Mirror of ``OrderExecutor.control_limit_chaser``: renew when the target drifted past the threshold."""
    return abs(target_price - order_price) > market_price * refresh_threshold


def stale_recovery_sell(trend_hold: bool, price: Decimal, proposal_price: Decimal, stale_pct: Decimal) -> bool:
    """Mirror of ``_maybe_cancel_stale_recovery_sell_executors`` for one recovery sell."""
    return trend_hold and stale_pct > 0 and proposal_price > 0 and price >= proposal_price * (1 + stale_pct)


def has_sell_price_gap(price: Decimal, last_sell_price: Optional[Decimal], policy: BagRecoveryPolicy) -> bool:
    """Mirror of ``_has_sufficient_price_gap`` for SELL: either direction from the last sell."""
    gap = policy.sell_price_gap_pct
    if gap <= 0:
        return True
    if price is None or price <= 0:
        return False
    if last_sell_price is None or last_sell_price <= 0:
        return True
    return abs(price - last_sell_price) / last_sell_price >= gap


def recovery_slice_amount(position_amount: Decimal, price: Decimal, policy: BagRecoveryPolicy, *,
                          min_amount: Decimal = ZERO, min_quote: Decimal = ZERO,
                          remainder_sweep_quote: Decimal = ZERO) -> Decimal:
    """OK RSI buy_sell recovery sizing.

    Target = min(position, slice_quote / price), taking the whole position when the
    remainder is worth at most ``remainder_sweep_quote`` (``_signal_sell_target_amount``).
    The base resolver (``HLRSIController._resolve_recovery_sell_amount``) lifts a
    sub-minimum target to the minimum and sweeps an unsellable remainder; OK RSI keeps
    that sweep only while it adds at most ``remainder_sweep_quote`` and otherwise falls
    back to a valid target (``OKRSIController._resolve_recovery_sell_amount``).
    """
    if position_amount <= 0 or price <= 0:
        return ZERO

    def unsellable(amount):
        return amount < min_amount or (min_quote > 0 and amount * price < min_quote)

    target = min(position_amount, policy.slice_quote / price) if policy.slice_quote > 0 else position_amount
    remainder = position_amount - target
    if remainder > 0 and remainder * price <= remainder_sweep_quote:
        target = position_amount
    if target <= 0 or unsellable(position_amount):
        return ZERO
    resolved = min_amount if unsellable(target) else target
    remainder = position_amount - resolved
    if remainder > 0 and unsellable(remainder):
        resolved = position_amount
    resolved = min(resolved, position_amount)
    if resolved <= target or (resolved - target) * price <= remainder_sweep_quote:
        return resolved
    if not unsellable(target) and target <= position_amount:
        return target
    return resolved


@dataclass(frozen=True)
class RecoveryTrail:
    breakeven_price: Decimal
    armed_price: Decimal
    peak_price: Decimal
    peak_rsi: Optional[float]
    tracked_amount: Decimal


def recovery_step(trail: Optional[RecoveryTrail], *, amount: Decimal, breakeven_price: Decimal,
                  remaining_fees_quote: Decimal, price: Decimal, rsi: Optional[float],
                  ema_fast: Optional[float], policy: BagRecoveryPolicy, sell_signal: bool = False,
                  trend_hold: bool = False) -> Tuple[Optional[RecoveryTrail], str]:
    """One recovery evaluation for one held position.

    Returns the next trail and a decision: ``below_target``, ``wait_overbought``,
    ``armed``, ``new_peak``, ``hold`` (no reversal / trend hold) or
    ``sell:<reason>``. On a sell decision the trail is returned unchanged; call
    ``reanchor_after_sell`` only when the slice is actually submitted (a sell
    blocked by the price gap keeps its trail, like the live controller).
    A breakeven change (new bag merged or partial fill) starts a new trail, as
    the live trail is keyed by breakeven.
    """
    pnl = net_pnl_pct(amount, breakeven_price, remaining_fees_quote, price)
    if pnl is None:
        return trail, "no_position"
    if trail is not None and trail.breakeven_price != Decimal(str(breakeven_price)).quantize(Decimal("1e-8")):
        trail = None
    if pnl < policy.effective_target:
        return None, "below_target"
    if trail is None:
        if rsi is None or rsi < policy.recovery_arm_rsi:
            return None, "wait_overbought"
        return RecoveryTrail(Decimal(str(breakeven_price)).quantize(Decimal("1e-8")), price, price, rsi, amount), "armed"
    trail = replace(trail, tracked_amount=amount)
    if price > trail.peak_price:
        peak_rsi = rsi if rsi is not None and (trail.peak_rsi is None or rsi > trail.peak_rsi) else trail.peak_rsi
        return replace(trail, peak_price=price, peak_rsi=peak_rsi), "new_peak"
    pullback_ready, rollover_ready = trail_reversal(trail, price, rsi, ema_fast, policy)
    if pullback_ready:
        reason = "pullback-trail"
    elif rollover_ready:
        reason = "rsi-rollover"
    elif sell_signal and not trend_hold:
        reason = "sell-signal"
    else:
        return trail, "hold"
    return trail, f"sell:{reason}"


def trail_reversal(trail: Optional[RecoveryTrail], price: Decimal, rsi: Optional[float],
                   ema_fast: Optional[float], policy: BagRecoveryPolicy) -> Tuple[bool, bool]:
    """Mirror of ``_recovery_reversal_details``: (pullback from peak, RSI rollover below the fast EMA)."""
    if trail is None or price is None or price <= 0 or trail.peak_price <= 0:
        return False, False
    pullback = max((trail.peak_price - price) / trail.peak_price, ZERO)
    rollover = (rsi is not None and trail.peak_rsi is not None
                and rsi <= trail.peak_rsi - policy.recovery_rsi_rollover_delta)
    below_ema = ema_fast is not None and price < Decimal(str(ema_fast))
    return pullback >= policy.recovery_trail_pullback_pct, bool(rollover and below_ema)


def reanchor_after_sell(trail: RecoveryTrail, price: Decimal, rsi: Optional[float]) -> RecoveryTrail:
    return replace(trail, armed_price=price, peak_price=price, peak_rsi=rsi)
