"""Native held-inventory ("bag") lifecycle for modular_spot, mirroring live OK RSI.

Pure policy lives in hummingbot.strategy_v2.components.held_inventory (parity-tested
against HLRSIController/OKRSIController/OrderExecutor). This module binds it to the
modular runtime: configuration, closed-candle bag features, per-tick decisions and a
persisted recovery trail. It never adopts foreign inventory: the bag is exactly the
controller's orchestrator PositionHold built from its own keep_position stops.
"""
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import numpy as np
import pandas as pd
from pydantic import Field, model_validator
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from hummingbot.strategy_v2.components.held_inventory import (
    BagRecoveryPolicy,
    RecoveryTrail,
    bag_sell_signal,
    has_sell_price_gap,
    held_is_dust,
    maker_sell_price,
    net_pnl_pct,
    reanchor_after_sell,
    recovery_min_sell_price,
    recovery_slice_amount,
    recovery_step,
    sell_signal_threshold,
    sell_trend_hold,
    should_hold_as_bag,
    stale_recovery_sell,
    trail_reversal,
)
from hummingbot.strategy_v2.components.program_budget import BAG_RECOVERY_LEVEL

from .config import PolicyModel
from .domain import PolicyError, decimal, identity

RECOVERY_LEVEL = BAG_RECOVERY_LEVEL  # the program budget recognises member bag sells by this level


class BagConfig(PolicyModel):
    """Live OK RSI BNB bag and recovery settings (V1/V2 mounted configs, 2026-09-25)."""
    early_stop_drawdown_pct: Decimal = Field(default=Decimal('0.03'), gt=0, lt=1)
    recovery_target_pct: Decimal = Field(default=Decimal('0.04'), ge=0, lt=1)
    min_profit_pct_for_sell: Decimal = Field(default=Decimal('0.02'), ge=0, lt=1)
    recovery_arm_rsi: Decimal = Field(default=Decimal('78'), gt=0, le=100)
    recovery_trail_pullback_pct: Decimal = Field(default=Decimal('0.008'), gt=0, lt=1)
    recovery_rsi_rollover_delta: Decimal = Field(default=Decimal('2'), ge=0, le=100)
    sell_price_gap_pct: Decimal = Field(default=Decimal('0.01'), ge=0, lt=1)
    slice_quote: Decimal = Field(default=Decimal('200'), gt=0)
    remainder_sweep_quote: Decimal = Field(default=Decimal('5'), ge=0)
    chaser_distance_pct: Decimal = Field(default=Decimal('0.002'), ge=0, lt=1)
    chaser_refresh_threshold_pct: Decimal = Field(default=Decimal('0.003'), gt=0, lt=1)
    stale_cancel_pct: Decimal = Field(default=Decimal('0.005'), ge=0, lt=1)
    sell_rsi_rollover_delta: Decimal = Field(default=Decimal('0.5'), ge=0, le=100)
    trend_sell_threshold_boost: Decimal = Field(default=Decimal('4'), ge=0, le=100)
    sell_trend_hold_min_confidence: Decimal = Field(default=Decimal('0.2'), ge=0, le=1)
    trend_adx_threshold: Decimal = Field(default=Decimal('18'), gt=0, le=100)
    trend_short_records: int = Field(default=500, ge=31, le=3000, strict=True)
    trend_higher_records: int = Field(default=85, ge=30, le=500, strict=True)

    def policy(self) -> BagRecoveryPolicy:
        return BagRecoveryPolicy(
            early_stop_drawdown_pct=self.early_stop_drawdown_pct, recovery_target_pct=self.recovery_target_pct,
            min_profit_pct_for_sell=self.min_profit_pct_for_sell, sell_only_if_profitable=True,
            recovery_arm_rsi=float(self.recovery_arm_rsi),
            recovery_trail_pullback_pct=self.recovery_trail_pullback_pct,
            recovery_rsi_rollover_delta=float(self.recovery_rsi_rollover_delta),
            sell_price_gap_pct=self.sell_price_gap_pct, slice_quote=self.slice_quote)

    def required_records(self) -> int:
        """Rows the live trend confirmation reads: short 1m window and 15m * higher records."""
        return max(self.trend_short_records, 15 * self.trend_higher_records)


class AdaptiveTrailingConfig(PolicyModel):
    """Live OK RSI continuous adaptive trailing (controllers.shared.trailing)."""
    baseline_atr_pct: Decimal = Field(default=Decimal('0.002'), gt=0, lt=1)
    activation_min: Decimal = Field(default=Decimal('0.003'), gt=0, lt=1)
    activation_max: Decimal = Field(default=Decimal('0.05'), gt=0, lt=1)
    delta_min: Decimal = Field(default=Decimal('0.001'), gt=0, lt=1)
    delta_max: Decimal = Field(default=Decimal('0.02'), gt=0, lt=1)
    min_locked_profit: Decimal = Field(default=Decimal('0.004'), gt=0, lt=1)
    activation_exponent: Decimal = Field(default=Decimal('0.70'), ge=0, le=10)
    delta_exponent: Decimal = Field(default=Decimal('0.35'), ge=0, le=10)
    min_multiplier: Decimal = Field(default=Decimal('0.5'), gt=0, le=100)
    max_multiplier: Decimal = Field(default=Decimal('2.5'), gt=0, le=100)

    @model_validator(mode='after')
    def bounds(self):
        if not (self.activation_min < self.activation_max and self.delta_min < self.delta_max
                and self.min_multiplier <= self.max_multiplier):
            raise ValueError('adaptive trailing bounds are invalid')
        return self

    def select(self, atr_pct, activation, delta):
        from controllers.shared.trailing import calculate_continuous_adaptive_trailing
        atr = float(decimal(atr_pct, 'entry ATR', positive=True))
        selection = calculate_continuous_adaptive_trailing(
            atr_pct=atr, baseline_atr_pct=float(self.baseline_atr_pct), base_activation=activation,
            base_delta=delta, activation_min=self.activation_min, activation_max=self.activation_max,
            delta_min=self.delta_min, delta_max=self.delta_max, min_locked_profit=self.min_locked_profit,
            activation_exponent=float(self.activation_exponent), delta_exponent=float(self.delta_exponent),
            min_multiplier=float(self.min_multiplier), max_multiplier=float(self.max_multiplier))
        return selection.activation, selection.delta


class LiveCapitalConfig(PolicyModel):
    """Live OK RSI capital model (HLRSIController buy gates), replacing the fixed allocation.

    Every buy plans ``slice_quote`` (``usd_per_entry``). With ``funded_cap_quote`` set (live
    V2 ``funding.max_position_quote``, at cost), the plan is first resized to the per-pair
    funding ceiling ``(funded_cap - used - pending) / (1 + funding_fee_reserve)``. The
    (resized) buy is admitted only while ``held bag + active buys`` marked at mid plus the buy
    stays within ``position_cap_quote`` (``max_total_position_usd``) and fewer than
    ``max_active_tranches`` buys are active (``max_executors_per_side``). Without funding
    (V1) a buy the wallet cannot pay for is refused (live ``balance_low``), never shrunk.
    Live BNB: V2 is 100 / 500 / 8 with funding 505 at cost (and a strategy-wide 2500
    allocation across pairs, not modelled per pair); V1 is 200 / 2500 / 8, unfunded.
    Every buy plans the full slice: the live scout/runner split and dynamic signal-strength
    sizing are entry-side behaviour, not modelled here.
    """
    slice_quote: Decimal = Field(default=Decimal('100'), gt=0)
    position_cap_quote: Optional[Decimal] = Field(default=None, gt=0)  # None: no position cap (live V2 BNB: 500)
    max_active_tranches: int = Field(default=8, ge=1, le=64, strict=True)
    funded_cap_quote: Optional[Decimal] = Field(default=None, gt=0)
    funding_fee_reserve: Decimal = Field(default=Decimal('0.003'), ge=0, lt=1)

    @model_validator(mode='after')
    def slice_within_cap(self):
        if self.position_cap_quote is not None and self.slice_quote > self.position_cap_quote:
            raise ValueError('slice_quote must not exceed position_cap_quote (live usd_per_entry <= max_total_position_usd)')
        return self


class BuyGapConfig(PolicyModel):
    """Live OK RSI ``_effective_price_gap`` (use_adaptive_filters): an ATR-scaled buy gap.

    gap = clamp(atr_pct * atr_multiplier, min_gap, max_gap), falling back to ``base_gap``
    without ATR. A new buy needs an absolute gap of at least ``gap`` from the last active buy
    (``_has_sufficient_price_gap``) and from the held cost basis
    (``_has_sufficient_avg_entry_gap_for_buy``), in either direction.
    Live BNB: base 0.8%, x3 ATR, clamped 0.5-1.8%.
    """
    base_gap: Decimal = Field(default=Decimal('0.008'), gt=0, lt=1)
    atr_multiplier: Decimal = Field(default=Decimal('3'), ge=0, le=100)
    min_gap: Decimal = Field(default=Decimal('0.005'), ge=0, lt=1)
    max_gap: Decimal = Field(default=Decimal('0.018'), gt=0, lt=1)

    @model_validator(mode='after')
    def ordered(self):
        if self.min_gap > self.max_gap:
            raise ValueError('min_gap must not exceed max_gap')
        return self


def live_buy_gap(cfg: BuyGapConfig, atr_pct: Optional[Decimal]) -> Decimal:
    dynamic = None if atr_pct is None else decimal(atr_pct, 'ATR fraction') * cfg.atr_multiplier
    gap = dynamic if dynamic is not None and dynamic > 0 else cfg.base_gap
    return min(max(gap, cfg.min_gap), cfg.max_gap)


STORED_EXECUTOR_WINDOW = 20  # ExecutorOrchestrator keeps each controller's 20 most recent stored executors


def live_sell_reference(stored, *, window: int = STORED_EXECUTOR_WINDOW) -> Optional[Decimal]:
    """Live ``_last_same_side_ref_price(SELL)`` over the orchestrator's stored window.

    ``stored`` holds one ``(order_key, filled_sell_price_or_None)`` per terminal executor of
    the controller, of any kind. Only the ``window`` most recent by ``order_key`` are visible
    (live ``executors_info``); the newest filled sell among them is the reference.
    """
    for _, price in sorted(stored, key=lambda row: row[0], reverse=True)[:window]:
        if price is not None:
            return decimal(price, 'sell reference', positive=True)
    return None


def live_capital_exposure(*, held_amount: Decimal, held_dust: bool, mid: Decimal, active) -> Decimal:
    """Live ``_total_position_value_usd``: the non-dust held bag at mid plus active buys
    (owned base at mid, unfilled remainder at its commitment price)."""
    mid = decimal(mid, 'mid', positive=True)
    exposure = Decimal('0') if held_dust else decimal(held_amount, 'held amount') * mid
    for owned, outstanding, price in active:
        exposure += decimal(owned, 'owned base') * mid
        if decimal(outstanding, 'outstanding base') > 0:
            exposure += outstanding * decimal(price, 'commitment price', positive=True)
    return exposure


def live_capital_decision(cap: LiveCapitalConfig, *, active_count: int, exposure: Decimal, available_quote: Decimal,
                          funded_used_quote: Optional[Decimal] = None):
    """Quote for the next buy, or ``(None, reason)``, in the live gate order. Pure, so the
    research verifier can recompute every recorded capital decision."""
    if active_count >= cap.max_active_tranches:
        return None, 'max_executors'
    quote = cap.slice_quote
    if cap.funded_cap_quote is not None:
        if funded_used_quote is None:
            raise PolicyError('funded capital requires the funding used quote')
        ceiling = (cap.funded_cap_quote - decimal(funded_used_quote, 'funded used')) / (1 + cap.funding_fee_reserve)
        quote = min(quote, max(ceiling, Decimal('0')))
        if quote <= 0:
            return None, 'funding_exhausted'
    if cap.position_cap_quote is not None and decimal(exposure, 'exposure') + quote > cap.position_cap_quote:
        return None, 'max_position'
    if cap.funded_cap_quote is None and decimal(available_quote, 'available quote') < quote:
        return None, 'cash_exhausted'  # live V1 balance_low: refused, not resized
    return quote, 'admitted'


def live_capital_quote(cap: LiveCapitalConfig, *, held_amount: Decimal, held_dust: bool, mid: Decimal, active,
                       available_quote: Decimal, funded_used_quote: Optional[Decimal] = None):
    """``live_capital_exposure`` then ``live_capital_decision``; ``active`` holds one
    ``(owned_base, outstanding_base, commitment_price)`` per active buy."""
    rows = tuple(active)
    exposure = live_capital_exposure(held_amount=held_amount, held_dust=held_dust, mid=mid, active=rows)
    return live_capital_decision(cap, active_count=len(rows), exposure=exposure, available_quote=available_quote,
                                 funded_used_quote=funded_used_quote)


@dataclass(frozen=True)
class BagFeatures:
    """Closed-candle inputs of the live bag/sell/trailing decisions."""
    rsi_prev: Decimal
    ema_fast: Decimal
    macd_hist: Decimal
    macd_hist_prev: Decimal
    atr_pct: Decimal
    trend_direction: str
    trend_confidence: Decimal


def bag_features(closed: pd.DataFrame, *, rsi_prev, now: float, short_records: int,
                 higher_records: int, adx_threshold) -> BagFeatures:
    """EMA9, MACD(14,22,9), Wilder ATR14 and the live multi-timeframe trend on closed candles.

    Live OK RSI reads ``short_records`` rows including the forming candle, so it keeps
    ``short_records - 1`` closed rows (and ``15 * higher_records - 1`` for 5m/15m).
    """
    from hummingbot.strategy_v2.components.candle_aggregation import derive_higher_timeframe_candles
    from hummingbot.strategy_v2.utils.market_analysis import analyze_multi_timeframe_trend

    if len(closed) < 30:
        raise PolicyError('bag feature warmup unavailable')
    close = closed.close.astype(float)
    ema = close.ewm(span=9, adjust=False).mean()
    macd = close.ewm(span=14, adjust=False).mean() - close.ewm(span=22, adjust=False).mean()
    hist = macd - macd.ewm(span=9, adjust=False).mean()
    high, low = closed.high.astype(float), closed.low.astype(float)
    true_range = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
    atr = true_range.ewm(alpha=1.0 / 14, min_periods=14, adjust=False).mean()
    values = (ema.iloc[-1], hist.iloc[-1], hist.iloc[-2], atr.iloc[-1] / close.iloc[-1])
    if not all(np.isfinite(v) for v in values):
        raise PolicyError('bag features are not finite')
    short_df = closed.iloc[-(short_records - 1):]
    base_df = closed.iloc[-(max(short_records, 15 * higher_records) - 1):]
    medium = derive_higher_timeframe_candles(base_df, base_interval='1m', target_interval='5m',
                                             current_time=now, max_records=higher_records)
    long = derive_higher_timeframe_candles(base_df, base_interval='1m', target_interval='15m',
                                           current_time=now, max_records=higher_records)
    # Same call as live rsi_signals.analyze_rsi_trend_confirmation (short/medium/long labels).
    trend = analyze_multi_timeframe_trend({'short': short_df, 'medium': medium, 'long': long},
                                          adx_length=14, adx_threshold=float(adx_threshold))
    return BagFeatures(Decimal(str(rsi_prev)), *(Decimal(str(v)) for v in values),
                       trend.direction.value, Decimal(str(float(trend.confidence or 0.0))))


@dataclass(frozen=True)
class HeldBag:
    """The controller's held position (orchestrator PositionSummary) seen by one tick."""
    amount: Decimal
    breakeven_price: Decimal
    remaining_fees_quote: Decimal

    @classmethod
    def from_positions(cls, positions, *, connector: str, pair: str) -> 'HeldBag':
        rows = [p for p in positions if getattr(p, 'connector_name', None) == connector
                and getattr(p, 'trading_pair', None) == pair and decimal(p.amount, 'held amount') > 0]
        if any(getattr(p.side, 'name', None) != 'BUY' for p in rows):
            raise PolicyError('bag mode holds long inventory only')
        if len(rows) > 1:
            raise PolicyError('one blended held position per market is required')
        if not rows:
            return cls(Decimal('0'), Decimal('0'), Decimal('0'))
        p = rows[0]
        fees = getattr(p, 'remaining_fees_quote', None)
        return cls(decimal(p.amount, 'held amount'), decimal(p.breakeven_price, 'held breakeven', positive=True),
                   decimal(p.cum_fees_quote if fees is None else fees, 'held fees'))


@dataclass(frozen=True)
class ActiveTranche:
    executor_id: str
    fill_basis: Decimal


@dataclass(frozen=True)
class LiveSell:
    executor_id: str
    proposal_price: Decimal


@dataclass(frozen=True)
class SellRequest:
    amount: Decimal
    proposal_price: Decimal
    min_sell_price: Optional[Decimal]
    reason: str


@dataclass(frozen=True)
class BagTick:
    trail: Optional[RecoveryTrail]
    cancels: tuple
    sell: Optional[SellRequest]
    stops: tuple
    reason: str


def decide(cfg: BagConfig, *, trail: Optional[RecoveryTrail], held: HeldBag, tranches, live_sells,
           last_sell_price: Optional[Decimal], mid: Decimal, best_ask: Decimal, features, bag: BagFeatures,
           first_tick_after_close: bool, tick: Decimal, amount_step: Decimal, minimum_amount: Decimal,
           minimum_notional: Decimal, trend=None) -> BagTick:
    """One live OK RSI tick for held inventory: stale cancel, recovery, then early stops.

    Mirrors HLRSIController.stop_actions_proposal order. ``features.rsi`` is the last
    closed RSI; the sell signal exists only on the first tick after a candle close.
    ``trend`` optionally supplies (direction, confidence) lazily; it is only called when
    a decision depends on it, otherwise ``bag.trend_direction/trend_confidence`` are used.
    """
    policy = cfg.policy()
    mid = decimal(mid, 'mid', positive=True)
    best_ask = decimal(best_ask, 'best ask', positive=True)
    rsi, rsi_prev, ema = float(features.rsi), float(bag.rsi_prev), float(bag.ema_fast)
    dust = held_is_dust(held.amount, mid, min_amount=max(amount_step, minimum_amount), min_notional=minimum_notional)
    if trail is not None and (dust or trail.breakeven_price != held.breakeven_price.quantize(Decimal('1e-8'))):
        trail = None  # live _sync_recovery_trails drops the trail of a bag that is gone (dust is no bag)
    cached = []

    def trend_state():
        if not cached:
            cached.append(trend() if trend is not None else (bag.trend_direction, bag.trend_confidence))
        return cached[0]

    def strong_up():
        direction, confidence = trend_state()
        return direction == 'up' and Decimal(str(confidence)) >= cfg.sell_trend_hold_min_confidence

    def trend_hold():
        ready = any(trail_reversal(trail, mid, rsi, ema, policy))
        return sell_trend_hold(strong_up(), mid, rsi, rsi_prev, ema, float(cfg.sell_rsi_rollover_delta),
                               trail_reversal_ready=ready)

    cancels = tuple(sell.executor_id for sell in live_sells
                    if mid >= sell.proposal_price * (1 + cfg.stale_cancel_pct)
                    and stale_recovery_sell(trend_hold(), mid, sell.proposal_price, cfg.stale_cancel_pct))
    sell, reason = None, 'no_bag'
    if not live_sells and not dust:
        signal = False
        if first_tick_after_close:
            common = dict(rsi=rsi, rsi_prev=rsi_prev, price=mid, ema_fast=ema, macd_hist=float(bag.macd_hist),
                          macd_hist_prev=float(bag.macd_hist_prev), policy=policy,
                          inventory_net_pnl=net_pnl_pct(held.amount, held.breakeven_price,
                                                        held.remaining_fees_quote, mid),
                          sell_rsi_rollover_delta=float(cfg.sell_rsi_rollover_delta))
            # The trend only moves the threshold up, or down to no lower than 60, so a signal that
            # fails even at min(60, base) cannot fire whatever the trend; only then is it evaluated.
            floor = min(60.0, float(policy.recovery_arm_rsi))
            if bag_sell_signal(sell_threshold=floor, strong_bullish_trend=False, trail_reversal_ready=False, **common):
                direction, confidence = trend_state()
                threshold = sell_signal_threshold(
                    float(policy.recovery_arm_rsi), trend_direction=direction, trend_confidence=float(confidence),
                    trend_sell_threshold_boost=float(cfg.trend_sell_threshold_boost))
                signal = bag_sell_signal(sell_threshold=threshold, strong_bullish_trend=strong_up(),
                                         trail_reversal_ready=any(trail_reversal(trail, mid, rsi, ema, policy)),
                                         **common)
        trail, reason = recovery_step(
            trail, amount=held.amount, breakeven_price=held.breakeven_price,
            remaining_fees_quote=held.remaining_fees_quote, price=mid, rsi=rsi, ema_fast=ema, policy=policy,
            sell_signal=signal, trend_hold=trend_hold() if signal else False)
        if reason.startswith('sell:'):
            proposal = maker_sell_price(mid, best_ask, tick)
            amount = recovery_slice_amount(held.amount, proposal, policy, min_amount=max(amount_step, minimum_amount),
                                           min_quote=minimum_notional, remainder_sweep_quote=cfg.remainder_sweep_quote)
            amount = (amount / amount_step).to_integral_value(rounding='ROUND_FLOOR') * amount_step
            if amount <= 0:
                reason = 'slice_below_minimum'
            elif not has_sell_price_gap(proposal, last_sell_price, policy):
                reason = 'price-gap'
            else:
                sell = SellRequest(amount, proposal, recovery_min_sell_price(
                    held.breakeven_price, held.amount, held.remaining_fees_quote, policy), reason.split(':', 1)[1])
                trail = reanchor_after_sell(trail, mid, rsi)
    elif live_sells:
        reason = 'recovery_sell_live'
    stops = tuple(t.executor_id for t in tranches if should_hold_as_bag(t.fill_basis, mid, policy))
    return BagTick(trail, cancels, sell, stops, reason)


def chaser_config(cfg: BagConfig):
    from hummingbot.strategy_v2.executors.order_executor.data_types import LimitChaserConfig
    return LimitChaserConfig(distance=cfg.chaser_distance_pct, refresh_threshold=cfg.chaser_refresh_threshold_pct)


def recovery_sell_config(controller_config, request: SellRequest, *, identifier: str, timestamp: float):
    """LIMIT_CHASER SELL OrderExecutor exactly as live OK RSI proposes a recovery exit."""
    from hummingbot.core.data_type.common import PositionAction, TradeType
    from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
    return OrderExecutorConfig(
        id=identifier, timestamp=timestamp, controller_id=controller_config.id,
        connector_name=controller_config.connector_name, trading_pair=controller_config.trading_pair,
        side=TradeType.SELL, amount=request.amount, price=request.proposal_price,
        execution_strategy=ExecutionStrategy.LIMIT_CHASER, chaser_config=chaser_config(controller_config.bag),
        position_action=PositionAction.CLOSE, leverage=1, level_id=f'{RECOVERY_LEVEL}:{request.reason}',
        min_sell_price=request.min_sell_price)


def is_recovery_sell(config) -> bool:
    return (getattr(config, 'type', None) == 'order_executor' and getattr(config.side, 'name', None) == 'SELL'
            and str(getattr(config, 'level_id', '')).startswith(RECOVERY_LEVEL))


class BagTrailStore:
    """Persisted recovery trail (live V2 persists trails; V1 loses them on restart)."""

    def __init__(self, recorder, controller_id: str, connector: str, pair: str, *, metadata_model=None):
        if metadata_model is None:
            from hummingbot.model.metadata import Metadata
            metadata_model = Metadata
        self.model, self.recorder = metadata_model, recorder
        self.key = 'modular-spot:bag:v1:' + identity([recorder.config_file_path, controller_id, connector, pair])

    @staticmethod
    def encode(trail: Optional[RecoveryTrail], evaluated_candle: Optional[int]) -> str:
        body = None if trail is None else {
            'breakeven_price': str(trail.breakeven_price), 'armed_price': str(trail.armed_price),
            'peak_price': str(trail.peak_price), 'peak_rsi': trail.peak_rsi,
            'tracked_amount': str(trail.tracked_amount)}
        return json.dumps({'trail': body, 'evaluated_candle': evaluated_candle}, sort_keys=True,
                          separators=(',', ':'), allow_nan=False)

    @staticmethod
    def decode(raw: str):
        try:
            data = json.loads(raw)
            if set(data) != {'trail', 'evaluated_candle'}:
                raise PolicyError('incomplete bag checkpoint')
            body = data['trail']
            trail = None if body is None else RecoveryTrail(
                decimal(body['breakeven_price'], 'trail breakeven', positive=True),
                decimal(body['armed_price'], 'trail arm', positive=True),
                decimal(body['peak_price'], 'trail peak', positive=True),
                None if body['peak_rsi'] is None else float(body['peak_rsi']),
                decimal(body['tracked_amount'], 'trail amount'))
            candle = data['evaluated_candle']
            if candle is not None and type(candle) is not int:
                raise PolicyError('invalid bag checkpoint candle')
            return trail, candle
        except (KeyError, TypeError, ValueError) as exc:
            raise PolicyError('invalid bag checkpoint') from exc

    def load(self):
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                row = session.get(self.model, self.key)
                raw = None if row is None else row.value
        except SQLAlchemyError as exc:
            raise PolicyError('bag store unavailable') from exc
        return (None, None, None) if raw is None else (*self.decode(raw), raw)

    def compare_and_swap(self, expected: Optional[str], trail, evaluated_candle) -> bool:
        payload = self.encode(trail, evaluated_candle)
        if payload == expected:
            return True
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                with session.begin():
                    if expected is None:
                        session.add(self.model(key=self.key, value=payload))
                        session.flush()
                        return True
                    changed = session.execute(update(self.model).where(
                        self.model.key == self.key, self.model.value == expected).values(value=payload))
                    return changed.rowcount == 1
        except IntegrityError:
            return False
        except SQLAlchemyError as exc:
            raise PolicyError('bag persistence failed; no dispatch') from exc
