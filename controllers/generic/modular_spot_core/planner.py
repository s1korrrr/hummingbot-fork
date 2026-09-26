"""Compose policies into bounded proposals; caller must persist before dispatch."""
from dataclasses import dataclass
from decimal import Decimal

from hummingbot.strategy_v2.components.held_inventory import held_is_dust

from .acquisition import next_stage
from .domain import PolicyError, decimal, identity
from .portfolio import allocate
from .risk import risk_update
from .signals import signal_decision
from .state import DecisionState, reconcile_claims


@dataclass(frozen=True)
class EntryProposal:
    identifier: str
    price: Decimal
    amount: Decimal
    timestamp: float
    cycle: int
    stage: int


def plan(state, inventory, *, fingerprint, budget, now, market, bid, ask, available_quote,
         amount_step, minimum_amount, minimum_notional, pnl, signal, acquisition, risk, allow_acquisition=True,
         bag=None, capital=None, buy_gap=None, depth_budget=None):
    """``bag`` (a HeldBag) selects live OK RSI capacity: active tranches only, slots and
    capital free as tranches close or are held, and a new buy keeps the live gap from the
    bag's breakeven. Without it the frozen B0 campaign semantics apply. ``capital`` (a
    LiveCapitalConfig, bag mode only) replaces the fixed allocation with the live position
    cap marked at mid, the live active-buy limit and per-buy ``slice_quote``."""
    if capital is not None and bag is None:
        raise PolicyError('live capital requires bag mode')
    if buy_gap is not None and capital is None:
        raise PolicyError('the live buy gap requires live capital')
    if depth_budget is not None and capital is None:
        raise PolicyError('the depth budget requires live capital')
    budget = decimal(budget, 'budget', positive=True)
    bid, ask = decimal(bid, 'bid', positive=True), decimal(ask, 'ask', positive=True)
    now = decimal(now, 'now')
    available_quote = decimal(available_quote, 'available quote')
    if ask < bid or market.candle_open > now:
        raise PolicyError('invalid current market snapshot')
    if state is None and inventory:
        raise PolicyError('native history without decision checkpoint')
    fresh_owner = state is None
    state = state or DecisionState(fingerprint=fingerprint, candle=market.candle_open)
    if state.fingerprint != fingerprint or market.candle_open < state.candle:
        raise PolicyError('configuration or candle identity regression')
    # In bag mode a terminal held tranche's inventory belongs to the controller's held
    # position (the bag), which is read and sold separately; its receipt keeps the base.
    if any((not value.terminal or value.remaining_base > 0) and key not in state.claims
           and not (bag is not None and value.terminal and value.held)
           for key, value in inventory.items()):
        raise PolicyError('unclaimed native execution')
    bag_empty = None if bag is None else held_is_dust(
        bag.amount, bid, min_amount=max(decimal(amount_step, 'amount step'), decimal(minimum_amount, 'minimum amount')),
        min_notional=decimal(minimum_notional, 'minimum notional'))
    assessment = risk_update(risk, budget=budget, pnl=pnl, day=int(now // 86400),
                             peak=state.risk_peak, previous_day=state.risk_day,
                             day_start=state.risk_day_start, paused=state.risk_paused)
    updated = state.model_copy(update={'risk_peak': assessment.peak, 'risk_day': assessment.day,
                                       'risk_day_start': assessment.day_start, 'risk_paused': assessment.paused})
    revised, reconciliation = reconcile_claims(updated, inventory, bag_empty=bag_empty)
    if reconciliation not in {'reconciled', 'campaign_flat'}:
        return updated, None, reconciliation
    if bag is None and revised.committed_quote > budget:
        raise PolicyError('committed quote exceeds allocation')
    if reconciliation == 'campaign_flat':
        return revised, None, 'campaign_flat'
    if assessment.paused:
        return revised, None, assessment.reason
    if type(allow_acquisition) is not bool:
        raise PolicyError('invalid acquisition permission')
    if not allow_acquisition:
        return revised, None, 'acquisition_disabled'
    if fresh_owner:
        return revised, None, 'seeded'
    if market.candle_open == state.candle:
        return revised, None, 'same_candle'
    revised = revised.model_copy(update={'candle': market.candle_open})
    if now - revised.last_entry_time < acquisition.cooldown_seconds:
        return revised, None, 'cooldown'
    pause = getattr(market, 'flash_pause_until', None)
    if pause is not None and now < decimal(pause, 'flash pause'):  # research order: cooldown, flash pause, gaps
        return revised, None, 'flash_crash_pause'
    if (ask - bid) / bid > risk.max_spread_fraction:
        return revised, None, 'spread_limit'
    signal_result = signal_decision(signal, previous_rsi=market.previous_rsi, rsi=market.rsi,
                                    breakout=market.breakout, trend_up=market.trend_up,
                                    turn=getattr(market, 'turn', False), score=getattr(market, 'score', 0),
                                    atr_fraction=getattr(market, 'atr_fraction', None))
    if bag is not None:
        return _bag_entry(revised, inventory, bag, bag_empty, fingerprint=fingerprint, budget=budget, now=now, market=market,
                          bid=bid, ask=ask, available_quote=available_quote, amount_step=amount_step,
                          minimum_amount=minimum_amount, minimum_notional=minimum_notional,
                          fresh_signal=signal_result.enter, acquisition=acquisition, risk=risk, capital=capital,
                          buy_gap=buy_gap, depth_budget=depth_budget)
    owned = [inventory[x] for x in revised.claims]
    acquired = [value for value in owned if value.entry_quote > 0]
    fraction = next_stage(acquisition, stage=len(owned), bid=bid,
                          last_fill_price=acquired[-1].entry_price if acquired else None,
                          fresh_signal=signal_result.enter,
                          entry_terminal=all(x.entry_terminal for x in owned),
                          distribution_started=any(x.sold_base > 0 for x in owned))
    if fraction is None:
        return revised, None, 'acquisition_not_eligible'
    allocation = allocate(stage_quote=budget * fraction, remaining_quote=budget - revised.committed_quote,
                          available_quote=available_quote, price=bid, amount_step=amount_step,
                          minimum_amount=minimum_amount, minimum_notional=minimum_notional,
                          fee_buffer=risk.fee_reserve_fraction, cash_reserve=risk.cash_reserve_quote)
    if allocation is None:
        return revised, None, 'allocation_below_minimum'
    identifier = identity([fingerprint, market.candle_open, revised.cycle, len(owned)])
    proposal = EntryProposal(identifier, bid, allocation.amount, float(now), revised.cycle, len(owned))
    revised = revised.model_copy(update={'claims': (*revised.claims, identifier),
                                         'committed_quote': revised.committed_quote + allocation.reserved_quote,
                                         'last_entry_time': now})
    return revised, proposal, 'entry_proposed'


def _bag_entry(state, inventory, bag, bag_empty, *, fingerprint, budget, now, market, bid, ask, available_quote,
               amount_step,
               minimum_amount, minimum_notional, fresh_signal, acquisition, risk, capital=None, buy_gap=None,
               depth_budget=None):
    """Live OK RSI admission: capacity counts active tranches; capital is what is tied up now."""
    active = [inventory[x] for x in state.claims if not inventory[x].terminal]
    acquired = [value for value in active if value.entry_quote > 0]
    if capital is not None:
        return _live_capital_entry(state, active, acquired, bag, bag_empty, capital, fingerprint=fingerprint,
                                   now=now, market=market, bid=bid, ask=ask, available_quote=available_quote,
                                   amount_step=amount_step, minimum_amount=minimum_amount,
                                   minimum_notional=minimum_notional, fresh_signal=fresh_signal,
                                   acquisition=acquisition, risk=risk, buy_gap=buy_gap, depth_budget=depth_budget)
    fraction = next_stage(acquisition, stage=len(active), bid=bid,
                          last_fill_price=acquired[-1].entry_price if acquired else None,
                          fresh_signal=fresh_signal, entry_terminal=all(x.entry_terminal for x in active),
                          distribution_started=False)
    if fraction is None:
        return state, None, 'acquisition_not_eligible'
    if not bag_empty and abs(bid - bag.breakeven_price) / bag.breakeven_price < acquisition.minimum_drop:
        return state, None, 'bag_gap_not_met'
    fee = 1 + risk.fee_reserve_fraction
    tied_up = sum((max(value.planned_quote, value.entry_quote) * fee for value in active), Decimal('0'))
    tied_up += bag.amount * bag.breakeven_price + bag.remaining_fees_quote
    if tied_up >= budget:
        return state, None, 'bag_capital_exhausted'
    allocation = allocate(stage_quote=budget * fraction, remaining_quote=budget - tied_up,
                          available_quote=available_quote, price=bid, amount_step=amount_step,
                          minimum_amount=minimum_amount, minimum_notional=minimum_notional,
                          fee_buffer=risk.fee_reserve_fraction, cash_reserve=risk.cash_reserve_quote)
    if allocation is None:
        return state, None, 'allocation_below_minimum'
    # Claims only grow inside a cycle, so identity uses the claim count, not the stage.
    identifier = identity([fingerprint, market.candle_open, state.cycle, len(state.claims)])
    proposal = EntryProposal(identifier, bid, allocation.amount, float(now), state.cycle, len(active))
    revised = state.model_copy(update={'claims': (*state.claims, identifier),
                                       'committed_quote': state.committed_quote + allocation.reserved_quote,
                                       'last_entry_time': now})
    return revised, proposal, 'entry_proposed'


def _live_capital_entry(state, active, acquired, bag, bag_empty, capital, *, fingerprint, now, market, bid, ask,
                        available_quote, amount_step, minimum_amount, minimum_notional, fresh_signal,
                        acquisition, risk, buy_gap=None, depth_budget=None):
    """Live OK RSI admission under the live capital model (see bag.LiveCapitalConfig), then the
    research depth budget (bag.DepthBudgetConfig) when configured."""
    from .bag import (
        depth_budget_allowed,
        depth_budget_decision,
        depth_reference,
        depth_slice_quote,
        live_buy_gap,
        live_capital_quote,
    )

    # Same entry evidence as next_stage, with the live active-buy limit instead of a stage count.
    if not fresh_signal or not all(x.entry_terminal for x in active):
        return state, None, 'acquisition_not_eligible'
    if buy_gap is None:
        if acquired and bid > acquired[-1].entry_price * (1 - acquisition.minimum_drop):
            return state, None, 'acquisition_not_eligible'
        gap = acquisition.minimum_drop
    else:  # live ATR-scaled absolute gap from the last active buy and from the held cost basis
        features = getattr(market, 'bag', None)
        gap = live_buy_gap(buy_gap, None if features is None else features.atr_pct)
        if acquired and abs(bid - acquired[-1].entry_price) / acquired[-1].entry_price < gap:
            return state, None, 'acquisition_not_eligible'
    if not bag_empty and abs(bid - bag.breakeven_price) / bag.breakeven_price < gap:
        return state, None, 'bag_gap_not_met'
    mid = (bid + ask) / 2
    rows = [(value.remaining_base, max(value.planned_quote - value.entry_quote, Decimal('0')) / value.entry_price
             if not value.entry_terminal else Decimal('0'), value.entry_price) for value in active]
    # Live V2 funding counts the bag and active buys at cost with fees, pending entries included.
    # Net realized losses also count live; native has no fill ledger here, and the no-loss exit
    # floor keeps realized PnL nonnegative, so inventory cost is the binding term.
    funded_used = None if capital.funded_cap_quote is None else (
        bag.amount * bag.breakeven_price + bag.remaining_fees_quote
        + sum((max(value.planned_quote, value.entry_quote) for value in active), Decimal('0'))
        * (1 + risk.fee_reserve_fraction))
    reference = None
    if depth_budget is not None:
        features = getattr(market, 'bag', None)
        if features is None or features.recent_high is None:
            return state, None, 'depth_history_unavailable'  # entries only; exits are planned elsewhere
        reference = depth_reference(features.recent_high, None if bag_empty else bag.breakeven_price)
        if depth_budget.max_slice_multiplier != 1:  # depth-scaled buy size enters the live capital gates
            depth, _ = depth_budget_allowed(depth_budget, reference_high=reference, price=bid)
            capital = capital.model_copy(update={'slice_quote': depth_slice_quote(depth_budget, capital.slice_quote, depth)})
    quote, reason = live_capital_quote(capital, held_amount=bag.amount, held_dust=bag_empty, mid=mid, active=rows,
                                       available_quote=available_quote, funded_used_quote=funded_used)
    if quote is None:
        return state, None, reason
    if depth_budget is not None:
        deployed = (bag.amount * bag.breakeven_price + bag.remaining_fees_quote
                    + sum((max(value.planned_quote, value.entry_quote) for value in active), Decimal('0')))
        quote, reason = depth_budget_decision(depth_budget, quote, deployed_quote=deployed,
                                              reference_high=reference, price=bid)
        if quote is None:
            return state, None, reason
    allocation = allocate(stage_quote=quote, remaining_quote=quote, available_quote=available_quote, price=bid,
                          amount_step=amount_step, minimum_amount=minimum_amount,
                          minimum_notional=minimum_notional, fee_buffer=risk.fee_reserve_fraction,
                          cash_reserve=risk.cash_reserve_quote)
    if allocation is None:
        return state, None, 'allocation_below_minimum'
    identifier = identity([fingerprint, market.candle_open, state.cycle, len(state.claims)])
    proposal = EntryProposal(identifier, bid, allocation.amount, float(now), state.cycle, len(active))
    revised = state.model_copy(update={'claims': (*state.claims, identifier),
                                       'committed_quote': state.committed_quote + allocation.reserved_quote,
                                       'last_entry_time': now})
    return revised, proposal, 'entry_proposed'
