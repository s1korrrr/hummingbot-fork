"""Compose policies into bounded proposals; caller must persist before dispatch."""
from dataclasses import dataclass
from decimal import Decimal

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
         bag=None):
    """``bag`` (a HeldBag) selects live OK RSI capacity: active tranches only, slots and
    capital free as tranches close or are held, and a new buy keeps the live gap from the
    bag's breakeven. Without it the frozen B0 campaign semantics apply."""
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
    if any((not value.terminal or value.remaining_base > 0) and key not in state.claims
           for key, value in inventory.items()):
        raise PolicyError('unclaimed native execution')
    assessment = risk_update(risk, budget=budget, pnl=pnl, day=int(now // 86400),
                             peak=state.risk_peak, previous_day=state.risk_day,
                             day_start=state.risk_day_start, paused=state.risk_paused)
    updated = state.model_copy(update={'risk_peak': assessment.peak, 'risk_day': assessment.day,
                                       'risk_day_start': assessment.day_start, 'risk_paused': assessment.paused})
    revised, reconciliation = reconcile_claims(updated, inventory,
                                               bag_amount=None if bag is None else bag.amount)
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
    if (ask - bid) / bid > risk.max_spread_fraction:
        return revised, None, 'spread_limit'
    signal_result = signal_decision(signal, previous_rsi=market.previous_rsi, rsi=market.rsi,
                                    breakout=market.breakout, trend_up=market.trend_up,
                                    turn=getattr(market, 'turn', False), score=getattr(market, 'score', 0),
                                    atr_fraction=getattr(market, 'atr_fraction', None))
    if bag is not None:
        return _bag_entry(revised, inventory, bag, fingerprint=fingerprint, budget=budget, now=now, market=market,
                          bid=bid, available_quote=available_quote, amount_step=amount_step,
                          minimum_amount=minimum_amount, minimum_notional=minimum_notional,
                          fresh_signal=signal_result.enter, acquisition=acquisition, risk=risk)
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


def _bag_entry(state, inventory, bag, *, fingerprint, budget, now, market, bid, available_quote, amount_step,
               minimum_amount, minimum_notional, fresh_signal, acquisition, risk):
    """Live OK RSI admission: capacity counts active tranches; capital is what is tied up now."""
    active = [inventory[x] for x in state.claims if not inventory[x].terminal]
    acquired = [value for value in active if value.entry_quote > 0]
    fraction = next_stage(acquisition, stage=len(active), bid=bid,
                          last_fill_price=acquired[-1].entry_price if acquired else None,
                          fresh_signal=fresh_signal, entry_terminal=all(x.entry_terminal for x in active),
                          distribution_started=False)
    if fraction is None:
        return state, None, 'acquisition_not_eligible'
    if bag.amount > 0 and abs(bid - bag.breakeven_price) / bag.breakeven_price < acquisition.minimum_drop:
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
