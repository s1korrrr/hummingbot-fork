"""Deterministic P0 ordinal comparator. No order, grant, ledger or fitted utility.

All headroom uses the original reconciled book. Proposed sales never finance
buys in the same target. Native admission must independently reconcile and
reserve any later execution; this module cannot make its caller an owner.
"""
from decimal import ROUND_FLOOR, Decimal

from hummingbot.strategy_v2.components.portfolio import allocate
from hummingbot.strategy_v2.components.recipe_contract import digest

from .capital import admissible_cash, committed_usage
from .contracts import AlphaProposal, P0Policy, PortfolioMandate, PortfolioSnapshot, TargetPortfolio, TargetQuantity

ZERO = Decimal(0)


def propose_target(mandate: PortfolioMandate, snapshot: PortfolioSnapshot,
                   proposals: tuple[AlphaProposal, ...], policy: P0Policy, *, recipe_sha256: str,
                   epoch: str, fence: int) -> TargetPortfolio:
    mandate = PortfolioMandate.model_validate(mandate.model_dump(mode='json'))
    snapshot = PortfolioSnapshot.model_validate(snapshot.model_dump(mode='json'))
    policy = P0Policy.model_validate(policy.model_dump(mode='json'))
    if len(proposals) > 4096:
        raise ValueError('bounded proposal set required')
    proposals = tuple(sorted((AlphaProposal.model_validate(p.model_dump(mode='json')) for p in proposals),
                             key=lambda p: p.identity))
    if (snapshot.portfolio_id != mandate.portfolio_id or snapshot.account_id != mandate.account_id
            or snapshot.mandate_sha256 != mandate.identity):
        raise ValueError('snapshot and mandate identities differ')
    if set(policy.pair_priority) != set(mandate.pairs) or {a.symbol for a in snapshot.assets} != set(mandate.pairs):
        raise ValueError('complete mandate instrument state and explicit priority required')
    seen = set()
    for proposal in proposals:
        opportunity = proposal.opportunity
        scope = opportunity.scope
        key = (proposal.producer, scope.trading_pair)
        if key in seen:
            raise ValueError('multiple revisions or actions from one producer/instrument')
        seen.add(key)
        if (proposal.kind != 'ordinal' or proposal.horizon_ms != policy.horizon_ms
                or proposal.producer not in policy.producers):
            raise ValueError('P0 requires registered ordinal producers at one frozen horizon')
        if (scope.program_id != mandate.portfolio_id or scope.owner_id != mandate.portfolio_id
                or scope.connector != 'okx' or scope.trading_pair not in mandate.pairs
                or scope.recipe_sha256 != recipe_sha256):
            raise ValueError('proposal scope differs from the mandate')

    reasons = set()
    changes = {a.symbol: ZERO for a in snapshot.assets}
    requested = ZERO
    now = snapshot.cutoff_ms

    def finish(status):
        quantities = tuple(TargetQuantity(symbol=a.symbol, quantity=a.quantity + changes[a.symbol],
                                          change=changes[a.symbol], reference_weight=(a.quantity + changes[a.symbol]) * a.mark / mandate.reference_quote,
                                          nav_weight=(a.quantity + changes[a.symbol]) * a.mark / snapshot.attributed_nav_quote
                                          if snapshot.attributed_nav_quote else None) for a in snapshot.assets)
        return TargetPortfolio(portfolio_id=mandate.portfolio_id, recipe_sha256=recipe_sha256, mandate_sha256=mandate.identity,
                               policy_sha256=policy.identity, snapshot_sha256=snapshot.identity,
                               forecast_sha256=digest([p.identity for p in proposals]), sequence=snapshot.sequence,
                               epoch=epoch, fence=fence, decision_time_ms=now, expires_at_ms=min([now + mandate.target_lifetime_ms,
                                                                                                  *[p.opportunity.expires_at_ms for p in proposals if p.opportunity.expires_at_ms > now]]),
                               quantities=quantities, requested_acquisition_quote=requested,
                               status=status, reasons=tuple(sorted(reasons)),
                               producer_reasons=tuple(sorted(f'{p.producer}:{p.opportunity.scope.trading_pair}:{p.reason}' for p in proposals)))

    if not snapshot.complete or not snapshot.reconciled or not snapshot.all_writers_accounted:
        reasons.add('NATIVE_RECONCILIATION_REQUIRED')
    if not 0 <= now - snapshot.native_available_at_ms <= mandate.max_age_ms:
        reasons.add('NATIVE_SNAPSHOT_STALE_OR_FUTURE')
    if any(not a.mark_event_ms <= a.available_at_ms <= now or now - a.mark_event_ms > mandate.max_age_ms
           for a in snapshot.assets):
        reasons.add('MARK_STALE_OR_FUTURE')
    if reasons:
        return finish('ENTRY_HOLD')

    assets = {a.symbol: a for a in snapshot.assets}
    pending = {a.symbol: tuple(c for c in snapshot.commitments if c.symbol == a.symbol) for a in snapshot.assets}
    pair_usage = {a.symbol: max(a.basis_quote, a.net_cash_consumed_quote, ZERO)
                  + sum((c.quote_authority for c in pending[a.symbol]), ZERO) for a in snapshot.assets}
    marked = sum((a.quantity * a.mark for a in snapshot.assets), ZERO)
    marked += sum((c.remaining_quantity * assets[c.symbol].mark for c in snapshot.commitments if c.side == 'buy'), ZERO)
    gross_limit = min(mandate.reference_quote * mandate.gross_reference_ratio,
                      snapshot.attributed_nav_quote * mandate.gross_nav_ratio)
    usage = committed_usage(snapshot)
    if marked > gross_limit or usage > mandate.funding_envelope_quote or any(v > mandate.per_asset_quote for v in pair_usage.values()):
        reasons.add('CURRENT_BOOK_EXCEEDS_MANDATE')
        return finish('CURRENT_BOOK_INFEASIBLE')
    cash = max(ZERO, admissible_cash(snapshot) - mandate.cash_reserve_quote)
    funding = max(ZERO, mandate.funding_envelope_quote - usage)
    gross = max(ZERO, gross_limit - marked)

    for symbol in policy.pair_priority:
        active = []
        for p in proposals:
            if p.opportunity.scope.trading_pair != symbol:
                continue
            o = p.opportunity
            if o.created_at_ms > now or any(r.available_time_ms > now for r in (o.primary, *o.dependencies)):
                reasons.add('FORECAST_NOT_AVAILABLE')
            elif now >= o.expires_at_ms:
                reasons.add('FORECAST_EXPIRED')
            elif p.ordinal > 0:
                active.append(o)
        actions = {o.action for o in active}
        if len(actions) > 1:
            reasons.add('FORECAST_CONFLICT')
            continue
        if not actions:
            continue
        a = assets[symbol]
        if pending[symbol]:
            reasons.add('NATIVE_AUTHORITY_PENDING')
            continue
        if 'acquire' in actions:
            if (snapshot.risk_paused or symbol not in snapshot.entry_universe or not a.acquisition_allowed
                    or any(not o.scope.acquisition_allowed for o in active)):
                reasons.add('ACQUISITION_HELD')
                continue
            # Fee-inclusive quote at ask, marked cap at mark. Both bind before rounding.
            gross_budget = gross / a.mark * a.ask * (1 + mandate.fee_reserve_fraction)
            if min(cash, funding, mandate.per_asset_quote - pair_usage[symbol], gross_budget) < mandate.order_quote:
                reasons.add('ACQUISITION_FULL_INCREMENT_UNAVAILABLE')
                continue
            allocation = allocate(stage_quote=mandate.order_quote,
                                  remaining_quote=min(funding, mandate.per_asset_quote - pair_usage[symbol], gross_budget),
                                  available_quote=cash, price=a.ask, amount_step=a.lot, minimum_amount=a.minimum_quantity,
                                  minimum_notional=a.minimum_notional, fee_buffer=mandate.fee_reserve_fraction, cash_reserve=0)
            if allocation is None:
                reasons.add('ACQUISITION_CAPACITY_OR_MINIMUM')
                continue
            changes[symbol] = allocation.amount
            requested += allocation.reserved_quote
            cash -= allocation.reserved_quote
            funding -= allocation.reserved_quote
            gross -= allocation.amount * a.mark
        else:
            if not a.reduction_allowed:
                reasons.add('REDUCTION_NOT_ADMITTED')
                continue
            if a.bid < a.minimum_sell_price:
                reasons.add('TARGET_BLOCKED_PROFIT_FLOOR')
                continue
            quantity = (min(a.sellable_quantity, mandate.order_quote / a.bid) / a.lot).to_integral_value(rounding=ROUND_FLOOR) * a.lot
            if quantity < a.minimum_quantity or quantity * a.bid < a.minimum_notional:
                reasons.add('REDUCTION_BELOW_MINIMUM')
                continue
            changes[symbol] = -quantity
    if not any(changes.values()):
        reasons.add('RETAIN_NATIVE_BOOK')
    return finish('ENTRY_HOLD' if snapshot.risk_paused else 'PROPOSAL_ONLY')
