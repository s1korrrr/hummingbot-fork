"""Read-only cash/reference arithmetic; native fills and grants remain authoritative."""
from decimal import Decimal

from pydantic import Field

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import HASH

from .contracts import PortfolioMandate, PortfolioSnapshot, TargetQuantity


def weights(mandate: PortfolioMandate, snapshot: PortfolioSnapshot):
    return tuple(TargetQuantity(symbol=a.symbol, quantity=a.quantity, change=0,
                                reference_weight=a.quantity * a.mark / mandate.reference_quote,
                                nav_weight=a.quantity * a.mark / snapshot.attributed_nav_quote if snapshot.attributed_nav_quote > 0 else None)
                 for a in snapshot.assets)


class _Approval(PolicyModel):
    approval_sha256: str = Field(pattern=HASH)


def rebase_weights(old, snapshot, new, *, approval_sha256):
    """An approval reference is input evidence, not a generated signature or trade."""
    _Approval(approval_sha256=approval_sha256)
    old = PortfolioMandate.model_validate(old.model_dump(mode='json'))
    new = PortfolioMandate.model_validate(new.model_dump(mode='json'))
    snapshot = PortfolioSnapshot.model_validate(snapshot.model_dump(mode='json'))
    if old.version == new.version or old.identity != snapshot.mandate_sha256:
        raise ValueError('rebase requires a new version and reconciled old mandate identity')
    before = old.model_dump(exclude={'version', 'reference_quote'})
    after = new.model_dump(exclude={'version', 'reference_quote'})
    if (before != after or not snapshot.reconciled or not snapshot.complete or not snapshot.all_writers_accounted
            or snapshot.account_id != old.account_id or snapshot.portfolio_id != old.portfolio_id
            or {a.symbol for a in snapshot.assets} != set(old.pairs)
            or not 0 <= snapshot.cutoff_ms - snapshot.native_available_at_ms <= old.max_age_ms
            or any(not a.available_at_ms <= snapshot.cutoff_ms or snapshot.cutoff_ms - a.mark_event_ms > old.max_age_ms
                   for a in snapshot.assets)):
        raise ValueError('reference-only rebase cannot alter capital, policy, scope or owned state')
    return weights(new, snapshot)


def admissible_cash(snapshot):
    residual = sum((c.quote_authority - c.venue_encumbered_quote for c in snapshot.commitments), Decimal(0))
    return max(Decimal(0), snapshot.cash_available_quote - residual)


def committed_usage(snapshot):
    basis = sum((a.basis_quote for a in snapshot.assets), Decimal(0))
    consumed = sum((a.net_cash_consumed_quote for a in snapshot.assets), Decimal(0))
    hard = sum((c.quote_authority for c in snapshot.commitments), Decimal(0))
    return max(consumed, basis, Decimal(0)) + hard
