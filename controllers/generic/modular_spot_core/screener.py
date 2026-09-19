"""Bounded screener dispatch. Observation is not capital permission."""
from dataclasses import dataclass
from decimal import Decimal

from .domain import PolicyError, decimal, identity
from .universe import pairs, select_universe


@dataclass(frozen=True)
class Opportunity:
    trading_pair: str
    score: Decimal


@dataclass(frozen=True)
class ScreenResult:
    opportunities: tuple[Opportunity, ...]
    scores: dict
    coverage: Decimal
    candle: int
    evidence_hash: str
    observed: tuple[str, ...]


@dataclass(frozen=True)
class CandidateProposal:
    trading_pair: str
    score: Decimal
    input_identity: str
    config_identity: str


@dataclass(frozen=True)
class NativeMarketReadiness:
    available: tuple[str, ...]
    tradable: tuple[str, ...]
    unready: tuple[str, ...] = ()
    blocked: tuple[str, ...] = ()
    stale: tuple[str, ...] = ()
    context_valid: bool = True


@dataclass(frozen=True)
class CapitalAuthority:
    universe: object
    managed: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()
    previous: tuple[str, ...] = ()
    selection_store: object = None
    budget: object = None
    native_executors: tuple = ()
    fingerprint: str = ''
    account: str = 'unassigned'


@dataclass(frozen=True)
class AdmissionResult:
    proposals: tuple[CandidateProposal, ...]
    observation: tuple[str, ...]
    management: tuple[str, ...]
    entries: tuple[str, ...]
    reason: str


def screen(catalog_snapshot, feature_bundle, policy) -> ScreenResult:
    """Ranked opportunities. No capital permission and no slot reservation."""
    now = float(policy.now)
    scores, coverage = catalog_snapshot.screen(feature_bundle, now=now)
    seconds = feature_bundle.interval_seconds()
    candle = int(now // seconds) * seconds - seconds
    ranked = tuple(Opportunity(trading_pair=pair, score=score)
                   for pair, score in sorted(scores.items(), key=lambda item: (-item[1], item[0])))
    observed = pairs(feature_bundle.data.symbols if feature_bundle.universe.observe_all
                     else feature_bundle.universe.symbols)
    evidence_hash = identity(
        [candle, [(item.trading_pair, str(item.score)) for item in ranked], str(coverage)])
    return ScreenResult(opportunities=ranked, scores=dict(scores), coverage=coverage,
                        candle=candle, evidence_hash=evidence_hash, observed=observed)


def admit_candidates(opportunities, native_market_readiness, capital_authority) -> AdmissionResult:
    """Bounded native proposals with input/config identity. Live slots still use BudgetStore.reserve."""
    policy = capital_authority.universe
    available = pairs(native_market_readiness.available)
    execution = set(policy.symbols) if policy.symbols else set(pairs(native_market_readiness.tradable))
    tradable = tuple(pair for pair in pairs(native_market_readiness.tradable) if pair in execution)
    stale = set(pairs(native_market_readiness.stale))
    unready = set(pairs(native_market_readiness.unready))
    blocked = set(pairs(native_market_readiness.blocked)) | stale | unready
    managed = pairs(capital_authority.managed)
    pending = pairs(capital_authority.pending)
    held = tuple(sorted(set(managed) | set(pending)))
    watched = tuple(sorted(available))
    if not native_market_readiness.context_valid:
        return AdmissionResult((), watched, held, (), 'stale_eligibility')
    if capital_authority.budget is None or capital_authority.account == 'unassigned':
        return AdmissionResult((), watched, held, (), 'topology_unproven')
    if not set(policy.benchmarks) <= set(available):
        selection = select_universe(
            policy, available=available, tradable=tradable, scores={},
            managed=managed, pending=pending, blocked=tuple(sorted(blocked)), previous=())
        return AdmissionResult((), selection.observation, selection.management, (), 'missing_context')

    clean = {}
    for item in opportunities.opportunities:
        try:
            clean[item.trading_pair] = decimal(item.score, f'{item.trading_pair} candidate score', minimum=None)
        except PolicyError:
            continue

    kwargs = dict(available=available, tradable=tradable, scores=clean,
                  managed=managed, pending=pending, blocked=tuple(sorted(blocked)))
    evidence_hash = identity([
        opportunities.evidence_hash, sorted(tradable), sorted(blocked), sorted(managed),
        sorted(pending), capital_authority.fingerprint])

    store = capital_authority.selection_store
    if store is not None:
        chosen = store.choose(
            candle=opportunities.candle, evidence_hash=evidence_hash,
            select=lambda previous: select_universe(policy, **kwargs, previous=previous).entries)
    else:
        chosen = select_universe(policy, **kwargs, previous=capital_authority.previous).entries
    selection = select_universe(policy, **kwargs, previous=chosen)

    occupied = ()
    if capital_authority.budget is not None:
        occupied = capital_authority.budget.occupied_pairs(
            native_executors=capital_authority.native_executors)
    occupied_bases = {pair.split('-')[0] for pair in occupied}
    max_assets = capital_authority.budget.policy.max_assets
    proposals = []
    used = set(occupied_bases)
    for pair in selection.entries:
        if pair in blocked or pair not in tradable or pair not in clean:
            continue
        base = pair.split('-')[0]
        if base in used or len(used) >= max_assets:
            continue
        proposals.append(CandidateProposal(
            trading_pair=pair, score=clean[pair], input_identity=evidence_hash,
            config_identity=capital_authority.fingerprint))
        used.add(base)
    reason = 'admitted' if proposals else 'not_selected_or_incomplete_universe'
    return AdmissionResult(tuple(proposals), selection.observation, selection.management,
                           tuple(item.trading_pair for item in proposals), reason)
