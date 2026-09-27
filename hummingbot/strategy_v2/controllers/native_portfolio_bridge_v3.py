"""Native portfolio admission in the owner's recorder. Not an order, fill or balance ledger.

One Metadata row holds every live grant for one portfolio owner on one recorder,
connector and quote. Admission, binding and release are compare-and-swap writes,
so concurrent evaluations cannot admit the same remaining capacity twice.

Release follows the native program-budget rules: expiry, process absence,
cancellation requests and acknowledgements never release a bound grant. Only a
terminal flat executor receipt, or an explicit held-inventory release proven by
the owner, does. This scope is one recorder; it does not coordinate other
processes sharing the same exchange account.
"""
import json
from decimal import Decimal

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from hummingbot.strategy_v2.components.domain import PolicyError, decimal, identity
from hummingbot.strategy_v2.components.program_budget import BudgetStore
from hummingbot.strategy_v2.portfolio_v3.contracts import TargetPortfolio
from hummingbot.strategy_v2.portfolio_v3.runtime_contracts_v1 import (
    SETUP_ALREADY_CONSUMED,
    AdmissionOutcome,
    GrantPolicy,
    PortfolioGrant,
)

ZERO = Decimal('0')
MAX_GRANTS = 1024
MAX_RELEASED = 65536
MAX_CONSUMED = 4096
CAS_ATTEMPTS = 32


def is_bag_sell(info):
    """A member's own bag recovery sell only reduces held inventory; it never spends quote."""
    return BudgetStore._is_member_bag_sell(info)


def is_terminal_flat(info, released=frozenset()):
    if info.id in released:
        return True
    if info.is_done is not True or info.is_active is not False:
        return False
    if is_bag_sell(info):
        return True
    try:
        receipt = (info.custom_info or {}).get('execution_receipt')
        if not isinstance(receipt, dict) or receipt.get('accounting_owner') != 'executor':
            return False
        return decimal(receipt.get('remaining_owned_base'), 'remaining native inventory') == 0
    except (PolicyError, AttributeError, TypeError):
        return False


def evidence_by_id(native_executors):
    evidence = {}
    for info in native_executors:
        if not isinstance(info.id, str) or not info.id or info.id in evidence:
            raise PolicyError('duplicate or missing native executor receipt')
        evidence[info.id] = info
    return evidence


class PortfolioGrantStore:
    def __init__(self, recorder, policy: GrantPolicy, *, metadata_model=None):
        if metadata_model is None:
            from hummingbot.model.metadata import Metadata
            metadata_model = Metadata
        self.model, self.recorder = metadata_model, recorder
        self.policy = GrantPolicy.model_validate(policy.model_dump(mode='json'))
        self.key = 'portfolio-grants:v1:' + identity([recorder.config_file_path, self.policy.connector,
                                                      self.policy.quote, self.policy.portfolio_id])
        self.policy_hash = self.policy.identity

    # ------------------------------------------------------------------ persistence
    def _parse(self, raw):
        if raw is None:
            return {}, frozenset(), ()
        try:
            value = json.loads(raw)
            if (not isinstance(value, dict)
                    or set(value) != {'schema_version', 'policy_hash', 'grants', 'released', 'consumed'}
                    or value['schema_version'] != 1):
                raise PolicyError('portfolio grant schema changed')
            if value['policy_hash'] != self.policy_hash:
                raise PolicyError('portfolio grant policy changed; explicit reconciliation required')
            grants = [PortfolioGrant.model_validate(item) for item in value['grants']]
            released, consumed = value['released'], value['consumed']
            if (len(grants) > MAX_GRANTS or len({g.grant_id for g in grants}) != len(grants)
                    or not isinstance(released, list) or len(released) > MAX_RELEASED
                    or released != sorted(set(released)) or any(not isinstance(x, str) or not x for x in released)
                    or not isinstance(consumed, list) or len(consumed) > MAX_CONSUMED
                    or len(set(consumed)) != len(consumed) or any(not isinstance(x, str) for x in consumed)):
                raise PolicyError('portfolio grant rows invalid')
            bound = [g.executor_id for g in grants if g.status == 'BOUND']
            if len(set(bound)) != len(bound) or set(bound) & set(released):
                raise PolicyError('native executor bound twice or released while bound')
            if any(g.symbol not in self.policy.pairs for g in grants):
                raise PolicyError('grant outside the portfolio instruments')
            self._check_limits(grants)
            return {g.grant_id: g for g in grants}, frozenset(released), tuple(consumed)
        except PolicyError:
            raise
        except (ValueError, TypeError, KeyError) as exc:
            raise PolicyError('portfolio grant checkpoint invalid') from exc

    def _check_limits(self, grants):
        if sum((g.reserved_quote for g in grants), ZERO) > self.policy.envelope_quote:
            raise PolicyError('persisted portfolio grants exceed the envelope')
        for pair in self.policy.pairs:
            if sum((g.reserved_quote for g in grants if g.symbol == pair), ZERO) > self.policy.per_asset_quote:
                raise PolicyError('persisted portfolio grants exceed an asset ceiling')

    def _encode(self, grants, released, consumed):
        return json.dumps({'schema_version': 1, 'policy_hash': self.policy_hash,
                           'grants': [g.model_dump(mode='json') for _, g in sorted(grants.items())],
                           'released': sorted(released), 'consumed': list(consumed)},
                          sort_keys=True, separators=(',', ':'), allow_nan=False)

    def _load(self):
        with self.recorder.sql_manager.get_new_session() as session:
            row = session.get(self.model, self.key)
            return None if row is None else row.value

    def _write(self, old, new):
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                with session.begin():
                    if old is None:
                        session.add(self.model(key=self.key, value=new))
                        session.flush()
                        return True
                    changed = session.execute(update(self.model).where(
                        self.model.key == self.key, self.model.value == old).values(value=new))
                    return changed.rowcount == 1
        except IntegrityError:
            return False

    def _transact(self, mutate):
        """Read, reconcile and conditionally replace the one grant row."""
        try:
            for _ in range(CAS_ATTEMPTS):
                raw = self._load()
                grants, released, consumed = self._parse(raw)
                result, grants, released, consumed = mutate(dict(grants), released, consumed)
                self._check_limits(list(grants.values()))
                encoded = self._encode(grants, released, consumed)
                if raw is not None and encoded == raw:
                    return result
                if raw is None and not grants and not released and not consumed:
                    return result
                if self._write(raw, encoded):
                    return result
            raise PolicyError('portfolio grant CAS contention; no admission')
        except (SQLAlchemyError, AttributeError) as exc:
            raise PolicyError('portfolio grant store unavailable') from exc

    # ------------------------------------------------------------------ native evidence
    def _reconcile(self, grants, released, evidence, now_ms):
        """Drop what native evidence proves finished; reject what it cannot explain."""
        owner = self.policy.controller_id
        bound = {g.executor_id: g for g in grants.values() if g.status == 'BOUND'}
        for key, info in evidence.items():
            config = info.config
            if getattr(config, 'connector_name', None) != self.policy.connector:
                continue
            pair = getattr(config, 'trading_pair', '')
            if not isinstance(pair, str) or pair.split('-')[-1] != self.policy.quote:
                continue
            if info.controller_id != owner:
                if not is_terminal_flat(info):
                    raise PolicyError('foreign spender on this connector quote; capital domain unproven')
                continue
            if getattr(config, 'controller_id', owner) != owner or pair not in self.policy.pairs:
                raise PolicyError('portfolio executor ownership differs')
            if key in bound or is_terminal_flat(info, released):
                continue
            raise PolicyError('unreserved or reappearing native inventory requires reconciliation')
        kept = {}
        for grant_id, grant in grants.items():
            if grant.status == 'ADMITTED':
                if now_ms < grant.submit_deadline_ms:
                    kept[grant_id] = grant  # Unbound and unsent: lapses only at its deadline.
                continue
            info = evidence.get(grant.executor_id)
            if info is not None and (info.controller_id != owner or info.config.trading_pair != grant.symbol):
                raise PolicyError('bound grant executor ownership differs')
            if info is None or not is_terminal_flat(info, released):
                kept[grant_id] = grant
        return kept

    # ------------------------------------------------------------------ operations
    def grants(self, *, now_ms, native_executors):
        """Live grants after native reconciliation. Read-only: never writes."""
        grants, released, _ = self._parse(self._load())
        return tuple(sorted(self._reconcile(grants, released, evidence_by_id(native_executors), now_ms).values(),
                            key=lambda g: g.grant_id))

    def released(self):
        return self._parse(self._load())[1]

    def admit(self, target: TargetPortfolio, *, priority, setup_ids, now_ms, submit_deadline_ms,
              native_executors, execution_capable):
        """Admit one full increment per positive target change, in explicit priority order.

        Each admitted symbol receives a ceiling of exactly one fee-inclusive order
        increment. Durable usage is rechecked here; target headroom is not trusted.
        """
        target = TargetPortfolio.model_validate(target.model_dump(mode='json'))
        if (target.portfolio_id != self.policy.portfolio_id or target.mandate_sha256 != self.policy.mandate_sha256
                or target.policy_sha256 != self.policy.policy_sha256):
            raise PolicyError('target scope differs from the grant policy')
        if tuple(sorted(priority)) != self.policy.pairs or len(set(priority)) != len(priority):
            raise PolicyError('complete explicit grant priority required')
        if type(now_ms) is not int or type(submit_deadline_ms) is not int:
            raise PolicyError('strict integer millisecond clocks required')
        changes = {q.symbol: q.change for q in target.quantities}
        wanted = [pair for pair in priority if changes.get(pair, ZERO) > 0]
        if not execution_capable or target.status != 'PROPOSAL_ONLY' or not wanted:
            reasons = () if wanted or target.status != 'PROPOSAL_ONLY' else ('NO_ACQUISITION_PROPOSED',)
            if wanted and not execution_capable:
                reasons = ('EXECUTION_CAPABILITY_UNBOUND',)
            return AdmissionOutcome(target_sha256=target.identity, admitted=(), denied=tuple(wanted),
                                    reasons=reasons, execution_capable=bool(execution_capable))
        if not target.decision_time_ms <= now_ms < target.expires_at_ms:
            raise PolicyError('target is expired or admission clock is invalid')
        deadline = min(submit_deadline_ms, target.expires_at_ms)
        if deadline <= now_ms:
            raise PolicyError('grant submission deadline must follow admission')
        evidence = evidence_by_id(native_executors)
        missing = [pair for pair in wanted if not isinstance(setup_ids.get(pair), str)]
        if missing:
            raise PolicyError('every admitted increment requires a setup identity')

        def mutate(grants, released, consumed):
            grants = self._reconcile(grants, released, evidence, now_ms)
            admitted, denied, reasons = [], [], set()
            consumed = list(consumed)
            for pair in wanted:
                setup = f'{pair}|{setup_ids[pair]}'
                existing = [g for g in grants.values() if g.symbol == pair and g.setup_id == setup_ids[pair]]
                if existing:
                    if existing[0].status == 'ADMITTED':
                        admitted.append(existing[0].grant_id)  # Exact replay of the same cut.
                    else:
                        denied.append(pair)
                        reasons.add(SETUP_ALREADY_CONSUMED)
                    continue
                if setup in consumed:
                    denied.append(pair)
                    reasons.add(SETUP_ALREADY_CONSUMED)
                    continue
                if any(g.symbol == pair and g.status == 'ADMITTED' for g in grants.values()):
                    denied.append(pair)
                    reasons.add('NATIVE_AUTHORITY_PENDING')
                    continue
                used = sum((g.reserved_quote for g in grants.values()), ZERO)
                pair_used = sum((g.reserved_quote for g in grants.values() if g.symbol == pair), ZERO)
                ceiling = self.policy.order_quote
                if (used + ceiling > self.policy.envelope_quote
                        or pair_used + ceiling > self.policy.per_asset_quote):
                    denied.append(pair)
                    reasons.add('ACQUISITION_FULL_INCREMENT_UNAVAILABLE')
                    continue
                grant = PortfolioGrant(
                    grant_id=identity([self.key, target.identity, pair, setup_ids[pair]]), symbol=pair,
                    setup_id=setup_ids[pair], target_sha256=target.identity, epoch=target.epoch, fence=target.fence,
                    ceiling_quote=ceiling, admitted_at_ms=now_ms, submit_deadline_ms=deadline, status='ADMITTED')
                grants[grant.grant_id] = grant
                consumed.append(setup)
                admitted.append(grant.grant_id)
            consumed = consumed[-MAX_CONSUMED:]
            outcome = AdmissionOutcome(target_sha256=target.identity, admitted=tuple(admitted),
                                       denied=tuple(denied), reasons=tuple(sorted(reasons)), execution_capable=True)
            return outcome, grants, released, tuple(consumed)
        return self._transact(mutate)

    def bind(self, *, executor_id, symbol, reserved_quote, now_ms, native_executors):
        """Attach one admitted, unexpired grant to the executor that will spend it."""
        reserved_quote = decimal(reserved_quote, 'reserved quote', positive=True)
        evidence = evidence_by_id(native_executors)
        if executor_id in evidence:
            return False  # Native ownership already exists; this is not a new admission.

        def mutate(grants, released, consumed):
            grants = self._reconcile(grants, released, evidence, now_ms)
            current = [g for g in grants.values() if g.executor_id == executor_id]
            if current:
                grant = current[0]
                return (grant.symbol == symbol and grant.bound_quote == reserved_quote), grants, released, consumed
            if executor_id in released:
                raise PolicyError('released native executor cannot be rebound')
            candidates = sorted((g for g in grants.values() if g.symbol == symbol and g.status == 'ADMITTED'
                                 and now_ms < g.submit_deadline_ms and reserved_quote <= g.ceiling_quote),
                                key=lambda g: g.grant_id)
            if not candidates:
                return False, grants, released, consumed
            grant = candidates[0]
            grants[grant.grant_id] = grant.model_copy(update={'status': 'BOUND', 'executor_id': executor_id,
                                                              'bound_quote': reserved_quote})
            PortfolioGrant.model_validate(grants[grant.grant_id].model_dump(mode='json'))
            return True, grants, released, consumed
        return self._transact(mutate)

    def contains(self, *, executor_id, symbol, reserved_quote, native_executors, now_ms):
        grants = self.grants(now_ms=now_ms, native_executors=native_executors)
        return any(g.status == 'BOUND' and g.executor_id == executor_id and g.symbol == symbol
                   and g.bound_quote == decimal(reserved_quote, 'reserved quote', positive=True) for g in grants)

    def withdraw(self, *, grant_ids, now_ms, native_executors):
        """Return admitted grants the symbol context did not use. Bound grants never withdraw."""
        ids = frozenset(grant_ids)
        evidence = evidence_by_id(native_executors)

        def mutate(grants, released, consumed):
            grants = self._reconcile(grants, released, evidence, now_ms)
            removed = tuple(sorted(key for key in ids if key in grants and grants[key].status == 'ADMITTED'))
            return removed, {k: v for k, v in grants.items() if k not in removed}, released, consumed
        return self._transact(mutate)

    def release_held(self, *, symbol, executor_ids, native_executors, now_ms):
        """Release terminal held tranches and bag sells the owner proved flat for this symbol."""
        ids = frozenset(executor_ids)
        if not ids or any(not isinstance(x, str) or not x for x in ids):
            raise PolicyError('release requires explicit executor ids')
        evidence = evidence_by_id(native_executors)
        owner = self.policy.controller_id

        def mutate(grants, released, consumed):
            for key in ids - released:
                info = evidence.get(key)
                if (info is None or info.controller_id != owner or info.config.controller_id != owner
                        or info.config.connector_name != self.policy.connector or info.config.trading_pair != symbol
                        or info.is_done is not True or info.is_active is not False):
                    raise PolicyError('release target is not a terminal executor of this symbol')
                receipt = (info.custom_info or {}).get('execution_receipt')
                held = isinstance(receipt, dict) and receipt.get('accounting_owner') == 'held_position'
                if not held and not is_bag_sell(info):
                    raise PolicyError('only held tranches and bag recovery sells can be released')
            merged = released | ids
            if len(merged) > MAX_RELEASED:
                raise PolicyError('portfolio released capacity exceeded')
            grants = {k: v for k, v in grants.items() if v.executor_id not in merged}
            grants = self._reconcile(grants, merged, evidence, now_ms)
            return merged, grants, merged, consumed
        return self._transact(mutate)
