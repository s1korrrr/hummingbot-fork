"""Candidate admission only. Native fills, executors and PositionHold own accounting.

One recorder/strategy envelope. Allocation and per-position quote are yaml-set.
The default remains one controller/pair; strategy scope binds every native peer.
This does not reserve funds from independent processes sharing the wallet.
"""
import json
import re
from decimal import Decimal
from typing import Literal

from pydantic import Field
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator, PositionHold

from .config import PolicyModel
from .domain import PolicyError, decimal, identity

ZERO = Decimal('0')
LEGACY_NEVER_STARTED = {'schema': 1, 'source': 'never_started_intent_v1'}


class CandidateFundingPolicy(PolicyModel):
    allocation_quote: Decimal = Field(gt=0, le=2500)
    fee_reserve_fraction: Decimal = Field(gt=0, le=Decimal('.1'))
    max_position_quote: Decimal = Field(default=50, gt=0, le=505)
    scope: Literal['controller', 'strategy'] = 'controller'
    resize_to_available_quote: bool = False


def bind_funding_scope(controllers):
    """Bind one runner's genuine owners; never coordinate independent recorders."""
    funded = [c for c in controllers.values() if getattr(c.config, 'funding', None) is not None]
    if not any(c.config.funding.scope == 'strategy' for c in funded):
        return
    from hummingbot.strategy_v2.controllers.native_policy_identity import is_modular_policy
    policy = funded[0].config.funding
    if (len(funded) != len(controllers) or not 1 <= len(funded) <= 1000
            or any(not is_modular_policy(c.config) or c.config.funding != policy
                   or c.config.funding.scope != 'strategy' for c in funded)):
        raise PolicyError('Strategy funding requires identical policies on every genuine native owner')
    if (len({c.config.id for c in funded}) != len(funded)
            or len({c.config.trading_pair for c in funded}) != len(funded)
            or len({(c.config.connector_name, c.config.execution_mode) for c in funded}) != 1
            or any(key != owner.config.id for key, owner in controllers.items())):
        raise PolicyError('Strategy funding requires unique pairs and owners on one native connector')
    peers = tuple(controllers[key] for key in sorted(controllers))
    for owner in peers:
        owner._funding_peers = peers
    return peers


def funding_projection(fills, connector, pair):
    """Transient projection through the native exact-fill and held-position logic."""
    base, quote = pair.split('-')
    position = PositionHold(connector, pair, TradeType.BUY)
    cash = ZERO
    fingerprints, by_order = {}, {}
    for fill in sorted(fills, key=lambda item: (item.timestamp, item.order_id, str(item.exchange_trade_id))):
        if (fill.market != connector or fill.symbol != pair or fill.base_asset != base or fill.quote_asset != quote
                or fill.leverage != 1 or fill.position not in (None, '', 'NIL')
                or fill.trade_type not in ('BUY', 'SELL')):
            raise ValueError('Foreign or non-spot funding fill')
        record = ExecutorOrchestrator._trade_fill_order_record(fill)
        decimal(fill.trade_fee_in_quote_decimal, 'native valued fee')
        quote_fee = ZERO
        for fee in fill.trade_fee['flat_fees']:
            amount = decimal(fee['amount'], 'actual fee')
            if fee['token'] not in (base, quote):
                raise ValueError('Third-token funding fee is unsupported')
            if fee['token'] == quote:
                quote_fee += amount
        amount, price = fill.amount_decimal, fill.price_decimal
        notional = amount * price
        cash += (notional if fill.trade_type == 'BUY' else -notional) + quote_fee
        key = identity([fill.market, fill.order_id, str(fill.exchange_trade_id)])
        if key in fingerprints:
            raise ValueError('Duplicate native funding fill')
        fingerprints[key] = identity(json.loads(json.dumps([fill.config_file_path, fill.timestamp, record, fill.trade_fee], default=str)))
        if fill.trade_type == 'BUY':
            by_order[fill.order_id] = by_order.get(fill.order_id, ZERO) + amount
        position.add_order_record(record)
        summary = position.get_position_summary(price)
        if summary.side != TradeType.BUY and summary.amount > 0:
            raise ValueError('Funding history sells unowned inventory')
    summary = position.get_position_summary(Decimal('1'))
    # Reuse native remaining fee attribution; do not invent another cost ledger.
    cost = summary.amount * summary.breakeven_price + summary.remaining_fees_quote
    if not cost.is_finite() or cost < 0:
        raise ValueError('Native acquisition cost is unavailable')
    return {'cash_used': cash, 'inventory_cost': cost, 'used': max(ZERO, cash, cost),
            'fills': fingerprints, 'bought_by_order': by_order}


def _config_identity(config, controller_id):
    payload = config.model_dump(mode='json')
    if payload.get('controller_id') not in (None, '', 'main', controller_id):
        raise PolicyError('Funding executor owner differs')
    payload['controller_id'] = controller_id
    return identity(payload)


class NativeFundingStore:
    """A small durable intent envelope, never an order/fill or balance ledger.

    Unused canceled/rejected intent stays reserved until native terminal reconciliation.
    Neither timeouts nor terminal flags release uncertain late-fill capacity.
    """

    def __init__(self, owner, recorder):
        self.owner, self.recorder = owner, recorder
        self.policy = owner.config.funding
        self.key = 'modular-candidate-funding:v1:' + identity([recorder.config_file_path])
        if self.policy.scope == 'strategy':
            peers = getattr(owner, '_funding_peers', ())
            if not peers or not any(peer is owner for peer in peers):
                raise PolicyError('Strategy funding requires runner-bound owners')
            self.owners = {peer.config.id: peer for peer in peers}
            bind_funding_scope(self.owners)
            self.binding = identity(['strategy', self.policy.model_dump(mode='json'),
                                     [[peer.config.id, peer.config.connector_name, peer.config.trading_pair,
                                       peer._composition_digest, peer._native_source_hash] for peer in peers]])
        else:
            self.owners = {owner.config.id: owner}
            self.binding = identity([owner.config.id, owner.config.connector_name, owner.config.trading_pair,
                                     owner._composition_digest, owner._native_source_hash])
        self.status = {}

    def _load(self, *, expected_binding=None):
        binding = self.binding if expected_binding is None else expected_binding
        with self.recorder.sql_manager.get_new_session() as session:
            row = session.get(Metadata, self.key)
            raw = None if row is None else row.value
            rows = session.query(Metadata).filter(Metadata.key.like(self.key + ':%')).all()
        header = {'schema': 1, 'binding': self.binding, 'revision': 0,
                  'audit_sha256': identity([[], []])} if raw is None else json.loads(raw)
        if (not isinstance(header, dict) or set(header) != {'schema', 'binding', 'revision', 'audit_sha256'}
                or type(header['revision']) is not int or header['revision'] < 0):
            raise PolicyError('Funding checkpoint header corrupt (FUNDING_AUDIT_CORRUPT)')
        if type(header['schema']) is not int or header['schema'] != 1:
            raise PolicyError('Funding checkpoint schema unsupported (FUNDING_SCHEMA_UNSUPPORTED)')
        if header['binding'] != binding:
            raise PolicyError('Funding checkpoint identity changed (FUNDING_BINDING_MISMATCH): '
                              f"persisted={header['binding']} current={binding} revision={header['revision']}")
        if raw is None and rows:
            raise PolicyError('Funding checkpoint missing with existing admissions')
        claims, fills, releases = {}, {}, {}
        foreign_claims = set()
        for row in rows:
            item = json.loads(row.value)
            if not isinstance(item, dict):
                raise PolicyError('Funding audit record identity changed')
            kind = row.key[len(self.key) + 1:].split(':', 1)[0]
            if item.get('binding') != binding:
                if kind != 'claim':
                    raise PolicyError('Funding audit record identity changed')
                foreign_claims.add(item.get('executor_id'))
            if kind == 'claim':
                if set(item) != {'binding', 'executor_id', 'claim'}:
                    raise PolicyError('Funding intent audit record corrupt')
                key, claim = item['executor_id'], item['claim']
                if (not isinstance(key, str) or not key or key in claims
                        or row.key != self.key + ':claim:' + identity(key)
                        or not isinstance(claim, dict) or set(claim) != {'config', 'amount', 'price', 'fee_fraction'}
                        or not isinstance(claim['config'], str) or not re.fullmatch('[a-f0-9]{64}', claim['config'])):
                    raise PolicyError('Funding intent checkpoint is corrupt')
                decimal(claim['amount'], 'funded amount', positive=True)
                decimal(claim['price'], 'funded price', positive=True)
                if decimal(claim['fee_fraction'], 'fee reserve') != self.policy.fee_reserve_fraction:
                    raise PolicyError('Funding fee bound changed')
                claims[key] = claim
            elif kind == 'release':
                if set(item) != {'binding', 'executor_id', 'receipt'}:
                    raise PolicyError('Funding release audit record corrupt')
                key = item['executor_id']
                if (not isinstance(key, str) or not key or key in releases
                        or row.key != self.key + ':release:' + identity(key) or not isinstance(item['receipt'], dict)):
                    raise PolicyError('Funding release identity corrupt')
                releases[key] = item['receipt']
            elif kind == 'fill':
                if set(item) != {'binding', 'fill_key', 'digest'}:
                    raise PolicyError('Funding fill audit record corrupt')
                key, digest = item['fill_key'], item['digest']
                if (not all(isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value) for value in (key, digest))
                        or row.key != self.key + ':fill:' + key or key in fills):
                    raise PolicyError('Funding fill fingerprint invalid')
                fills[key] = digest
            else:
                raise PolicyError('Unknown funding audit record')
        if any(releases.get(key) != LEGACY_NEVER_STARTED for key in foreign_claims):
            raise PolicyError('Funding audit record identity changed')
        state = {**header, 'claims': claims, 'fills': fills, 'releases': releases}
        if header['audit_sha256'] != self._audit_hash(state):
            raise PolicyError('Funding admission or fill audit records disappeared or changed')
        if not set(releases).issubset(claims):
            raise PolicyError('Funding release has no admitted intent')
        return raw, state

    @staticmethod
    def _audit_hash(state):
        # Preserve existing v1 checkpoint hashes until the first release.
        parts = [sorted(state['claims'].items()), sorted(state['fills'].items())]
        if state['releases']:
            parts.append(sorted(state['releases'].items()))
        return identity(parts)

    def _write(self, raw, state):
        header = {'schema': 1, 'binding': self.binding, 'revision': state['revision'] + 1,
                  'audit_sha256': self._audit_hash(state)}
        encoded = json.dumps(header, sort_keys=True, separators=(',', ':'), allow_nan=False)
        records = [(self.key + ':claim:' + identity(key),
                    {'binding': self.binding, 'executor_id': key, 'claim': claim})
                   for key, claim in state['claims'].items()]
        records.extend((self.key + ':fill:' + key, {'binding': self.binding, 'fill_key': key, 'digest': digest})
                       for key, digest in state['fills'].items())
        records.extend((self.key + ':release:' + identity(key),
                        {'binding': self.binding, 'executor_id': key, 'receipt': receipt})
                       for key, receipt in state['releases'].items())
        try:
            with self.recorder.sql_manager.get_new_session() as session, session.begin():
                if raw is None:
                    session.add(Metadata(key=self.key, value=encoded))
                    session.flush()
                else:
                    result = session.execute(update(Metadata).where(Metadata.key == self.key, Metadata.value == raw).values(value=encoded))
                    if result.rowcount != 1:
                        return False
                # Immutable admission facts and fill hashes, not another native
                # ledger. No lifetime order/fill count causes an artificial stop.
                for key, value in records:
                    value = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
                    previous = session.get(Metadata, key)
                    if previous is None:
                        session.add(Metadata(key=key, value=value))
                    elif previous.value != value:
                        raise PolicyError('Immutable funding audit record changed')
                return True
        except IntegrityError:
            return False

    def _evidence(self, state, raw, *, reconcile=False):
        config = self.owner.config
        if self.policy.scope == 'strategy':
            bind_funding_scope(self.owners)
            if any(peer.config.funding != self.policy or not peer._composition_unchanged() for peer in self.owners.values()):
                raise PolicyError('Strategy funding owner configuration or implementation changed')
        saved = self.recorder.get_all_executors()
        current = [info for peer in self.owners.values() for info in peer.executors_info]
        if any(info.controller_id not in self.owners for info in [*saved, *current]):
            raise PolicyError('Candidate funding admits only its bound native controllers per recorder')
        infos = {}
        for collection in (saved, current):
            seen = set()
            for info in collection:
                if info.id in seen:
                    raise PolicyError('Duplicate native executor evidence')
                seen.add(info.id)
                previous = infos.get(info.id)
                if previous is not None and _config_identity(previous.config, info.controller_id) != _config_identity(info.config, info.controller_id):
                    raise PolicyError('Saved/current native executor configuration conflict')
                infos[info.id] = info
        fills = self.recorder.get_trades_for_config(self.recorder.config_file_path)
        connector = self.owner.market_data_provider.connectors[config.connector_name]
        orders = self.recorder.get_orders_for_config_and_market(self.recorder.config_file_path, connector)
        if raw is None and (infos or fills or orders or any(peer.positions_held for peer in self.owners.values())):
            raise PolicyError('Native history exists without funding checkpoint; reconcile explicitly')
        owned = {}
        for info in infos.values():
            cfg = info.config
            owner_config = self.owners[info.controller_id].config
            if cfg.connector_name != owner_config.connector_name or cfg.trading_pair != owner_config.trading_pair:
                raise PolicyError('Foreign native executor scope in funding recorder')
            if cfg.side == TradeType.BUY:
                claim = state['claims'].get(info.id)
                if claim is None or claim['config'] != _config_identity(cfg, info.controller_id):
                    raise PolicyError('Native BUY lacks immutable funding admission')
            for _, _, order_id in ExecutorOrchestrator._owned_order_keys(info):
                if order_id in owned and owned[order_id] != info.id:
                    raise PolicyError('Native order has ambiguous funding owner')
                owned[order_id] = info.id
        if any(order.id not in owned for order in orders):
            raise PolicyError('Native order lacks executor ownership')
        if any(key not in infos and state['releases'].get(key) != LEGACY_NEVER_STARTED for key in state['claims']):
            raise PolicyError('Unresolved funded intent requires explicit reconciliation')
        if any(info.is_active and info.id not in {item.id for item in current} for info in saved):
            raise PolicyError('Unresolved native executor after restart')
        if any(fill.order_id not in owned for fill in fills):
            raise PolicyError('Native fill lacks executor ownership')
        connector = self.owner.market_data_provider.connectors[config.connector_name]
        tracker = connector._order_tracker
        recorded = {(fill.order_id, str(fill.exchange_trade_id)) for fill in fills}
        for order_id, order in tracker.all_orders.items():
            if order_id not in owned:
                raise PolicyError('Unowned order in candidate connector')
            if any((order_id, str(trade)) not in recorded for trade in order.order_fills):
                raise PolicyError('Native fill is not durably recorded yet')
        pairs = {peer.config.trading_pair for peer in self.owners.values()}
        if any(fill.symbol not in pairs for fill in fills):
            raise PolicyError('Native fill has an unbound pair')
        by_pair = {pair: funding_projection([fill for fill in fills if fill.symbol == pair], config.connector_name, pair)
                   for pair in sorted(pairs)}
        projections = list(by_pair.values())
        projection = {key: sum((part[key] for part in projections), ZERO)
                      for key in ('cash_used', 'inventory_cost', 'used')}
        projection.update({key: {item: value for part in projections for item, value in part[key].items()}
                           for key in ('fills', 'bought_by_order')})
        projection['used_by_pair'] = {pair: part['used'] for pair, part in by_pair.items()}
        projection['pending_by_pair'] = {pair: ZERO for pair in pairs}
        if any(projection['fills'].get(key) != value for key, value in state['fills'].items()):
            raise PolicyError('Recorded funding fills disappeared or changed')
        for fill in fills:
            if fill.trade_type == 'BUY':
                claim = state['claims'].get(owned[fill.order_id])
                if claim is None or fill.price_decimal > decimal(claim['price'], 'funded ceiling'):
                    raise PolicyError('Native acquisition exceeds funded price')
                if fill.trade_fee_in_quote_decimal > fill.amount_decimal * fill.price_decimal * self.policy.fee_reserve_fraction:
                    raise PolicyError('Actual native fee exceeds funded bound')
        pending = ZERO
        projection['legacy_uncertain_quote'] = ZERO
        for executor_id, claim in state['claims'].items():
            quantity = sum((amount for order, amount in projection['bought_by_order'].items()
                            if owned.get(order) == executor_id), ZERO)
            info = infos.get(executor_id)
            if info is not None:
                receipt = (info.custom_info or {}).get('execution_receipt')
                if not isinstance(receipt, dict) or decimal(receipt.get('entry_gross_base'), 'native entry receipt') != quantity:
                    raise PolicyError('Native executor and funding fill coverage differ')
            authorized = decimal(claim['amount'], 'funded amount', positive=True)
            if quantity > authorized:
                raise PolicyError('Native acquisition exceeds funded quantity')
            released = state['releases'].get(executor_id)
            if info is None and released == LEGACY_NEVER_STARTED and quantity == ZERO:
                # The old overlay inferred this marker from absence alone. Keep
                # the provenance but reserve the full commitment until proven.
                reserved = authorized * decimal(claim['price'], 'funded price') * (1 + self.policy.fee_reserve_fraction)
                pending += reserved
                projection['legacy_uncertain_quote'] += reserved
                # Legacy claims lack pair identity: conservatively charge every
                # pair limit, and the shared allocation exactly once.
                for pair in pairs:
                    projection['pending_by_pair'][pair] += reserved
                continue
            receipt = (info.custom_info or {}).get('entry_reconciliation') if info is not None else None
            if released is not None or (reconcile and receipt is not None):
                self._validate_release(info, receipt, quantity, fills, orders)
                if released is not None and released != receipt:
                    raise PolicyError('Native terminal reconciliation changed after funding release')
                state['releases'][executor_id] = receipt
            else:
                reserved = (authorized - quantity) * decimal(claim['price'], 'funded price') * (1 + self.policy.fee_reserve_fraction)
                pending += reserved
                projection['pending_by_pair'][info.config.trading_pair] += reserved
        return projection, pending, infos

    def _validate_release(self, info, receipt, quantity, fills, orders):
        if info is None or not isinstance(receipt, dict):
            raise PolicyError('Native terminal reconciliation scope or quantity differs')
        if receipt.get('source') == 'native_terminal_entry_set_v1':
            if (set(receipt) != {'schema', 'source', 'orders'} or type(receipt['schema']) is not int
                    or receipt['schema'] != 1 or not isinstance(receipt['orders'], list) or not receipt['orders']):
                raise PolicyError('Native terminal entry set is invalid')
            proofs = receipt['orders']
        else:
            proofs = [receipt]
        if info.config.entry_order_policy == 'single_attempt' and len(proofs) != 1:
            raise PolicyError('Single-attempt reconciliation contains multiple entry orders')
        snapshots = (info.custom_info or {}).get('execution_order_snapshots')
        if not isinstance(snapshots, list):
            raise PolicyError('Native entry order snapshots unavailable')
        entries = {row['client_order_id']: row for row in snapshots if row.get('trade_type') == 'BUY'}
        proof_ids = [proof.get('order_id') for proof in proofs if isinstance(proof, dict)]
        if (len(proof_ids) != len(proofs) or len(set(proof_ids)) != len(proofs) or set(proof_ids) != set(entries)
                or sum((decimal(proof.get('filled_base'), 'terminal entry total') for proof in proofs), ZERO) != quantity):
            raise PolicyError('Native terminal reconciliation trade coverage differs')
        for proof in proofs:
            snapshot = entries[proof['order_id']]
            if (snapshot.get('exchange_order_id') != proof.get('exchange_order_id')
                    or decimal(snapshot.get('amount'), 'native submitted amount', positive=True)
                    != decimal(proof.get('amount'), 'terminal submitted amount', positive=True)):
                raise PolicyError('Native terminal entry snapshot differs')
            self._validate_order_release(info, proof, decimal(proof['filled_base'], 'terminal fill total'), fills, orders)

    def _validate_order_release(self, info, receipt, quantity, fills, orders):
        fields = {'schema', 'source', 'order_id', 'exchange_order_id', 'trading_pair',
                  'side', 'amount', 'filled_base', 'state', 'trade_ids'}
        if (info is None or not isinstance(receipt, dict) or set(receipt) != fields
                or type(receipt['schema']) is not int or receipt['schema'] != 1
                or (info.config.entry_order_policy != 'single_attempt' and info.is_active)
                or receipt['trading_pair'] != info.config.trading_pair or receipt['side'] != 'BUY'
                or not quantity <= decimal(receipt['amount'], 'reconciled amount', positive=True) <= info.config.amount
                or decimal(receipt['filled_base'], 'reconciled filled quantity') != quantity):
            raise PolicyError('Native terminal reconciliation scope or quantity differs')
        order_id = receipt['order_id']
        owned = {key[2] for key in ExecutorOrchestrator._owned_order_keys(info)}
        if not isinstance(order_id, str) or not order_id or order_id not in owned:
            raise PolicyError('Native terminal reconciliation order is unowned')
        rows = [fill for fill in fills if fill.order_id == order_id]
        if (any(fill.trade_type != 'BUY' for fill in rows)
                or sum((fill.amount_decimal for fill in rows), ZERO) != quantity
                or receipt['trade_ids'] != sorted(str(fill.exchange_trade_id) for fill in rows)):
            raise PolicyError('Native terminal reconciliation trade coverage differs')
        mode = self.owner.config.execution_mode
        source = receipt['source']
        rejected = source == 'okx_submission_rejected_v1'
        if rejected:
            valid = mode == 'live' and receipt['state'] == 'REJECTED' and quantity == 0 and receipt['exchange_order_id'] is None
        else:
            expected = 'native_paper_terminal_v1' if mode == 'paper' else 'okx_terminal_status_v1'
            valid = (source == expected and receipt['state'] in {'CANCELED', 'FILLED'}
                     and isinstance(receipt['exchange_order_id'], str) and bool(receipt['exchange_order_id']))
        if not valid:
            raise PolicyError('Native terminal reconciliation authority unavailable')
        recorded_order = next((order for order in orders if order.id == order_id), None)
        if not rejected and (recorded_order is None
                             or recorded_order.exchange_order_id != receipt['exchange_order_id']
                             or recorded_order.symbol != receipt['trading_pair']
                             or recorded_order.market != self.owner.config.connector_name):
            raise PolicyError('Native recorded order conflicts with terminal funding release')
        # Terminal proof seals a single native submission. A later changed fill
        # is a contradiction and blocks new risk; exits continue independently.
        tracker = self.owner.market_data_provider.connectors[self.owner.config.connector_name]._order_tracker
        tracked = tracker.all_orders.get(order_id)
        if tracked is not None and (not tracked.is_done or tracked.executed_amount_base != quantity
                                    or tracked.exchange_order_id != receipt['exchange_order_id']):
            raise PolicyError('Native tracker conflicts with terminal funding release')

    def plan_binding_transition(self, expected_binding):
        """Read-only proposal; never called by automatic entry admission."""
        from .native_funding_transition import plan_transition
        return plan_transition(self, expected_binding)

    def apply_binding_transition(self, transition_id):
        """Explicit maintenance operation, requiring an unchanged reviewed plan."""
        from .native_funding_transition import apply_transition
        return apply_transition(self, transition_id)

    def reconcile(self):
        """Release only native-proven unused capacity; safe to retry after restart.

        This performs no exchange requests, order actions or balance mutations.
        The native executor/connector first produces authoritative evidence.
        """
        for _ in range(8):
            raw, state = self._load()
            previous = self._audit_hash(state)
            projection, pending, _ = self._evidence(state, raw, reconcile=True)
            state['fills'] = projection['fills']
            if previous == self._audit_hash(state) or self._write(raw, state):
                self.status = {'released_intents': sum(receipt != LEGACY_NEVER_STARTED for receipt in state['releases'].values()), 'pending_quote': str(pending), 'legacy_uncertain_quote': str(projection['legacy_uncertain_quote']),
                               'available_quote': str(max(ZERO, self.policy.allocation_quote - projection['used'] - pending)),
                               'unused_intent_release': 'native_terminal_reconciliation'}
                return dict(self.status)
        raise PolicyError('Funding reconciliation contention; no release')

    def entry_notional_ceiling(self):
        """Read-only sizing hint; admission still rechecks and durably claims it."""
        if any(not owner._composition_unchanged() for owner in self.owners.values()):
            raise PolicyError('Funding owner configuration changed')
        raw, state = self._load()
        projection, pending, _ = self._evidence(state, raw, reconcile=True)
        pair = self.owner.config.trading_pair
        available = min(self.policy.allocation_quote - projection['used'] - pending,
                        self.policy.max_position_quote - projection['used_by_pair'][pair]
                        - projection['pending_by_pair'][pair])
        return max(ZERO, available / (1 + self.policy.fee_reserve_fraction))

    def assess(self):
        """Inspect current native evidence without writing a claim or release.

        This is readiness evidence only. Admission rechecks the actual proposal
        against fresh evidence and persists its reservation before execution.
        """
        if any(not owner._composition_unchanged() for owner in self.owners.values()):
            raise PolicyError('Funding owner configuration changed')
        raw, state = self._load()
        projection, pending, _ = self._evidence(state, raw, reconcile=False)
        return {'assessment': 'read_only_native_evidence', 'binding': self.binding,
                'revision': state['revision'], 'pending_quote': str(pending),
                'inventory_cost': str(projection['inventory_cost']),
                'legacy_uncertain_quote': str(projection['legacy_uncertain_quote']),
                'available_quote': str(max(ZERO, self.policy.allocation_quote - projection['used'] - pending))}

    def admit(self, executor_config):
        config = self.owner.config
        if (executor_config.side != TradeType.BUY or executor_config.type != 'position_executor'
                or executor_config.connector_name != config.connector_name or executor_config.trading_pair != config.trading_pair):
            raise PolicyError('Unsupported funded acquisition')
        amount = decimal(executor_config.amount, 'candidate acquisition', positive=True)
        price = decimal(executor_config.entry_price, 'candidate limit price', positive=True)
        order_type = executor_config.triple_barrier_config.open_order_type
        if not order_type.is_limit_type():
            raise PolicyError('Funding requires a native bounded-price limit acquisition')
        connector = self.owner.market_data_provider.connectors[config.connector_name]
        base, quote = config.trading_pair.split('-')
        for maker in (False, True):
            fee = connector.get_fee(base_currency=base, quote_currency=quote, order_type=order_type,
                                    order_side=TradeType.BUY, amount=amount, price=price, is_maker=maker)
            rate = decimal(fee.percent, 'native estimated fee')
            if fee.percent_token not in (None, base, quote) or fee.flat_fees or rate > self.policy.fee_reserve_fraction:
                raise PolicyError('Native fee exceeds explicit funding fee bound or has unsupported fixed fees')
        self.reconcile()
        claim = {'config': _config_identity(executor_config, config.id), 'amount': str(amount),
                 'price': str(price), 'fee_fraction': str(self.policy.fee_reserve_fraction)}
        for _ in range(8):
            raw, state = self._load()
            projection, pending, infos = self._evidence(state, raw)
            existing = state['claims'].get(executor_config.id)
            if existing is not None:
                if existing != claim or executor_config.id in infos:
                    raise PolicyError('Duplicate or mutated funded native executor')
                raise PolicyError('Unresolved funded intent requires explicit reconciliation')
            required = amount * price * (1 + self.policy.fee_reserve_fraction)
            if (required + projection['used_by_pair'][config.trading_pair]
                    + projection['pending_by_pair'][config.trading_pair] > self.policy.max_position_quote):
                raise PolicyError('Candidate per-position funding allocation exhausted')
            self.status = {'allocation_quote': str(self.policy.allocation_quote),
                           'native_cash_used': str(projection['cash_used']),
                           'native_inventory_cost': str(projection['inventory_cost']),
                           'pending_quote': str(pending), 'legacy_uncertain_quote': str(projection['legacy_uncertain_quote']), 'new_quote': str(required),
                           'available_quote': str(max(ZERO, self.policy.allocation_quote - projection['used'] - pending)),
                           'released_intents': sum(receipt != LEGACY_NEVER_STARTED for receipt in state['releases'].values()),
                           'unused_intent_release': 'native_terminal_reconciliation'}
            if projection['used'] + pending + required > self.policy.allocation_quote:
                raise PolicyError('Candidate funding allocation exhausted')
            state['claims'][executor_config.id] = claim
            state['fills'] = projection['fills']
            if self._write(raw, state):
                return True
        raise PolicyError('Funding checkpoint contention; no acquisition')
