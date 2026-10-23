"""Candidate admission only. Native fills, executors and PositionHold own accounting.

One recorder/strategy envelope. Allocation and per-position quote are yaml-set.
The default remains one controller/pair; strategy scope binds every native peer.
This does not reserve funds from independent processes sharing the wallet.
"""
import copy
import json
import re
from decimal import Decimal
from types import SimpleNamespace
from typing import Literal

from pydantic import Field
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1, validate_acquisition_chain
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator, PositionHold
from hummingbot.strategy_v2.models.executors import CloseType

from .config import PolicyModel
from .domain import PolicyError, decimal, identity

ZERO = Decimal('0')
LEGACY_NEVER_STARTED = {'schema': 1, 'source': 'never_started_intent_v1'}
_TERMINAL_ORDER_STATUS_KIND = {
    'BuyOrderCompleted': 'BUY_FILLED',
    'SellOrderCompleted': 'SELL_FILLED',
    'OrderCancelled': 'CANCELED',
    'OrderCanceled': 'CANCELED',
    'OrderExpired': 'EXPIRED',
    'OrderFailure': 'FAILED',
}


class CandidateFundingPolicy(PolicyModel):
    allocation_quote: Decimal | None = Field(..., gt=0, le=2500)
    fee_reserve_fraction: Decimal = Field(gt=0, le=Decimal('.1'))
    max_position_quote: Decimal | None = Field(default=50, gt=0, le=505)
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
        valued_fee = decimal(fill.trade_fee_in_quote_decimal, 'native valued fee', minimum=None)
        quote_fee = actual_quote_fee = ZERO
        for fee in fill.trade_fee['flat_fees']:
            amount = decimal(fee['amount'], 'actual fee', minimum=None)
            if fee['token'] not in (base, quote):
                raise ValueError('Third-token funding fee is unsupported')
            if fee['token'] == quote:
                quote_fee += amount
            actual_quote_fee += amount * (fill.price_decimal if fee['token'] == base else Decimal('1'))
        if actual_quote_fee != valued_fee:
            raise ValueError('Native valued fee differs from actual currency fees')
        amount, price = fill.amount_decimal, fill.price_decimal
        notional = amount * price
        cash += (notional if fill.trade_type == 'BUY' else -notional) + quote_fee
        key = identity([fill.market, fill.order_id, str(fill.exchange_trade_id)])
        if key in fingerprints:
            raise ValueError('Duplicate native funding fill')
        fingerprints[key] = identity(json.loads(json.dumps(
            [fill.config_file_path, fill.timestamp, record, fill.trade_fee], default=str)))
        if fill.trade_type == 'BUY':
            by_order[fill.order_id] = by_order.get(fill.order_id, ZERO) + amount
        position.add_order_record(record)
        # A validated BUY adds nonnegative native base and cannot create a
        # short position. Replaying every BUY prefix is quadratic in retained
        # history. Keep the native prefix check at every SELL so a later BUY
        # cannot conceal an earlier sale of unowned inventory.
        if fill.trade_type == 'SELL':
            summary = position.get_position_summary(price)
            if summary.side != TradeType.BUY and summary.amount > 0:
                raise ValueError('Funding history sells unowned inventory')
    summary = position.get_position_summary(Decimal('1'))
    # Reuse native remaining fee attribution; do not invent another cost ledger.
    cost = summary.amount * summary.breakeven_price + summary.remaining_fees_quote
    if not cost.is_finite() or cost < 0:
        raise ValueError('Native acquisition cost is unavailable')
    return {'cash_used': cash, 'inventory_cost': cost, 'inventory_base': summary.amount, 'used': max(ZERO, cash, cost),
            'fills': fingerprints, 'bought_by_order': by_order}


def _config_identity(config, controller_id):
    payload = config.model_dump(mode='json')
    # The V2 opt-in field did not exist in historical down-only executor
    # payloads. Its default None is not a change to that immutable intent.
    # A concrete V2 contract remains part of the hash.
    if (getattr(config, 'entry_acquisition_policy', None) == 'down_only_maker_v1'
            and payload.get('entry_chain_contract') is None):
        payload.pop('entry_chain_contract', None)
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
            self._binding_peers = tuple(peers)
        else:
            self.owners = {owner.config.id: owner}
            self._binding_peers = (owner,)
        self.binding = self._binding_for_source(None)
        self._code_upgrade_checked = False
        self.status = {}

    def _available_quote(self, projection, pending):
        """Quote still admissible after native unresolved commitments.

        The bounded envelope uses its historical fill-derived allocation. Wallet
        mode uses the connector's current free USDC, with unresolved native
        claims reserved before another owner may submit a buy.
        """
        if self.policy.allocation_quote is not None:
            return max(ZERO, self.policy.allocation_quote - projection['used'] - pending)
        connector = self.owner.market_data_provider.connectors[self.owner.config.connector_name]
        free = decimal(connector.get_available_balance('USDC'), 'native available USDC')
        return max(ZERO, free - pending)

    def _binding_for_source(self, source_sha256):
        """The binding formula; ``source_sha256`` overrides every owner's native source hash."""
        def source(peer):
            return peer._native_source_hash if source_sha256 is None else source_sha256
        if self.policy.scope == 'strategy':
            return identity(['strategy', self.policy.model_dump(mode='json'),
                             [[peer.config.id, peer.config.connector_name, peer.config.trading_pair,
                               peer._composition_digest, source(peer)] for peer in self._binding_peers]])
        owner = self._binding_peers[0]
        return identity([owner.config.id, owner.config.connector_name, owner.config.trading_pair,
                         owner._composition_digest, source(owner)])

    @property
    def _source_key(self):
        return 'modular-funding-source:v1:' + identity([self.key])

    def _source_record(self):
        return json.dumps({'binding': self.binding,
                           'source_sha256': sorted({peer._native_source_hash for peer in self._binding_peers})},
                          sort_keys=True, separators=(',', ':'), allow_nan=False)

    def _previous_source_hashes(self, session):
        """Native source hashes these owners ran under before.

        The funding store records its own (written with every ledger write);
        held-recovery checkpoints are a second witness for older ledgers.
        """
        hashes = set()
        row = session.get(Metadata, self._source_key)
        try:
            recorded = None if row is None else json.loads(row.value).get('source_sha256')
        except (TypeError, ValueError, AttributeError):
            recorded = None
        if isinstance(recorded, list):
            hashes.update(value for value in recorded if isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value))
        for owner in self.owners.values():
            key = 'modular-native-held:v1:' + identity([
                self.recorder.config_file_path, owner.config.id, owner.config.connector_name, owner.config.trading_pair])
            row = session.get(Metadata, key)
            try:
                value = None if row is None else json.loads(row.value).get('source_sha256')
            except (TypeError, ValueError, AttributeError):
                value = None
            if isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value):
                hashes.add(value)
        return hashes

    def _rebind_after_code_upgrade(self, persisted_binding):
        """Carry the funding ledger across a pure code upgrade of the same owners.

        Only when the persisted binding recomputes exactly from a previously
        checkpointed native source hash (same owners, pairs, policy and
        composition): every claim, fill and release stays byte-identical except
        its binding, the prior rows are archived in a receipt, and entry
        admission continues. Any other identity change is still refused.
        """
        with self.recorder.sql_manager.get_new_session() as session, session.begin():
            session.connection().exec_driver_sql('BEGIN IMMEDIATE')
            header_row = session.get(Metadata, self.key)
            if header_row is None:
                return False
            header = json.loads(header_row.value)
            if not isinstance(header, dict) or header.get('binding') != persisted_binding:
                return False
            previous = next((source for source in sorted(self._previous_source_hashes(session))
                             if self._binding_for_source(source) == persisted_binding), None)
            if previous is None:
                return False
            rows = session.query(Metadata).filter(Metadata.key.like(self.key + ':%')).all()
            before = {self.key: header_row.value, **{row.key: row.value for row in rows}}
            for row in rows:
                value = json.loads(row.value)
                if isinstance(value, dict) and value.get('binding') == persisted_binding:
                    value['binding'] = self.binding
                    row.value = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
            header['binding'] = self.binding
            header['revision'] = header['revision'] + 1
            header_row.value = json.dumps(header, sort_keys=True, separators=(',', ':'), allow_nan=False)
            receipt = {'schema': 1, 'kind': 'native_code_upgrade_v1', 'from_binding': persisted_binding,
                       'to_binding': self.binding, 'from_source_sha256': previous,
                       'to_source_sha256': sorted({peer._native_source_hash for peer in self._binding_peers}),
                       'before': before}
            source_row = session.get(Metadata, self._source_key)
            if source_row is None:
                session.add(Metadata(key=self._source_key, value=self._source_record()))
            else:
                source_row.value = self._source_record()
            session.add(Metadata(key='modular-funding-code-upgrade:v1:' + identity([self.key, persisted_binding,
                                                                                   self.binding]),
                                 value=json.dumps(receipt, sort_keys=True, separators=(',', ':'), allow_nan=False)))
        self.owner.logger().warning(
            'Funding ledger carried across native code upgrade | from_source=%s binding=%s->%s revision=%s',
            previous, persisted_binding, self.binding, header['revision'])
        return True

    def _load(self, *, expected_binding=None, allow_code_upgrade=True):
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
        if (allow_code_upgrade and header['binding'] != binding and expected_binding is None and raw is not None
                and not self._code_upgrade_checked):
            rebound = self._rebind_after_code_upgrade(header['binding'])  # a lock error retries next time
            self._code_upgrade_checked = True
            if rebound:
                return self._load()
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
                    result = session.execute(update(Metadata).where(Metadata.key == self.key,
                                             Metadata.value == raw).values(value=encoded))
                    if result.rowcount != 1:
                        return False
                if getattr(self, '_binding_peers', None):
                    source_row = session.get(Metadata, self._source_key)
                    if source_row is None:
                        session.add(Metadata(key=self._source_key, value=self._source_record()))
                    else:
                        source_row.value = self._source_record()
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

    def _legacy_chain_compatibility(self, info, claim):
        """Verify an explicit, claimed offline migration of one old BTC chain.

        The marker is only a pointer. The claimed planned-resume manifest binds
        original and resume configs, every native child, and the closed chain.
        No legacy order set is inferred or upgraded from its shape alone.
        """
        marker = (info.custom_info or {}).get('planned_position_resume_compatibility_v1')
        fields = {'schema', 'kind', 'manifest_id', 'executor_id', 'original_config_sha256',
                  'resume_config_sha256', 'acquisition_chain_sha256', 'entry_order_ids'}
        if (not isinstance(marker, dict) or set(marker) != fields
                or type(marker['schema']) is not int or marker['schema'] != 1
                or marker['kind'] != 'legacy_single_post_only_repair_v1'
                or marker['executor_id'] != info.id
                or not isinstance(marker['manifest_id'], str) or not marker['manifest_id']
                or not isinstance(marker['entry_order_ids'], list)
                or len(marker['entry_order_ids']) != 2
                or len(set(marker['entry_order_ids'])) != 2
                or any(not isinstance(value, str) or not re.fullmatch('[a-f0-9]{64}', value)
                       for value in (marker['original_config_sha256'], marker['resume_config_sha256'],
                                     marker['acquisition_chain_sha256']))
                or not isinstance(claim, dict)):
            raise PolicyError('Legacy acquisition compatibility receipt missing or invalid')
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                row = session.connection().exec_driver_sql(
                    'SELECT state, payload FROM PlannedPositionResume WHERE id=?',
                    (marker['manifest_id'],)).fetchone()
        except SQLAlchemyError as error:
            raise PolicyError('Legacy acquisition compatibility manifest is unavailable') from error
        if row is None or row[0] != 'claimed':
            raise PolicyError('Legacy acquisition compatibility manifest is not claimed')
        try:
            manifest = json.loads(row[1])
            positions = [position for position in manifest['positions'] if position['executor_id'] == info.id]
            current_reference = (info.custom_info or {}).get('planned_position_resume')
            if manifest['id'] != marker['manifest_id'] or len(positions) != 1 or not current_reference:
                raise ValueError('manifest identity')
            position = positions[0]
            if current_reference != manifest['id']:
                # Later handovers reference their own claimed manifest while the
                # immutable compatibility bridge remains anchored to the first.
                with self.recorder.sql_manager.get_new_session() as session:
                    current_row = session.connection().exec_driver_sql(
                        'SELECT state, payload FROM PlannedPositionResume WHERE id=?',
                        (current_reference,)).fetchone()
                if current_row is None or current_row[0] != 'claimed':
                    raise ValueError('current handover is not claimed')
                current_manifest = json.loads(current_row[1])
                current_positions = [row for row in current_manifest['positions'] if row['executor_id'] == info.id]
                if current_manifest['id'] != current_reference or len(current_positions) != 1:
                    raise ValueError('current handover identity')
                current_position = current_positions[0]
                if (current_position.get('planned_position_resume_compatibility_v1') != marker
                        or current_position.get('entry_orders') != position['entry_orders']
                        or current_position.get('entry_reconciliation') != position['entry_reconciliation']
                        or current_position.get('acquisition_chain') != position['acquisition_chain']
                        or current_position.get('resume_config', current_position['config']) != position['resume_config']):
                    raise ValueError('current handover compatibility archive differs')
            old_raw, resume_raw = position['config'], position['resume_config']
            chain = position['acquisition_chain']
            if (identity(old_raw) != marker['original_config_sha256']
                    or identity(resume_raw) != marker['resume_config_sha256']
                    or identity(chain) != marker['acquisition_chain_sha256']
                    or [child['order_id'] for child in chain['children']] != marker['entry_order_ids']
                    or chain['children'][1]['purpose'] != 'post_only_repair'
                    or (info.custom_info or {}).get('entry_reconciliation') != position['entry_reconciliation']):
                raise ValueError('manifest evidence')
            from hummingbot.strategy_v2.executors.position_executor.data_types import parse_position_config
            original = parse_position_config(old_raw)
            resume = parse_position_config(resume_raw)
            expected_resume = original.model_dump(mode='json')
            expected_resume['entry_chain_contract'] = ACQUISITION_CHAIN_V1
            if (resume.model_dump(mode='json') != expected_resume
                    or getattr(original, 'entry_chain_contract', None) is not None
                    or getattr(resume, 'entry_chain_contract', None) != ACQUISITION_CHAIN_V1
                    or _config_identity(original, info.controller_id) != claim['config']
                    or _config_identity(info.config, info.controller_id) not in {
                        _config_identity(original, info.controller_id),
                        _config_identity(resume, info.controller_id)}):
                raise ValueError('configuration transition')
            snapshots = position['entry_orders']
            current_snapshots = (info.custom_info or {}).get('execution_order_snapshots')
            if (not isinstance(snapshots, list) or not isinstance(current_snapshots, list)
                    or any(not isinstance(item, dict) for item in current_snapshots)
                    or [item for item in current_snapshots if item.get('trade_type') == 'BUY'] != snapshots
                    or snapshots[0].get('cancel_source') != '31'):
                raise ValueError('native order archive')
            compatible_info = dict(info.custom_info or {}, acquisition_chain=chain,
                                   execution_order_snapshots=snapshots)
            validate_acquisition_chain(resume, compatible_info, position['entry_reconciliation'])
            return marker
        except (KeyError, IndexError, TypeError, ValueError, AttributeError, json.JSONDecodeError, SQLAlchemyError) as error:
            raise PolicyError('Legacy acquisition compatibility manifest differs') from error

    @staticmethod
    def _committed_unstarted_intent(info, config):
        """The exact just-ACKed create, before its controller writes a claim."""
        return (config is not None
                and ExecutorOrchestrator.is_committed_unstarted_intent(info, config, config.controller_id)
                and _config_identity(info.config, info.controller_id) == _config_identity(config, config.controller_id))

    def _evidence(self, state, raw, *, reconcile=False, allow_quiescent_legacy=False,
                  committed_intent_config=None):
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
                    if not (getattr(previous.config, 'entry_acquisition_policy', None) == 'down_only_maker_v1'
                            and getattr(info.config, 'entry_acquisition_policy', None) == 'down_only_maker_v1'
                            and (previous.custom_info or {}).get('planned_position_resume_compatibility_v1')
                            and (info.custom_info or {}).get('planned_position_resume_compatibility_v1')):
                        raise PolicyError('Saved/current native executor configuration conflict')
                    claim = state['claims'].get(info.id)
                    self._legacy_chain_compatibility(previous, claim)
                    self._legacy_chain_compatibility(info, claim)
                infos[info.id] = info
        fills = self.recorder.get_trades_for_config(self.recorder.config_file_path)
        connector = self.owner.market_data_provider.connectors[config.connector_name]
        orders = self.recorder.get_orders_for_config_and_market(self.recorder.config_file_path, connector)
        if raw is None and (any(not ExecutorOrchestrator.is_refused_unstarted_intent(info)
                                and not self._committed_unstarted_intent(info, committed_intent_config)
                                for info in infos.values())
                            or fills or orders or any(peer.positions_held for peer in self.owners.values())):
            raise PolicyError('Native history exists without funding checkpoint; reconcile explicitly')
        owned = {}
        for info in infos.values():
            cfg = info.config
            owner_config = self.owners[info.controller_id].config
            if cfg.connector_name != owner_config.connector_name or cfg.trading_pair != owner_config.trading_pair:
                raise PolicyError('Foreign native executor scope in funding recorder')
            if cfg.side == TradeType.BUY:
                claim = state['claims'].get(info.id)
                if claim is None:
                    if (ExecutorOrchestrator.is_refused_unstarted_intent(info)
                            or self._committed_unstarted_intent(info, committed_intent_config)):
                        continue
                    raise PolicyError('Native BUY lacks immutable funding admission')
                if claim['config'] != _config_identity(cfg, info.controller_id):
                    if not ((info.custom_info or {}).get('planned_position_resume_compatibility_v1')
                            and getattr(cfg, 'entry_acquisition_policy', None) == 'down_only_maker_v1'):
                        raise PolicyError('Native BUY lacks immutable funding admission')
                    self._legacy_chain_compatibility(info, claim)
            for _, _, order_id in ExecutorOrchestrator._owned_order_keys(info):
                if order_id in owned and owned[order_id] != info.id:
                    raise PolicyError('Native order has ambiguous funding owner')
                owned[order_id] = info.id
        if any(order.id not in owned for order in orders):
            raise PolicyError('Native order lacks executor ownership')
        if any(key not in infos and state['releases'].get(key) != LEGACY_NEVER_STARTED for key in state['claims']):
            raise PolicyError('Unresolved funded intent requires explicit reconciliation')
        if any(info.is_active and info.id not in {item.id for item in current}
               and not self._committed_unstarted_intent(info, committed_intent_config) for info in saved):
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
            refused_unstarted = (info is not None
                                 and ExecutorOrchestrator.is_refused_unstarted_intent(info))
            if info is not None and not refused_unstarted:
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
            never_submitted = self._never_submitted_release(info, quantity)
            if never_submitted is not None and released in (None, never_submitted):
                # Its own pre-submission budget check stopped it before any order
                # existed, so no venue commitment can remain to reserve.
                if reconcile or released is not None:
                    state['releases'][executor_id] = never_submitted
                continue
            receipt = (info.custom_info or {}).get('entry_reconciliation') if info is not None else None
            legacy_orders = (receipt.get('orders') if isinstance(receipt, dict)
                             and receipt.get('source') == 'native_terminal_entry_set_v1' else None)
            defer_legacy_release = (info is not None
                                    and getattr(info.config, 'entry_acquisition_policy', None) == 'down_only_maker_v1'
                                    and getattr(info.config, 'entry_chain_contract', None) is None
                                    and isinstance(legacy_orders, list) and len(legacy_orders) > 1
                                    and (info.custom_info or {}).get('planned_position_resume_compatibility_v1') is None)
            # Only an explicit quiescent transition retains a legacy multi-order
            # set unreleased; ordinary reconciliation releases it through the
            # single-filled-order rule with full per-order checks.
            defer_legacy_release = defer_legacy_release and allow_quiescent_legacy
            if defer_legacy_release and quantity != authorized:
                raise PolicyError('Legacy acquisition transition requires fully consumed authorization')
            chain = None
            if info is not None and getattr(info.config, 'entry_chain_contract', None) == ACQUISITION_CHAIN_V1:
                # An in-progress parent keeps its entire remaining commitment,
                # even when the previous child is terminal and the next child
                # has been authorized but has no exchange order ID yet.
                chain = validate_acquisition_chain(info.config, info.custom_info, receipt, require_closed=False,
                                                   allow_unacknowledged_tail=True)
            if released is not None or (reconcile and receipt is not None and not defer_legacy_release
                                        and (chain is None or chain['membership_closed'])):
                self._validate_release(info, receipt, quantity, fills, orders, state['claims'][executor_id])
                if released is not None and released != receipt:
                    raise PolicyError('Native terminal reconciliation changed after funding release')
                state['releases'][executor_id] = receipt
            else:
                reserved = (authorized - quantity) * \
                    decimal(claim['price'], 'funded price') * (1 + self.policy.fee_reserve_fraction)
                pending += reserved
                projection['pending_by_pair'][info.config.trading_pair] += reserved
        return projection, pending, infos

    @staticmethod
    def _never_submitted_release(info, quantity):
        """Release receipt for an executor that closed INSUFFICIENT_BALANCE before any order.

        `ExecutorBase.on_start` runs `validate_sufficient_balance()` before creating an
        order and closes INSUFFICIENT_BALANCE without a terminal entry receipt. Any
        owned order id, order snapshot, fill or receipt means a submission may exist,
        so the commitment stays reserved until native reconciliation proves it.
        """
        custom = (info.custom_info or {}) if info is not None else {}
        if (quantity == ZERO and info is not None
                and ExecutorOrchestrator.is_refused_unstarted_intent(info)):
            return {'schema': 1, 'source': 'native_refused_before_activation_v1', 'executor_id': info.id}
        if (info is None or quantity != ZERO or info.is_active
                or getattr(info, 'close_type', None) != CloseType.INSUFFICIENT_BALANCE
                or custom.get('entry_reconciliation') is not None or custom.get('order_ids')
                or custom.get('execution_order_snapshots') or ExecutorOrchestrator._owned_order_keys(info)):
            return None
        return {'schema': 1, 'source': 'native_insufficient_balance_before_submission_v1', 'executor_id': info.id}

    def _validate_release(self, info, receipt, quantity, fills, orders, claim=None):
        if info is None or not isinstance(receipt, dict):
            raise PolicyError('Native terminal reconciliation scope or quantity differs')
        if receipt.get('source') == 'native_terminal_entry_set_v1':
            if (set(receipt) != {'schema', 'source', 'orders'} or type(receipt['schema']) is not int
                    or receipt['schema'] != 1 or not isinstance(receipt['orders'], list) or not receipt['orders']):
                raise PolicyError('Native terminal entry set is invalid')
            proofs = receipt['orders']
        else:
            proofs = [receipt]
        if getattr(info.config, 'entry_chain_contract', None) == ACQUISITION_CHAIN_V1:
            validate_acquisition_chain(info.config, info.custom_info, proofs)
        elif info.config.entry_order_policy == 'single_attempt' and len(proofs) != 1:
            # A legacy down-only maker intent may replace an unfilled order
            # (post-only repair, downward reprice); it never replaces after any
            # fill. A claimed offline migration additionally binds its marker.
            if (info.custom_info or {}).get('planned_position_resume_compatibility_v1') is not None:
                self._legacy_chain_compatibility(info, claim)
            filled = [proof for proof in proofs if isinstance(proof, dict)
                      and decimal(proof.get('filled_base'), 'terminal entry total') > ZERO]
            if len(filled) > 1:
                raise PolicyError('Single-attempt reconciliation contains multiple filled entry orders')
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
            self._validate_order_release(info, proof, decimal(
                proof['filled_base'], 'terminal fill total'), fills, orders)

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
        snapshots = (info.custom_info or {}).get('execution_order_snapshots') or []
        native = [item for item in snapshots if isinstance(item, dict) and item.get('client_order_id') == order_id]
        if len(native) != 1:
            raise PolicyError('Native terminal entry snapshot is unavailable or ambiguous')
        from hummingbot.core.data_type.in_flight_order import InFlightOrder
        order = InFlightOrder.from_json(native[0])
        base, quote = info.config.trading_pair.split('-')
        for fill in rows:
            trade = order.order_fills.get(str(fill.exchange_trade_id))
            if (trade is None or trade.fill_base_amount != fill.amount_decimal
                    or trade.fill_price != fill.price_decimal
                    or trade.fill_quote_amount != fill.amount_decimal * fill.price_decimal
                    or trade.fee.to_json() != fill.trade_fee):
                raise PolicyError('Native terminal entry fill or fee differs from recorder')
            valued_fee = sum((decimal(fee.amount, 'actual entry fee', minimum=None) *
                              (trade.fill_price if fee.token == base else Decimal('1'))
                              for fee in trade.fee.flat_fees if fee.token in (base, quote)), ZERO)
            if (len(trade.fee.flat_fees) != sum(fee.token in (base, quote) for fee in trade.fee.flat_fees)
                    or trade.fee.percent != ZERO or valued_fee != fill.trade_fee_in_quote_decimal):
                raise PolicyError('Native terminal entry valued fee differs from recorder')
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
        if getattr(info.config, 'entry_chain_contract', None) == ACQUISITION_CHAIN_V1:
            status_history = (self.recorder.get_order_status_history(order_id)
                              if recorded_order is not None else [])
            terminal_kinds = {_TERMINAL_ORDER_STATUS_KIND[status] for status in status_history
                              if status in _TERMINAL_ORDER_STATUS_KIND}
            if len(terminal_kinds) > 1:
                raise PolicyError('Native recorded child has conflicting terminal order receipts')
            if rejected:
                if (recorded_order is not None
                        and (recorded_order.last_status != 'OrderFailure'
                             or recorded_order.exchange_order_id not in (None, '')
                             or recorded_order.symbol != receipt['trading_pair']
                             or recorded_order.market != self.owner.config.connector_name)):
                    raise PolicyError('Native recorded child status conflicts with terminal funding release')
                if terminal_kinds and terminal_kinds != {'FAILED'}:
                    raise PolicyError('Native recorded child status conflicts with terminal funding release')
            else:
                expected_statuses = ({'OrderCancelled', 'OrderCanceled'} if receipt['state'] == 'CANCELED'
                                     else {'BuyOrderCompleted'})
                if recorded_order.last_status not in expected_statuses:
                    raise PolicyError('Native recorded child status conflicts with terminal funding release')
                expected_kind = 'CANCELED' if receipt['state'] == 'CANCELED' else 'BUY_FILLED'
                if terminal_kinds and terminal_kinds != {expected_kind}:
                    raise PolicyError('Native recorded child status conflicts with terminal funding release')
        # Terminal proof seals a single native submission. A later changed fill
        # is a contradiction and blocks new risk; exits continue independently.
        tracker = self.owner.market_data_provider.connectors[self.owner.config.connector_name]._order_tracker
        tracked = tracker.all_orders.get(order_id)
        if tracked is not None and (not tracked.is_done or tracked.executed_amount_base != quantity
                                    or tracked.exchange_order_id != receipt['exchange_order_id']):
            raise PolicyError('Native tracker conflicts with terminal funding release')

    def plan_binding_transition(self, expected_binding, *, legacy_evidence=None,
                                allocation_only_previous_configs=None):
        """Read-only proposal; never called by automatic entry admission."""
        from .native_funding_transition import plan_transition
        return plan_transition(self, expected_binding, legacy_evidence=legacy_evidence,
                               allocation_only_previous_configs=allocation_only_previous_configs)

    def prepare_runtime_binding(self):
        """Run the existing source-only upgrade policy once for a live owner.

        This is a mutating runtime step, separate from read-only readiness and
        sizing. Configuration and ownership changes still require the explicit
        stopped-owner transition. A failed load never marks preparation complete.
        """
        if not self._code_upgrade_checked:
            self._load()
            self._code_upgrade_checked = True

    def apply_binding_transition(self, transition_id):
        """Explicit maintenance operation, requiring an unchanged reviewed plan."""
        from .native_funding_transition import apply_transition
        return apply_transition(self, transition_id)

    def reconcile(self, *, committed_intent_config=None):
        """Release only native-proven unused capacity; safe to retry after restart.

        This performs no exchange requests, order actions or balance mutations.
        The native executor/connector first produces authoritative evidence.
        """
        for _ in range(8):
            raw, state = self._load()
            previous = self._audit_hash(state)
            projection, pending, _ = self._evidence(
                state, raw, reconcile=True, committed_intent_config=committed_intent_config)
            state['fills'] = projection['fills']
            if previous == self._audit_hash(state) or self._write(raw, state):
                self.status = {'released_intents': sum(receipt != LEGACY_NEVER_STARTED for receipt in state['releases'].values()), 'pending_quote': str(pending), 'legacy_uncertain_quote': str(projection['legacy_uncertain_quote']),
                               'available_quote': str(self._available_quote(projection, pending)),
                               'unused_intent_release': 'native_terminal_reconciliation'}
                return dict(self.status)
        raise PolicyError('Funding reconciliation contention; no release')

    def entry_notional_ceiling(self):
        """Read-only sizing hint; admission still rechecks and durably claims it."""
        if any(not owner._composition_unchanged() for owner in self.owners.values()):
            raise PolicyError('Funding owner configuration changed')
        raw, state = self._load(allow_code_upgrade=False)
        projection, pending, _ = self._evidence(state, raw, reconcile=True)
        pair = self.owner.config.trading_pair
        available = self._available_quote(projection, pending)
        if self.policy.max_position_quote is not None:
            available = min(available, self.policy.max_position_quote - projection['used_by_pair'][pair]
                            - projection['pending_by_pair'][pair])
        return max(ZERO, available / (1 + self.policy.fee_reserve_fraction))

    def assess(self):
        """Inspect current native evidence without any durable funding writes.

        This is readiness evidence only. Admission rechecks the actual proposal
        against fresh evidence and persists its reservation before execution.
        A source-binding mismatch is reported without running code-upgrade
        maintenance; only mutating runtime or explicit maintenance paths rebind.
        """
        if any(not owner._composition_unchanged() for owner in self.owners.values()):
            raise PolicyError('Funding owner configuration changed')
        raw, state = self._load(allow_code_upgrade=False)
        projection, pending, _ = self._evidence(state, raw, reconcile=False)
        return {'assessment': 'read_only_native_evidence', 'binding': self.binding,
                'revision': state['revision'], 'pending_quote': str(pending),
                'inventory_cost': str(projection['inventory_cost']),
                'legacy_uncertain_quote': str(projection['legacy_uncertain_quote']),
                'available_quote': str(self._available_quote(projection, pending))}

    def assess_with_revision_check(self):
        """Reject a read snapshot crossed by a durable funding checkpoint write."""
        status = self.assess()
        _, latest = self._load(allow_code_upgrade=False)
        status['revision_verified'] = latest['revision'] == status['revision']
        return status

    def assessment_snapshot(self):
        """Freeze controller-owned inputs and isolate recorder reads for worker assessment.

        The read-only assessment still uses this store's validation and projection
        code. The snapshot prevents the worker from traversing live controller or
        connector objects, and the recorder adapter always opens fresh SQL sessions
        instead of observing a synchronous decision's process-local read cache.
        """
        from hummingbot.model.executors import Executors

        owners = {}
        for controller_id, owner in self.owners.items():
            config = owner.config.model_copy(deep=True)
            connector = owner.market_data_provider.connectors[config.connector_name]
            balance = (connector.get_available_balance('USDC')
                       if self.policy.allocation_quote is None else ZERO)
            orders = {}
            for order_id, order in connector._order_tracker.all_orders.items():
                orders[order_id] = SimpleNamespace(
                    order_fills=set(order.order_fills),
                    is_done=order.is_done,
                    executed_amount_base=order.executed_amount_base,
                    exchange_order_id=order.exchange_order_id,
                )
            connector_view = SimpleNamespace(
                display_name=connector.display_name,
                get_available_balance=lambda asset, value=balance: value if asset == 'USDC' else ZERO,
                _order_tracker=SimpleNamespace(all_orders=orders),
            )
            owner_view = SimpleNamespace(
                config=config,
                executors_info=[info.model_copy(deep=True) for info in owner.executors_info],
                positions_held=copy.deepcopy(owner.positions_held),
                _native_source_hash=owner._native_source_hash,
                _composition_digest=owner._composition_digest,
                _composition_unchanged=lambda value=owner._composition_unchanged(): value,
                market_data_provider=SimpleNamespace(connectors={config.connector_name: connector_view}),
            )
            owners[controller_id] = owner_view
        peers = tuple(owners[key] for key in sorted(owners))
        for owner in peers:
            owner._funding_peers = peers

        class FreshReadRecorder:
            def __init__(self, recorder):
                self._recorder = recorder
                self.config_file_path = recorder.config_file_path
                self.sql_manager = recorder.sql_manager

            def __getattr__(self, name):
                return getattr(self._recorder, name)

            def get_all_executors(self):
                with self.sql_manager.get_new_session() as session:
                    rows = session.query(Executors).all()
                    return [row.to_executor_info() for row in rows]

        snapshot = copy.copy(self)
        snapshot.owner = owners[self.owner.config.id]
        snapshot.owners = owners
        snapshot._binding_peers = peers
        snapshot.policy = self.policy.model_copy(deep=True)
        snapshot.recorder = FreshReadRecorder(self.recorder)
        return snapshot

    def admit(self, executor_config, *, committed_intent_config=None):
        config = self.owner.config
        if (executor_config.side != TradeType.BUY or executor_config.type != 'position_executor'
                or executor_config.connector_name != config.connector_name or executor_config.trading_pair != config.trading_pair):
            raise PolicyError('Unsupported funded acquisition')
        if (getattr(self.owner, '_native_family', None) == 'ok_rsi'
                and getattr(executor_config, 'entry_acquisition_policy', None) == 'down_only_maker_v1'
                and getattr(executor_config, 'entry_chain_contract', None) != ACQUISITION_CHAIN_V1):
            raise PolicyError('V2 bounded acquisition funding contract is unavailable')
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
        self.reconcile(committed_intent_config=committed_intent_config)
        claim = {'config': _config_identity(executor_config, config.id), 'amount': str(amount),
                 'price': str(price), 'fee_fraction': str(self.policy.fee_reserve_fraction)}
        for _ in range(8):
            raw, state = self._load()
            projection, pending, infos = self._evidence(
                state, raw, committed_intent_config=committed_intent_config)
            existing = state['claims'].get(executor_config.id)
            if existing is not None:
                if existing != claim or executor_config.id in infos:
                    raise PolicyError('Duplicate or mutated funded native executor')
                raise PolicyError('Unresolved funded intent requires explicit reconciliation')
            required = amount * price * (1 + self.policy.fee_reserve_fraction)
            if (self.policy.max_position_quote is not None
                    and required + projection['used_by_pair'][config.trading_pair]
                    + projection['pending_by_pair'][config.trading_pair] > self.policy.max_position_quote):
                raise PolicyError('Candidate per-position funding allocation exhausted')
            available = self._available_quote(projection, pending)
            self.status = {'allocation_quote': None if self.policy.allocation_quote is None else str(self.policy.allocation_quote),
                           'native_cash_used': str(projection['cash_used']),
                           'native_inventory_cost': str(projection['inventory_cost']),
                           'pending_quote': str(pending), 'legacy_uncertain_quote': str(projection['legacy_uncertain_quote']), 'new_quote': str(required),
                           'available_quote': str(available),
                           'released_intents': sum(receipt != LEGACY_NEVER_STARTED for receipt in state['releases'].values()),
                           'unused_intent_release': 'native_terminal_reconciliation'}
            if required > available:
                raise PolicyError('Candidate funding allocation exhausted')
            state['claims'][executor_config.id] = claim
            state['fills'] = projection['fills']
            if self._write(raw, state):
                return True
        raise PolicyError('Funding checkpoint contention; no acquisition')
