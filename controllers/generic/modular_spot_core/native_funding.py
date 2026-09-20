"""Candidate admission only. Native fills, executors and PositionHold own accounting.

One recorder/strategy/controller/pair, at most 50 USDC for this opt-in candidate.
This does not reserve funds from independent processes sharing the wallet.
"""
import json
import re
from decimal import Decimal

from pydantic import Field
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator, PositionHold

from .config import PolicyModel
from .domain import PolicyError, decimal, identity

ZERO = Decimal('0')


class CandidateFundingPolicy(PolicyModel):
    allocation_quote: Decimal = Field(gt=0, le=50)
    fee_reserve_fraction: Decimal = Field(gt=0, le=Decimal('.1'))


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

    Unused canceled/rejected intent remains reserved until explicit reconciliation.
    Neither timeouts nor terminal flags release uncertain late-fill capacity.
    """

    def __init__(self, owner, recorder):
        self.owner, self.recorder = owner, recorder
        self.policy = owner.config.funding
        self.key = 'modular-candidate-funding:v1:' + identity([recorder.config_file_path])
        self.binding = identity([owner.config.id, owner.config.connector_name, owner.config.trading_pair,
                                 owner._composition_digest, owner._native_source_hash])
        self.status = {}

    def _load(self):
        with self.recorder.sql_manager.get_new_session() as session:
            row = session.get(Metadata, self.key)
            raw = None if row is None else row.value
            rows = session.query(Metadata).filter(Metadata.key.like(self.key + ':%')).all()
        header = {'schema': 1, 'binding': self.binding, 'revision': 0,
                  'audit_sha256': identity([[], []])} if raw is None else json.loads(raw)
        if (not isinstance(header, dict) or set(header) != {'schema', 'binding', 'revision', 'audit_sha256'}
                or type(header['schema']) is not int or header['schema'] != 1 or header['binding'] != self.binding
                or type(header['revision']) is not int or header['revision'] < 0):
            raise PolicyError('Funding checkpoint identity/schema changed')
        if raw is None and rows:
            raise PolicyError('Funding checkpoint missing with existing admissions')
        claims, fills = {}, {}
        for row in rows:
            item = json.loads(row.value)
            if not isinstance(item, dict) or item.get('binding') != self.binding:
                raise PolicyError('Funding audit record identity changed')
            kind = row.key[len(self.key) + 1:].split(':', 1)[0]
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
        if header['audit_sha256'] != identity([sorted(claims.items()), sorted(fills.items())]):
            raise PolicyError('Funding admission or fill audit records disappeared or changed')
        return raw, {**header, 'claims': claims, 'fills': fills}

    def _write(self, raw, state):
        header = {'schema': 1, 'binding': self.binding, 'revision': state['revision'] + 1,
                  'audit_sha256': identity([sorted(state['claims'].items()), sorted(state['fills'].items())])}
        encoded = json.dumps(header, sort_keys=True, separators=(',', ':'), allow_nan=False)
        records = [(self.key + ':claim:' + identity(key),
                    {'binding': self.binding, 'executor_id': key, 'claim': claim})
                   for key, claim in state['claims'].items()]
        records.extend((self.key + ':fill:' + key, {'binding': self.binding, 'fill_key': key, 'digest': digest})
                       for key, digest in state['fills'].items())
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

    def _evidence(self, state, raw):
        config = self.owner.config
        saved = self.recorder.get_all_executors()
        current = list(self.owner.executors_info)
        if any(info.controller_id != config.id for info in [*saved, *current]):
            raise PolicyError('Candidate funding admits one native controller per recorder')
        infos = {}
        for collection in (saved, current):
            seen = set()
            for info in collection:
                if info.id in seen:
                    raise PolicyError('Duplicate native executor evidence')
                seen.add(info.id)
                previous = infos.get(info.id)
                if previous is not None and _config_identity(previous.config, config.id) != _config_identity(info.config, config.id):
                    raise PolicyError('Saved/current native executor configuration conflict')
                infos[info.id] = info
        fills = self.recorder.get_trades_for_config(self.recorder.config_file_path)
        connector = self.owner.market_data_provider.connectors[config.connector_name]
        orders = self.recorder.get_orders_for_config_and_market(self.recorder.config_file_path, connector)
        if raw is None and (infos or fills or orders or self.owner.positions_held):
            raise PolicyError('Native history exists without funding checkpoint; reconcile explicitly')
        owned = {}
        for info in infos.values():
            cfg = info.config
            if cfg.connector_name != config.connector_name or cfg.trading_pair != config.trading_pair:
                raise PolicyError('Foreign native executor scope in funding recorder')
            if cfg.side == TradeType.BUY:
                claim = state['claims'].get(info.id)
                if claim is None or claim['config'] != _config_identity(cfg, config.id):
                    raise PolicyError('Native BUY lacks immutable funding admission')
            for _, _, order_id in ExecutorOrchestrator._owned_order_keys(info):
                if order_id in owned and owned[order_id] != info.id:
                    raise PolicyError('Native order has ambiguous funding owner')
                owned[order_id] = info.id
        if any(order.id not in owned for order in orders):
            raise PolicyError('Native order lacks executor ownership')
        if any(key not in infos for key in state['claims']):
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
        projection = funding_projection(fills, config.connector_name, config.trading_pair)
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
            pending += (authorized - quantity) * decimal(claim['price'], 'funded price') * (1 + self.policy.fee_reserve_fraction)
        return projection, pending, infos

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
            self.status = {'allocation_quote': str(self.policy.allocation_quote),
                           'native_cash_used': str(projection['cash_used']),
                           'native_inventory_cost': str(projection['inventory_cost']),
                           'pending_quote': str(pending), 'new_quote': str(required),
                           'available_quote': str(max(ZERO, self.policy.allocation_quote - projection['used'] - pending)),
                           'unused_intent_release': 'explicit_reconciliation_required'}
            if projection['used'] + pending + required > self.policy.allocation_quote:
                raise PolicyError('Candidate funding allocation exhausted')
            state['claims'][executor_config.id] = claim
            state['fills'] = projection['fills']
            if self._write(raw, state):
                return True
        raise PolicyError('Funding checkpoint contention; no acquisition')
