"""External sell-only admission. Native receipts only; no wallet adoption or second OMS."""
import json
import re
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from hummingbot.core.data_type.common import PositionAction, TradeType
from hummingbot.strategy_v2.executors.order_accounting import base_fee_amount
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus

from .config import TrailingPolicy
from .domain import PolicyError, decimal, identity
from .inventory import inventory_view
from .trailing import protected_sell_price

OWNER = re.compile(r'^modular_spot_[A-Za-z0-9_-]+$')
PAIR = re.compile(r'^[A-Z0-9]+-[A-Z0-9]+$')


@dataclass(frozen=True)
class InventoryHandoff:
    account: str
    connector: str
    trading_pair: str
    prior_owner: str
    new_owner: str
    available_base: Decimal
    reserved_base: Decimal
    entry_gross_base: Decimal
    entry_gross_quote: Decimal
    fees_quote: Decimal
    fees_base: Decimal
    reconciliation_id: str
    policy_revision: str
    authority: str


@dataclass(frozen=True)
class ManagementScope:
    lots: tuple


@dataclass(frozen=True)
class ExitObservation:
    trading_pair: str
    bid: Decimal
    ask: Decimal
    sell_signal: bool
    amount_step: Decimal
    minimum_amount: Decimal
    minimum_notional: Decimal
    price_step: Decimal
    sell_fee_rate: Decimal
    sell_fee_base: Decimal
    trailing: TrailingPolicy
    timestamp: float


@dataclass(frozen=True)
class Reduction:
    action: str
    reason: str
    amount: Decimal | None = None
    price: Decimal | None = None
    min_sell_price: Decimal | None = None
    identifier: str | None = None
    config: object | None = None
    remaining_base: Decimal | None = None
    sold_base: Decimal | None = None
    entry_quote: Decimal | None = None


def _items(handoff):
    if isinstance(handoff, InventoryHandoff):
        return (handoff,)
    if isinstance(handoff, (list, tuple)) and handoff and all(isinstance(x, InventoryHandoff) for x in handoff):
        return tuple(handoff)
    raise PolicyError('invalid inventory handoff')


def _executors(native_reconciliation):
    if native_reconciliation is None:
        raise PolicyError('native reconciliation receipt required')
    rows = tuple(native_reconciliation) if not isinstance(native_reconciliation, (str, bytes)) else ()
    if not rows:
        raise PolicyError('native reconciliation receipt required')
    executors = []
    for item in rows:
        if hasattr(item, 'amount') and not hasattr(item, 'config'):
            if decimal(getattr(item, 'amount', 0), 'wallet amount') > 0:
                raise PolicyError('wallet balance is not native reconciliation')
            continue
        if not hasattr(item, 'config') or not hasattr(item, 'custom_info'):
            raise PolicyError('native reconciliation receipt required')
        executors.append(item)
    if not executors:
        raise PolicyError('native reconciliation receipt required')
    return tuple(executors)


def _handoff_ok(item: InventoryHandoff):
    if not isinstance(item.account, str) or not item.account or item.account == 'unassigned':
        raise PolicyError('account identity required')
    if not isinstance(item.connector, str) or not item.connector:
        raise PolicyError('connector identity required')
    if not isinstance(item.trading_pair, str) or PAIR.fullmatch(item.trading_pair) is None:
        raise PolicyError('invalid instrument identity')
    if not isinstance(item.prior_owner, str) or not item.prior_owner:
        raise PolicyError('prior owner identity required')
    if not isinstance(item.new_owner, str) or OWNER.fullmatch(item.new_owner) is None:
        raise PolicyError('new owner must be a modular native controller')
    if item.prior_owner == item.new_owner:
        raise PolicyError('handoff requires a distinct prior owner')
    if not isinstance(item.reconciliation_id, str) or not item.reconciliation_id:
        raise PolicyError('reconciliation receipt identity required')
    if not isinstance(item.policy_revision, str) or not item.policy_revision:
        raise PolicyError('policy revision required')
    if not isinstance(item.authority, str) or len(item.authority) < 8:
        raise PolicyError('explicit authority evidence required')
    if item.authority in {'wallet', 'balance', 'mark_price', 'current_price', 'config'}:
        raise PolicyError('configuration or mark price cannot supply acquisition cost')
    available = decimal(item.available_base, 'available base', positive=True)
    reserved = decimal(item.reserved_base, 'reserved base')
    try:
        entry_base = decimal(item.entry_gross_base, 'entry base', positive=True)
        entry_quote = decimal(item.entry_gross_quote, 'entry quote', positive=True)
    except PolicyError as exc:
        raise PolicyError('missing or unknown acquisition cost') from exc
    decimal(item.fees_quote, 'entry quote fees')
    decimal(item.fees_base, 'entry base fees')
    if reserved > available:
        raise PolicyError('reserved base exceeds assigned inventory')
    if entry_quote <= 0:
        raise PolicyError('missing or unknown acquisition cost')
    if available > entry_base:
        raise PolicyError('available base exceeds native entry quantity')
    return item


def _active(info):
    done = info.is_done if type(info.is_done) is bool else False
    active = info.is_active if type(getattr(info, 'is_active', False)) is bool else not done
    return (not done) or active


def _same_instrument(info, connector, pair):
    config = info.config
    return getattr(config, 'connector_name', None) == connector and getattr(config, 'trading_pair', None) == pair


def _foreign_lot(infos, item: InventoryHandoff):
    owners = {item.prior_owner, item.new_owner}
    for info in infos:
        if not _same_instrument(info, item.connector, item.trading_pair):
            continue
        if info.controller_id in owners:
            if info.controller_id == item.new_owner and _active(info):
                raise PolicyError('duplicated or conflicting handoff')
            continue
        if _active(info):
            raise PolicyError('another owner already manages this lot')


def _native_fee_evidence(info):
    """Validate complete native fill snapshots; only quote-denominated fees are admitted."""
    snapshots = info.custom_info.get('execution_order_snapshots')
    if not isinstance(snapshots, list) or not snapshots:
        raise PolicyError('native fee reconciliation snapshots unavailable')
    base, quote = info.config.trading_pair.split('-')
    total = Decimal('0')
    entry_base = entry_quote = Decimal('0')
    for order in snapshots:
        fills = order.get('order_fills')
        if not isinstance(fills, dict):
            raise PolicyError('native fee reconciliation fills unavailable')
        filled_base = filled_quote = Decimal('0')
        for fill in fills.values():
            amount = decimal(fill.get('fill_base_amount'), 'native fill base')
            value = decimal(fill.get('fill_quote_amount'), 'native fill quote')
            filled_base += amount
            filled_quote += value
            fee = fill.get('fee')
            if not isinstance(fee, dict):
                raise PolicyError('native fee reconciliation unavailable')
            if fee.get('fee_type') not in {'AddedToCost', 'DeductedFromReturns'}:
                raise PolicyError('unsupported native fee type')
            rate = decimal(fee.get('percent'), 'native fee percent')
            token = fee.get('percent_token')
            if token is None:
                token = base if (fee.get('fee_type') == 'DeductedFromReturns'
                                 and order.get('trade_type') == 'BUY') else quote
            if rate and token != quote:
                raise PolicyError('unsupported native non-quote fee')
            total += rate * value
            for item in fee.get('flat_fees', ()):
                amount = decimal(item.get('amount'), 'native flat fee')
                if amount and item.get('token') != quote:
                    raise PolicyError('unsupported native non-quote fee')
                total += amount
        if (filled_base != decimal(order.get('executed_amount_base'), 'native executed base')
                or filled_quote != decimal(order.get('executed_amount_quote'), 'native executed quote')):
            raise PolicyError('native fill and fee reconciliation incomplete')
        if order.get('trade_type') == 'BUY':
            entry_base += filled_base
            entry_quote += filled_quote
    receipt = info.custom_info['execution_receipt']
    if (entry_base != decimal(receipt.get('entry_gross_base'), 'native entry base')
            or entry_quote != decimal(receipt.get('entry_gross_quote'), 'native entry quote')):
        raise PolicyError('native reconciliation changed; entry snapshots differ')
    reported = decimal(receipt.get('fees_quote'), 'native quote fees')
    if total != reported:
        raise PolicyError('native fee reconciliation differs from receipt')
    return total


def _view_for(infos, item: InventoryHandoff):
    matched = [info for info in infos if info.controller_id == item.prior_owner
               and _same_instrument(info, item.connector, item.trading_pair)]
    if not matched:
        raise PolicyError('missing native reconciliation receipt')
    views = inventory_view(matched, [], controller_id=item.prior_owner,
                           connector=item.connector, pair=item.trading_pair)
    view = views.get(item.reconciliation_id)
    if view is None:
        raise PolicyError('missing native reconciliation receipt')
    if not view.terminal:
        raise PolicyError('prior owner still manages this lot')
    if view.remaining_base <= 0:
        raise PolicyError('no remaining assigned inventory')
    if view.entry_quote <= 0 or view.entry_price <= 0:
        raise PolicyError('missing or unknown acquisition cost')
    receipt = None
    for info in matched:
        if info.id == item.reconciliation_id:
            receipt = info.custom_info.get('execution_receipt')
            break
    if not isinstance(receipt, dict):
        raise PolicyError('native execution receipt unavailable')
    native_fees = _native_fee_evidence(info)
    if native_fees != item.fees_quote or item.fees_base != 0:
        raise PolicyError('handoff fee evidence conflicts with native reconciliation')
    native_entry = decimal(receipt.get('entry_gross_base'), 'native entry base', positive=True)
    if (view.remaining_base != decimal(item.available_base, 'available base', positive=True)
            or view.entry_quote != decimal(item.entry_gross_quote, 'entry quote', positive=True)
            or native_entry != decimal(item.entry_gross_base, 'entry base', positive=True)):
        raise PolicyError('handoff conflicts with native reconciliation')
    return view


def admit_inventory(handoff, native_reconciliation):
    items = tuple(_handoff_ok(item) for item in _items(handoff))
    infos = _executors(native_reconciliation)
    seen_ids, seen_pairs = set(), set()
    lots = []
    for item in items:
        if item.reconciliation_id in seen_ids or item.trading_pair in seen_pairs:
            raise PolicyError('duplicated or conflicting handoff')
        seen_ids.add(item.reconciliation_id)
        seen_pairs.add(item.trading_pair)
        _foreign_lot(infos, item)
        _view_for(infos, item)
        lots.append(item)
    return ManagementScope(tuple(lots))


def reduction_config(*, identifier, timestamp, controller_id, connector, pair, amount, price, min_sell_price):
    return OrderExecutorConfig(
        id=identifier, timestamp=float(timestamp), controller_id=controller_id,
        connector_name=connector, trading_pair=pair, side=TradeType.SELL, amount=amount,
        price=price, position_action=PositionAction.CLOSE, execution_strategy=ExecutionStrategy.LIMIT,
        leverage=1, level_id=f'modular_spot:v1:sell_only:{identifier}', min_sell_price=min_sell_price)


def _side_name(config):
    side = getattr(config, 'side', None)
    return side.name if hasattr(side, 'name') else side


def _consumed_sell(info, base):
    custom = info.custom_info or {}
    executed = decimal(custom.get('executed_amount_base', 0), 'sold base')
    extra = Decimal('0')
    for payload in custom.get('held_position_orders') or ():
        extra += base_fee_amount(payload, base)
    return executed + extra


def _sell_progress(infos, lot: InventoryHandoff):
    base = lot.trading_pair.split('-')[0]
    sold, outstanding = Decimal('0'), Decimal('0')
    for info in infos:
        if not _same_instrument(info, lot.connector, lot.trading_pair):
            continue
        if info.controller_id not in {lot.new_owner, lot.prior_owner}:
            if _active(info) and _side_name(info.config) == 'SELL':
                return None, None, 'another owner already manages this lot'
            continue
        if _side_name(info.config) != 'SELL':
            if _active(info) and info.controller_id == lot.new_owner:
                return None, None, 'existing_native_owner_manages_exits'
            continue
        consumed = _consumed_sell(info, base)
        sold += consumed
        amount = decimal(info.config.amount, 'sell amount', positive=True)
        if _active(info):
            outstanding += max(amount - consumed, Decimal('0'))
    return sold, outstanding, None


def _owned_base(infos, lot: InventoryHandoff):
    owned = decimal(lot.available_base, 'available base', positive=True)
    cost = decimal(lot.entry_gross_quote, 'entry quote', positive=True)
    matched = [info for info in infos if info.controller_id == lot.prior_owner
               and _same_instrument(info, lot.connector, lot.trading_pair) and info.id == lot.reconciliation_id]
    if len(matched) != 1:
        raise PolicyError('current native reconciliation receipt unavailable')
    views = inventory_view(matched, [], controller_id=lot.prior_owner,
                           connector=lot.connector, pair=lot.trading_pair)
    view = views[lot.reconciliation_id]
    fees = _native_fee_evidence(matched[0])
    if (not view.terminal or view.entry_quote < cost or fees < lot.fees_quote
            or decimal(matched[0].custom_info['execution_receipt']['entry_gross_base'],
                       'native entry base') != lot.entry_gross_base):
        raise PolicyError('native reconciliation changed; renewed handoff required')
    owned = min(owned, view.remaining_base)
    return owned, max(cost, view.entry_quote) + max(fees, lot.fees_quote)


def _hold(reason, *, remaining=None, sold=None, cost=None):
    return Reduction('hold', reason, remaining_base=remaining, sold_base=sold, entry_quote=cost)


def propose_reduction(scope, exit_observation, current_native_evidence):
    if not isinstance(scope, ManagementScope) or not scope.lots:
        return _hold('no_admitted_inventory')
    obs = exit_observation
    if type(obs.sell_signal) is not bool:
        raise PolicyError('invalid exit observation')
    pair = obs.trading_pair
    lots = tuple(lot for lot in scope.lots if lot.trading_pair == pair)
    if not lots:
        return _hold('no_admitted_inventory')
    if len(lots) != 1:
        raise PolicyError('duplicated or conflicting handoff')
    lot = lots[0]
    infos = tuple(current_native_evidence or ())
    bid = decimal(obs.bid, 'bid', positive=True)
    ask = decimal(obs.ask, 'ask', positive=True)
    step = decimal(obs.amount_step, 'amount step', positive=True)
    minimum = decimal(obs.minimum_amount, 'minimum amount', positive=True)
    notional = decimal(obs.minimum_notional, 'minimum notional')
    tick = decimal(obs.price_step, 'price increment', positive=True)
    rate = decimal(obs.sell_fee_rate, 'sell fee')
    fee_base = decimal(obs.sell_fee_base, 'sell base fee')
    if ask < bid or rate < 0 or rate >= 1 or fee_base < 0 or fee_base >= 1:
        return _hold('unsupported_fees_or_book')
    owned, cost = _owned_base(infos, lot)
    sold, outstanding, blocked = _sell_progress(infos, lot)
    if blocked:
        return _hold(blocked, remaining=owned, sold=Decimal('0'), cost=cost)
    remaining = owned - sold
    if remaining < 0:
        return _hold('quantity_not_conserved', remaining=remaining, sold=sold, cost=cost)
    reserved = decimal(lot.reserved_base, 'reserved base')
    committed = reserved + outstanding
    if not obs.sell_signal:
        return _hold('no_sell_signal', remaining=remaining, sold=sold, cost=cost)
    if outstanding > 0:
        return _hold('outstanding_sell_commitment', remaining=remaining, sold=sold, cost=cost)
    free = remaining - committed
    if free <= 0:
        return _hold('existing_reservation_occupies_base' if reserved > 0 else 'outstanding_sell_commitment',
                     remaining=remaining, sold=sold, cost=cost)
    cap = (free / (1 + fee_base) / step).to_integral_value(rounding=ROUND_FLOOR) * step
    if cap <= 0 or cap < minimum or cap * bid < notional:
        return _hold('dust_below_exchange_minimum', remaining=remaining, sold=sold, cost=cost)
    entry_base = decimal(lot.entry_gross_base, 'entry base', positive=True)
    native_floor = (cost / entry_base) / (1 - rate)
    try:
        floor = protected_sell_price(native_floor, obs.trailing, tick)
    except PolicyError:
        return _hold('unknown_cost_or_fee_floor', remaining=remaining, sold=sold, cost=cost)
    if bid < floor:
        return _hold('gap_below_profit_floor', remaining=remaining, sold=sold, cost=cost)
    identifier = identity([lot.reconciliation_id, lot.new_owner, str(sold), str(cap), str(floor)])
    config = reduction_config(
        identifier=identifier, timestamp=obs.timestamp, controller_id=lot.new_owner,
        connector=lot.connector, pair=lot.trading_pair, amount=cap, price=bid, min_sell_price=floor)
    return Reduction('sell', 'bounded_native_limit_sell', amount=cap, price=bid, min_sell_price=floor,
                     identifier=identifier, config=config, remaining_base=remaining, sold_base=sold,
                     entry_quote=cost)


class SellOnlyStore:
    def __init__(self, recorder, controller_id: str, connector: str, pair: str, *, metadata_model=None):
        if metadata_model is None:
            from hummingbot.model.metadata import Metadata
            metadata_model = Metadata
        self.model, self.recorder = metadata_model, recorder
        self.key = 'modular-spot-sell-only:v1:' + identity([recorder.config_file_path, controller_id, connector, pair])

    def load(self):
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                row = session.get(self.model, self.key)
                raw = None if row is None else row.value
            return (None, None) if raw is None else (self._decode(raw), raw)
        except SQLAlchemyError as exc:
            raise PolicyError('sell-only store unavailable') from exc

    def replace(self, lots: tuple[InventoryHandoff, ...]) -> bool:
        payload = self._encode(lots)
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                with session.begin():
                    row = session.get(self.model, self.key)
                    if row is None:
                        session.add(self.model(key=self.key, value=payload))
                        session.flush()
                        return True
                    changed = session.execute(update(self.model).where(
                        self.model.key == self.key).values(value=payload))
                    return changed.rowcount == 1
        except IntegrityError:
            return False
        except SQLAlchemyError as exc:
            raise PolicyError('sell-only persistence failed') from exc

    def _encode(self, lots):
        rows = []
        for lot in lots:
            _handoff_ok(lot)
            rows.append({
                'account': lot.account, 'connector': lot.connector, 'trading_pair': lot.trading_pair,
                'prior_owner': lot.prior_owner, 'new_owner': lot.new_owner,
                'available_base': str(lot.available_base), 'reserved_base': str(lot.reserved_base),
                'entry_gross_base': str(lot.entry_gross_base), 'entry_gross_quote': str(lot.entry_gross_quote),
                'fees_quote': str(lot.fees_quote), 'fees_base': str(lot.fees_base),
                'reconciliation_id': lot.reconciliation_id, 'policy_revision': lot.policy_revision,
                'authority': lot.authority,
            })
        return json.dumps({'schema_version': 1, 'lots': rows}, sort_keys=True, separators=(',', ':'), allow_nan=False)

    def _decode(self, raw):
        data = json.loads(raw)
        if not isinstance(data, dict) or data.get('schema_version') != 1 or not isinstance(data.get('lots'), list):
            raise PolicyError('invalid sell-only checkpoint')
        return ManagementScope(tuple(InventoryHandoff(
            account=row['account'], connector=row['connector'], trading_pair=row['trading_pair'],
            prior_owner=row['prior_owner'], new_owner=row['new_owner'],
            available_base=Decimal(row['available_base']), reserved_base=Decimal(row['reserved_base']),
            entry_gross_base=Decimal(row['entry_gross_base']), entry_gross_quote=Decimal(row['entry_gross_quote']),
            fees_quote=Decimal(row['fees_quote']), fees_base=Decimal(row['fees_base']),
            reconciliation_id=row['reconciliation_id'], policy_revision=row['policy_revision'],
            authority=row['authority']) for row in data['lots']))


def supported_reduction_fee(fee, quote):
    """The native reservation can currently admit quote percentage fees only."""
    rate = decimal(fee.percent, 'sell fee')
    return (rate < 1 and (rate == 0 or fee.percent_token in (None, quote))
            and all(decimal(item.amount, 'flat sell fee') == 0 for item in fee.flat_fees))


def is_limit_reduce(config):
    return (getattr(config, 'type', None) == 'order_executor'
            and getattr(config, 'side', None) == TradeType.SELL
            and getattr(config, 'position_action', None) == PositionAction.CLOSE
            and getattr(config, 'execution_strategy', None) != ExecutionStrategy.MARKET
            and getattr(config, 'min_sell_price', None) is not None
            and getattr(config, 'leverage', None) == 1)


class ModularReduceExecutor(OrderExecutor):
    def __init__(self, strategy, config, update_interval=1.0, max_retries=10):
        if not is_limit_reduce(config):
            raise PolicyError('sell-only reduce path refuses buy, short, market, or unfloored sales')
        super().__init__(strategy, config, update_interval, max_retries=0)

    def control_order(self):
        # Native OrderExecutor checks retries after submitting; tighten before submission.
        self.evaluate_max_retries()
        if self.status != RunnableStatus.RUNNING:
            return
        if self._order is None:
            base, quote = self.config.trading_pair.split('-')
            fee = self.connectors[self.config.connector_name].get_fee(
                base_currency=base, quote_currency=quote, order_type=self.get_order_type(),
                order_side=TradeType.SELL, amount=self.config.amount, price=self.config.price, is_maker=False)
            if not supported_reduction_fee(fee, quote):
                self.logger().error('Modular sell-only held: unsupported current exit fee')
                self.early_stop()
                return
        super().control_order()
