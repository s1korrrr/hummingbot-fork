"""Explicit, quiescent funding upgrades; never infer a release or reset accounting.

The caller must separately own maintenance authority. Automatic admission cannot
call this path. Plan/apply bind the exact old audit, native evidence and target
configuration. All reads and writes for apply share one SQLite write transaction.
"""
import copy
import json
from contextlib import contextmanager
from types import SimpleNamespace

from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.model.market_state import MarketState
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator

from .domain import PolicyError, identity

NATIVE_TABLES = ('Executors', 'Order', 'OrderStatus', 'OrderExact', 'TradeFill',
                 'TradeFillExact', 'Position', 'PositionHoldLedger', 'MarketState',
                 'PlannedPositionResume')


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _view(store, session):
    @contextmanager
    def same_session():
        yield session
    result = copy.copy(store)
    result.recorder = copy.copy(store.recorder)
    result.recorder._sql_manager = SimpleNamespace(get_new_session=same_session)
    return result


def _native_digest(session):
    connection = session.connection()
    present = {r[0] for r in connection.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'")}
    records = {}
    for table in NATIVE_TABLES:
        if table in present:
            records[table] = sorted(_json(list(row)) for row in connection.exec_driver_sql(f'SELECT * FROM "{table}"'))
    records['submitted'] = sorted((row.key, row.value) for row in session.query(Metadata).all()
                                  if row.key.startswith(('v2-submitted-order:', 'v2-supervised-orders:')))
    return identity(records)


def _check_persisted_submissions(store, session, infos, orders, connector):
    """A pre-submit durable snapshot can outlive a crash before Order creation."""
    owned = {key[2] for info in infos for key in ExecutorOrchestrator._owned_order_keys(info)}
    recorded = {order.id: order for order in orders}
    native = {}
    for info in infos:
        custom = info.custom_info or {}
        for raw in [*(custom.get('execution_order_snapshots') or []), *(custom.get('held_position_orders') or [])]:
            order = InFlightOrder.from_json(raw)
            previous = native.get(order.client_order_id)

            def order_identity(item):
                return (item.trading_pair, item.trade_type, item.order_type, item.amount,
                        None if item.order_type == OrderType.MARKET else item.price, item.exchange_order_id)
            if previous is not None:
                if (order_identity(previous) != order_identity(order)
                        or previous.is_done and order.is_done and previous.current_state != order.current_state):
                    raise PolicyError('Funding transition native order snapshots contradict')
                if any(previous.order_fills[key].to_json() != order.order_fills[key].to_json()
                       for key in previous.order_fills.keys() & order.order_fills.keys()):
                    raise PolicyError('Funding transition native fill snapshots contradict')
                if previous.executed_amount_base > order.executed_amount_base or previous.is_done and not order.is_done:
                    continue
            native[order.client_order_id] = order
    snapshots = []
    for row in session.query(Metadata).all():
        if row.key.startswith(('v2-submitted-order:', 'v2-supervised-orders:')):
            value = json.loads(row.value)
            if value.get('owner') != store.recorder.config_file_path:
                raise PolicyError('Funding transition found foreign durable submission')
            if row.key.startswith('v2-submitted-order:'):
                snapshots.append(value['order'])
                if row.key != 'v2-submitted-order:' + value['order']['client_order_id']:
                    raise PolicyError('Funding transition durable submission identity differs')
            else:
                snapshots.extend(value['orders'])
    for row in session.query(MarketState).all():
        if row.config_file_path != store.recorder.config_file_path or row.market != connector.display_name:
            raise PolicyError('Funding transition found foreign native tracker')
        tracker = row.saved_state
        if store.owner.config.execution_mode == 'paper':
            if tracker.get('schema') != 'native_spot_paper_v1':
                raise PolicyError('Funding transition paper tracker schema differs')
            tracker = tracker['orders']
        snapshots.extend(tracker.values())
    fills = store.recorder.get_trades_for_config(store.recorder.config_file_path)
    for raw in snapshots:
        order = InFlightOrder.from_json(raw)
        saved = recorded.get(order.client_order_id)
        if order.client_order_id not in owned or saved is None:
            raise PolicyError('Funding transition has unresolved durable submission without native ownership')
        if (saved.symbol != order.trading_pair
                or saved.order_type != order.order_type.name
                or order.exchange_order_id not in (None, saved.exchange_order_id)):
            raise PolicyError('Funding transition durable submission identity differs')
        terminal = native.get(order.client_order_id)
        if (terminal is None or not terminal.is_done or terminal.trading_pair != order.trading_pair
                or terminal.trade_type != order.trade_type or terminal.order_type != order.order_type
                or terminal.amount != order.amount
                or (order.order_type != OrderType.MARKET and terminal.price != order.price)
                or terminal.exchange_order_id != saved.exchange_order_id):
            raise PolicyError('Funding transition durable submission lacks matching terminal native snapshot')
        recorded_fills = {str(fill.exchange_trade_id): fill for fill in fills if fill.order_id == order.client_order_id}
        if (terminal.executed_amount_base != sum((fill.amount_decimal for fill in recorded_fills.values()), 0)
                or {str(key) for key in terminal.order_fills} != set(recorded_fills)
                or order.executed_amount_base > terminal.executed_amount_base
                or any(str(key) not in recorded_fills for key in order.order_fills)):
            raise PolicyError('Funding transition durable submission has unrecorded fills')
        for trade_id, fill in recorded_fills.items():
            record = ExecutorOrchestrator._trade_fill_order_record(fill)
            trade = terminal.order_fills[trade_id]
            if (record['executed_amount_base'] != trade.fill_base_amount
                    or record['price'] != trade.fill_price or fill.trade_fee != trade.fee.to_json()):
                raise PolicyError('Funding transition terminal fill or fee differs from native recorder')


def _legacy_evidence(view, session, evidence, infos, orders):
    """Attest one explicitly selected old fully filled repair while retaining its claim."""
    if (not isinstance(evidence, dict) or set(evidence) != {'executor_id', 'order_ids', 'preflight'}
            or not isinstance(evidence['executor_id'], str) or not evidence['executor_id']
            or not isinstance(evidence['order_ids'], list) or len(evidence['order_ids']) != 2
            or len(set(evidence['order_ids'])) != 2
            or not isinstance(evidence['preflight'], dict)):
        raise PolicyError('Funding legacy transition requires one exact reviewed executor and preflight')
    executor_id = evidence['executor_id']
    info = next((item for item in infos if item.id == executor_id), None)
    if info is None or info.is_active or info.is_trading:
        raise PolicyError('Funding legacy transition executor is not quiescent')
    row = session.connection().exec_driver_sql('SELECT * FROM Executors WHERE id=?', (executor_id,)).mappings().first()
    if row is None:
        raise PolicyError('Funding legacy transition executor row is missing')
    from hummingbot.strategy_v2.executors.planned_position_resume import (
        decode_row,
        validate_legacy_post_only_repair_transition,
    )
    from hummingbot.strategy_v2.models.executors import CloseType
    preflight, stopped = decode_row(evidence['preflight']), decode_row(dict(row))
    if (preflight.get('id') != executor_id or preflight.get('status') != 2
            or not preflight.get('is_active') or preflight.get('close_type') is not None
            or stopped.get('status') != 4 or stopped.get('is_active')
            or stopped.get('close_type') != CloseType.POSITION_HOLD.value):
        raise PolicyError('Funding legacy transition requires exact running-to-held evidence')
    transition = validate_legacy_post_only_repair_transition(preflight, stopped)
    chain = transition['acquisition_chain']
    child_ids = [child['order_id'] for child in chain['children']]
    if child_ids != evidence['order_ids']:
        raise PolicyError('Funding legacy transition reviewed child ids differ')
    receipt = (info.custom_info or {}).get('entry_reconciliation')
    if (not isinstance(receipt, dict) or receipt.get('source') != 'native_terminal_entry_set_v1'
            or receipt.get('schema') != 1 or len(receipt.get('orders', [])) != 2
            or [proof.get('order_id') for proof in receipt['orders']] != child_ids):
        raise PolicyError('Funding legacy transition terminal child proof set differs')
    fills = view.recorder.get_trades_for_config(view.recorder.config_file_path)
    for proof in receipt['orders']:
        quantity = sum((fill.amount_decimal for fill in fills if fill.order_id == proof['order_id']), 0)
        view._validate_order_release(info, proof, quantity, fills, orders)
    return {'executor_id': executor_id, 'order_ids': child_ids,
            'preflight_sha256': identity(preflight), 'stopped_sha256': identity(stopped),
            'original_config_sha256': identity(stopped['config']),
            'resume_config_sha256': identity(transition['resume_config']),
            'acquisition_chain_sha256': identity(chain), 'terminal_proofs_sha256': identity(receipt)}


def _proposal(store, session, expected_binding, legacy_evidence=None):
    if not isinstance(expected_binding, str) or expected_binding == store.binding:
        raise PolicyError('Funding transition requires the exact previous binding')
    from .native_funding import NativeFundingStore
    if (any(not owner._composition_unchanged() for owner in store.owners.values())
            or NativeFundingStore(store.owner, store.recorder).binding != store.binding):
        raise PolicyError('Funding transition target configuration or identity changed')
    view = _view(store, session)
    raw, state = view._load(expected_binding=expected_binding)
    if raw is None:
        raise PolicyError('Funding transition requires an existing checkpoint')
    infos = view.recorder.get_all_executors()
    current = [info for owner in store.owners.values() for info in owner.executors_info]
    connector = store.owner.market_data_provider.connectors[store.owner.config.connector_name]
    if any(info.is_active or info.is_trading for info in [*infos, *current]):
        raise PolicyError('Funding transition requires quiescent native executors')
    if any(not order.is_done for order in connector._order_tracker.all_orders.values()):
        raise PolicyError('Funding transition refuses active native orders')
    orders = view.recorder.get_orders_for_config_and_market(view.recorder.config_file_path, connector)
    if any(order.last_status not in {'BuyOrderCompleted', 'SellOrderCompleted', 'OrderCancelled', 'OrderFailure'}
           for order in orders):
        raise PolicyError('Funding transition requires terminal native order records')
    _check_persisted_submissions(view, session, infos, orders, connector)
    legacy = (_legacy_evidence(view, session, legacy_evidence, infos, orders)
              if legacy_evidence is not None else None)
    projection, pending, _ = view._evidence(
        state, raw, reconcile=legacy is None, allow_quiescent_legacy=legacy is not None)
    if legacy is not None:
        unresolved = [info.id for info in infos if info.config.side.name == 'BUY'
                      and isinstance((info.custom_info or {}).get('entry_reconciliation'), dict)
                      and (info.custom_info or {})['entry_reconciliation'].get('source') == 'native_terminal_entry_set_v1'
                      and len((info.custom_info or {})['entry_reconciliation'].get('orders', [])) > 1
                      and info.id not in state['releases']]
        if unresolved != [legacy['executor_id']]:
            raise PolicyError('Funding legacy transition has other unresolved acquisition chains')
    if pending != projection['legacy_uncertain_quote']:
        raise PolicyError('Funding transition retains unresolved commitments; reconcile terminal evidence first')
    if (projection['used'] + pending > store.policy.allocation_quote
            or any(value + projection['pending_by_pair'][pair] > store.policy.max_position_quote
                   for pair, value in projection['used_by_pair'].items())):
        raise PolicyError('Funding transition target cannot contain existing native exposure')
    state['fills'] = projection['fills']
    before = {row.key: row.value for row in session.query(Metadata).all()
              if row.key == store.key or row.key.startswith(store.key + ':')}
    proposal = {'schema': 1, 'from_binding': expected_binding, 'to_binding': store.binding,
                'previous_revision': state['revision'], 'before': before,
                'native_sha256': _native_digest(session), 'state': state,
                'inventory_cost': str(projection['inventory_cost']), 'cash_used': str(projection['cash_used']),
                'pending_quote': str(pending), 'legacy_uncertain_quote': str(projection['legacy_uncertain_quote']),
                'available_quote': str(store.policy.allocation_quote - projection['used'] - pending)}
    if legacy is not None:
        proposal['legacy_retained_unreleased'] = legacy
    proposal['transition_id'] = identity(proposal)
    return proposal


def plan_transition(store, expected_binding, *, legacy_evidence=None):
    with store.recorder.sql_manager.get_new_session() as session:
        session.connection().exec_driver_sql('BEGIN')
        proposal = _proposal(store, session, expected_binding, legacy_evidence)
    plans = getattr(store, '_binding_transition_plans', {})
    plans[proposal['transition_id']] = (expected_binding, legacy_evidence)
    store._binding_transition_plans = plans
    return proposal


def apply_transition(store, transition_id):
    receipt_key = 'modular-funding-transition:v1:' + identity([store.key, transition_id])
    with store.recorder.sql_manager.get_new_session() as session, session.begin():
        session.connection().exec_driver_sql('BEGIN IMMEDIATE')
        previous = session.get(Metadata, receipt_key)
        if previous is not None:
            receipt = json.loads(previous.value)
            header = session.get(Metadata, store.key)
            if (identity({k: v for k, v in receipt.items() if k != 'transition_id'}) != transition_id
                    or receipt.get('transition_id') != transition_id or receipt.get('to_binding') != store.binding
                    or header is None or json.loads(header.value).get('binding') != store.binding):
                raise PolicyError('Funding transition receipt conflicts with current identity')
            return receipt
        planned = getattr(store, '_binding_transition_plans', {}).get(transition_id)
        if planned is None:
            raise PolicyError('Funding transition plan unavailable; prepare and review again')
        expected, legacy_evidence = planned if isinstance(planned, tuple) else (planned, None)
        proposal = _proposal(store, session, expected, legacy_evidence)
        if proposal['transition_id'] != transition_id:
            raise PolicyError('Funding transition plan is stale; native evidence or audit changed')
        state = proposal['state']
        # Archive the exact prior rows before rebinding their identity. Claim,
        # fill and release contents remain intact; native tables are never edited.
        for key, raw in proposal['before'].items():
            if key == store.key:
                continue
            value = json.loads(raw)
            value['binding'] = store.binding
            session.get(Metadata, key).value = _json(value)
        for key, digest in state['fills'].items():
            row_key = store.key + ':fill:' + key
            value = _json({'binding': store.binding, 'fill_key': key, 'digest': digest})
            row = session.get(Metadata, row_key)
            if row is None:
                session.add(Metadata(key=row_key, value=value))
            elif row.value != value:
                raise PolicyError('Funding transition changed an immutable fill')
        for key, receipt in state['releases'].items():
            row_key = store.key + ':release:' + identity(key)
            value = _json({'binding': store.binding, 'executor_id': key, 'receipt': receipt})
            row = session.get(Metadata, row_key)
            if row is None:
                session.add(Metadata(key=row_key, value=value))
            elif row.value != value:
                raise PolicyError('Funding transition changed an immutable release')
        header = {'schema': 1, 'binding': store.binding, 'revision': state['revision'] + 1,
                  'audit_sha256': store._audit_hash(state)}
        session.get(Metadata, store.key).value = _json(header)
        session.add(Metadata(key=receipt_key, value=_json(proposal)))
        return proposal
