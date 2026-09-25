"""Explicit stopped-database transfer of one filled position back to its executor.

This is an operator-assisted migration, never general crash recovery. A consumed
claim is not replayed. Evidence contains public order receipts, never credentials.
"""
import argparse
import hashlib
import json
import sqlite3
import time
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import yaml

from hummingbot.connector.exchange.okx.native_reconciliation import SPOT_ALGO_TYPES
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1, validate_acquisition_chain
from hummingbot.strategy_v2.executors.order_accounting import base_fee_amount
from hummingbot.strategy_v2.executors.position_executor.data_types import parse_position_config
from hummingbot.strategy_v2.executors.position_executor.position_executor import validate_planned_entry_reconciliation
from hummingbot.strategy_v2.models.executors import CloseType

TABLE = 'PlannedPositionResume'
ROOT = Path(__file__).resolve().parents[3]
SOURCES = (
    'hummingbot/strategy_v2/executors/acquisition_chain.py',
    'hummingbot/strategy_v2/executors/planned_position_resume.py',
    'hummingbot/strategy_v2/executors/position_executor/position_executor.py',
    'hummingbot/strategy_v2/executors/position_executor/data_types.py',
    'hummingbot/strategy_v2/executors/executor_orchestrator.py',
    'hummingbot/strategy_v2/executors/order_accounting.py',
    'hummingbot/strategy/strategy_v2_base.py',
    'scripts/v2_with_controllers.py',
    'hummingbot/connector/markets_recorder.py',
    'hummingbot/connector/exchange/okx/okx_exchange.py',
    'hummingbot/connector/exchange/okx/native_reconciliation.py',
    'hummingbot/strategy_v2/models/executors_info.py',
    'hummingbot/strategy_v2/controllers/native_policy_identity.py',
    'controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py',
    'controllers/directional_trading/ok_rsi_core/down_only_entry.py',
    'controllers/directional_trading/ok_rsi_core/execution.py',
)
STARTUP_SEALED_RESTORE = 'startup-sealed-restore'  # legacy V1 marker written by local sealed restore
TRAILING_KEYS = ('trailing_state', 'trailing_stop_trigger_pct', 'trailing_move_count',
                 'trailing_armed_timestamp', 'trailing_last_update_timestamp')
BOUND_TABLES = ('Executors', 'Order', 'OrderStatus', 'TradeFill', 'OrderExact', 'TradeFillExact',
                'PositionHoldLedger', 'Position')
SUPPORTED_ACQUISITION_CHAIN_CONTRACTS = frozenset({ACQUISITION_CHAIN_V1})


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def decimal(value):
    result = Decimal(str(value))
    require(result.is_finite(), 'non-finite receipt')
    return result


def decode_row(row):
    result = dict(row)
    for key in ('config', 'custom_info'):
        if isinstance(result.get(key), str):
            result[key] = json.loads(result[key])
    return result


def validate_position(preflight, stopped, *, allow_legacy_post_only_repair_transition=False,
                      terminal_proofs=None):
    """Validate exact identity and terminal state without I/O or rounded scalars."""
    preflight, stopped = decode_row(preflight), decode_row(stopped)
    require(stopped['type'] == 'position_executor', 'only PositionExecutor is supported')
    require(preflight['status'] == 2 and preflight['is_active'] and preflight['close_type'] is None,
            'preflight executor must be running with no exit trigger')
    require(stopped['status'] == 4 and not stopped['is_active']
            and stopped['close_type'] == CloseType.POSITION_HOLD.value, 'strict terminal POSITION_HOLD required')
    # Apply the singleton reader's ownership/economic invariants before either
    # opt-in path; the chain reader must not weaken the position contract.
    require(preflight['config'] == stopped['config'], 'preflight configuration differs')
    original = parse_position_config(stopped['config'])
    require(original.id == stopped['id'] and original.controller_id == stopped['controller_id']
            and original.timestamp == stopped['timestamp'], 'executor owner/timestamp differs')
    require(original.side == TradeType.BUY and original.leverage == 1 and original.connector_name == 'okx'
            and original.amount > 0 and original.entry_price > 0,
            'only funded OKX 1x spot BUY positions are supported')
    barriers = original.triple_barrier_config
    require(barriers.trailing_stop is not None and barriers.time_limit is None
            and barriers.stop_loss is None and barriers.take_profit is None,
            'only trailing-only positions are supported')
    if allow_legacy_post_only_repair_transition:
        transition = validate_legacy_post_only_repair_transition(preflight, stopped)
        require(isinstance(terminal_proofs, dict) and terminal_proofs.get('source') == 'native_terminal_entry_set_v1'
                and terminal_proofs.get('schema') == 1 and len(terminal_proofs.get('orders', [])) == 2,
                'explicit fresh terminal proof for both legacy children required')
        before, after = json.loads(canonical(preflight)), json.loads(canonical(stopped))
        require(before['custom_info'].get('planned_position_resume')
                == after['custom_info'].get('planned_position_resume'),
                'preflight consumed manifest reference differs from stopped owner')
        resume_config = transition['resume_config']
        for row in (before, after):
            row['config'] = resume_config
            row['custom_info']['acquisition_chain'] = transition['acquisition_chain']
            row['custom_info']['execution_order_snapshots'] = transition['entry_orders']
            row['custom_info']['order_ids'] = [child['order_id'] for child in transition['acquisition_chain']['children']]
            row['custom_info']['entry_acquisition'] = {
                'intent_id': resume_config['id'], 'policy': 'down_only_maker_v1',
                'order_ids': row['custom_info']['order_ids'],
            }
            row['custom_info']['entry_reconciliation'] = terminal_proofs
        after['custom_info']['entry_child_lifecycles'] = transition['entry_child_lifecycles']
        result = _validate_chained_position_receipts(
            before, after, parse_position_config(resume_config), after['custom_info'])
        # Keep the original preflight row for later prepare/claim revalidation;
        # the synthesized copy above is only the validator input.
        result.update(config=stopped['config'], resume_config=resume_config, preflight=preflight,
                      stopped_digest=digest(stopped),
                      legacy_transition={'schema': 1, 'kind': 'legacy_single_post_only_repair_v1',
                                         'original_config_sha256': transition['original_config_sha256']})
        return result
    return validate_position_receipts(preflight, stopped)


def validate_legacy_post_only_repair_transition(preflight, stopped):
    """Attest the one historical V2 repair shape without mutating persisted data.

    This only constructs a candidate resume config and chain from exact native
    snapshots. A caller still needs explicit opt-in and fresh terminal venue
    evidence for both children before the candidate can be claimed.
    """
    preflight, stopped = decode_row(preflight), decode_row(stopped)
    require(preflight['config'] == stopped['config'], 'legacy transition original config differs')
    config = parse_position_config(stopped['config'])
    require(getattr(config, 'entry_acquisition_policy', None) == 'down_only_maker_v1'
            and getattr(config, 'entry_chain_contract', None) is None,
            'legacy transition requires the original down-only config without chain opt-in')
    before, after = preflight['custom_info'], stopped['custom_info']
    old_ids = after.get('order_ids')
    require(isinstance(old_ids, list) and len(old_ids) == 2 and len(set(old_ids)) == 2
            and before.get('order_ids') == old_ids,
            'legacy transition requires exactly two persisted order ids')
    snapshots = after.get('execution_order_snapshots')
    require(isinstance(snapshots, list) and len(snapshots) == 2
            and before.get('execution_order_snapshots') == snapshots,
            'legacy transition requires exact preflight and stopped order snapshots')
    by_id = {order.client_order_id: order for order in (InFlightOrder.from_json(row) for row in snapshots)}
    require(list(by_id) == old_ids, 'legacy order snapshot order differs')
    predecessor, final = (by_id[oid] for oid in old_ids)
    acquisition = after.get('entry_acquisition')
    require(isinstance(acquisition, dict)
            and acquisition.get('intent_id') == config.id
            and acquisition.get('policy') == 'down_only_maker_v1'
            and acquisition.get('order_ids') == old_ids
            and acquisition.get('reprice_count') == 1,
            'legacy transition acquisition identity/reprice count differs')
    require(predecessor.trade_type is TradeType.BUY and predecessor.trading_pair == config.trading_pair
            and predecessor.current_state is OrderState.CANCELED
            and predecessor.executed_amount_base == 0 and predecessor.order_fills == {}
            and predecessor.cancel_source in (31, '31'),
            'legacy predecessor must be a source-31 zero-fill cancellation')
    require(final.trade_type is TradeType.BUY and final.trading_pair == config.trading_pair
            and final.current_state is OrderState.FILLED and final.executed_amount_base == final.amount == config.amount
            and final.exchange_order_id and final.executed_amount_quote > 0,
            'legacy repair child must be fully filled')
    require(predecessor.amount == final.amount == config.amount
            and Decimal(str(predecessor.price)) <= config.maximum_acceptable_price
            and Decimal(str(final.price)) <= config.maximum_acceptable_price,
            'legacy repair child quantities or prices exceed original authorization')
    lifecycle = after.get('entry_lifecycle')
    require(isinstance(lifecycle, dict)
            and lifecycle.get('cancel_source') in (31, '31')
            and lifecycle.get('replacement_purpose') == 'post_only_repair'
            and lifecycle.get('replacement_of_order_id') == old_ids[0]
            and lifecycle.get('order_id') == old_ids[1]
            and lifecycle.get('reprice_count') == 1
            and lifecycle.get('cancel_event_timestamp') is not None,
            'legacy entry lifecycle does not attest exact source-31 one-repair transition')
    resume_config = json.loads(canonical(stopped['config']))
    resume_config['entry_chain_contract'] = ACQUISITION_CHAIN_V1
    # Preserve other subclass settings and bind only the new recovery contract.
    config_class = type(config)
    resume_config_obj = config_class.model_validate(resume_config)
    chain = {
        'schema': 1, 'contract': ACQUISITION_CHAIN_V1, 'parent_id': config.id,
        'authorized_base': str(config.amount), 'maximum_price': str(config.maximum_acceptable_price),
        'children': [
            {'generation': 0, 'order_id': old_ids[0], 'predecessor_id': None, 'purpose': 'initial',
             'submitted_base': str(predecessor.amount), 'limit_price': str(predecessor.price)},
            {'generation': 1, 'order_id': old_ids[1], 'predecessor_id': old_ids[0], 'purpose': 'post_only_repair',
             'submitted_base': str(final.amount), 'limit_price': str(final.price)},
        ],
        'successor_pending': None, 'membership_closed': True,
        'gross_filled_base': str(final.executed_amount_base),
    }
    return {'resume_config': json.loads(resume_config_obj.model_dump_json()),
            'acquisition_chain': chain, 'entry_orders': [order.to_json() for order in (predecessor, final)],
            # The historical image did not persist per-child lifecycle maps;
            # retain that fact rather than manufacturing old event telemetry.
            'entry_child_lifecycles': {},
            'entry_lifecycle': json.loads(canonical(lifecycle)),
            'original_config_sha256': digest(stopped['config'])}


def _validate_trailing_evolution(config, before_custom, after_custom):
    """Require held trailing state to move only forward between the two captures."""
    for custom in (before_custom, after_custom):
        require(all(key in custom for key in TRAILING_KEYS), 'complete persisted trailing state required')
    before = {key: before_custom[key] for key in TRAILING_KEYS}
    after = {key: after_custom[key] for key in TRAILING_KEYS}

    def validate_snapshot(snapshot):
        state = snapshot['trailing_state']
        require(state in ('waiting', 'armed', 'disabled'), 'trailing has triggered or is unavailable')
        require((config.triple_barrier_config.trailing_stop is None) == (state == 'disabled'),
                'trailing config mismatch')
        moves = snapshot['trailing_move_count']
        require(isinstance(moves, int) and not isinstance(moves, bool) and moves >= 0,
                'invalid trailing move count')
        if state == 'armed':
            decimal(snapshot['trailing_stop_trigger_pct'])
            require(snapshot['trailing_armed_timestamp'] is not None
                    and snapshot['trailing_last_update_timestamp'] is not None,
                    'armed trailing timestamps required')
            require(config.timestamp <= decimal(snapshot['trailing_armed_timestamp'])
                    <= decimal(snapshot['trailing_last_update_timestamp']), 'invalid trailing timestamps')
        else:
            require(snapshot['trailing_stop_trigger_pct'] is None
                    and snapshot['trailing_armed_timestamp'] is None
                    and snapshot['trailing_last_update_timestamp'] is None and moves == 0,
                    'inconsistent unarmed trailing state')
        return state

    before_state = validate_snapshot(before)
    after_state = validate_snapshot(after)
    require(before_state == after_state or (before_state == 'waiting' and after_state == 'armed'),
            'trailing state regressed between preflight and stopped owner')
    if before_state == after_state == 'armed':
        trigger_advanced = decimal(after['trailing_stop_trigger_pct']) > decimal(before['trailing_stop_trigger_pct'])
        require(decimal(after['trailing_stop_trigger_pct']) >= decimal(before['trailing_stop_trigger_pct'])
                and after['trailing_move_count'] >= before['trailing_move_count']
                and decimal(after['trailing_armed_timestamp']) == decimal(before['trailing_armed_timestamp'])
                and decimal(after['trailing_last_update_timestamp']) >= decimal(before['trailing_last_update_timestamp']),
                'trailing high-water regressed between preflight and stopped owner')
        require((not trigger_advanced or after['trailing_move_count'] > before['trailing_move_count'])
                and (after['trailing_move_count'] == before['trailing_move_count'] or trigger_advanced),
                'trailing high-water and move count disagree')
    return after


def _validate_chained_position_receipts(preflight, stopped, config, custom):
    """Validate a fully closed, bounded replacement chain for resume."""
    acquisition = custom.get('entry_acquisition')
    require(config.entry_chain_contract in SUPPORTED_ACQUISITION_CHAIN_CONTRACTS
            and config.entry_acquisition_policy == 'down_only_maker_v1',
            'unsupported planned acquisition chain contract')
    require(isinstance(acquisition, dict) and acquisition.get('intent_id') == config.id
            and acquisition.get('policy') == 'down_only_maker_v1',
            'planned acquisition chain parent differs')
    preflight_custom = preflight['custom_info']
    snapshots = custom.get('execution_order_snapshots')
    before_snapshots = preflight_custom.get('execution_order_snapshots')
    require(isinstance(snapshots, list) and isinstance(before_snapshots, list)
            and len(snapshots) == len(before_snapshots),
            'preflight acquisition snapshot coverage differs from stopped owner')
    snapshot_fields = ('client_order_id', 'exchange_order_id', 'trading_pair', 'trade_type', 'order_type',
                       'amount', 'price', 'executed_amount_base', 'executed_amount_quote', 'order_fills')
    require(all(all(left.get(field) == right.get(field) for field in snapshot_fields)
                for left, right in zip(before_snapshots, snapshots)),
            'preflight acquisition order receipt differs from stopped owner')
    require(custom.get('acquisition_chain') == preflight_custom.get('acquisition_chain'),
            'preflight acquisition chain differs from stopped owner')
    require(preflight_custom.get('planned_position_resume') == custom.get('planned_position_resume'),
            'preflight consumed manifest reference differs from stopped owner')
    chain = validate_acquisition_chain(config, custom, custom.get('entry_reconciliation'), require_closed=True)
    order_ids = [child['order_id'] for child in chain['children']]
    require(custom.get('order_ids') == order_ids and acquisition.get('order_ids') == order_ids
            and preflight_custom.get('order_ids') == order_ids,
            'acquisition chain order identity changed or extra order exists')
    require(custom.get('close_order_filled_amount_base') == '0'
            and not custom.get('exit_trigger_reason'), 'exit already triggered or filled')
    orders_by_id = {}
    for snapshot in snapshots:
        order = InFlightOrder.from_json(snapshot)
        if order.trade_type is TradeType.BUY:
            orders_by_id[order.client_order_id] = order
        else:
            raise ValueError('planned replacement chains cannot contain exit orders')
    orders = [orders_by_id[order_id] for order_id in order_ids]
    require(all(order.current_state in (OrderState.CANCELED, OrderState.FILLED) for order in orders),
            'acquisition chain has an unfinished child')
    filled_orders = [order for order in orders if order.executed_amount_base > 0]
    require(len(filled_orders) == 1 and filled_orders[-1] is orders[-1]
            and orders[-1].current_state is OrderState.FILLED
            and orders[-1].executed_amount_base == orders[-1].amount,
            'only a fully filled final child after zero-fill predecessors is supported')
    require(sum((order.executed_amount_base for order in orders), Decimal('0')) == config.amount,
            'closed acquisition chain does not fill its full authorization')
    held = custom.get('held_position_orders', [])
    require(len(held) == 1 and held[0].get('client_order_id') == orders[-1].client_order_id,
            'one final filled child receipt is required for held ownership')
    base_asset = config.trading_pair.split('-')[0]
    net = sum((order.executed_amount_base - base_fee_amount(order.to_json(), base_asset) for order in orders), Decimal('0'))
    gross_quote = sum((order.executed_amount_quote for order in orders), Decimal('0'))
    require(net > 0 and decimal(custom['remaining_position_amount_base']) == net
            and decimal(custom['open_order_filled_amount_base']) == net
            and decimal(custom['open_order_filled_amount_quote']) == net * gross_quote / config.amount,
            'remaining position differs from exact acquisition chain receipts')
    trailing = _validate_trailing_evolution(config, preflight_custom, custom)
    result = {
        'executor_id': config.id, 'controller_id': config.controller_id,
        'config': stopped['config'], 'entry': orders[-1].to_json(),
        'entry_orders': [order.to_json() for order in orders],
        'entry_reconciliation': custom.get('entry_reconciliation'),
        'acquisition_chain': chain,
        'entry_child_lifecycles': custom.get('entry_child_lifecycles', {}),
        'entry_lifecycle': custom.get('entry_lifecycle', {}),
        'entry_terminal_reason': acquisition.get('terminal_reason'),
        'trailing': trailing, 'net_base': str(net),
        'stopped_digest': digest(stopped), 'preflight': preflight,
    }
    compatibility = custom.get('planned_position_resume_compatibility_v1')
    if compatibility is not None:
        require(isinstance(compatibility, dict)
                and compatibility.get('schema') == 1
                and compatibility.get('kind') == 'legacy_single_post_only_repair_v1'
                and isinstance(compatibility.get('manifest_id'), str),
                'planned resume compatibility marker is invalid')
        result['planned_position_resume_compatibility_v1'] = json.loads(canonical(compatibility))
    return result


def validate_position_receipts(preflight, stopped):
    """Validate unchanged position economics; callers must separately prove lifecycle state."""
    preflight, stopped = decode_row(preflight), decode_row(stopped)
    require(stopped['type'] == 'position_executor', 'only PositionExecutor is supported')
    for key in ('id', 'controller_id', 'timestamp', 'config', 'type'):
        require(preflight[key] == stopped[key], f'preflight identity/configuration differs: {key}')
    config = parse_position_config(stopped['config'])
    require(config.id == stopped['id'] and config.controller_id == stopped['controller_id'], 'executor owner differs')
    require(config.side == TradeType.BUY and config.leverage == 1 and config.connector_name == 'okx',
            'only fully filled OKX 1x spot BUY is supported')
    require(config.timestamp == stopped['timestamp'] and config.amount > 0 and config.entry_price > 0,
            'invalid original position configuration')
    barriers = config.triple_barrier_config
    require(barriers.trailing_stop is not None and barriers.time_limit is None
            and barriers.stop_loss is None and barriers.take_profit is None,
            'only trailing-only positions are supported')
    require(preflight['custom_info'].get('planned_position_resume')
            == stopped['custom_info'].get('planned_position_resume'),
            'preflight consumed manifest reference differs from stopped owner')
    if getattr(config, 'entry_chain_contract', None) in SUPPORTED_ACQUISITION_CHAIN_CONTRACTS:
        return _validate_chained_position_receipts(preflight, stopped, config, stopped['custom_info'])
    require(barriers.trailing_stop is not None and barriers.time_limit is None
            and barriers.stop_loss is None and barriers.take_profit is None, 'only trailing-only positions are supported')
    custom = stopped['custom_info']
    require(preflight['custom_info'].get('planned_position_resume') == custom.get('planned_position_resume'),
            'preflight consumed manifest reference differs from stopped owner')
    entries = custom.get('held_position_orders', [])
    require(len(entries) == 1, 'one singleton terminal entry receipt required')
    entry = entries[0]
    order_id = entry['client_order_id']
    acquisition = custom.get('entry_acquisition')
    if acquisition is not None or getattr(config, 'entry_acquisition_policy', None) is not None:
        require(getattr(config, 'entry_acquisition_policy', None) == 'down_only_maker_v1',
                'missing native down-only configuration; explicit reconciliation required')
        for snapshot in (preflight['custom_info'], custom):
            receipt = snapshot.get('entry_acquisition')
            require(isinstance(receipt, dict) and receipt.get('policy') == 'down_only_maker_v1'
                    and receipt.get('intent_id') == config.id and receipt.get('order_ids') == [order_id]
                    and type(receipt.get('reprice_count')) is int and receipt['reprice_count'] == 0
                    and receipt.get('replacement_purpose') is None
                    and receipt.get('replacement_of_order_id') is None,
                    'planned down-only resume supports an unreplaced singleton entry only')
            require(decimal(receipt.get('cumulative_filled_base')) == config.amount
                    and decimal(receipt.get('remaining_base')) == 0,
                    'planned down-only resume requires a completely filled entry')
    require(custom.get('order_ids') == [order_id] and preflight['custom_info'].get('order_ids') == [order_id],
            'singleton entry identity changed or extra order exists')
    order = InFlightOrder.from_json(entry)
    require(order.current_state == OrderState.FILLED and order.executed_amount_base == order.amount,
            'only a fully FILLED entry is supported')
    require(order.trade_type == TradeType.BUY and order.trading_pair == config.trading_pair
            and order.amount == config.amount and order.exchange_order_id, 'entry identity/quantity differs')
    require(order.order_fills, 'exact entry fills required')
    fills = list(order.order_fills.values())
    require(sum((f.fill_base_amount for f in fills), Decimal(0)) == order.executed_amount_base
            and sum((f.fill_quote_amount for f in fills), Decimal(0)) == order.executed_amount_quote,
            'entry cumulative receipt differs from fills')
    for fill in fills:
        require(fill.client_order_id == order_id and fill.exchange_order_id == order.exchange_order_id
                and fill.trading_pair == config.trading_pair and fill.fill_base_amount > 0
                and fill.fill_price > 0 and fill.fill_quote_amount == fill.fill_price * fill.fill_base_amount,
                'invalid entry fill identity or amounts')
        fee = fill.fee.to_json()
        base, quote = config.trading_pair.split('-')
        require(decimal(fee.get('percent', 0)) >= 0 and fee.get('percent_token') in (None, base, quote)
                and all(f['token'] in (base, quote) and decimal(f['amount']) >= 0 for f in fee.get('flat_fees', [])),
                'unsupported fee currency or rebate')
    reconciliation = validate_planned_entry_reconciliation(
        config, order, custom.get('entry_reconciliation'))
    net = order.executed_amount_base - base_fee_amount(entry, config.trading_pair.split('-')[0])
    require(net > 0 and decimal(custom['remaining_position_amount_base']) == net
            and decimal(custom['open_order_filled_amount_base']) == net
            and decimal(custom['open_order_filled_amount_quote']) == net * (order.executed_amount_quote / order.executed_amount_base),
            'remaining position differs from exact entry receipt')
    for snapshot in (preflight['custom_info'], custom):
        require(decimal(snapshot['close_order_filled_amount_base']) == 0 and not snapshot.get('exit_trigger_reason'),
                'exit already triggered or filled')
    trailing = _validate_trailing_evolution(config, preflight['custom_info'], custom)
    return {'executor_id': config.id, 'controller_id': config.controller_id, 'config': stopped['config'],
            'entry': entry, 'entry_reconciliation': reconciliation, 'trailing': trailing,
            'net_base': str(net), 'stopped_digest': digest(stopped),
            'preflight': preflight}


def connect(path, writable=False):
    path = Path(path).resolve(strict=True)
    connection = sqlite3.connect(f'{path.as_uri()}?mode={"rw" if writable else "ro"}', uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    return connection


def tables(connection):
    return {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def database_digest(connection):
    existing = tables(connection)
    values = {}
    for table in BOUND_TABLES:
        values[table] = []
        if table in existing:
            rows = [dict(row) for row in connection.execute(f'SELECT * FROM "{table}"')]
            if table == 'TradeFill':
                rows = [{key: value for key, value in row.items()
                         if not (key.startswith('exact_') and value is None)} for row in rows]
            values[table] = sorted(rows, key=canonical)
    return digest(values)


def bound_files(config_paths, root=ROOT):
    result = {}
    require(config_paths and all(str(path).startswith('conf/') and '..' not in Path(path).parts
                                 for path in config_paths), 'bind explicit conf/ script and controller files')
    for path in (*SOURCES, *config_paths):
        result[path] = hashlib.sha256((Path(root) / path).read_bytes()).hexdigest()
    return result


def validate_receipts(connection, position, proof, *, pending_hold=False):
    """Match every fill to venue proof, native event storage and held owner ledger."""
    entry = position['entry']
    oid = entry['client_order_id']
    config = position['config']
    if position.get('acquisition_chain'):
        chain_orders = position.get('entry_orders')
        require(isinstance(chain_orders, list) and len(chain_orders) == len(position['acquisition_chain']['children']),
                'native acquisition chain snapshots are incomplete')
        require([row['client_order_id'] for row in chain_orders]
                == [row['order_id'] for row in position['acquisition_chain']['children']],
                'native acquisition chain snapshot order differs')
        for child in chain_orders[:-1]:
            child_id = child['client_order_id']
            native_child = connection.execute('SELECT * FROM "Order" WHERE id=?', (child_id,)).fetchone()
            require(native_child is not None and native_child['last_status'] in ('OrderCancelled', 'OrderCanceled')
                    and native_child['exchange_order_id'] == child.get('exchange_order_id')
                    and native_child['market'] == config['connector_name']
                    and native_child['symbol'] == config['trading_pair'],
                    'native canceled predecessor order differs')
            for field in ('amount', 'price'):
                require(decimal(native_child[field]) == int(decimal(child[field]) * 1000000),
                        'native predecessor quantity/price differs')
            require(not connection.execute('SELECT 1 FROM TradeFill WHERE order_id=? LIMIT 1',
                                           (child_id,)).fetchone(),
                    'native predecessor unexpectedly has fills')
            if 'OrderExact' in tables(connection):
                exact = connection.execute('SELECT * FROM OrderExact WHERE order_id=?', (child_id,)).fetchone()
                if exact is not None:
                    require(all(decimal(exact[field]) == decimal(child[field]) for field in ('amount', 'price')),
                            'native exact predecessor companion conflicts')
    require(proof['client_order_id'] == oid and proof['exchange_order_id'] == entry['exchange_order_id']
            and proof['state'] == 'filled' and proof['trading_pair'] == config['trading_pair'], 'venue order differs')
    require(decimal(proof['filled_base']) == decimal(entry['executed_amount_base'])
            and decimal(proof['filled_quote']) == decimal(entry['executed_amount_quote'])
            and decimal(proof['net_base']) == decimal(position['net_base']), 'venue cumulative amounts differ')
    fills = entry['order_fills']
    require(set(proof['fills']) == set(fills), 'venue fill set differs')
    native_order = connection.execute('SELECT * FROM "Order" WHERE id=?', (oid,)).fetchone()
    require(native_order is not None and native_order['last_status'] == 'BuyOrderCompleted'
            and native_order['exchange_order_id'] == entry['exchange_order_id']
            and native_order['market'] == config['connector_name'] and native_order['symbol'] == config['trading_pair'],
            'native terminal order differs')
    for field in ('amount', 'price'):
        require(decimal(native_order[field]) == int(decimal(entry[field]) * 1000000),
                'native order quantity/price differs')
    existing_tables = tables(connection)
    if 'OrderExact' in existing_tables:
        exact_order = connection.execute('SELECT * FROM OrderExact WHERE order_id=?', (oid,)).fetchone()
        if exact_order is not None:
            require(all(decimal(exact_order[field]) == decimal(entry[field]) for field in ('amount', 'price')),
                    'exact order companion conflicts')
    native_fills = connection.execute('SELECT * FROM TradeFill WHERE order_id=?', (oid,)).fetchall()
    require({row['exchange_trade_id'] for row in native_fills} == set(fills), 'native fill set differs')
    for row in native_fills:
        fill = fills[row['exchange_trade_id']]
        venue = proof['fills'][row['exchange_trade_id']]
        require(row['market'] == config['connector_name'] and row['symbol'] == config['trading_pair']
                and row['trade_type'] == 'BUY' and row['config_file_path'] == native_order['config_file_path'],
                'native fill ownership differs')
        for field, receipt_field in (('amount', 'fill_base_amount'), ('price', 'fill_price')):
            exact = decimal(fill[receipt_field])
            require(decimal(row[field]) == int(exact * 1000000), 'native rounded fill conflicts with exact receipt')
            require(decimal(venue[receipt_field]) == exact, 'venue fill values differ')
            exact_field = f'exact_{field}'
            if exact_field in row.keys() and row[exact_field] is not None:
                require(decimal(row[exact_field]) == exact, 'native exact fill differs')
        require(json.loads(row['trade_fee']) == fill['fee'] == venue['fee'], 'fill fee receipt differs')
        require(row['base_asset'] == config['trading_pair'].split('-')[0]
                and row['quote_asset'] == config['trading_pair'].split('-')[1]
                and row['leverage'] == 1 and row['position'] in (None, 'NIL'), 'native spot fill attribution differs')
        if any(key.startswith('exact_') and row[key] is not None for key in row.keys()):
            require(row['exact_receipt_source'] == 'order_filled_event_v1'
                    and row['exact_amount'] is not None and row['exact_price'] is not None,
                    'native exact receipt provenance is incomplete')
        if 'TradeFillExact' in existing_tables:
            exact_fill = connection.execute('SELECT * FROM TradeFillExact WHERE market=? AND order_id=? AND exchange_trade_id=?',
                                            (row['market'], oid, row['exchange_trade_id'])).fetchone()
            if exact_fill is not None:
                require(decimal(exact_fill['amount']) == decimal(fill['fill_base_amount'])
                        and decimal(exact_fill['price']) == decimal(fill['fill_price']), 'exact fill companion conflicts')
    ledger = connection.execute('SELECT * FROM PositionHoldLedger WHERE client_order_id=?', (oid,)).fetchall()
    require(len(ledger) == 1 and ledger[0]['executor_id'] == position['executor_id']
            and ledger[0]['controller_id'] == position['controller_id']
            and ledger[0]['connector_name'] == config['connector_name']
            and ledger[0]['trading_pair'] == config['trading_pair'], 'held ledger owner missing or ambiguous')
    payload = json.loads(ledger[0]['order_payload'])
    for field in ('client_order_id', 'exchange_order_id', 'trade_type', 'executed_amount_base',
                  'executed_amount_quote', 'order_fills'):
        require(payload.get(field) == entry.get(field), f'held ledger receipt differs: {field}')
    # Scope aggregates are rounded projections. Only skip them at startup when
    # their inventory can be reconstructed from the complete exact ledger.
    scope = (position['controller_id'], config['connector_name'], config['trading_pair'])
    scope_ledger = connection.execute('SELECT order_payload FROM PositionHoldLedger WHERE '
                                      'controller_id=? AND connector_name=? AND trading_pair=?', scope).fetchall()
    inventory = Decimal(0)
    for row in scope_ledger:
        receipt = json.loads(row[0])
        require(receipt.get('order_fills'), 'scope contains unverifiable held inventory')
        sign = Decimal(1) if receipt['trade_type'] == 'BUY' else Decimal(-1)
        inventory += sign * decimal(receipt['executed_amount_base']) - base_fee_amount(receipt, config['trading_pair'].split('-')[0])
    aggregates = connection.execute('SELECT * FROM Position WHERE controller_id=? AND connector_name=? AND trading_pair=?', scope).fetchall()
    if aggregates:
        # Exceptional hold completion may corroborate a projection which predates
        # this exact pending owner's transfer. The singleton owned ledger above
        # proves the only quantity excluded; arbitrary caller offsets are forbidden.
        projected_inventory = inventory - decimal(position['net_base']) if pending_hold else inventory
        require(len(aggregates) == 1 and aggregates[0]['side'] == 'BUY'
                and projected_inventory >= 0
                and decimal(aggregates[0]['amount']) == int(projected_inventory * 1000000),
                'held aggregate is not completely covered by exact ledger')
    return {'ledger_rows': [dict(row) for row in ledger], 'position_rows': [dict(row) for row in aggregates]}


def validate_reconciliation(db_path, evidence, now):
    """Check the raw GET observation, not only caller-supplied success flags."""
    report = evidence['reconciliation']
    require(report.get('schema_version') == 1 and report.get('kind') == 'okx_stopped_owner_get_reconciliation'
            and report.get('venue_queried') is True and report.get('read_only') is True
            and report.get('result') == 'EVIDENCE_COMPLETE' and report.get('blockers') == []
            and report.get('owner') == evidence['owner'], 'complete authenticated owner reconciliation required')
    finished = datetime.fromisoformat(report['finished_at'].replace('Z', '+00:00')).timestamp()
    started = datetime.fromisoformat(report['started_at'].replace('Z', '+00:00')).timestamp()
    require(0 <= now - finished <= 300 and 0 <= finished - started <= 180, 'raw venue observation is stale')
    wal = Path(str(db_path) + '-wal')
    require(not wal.exists() or wal.stat().st_size == 0, 'checkpoint stopped database WAL before raw observation')
    sha = hashlib.sha256(Path(db_path).read_bytes()).hexdigest()
    require(report['database']['sha256'] == report['database']['sha256_after'] == sha,
            'raw reconciliation database bytes differ')
    selected = {proof['executor_id']: proof for proof in evidence['executors']}
    # Native terminal child proofs live with the stopped Executor snapshot.
    # Bind that persisted proof set into the read-only venue comparison when
    # the outer evidence envelope carries only the final-order summary.
    with connect(db_path) as connection:
        for executor_id, proof in selected.items():
            if proof.get('entry_reconciliation') is None:
                row = connection.execute('SELECT custom_info FROM Executors WHERE id=?', (executor_id,)).fetchone()
                if row is not None:
                    custom = json.loads(row['custom_info'] or '{}')
                    terminal_set = custom.get('entry_reconciliation') if isinstance(custom, dict) else None
                    if isinstance(terminal_set, dict) and terminal_set.get('source') == 'native_terminal_entry_set_v1':
                        proof['entry_reconciliation'] = terminal_set
    require(set(report['selected_executor_ids']) == set(selected)
            and {item['executor_id'] for item in report['selected_executors']} == set(selected),
            'raw reconciliation executor selection differs')
    for phase in ('pending_before', 'pending_after'):
        require({row['pair'] for row in report[phase]} == set(report['controller_pairs'].values()),
                'venue pending-order scope is incomplete')
        for row in report[phase]:
            require(row['regular'] == [] and row['regular_count'] == 0 and row['algo_count'] == 0
                    and set(row['algos']) == set(SPOT_ALGO_TYPES) and all(orders == [] for orders in row['algos'].values()),
                    'venue regular/algo orders remain')
    expected_balances = {}
    for item in report['selected_executors']:
        proof = selected[item['executor_id']]
        require(item['pair'] == proof['trading_pair']
                and report['controller_pairs'].get(item['controller_id']) == item['pair'],
                'raw venue executor owner differs')
        terminal_set = proof.get('entry_reconciliation')
        child_proofs = ([dict(child) for child in terminal_set.get('orders', [])]
                        if isinstance(terminal_set, dict)
                        and terminal_set.get('source') == 'native_terminal_entry_set_v1' else None)
        if child_proofs is None:
            child_proofs = [{'order_id': proof['client_order_id'],
                             'exchange_order_id': proof['exchange_order_id'], 'state': 'FILLED',
                             'amount': proof.get('amount', proof['filled_base']),
                             'filled_base': proof['filled_base'],
                             'trade_ids': list(proof['fills']),
                             'fills': proof['fills'], 'filled_quote': proof['filled_quote'],
                             'net_base': proof['net_base'], 'trading_pair': proof['trading_pair']}]
        for child_proof in child_proofs:
            child_fills_by_id = proof.get('child_fills', {})
            child_proof['fills'] = child_fills_by_id.get(child_proof['order_id'])
            if child_proof['fills'] is None:
                child_proof['fills'] = (proof['fills']
                                        if child_proof['order_id'] == proof.get('client_order_id') else {})
        require(len(item['venue_orders']) == len(child_proofs) and child_proofs,
                'raw venue terminal child coverage is incomplete')
        require([row['order_id'] for row in child_proofs]
                == [row['raw_order']['clOrdId'] for row in item['venue_orders']],
                'raw venue child ordering differs')
        for child_proof, venue in zip(child_proofs, item['venue_orders']):
            order = venue['raw_order']
            pair = child_proof.get('trading_pair', proof['trading_pair'])
            require(order['clOrdId'] == child_proof['order_id']
                    and order['ordId'] == child_proof['exchange_order_id']
                    and order['instId'] == pair and order['side'] == 'buy'
                    and order['state'] == child_proof['state'].lower()
                    and order['tdMode'] == 'cash', 'raw venue terminal child differs')
            require(decimal(order['sz']) == decimal(child_proof['amount'])
                    and decimal(order['accFillSz']) == decimal(child_proof['filled_base']),
                    'raw venue cumulative child fill differs')
            fills = venue['raw_fills']
            child_fills = child_proof.get('fills', {})
            require(len(fills) == len(child_fills) == len(child_proof['trade_ids'])
                    and {str(fill['tradeId']) for fill in fills} == set(child_fills)
                    == set(child_proof['trade_ids']), 'raw venue child fill set differs')
            base, quote = pair.split('-')
            gross = Decimal(0)
            base_fee = Decimal(0)
            for raw in fills:
                fill = child_fills[str(raw['tradeId'])]
                require(raw['ordId'] == order['ordId'] and raw['instId'] == order['instId'] and raw['side'] == 'buy'
                        and decimal(raw['fillSz']) == decimal(fill['fill_base_amount'])
                        and decimal(raw['fillPx']) == decimal(fill['fill_price']),
                        'raw venue child fill identity/values differ')
                paid = -decimal(raw['fee'])
                require(paid >= 0 and raw['feeCcy'] in (base, quote), 'unsupported raw venue fee')
                fee = fill['fee']
                require(decimal(fee.get('percent', 0)) == 0, 'actual fee token receipts required')
                native_fees = {}
                for flat in fee['flat_fees']:
                    native_fees[flat['token']] = native_fees.get(flat['token'], Decimal(0)) + decimal(flat['amount'])
                native_fees = {token: amount for token, amount in native_fees.items() if amount}
                require(native_fees == ({raw['feeCcy']: paid} if paid else {}),
                        'raw venue child fee differs from native receipt')
                gross += decimal(raw['fillSz']) * decimal(raw['fillPx'])
                base_fee += paid if raw['feeCcy'] == base else Decimal(0)
            require(gross == sum((decimal(fill['fill_base_amount']) * decimal(fill['fill_price'])
                                  for fill in child_fills.values()), Decimal(0))
                    and decimal(child_proof['filled_base']) - base_fee >= 0,
                    'raw venue child gross/net receipt differs')
            if child_proof['state'] == 'FILLED':
                expected_quote = child_proof.get('filled_quote', gross)
                expected_net = child_proof.get('net_base', decimal(child_proof['filled_base']) - base_fee)
                if child_proof['order_id'] == proof.get('client_order_id'):
                    expected_quote = child_proof.get('filled_quote', proof['filled_quote'])
                    expected_net = child_proof.get('net_base', proof['net_base'])
                require(gross == decimal(expected_quote)
                        and decimal(child_proof['filled_base']) - base_fee == decimal(expected_net),
                        'raw venue net receipt differs for filled child')
                expected_balances[base] = expected_balances.get(base, Decimal(0)) + decimal(child_proof['filled_base']) - base_fee
            else:
                require(child_proof['state'] == 'CANCELED' and not fills
                        and decimal(child_proof['filled_base']) == 0 and gross == 0,
                        'only zero-fill canceled predecessors are supported')
            require(venue['receipt']['blockers'] == []
                    and all(row['matched'] is True for row in venue['local_fill_comparisons']),
                    'raw venue reconciliation contains conflicts')
    balances = {row['asset']: row for row in report['balance_coverage']}
    require(set(balances) == set(expected_balances), 'raw balance coverage is incomplete')
    for asset, amount in expected_balances.items():
        row = balances[asset]
        require(row['covered'] is True and decimal(row['owned_residual']) == amount
                and min(decimal(row['cash_balance']), decimal(row['available_balance'])) >= amount,
                'raw available balance does not cover selected inventory')


def consumed_history(connection, pending_id=None):
    """Read immutable consumed receipts; another prepared or unknown row blocks reuse."""
    rows = ([dict(row) for row in connection.execute(f'SELECT * FROM {TABLE} ORDER BY id')
             if row['id'] != pending_id] if TABLE in tables(connection) else [])
    payloads = {}
    for row in rows:
        require(row['state'] == 'claimed', 'manifest history contains an unconsumed or unknown state')
        try:
            payload = json.loads(row['payload'])
            payloads[row['id']] = payload
            require(payload['schema'] == 1 and payload['id'] == row['id']
                    and isinstance(payload['positions'], list) and payload['positions']
                    and isinstance(payload['evidence'], dict) and isinstance(payload['files'], dict)
                    and payload['database_digest'] and payload['preflight_digest'],
                    'invalid consumed manifest payload')
            require(decimal(payload['created_at']) <= decimal(row['claimed_at']) < decimal(payload['expires_at']),
                    'invalid consumed manifest timestamp')
        except (KeyError, TypeError, ValueError, ArithmeticError) as error:
            raise ValueError('invalid consumed manifest history') from error
    # Follow bound predecessor history, not UUID order or potentially tied timestamps.
    prefix = []
    remaining = list(rows)
    while remaining:
        expected = digest(sorted(prefix, key=lambda row: row['id']))
        successors = [row for row in remaining
                      if payloads[row['id']].get('claimed_history_digest', digest([])) == expected]
        require(len(successors) == 1, 'consumed manifest history chain is incomplete or ambiguous')
        successor = successors[0]
        require(not prefix or decimal(payloads[successor['id']]['created_at']) >= decimal(prefix[-1]['claimed_at']),
                'consumed manifest history chronology differs')
        prefix.append(successor)
        remaining.remove(successor)
    # A deleted sole receipt or whole table must not turn an already-resumed owner into a first handover.
    if 'Executors' in tables(connection):
        for executor in connection.execute('SELECT id, custom_info FROM Executors'):
            custom = json.loads(executor['custom_info'] or '{}')
            reference = custom.get('planned_position_resume') if isinstance(custom, dict) else None
            # V1's local startup sealed restore marker is not a consumed PlannedPositionResume receipt.
            if reference is not None and reference != STARTUP_SEALED_RESTORE:
                require(reference in payloads and any(position.get('executor_id') == executor['id']
                        for position in payloads[reference]['positions']),
                        'executor references missing or mismatched consumed manifest history')
            marker = custom.get('planned_position_resume_compatibility_v1') if isinstance(custom, dict) else None
            if marker is not None:
                anchor = marker.get('manifest_id') if isinstance(marker, dict) else None
                require(anchor in payloads, 'legacy compatibility anchor is missing from consumed manifest history')
                position = next((row for row in payloads[anchor]['positions']
                                 if row.get('executor_id') == executor['id']), None)
                require(position is not None and position.get('legacy_transition', {}).get('kind')
                        == marker.get('kind')
                        and position.get('legacy_transition', {}).get('original_config_sha256')
                        == marker.get('original_config_sha256')
                        and digest(position.get('resume_config')) == marker.get('resume_config_sha256')
                        and digest(position.get('acquisition_chain')) == marker.get('acquisition_chain_sha256')
                        and [child.get('order_id') for child in position.get('acquisition_chain', {}).get('children', [])]
                        == marker.get('entry_order_ids'),
                        'legacy compatibility marker differs from original claimed manifest')
    return rows


def validate_history_binding(connection, manifest):
    history = consumed_history(connection, pending_id=manifest['id'])
    # Legacy first manifests had no history. Never infer missing history for a later handover.
    require(manifest.get('claimed_history_digest', digest([])) == digest(history),
            'consumed manifest history binding changed')


def prepare(db_path, preflight_path, evidence, now=None, root=ROOT, *,
            legacy_transition_executor_ids=()):
    """Register only; caller must stop the owner and reconcile the venue first."""
    now = time.time() if now is None else now
    require(evidence.get('schema') == 1 and evidence.get('owner_stopped') is True
            and evidence.get('venue_open_orders') == [], 'stopped owner and zero venue open orders proof required')
    require(0 <= now - evidence['observed_at'] <= 300 and now < evidence['expires_at'] <= now + 900,
            'evidence is stale or expiry exceeds 15 minutes')
    require(evidence['executors'] and len({p['executor_id'] for p in evidence['executors']}) == len(evidence['executors']),
            'explicit unique executor selection required')
    selected_ids = {proof['executor_id'] for proof in evidence['executors']}
    if isinstance(legacy_transition_executor_ids, str):
        raise ValueError('legacy transition executor ids must be an explicit collection')
    legacy_transition_executor_ids = set(legacy_transition_executor_ids)
    require(all(isinstance(item, str) and item for item in legacy_transition_executor_ids)
            and legacy_transition_executor_ids <= selected_ids,
            'legacy transition executor id is not in the explicit selection')
    config_paths = evidence['config_paths']
    require(f"conf/scripts/{evidence['script_config']}" in config_paths
            and 'conf/conf_client.yml' in config_paths and 'conf/connectors/okx.yml' in config_paths
            and any(path.startswith('conf/controllers/') for path in config_paths),
            'script, controller, client and encrypted connector config bindings required')
    script = yaml.safe_load((Path(root) / f"conf/scripts/{evidence['script_config']}").read_text())
    require(script.get('script_file_name') == 'v2_with_controllers.py' and script.get('controllers_config'),
            'native controller script config required')
    required_controllers = {f"conf/controllers/{name}" for name in script['controllers_config']}
    require(required_controllers <= set(config_paths), 'all configured controller files must be bound')
    controllers = [yaml.safe_load((Path(root) / name).read_text()) for name in required_controllers]
    require({row['id']: row['trading_pair'] for row in controllers} == evidence['reconciliation']['controller_pairs']
            and all(row['connector_name'] == 'okx' for row in controllers), 'controller owner/pair binding differs')
    validate_reconciliation(db_path, evidence, now)
    files = bound_files(config_paths, root)
    with connect(preflight_path) as preflight, connect(db_path, writable=True) as connection:
        connection.execute('BEGIN IMMEDIATE')
        require(not connection.execute('SELECT id FROM Executors WHERE status != 4').fetchone(),
                'database still has unfinished executors')
        history = consumed_history(connection)
        require(all(decimal(now) >= decimal(row['claimed_at']) for row in history),
                'new manifest predates consumed history')
        positions = []
        for proof in evidence['executors']:
            identity = (proof['executor_id'],)
            before = preflight.execute('SELECT * FROM Executors WHERE id=?', identity).fetchone()
            after = connection.execute('SELECT * FROM Executors WHERE id=?', identity).fetchone()
            require(before is not None and after is not None, 'selected executor missing')
            allow_transition = proof['executor_id'] in legacy_transition_executor_ids
            if allow_transition:
                require(isinstance(proof, dict), 'selected legacy executor proof missing')
                terminal_proofs = proof.get('entry_reconciliation')
                require(isinstance(terminal_proofs, dict), 'fresh terminal child proofs required for legacy transition')
                position = validate_position(before, after, allow_legacy_post_only_repair_transition=True,
                                             terminal_proofs=terminal_proofs)
            else:
                position = validate_position(before, after)
            if position.get('acquisition_chain'):
                # Native chain proof is sealed into the position manifest. The
                # venue GET comparison above ran against the evidence's copy, so
                # the sealed set must be that same set, child for child.
                supplied = proof.get('entry_reconciliation')
                require(supplied is None or supplied == position['entry_reconciliation'],
                        'venue-checked terminal proof set differs from the sealed owner set')
                proof['entry_reconciliation'] = position['entry_reconciliation']
            native_config = connection.execute('SELECT config_file_path FROM "Order" WHERE id=?',
                                               (position['entry']['client_order_id'],)).fetchone()
            require(native_config is not None and native_config[0] == evidence['script_config'],
                    'recorder script ownership differs')
            position['ownership_archive'] = validate_receipts(connection, position, proof)
            positions.append(position)
        manifest = {'schema': 1, 'id': str(uuid.uuid4()), 'created_at': now, 'expires_at': evidence['expires_at'],
                    'database_digest': database_digest(connection), 'preflight_digest': database_digest(preflight),
                    'claimed_history_digest': digest(history),
                    'files': files, 'evidence': evidence, 'positions': positions}
        connection.execute(f'CREATE TABLE IF NOT EXISTS {TABLE} (id TEXT PRIMARY KEY, state TEXT NOT NULL, '
                           'payload TEXT NOT NULL, claimed_at REAL)')
        connection.execute(f'INSERT INTO {TABLE} VALUES (?, ?, ?, NULL)', (manifest['id'], 'prepared', canonical(manifest)))
    return manifest


def inspect_manifest(db_path):
    with connect(db_path) as connection:
        if TABLE not in tables(connection):
            return []
        return [dict(row) for row in connection.execute(f'SELECT * FROM {TABLE}')]


def pending_manifest(db_path, now=None, root=ROOT):
    now = time.time() if now is None else now
    rows = inspect_manifest(db_path)
    pending = [row for row in rows if row['state'] == 'prepared']
    if not pending:
        with connect(db_path) as connection:
            consumed_history(connection)
        return None
    require(len(pending) == 1, 'multiple pending resume manifests')
    manifest = json.loads(pending[0]['payload'])
    require(manifest['created_at'] <= now < manifest['expires_at'], 'resume manifest expired')
    require(bound_files(manifest['evidence']['config_paths'], root) == manifest['files'], 'source/config binding changed')
    with connect(db_path) as connection:
        validate_history_binding(connection, manifest)
        require(database_digest(connection) == manifest['database_digest'], 'database binding changed')
        proofs = {proof['executor_id']: proof for proof in manifest['evidence']['executors']}
        for position in manifest['positions']:
            row = connection.execute('SELECT * FROM Executors WHERE id=?', (position['executor_id'],)).fetchone()
            transition = bool(position.get('legacy_transition'))
            proof = next(item for item in manifest['evidence']['executors']
                         if item['executor_id'] == position['executor_id'])
            require(row is not None and validate_position(position['preflight'], row,
                    allow_legacy_post_only_repair_transition=transition,
                    terminal_proofs=proof.get('entry_reconciliation') if transition else None)
                    == {key: value for key, value in position.items() if key != 'ownership_archive'},
                    'sealed position payload differs from stopped owner')
            require(validate_receipts(connection, position, proofs[position['executor_id']]) == position['ownership_archive'],
                    'held ownership archive differs')
    return manifest


def claim(db_path, manifest, now=None):
    now = time.time() if now is None else now
    with connect(db_path, writable=True) as connection:
        connection.execute('BEGIN IMMEDIATE')
        require(manifest['created_at'] <= now < manifest['expires_at'], 'resume manifest expired')
        row = connection.execute(f'SELECT * FROM {TABLE} WHERE id=?', (manifest['id'],)).fetchone()
        require(row is not None and row['state'] == 'prepared' and json.loads(row['payload']) == manifest,
                'resume claim already consumed or changed')
        validate_history_binding(connection, manifest)
        require(database_digest(connection) == manifest['database_digest'], 'database changed before claim')
        for position in manifest['positions']:
            record = connection.execute('SELECT * FROM Executors WHERE id=?', (position['executor_id'],)).fetchone()
            require(digest(decode_row(record)) == position['stopped_digest'], 'executor changed before claim')
            for held in position['ownership_archive']['ledger_rows']:
                removed = connection.execute('DELETE FROM PositionHoldLedger WHERE id=? AND executor_id=?',
                                             (held['id'], position['executor_id'])).rowcount
                require(removed == 1, 'selected held ownership row missing during claim')
            for aggregate in position['ownership_archive']['position_rows']:
                # Multiple selected owners may share a corroborated projection.
                connection.execute('DELETE FROM Position WHERE id=?', (aggregate['id'],))
            custom = json.loads(record['custom_info'])
            custom['planned_position_resume'] = manifest['id']
            if position.get('legacy_transition'):
                chain = position['acquisition_chain']
                marker = {
                    'schema': 1, 'kind': 'legacy_single_post_only_repair_v1',
                    'manifest_id': manifest['id'], 'executor_id': position['executor_id'],
                    'original_config_sha256': position['legacy_transition']['original_config_sha256'],
                    'resume_config_sha256': digest(position['resume_config']),
                    'acquisition_chain_sha256': digest(chain),
                    'entry_order_ids': [child['order_id'] for child in chain['children']],
                }
                position['planned_position_resume_compatibility_v1'] = marker
                custom['planned_position_resume_compatibility_v1'] = marker
            connection.execute('UPDATE Executors SET status=2, is_active=1, close_type=NULL, close_timestamp=NULL, '
                               'custom_info=? WHERE id=?', (canonical(custom), position['executor_id']))
        connection.execute(f'UPDATE {TABLE} SET state=\'claimed\', claimed_at=? WHERE id=?', (now, manifest['id']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('inspect', 'prepare'))
    parser.add_argument('--db', required=True)
    parser.add_argument('--preflight-db')
    parser.add_argument('--evidence')
    parser.add_argument('--legacy-post-only-repair-executor-id', action='append', default=[],
                        help='explicit selected executor ID eligible for the exact historical single-repair transition; repeat as needed')
    parser.add_argument('--runtime-root', type=Path, default=ROOT,
                        help='Final staged source/config tree for prepare bindings; native startup uses its own root')
    args = parser.parse_args()
    if args.action == 'inspect':
        result = inspect_manifest(args.db)
    else:
        require(args.preflight_db and args.evidence, 'prepare requires --preflight-db and --evidence')
        result = prepare(args.db, args.preflight_db, json.loads(Path(args.evidence).read_text()),
                         root=args.runtime_root,
                         legacy_transition_executor_ids=args.legacy_post_only_repair_executor_id or ())
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
