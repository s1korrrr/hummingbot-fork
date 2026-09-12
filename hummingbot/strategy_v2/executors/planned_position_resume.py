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
from hummingbot.strategy_v2.executors.order_accounting import base_fee_amount
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.executors import CloseType

TABLE = 'PlannedPositionResume'
ROOT = Path(__file__).resolve().parents[3]
SOURCES = (
    'hummingbot/strategy_v2/executors/planned_position_resume.py',
    'hummingbot/strategy_v2/executors/position_executor/position_executor.py',
    'hummingbot/strategy_v2/executors/position_executor/data_types.py',
    'hummingbot/strategy_v2/executors/executor_orchestrator.py',
    'hummingbot/strategy_v2/executors/order_accounting.py',
    'hummingbot/strategy/strategy_v2_base.py',
    'hummingbot/connector/markets_recorder.py',
    'hummingbot/connector/exchange/okx/okx_exchange.py',
    'hummingbot/connector/exchange/okx/native_reconciliation.py',
)
TRAILING_KEYS = ('trailing_state', 'trailing_stop_trigger_pct', 'trailing_move_count',
                 'trailing_armed_timestamp', 'trailing_last_update_timestamp')
BOUND_TABLES = ('Executors', 'Order', 'OrderStatus', 'TradeFill', 'OrderExact', 'TradeFillExact',
                'PositionHoldLedger', 'Position')


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


def validate_position(preflight, stopped):
    """Validate exact identity and terminal state without I/O or rounded scalars."""
    preflight, stopped = decode_row(preflight), decode_row(stopped)
    require(stopped['type'] == 'position_executor', 'only PositionExecutor is supported')
    require(preflight['status'] == 2 and preflight['is_active'] and preflight['close_type'] is None,
            'preflight executor must be running with no exit trigger')
    require(stopped['status'] == 4 and not stopped['is_active']
            and stopped['close_type'] == CloseType.POSITION_HOLD.value, 'strict terminal POSITION_HOLD required')
    for key in ('id', 'controller_id', 'timestamp', 'config', 'type'):
        require(preflight[key] == stopped[key], f'preflight identity/configuration differs: {key}')
    config = PositionExecutorConfig.model_validate(stopped['config'])
    require(config.id == stopped['id'] and config.controller_id == stopped['controller_id'], 'executor owner differs')
    require(config.side == TradeType.BUY and config.leverage == 1 and config.connector_name == 'okx',
            'only fully filled OKX 1x spot BUY is supported')
    require(config.timestamp == stopped['timestamp'] and config.amount > 0 and config.entry_price > 0,
            'invalid original position configuration')
    barriers = config.triple_barrier_config
    require(barriers.trailing_stop is not None and barriers.time_limit is None
            and barriers.stop_loss is None and barriers.take_profit is None, 'only trailing-only positions are supported')
    custom = stopped['custom_info']
    entries = custom.get('held_position_orders', [])
    require(len(entries) == 1, 'one singleton terminal entry receipt required')
    entry = entries[0]
    order_id = entry['client_order_id']
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
    net = order.executed_amount_base - base_fee_amount(entry, config.trading_pair.split('-')[0])
    require(net > 0 and decimal(custom['remaining_position_amount_base']) == net
            and decimal(custom['open_order_filled_amount_base']) == net
            and decimal(custom['open_order_filled_amount_quote']) == net * (order.executed_amount_quote / order.executed_amount_base),
            'remaining position differs from exact entry receipt')
    for snapshot in (preflight['custom_info'], custom):
        require(decimal(snapshot['close_order_filled_amount_base']) == 0 and not snapshot.get('exit_trigger_reason'),
                'exit already triggered or filled')
    require(all(key in custom for key in TRAILING_KEYS), 'complete persisted trailing state required')
    trailing = {key: custom[key] for key in TRAILING_KEYS}
    state = trailing['trailing_state']
    require(state in ('waiting', 'armed', 'disabled'), 'trailing has triggered or is unavailable')
    require((config.triple_barrier_config.trailing_stop is None) == (state == 'disabled'), 'trailing config mismatch')
    moves = trailing['trailing_move_count']
    require(isinstance(moves, int) and not isinstance(moves, bool) and moves >= 0, 'invalid trailing move count')
    if state == 'armed':
        decimal(trailing['trailing_stop_trigger_pct'])
        require(trailing['trailing_armed_timestamp'] is not None and trailing['trailing_last_update_timestamp'] is not None,
                'armed trailing timestamps required')
        require(config.timestamp <= decimal(trailing['trailing_armed_timestamp'])
                <= decimal(trailing['trailing_last_update_timestamp']), 'invalid trailing timestamps')
    else:
        require(trailing['trailing_stop_trigger_pct'] is None and trailing['trailing_armed_timestamp'] is None
                and trailing['trailing_last_update_timestamp'] is None and moves == 0, 'inconsistent unarmed trailing state')
    return {'executor_id': config.id, 'controller_id': config.controller_id, 'config': stopped['config'],
            'entry': entry, 'trailing': trailing, 'net_base': str(net), 'stopped_digest': digest(stopped),
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


def validate_receipts(connection, position, proof):
    """Match every fill to venue proof, native event storage and held owner ledger."""
    entry = position['entry']
    oid = entry['client_order_id']
    config = position['config']
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
        require(len(aggregates) == 1 and aggregates[0]['side'] == 'BUY'
                and decimal(aggregates[0]['amount']) == int(inventory * 1000000),
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
        require(len(item['venue_orders']) == 1 and item['pair'] == proof['trading_pair']
                and report['controller_pairs'].get(item['controller_id']) == item['pair'],
                'raw venue executor owner differs')
        venue = item['venue_orders'][0]
        order = venue['raw_order']
        require(order['clOrdId'] == proof['client_order_id'] and order['ordId'] == proof['exchange_order_id']
                and order['instId'] == proof['trading_pair'] and order['side'] == 'buy'
                and order['state'] == 'filled' and order['tdMode'] == 'cash', 'raw venue terminal order differs')
        require(decimal(order['sz']) == decimal(order['accFillSz']) == decimal(proof['filled_base']),
                'raw venue cumulative fill differs')
        fills = venue['raw_fills']
        require(len(fills) == len(proof['fills']) and {str(fill['tradeId']) for fill in fills} == set(proof['fills']),
                'raw venue fill set differs')
        base, quote = proof['trading_pair'].split('-')
        gross = Decimal(0)
        base_fee = Decimal(0)
        for raw in fills:
            fill = proof['fills'][str(raw['tradeId'])]
            require(raw['ordId'] == order['ordId'] and raw['instId'] == order['instId'] and raw['side'] == 'buy'
                    and decimal(raw['fillSz']) == decimal(fill['fill_base_amount'])
                    and decimal(raw['fillPx']) == decimal(fill['fill_price']), 'raw venue fill identity/values differ')
            paid = -decimal(raw['fee'])
            require(paid >= 0 and raw['feeCcy'] in (base, quote), 'unsupported raw venue fee')
            fee = fill['fee']
            require(decimal(fee.get('percent', 0)) == 0, 'actual fee token receipts required')
            native_fees = {}
            for flat in fee['flat_fees']:
                native_fees[flat['token']] = native_fees.get(flat['token'], Decimal(0)) + decimal(flat['amount'])
            native_fees = {token: amount for token, amount in native_fees.items() if amount}
            require(native_fees == ({raw['feeCcy']: paid} if paid else {}), 'raw venue fee differs from native receipt')
            gross += decimal(raw['fillSz']) * decimal(raw['fillPx'])
            base_fee += paid if raw['feeCcy'] == base else Decimal(0)
        require(gross == decimal(proof['filled_quote'])
                and decimal(proof['filled_base']) - base_fee == decimal(proof['net_base']),
                'raw venue net receipt differs')
        require(venue['receipt']['blockers'] == []
                and all(row['matched'] is True for row in venue['local_fill_comparisons']),
                'raw venue reconciliation contains conflicts')
        expected_balances[base] = expected_balances.get(base, Decimal(0)) + decimal(proof['net_base'])
    balances = {row['asset']: row for row in report['balance_coverage']}
    require(set(balances) == set(expected_balances), 'raw balance coverage is incomplete')
    for asset, amount in expected_balances.items():
        row = balances[asset]
        require(row['covered'] is True and decimal(row['owned_residual']) == amount
                and min(decimal(row['cash_balance']), decimal(row['available_balance'])) >= amount,
                'raw available balance does not cover selected inventory')


def prepare(db_path, preflight_path, evidence, now=None, root=ROOT):
    """Register only; caller must stop the owner and reconcile the venue first."""
    now = time.time() if now is None else now
    require(evidence.get('schema') == 1 and evidence.get('owner_stopped') is True
            and evidence.get('venue_open_orders') == [], 'stopped owner and zero venue open orders proof required')
    require(0 <= now - evidence['observed_at'] <= 300 and now < evidence['expires_at'] <= now + 900,
            'evidence is stale or expiry exceeds 15 minutes')
    require(evidence['executors'] and len({p['executor_id'] for p in evidence['executors']}) == len(evidence['executors']),
            'explicit unique executor selection required')
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
        if TABLE in tables(connection):
            require(not connection.execute(f'SELECT 1 FROM {TABLE}').fetchone(), 'a manifest already exists; no automatic reuse')
        positions = []
        for proof in evidence['executors']:
            identity = (proof['executor_id'],)
            before = preflight.execute('SELECT * FROM Executors WHERE id=?', identity).fetchone()
            after = connection.execute('SELECT * FROM Executors WHERE id=?', identity).fetchone()
            require(before is not None and after is not None, 'selected executor missing')
            position = validate_position(before, after)
            native_config = connection.execute('SELECT config_file_path FROM "Order" WHERE id=?',
                                               (position['entry']['client_order_id'],)).fetchone()
            require(native_config is not None and native_config[0] == evidence['script_config'],
                    'recorder script ownership differs')
            position['ownership_archive'] = validate_receipts(connection, position, proof)
            positions.append(position)
        manifest = {'schema': 1, 'id': str(uuid.uuid4()), 'created_at': now, 'expires_at': evidence['expires_at'],
                    'database_digest': database_digest(connection), 'preflight_digest': database_digest(preflight),
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
        return None
    require(len(pending) == 1, 'multiple pending resume manifests')
    manifest = json.loads(pending[0]['payload'])
    require(manifest['created_at'] <= now < manifest['expires_at'], 'resume manifest expired')
    require(bound_files(manifest['evidence']['config_paths'], root) == manifest['files'], 'source/config binding changed')
    with connect(db_path) as connection:
        require(database_digest(connection) == manifest['database_digest'], 'database binding changed')
        proofs = {proof['executor_id']: proof for proof in manifest['evidence']['executors']}
        for position in manifest['positions']:
            row = connection.execute('SELECT * FROM Executors WHERE id=?', (position['executor_id'],)).fetchone()
            require(row is not None and validate_position(position['preflight'], row)
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
            connection.execute('UPDATE Executors SET status=2, is_active=1, close_type=NULL, close_timestamp=NULL, '
                               'custom_info=? WHERE id=?', (canonical(custom), position['executor_id']))
        connection.execute(f'UPDATE {TABLE} SET state=\'claimed\', claimed_at=? WHERE id=?', (now, manifest['id']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('inspect', 'prepare'))
    parser.add_argument('--db', required=True)
    parser.add_argument('--preflight-db')
    parser.add_argument('--evidence')
    parser.add_argument('--runtime-root', type=Path, default=ROOT,
                        help='Final staged source/config tree for prepare bindings; native startup uses its own root')
    args = parser.parse_args()
    if args.action == 'inspect':
        result = inspect_manifest(args.db)
    else:
        require(args.preflight_db and args.evidence, 'prepare requires --preflight-db and --evidence')
        result = prepare(args.db, args.preflight_db, json.loads(Path(args.evidence).read_text()), root=args.runtime_root)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
