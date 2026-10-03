"""Explicit stopped-database transfer of one filled position back to its executor.

This is an operator-assisted migration, never general crash recovery. A consumed
claim is not replayed. Evidence contains public order receipts, never credentials.
"""
import argparse
import base64
import hashlib
import hmac
import json
import math
import sqlite3
import stat
import time
import uuid
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import yaml

from hummingbot.client.config.security import Security
from hummingbot.connector.exchange.okx.native_reconciliation import SPOT_ALGO_TYPES
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1, validate_acquisition_chain
from hummingbot.strategy_v2.executors.order_accounting import base_fee_amount, base_fee_quote_amount
from hummingbot.strategy_v2.executors.position_executor.data_types import parse_position_config
from hummingbot.strategy_v2.executors.position_executor.position_executor import validate_planned_entry_reconciliation
from hummingbot.strategy_v2.executors.recovery_inventory import (
    corroborate_order_receipt,
    native_inventory_proof,
    require_inventory_conservation,
)
from hummingbot.strategy_v2.executors.restart_continuity import RestartContinuityError, continuity_plan
from hummingbot.strategy_v2.migrations.entry_risk import (
    financial_state_digest_before_prospective_daily_enrollment,
    validate_prospective_daily_enrollment_archive,
)
from hummingbot.strategy_v2.models.executors import CloseType

TABLE = 'PlannedPositionResume'
ROOT = Path(__file__).resolve().parents[3]
SOURCES = (
    'hummingbot/strategy_v2/executors/acquisition_chain.py',
    'hummingbot/strategy_v2/executors/planned_position_resume.py',
    'hummingbot/strategy_v2/executors/position_executor/position_executor.py',
    'hummingbot/strategy_v2/executors/position_executor/data_types.py',
    'hummingbot/strategy_v2/executors/executor_orchestrator.py',
    'hummingbot/strategy_v2/executors/restart_continuity.py',
    'hummingbot/strategy_v2/executors/recovery_inventory.py',
    'hummingbot/model/exact_order_values.py',
    'hummingbot/model/trade_fill.py',
    'hummingbot/model/executors.py',
    'hummingbot/model/decimal_type_decorator.py',
    'hummingbot/core/data_type/common.py',
    'hummingbot/core/data_type/in_flight_order.py',
    'hummingbot/core/data_type/trade_fee.py',
    'controllers/generic/meridian_v3_unified_core/terminal_dust.py',
    'controllers/generic/modular_spot_core/inventory.py',
    'controllers/generic/modular_spot_core/native_funding.py',
    'controllers/generic/modular_spot_core/domain.py',
    'hummingbot/strategy_v2/components/domain.py',
    'hummingbot/strategy_v2/executors/order_accounting.py',
    'hummingbot/strategy/strategy_v2_base.py',
    'scripts/v2_with_controllers.py',
    'hummingbot/connector/markets_recorder.py',
    'hummingbot/connector/exchange/okx/okx_exchange.py',
    'hummingbot/connector/exchange/okx/native_reconciliation.py',
    'hummingbot/strategy_v2/models/executors_info.py',
    'hummingbot/strategy_v2/migrations/entry_risk.py',
    'hummingbot/strategy_v2/models/daily_entry_risk.py',
    'hummingbot/strategy_v2/models/entry_risk_enrollment.py',
    'hummingbot/strategy_v2/controllers/native_policy_identity.py',
    'controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py',
    'controllers/directional_trading/ok_rsi_core/down_only_entry.py',
    'controllers/directional_trading/ok_rsi_core/execution.py',
)
STARTUP_SEALED_RESTORE = 'startup-sealed-restore'  # legacy V1 marker written by local sealed restore
TRAILING_KEYS = ('trailing_state', 'trailing_stop_trigger_pct', 'trailing_move_count',
                 'trailing_armed_timestamp', 'trailing_last_update_timestamp')
SOL_MARK_ONLY_CUSTOM = frozenset({
    'close_price', 'current_market_price', 'trailing_current_price',
    'distance_to_trailing_activation_pct', 'distance_to_trailing_trigger_pct',
    'open_order_age_seconds',
})
SOL_MARK_ONLY_OUTER = frozenset({'net_pnl_quote', 'net_pnl_pct'})
BOUND_TABLES = ('Executors', 'Order', 'OrderStatus', 'TradeFill', 'OrderExact', 'TradeFillExact',
                'PositionHoldLedger', 'Position')
FLEET_OWNERS = {'rsi_modular_v2': 'rsi_modular_v2.yml', 'meridian_v3': 'meridian_v3.yml'}
FLEET_TABLE_DIGEST_FORMAT = 'sqlite-native-tables-pk-ordered-v1'
FLEET_REQUIRED_TABLES = {'Metadata', 'Executors', 'PositionHoldLedger', 'Position', 'Order'}
FLEET_AUTHENTICATOR_DOMAIN = b'rsibot:v1-terminal-fleet-ownership:v1\0'
OKX_REQUIRED_KEY_FIELDS = frozenset({'okx_api_key', 'okx_secret_key', 'okx_passphrase'})
OKX_ALLOWED_KEY_FIELDS = OKX_REQUIRED_KEY_FIELDS | {'okx_registration_sub_domain'}
SUPPORTED_ACQUISITION_CHAIN_CONTRACTS = frozenset({ACQUISITION_CHAIN_V1})


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _validated_native_okx_keys(native_keys):
    require(isinstance(native_keys, dict) and OKX_REQUIRED_KEY_FIELDS <= set(native_keys)
            and set(native_keys) <= OKX_ALLOWED_KEY_FIELDS
            and all(isinstance(value, str) and value.strip() for value in native_keys.values()),
            'decrypted native OKX credential set is missing required fields')
    if 'okx_registration_sub_domain' in native_keys:
        require(native_keys['okx_registration_sub_domain'] in {'www', 'app', 'my'},
                'decrypted native OKX registration subdomain is invalid')
    return native_keys


def _fleet_auth_key(native_keys):
    keys = _validated_native_okx_keys(native_keys)
    return hashlib.sha256(FLEET_AUTHENTICATOR_DOMAIN + canonical(keys).encode()).digest()


def _fleet_evidence_digest(evidence):
    require(isinstance(evidence, dict), 'full recovery evidence is required for fleet authentication')
    payload = dict(evidence)
    payload.pop('fleet_ownership', None)
    reconciliation = payload.get('reconciliation')
    if isinstance(reconciliation, dict):
        reconciliation = dict(reconciliation)
        reconciliation.pop('fleet_ownership_authenticator', None)
        payload['reconciliation'] = reconciliation
    return digest(payload)


def seal_fleet_ownership(bundle, native_keys, evidence):
    """Authenticate complete recovery evidence without storing account fingerprints."""
    _validated_native_okx_keys(native_keys)
    bundle['sealed_evidence_sha256'] = _fleet_evidence_digest(evidence)
    payload = {key: value for key, value in bundle.items() if key != 'authenticator'}
    bundle['authenticator'] = hmac.new(
        _fleet_auth_key(native_keys), FLEET_AUTHENTICATOR_DOMAIN + canonical(payload).encode(), hashlib.sha256
    ).hexdigest()
    return bundle


def verify_fleet_ownership(bundle, native_keys, evidence=None):
    """Fail closed unless the complete bundle is authenticated by unlocked native OKX keys."""
    _validated_native_okx_keys(native_keys)
    require(isinstance(bundle, dict) and isinstance(bundle.get('authenticator'), str),
            'authenticated fleet ownership bundle is required')
    payload = {key: value for key, value in bundle.items() if key != 'authenticator'}
    expected = hmac.new(
        _fleet_auth_key(native_keys), FLEET_AUTHENTICATOR_DOMAIN + canonical(payload).encode(), hashlib.sha256
    ).hexdigest()
    require(hmac.compare_digest(bundle['authenticator'], expected),
            'fleet ownership authenticator does not match current native OKX credentials')
    if evidence is not None:
        require(bundle.get('sealed_evidence_sha256') == _fleet_evidence_digest(evidence),
                'sealed recovery evidence changed after fleet authentication')
    return True


def unlock_native_okx(password_file):
    """Unlock native connector config from a private password file without emitting secrets."""
    from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger

    password_path = Path(password_file)
    require(not any(path.is_symlink() for path in (password_path, *password_path.parents)),
            'native password file traverses a symbolic link')
    info = password_path.stat()
    require(stat.S_ISREG(info.st_mode) and not info.st_mode & 0o077 and info.st_size <= 65536,
            'private bounded native password file required')
    password = password_path.read_text().rstrip('\r\n')
    require(bool(password) and not any(char in password for char in '\x00\r\n'),
            'native password file is empty or malformed')
    require(Security.login(ETHKeyFileSecretManger(password)), 'native connector credential unlock failed')
    keys = Security.api_keys('okx')
    _validated_native_okx_keys(keys)
    return keys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def decimal(value):
    result = Decimal(str(value))
    require(result.is_finite(), 'non-finite receipt')
    return result


def position_hold_net_base(receipt, base_asset):
    """Return the signed net base amount used by native PositionHold accounting."""
    trade_type = receipt.get('trade_type')
    require(trade_type in ('BUY', 'SELL'), 'held ledger side is invalid')
    gross = decimal(receipt.get('executed_amount_base', 0))
    fee = base_fee_amount(receipt, base_asset)
    require(gross >= 0, 'held ledger gross amount is invalid')
    sign = Decimal(1) if trade_type == 'BUY' else Decimal(-1)
    return sign * gross - fee


def _sqlite_value(value):
    if isinstance(value, bytes):
        return {'blob_base64': base64.b64encode(value).decode('ascii')}
    if isinstance(value, float):
        require(math.isfinite(value), 'native snapshot contains a non-finite SQLite value')
    return value


def native_snapshot_tables_digest(connection):
    """Hash every native user table in deterministic PK order, except our own manifest table."""
    connection.row_factory = sqlite3.Row
    names = [row[0] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        if row[0] != TABLE]
    require(FLEET_REQUIRED_TABLES <= set(names), 'fleet ownership snapshot lacks required native tables')
    snapshot = []
    for name in names:
        quoted = '"' + name.replace('"', '""') + '"'
        schema = [dict(row) for row in connection.execute(f'PRAGMA table_info({quoted})')]
        primary_key = [row['name'] for row in sorted(schema, key=lambda item: item['pk']) if row['pk']]
        require(primary_key, f'fleet ownership table {name} has no stable primary key')
        columns = [row['name'] for row in schema]
        order = ', '.join('"' + column.replace('"', '""') + '"' for column in primary_key)
        rows = [[_sqlite_value(value) for value in row]
                for row in connection.execute(f'SELECT * FROM {quoted} ORDER BY {order}')]
        snapshot.append({'name': name, 'schema': schema, 'columns': columns, 'rows': rows})
    return digest({'format': FLEET_TABLE_DIGEST_FORMAT, 'tables': snapshot})


def _validated_receipt_net_base(receipt, pair, expected_trade_type=None):
    require(isinstance(receipt, dict), 'fleet native order receipt is malformed')
    trade_type = str(receipt.get('trade_type', expected_trade_type or '')).upper()
    require(trade_type in ('BUY', 'SELL') and (expected_trade_type is None or trade_type == expected_trade_type),
            'fleet native order side is unsupported or inconsistent')
    base, quote = pair.split('-')
    fills = receipt.get('order_fills')
    require(isinstance(fills, dict) and fills, 'fleet native inventory lacks exact fill receipts')
    gross_base = Decimal(0)
    net_base = Decimal(0)
    for trade_id, fill in fills.items():
        require(isinstance(fill, dict) and fill.get('trading_pair', pair) == pair,
                'fleet native fill pair is inconsistent')
        size = decimal(fill.get('fill_base_amount'))
        price = decimal(fill.get('fill_price'))
        quote_amount = decimal(fill.get('fill_quote_amount', size * price))
        require(size > 0 and price > 0 and quote_amount == size * price,
                'fleet native fill amount or quote is inconsistent')
        fee = fill.get('fee')
        require(isinstance(fee, dict) and decimal(fee.get('percent', 0)) == 0,
                'fleet native inventory requires exact flat-fee receipts')
        fees = defaultdict(Decimal)
        flat_fees = fee.get('flat_fees')
        require(isinstance(flat_fees, list), 'fleet native fill fee receipt is malformed')
        for item in flat_fees:
            require(isinstance(item, dict) and item.get('token') in (base, quote),
                    'fleet native fill fee asset is unsupported')
            amount = decimal(item.get('amount'))
            # Native actual-currency fees are signed: a maker rebate is negative
            # and adds owned base. The exact fill match below validates it.
            fees[item['token']] += amount
        base_fee = fees[base]
        signed_gross = size if trade_type == 'BUY' else -size
        net_base += signed_gross - base_fee
        gross_base += size
        require(trade_id is not None, 'fleet native trade identity is missing')
    expected_gross = decimal(receipt.get('executed_amount_base'))
    require(gross_base == expected_gross and expected_gross > 0,
            'fleet native order cumulative base differs from its exact fills')
    return net_base


def _owner_snapshot_readonly(path):
    path = Path(path)
    require(path.is_absolute() and path.is_file() and not path.is_symlink(),
            'fleet ownership snapshot path is unavailable or symbolic')
    _require_no_snapshot_sidecars(path)
    before_stat = path.stat()
    file_digest = hashlib.sha256(path.read_bytes()).hexdigest()
    connection = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute('PRAGMA query_only=ON')
        connection.execute('BEGIN')
        require(connection.execute('PRAGMA integrity_check').fetchone()[0] == 'ok',
                'fleet ownership snapshot integrity check failed')
        digest_value = native_snapshot_tables_digest(connection)
        return connection, file_digest, digest_value, before_stat
    except Exception:
        connection.close()
        raise


def _require_no_snapshot_sidecars(path):
    wal = Path(str(path) + '-wal')
    shm = Path(str(path) + '-shm')
    require((not wal.exists() or wal.stat().st_size == 0)
            and (not shm.exists() or shm.stat().st_size == 0),
            'fleet ownership snapshot has uncheckpointed SQLite sidecars')


def _derive_owner_residuals(connection, owner, quantity_increments=None):
    """Derive positive spot inventory from unique held receipts and active exact executor receipts."""
    totals = defaultdict(Decimal)
    ledger_by_executor = defaultdict(Decimal)
    seen_fills = set()
    script_config = FLEET_OWNERS[owner]
    dust_totals, fill_totals, native_fills = native_inventory_proof(connection, script_config, quantity_increments)
    risk_row = connection.execute('SELECT value FROM Metadata WHERE key=?',
                                  (f'strategy-risk:{script_config}',)).fetchone()
    risk_controllers = None
    if risk_row is not None:
        try:
            risk_state = json.loads(risk_row['value'])
            require(isinstance(risk_state, dict) and isinstance(risk_state.get('controllers'), list)
                    and len(risk_state['controllers']) == len(set(risk_state['controllers'])),
                    'fleet native risk metadata controller scope is malformed')
            risk_controllers = set(risk_state['controllers'])
        except (TypeError, ValueError):
            raise ValueError('fleet native risk metadata is malformed') from None
    ledger_rows = connection.execute('SELECT * FROM PositionHoldLedger ORDER BY id').fetchall()
    for row in ledger_rows:
        receipt = json.loads(row['order_payload'])
        pair = row['trading_pair']
        require(row['connector_name'] == 'okx' and '-' in pair
                and receipt.get('client_order_id', row['client_order_id']) == row['client_order_id'],
                'fleet held receipt owner or order identity differs')
        order = connection.execute('SELECT config_file_path FROM "Order" WHERE id=?',
                                   (row['client_order_id'],)).fetchone()
        require(order is not None and order['config_file_path'] == script_config,
                'fleet held receipt native Order is not bound to its owner script')
        if risk_controllers is not None:
            require(row['controller_id'] in risk_controllers,
                    'fleet held receipt controller is outside native risk metadata')
        fills = receipt.get('order_fills')
        require(isinstance(fills, dict), 'fleet held receipt has no exact fills')
        for trade_id in fills:
            key = (row['connector_name'], pair, row['client_order_id'], str(trade_id))
            require(key not in seen_fills, 'fleet native fill is duplicated across ownership rows')
            seen_fills.add(key)
        amount = _validated_receipt_net_base(receipt, pair, row['order_side'])
        corroborate_order_receipt(receipt, pair, row['client_order_id'], row['order_side'], native_fills)
        require(amount == position_hold_net_base(receipt, pair.split('-')[0]),
                'fleet held receipt net amount differs from native held accounting')
        asset = pair.split('-')[0]
        totals[asset] += amount
        ledger_by_executor[row['executor_id']] += amount

    executor_rows = connection.execute('SELECT * FROM Executors ORDER BY id').fetchall()
    active_ids = set()
    for row in executor_rows:
        custom = json.loads(row['custom_info'] or '{}')
        require(isinstance(custom, dict), 'fleet executor custom state is malformed')
        remaining_value = custom.get('remaining_position_amount_base')
        remaining = decimal(remaining_value) if remaining_value is not None else Decimal(0)
        active = row['status'] in (2, 3)
        if active:
            require(row['is_active'] == 1 and row['close_type'] is None,
                    'fleet active executor lifecycle is inconsistent')
            config = json.loads(row['config'])
            require(config.get('connector_name') == 'okx' and '-' in config.get('trading_pair', '')
                    and config.get('leverage', 1) in (1, '1', None),
                    'fleet active executor is not supported spot inventory')
            pair = config['trading_pair']
            if risk_controllers is not None:
                require(row['controller_id'] in risk_controllers,
                        'fleet active executor controller is outside native risk metadata')
            snapshots = custom.get('execution_order_snapshots')
            require(isinstance(snapshots, list) and snapshots,
                    'fleet active executor lacks exact order snapshots')
            net = Decimal(0)
            local_seen = set()
            for snapshot in snapshots:
                order = InFlightOrder.from_json(snapshot)
                require(order.trading_pair == pair and order.current_state is OrderState.FILLED
                        and order.executed_amount_base == order.amount,
                        'fleet active executor contains an open or partial order')
                order_record = connection.execute('SELECT config_file_path FROM "Order" WHERE id=?',
                                                  (order.client_order_id,)).fetchone()
                require(order_record is not None and order_record['config_file_path'] == script_config,
                        'fleet active native Order is not bound to its owner script')
                order_payload = order.to_json()
                order_fills = order_payload.get('order_fills') or {}
                fill_keys = {(order.client_order_id, str(trade_id)) for trade_id in order_fills}
                require(fill_keys and not (local_seen & fill_keys),
                        'fleet active executor contains duplicate or missing fill identities')
                local_seen.update(fill_keys)
                key_prefix = ('okx', pair)
                for client_id, trade_id in fill_keys:
                    key = (*key_prefix, client_id, trade_id)
                    require(key not in seen_fills, 'fleet executor fill duplicates a held-ledger receipt')
                    seen_fills.add(key)
                receipt = dict(order_payload, trade_type=order.trade_type.name,
                               executed_amount_base=str(order.executed_amount_base))
                corroborate_order_receipt(receipt, pair, order.client_order_id, order.trade_type.name, native_fills)
                net += _validated_receipt_net_base(receipt, pair, order.trade_type.name)
            require(remaining > 0 and net == remaining,
                    'fleet active executor remaining inventory differs from exact fills')
            asset = pair.split('-')[0]
            totals[asset] += net
            active_ids.add(row['id'])
        elif remaining > 0:
            require(row['status'] == 4 and row['close_type'] == 10
                    and ledger_by_executor[row['id']] == remaining,
                    'fleet terminal positive inventory lacks matching held-ledger ownership')
        else:
            require(row['status'] == 4 and row['is_active'] == 0 and row['close_type'] is not None,
                    'fleet terminal executor has unresolved lifecycle ownership')

    require(active_ids == {row['id'] for row in executor_rows if row['status'] in (2, 3)},
            'fleet active executor classification is incomplete')
    # Position is a managed/held projection and may omit completed physical dust.
    position_rows = connection.execute('SELECT connector_name, trading_pair, amount FROM Position').fetchall()
    projected = defaultdict(Decimal)
    for row in position_rows:
        require(row['connector_name'] == 'okx' and '-' in row['trading_pair'],
                'fleet position projection has unsupported owner scope')
        # Position.amount uses SqliteDecimal(6), so the raw SQLite integer is micro-base.
        amount = decimal(row['amount']) / Decimal(1_000_000)
        require(amount >= 0, 'fleet position projection has negative spot inventory')
        if amount:
            projected[row['trading_pair'].split('-')[0]] += amount
    for asset, amount in projected.items():
        require(amount > 0 and abs(amount - totals[asset]) < Decimal('0.000001'),
                'fleet positive position projection is not explained by exact receipts')
    for asset, amount in dust_totals.items():
        totals[asset] += amount
    require_inventory_conservation(totals, fill_totals)
    result = {asset: format(amount, 'f') for asset, amount in sorted(totals.items()) if amount}
    require(all(decimal(amount) > 0 for amount in result.values()),
            'fleet ownership contains non-positive net inventory')
    return result


def inspect_fleet_owner_snapshot(path, owner, *, quantity_increments=None):
    """Read and classify one frozen owner snapshot without writing to it."""
    require(owner in FLEET_OWNERS, 'fleet owner is not a supported V2/V3 identity')
    path = Path(path)
    connection, file_hash, tables_hash, before_stat = _owner_snapshot_readonly(path)
    try:
        residuals = _derive_owner_residuals(connection, owner, quantity_increments)
        after_stat = path.stat()
        after_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        require((before_stat.st_dev, before_stat.st_ino, before_stat.st_size, before_stat.st_mtime_ns)
                == (after_stat.st_dev, after_stat.st_ino, after_stat.st_size, after_stat.st_mtime_ns)
                and file_hash == after_hash and not path.is_symlink(),
                'fleet ownership snapshot changed during read-only inspection')
        _require_no_snapshot_sidecars(path)
    finally:
        connection.close()
    return {'owner': owner, 'script_config': FLEET_OWNERS[owner],
            'database_snapshot_path': str(Path(path)), 'database_snapshot_sha256': file_hash,
            'tables_digest': tables_hash, 'owned_residual_by_asset': residuals}


def validate_fleet_ownership(evidence, now, native_keys):
    bundle = evidence.get('fleet_ownership')
    require(isinstance(bundle, dict) and set(bundle) == {
        'schema', 'observed_at', 'expires_at', 'owner_observations', 'sealed_evidence_sha256', 'authenticator'
    } and bundle['schema'] == 1, 'fresh same-account fleet ownership bundle is required')
    verify_fleet_ownership(bundle, native_keys, evidence)
    observed_at, expires_at = decimal(bundle['observed_at']), decimal(bundle['expires_at'])
    now_value = decimal(now)
    require(0 <= now_value - observed_at <= 300
            and observed_at < expires_at <= min(now_value + 300, observed_at + 300),
            'fleet ownership observation is stale or has an excessive expiry')
    observations = bundle['owner_observations']
    require(isinstance(observations, list) and len(observations) == len(FLEET_OWNERS),
            'fleet ownership must contain exactly the V2 and V3 owner observations')
    by_owner = {}
    total = defaultdict(Decimal)
    for item in observations:
        required = {'owner', 'script_config', 'observed_at',
                    'database_snapshot_path', 'database_snapshot_sha256', 'source_identity', 'tables_digest',
                    'owned_residual_by_asset', 'quantity_increments'}
        require(isinstance(item, dict) and set(item) == required,
                'fleet owner observation has an invalid shape')
        owner = item['owner']
        require(owner in FLEET_OWNERS and owner not in by_owner
                and item['script_config'] == FLEET_OWNERS[owner],
                'fleet owner or script mapping differs')
        owner_observed = decimal(item['observed_at'])
        require(observed_at <= owner_observed <= now_value and now_value - owner_observed <= 300
                and owner_observed <= expires_at,
                'fleet owner database observation is stale or outside the bundle interval')
        identity = item['source_identity']
        require(isinstance(identity, dict) and set(identity) == {'container_id', 'image_id', 'engine_source_sha256'}
                and all(isinstance(value, str) and value for value in identity.values())
                and len(identity['engine_source_sha256']) == 64
                and all(char in '0123456789abcdef' for char in identity['engine_source_sha256']),
                'fleet owner source identity is incomplete')
        derived_snapshot = inspect_fleet_owner_snapshot(item['database_snapshot_path'], owner,
                                                        quantity_increments=item['quantity_increments'])
        require(derived_snapshot['database_snapshot_sha256'] == item['database_snapshot_sha256']
                and derived_snapshot['tables_digest'] == item['tables_digest'],
                'fleet owner database snapshot digest differs')
        derived = derived_snapshot['owned_residual_by_asset']
        reported = item['owned_residual_by_asset']
        require(isinstance(reported, dict) and set(reported) == set(derived)
                and all(decimal(reported[asset]) == decimal(derived[asset]) for asset in derived),
                'fleet owner residual differs from native ledger and executor receipts')
        for asset, amount in derived.items():
            total[asset] += decimal(amount)
        by_owner[owner] = item
    require(set(by_owner) == set(FLEET_OWNERS), 'fleet ownership observation is missing an owner')
    return {'bundle': bundle, 'by_owner': by_owner,
            'owned_residual_by_asset': {asset: format(amount, 'f')
                                        for asset, amount in sorted(total.items()) if amount}}


def validate_v1_terminal_inventory_population(connection, selected_terminal_ids, continuity_executor_ids=()):
    """Require every untransferred positive terminal V1 bag to have native ledger ownership."""
    selected_terminal_ids = set(selected_terminal_ids)
    continuity_executor_ids = set(continuity_executor_ids)
    candidates = set()
    held_by_executor = defaultdict(Decimal)
    for row in connection.execute('SELECT executor_id, trading_pair, order_side, order_payload FROM PositionHoldLedger'):
        receipt = json.loads(row['order_payload'])
        pair = row['trading_pair']
        require(isinstance(pair, str) and '-' in pair,
                'V1 held receipt lacks a trading-pair identity')
        receipt = dict(receipt, trade_type=receipt.get('trade_type', row['order_side']))
        held_by_executor[row['executor_id']] += position_hold_net_base(receipt, pair.split('-')[0])
    for row in connection.execute('SELECT * FROM Executors'):
        custom = json.loads(row['custom_info'] or '{}')
        require(isinstance(custom, dict), 'V1 executor custom state is malformed')
        require(row['close_type'] is not None or row['id'] in selected_terminal_ids | continuity_executor_ids,
                'V1 unselected executor has unresolved lifecycle ownership')
        remaining = decimal(custom.get('remaining_position_amount_base', 0))
        if row['status'] == 4 and not row['is_active'] and row['close_type'] is None \
                and row['close_timestamp'] is None and remaining > 0:
            marker = custom.get('restart_suspended')
            if isinstance(marker, dict) and marker.get('reason') == 'planned_restart':
                candidates.add(row['id'])
        if remaining > 0 and row['id'] not in selected_terminal_ids | continuity_executor_ids:
            require(row['status'] == 4 and row['close_type'] == 10
                    and abs(held_by_executor[row['id']] - remaining) < Decimal('0.000001'),
                    'V1 positive terminal inventory lacks complete held-ledger ownership')
    require(candidates - continuity_executor_ids == selected_terminal_ids
            and candidates <= selected_terminal_ids | continuity_executor_ids,
            'V1 planned-suspended terminal inventory is not the exact singleton selection')


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


def validate_suspended_terminal_position(preflight, stopped):
    """Validate one explicitly suspended, full-entry executor stopped by its owner.

    This exceptional path retains the original terminal row and requires the
    preflight/stopped pair to prove that the same planned-restart suspension
    owns exactly one filled BUY entry and no exit lifecycle. It is separate from
    the normal POSITION_HOLD transfer contract because its inventory has not
    been written to PositionHoldLedger.
    """
    preflight, stopped = decode_row(preflight), decode_row(stopped)
    require(stopped.get('type') == 'position_executor', 'only PositionExecutor is supported')
    require(preflight.get('id') == stopped.get('id')
            and preflight.get('controller_id') == stopped.get('controller_id')
            and preflight.get('timestamp') == stopped.get('timestamp')
            and preflight.get('config') == stopped.get('config'),
            'suspended preflight owner/configuration differs from stopped executor')
    preflight_stopped = (preflight.get('status') == 4 and not preflight.get('is_active')
                         and preflight.get('close_type') is None)
    require(stopped.get('status') == 4 and not stopped.get('is_active')
            and stopped.get('close_type') is None and stopped.get('close_timestamp') is None,
            'suspended terminal executor must be inactive with no close type or close timestamp')
    require(preflight_stopped and preflight.get('close_timestamp') is None,
            'suspended terminal preflight must already be inactive and unclosed')
    require(preflight == stopped,
            'terminal preflight must match stopped row exactly')
    before_custom = dict(preflight.get('custom_info') or {})
    after_custom = dict(stopped.get('custom_info') or {})
    stopped_marker = after_custom.get('restart_suspended')
    require(isinstance(stopped_marker, dict) and stopped_marker.get('reason') == 'planned_restart',
            'stopped row must retain the explicit planned-restart suspension marker')
    before_marker = before_custom.get('restart_suspended')
    require(isinstance(before_marker, dict) and before_marker.get('reason') == 'planned_restart',
            'preflight must retain its original planned-restart marker')
    for custom in (before_custom, after_custom):
        marker = custom.get('restart_suspended')
        require(not custom.get('held_position_orders'),
                'suspended terminal executor was already transferred to held ownership')
        require(not custom.get('exit_trigger_reason')
                and decimal(custom.get('close_order_filled_amount_base', 0)) == 0,
                'suspended entry has an exit trigger or fill')
    snapshots = after_custom.get('execution_order_snapshots')
    before_snapshots = before_custom.get('execution_order_snapshots')
    require(isinstance(snapshots, list) and len(snapshots) == 1
            and isinstance(before_snapshots, list) and len(before_snapshots) == 1,
            'one exact entry snapshot is required')
    order = InFlightOrder.from_json(snapshots[0])
    before_order = InFlightOrder.from_json(before_snapshots[0])
    require(order.to_json() == before_order.to_json(), 'preflight entry receipt differs from stopped entry')
    require(order.trade_type is TradeType.BUY and order.current_state is OrderState.FILLED
            and order.executed_amount_base == order.amount and order.amount > 0
            and order.trading_pair == (stopped.get('config') or {}).get('trading_pair')
            and bool(order.exchange_order_id),
            'only one fully filled terminal BUY entry is supported')
    for custom, marker in ((before_custom, before_marker), (after_custom, stopped_marker)):
        if marker is not None:
            try:
                suspended_remaining = decimal(marker.get('remaining_position_amount_base'))
                executor_remaining = decimal(custom.get('remaining_position_amount_base'))
            except (ArithmeticError, TypeError, ValueError):
                raise ValueError('suspension remaining amount is unavailable') from None
            require(suspended_remaining == executor_remaining,
                    'suspension remaining amount differs from executor receipt')
    expected_ids = [order.client_order_id]
    require(after_custom.get('order_ids') == expected_ids
            and before_custom.get('order_ids') == expected_ids,
            'entry ownership is not a singleton or includes an exit order')
    # The general exact-receipt validator is shared with the POSITION_HOLD
    # handoff path. Add the entry snapshot only to these in-memory copies; never
    # manufacture a persisted held ledger row.
    before_custom['held_position_orders'] = [before_snapshots[0]]
    after_custom['held_position_orders'] = [snapshots[0]]
    normalized_preflight = dict(preflight, custom_info=before_custom)
    normalized_stopped = dict(stopped, custom_info=after_custom)
    config = parse_position_config(stopped['config'])
    require(stopped.get('controller_id') == 'ok-rsi-bnb' and config.controller_id == 'ok-rsi-bnb'
            and config.connector_name == 'okx' and config.trading_pair == 'BNB-USDC',
            'suspended terminal mode is restricted to the V1 BNB owner')
    result = validate_position_receipts(normalized_preflight, normalized_stopped)
    result['preflight'] = preflight
    result['stopped_digest'] = digest(stopped)
    result['resume_kind'] = 'restart_suspended_terminal_v1'
    return result


def validate_nonselected_suspended_continuity(preflight, stopped):
    """Seal one unchanged SOL executor suspended by the normal V1 restart path."""
    preflight, stopped = decode_row(preflight), decode_row(stopped)
    require(preflight.get('id') == stopped.get('id')
            and preflight.get('controller_id') == stopped.get('controller_id')
            and preflight.get('timestamp') == stopped.get('timestamp')
            and preflight.get('type') == stopped.get('type') == 'position_executor'
            and preflight.get('config') == stopped.get('config'),
            'nonselected continuity executor identity/configuration changed')
    terminalized = (stopped.get('status') == 4 and stopped.get('is_active') in (False, 0)
                    and stopped.get('is_trading') in (False, 0))
    require(preflight.get('status') == 2 and stopped.get('status') in (2, 4)
            and preflight.get('is_active') in (True, 1)
            and (terminalized or stopped.get('is_active') in (True, 1))
            and (not terminalized or preflight.get('is_trading') in (True, 1))
            and preflight.get('close_type') is None and stopped.get('close_type') is None
            and preflight.get('close_timestamp') is None and stopped.get('close_timestamp') is None,
            'nonselected continuity executor is not RUNNING and restart-suspended')
    before_custom, after_custom = dict(preflight.get('custom_info') or {}), dict(stopped.get('custom_info') or {})
    before_marker = before_custom.pop('restart_suspended', None)
    after_marker = after_custom.pop('restart_suspended', None)
    require(before_marker is None,
            'nonselected continuity preflight already has a restart-suspension marker')
    require(
        isinstance(after_marker, dict) and set(after_marker) == {
            'timestamp', 'wall_timestamp', 'reason', 'remaining_position_amount_base'
        } and after_marker.get('reason') == 'planned_restart',
        'nonselected continuity row lacks the exact planned-restart marker',
    )
    for key in SOL_MARK_ONLY_CUSTOM:
        require(key in before_custom and key in after_custom,
                'nonselected SOL mark projection is incomplete')
        before_custom.pop(key)
        after_custom.pop(key)
    require(before_custom == after_custom,
            'nonselected continuity executor state, fills, orders, or trailing state changed')
    before_outer, after_outer = dict(preflight), dict(stopped)
    before_outer.pop('custom_info', None)
    after_outer.pop('custom_info', None)
    for key in SOL_MARK_ONLY_OUTER:
        require(key in before_outer and key in after_outer,
                'nonselected SOL PnL projection is incomplete')
        before_outer.pop(key)
        after_outer.pop(key)
    if terminalized:
        for key in ('status', 'is_active', 'is_trading'):
            after_outer[key] = before_outer[key]
    require(before_outer == after_outer,
            'nonselected continuity executor lifecycle or accounting fields changed')
    try:
        marker_time = decimal(after_marker['timestamp'])
        wall_time = decimal(after_marker['wall_timestamp'])
        marker_amount = decimal(after_marker['remaining_position_amount_base'])
        remaining = decimal(after_custom['remaining_position_amount_base'])
    except (ArithmeticError, KeyError, TypeError, ValueError):
        raise ValueError('nonselected continuity suspension timestamp or remaining amount is invalid') from None
    require(marker_time.is_finite() and wall_time.is_finite() and marker_time >= 0
            and wall_time >= marker_time and marker_amount > 0
            and marker_amount == remaining,
            'nonselected continuity suspension remaining amount differs from its executor state')
    config = parse_position_config(stopped['config'])
    require(stopped.get('controller_id') == 'ok-rsi-sol' and config.controller_id == 'ok-rsi-sol'
            and config.connector_name == 'okx' and config.trading_pair == 'SOL-USDC',
            'nonselected continuity is restricted to the identified V1 SOL owner')
    info = SimpleNamespace(id=stopped['id'], type=stopped['type'], status=2,
                           close_type=stopped['close_type'], controller_id=stopped['controller_id'],
                           config=config, custom_info={**after_custom, 'restart_suspended': after_marker})
    try:
        plan = continuity_plan(info)
    except (RestartContinuityError, ValueError, TypeError, KeyError, ArithmeticError) as error:
        raise ValueError(f'nonselected SOL executor is not restorable: {type(error).__name__}') from None
    require(plan is not None and plan.suspended and plan.status.name == 'RUNNING'
            and plan.close_type is None and plan.open_exit_order is None
            and len(plan.entry_orders) == 1 and len(plan.exit_orders) == 0
            and plan.entry_orders[0].current_state is OrderState.FILLED
            and plan.entry_orders[0].executed_amount_base == plan.entry_orders[0].amount
            and plan.entry_orders[0].amount > 0,
            'nonselected SOL executor does not satisfy native restart-continuity requirements')
    validate_nonselected_sol_mark_projection(preflight, stopped, plan.entry_orders[0], config)
    return {'executor_id': stopped['id'], 'preflight': preflight, 'stopped_digest': digest(stopped),
            'remaining_position_amount_base': str(remaining), 'controller_id': stopped['controller_id'],
            'connector_name': config.connector_name, 'trading_pair': config.trading_pair,
            'restart_suspended': after_marker}


def validate_nonselected_sol_mark_projection(preflight, stopped, order, config):
    """Only current mark and elapsed age may drift; fill-net cost and trailing state stay sealed."""
    before, after = preflight['custom_info'], stopped['custom_info']
    price = decimal(after['current_market_price'])
    require(price > 0 and decimal(after['close_price']) == price
            and decimal(after['trailing_current_price']) == price,
            'nonselected SOL current mark projection differs')
    before_age, after_age = decimal(before['open_order_age_seconds']), decimal(after['open_order_age_seconds'])
    require(0 <= before_age <= after_age and math.isfinite(float(after_age)),
            'nonselected SOL open-order age moved backward or is invalid')
    receipt = order.to_json()
    gross_quote = decimal(receipt['executed_amount_quote'])
    cost = gross_quote - base_fee_quote_amount(receipt, 'SOL')
    owned = decimal(after['remaining_position_amount_base'])
    expected_owned = decimal(receipt['executed_amount_base']) - base_fee_amount(receipt, 'SOL')
    fee_quote = decimal(receipt['cumulative_fee_paid_quote'])
    require(cost > 0 and owned > 0 and owned == expected_owned
            and math.isfinite(float(cost)) and math.isfinite(float(fee_quote))
            and float(decimal(stopped['filled_amount_quote'])) == float(cost)
            and float(decimal(stopped['cum_fees_quote'])) == float(fee_quote),
            'nonselected SOL acquisition cost or native fee projection differs')
    # PositionExecutor uses fee-net cost per owned unit and charges total valued
    # fees once. SQLite stores these report-only columns as binary floats.
    basis = cost / owned
    fee_pct = fee_quote / cost
    trailing = config.triple_barrier_config.trailing_stop
    require(trailing is not None, 'nonselected SOL trailing configuration disappeared')
    activation = decimal(after['trailing_activation_price'])
    require(activation == basis * (Decimal(1) + fee_pct + decimal(trailing.activation_price))
            and decimal(after['distance_to_trailing_activation_pct'])
            == max(activation - price, Decimal(0)) / price,
            'nonselected SOL activation projection differs from sealed cost or current mark')
    trigger = after['trailing_trigger_price']
    trigger_pct = after['trailing_stop_trigger_pct']
    if trigger is None:
        require(trigger_pct is None and after['distance_to_trailing_trigger_pct'] is None,
                'nonselected SOL trigger distance exists without an armed trigger')
    else:
        trigger_price = decimal(trigger)
        require(trigger_pct is not None
                and trigger_price == basis * (Decimal(1) + fee_pct + decimal(trigger_pct))
                and decimal(after['distance_to_trailing_trigger_pct'])
                == max(price - trigger_price, Decimal(0)) / price,
                'nonselected SOL trigger projection differs from sealed cost or current mark')
    expected_quote = ((price - basis) / basis) * cost - fee_quote
    expected_pct = expected_quote / cost
    require(math.isfinite(float(expected_quote)) and math.isfinite(float(expected_pct))
            and float(decimal(stopped['net_pnl_quote'])) == float(expected_quote)
            and float(decimal(stopped['net_pnl_pct'])) == float(expected_pct),
            'nonselected SOL mark PnL differs from sealed fill cost and fee')


def validate_continuity_venue_order(native_order, venue):
    """Match a suspended SOL entry snapshot to its fresh raw OKX order and fills."""
    require(isinstance(venue, dict) and set(venue) == {
        'raw_order', 'raw_fills', 'receipt', 'local_fill_comparisons'
    }, 'fresh SOL venue order proof has an invalid shape')
    raw_order = venue['raw_order']
    require(isinstance(raw_order, dict)
            and raw_order.get('clOrdId') == native_order.client_order_id
            and raw_order.get('ordId') == native_order.exchange_order_id
            and raw_order.get('instId') == native_order.trading_pair
            and raw_order.get('side') == 'buy' and raw_order.get('state') == 'filled'
            and raw_order.get('tdMode') == 'cash'
            and decimal(raw_order.get('sz')) == native_order.amount
            and decimal(raw_order.get('accFillSz')) == native_order.executed_amount_base,
            'fresh SOL venue order identity or cumulative fill differs')
    local_fills = native_order.to_json().get('order_fills') or {}
    raw_fills = venue['raw_fills']
    require(isinstance(raw_fills, list) and len(raw_fills) == len(local_fills) > 0
            and {str(row.get('tradeId')) for row in raw_fills} == set(local_fills),
            'fresh SOL venue fill identities differ')
    base, quote = native_order.trading_pair.split('-')
    net_base = Decimal(0)
    for raw in raw_fills:
        trade_id = str(raw['tradeId'])
        fill = local_fills[trade_id]
        require(raw.get('ordId') == native_order.exchange_order_id
                and raw.get('instId') == native_order.trading_pair and raw.get('side') == 'buy'
                and decimal(raw.get('fillSz')) == decimal(fill['fill_base_amount'])
                and decimal(raw.get('fillPx')) == decimal(fill['fill_price'])
                and decimal(fill.get('fill_quote_amount', 0))
                == decimal(raw['fillSz']) * decimal(raw['fillPx']),
                'fresh SOL venue fill values differ from native receipt')
        fee = fill['fee']
        require(decimal(fee.get('percent', 0)) == 0,
                'fresh SOL fee-token receipt is required')
        native_fees = {}
        for flat in fee['flat_fees']:
            native_fees[flat['token']] = native_fees.get(flat['token'], Decimal(0)) + decimal(flat['amount'])
        native_fees = {token: amount for token, amount in native_fees.items() if amount}
        paid = -decimal(raw.get('fee'))
        require(paid >= 0 and raw.get('feeCcy') in (base, quote)
                and native_fees == ({raw['feeCcy']: paid} if paid else {}),
                'fresh SOL venue fee amount or currency differs from native receipt')
        net_base += decimal(raw['fillSz']) - (paid if raw['feeCcy'] == base else Decimal(0))
    receipt = venue['receipt']
    require(isinstance(receipt, dict) and receipt.get('blockers') == [],
            'fresh SOL venue receipt contains blockers')
    comparisons = venue['local_fill_comparisons']
    require(isinstance(comparisons, list) and len(comparisons) == len(local_fills)
            and all(isinstance(row, dict) and row.get('matched') is True for row in comparisons),
            'fresh SOL local fill comparison is incomplete or mismatched')
    return net_base


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


def validate_receipts(connection, position, proof, *, pending_hold=False, suspended_terminal=False):
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
    if suspended_terminal:
        executor_ledger = connection.execute('SELECT 1 FROM PositionHoldLedger WHERE executor_id=? LIMIT 1',
                                             (position['executor_id'],)).fetchone()
        require(position.get('resume_kind') == 'restart_suspended_terminal_v1' and not ledger and not executor_ledger,
                'suspended terminal entry must not already have a held ledger owner')
    else:
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
        inventory += position_hold_net_base(receipt, config['trading_pair'].split('-')[0])
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


def validate_reconciliation(db_path, evidence, now, *, terminal_suspended_executor_ids=(),
                            continuity_executor_ids=(), preflight_db_path=None):
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
    # Validation may supplement a proof from the stopped DB; never mutate the
    # caller's HMAC-authenticated evidence while normalizing it.
    selected = {proof['executor_id']: dict(proof) for proof in evidence['executors']}
    terminal_suspended_executor_ids = set(terminal_suspended_executor_ids)
    continuity_executor_ids = set(continuity_executor_ids)
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
    continuity_evidence = evidence.get('continuity_executors', [])
    require(isinstance(continuity_evidence, list)
            and all(isinstance(row, dict) and set(row) == {'executor_id'} for row in continuity_evidence)
            and {row['executor_id'] for row in continuity_evidence} == continuity_executor_ids
            and len(continuity_evidence) == len(continuity_executor_ids),
            'explicit nonselected SOL executor reconciliation selection differs')
    raw_continuity = report.get('continuity_executors', [])
    require(isinstance(raw_continuity, list)
            and all(isinstance(row, dict)
                    and set(row) == {'executor_id', 'controller_id', 'pair', 'venue_orders'}
                    for row in raw_continuity)
            and {row['executor_id'] for row in raw_continuity} == continuity_executor_ids
            and len(raw_continuity) == len(continuity_executor_ids),
            'fresh raw SOL venue order selection differs')
    if continuity_executor_ids:
        require(preflight_db_path is not None,
                'fresh SOL venue comparison requires the bound preflight database')
        raw_by_id = {row['executor_id']: row for row in raw_continuity}
        with connect(preflight_db_path) as preflight, connect(db_path) as stopped:
            for executor_id in sorted(continuity_executor_ids):
                before = preflight.execute('SELECT * FROM Executors WHERE id=?', (executor_id,)).fetchone()
                after = stopped.execute('SELECT * FROM Executors WHERE id=?', (executor_id,)).fetchone()
                continuity = validate_nonselected_suspended_continuity(before, after)
                row = raw_by_id[executor_id]
                require(row['controller_id'] == continuity['controller_id']
                        and row['pair'] == continuity['trading_pair'] == 'SOL-USDC'
                        and len(row['venue_orders']) == 1,
                        'fresh SOL venue owner or order scope differs')
                custom = json.loads(after['custom_info'] or '{}')
                snapshots = custom.get('execution_order_snapshots')
                require(isinstance(snapshots, list) and len(snapshots) == 1,
                        'native SOL continuity entry snapshot is not a singleton')
                native_order = InFlightOrder.from_json(snapshots[0])
                net_base = validate_continuity_venue_order(native_order, row['venue_orders'][0])
                require(net_base == decimal(continuity['remaining_position_amount_base']),
                        'fresh SOL venue net entry differs from suspended inventory')
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
    selected_order_ids = set()
    if terminal_suspended_executor_ids:
        require(terminal_suspended_executor_ids <= set(selected),
                'suspended terminal venue proof is outside the explicit executor selection')
        for executor_id in selected:
            with connect(db_path) as connection:
                record = connection.execute('SELECT controller_id, config, custom_info FROM Executors WHERE id=?',
                                            (executor_id,)).fetchone()
            require(record is not None, 'selected executor is missing from stopped database')
            custom = json.loads(record['custom_info'] or '{}')
            selected_order_ids.update(custom.get('order_ids') or [])
    if continuity_executor_ids:
        require(terminal_suspended_executor_ids and preflight_db_path is not None,
                'nonselected continuity rows require the explicit terminal mode and preflight snapshot')
        with connect(preflight_db_path) as preflight, connect(db_path) as connection:
            for executor_id in sorted(continuity_executor_ids):
                before = preflight.execute('SELECT * FROM Executors WHERE id=?', (executor_id,)).fetchone()
                after = connection.execute('SELECT * FROM Executors WHERE id=?', (executor_id,)).fetchone()
                require(before is not None and after is not None,
                        'nonselected continuity executor is missing from preflight or stopped database')
                continuity = validate_nonselected_suspended_continuity(before, after)
                marker = continuity['restart_suspended']
                require(decimal(marker['wall_timestamp']) <= finished <= now,
                        'nonselected SOL suspension must predate fresh stopped-owner reconciliation')
                base = continuity['trading_pair'].split('-')[0]
                expected_balances[base] = (expected_balances.get(base, Decimal(0))
                                           + decimal(continuity['remaining_position_amount_base']))
                custom = json.loads(after['custom_info'] or '{}')
                selected_order_ids.update(custom.get('order_ids') or [])
    if terminal_suspended_executor_ids:
        with connect(db_path) as connection:
            validate_v1_terminal_inventory_population(
                connection, terminal_suspended_executor_ids, continuity_executor_ids)
            dust_totals, fill_totals, native_fills = native_inventory_proof(
                connection, 'ok_rsi.yml', evidence.get('quantity_increments'))
            seen_fills = set()
            for row in connection.execute('SELECT * FROM PositionHoldLedger ORDER BY id'):
                require(row['client_order_id'] not in selected_order_ids,
                        'selected or active V1 executor is duplicated in held inventory')
                receipt = json.loads(row['order_payload'])
                pair = row['trading_pair']
                require(row['connector_name'] == 'okx',
                        'V1 held inventory has unsupported connector ownership')
                for trade_id in receipt.get('order_fills', {}):
                    key = (pair, row['client_order_id'], str(trade_id))
                    require(key not in seen_fills, 'V1 held fill is duplicated across ownership rows')
                    seen_fills.add(key)
                net_base = _validated_receipt_net_base(receipt, pair, row['order_side'])
                corroborate_order_receipt(receipt, pair, row['client_order_id'], row['order_side'], native_fills)
                require(net_base == position_hold_net_base(receipt, pair.split('-')[0]),
                        'V1 held receipt differs from exact fill accounting')
                base = pair.split('-')[0]
                expected_balances[base] = expected_balances.get(base, Decimal(0)) + net_base
            for asset, amount in dust_totals.items():
                expected_balances[asset] = expected_balances.get(asset, Decimal(0)) + amount
            require_inventory_conservation(expected_balances, fill_totals)
    fleet_ownership = None
    if terminal_suspended_executor_ids:
        fleet_ownership = validate_fleet_ownership(evidence, now, Security.api_keys('okx'))
        require(report.get('fleet_ownership_authenticator') == fleet_ownership['bundle']['authenticator'],
                'stopped venue reconciliation is not bound to the fresh fleet ownership bundle')
        for asset, amount in fleet_ownership['owned_residual_by_asset'].items():
            expected_balances[asset] = expected_balances.get(asset, Decimal(0)) + decimal(amount)
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


def validate_continuity_bindings(connection, continuity_rows):
    require(isinstance(continuity_rows, list), 'sealed nonselected continuity list is invalid')
    executor_ids = [row.get('executor_id') for row in continuity_rows]
    require(all(isinstance(item, str) and item for item in executor_ids)
            and len(executor_ids) == len(set(executor_ids)),
            'sealed nonselected continuity identities are invalid')
    for proof in continuity_rows:
        stopped = connection.execute('SELECT * FROM Executors WHERE id=?', (proof['executor_id'],)).fetchone()
        require(stopped is not None, 'sealed nonselected continuity executor disappeared')
        current = validate_nonselected_suspended_continuity(proof.get('preflight'), stopped)
        require(current['stopped_digest'] == proof.get('stopped_digest')
                and current['remaining_position_amount_base'] == proof.get('remaining_position_amount_base')
                and current['controller_id'] == proof.get('controller_id')
                and current['connector_name'] == proof.get('connector_name')
                and current['trading_pair'] == proof.get('trading_pair'),
                'sealed nonselected continuity executor differs from stopped state')
    return continuity_rows


def risk_state_digest(connection, script_config, expected_controllers):
    state = risk_state_value(connection, script_config)
    controllers = set(expected_controllers)
    paused = state.get('operator_paused') if isinstance(state, dict) else None
    require(isinstance(state, dict) and type(state.get('version')) is int and state.get('version') == 1
            and isinstance(state.get('controllers'), list)
            and set(state['controllers']) == controllers
            and isinstance(state.get('max_pnl_by_controller'), dict)
            and set(state['max_pnl_by_controller']) == controllers
            and isinstance(state.get('drawdown_exited_controllers'), list)
            and len(state['drawdown_exited_controllers']) == len(set(state['drawdown_exited_controllers']))
            and set(state['drawdown_exited_controllers']) <= controllers
            and type(state.get('stopped')) is bool and state['stopped'] is False
            and isinstance(paused, dict) and set(paused) == controllers
            and all(type(value) is bool and value for value in paused.values())
            and (state.get('daily_entry_risk') is None),
            'all configured V1 controller entries must be durably paused')
    try:
        peak_values = [decimal(state['max_global_pnl'])]
        peak_values.extend(decimal(value) for value in state['max_pnl_by_controller'].values())
    except (ArithmeticError, KeyError, TypeError, ValueError):
        raise ValueError('durable V1 risk peak state is malformed') from None
    require(all(value.is_finite() for value in peak_values), 'durable V1 risk peak state is non-finite')
    watermarks = state.get('operator_watermarks', {})
    require(isinstance(watermarks, dict) and set(watermarks) <= controllers,
            'durable V1 operator watermark scope is malformed')
    for value in watermarks.values():
        require(isinstance(value, dict) and set(value) == {'requested_at', 'command_id'}
                and isinstance(value['requested_at'], (int, float))
                and not isinstance(value['requested_at'], bool)
                and decimal(value['requested_at']) >= 0
                and isinstance(value['command_id'], str) and 1 <= len(value['command_id']) <= 128,
                'durable V1 operator watermark is malformed')
    return digest(state)


def risk_state_value(connection, script_config):
    require('Metadata' in tables(connection), 'durable V1 strategy risk state is unavailable')
    row = connection.execute('SELECT value FROM Metadata WHERE key=?',
                             (f'strategy-risk:{script_config}',)).fetchone()
    require(row is not None, 'durable V1 strategy risk state is unavailable')
    try:
        state = json.loads(row['value'])
    except (TypeError, ValueError):
        raise ValueError('durable V1 risk state is malformed') from None
    require(isinstance(state, dict), 'durable V1 risk state is malformed')
    return state


def staged_daily_entry_limit(script_config, root=ROOT):
    script_path = Path(root) / 'conf/scripts' / script_config
    try:
        script = yaml.safe_load(script_path.read_text())
        value = script.get('max_daily_loss_quote') if isinstance(script, dict) else None
        require(type(value) in (str, int), 'staged V1 daily entry limit must be a quoted decimal or integer')
        limit = decimal(value)
    except (OSError, TypeError, ValueError, ArithmeticError):
        raise ValueError('staged V1 daily entry limit is unavailable or malformed') from None
    require(value is not None and limit.is_finite() and limit > 0,
            'staged V1 daily entry limit is unavailable or malformed')
    return str(limit)


def risk_transition_binding(connection, script_config, controllers, before_state, current_state, configured_limit):
    enrollment = current_state.get('entry_risk_enrollment')
    require(isinstance(enrollment, dict) and isinstance(enrollment.get('command_id'), str),
            'pending native V1 daily enrollment receipt is unavailable')
    archive_key = 'strategy-risk-transition:' + digest([script_config, enrollment['command_id']])
    row = connection.execute('SELECT value FROM Metadata WHERE key=?', (archive_key,)).fetchone()
    require(row is not None, 'native V1 daily-risk transition archive is unavailable')
    try:
        archived = json.loads(row['value'])
    except (TypeError, ValueError):
        raise ValueError('native V1 daily-risk transition archive is malformed') from None
    require(isinstance(archived, dict) and isinstance(archived.get('before'), dict),
            'native V1 daily-risk transition archive is malformed')
    expected_native_state = financial_state_digest_before_prospective_daily_enrollment(
        connection, script_config, archived['before'], archive_key)
    return validate_prospective_daily_enrollment_archive(
        archive_key, row['value'], script_config=script_config, controllers=controllers,
        before_state=before_state, current_state=current_state, configured_limit=configured_limit,
        expected_native_state_sha256=expected_native_state)


def validate_risk_state_binding(connection, manifest, root=ROOT):
    if not manifest.get('terminal_suspended_executor_ids'):
        return None
    controllers = manifest.get('risk_controllers')
    require(isinstance(controllers, list) and controllers,
            'sealed V1 risk controller set is unavailable')
    script_config = manifest['evidence']['script_config']
    current_state = risk_state_value(connection, script_config)
    transition = manifest.get('risk_transition_binding')
    if transition is None:
        current = risk_state_digest(connection, script_config, controllers)
    else:
        require(isinstance(transition, dict), 'sealed V1 daily-risk transition binding is malformed')
        require(bound_files(manifest['evidence']['config_paths'], root) == manifest['files'],
                'V1 source/config binding changed after daily-risk enrollment')
        configured_limit = staged_daily_entry_limit(script_config, root)
        require(configured_limit == transition.get('limit_quote'),
                'staged V1 daily entry limit changed after preparation')
        current = digest(current_state)
        require(current == manifest.get('risk_state_digest'),
                'durable V1 risk state changed after terminal preparation')
        validated = risk_transition_binding(connection, script_config, controllers,
                                            before_state=None, current_state=current_state,
                                            configured_limit=configured_limit)
        require(validated == transition,
                'native V1 daily-risk transition receipt changed after terminal preparation')
    require(current == manifest.get('risk_state_digest'),
            'durable V1 risk state changed after terminal preparation')
    return current


def validate_history_binding(connection, manifest):
    history = consumed_history(connection, pending_id=manifest['id'])
    # Legacy first manifests had no history. Never infer missing history for a later handover.
    require(manifest.get('claimed_history_digest', digest([])) == digest(history),
            'consumed manifest history binding changed')


def prepare(db_path, preflight_path, evidence, now=None, root=ROOT, *,
            legacy_transition_executor_ids=(), terminal_suspended_executor_ids=(),
            continuity_executor_ids=()):
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
    if isinstance(terminal_suspended_executor_ids, str):
        raise ValueError('suspended terminal executor ids must be an explicit collection')
    terminal_suspended_executor_ids = set(terminal_suspended_executor_ids)
    if isinstance(continuity_executor_ids, str):
        raise ValueError('nonselected continuity executor ids must be an explicit collection')
    continuity_executor_ids = set(continuity_executor_ids)
    require(all(isinstance(item, str) and item for item in legacy_transition_executor_ids)
            and legacy_transition_executor_ids <= selected_ids,
            'legacy transition executor id is not in the explicit selection')
    require(all(isinstance(item, str) and item for item in terminal_suspended_executor_ids)
            and terminal_suspended_executor_ids <= selected_ids
            and not (terminal_suspended_executor_ids & legacy_transition_executor_ids),
            'suspended terminal executor id is not in the explicit non-overlapping selection')
    require(all(isinstance(item, str) and item for item in continuity_executor_ids)
            and not (continuity_executor_ids & selected_ids)
            and not (continuity_executor_ids & terminal_suspended_executor_ids),
            'nonselected continuity executor IDs must be explicit and disjoint from the target')
    require(not continuity_executor_ids or terminal_suspended_executor_ids,
            'nonselected continuity rows are admitted only with the V1 terminal singleton mode')
    config_paths = evidence['config_paths']
    require(f"conf/scripts/{evidence['script_config']}" in config_paths
            and 'conf/conf_client.yml' in config_paths and 'conf/connectors/okx.yml' in config_paths
            and any(path.startswith('conf/controllers/') for path in config_paths),
            'script, controller, client and encrypted connector config bindings required')
    script = yaml.safe_load((Path(root) / f"conf/scripts/{evidence['script_config']}").read_text())
    require(script.get('script_file_name') == 'v2_with_controllers.py' and script.get('controllers_config'),
            'native controller script config required')
    if terminal_suspended_executor_ids:
        require(evidence['script_config'] == 'ok_rsi.yml',
                'suspended terminal mode is restricted to the V1 OK-RSI script')
    required_controllers = {f"conf/controllers/{name}" for name in script['controllers_config']}
    require(required_controllers <= set(config_paths), 'all configured controller files must be bound')
    controllers = [yaml.safe_load((Path(root) / name).read_text()) for name in required_controllers]
    require({row['id']: row['trading_pair'] for row in controllers} == evidence['reconciliation']['controller_pairs']
            and all(row['connector_name'] == 'okx' for row in controllers), 'controller owner/pair binding differs')
    if terminal_suspended_executor_ids:
        require(len(terminal_suspended_executor_ids) == 1
                and terminal_suspended_executor_ids == selected_ids,
                'suspended terminal selection must contain only the suspended terminal executor')
        require({row['id']: row['trading_pair'] for row in controllers}.get('ok-rsi-bnb') == 'BNB-USDC',
                'suspended terminal mode requires one executor under the exact V1 BNB controller owner')
        risk_controllers = sorted(row['id'] for row in controllers)
        risk_transition_proof = None
        with connect(preflight_path) as preflight, connect(db_path) as stopped:
            preflight_state = risk_state_value(preflight, evidence['script_config'])
            preflight_risk = risk_state_digest(preflight, evidence['script_config'], risk_controllers)
            current_state = risk_state_value(stopped, evidence['script_config'])
            if current_state.get('version') == 1:
                stopped_risk = risk_state_digest(stopped, evidence['script_config'], risk_controllers)
                require(preflight_risk == stopped_risk,
                        'durable V1 risk state changed between preflight and stopped snapshots')
            else:
                configured_limit = staged_daily_entry_limit(evidence['script_config'], root)
                risk_transition_proof = risk_transition_binding(
                    stopped, evidence['script_config'], risk_controllers,
                    before_state=preflight_state, current_state=current_state,
                    configured_limit=configured_limit)
                require(risk_transition_proof['before_state_sha256'] == preflight_risk,
                        'daily-risk enrollment receipt differs from original preflight state')
                stopped_risk = digest(current_state)
    else:
        risk_controllers = []
        stopped_risk = None
        risk_transition_proof = None
    with connect(preflight_path) as preflight, connect(db_path) as stopped:
        unfinished_ids = {row[0] for row in stopped.execute('SELECT id FROM Executors WHERE status != 4')}
        if terminal_suspended_executor_ids:
            require(unfinished_ids <= continuity_executor_ids,
                    'unfinished executor set differs from explicit nonselected continuity selection')
            for executor_id in sorted(continuity_executor_ids):
                before = preflight.execute('SELECT * FROM Executors WHERE id=?', (executor_id,)).fetchone()
                after = stopped.execute('SELECT * FROM Executors WHERE id=?', (executor_id,)).fetchone()
                require(before is not None and after is not None,
                        'nonselected continuity executor is missing from preflight or stopped database')
                continuity = validate_nonselected_suspended_continuity(before, after)
                marker = continuity['restart_suspended']
                require(decimal(marker['timestamp']) <= decimal(marker['wall_timestamp'])
                        <= decimal(evidence['observed_at']) <= now,
                        'nonselected SOL suspension must predate fresh stopped-owner reconciliation')
        else:
            require(not unfinished_ids and not continuity_executor_ids,
                    'database still has unfinished executors')
    validate_reconciliation(db_path, evidence, now,
                            terminal_suspended_executor_ids=terminal_suspended_executor_ids,
                            continuity_executor_ids=continuity_executor_ids,
                            preflight_db_path=preflight_path)
    files = bound_files(config_paths, root)
    with connect(preflight_path) as preflight, connect(db_path, writable=True) as connection:
        connection.execute('BEGIN IMMEDIATE')
        unfinished_ids = {row[0] for row in connection.execute('SELECT id FROM Executors WHERE status != 4')}
        if terminal_suspended_executor_ids:
            require(unfinished_ids <= continuity_executor_ids,
                    'unfinished executor set differs from explicit nonselected continuity selection')
        else:
            require(not unfinished_ids and not continuity_executor_ids,
                    'database still has unfinished executors')
        continuity_rows = []
        for executor_id in sorted(continuity_executor_ids):
            before = preflight.execute('SELECT * FROM Executors WHERE id=?', (executor_id,)).fetchone()
            after = connection.execute('SELECT * FROM Executors WHERE id=?', (executor_id,)).fetchone()
            require(before is not None and after is not None,
                    'nonselected continuity executor is missing from preflight or stopped database')
            continuity_rows.append(validate_nonselected_suspended_continuity(before, after))
        history = consumed_history(connection)
        require(all(decimal(now) >= decimal(row['claimed_at']) for row in history),
                'new manifest predates consumed history')
        positions = []
        for supplied_proof in evidence['executors']:
            proof = dict(supplied_proof)
            identity = (proof['executor_id'],)
            before = preflight.execute('SELECT * FROM Executors WHERE id=?', identity).fetchone()
            after = connection.execute('SELECT * FROM Executors WHERE id=?', identity).fetchone()
            require(before is not None and after is not None, 'selected executor missing')
            allow_transition = proof['executor_id'] in legacy_transition_executor_ids
            if proof['executor_id'] in terminal_suspended_executor_ids:
                position = validate_suspended_terminal_position(before, after)
                marker = (position['preflight']['custom_info'].get('restart_suspended')
                          or decode_row(after)['custom_info'].get('restart_suspended'))
                try:
                    suspended_at = decimal(marker.get('wall_timestamp'))
                    marker_timestamp = decimal(marker.get('timestamp'))
                except (ArithmeticError, TypeError, ValueError):
                    raise ValueError('planned-restart suspension time is unavailable') from None
                require(marker_timestamp >= decimal(position['config']['timestamp'])
                        and marker_timestamp <= suspended_at <= decimal(evidence['observed_at']) <= decimal(now),
                        'planned-restart suspension must predate fresh stopped-owner reconciliation')
            elif allow_transition:
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
            position['ownership_archive'] = validate_receipts(
                connection, position, proof,
                suspended_terminal=position.get('resume_kind') == 'restart_suspended_terminal_v1')
            positions.append(position)
        manifest = {'schema': 1, 'id': str(uuid.uuid4()), 'created_at': now, 'expires_at': evidence['expires_at'],
                    'database_digest': database_digest(connection), 'preflight_digest': database_digest(preflight),
                    'claimed_history_digest': digest(history), 'continuity_rows': continuity_rows,
                    'terminal_suspended_executor_ids': sorted(terminal_suspended_executor_ids),
                    'risk_controllers': risk_controllers, 'risk_state_digest': stopped_risk,
                    'risk_transition_binding': risk_transition_proof,
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
        validate_continuity_bindings(connection, manifest.get('continuity_rows', []))
        validate_risk_state_binding(connection, manifest, root)
        proofs = {proof['executor_id']: proof for proof in manifest['evidence']['executors']}
        for position in manifest['positions']:
            row = connection.execute('SELECT * FROM Executors WHERE id=?', (position['executor_id'],)).fetchone()
            transition = bool(position.get('legacy_transition'))
            proof = next(item for item in manifest['evidence']['executors']
                         if item['executor_id'] == position['executor_id'])
            require(row is not None, 'sealed executor disappeared before startup')
            if position.get('resume_kind') == 'restart_suspended_terminal_v1':
                current_position = validate_suspended_terminal_position(position['preflight'], row)
            else:
                current_position = validate_position(position['preflight'], row,
                                                     allow_legacy_post_only_repair_transition=transition,
                                                     terminal_proofs=proof.get('entry_reconciliation') if transition else None)
            require(current_position
                    == {key: value for key, value in position.items() if key != 'ownership_archive'},
                    'sealed position payload differs from stopped owner')
            require(validate_receipts(connection, position, proofs[position['executor_id']],
                                      suspended_terminal=(position.get('resume_kind')
                                                          == 'restart_suspended_terminal_v1'))
                    == position['ownership_archive'],
                    'held ownership archive differs')
    return manifest


def claim(db_path, manifest, now=None, root=ROOT):
    now = time.time() if now is None else now
    with connect(db_path, writable=True) as connection:
        connection.execute('BEGIN IMMEDIATE')
        require(manifest['created_at'] <= now < manifest['expires_at'], 'resume manifest expired')
        row = connection.execute(f'SELECT * FROM {TABLE} WHERE id=?', (manifest['id'],)).fetchone()
        require(row is not None and row['state'] == 'prepared' and json.loads(row['payload']) == manifest,
                'resume claim already consumed or changed')
        validate_history_binding(connection, manifest)
        require(database_digest(connection) == manifest['database_digest'], 'database changed before claim')
        validate_continuity_bindings(connection, manifest.get('continuity_rows', []))
        validate_risk_state_binding(connection, manifest, root)
        for position in manifest['positions']:
            record = connection.execute('SELECT * FROM Executors WHERE id=?', (position['executor_id'],)).fetchone()
            require(digest(decode_row(record)) == position['stopped_digest'], 'executor changed before claim')
            for held in position['ownership_archive']['ledger_rows']:
                removed = connection.execute('DELETE FROM PositionHoldLedger WHERE id=? AND executor_id=?',
                                             (held['id'], position['executor_id'])).rowcount
                require(removed == 1, 'selected held ownership row missing during claim')
            aggregates = (() if position.get('resume_kind') == 'restart_suspended_terminal_v1'
                          else position['ownership_archive']['position_rows'])
            for aggregate in aggregates:
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
        for continuation in manifest.get('continuity_rows', []):
            current = connection.execute('SELECT status FROM Executors WHERE id=?',
                                         (continuation['executor_id'],)).fetchone()
            if current is not None and current['status'] == 4:
                changed = connection.execute(
                    'UPDATE Executors SET status=2, is_active=1, is_trading=1 '
                    'WHERE id=? AND status=4 AND is_active=0 AND is_trading=0 '
                    'AND close_type IS NULL AND close_timestamp IS NULL',
                    (continuation['executor_id'],)).rowcount
                require(changed == 1, 'sealed nonselected terminal continuation changed during claim')
        connection.execute(f'UPDATE {TABLE} SET state=\'claimed\', claimed_at=? WHERE id=?', (now, manifest['id']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('inspect', 'prepare'))
    parser.add_argument('--db', required=True)
    parser.add_argument('--preflight-db')
    parser.add_argument('--evidence')
    parser.add_argument('--legacy-post-only-repair-executor-id', action='append', default=[],
                        help='explicit selected executor ID eligible for the exact historical single-repair transition; repeat as needed')
    parser.add_argument('--restart-suspended-terminal-executor-id', action='append', default=[],
                        help='explicit selected executor ID eligible for one sealed restart_suspended terminal entry; repeat as needed')
    parser.add_argument('--restart-continuity-executor-id', action='append', default=[],
                        help='explicit nonselected V1 SOL executor ID eligible for exact planned-restart continuity alongside a terminal singleton')
    parser.add_argument('--password-file', type=Path,
                        help='private file containing the native Hummingbot password; required for terminal-mode prepare')
    parser.add_argument('--runtime-root', type=Path, default=ROOT,
                        help='Final staged source/config tree for prepare bindings; native startup uses its own root')
    args = parser.parse_args()
    if args.action == 'inspect':
        result = inspect_manifest(args.db)
    else:
        require(args.preflight_db and args.evidence, 'prepare requires --preflight-db and --evidence')
        terminal_mode = bool(args.restart_suspended_terminal_executor_id)
        unlocked = False
        try:
            if terminal_mode:
                require(args.password_file is not None,
                        'terminal-mode prepare requires a private native password file')
                unlocked = True
                unlock_native_okx(args.password_file)
            result = prepare(args.db, args.preflight_db, json.loads(Path(args.evidence).read_text()),
                             root=args.runtime_root,
                             legacy_transition_executor_ids=args.legacy_post_only_repair_executor_id or (),
                             terminal_suspended_executor_ids=args.restart_suspended_terminal_executor_id or (),
                             continuity_executor_ids=args.restart_continuity_executor_id or ())
        finally:
            if unlocked:
                Security._secure_configs.clear()
                Security.secrets_manager = None
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
