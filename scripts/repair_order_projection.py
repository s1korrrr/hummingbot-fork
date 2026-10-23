"""Explicit, stopped V2 terminal-order projection maintenance; never places orders.

The owner restart caller must first stop V2, preserve a checkpointed backup and
verify native obligations. Planning uses the existing authenticated GET-only
client. Applying has no network, consumes the exact plan and obtains the same
native writer lock as V2. A committed repair and WAL checkpoint are separate
outcomes; an uncertain result must never permit an automatic owner boot.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import sys
import time
from contextlib import closing
from pathlib import Path

OWNER = 'rsi_modular_v2.yml'
DB = Path('/home/hummingbot/data/rsi_modular_v2.sqlite')
ADMISSION = Path('/admission')
ROOT = Path('/home/hummingbot')
MAX_PLAN_BYTES = 2 * 1024 * 1024


def _require(value, message):
    if not value:
        raise ValueError(message)


def _stack_helpers():
    if '/opt/stack' not in sys.path:
        sys.path.insert(0, '/opt/stack')
    import recovery_missing_order
    import recovery_reconcile
    from v2_recovery import owner_lock
    return recovery_missing_order, recovery_reconcile, owner_lock


def _owner_lock(path):
    return _stack_helpers()[2](path)


def _checked_stopped(path):
    return _stack_helpers()[1].stopped_database(path)


def _rw_connection(path):
    return closing(_stack_helpers()[0]._existing_rw_connection(path))


def _postcommit_checkpoint(path):
    return _stack_helpers()[0].postcommit_checkpoint(path)


def _native_api():
    from hummingbot.strategy_v2.migrations.order_projection import apply_order_projection, plan_order_projection
    return plan_order_projection, apply_order_projection


def _target(connection, order_id):
    rows = connection.execute('SELECT symbol, exchange_order_id FROM "Order" '
                              'WHERE id=? AND config_file_path=? AND market=?',
                              (order_id, OWNER, 'okx')).fetchall()
    _require(len(rows) == 1 and rows[0][0] in _stack_helpers()[0].PAIRS and rows[0][1],
             'Exact native V2 spot order identity required')
    return rows[0][0]


def checkpoint(db, order_id, admission):
    helper = _stack_helpers()[0]
    status = helper._pre_maintenance_checkpoint(
        db, admission, lambda connection: _target(connection, order_id),
        validation_timeout_seconds=180.0)
    return {'order_id': order_id, 'checkpoint_status': status}


async def collect_venue(password_file, order_id, pair):
    """Fresh authenticated spot order/fill GETs; no connector order interface."""
    _, helper, _ = _stack_helpers()
    from entrypoint import read_private

    from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger
    from hummingbot.client.config.security import Security

    _, paths = helper.configs(ROOT)
    before = helper.configuration_hashes(ROOT, paths)
    secret = read_private(password_file, max_bytes=65536).rstrip('\r\n')
    _require(secret and not any(c in secret for c in '\x00\r\n'), 'Invalid native password file')
    _require(Security.login(ETHKeyFileSecretManger(secret)), 'Native credential unlock failed')
    await Security.wait_til_decryption_done()
    keys = Security.api_keys('okx')
    _require(keys, 'Native OKX credentials unavailable')
    connector = await helper.native_readonly_okx_client(keys)
    try:
        response = await asyncio.wait_for(connector._api_get(
            path_url='/api/v5/trade/order', params={'instId': pair, 'clOrdId': order_id},
            is_auth_required=True), timeout=20)
        _require(response.get('code') == '0' and isinstance(response.get('data'), list)
                 and len(response['data']) == 1, 'Exact venue order unavailable')
        order = response['data'][0]
        _require(order.get('ordId'), 'Venue order exchange identity missing')
        response = await asyncio.wait_for(connector._api_get(
            path_url='/api/v5/trade/fills', params={'instType': 'SPOT', 'instId': pair,
                                                    'ordId': order['ordId'], 'limit': '100'},
            is_auth_required=True), timeout=20)
        _require(response.get('code') == '0' and isinstance(response.get('data'), list)
                 and len(response['data']) < 100, 'Complete bounded venue fills required')
        _require(helper.configuration_hashes(ROOT, paths) == before,
                 'Native account/configuration changed during venue collection')
        return {'schema': 1, 'source': 'authenticated_okx_get',
                'observed_at_ms': int(time.time() * 1000), 'order': order, 'fills': response['data']}
    finally:
        await helper.close_readonly_http_session(connector)


def make_plan(db, order_id, admission, password_file):
    helper = _stack_helpers()[0]
    with _owner_lock(admission):
        _checked_stopped(db)
        with closing(helper._existing_ro_connection(db)) as connection:
            pair = _target(connection, order_id)
        configuration = _configuration_hashes()
        proof = asyncio.run(collect_venue(password_file, order_id, pair))
        _require(configuration == _configuration_hashes(), 'Native configuration changed during venue collection')
        with closing(helper._existing_ro_connection(db)) as connection:
            native, _ = _native_api()
            plan = native(connection, order_id=order_id, venue_proof=proof)
        envelope = {'status': 'ORDER_PROJECTION_PLANNED', 'order_id': order_id,
                    'configuration_sha256': configuration, 'native_plan': plan}
        envelope['plan_sha256'] = _seal_envelope(envelope)
        return envelope


def _configuration_hashes():
    _, helper, _ = _stack_helpers()
    _, paths = helper.configs(ROOT)
    result = helper.configuration_hashes(ROOT, paths)
    from entrypoint import read_private
    password = Path('/run/secrets/rsi/config_password')
    # Validate the private path/mode before hashing; never return secret bytes.
    read_private(password, max_bytes=65536)
    result['credential_reference_sha256'] = hashlib.sha256(password.read_bytes()).hexdigest()
    return result


def _seal_envelope(envelope):
    body = {key: value for key, value in envelope.items() if key != 'plan_sha256'}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _validate_envelope(envelope, order_id, expected):
    _require(isinstance(envelope, dict) and envelope.get('status') == 'ORDER_PROJECTION_PLANNED'
             and envelope.get('order_id') == order_id and envelope.get('plan_sha256') == expected
             and _seal_envelope(envelope) == expected
             and isinstance(envelope.get('configuration_sha256'), dict)
             and envelope['configuration_sha256']
             and isinstance(envelope.get('native_plan'), dict)
             and envelope['native_plan'].get('order_id') == order_id
             and re.fullmatch(r'[a-f0-9]{64}', str(envelope['native_plan'].get('plan_sha256', ''))),
             'Exact sealed native order/configuration plan required')


def verify_configuration(admission, envelope, order_id, expected):
    _validate_envelope(envelope, order_id, expected)
    with _owner_lock(admission):
        _require(_configuration_hashes() == envelope['configuration_sha256'],
                 'Native owner configuration or credentials changed after planning')
    return {'status': 'ORDER_PROJECTION_CONFIGURATION_VERIFIED', 'order_id': order_id,
            'plan_sha256': expected}


def apply_plan(db, order_id, admission, envelope, expected):
    _validate_envelope(envelope, order_id, expected)
    with _owner_lock(admission):
        _require(_configuration_hashes() == envelope['configuration_sha256'],
                 'Native owner configuration or credentials changed after planning')
        _checked_stopped(db)
        with _rw_connection(db) as connection:
            _, native = _native_api()
            receipt = native(connection, plan=envelope['native_plan'],
                             expected_plan_sha256=envelope['native_plan']['plan_sha256'])
        status = _postcommit_checkpoint(db)
    return {'status': 'ORDER_PROJECTION_REPAIRED' if status == 'complete'
            else 'ORDER_PROJECTION_REPAIRED_CHECKPOINT_PENDING',
            'order_id': order_id, 'plan_sha256': expected, 'checkpoint_status': status,
            'receipt': receipt}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('checkpoint', 'plan', 'apply', 'verify-config'))
    parser.add_argument('--db', type=Path, required=True)
    parser.add_argument('--order-id', required=True)
    parser.add_argument('--admission', type=Path, required=True)
    parser.add_argument('--password-file', type=Path)
    parser.add_argument('--expected-plan-sha256')
    args = parser.parse_args()
    _require(args.db == DB and args.admission == ADMISSION
             and re.fullmatch(r'[A-Za-z0-9_-]{1,32}', args.order_id),
             'Canonical stopped V2 data/admission and exact native order ID required')
    if args.action == 'checkpoint':
        result = checkpoint(args.db, args.order_id, args.admission)
    elif args.action == 'plan':
        _require(args.password_file == Path('/run/secrets/rsi/config_password'),
                 'Canonical V2 credential reference required')
        result = make_plan(args.db, args.order_id, args.admission, args.password_file)
    else:
        _require(re.fullmatch(r'[a-f0-9]{64}', args.expected_plan_sha256 or ''),
                 'Explicit sealed plan digest required')
        data = sys.stdin.buffer.read(MAX_PLAN_BYTES + 1)
        _require(len(data) <= MAX_PLAN_BYTES, 'Native maintenance plan exceeds bound')
        envelope = json.loads(data)
        result = (verify_configuration(args.admission, envelope, args.order_id, args.expected_plan_sha256)
                  if args.action == 'verify-config' else
                  apply_plan(args.db, args.order_id, args.admission, envelope, args.expected_plan_sha256))
    print(json.dumps(result, sort_keys=True, separators=(',', ':'), allow_nan=False))


if __name__ == '__main__':
    main()
