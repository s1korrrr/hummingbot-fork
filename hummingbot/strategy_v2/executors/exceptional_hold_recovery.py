"""Operator-assisted completion of a proven stopped POSITION_HOLD transfer.

Never restores an executor to RUNNING and never submits an order. Original rows
and removed rounded projections remain in an atomic, append-only receipt.
"""
import argparse
import hashlib
import json
import time
import uuid
from pathlib import Path

import yaml

from hummingbot.strategy_v2.executors import planned_position_resume as resume

TABLE = 'ExceptionalHoldRecovery'
SOURCE = 'hummingbot/strategy_v2/executors/exceptional_hold_recovery.py'


def validate_pending_hold(before, after):
    before, after = resume.decode_row(before), resume.decode_row(after)
    for row in (before, after):
        resume.require(row['status'] == 3 and row['close_type'] == 10
                       and not row['is_active'] and not row.get('is_trading', False),
                       'both snapshots must be inactive SHUTTING_DOWN POSITION_HOLD')
    result = resume.validate_position_receipts(before, after)
    # The incident already existed before the operator stopped the process.
    # No intervening receipt or trailing-state change may be silently discarded.
    for key in ('held_position_orders', 'order_ids', 'remaining_position_amount_base',
                'close_order_filled_amount_base', *resume.TRAILING_KEYS):
        resume.require(before['custom_info'].get(key) == after['custom_info'].get(key),
                       f'pending hold changed since preflight: {key}')
    resume.require(not after['custom_info'].get('exceptional_hold_recovery'), 'hold already recovered')
    return result


def restored_risk_state(before, after):
    """Release only the administrative stop bit; never reset peaks or pauses."""
    resume.require(isinstance(before, dict) and isinstance(after, dict)
                   and before.get('version') == after.get('version') == 1,
                   'risk state version unavailable')
    resume.require(before.get('stopped') is False and after.get('stopped') is True,
                   'risk latch is not a proven administrative transition')
    resume.require(set(before) == set(after)
                   and all(before[k] == after[k] for k in before if k != 'stopped'),
                   'risk state changed beyond administrative stop')
    required = {'controllers', 'drawdown_exited_controllers', 'max_global_pnl',
                'max_pnl_by_controller', 'operator_paused', 'stopped', 'version'}
    resume.require(required <= set(before), 'incomplete original risk state')
    controllers = set(before['controllers'])
    resume.require(set(before['max_pnl_by_controller']) == controllers
                   and set(before['operator_paused']) == controllers
                   and set(before['drawdown_exited_controllers']) <= controllers
                   and all(type(v) is bool for v in before['operator_paused'].values()), 'invalid risk scope')
    for value in [before['max_global_pnl'], *before['max_pnl_by_controller'].values()]:
        resume.decimal(value)
    return dict(before)


def recover(db_path, preflight_path, evidence, *, restore_administrative_stop=False, now=None, root=resume.ROOT):
    """One explicit stopped-owner migration, bound to fresh raw venue evidence."""
    now = time.time() if now is None else now
    resume.require(evidence.get('schema') == 1 and evidence.get('owner_stopped') is True
                   and evidence.get('venue_open_orders') == [], 'stopped owner proof required')
    resume.require(0 <= now - evidence['observed_at'] <= 300
                   and now < evidence['expires_at'] <= now + 900, 'recovery evidence expired')
    resume.require(len(evidence['executors']) == 1, 'exactly one pending hold selection required')
    config_paths = evidence['config_paths']
    resume.require(f"conf/scripts/{evidence['script_config']}" in config_paths
                   and 'conf/conf_client.yml' in config_paths and 'conf/connectors/okx.yml' in config_paths,
                   'script, client and connector bindings required')
    script = yaml.safe_load((Path(root) / f"conf/scripts/{evidence['script_config']}").read_text())
    resume.require(script.get('script_file_name') == 'v2_with_controllers.py'
                   and script.get('controllers_config'), 'native controller script required')
    controller_paths = {f'conf/controllers/{name}' for name in script['controllers_config']}
    resume.require(controller_paths <= set(config_paths), 'all controller configurations must be bound')
    controllers = [yaml.safe_load((Path(root) / name).read_text()) for name in controller_paths]
    resume.require({row['id']: row['trading_pair'] for row in controllers}
                   == evidence['reconciliation']['controller_pairs']
                   and all(row['connector_name'] == 'okx' for row in controllers), 'controller scope differs')
    files = resume.bound_files(config_paths, root)
    files[SOURCE] = hashlib.sha256((Path(root) / SOURCE).read_bytes()).hexdigest()
    resume.require(evidence.get('files') == files, 'source/config binding differs')
    resume.require(evidence.get('preflight_sha256') == hashlib.sha256(Path(preflight_path).read_bytes()).hexdigest(),
                   'preflight snapshot binding differs')
    for phase in ('account_pending_before', 'account_pending_after'):
        pending = evidence['reconciliation'][phase]
        resume.require(pending['regular'] == [] and set(pending['algos']) == set(resume.SPOT_ALGO_TYPES)
                       and all(value == [] for value in pending['algos'].values()),
                       'account-wide venue orders remain or coverage is incomplete')
    resume.validate_reconciliation(db_path, evidence, now)
    proof = evidence['executors'][0]
    with resume.connect(preflight_path) as preflight, resume.connect(db_path, writable=True) as connection:
        connection.execute('BEGIN IMMEDIATE')
        for path, database in ((db_path, connection), (preflight_path, preflight)):
            wal = Path(str(path) + '-wal')
            resume.require(not wal.exists() or wal.stat().st_size == 0, 'stopped snapshot has a live WAL')
            resume.require(database.execute('PRAGMA integrity_check').fetchone()[0] == 'ok',
                           'database integrity check failed')
        resume.require(hashlib.sha256(Path(db_path).read_bytes()).hexdigest()
                       == evidence['reconciliation']['database']['sha256'], 'database changed before recovery lock')
        resume.require(hashlib.sha256(Path(preflight_path).read_bytes()).hexdigest() == evidence['preflight_sha256'],
                       'preflight changed before recovery lock')
        # Excludes overlapping recovery and ordinary resume claims.
        if resume.TABLE in resume.tables(connection):
            resume.require(not connection.execute(f"SELECT id FROM {resume.TABLE} WHERE state != 'claimed'").fetchone(),
                           'another planned resume is pending')
        unfinished = connection.execute('SELECT id FROM Executors WHERE status != 4').fetchall()
        resume.require([row['id'] for row in unfinished] == [proof['executor_id']],
                       'unrelated unfinished executors remain')
        before = preflight.execute('SELECT * FROM Executors WHERE id=?', (proof['executor_id'],)).fetchone()
        after = connection.execute('SELECT * FROM Executors WHERE id=?', (proof['executor_id'],)).fetchone()
        resume.require(before is not None and after is not None, 'selected executor missing')
        position = validate_pending_hold(before, after)
        native = connection.execute('SELECT config_file_path FROM "Order" WHERE id=?',
                                    (position['entry']['client_order_id'],)).fetchone()
        resume.require(native is not None and native[0] == evidence['script_config'], 'script owner differs')
        archive = resume.validate_receipts(connection, position, proof, pending_hold=True)
        risk = None
        if restore_administrative_stop:
            key = f"strategy-risk:{evidence['script_config']}"
            prior = preflight.execute('SELECT value FROM Metadata WHERE key=?', (key,)).fetchone()
            current = connection.execute('SELECT value FROM Metadata WHERE key=?', (key,)).fetchone()
            resume.require(prior is not None and current is not None, 'risk snapshots missing')
            restored = restored_risk_state(json.loads(prior[0]), json.loads(current[0]))
            resume.require(set(restored['controllers']) == {row['id'] for row in controllers},
                           'risk controller scope differs from configuration')
            risk = {'key': key, 'before': current[0], 'restored': restored}
        receipt = {'schema': 1, 'id': str(uuid.uuid4()), 'kind': 'exceptional_stopped_hold_completion',
                   'created_at': now, 'original_preflight': dict(before), 'original_stopped': dict(after),
                   'database_digest_before': resume.database_digest(connection),
                   'preflight_digest': resume.database_digest(preflight), 'files': files,
                   'evidence': evidence, 'ownership_archive': archive, 'risk': risk}
        custom = json.loads(after['custom_info'])
        custom['exceptional_hold_recovery'] = receipt['id']
        # This completes pending held ownership, not fictional normal shutdown.
        connection.execute('UPDATE Executors SET status=4, is_active=0, is_trading=0, custom_info=? WHERE id=?',
                           (resume.canonical(custom), proof['executor_id']))
        # Exact ledger is already durable. Remove only its corroborated stale
        # rounded projection; the native reader reconstructs the full scope.
        for aggregate in archive['position_rows']:
            connection.execute('DELETE FROM Position WHERE id=?', (aggregate['id'],))
        if risk:
            connection.execute('UPDATE Metadata SET value=? WHERE key=?',
                               (resume.canonical(risk['restored']), risk['key']))
        receipt['database_digest_after'] = resume.database_digest(connection)
        connection.execute(f'CREATE TABLE IF NOT EXISTS {TABLE} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        connection.execute(f'INSERT INTO {TABLE} VALUES (?, ?)', (receipt['id'], resume.canonical(receipt)))
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True)
    parser.add_argument('--preflight-db', required=True)
    parser.add_argument('--evidence', required=True)
    parser.add_argument('--runtime-root', type=Path, default=resume.ROOT)
    parser.add_argument('--restore-administrative-stop', action='store_true')
    args = parser.parse_args()
    result = recover(args.db, args.preflight_db, json.loads(Path(args.evidence).read_text()),
                     restore_administrative_stop=args.restore_administrative_stop, root=args.runtime_root)
    print(resume.canonical(result))


if __name__ == '__main__':
    main()
