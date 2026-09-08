"""Inspect an explicit local bot database without loading connectors or changing state."""
import argparse
import json
import sqlite3
from pathlib import Path


def inspect_database(path: Path, config_file: str) -> dict:
    with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute('SELECT id, controller_id, status, config, custom_info FROM Executors').fetchall()
        risk = connection.execute('SELECT value FROM Metadata WHERE key = ?',
                                  ('strategy-risk:' + config_file,)).fetchone()
    unfinished = []
    for row in rows:
        # RunnableStatus.TERMINATED == 4; importing native runtime is unnecessary for this read-only diagnostic.
        if row['status'] == 4:
            continue
        config = json.loads(row['config'])
        custom = json.loads(row['custom_info'])
        unfinished.append({'executor_id': row['id'], 'controller_id': row['controller_id'],
                           'status': row['status'], 'connector': config.get('connector_name'),
                           'trading_pair': config.get('trading_pair'), 'order_ids': custom.get('order_ids', [])})
    return {'database': str(path.resolve()), 'config_file': config_file,
            'unfinished_executors': unfinished, 'risk_state': json.loads(risk['value']) if risk else None,
            'requires_reconciliation': bool(unfinished) or (bool(rows) and risk is None),
            'authority': 'read-only local diagnosis; no exchange reconciliation or activation'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('database', type=Path)
    parser.add_argument('--config-file', required=True, help='Exact recorder config_file_path key')
    args = parser.parse_args()
    print(json.dumps(inspect_database(args.database, args.config_file), indent=2))


if __name__ == '__main__':
    main()
