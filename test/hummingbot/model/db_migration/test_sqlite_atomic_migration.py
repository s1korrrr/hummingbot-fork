import sqlite3
import threading
from pathlib import Path

import pytest
from sqlalchemy import event
from sqlalchemy.exc import SQLAlchemyError

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.model.db_migration.migrator import Migrator
from hummingbot.model.db_migration.transformations import AddExactTradeFillReceipts
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType


@pytest.fixture
def legacy_manager(tmp_path):
    manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                   db_path=str(tmp_path / 'atomic.sqlite'))
    manager.engine.dispose()
    connection = sqlite3.connect(manager.db_path)
    for name in ('exact_amount', 'exact_price', 'exact_trade_fee_in_quote', 'exact_receipt_source'):
        connection.execute(f'ALTER TABLE TradeFill DROP COLUMN {name}')
    connection.execute("UPDATE Metadata SET value='20260327' WHERE key='local_db_version'")
    connection.commit()
    connection.close()
    yield manager
    manager.engine.dispose()


def migrate(manager):
    return Migrator().migrate_db_to_version(ClientConfigAdapter(ClientConfigMap()), manager, 20260327, 20260910)


def test_concurrent_commit_after_snapshot_is_preserved(legacy_manager, monkeypatch):
    original_apply = AddExactTradeFillReceipts.apply
    original_atomic = getattr(AddExactTradeFillReceipts, 'apply_sqlite_transaction', None)
    started = threading.Event()
    finished = threading.Event()
    errors = []
    threads = []

    def write_original():
        connection = sqlite3.connect(legacy_manager.db_path, timeout=5)
        try:
            started.set()
            connection.execute(
                'INSERT INTO TradeFill (config_file_path, strategy, market, symbol, base_asset, quote_asset, '
                'timestamp, order_id, trade_type, order_type, price, amount, leverage, trade_fee, '
                'exchange_trade_id, position, trade_fee_in_quote) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                ('test.yml', 'test', 'okx', 'BTC-USDC', 'BTC', 'USDC', 1789021246000, 'order', 'BUY', 'LIMIT',
                 78207500000, 28, 1, '{}', 'concurrent-fill', 'NIL', 1766))
            connection.commit()
        except Exception as error:
            errors.append(error)
        finally:
            connection.close()
            finished.set()

    def begin_writer():
        worker = threading.Thread(target=write_original)
        threads.append(worker)
        worker.start()
        assert started.wait(1)
        finished.wait(.1)

    def wrapped_apply(self, handle):
        begin_writer()
        return original_apply(self, handle)

    def wrapped_atomic(self, connection):
        begin_writer()
        return original_atomic(self, connection)

    monkeypatch.setattr(AddExactTradeFillReceipts, 'apply', wrapped_apply)
    monkeypatch.setattr(AddExactTradeFillReceipts, 'apply_sqlite_transaction', wrapped_atomic, raising=False)
    inode = Path(legacy_manager.db_path).stat().st_ino
    assert migrate(legacy_manager)
    for worker in threads:
        worker.join(5)
        assert not worker.is_alive()
    assert not errors
    connection = sqlite3.connect(legacy_manager.db_path)
    try:
        assert connection.execute(
            "SELECT amount, price FROM TradeFill WHERE exchange_trade_id='concurrent-fill'"
        ).fetchone() == (28, 78207500000)
        assert connection.execute("SELECT value FROM Metadata WHERE key='local_db_version'").fetchone() == ('20260910',)
    finally:
        connection.close()
    assert Path(legacy_manager.db_path).stat().st_ino == inode


@pytest.mark.parametrize('stage', ['after_column', 'after_version'])
def test_schema_and_version_rollback_together(legacy_manager, stage):
    fired = []

    def fail_after_statement(connection, cursor, statement, parameters, context, executemany):
        is_column = statement.startswith('ALTER TABLE "TradeFill" ADD COLUMN')
        is_version = statement.startswith('UPDATE "Metadata"')
        if (stage == 'after_column' and is_column) or (stage == 'after_version' and is_version):
            fired.append(statement)
            raise SQLAlchemyError('injected migration failure')

    event.listen(legacy_manager.engine, 'after_cursor_execute', fail_after_statement)
    try:
        assert not migrate(legacy_manager)
    finally:
        event.remove(legacy_manager.engine, 'after_cursor_execute', fail_after_statement)
    assert fired
    connection = sqlite3.connect(legacy_manager.db_path)
    try:
        columns = {row[1] for row in connection.execute('PRAGMA table_info(TradeFill)')}
        assert 'exact_amount' not in columns
        assert connection.execute("SELECT value FROM Metadata WHERE key='local_db_version'").fetchone() == ('20260327',)
    finally:
        connection.close()


def test_unsupported_legacy_copy_swap_migration_is_rejected(legacy_manager):
    connection = sqlite3.connect(legacy_manager.db_path)
    connection.execute("UPDATE Metadata SET value='20210119' WHERE key='local_db_version'")
    connection.commit()
    connection.close()
    with pytest.raises(RuntimeError, match='transactional'):
        Migrator().migrate_db_to_version(ClientConfigAdapter(ClientConfigMap()), legacy_manager, 20210119, 20260910)
    assert not Path(legacy_manager.db_path + '.new').exists()
