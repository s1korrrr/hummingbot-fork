import sqlite3
import tempfile
from pathlib import Path
from unittest import TestCase

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.model.metadata import Metadata as LocalMetadata
from hummingbot.model.position_hold_ledger import PositionHoldLedger
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType


class SQLConnectionManagerTests(TestCase):
    def test_sqlite_connections_use_wal_and_busy_timeout(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "sqlite_pragmas.sqlite"
            manager = SQLConnectionManager(
                ClientConfigAdapter(ClientConfigMap()),
                SQLConnectionType.TRADE_FILLS,
                db_path=str(db_path),
            )

            with manager.engine.connect() as connection:
                journal_mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar()
                busy_timeout = connection.exec_driver_sql("PRAGMA busy_timeout").scalar()

            self.assertEqual("wal", str(journal_mode).lower())
            self.assertEqual(SQLConnectionManager.SQLITE_BUSY_TIMEOUT_MS, int(busy_timeout))

    def test_check_and_migrate_db_updates_version_and_keeps_manager_writable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "migration_test.sqlite"
            connection = sqlite3.connect(db_path)
            connection.execute('CREATE TABLE "Metadata" ("key" TEXT PRIMARY KEY NOT NULL, "value" TEXT NOT NULL)')
            connection.execute(
                'INSERT INTO "Metadata" ("key", "value") VALUES (?, ?)',
                (SQLConnectionManager.LOCAL_DB_VERSION_KEY, "20230516"),
            )
            connection.commit()
            connection.close()

            manager = SQLConnectionManager(
                ClientConfigAdapter(ClientConfigMap()),
                SQLConnectionType.TRADE_FILLS,
                db_path=str(db_path),
            )

            with manager.get_new_session() as session:
                local_db_version = manager.get_local_db_version(session=session)
                self.assertIsNotNone(local_db_version)
                self.assertEqual(SQLConnectionManager.LOCAL_DB_VERSION_VALUE, local_db_version.value)
                self.assertEqual(0, session.query(PositionHoldLedger).count())

                probe_entry = LocalMetadata(key="migration_probe", value="ok")
                session.add(probe_entry)
                session.commit()

            with manager.get_new_session() as session:
                stored_probe = session.query(LocalMetadata).filter(
                    LocalMetadata.key == "migration_probe"
                ).one_or_none()
                self.assertIsNotNone(stored_probe)
                self.assertEqual("ok", stored_probe.value)
