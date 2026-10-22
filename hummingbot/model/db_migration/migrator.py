import logging
import sqlite3
from contextlib import closing
from inspect import getmembers, isabstract, isclass

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.model.db_migration.base_transformation import DatabaseTransformation
from hummingbot.model.sql_connection_manager import SQLConnectionManager


class Migrator:
    @classmethod
    def _get_transformations(cls):
        import hummingbot.model.db_migration.transformations as transformations
        return [o for _, o in getmembers(transformations,
                                         predicate=lambda c: isclass(c) and
                                         issubclass(c, DatabaseTransformation) and
                                         not isabstract(c))]

    def __init__(self):
        self.transformations = [t(self) for t in self._get_transformations()]

    def migrate_db_to_version(self, client_config_map: ClientConfigAdapter, db_handle, from_version, to_version):
        transformations = sorted(t for t in self.transformations
                                 if t.does_apply_to_version(from_version, to_version))
        if db_handle.engine.dialect.name != "sqlite" or any(
                not callable(getattr(t, "apply_sqlite_transaction", None)) for t in transformations):
            raise RuntimeError("Automatic database migration requires supported transactional transformations; "
                               "legacy copy/swap migration is disabled")
        backup_db_path = db_handle.db_path + '.backup_' + pd.Timestamp.now("UTC").strftime("%Y%m%d-%H%M%S-%f")
        try:
            with db_handle.engine.connect() as connection:
                # Reserve the writer before snapshotting. Other writers wait and then write to the same file.
                # Explicit BEGIN also makes SQLite DDL participate in rollback alongside the version update.
                connection.exec_driver_sql("BEGIN IMMEDIATE")
                current = connection.exec_driver_sql(
                    'SELECT value FROM "Metadata" WHERE key = ?',
                    (SQLConnectionManager.LOCAL_DB_VERSION_KEY,),
                ).scalar_one_or_none()
                if current == str(to_version):
                    connection.rollback()
                    return True
                if current != str(from_version):
                    raise RuntimeError("Database version changed before transactional migration")
                with closing(sqlite3.connect(db_handle.db_path, timeout=2)) as source:
                    with closing(sqlite3.connect(backup_db_path)) as backup:
                        source.backup(backup)
                for transformation in transformations:
                    logging.getLogger().info(f"Applying {transformation.name} transactionally...")
                    transformation.apply_sqlite_transaction(connection)
                connection.exec_driver_sql(
                    'UPDATE "Metadata" SET value = ? WHERE key = ?',
                    (str(to_version), SQLConnectionManager.LOCAL_DB_VERSION_KEY),
                )
                connection.commit()
        except SQLAlchemyError:
            logging.getLogger().error("Database migration rolled back; schema and version were not advanced.",
                                      exc_info=True)
            return False
        return True
