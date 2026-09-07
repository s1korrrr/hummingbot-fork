import sqlite3
import tempfile
from pathlib import Path
from unittest import TestCase

from sqlalchemy import inspect

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.model.controller_candle_watermark import (
    ControllerCandleWatermark,
    ControllerCandleWatermarkNamespace,
    ControllerCandleWatermarkOutcome,
    ControllerCandleWatermarkResult,
    ControllerCandleWatermarkState,
)
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType


class ControllerCandleWatermarkModelTests(TestCase):
    def test_outcome_catalog_and_proposal_eligibility_are_exact(self):
        self.assertEqual(
            [outcome.value for outcome in ControllerCandleWatermarkOutcome],
            [
                "SEEDED",
                "ADVANCED_BY_CALLER",
                "ALREADY_COMMITTED",
                "REGRESSION",
                "CAS_CONFLICT",
                "INVALID_STATE",
                "STORE_UNAVAILABLE",
                "COMMIT_FAILED",
            ],
        )
        for outcome in ControllerCandleWatermarkOutcome:
            with self.subTest(outcome=outcome):
                result = ControllerCandleWatermarkResult(outcome=outcome)
                self.assertEqual(
                    result.proposal_eligible,
                    outcome is ControllerCandleWatermarkOutcome.ADVANCED_BY_CALLER,
                )

    def test_namespace_and_state_are_frozen_and_strictly_validated(self):
        namespace = ControllerCandleWatermarkNamespace(
            controller_id="ok-rsi-btc",
            connector_name="okx",
            trading_pair="BTC-USDC",
            interval="1m",
        )
        state = ControllerCandleWatermarkState(
            namespace=namespace,
            candle_open_timestamp_s=1_700_033_100,
            revision=0,
            schema_version=1,
            updated_at_ms=1_700_033_160_000,
        )

        self.assertEqual(state.candle_open_timestamp_s, 1_700_033_100)
        with self.assertRaisesRegex(Exception, "cannot assign"):
            state.revision = 1

        invalid_namespaces = (
            {"controller_id": ""},
            {"connector_name": " okx"},
            {"trading_pair": "BTC-USDC "},
            {"interval": "1M"},
        )
        for override in invalid_namespaces:
            values = {
                "controller_id": "ok-rsi-btc",
                "connector_name": "okx",
                "trading_pair": "BTC-USDC",
                "interval": "1m",
                **override,
            }
            with self.subTest(override=override), self.assertRaises(ValueError):
                ControllerCandleWatermarkNamespace(**values)

        invalid_states = (
            {"candle_open_timestamp_s": 0},
            {"candle_open_timestamp_s": 1_700_033_101},
            {"revision": -1},
            {"schema_version": 2},
            {"updated_at_ms": 0},
        )
        for override in invalid_states:
            values = {
                "namespace": namespace,
                "candle_open_timestamp_s": 1_700_033_100,
                "revision": 0,
                "schema_version": 1,
                "updated_at_ms": 1_700_033_160_000,
                **override,
            }
            with self.subTest(override=override), self.assertRaises(ValueError):
                ControllerCandleWatermarkState(**values)

    def test_registered_model_creates_exact_additive_schema_and_preserves_existing_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "current.sqlite"
            raw = sqlite3.connect(db_path)
            raw.execute("CREATE TABLE sentinel (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
            raw.execute("INSERT INTO sentinel (id, value) VALUES (1, 'preserved')")
            raw.commit()
            raw.close()

            manager = SQLConnectionManager(
                ClientConfigAdapter(ClientConfigMap()),
                SQLConnectionType.TRADE_FILLS,
                db_path=str(db_path),
            )

            inspector = inspect(manager.engine)
            self.assertIn(ControllerCandleWatermark.__tablename__, inspector.get_table_names())
            columns = inspector.get_columns(ControllerCandleWatermark.__tablename__)
            self.assertEqual(
                [
                    (column["name"], str(column["type"]), column["nullable"], column["primary_key"])
                    for column in columns
                ],
                [
                    ("controller_id", "TEXT", False, 1),
                    ("connector_name", "TEXT", False, 2),
                    ("trading_pair", "TEXT", False, 3),
                    ("interval", "TEXT", False, 4),
                    ("candle_open_timestamp_s", "BIGINT", False, 0),
                    ("revision", "BIGINT", False, 0),
                    ("schema_version", "INTEGER", False, 0),
                    ("updated_at_ms", "BIGINT", False, 0),
                ],
            )
            self.assertEqual(
                inspector.get_pk_constraint(ControllerCandleWatermark.__tablename__)["constrained_columns"],
                ["controller_id", "connector_name", "trading_pair", "interval"],
            )
            with manager.engine.connect() as connection:
                sentinel = connection.exec_driver_sql("SELECT value FROM sentinel WHERE id = 1").scalar_one()
            self.assertEqual(sentinel, "preserved")

    def test_prior_version_migration_keeps_sentinel_and_additive_table_without_version_bump(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "prior.sqlite"
            raw = sqlite3.connect(db_path)
            raw.execute('CREATE TABLE "Metadata" ("key" TEXT PRIMARY KEY NOT NULL, "value" TEXT NOT NULL)')
            raw.execute(
                'INSERT INTO "Metadata" ("key", "value") VALUES (?, ?)',
                (SQLConnectionManager.LOCAL_DB_VERSION_KEY, "20230516"),
            )
            raw.execute("CREATE TABLE sentinel (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
            raw.execute("INSERT INTO sentinel (id, value) VALUES (1, 'preserved')")
            raw.commit()
            raw.close()

            manager = SQLConnectionManager(
                ClientConfigAdapter(ClientConfigMap()),
                SQLConnectionType.TRADE_FILLS,
                db_path=str(db_path),
            )

            self.assertEqual(SQLConnectionManager.LOCAL_DB_VERSION_VALUE, "20260327")
            inspector = inspect(manager.engine)
            self.assertIn(ControllerCandleWatermark.__tablename__, inspector.get_table_names())
            with manager.engine.connect() as connection:
                sentinel = connection.exec_driver_sql("SELECT value FROM sentinel WHERE id = 1").scalar_one()
            self.assertEqual(sentinel, "preserved")


if __name__ == "__main__":
    import unittest

    unittest.main()
