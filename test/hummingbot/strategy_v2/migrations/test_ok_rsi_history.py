import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from hummingbot.model import get_declarative_base
from hummingbot.model.position import Position


class OKRSIHistoryMigrationTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.source = self.root / "rsi_v5.sqlite"
        self.target = self.root / "ok_rsi.sqlite"
        self.wallet = self.root / "wallet.json"
        engine = create_engine(f"sqlite:///{self.source}")
        get_declarative_base().metadata.create_all(engine)
        with Session(engine) as session:
            for asset, amount, basis in (
                ("BTC", "0.004057", "65000"),
                ("ETH", "1.25", "3000"),
                ("SOL", "2", "100"),
                ("XRP", "30", "1.2"),
                ("BNB", "0.5", "600"),
            ):
                session.add(Position(
                    id=f"source-{asset.lower()}", controller_id=f"rsi-v5-{asset.lower()}-usdc-1m",
                    connector_name="binance", side="BUY", trading_pair=f"{asset}-USDC", timestamp=1000,
                    volume_traded_quote=Decimal("1"), amount=Decimal(amount), breakeven_price=Decimal(basis),
                    unrealized_pnl_quote=Decimal("0"), realized_pnl_quote=Decimal("0"),
                    cum_fees_quote=Decimal("1.25"),
                ))
            session.commit()
        engine.dispose()
        self._write_wallet()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _write_wallet(self, btc="0.004057"):
        balances = {"BTC": btc, "ETH": "1.25", "SOL": "2", "XRP": "30", "BNB": "0.5"}
        payload = {
            "schema_version": 1, "connector": "okx", "source_config_identity": "ok_rsi.yml",
            "captured_at_ms": 2000, "pairs": {
                f"{asset}-USDC": {
                    "available_base": amount, "total_base": amount, "available_quote": "100",
                    "mid_price": "70000", "min_order_size": "0.000001", "min_notional_size": "1",
                } for asset, amount in balances.items()
            },
        }
        self.wallet.write_text(json.dumps(payload))

    def test_inspect_and_reconcile_exact_five_mappings(self):
        from hummingbot.strategy_v2.migrations.ok_rsi_history import plan_ok_rsi_migration

        plan = plan_ok_rsi_migration(self.source, self.target, self.wallet)
        self.assertEqual([seed.mapping.target_controller_id for seed in plan.inventory_seeds], [
            "ok-rsi-btc", "ok-rsi-eth", "ok-rsi-sol", "ok-rsi-xrp", "ok-rsi-bnb",
        ])
        self.assertEqual(plan.inventory_seeds[0].amount, Decimal("0.004057"))
        self.assertEqual(plan.inventory_seeds[0].breakeven_price, Decimal("65000"))
        self.assertEqual(plan.inventory_seeds[0].untracked_balance, Decimal("0"))

    def test_wallet_shortfall_fails_closed(self):
        from hummingbot.strategy_v2.migrations.ok_rsi_history import MigrationValidationError, plan_ok_rsi_migration

        self._write_wallet(btc="0.004")
        with self.assertRaisesRegex(MigrationValidationError, "BTC-USDC.*shortfall"):
            plan_ok_rsi_migration(self.source, self.target, self.wallet)

    def test_apply_is_idempotent_and_seeds_current_identity(self):
        from hummingbot.strategy_v2.migrations.ok_rsi_history import (
            apply_ok_rsi_migration,
            initialize_target_database,
            plan_ok_rsi_migration,
        )

        initialize_target_database(self.target)
        plan = plan_ok_rsi_migration(self.source, self.target, self.wallet)
        first = apply_ok_rsi_migration(plan)
        second = apply_ok_rsi_migration(plan_ok_rsi_migration(self.source, self.target, self.wallet))
        self.assertEqual(first.seed_count, 5)
        self.assertEqual(second.seed_count, 0)
        with closing(sqlite3.connect(self.target)) as connection:
            current = connection.execute(
                'select controller_id, connector_name, trading_pair from "Position" '
                "where controller_id like 'ok-rsi-%' order by controller_id"
            ).fetchall()
            ledger = connection.execute(
                'select count(*) from "PositionHoldLedger" where controller_id like \'ok-rsi-%\''
            ).fetchone()[0]
        self.assertEqual(len(current), 5)
        self.assertEqual(ledger, 5)
        self.assertTrue(all(row[1] == "okx" and row[2].endswith("-USDC") for row in current))

    def test_first_sell_economics_are_conditional_and_bounded(self):
        from hummingbot.strategy_v2.migrations.ok_rsi_history import calculate_first_sell_economics

        report = calculate_first_sell_economics(
            controller_id="ok-rsi-btc", pair="BTC-USDC", wallet_available=Decimal("0.005"),
            tracked_amount=Decimal("0.004057"), untracked_amount=Decimal("0.000943"),
            breakeven_price=Decimal("65000"), min_profit_pct=Decimal("0.01"),
            bag_recovery_target_pct=Decimal("0.02"), recovery_partial_exit_fraction=Decimal("0.10"),
            recovery_pullback_pct=Decimal("0.003"),
        )
        self.assertEqual(report.minimum_signal_price, Decimal("65650"))
        self.assertEqual(report.recovery_arm_price, Decimal("66300"))
        self.assertEqual(report.first_recovery_amount, Decimal("0.0004057"))
        self.assertIn("conditional", " ".join(report.conditions).lower())


if __name__ == "__main__":
    unittest.main()
