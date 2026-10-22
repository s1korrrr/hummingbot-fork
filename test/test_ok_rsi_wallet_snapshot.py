import ast
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace


class FakeOKXConnector:
    name = "okx"
    ready = True

    def __init__(self):
        self.trading_rules = {
            "BTC-USDC": SimpleNamespace(
                min_order_size=Decimal("0.00001"),
                min_notional_size=Decimal("1"),
            ),
            "ETH-USDC": SimpleNamespace(
                min_order_size=Decimal("0.0001"),
                min_notional_size=Decimal("1"),
            ),
        }

    def get_available_balance(self, asset):
        return {"BTC": Decimal("0.25"), "ETH": Decimal("2"), "USDC": Decimal("400")}[asset]

    def get_balance(self, asset):
        return {"BTC": Decimal("0.30"), "ETH": Decimal("2.1"), "USDC": Decimal("500")}[asset]

    def get_mid_price(self, trading_pair):
        return {"BTC-USDC": Decimal("65000"), "ETH-USDC": Decimal("3500")}[trading_pair]


class OKRSIWalletSnapshotTest(unittest.TestCase):
    def test_build_wallet_snapshot_uses_read_only_connector_values(self):
        from scripts.ok_rsi_wallet_snapshot import build_wallet_snapshot

        snapshot = build_wallet_snapshot(
            connector=FakeOKXConnector(),
            trading_pairs=["BTC-USDC", "ETH-USDC"],
            captured_at_ms=1_787_940_000_000,
            source_config_identity="ok_rsi.yml",
        )

        self.assertEqual(snapshot["schema_version"], 1)
        self.assertEqual(snapshot["connector"], "okx")
        self.assertEqual(snapshot["pairs"]["BTC-USDC"]["available_base"], "0.25")
        self.assertEqual(snapshot["pairs"]["BTC-USDC"]["total_base"], "0.3")
        self.assertEqual(snapshot["pairs"]["BTC-USDC"]["available_quote"], "400")
        self.assertEqual(snapshot["pairs"]["BTC-USDC"]["mid_price"], "65000")
        self.assertEqual(snapshot["pairs"]["BTC-USDC"]["min_order_size"], "0.00001")

    def test_atomic_writer_replaces_target_with_canonical_json(self):
        from scripts.ok_rsi_wallet_snapshot import write_wallet_snapshot

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "snapshot.json"
            path.write_text("stale")
            write_wallet_snapshot(path, {"z": 1, "a": {"b": 2}})
            self.assertEqual(json.loads(path.read_text()), {"a": {"b": 2}, "z": 1})
            self.assertFalse(path.with_suffix(".json.tmp").exists())

    def test_strategy_source_contains_no_order_capable_calls(self):
        source_path = Path(__file__).parents[1] / "scripts" / "ok_rsi_wallet_snapshot.py"
        tree = ast.parse(source_path.read_text())
        forbidden = {"buy", "sell", "cancel", "create_order", "execute_action", "execute_actions"}
        called_attributes = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertTrue(forbidden.isdisjoint(called_attributes))


if __name__ == "__main__":
    unittest.main()
