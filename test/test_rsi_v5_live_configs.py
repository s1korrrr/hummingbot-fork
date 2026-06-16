import unittest
from pathlib import Path

import yaml


class RSIv5LiveConfigTest(unittest.TestCase):
    def test_live_usdc_configs_validate_against_controller_schema(self):
        from controllers.directional_trading.rsi_v5 import RSIv5ControllerConfig

        repo_root = Path(__file__).resolve().parents[1]
        expected_pairs = {
            "rsi_v5_btc.yml": "BTC-USDC",
            "rsi_v5_bnb.yml": "BNB-USDC",
            "rsi_v5_eth.yml": "ETH-USDC",
            "rsi_v5_sol.yml": "SOL-USDC",
            "rsi_v5_xrp.yml": "XRP-USDC",
        }
        configs_dir = repo_root / "conf" / "controllers"
        missing_configs = [filename for filename in expected_pairs if not (configs_dir / filename).exists()]
        if missing_configs:
            self.skipTest(f"local RSI v5 live configs are absent: {', '.join(missing_configs)}")

        for filename, pair in expected_pairs.items():
            with self.subTest(filename=filename):
                payload = yaml.safe_load((configs_dir / filename).read_text())
                config = RSIv5ControllerConfig(**payload)
                self.assertEqual(config.controller_name, "rsi_v5")
                self.assertEqual(config.connector_name, "binance")
                self.assertEqual(config.trading_pair, pair)
                self.assertEqual(config.interval, "1m")
                self.assertGreater(config.usd_per_entry, 0)
                self.assertGreaterEqual(config.max_total_position_usd, config.usd_per_entry)
                self.assertGreaterEqual(config.max_executors_per_side, 1)

        xrp_payload = yaml.safe_load((configs_dir / "rsi_v5_xrp.yml").read_text())
        xrp_config = RSIv5ControllerConfig(**xrp_payload)
        self.assertTrue(getattr(xrp_config, "manual_kill_switch", False))


if __name__ == "__main__":
    unittest.main()
