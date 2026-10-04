import asyncio
import importlib
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import yaml

from hummingbot.core.data_type.common import PriceType, TradeType

ROOT_PATH = Path(__file__).resolve().parent.parent
ROOT_PATH_STR = str(ROOT_PATH)


def ensure_local_controller_imports():
    while ROOT_PATH_STR in sys.path:
        sys.path.remove(ROOT_PATH_STR)
    sys.path.insert(0, ROOT_PATH_STR)
    for module_name in list(sys.modules):
        if module_name == "controllers" or module_name.startswith("controllers."):
            sys.modules.pop(module_name, None)
    importlib.invalidate_caches()


class HLRSIControllerTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        ensure_local_controller_imports()

    @staticmethod
    def _provider(*, base_balance: Decimal = Decimal("0")):
        provider = MagicMock()
        provider.initialize_rate_sources.return_value = None
        provider.time.return_value = 1_725_000_000.0
        provider.get_available_balance.side_effect = lambda connector, asset: (
            base_balance if asset == "HYPE" else Decimal("1000")
        )
        provider.get_trading_rules.return_value = SimpleNamespace(
            min_price_increment=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
            min_base_amount_increment=Decimal("0.001"),
            min_notional_size=Decimal("10"),
            min_order_value=Decimal("10"),
        )
        provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
        provider.get_price_by_type.side_effect = lambda connector, pair, price_type: (
            Decimal("25.000") if price_type == PriceType.BestBid else Decimal("25.010")
        )
        return provider

    def _controller(self, *, trade_mode: str, base_balance: Decimal = Decimal("0")):
        from controllers.directional_trading.hl_rsi import HLRSIController, HLRSIControllerConfig

        controller = HLRSIController(
            config=HLRSIControllerConfig(
                id=f"hl-rsi-{trade_mode}",
                connector_name="hyperliquid",
                trading_pair="HYPE-USDC",
                trade_mode=trade_mode,
                sell_only_if_profitable=False,
                strict_hmm_mode=False,
            ),
            market_data_provider=self._provider(base_balance=base_balance),
            actions_queue=asyncio.Queue(),
        )
        controller.positions_held = []
        controller.executors_info = []
        controller.processed_data = {"signal": 1, "signal_state": {"condition_ok": True}}
        return controller

    def test_module_exposes_hl_identity_and_hyperliquid_spot_defaults(self):
        from controllers.directional_trading.hl_rsi import HLRSIController, HLRSIControllerConfig

        config = HLRSIControllerConfig(id="hl-rsi-default")

        self.assertIsNotNone(HLRSIController)
        self.assertEqual(config.controller_name, "hl_rsi")
        self.assertEqual(config.connector_name, "hyperliquid")
        self.assertEqual(config.trading_pair, "HYPE-USDC")
        self.assertEqual(config.trade_mode, "buy_sell")

    def test_config_rejects_non_hyperliquid_connector(self):
        from controllers.directional_trading.hl_rsi import HLRSIControllerConfig

        with self.assertRaisesRegex(ValueError, "Hyperliquid spot"):
            HLRSIControllerConfig(
                id="hl-rsi-invalid-connector",
                connector_name="binance",
                trading_pair="HYPE-USDC",
            )

    def test_sell_only_blocks_buy_signal_at_executor_boundary(self):
        from hummingbot.connector.markets_recorder import MarketsRecorder

        # Explicit complete empty native history, so exposure is finite and the
        # trade-mode gate (not the fail-closed exposure gate) is what refuses BUY.
        recorder = MagicMock()
        recorder.get_executors_by_controller.return_value = []
        recorder.get_position_hold_ledger_entries_by_controller.return_value = []
        controller = self._controller(trade_mode="sell_only", base_balance=Decimal("2"))

        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            self.assertFalse(controller.can_create_executor(1))
        self.assertEqual(controller._last_gate_reason[TradeType.BUY], "trade_mode_sell_only")

    def test_sell_only_refuses_direct_buy_executor_construction(self):
        controller = self._controller(trade_mode="sell_only", base_balance=Decimal("2"))

        with self.assertRaisesRegex(ValueError, "sell_only"):
            controller.get_executor_config(TradeType.BUY, Decimal("25"), Decimal("1"))

    def test_sell_only_uses_available_wallet_inventory_without_ledger_position(self):
        controller = self._controller(trade_mode="sell_only", base_balance=Decimal("2.5"))

        self.assertEqual(controller._get_sellable_inventory_amount(), Decimal("2.5"))
        self.assertTrue(controller._has_buy_inventory())

    def test_sell_only_sell_executor_cannot_exceed_available_wallet_inventory(self):
        controller = self._controller(trade_mode="sell_only", base_balance=Decimal("0.75"))

        executor = controller.get_executor_config(TradeType.SELL, Decimal("25"), Decimal("100"))

        self.assertEqual(executor.side, TradeType.SELL)
        self.assertGreater(executor.amount, Decimal("0"))
        self.assertLessEqual(executor.amount, Decimal("0.75"))

    def test_buy_sell_mode_does_not_treat_untracked_wallet_balance_as_managed_inventory(self):
        controller = self._controller(trade_mode="buy_sell", base_balance=Decimal("2.5"))

        self.assertEqual(controller._get_sellable_inventory_amount(), Decimal("0"))
        self.assertFalse(controller._has_buy_inventory())

    def test_local_config_pack_loads_through_strategy_v2_discovery(self):
        from scripts.v2_with_controllers import V2WithControllersConfig

        script_payload = yaml.safe_load((ROOT_PATH / "conf" / "scripts" / "hl_rsi.yml").read_text())
        script_config = V2WithControllersConfig(**script_payload)

        loaded = script_config.load_controller_configs()

        expected = {
            "hl-rsi-hype-usdc-sell-only": ("HYPE-USDC", "sell_only"),
            "hl-rsi-ubtc-usdc-1m": ("UBTC-USDC", "buy_sell"),
            "hl-rsi-ueth-usdc-1m": ("UETH-USDC", "buy_sell"),
            "hl-rsi-usol-usdc-1m": ("USOL-USDC", "buy_sell"),
        }
        actual = {config.id: (config.trading_pair, config.trade_mode) for config in loaded}

        self.assertEqual(actual, expected)
        self.assertEqual(len({config.trading_pair for config in loaded}), len(loaded))
        self.assertTrue(all(config.controller_name == "hl_rsi" for config in loaded))
        self.assertTrue(all(config.connector_name == "hyperliquid" for config in loaded))
        self.assertTrue(all(not config.manual_kill_switch for config in loaded))
        self.assertTrue(all(config.usd_per_entry == Decimal("25") for config in loaded))

    def test_buy_sell_configs_preserve_matching_rsi_v5_profile_parameters(self):
        identity_keys = {
            "controller_name",
            "id",
            "connector_name",
            "trading_pair",
            "trade_mode",
            "manual_kill_switch",
            "usd_per_entry",
        }
        for asset, hyperliquid_pair in {
            "btc": "UBTC-USDC",
            "eth": "UETH-USDC",
            "sol": "USOL-USDC",
        }.items():
            with self.subTest(asset=asset):
                source = yaml.safe_load((ROOT_PATH / "conf" / "controllers" / f"rsi_v5_{asset}.yml").read_text())
                target = yaml.safe_load((ROOT_PATH / "conf" / "controllers" / f"hl_rsi_{asset}.yml").read_text())

                self.assertEqual(target["controller_name"], "hl_rsi")
                self.assertEqual(target["connector_name"], "hyperliquid")
                self.assertEqual(target["trading_pair"], hyperliquid_pair)
                self.assertEqual(target["trade_mode"], "buy_sell")
                self.assertNotIn("manual_kill_switch", target)
                self.assertEqual(target["usd_per_entry"], "25")
                source_profile = {key: value for key, value in source.items() if key not in identity_keys}
                target_profile = {key: value for key, value in target.items() if key not in identity_keys}
                self.assertEqual(target_profile, source_profile)

    def test_legacy_manual_stop_honors_spot_keep_position_preference(self):
        from hummingbot.strategy_v2.models.base import RunnableStatus
        from scripts.v2_with_controllers import V2WithControllers

        executor = SimpleNamespace(id="executor-1", controller_id="hl-rsi-controller")
        controller = SimpleNamespace(
            config=SimpleNamespace(manual_kill_switch=True, early_stop_keep_position=True),
            status=RunnableStatus.RUNNING,
            stop=MagicMock(),
        )
        strategy = SimpleNamespace(
            controllers={"hl-rsi-controller": controller},
            drawdown_exited_controllers=[],
            executor_orchestrator=MagicMock(),
            get_executors_by_controller=MagicMock(return_value=[executor]),
            logger=MagicMock(return_value=MagicMock()),
        )

        V2WithControllers.check_manual_kill_switch(strategy)

        controller.stop.assert_called_once_with()
        actions = strategy.executor_orchestrator.execute_actions.call_args.args[0]
        self.assertEqual(len(actions), 1)
        self.assertTrue(actions[0].keep_position)

    def test_minimal_mqtt_compose_exposes_only_loopback_broker_port(self):
        payload = yaml.safe_load((ROOT_PATH / "docker-compose.mqtt.yml").read_text())

        self.assertEqual(set(payload["services"]), {"mqtt"})
        broker = payload["services"]["mqtt"]
        self.assertEqual(broker["image"], "emqx:5.8.8")
        self.assertEqual(broker["ports"], ["127.0.0.1:1883:1883"])
        self.assertIn("healthcheck", broker)


if __name__ == "__main__":
    unittest.main()
