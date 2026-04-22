import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

ROOT_PATH = Path(__file__).resolve().parent.parent
BIN_PATH = ROOT_PATH / "bin"
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))
if str(BIN_PATH) not in sys.path:
    sys.path.insert(0, str(BIN_PATH))

quickstart_spec = importlib.util.spec_from_file_location(
    "hummingbot_quickstart",
    BIN_PATH / "hummingbot_quickstart.py",
)
quickstart_module = importlib.util.module_from_spec(quickstart_spec)
assert quickstart_spec.loader is not None
quickstart_spec.loader.exec_module(quickstart_module)
CmdlineParser = quickstart_module.CmdlineParser
quick_start = quickstart_module.quick_start


class HummingbotQuickstartParserTest(unittest.TestCase):
    def test_deprecated_config_flag_maps_to_v2_conf(self):
        args = CmdlineParser().parse_args(["-p", "ff", "-f", "v2_with_controllers.py", "-c", "rsi_v5.yml"])

        self.assertEqual("ff", args.config_password)
        self.assertEqual("v2_with_controllers.py", args.config_file_name)
        self.assertEqual("rsi_v5.yml", args.v2_conf)

    def test_v2_flag_maps_to_v2_conf(self):
        args = CmdlineParser().parse_args(["--v2", "rsi_v5.yml"])

        self.assertEqual("rsi_v5.yml", args.v2_conf)


class HummingbotQuickstartTest(unittest.IsolatedAsyncioTestCase):
    @patch.object(quickstart_module, "run_application", new_callable=AsyncMock)
    @patch.object(quickstart_module, "wait_for_gateway_ready", new_callable=AsyncMock)
    @patch.object(quickstart_module, "load_and_start_strategy", new_callable=AsyncMock)
    @patch.object(quickstart_module, "HummingbotApplication")
    @patch.object(quickstart_module, "AllConnectorSettings")
    @patch.object(quickstart_module, "read_system_configs_from_yml", new_callable=AsyncMock)
    @patch.object(quickstart_module, "create_yml_files_legacy", new_callable=AsyncMock)
    @patch.object(quickstart_module, "init_logging")
    @patch.object(quickstart_module.Security, "wait_til_decryption_done", new_callable=AsyncMock)
    @patch.object(quickstart_module.Security, "login")
    @patch.object(quickstart_module, "load_client_config_map_from_file")
    async def test_quick_start_decrypts_loaded_client_config_after_login(
        self,
        load_client_config_map_from_file_mock,
        security_login_mock,
        _wait_til_decryption_done_mock,
        _init_logging_mock,
        _create_yml_files_legacy_mock,
        _read_system_configs_from_yml_mock,
        all_connector_settings_mock,
        hummingbot_application_mock,
        load_and_start_strategy_mock,
        wait_for_gateway_ready_mock,
        run_application_mock,
    ):
        client_config_map = Mock()
        client_config_map.paper_trade.paper_trade_exchanges = []
        client_config_map.mqtt_bridge.mqtt_autostart = False
        client_config_map.debug_console = False
        client_config_map.decrypt_all_secure_data = Mock()
        load_client_config_map_from_file_mock.return_value = client_config_map
        security_login_mock.return_value = True
        hb_instance = Mock()
        hummingbot_application_mock.main_application.return_value = hb_instance
        load_and_start_strategy_mock.return_value = True
        all_connector_settings_mock.initialize_paper_trade_settings = Mock()

        args = SimpleNamespace(
            auto_set_permissions=None,
            headless=False,
            v2_conf=None,
            config_file_name=None,
        )

        await quick_start(args, Mock())

        client_config_map.decrypt_all_secure_data.assert_called_once()


if __name__ == "__main__":
    unittest.main()
