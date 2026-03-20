import importlib.util
import sys
import unittest
from pathlib import Path

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


class HummingbotQuickstartParserTest(unittest.TestCase):
    def test_deprecated_config_flag_maps_to_v2_conf(self):
        args = CmdlineParser().parse_args(["-p", "ff", "-f", "v2_with_controllers.py", "-c", "rsi_v5.yml"])

        self.assertEqual("ff", args.config_password)
        self.assertEqual("v2_with_controllers.py", args.config_file_name)
        self.assertEqual("rsi_v5.yml", args.v2_conf)

    def test_v2_flag_maps_to_v2_conf(self):
        args = CmdlineParser().parse_args(["--v2", "rsi_v5.yml"])

        self.assertEqual("rsi_v5.yml", args.v2_conf)


if __name__ == "__main__":
    unittest.main()
