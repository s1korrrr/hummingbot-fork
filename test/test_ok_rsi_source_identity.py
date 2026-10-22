from pathlib import Path

import pytest

from controllers.directional_trading.ok_rsi import OKRSIController
from controllers.shared import trailing as shared_trailing
from hummingbot.strategy.strategy_v2_base import ControllerImplementationChanged, StrategyV2ConfigBase

OK_RSI_MODULE = "controllers.directional_trading.ok_rsi"


def test_ok_rsi_source_seal_includes_shared_trailing():
    sources = {Path(path).resolve() for path in OKRSIController.implementation_source_files()}
    assert Path(shared_trailing.__file__).resolve() in sources


def test_shared_trailing_content_change_requires_ok_rsi_restart():
    trailing_path = Path(shared_trailing.__file__).resolve()
    original = trailing_path.read_bytes()
    config = StrategyV2ConfigBase()
    config._load_controller_module(OK_RSI_MODULE)
    try:
        trailing_path.write_bytes(original + b"\n# identity-probe\n")
        with pytest.raises(ControllerImplementationChanged, match="restart"):
            config._load_controller_module(OK_RSI_MODULE, reload_changed_modules=True)
    finally:
        trailing_path.write_bytes(original)
    restored = StrategyV2ConfigBase()
    restored._load_controller_module(OK_RSI_MODULE)
