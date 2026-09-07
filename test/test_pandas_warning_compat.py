import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from controllers.directional_trading.pandas_compat import pandas_copy_on_write_warning_category

ROOT = Path(__file__).resolve().parents[1]


class _Pandas4Warning(Warning):
    pass


def test_copy_on_write_warning_uses_pandas_3_category_when_available():
    errors = SimpleNamespace(Pandas4Warning=_Pandas4Warning)

    assert pandas_copy_on_write_warning_category(errors) is _Pandas4Warning


def test_copy_on_write_warning_falls_back_for_pandas_2():
    assert pandas_copy_on_write_warning_category(SimpleNamespace()) is FutureWarning


def test_copy_on_write_warning_matches_installed_pandas():
    expected = getattr(pd.errors, "Pandas4Warning", FutureWarning)

    assert pandas_copy_on_write_warning_category(pd.errors) is expected


def test_rsi_v5_imports_under_api_bots_package_without_top_level_controllers(tmp_path):
    script = """
import importlib
import importlib.abc
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd

root = Path(sys.argv[1])
if hasattr(pd.errors, "Pandas4Warning"):
    delattr(pd.errors, "Pandas4Warning")

class RejectTopLevelControllers(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "controllers" or fullname.startswith("controllers."):
            raise ModuleNotFoundError("top-level controllers package is unavailable")
        return None

sys.meta_path.insert(0, RejectTopLevelControllers())
for name, package_path in (
    ("bots", root),
    ("bots.controllers", root / "controllers"),
    ("bots.controllers.directional_trading", root / "controllers" / "directional_trading"),
):
    package = ModuleType(name)
    package.__path__ = [str(package_path)]
    sys.modules[name] = package

module = importlib.import_module("bots.controllers.directional_trading.rsi_v5")
assert module.__name__ == "bots.controllers.directional_trading.rsi_v5"
assert module._PANDAS_COPY_ON_WRITE_WARNING is FutureWarning
"""
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT)

    result = subprocess.run(
        [sys.executable, "-c", script, str(ROOT)],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
