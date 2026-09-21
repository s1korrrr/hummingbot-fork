import subprocess
import sys
from pathlib import Path

import pytest

from hummingbot.strategy_v2.components.recipe_contract import source_digest, source_records
from hummingbot.strategy_v2.components.source_identity import component_source_files

ROOT = Path(__file__).resolve().parents[2]
CONTROLLER = ROOT / 'controllers/generic/momentum_fusion_v2.py'
CORE = ROOT / 'controllers/generic/momentum_fusion_v2_core'


def test_import_does_not_load_legacy_families_or_start_services():
    try:
        from hummingbot.connector.connector_base import ConnectorBase  # noqa: F401
    except ModuleNotFoundError:
        pytest.skip('compiled hummingbot native package required')
    script = r'''
import sys
from controllers.generic.momentum_fusion_v2 import MomentumFusionV2Config, MomentumFusionV2Controller
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
assert MomentumFusionV2Controller.__bases__ == (ControllerBase,)
assert MomentumFusionV2Config(id='mom').execution_enabled is False
banned = [name for name in sys.modules if "ok_rsi" in name or name.endswith("rsi_modular") or "rsi_v5" in name]
assert banned == [], banned
'''
    result = subprocess.run([sys.executable, '-c', script], cwd=ROOT, text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_momentum_source_does_not_mutate_component_tree(tmp_path, monkeypatch):
    from hummingbot.strategy_v2.components import source_identity

    components = tmp_path / 'components'
    components.mkdir()
    for name in ('__init__.py', 'source_identity.py', 'recipe_contract.py', 'domain.py'):
        (components / name).write_text('# component\n')
    monkeypatch.setattr(source_identity, 'component_root', lambda: components)
    controller = tmp_path / 'controllers'
    controller.mkdir()
    source = controller / 'momentum_fusion_v2.py'
    source.write_text('from hummingbot.strategy_v2.components.domain import PolicyError\n')
    first = source_digest(controller, [source])
    records = dict(source_records(controller, [source]))
    assert '@strategy_components/domain.py' in records
    (components / 'extra.py').write_text('# additive component file\n')
    changed = source_digest(controller, [source])
    assert changed != first
    assert source_digest(controller, [source]) == changed
    files = [CONTROLLER, *sorted(CORE.glob('*.py'))]
    assert files
    assert all('ok_rsi' not in path.name and 'rsi_modular' not in path.name for path in files)


def test_canonical_component_manifest_still_complete():
    names = {path.name for path in component_source_files()}
    assert {'__init__.py', 'source_identity.py', 'recipe_contract.py'} <= names


def test_controller_emit_path_calls_native_planners():
    source = CONTROLLER.read_text()
    assert 'runtime.plan_acquisition' in source
    assert 'runtime.plan_management' in source
    assert 'select(tuple(self._states.values()), (),' not in source
    assert "getattr(row, 'confirmed', None) is False" not in source
    assert 'closed_native_bars' in source
    emit = source.split('def determine_executor_actions')[1].split('def get_status_summary')[0]
    assert 'return []' not in emit
