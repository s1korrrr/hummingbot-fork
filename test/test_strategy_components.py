"""The reusable library loads without controller families and preserves old identities."""
import importlib
import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

MODULES = ('domain', 'candle_data', 'candle_transport', 'candle_stream', 'data_reader',
           'feature_cache', 'feature_registry', 'decision_protocol', 'capital_policy', 'exit_policy',
           'universe', 'screener', 'selection_state', 'program_budget', 'portfolio', 'acquisition', 'risk', 'trailing')
ROOT = Path(__file__).resolve().parents[1]


def test_components_load_without_any_controller_import():
    script = '''
import importlib, importlib.abc, sys
class NoControllers(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'controllers' or fullname.startswith('controllers.'):
            raise AssertionError('Reusable component imported strategy family: ' + fullname)
sys.meta_path.insert(0, NoControllers())
for name in %s:
    importlib.import_module('hummingbot.strategy_v2.components.' + name)
assert not any(name == 'controllers' or name.startswith('controllers.') for name in sys.modules)
''' % repr((*MODULES, 'config', 'recipe_contract', 'strict_json', 'candle_aggregation'))
    result = subprocess.run([sys.executable, '-c', script], cwd=ROOT, text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('name', MODULES)
def test_legacy_module_alias_keeps_class_and_monkeypatch_identity(name):
    canonical = importlib.import_module('hummingbot.strategy_v2.components.' + name)
    legacy = importlib.import_module('controllers.generic.modular_spot_core.' + name)
    assert legacy is canonical
    with patch.object(legacy, '_component_patch_probe', object(), create=True) as marker:
        assert canonical._component_patch_probe is marker


def test_neutral_contract_classes_are_same_objects_through_legacy_imports():
    from controllers.generic.modular_spot_core import config as legacy_config, recipe_contract as legacy_contract
    from hummingbot.strategy_v2.components import config, recipe_contract
    for name in ('PolicyModel', 'AcquisitionPolicy', 'TrailingPolicy', 'RiskPolicy'):
        assert getattr(config, name) is getattr(legacy_config, name)
    assert not hasattr(config, 'SignalPolicy')
    assert not hasattr(recipe_contract, 'StrategyRecipe')
    for name in ('RecipeBinding', 'canonical_json', 'digest', 'source_digest', 'configuration_digest', 'verify_binding'):
        assert getattr(recipe_contract, name) is getattr(legacy_contract, name)


def test_aggregation_and_strict_json_remain_exact_compatibility_exports():
    from controllers.directional_trading.ok_rsi_core.candles import derive_higher_timeframe_candles as old
    from controllers.generic.modular_spot_core.recipe_compiler import strict_json as old_json
    from hummingbot.strategy_v2.components.candle_aggregation import derive_higher_timeframe_candles
    from hummingbot.strategy_v2.components.strict_json import strict_json
    assert derive_higher_timeframe_candles is old
    assert strict_json is old_json
    assert strict_json('{"zero":0}') == json.loads('{"zero":0}')
    for raw in ('{"key":1,"key":2}', '{"n":NaN}', '{"n":Infinity}'):
        with pytest.raises(ValueError):
            strict_json(raw)


@pytest.mark.parametrize('module', [
    'controllers.generic.modular_spot_core.candle_transport',
    'hummingbot.strategy_v2.components.candle_transport',
])
def test_candle_service_cli_compatibility_does_not_start_on_help(module):
    result = subprocess.run([sys.executable, '-m', module, '--help'], cwd=ROOT, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert 'Read-only native candle distributor; no execution services' in result.stdout
