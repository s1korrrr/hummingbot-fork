import importlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from controllers.generic.modular_spot_core.profiles import ProgramProfile, build_profile
from controllers.generic.modular_spot_core.recipe_contract import StrategyRecipe


def api():
    try:
        return importlib.import_module('controllers.generic.modular_spot_core.recipe_compiler')
    except ModuleNotFoundError:
        pytest.fail('The native-profile recipe compiler is not implemented')


def recipe(family='rebound', **changes):
    values = dict(name='test', family=family, symbols=('BTC-USDC', 'ETH-USDC'), quote_budget='200', max_positions=1)
    values.update(changes)
    return StrategyRecipe(**values)


@pytest.mark.parametrize('family', ['rebound', 'impulse', 'sell_only'])
def test_render_reuses_native_profile_and_never_authorizes_execution(family):
    value = recipe(family)
    result = api().render_recipe(value)
    profile = ProgramProfile(name=value.name, mode='fixed' if family == 'rebound' else family,
                             connector=value.connector, symbols=value.symbols, quote_budget=value.quote_budget,
                             max_positions=value.max_positions)
    expected = build_profile(profile)
    assert result['candle_manifest'] == expected['candle_manifest']
    assert result['execution_authorized'] is False
    assert result['native_validation'] == 'NOT_RUN'
    assert result['inventory_adoption_supported'] is False
    for produced, original in zip(result['controllers'], expected['controllers']):
        for key in ('id', 'data', 'budget', 'universe', 'total_amount_quote'):
            assert produced[key] == original[key]
        assert produced['execution_enabled'] is False
        assert produced['acquisition_enabled'] is (family != 'sell_only')
        assert produced['initial_positions'] == []


def test_rebound_preserves_explicit_native_tranches_and_trailing():
    value = recipe(acquisition={'fractions': ['0.3', '0.4', '0.3']}, trailing={'activation': '.02', 'delta': '.007'})
    result = api().render_recipe(value)
    assert result['controllers'][0]['acquisition'] == value.acquisition.model_dump(mode='json')
    assert result['controllers'][0]['trailing'] == value.trailing.model_dump(mode='json')
    assert result['recipe_sha256'] == value.digest


def test_duplicate_json_keys_and_nonfinite_constants_rejected():
    for raw in ('{"name":"a","name":"b"}', '{"quote_budget":NaN}'):
        with pytest.raises(ValueError):
            api().strict_json(raw)


def test_inspect_command_is_data_only(tmp_path):
    m = api()
    source = tmp_path / 'recipe.json'
    source.write_text(recipe().model_dump_json())
    result = subprocess.run([sys.executable, '-m', m.__name__, 'inspect', str(source)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload['native_validation'] == 'NOT_RUN'
    assert payload['execution_authorized'] is False


@pytest.mark.parametrize('command', ['inspect', 'export'])
def test_compiler_requires_native_import_and_leaves_no_output_when_unavailable(tmp_path, command):
    m = api()
    source, output = tmp_path / 'recipe.json', tmp_path / 'export'
    source.write_text(recipe().model_dump_json())
    blocker = '''
import runpy
import sys
class WithoutNative:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'hummingbot' or fullname.startswith('hummingbot.'):
            raise ModuleNotFoundError('native package unavailable for this test')
sys.meta_path.insert(0, WithoutNative())
module = sys.argv.pop(1)
runpy.run_module(module, run_name='__main__')
'''
    args = [sys.executable, '-c', blocker, m.__name__, command, str(source)]
    if command == 'export':
        args.extend(['--output', str(output)])
    result = subprocess.run(args, capture_output=True, text=True)
    assert result.returncode != 0 and 'native package unavailable for this test' in result.stderr
    assert not output.exists()


def test_export_writer_refuses_existing_or_symlink_destination(tmp_path):
    m = api()
    existing = tmp_path / 'existing'
    existing.mkdir()
    (existing / 'keep').write_text('unaltered')
    with pytest.raises(ValueError):
        m.write_bundle({'native_validation': 'PASSED'}, existing)
    assert (existing / 'keep').read_text() == 'unaltered'
    alias = tmp_path / 'alias'
    alias.symlink_to(existing, target_is_directory=True)
    with pytest.raises(ValueError):
        m.write_bundle({'native_validation': 'PASSED'}, alias / 'new')


def test_unvalidated_bundle_cannot_be_exported(tmp_path):
    with pytest.raises(ValueError):
        api().write_bundle(api().render_recipe(recipe()), tmp_path / 'export')


@pytest.mark.parametrize('filename', ['rebound.json', 'impulse.json', 'sell-only.json'])
def test_documented_reference_recipes_render_without_activation(filename):
    root = Path(__file__).resolve().parents[2]
    source = root / 'docs/controllers/modular_spot/recipe-examples' / filename
    module = importlib.import_module('controllers.generic.modular_spot_core.recipe_compiler')
    recipe = module.StrategyRecipe.model_validate(module.strict_json(source.read_text()))
    bundle = module.render_recipe(recipe)
    assert all(not config['execution_enabled'] and not config['initial_positions'] for config in bundle['controllers'])
    assert bundle['native_runtime_validation'] == 'NOT_RUN'
