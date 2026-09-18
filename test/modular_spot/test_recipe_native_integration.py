"""Requires genuine native Hummingbot; a whole-module skip is NOT a native pass."""
import importlib.util
import json

import pytest

if importlib.util.find_spec('hummingbot') is None:
    pytest.skip('genuine Hummingbot package unavailable; native recipe gate remains open', allow_module_level=True)

from controllers.generic.modular_spot import ModularSpotControllerConfig
from controllers.generic.modular_spot_core.recipe_compiler import bind_native_bundle, write_bundle
from controllers.generic.modular_spot_core.recipe_contract import StrategyRecipe


@pytest.mark.parametrize('family', ['rebound', 'impulse', 'sell_only'])
def test_actual_native_recipe_roundtrip(family, tmp_path):
    recipe = StrategyRecipe(name='native', family=family, symbols=('BTC-USDC',), quote_budget='100', max_positions=1)
    bundle = bind_native_bundle(recipe)
    assert bundle['native_validation'] == 'CONFIGURATION_PASSED'
    assert bundle['native_runtime_validation'] == 'NOT_RUN'
    config = ModularSpotControllerConfig.model_validate(bundle['controllers'][0])
    assert not config.execution_enabled
    assert config.acquisition_enabled == (family != 'sell_only')
    assert config.recipe_binding.recipe_sha256 == recipe.digest
    output = tmp_path / family
    write_bundle(bundle, output)
    stored = json.loads((output / (config.id + '.yml')).read_text())
    assert ModularSpotControllerConfig.model_validate(stored) == config
    changed = dict(stored)
    changed['total_amount_quote'] = '90'
    with pytest.raises(ValueError, match='configuration changed'):
        ModularSpotControllerConfig.model_validate(changed)
    changed = dict(stored)
    changed['acquisition_enabled'] = False
    ModularSpotControllerConfig.model_validate(changed)
    assert not (output / 'INCOMPLETE').exists()


def test_sell_only_recipe_cannot_reenable_acquisition():
    recipe = StrategyRecipe(name='native', family='sell_only', symbols=('BTC-USDC',), quote_budget='100', max_positions=1)
    payload = bind_native_bundle(recipe)['controllers'][0]
    payload['acquisition_enabled'] = True
    with pytest.raises(ValueError, match='sell-only'):
        ModularSpotControllerConfig.model_validate(payload)
