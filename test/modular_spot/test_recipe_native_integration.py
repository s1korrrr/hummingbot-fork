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


def test_recipe_preserves_assigned_native_topology_and_revisions():
    recipe = StrategyRecipe(name='bound', family='rebound', symbols=('BTC-USDC',),
                            quote_budget='100', max_positions=1, account='offline_audit',
                            native_owners=('modular_spot_existing',), benchmarks=('ETH-USDC',),
                            program_revision='2', policy_revision='3')
    bundle = bind_native_bundle(recipe)
    config = ModularSpotControllerConfig.model_validate(bundle['controllers'][0])
    assert config.account == 'offline_audit'
    assert config.id == 'modular_spot_existing'
    assert config.universe.benchmarks == ('ETH-USDC',)
    assert config.data.symbols == ('BTC-USDC', 'ETH-USDC')
    assert (config.program_revision, config.policy_revision) == ('2', '3')
    assert not config.execution_enabled and not config.initial_positions
    changed = config.model_dump(mode='json')
    changed['account'] = 'other_account'
    with pytest.raises(ValueError, match='configuration changed'):
        ModularSpotControllerConfig.model_validate(changed)


def test_native_performance_report_exposes_only_bounded_modular_metadata():
    import asyncio
    from decimal import Decimal
    from types import SimpleNamespace

    from controllers.generic.modular_spot import ModularSpotController
    from scripts.v2_with_controllers import V2WithControllers

    recipe = StrategyRecipe(name='native', family='rebound', symbols=('BTC-USDC',), quote_budget='100', max_positions=1)
    config = ModularSpotControllerConfig.model_validate(bind_native_bundle(recipe)['controllers'][0])

    class Provider:
        ready = False

    controller = ModularSpotController(config, Provider(), asyncio.Queue())
    controller.runtime.reason = 'data_unavailable:private_exception_details'
    controller.runtime.coverage = Decimal('0.5')
    controller.processed_data = {'candle_open': 120, 'secret': 'never_report'}
    strategy = SimpleNamespace(controllers={config.id: controller}, controller_reports={})
    result = V2WithControllers.get_controller_report(strategy, config.id)
    info = result['custom_info']
    assert info['modular_status'] == 'data_unavailable'
    assert info['universe_coverage'] == '0.5'
    assert info['recipe_binding']['recipe_sha256'] == recipe.digest
    assert info['data_source'] == 'shared' and info['execution_enabled'] is False
    assert info['candle_open'] == 120
    encoded = json.dumps(result, allow_nan=False)
    assert 'never_report' not in encoded and config.data.token_file not in encoded
    assert 'private_exception_details' not in encoded
    assert len(json.dumps(info)) < 1024
    info['recipe_binding']['recipe_id'] = 'changed'
    assert controller.get_custom_info()['recipe_binding']['recipe_id'] == 'native'


def test_stream_recipe_exports_real_native_disabled_configuration(tmp_path):
    recipe = StrategyRecipe(name='stream', family='rebound', symbols=('BTC-USDC',),
                            quote_budget='100', max_positions=1, data_transport='stream', history=90)
    bundle = bind_native_bundle(recipe)
    output = tmp_path / 'stream-native'
    write_bundle(bundle, output)
    config = ModularSpotControllerConfig.model_validate_json(
        (output / 'modular_spot_stream_BTC_USDC.yml').read_text())
    assert config.max_records == 90
    assert config.data.transport == 'stream'
    assert not config.execution_enabled
    assert config.recipe_binding.recipe_sha256 == recipe.digest
    changed = config.model_dump(mode='json')
    changed['data']['transport'] = 'snapshot'
    with pytest.raises(ValueError, match='configuration changed'):
        ModularSpotControllerConfig.model_validate(changed)


def test_stream_capacity_is_admitted_before_native_startup():
    from controllers.generic.modular_spot_core.profiles import ProgramProfile

    values = dict(name='stream', symbols=('BTC-USDC',), quote_budget='100', max_positions=1)
    assert StrategyRecipe(**values, family='rebound', data_transport='stream').history == 300
    assert ProgramProfile(**values, connector='okx', mode='fixed', data_transport='stream').history == 300
    data = dict(mode='shared', transport='stream', origin='http://127.0.0.1:8100', token_file='/unused')
    config = ModularSpotControllerConfig(
        id='modular_spot_capacity', connector_name='okx', trading_pair='BTC-USDC', data=data)
    assert config.max_records == 300
    with pytest.raises(ValueError, match='USDC'):
        ModularSpotControllerConfig(id='modular_spot_quote', connector_name='okx', trading_pair='BTC-USDT', max_records=90, data=data)
    default = StrategyRecipe(**values, family='rebound')
    assert default.data_transport == 'snapshot' and default.history == 300
    config = ModularSpotControllerConfig.model_validate(bind_native_bundle(default)['controllers'][0])
    assert config.data.transport == 'snapshot' and config.max_records == 300
