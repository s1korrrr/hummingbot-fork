"""One public entrypoint selects genuine native policies without a second owner."""
import asyncio
import importlib
from types import SimpleNamespace

import pytest
from test_native_lifecycle import recorder as _recorder_fixture

recorder = _recorder_fixture


def payload(profile, **changes):
    return dict(controller_name='rsi_modular', controller_type='generic', profile=profile,
                id='modular_spot_unified', trading_pair='BTC-USDC', strict_hmm_mode=False, **changes)


@pytest.mark.parametrize('profile', ['ok_rsi', 'rsi_v5'])
def test_public_profile_roundtrip_and_exact_authority(profile):
    from hummingbot.strategy_v2.controllers.controller_loader import load_controller_config
    from hummingbot.strategy_v2.controllers.native_policy_identity import is_modular_policy, is_ok_rsi_policy
    config = load_controller_config(payload(profile))
    assert config.profile == profile
    assert type(load_controller_config(config.model_dump(mode='json'))) is type(config)
    assert is_modular_policy(config)
    assert is_ok_rsi_policy(config) is (profile == 'ok_rsi')
    assert not is_modular_policy(SimpleNamespace(controller_name='rsi_modular', profile=profile))
    assert not is_ok_rsi_policy(SimpleNamespace(controller_name='rsi_modular', profile=profile))


@pytest.mark.parametrize('profile', [None, '', 'hybrid', True])
def test_missing_unknown_profile_has_no_default(profile):
    from hummingbot.strategy_v2.controllers.controller_loader import load_controller_config
    with pytest.raises(ValueError):
        load_controller_config(payload(profile))


def test_wrong_profile_fields_and_forged_subclass_reject():
    from controllers.generic.rsi_modular import get_config_class
    from hummingbot.strategy_v2.controllers.native_policy_identity import is_modular_policy, is_ok_rsi_policy
    with pytest.raises(ValueError):
        get_config_class('rsi_v5')(**payload('rsi_v5', episode_enabled=True))

    class Forged(get_config_class('ok_rsi')):
        pass
    forged = Forged(**payload('ok_rsi'))
    assert not is_modular_policy(forged)
    assert not is_ok_rsi_policy(forged)


@pytest.mark.parametrize('profile', ['ok_rsi', 'rsi_v5'])
def test_native_factory_parity_and_public_source_closure(profile):
    from test_native_composition import D, provider

    from controllers.generic.rsi_modular import get_config_class, source_files
    public = get_config_class(profile)(**payload(profile))
    legacy_module = importlib.import_module('controllers.generic.modular_' + profile)
    old_values = public.model_dump(exclude={'profile'})
    old_values['controller_name'] = 'modular_' + profile
    old = getattr(legacy_module, legacy_module.CONFIG_CLASS)(**old_values)
    owners = [c.get_controller_class()(c, provider(), asyncio.Queue()) for c in (public, old)]
    for owner in owners:
        owner._latest_atr_pct = .01
        owner.processed_data = {'signal': 1, 'signal_state': {'condition_ok': True, 'size_multiplier': 1.0}}
    actions = [owner.get_executor_config(__import__('hummingbot.core.data_type.common', fromlist=['TradeType']).TradeType.BUY,
                                         D('100'), D('.3')) for owner in owners]
    assert actions[0].model_dump(exclude={'id'}) == actions[1].model_dump(exclude={'id'})
    files = {str(path) for path in source_files(profile)}
    assert any(path.endswith('/generic/rsi_modular.py') for path in files)
    assert any(path.endswith('/directional_trading/' + profile + '.py') for path in files)
    with pytest.raises(ValueError, match='restart required|profile'):
        owners[0].update_config(get_config_class('rsi_v5' if profile == 'ok_rsi' else 'ok_rsi')(
            **payload('rsi_v5' if profile == 'ok_rsi' else 'ok_rsi')))


@pytest.mark.parametrize('order', [('ok_rsi', 'rsi_v5'), ('rsi_v5', 'ok_rsi')])
@pytest.mark.asyncio
async def test_real_backtest_run_selects_correct_native_class_in_both_cache_orders(order, monkeypatch):
    from unittest.mock import AsyncMock, Mock

    from test_native_composition import provider

    from controllers.generic.rsi_modular import get_config_class
    from hummingbot.strategy_v2.backtesting.backtesting_engine_base import BacktestingEngineBase
    engine = BacktestingEngineBase()
    engine.backtesting_data_provider = provider()
    engine.backtesting_data_provider.initialize_trading_rules = AsyncMock()
    engine.initialize_backtesting_data_provider = AsyncMock()
    engine.simulate_execution = AsyncMock(return_value=[])
    engine.summarize_results = Mock(return_value={})
    for profile in (*order, order[0]):
        config = get_config_class(profile)(**payload(profile))
        controller_type = config.get_controller_class()
        monkeypatch.setattr(controller_type, 'update_processed_data', AsyncMock())
        await engine.run_backtesting(config, start=600000, end=600060)
        assert type(engine.controller) is controller_type
        assert engine.controller._native_family == profile
    # This isolates actual class-cache dispatch, not strategy/economic evaluation.


@pytest.mark.parametrize('profile', ['ok_rsi', 'rsi_v5'])
def test_runtime_yaml_and_backtest_loader_share_public_schema(profile, monkeypatch, tmp_path):
    import yaml

    from controllers.generic.rsi_modular import get_config_class
    from hummingbot.client import settings
    from hummingbot.strategy.strategy_v2_base import StrategyV2ConfigBase
    from hummingbot.strategy_v2.backtesting.backtesting_engine_base import BacktestingEngineBase
    monkeypatch.setattr(settings, 'CONTROLLERS_CONF_DIR_PATH', str(tmp_path))
    filename = tmp_path / 'public.yml'
    filename.write_text(yaml.safe_dump(payload(profile)))
    runtime = StrategyV2ConfigBase(controllers_config=['public.yml']).load_controller_configs()[0]
    backtest = BacktestingEngineBase.get_controller_config_instance_from_dict(payload(profile))
    assert type(runtime) is type(backtest) is get_config_class(profile)
    assert runtime.model_dump(mode='json') == backtest.model_dump(mode='json')


@pytest.mark.parametrize('profile', ['ok_rsi', 'rsi_v5'])
@pytest.mark.asyncio
async def test_cli_prompts_for_profile_before_native_config_creation(profile, monkeypatch):
    from unittest.mock import AsyncMock, Mock

    from controllers.generic.rsi_modular import get_config_class
    from hummingbot.client.command.create_command import CreateCommand
    command = CreateCommand()
    command.app = SimpleNamespace(prompt=AsyncMock(return_value=profile), to_stop_config=False,
                                  change_prompt=Mock(), input_field=SimpleNamespace(completer=None), hide_input=True)
    command.notify = Mock()
    command.reset_application_state = Mock()
    command.prompt_for_model_config = AsyncMock()
    command.save_config = AsyncMock(return_value='public.yml')
    monkeypatch.setattr('hummingbot.client.command.create_command.load_completer', lambda _: None)
    await command.prompt_for_controller_config('generic.rsi_modular')
    command.reset_application_state.assert_not_called()
    command.prompt_for_model_config.assert_awaited_once()
    model = command.prompt_for_model_config.call_args.args[0].hb_config
    assert type(model) is get_config_class(profile)
    assert model.profile == profile and model.id.startswith('modular_spot_')
    assert get_config_class(profile).model_validate(model.model_dump()).profile == profile


@pytest.mark.parametrize('profile', ['ok_rsi', 'rsi_v5'])
def test_profile_is_immutable_even_on_direct_assignment(profile):
    from controllers.generic.rsi_modular import get_config_class
    config = get_config_class(profile)(**payload(profile))
    with pytest.raises(ValueError, match='Input should be'):
        config.profile = 'rsi_v5' if profile == 'ok_rsi' else 'ok_rsi'
    assert config.profile == profile


@pytest.mark.parametrize('profile', ['ok_rsi', 'rsi_v5'])
def test_public_native_action_path_preserves_policy(profile, recorder):
    from test_native_composition import FAMILIES, test_actual_native_create_action_path_matches

    from controllers.generic.rsi_modular import get_config_class
    config_class = get_config_class(profile)
    native_config, native = next(row[3:] for row in FAMILIES if row[0] == profile)
    test_actual_native_create_action_path_matches(
        profile, config_class, config_class(**payload(profile)).get_controller_class(), native_config, native, recorder)


@pytest.mark.parametrize('profile', ['ok_rsi', 'rsi_v5'])
def test_native_observation_publishes_genuine_config_profile(profile):
    from test_native_composition import provider

    from controllers.generic.rsi_modular import get_config_class
    from hummingbot.strategy.native_observability import controller_observation
    config = get_config_class(profile)(**payload(profile))
    owner = config.get_controller_class()(config, provider(), asyncio.Queue())
    wrong = 'rsi_v5' if profile == 'ok_rsi' else 'ok_rsi'
    owner.get_custom_info = lambda: {'controller_name': 'forged', 'profile': wrong,
                                     'modular_composition': {'controller_name': 'forged', 'profile': wrong}}
    result = controller_observation({'controller': owner, 'controller_id': config.id,
                                     'summary': {'controller_name': 'forged', 'profile': wrong}})
    assert result['controller_name'] == 'rsi_modular'
    assert result['profile'] == profile
    assert result['controller_id'] == config.id
    assert result['observation_status'] == 'available'
