
import pytest

from hummingbot.strategy_v2.components.recipe_contract import source_digest, source_records
from hummingbot.strategy_v2.components.source_identity import component_source_files


def test_composite_records_include_canonical_components_and_detect_mutation(tmp_path, monkeypatch):
    from hummingbot.strategy_v2.components import source_identity
    root = tmp_path / 'controllers'
    root.mkdir()
    source = root / 'example.py'
    source.write_text('from hummingbot.strategy_v2.components.domain import PolicyError\n')
    components = tmp_path / 'components'
    components.mkdir()
    for name in ('__init__.py', 'source_identity.py', 'recipe_contract.py', 'domain.py'):
        (components / name).write_text('# component\n')
    monkeypatch.setattr(source_identity, 'component_root', lambda: components)
    records = dict(source_records(root, [source]))
    assert '@strategy_components/domain.py' in records
    first = source_digest(root, [source])
    (components / 'domain.py').write_text('# changed\n')
    assert source_digest(root, [source]) != first


def test_legacy_sources_keep_controller_only_digest(tmp_path):
    from hashlib import sha256

    from hummingbot.strategy_v2.components.recipe_contract import digest
    source = tmp_path / 'legacy.py'
    source.write_text('value = 1\n')
    assert source_digest(tmp_path, [source]) == digest([('legacy.py', sha256(source.read_bytes()).hexdigest())])


@pytest.mark.parametrize('mode', ['missing', 'symlink_file', 'symlink_directory', 'oversize'])
def test_component_manifest_rejects_unsafe_source(tmp_path, mode):
    for name in ('__init__.py', 'source_identity.py', 'recipe_contract.py'):
        (tmp_path / name).write_text('# component\n')
    if mode == 'missing':
        (tmp_path / 'recipe_contract.py').unlink()
    elif mode == 'symlink_file':
        (tmp_path / 'escape.py').symlink_to('/etc/hosts')
    elif mode == 'symlink_directory':
        (tmp_path / 'escape').symlink_to('/private/tmp', target_is_directory=True)
    else:
        (tmp_path / 'big.py').write_bytes(b'x' * (2 * 1024 * 1024 + 1))
    with pytest.raises(ValueError):
        component_source_files(tmp_path)


def test_actual_native_and_generic_seals_and_runtime_guards_cover_components(tmp_path, monkeypatch):
    import shutil

    from controllers.generic import modular_spot, rsi_modular
    from controllers.generic.modular_spot_core.native_recipe import NativePolicyRecipe
    from controllers.generic.modular_spot_core.recipe_compiler import bind_native_bundle
    from hummingbot.strategy_v2.components import source_identity

    # Model the production layout: controllers and the installed engine package
    # are separate roots; the canonical component namespace must still be sealed.
    installed = tmp_path / 'site-packages/hummingbot/strategy_v2/components'
    shutil.copytree(source_identity.component_root(), installed, ignore=shutil.ignore_patterns('__pycache__'))
    monkeypatch.setattr(source_identity, 'component_root', lambda: installed)
    recipes = [NativePolicyRecipe(name='native_identity', family=profile,
                                  controller_id='modular_spot_identity', parameters={'trading_pair': 'BTC-USDC'})
               for profile in ('ok_rsi', 'rsi_v5')]
    bundles = [bind_native_bundle(recipe) for recipe in recipes]
    assert all(bundle['receipt_kind'] == 'native_policy_composition_v2' for bundle in bundles)
    assert all('@strategy_components/domain.py' in bundle['source_files'] for bundle in bundles)
    configs = [rsi_modular.get_config_class(profile).model_validate(bundle['controllers'][0])
               for profile, bundle in zip(('ok_rsi', 'rsi_v5'), bundles)]
    generic = modular_spot.ModularSpotControllerConfig(id='modular_spot_component_identity',
                                                       connector_name='okx', trading_pair='BTC-USDC')
    fingerprint = generic.fingerprint()
    assert installed / 'domain.py' in modular_spot.ModularSpotController.implementation_source_files()
    assert all(installed / 'domain.py' in cfg.get_controller_class().implementation_source_files() for cfg in configs)
    (installed / 'domain.py').write_text((installed / 'domain.py').read_text() + '\n# changed component\n')
    assert generic.fingerprint() != fingerprint
    for cfg in configs:
        with pytest.raises(ValueError, match='implementation source changed'):
            type(cfg).model_validate(cfg.model_dump(mode='json'))
