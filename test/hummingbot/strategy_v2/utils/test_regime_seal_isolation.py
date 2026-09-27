"""An unwired research module must not join the sealed source closure of native controllers."""
from hummingbot.strategy_v2.components.recipe_contract import source_records
from hummingbot.strategy_v2.components.source_identity import component_source_files


def test_regime_module_is_outside_the_component_seal():
    assert not any(path.name == 'regime.py' for path in component_source_files())


def test_native_controller_seals_exclude_the_regime_module():
    from controllers.generic import modular_spot, rsi_modular
    from controllers.generic.modular_spot_core import native_composition

    sealed = {name for name, _ in source_records(modular_spot.source_root(), modular_spot.source_files())}
    for profile in ('ok_rsi', 'rsi_v5'):
        sealed |= {name for name, _ in source_records(native_composition.source_root(), rsi_modular.source_files(profile))}
    assert not any(name.endswith('regime.py') for name in sealed)
