"""The market-context package lives outside the sealed component tree, so existing recipe seals are unchanged."""
from pathlib import Path

from controllers.generic import modular_spot, modular_spot_research
from hummingbot.strategy_v2.components import source_identity
from hummingbot.strategy_v2.components.recipe_contract import source_records
from hummingbot.strategy_v2.market_context import provenance


def test_package_is_not_in_component_or_recipe_seals():
    package = provenance.package_root()
    component_root = source_identity.component_root().resolve()
    assert not package.is_relative_to(component_root)
    assert not any('market_context' in str(path) for path in source_identity.component_source_files())
    for files in (modular_spot.source_files(), modular_spot_research.source_files()):
        assert not any('market_context' in str(Path(path)) for path in files)
    records = source_records(modular_spot.source_root(), modular_spot.source_files())
    assert not any('market_context' in name for name, _ in records)


def test_package_code_identity_covers_itself_and_components():
    files = provenance.package_source_files()
    assert {'contracts.py', 'universe.py', 'alignment.py', 'features.py', 'state.py'} <= {p.name for p in files}
    records = dict(source_records(provenance.package_root(), files))
    assert 'features.py' in records and '@strategy_components/recipe_contract.py' in records
    assert provenance.package_source_digest() == provenance.package_source_digest()
