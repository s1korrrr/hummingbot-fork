"""The isolated lifecycle preserves the real frozen-consumer seal. Path-only: no controller import."""
from pathlib import Path

from hummingbot.strategy_v2.components.recipe_contract import digest, source_records
from hummingbot.strategy_v2.components.source_identity import NAMESPACE, component_root

ADDED = NAMESPACE + 'spot_lifecycle_v2.py'
GENERIC = Path(__file__).resolve().parents[2] / 'controllers' / 'generic'


def modular_spot_records():
    files = [GENERIC / 'modular_spot.py', *sorted((GENERIC / 'modular_spot_core').rglob('*.py'))]
    return source_records(GENERIC, files)


def test_isolated_component_stays_outside_the_frozen_consumer_closure():
    assert not (component_root() / 'spot_lifecycle_v2.py').exists()
    assert ADDED not in {name for name, _ in modular_spot_records()}


def test_frozen_consumer_seal_matches_current_master_baseline():
    records = modular_spot_records()
    assert digest(records) == '14e76708f297834597beaa7b7eb38b89aa00e5ce1f781b01a406a8264c625224'  # noqa: documentation


def test_only_the_component_namespace_is_shared_with_the_frozen_consumer():
    owned = {name for name, _ in modular_spot_records() if not name.startswith(NAMESPACE)}
    assert owned and not any(name.startswith('spot_recipe_v2') for name in owned)
