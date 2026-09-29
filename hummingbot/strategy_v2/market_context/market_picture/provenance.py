"""Market Picture source identity, including its neutral owner dependencies."""

from hummingbot.strategy_v2.components.recipe_contract import source_digest
from hummingbot.strategy_v2.market_context.provenance import package_root


def package_source_digest() -> str:
    # Canonical context retains its independent identity. The picture extension
    # imports it and must additionally seal its own nested calculation files.
    root = package_root()
    return source_digest(root, sorted(root.rglob('*.py')))
