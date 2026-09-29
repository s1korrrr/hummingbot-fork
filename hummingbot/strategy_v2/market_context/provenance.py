"""Code identity of this package for snapshot provenance. Reads its own source files only when called."""
from pathlib import Path

from hummingbot.strategy_v2.components.recipe_contract import source_digest


def package_root() -> Path:
    return Path(__file__).resolve().parent


def package_source_files() -> list[Path]:
    return sorted(package_root().glob('*.py'))


def package_source_digest() -> str:
    """Digest of this package plus, because it imports them, every sealed strategy component source."""
    return source_digest(package_root(), package_source_files())
