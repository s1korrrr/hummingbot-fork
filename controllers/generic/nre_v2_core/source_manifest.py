"""Explicit source inspection for this opt-in package; call before and after research replay."""
import os
from pathlib import Path

from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import digest, source_records
from hummingbot.strategy_v2.components.source_identity import checked_source

MODULES = frozenset({'__init__.py', 'recovery_path.py', 'binding.py', 'admission.py', 'source_manifest.py'})


def _package_files(root: Path) -> tuple[Path, ...]:
    root = Path(root).resolve()
    if not root.is_absolute() or any(p.is_symlink() for p in (root, *root.parents)) or not root.is_dir():
        raise PolicyError('regular absolute package root is required')
    found = {}

    def inaccessible(error):
        raise error

    for directory, dirs, files in os.walk(root, followlinks=False, onerror=inaccessible):
        if any((Path(directory) / name).is_symlink() for name in (*dirs, *files)):
            raise PolicyError('source links are forbidden')
        for name in files:
            p = Path(directory) / name
            if p.suffix == '.py':
                key = p.relative_to(root).as_posix()
                if key not in MODULES:
                    raise PolicyError('undeclared implementation module')
                checked_source(p)
                found[key] = p
    if set(found) != MODULES:
        raise PolicyError('implementation module set is incomplete')
    return tuple(found[name] for name in sorted(found))


def source_manifest() -> tuple[tuple[str, str], ...]:
    package = Path(__file__).absolute().parent
    files = list(_package_files(package))
    owner = package.parents[1]
    for parent in (owner, package.parent):
        initializer = parent / '__init__.py'
        if initializer.exists() or initializer.is_symlink():
            checked_source(initializer)
            files.append(initializer)
    return tuple(source_records(owner, files))


def implementation_sha256() -> str:
    return digest(source_manifest())


def verify_implementation(expected_sha256: str) -> str:
    observed = implementation_sha256()
    if type(expected_sha256) is not str or observed != expected_sha256:
        raise PolicyError('actual NRE source closure differs from the externally pinned implementation')
    return observed
