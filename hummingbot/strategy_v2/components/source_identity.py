"""Bounded source identities for neutral strategy components, never trading authority."""
import ast
import os
from pathlib import Path

MAX_FILES = 1000
MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_TOTAL_BYTES = 16 * 1024 * 1024
NAMESPACE = '@strategy_components/'


def component_root():
    return Path(__file__).parent


def checked_source(path):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts or any(p.is_symlink() for p in (path, *path.parents)) or not path.is_file():
        raise ValueError('implementation source must be an absolute non-symlink regular file')
    with path.open('rb') as stream:
        data = stream.read(MAX_FILE_BYTES + 1)
    if len(data) > MAX_FILE_BYTES:
        raise ValueError('implementation source exceeds the admitted size')
    return data


def component_source_files(root=None):
    root = Path(root) if root is not None else component_root()
    if not root.is_absolute() or '..' in root.parts or any(p.is_symlink() for p in (root, *root.parents)) or not root.is_dir():
        raise ValueError('component source root must be an absolute non-symlink directory')
    paths, total = [], 0

    def inaccessible(error):
        raise error

    for directory, dirs, files in os.walk(root, followlinks=False, onerror=inaccessible):
        # Reject every link before filtering: dangling directory links are reported as files.
        if any((Path(directory) / name).is_symlink() for name in (*dirs, *files)):
            raise ValueError('component source cannot contain symlink entries')
        for name in files:
            path = Path(directory) / name
            if path.suffix != '.py':
                continue
            total += len(checked_source(path))
            paths.append(path)
            if len(paths) > MAX_FILES or total > MAX_TOTAL_BYTES:
                raise ValueError('component source exceeds admitted capacity')
    if not {'__init__.py', 'source_identity.py', 'recipe_contract.py'} <= {p.relative_to(root).as_posix() for p in paths}:
        raise ValueError('canonical component source manifest is incomplete')
    return sorted(paths)


def depends_on_components(data):
    """Recognize explicit Python dependencies without importing inspected source."""
    prefix = 'hummingbot.strategy_v2.components'
    for node in ast.walk(ast.parse(data)):
        if isinstance(node, ast.Import) and any(a.name == prefix or a.name.startswith(prefix + '.') for a in node.names):
            return True
        if isinstance(node, ast.ImportFrom):
            if node.module == prefix or (node.module or '').startswith(prefix + '.'):
                return True
            if node.module == 'hummingbot.strategy_v2' and any(a.name == 'components' for a in node.names):
                return True
    return False
