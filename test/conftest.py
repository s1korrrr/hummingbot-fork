"""Process-wide test isolation guards for the hummingbot test tree."""
import sys

import pytest


def _controller_modules():
    return {name: module for name, module in sys.modules.items()
            if name == "controllers" or name.startswith("controllers.")}


@pytest.fixture(autouse=True)
def _restore_purged_controller_modules():
    """Undo a test's purge of the ``controllers`` package after the test ends.

    Several legacy controller tests drop every ``controllers.*`` entry from
    ``sys.modules`` in ``setUp`` and import fresh copies. Without a restore, files
    collected earlier keep the old classes in their globals while production code
    that imports at call time (for example ``NativePaperSession`` admission or
    ``get_controller_class``) resolves the new copies, so identity checks fail
    only when those files share a process.

    Additive imports into the existing package tree are consistent and kept. If
    the test replaced or dropped any module that existed before it, the whole
    namespace is returned to the exact pre-test mapping, including dropping the
    re-imported copies that bind to the purged tree.
    """
    before = _controller_modules()
    yield
    after = _controller_modules()
    if all(after.get(name) is module for name, module in before.items()):
        return
    for name in after.keys() - before.keys():
        sys.modules.pop(name, None)
    sys.modules.update(before)
