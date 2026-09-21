import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

from hummingbot.strategy_v2.components import source_identity
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import (
    RecipeBinding,
    configuration_digest,
    source_digest,
    verify_binding,
)
from hummingbot.strategy_v2.components.spot_lifecycle_v2 import NativeScopeV2, NativeSnapshotV2, decode_snapshot

from .test_lifecycle import scope, snapshot
from .test_native_views import capture, native_info


def test_scope_reuses_canonical_binding_type():
    assert NativeScopeV2.model_fields['recipe_binding'].annotation is RecipeBinding


def test_neutral_component_has_no_controller_imports_or_new_threads():
    module = sys.modules[NativeSnapshotV2.__module__]
    tree = ast.parse(Path(module.__file__).read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert all(not alias.name.startswith('controllers') for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert not (node.module or '').startswith('controllers')
    script = '''import threading, sys
before = len(threading.enumerate())
from hummingbot.strategy_v2.components.spot_lifecycle_v2 import NativeSnapshotV2
assert len(threading.enumerate()) == before
assert not any(name.startswith("controllers.") for name in sys.modules)
'''
    result = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True,
                            env=os.environ.copy(), timeout=20)
    assert result.returncode == 0, result.stderr


def test_added_component_invalidates_existing_seal_instead_of_auto_resealing(tmp_path, monkeypatch):
    native = tmp_path / 'native'
    components = tmp_path / 'components'
    native.mkdir()
    components.mkdir()
    controller = native / 'consumer.py'
    controller.write_text('from hummingbot.strategy_v2.components import recipe_contract\n')
    for filename in ('__init__.py', 'source_identity.py', 'recipe_contract.py'):
        (components / filename).write_text('\n')
    monkeypatch.setattr(source_identity, 'component_root', lambda: components)
    before = source_digest(native, [controller])
    payload = {'constant': 'preserved'}
    old = RecipeBinding(recipe_id='baseline', recipe_sha256='a' * 64, source_sha256=before,
                        configuration_sha256=configuration_digest(payload))
    verify_binding(old, payload, source_sha256=before)
    (components / 'spot_lifecycle_v2.py').write_text('VALUE = 2\n')
    after = source_digest(native, [controller])
    assert before != after
    with pytest.raises(PolicyError, match='source changed'):
        verify_binding(old, payload, source_sha256=after)
    assert old.source_sha256 == before


def test_copied_nested_scope_is_revalidated_at_assessment_boundary():
    raw = snapshot().model_dump(mode='json')
    raw['scope']['recipe_binding']['source_sha256'] = 'bad'
    import json
    with pytest.raises(ValueError):
        decode_snapshot(json.dumps(raw))


@pytest.mark.parametrize('value', [float('nan'), {'x': 'y' * 1025}, {'x': list(range(129))}])
def test_native_receipt_hashing_is_bounded_and_rejects_nonfinite_fields(value):
    info = native_info()
    info.custom_info['execution_receipt']['additional_evidence'] = value
    with pytest.raises(ValueError):
        capture(info)


def test_recursive_native_receipt_rejected_without_unbounded_recursion():
    info = native_info()
    receipt = info.custom_info['execution_receipt']
    receipt['loop'] = receipt
    with pytest.raises(ValueError, match='structure bound'):
        capture(info)


def test_paper_receipt_cannot_be_rebound_as_live_inventory():
    with pytest.raises(ValueError, match='scope'):
        capture(scope=scope(connector='okx', execution_mode='live'))
