import ast
import importlib
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

from .test_composition import api, evaluate, recipe, recover


def test_added_components_change_composite_seal_and_old_binding_rejects(tmp_path, monkeypatch):
    root = source_identity.component_root()
    frozen = tmp_path / 'components'
    frozen.mkdir()
    added = {'market_stress_v1.py', 'recovery_evidence_v1.py'}
    for path in source_identity.component_source_files(root):
        if path.name not in added:
            target = frozen / path.relative_to(root)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(path.read_bytes())
    consumer = tmp_path / 'consumer.py'
    consumer.write_text('from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding\n')
    monkeypatch.setattr(source_identity, 'component_root', lambda: frozen)
    old = source_digest(tmp_path, [consumer])
    binding = RecipeBinding(recipe_id='test', recipe_sha256='a' * 64, source_sha256=old,
                            configuration_sha256=configuration_digest({}))
    for name in added:
        (frozen / name).write_bytes((root / name).read_bytes())
    new = source_digest(tmp_path, [consumer])
    assert old != new
    with pytest.raises(PolicyError, match='source changed'):
        verify_binding(binding, {}, source_sha256=new)
    verify_binding(binding, {}, source_sha256=old)


def test_neutral_components_never_import_a_controller_family():
    for name in ('market_stress_v1', 'recovery_evidence_v1'):
        module = importlib.import_module('hummingbot.strategy_v2.components.' + name)
        tree = ast.parse(Path(module.__file__).read_text())
        imports = [node.module or '' for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        imports += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
        assert not any(x.startswith('controllers') for x in imports)
        assert not any(x.startswith(('socket', 'sqlite3', 'sqlalchemy', 'subprocess')) for x in imports)


def test_new_imports_and_evaluation_do_not_start_services_or_write_files():
    script = 'import sys; sys.path.insert(0, ' + repr(str(Path(__file__).resolve().parents[1])) + ')\n' + '''
import sys
from crash_recovery_v2.test_evidence import packet, policies
from hummingbot.strategy_v2.components.candle_data import CandleSnapshot
from hummingbot.strategy_v2.components.decision_protocol import decide
import socket, sqlite3, subprocess, threading

def forbidden(*args, **kwargs):
    raise AssertionError('unexpected service, network or database action')
socket.socket = forbidden
sqlite3.connect = forbidden
subprocess.Popen = forbidden
threading.Thread.start = forbidden
def audit(event, args):
    if event in ('os.mkdir', 'os.remove', 'os.rename', 'socket.connect', 'sqlite3.connect'):
        forbidden()
    if event == 'open' and (any(c in (args[1] or '') for c in 'wax+') or args[2] & 3):
        forbidden()
sys.addaudithook(audit)
from hummingbot.strategy_v2.components.market_stress_v1 import evaluate_stress
from hummingbot.strategy_v2.components.recovery_evidence_v1 import evaluate_recovery
p = packet(())
a, b = policies()
s = evaluate_stress(None, p, a, p.received_at_ms)
r = evaluate_recovery(None, s, b, p.received_at_ms)
assert r.status == 'HOLD'
assert not any(n.startswith('controllers.directional_trading') for n in sys.modules)
print('no service or controller-family import')
'''
    result = subprocess.run([sys.executable, '-B', '-c', script], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr


def test_recipe_does_not_register_as_native_controller_or_create_orders():
    _, runtime = api()
    module = importlib.import_module('controllers.generic.crash_recovery_v2_core.recipe')
    for obj in vars(module).values():
        if isinstance(obj, type) and obj.__module__ == module.__name__:
            assert not any(base.__name__ in ('ControllerBase', 'PositionExecutor') for base in obj.__mro__)
    x = recover()
    assert x.execution_authorized is False and x.trace.execution_authorized is False
    assert not hasattr(runtime, 'create_order') and not hasattr(runtime, 'create_executor')


def test_partial_or_clock_corrupted_composition_checkpoint_rejects():
    x = recover()
    for change in ({'recovery': None}, {'clock_ms': True}, {'schema_version': True},
                   {'clock_ms': x.state.clock_ms - 1}):
        with pytest.raises(ValueError):
            evaluate(x.state.model_copy(update=change), (90, 90, 91, 94, 94))


def test_recipe_limits_and_extra_runtime_controls_reject():
    from .test_evidence import DATASET
    for changes in ({'dataset': DATASET.model_copy(update={'connector': 'binance', 'source_profile': 'proxy'})},
                    {'lifetime_ms': 120001}, {'unknown_live_fallback': True},
                    {'dataset': DATASET.model_copy(update={'history': 61})}):
        with pytest.raises(ValueError):
            recipe(**changes)
