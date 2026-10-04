import ast
import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from hummingbot.strategy_v2.components.recipe_contract import source_digest

from .test_composition import api, evaluate, recipe, recover


def test_crash_additions_preserve_frozen_modular_spot_source_identity():
    from controllers.generic.modular_spot import source_files, source_root
    from hummingbot.strategy_v2.components.recipe_contract import source_records
    rows = source_records(source_root(), source_files())
    assert not any(name.endswith(('market_stress_v1.py', 'recovery_evidence_v1.py', 'terminal_dust.py')) for name, _ in rows)
    # The signed-fee and native candle-ownership changes are recorded in
    # docs/controllers/modular_spot/RELEASE_AUDIT_20261003.md, and the V2 runtime
    # root-cause changes in RELEASE_AUDIT_20261004.md: the same 67-file closure,
    # with native_composition.py, native_funding.py and the candle_data,
    # candle_stream and data_reader components changed; crash and V3-only dust
    # modules remain outside it. Historical bindings are not rewritten. The
    # stalled-loop heartbeat drain in candle_stream is recorded there as well.
    assert source_digest(source_root(), source_files()) == '7d77a067a492161944c8038bf4c3e22184bd5916196d69bc57fb406de167179f'  # noqa: documentation


def test_neutral_components_never_import_a_controller_family():
    for name in ('market_stress_v1', 'recovery_evidence_v1'):
        module = importlib.import_module('controllers.generic.crash_recovery_v2_core.' + name)
        tree = ast.parse(Path(module.__file__).read_text())
        imports = [node.module or '' for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        imports += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
        assert not any(x.startswith('controllers') for x in imports)
        assert not any(x.startswith(('socket', 'sqlite3', 'sqlalchemy', 'subprocess')) for x in imports)


def test_new_imports_and_evaluation_do_not_start_services_or_write_files():
    repo = str(Path(__file__).resolve().parents[2])
    script = '''
from test.crash_recovery_v2.test_evidence import packet, policies
import socket, sqlite3, subprocess, sys, threading

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
from controllers.generic.crash_recovery_v2_core.market_stress_v1 import evaluate_stress
from controllers.generic.crash_recovery_v2_core.recovery_evidence_v1 import evaluate_recovery
p = packet(())
a, b = policies()
s = evaluate_stress(None, p, a, p.received_at_ms)
r = evaluate_recovery(None, s, b, p.received_at_ms)
assert r.status == 'HOLD'
assert not any(n.startswith('controllers.directional_trading') for n in sys.modules)
print('no service or controller-family import')
'''
    result = subprocess.run([sys.executable, '-B', '-c', script], capture_output=True, text=True,
                            timeout=180, env={**os.environ, 'PYTHONPATH': repo})
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
