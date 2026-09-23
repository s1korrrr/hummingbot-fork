import ast
import subprocess
import sys
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parents[2] / 'controllers/generic/mr_context_v2_core'


def test_source_imports_only_pure_contract_dependencies():
    forbidden = ('controllers.', 'services.', 'hummingbot.connector', 'hummingbot.strategy_v2.executors',
                 'hummingbot.strategy_v2.controllers', 'asyncio', 'socket', 'requests', 'sqlite3', 'subprocess')
    adapter_hummingbot = ('hummingbot.strategy_v2.models.executor_actions',
                          'hummingbot.core.data_type.common')
    for path in sorted(PACKAGE.glob('*.py')):
        if path.name == 'host.py':
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                names = [node.module or '']
            else:
                continue
            if path.name == 'native_adapter.py':
                blocked = [name for name in names if name.startswith(forbidden)
                           and not name.startswith(adapter_hummingbot)]
                assert not blocked, path
                extra = adapter_hummingbot
            else:
                assert not any(name.startswith(forbidden) for name in names), path
                extra = ()
            if any(name.startswith('hummingbot.') for name in names):
                assert all(name.startswith('hummingbot.strategy_v2.components.') or name.startswith(extra)
                           for name in names), path


def test_import_starts_no_services_and_loads_no_legacy_family():
    script = '''
import hummingbot, pydantic
import socket, threading

def forbidden(*args, **kwargs):
    raise AssertionError('candidate import attempted I/O or thread startup')

socket.create_connection = forbidden
socket.socket.connect = forbidden
threading.Thread.start = forbidden
import controllers.generic.mr_context_v2_core.snapshot
import controllers.generic.mr_context_v2_core.recipe
import controllers.generic.mr_context_v2_core.policy
import controllers.generic.mr_context_v2_core.producer
import controllers.generic.mr_context_v2_core.native_adapter
import sys
assert not any(name.startswith(('controllers.directional_trading',
                               'controllers.generic.modular_spot_core')) for name in sys.modules)
assert not any(name.startswith('hummingbot.strategy_v2.executors')
               and not name.startswith('hummingbot.strategy_v2.executors.data_types')
               and name != 'hummingbot.strategy_v2.executors'
               for name in sys.modules)
'''
    result = subprocess.run([sys.executable, '-c', script], cwd=PACKAGE.parents[2],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr


def test_canonical_binding_and_protocol_objects_are_not_replaced():
    from controllers.generic.mr_context_v2_core import policy, recipe, snapshot
    from hummingbot.strategy_v2.components.decision_protocol import DecisionTrace, ObservationRef, Prerequisite, decide
    from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding
    assert policy.DecisionTrace is DecisionTrace
    assert policy.Prerequisite is Prerequisite
    assert policy.decide is decide
    assert snapshot.ObservationRef is ObservationRef
    assert recipe.RecipeBinding is RecipeBinding


@pytest.mark.parametrize('source', ['snapshot.py', 'recipe.py', 'policy.py', 'producer.py', 'native_adapter.py'])
def test_candidate_code_declares_no_order_or_balance_mutation_api(source):
    tree = ast.parse((PACKAGE / source).read_text())
    definitions = {node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert not definitions & {'buy', 'sell', 'create_executor', 'reserve', 'release', 'settle', 'start', 'stop'}


def test_rsi_modular_source_seal_excludes_context_package():
    try:
        from controllers.generic.rsi_modular import source_files
        from hummingbot.strategy_v2.components.source_identity import component_source_files
    except ModuleNotFoundError:
        pytest.skip('compiled hummingbot extensions unavailable')
    for profile in ('ok_rsi', 'rsi_v5'):
        files = source_files(profile)
        assert not any('mr_context' in str(path) for path in files)
        assert any(str(path).endswith(f'/{profile}.py') for path in files)
        assert any(str(path).endswith('/generic/rsi_modular.py') for path in files)
        assert any('/modular_spot_core/' in str(path) for path in files)
    assert not any('mr_context' in str(path) for path in component_source_files())
