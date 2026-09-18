"""Structural isolation checks, not substitutes for native runtime tests."""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENTRY = ROOT / 'controllers/generic/modular_spot.py'
CORE = ENTRY.parent / 'modular_spot_core'


def test_controller_does_not_inherit_a_strategy_family():
    tree = ast.parse(ENTRY.read_text())
    controller = next(x for x in tree.body if isinstance(x, ast.ClassDef) and x.name == 'ModularSpotController')
    assert [ast.unparse(base) for base in controller.bases] == ['ControllerBase']


def test_no_legacy_controller_imports_or_shared_registry_mutations():
    for path in [ENTRY, *CORE.glob('*.py')]:
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith('controllers.'):
                    assert node.module.startswith('controllers.generic.modular_spot_core')
            if isinstance(node, ast.Import):
                assert all(not name.name.startswith('controllers.directional_trading') for name in node.names)
            if isinstance(node, ast.Attribute):
                assert node.attr not in {'_executor_mapping', 'monkeypatch'}
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in {'buy', 'sell', 'place_order', 'create_order'}


def test_execution_config_remains_native_and_order_lifecycle_is_inherited():
    tree = ast.parse((CORE / 'execution.py').read_text())
    classes = [x for x in tree.body if isinstance(x, ast.ClassDef)]
    assert len(classes) == 1 and classes[0].name == 'ModularPositionExecutor'
    assert [ast.unparse(x) for x in classes[0].bases] == ['PositionExecutor']
    methods = {x.name for x in classes[0].body if isinstance(x, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert not methods.intersection({'process_order_filled_event', 'process_order_canceled_event',
                                     'process_order_failed_event', '_control_single_attempt_entry',
                                     'control_shutdown_process', 'place_open_order'})


def test_no_import_time_tasks_or_client_construction():
    for path in [ENTRY, *CORE.glob('*.py')]:
        tree = ast.parse(path.read_text())
        for statement in tree.body:
            if isinstance(statement, ast.Expr):
                assert isinstance(statement.value, ast.Constant)
