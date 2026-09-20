"""Source wiring checks; these are explicitly not native execution evidence."""
import ast
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[2] / 'controllers/generic/modular_spot.py'


def test_recipe_binding_is_on_real_native_schema():
    tree = ast.parse(SOURCE.read_text())
    config = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'ModularSpotControllerConfig')
    fields = {node.target.id: node for node in config.body if isinstance(node, ast.AnnAssign)}
    assert 'recipe_binding' in fields, 'native configuration must validate the optional recipe binding'
    assert isinstance(fields['recipe_binding'].value, ast.Constant) and fields['recipe_binding'].value.value is None
    method = next(node for node in config.body if isinstance(node, ast.FunctionDef) and node.name == 'validate_composition')
    calls = {node.func.id for node in ast.walk(method) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
    assert {'source_digest', 'verify_binding'} <= calls


def test_recipe_projection_does_not_create_runtime_authority():
    tree = ast.parse(SOURCE.read_text())
    controller = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'ModularSpotController')
    method = next(node for node in controller.body if isinstance(node, ast.AsyncFunctionDef) and node.name == 'update_processed_data')
    keys = {node.value for node in ast.walk(method) if isinstance(node, ast.Constant) and isinstance(node.value, str)}
    assert 'recipe_binding' in keys
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                   and node.func.attr in {'actions', 'start', 'buy', 'sell'} for node in ast.walk(method))
