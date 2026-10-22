import ast
import asyncio
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from hummingbot.strategy_v2.components.recipe_contract import source_digest, source_records

CONTROLLERS = Path(__file__).resolve().parents[2] / 'controllers'
FAMILIES = {'ok_rsi': 'modular_ok_rsi', 'rsi_v5': 'modular_rsi_v5'}


def profile_source_files(family, *, public_module=None):
    root = CONTROLLERS
    pending = [root / 'generic' / ((public_module or FAMILIES[family]) + '.py'),
               root / 'directional_trading' / (family + '.py'),
               *sorted((root / 'generic' / 'modular_spot_core').rglob('*.py'))]
    seen = set()

    def enqueue(module):
        if module and module[0] == 'controllers':
            path = root.joinpath(*module[1:])
            for candidate in (path.with_suffix('.py'), path / '__init__.py'):
                if candidate.is_file() and candidate not in seen:
                    pending.append(candidate)

    while pending:
        path = pending.pop()
        if path in seen:
            continue
        seen.add(path)
        if len(seen) > 1000:
            raise AssertionError('native controller dependency closure exceeds bound')
        relative = path.relative_to(root)
        package = ('controllers', *relative.parts[:-1])
        for count in range(1, len(package) + 1):
            initializer = root.joinpath(*package[1:count]) / '__init__.py'
            if initializer.is_file() and initializer not in seen:
                pending.append(initializer)
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    enqueue(tuple(alias.name.split('.')))
            elif isinstance(node, ast.ImportFrom):
                suffix = tuple(node.module.split('.')) if node.module else ()
                module = (*package[:len(package) - node.level + 1], *suffix) if node.level else suffix
                enqueue(module)
                for alias in node.names:
                    enqueue((*module, alias.name))
            elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                  and node.func.attr == 'import_module' and node.args
                  and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)):
                enqueue(tuple(node.args[0].value.split('.')))
    return sorted(seen)


@pytest.mark.parametrize('family', ['ok_rsi', 'rsi_v5'])
@pytest.mark.parametrize('public_module', [None, 'rsi_modular'])
def test_legacy_and_public_profile_seals_exclude_exit_a(family, public_module):
    files = profile_source_files(family, public_module=public_module)
    relative = [path.relative_to(CONTROLLERS).as_posix() for path in files]
    assert relative
    assert not any('mean_reversion_v2' in path or 'exit_a' in path for path in relative)
    digest = source_digest(CONTROLLERS, files)
    records = dict(source_records(CONTROLLERS, files))
    assert len(digest) == 64
    assert not any(path.startswith('generic/mean_reversion_v2_core/') for path in records)


def test_exit_a_binding_seal_stays_outside_profile_closure():
    from test.mean_reversion_v2.test_exit_a_binding_and_admission import recipe

    from controllers.generic.mean_reversion_v2_core.exit_a.recipe import bind_exit_a_v2
    bound = bind_exit_a_v2(recipe())
    profile = {path.relative_to(CONTROLLERS).as_posix()
               for family in FAMILIES for path in profile_source_files(family, public_module='rsi_modular')}
    local = {path for path, _ in bound.source_records if not path.startswith('@')}
    assert local.isdisjoint(profile)


def test_compiled_loader_constructs_existing_profile_without_exit_a():
    pytest.importorskip(
        'hummingbot.core.network_iterator',
        reason='compiled network_iterator unavailable; profile constructor NOT RUN',
    )
    from hummingbot.strategy_v2.controllers.controller_loader import load_controller_config
    config = load_controller_config({
        'controller_name': 'rsi_modular', 'controller_type': 'generic', 'profile': 'ok_rsi',
        'id': 'modular_spot_exit_a_isolation', 'trading_pair': 'BTC-USDC', 'strict_hmm_mode': False,
    })
    provider = MagicMock()
    provider.ready = True
    provider.time.return_value = 600000.0
    controller = config.get_controller_class()(config, provider, asyncio.Queue())
    assert controller._native_family == 'ok_rsi'
    assert 'mean_reversion_v2_core' not in type(controller).__module__
