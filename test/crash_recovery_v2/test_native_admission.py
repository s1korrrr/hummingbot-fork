import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import source_digest, verify_binding
from hummingbot.strategy_v2.components.source_identity import component_source_files
from hummingbot.strategy_v2.controllers.native_policy_identity import is_modular_policy, is_ok_rsi_policy

from .test_composition import recipe

FROZEN = ('ok_rsi', 'rsi_v5', 'rsi_modular', 'modular_ok_rsi', 'modular_rsi_v5')
ROOT = Path(__file__).resolve().parents[2]


def host_config(**changes):
    from controllers.generic.crash_recovery_v2 import CrashRecoveryV2ControllerConfig
    values = dict(id='crash_recovery_v2_lot1', connector_name='okx', trading_pair='BNB-USDC', recipe=recipe())
    values.update(changes)
    return CrashRecoveryV2ControllerConfig(**values)


def host_controller():
    from controllers.generic.crash_recovery_v2 import CrashRecoveryV2Controller
    controller = CrashRecoveryV2Controller.__new__(CrashRecoveryV2Controller)
    controller.config = host_config()
    return controller


def host_binding():
    from controllers.generic.crash_recovery_v2 import bind_host_recipe, source_files, source_root
    return bind_host_recipe(recipe()), source_digest(source_root(), source_files())


def host_evaluate(state=None, prices=(), **changes):
    from .test_composition import evaluate
    binding, source = host_binding()
    return evaluate(state, prices, binding=binding, expected_source_sha256=source, **changes)


def test_loading_host_does_not_import_frozen_controller_families():
    script = (
        'import sys\n'
        'import controllers.generic.crash_recovery_v2\n'
        f'frozen = {FROZEN!r}\n'
        'hits = []\n'
        'for name in frozen:\n'
        '    hits.extend(n for n in sys.modules\n'
        '                if n == "controllers.generic." + name\n'
        '                or n == "controllers.directional_trading." + name\n'
        '                or n.startswith("controllers.generic." + name + ".")\n'
        '                or n.startswith("controllers.directional_trading." + name + "."))\n'
        'assert not hits, hits\n'
    )
    result = subprocess.run([sys.executable, '-B', '-c', script], capture_output=True, text=True,
                            env={**os.environ, 'PYTHONPATH': str(ROOT)}, timeout=300)
    assert result.returncode == 0, result.stderr


def test_config_class_resolves_and_policy_identity_rejects_the_host():
    module = importlib.import_module('controllers.generic.crash_recovery_v2')
    assert module.CONFIG_CLASS == 'CrashRecoveryV2ControllerConfig'
    cls = getattr(module, module.CONFIG_CLASS)
    cfg = host_config()
    assert type(cfg) is cls
    assert cfg.controller_name == 'crash_recovery_v2'
    assert is_ok_rsi_policy(cfg) is False
    assert is_modular_policy(cfg) is False


def test_execution_enabled_cannot_be_forged_true():
    with pytest.raises(ValidationError):
        host_config(execution_enabled=True)


def test_host_emits_no_executor_actions_and_rejects_every_executor_config():
    controller = host_controller()
    assert controller.determine_executor_actions() == []
    assert controller.validate_executor_config(object()) is False
    assert controller.validate_executor_config(None) is False


def test_host_bind_hashes_actual_source_files_and_rejects_a_wrong_digest():
    from controllers.generic.crash_recovery_v2 import bind_host_recipe, source_files, source_root
    owned = source_files()
    names = {path.name for path in owned}
    assert 'crash_recovery_v2.py' in names
    assert any(path.parent.name == 'crash_recovery_v2_core' for path in owned)
    source = source_digest(source_root(), owned)
    binding = bind_host_recipe(recipe())
    assert binding.source_sha256 == source
    payload = recipe().model_dump(mode='json')
    verify_binding(binding, payload, source_sha256=source)
    with pytest.raises(PolicyError, match='source changed'):
        verify_binding(binding, payload, source_sha256='0' * 64)


def test_implementation_source_files_include_host_core_and_components():
    from controllers.generic.crash_recovery_v2 import CrashRecoveryV2Controller, source_files
    files = CrashRecoveryV2Controller.implementation_source_files()
    assert not CrashRecoveryV2Controller.allow_implementation_hot_reload
    for path in source_files():
        assert path in files
    for path in component_source_files():
        assert path in files
