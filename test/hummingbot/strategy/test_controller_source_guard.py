import importlib
import os
import sys

import pytest

from hummingbot.strategy.strategy_v2_base import StrategyV2ConfigBase


@pytest.fixture
def protected_module(tmp_path, monkeypatch):
    name = 'protected_controller_fixture'
    helper = tmp_path / 'helper.py'
    helper.write_text('HELPER = 1\n')
    source = tmp_path / (name + '.py')
    source.write_text('''from pathlib import Path
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
VERSION = 1
class ProtectedController(ControllerBase):
    allow_implementation_hot_reload = False
    @classmethod
    def implementation_source_files(cls):
        return [__file__, str(Path(__file__).with_name('helper.py'))]
    def marker(self):
        return VERSION
''')
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    config = StrategyV2ConfigBase()
    module = config._load_controller_module(name)
    yield config, module, source, helper
    sys.modules.pop(name, None)


def test_protected_module_change_is_rejected_before_mutating_live_globals(protected_module):
    config, module, source, _ = protected_module
    old_class = module.ProtectedController
    source.write_text(source.read_text().replace('VERSION = 1', 'VERSION = 2'))
    os.utime(source, (source.stat().st_atime, source.stat().st_mtime + 5))
    with pytest.raises(RuntimeError, match='restart'):
        config._load_controller_module(module.__name__, reload_changed_modules=True)
    assert module.VERSION == 1
    assert module.ProtectedController is old_class
    assert old_class.marker(None) == 1


def test_touch_without_content_change_keeps_class_identity(protected_module):
    config, module, source, _ = protected_module
    old_class = module.ProtectedController
    os.utime(source, (source.stat().st_atime, source.stat().st_mtime + 5))
    config._load_controller_module(module.__name__, reload_changed_modules=True)
    assert module.ProtectedController is old_class


def test_helper_change_requires_restart_even_when_main_file_is_unchanged(protected_module):
    config, module, _, helper = protected_module
    helper.write_text('HELPER = 2\n')
    with pytest.raises(RuntimeError, match='restart'):
        config._load_controller_module(module.__name__, reload_changed_modules=True)
    assert module.VERSION == 1


def test_deleted_source_cannot_disable_guard(protected_module):
    config, module, source, _ = protected_module
    source.unlink()
    with pytest.raises(RuntimeError, match='restart'):
        config._load_controller_module(module.__name__, reload_changed_modules=True)


def test_config_refresh_preflights_protected_sources(protected_module):
    config, module, _, helper = protected_module
    helper.write_text('HELPER = 2\n')
    with pytest.raises(RuntimeError, match='restart'):
        config.load_controller_configs(reload_changed_modules=True)
    assert module.VERSION == 1


def test_new_config_in_same_process_cannot_rebaseline_old_loaded_code(protected_module):
    _, module, source, _ = protected_module
    source.write_text(source.read_text().replace('VERSION = 1', 'VERSION = 2'))
    new_config = StrategyV2ConfigBase()
    with pytest.raises(RuntimeError, match='restart'):
        new_config._load_controller_module(module.__name__)
    assert module.VERSION == 1
