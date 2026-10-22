"""Bind native restart YAML reads to the exact prevalidated configuration bytes."""
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path

import yaml

native_sources = ContextVar('native_config_sources', default=None)


@dataclass
class NativeSourceGuard:
    sources: dict
    active: bool = True


class NativeSourceMismatch(ValueError):
    pass


def read_config_text(path):
    guard = native_sources.get()
    if guard is None or not guard.active:
        with open(path, 'r') as file:
            return file.read()
    expected = guard.sources
    path = Path(path).resolve()
    try:
        content = path.read_bytes()
    except OSError as error:
        if expected is not None:
            raise NativeSourceMismatch('Native configuration source became unreadable') from error
        raise
    if expected is not None and expected.get(path) != content:
        raise NativeSourceMismatch('Native configuration changed or source was not registered')
    return content.decode('utf-8')


def capture_sources(config_file, script_file, controller_ids):
    from hummingbot.client import settings

    if Path(config_file).name != config_file:
        raise NativeSourceMismatch('Native config must be a simple filename')
    script_path = (settings.SCRIPT_STRATEGY_CONF_DIR_PATH / config_file).resolve()
    script_bytes = script_path.read_bytes()
    data = yaml.safe_load(script_bytes)
    if not isinstance(data, dict) or data.get('script_file_name') != script_file:
        raise NativeSourceMismatch('Configured script does not match native identity')
    paths = data.get('controllers_config')
    if not isinstance(paths, list) or not paths:
        raise NativeSourceMismatch('Native controller source list is unavailable')
    sources = {script_path: script_bytes}
    ids = []
    for filename in paths:
        if not isinstance(filename, str) or Path(filename).name != filename:
            raise NativeSourceMismatch('Native controller config must be a simple filename')
        path = (Path(settings.CONTROLLERS_CONF_DIR_PATH) / filename).resolve()
        content = path.read_bytes()
        controller = yaml.safe_load(content)
        if not isinstance(controller, dict) or not isinstance(controller.get('id'), str):
            raise NativeSourceMismatch('Explicit controller identity is unavailable')
        ids.append(controller['id'])
        sources[path] = content
    if len(ids) != len(set(ids)) or (controller_ids is not None and sorted(ids) != controller_ids):
        raise NativeSourceMismatch('Configured controller membership changed')
    return sources
