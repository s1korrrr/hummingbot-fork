import pytest

from hummingbot.client.native_sources import (
    NativeSourceGuard,
    NativeSourceMismatch,
    capture_sources,
    native_sources,
    read_config_text,
)


def test_membership_and_exact_bytes_are_checked_before_owner_loading(tmp_path, monkeypatch):
    from hummingbot.client import settings
    monkeypatch.setattr(settings, 'SCRIPT_STRATEGY_CONF_DIR_PATH', tmp_path)
    monkeypatch.setattr(settings, 'CONTROLLERS_CONF_DIR_PATH', tmp_path)
    (tmp_path / 'bot.yml').write_text('script_file_name: v2_with_controllers.py\ncontrollers_config: [controller.yml]\n')
    controller = tmp_path / 'controller.yml'
    controller.write_text('id: expected\n')
    with pytest.raises(NativeSourceMismatch):
        capture_sources('bot.yml', 'v2_with_controllers.py', ['wrong'])
    snapshot = capture_sources('bot.yml', 'v2_with_controllers.py', ['expected'])
    token = native_sources.set(NativeSourceGuard(snapshot))
    try:
        assert read_config_text(controller) == 'id: expected\n'
        controller.write_text('id: changed\n')
        with pytest.raises(NativeSourceMismatch):
            read_config_text(controller)
    finally:
        native_sources.reset(token)
    assert read_config_text(controller) == 'id: changed\n'


def test_deleted_native_source_raises_specific_guard_error(tmp_path):
    path = tmp_path / 'config.yml'
    path.write_text('id: expected\n')
    token = native_sources.set(NativeSourceGuard({path.resolve(): path.read_bytes()}))
    path.unlink()
    try:
        with pytest.raises(NativeSourceMismatch):
            read_config_text(path)
    finally:
        native_sources.reset(token)


@pytest.mark.asyncio
async def test_inherited_guard_expires_after_startup_scope(tmp_path):
    import asyncio
    path = tmp_path / 'config.yml'
    path.write_text('id: initial\n')
    guard = NativeSourceGuard({path.resolve(): path.read_bytes()})
    release = asyncio.Event()

    async def child():
        await release.wait()
        return read_config_text(path)

    token = native_sources.set(guard)
    try:
        task = asyncio.create_task(child())
        path.write_text('id: edited\n')
        with pytest.raises(NativeSourceMismatch):
            read_config_text(path)
    finally:
        guard.active = False
        native_sources.reset(token)
    release.set()
    assert await task == 'id: edited\n'
