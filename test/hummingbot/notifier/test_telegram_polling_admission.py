"""Operator admission and same-kernel leases use only disposable local fixtures."""
import asyncio
import json
import os
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from hummingbot.notifier import telegram_notifier as notifier_module, telegram_polling as polling

ENV = "HB_TELEGRAM_POLLING_ADMISSION_FILE"


def write_gate(path, **changes):
    value = {"schema_version": 1, "enabled": True, "legacy_host_pollers_absent": True}
    value.update(changes)
    path.write_text(json.dumps(value))
    path.chmod(0o600)


def notifier(loop):
    config = SimpleNamespace(telegram_bot_token=SimpleNamespace(get_secret_value=lambda: "synthetic-test-token"),
                             telegram_chat_id="fixture-chat", telegram_set_my_commands=False)
    app = SimpleNamespace(ev_loop=loop, client_config_map=SimpleNamespace(telegram_notifications=config))
    return notifier_module.TelegramNotifier(app)


def test_unconfigured_admission_preserves_native_behavior(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    assert polling.polling_admitted()


@pytest.mark.parametrize("changes", [{"enabled": False}, {"legacy_host_pollers_absent": False},
                                     {"schema_version": True}, {"schema_version": 2}, {"enabled": 1},
                                     {"extra": "not-the-schema"}])
def test_admission_requires_explicit_exact_claims(tmp_path, monkeypatch, changes):
    path = tmp_path / "admission.json"
    monkeypatch.setenv(ENV, str(path))
    write_gate(path, **changes)
    assert not polling.polling_admitted()


@pytest.mark.parametrize("fault", ["missing", "empty-env", "relative", "malformed", "duplicate",
                                   "oversized", "permissions", "directory", "fifo", "leaf-link", "parent-link"])
def test_bad_admission_file_fails_closed(tmp_path, monkeypatch, fault):
    path = tmp_path / "admission.json"
    monkeypatch.setenv(ENV, str(path))
    if fault == "missing":
        pass
    elif fault == "empty-env":
        monkeypatch.setenv(ENV, "")
    elif fault == "relative":
        monkeypatch.setenv(ENV, "admission.json")
    elif fault == "directory":
        path.mkdir(mode=0o600)
    elif fault == "fifo":
        os.mkfifo(path, 0o600)
    elif fault in {"leaf-link", "parent-link"}:
        actual = tmp_path / "real"
        actual.mkdir()
        write_gate(actual / "admission.json")
        if fault == "leaf-link":
            path.symlink_to(actual / "admission.json")
        else:
            linked = tmp_path / "linked"
            linked.symlink_to(actual, target_is_directory=True)
            monkeypatch.setenv(ENV, str(linked / "admission.json"))
    else:
        write_gate(path)
        if fault == "permissions":
            path.chmod(0o644)
        elif fault == "malformed":
            path.write_text("{")
        elif fault == "duplicate":
            path.write_text('{"schema_version":1,"enabled":false,"enabled":true,"legacy_host_pollers_absent":true}')
        elif fault == "oversized":
            path.write_text(" " * 4097)
    assert not polling.polling_admitted()


def test_private_owned_gate_can_be_revoked_without_cached_admission(tmp_path, monkeypatch):
    path = tmp_path / "admission.json"
    monkeypatch.setenv(ENV, str(path))
    write_gate(path)
    assert polling.polling_admitted()
    write_gate(path, enabled=False)
    assert not polling.polling_admitted()
    write_gate(path)
    monkeypatch.setattr(polling.os, "geteuid", lambda: path.stat().st_uid + 1)
    assert not polling.polling_admitted()


@pytest.mark.parametrize("method", ["getUpdates", "setMyCommands"])
def test_denied_admission_blocks_request_boundary(tmp_path, monkeypatch, method):
    monkeypatch.setenv(ENV, str(tmp_path / "missing.json"))
    client = notifier(None)
    network = Mock()
    monkeypatch.setattr(notifier_module.request, "urlopen", network)
    with pytest.raises(polling.TelegramPollingAdmissionDenied):
        client._telegram_request(method, {})
    network.assert_not_called()


def test_denied_commands_do_not_disable_notification_sending(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV, str(tmp_path / "missing.json"))
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.read.return_value = b'{"ok":true,"result":{}}'
    network = Mock(return_value=response)
    monkeypatch.setattr(notifier_module.request, "urlopen", network)
    assert notifier(None)._telegram_request("sendMessage", {"text": "fixture-only"}) == {}
    network.assert_called_once()


def test_missing_admission_waits_then_allows_and_releases_on_revocation(tmp_path, monkeypatch):
    async def scenario():
        path = tmp_path / "admission.json"
        monkeypatch.setenv(ENV, str(path))
        lease = polling.TelegramPollingLease("fixture", tmp_path / "lease")
        monkeypatch.setattr(notifier_module, "TelegramPollingLease", lambda token: lease)
        client = notifier(asyncio.get_running_loop())
        calls = []

        async def cycle():
            calls.append("cycle")
            if len(calls) > 1:
                raise asyncio.CancelledError()
            write_gate(path, enabled=False)

        async def sleep(_delay):
            assert lease._fd is None
            if not calls:
                write_gate(path)
            else:
                raise asyncio.CancelledError()

        client._run_command_cycle = cycle
        client._sleep = sleep
        with pytest.raises(asyncio.CancelledError):
            await client._run_command_loop()
        assert calls == ["cycle"]
        assert lease._fd is None

    asyncio.run(scenario())


def test_revocation_is_rechecked_while_waiting_for_lease(tmp_path, monkeypatch):
    async def scenario():
        path = tmp_path / "admission.json"
        monkeypatch.setenv(ENV, str(path))
        write_gate(path)
        owner = polling.TelegramPollingLease("fixture", tmp_path / "lease")
        standby = polling.TelegramPollingLease("fixture", tmp_path / "lease")
        assert owner.acquire()
        monkeypatch.setattr(notifier_module, "TelegramPollingLease", lambda token: standby)
        client = notifier(asyncio.get_running_loop())
        client._run_command_cycle = AsyncMock(side_effect=AssertionError("must not poll"))
        waits = 0

        async def sleep(_delay):
            nonlocal waits
            waits += 1
            if waits == 1:
                write_gate(path, enabled=False)
                owner.close()
            else:
                assert standby._fd is None
                raise asyncio.CancelledError()

        client._sleep = sleep
        try:
            with pytest.raises(asyncio.CancelledError):
                await client._run_command_loop()
        finally:
            owner.close()
        client._run_command_cycle.assert_not_called()

    asyncio.run(scenario())


def test_revoked_long_poll_drains_before_lease_release_and_drops_updates(tmp_path, monkeypatch):
    async def scenario():
        path = tmp_path / "admission.json"
        monkeypatch.setenv(ENV, str(path))
        write_gate(path)
        owner = polling.TelegramPollingLease("fixture", tmp_path / "lease")
        contender = polling.TelegramPollingLease("fixture", tmp_path / "lease")
        monkeypatch.setattr(notifier_module, "TelegramPollingLease", lambda token: owner)
        client = notifier(asyncio.get_running_loop())
        client._last_update_id = 0
        client._handle_update = AsyncMock()
        started, released, denied = threading.Event(), threading.Event(), asyncio.Event()

        def fetch():
            started.set()
            assert released.wait(2)
            return [{"update_id": 1}]

        async def sleep(_delay):
            denied.set()
            await asyncio.Future()

        client._fetch_updates = fetch
        client._sleep = sleep
        task = asyncio.create_task(client._run_command_loop())
        try:
            assert await asyncio.to_thread(started.wait, 2)
            write_gate(path, enabled=False)
            assert not contender.acquire()
            released.set()
            await asyncio.wait_for(denied.wait(), 2)
            assert contender.acquire()
            client._handle_update.assert_not_called()
        finally:
            released.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            contender.close()

    asyncio.run(scenario())


def test_admission_is_rechecked_between_registration_and_next_poll(tmp_path, monkeypatch):
    async def scenario():
        path = tmp_path / "admission.json"
        monkeypatch.setenv(ENV, str(path))
        write_gate(path)
        client = notifier(asyncio.get_running_loop())
        client._last_update_id = 0
        client._config.telegram_set_my_commands = True

        async def register():
            write_gate(path, enabled=False)
            return True

        client._publish_command_menu = register
        client._fetch_updates = Mock(return_value=[])
        with pytest.raises(polling.TelegramPollingAdmissionDenied):
            await client._run_command_cycle()
        client._fetch_updates.assert_not_called()

    asyncio.run(scenario())


@pytest.mark.parametrize("registration", [False, True])
def test_cancel_retains_lease_until_pending_request_returns(tmp_path, monkeypatch, registration):
    async def scenario():
        path = tmp_path / "admission.json"
        monkeypatch.setenv(ENV, str(path))
        write_gate(path)
        owner = polling.TelegramPollingLease("fixture", tmp_path / "lease")
        contender = polling.TelegramPollingLease("fixture", tmp_path / "lease")
        monkeypatch.setattr(notifier_module, "TelegramPollingLease", lambda token: owner)
        client = notifier(asyncio.get_running_loop())
        client._last_update_id = 0
        client._config.telegram_set_my_commands = registration
        started, released = threading.Event(), threading.Event()
        calls = []

        def pending_request(*args):
            calls.append(args)
            started.set()
            assert released.wait(2)
            return []

        if registration:
            client._telegram_request = pending_request
        else:
            client._fetch_updates = pending_request
        task = asyncio.create_task(client._run_command_loop())
        try:
            assert await asyncio.to_thread(started.wait, 2)
            write_gate(path, enabled=False)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            assert not contender.acquire()
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            assert not contender.acquire()
            released.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert contender.acquire()
            assert len(calls) == 1
        finally:
            released.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            contender.close()

    asyncio.run(scenario())


def test_same_kernel_processes_share_polling_lease(tmp_path):
    lease = polling.TelegramPollingLease("fixture", tmp_path)
    code = ("from pathlib import Path; from hummingbot.notifier.telegram_polling import TelegramPollingLease; "
            "import sys; lease=TelegramPollingLease('fixture',Path(sys.argv[1])); print(lease.acquire()); lease.close()")
    assert lease.acquire()
    try:
        assert subprocess.check_output([sys.executable, "-c", code, str(tmp_path)], text=True).strip() == "False"
    finally:
        lease.close()
    assert subprocess.check_output([sys.executable, "-c", code, str(tmp_path)], text=True).strip() == "True"
