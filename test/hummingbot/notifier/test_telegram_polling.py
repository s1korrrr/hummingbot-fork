import asyncio
import threading

import pytest

from hummingbot.notifier.telegram_polling import TelegramPollingLease, wait_for_poll


def test_one_owner_per_token_and_release(tmp_path):
    first = TelegramPollingLease('shared-token', tmp_path)
    second = TelegramPollingLease('shared-token', tmp_path)
    different = TelegramPollingLease('different-token', tmp_path)
    try:
        assert first.acquire()
        assert not second.acquire()
        assert different.acquire()
        first.close()
        assert second.acquire()
        assert all('token' not in p.name for p in tmp_path.iterdir())
    finally:
        first.close()
        second.close()
        different.close()


@pytest.mark.asyncio
async def test_cancellation_waits_for_inflight_poll_before_releasing_owner(tmp_path):
    entered = threading.Event()
    finish = threading.Event()
    lease = TelegramPollingLease('shared-token', tmp_path)
    contender = TelegramPollingLease('shared-token', tmp_path)

    def poll():
        entered.set()
        assert finish.wait(5)
        return []

    async def owner():
        assert lease.acquire()
        try:
            await wait_for_poll(asyncio.get_running_loop(), poll)
        finally:
            lease.close()

    task = asyncio.create_task(owner())
    try:
        while not entered.is_set():
            await asyncio.sleep(0.001)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert not contender.acquire()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert contender.acquire()
    finally:
        finish.set()
        await asyncio.gather(task, return_exceptions=True)
        lease.close()
        contender.close()


def test_lease_excludes_other_process_and_recovers_after_exit(tmp_path):
    import subprocess
    import sys
    code = (
        'from pathlib import Path; import sys; '
        'from hummingbot.notifier.telegram_polling import TelegramPollingLease; '
        'lease=TelegramPollingLease("shared-token",Path(sys.argv[1])); '
        'print(lease.acquire())'
    )
    lease = TelegramPollingLease('shared-token', tmp_path)
    try:
        assert lease.acquire()
        result = subprocess.run([sys.executable, '-c', code, str(tmp_path)], capture_output=True, text=True, check=True)
        assert result.stdout.strip() == 'False'
        lease.close()
        result = subprocess.run([sys.executable, '-c', code, str(tmp_path)], capture_output=True, text=True, check=True)
        assert result.stdout.strip() == 'True'
        assert lease.acquire()  # Child exited without explicitly closing its lease.
    finally:
        lease.close()
