"""Single-host Telegram polling ownership; notification sending is independent."""
import asyncio
import fcntl
import hashlib
import os
from pathlib import Path


class TelegramPollingConflict(RuntimeError):
    """Telegram rejected polling because another consumer or webhook owns updates."""


class TelegramPollingLease:
    def __init__(self, token: str, directory: Path = None):
        if not token:
            raise ValueError('Telegram polling requires a nonempty token')
        self.directory = directory if directory is not None else Path.home() / '.cache/hummingbot/telegram-polling'
        self.path = self.directory / (hashlib.sha256(token.encode()).hexdigest() + '.lock')
        self._fd = None

    def acquire(self) -> bool:
        if self._fd is not None:
            return True
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return False
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd
        return True

    def close(self):
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        # Never unlink a lock file: contenders must continue using the same inode.


async def wait_for_poll(loop, callback):
    pending = loop.run_in_executor(None, callback)
    try:
        return await asyncio.shield(pending)
    except asyncio.CancelledError:
        # Cancelling an asyncio waiter does not stop urllib's worker thread.
        # Retain ownership until that bounded request has actually returned.
        while not pending.done():
            try:
                await asyncio.shield(pending)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not pending.cancelled():
            pending.exception()  # Retrieve any late exception without masking cancellation.
        raise
