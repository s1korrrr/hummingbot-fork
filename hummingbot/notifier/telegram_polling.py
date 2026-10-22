"""Single-host Telegram polling ownership; notification sending is independent."""
import asyncio
import fcntl
import hashlib
import json
import os
import stat
from contextlib import ExitStack
from pathlib import Path


class TelegramPollingConflict(RuntimeError):
    """Telegram rejected polling because another consumer or webhook owns updates."""


class TelegramPollingAdmissionDenied(RuntimeError):
    """Explicit operator admission is absent, invalid, or revoked."""


def polling_admitted() -> bool:
    """Read the optional operator gate afresh; never infer or write admission."""
    value = os.environ.get('HB_TELEGRAM_POLLING_ADMISSION_FILE')
    if value is None:
        return True
    path = Path(value)
    if not value or not path.is_absolute() or '..' in path.parts or len(path.parts) < 2:
        return False

    def unique_object(pairs):
        if len({key for key, _ in pairs}) != len(pairs):
            raise ValueError('Duplicate admission fields')
        return dict(pairs)

    try:
        with ExitStack() as stack:
            # Walk directory descriptors without following any symlink, including
            # ancestors. The opened leaf remains authoritative across replacement.
            directory = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
            stack.callback(os.close, directory)
            for component in path.parts[1:-1]:
                directory = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                stack.callback(os.close, directory)
            descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            with os.fdopen(descriptor, 'rb') as stream:
                info = os.fstat(stream.fileno())
                if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                        or info.st_uid != os.geteuid() or info.st_size > 4096):
                    return False
                payload = stream.read(4097)
                if len(payload) > 4096:
                    return False
        admission = json.loads(payload, object_pairs_hook=unique_object)
        return (isinstance(admission, dict)
                and set(admission) == {'schema_version', 'enabled', 'legacy_host_pollers_absent'}
                and type(admission['schema_version']) is int and admission['schema_version'] == 1
                and admission['enabled'] is True and admission['legacy_host_pollers_absent'] is True)
    except (OSError, ValueError, UnicodeError, RecursionError):
        return False


def require_polling_admission() -> None:
    if not polling_admitted():
        raise TelegramPollingAdmissionDenied('Telegram command polling admission is unavailable')


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
