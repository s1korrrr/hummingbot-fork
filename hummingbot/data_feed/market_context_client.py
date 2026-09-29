"""Cached, read-only shared market-context client for Hummingbot runtimes.

``refresh`` is asynchronous and bounded; ``select`` is a synchronous cache read with no network I/O. The client
holds no connector credential, places nothing and grants no order, target, capital or inventory authority.
A new producer epoch is accepted only after the canonical store confirms it; sequences are compared only
inside one epoch. Consumer receipt time is recorded once per snapshot and never extends expiry.
"""
import asyncio
import base64
import logging
import os
import stat
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Protocol

import aiohttp

from hummingbot.strategy_v2.components.candle_transport import validate_origin
from hummingbot.strategy_v2.market_context.contracts import (
    MAX_SNAPSHOT_BYTES,
    ClockView,
    ContextError,
    ContextRequirement,
    ContextSelection,
    ContextSnapshot,
    ContextStreamStatus,
    NotificationEnvelope,
    parse_model,
    parse_snapshot,
    validate_context,
)

LOGGER = logging.getLogger(__name__)
MAX_PENDING_NOTIFICATIONS = 32
MAX_STATUS_BYTES = 65_536
REFRESH_TIMEOUT_SECONDS = 15.0
ROUTE_PREFIX = '/screener/market-context/v1'


class MarketContextTransport(Protocol):
    async def status(self) -> bytes:
        """Canonical-store stream status used for the epoch handshake."""

    async def latest(self) -> bytes:
        """Exact canonical bytes of the latest published snapshot."""

    async def snapshot(self, snapshot_id: str) -> bytes:
        """Exact canonical bytes of one published snapshot."""


class HttpMarketContextTransport:
    """Bounded HTTP reads of the API's read-only routes using existing scoped API read credentials."""

    def __init__(self, origin: str, credentials_file: str, *, timeout_seconds: float = 5.0):
        self.origin = validate_origin(origin)
        self.credentials_file = credentials_file
        self.timeout = aiohttp.ClientTimeout(total=float(timeout_seconds))
        self._session = None

    def authorization(self) -> str:
        path = Path(self.credentials_file)
        if not path.is_absolute() or any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError('context credentials must use an absolute non-symlink path')
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or not 3 <= info.st_size <= 4096:
                raise ValueError('context credentials must be a private regular file')
            value = os.read(descriptor, 4097).decode('utf-8').rstrip('\n')
        finally:
            os.close(descriptor)
        user, separator, password = value.partition(':')
        if not separator or not user or not password or any(ch.isspace() for ch in value):
            raise ValueError('context credentials must be user:password')
        return 'Basic ' + base64.b64encode(value.encode('utf-8')).decode('ascii')

    async def _get(self, path: str, maximum: int) -> bytes:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self.timeout, trust_env=False)
        request = {'allow_redirects': False, 'headers': {'Authorization': self.authorization()}}
        if self.origin.startswith('https://'):
            request['ssl'] = True
        async with self._session.get(self.origin + ROUTE_PREFIX + path, **request) as response:
            if response.status != 200:
                raise ContextError(('CONTEXT_UNAVAILABLE',), f'HTTP {response.status}')
            if response.content_length is not None and response.content_length > maximum:
                raise ContextError(('PAYLOAD_TOO_LARGE',))
            body = bytearray()
            async for chunk in response.content.iter_chunked(65_536):
                if len(body) + len(chunk) > maximum:
                    raise ContextError(('PAYLOAD_TOO_LARGE',))
                body.extend(chunk)
            return bytes(body)

    async def status(self) -> bytes:
        return await self._get('/status', MAX_STATUS_BYTES)

    async def latest(self) -> bytes:
        return await self._get('/latest', MAX_SNAPSHOT_BYTES)

    async def snapshot(self, snapshot_id: str) -> bytes:
        return await self._get(f'/snapshots/{snapshot_id}', MAX_SNAPSHOT_BYTES)

    async def close(self):
        session, self._session = self._session, None
        if session is not None:
            await session.close()


@dataclass(frozen=True)
class CachedContext:
    snapshot: ContextSnapshot
    receipt_ms: int
    generation: int


class MarketContextClient:
    def __init__(self, transport: MarketContextTransport, *, stream_id: str, clock_ms: Callable[[], int],
                 poll_seconds: float = 5.0, max_backoff_seconds: float = 30.0):
        if not 0 < poll_seconds <= max_backoff_seconds <= 300:
            raise ValueError('poll and backoff must be positive and bounded')
        self.transport = transport
        self.stream_id = stream_id
        self.clock_ms = clock_ms
        self.poll_seconds = float(poll_seconds)
        self.max_backoff_seconds = float(max_backoff_seconds)
        self.current: CachedContext | None = None
        self.last_reasons: tuple[str, ...] = ()
        self.counters = {'accepted': 0, 'rejected': 0, 'refresh_failures': 0, 'foreign_notifications': 0,
                         'notification_overflow': 0}
        self.task = None
        self._pending: deque = deque(maxlen=MAX_PENDING_NOTIFICATIONS)
        self._resync = False
        self._wake: asyncio.Event | None = None
        self.last_notification_ms: int | None = None

    def on_notification(self, payload: Mapping | bytes, *, received_at_ms: int) -> None:
        """Queue a broker pointer; it is never a payload, and gaps/overflow recover from the canonical store."""
        if isinstance(payload, (bytes, str)):
            envelope = parse_model(payload, NotificationEnvelope)
        else:
            envelope = NotificationEnvelope.model_validate(dict(payload))
        if envelope.stream_id != self.stream_id:
            self.counters['foreign_notifications'] += 1
            return
        self.last_notification_ms = int(received_at_ms)  # diagnostics only; snapshot receipt is its fetch time
        if len(self._pending) == self._pending.maxlen:
            self.counters['notification_overflow'] += 1
            self._resync = True
        self._pending.append(envelope)
        if self._wake is not None:
            self._wake.set()

    def select(self, requirement: ContextRequirement, clock: ClockView) -> ContextSelection:
        """Bounded synchronous read of the cached snapshot for one decision; performs no I/O."""
        current = self.current
        if current is None:
            return ContextSelection(accepted=False, snapshot=None, reasons=('CONTEXT_UNAVAILABLE',), snapshot_id=None,
                                    effective_available_at_ms=None, decision_ms=clock.decision_ms)
        receipt = max(current.receipt_ms, clock.receipt_ms if clock.receipt_ms is not None else 0)
        return validate_context(current.snapshot, requirement,
                                ClockView(decision_ms=clock.decision_ms, receipt_ms=receipt))

    def _confirm(self, snapshot: ContextSnapshot, status: ContextStreamStatus) -> None:
        current = self.current
        if (status.stream_id != self.stream_id or status.epoch != snapshot.epoch or status.sequence is None
                or status.sequence < snapshot.sequence
                or (current is not None and status.lease_generation <= current.generation)):
            raise ContextError(('EPOCH_UNCONFIRMED',), 'the canonical store does not confirm this producer epoch')

    def _install(self, snapshot: ContextSnapshot, receipt_ms: int, status: ContextStreamStatus | None) -> None:
        if snapshot.stream_id != self.stream_id:
            raise ContextError(('STREAM_MISMATCH',))
        current = self.current
        if current is not None and snapshot.epoch == current.snapshot.epoch:
            if snapshot.sequence == current.snapshot.sequence and snapshot.snapshot_id != current.snapshot.snapshot_id:
                raise ContextError(('SNAPSHOT_CONFLICT',), 'two payloads claim one epoch sequence')
            if snapshot.sequence <= current.snapshot.sequence:
                return  # duplicate or older delivery: keep the newest and its first receipt
            self.current = CachedContext(snapshot, receipt_ms, current.generation)
        else:
            if status is None:
                raise ContextError(('EPOCH_UNCONFIRMED',))
            self._confirm(snapshot, status)
            self.current = CachedContext(snapshot, receipt_ms, status.lease_generation)
        self.counters['accepted'] += 1

    def _needs_handshake(self, snapshot: ContextSnapshot) -> bool:
        return self.current is None or self.current.snapshot.epoch != snapshot.epoch

    async def _refresh_once(self) -> None:
        notes = list(self._pending)
        self._pending.clear()
        resync, self._resync = self._resync, False
        current, target = self.current, None
        if notes and not resync:
            newer = [n for n in notes if current is not None and n.epoch == current.snapshot.epoch
                     and n.sequence > current.snapshot.sequence]
            foreign_epoch = [n for n in notes if current is None or n.epoch != current.snapshot.epoch]
            if newer:
                target = max(newer, key=lambda n: n.sequence)
            elif foreign_epoch:
                target = foreign_epoch[-1]
            else:
                return
        if target is not None:
            snapshot = parse_snapshot(await self.transport.snapshot(target.snapshot_id))
            if (snapshot.snapshot_id, snapshot.stream_id, snapshot.epoch, snapshot.sequence) != (
                    target.snapshot_id, target.stream_id, target.epoch, target.sequence):
                raise ContextError(('SNAPSHOT_HASH_MISMATCH',), 'fetched snapshot differs from its notification')
        else:
            snapshot = parse_snapshot(await self.transport.latest())
        receipt = int(self.clock_ms())
        status = None
        if self._needs_handshake(snapshot):
            status = parse_model(await self.transport.status(), ContextStreamStatus, MAX_STATUS_BYTES)
        self._install(snapshot, receipt, status)

    async def refresh(self) -> None:
        """One bounded refresh. Validation failures keep the previous snapshot and record stable reasons."""
        try:
            await self._refresh_once()
            self.last_reasons = ()
        except ContextError as exc:
            self.counters['rejected'] += 1
            self.last_reasons = exc.reasons
            LOGGER.warning('market context rejected stream=%s reasons=%s', self.stream_id, ','.join(exc.reasons))

    def accept_recorded(self, raw: bytes, *, received_at_ms: int, status: bytes) -> None:
        """Replay a recorded delivery with its recorded receipt time and the status seen at that time."""
        snapshot = parse_snapshot(raw)
        confirmed = parse_model(status, ContextStreamStatus, MAX_STATUS_BYTES) if self._needs_handshake(snapshot) else None
        self._install(snapshot, int(received_at_ms), confirmed)

    def start(self) -> None:
        if self.task is None:
            self._wake = asyncio.Event()
            self.task = asyncio.create_task(self._run(), name='market-context-client')

    async def close(self) -> None:
        task, self.task = self.task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        close = getattr(self.transport, 'close', None)
        if close is not None:
            await close()

    async def _run(self) -> None:
        delay = self.poll_seconds
        while True:
            try:
                await asyncio.wait_for(self._refresh_once(), timeout=REFRESH_TIMEOUT_SECONDS)
                self.last_reasons = ()
                delay = self.poll_seconds
            except asyncio.CancelledError:
                raise
            except (ContextError, OSError, ValueError, asyncio.TimeoutError, aiohttp.ClientError) as exc:
                self.counters['refresh_failures'] += 1
                self.last_reasons = exc.reasons if isinstance(exc, ContextError) else ('CONTEXT_UNAVAILABLE',)
                LOGGER.warning('market context refresh failed stream=%s error=%s', self.stream_id, type(exc).__name__)
                delay = min(delay * 2, self.max_backoff_seconds)
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
