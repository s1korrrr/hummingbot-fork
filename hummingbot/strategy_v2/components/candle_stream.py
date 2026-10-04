"""Opt-in confirmed-candle stream adapter; execution remains native Hummingbot."""
import asyncio
import json
import math
import re
import time
from collections import deque
from urllib.parse import urlsplit

import aiohttp

from .candle_data import MAX_CANDLE_HISTORY, Candle, CandleSnapshot, Replica
from .candle_transport import MAX_MESSAGE, read_token, validate_origin
from .domain import DataUnavailable, PolicyError

# After a missed heartbeat deadline, frames already delivered during an event-loop stall are read
# for this long (and at most this many) before the session is declared dead.
HEARTBEAT_GRACE_SECONDS = 1.0
HEARTBEAT_GRACE_FRAMES = 256


class CandleStreamClient:
    """Bounded incremental wire-v1 reader with fail-closed resynchronization."""

    def __init__(self, origin, token_file, datasets, *, clock=time.time):
        self.origin = validate_origin(origin)
        self.token_file = token_file
        self.clock = clock
        self.replica = Replica(datasets, max_transport_age=10)
        self.by_pair = {spec.trading_pair: spec for spec in datasets}
        if (len(self.by_pair) != len(datasets) or len(datasets) > 1000
                or len(datasets) * max(spec.history for spec in datasets) > 3_000_000
                or any(spec.history > MAX_CANDLE_HISTORY or spec.connector != 'okx' or spec.interval != '1m' or spec.source_profile != 'native'
                       or not spec.trading_pair.endswith('-USDC') for spec in datasets)):
            raise ValueError('stream requires unique bounded OKX spot 1m datasets')
        self.history = max(spec.history for spec in datasets)
        self.task = None
        self.error = None
        self.dataset_errors = {}
        self.sequences = {}
        self.update_receipts = deque(maxlen=4096)
        self.receipt_overflows = 0
        self.sessions = 0
        self.failures = 0
        self.last_heartbeat = None
        # Observation-only receipt times on ``clock``; never consulted by readiness or sequencing.
        self.last_heartbeat_at = None
        self.dataset_updated_at = {}

    def start(self):
        if self.task is None:
            self.task = asyncio.create_task(self.run(), name='modular-candle-stream')

    async def close(self):
        self.replica.disconnect()
        task, self.task = self.task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def begin_session(self):
        self.replica.begin_session()
        self.sequences.clear()
        self.dataset_errors.clear()
        self.last_heartbeat = time.monotonic()
        self.sessions += 1

    @staticmethod
    def _number(value):
        if type(value) not in (int, float) or value > 253402300799 or not math.isfinite(value) or value < 0:
            raise PolicyError('invalid stream timestamp')
        return value

    @staticmethod
    def _sequence(value):
        if type(value) is not int or value < 0:
            raise PolicyError('invalid stream sequence')
        return value

    @staticmethod
    def _row(value):
        fields = {'open_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'confirmed'}
        if (not isinstance(value, dict) or set(value) != fields
                or any(not isinstance(value[name], str) or len(value[name]) > 128
                       for name in ('open', 'high', 'low', 'close', 'volume', 'quote_volume'))):
            raise PolicyError('invalid stream candle shape or decimal encoding')
        return Candle.model_validate(value)

    def apply(self, message):
        """Apply exactly one decoded message; any protocol violation invalidates the session."""
        try:
            self._apply(message)
        except (ValueError, TypeError, KeyError, AttributeError):
            self.replica.disconnect()
            raise

    def _apply(self, message):
        now = self.clock()
        if not self.replica.connected or not isinstance(message, dict):
            raise PolicyError('disconnected or malformed candle stream')
        base = {'v', 'type', 'epoch', 'published_at'}
        kind = message.get('type')
        shapes = {'snapshot': {'pair', 'sequence', 'received_at', 'rows'},
                  'candle': {'pair', 'sequence', 'received_at', 'row'},
                  'status': {'pair', 'sequence', 'ready', 'reason'}, 'heartbeat': {'states'}}
        if (type(message.get('v')) is not int or message['v'] != 1 or kind not in shapes
                or set(message) != base | shapes[kind]):
            raise PolicyError('unsupported candle stream schema')
        epoch = message['epoch']
        published = self._number(message['published_at'])
        if (not isinstance(epoch, str) or re.fullmatch(r'[A-Za-z0-9_-]{1,128}', epoch) is None
                or not 0 <= now - published <= self.replica.max_transport_age
                or self.replica.epoch is not None and self.replica.epoch != epoch):
            raise PolicyError('stale publication or changed producer epoch')
        self.replica.epoch = epoch
        if kind == 'heartbeat':
            self._heartbeat(message, published)
            self.last_heartbeat_at = now
            return
        pair = message['pair']
        if not isinstance(pair, str) or pair not in self.by_pair:
            raise PolicyError('unsubscribed stream pair')
        spec = self.by_pair[pair]
        key = spec.key
        sequence = self._sequence(message['sequence'])
        previous_sequence = self.sequences.get(pair)
        if previous_sequence is not None and sequence < previous_sequence:
            raise PolicyError('stream sequence regressed')
        if kind == 'status':
            if (message['ready'] is not False or not isinstance(message['reason'], str)
                    or not 1 <= len(message['reason']) <= 256):
                raise PolicyError('invalid stream health status')
            self.sequences[pair] = sequence
            self.replica.snapshots.pop(key, None)
            self.replica.seed_bars.pop(key, None)
            self.replica.revision += 1
            self.dataset_errors[key] = message['reason']
            return
        received = self._number(message['received_at'])
        if received > published or sequence < 1:
            raise PolicyError('invalid upstream receipt or ready sequence')
        old = self.replica.snapshots.get(key)
        if kind == 'snapshot':
            if old is not None:
                raise PolicyError('unsolicited snapshot requires unavailable status or new session')
            rows = message['rows']
            if not isinstance(rows, list) or not spec.history <= len(rows) <= self.history:
                raise PolicyError('snapshot does not satisfy requested history')
            rows = tuple(self._row(row) for row in rows)
        else:
            if old is None or sequence != previous_sequence + 1:
                raise PolicyError('incremental candle sequence gap requires resynchronization')
            row = self._row(message['row'])
            latest = old.rows[-1].open_time
            if row.open_time == latest:
                rows = (*old.rows[:-1], row)
            elif row.open_time == latest + spec.seconds:
                rows = (*old.rows, row)[-spec.history:]
            else:
                raise PolicyError('incremental candle time gap or regression')
        snapshot = CandleSnapshot(dataset=spec, epoch=epoch, sequence=sequence,
                                  published_at=published, rows=rows[-spec.history:])
        self.replica.accept(snapshot, now=now)
        self.sequences[pair] = sequence
        self.dataset_errors.pop(key, None)
        self.dataset_updated_at[key] = now
        if len(self.update_receipts) == self.update_receipts.maxlen:
            self.receipt_overflows += 1
        self.update_receipts.append({'pair': pair, 'open_time': rows[-1].open_time,
                                     'received_at': received, 'adapter_at': self.clock(),
                                     'sequence': sequence, 'type': kind, 'epoch': epoch})

    def _heartbeat(self, message, published):
        states = message['states']
        if not isinstance(states, dict) or set(states) != set(self.by_pair):
            raise PolicyError('heartbeat subscription set mismatch')
        for pair, state in states.items():
            if (not isinstance(state, dict) or set(state) != {'ready', 'sequence'}
                    or type(state['ready']) is not bool
                    or self._sequence(state['sequence']) != self.sequences.get(pair)):
                raise PolicyError('heartbeat sequence or readiness mismatch')
            spec = self.by_pair[pair]
            current = self.replica.snapshots.get(spec.key)
            if state['ready'] != (current is not None):
                raise PolicyError('heartbeat readiness requires matching snapshot or status')
            if current is not None:
                if current.published_at > published:
                    raise PolicyError('heartbeat publication regressed')
                self.replica.snapshots[spec.key] = current.model_copy(update={'published_at': published})
        self.replica.revision += 1
        self.last_heartbeat = time.monotonic()
        self.error = None

    async def consume(self, ws):
        self.begin_session()
        await ws.send_json({'v': 1, 'op': 'subscribe', 'pairs': sorted(self.by_pair), 'history': self.history})
        while True:
            remaining = 10 - (time.monotonic() - self.last_heartbeat)
            try:
                if remaining <= 0:
                    raise asyncio.TimeoutError
                await self._receive(ws, remaining)
            except asyncio.TimeoutError:
                # Both the deadline found expired and a receive that timed out while this loop was
                # stalled may hide a heartbeat the server already delivered.
                if not await self._heartbeat_already_delivered(ws):
                    raise DataUnavailable('candle stream heartbeat deadline expired') from None

    async def _receive(self, ws, timeout):
        message = await asyncio.wait_for(ws.receive(), timeout=timeout)
        if message.type != aiohttp.WSMsgType.TEXT:
            raise DataUnavailable('candle stream closed or non-text frame')
        self.apply(json.loads(message.data))

    async def _heartbeat_already_delivered(self, ws):
        """A missed deadline may be this process's own stalled event loop, not a silent server.

        Frames the server sent while the loop was stalled are read for a short grace; only a
        heartbeat among them keeps the session (busy data frames never extend the deadline).
        Freshness for decisions is still bounded by the replica's transport age.
        """
        missed = self.last_heartbeat
        deadline = asyncio.get_running_loop().time() + HEARTBEAT_GRACE_SECONDS
        for _ in range(HEARTBEAT_GRACE_FRAMES):
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                await self._receive(ws, remaining)
            except asyncio.TimeoutError:
                break
            if self.last_heartbeat != missed:
                return True
        return False

    async def run(self):
        delay = .5

        async def reject_redirect(session, context, params):
            raise PolicyError('market-data redirects are forbidden')

        trace = aiohttp.TraceConfig()
        trace.on_request_redirect.append(reject_redirect)
        while True:
            started = time.monotonic()
            try:
                token = read_token(self.token_file)
                async with aiohttp.ClientSession(trust_env=False, trace_configs=[trace],
                                                 timeout=aiohttp.ClientTimeout(total=10)) as session:
                    async with session.ws_connect(f'{self.origin}/v1/stream',
                                                  headers={'Authorization': 'Bearer ' + token},
                                                  max_msg_size=MAX_MESSAGE, compress=0, ssl=True) as ws:
                        parsed = urlsplit(str(ws._response.url))
                        if f'{parsed.scheme}://{parsed.netloc}' != self.origin:
                            raise PolicyError('market-data origin changed')
                        await self.consume(ws)
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, OSError, ValueError, TypeError, RecursionError, asyncio.TimeoutError) as exc:
                self.error = type(exc).__name__
                self.failures += 1
            finally:
                self.replica.disconnect()
            if time.monotonic() - started >= 30:
                delay = .5
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)
