"""Bounded read-only HTTP/WebSocket distribution, independent of execution owners."""
import asyncio
import json
import os
from pathlib import Path
import secrets
import stat
import time
from urllib.parse import urlsplit
from uuid import uuid4

import aiohttp
from aiohttp import web

from .candle_data import CandleSnapshot, Dataset, Replica, SnapshotHub
from .domain import DataUnavailable, PolicyError, decimal

MAX_MESSAGE = 8 * 1024 * 1024


def read_token(path: str) -> str:
    target = Path(path)
    if not target.is_absolute() or any(p.is_symlink() for p in (target, *target.parents)):
        raise ValueError('token must use an absolute non-symlink path')
    descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or not 32 <= info.st_size <= 4096:
            raise ValueError('token file must be private, regular and between 32 and 4096 bytes')
        token = os.read(descriptor, 4097).decode('ascii').rstrip('\n')
        if len(token) < 32 or any(c.isspace() for c in token):
            raise ValueError('invalid read-only data token')
        return token
    finally:
        os.close(descriptor)


def validate_origin(value: str) -> str:
    parsed = urlsplit(value)
    if (parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username or parsed.password
            or parsed.path not in {'', '/'} or parsed.query or parsed.fragment):
        raise ValueError('data origin must be a fixed HTTP(S) origin without credentials or a path')
    if parsed.scheme == 'http' and parsed.hostname not in {'127.0.0.1', '::1'}:
        raise ValueError('remote market-data transport requires verified TLS')
    if parsed.port is not None and not 1 <= parsed.port <= 65535:
        raise ValueError('invalid data-service port')
    return value.rstrip('/')


def create_app(hub: SnapshotHub, *, token: str, clock=time.time, poll_seconds: float = 1.0,
               max_clients: int = 128) -> web.Application:
    poll_seconds = float(decimal(poll_seconds, 'poll interval', positive=True))
    if len(token) < 32 or not 1 <= max_clients <= 1024:
        raise ValueError('invalid service authentication or client bound')
    clients = set()

    @web.middleware
    async def authenticate(request, handler):
        candidate = request.headers.get('Authorization', '')
        if not secrets.compare_digest(candidate, 'Bearer ' + token):
            raise web.HTTPUnauthorized()
        return await handler(request)

    app = web.Application(middlewares=[authenticate], client_max_size=65536)

    async def catalog(request):
        return web.json_response({'schema_version': 1, 'epoch': hub.epoch,
                                  'datasets': [spec.model_dump(mode='json') for spec in hub.datasets.values()],
                                  'errors': dict(hub.errors)})

    async def snapshot(request):
        try:
            value = hub.latest(request.match_info['key'])
            value.frame(now=clock(), max_age=hub.max_age)
            return web.json_response(value.model_dump(mode='json'))
        except PolicyError as exc:
            raise web.HTTPServiceUnavailable(text=str(exc)) from exc

    async def stream(request):
        if len(clients) >= max_clients:
            raise web.HTTPServiceUnavailable(text='bounded candle client capacity reached')
        ws = web.WebSocketResponse(heartbeat=10, max_msg_size=65536, compress=False)
        clients.add(ws)
        try:
            await ws.prepare(request)
            initial = await asyncio.wait_for(ws.receive_json(), timeout=10)
            if not isinstance(initial, dict) or set(initial) != {'datasets'}:
                raise PolicyError('exact dataset subscription required')
            keys = initial['datasets']
            if (not isinstance(keys, list) or not 1 <= len(keys) <= 1000
                    or any(not isinstance(key, str) for key in keys)
                    or len(set(keys)) != len(keys) or not set(keys) <= set(hub.datasets)):
                raise PolicyError('unknown, duplicated or unbounded dataset subscription')
            sent = {}
            while not ws.closed:
                current = {}
                errors = {}
                for key in keys:
                    try:
                        value = hub.latest(key)
                        value.frame(now=clock(), max_age=hub.max_age)
                        current[key] = value
                    except PolicyError as exc:
                        sent.pop(key, None)
                        errors[key] = str(exc)
                for key, value in current.items():
                    if sent.get(key) != value.sequence:
                        payload = {'type': 'snapshot', 'snapshot': value.model_dump(mode='json')}
                        wire = json.dumps(payload, separators=(',', ':'), allow_nan=False)
                        if len(wire.encode()) > MAX_MESSAGE:
                            raise PolicyError('snapshot exceeds bounded transport size')
                        await asyncio.wait_for(ws.send_str(wire), timeout=5)
                        sent[key] = value.sequence
                heartbeat = {key: {'sequence': value.sequence, 'digest': value.digest,
                                   'published_at': value.published_at} for key, value in current.items()}
                await asyncio.wait_for(ws.send_json({'type': 'heartbeat', 'epoch': hub.epoch,
                                                     'datasets': heartbeat, 'errors': errors}), timeout=5)
                try:
                    message = await ws.receive(timeout=poll_seconds)
                except asyncio.TimeoutError:
                    continue
                if message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                    break
                if message.type == aiohttp.WSMsgType.TEXT:
                    raise PolicyError('changing subscriptions requires a new scoped session')
        except (ValueError, TypeError, PolicyError, asyncio.TimeoutError):
            await ws.close(code=1008, message=b'Invalid or unavailable candle subscription')
        finally:
            clients.discard(ws)
        return ws

    async def lifecycle(application):
        hub.start()
        hub.capture(now=clock())
        async def capture_loop():
            while True:
                await asyncio.sleep(poll_seconds)
                try:
                    hub.capture(now=clock())
                except (ValueError, TypeError, RuntimeError, ArithmeticError) as exc:
                    hub.errors = {key: f'capture unavailable: {type(exc).__name__}' for key in hub.datasets}
        task = asyncio.create_task(capture_loop(), name='modular-candle-capture')
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.gather(*(client.close(code=1001) for client in tuple(clients)), return_exceptions=True)
            hub.stop()

    app.cleanup_ctx.append(lifecycle)
    app.router.add_get('/v1/catalog', catalog)
    app.router.add_get('/v1/snapshot/{key}', snapshot)
    app.router.add_get('/v1/stream', stream)
    return app


class CandleClient:
    """An opt-in consumer. Reconnect warms state but never authorizes a historical entry."""

    def __init__(self, origin: str, token_file: str, datasets: tuple[Dataset, ...], *, clock=time.time):
        self.origin = validate_origin(origin)
        self.token_file = token_file
        self.replica = Replica(datasets)
        self.clock = clock
        self.task = None
        self.error = None

    def start(self):
        if self.task is None:
            self.task = asyncio.create_task(self.run(), name='modular-candle-reader')

    async def close(self):
        self.replica.disconnect()
        task, self.task = self.task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def run(self):
        delay = 1.0
        async def reject_redirect(*args):
            raise PolicyError('market-data redirects are not admitted')
        trace = aiohttp.TraceConfig()
        trace.on_request_redirect.append(reject_redirect)
        try:
            token = read_token(self.token_file)
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15), trust_env=False,
                                             trace_configs=[trace]) as session:
                while True:
                    try:
                        async with session.ws_connect(self.origin + '/v1/stream', heartbeat=10, compress=0,
                                                      max_msg_size=MAX_MESSAGE, ssl=True,
                                                      headers={'Authorization': 'Bearer ' + token}) as socket:
                            self.replica.begin_session()
                            await socket.send_json({'datasets': list(self.replica.datasets)})
                            while True:
                                message = await asyncio.wait_for(socket.receive(), timeout=15)
                                if message.type != aiohttp.WSMsgType.TEXT:
                                    if message.type in {aiohttp.WSMsgType.PING, aiohttp.WSMsgType.PONG}:
                                        continue
                                    raise DataUnavailable('candle socket disconnected')
                                body = json.loads(message.data)
                                self.consume(body)
                                delay, self.error = 1.0, None
                    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError, KeyError) as exc:
                        self.replica.disconnect()
                        self.error = f'{type(exc).__name__}: {exc}'
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 30.0)
        finally:
            self.replica.disconnect()

    def consume(self, body):
        try:
            self._consume(body)
            self.replica.revision += 1
        except (ValueError, TypeError, KeyError, AttributeError):
            self.replica.disconnect()
            raise

    def _consume(self, body):
        if not isinstance(body, dict):
            raise PolicyError('invalid candle message')
        now = self.clock()
        if body.get('type') == 'snapshot' and set(body) == {'type', 'snapshot'}:
            self.replica.accept(CandleSnapshot.model_validate(body['snapshot']), now=now)
        elif body.get('type') == 'heartbeat' and set(body) == {'type', 'epoch', 'datasets', 'errors'}:
            if (not isinstance(body['epoch'], str) or not self.replica.connected
                    or (self.replica.epoch is not None and body['epoch'] != self.replica.epoch)):
                raise PolicyError('heartbeat source epoch changed')
            if not isinstance(body['datasets'], dict) or not isinstance(body['errors'], dict):
                raise PolicyError('invalid dataset health')
            if set(body['datasets']) | set(body['errors']) != set(self.replica.datasets):
                raise PolicyError('incomplete or foreign dataset heartbeat')
            if set(body['datasets']) & set(body['errors']):
                raise PolicyError('conflicting dataset health')
            for key in body['errors']:
                self.replica.snapshots.pop(key, None)
                self.replica.seed_bars.pop(key, None)
            for key, metadata in body['datasets'].items():
                if not isinstance(metadata, dict) or set(metadata) != {'sequence', 'digest', 'published_at'}:
                    raise PolicyError('invalid revision heartbeat')
                self.replica.touch(key, epoch=body['epoch'], now=now, **metadata)
        else:
            raise PolicyError('unsupported candle protocol')


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Read-only native candle distributor; no execution services')
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--token-file', required=True)
    parser.add_argument('--port', type=int, default=8100)
    args = parser.parse_args()
    source = Path(args.manifest)
    if source.stat().st_size > 1024 * 1024:
        raise ValueError('candle manifest is too large')
    manifest = json.loads(source.read_text())
    if not isinstance(manifest, dict) or set(manifest) != {'schema_version', 'datasets'} or type(manifest['schema_version']) is not int or manifest['schema_version'] != 1:
        raise ValueError('unsupported candle manifest')
    specs = tuple(Dataset.model_validate(row) for row in manifest['datasets'])
    from hummingbot.client.settings import AllConnectorSettings, ConnectorType
    from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
    from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
    from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
    throttlers = {}
    def native_factory(spec):
        setting = AllConnectorSettings.get_connector_settings().get(spec.connector)
        if spec.source_profile != 'native' or setting is None or setting.type != ConnectorType.Exchange:
            raise PolicyError('only the declared native centralized spot source is admitted')
        feed = CandlesFactory.get_candle(CandlesConfig(connector=spec.connector, trading_pair=spec.trading_pair,
                                                       interval=spec.interval, max_records=spec.history))
        if getattr(feed, 'closed_candles_only', False) is not True:
            raise PolicyError('native candle source has no explicit finalized-only contract')
        if spec.connector not in throttlers:
            throttlers[spec.connector] = AsyncThrottler(rate_limits=feed.rate_limits)
        feed.set_throttler(throttlers[spec.connector])
        return feed
    hub = SnapshotHub(specs, factory=native_factory, epoch=uuid4().hex)
    web.run_app(create_app(hub, token=read_token(args.token_file)), host='127.0.0.1', port=args.port)


if __name__ == '__main__':
    main()
