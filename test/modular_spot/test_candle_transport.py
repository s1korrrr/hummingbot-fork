import asyncio
import inspect
from types import SimpleNamespace as NS
from unittest.mock import patch

import aiohttp
import pytest
from aiohttp import web
from test_shared_data import Feed, dataset

from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig
from controllers.generic.modular_spot_core.candle_data import Replica, SnapshotHub
from controllers.generic.modular_spot_core.candle_transport import (
    MAX_MESSAGE,
    CandleClient,
    create_app,
    read_token,
    validate_origin,
)
from controllers.generic.modular_spot_core.data_reader import DataPolicy, DataPool
from controllers.generic.modular_spot_core.domain import PolicyError

TOKEN = 'test-only-token-that-is-at-least-32-characters'
TOKEN_OTHER = 'other-read-token-that-is-at-least-32-ch'


def token_path(tmp_path):
    path = tmp_path.resolve() / 'token'
    path.write_text(TOKEN)
    path.chmod(0o600)
    return str(path)


def test_token_permissions_and_origin_are_not_silently_relaxed(tmp_path):
    path = tmp_path / 'token'
    path.write_text(TOKEN)
    path.chmod(0o600)
    assert read_token(str(path)) == TOKEN
    path.chmod(0o644)
    with pytest.raises(ValueError):
        read_token(str(path))
    for origin in ('http://remote.example', 'http://user:pass@127.0.0.1', 'http://127.0.0.1/?token=secret',
                   'file:///tmp/data', 'https://host.example/path'):
        with pytest.raises(ValueError):
            validate_origin(origin)
    assert validate_origin('http://127.0.0.1:8100') == 'http://127.0.0.1:8100'


def test_real_http_websocket_auth_filtering_and_subscriber_lifetime():
    async def scenario():
        specs = (dataset(), dataset('ETH-USDC'))
        hub = SnapshotHub(specs, factory=Feed, epoch='actual-loopback')
        runner = web.AppRunner(create_app(hub, token=TOKEN, clock=lambda: 6000.0, poll_seconds=.05))
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        base = f'http://127.0.0.1:{port}'
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(base + '/v1/catalog') as response:
                    assert response.status == 401
                headers = {'Authorization': f'Bearer {TOKEN}'}
                async with client.get(base + '/v1/catalog', headers=headers) as response:
                    body = await response.json()
                    assert response.status == 200 and len(body['datasets']) == 2
                first = await client.ws_connect(base + '/v1/stream', headers=headers)
                second = await client.ws_connect(base + '/v1/stream', headers=headers)
                await first.send_json({'datasets': [specs[0].key]})
                await second.send_json({'datasets': [specs[1].key]})
                a = await first.receive_json(timeout=2)
                b = await second.receive_json(timeout=2)
                assert a['type'] == b['type'] == 'snapshot'
                assert a['snapshot']['dataset']['trading_pair'] == 'BTC-USDC'
                assert b['snapshot']['dataset']['trading_pair'] == 'ETH-USDC'
                await first.close()
                assert hub.started and all(f.starts == 1 and f.stops == 0 for f in hub.feeds.values())
                heartbeat = await second.receive_json(timeout=2)
                assert heartbeat['type'] == 'heartbeat'
                assert set(heartbeat['datasets']) == {specs[1].key}
                await second.close()
        finally:
            await runner.cleanup()
        assert not hub.started
    asyncio.run(scenario())


def test_heartbeat_cannot_make_wrong_revision_or_disconnected_data_current():
    from test_shared_data import snapshot
    reader = Replica((dataset(),))
    reader.begin_session()
    value = snapshot()
    reader.accept(value, now=6000)
    reader.touch(dataset().key, epoch=value.epoch, sequence=1, digest=value.digest, published_at=6005, now=6005)
    assert reader.frame(dataset().key, now=6005, max_age=120).shape[0] == 60
    with pytest.raises(ValueError):
        reader.touch(dataset().key, epoch=value.epoch, sequence=2, digest=value.digest, published_at=6006, now=6006)
    assert not reader.connected


def test_http_snapshot_supports_etag_not_modified_without_hiding_corrections():
    async def scenario():
        hub = SnapshotHub((dataset(),), factory=Feed, epoch='etag-loopback')
        runner = web.AppRunner(create_app(hub, token=TOKEN, clock=lambda: 6000.0, poll_seconds=.05))
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        key = dataset().key
        headers = {'Authorization': f'Bearer {TOKEN}'}
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f'http://127.0.0.1:{port}/v1/snapshot/{key}', headers=headers) as response:
                    assert response.status == 200
                    etag = response.headers['ETag']
                    assert 'rows' in await response.json()
                async with client.get(f'http://127.0.0.1:{port}/v1/snapshot/{key}',
                                      headers={**headers, 'If-None-Match': etag}) as response:
                    assert response.status == 304
                    assert response.headers['ETag'] == etag
                hub.feeds[key].candles_df.loc[hub.feeds[key].candles_df.index[-1], 'close'] = 101
                hub.capture(now=6000.0)
                async with client.get(f'http://127.0.0.1:{port}/v1/snapshot/{key}',
                                      headers={**headers, 'If-None-Match': etag}) as response:
                    assert response.status == 200
                    assert response.headers['ETag'] != etag
                    assert (await response.json())['rows'][-1]['close'] == '101'
        finally:
            await runner.cleanup()
    asyncio.run(scenario())


def test_http_snapshot_client_polls_304_then_sees_correction(tmp_path):
    async def scenario():
        hub = SnapshotHub((dataset(),), factory=Feed, epoch='client-etag')
        runner = web.AppRunner(create_app(hub, token=TOKEN, clock=lambda: 6000.0, poll_seconds=.05))
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        key = dataset().key
        client = CandleClient(f'http://127.0.0.1:{port}', token_path(tmp_path), (dataset(),),
                              clock=lambda: 6000.0)
        try:
            async with aiohttp.ClientSession() as session:
                first = await client.poll(session, TOKEN)
                assert first == [200]
                assert client.replica.snapshots[key].rows[-1].close == 100
                second = await client.poll(session, TOKEN)
                assert second == [304]
                assert client.replica.snapshots[key].rows[-1].close == 100
                hub.feeds[key].candles_df.loc[hub.feeds[key].candles_df.index[-1], 'close'] = 101
                hub.capture(now=6000.0)
                third = await client.poll(session, TOKEN)
                assert third == [200]
                assert client.replica.snapshots[key].rows[-1].close == 101
        finally:
            await runner.cleanup()
    asyncio.run(scenario())


def test_data_pool_start_constructs_http_snapshot_polling_client(tmp_path):
    async def scenario():
        recorded = []
        hub = SnapshotHub((dataset(),), factory=Feed, epoch='pool-http')
        app = create_app(hub, token=TOKEN, clock=lambda: 6000.0, poll_seconds=.05)

        @web.middleware
        async def record(request, handler):
            response = await handler(request)
            recorded.append((request.method, request.path, response.status))
            return response

        app.middlewares.append(record)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        origin = f'http://127.0.0.1:{port}'
        pool = DataPool(DataPolicy(mode='shared', origin=origin, token_file=token_path(tmp_path)),
                        clock=lambda: 6000.0)
        pool.attach((dataset(),))
        try:
            pool.start()
            assert isinstance(pool.client, CandleClient)
            assert "ws_connect" not in inspect.getsource(CandleClient.run)
            for _ in range(40):
                if pool.client.replica.snapshots:
                    break
                await asyncio.sleep(0.05)
            key = dataset().key
            assert key in pool.client.replica.snapshots
            paths = [path for _method, path, _status in recorded]
            assert any(path.startswith('/v1/snapshot/') for path in paths)
            assert not any(path == '/v1/stream' for path in paths)
        finally:
            await pool.close()
            await runner.cleanup()
    asyncio.run(scenario())


async def _serve(hub, **kwargs):
    runner = web.AppRunner(create_app(hub, token=TOKEN, clock=lambda: 6000.0, poll_seconds=.05, **kwargs))
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f'http://127.0.0.1:{port}'


def test_two_http_consumers_stop_isolation_and_token_scope(tmp_path):
    async def scenario():
        btc, eth = dataset(), dataset('ETH-USDC')
        hub = SnapshotHub((btc, eth), factory=Feed, epoch='scope-loopback')
        grants = {TOKEN: {btc.key: 60}, TOKEN_OTHER: {eth.key: 60}}
        runner, origin = await _serve(hub, grants=grants)
        headers = {'Authorization': f'Bearer {TOKEN}'}
        try:
            async with aiohttp.ClientSession() as http:
                async with http.get(f'{origin}/v1/snapshot/{eth.key}', headers=headers) as response:
                    assert response.status == 403
                async with http.get(f'{origin}/v1/snapshot/{btc.key}', headers=headers) as response:
                    assert response.status == 200
            left = CandleClient(origin, token_path(tmp_path), (btc,), clock=lambda: 6000.0)
            other_path = tmp_path.resolve() / 'token-other'
            other_path.write_text(TOKEN_OTHER)
            other_path.chmod(0o600)
            right = CandleClient(origin, str(other_path), (eth,), clock=lambda: 6000.0)
            async with aiohttp.ClientSession() as session:
                assert await left.poll(session, TOKEN) == [200]
                assert await right.poll(session, TOKEN_OTHER) == [200]
                await left.close()
                assert hub.started and all(feed.stops == 0 for feed in hub.feeds.values())
                assert await right.poll(session, TOKEN_OTHER) == [304]
            grants.pop(TOKEN)
            async with aiohttp.ClientSession() as http:
                async with http.get(f'{origin}/v1/snapshot/{btc.key}', headers=headers) as response:
                    assert response.status == 401
        finally:
            await runner.cleanup()
        assert not hub.started
    asyncio.run(scenario())


def test_reader_refuses_redirect_and_origin_change(tmp_path):
    async def scenario():
        calls = []

        async def redirected(request):
            calls.append(request.path)
            raise web.HTTPFound('http://127.0.0.1/private')

        app = web.Application()
        app.router.add_get('/v1/snapshot/{key}', redirected)
        app.router.add_get('/private', redirected)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        client = CandleClient(f'http://127.0.0.1:{port}', token_path(tmp_path), (dataset(),),
                              clock=lambda: 6000.0)
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as session:
                with pytest.raises(PolicyError):
                    await client.poll(session, TOKEN)
            assert calls == [f'/v1/snapshot/{dataset().key}']
            with pytest.raises(ValueError):
                validate_origin('http://evil.example')
            with pytest.raises(ValueError):
                validate_origin('https://host.example/path')
        finally:
            await runner.cleanup()
    asyncio.run(scenario())


def test_oversized_snapshot_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr('controllers.generic.modular_spot_core.candle_transport.MAX_MESSAGE', 100)

    async def scenario():
        async def oversized(request):
            return web.Response(body=b'{' + b' ' * 1000, content_type='application/json')

        app = web.Application()
        app.router.add_get('/v1/snapshot/{key}', oversized)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        client = CandleClient(f'http://127.0.0.1:{port}', token_path(tmp_path), (dataset(),),
                              clock=lambda: 6000.0)
        try:
            async with aiohttp.ClientSession() as session:
                with pytest.raises(PolicyError, match='bounded transport'):
                    await client.poll(session, TOKEN)
            assert not client.replica.connected or dataset().key not in client.replica.snapshots
        finally:
            await runner.cleanup()
    asyncio.run(scenario())
    assert MAX_MESSAGE == 8 * 1024 * 1024


def test_control_loop_failure_closes_shared_reader(tmp_path):
    async def scenario():
        closed = []
        value = ModularSpotControllerConfig(
            id='modular_spot_test', connector_name='okx', trading_pair='BTC-USDC',
            data={'mode': 'shared', 'origin': 'http://127.0.0.1:8100', 'token_file': token_path(tmp_path)})
        with patch('controllers.generic.modular_spot_core.runtime.attach_pool', return_value=None):
            controller = ModularSpotController(value, NS(ready=False), asyncio.Queue())
        controller.runtime.close_data = lambda: closed.append(1) or None
        task = asyncio.create_task(controller.control_loop())
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert closed == [1]
    asyncio.run(scenario())


def test_unavailable_dataset_does_not_disconnect_healthy_peer(tmp_path):
    async def scenario():
        btc, eth = dataset(), dataset('ETH-USDC')
        hub = SnapshotHub((btc, eth), factory=Feed, epoch='partial-availability')
        runner, origin = await _serve(hub)
        client = CandleClient(origin, token_path(tmp_path), (btc, eth), clock=lambda: 6000.0)
        try:
            async with aiohttp.ClientSession() as session:
                assert await client.poll(session, TOKEN) == [200, 200]
                # No recapture can restore this missing stream during the request.
                hub.feeds.pop(btc.key)
                hub.errors[btc.key] = 'native feed unavailable'
                assert await client.poll(session, TOKEN) == [503, 304]
                assert client.replica.connected
                assert btc.key not in client.replica.snapshots
                assert eth.key in client.replica.snapshots
                assert btc.key not in client.etags
                assert client.dataset_errors[btc.key] == 'candle HTTP status 503'
                hub.errors.pop(btc.key)
                assert await client.poll(session, TOKEN) == [200, 304]
                assert not client.replica.entry_ready(btc.key)
                assert btc.key not in client.dataset_errors
        finally:
            await client.close()
            await runner.cleanup()
    asyncio.run(scenario())


def test_snapshot_must_match_requested_dataset_even_when_both_are_subscribed(tmp_path):
    from test_shared_data import snapshot

    from controllers.generic.modular_spot_core.candle_transport import snapshot_etag

    async def scenario():
        btc, eth = dataset(), dataset('ETH-USDC')
        wrong = snapshot().model_copy(update={'dataset': eth})

        async def swapped(request):
            return web.json_response(wrong.model_dump(mode='json'), headers={'ETag': snapshot_etag(wrong)})

        app = web.Application()
        app.router.add_get('/v1/snapshot/{key}', swapped)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        client = CandleClient(f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}',
                              token_path(tmp_path), (btc, eth), clock=lambda: 6000.0)
        try:
            async with aiohttp.ClientSession() as session:
                with pytest.raises(PolicyError, match='requested dataset'):
                    await client.poll(session, TOKEN)
            assert not client.replica.snapshots
        finally:
            await client.close()
            await runner.cleanup()
    asyncio.run(scenario())


def test_snapshot_size_limit_stops_stream_before_full_body_read(tmp_path, monkeypatch):
    monkeypatch.setattr('controllers.generic.modular_spot_core.candle_transport.MAX_MESSAGE', 100)

    async def scenario():
        class Chunks:
            async def iter_chunked(self, size):
                yield b'{' + b' ' * 100
                pytest.fail('consumer read beyond bounded response size')

        response = NS(content_type='application/json', content_length=None, content=Chunks())
        client = CandleClient('http://127.0.0.1:8100', token_path(tmp_path), (dataset(),))
        with pytest.raises(PolicyError, match='bounded transport'):
            await client._apply_snapshot(dataset().key, response)
    asyncio.run(scenario())


def test_http_poll_uses_bounded_parallel_requests_for_large_universe(tmp_path):
    async def scenario():
        active = 0
        peak = 0

        async def warming(request):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(.01)
            active -= 1
            return web.Response(status=503)

        app = web.Application()
        app.router.add_get('/v1/snapshot/{key}', warming)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        specs = tuple(dataset(f'ASSET{i}-USDC') for i in range(24))
        client = CandleClient(f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}',
                              token_path(tmp_path), specs, clock=lambda: 6000.0)
        try:
            async with aiohttp.ClientSession() as session:
                assert await client.poll(session, TOKEN) == [503] * 24
            assert 2 <= peak <= 8
            assert len(client.dataset_errors) == 24
        finally:
            await client.close()
            await runner.cleanup()
    asyncio.run(scenario())


def test_large_capture_yields_to_connection_watchdogs():
    import time

    class SlowFeed(Feed):
        def __getattribute__(self, name):
            if name == 'candles_df':
                time.sleep(.004)
            return super().__getattribute__(name)

    async def scenario():
        hub = SnapshotHub(tuple(dataset(f'ASSET{i}-USDC') for i in range(24)),
                          factory=SlowFeed, epoch='yield-capture')
        hub.start()
        ticks = []
        capture = asyncio.create_task(hub.capture_async(now=6000))
        try:
            while not capture.done():
                await asyncio.sleep(.001)
                ticks.append(time.monotonic())
            await capture
            assert len(ticks) >= 5
            assert len(hub.snapshots) == 24
        finally:
            hub.stop()
    asyncio.run(scenario())
