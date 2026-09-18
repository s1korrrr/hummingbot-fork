import asyncio

import aiohttp
from aiohttp import web
import pytest

from controllers.generic.modular_spot_core.candle_data import Replica, SnapshotHub
from controllers.generic.modular_spot_core.candle_transport import create_app, read_token, validate_origin
from test_shared_data import Feed, dataset


TOKEN = 'test-only-token-that-is-at-least-32-characters'


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
