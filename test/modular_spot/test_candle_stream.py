import asyncio
from collections import deque
from types import SimpleNamespace
from unittest.mock import patch

import aiohttp
import pytest
from aiohttp import web
from test_shared_data import dataset

from controllers.generic.modular_spot_core.candle_stream import CandleStreamClient
from controllers.generic.modular_spot_core.data_reader import DataPolicy, DataPool
from controllers.generic.modular_spot_core.domain import DataUnavailable, PolicyError

TOKEN = 'test-only-candle-stream-token-1234567890'


@pytest.mark.parametrize('history', [500, 3660, 7515, 10000])
def test_native_warmup_history_is_preserved_without_replayed_entry(history):
    now = [history * 60 + 6000]
    spec = dataset(records=history)
    reader = CandleStreamClient('http://127.0.0.1:1', '/unused', (spec,), clock=lambda: now[0])
    reader.begin_session()
    reader.apply(message(at=now[0], rows=[row(t) for t in range(now[0] - history * 60, now[0], 60)]))
    assert len(reader.replica.frame(spec.key, now=now[0], max_age=120)) == history
    assert not reader.replica.entry_ready(spec.key)
    now[0] += 60
    reader.apply(message('candle', sequence=2, at=now[0]))
    assert len(reader.replica.snapshots[spec.key].rows) == history
    assert reader.replica.entry_ready(spec.key)


def test_stream_subscription_caps_the_negotiated_wire_history():
    # The wire negotiates one maximum history for every pair, even when local
    # requests have different window lengths. Bound that actual allocation.
    specs = (dataset('LARGE-USDC', records=10000),
             *(dataset(f'P{i}-USDC', records=60) for i in range(300)))
    with pytest.raises(ValueError, match='bounded'):
        CandleStreamClient('http://127.0.0.1:1', '/unused', specs)
    with pytest.raises(ValueError):
        dataset(records=10001)


def row(stamp=5940, close='100'):
    return dict(open_time=stamp, open='100', high='102', low='98', close=close,
                volume='5', quote_volume='500', confirmed=True)


def message(kind='snapshot', pair='BTC-USDC', sequence=1, at=6000, **changes):
    result = dict(v=1, type=kind, epoch='test-epoch', published_at=at)
    if kind == 'heartbeat':
        result['states'] = {pair: dict(ready=True, sequence=sequence)}
    else:
        result.update(pair=pair, sequence=sequence)
        if kind == 'status':
            result.update(ready=False, reason='upstream disconnected')
        elif kind == 'snapshot':
            result.update(received_at=at, rows=[row(t) for t in range(int(at) - 3600, int(at), 60)])
        else:
            result.update(received_at=at, row=row(int(at) - 60))
    result.update(changes)
    return result


def client(pairs=('BTC-USDC',), clock=lambda: 6000):
    result = CandleStreamClient('http://127.0.0.1:1', '/unused', tuple(dataset(p) for p in pairs), clock=clock)
    result.begin_session()
    return result


def test_snapshot_delta_correction_and_recovery_entry_semantics():
    now = [6000]
    reader = client(clock=lambda: now[0])
    key = dataset().key
    reader.apply(message())
    assert not reader.replica.entry_ready(key)
    now[0] = 6060
    reader.apply(message('candle', sequence=2, at=6060))
    assert reader.replica.entry_ready(key)
    reader.apply(message('candle', sequence=3, at=6060, row=row(6000, '101')))
    assert reader.replica.entry_ready(key)
    assert reader.replica.frame(key, now=6060, max_age=120).close.iloc[-1] == 101
    reader.apply(message('status', sequence=4, at=6060))
    with pytest.raises(DataUnavailable):
        reader.replica.frame(key, now=6060, max_age=120)
    reader.apply(message(sequence=9, at=6060))
    assert not reader.replica.entry_ready(key)
    now[0] = 6120
    reader.apply(message('candle', sequence=10, at=6120))
    assert reader.replica.entry_ready(key)
    assert reader.update_receipts[-1]['epoch'] == 'test-epoch'
    assert reader.replica.frame(key, now=6120, max_age=120).timestamp.iloc[-1] == 6060


def test_unhealthy_pair_does_not_wipe_healthy_peer():
    reader = client(('BTC-USDC', 'ETH-USDC'))
    for pair in reader.by_pair:
        reader.apply(message(pair=pair))
    reader.apply(message('status', sequence=2))
    reader.apply(message('heartbeat', states={'BTC-USDC': dict(ready=False, sequence=2),
                                              'ETH-USDC': dict(ready=True, sequence=1)}))
    assert dataset().key not in reader.replica.snapshots
    assert len(reader.replica.frame(dataset('ETH-USDC').key, now=6000, max_age=120)) == 60
    assert reader.replica.connected


@pytest.mark.parametrize('changes', [
    {'v': True}, {'v': 2}, {'epoch': 'changed'}, {'published_at': 6001},
    {'published_at': float('nan')}, {'published_at': 10 ** 1000}, {'sequence': True}, {'sequence': 3},
    {'pair': 'ETH-USDC'}, {'extra': 1}, {'received_at': 6001},
    {'row': row(5880)}, {'row': row(6060)}, {'row': {**row(), 'confirmed': False}},
    {'row': {**row(), 'quote_volume': None}}, {'row': {**row(), 'volume': 'NaN'}},
    {'row': {**row(), 'high': '50'}}, {'row': {**row(), 'open': 100}},
])
def test_malformed_or_gapped_delta_fails_closed(changes):
    reader = client()
    reader.apply(message())
    with pytest.raises(ValueError):
        reader.apply({**message('candle', sequence=2), **changes})
    assert not reader.replica.connected


@pytest.mark.parametrize('states', [{}, {'OTHER-USDC': dict(ready=True, sequence=1)},
                                    {'BTC-USDC': dict(ready=True, sequence=2)},
                                    {'BTC-USDC': dict(ready=False, sequence=1)},
                                    {'BTC-USDC': dict(ready=True, sequence=True)}])
def test_heartbeat_exact_set_sequence_and_readiness(states):
    reader = client()
    reader.apply(message())
    with pytest.raises(PolicyError):
        reader.apply(message('heartbeat', states=states))
    assert not reader.replica.connected


def test_heartbeat_cannot_make_old_candle_fresh_or_authorize_entry():
    now = [6000]
    reader = client(clock=lambda: now[0])
    reader.apply(message())
    now[0] = 6200
    reader.apply(message('heartbeat', at=6200))
    assert not reader.replica.entry_ready(dataset().key)
    with pytest.raises(DataUnavailable):
        reader.replica.frame(dataset().key, now=6200, max_age=120)


def test_snapshot_capacity_continuity_and_initial_status():
    reader = client()
    reader.apply(message('status', sequence=0))
    reader.apply(message('heartbeat', states={'BTC-USDC': dict(ready=False, sequence=0)}))
    with pytest.raises(PolicyError):
        reader.apply(message(rows=[row()]))
    reader.begin_session()
    rows = message()['rows']
    rows[20] = row(1000)
    with pytest.raises(ValueError):
        reader.apply(message(rows=rows))


def test_receipts_are_bounded_and_overflow_is_observable():
    reader = client()
    assert reader.update_receipts.maxlen == 4096
    reader.update_receipts = deque(maxlen=2)
    reader.apply(message())
    reader.apply(message('candle', sequence=2))
    reader.apply(message('candle', sequence=3))
    assert len(reader.update_receipts) == 2 and reader.receipt_overflows == 1


def test_transport_opt_in_and_native_path_unchanged():
    assert DataPolicy().transport == 'snapshot'
    with pytest.raises(ValueError):
        DataPolicy(transport='stream')
    for spec in (dataset('BTC-USDT'), dataset().model_copy(update={'interval': '5m'}),
                 dataset().model_copy(update={'source_profile': 'other'})):
        with pytest.raises(ValueError):
            CandleStreamClient('http://127.0.0.1', '/unused', (spec,))
    policy = DataPolicy(mode='shared', transport='stream', origin='http://127.0.0.1', token_file='/unused')
    pool = DataPool(policy)
    pool.attach((dataset(),))
    with patch.object(CandleStreamClient, 'start'):
        pool.start()
    assert isinstance(pool.client, CandleStreamClient)


def test_missing_heartbeat_deadline_is_not_reset_by_busy_data():
    async def scenario():
        reader = client()
        ws = SimpleNamespace(send_json=lambda value: asyncio.sleep(0))
        with patch('controllers.generic.modular_spot_core.candle_stream.time') as clock:
            clock.monotonic.side_effect = [0, 11]
            with pytest.raises(DataUnavailable, match='heartbeat deadline'):
                await reader.consume(ws)
    asyncio.run(scenario())


def test_real_loopback_adapter_auth_updates_disconnect_and_reseed(tmp_path):
    async def scenario():
        token = tmp_path.resolve() / 'token'
        token.write_text(TOKEN)
        token.chmod(0o600)
        connected = 0
        second_ready = asyncio.Event()
        release = asyncio.Event()
        now = [6000]

        async def stream(request):
            nonlocal connected
            assert request.headers['Authorization'] == 'Bearer ' + TOKEN
            assert not request.query
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            subscription = await ws.receive_json()
            assert subscription == dict(v=1, op='subscribe', pairs=['BTC-USDC'], history=60)
            connected += 1
            await ws.send_json(message(at=now[0]))
            await ws.send_json(message('heartbeat', at=now[0]))
            if connected == 1:
                await asyncio.sleep(.03)
                now[0] = 6060
                await ws.send_json(message('candle', sequence=2, at=6060))
                await ws.close()
            else:
                second_ready.set()
                await release.wait()
                await ws.close()
            return ws

        app = web.Application()
        app.router.add_get('/v1/stream', stream)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        reader = CandleStreamClient(f'http://127.0.0.1:{port}', str(token), (dataset(),), clock=lambda: now[0])
        try:
            reader.start()
            await asyncio.wait_for(second_ready.wait(), 4)
            for _ in range(50):
                if reader.sessions == 2 and reader.replica.snapshots:
                    break
                await asyncio.sleep(.01)
            assert reader.sessions == 2 and reader.failures == 1
            assert len(reader.replica.frame(dataset().key, now=6060, max_age=120)) == 60
            assert not reader.replica.entry_ready(dataset().key)
            assert [r['type'] for r in reader.update_receipts] == ['snapshot', 'candle', 'snapshot']
        finally:
            await reader.close()
            release.set()
            await runner.cleanup()
        assert not reader.replica.connected
    asyncio.run(scenario())


def test_redirect_rejected_before_token_sent_to_other_origin(tmp_path):
    async def scenario():
        token = tmp_path.resolve() / 'token'
        token.write_text(TOKEN)
        token.chmod(0o600)
        hits = []

        async def redirect(request):
            raise web.HTTPFound('/leak')

        async def leak(request):
            hits.append(request.headers.get('Authorization'))
            return web.Response()

        app = web.Application()
        app.router.add_get('/v1/stream', redirect)
        app.router.add_get('/leak', leak)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        reader = CandleStreamClient(f'http://127.0.0.1:{port}', str(token), (dataset(),))
        try:
            reader.start()
            for _ in range(50):
                if reader.failures:
                    break
                await asyncio.sleep(.01)
            assert reader.failures == 1 and not hits
            assert not reader.replica.connected
        finally:
            await reader.close()
            await runner.cleanup()
    asyncio.run(scenario())


def test_same_origin_different_transports_never_share_a_pool():
    from controllers.generic.modular_spot import ModularSpotControllerConfig
    from controllers.generic.modular_spot_core.data_reader import attach_pool, release_pool

    class Provider:
        pass

    provider = Provider()
    configs = [
        ModularSpotControllerConfig(
            id='modular_spot_test', connector_name='okx', trading_pair='BTC-USDC', max_records=90,
            data=DataPolicy(mode='shared', transport=transport,
                            origin='http://127.0.0.1', token_file='/unused'))
        for transport in ('snapshot', 'stream')]
    pools = [attach_pool(provider, config) for config in configs]
    assert pools[0] is not pools[1]
    for pool in pools:
        release_pool(provider, pool)


def test_busy_frames_cannot_extend_missing_heartbeat_deadline():
    async def scenario():
        reader = client()

        async def send(value):
            pass

        async def receive():
            return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=__import__('json').dumps(message()))

        ws = SimpleNamespace(send_json=send, receive=receive)
        with patch('controllers.generic.modular_spot_core.candle_stream.time') as clock:
            clock.monotonic.side_effect = [0, 1, 11]
            with pytest.raises(DataUnavailable, match='heartbeat deadline'):
                await reader.consume(ws)
        assert len(reader.update_receipts) == 1
    asyncio.run(scenario())
