"""A real loopback server keeps heartbeating (1 s) on its own thread while the client's event loop
is stalled by a blocking callback. A stall past the 10 s heartbeat deadline must not end a session
whose heartbeats were delivered during the stall (adapted from the hummingbot#123 review repro)."""
import asyncio
import threading
import time

import aiohttp
import pytest
from aiohttp import web
from test_shared_data import dataset

from controllers.generic.modular_spot_core.candle_stream import CandleStreamClient


def serve(ready, holder):
    loop = asyncio.new_event_loop()

    async def stream(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json(dict(v=1, type='status', epoch='e', published_at=time.time(), pair='BTC-USDC',
                                sequence=1, ready=False, reason='warming'))
        while not ws.closed:
            await ws.send_json(dict(v=1, type='heartbeat', epoch='e', published_at=time.time(),
                                    states={'BTC-USDC': dict(ready=False, sequence=1)}))
            await asyncio.sleep(1)
        return ws

    async def main():
        app = web.Application()
        app.router.add_get('/v1/stream', stream)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        holder['port'] = site._server.sockets[0].getsockname()[1]
        ready.set()
        await asyncio.Event().wait()
    loop.run_until_complete(main())


# 10.2 s passes the 10 s deadline while the oldest buffered heartbeat stays inside the 10 s
# transport-age bound; 9.0 s never reaches the deadline (control).
@pytest.mark.parametrize('stall', [10.2, 9.0])
def test_session_survives_a_stall_while_parked_in_receive(stall):
    ready, holder = threading.Event(), {}
    threading.Thread(target=serve, args=(ready, holder), daemon=True).start()
    assert ready.wait(5)

    async def scenario():
        reader = CandleStreamClient('http://127.0.0.1:1', '/unused', (dataset(),))
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{holder['port']}/v1/stream") as ws:
                task = asyncio.create_task(reader.consume(ws))
                while reader.last_heartbeat_at is None:
                    await asyncio.sleep(.01)
                first = reader.last_heartbeat
                await asyncio.sleep(.3)  # the stall begins while consume is parked in receive()
                asyncio.get_running_loop().call_soon(time.sleep, stall)
                await asyncio.sleep(3)
                outcome = 'alive' if not task.done() else repr(task.exception())
                advanced = reader.last_heartbeat != first
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                return outcome, advanced
    outcome, advanced = asyncio.run(scenario())
    assert outcome == 'alive', outcome
    assert advanced
