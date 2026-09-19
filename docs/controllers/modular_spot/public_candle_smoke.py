"""Explicit opt-in public feed smoke: no credentials, execution connectors or orders.

Run from the owner checkout with PYTHONPATH=. and native Hummingbot Python.
Only native OKX public candles, a temporary loopback server and private test token.
"""
import argparse
import asyncio
import json
import secrets
import tempfile
import time
from collections import Counter
from pathlib import Path
from uuid import uuid4

from aiohttp import web

from controllers.generic.modular_spot_core.candle_data import Dataset, SnapshotHub
from controllers.generic.modular_spot_core.candle_transport import create_app
from controllers.generic.modular_spot_core.data_reader import DataPolicy, DataPool
from controllers.generic.modular_spot_core.domain import DataUnavailable
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig


async def run(seconds):
    specs = tuple(Dataset(connector='okx', trading_pair=pair, history=90)
                  for pair in ('BTC-USDC', 'ETH-USDC'))
    observations = {spec.key: {'pair': spec.trading_pair, 'confirmed_ws_bars': set(),
                               'rest_warmup_requests': 0, 'ws_messages': 0} for spec in specs}
    throttlers = {}
    status = Counter()

    def factory(spec):
        feed = CandlesFactory.get_candle(CandlesConfig(
            connector=spec.connector, trading_pair=spec.trading_pair,
            interval=spec.interval, max_records=spec.history))
        if feed.closed_candles_only is not True:
            raise RuntimeError('native source does not guarantee exchange-confirmed bars')
        throttlers.setdefault(spec.connector, AsyncThrottler(rate_limits=feed.rate_limits))
        feed.set_throttler(throttlers[spec.connector])
        parse, fetch = feed._parse_websocket_message, feed.fetch_candles
        evidence = observations[spec.key]

        def observed_parse(data):
            evidence['ws_messages'] += 1
            result = parse(data)
            if isinstance(result, dict):
                evidence['confirmed_ws_bars'].add(int(result['timestamp']))
            return result

        async def observed_fetch(*args, **kwargs):
            evidence['rest_warmup_requests'] += 1
            return await fetch(*args, **kwargs)

        feed._parse_websocket_message = observed_parse
        feed.fetch_candles = observed_fetch
        return feed

    hub = SnapshotHub(specs, factory=factory, epoch=uuid4().hex)
    token = secrets.token_urlsafe(48)
    app = create_app(hub, token=token)

    @web.middleware
    async def count_responses(request, handler):
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            status[str(exc.status)] += 1
            raise
        status[str(response.status)] += 1
        return response

    app.middlewares.append(count_responses)
    runner = web.AppRunner(app, access_log=None)
    started = time.time()
    result = {'started_at': started, 'passed': False, 'credentials_used': False,
              'execution_connectors_created': False, 'transport': 'native OKX WebSocket -> loopback HTTP -> DataPool'}
    pools = []
    reconnected = False
    ready_before = False
    try:
        with tempfile.TemporaryDirectory(prefix='pr55-public-candles-') as tmp:
            token_file = Path(tmp).resolve() / 'token'
            token_file.write_text(token)
            token_file.chmod(0o600)
            await runner.setup()
            site = web.TCPSite(runner, '127.0.0.1', 0)
            await site.start()
            port = site._server.sockets[0].getsockname()[1]
            policy = DataPolicy(mode='shared', origin=f'http://127.0.0.1:{port}', token_file=str(token_file))
            for _ in range(2):
                pool = DataPool(policy)
                pool.attach(specs)
                pool.start()
                pools.append(pool)
            while time.time() - started < seconds:
                await asyncio.sleep(1)
                available = []
                for pool in pools:
                    try:
                        available.append(all(len(pool.snapshot(spec, now=time.time(), max_age=120).rows) == 90
                                             for spec in specs))
                    except DataUnavailable:
                        available.append(False)
                ready = all(available) and all(pool.entry_ready(spec) for pool in pools for spec in specs)
                if ready and not reconnected:
                    ready_before = True
                    await pools[0].close()
                    assert hub.started and pools[1].client.replica.connected
                    pools[0].start()
                    reconnected = True
                    print(json.dumps({'event': 'reader_reconnect', 'elapsed': time.time() - started}), flush=True)
                elif ready and reconnected:
                    assert all(hub.latest(spec.key).rows[-1].open_time in observations[spec.key]['confirmed_ws_bars']
                               for spec in specs)
                    result['passed'] = True
                    result['final_snapshots'] = {
                        spec.trading_pair: {'rows': len(hub.latest(spec.key).rows),
                                            'latest_open': hub.latest(spec.key).rows[-1].open_time,
                                            'sequence': hub.latest(spec.key).sequence,
                                            'candle_age_seconds': time.time() - hub.latest(spec.key).rows[-1].open_time - 60}
                        for spec in specs}
                    break
    finally:
        result['hub_errors_at_completion'] = dict(hub.errors)
        result['readers_at_completion'] = [
            {'error': pool.client.error, 'dataset_errors': dict(pool.client.dataset_errors),
             'connected': pool.client.replica.connected}
            for pool in pools if pool.client is not None]
        for pool in pools:
            await pool.close()
        await runner.cleanup()
        await asyncio.sleep(0)
    result.update(elapsed_seconds=time.time() - started, reader_ready_before_reconnect=ready_before,
                  reader_reconnected=reconnected, http_status_counts=dict(status),
                  shutdown_complete=not hub.started and all(pool.client is None for pool in pools),
                  feeds=[{**value, 'confirmed_ws_bars': sorted(value['confirmed_ws_bars'])}
                         for value in observations.values()])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, default=240)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not 60 <= args.seconds <= 600:
        parser.error('seconds must be between 60 and 600')
    result = asyncio.run(run(args.seconds))
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2), flush=True)
    raise SystemExit(0 if result['passed'] else 1)


if __name__ == '__main__':
    main()
