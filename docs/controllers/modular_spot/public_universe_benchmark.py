"""Opt-in public OKX USDC diagnostic using native assistants/parser, never orders.

This multiplexed transport benchmark is NOT the per-pair native collector or a
production publisher deployment. It measures exchange-confirmed candle availability.
"""
import argparse
import asyncio
import json
import math
import random
import resource
import time
from collections import Counter
from pathlib import Path

import aiohttp

from controllers.generic.modular_spot_core.candle_data import Candle
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSPlainTextRequest
from hummingbot.data_feed.candles_feed.okx_spot_candles.okx_spot_candles import OKXSpotCandles

PONG_TIMEOUT = 10


class Pacer:
    def __init__(self, interval=1.05):
        self.interval = interval
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            await asyncio.sleep(max(0.0, self.next_at - time.monotonic()))
            self.next_at = time.monotonic() + self.interval


def subscription(pairs):
    if not pairs or len(set(pairs)) != len(pairs):
        raise ValueError('subscription must be nonempty and unique')
    value = {'op': 'subscribe', 'args': [{'channel': 'candle1m', 'instId': pair} for pair in pairs]}
    if len(json.dumps(value).encode()) > 64000:
        raise ValueError('subscription exceeds 64KB bound')
    return value


def quantiles(values):
    values = sorted(values)
    if not values:
        return {'count': 0, 'p50': None, 'p95': None, 'p99': None, 'max': None}
    return {'count': len(values), **{f'p{q}': values[math.ceil(q / 100 * len(values)) - 1]
                                     for q in (50, 95, 99)}, 'max': values[-1]}


def completed_minutes(start, end, grace=10):
    first = math.ceil(start / 60) * 60
    # These are OPEN times: only bars opened after full subscription and closed with grace.
    last = math.floor((end - grace) / 60) * 60 - 60
    return list(range(first, last + 1, 60))


async def clock_sample(session):
    sent = time.time()
    begin = time.monotonic()
    async with session.get('https://www.okx.com/api/v5/public/time', allow_redirects=False) as response:
        if response.status != 200:
            raise RuntimeError(f'public clock HTTP {response.status}')
        body = await response.json()
    received = time.time()
    if body.get('code') != '0':
        raise RuntimeError('public clock returned error')
    return {'offset_seconds': int(body['data'][0]['ts']) / 1000 - (sent + received) / 2,
            'rtt_seconds': time.monotonic() - begin, 'sampled_at': received}


class Benchmark:
    def __init__(self, pairs, output, *, seconds=300, batch=100):
        if not pairs or len(set(pairs)) != len(pairs) or any(not p.endswith('-USDC') for p in pairs):
            raise ValueError('unique USDC universe required')
        self.pairs, self.output, self.seconds = pairs, output, seconds
        self.groups = [pairs[i:i + batch] for i in range(0, len(pairs), batch)]
        self.parsers = {p: OKXSpotCandles(p, interval='1m', max_records=60) for p in pairs}
        self.pacer, self.stop = Pacer(), asyncio.Event()
        self.acks, self.first, self.first_confirmed, self.latest, self.bars = set(), {}, {}, {}, {}
        self.messages, self.duplicates, self.corrections = Counter(), 0, 0
        self.connections, self.errors, self.recoveries = [], [], []
        self.sockets, self.injected = {}, set()
        self.ping_rtts, self.parse_durations, self.loop_lags = [], [], []
        self.clock_samples, self.offset = [], 0.0
        self.started, self.steady_at = None, None
        self.receipts = None
        self.capture_end = None

    def message(self, body, pairs, socket_id, generation):
        if self.capture_end is not None:
            return
        begin, received = time.monotonic(), time.time()
        if not isinstance(body, dict):
            raise ValueError('unexpected public message')
        if body.get('event') == 'error':
            raise RuntimeError(f"OKX subscription error code={body.get('code')} msg={body.get('msg')}")
        arg = body.get('arg', {})
        pair = arg.get('instId')
        if pair not in pairs or arg.get('channel') != 'candle1m':
            raise ValueError('foreign instrument or channel')
        if body.get('event') == 'subscribe':
            self.acks.add(pair)
            return
        rows = body.get('data')
        if not isinstance(rows, list) or not rows:
            raise ValueError('empty/malformed candle payload')
        self.messages[pair] += 1
        self.first.setdefault(pair, received)
        for row in rows:
            if not isinstance(row, list) or len(row) < 9 or str(row[8]) not in {'0', '1'}:
                raise ValueError('invalid confirmation payload')
            parsed = self.parsers[pair]._parse_websocket_message({'data': [row]})
            if parsed is None:
                continue
            stamp = int(parsed['timestamp'])
            Candle(open_time=stamp, open=parsed['open'], high=parsed['high'], low=parsed['low'],
                   close=parsed['close'], volume=parsed['volume'], quote_volume=parsed['quote_asset_volume'])
            key = (pair, stamp)
            content = tuple(row)
            if key in self.bars:
                if self.bars[key]['content'] == content:
                    self.duplicates += 1
                else:
                    self.corrections += 1
                    if self.receipts:
                        self.receipts.write(json.dumps({'type': 'correction', 'pair': pair, 'open_time': stamp,
                                                       'received_at': received, 'content': content}) + '\n')
                    self.bars[key]['content'] = content
                continue
            entry = {'pair': pair, 'open_time': stamp, 'received_at': received,
                     'close_delay_seconds': received + self.offset - (stamp + 60),
                     'socket': socket_id, 'generation': generation, 'content': content}
            self.bars[key] = entry
            self.first_confirmed.setdefault(pair, received)
            self.latest[pair] = max(stamp, self.latest.get(pair, 0))
            if self.receipts:
                self.receipts.write(json.dumps(entry) + '\n')
        self.parse_durations.append(time.monotonic() - begin)

    async def socket(self, index, pairs):
        parser = self.parsers[pairs[0]]
        generation = 0
        expected_reconnect = False
        unexpected = 0
        while not self.stop.is_set() and unexpected <= 3:
            ws = None
            try:
                await self.pacer.wait()
                ws = await parser._api_factory.get_ws_assistant()
                self.connections.append({'socket': index, 'generation': generation, 'attempted_at': time.time()})
                await asyncio.wait_for(ws.connect(parser.wss_url, ping_timeout=None), timeout=20)
                self.sockets[index] = ws
                await ws.send(WSJSONRequest(payload=subscription(pairs)))
                pending, connected_at = set(pairs), time.monotonic()
                last_ping = None
                pong_due = None
                while not self.stop.is_set():
                    if pending and time.monotonic() - connected_at > 30:
                        raise TimeoutError(f'{len(pending)} subscription acknowledgements missing')
                    # Periodic application ping also exercises pong on busy streams.
                    if pong_due is not None and time.monotonic() >= pong_due:
                        raise TimeoutError('application pong deadline exceeded')
                    if pong_due is None and (last_ping is None or time.monotonic() - last_ping > 20):
                        sent = time.monotonic()
                        await ws.send(WSPlainTextRequest(payload='ping'))
                        last_ping = sent
                        pong_due = sent + PONG_TIMEOUT
                    timeout = min(15, max(.001, pong_due - time.monotonic())) if pong_due else 15
                    try:
                        response = await asyncio.wait_for(ws.receive(), timeout=timeout)
                    except asyncio.TimeoutError:
                        if pong_due is not None:
                            raise TimeoutError('application pong deadline exceeded')
                        last_ping = time.monotonic()
                        await ws.send(WSPlainTextRequest(payload='ping'))
                        pong_due = last_ping + PONG_TIMEOUT
                        continue
                    if self.capture_end is not None:
                        break
                    if response is None:
                        raise ConnectionError('benchmark socket disconnected')
                    if response.data == 'pong':
                        if pong_due is not None:
                            self.ping_rtts.append(time.monotonic() - last_ping)
                        pong_due = None
                        continue
                    self.message(response.data, set(pairs), index, generation)
                    if response.data.get('event') == 'subscribe':
                        pending.discard(response.data['arg']['instId'])
                    if pending and time.monotonic() - connected_at > 30:
                        raise TimeoutError(f'{len(pending)} subscription acknowledgements missing')
                    if not pending and expected_reconnect:
                        self.recoveries.append({'socket': index, 'generation': generation,
                                                'resubscribed_at': time.time()})
                        expected_reconnect = False
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, TimeoutError, ConnectionError, ValueError, TypeError, RuntimeError) as exc:
                is_injected = index in self.injected
                self.injected.discard(index)
                expected_reconnect = is_injected
                self.errors.append({'socket': index, 'at': time.time(), 'expected_injection': is_injected,
                                    'error': f'{type(exc).__name__}: {exc}'})
                if not is_injected:
                    unexpected += 1
                if isinstance(exc, RuntimeError) and 'subscription error' in str(exc):
                    self.stop.set()
                    break
                await asyncio.sleep(min(2 ** unexpected, 10) + random.uniform(0, .2))
            finally:
                self.sockets.pop(index, None)
                if ws is not None:
                    await ws.disconnect()
            generation += 1

    async def run(self):
        self.output.mkdir(parents=True, exist_ok=False)
        started_cpu = time.process_time()
        self.started = time.time()
        started_mono = time.monotonic()
        steady_mono = None
        tasks = []
        forced_at = None
        next_progress = time.monotonic()
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), trust_env=False) as http:
                for _ in range(5):
                    self.clock_samples.append(await clock_sample(http))
                    await asyncio.sleep(.3)
                self.offset = min(self.clock_samples, key=lambda v: v['rtt_seconds'])['offset_seconds']
                with (self.output / 'confirmed-bars.jsonl').open('w') as self.receipts:
                    tasks = [asyncio.create_task(self.socket(i, group)) for i, group in enumerate(self.groups)]
                    while time.monotonic() - started_mono < min(600, self.seconds + 120) and not self.stop.is_set():
                        tick = time.monotonic()
                        await asyncio.sleep(.25)
                        self.loop_lags.append(max(0, time.monotonic() - tick - .25))
                        if len(self.acks) == len(self.pairs) and self.steady_at is None:
                            self.steady_at = time.time()
                            steady_mono = time.monotonic()
                        elapsed = 0 if steady_mono is None else time.monotonic() - steady_mono
                        if self.steady_at and elapsed > 120 and forced_at is None and 0 in self.sockets:
                            forced_at = time.time()
                            self.injected.add(0)
                            await self.sockets[0].disconnect()
                        if self.steady_at and elapsed >= self.seconds:
                            break
                        if time.monotonic() >= next_progress:
                            print(json.dumps({'elapsed': time.time() - self.started, 'acks': len(self.acks),
                                              'pairs_with_updates': len(self.first), 'pairs_with_closed_bars': len(self.latest),
                                              'bars': len(self.bars), 'errors': len(self.errors)}), flush=True)
                            next_progress = time.monotonic() + 30
                    self.capture_end = time.time()
                    self.stop.set()
                    self.receipts = None
                try:
                    self.clock_samples.append(await clock_sample(http))
                except (aiohttp.ClientError, TimeoutError, ValueError, RuntimeError) as exc:
                    self.errors.append({'phase': 'final_clock', 'error': str(exc), 'expected_injection': False})
        except Exception as exc:
            self.errors.append({'phase': 'setup_or_capture', 'error': f'{type(exc).__name__}: {exc}',
                                'expected_injection': False})
        finally:
            self.capture_end = self.capture_end or time.time()
            self.stop.set()
            for task in tasks:
                task.cancel()
            outcomes = await asyncio.gather(*tasks, return_exceptions=True)
            for value in outcomes:
                if isinstance(value, Exception):
                    self.errors.append({'phase': 'socket_task', 'error': str(value), 'expected_injection': False})
            await next(iter(self.parsers.values()))._api_factory.close()
        end = self.capture_end
        self.bars = {key: value for key, value in self.bars.items() if value['received_at'] <= end}
        minutes = completed_minutes((self.steady_at or end) + self.offset, end + self.offset)
        coverage = [{'open_time': t, 'received': sum((p, t) in self.bars for p in self.pairs),
                     'missing': [p for p in self.pairs if (p, t) not in self.bars]} for t in minutes]
        selected = [v for (pair, stamp), v in self.bars.items() if stamp in minutes]
        clock_best = min(self.clock_samples[:5], key=lambda v: v['rtt_seconds']) if self.clock_samples else None
        result = {'scope': 'bounded native-assistant multiplex transport diagnostic; not production collector',
                  'pairs': self.pairs, 'pair_count': len(self.pairs), 'socket_count': len(self.groups),
                  'started_at': self.started, 'steady_at': self.steady_at, 'finished_at': end,
                  'steady_seconds': None if self.steady_at is None else end - self.steady_at,
                  'acknowledged': sorted(self.acks), 'no_updates': sorted(set(self.pairs) - self.first.keys()),
                  'no_confirmed_bars': sorted(set(self.pairs) - self.latest.keys()), 'coverage': coverage,
                  'close_to_receipt_seconds': quantiles([v['close_delay_seconds'] for v in selected]),
                  'ping_rtt_seconds': quantiles(self.ping_rtts), 'clock_samples': self.clock_samples,
                  'clock_offset_used_seconds': self.offset,
                  'clock_uncertainty_seconds': clock_best['rtt_seconds'] / 2 if clock_best else None,
                  'parse_seconds': quantiles(self.parse_durations), 'event_loop_delay_seconds': quantiles(self.loop_lags),
                  'first_update_seconds': quantiles([v - self.started for v in self.first.values()]),
                  'first_update_at_by_pair': self.first, 'first_confirmed_at_by_pair': self.first_confirmed,
                  'messages': sum(self.messages.values()), 'duplicate_confirmations': self.duplicates,
                  'corrections': self.corrections, 'connections': self.connections, 'errors': self.errors,
                  'forced_disconnect_at': forced_at, 'recoveries': self.recoveries,
                  'cpu_seconds': time.process_time() - started_cpu,
                  'peak_rss_bytes_darwin': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                  'credentials_used': False, 'execution_connectors_created': False}
        result['complete_coverage'] = bool(coverage) and all(v['received'] == len(self.pairs) for v in coverage)
        recovered_bars = [v for v in self.bars.values() if v['generation'] > 0]
        result['post_reconnect_confirmed_pairs'] = sorted({v['pair'] for v in recovered_bars})
        result['passed'] = (result['complete_coverage'] and len(self.acks) == len(self.pairs)
                            and not result['no_updates'] and len(coverage) >= 3 and len(self.recoveries) == 1
                            and len(result['post_reconnect_confirmed_pairs']) == len(self.groups[0])
                            and not any(not e['expected_injection'] for e in self.errors))
        (self.output / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({k: result[k] for k in ('passed', 'pair_count', 'complete_coverage',
                                                 'close_to_receipt_seconds', 'ping_rtt_seconds', 'errors')}), flush=True)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--universe', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=300)
    args = parser.parse_args()
    if not 240 <= args.seconds <= 480:
        parser.error('seconds must be in 240..480 (hard wall time cap 600s)')
    pairs = json.loads(args.universe.read_text())['pairs']
    result = asyncio.run(Benchmark(pairs, args.output, seconds=args.seconds).run())
    raise SystemExit(0 if result['passed'] else 1)


if __name__ == '__main__':
    main()
