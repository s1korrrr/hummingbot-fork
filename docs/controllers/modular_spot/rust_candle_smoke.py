"""Explicit public read-only Rust -> native DataPool smoke; never execution connectors."""
import argparse
import asyncio
import hashlib
import json
import math
import secrets
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import aiohttp

from controllers.generic.modular_spot_core.candle_data import Dataset
from controllers.generic.modular_spot_core.data_reader import DataPolicy, DataPool
from controllers.generic.modular_spot_core.domain import DataUnavailable

ROOT = Path(__file__).resolve().parents[3]


def quantiles(values):
    values = sorted(values)
    if not values:
        return {'count': 0, 'p50': None, 'p95': None, 'p99': None, 'max': None}
    return {'count': len(values), **{f'p{q}': values[math.ceil(q / 100 * len(values)) - 1]
                                     for q in (50, 95, 99)}, 'max': values[-1]}


def completed_minutes(start, end, grace=10):
    return list(range(math.ceil(start / 60) * 60,
                      math.floor((end - grace) / 60) * 60, 60))


def identities():
    paths = [*ROOT.glob('services/okx-candle-service/src/**/*.rs'),
             *ROOT.glob('services/okx-candle-service/Cargo.*'),
             *ROOT.glob('controllers/generic/modular_spot_core/*.py'),
             ROOT / 'controllers/generic/modular_spot.py', Path(__file__)]
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


async def exchange_clock(session):
    start, before = time.monotonic(), time.time()
    async with session.get('https://www.okx.com/api/v5/public/time', allow_redirects=False) as response:
        response.raise_for_status()
        body = await response.json()
    after = time.time()
    if body.get('code') != '0':
        raise ValueError('public exchange clock failed')
    return {'offset': int(body['data'][0]['ts']) / 1000 - (before + after) / 2,
            'rtt': time.monotonic() - start, 'sampled_at': after}


class Smoke:
    def __init__(self, args):
        self.args = args
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.pairs = json.loads(args.universe.read_text())['pairs']
        if not self.pairs or len(set(self.pairs)) != len(self.pairs):
            raise ValueError('nonempty unique universe required')
        self.producer_universe = self.output / 'producer-universe.json'
        self.producer_universe.write_text(json.dumps({'pairs': self.pairs}, indent=2) + '\n')
        self.cache_dir = self.output / 'candle-cache' if getattr(args, 'cache', False) else None
        self.specs = {p: Dataset(connector='okx', trading_pair=p, history=args.history) for p in self.pairs}
        self.proc = None
        self.pools = []
        self.seen = [set(), set()]
        self.timely = [set(), set()]
        self.delays = [[], []]
        self.close_delays = [[], []]
        self.frame_delays = [[], []]
        self.read_costs = []
        self.events, self.errors, self.health = [], [], []
        self.loop_lags, self.memory_kib = [], []
        self.clocks = []
        self.offset = 0
        self.started, self.steady, self.finished = time.time(), None, None
        self.started_mono, self.steady_mono, self.finished_mono = time.monotonic(), None, None
        self.outage_start_mono = None
        self.outages = [[], []]
        self.consumer_recovery_start = None
        self.consumer_recovery_mono = None
        self.consumer_outage = None
        self.consumer_recovered = False
        self.consumer_restarted = False
        self.producer_restarted = False
        self.outage_start = None
        self.recovered_after_restart = False
        self.frames_checked = 0
        self.last_client_errors = [None, None]
        self.final_ready = [0, 0]

    async def launch(self, token_path, port):
        command = [str(self.args.binary.resolve()), '--universe', str(self.producer_universe),
                   '--token-file', str(token_path), '--listen', f'127.0.0.1:{port}',
                   '--history', str(self.args.history)]
        if self.cache_dir is not None:
            command.extend(['--cache-dir', str(self.cache_dir)])
        self.proc = subprocess.Popen(command, cwd=ROOT, stdout=self.service_log, stderr=subprocess.STDOUT)
        self.events.append({'type': 'producer_started', 'at': time.time(), 'pid': self.proc.pid})

    async def stop_producer(self):
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            deadline = time.monotonic() + 10
            while self.proc.poll() is None and time.monotonic() < deadline:
                await asyncio.sleep(.05)
            if self.proc.poll() is None:
                self.proc.kill()
                self.errors.append({'type': 'producer_shutdown_timeout'})
        self.proc.wait()
        self.events.append({'type': 'producer_stopped', 'at': time.time(), 'returncode': self.proc.returncode})
        self.proc = None

    def ready(self, pool):
        if pool.client is None or not pool.client.replica.connected:
            return 0
        now = time.time()
        return sum(len(s.rows) >= self.args.history and 0 <= now - s.published_at < 10
                   and 0 <= now - s.rows[-1].open_time - 60 < 120
                   for s in pool.client.replica.snapshots.values())

    async def collect(self, stream):
        for index, pool in enumerate(self.pools):
            client = pool.client
            if client is None:
                continue
            error = client.error
            if error != self.last_client_errors[index]:
                if error is not None:
                    fault = {'type': 'reader_error', 'reader': index, 'at': time.time(), 'error': error}
                    self.events.append(fault)
                    starting = self.steady is None and error == 'ClientConnectorError'
                    expected_restart = (not self.recovered_after_restart and self.outage_start is not None
                                        and time.monotonic() - self.outage_start_mono < 120
                                        and error in {'ClientConnectorError', 'ClientOSError',
                                                      'ServerDisconnectedError', 'DataUnavailable'})
                    if not starting and not expected_restart:
                        self.errors.append({**fault, 'type': 'unexpected_reader_failure'})
                self.last_client_errors[index] = error
            count = 0
            while client.update_receipts:
                receipt = client.update_receipts.popleft()
                stream.write(json.dumps({'reader': index, **receipt}) + '\n')
                pair = receipt['pair']
                spec = self.specs[pair]
                current = client.replica.snapshots.get(spec.key)
                if current is not None:
                    self.seen[index].update((pair, row.open_time) for row in current.rows)
                if receipt['type'] == 'candle' and self.steady is not None:
                    self.timely[index].add((pair, receipt['open_time']))
                    self.delays[index].append(receipt['adapter_at'] - receipt['received_at'])
                    self.close_delays[index].append(receipt['adapter_at'] + self.offset - receipt['open_time'] - 60)
                    before = time.perf_counter()
                    try:
                        # Public native DataPool boundary used by modular controllers; no controller starts.
                        snapshot = pool.snapshot(spec, now=time.time(), max_age=120)
                        assert snapshot.rows[-1].open_time >= receipt['open_time']
                        self.frames_checked += 1
                        self.frame_delays[index].append(time.time() - receipt['received_at'])
                    except DataUnavailable:
                        fault = {'type': 'native_frame_unavailable', 'pair': pair,
                                 'reader': index, 'at': time.time()}
                        self.events.append(fault)
                        if not self.during_outage(index, fault['at']):
                            self.errors.append(fault)
                    self.read_costs.append(time.perf_counter() - before)
                count += 1
                if count % 8 == 0:
                    await asyncio.sleep(0)
            if getattr(client, 'receipt_overflows', 0):
                self.errors.append({'type': 'receipt_overflow', 'reader': index,
                                    'count': client.receipt_overflows})

    def during_outage(self, reader, at):
        return any(start <= at <= (end or float('inf')) for start, end in self.outages[reader])

    async def run(self):
        with tempfile.TemporaryDirectory(prefix='rust-candle-smoke-') as temporary:
            token_path = Path(temporary).resolve() / 'token'
            token = secrets.token_urlsafe(40)
            token_path.write_text(token)
            token_path.chmod(0o600)
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                port = listener.getsockname()[1]
            origin = f'http://127.0.0.1:{port}'
            headers = {'Authorization': f'Bearer {token}'}
            policy = DataPolicy(mode='shared', transport='stream', origin=origin, token_file=str(token_path))
            self.pools = [DataPool(policy), DataPool(policy)]
            for pool in self.pools:
                pool.attach(tuple(self.specs.values()))
            self.service_log = (self.output / 'service.log').open('w')
            manifest = {'files': identities(), 'binary_sha256': hashlib.sha256(self.args.binary.read_bytes()).hexdigest(),
                        'universe_sha256': hashlib.sha256(self.args.universe.read_bytes()).hexdigest(),
                        'producer_universe_sha256': hashlib.sha256(self.producer_universe.read_bytes()).hexdigest(),
                        'duration_seconds': self.args.seconds, 'history': self.args.history,
                        'persistent_cache': self.cache_dir is not None,
                        'pair_count': len(self.pairs), 'execution_enabled': False, 'live_stack_touched': False}
            (self.output / 'source.json').write_text(json.dumps(manifest, indent=2) + '\n')
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5), trust_env=False) as session:
                    for _ in range(3):
                        self.clocks.append(await exchange_clock(session))
                    self.offset = min(self.clocks, key=lambda row: row['rtt'])['offset']
                    await self.launch(token_path, port)
                    for pool in self.pools:
                        pool.start()
                    next_progress, next_health = 0, 0
                    auth_checked = False
                    with (self.output / 'adapter-receipts.jsonl').open('w') as stream:
                        while True:
                            tick = time.monotonic()
                            await asyncio.sleep(.02)
                            self.loop_lags.append(max(0, time.monotonic() - tick - .02))
                            await self.collect(stream)
                            now, mono = time.time(), time.monotonic()
                            ready = [self.ready(pool) for pool in self.pools]
                            if self.steady is None and all(n == len(self.pairs) for n in ready):
                                self.steady, self.steady_mono = now, mono
                                self.events.append({'type': 'all_readers_ready', 'at': now})
                            elapsed = 0 if self.steady_mono is None else mono - self.steady_mono
                            if self.steady is None and mono - self.started_mono > 300:
                                raise RuntimeError(f'initial all-pair readiness timed out: {ready}')
                            if self.steady is not None and elapsed >= self.args.seconds:
                                self.finished, self.finished_mono = now, mono
                                self.clocks.append(await exchange_clock(session))
                                break
                            if self.args.seconds >= 900:
                                if elapsed >= 240 and not self.consumer_restarted:
                                    self.events.append({'type': 'consumer_restart', 'reader': 0, 'at': now})
                                    self.consumer_recovery_start = now
                                    self.consumer_recovery_mono = mono
                                    self.consumer_outage = [now, None]
                                    self.outages[0].append(self.consumer_outage)
                                    await self.pools[0].close()
                                    self.pools[0].start()
                                    self.consumer_restarted = True
                                if self.consumer_restarted and not self.consumer_recovered and self.ready(self.pools[0]) == len(self.pairs):
                                    self.consumer_recovered = True
                                    self.consumer_outage[1] = time.time()
                                    self.events.append({'type': 'consumer_recovered', 'reader': 0, 'at': time.time()})
                                if (self.consumer_restarted and not self.consumer_recovered
                                        and mono - self.consumer_recovery_mono > 30):
                                    raise RuntimeError('consumer recovery timed out')
                                if elapsed >= 450 and not self.producer_restarted:
                                    self.outage_start, self.outage_start_mono = now, mono
                                    for windows in self.outages:
                                        windows.append([now, None])
                                    self.events.append({'type': 'planned_producer_restart', 'at': now})
                                    await self.stop_producer()
                                    await asyncio.sleep(2)
                                    if any(pool.client.replica.connected for pool in self.pools):
                                        raise RuntimeError('reader remained connected after producer stopped')
                                    await self.launch(token_path, port)
                                    self.producer_restarted = True
                                if self.producer_restarted and not self.recovered_after_restart:
                                    if all(n == len(self.pairs) for n in ready):
                                        # Use fresh state, not the readiness sampled before the restart action.
                                        if all(self.ready(pool) == len(self.pairs) for pool in self.pools):
                                            self.events.append({'type': 'all_readers_recovered', 'at': time.time(),
                                                                'seconds': time.monotonic() - self.outage_start_mono})
                                            for windows in self.outages:
                                                windows[-1][1] = time.time()
                                            self.recovered_after_restart = True
                                    if mono - self.outage_start_mono > 240:
                                        raise RuntimeError('producer recovery timed out')
                            if self.proc is not None and self.proc.poll() is not None:
                                raise RuntimeError(f'producer unexpectedly exited: {self.proc.returncode}')
                            if mono >= next_health:
                                try:
                                    if not auth_checked:
                                        async with session.get(origin + '/health') as response:
                                            if response.status != 401:
                                                raise RuntimeError('health endpoint accepted missing credentials')
                                        auth_checked = True
                                    async with session.get(origin + '/health', headers=headers) as response:
                                        response.raise_for_status()
                                        health = await response.json()
                                    self.health.append({'at': now, 'body': health})
                                    if self.proc is not None:
                                        rss = subprocess.check_output(['ps', '-o', 'rss=', '-p', str(self.proc.pid)], text=True)
                                        self.memory_kib.append(int(rss.strip()))
                                except (aiohttp.ClientError, asyncio.TimeoutError, subprocess.SubprocessError):
                                    pass  # Startup/restart health absence is captured by explicit readiness deadlines.
                                next_health = mono + 5
                            if mono >= next_progress:
                                print(json.dumps({'elapsed_steady': round(elapsed, 1), 'ready': ready,
                                                  'candle_events': [len(v) for v in self.delays],
                                                  'restart_recovered': self.recovered_after_restart}), flush=True)
                                next_progress = mono + 30
            except Exception as exc:
                self.errors.append({'type': type(exc).__name__, 'error': str(exc), 'at': time.time()})
            finally:
                self.finished = self.finished or time.time()
                self.finished_mono = self.finished_mono or time.monotonic()
                self.final_ready = [self.ready(pool) for pool in self.pools]
                for pool in self.pools:
                    await pool.close()
                await self.stop_producer()
                self.service_log.close()
        return self.finish()

    def finish(self):
        minutes = completed_minutes((self.steady or self.finished) + self.offset, self.finished + self.offset)
        coverage = [[{'open_time': minute, 'available_by_end': sum((pair, minute) in seen for pair in self.pairs),
                      'incremental_deliveries': sum((pair, minute) in self.timely[index] for pair in self.pairs),
                      'missing': [pair for pair in self.pairs if (pair, minute) not in seen]}
                     for minute in minutes] for index, seen in enumerate(self.seen)]
        for index, rows in enumerate(coverage):
            for row in rows:
                close = row['open_time'] + 60 - self.offset
                row['planned_interruption'] = any(start <= close + 5 and close <= (end or self.finished)
                                                  for start, end in self.outages[index])
        incremental_passed = bool(minutes) and all(row['planned_interruption'] or row['incremental_deliveries'] == len(self.pairs)
                                                   for rows in coverage for row in rows)
        clock_step = ((self.finished - self.started) - (self.finished_mono - self.started_mono))
        if abs(clock_step) > .25:
            self.errors.append({'type': 'local_clock_step', 'seconds': clock_step})
        if any(value < 0 for reader in self.delays for value in reader):
            self.errors.append({'type': 'negative_local_delivery_delay'})
        stats = [quantiles(values) for values in self.delays]
        report = {'requested_seconds': self.args.seconds, 'observed_seconds': None if self.steady is None else self.finished_mono - self.steady_mono,
                  'pairs': self.pairs, 'pair_count': len(self.pairs), 'readers': 2, 'history': self.args.history,
                  'started_at': self.started, 'steady_at': self.steady, 'finished_at': self.finished,
                  'planned_outages': self.outages, 'clock_step_seconds': clock_step,
                  'clock_samples': self.clocks, 'clock_offset_seconds': self.offset,
                  'receipt_to_adapter_seconds': stats,
                  'close_to_adapter_seconds_including_recovery': [quantiles(values) for values in self.close_delays],
                  'receipt_to_native_read_seconds': [quantiles(values) for values in self.frame_delays],
                  'native_frame_read_seconds': quantiles(self.read_costs), 'native_frames_checked': self.frames_checked,
                  'event_loop_lag_seconds': quantiles(self.loop_lags), 'producer_rss_kib': quantiles(self.memory_kib),
                  'coverage': coverage, 'events': self.events, 'errors': self.errors,
                  'consumer_restart_exercised': self.consumer_restarted, 'producer_restart_exercised': self.producer_restarted,
                  'producer_restart_recovered': self.recovered_after_restart, 'final_ready': self.final_ready,
                  'authority': 'repository-process-only', 'execution_connectors_created': False}
        report['coverage_passed'] = bool(minutes) and all(not row['missing'] for rows in coverage for row in rows)
        report['incremental_coverage_passed'] = incremental_passed
        report['local_latency_target_passed'] = all(row['p99'] is not None and row['p99'] < .1 for row in stats)
        report['passed'] = (not self.errors and report['coverage_passed'] and report['incremental_coverage_passed']
                            and report['local_latency_target_passed']
                            and self.frames_checked > 0 and self.args.seconds >= 900
                            and report['observed_seconds'] >= 900 and self.consumer_restarted
                            and self.consumer_recovered and self.producer_restarted and self.recovered_after_restart
                            and all(count == len(self.pairs) for count in self.final_ready))
        (self.output / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
        (self.output / 'health.json').write_text(json.dumps(self.health, indent=2) + '\n')
        print(json.dumps({key: report[key] for key in ('passed', 'coverage_passed', 'local_latency_target_passed',
                                                       'receipt_to_adapter_seconds', 'errors')}), flush=True)
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--universe', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binary', type=Path, default=ROOT / 'services/okx-candle-service/target/release/okx-candle-service')
    parser.add_argument('--seconds', type=int, default=900)
    parser.add_argument('--history', type=int, default=90)
    parser.add_argument('--cache', action='store_true', help='Use a run-owned cache across producer restart')
    args = parser.parse_args()
    if not 60 <= args.seconds <= 1800:
        parser.error('bounded smoke seconds must be in60..1800; final acceptance requires900')
    result = asyncio.run(Smoke(args).run())
    raise SystemExit(0 if result['passed'] else 1)


if __name__ == '__main__':
    main()
