"""Metrics and frame validation for explicitly invoked public benchmark."""
import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / 'docs/controllers/modular_spot/public_universe_benchmark.py'
spec = importlib.util.spec_from_file_location('public_universe_benchmark', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_quantiles_and_complete_minutes_exclude_initial_replay_and_last_partial():
    assert module.quantiles([5, 1, 3]) == {'count': 3, 'p50': 3, 'p95': 5, 'p99': 5, 'max': 5}
    assert module.quantiles([])['max'] is None
    assert module.completed_minutes(121, 430) == [180, 240, 300, 360]
    assert module.completed_minutes(121, 429) == [180, 240, 300]


def test_subscription_identity_and_size_are_bounded():
    assert module.subscription(['BTC-USDC'])['args'][0]['channel'] == 'candle1m'
    for pairs in ([], ['BTC-USDC', 'BTC-USDC'], ['A' * 65000 + '-USDC']):
        with pytest.raises(ValueError):
            module.subscription(pairs)


def test_native_parser_handles_every_confirmed_row_and_excludes_provisional(tmp_path):
    bench = module.Benchmark(['BTC-USDC'], tmp_path)
    rows = [[str(t * 1000), '100', '110', '90', '101', '2', '2', '202', '1']
            for t in (1700000040, 1700000100)]
    provisional = [str(1700000160000), '100', '110', '90', '101', '2', '2', '202', '0']
    body = {'arg': {'channel': 'candle1m', 'instId': 'BTC-USDC'}, 'data': rows + [provisional]}
    bench.message(body, {'BTC-USDC'}, 0, 0)
    assert len(bench.bars) == 2
    assert bench.latest['BTC-USDC'] == 1700000100
    bench.message(body, {'BTC-USDC'}, 0, 0)
    assert bench.duplicates == 2
    assert len(bench.bars) == 2


def test_subscription_errors_foreign_data_and_invalid_confirm_fail(tmp_path):
    bench = module.Benchmark(['BTC-USDC'], tmp_path)
    with pytest.raises(RuntimeError, match='subscription error'):
        bench.message({'event': 'error', 'code': '60018', 'msg': 'invalid'}, {'BTC-USDC'}, 0, 0)
    with pytest.raises(ValueError, match='foreign'):
        bench.message({'arg': {'channel': 'candle1m', 'instId': 'ETH-USDC'}, 'data': []}, {'BTC-USDC'}, 0, 0)
    with pytest.raises(ValueError, match='confirmation'):
        bench.message({'arg': {'channel': 'candle1m', 'instId': 'BTC-USDC'},
                       'data': [['1700000040000', '100', '110', '90', '101', '2', '2', '202', '?']]},
                      {'BTC-USDC'}, 0, 0)


def test_late_frames_cannot_change_capture_evidence(tmp_path):
    bench = module.Benchmark(['BTC-USDC'], tmp_path)
    bench.capture_end = 1700000110
    bench.message({'arg': {'channel': 'candle1m', 'instId': 'BTC-USDC'},
                   'data': [['1700000040000', '100', '110', '90', '101', '2', '2', '202', '1']]},
                  {'BTC-USDC'}, 0, 0)
    assert not bench.bars and not bench.messages and not bench.first


def test_native_close_type_error_recovers_and_reconfirms(tmp_path):
    import asyncio
    from types import SimpleNamespace

    async def scenario():
        bench = module.Benchmark(['BTC-USDC'], tmp_path)
        bench.pacer = module.Pacer(interval=0)
        ack = {'event': 'subscribe', 'arg': {'channel': 'candle1m', 'instId': 'BTC-USDC'}}
        candle = {'arg': ack['arg'], 'data': [
            ['1700000040000', '100', '110', '90', '101', '2', '2', '202', '1']]}
        sockets = []

        class Assistant:
            def __init__(self):
                self.count, self.closed = 0, False
                sockets.append(self)

            async def connect(self, *args, **kwargs):
                pass

            async def send(self, request):
                pass

            async def receive(self):
                self.count += 1
                if self.count == 1:
                    return SimpleNamespace(data=ack)
                if len(sockets) == 1:
                    bench.injected.add(0)
                    raise TypeError('the JSON object must be str, bytes or bytearray, not NoneType')
                bench.stop.set()
                return SimpleNamespace(data=candle)

            async def disconnect(self):
                self.closed = True

        async def get_assistant():
            return Assistant()

        bench.parsers['BTC-USDC']._api_factory = SimpleNamespace(get_ws_assistant=get_assistant)
        await asyncio.wait_for(bench.socket(0, bench.pairs), timeout=3)
        assert len(sockets) == 2 and all(ws.closed for ws in sockets)
        assert len(bench.errors) == 1 and bench.errors[0]['expected_injection']
        assert len(bench.recoveries) == 1 and bench.recoveries[0]['generation'] == 1
        assert next(iter(bench.bars.values()))['generation'] == 1
    asyncio.run(scenario())


def test_busy_stream_cannot_suppress_pong_deadline(tmp_path, monkeypatch):
    import asyncio
    from types import SimpleNamespace

    monkeypatch.setattr(module, 'PONG_TIMEOUT', .02)

    async def scenario():
        bench = module.Benchmark(['BTC-USDC'], tmp_path)
        bench.pacer = module.Pacer(interval=0)
        attempts = []

        class Assistant:
            async def connect(self, *args, **kwargs):
                attempts.append(1)
                if len(attempts) == 2:
                    bench.stop.set()

            async def send(self, request):
                pass

            async def receive(self):
                await asyncio.sleep(0)
                return SimpleNamespace(data={'event': 'subscribe',
                                             'arg': {'channel': 'candle1m', 'instId': 'BTC-USDC'}})

            async def disconnect(self):
                pass

        async def get_assistant():
            return Assistant()

        bench.parsers['BTC-USDC']._api_factory = SimpleNamespace(get_ws_assistant=get_assistant)
        await asyncio.wait_for(bench.socket(0, bench.pairs), timeout=4)
        assert len(attempts) == 2
        assert len(bench.errors) == 1 and 'pong deadline' in bench.errors[0]['error']
        assert not bench.errors[0]['expected_injection']
    asyncio.run(scenario())


def test_unexpected_connection_failures_stop_at_retry_bound(tmp_path, monkeypatch):
    import asyncio
    from types import SimpleNamespace
    original_sleep = asyncio.sleep

    async def immediate_sleep(_):
        await original_sleep(0)

    monkeypatch.setattr(module.asyncio, 'sleep', immediate_sleep)

    async def scenario():
        bench = module.Benchmark(['BTC-USDC'], tmp_path)
        bench.pacer = module.Pacer(interval=0)

        class Assistant:
            async def connect(self, *args, **kwargs):
                raise ConnectionError('unavailable')

            async def disconnect(self):
                pass

        async def get_assistant():
            return Assistant()

        bench.parsers['BTC-USDC']._api_factory = SimpleNamespace(get_ws_assistant=get_assistant)
        await asyncio.wait_for(bench.socket(0, bench.pairs), timeout=1)
        assert len(bench.connections) == 4 and len(bench.errors) == 4
        assert all(not error['expected_injection'] for error in bench.errors)
    asyncio.run(scenario())
