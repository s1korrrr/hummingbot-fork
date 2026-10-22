"""T7 cached market-context client: asynchronous refresh, synchronous bounded selection, no authority."""
import asyncio
import json
import os
import socket
import stat

import pytest

from hummingbot.data_feed import market_context_client as mcc
from hummingbot.strategy_v2.market_context import contracts as c

M = 60_000
T = 1_800_000_000_000 - 1_800_000_000_000 % M
SHA = '9' * 64
STREAM = 'okx-usdc-basket5'


def snapshot(epoch='e1', sequence=1, cutoff=T):
    feature = c.NamedFeature(name='market_return_eq_h1', value=0.001, unit='return_fraction', horizon_minutes=1,
                             status='VALID', reasons=(), valid_count=5, expected_count=5, valid_weight_fraction=1.0,
                             model_id=None, input_available_at_ms=cutoff + 500)
    content = c.SnapshotContent(
        venue='okx', numeraire='USDC', cutoff_ms=cutoff, max_input_available_at_ms=cutoff + 500,
        provenance=c.Provenance(code_digest=SHA, config_digest=SHA, universe_hash=SHA, input_manifest_digest=SHA,
                                model_digest=SHA, serializer_version=c.SERIALIZER_VERSION, artifact_refs=()),
        coverage=c.Coverage(expected_count=5, valid_count=5, valid_weight_fraction=1.0, missing=()),
        status='READY', reasons=(), market=(feature,), assets=())
    return c.seal_snapshot(content, stream_id=STREAM, epoch=epoch, sequence=sequence, source_kind='observed',
                           available_at_ms=cutoff + 3_000, expires_at_ms=cutoff + 65_000, supersedes=None)


def status(value, generation=1):
    return c.ContextStreamStatus(stream_id=STREAM, epoch=value.epoch, lease_generation=generation,
                                 sequence=value.sequence, snapshot_id=value.snapshot_id, status=value.status,
                                 available_at_ms=value.available_at_ms, expires_at_ms=value.expires_at_ms,
                                 authority='observation_only').model_dump_json().encode()


def note(value):
    return {'stream_id': value.stream_id, 'epoch': value.epoch, 'sequence': value.sequence,
            'snapshot_id': value.snapshot_id, 'available_at_ms': value.available_at_ms,
            'expires_at_ms': value.expires_at_ms, 'locator': f'/screener/market-context/v1/snapshots/{value.snapshot_id}'}


class FakeTransport:
    def __init__(self):
        self.snapshots, self.latest_value, self.status_value, self.calls = {}, None, None, []

    def put(self, value, *, latest=True, generation=1):
        raw = c.serialize_snapshot(value)
        self.snapshots[value.snapshot_id] = raw
        if latest:
            self.latest_value = raw
            self.status_value = status(value, generation)
        return raw

    async def status(self):
        self.calls.append('status')
        return self.status_value

    async def latest(self):
        self.calls.append('latest')
        return self.latest_value

    async def snapshot(self, snapshot_id):
        self.calls.append(f'snapshot:{snapshot_id[:6]}')
        return self.snapshots[snapshot_id]


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


def requirement(**changes):
    values = dict(consumer='execution', schema_major=1, stream_id=STREAM, venue='okx', numeraire='USDC',
                  config_digest=SHA, universe_hash=SHA, max_age_ms=120_000, min_count_coverage='0.95',
                  min_weight_coverage='0.95', mandatory_features=('market_return_eq_h1',),
                  allowed_source_kinds=('observed',))
    values.update(changes)
    return c.ContextRequirement(**values)


def clock_view(decision):
    return c.ClockView(decision_ms=decision, receipt_ms=None)


def client(transport, clock):
    return mcc.MarketContextClient(transport, stream_id=STREAM, clock_ms=clock)


def test_select_performs_no_network_io(monkeypatch):
    transport, clock = FakeTransport(), Clock(T + 3_200)
    first = snapshot()
    transport.put(first)
    reader = client(transport, clock)
    assert reader.select(requirement(), clock_view(T + 3_300)).reasons == ('CONTEXT_UNAVAILABLE',)
    asyncio.run(reader.refresh())
    calls = list(transport.calls)

    def forbidden(*args, **kwargs):
        raise AssertionError('select attempted network I/O')

    monkeypatch.setattr(socket, 'socket', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    selection = reader.select(requirement(), clock_view(T + 3_300))
    assert selection.accepted and selection.snapshot == first
    assert selection.effective_available_at_ms == T + 3_200  # consumer receipt is later than producer availability
    assert transport.calls == calls
    assert reader.select(requirement(), clock_view(T + 3_100)).reasons == ('CONTEXT_FUTURE',)


def test_epoch_restart_requires_handshake():
    transport, clock = FakeTransport(), Clock(T + 3_200)
    old = snapshot('e1', 50)
    transport.put(old)
    reader = client(transport, clock)
    asyncio.run(reader.refresh())
    assert transport.calls == ['latest', 'status']  # even the first epoch is confirmed against the canonical store
    restarted = snapshot('e2', 1, T + M)
    transport.latest_value = transport.put(restarted, latest=False)
    transport.status_value = status(old)  # the store still reports the old epoch: no handshake yet
    clock.now = T + M + 3_200
    asyncio.run(reader.refresh())
    assert reader.current.snapshot.snapshot_id == old.snapshot_id
    assert reader.last_reasons == ('EPOCH_UNCONFIRMED',)
    transport.status_value = status(restarted, generation=2)
    asyncio.run(reader.refresh())
    assert reader.current.snapshot.snapshot_id == restarted.snapshot_id  # sequence 1 < 50 across epochs is fine
    stale = snapshot('e1', 51, T + 2 * M)  # a fenced former writer's later sequence never returns
    transport.latest_value = transport.put(stale, latest=False)
    transport.status_value = status(stale, generation=1)
    asyncio.run(reader.refresh())
    assert reader.current.snapshot.snapshot_id == restarted.snapshot_id
    assert reader.last_reasons == ('EPOCH_UNCONFIRMED',)


def test_out_of_order_notification_keeps_newest():
    transport, clock = FakeTransport(), Clock(T + 3_200)
    one, two, three = snapshot('e1', 1), snapshot('e1', 2, T + M), snapshot('e1', 3, T + 2 * M)
    transport.put(one)
    reader = client(transport, clock)
    asyncio.run(reader.refresh())
    transport.put(two, latest=False)
    transport.put(three)
    clock.now = T + 2 * M + 3_200
    reader.on_notification(note(three), received_at_ms=clock.now)
    reader.on_notification(note(two), received_at_ms=clock.now)
    reader.on_notification(note(three), received_at_ms=clock.now)  # duplicate
    asyncio.run(reader.refresh())
    assert reader.current.snapshot.sequence == 3
    reader.on_notification(note(two), received_at_ms=clock.now)
    asyncio.run(reader.refresh())
    assert reader.current.snapshot.sequence == 3
    reader.on_notification({**note(two), 'stream_id': 'other-stream'}, received_at_ms=clock.now)
    assert reader.counters['foreign_notifications'] == 1
    with pytest.raises(ValueError):
        reader.on_notification({**note(two), 'capital': 1}, received_at_ms=clock.now)
    for sequence in range(mcc.MAX_PENDING_NOTIFICATIONS + 5):  # bounded queue; overflow forces a canonical refetch
        reader.on_notification({**note(two), 'sequence': 10 + sequence}, received_at_ms=clock.now)
    assert len(reader._pending) == mcc.MAX_PENDING_NOTIFICATIONS and reader.counters['notification_overflow'] == 5


def test_gap_refetch_validates_hash():
    transport, clock = FakeTransport(), Clock(T + 3_200)
    one, five = snapshot('e1', 1), snapshot('e1', 5, T + M)
    transport.put(one)
    reader = client(transport, clock)
    asyncio.run(reader.refresh())
    clock.now = T + M + 3_200
    raw = json.loads(transport.put(five))
    raw['market'][0]['value'] = 0.5  # tampered payload keeps the claimed identity
    transport.snapshots[five.snapshot_id] = json.dumps(raw).encode()
    reader.on_notification(note(five), received_at_ms=clock.now)  # sequence gap 1 -> 5: fetch the canonical id
    asyncio.run(reader.refresh())
    assert f'snapshot:{five.snapshot_id[:6]}' in transport.calls
    assert reader.current.snapshot.sequence == 1 and reader.last_reasons == ('SNAPSHOT_HASH_MISMATCH',)
    other = snapshot('e1', 5, T + 2 * M)
    transport.snapshots[five.snapshot_id] = c.serialize_snapshot(other)  # valid bytes for another id
    reader.on_notification(note(five), received_at_ms=clock.now)
    asyncio.run(reader.refresh())
    assert reader.current.snapshot.sequence == 1 and reader.last_reasons == ('SNAPSHOT_HASH_MISMATCH',)
    transport.snapshots[five.snapshot_id] = c.serialize_snapshot(five)
    reader.on_notification(note(five), received_at_ms=clock.now)
    asyncio.run(reader.refresh())
    assert reader.current.snapshot.sequence == 5 and reader.last_reasons == ()


def test_expiry_not_extended_by_delivery():
    transport, clock = FakeTransport(), Clock(T + 3_200)
    first = snapshot()
    transport.put(first)
    reader = client(transport, clock)
    asyncio.run(reader.refresh())
    clock.now = T + 64_000
    asyncio.run(reader.refresh())  # the same snapshot redelivered later keeps its first receipt
    assert reader.current.receipt_ms == T + 3_200
    assert reader.select(requirement(), clock_view(T + 64_999)).accepted
    assert reader.select(requirement(), clock_view(T + 65_000)).reasons == ('CONTEXT_EXPIRED',)
    assert reader.select(requirement(max_age_ms=10_000), clock_view(T + 10_001)).reasons == ('CONTEXT_EXPIRED',)


def test_receipt_time_applies_to_replay():
    first = snapshot()
    reader = client(FakeTransport(), Clock(0))
    reader.accept_recorded(c.serialize_snapshot(first), received_at_ms=T + 9_000, status=status(first))
    assert reader.select(requirement(), clock_view(T + 5_000)).reasons == ('CONTEXT_FUTURE',)
    accepted = reader.select(requirement(), clock_view(T + 9_000))
    assert accepted.accepted and accepted.effective_available_at_ms == T + 9_000
    with pytest.raises(c.ContextError) as error:  # replay cannot skip the epoch handshake either
        reader.accept_recorded(c.serialize_snapshot(snapshot('e9', 1, T + M)), received_at_ms=T + M + 4_000,
                               status=status(first))
    assert error.value.reasons == ('EPOCH_UNCONFIRMED',)


def test_background_refresh_backs_off_and_cancels():
    class Failing(FakeTransport):
        async def latest(self):
            self.calls.append('latest')
            raise OSError('connection refused')

    async def scenario():
        transport = Failing()
        reader = mcc.MarketContextClient(transport, stream_id=STREAM, clock_ms=Clock(T), poll_seconds=0.01,
                                         max_backoff_seconds=0.05)
        reader.start()
        await asyncio.sleep(0.2)
        await reader.close()
        return transport, reader

    transport, reader = asyncio.run(scenario())
    assert reader.task is None and reader.counters['refresh_failures'] >= 2
    assert reader.select(requirement(), clock_view(T)).reasons == ('CONTEXT_UNAVAILABLE',)
    assert len(transport.calls) < 20  # failures back off instead of spinning


def test_http_transport_is_bounded_and_credentials_private(tmp_path):
    with pytest.raises(ValueError):
        mcc.HttpMarketContextTransport('http://example.com', str(tmp_path / 'creds'))
    creds = tmp_path / 'creds'
    creds.write_text('reader:secret-password\n')
    os.chmod(creds, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP)
    transport = mcc.HttpMarketContextTransport('http://127.0.0.1:8000', str(creds))
    with pytest.raises(ValueError):  # group-readable credentials are refused when first used
        transport.authorization()
    os.chmod(creds, stat.S_IRUSR | stat.S_IWUSR)
    assert transport.authorization().startswith('Basic ')
    assert not {'api_key', 'secret', 'connector'} & set(vars(mcc.MarketContextClient(
        transport, stream_id=STREAM, clock_ms=Clock(0))))
