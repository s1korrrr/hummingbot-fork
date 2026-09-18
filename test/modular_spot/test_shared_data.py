from decimal import Decimal as D

import pandas as pd
import pytest

from controllers.generic.modular_spot_core.candle_data import (
    Candle, CandleSnapshot, Dataset, Replica, SnapshotHub,
)
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.feature_cache import FeatureCache


def dataset(pair='BTC-USDC', records=60):
    return Dataset(connector='okx', trading_pair=pair, interval='1m', history=records)


def rows(end=5940, count=60, price='100'):
    return tuple(Candle(open_time=t, open=D(price), high=D(price) + 1, low=D(price) - 1,
                        close=D(price), volume=D('5')) for t in range(end - (count - 1) * 60, end + 1, 60))


def snapshot(*, seq=1, at=6000.0, candles=None, stream=None, epoch='boot-one'):
    return CandleSnapshot(dataset=stream or dataset(), epoch=epoch, sequence=seq,
                          published_at=at, rows=candles or rows())


def test_dataset_deduplication_does_not_include_history():
    assert dataset(records=60).key == dataset(records=1000).key
    assert dataset().key != dataset('ETH-USDC').key


def test_frame_is_a_private_copy_and_unsupported_fields_are_absent():
    snap = snapshot()
    frame = snap.frame(now=6000, max_age=120)
    frame.loc[0, 'close'] = 1
    assert snap.frame(now=6000, max_age=120).close.iloc[0] == 100
    assert 'n_trades' not in frame.columns
    assert 'quote_asset_volume' not in frame.columns


@pytest.mark.parametrize('bad', [dict(close='NaN'), dict(low='110'), dict(volume='-1'), dict(confirmed=False)])
def test_invalid_candle_rejected(bad):
    payload = rows()[0].model_dump()
    with pytest.raises(ValueError):
        Candle(**{**payload, **bad})


def test_snapshot_requires_ordered_contiguous_and_aligned_candles():
    original = rows()
    for invalid in (original[::-1], original[:-2] + original[-1:], original[:-1] + original[-2:-1]):
        with pytest.raises(ValueError):
            snapshot(candles=invalid)


@pytest.mark.parametrize('now', [5999, 6121, float('nan')])
def test_future_or_stale_snapshot_does_not_become_live(now):
    with pytest.raises(PolicyError):
        snapshot().frame(now=now, max_age=120)


def test_replica_detects_conflict_and_requires_explicit_new_session():
    replica = Replica((dataset(),))
    replica.begin_session()
    assert replica.accept(snapshot(), now=6000)
    assert not replica.accept(snapshot(), now=6000)
    assert not replica.entry_ready(dataset().key)
    newer = snapshot(seq=2, at=6060, candles=rows(end=6000))
    assert replica.accept(newer, now=6060)
    assert replica.entry_ready(dataset().key)
    with pytest.raises(PolicyError):
        replica.accept(snapshot(seq=2, at=6060, candles=rows(end=6000, price='101')), now=6060)
    assert not replica.entry_ready(dataset().key)
    replica.begin_session()
    assert replica.accept(snapshot(epoch='boot-two', seq=1, at=6120, candles=rows(end=6060)), now=6120)
    assert not replica.entry_ready(dataset().key)


def test_snapshot_jumps_are_full_resynchronization_not_historical_signals():
    replica = Replica((dataset(),))
    replica.begin_session()
    replica.accept(snapshot(), now=6000)
    replica.accept(snapshot(seq=5, at=6300, candles=rows(end=6240)), now=6300)
    assert not replica.entry_ready(dataset().key)
    replica.accept(snapshot(seq=6, at=6360, candles=rows(end=6300)), now=6360)
    assert replica.entry_ready(dataset().key)
    replica.disconnect()
    assert not replica.entry_ready(dataset().key)
    with pytest.raises(PolicyError):
        replica.frame(dataset().key, now=6360, max_age=120)


def test_correction_recomputes_but_does_not_advance_signal_clock():
    replica = Replica((dataset(),))
    replica.begin_session()
    replica.accept(snapshot(), now=6000)
    replica.accept(snapshot(seq=2, candles=rows(price='101')), now=6000)
    assert not replica.entry_ready(dataset().key)


def test_replica_rejects_foreign_source_and_future_publication():
    replica = Replica((dataset(),))
    replica.begin_session()
    with pytest.raises(PolicyError):
        replica.accept(snapshot(stream=dataset('ETH-USDC')), now=6000)
    replica.begin_session()
    with pytest.raises(PolicyError):
        replica.accept(snapshot(at=6001), now=6000)


def test_cache_checks_age_on_hits_and_never_shares_different_specs():
    calls = []
    cache = FeatureCache(capacity=2)
    def compute(frame):
        calls.append(len(frame))
        return (frame.close.iloc[-1],)
    one = cache.get(snapshot(), spec={'period': 14}, implementation='test-v1', now=6000, max_age=120, compute=compute)
    assert one == cache.get(snapshot(), spec={'period': 14}, implementation='test-v1', now=6000, max_age=120, compute=compute)
    assert len(calls) == 1
    cache.get(snapshot(), spec={'period': 17}, implementation='test-v1', now=6000, max_age=120, compute=compute)
    assert len(calls) == 2
    with pytest.raises(PolicyError):
        cache.get(snapshot(), spec={'period': 14}, implementation='test-v1', now=7000, max_age=120, compute=compute)


class Feed:
    closed_candles_only = True
    def __init__(self, spec):
        self.spec = spec
        self.starts = self.stops = 0
        self.candles_df = pd.DataFrame([{'timestamp': r.open_time, 'open': r.open, 'high': r.high,
                                      'low': r.low, 'close': r.close, 'volume': r.volume} for r in rows(count=spec.history)])
    def start(self):
        self.starts += 1
    def stop(self):
        self.stops += 1


def test_one_upstream_feed_uses_maximum_admitted_history():
    made = []
    def factory(spec):
        feed = Feed(spec)
        made.append(feed)
        return feed
    hub = SnapshotHub((dataset(records=60), dataset(records=90)), factory=factory, epoch='test')
    assert not made
    hub.start()
    hub.start()
    assert len(made) == 1 and made[0].starts == 1 and made[0].spec.history == 90
    hub.capture(now=6000)
    assert len(hub.latest(dataset().key).rows) == 90
    hub.stop()
    assert made[0].stops == 1


def test_unconfirmed_native_feed_cannot_be_promoted_by_elapsed_wall_clock():
    feed = Feed(dataset())
    feed.closed_candles_only = False
    hub = SnapshotHub((dataset(),), factory=lambda _: feed, epoch='test')
    hub.start()
    hub.capture(now=6000)
    with pytest.raises(PolicyError):
        hub.latest(dataset().key)
    assert 'unconfirmed' in hub.errors[dataset().key]


def test_hub_retains_sequence_on_unchanged_rows_but_health_is_current():
    hub = SnapshotHub((dataset(),), factory=Feed, epoch='test')
    hub.start()
    hub.capture(now=6000)
    first = hub.latest(dataset().key)
    hub.capture(now=6001)
    assert hub.latest(dataset().key).sequence == first.sequence
    hub.capture(now=7000)
    with pytest.raises(PolicyError):
        hub.latest(dataset().key)
