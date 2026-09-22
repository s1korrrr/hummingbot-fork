from decimal import Decimal as D

import pandas as pd
import pytest

from controllers.generic.modular_spot_core.candle_data import (
    Candle,
    CandleSnapshot,
    Dataset,
    Replica,
    SnapshotHub,
    reject_execution_database,
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


def test_forming_bar_same_open_time_does_not_revoke_entry_ready():
    replica = Replica((dataset(),))
    replica.begin_session()
    replica.accept(snapshot(), now=6000)
    seed = replica.seed_bars[dataset().key]
    assert not replica.entry_ready(dataset().key)
    replica.accept(snapshot(seq=2, at=6060, candles=rows(end=6000)), now=6060)
    assert replica.entry_ready(dataset().key)
    assert replica.seed_bars[dataset().key] == seed
    forming = snapshot(seq=3, at=6060, candles=rows(end=6000, price='101'))
    replica.accept(forming, now=6060)
    assert replica.entry_ready(dataset().key)
    assert replica.seed_bars[dataset().key] == seed
    frame = replica.frame(dataset().key, now=6060, max_age=120)
    assert frame.close.iloc[-1] == 101


def test_first_observation_seeds_and_the_next_closed_bar_is_the_entry():
    replica = Replica((dataset(),))
    replica.begin_session()
    replica.accept(snapshot(), now=6000)
    assert replica.seed_bars[dataset().key] == 5940
    assert not replica.entry_ready(dataset().key)
    assert len(replica.frame(dataset().key, now=6000, max_age=120)) == 60
    replica.accept(snapshot(seq=2, at=6060, candles=rows(end=6000)), now=6060)
    assert replica.entry_ready(dataset().key)
    assert replica.seed_bars[dataset().key] == 5940


def test_reconnect_reseed_is_false_on_the_seed_bar_and_true_on_the_next_closed_bar():
    replica = Replica((dataset(),))
    replica.begin_session()
    replica.accept(snapshot(), now=6000)
    replica.accept(snapshot(seq=2, at=6060, candles=rows(end=6000)), now=6060)
    assert replica.entry_ready(dataset().key)
    replica.begin_session()
    replica.accept(snapshot(epoch='boot-two', seq=1, at=6120, candles=rows(end=6060)), now=6120)
    assert not replica.entry_ready(dataset().key)
    rsi_frame = replica.frame(dataset().key, now=6120, max_age=120)
    assert rsi_frame.close.iloc[-1] == 100
    replica.accept(snapshot(epoch='boot-two', seq=2, at=6180, candles=rows(end=6120)), now=6180)
    assert replica.entry_ready(dataset().key)
    replica.accept(snapshot(epoch='boot-two', seq=3, at=6180, candles=rows(end=6120, price='101')), now=6180)
    assert replica.entry_ready(dataset().key)
    assert replica.frame(dataset().key, now=6180, max_age=120).close.iloc[-1] == 101


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


def test_schema_version_requires_integer_not_boolean():
    with pytest.raises(ValueError):
        CandleSnapshot(**dict(snapshot().model_dump(), schema_version=True))


def test_identical_base_and_quote_are_not_a_dataset():
    with pytest.raises(ValueError):
        Dataset(connector='okx', trading_pair='BTC-BTC', interval='1m')


def test_native_dataset_identity_is_okx_spot_trade_utc_and_excludes_foreign_venues():
    spec = dataset()
    payload = spec.model_dump()
    assert payload['market_type'] == 'spot'
    assert payload['source_domain'] == 'okx'
    assert payload['price_type'] == 'trade'
    assert payload['boundary'] == 'utc_epoch'
    assert spec.interval == '1m'
    with pytest.raises(ValueError):
        Dataset(connector='binance', trading_pair='BTC-USDC', interval='1m')


def test_field_absence_is_not_zero_and_unsupported_taker_fields_do_not_exist():
    row = Candle(open_time=0, open='100', high='100', low='100', close='100', volume='0')
    assert row.quote_volume is None
    assert 'taker_buy_base_volume' not in type(row).model_fields


def test_content_identity_preserves_decimal_digits():
    left = snapshot(candles=rows(price='100.1234567890123456789012345678901'))
    right = snapshot(candles=rows(price='100.1234567890123456789012345678902'))
    assert left.digest != right.digest


def test_cache_requires_declared_fields_and_evicts_failed_recompute():
    cache = FeatureCache()

    def compute(frame):
        return (frame.close.iloc[-1],)

    def boom(frame):
        raise ValueError('nan')

    cache.get(snapshot(), spec={'period': 14}, implementation='test-v1', now=6000, max_age=120,
              compute=compute, initialization='closed-bar-complete-window', required_fields=('close',))
    with pytest.raises(ValueError):
        cache.get(snapshot(), spec={'period': 14}, implementation='test-v1', now=6000, max_age=120,
                  compute=compute, required_fields=('taker_buy_base_volume',))
    with pytest.raises(ValueError):
        cache.get(snapshot(), spec={'period': 14}, implementation='test-v1', now=6000, max_age=120,
                  compute=boom, force_recompute=True)
    called = []
    cache.get(snapshot(), spec={'period': 14}, implementation='test-v1', now=6000, max_age=120,
              compute=lambda frame: called.append(1) or (2,), force_recompute=True)
    assert called == [1]


def test_changed_implementation_or_initialization_does_not_reuse_cache():
    cache = FeatureCache()
    calls = []

    def compute(frame):
        calls.append(1)
        return (1,)

    cache.get(snapshot(), spec={'period': 14}, implementation='a' * 64, now=6000, max_age=120, compute=compute)
    cache.get(snapshot(), spec={'period': 17}, implementation='a' * 64, now=6000, max_age=120, compute=compute)
    cache.get(snapshot(), spec={'period': 14}, implementation='b' * 64, now=6000, max_age=120, compute=compute)
    cache.get(snapshot(), spec={'period': 14}, implementation='a' * 64, now=6000, max_age=120, compute=compute,
              initialization='alternate-window')
    assert len(calls) == 4


def test_history_demands_share_one_dataset():
    small = Dataset(connector='okx', trading_pair='BTC-USDC', history=300)
    large = Dataset(connector='okx', trading_pair='BTC-USDC', history=1000)
    hub = SnapshotHub((small, large), factory=lambda spec: None, epoch='test')
    assert small.key == large.key
    assert len(hub.datasets) == 1
    assert hub.datasets[small.key].history == 1000
    assert not hub.started


def test_two_consumers_different_history_share_one_dataset():
    made = []

    def factory(spec):
        feed = Feed(spec)
        made.append(feed)
        return feed

    small, large = dataset(records=60), dataset(records=90)
    hub = SnapshotHub((small, large), factory=factory, epoch='test')
    hub.start()
    hub.capture(now=6000)
    value = hub.latest(small.key)
    left, right = Replica((small,)), Replica((large,))
    left.begin_session()
    right.begin_session()
    left.accept(value, now=6000)
    right.accept(value, now=6000)
    assert len(made) == 1
    assert len(left.frame(small.key, now=6000, max_age=120)) == 60
    assert len(right.frame(large.key, now=6000, max_age=120)) == 90
    left.disconnect()
    hub.capture(now=6001)
    assert right.connected and made[0].stops == 0
    assert len(hub.latest(small.key).rows) == 90
    assert len(right.frame(large.key, now=6001, max_age=120)) == 90
    hub.stop()
    assert made[0].stops == 1


def test_failed_update_retains_last_healthy_snapshot_without_renewing_freshness():
    feed = Feed(dataset())
    hub = SnapshotHub((dataset(),), factory=lambda _: feed, epoch='test')
    hub.start()
    hub.capture(now=6000)
    healthy = hub.latest(dataset().key)
    feed.candles_df.loc[feed.candles_df.index[-1], 'close'] = float('nan')
    hub.capture(now=6001)
    retained = hub.latest(dataset().key)
    assert retained.digest == healthy.digest
    assert retained.published_at == healthy.published_at
    assert retained.rows[-1].close == 100


def test_retained_snapshot_stays_available_after_stop_and_start_without_recapture():
    feed = Feed(dataset())
    hub = SnapshotHub((dataset(),), factory=lambda _: feed, epoch='test')
    hub.start()
    hub.capture(now=6000)
    healthy = hub.latest(dataset().key)
    hub.stop()
    with pytest.raises(PolicyError, match='producer stopped'):
        hub.latest(dataset().key)
    hub.start()
    retained = hub.latest(dataset().key)
    assert retained.digest == healthy.digest
    assert retained.sequence == healthy.sequence
    assert retained.published_at == healthy.published_at == 6000
    assert retained.frame(now=6000, max_age=120).close.iloc[-1] == 100
    feed.candles_df.loc[feed.candles_df.index[-1], 'close'] = float('nan')
    hub.capture(now=6001)
    after_failed_recapture = hub.latest(dataset().key)
    assert after_failed_recapture.digest == healthy.digest
    assert after_failed_recapture.published_at == 6000


def test_epoch_change_disconnects_without_replaying_historical_entry():
    replica = Replica((dataset(),))
    replica.begin_session()
    replica.accept(snapshot(epoch='boot-one'), now=6000)
    replica.accept(snapshot(epoch='boot-one', seq=2, at=6060, candles=rows(end=6000)), now=6060)
    assert replica.entry_ready(dataset().key)
    with pytest.raises(PolicyError):
        replica.accept(snapshot(epoch='boot-two', seq=3, at=6120, candles=rows(end=6060)), now=6120)
    assert not replica.connected
    assert not replica.entry_ready(dataset().key)


def test_corrected_historical_candle_does_not_rearm_or_revoke_entry():
    replica = Replica((dataset(),))
    replica.begin_session()
    replica.accept(snapshot(), now=6000)
    seed = replica.seed_bars[dataset().key]
    replica.accept(snapshot(seq=2, at=6060, candles=rows(end=6000)), now=6060)
    assert replica.entry_ready(dataset().key)
    candles = list(rows(end=6000, price='100'))
    candles[0] = candles[0].model_copy(update={'close': D('101'), 'high': D('102')})
    replica.accept(snapshot(seq=3, at=6060, candles=tuple(candles)), now=6060)
    assert replica.connected
    assert replica.entry_ready(dataset().key)
    assert replica.seed_bars[dataset().key] == seed
    assert replica.frame(dataset().key, now=6060, max_age=120).close.iloc[0] == 101


def test_lost_context_dependency_keeps_unready_and_unframed():
    replica = Replica((dataset(), dataset('ETH-USDC')))
    replica.begin_session()
    replica.accept(snapshot(), now=6000)
    assert not replica.entry_ready(dataset('ETH-USDC').key)
    with pytest.raises(PolicyError):
        replica.frame(dataset('ETH-USDC').key, now=6000, max_age=120)


def test_stale_heartbeat_then_correction_still_requires_next_bar():
    replica = Replica((dataset(),))
    replica.begin_session()
    value = snapshot()
    replica.accept(value, now=6000)
    replica.touch(dataset().key, epoch=value.epoch, sequence=1, digest=value.digest, published_at=6005, now=6005)
    replica.accept(snapshot(seq=2, at=6005, candles=rows(price='101')), now=6005)
    assert not replica.entry_ready(dataset().key)


def test_cache_hits_validate_provenance_and_transport_readiness():
    cache = FeatureCache()
    calls = []

    def compute(frame):
        calls.append(1)
        return (frame.close.iloc[-1],)

    first = cache.get(snapshot(), spec={'period': 14}, implementation='test-v1', now=6000, max_age=120,
                      compute=compute)
    second = cache.get(snapshot(), spec={'period': 14}, implementation='test-v1', now=6000, max_age=120,
                       compute=compute)
    assert first == second and len(calls) == 1
    with pytest.raises(PolicyError):
        cache.get(snapshot(), spec={'period': 14}, implementation='test-v1', now=6020, max_age=120, compute=compute)
    cache.get(snapshot(epoch='other'), spec={'period': 14}, implementation='test-v1', now=6000, max_age=120,
              compute=compute)
    assert len(calls) == 2


def test_hub_keeps_two_recent_snapshots_not_as_of_replay():
    feed = Feed(dataset())
    hub = SnapshotHub((dataset(),), factory=lambda _: feed, epoch='test')
    hub.start()
    hub.capture(now=6000)
    feed.candles_df.loc[feed.candles_df.index[-1], ['high', 'close']] = (102, 101)
    hub.capture(now=6000)
    feed.candles_df.loc[feed.candles_df.index[-1], ['high', 'close']] = (103, 102)
    hub.capture(now=6000)
    key = dataset().key
    assert hub.prior[key].rows[-1].close == 101
    assert hub.latest(key).rows[-1].close == 102
    assert set(hub.prior) == {key} and set(hub.snapshots) == {key}


def test_store_cannot_open_execution_database(tmp_path):
    import sqlite3
    path = tmp_path / 'owner.db'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE Orders (id TEXT PRIMARY KEY)')
    with pytest.raises(ValueError, match='foreign'):
        reject_execution_database(path)
    with pytest.raises(ValueError, match='foreign'):
        SnapshotHub((dataset(),), factory=Feed, epoch='test', checkpoint=path)


def test_existing_empty_file_is_not_assumed_to_be_a_new_market_store(tmp_path):
    path = tmp_path / 'possible_execution_startup.db'
    path.touch()
    with pytest.raises(ValueError, match='foreign'):
        reject_execution_database(path)
    with pytest.raises(ValueError, match='foreign'):
        SnapshotHub((dataset(),), factory=Feed, epoch='test', checkpoint=path)
