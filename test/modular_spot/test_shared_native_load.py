"""Local native OKX 1m fixture. Public OKX is not contacted."""
import importlib.util
import json
import os
import resource
import time
from pathlib import Path

import pytest

if importlib.util.find_spec('hummingbot') is None:
    pytest.fail('native Hummingbot package/compiled runtime is unavailable')

from controllers.generic.modular_spot_core.candle_data import (
    FIRST_DELIVERY,
    FIRST_DELIVERY_LOAD,
    Dataset,
    Replica,
    SnapshotHub,
)
from controllers.generic.modular_spot_core.candle_transport import MAX_MESSAGE
from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.data_feed.candles_feed.okx_spot_candles.okx_spot_candles import OKXSpotCandles


def _spec(pair, history):
    return Dataset(connector='okx', trading_pair=pair, interval='1m', history=history)


def _fill(feed, spec, end=5940):
    feed._candles.clear()
    step = spec.seconds
    for index in range(spec.history):
        stamp = end - (spec.history - 1 - index) * step
        feed._candles.append([float(stamp), 100.0, 101.0, 99.0, 100.0, 5.0, 500.0, 0.0, 0.0, 0.0])


def local_okx_factory(spec):
    feed = CandlesFactory.get_candle(CandlesConfig(
        connector=spec.connector, trading_pair=spec.trading_pair, interval=spec.interval,
        max_records=spec.history))
    if type(feed) is not OKXSpotCandles:
        pytest.fail(f'expected OKXSpotCandles, got {type(feed)!r}')
    if feed.closed_candles_only is not True:
        pytest.fail('native OKX spot adapter is not a confirmed-candle source')
    feed.start = lambda: None
    feed.stop = lambda: None
    _fill(feed, spec)
    return feed


def test_native_okx_spot_1m_local_fixture_bounds():
    history = FIRST_DELIVERY_LOAD['history']
    specs = (_spec('BTC-USDC', history), _spec('ETH-USDC', history))
    assert FIRST_DELIVERY['connector'] == 'okx' and FIRST_DELIVERY['interval'] == '1m'
    cpu_before = time.process_time()
    rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started = time.perf_counter()
    hub = SnapshotHub(specs, factory=local_okx_factory, epoch='local-okx-fixture')
    hub.start()
    hub.capture(now=6000.0)
    capture_s = time.perf_counter() - started
    cpu_s = time.process_time() - cpu_before
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    feeds = list(hub.feeds.values())
    assert len(feeds) == FIRST_DELIVERY_LOAD['feeds']
    assert all(type(feed) is OKXSpotCandles for feed in feeds)
    rows = []
    bytes_out = 0
    lags = []
    backlogs = []
    for spec in specs:
        snap = hub.latest(spec.key)
        assert snap.dataset.interval == '1m'
        assert snap.dataset.connector == 'okx'
        assert len(snap.rows) == history
        body = json.dumps(snap.model_dump(mode='json'), separators=(',', ':'), allow_nan=False).encode()
        bytes_out += len(body)
        close = snap.rows[-1].open_time + snap.dataset.seconds
        lags.append(6000.0 - close)
        backlogs.append(spec.history - len(snap.rows))
        rows.append(len(snap.rows))
    small = Replica((_spec('BTC-USDC', 60),))
    large = Replica((_spec('BTC-USDC', history),))
    small.begin_session()
    large.begin_session()
    shared = hub.latest(specs[0].key)
    small.accept(shared, now=6000.0)
    large.accept(shared, now=6000.0)
    assert len(small.frame(specs[0].key, now=6000.0, max_age=120)) == 60
    assert len(large.frame(specs[0].key, now=6000.0, max_age=120)) == history
    small.disconnect()
    assert large.connected
    hub.stop()
    hub.start()
    hub.capture(now=6000.0)
    recovered = hub.latest(specs[0].key)
    hub.stop()
    measured = {
        'feed_count': len(feeds),
        'warmup_rows': min(rows),
        'capture_seconds': capture_s,
        'cpu_seconds': cpu_s,
        'rss': rss,
        'rss_delta': rss - rss_before,
        'rss_unit': 'darwin-ru_maxrss-bytes',
        'response_bytes': bytes_out,
        'candle_lag_seconds': max(lags),
        'backlog': max(backlogs),
        'reconnect_rows': len(recovered.rows),
        'consumers': FIRST_DELIVERY_LOAD['consumers'],
        'history': history,
        'public_okx_soak': False,
        'storage': 'bounded recovery',
    }
    evidence = os.environ.get('EVIDENCE_DIR')
    if evidence:
        Path(evidence).mkdir(parents=True, exist_ok=True)
        Path(evidence, 'm3-native-load.json').write_text(json.dumps(measured, indent=2) + '\n')
    assert measured['feed_count'] == FIRST_DELIVERY_LOAD['feeds']
    assert measured['warmup_rows'] == history
    assert measured['capture_seconds'] < 2.0
    assert measured['cpu_seconds'] >= 0
    assert measured['response_bytes'] < MAX_MESSAGE
    assert measured['candle_lag_seconds'] == 0
    assert measured['backlog'] == 0
    assert measured['reconnect_rows'] == history
    assert measured['rss'] > 0
