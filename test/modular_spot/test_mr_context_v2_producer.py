import json
from decimal import Decimal
from pathlib import Path

import pytest

from controllers.generic.mr_context_v2_core.producer import snapshot_from_candle_cache
from controllers.generic.mr_context_v2_core.snapshot import ContextRequirementsV2, context_blockers

START = 1_800_000_000
CACHE_DIR = Path('/Users/s1kor/.local/share/rsibot-stack-v2/candles/cache-rsi')


def rows(n, *, close_start='100', step='0'):
    step_d = Decimal(step)
    start_d = Decimal(close_start)
    out = []
    for index in range(n):
        close = start_d + step_d * index
        open_time = START + index * 60
        out.append({
            'open_time': open_time, 'open': str(close), 'high': str(close), 'low': str(close),
            'close': str(close), 'volume': '1', 'quote_volume': '100', 'confirmed': True,
        })
    return out


def payload(pair, n=20, *, close_start='100', step='0', extra=None, saved_at=None):
    data = rows(n, close_start=close_start, step=step)
    if extra:
        data.extend(extra)
    last_close = START + n * 60
    body = {
        'v': 1, 'source': 'okx-public-spot', 'pair': pair, 'interval': '1m',
        'capacity': n, 'saved_at': saved_at if saved_at is not None else last_close + 1,
        'rows': data, 'sha256': '0' * 64,
    }
    return json.dumps(body, separators=(',', ':')).encode()


def test_disk_cache_without_replica_ready_is_reconstructed():
    now_ms = (START + 20 * 60) * 1000 + 5000
    snapshot = snapshot_from_candle_cache(
        (payload('BTC-USDC', step='1'), payload('ETH-USDC', step='1')), now_ms=now_ms)
    assert snapshot.evidence_kind == 'reconstructed'
    assert snapshot.fresh_members == 0


def test_latest_confirmed_cache_is_observed_and_equal_weight():
    now_ms = (START + 20 * 60) * 1000 + 5000
    snapshot = snapshot_from_candle_cache(
        (payload('BTC-USDC', step='1'), payload('ETH-USDC', step='1')),
        now_ms=now_ms, replica_ready=True)
    assert snapshot.evidence_kind == 'observed'
    assert snapshot.expected_members == snapshot.ready_members == snapshot.fresh_members == 2
    assert snapshot.bar_open_ms == (START + 19 * 60) * 1000
    assert snapshot.components.return_1m_fraction == (Decimal('1') / Decimal('118')).quantize(Decimal('1e-18'))


def test_as_of_is_prefix_invariant_when_later_bars_are_appended():
    now_early = (START + 15 * 60) * 1000
    first = snapshot_from_candle_cache(
        (payload('BTC-USDC', n=15, saved_at=START + 15 * 60),
         payload('ETH-USDC', n=15, saved_at=START + 15 * 60)),
        now_ms=now_early, replica_ready=True)
    later_now = (START + 20 * 60) * 1000 + 5000
    second = snapshot_from_candle_cache(
        (payload('BTC-USDC', n=20), payload('ETH-USDC', n=20)),
        now_ms=later_now, as_of_ms=now_early)
    assert first.bar_open_ms == second.bar_open_ms
    assert first.components == second.components
    assert first.source_content_sha256 == second.source_content_sha256
    assert first.dataset_sha256 == second.dataset_sha256
    assert first.evidence_kind == 'observed'
    assert second.evidence_kind == 'reconstructed'
    requirements = ContextRequirementsV2(
        source='okx', dataset_sha256=first.dataset_sha256,
        implementation_sha256=first.implementation_sha256,
        parameters_sha256=first.parameters_sha256,
        initialization_sha256=first.initialization_sha256,
        universe_sha256=first.universe_sha256, universe_revision=1,
        min_coverage_fraction='0.9', min_members=2, min_history_bars=10,
        max_source_age_ms=60000, max_transport_age_ms=10000)
    assert 'availability_unobserved' in context_blockers(second, requirements, now_ms=later_now)


def test_unconfirmed_and_future_bars_are_not_live_evidence():
    extra = [{'open_time': START + 20 * 60, 'open': '200', 'high': '200', 'low': '200',
              'close': '200', 'volume': '1', 'quote_volume': '100', 'confirmed': False}]
    now_ms = (START + 21 * 60) * 1000
    snapshot = snapshot_from_candle_cache(
        (payload('BTC-USDC', extra=extra, saved_at=START + 21 * 60),
         payload('ETH-USDC', extra=extra, saved_at=START + 21 * 60)),
        now_ms=now_ms)
    assert snapshot.bar_open_ms == (START + 19 * 60) * 1000
    with pytest.raises(ValueError, match='future'):
        snapshot_from_candle_cache(
            (payload('BTC-USDC'), payload('ETH-USDC')),
            now_ms=now_ms, as_of_ms=now_ms + 1)


def test_binance_or_unaligned_cache_is_rejected():
    raw = payload('BTC-USDC')
    data = json.loads(raw)
    data['source'] = 'binance-public-spot'
    with pytest.raises(ValueError, match='unsupported'):
        snapshot_from_candle_cache((json.dumps(data).encode(),), now_ms=(START + 20 * 60) * 1000)
    data = json.loads(payload('BTC-USDT'))
    with pytest.raises(ValueError, match='USDC'):
        snapshot_from_candle_cache((json.dumps(data).encode(),), now_ms=(START + 20 * 60) * 1000)


@pytest.mark.skipif(not CACHE_DIR.exists(), reason='stack-v2 candles-rsi cache is not on this host')
def test_live_rsi_cache_maps_without_claiming_reconstructed_history_as_live():
    files = tuple(sorted(CACHE_DIR.glob('*-USDC.json')))
    assert [path.name for path in files] == ['BNB-USDC.json', 'BTC-USDC.json', 'ETH-USDC.json',
                                             'SOL-USDC.json', 'XRP-USDC.json']
    payloads = tuple(path.read_bytes() for path in files)
    latest = max(json.loads(raw)['rows'][-1]['open_time'] for raw in payloads)
    saved = max(json.loads(raw)['saved_at'] for raw in payloads)
    now_ms = int(Decimal(str(saved)) * 1000) + 1
    snapshot = snapshot_from_candle_cache(payloads, now_ms=now_ms)
    assert snapshot.source == 'okx'
    assert snapshot.expected_members == 5
    assert snapshot.confirmed is True
    assert snapshot.evidence_kind == 'reconstructed'
    historic = snapshot_from_candle_cache(payloads, now_ms=now_ms, as_of_ms=(latest - 3600) * 1000)
    assert historic.evidence_kind == 'reconstructed'
    assert historic.content_sha256 != snapshot.content_sha256
