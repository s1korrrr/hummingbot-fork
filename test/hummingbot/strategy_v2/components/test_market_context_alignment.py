"""T3 causal event-time alignment of confirmed candles (observation only)."""
import random
from decimal import Decimal

import numpy as np
import pytest

from hummingbot.strategy_v2.market_context import alignment as a, universe as u
from hummingbot.strategy_v2.market_context.contracts import ClockView, ContextError

M = 60_000
T = 1_800_000_000_000 - 1_800_000_000_000 % M  # cutoff on a minute boundary
DEADLINE = T + 3_000
CONFIG = a.AlignmentConfig(interval_ms=M, history_minutes=9)
RAW = 'b' * 64


def binding(assets, capped=None, mode='liquid_top_n', benchmarks=()):
    n = len(assets)
    capped = capped or [Decimal(1) / Decimal(n)] * n
    values = dict(role='reference', universe_id='test-universe', mode=mode, requested_count=n, venue='okx',
                  numeraire='USDC', member_asset_ids=tuple(assets),
                  member_instrument_ids=tuple(f'{x}-USDC' for x in assets), benchmarks=tuple(benchmarks),
                  selection_as_of_ms=0, effective_from_ms=0, available_at_ms=0, metadata_revision='m',
                  classification_revision='c', algorithm_version='mc-universe-1', weighting_algorithm='mc-weights-1',
                  trailing_turnover=tuple(Decimal(1) for _ in assets),
                  equal_weights=tuple(Decimal(1) / Decimal(n) for _ in assets), capped_weights=tuple(capped),
                  excluded=())
    values['membership_hash'] = u.binding_hash(values)
    return u.UniverseBinding(**values)


def instrument(asset, quote='USDC'):
    return a.Instrument(dataset_id=f'okx:{asset}-{quote}:1m', venue='okx', instrument_id=f'{asset}-{quote}',
                        asset_id=asset, numeraire=quote, interval_ms=M)


def obs(asset, close_ms, price, *, revision=0, confirmed=True, available=None, volume='1', quote='USDC',
        kind='observed', sequence=1, epoch='e1'):
    price = Decimal(str(price))
    return a.CandleObservation(
        instrument=instrument(asset, quote), open_time_ms=close_ms - M, close_time_ms=close_ms, open=price,
        high=price, low=price, close=price, base_volume=Decimal(volume), quote_volume=Decimal(volume) * price,
        confirmed=confirmed, source_epoch=epoch, source_sequence=sequence, received_at_ms=None,
        available_at_ms=available if available is not None else close_ms + 500, availability_kind=kind,
        revision=revision, raw_sha256=RAW)


def series(asset, *, skip=(), start=100.0, step=1.0, **kwargs):
    return [obs(asset, T - k * M, start + step * (10 - k), sequence=10 - k, **kwargs)
            for k in range(10) if k not in skip]


def align(rows, universe, decision=DEADLINE, config=CONFIG):
    batch = a.batch_from_observations(T, rows)
    return a.align_panel(batch, universe, config, ClockView(decision_ms=decision, receipt_ms=None))


def reasons(panel, asset):
    return dict(panel.asset_reasons)[asset]


def test_full_panel_is_immutable_and_ordered():
    panel = align(series('ETH') + series('BTC'), binding(['BTC', 'ETH']))
    assert panel.asset_ids == ('BTC', 'ETH') and panel.valid.all()
    assert panel.times[-1] == T and len(panel.times) == 10
    assert panel.closes[0, -1] == 110.0 and panel.max_input_available_at_ms == T + 500
    with pytest.raises(ValueError):
        panel.closes[0, 0] = 1.0
    rows = series('BTC') + series('ETH')
    random.Random(1).shuffle(rows)
    again = align(rows, binding(['BTC', 'ETH']))
    assert again.input_manifest_digest == panel.input_manifest_digest
    assert np.array_equal(again.closes, panel.closes)


def test_unconfirmed_bar_excluded():
    rows = series('BTC', skip=(0,)) + [obs('BTC', T, 999, confirmed=False)]
    panel = align(rows, binding(['BTC']))
    assert not panel.valid[0, -1] and panel.valid[0, :-1].all()
    assert panel.closes[0, -1] == 0.0  # masked slots never carry a usable price
    assert reasons(panel, 'BTC') == ('INPUT_UNCONFIRMED',)


def test_late_candle_not_backdated():
    rows = series('BTC', skip=(0,)) + [obs('BTC', T, 111, available=DEADLINE + 1)]
    panel = align(rows, binding(['BTC']))
    assert not panel.valid[0, -1] and reasons(panel, 'BTC') == ('INPUT_STALE',)
    assert panel.max_input_available_at_ms == T - M + 500
    # The same row is admitted by a later decision; the cutoff never moves back in time.
    later = align(rows, binding(['BTC']), decision=DEADLINE + 1)
    assert later.valid[0, -1] and later.max_input_available_at_ms == DEADLINE + 1
    # A correction available after the decision cannot replace the earlier revision.
    corrected = series('BTC') + [obs('BTC', T, 222, revision=1, available=DEADLINE + 10)]
    panel = align(corrected, binding(['BTC']))
    assert panel.closes[0, -1] == 110.0
    assert align(corrected, binding(['BTC']), decision=DEADLINE + 10).closes[0, -1] == 222.0
    # Bars closing after the cutoff are outside the panel, not errors.
    assert align(series('BTC') + [obs('BTC', T + M, 5)], binding(['BTC'])).valid.all()


def test_revision_conflict_quarantined():
    duplicate = series('BTC') + [obs('BTC', T - M, 109)]  # exact duplicate is idempotent
    panel = align(duplicate, binding(['BTC']))
    assert panel.valid.all() and reasons(panel, 'BTC') == ()
    conflict = series('BTC') + [obs('BTC', T - M, 50, sequence=99)]  # same revision, different payload
    panel = align(conflict, binding(['BTC']))
    assert not panel.valid[0, -2] and panel.valid[0, -1]
    assert reasons(panel, 'BTC') == ('INPUT_GAP', 'INPUT_REVISION_CONFLICT')
    superseded = conflict + [obs('BTC', T - M, 60, revision=1)]  # a later revision resolves the key
    panel = align(superseded, binding(['BTC']))
    assert panel.valid.all() and panel.closes[0, -2] == 60.0


def test_no_forward_fill_across_gap():
    panel = align(series('BTC', skip=(3, 4)) + series('ETH'), binding(['BTC', 'ETH']))
    assert panel.valid[0].tolist() == [True] * 5 + [False, False] + [True] * 3
    assert panel.closes[0, 5] == 0.0 and panel.closes[0, 6] == 0.0
    assert reasons(panel, 'BTC') == ('INPUT_GAP',)
    missing_now = align(series('BTC', skip=(0,)), binding(['BTC']))
    assert reasons(missing_now, 'BTC') == ('INPUT_MISSING',)
    warming = align(series('BTC', skip=(9, 8)), binding(['BTC']))
    assert reasons(warming, 'BTC') == ('WARMUP_INCOMPLETE',)
    quote = align(series('BTC', quote='USDT'), binding(['BTC']))
    assert not quote.valid.any() and reasons(quote, 'BTC') == ('QUOTE_MISMATCH',)


def test_zero_volume_is_not_missing():
    rows = series('BTC', skip=(2,)) + [obs('BTC', T - 2 * M, 108, volume='0')]
    panel = align(rows, binding(['BTC']))
    assert panel.valid.all() and reasons(panel, 'BTC') == ()


def test_feature_specific_coverage():
    assets = [f'A{i:03d}' for i in range(100)]
    capped = [Decimal('0.02')] * 5 + [Decimal('0.9') / 95] * 95  # five heavy members hold 10% weight
    universe = binding(assets, capped=capped)
    valid = np.array([i >= 5 for i in range(100)])
    gate = a.CoverageGate(min_count_fraction=Decimal('0.95'), min_weight_fraction=Decimal('0.95'),
                          min_valid_assets=5)
    count_only = a.coverage(universe, valid, 'equal', gate)
    assert count_only.passed and count_only.valid_count == 95
    weighted = a.coverage(universe, valid, 'capped', gate)
    assert not weighted.passed and weighted.reasons == ('COVERAGE_LOW',)
    assert abs(weighted.valid_weight_fraction - 0.9) < 1e-12
    basket = binding(['ADA', 'BTC', 'DOGE', 'ETH', 'SOL'], mode='basket_5', benchmarks=('BTC', 'ETH'))
    four = a.coverage(basket, np.array([True, True, True, True, False]), 'equal', a.CoverageGate(
        min_count_fraction=Decimal('0.5'), min_weight_fraction=Decimal('0.5'), min_valid_assets=1))
    assert not four.passed and four.reasons == ('COVERAGE_LOW',)
    no_benchmark = a.coverage(basket, np.array([True, False, True, True, True]), 'equal', a.CoverageGate(
        min_count_fraction=Decimal('0.5'), min_weight_fraction=Decimal('0.5'), min_valid_assets=1))
    assert no_benchmark.reasons == ('BENCHMARK_MISSING', 'COVERAGE_LOW')
    # Each feature gates its own eligible set: a separate mask never borrows another's readiness.
    breadth = a.coverage(universe, np.ones(100, dtype=bool), 'capped', gate)
    residual = a.coverage(universe, np.zeros(100, dtype=bool), 'capped', gate)
    assert breadth.passed and not residual.passed and residual.valid_count == 0


def test_contract_rejections():
    with pytest.raises(ValueError):
        obs('BTC', T, -1)
    with pytest.raises(ValueError):  # misaligned bar
        a.CandleObservation(**{**obs('BTC', T, 1).model_dump(), 'open_time_ms': T - M + 1, 'close_time_ms': T + 1})
    with pytest.raises(ValueError):  # available before the bar closed
        obs('BTC', T, 1, available=T - 1)
    with pytest.raises(ValueError):
        row = obs('BTC', T, 1).model_dump()
        row['high'] = Decimal('0.5')
        a.CandleObservation(**row)
    with pytest.raises(ValueError):
        obs('BTC', T, 1, volume='-1')
    with pytest.raises(ValueError):
        obs('BTC', T, 'NaN')
    with pytest.raises(ContextError) as error:
        a.CandleBlock(instrument=instrument('BTC'), availability_kind='observed', source_epochs=('e1',),
                      open_time_ms=np.array([T - M]), open=np.array([np.inf]), high=np.array([np.inf]),
                      low=np.array([1.0]), close=np.array([1.0]), base_volume=np.array([1.0]),
                      quote_volume=None, confirmed=np.array([True]), available_at_ms=np.array([T + 1]),
                      revision=np.array([0]), epoch_index=np.array([0]), source_sequence=np.array([1]),
                      raw_sha256=np.array([bytes(32)], dtype='S32'))
    assert error.value.reasons == ('INPUT_NONFINITE',)
    with pytest.raises(ContextError):  # a batch cutoff must be on a minute boundary
        a.CandleBatch(cutoff_ms=T + 1, blocks=())
