"""T4 neutral feature kernels F01-F04 (observation only; no economic claim follows from these tests).

Tolerances: every numeric comparison with the independent reference uses abs 1e-12 on float64; identifiers,
membership, statuses and reason codes must match exactly.
"""
import math
from decimal import Decimal

import numpy as np
import pytest

from hummingbot.strategy_v2.market_context import alignment as a, features as f, state as s, universe as u
from hummingbot.strategy_v2.market_context.contracts import ClockView, ContextError

M = 60_000
T = 1_800_000_000_000 - 1_800_000_000_000 % (60 * M) + 17 * M  # deliberately not hour-aligned
SHA = 'c' * 64
GATE = a.CoverageGate(min_count_fraction=Decimal('0.95'), min_weight_fraction=Decimal('0.95'), min_valid_assets=5)
ASSETS = ('ADA', 'BTC', 'DOGE', 'ETH', 'SOL', 'XRP')


def small_config(**changes):
    values = dict(config_id='test-small', horizons_minutes=(1, 5), estimator_history_minutes=120,
                  min_estimator_samples=20, breadth_noise_coefficient=Decimal('0.1'),
                  downside_z_threshold=Decimal('-2'), volatility_floor=Decimal('0.000001'),
                  factor_variance_floor=Decimal('1e-12'), residual_scale_floor=Decimal('0.000001'),
                  min_factor_peers=4, min_rank_assets=5, change_lag_minutes=5, strength_horizon_minutes=5,
                  factor_weighting='equal', gate=GATE,
                  mandatory_features=('breadth_positive_eq_h1', 'market_return_eq_h1'), estimator_version='mc-est-1')
    values.update(changes)
    return f.FeatureConfig(**values)


def binding(assets=ASSETS, mode='liquid_top_n', capped=None):
    n = len(assets)
    values = dict(role='reference', universe_id='test-universe', mode=mode, requested_count=n, venue='okx',
                  numeraire='USDC', member_asset_ids=tuple(assets),
                  member_instrument_ids=tuple(f'{x}-USDC' for x in assets), benchmarks=(), selection_as_of_ms=0,
                  effective_from_ms=0, available_at_ms=0, metadata_revision='m', classification_revision='c',
                  algorithm_version='mc-universe-1', weighting_algorithm='mc-weights-1',
                  trailing_turnover=tuple(Decimal(i + 1) for i in range(n)),
                  equal_weights=tuple(Decimal(1) / Decimal(n) for _ in assets),
                  capped_weights=tuple(capped or u.capped_weights([Decimal(i + 1) for i in range(n)],
                                                                  max(Decimal('0.05'), Decimal(1) / n))),
                  excluded=())
    values['membership_hash'] = u.binding_hash(values)
    return u.UniverseBinding(**values)


def paths(returns):
    """Close paths from one-minute returns (N x L-1) starting at 100."""
    closes = 100.0 * np.cumprod(np.column_stack([np.ones(len(returns)), 1.0 + returns]), axis=1)
    return closes


def batch(closes, assets=ASSETS, valid=None, available_delay=500):
    length = closes.shape[1]
    times = T - (length - 1 - np.arange(length)) * M
    valid = np.ones(closes.shape, dtype=bool) if valid is None else valid
    blocks = []
    for i, asset in enumerate(assets):
        keep = valid[i]
        if not keep.any():
            continue
        c = closes[i][keep]
        n = int(keep.sum())
        blocks.append(a.CandleBlock(
            instrument=a.Instrument(dataset_id=f'okx:{asset}', venue='okx', instrument_id=f'{asset}-USDC',
                                    asset_id=asset, numeraire='USDC', interval_ms=M),
            availability_kind='observed', source_epochs=('e1',), open_time_ms=times[keep] - M, open=c, high=c, low=c,
            close=c, base_volume=np.ones(n), quote_volume=c, confirmed=np.ones(n, dtype=bool),
            available_at_ms=times[keep] + available_delay, revision=np.zeros(n, dtype=int),
            epoch_index=np.zeros(n, dtype=int), source_sequence=np.arange(n),
            raw_sha256=np.array([bytes(32)] * n, dtype='S32')))
    return a.CandleBatch(cutoff_ms=T, blocks=tuple(blocks))


def build(closes, config=None, universe=None, valid=None, assets=ASSETS):
    config = config or small_config()
    universe = universe or binding(assets)
    panel = a.align_panel(batch(closes, assets, valid), universe,
                          a.AlignmentConfig(interval_ms=M, history_minutes=config.required_history_minutes),
                          ClockView(decision_ms=T + 3_000, receipt_ms=None))
    state = s.initial_state(config, initialization_id='init-1', code_digest=SHA)
    return f.compute_context(panel, config, state)


def market(result, name):
    return next(x for x in result.content.market if x.name == name)


def asset_feature(result, asset, name):
    record = next(r for r in result.content.assets if r.asset_id == asset)
    return next(x for x in record.features if x.name == name)


def noise(n, length, scale=0.001, seed=7):
    rng = np.random.default_rng(seed)
    return rng.normal(0, scale, size=(n, length))


def test_breadth_parts_sum_to_one():
    config = small_config()
    length = config.required_history_minutes
    for seed in range(4):
        result = build(paths(noise(6, length, seed=seed)))
        for scheme in ('eq', 'cap'):
            for h in (1, 5):
                parts = [market(result, f'breadth_{kind}_{scheme}_h{h}') for kind in ('positive', 'negative', 'neutral')]
                assert all(p.status == 'VALID' and p.unit == 'share' for p in parts)
                assert abs(sum(p.value for p in parts) - 1.0) < 1e-12
    alternating = np.tile([0.002, -0.002], (6, length // 2 + 1))[:, :length]
    for final, kind in ((0.01, 'positive'), (-0.01, 'negative'), (0.0, 'neutral')):
        returns = alternating.copy()
        returns[:, -1] = final
        result = build(paths(returns))
        assert market(result, f'breadth_{kind}_eq_h1').value == 1.0
        assert result.content.status == 'READY' and result.content.reasons == ()


def test_membership_change_does_not_create_recovery():
    config = small_config()
    length = config.required_history_minutes
    returns = np.tile([0.002, -0.002], (6, length // 2 + 1))[:, :length]
    returns[:, -1] = 0.0
    returns[:, -1 - 5] = 0.0
    returns[5, -1] = 0.01  # the replacement member (XRP) is strong both now and five minutes ago
    returns[5, -1 - 5] = 0.01
    result = build(paths(returns))
    assert market(result, 'breadth_positive_eq_h1').value == pytest.approx(1 / 6)
    change = market(result, 'breadth_positive_change_5m_eq_h1')
    assert change.status == 'VALID' and change.value == 0.0  # same cohort at both times: no artificial recovery
    # If the new member lacks history at T-5 the change is unavailable, never a recovery.
    valid = np.ones((6, length + 1), dtype=bool)
    valid[5, :-4] = False
    blocked = build(paths(returns), valid=valid)
    assert market(blocked, 'breadth_positive_change_5m_eq_h1').status != 'VALID'
    assert market(blocked, 'breadth_positive_change_5m_eq_h1').value is None
    assert 'COVERAGE_LOW' in market(blocked, 'breadth_positive_change_5m_eq_h1').reasons


def test_systemic_selloff_not_cross_sectionally_centered():
    config = small_config()
    length = config.required_history_minutes
    returns = np.tile([0.01, -0.01], (6, length // 2 + 1))[:, :length] + noise(6, length, scale=0.0005)
    returns[:, -1] = -0.03
    result = build(paths(returns))
    stress = market(result, 'downside_shock_fraction_eq_h1')
    assert stress.status == 'VALID' and stress.value == 1.0
    assert market(result, 'breadth_negative_eq_h1').value == 1.0
    assert market(result, 'market_return_eq_h1').value == pytest.approx(-0.03)
    residual = asset_feature(result, 'BTC', 'residual_h1')
    assert residual.status == 'VALID' and abs(residual.value) < 0.005  # stress and residuals stay distinct
    single = returns.copy()
    single[1:, -1] = 0.0
    result = build(paths(single))
    assert market(result, 'downside_shock_fraction_eq_h1').value == pytest.approx(1 / 6)


def reference_fit(returns_matrix, target, weights):
    others = [j for j in range(len(weights)) if j != target]
    w = np.array([weights[j] for j in others])
    factor = (w[:, None] * returns_matrix[others]).sum(axis=0) / w.sum()
    r = returns_matrix[target]
    beta = np.cov(r, factor, ddof=1)[0, 1] / np.var(factor, ddof=1)
    alpha = r.mean() - beta * factor.mean()
    residuals = r - alpha - beta * factor
    return alpha, beta, np.std(residuals, ddof=1)


def test_residual_uses_leave_one_out_prior_fit():
    assert f.residual(-0.03, 0.0, 1.2, -0.025) == pytest.approx(0.0, abs=1e-15)
    assert f.residual(-0.03, 0.0, 1.2, -0.002) == pytest.approx(-0.0276)
    config = small_config()
    length = config.required_history_minutes
    rng = np.random.default_rng(3)
    common = rng.normal(0, 0.002, size=length)
    returns = common * np.array([[0.5], [1.0], [1.5], [0.8], [1.2], [2.0]]) + rng.normal(0, 0.001, size=(6, length))
    closes = paths(returns)
    result = build(closes)
    h = 5
    ends = [e for e in range(T - config.estimator_history_minutes * M, T - h * M + 1, M) if e % (h * M) == 0]
    index = {t: k for k, t in enumerate(T - (closes.shape[1] - 1 - np.arange(closes.shape[1])) * M)}
    matrix = np.array([[closes[i, index[e]] / closes[i, index[e - h * M]] - 1 for e in ends] for i in range(6)])
    weights = [1.0] * 6
    for target, asset in enumerate(ASSETS):
        alpha, beta, scale = reference_fit(matrix, target, weights)
        assert asset_feature(result, asset, f'beta_h{h}').value == pytest.approx(beta, abs=1e-12)
        assert asset_feature(result, asset, f'alpha_h{h}').value == pytest.approx(alpha, abs=1e-12)
        current = closes[:, -1] / closes[:, -1 - h] - 1
        factor_now = np.mean([current[j] for j in range(6) if j != target])
        expected = current[target] - alpha - beta * factor_now
        assert asset_feature(result, asset, f'residual_h{h}').value == pytest.approx(expected, abs=1e-12)
        assert asset_feature(result, asset, f'residual_z_h{h}').value == pytest.approx(expected / scale, abs=1e-9)
        fit = next(x for x in next(r for r in result.content.assets if r.asset_id == asset).fits
                   if x.horizon_minutes == h)
        assert fit.training_samples == len(ends) and fit.history_cutoff_ms == ends[-1] and fit.status == 'VALID'
    # The current shock never enters its own fit: changing only the latest bar leaves beta/alpha unchanged.
    shocked = closes.copy()
    shocked[1, -1] *= 0.9
    again = build(shocked)
    assert asset_feature(again, 'BTC', 'beta_h5').value == asset_feature(result, 'BTC', 'beta_h5').value
    assert asset_feature(again, 'BTC', 'residual_h5').value < asset_feature(result, 'BTC', 'residual_h5').value
    # Fewer than four peers, or any unavailable peer history, makes every model unavailable.
    few = build(closes[:4], universe=binding(ASSETS[:4]), assets=ASSETS[:4],
                config=small_config(gate=a.CoverageGate(min_count_fraction=Decimal('0.95'),
                                                        min_weight_fraction=Decimal('0.95'), min_valid_assets=4)))
    assert asset_feature(few, 'BTC', 'beta_h5').status == 'MODEL_UNAVAILABLE'
    assert asset_feature(few, 'BTC', 'beta_h5').value is None
    valid = np.ones(closes.shape, dtype=bool)
    valid[5, 10] = False
    gap = build(closes, valid=valid)
    assert asset_feature(gap, 'BTC', 'residual_h5').status == 'MODEL_UNAVAILABLE'
    assert 'MODEL_UNAVAILABLE' in asset_feature(gap, 'BTC', 'residual_h5').reasons
    flat = np.tile([0.002, -0.002], (6, length // 2 + 1))[:, :length]
    degenerate = build(paths(flat))  # identical peers make the factor variance collapse or the residual scale zero
    assert asset_feature(degenerate, 'BTC', 'beta_h1').status != 'VALID'
    assert set(asset_feature(degenerate, 'BTC', 'beta_h1').reasons) & {'FACTOR_VARIANCE_LOW', 'SCALE_DEGENERATE'}


def test_tied_ranks_are_half():
    assert f.average_rank_percentile(np.array([0.3] * 5)).tolist() == [0.5] * 5
    values = np.array([0.1, -0.2, 0.1, 0.5, -1.0])
    ranks = f.average_rank_percentile(values)
    assert ranks.tolist() == [0.625, 0.25, 0.625, 1.0, 0.0]
    permutation = [3, 0, 4, 1, 2]
    assert f.average_rank_percentile(values[permutation]).tolist() == ranks[permutation].tolist()
    config = small_config()
    result = build(paths(noise(6, config.required_history_minutes, seed=11)))
    strength = [asset_feature(result, x, 'strength_percentile_h5') for x in ASSETS]
    assert all(v.status == 'VALID' and v.unit == 'ordinal' and v.valid_count == 6 for v in strength)
    assert sorted(v.value for v in strength) == [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
    valid = np.ones((6, config.required_history_minutes + 1), dtype=bool)
    valid[:2, -1] = False
    sparse = build(paths(noise(6, config.required_history_minutes, seed=11)), valid=valid,
                   config=small_config(gate=a.CoverageGate(min_count_fraction=Decimal('0.5'),
                                                           min_weight_fraction=Decimal('0.5'), min_valid_assets=4)))
    assert asset_feature(sparse, 'ETH', 'strength_percentile_h5').reasons == ('MODEL_UNAVAILABLE',)


def test_insufficient_seven_day_history_is_unavailable():
    config = f.RESEARCH_V1
    assert config.estimator_history_minutes == 10_080 and config.required_history_minutes == 10_080 + 60 + 5
    length = config.required_history_minutes
    closes = paths(noise(6, length, seed=5))
    full = build(closes, config=config)
    assert market(full, 'breadth_positive_eq_h60').status == 'VALID'
    assert full.content.status == 'READY'
    valid = np.ones(closes.shape, dtype=bool)
    valid[:, : length + 1 - (10_080 + 60 + 1)] = False  # exactly 10141 closes: enough for h=60 levels
    exact = build(closes, config=config, valid=valid)
    assert market(exact, 'breadth_positive_eq_h60').status == 'VALID'
    assert market(exact, 'breadth_positive_change_5m_eq_h60').status == 'WARMUP_INCOMPLETE'
    valid[:, length + 1 - (10_080 + 60 + 1)] = False  # one close short
    short = build(closes, config=config, valid=valid)
    breadth = market(short, 'breadth_positive_eq_h60')
    assert breadth.status == 'WARMUP_INCOMPLETE' and breadth.value is None
    assert 'WARMUP_INCOMPLETE' in breadth.reasons
    assert market(short, 'market_return_eq_h60').status == 'VALID'  # needs only its own interval
    assert market(short, 'breadth_positive_eq_h1').status == 'VALID'
    assert asset_feature(short, 'BTC', 'beta_h60').status == 'MODEL_UNAVAILABLE'
    assert short.content.status == 'WARMING'
    with pytest.raises(ContextError):  # a shorter panel is never silently accepted as a shorter estimator
        panel = a.align_panel(batch(closes[:, 1:]), binding(),
                              a.AlignmentConfig(interval_ms=M, history_minutes=length - 1),
                              ClockView(decision_ms=T + 3_000, receipt_ms=None))
        f.compute_context(panel, config, s.initial_state(config, initialization_id='i', code_digest=SHA))


def reference_market(closes, h, weights, coefficient, config):
    ends = [e for e in range(T - config.estimator_history_minutes * M, T - h * M + 1, M) if e % (h * M) == 0]
    index = {t: k for k, t in enumerate(T - (closes.shape[1] - 1 - np.arange(closes.shape[1])) * M)}
    total = positive = 0.0
    for i in range(len(closes)):
        est = [closes[i, index[e]] / closes[i, index[e - h * M]] - 1 for e in ends]
        mean = sum(est) / len(est)
        sigma = math.sqrt(sum((x - mean) ** 2 for x in est) / (len(est) - 1))
        r = closes[i, -1] / closes[i, -1 - h] - 1
        total += weights[i] * r
        positive += weights[i] * (r > coefficient * sigma)
    return total / sum(weights), positive / sum(weights)


def test_batch_reference_parity_and_determinism():
    config = small_config()
    closes = paths(noise(6, config.required_history_minutes, seed=19))
    result = build(closes)
    universe = binding()
    for scheme, weights in (('eq', [1.0] * 6), ('cap', [float(w) for w in universe.capped_weights])):
        for h in (1, 5):
            total, positive = reference_market(closes, h, weights, 0.1, config)
            assert market(result, f'market_return_{scheme}_h{h}').value == pytest.approx(total, abs=1e-12)
            assert market(result, f'breadth_positive_{scheme}_h{h}').value == pytest.approx(positive, abs=1e-12)
    again = build(closes)
    assert again.content == result.content
    assert result.state.last_cutoff_ms == T and result.state.builds == 1
    # Input arrival order never changes outputs or their canonical asset order.
    original = batch(closes)
    shuffled = a.CandleBatch(cutoff_ms=T, blocks=tuple(original.blocks[i] for i in [3, 0, 5, 1, 4, 2]))
    alignment = a.AlignmentConfig(interval_ms=M, history_minutes=config.required_history_minutes)
    clock = ClockView(decision_ms=T + 3_000, receipt_ms=None)
    fresh = s.initial_state(config, initialization_id='init-1', code_digest=SHA)
    permuted = f.compute_context(a.align_panel(shuffled, binding(), alignment, clock), config, fresh)
    assert permuted.content == result.content
    with pytest.raises(ContextError) as error:
        panel = a.align_panel(batch(closes), binding(),
                              a.AlignmentConfig(interval_ms=M, history_minutes=config.required_history_minutes),
                              ClockView(decision_ms=T + 3_000, receipt_ms=None))
        f.compute_context(panel, config, result.state)
    assert error.value.reasons == ('CUTOFF_NOT_MONOTONIC',)
    with pytest.raises(ContextError) as error:
        f.compute_context(panel, small_config(config_id='other'), result.state)
    assert error.value.reasons == ('VERSION_MISMATCH',)
