"""F01-F04 shared market-context kernels over one aligned panel.

Batch reference implementation: every fit is recomputed from prior, non-overlapping, UTC-aligned windows of
the panel. Returns are fractions. Nothing here is a forecast, a signal to trade or a capital permission.
"""
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

import numpy as np
from pydantic import Field, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.recipe_contract import digest

from .alignment import MAX_BLOCK_ROWS, AlignedPanel, CoverageGate, coverage, nominal_weights
from .contracts import (
    MAX_FEATURES,
    MINUTE_MS,
    SERIALIZER_VERSION,
    AssetRecord,
    ContextError,
    Coverage,
    FitRecord,
    MissingAsset,
    NamedFeature,
    Provenance,
    SnapshotContent,
)
from .state import ContextState

SCHEMES = (('eq', 'equal'), ('cap', 'capped'))
LEVELS = ('breadth_positive', 'breadth_negative', 'breadth_neutral')
INVALID_STATUSES = ('WARMUP_INCOMPLETE', 'INPUT_MISSING', 'INPUT_STALE', 'MODEL_UNAVAILABLE', 'INVALID')


class FeatureConfig(PolicyModel):
    """Every numerical setting is explicit; there are no hidden production defaults."""
    config_id: str = Field(pattern=r'^[a-z0-9][a-z0-9_.-]{0,63}$')
    horizons_minutes: tuple[int, ...] = Field(min_length=1, max_length=8)
    estimator_history_minutes: int = Field(ge=2, le=20_000, strict=True)
    min_estimator_samples: int = Field(ge=3, strict=True)
    breadth_noise_coefficient: Decimal = Field(ge=0, le=10)
    downside_z_threshold: Decimal = Field(ge=-100, lt=0)
    volatility_floor: Decimal = Field(gt=0)
    factor_variance_floor: Decimal = Field(gt=0)
    residual_scale_floor: Decimal = Field(gt=0)
    min_factor_peers: int = Field(ge=1, strict=True)
    min_rank_assets: int = Field(ge=2, strict=True)
    change_lag_minutes: int = Field(ge=1, le=1440, strict=True)
    strength_horizon_minutes: int = Field(ge=1, strict=True)
    factor_weighting: Literal['equal', 'capped']
    gate: CoverageGate
    mandatory_features: tuple[str, ...] = Field(min_length=1, max_length=MAX_FEATURES)
    estimator_version: Literal['mc-est-1']

    @model_validator(mode='after')
    def coherent(self):
        horizons = self.horizons_minutes
        if list(horizons) != sorted(set(horizons)) or any(type(h) is not int or not 1 <= h <= 1440 for h in horizons):
            raise ValueError('horizons must be sorted unique integer minutes')
        for h in horizons:
            if self.estimator_history_minutes // h - 1 < self.min_estimator_samples:
                raise ValueError(f'estimator history cannot supply the minimum samples at {h}m')
        if self.strength_horizon_minutes not in horizons:
            raise ValueError('strength horizon must be a configured horizon')
        if not set(self.mandatory_features) <= set(market_feature_names(self)) \
                or len(set(self.mandatory_features)) != len(self.mandatory_features):
            raise ValueError('mandatory features must be unique published market features')
        if self.required_history_minutes + 1 > MAX_BLOCK_ROWS:
            raise ValueError('required history exceeds the bounded panel')
        return self

    @property
    def required_history_minutes(self) -> int:
        return self.estimator_history_minutes + max(self.horizons_minutes) + self.change_lag_minutes

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode='json'))


def market_feature_names(config) -> list[str]:
    names = []
    lag = config.change_lag_minutes
    for scheme, _ in SCHEMES:
        for h in config.horizons_minutes:
            names += [f'market_return_{scheme}_h{h}', *(f'{kind}_{scheme}_h{h}' for kind in LEVELS),
                      f'breadth_positive_change_{lag}m_{scheme}_h{h}', f'downside_shock_fraction_{scheme}_h{h}',
                      f'downside_shock_fraction_change_{lag}m_{scheme}_h{h}']
    return sorted(names)


def _research_v1():
    horizons = (1, 5, 15, 60)
    mandatory = []
    for h in horizons:
        mandatory += [f'market_return_eq_h{h}', f'breadth_positive_eq_h{h}', f'breadth_negative_eq_h{h}',
                      f'breadth_neutral_eq_h{h}', f'breadth_positive_change_5m_eq_h{h}',
                      f'downside_shock_fraction_eq_h{h}', f'downside_shock_fraction_change_5m_eq_h{h}']
    return FeatureConfig(
        config_id='research-v1', horizons_minutes=horizons, estimator_history_minutes=10_080, min_estimator_samples=64,
        breadth_noise_coefficient=Decimal('0.1'), downside_z_threshold=Decimal('-2'),
        volatility_floor=Decimal('0.000001'), factor_variance_floor=Decimal('1e-12'),
        residual_scale_floor=Decimal('0.000001'), min_factor_peers=4, min_rank_assets=5, change_lag_minutes=5,
        strength_horizon_minutes=60, factor_weighting='equal',
        gate=CoverageGate(min_count_fraction=Decimal('0.95'), min_weight_fraction=Decimal('0.95'), min_valid_assets=5),
        mandatory_features=tuple(sorted(mandatory)), estimator_version='mc-est-1')


# Named frozen research configuration from FEATURES.md; candidate settings, not tuned or approved defaults.
RESEARCH_V1 = _research_v1()


def residual(r: float, alpha: float, beta: float, factor: float) -> float:
    return r - alpha - beta * factor


def average_rank_percentile(values) -> np.ndarray:
    """Average ranks mapped to [0, 1], weakest 0 and strongest 1; all-equal inputs give 0.5."""
    values = np.asarray(values, dtype='<f8')
    n = len(values)
    if n < 2 or not np.isfinite(values).all():
        raise ValueError('ranking requires at least two finite values')
    order = np.argsort(values, kind='stable')
    ordered = values[order]
    ranks = np.empty(n, dtype='<f8')
    i = 0
    while i < n:
        j = i
        while j + 1 < n and ordered[j + 1] == ordered[i]:
            j += 1
        ranks[order[i:j + 1]] = (i + j) / 2 + 1
        i = j + 1
    return (ranks - 1) / (n - 1)


@dataclass(frozen=True)
class _Eligibility:
    ok: np.ndarray
    statuses: tuple[str, ...]
    reasons: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class ContextBuildResult:
    """Proposed snapshot content and the next builder state; publication belongs to the owner."""
    content: SnapshotContent
    state: ContextState
    reasons: tuple[str, ...]


class _Kernel:
    def __init__(self, panel: AlignedPanel, config: FeatureConfig):
        self.panel, self.config = panel, config
        self.n, self.length = panel.closes.shape
        self.invalid = np.concatenate([np.zeros((self.n, 1), dtype='<i8'), np.cumsum(~panel.valid, axis=1)], axis=1)
        self.asset_reasons = dict(panel.asset_reasons)
        self.first_valid = [int(np.argmax(row)) if row.any() else None for row in panel.valid]
        self.cache = {}

    def range_status(self, a: int, b: int) -> _Eligibility:
        ok = (self.invalid[:, b + 1] - self.invalid[:, a]) == 0
        statuses, reasons = [], []
        for i, asset in enumerate(self.panel.asset_ids):
            if ok[i]:
                statuses.append('VALID')
                reasons.append(())
                continue
            known = self.asset_reasons[asset]
            first = self.first_valid[i]
            if 'QUOTE_MISMATCH' in known:
                statuses.append('INVALID')
                reasons.append(('QUOTE_MISMATCH',))
            elif first is not None and first > a and (first > b or self.panel.valid[i, first:b + 1].all()):
                statuses.append('WARMUP_INCOMPLETE')
                reasons.append(('WARMUP_INCOMPLETE',))
            elif first is None or not self.panel.valid[i, b]:
                causes = {'INPUT_STALE', 'INPUT_UNCONFIRMED', 'INPUT_REVISION_CONFLICT', 'INPUT_MISSING'}
                found = tuple(x for x in known if x in causes) if b == self.length - 1 else ()
                found = found or ('INPUT_MISSING',)
                statuses.append('INPUT_STALE' if found == ('INPUT_STALE',) else
                                'INVALID' if 'INPUT_REVISION_CONFLICT' in found else 'INPUT_MISSING')
                reasons.append(found)
            else:
                statuses.append('INPUT_MISSING')
                reasons.append(('INPUT_GAP',))
        return _Eligibility(ok=ok, statuses=tuple(statuses), reasons=tuple(reasons))

    def index(self, offset: int) -> int:
        return self.length - 1 - offset

    def current(self, s: int, h: int):
        key = ('current', s, h)
        if key not in self.cache:
            a, b = self.index(s + h), self.index(s)
            state = self.range_status(a, b)
            with np.errstate(divide='ignore', invalid='ignore'):
                r = np.where(state.ok, self.panel.closes[:, b] / self.panel.closes[:, a] - 1, np.nan)
            self.cache[key] = (state, r)
        return self.cache[key]

    def estimator(self, s: int, h: int):
        key = ('estimator', s, h)
        if key not in self.cache:
            history, step = self.config.estimator_history_minutes, h * MINUTE_MS
            cutoff = self.panel.cutoff_ms - s * MINUTE_MS
            first_end = -(-(cutoff - history * MINUTE_MS) // step) * step
            last_end = (cutoff - step) // step * step
            ends = np.arange(first_end, last_end + 1, step, dtype='<i8')
            state = self.range_status(self.index(s + h + history), self.index(s))
            end_index = self.length - 1 - (self.panel.cutoff_ms - ends) // MINUTE_MS
            with np.errstate(divide='ignore', invalid='ignore'):
                returns = self.panel.closes[:, end_index] / self.panel.closes[:, end_index - h] - 1
            returns = np.where(state.ok[:, None], returns, np.nan)
            sigma = np.full(self.n, np.nan)
            if state.ok.any():
                sigma[state.ok] = returns[state.ok].std(axis=1, ddof=1)
            scaled = state.ok & np.isfinite(sigma) & (sigma >= float(self.config.volatility_floor))
            statuses = tuple('INVALID' if state.ok[i] and not scaled[i] else state.statuses[i] for i in range(self.n))
            reasons = tuple(('SCALE_DEGENERATE',) if state.ok[i] and not scaled[i] else state.reasons[i]
                            for i in range(self.n))
            self.cache[key] = (state, _Eligibility(ok=scaled, statuses=statuses, reasons=reasons), ends, returns, sigma)
        return self.cache[key]

    def directional(self, s: int, h: int):
        """Eligibility plus direction and stress indicators at one cutoff offset."""
        state, r = self.current(s, h)
        _, scale, _, _, sigma = self.estimator(s, h)
        ok = state.ok & scale.ok
        statuses = tuple(state.statuses[i] if not state.ok[i] else scale.statuses[i] for i in range(self.n))
        reasons = tuple(state.reasons[i] if not state.ok[i] else scale.reasons[i] for i in range(self.n))
        tau = float(self.config.breadth_noise_coefficient) * sigma
        with np.errstate(invalid='ignore', divide='ignore'):
            positive, negative = r > tau, r < -tau
            stressed = (r / sigma) < float(self.config.downside_z_threshold)
        return _Eligibility(ok=ok, statuses=statuses, reasons=reasons), positive, negative, stressed


def _combine(states) -> _Eligibility:
    ok = np.logical_and.reduce([s.ok for s in states])
    statuses, reasons = [], []
    for i in range(len(ok)):
        failing = next((s for s in states if not s.ok[i]), None)
        statuses.append('VALID' if failing is None else failing.statuses[i])
        reasons.append(() if failing is None else failing.reasons[i])
    return _Eligibility(ok=ok, statuses=tuple(statuses), reasons=tuple(reasons))


def _feature(kernel, name, unit, h, eligibility, scheme, value_fn) -> NamedFeature:
    universe = kernel.panel.universe
    result = coverage(universe, eligibility.ok, scheme, kernel.config.gate)
    common = dict(name=name, unit=unit, horizon_minutes=h, valid_count=result.valid_count,
                  expected_count=result.expected_count, valid_weight_fraction=result.valid_weight_fraction,
                  model_id=None, input_available_at_ms=kernel.panel.max_input_available_at_ms)
    if result.passed:
        value = float(value_fn(eligibility.ok, nominal_weights(universe, scheme)))
        if math.isfinite(value):
            return NamedFeature(value=value, status='VALID', reasons=(), **common)
        return NamedFeature(value=None, status='INVALID', reasons=('INPUT_NONFINITE',), **common)
    failing = [i for i in range(len(eligibility.ok)) if not eligibility.ok[i]]
    statuses = {eligibility.statuses[i] for i in failing}
    status = statuses.pop() if len(statuses) == 1 else 'INPUT_MISSING'
    reasons = set(result.reasons)
    for i in failing:
        reasons.update(eligibility.reasons[i])
    return NamedFeature(value=None, status=status, reasons=tuple(sorted(reasons)), **common)


def _weighted_share(mask):
    return lambda ok, w: (w[ok] * mask[ok]).sum() / w[ok].sum()


def _market_features(kernel: _Kernel) -> list[NamedFeature]:
    config, lag = kernel.config, kernel.config.change_lag_minutes
    features = []
    for scheme, full in SCHEMES:
        for h in config.horizons_minutes:
            state, r = kernel.current(0, h)
            features.append(_feature(kernel, f'market_return_{scheme}_h{h}', 'return_fraction', h, state, full,
                                     lambda ok, w, r=r: (w[ok] * r[ok]).sum() / w[ok].sum()))
            now, positive, negative, stressed = kernel.directional(0, h)
            before, positive_lag, _, stressed_lag = kernel.directional(lag, h)
            neutral = ~positive & ~negative
            for kind, mask in zip(LEVELS, (positive, negative, neutral)):
                features.append(_feature(kernel, f'{kind}_{scheme}_h{h}', 'share', h, now, full, _weighted_share(mask)))
            features.append(_feature(kernel, f'downside_shock_fraction_{scheme}_h{h}', 'share', h, now, full,
                                     _weighted_share(stressed)))
            both = _combine((now, before))  # one frozen cohort and weights at both cutoffs
            features.append(_feature(
                kernel, f'breadth_positive_change_{lag}m_{scheme}_h{h}', 'share_change', h, both, full,
                lambda ok, w, p=positive, q=positive_lag: _weighted_share(p)(ok, w) - _weighted_share(q)(ok, w)))
            features.append(_feature(
                kernel, f'downside_shock_fraction_change_{lag}m_{scheme}_h{h}', 'share_change', h, both, full,
                lambda ok, w, p=stressed, q=stressed_lag: _weighted_share(p)(ok, w) - _weighted_share(q)(ok, w)))
    return sorted(features, key=lambda x: x.name)


def _factor_id(kernel, asset, h, ends, weights) -> str:
    universe = kernel.panel.universe
    return digest({'kind': 'leave_one_out_market', 'estimator': kernel.config.estimator_version, 'target': asset,
                   'members': list(universe.member_asset_ids), 'universe': universe.membership_hash,
                   'weighting': kernel.config.factor_weighting, 'weights': [repr(float(w)) for w in weights],
                   'horizon_minutes': h, 'history_minutes': kernel.config.estimator_history_minutes,
                   'history_cutoff_ms': int(ends[-1]), 'samples': len(ends)})


def _asset_fits(kernel: _Kernel):
    """Leave-one-asset-out OLS on prior windows; returns per-horizon arrays and per-asset statuses."""
    config, n = kernel.config, kernel.n
    weights = nominal_weights(kernel.panel.universe, config.factor_weighting)
    fits = {}
    for h in config.horizons_minutes:
        history, _, ends, returns, _ = kernel.estimator(0, h)
        current, r_now = kernel.current(0, h)
        own = _combine((history, current))
        empty = np.full(n, np.nan)
        result = {'alpha': empty.copy(), 'beta': empty.copy(), 'residual': empty.copy(), 'z': empty.copy(),
                  'ends': ends, 'ok': np.zeros(n, dtype=bool), 'statuses': [], 'reasons': [], 'ids': [None] * n}
        if n - 1 < config.min_factor_peers or not own.ok.all():
            for i in range(n):
                result['statuses'].append('MODEL_UNAVAILABLE')
                result['reasons'].append(tuple(sorted({'MODEL_UNAVAILABLE', *own.reasons[i]})))
            fits[h] = result
            continue
        total_weight = weights.sum()
        loo_weight = total_weight - weights
        with np.errstate(divide='ignore', invalid='ignore'):
            factor = (weights @ returns - weights[:, None] * returns) / loo_weight[:, None]
            samples = returns.shape[1]
            dm = factor - factor.mean(axis=1)[:, None]
            dr = returns - returns.mean(axis=1)[:, None]
            var_m = (dm ** 2).sum(axis=1) / (samples - 1)
            beta = (dm * dr).sum(axis=1) / (samples - 1) / var_m
            alpha = returns.mean(axis=1) - beta * factor.mean(axis=1)
            scale = (returns - alpha[:, None] - beta[:, None] * factor).std(axis=1, ddof=1)
            factor_now = (weights @ r_now - weights * r_now) / loo_weight
            eps = r_now - alpha - beta * factor_now
            z = eps / scale
        for i, asset in enumerate(kernel.panel.asset_ids):
            if not var_m[i] >= float(config.factor_variance_floor):
                status, reasons = 'MODEL_UNAVAILABLE', ('FACTOR_VARIANCE_LOW', 'MODEL_UNAVAILABLE')
            elif not scale[i] >= float(config.residual_scale_floor):
                status, reasons = 'MODEL_UNAVAILABLE', ('MODEL_UNAVAILABLE', 'SCALE_DEGENERATE')
            elif not all(math.isfinite(float(v[i])) for v in (alpha, beta, eps, z)):
                status, reasons = 'MODEL_UNAVAILABLE', ('MODEL_UNAVAILABLE',)
            else:
                status, reasons = 'VALID', ()
                result['ok'][i] = True
                result['ids'][i] = _factor_id(kernel, asset, h, ends, weights)
                for key, values in (('alpha', alpha), ('beta', beta), ('residual', eps), ('z', z)):
                    result[key][i] = values[i]
            result['statuses'].append(status)
            result['reasons'].append(reasons)
        fits[h] = result
    return fits


def _asset_records(kernel: _Kernel, fits) -> list[AssetRecord]:
    config, panel = kernel.config, kernel.panel
    strength_fit = fits[config.strength_horizon_minutes]
    ranked = strength_fit['ok']
    percentiles = np.full(kernel.n, np.nan)
    if ranked.sum() >= config.min_rank_assets:
        percentiles[ranked] = average_rank_percentile(strength_fit['z'][ranked])
    records = []
    for i, asset in enumerate(panel.asset_ids):
        features, records_fit = [], []
        for h, fit in fits.items():
            valid = bool(fit['ok'][i])
            members = int(sum(fit['ok']))
            for key, name, unit in (('alpha', 'alpha', 'return_fraction'), ('beta', 'beta', 'regression_coefficient'),
                                    ('residual', 'residual', 'return_fraction'), ('z', 'residual_z', 'standard_score')):
                features.append(NamedFeature(
                    name=f'{name}_h{h}', value=float(fit[key][i]) if valid else None, unit=unit, horizon_minutes=h,
                    status=fit['statuses'][i], reasons=fit['reasons'][i], valid_count=members,
                    expected_count=kernel.n, valid_weight_fraction=None, model_id=fit['ids'][i],
                    input_available_at_ms=panel.max_input_available_at_ms))
            records_fit.append(FitRecord(
                horizon_minutes=h, status=fit['statuses'][i], reasons=fit['reasons'][i],
                training_samples=len(fit['ends']) if valid else 0,
                history_cutoff_ms=int(fit['ends'][-1]) if valid else None, factor_id=fit['ids'][i]))
        h = config.strength_horizon_minutes
        if not ranked[i]:
            status, reasons = strength_fit['statuses'][i], strength_fit['reasons'][i]
        elif ranked.sum() < config.min_rank_assets:
            status, reasons = 'MODEL_UNAVAILABLE', ('MODEL_UNAVAILABLE', 'RANK_POPULATION_LOW')
        else:
            status, reasons = 'VALID', ()
        features.append(NamedFeature(
            name=f'strength_percentile_h{h}', value=float(percentiles[i]) if status == 'VALID' else None,
            unit='ordinal', horizon_minutes=h, status=status, reasons=reasons, valid_count=int(ranked.sum()),
            expected_count=kernel.n, valid_weight_fraction=None, model_id=None,
            input_available_at_ms=panel.max_input_available_at_ms))
        records.append(AssetRecord(asset_id=asset, instrument_id=panel.instrument_ids[i],
                                   reasons=kernel.asset_reasons[asset], features=tuple(sorted(features, key=lambda x: x.name)),
                                   fits=tuple(records_fit)))
    return records


def _envelope(config: FeatureConfig, market: list[NamedFeature]):
    by_name = {x.name: x for x in market}
    mandatory = [by_name[name] for name in config.mandatory_features]
    failing = [x for x in mandatory if x.status != 'VALID']
    if not failing:
        return 'READY', ()
    reasons = tuple(sorted({r for x in failing for r in x.reasons}))
    if all(x.status == 'WARMUP_INCOMPLETE' for x in failing):
        return 'WARMING', reasons
    if len(failing) < len(mandatory):
        return 'DEGRADED', reasons
    return 'INVALID', reasons


def compute_context(panel: AlignedPanel, config: FeatureConfig, state: ContextState) -> ContextBuildResult:
    """Compute F01-F04 for one cutoff. Deterministic; performs no I/O and reads no clock."""
    if state.config_digest != config.digest:
        raise ContextError(('VERSION_MISMATCH',), 'builder state was initialized for another configuration')
    if state.last_cutoff_ms is not None and panel.cutoff_ms <= state.last_cutoff_ms:
        raise ContextError(('CUTOFF_NOT_MONOTONIC',))
    if panel.interval_ms != MINUTE_MS or len(panel.times) < config.required_history_minutes + 1:
        raise ContextError(('INPUT_INVALID',), 'panel is shorter than the configured history; no shorter estimator')
    kernel = _Kernel(panel, config)
    market = _market_features(kernel)
    fits = _asset_fits(kernel)
    assets = _asset_records(kernel, fits)
    status, reasons = _envelope(config, market)
    universe = panel.universe
    latest = panel.valid[:, -1]
    weights = nominal_weights(universe, 'capped')
    missing = tuple(MissingAsset(asset_id=asset, reasons=kernel.asset_reasons[asset] or ('INPUT_MISSING',))
                    for i, asset in enumerate(panel.asset_ids) if not latest[i])
    model_ids = sorted(fid for fit in fits.values() for fid in fit['ids'] if fid is not None)
    content = SnapshotContent(
        venue=universe.venue, numeraire=universe.numeraire, cutoff_ms=panel.cutoff_ms,
        max_input_available_at_ms=panel.max_input_available_at_ms,
        provenance=Provenance(code_digest=state.code_digest, config_digest=config.digest,
                              universe_hash=universe.membership_hash, input_manifest_digest=panel.input_manifest_digest,
                              model_digest=digest(model_ids), serializer_version=SERIALIZER_VERSION, artifact_refs=()),
        coverage=Coverage(expected_count=kernel.n, valid_count=int(latest.sum()),
                          valid_weight_fraction=min(1.0, float(weights[latest].sum() / weights.sum())), missing=missing),
        status=status, reasons=reasons, market=tuple(market), assets=tuple(assets))
    next_state = state.model_copy(update={'last_cutoff_ms': panel.cutoff_ms, 'builds': state.builds + 1})
    return ContextBuildResult(content=content, state=next_state, reasons=reasons)
