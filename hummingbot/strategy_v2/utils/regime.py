"""Causal market regime (regime_v1) shared by backtest replay and live evaluation.

Two engines share one contract:

* ``HtfVolSpec``: the volatility level of completed higher-timeframe bars (mean true
  range over a fixed window of UTC-aligned bars, as a fraction of the last close),
  bucketed by absolute edges the caller must supply. A trend score on another completed
  higher timeframe is reported as a diagnostic and joins the label only when
  ``trend_in_label`` is set. Nothing is fitted at evaluation time, so every output is a
  pure function of the spec and a fixed trailing window of completed bars. The edges
  themselves are the caller's data-derived choice (for example in-sample terciles) and
  have no default: record their provenance, and treat any evaluation on the window that
  chose them as in-sample and exploratory.
* ``HmmSpec``: a Gaussian HMM on finite-window 1m features, fitted on the fixed UTC
  refit grid from bars ``[b - W, b)``, several seeds (best converged log-likelihood),
  a t-statistic/occupancy label gate and Hungarian anchoring of states to fixed spec
  anchors. Only the filtered (forward) posterior is used.

Time semantics: ``evaluate(spec, bars, now)`` uses only base bars complete at ``now``
(``timestamp + 60 <= now``). ``replay(spec, bars)`` returns, at row ``i``, the regime
available at ``bars.timestamp[i]`` (bars ``< i``); it calls the same window and filter
functions as ``evaluate``, so a replayed row equals a live evaluation at that time.

Failures are explicit ``Unavailable`` values with a reason; nothing falls back to a
default regime. Importing this module starts no services and imports no controller.
The HMM engine imports ``hmmlearn`` and ``scipy`` only when it fits.

Research-only until separately approved: nothing here is wired into a controller.
It lives in ``strategy_v2/utils`` rather than ``strategy_v2/components`` because every
file under ``components`` joins the sealed source closure of the native controllers
(live ``rsi_modular`` included); an unwired research module must not change those seals.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field, fields
from functools import cached_property
from typing import Optional, Union

import numpy as np

SCHEMA = "hummingbot.regime.v1"
BASE_SECONDS = 60
INTERVAL_SECONDS = {"5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400}
VOL_LABELS = ("vol_low", "vol_mid", "vol_high")
TREND_LABELS = {-1: "trend_down", 0: "trend_flat", 1: "trend_up"}
HMM_FEATURES = ("log_return", "atr_fraction", "signed_efficiency", "volume_ratio")
UNASSIGNED = "Unassigned"
REASONS = (
    "insufficient_history",  # not enough bars before the window
    "gap",                   # a base minute is missing inside the window
    "invalid_bar",           # non-finite, non-positive or inconsistent OHLCV inside the window
    "flat_range",            # zero true range (price did not move) where a range is required
    "flat_volume",           # zero volume where the volume ratio is required
    "degenerate_feature",    # a fitted feature has no variance in the fit window
    "nonconverged_fit",      # no seed converged before the iteration cap
    "label_gate",            # too much fitted occupancy failed the state label gate
    "unassigned_state",      # the filtered posterior's top label is an unassigned state
    "model_mismatch",        # a supplied model belongs to another spec or refit boundary
)

# Anchors for Hungarian state canonicalization, in standardized feature space
# (log_return, atr_fraction, signed_efficiency, volume_ratio). They fix state identity
# only; labels still come from the fitted statistics and the gate.
DEFAULT_HMM_ANCHORS = (
    (0.0, -1.0, 0.0, -0.5),   # quiet range
    (0.0, 1.0, 0.0, 0.5),     # volatile range
    (0.5, -0.5, 1.0, 0.0),    # quiet up
    (0.5, 1.5, 1.0, 1.0),     # volatile up
    (-0.5, -0.5, -1.0, 0.0),  # quiet down
    (-0.5, 1.5, -1.0, 1.0),   # volatile down
)


# --------------------------------------------------------------------------- values


@dataclass(frozen=True)
class Unavailable:
    """An explicit absence of a regime. ``reason`` is one of ``REASONS``."""
    reason: str
    detail: str = ""
    timestamp: Optional[int] = None

    def __post_init__(self):
        if self.reason not in REASONS:
            raise ValueError(f"unknown regime unavailability reason: {self.reason}")


@dataclass(frozen=True)
class RegimeState:
    """The regime available at ``timestamp`` (decision time, UTC seconds)."""
    timestamp: int
    label: str
    engine: str
    spec_hash: str
    model_hash: str
    vol_bucket: Optional[int] = None          # 0 low, 1 mid, 2 high (htf_vol)
    vol_atr_fraction: Optional[float] = None  # htf_vol level that decided the bucket
    trend: Optional[int] = None               # -1, 0, 1 (htf_vol diagnostic)
    trend_score: Optional[float] = None       # fitted log move over the window, in ATR units
    confidence: Optional[float] = None        # hmm: filtered posterior mass of ``label``
    probabilities: tuple = ()                 # hmm: ((label, mass), ...) sorted by label
    source_bar: Optional[int] = None          # start of the last completed bar that was used


RegimeResult = Union[RegimeState, Unavailable]


def _canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


# ---------------------------------------------------------------------------- specs


@dataclass(frozen=True)
class HtfVolSpec:
    """Higher-timeframe volatility regime; the caller supplies the absolute vol_edges (no default)."""
    vol_interval: str = "1h"
    vol_window: int = 24
    vol_edges: Optional[tuple] = None
    trend_interval: str = "4h"
    trend_window: int = 24
    trend_threshold: float = 2.0
    trend_in_label: bool = False
    engine: str = field(default="htf_vol", init=False)

    def __post_init__(self):
        for name in ("vol_interval", "trend_interval"):
            if getattr(self, name) not in INTERVAL_SECONDS:
                raise ValueError(f"{name} must be one of {sorted(INTERVAL_SECONDS)}")
        for name in ("vol_window", "trend_window"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or not 2 <= value <= 1000:
                raise ValueError(f"{name} must be an integer in [2, 1000]")
        if self.vol_edges is None:
            raise ValueError("vol_edges are required: supply the edges and record their provenance")
        edges = tuple(float(edge) for edge in self.vol_edges)
        if len(edges) != 2 or not all(math.isfinite(edge) for edge in edges) or not 0 < edges[0] < edges[1] < 1:
            raise ValueError("vol_edges must be two increasing finite fractions in (0, 1)")
        object.__setattr__(self, "vol_edges", edges)
        threshold = float(self.trend_threshold)
        if not math.isfinite(threshold) or threshold <= 0:
            raise ValueError("trend_threshold must be positive")
        object.__setattr__(self, "trend_threshold", threshold)
        if not isinstance(self.trend_in_label, bool):
            raise ValueError("trend_in_label must be a bool")

    def to_dict(self) -> dict:
        value = asdict(self)
        value["vol_edges"] = list(self.vol_edges)
        return {"schema": SCHEMA, **value}

    @cached_property
    def spec_hash(self) -> str:
        return _digest(self.to_dict())


@dataclass(frozen=True)
class HmmSpec:
    """Gaussian HMM on finite-window 1m features, refit on a fixed UTC grid."""
    n_states: int = 6
    fit_bars: int = 2880
    refit_seconds: int = 86400
    warm_bars: int = 500
    seeds: tuple = (0, 1, 2, 3)
    n_iter: int = 200
    tol: float = 1e-3
    feature_window: int = 14
    volume_window: int = 20
    clip_quantiles: tuple = (1.0, 99.0)
    min_occupancy: float = 0.02
    max_unassigned: float = 0.10
    max_invalid_fit_fraction: float = 0.01
    trend_t: float = 2.0
    anchors: tuple = DEFAULT_HMM_ANCHORS
    engine: str = field(default="hmm", init=False)

    def __post_init__(self):
        def positive_int(name, low=1):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < low:
                raise ValueError(f"{name} must be an integer >= {low}")
        positive_int("n_states", 2)
        positive_int("fit_bars", 50)
        positive_int("warm_bars", 1)
        positive_int("n_iter", 2)
        positive_int("feature_window", 2)
        positive_int("volume_window", 2)
        positive_int("refit_seconds", BASE_SECONDS)
        if self.refit_seconds % BASE_SECONDS:
            raise ValueError("refit_seconds must be a whole number of minutes")
        seeds = tuple(self.seeds)
        if not seeds or len(set(seeds)) != len(seeds) or not all(isinstance(s, int) and s >= 0 for s in seeds):
            raise ValueError("seeds must be distinct non-negative integers")
        object.__setattr__(self, "seeds", seeds)
        anchors = tuple(tuple(float(v) for v in row) for row in self.anchors)
        if len(anchors) != self.n_states or any(len(row) != len(HMM_FEATURES) for row in anchors):
            raise ValueError("anchors need one row of len(HMM_FEATURES) values per state")
        object.__setattr__(self, "anchors", anchors)
        low, high = (float(q) for q in self.clip_quantiles)
        if not 0 <= low < high <= 100:
            raise ValueError("clip_quantiles must satisfy 0 <= low < high <= 100")
        object.__setattr__(self, "clip_quantiles", (low, high))
        for name in ("tol", "min_occupancy", "max_unassigned", "max_invalid_fit_fraction", "trend_t"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
            object.__setattr__(self, name, value)

    @property
    def lookback(self) -> int:
        return max(self.feature_window, self.volume_window)

    def to_dict(self) -> dict:
        value = asdict(self)
        value.update(seeds=list(self.seeds), clip_quantiles=list(self.clip_quantiles),
                     anchors=[list(row) for row in self.anchors])
        return {"schema": SCHEMA, **value}

    @cached_property
    def spec_hash(self) -> str:
        return _digest(self.to_dict())


RegimeSpec = Union[HtfVolSpec, HmmSpec]


def spec_from_dict(value: dict) -> RegimeSpec:
    """Strict inverse of ``spec.to_dict()``."""
    value = dict(value)
    if value.pop("schema", None) != SCHEMA:
        raise ValueError("regime spec schema differs")
    engine = value.pop("engine", None)
    cls = {"htf_vol": HtfVolSpec, "hmm": HmmSpec}.get(engine)
    if cls is None:
        raise ValueError(f"unknown regime engine: {engine}")
    names = {f.name for f in fields(cls) if f.init}
    if set(value) != names:
        raise ValueError(f"regime spec fields differ: {sorted(set(value) ^ names)}")
    if cls is HtfVolSpec:
        value["vol_edges"] = tuple(value["vol_edges"])
    else:
        value.update(seeds=tuple(value["seeds"]), clip_quantiles=tuple(value["clip_quantiles"]),
                     anchors=tuple(tuple(row) for row in value["anchors"]))
    return cls(**value)


# ----------------------------------------------------------------------------- bars


@dataclass(frozen=True)
class Bars:
    """Base 1m bars keyed by open time (UTC seconds). Missing minutes are allowed and
    handled explicitly; rows must be strictly increasing whole minutes."""
    timestamp: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray

    def __post_init__(self):
        stamps = np.asarray(self.timestamp)
        if stamps.ndim != 1 or not np.issubdtype(stamps.dtype, np.integer):
            raise ValueError("bar timestamps must be a 1-D integer array of seconds")
        stamps = stamps.astype(np.int64, copy=True)
        if len(stamps) and (np.any(np.diff(stamps) <= 0) or np.any(stamps % BASE_SECONDS)):
            raise ValueError("bar timestamps must be strictly increasing whole minutes")
        stamps.setflags(write=False)
        object.__setattr__(self, "timestamp", stamps)
        for name in ("open", "high", "low", "close", "volume"):
            values = np.array(getattr(self, name), dtype=np.float64, copy=True)
            if values.shape != stamps.shape:
                raise ValueError(f"bar column {name} length differs from timestamps")
            values.setflags(write=False)
            object.__setattr__(self, name, values)

    @classmethod
    def from_frame(cls, frame) -> "Bars":
        return cls(timestamp=frame["timestamp"].to_numpy(dtype=np.int64),
                   **{name: frame[name].to_numpy(dtype=np.float64)
                      for name in ("open", "high", "low", "close", "volume")})

    def __len__(self) -> int:
        return len(self.timestamp)

    def before(self, now: int) -> "Bars":
        """Bars complete at ``now`` (open time + 60 <= now)."""
        stop = int(np.searchsorted(self.timestamp, int(now) - BASE_SECONDS, side="right"))
        return self.rows(0, stop)

    def rows(self, start: int, stop: int) -> "Bars":
        return Bars(**{name: getattr(self, name)[start:stop]
                       for name in ("timestamp", "open", "high", "low", "close", "volume")})

    def invalid(self) -> np.ndarray:
        """Per-row invalid OHLCV flags (non-finite, non-positive price, inconsistent range)."""
        o, h, low, c, v = self.open, self.high, self.low, self.close, self.volume
        with np.errstate(invalid="ignore"):
            finite = np.isfinite(o) & np.isfinite(h) & np.isfinite(low) & np.isfinite(c) & np.isfinite(v)
            ok = finite & (o > 0) & (h > 0) & (low > 0) & (c > 0) & (v >= 0)
            ok &= (h >= np.maximum(o, c)) & (low <= np.minimum(o, c)) & (h >= low)
        return ~ok


# ------------------------------------------------------------------------- htf_vol


def _htf_window(bars: Bars, invalid_cumsum: np.ndarray, interval: str, window: int,
                last_start: int) -> Union[Unavailable, tuple]:
    """``window + 1`` complete UTC-aligned bars ending with the one starting at ``last_start``.

    Buckets follow ``candle_aggregation.derive_higher_timeframe_candles``: a bucket is
    ``floor(ts / interval) * interval`` and is used only when all its base minutes exist.
    Returns (atr_fraction, trend_score) or Unavailable.
    """
    seconds = INTERVAL_SECONDS[interval]
    per_bucket = seconds // BASE_SECONDS
    first = last_start - window * seconds
    stamps = bars.timestamp
    if not len(stamps) or first < stamps[0]:
        return Unavailable("insufficient_history", f"{interval} window starts {first}")
    lo = int(np.searchsorted(stamps, first))
    hi = int(np.searchsorted(stamps, last_start + seconds))
    need = (window + 1) * per_bucket
    if hi - lo != need:  # strictly increasing whole minutes: count == need <=> contiguous
        return Unavailable("gap", f"{interval} window has {hi - lo} of {need} base bars")
    if invalid_cumsum[hi] - invalid_cumsum[lo]:
        return Unavailable("invalid_bar", f"{interval} window has invalid base bars")
    high = bars.high[lo:hi].reshape(window + 1, per_bucket).max(axis=1)
    low = bars.low[lo:hi].reshape(window + 1, per_bucket).min(axis=1)
    close = bars.close[lo:hi].reshape(window + 1, per_bucket)[:, -1]
    previous = close[:-1]
    high, low = high[1:], low[1:]
    true_range = np.maximum(high - low, np.maximum(np.abs(high - previous), np.abs(low - previous)))
    atr_fraction = float(true_range.mean() / close[-1])
    if not atr_fraction > 0:
        return Unavailable("flat_range", f"{interval} window has zero true range")
    x = np.arange(window, dtype=np.float64) - (window - 1) / 2.0
    slope = float((x * np.log(close[1:])).sum() / (x * x).sum())
    return atr_fraction, slope * (window - 1) / atr_fraction


def _last_start(now: int, interval: str) -> int:
    seconds = INTERVAL_SECONDS[interval]
    return (int(now) // seconds) * seconds - seconds


def _htf_state(spec: HtfVolSpec, bars: Bars, invalid_cumsum: np.ndarray, now: int, cache=None) -> RegimeResult:
    def window(interval, size):
        start = _last_start(now, interval)
        key = (interval, size, start)
        if cache is not None and key in cache:
            return cache[key], start
        value = _htf_window(bars, invalid_cumsum, interval, size, start)
        if cache is not None:
            cache[key] = value
        return value, start

    vol, vol_start = window(spec.vol_interval, spec.vol_window)
    if isinstance(vol, Unavailable):
        return Unavailable(vol.reason, vol.detail, int(now))
    atr_fraction = vol[0]
    bucket = int(np.searchsorted(np.asarray(spec.vol_edges), atr_fraction, side="right"))
    trend_result, _ = window(spec.trend_interval, spec.trend_window)
    trend = score = None
    if not isinstance(trend_result, Unavailable):
        score = trend_result[1]
        trend = 1 if score > spec.trend_threshold else -1 if score < -spec.trend_threshold else 0
    elif spec.trend_in_label:
        return Unavailable(trend_result.reason, trend_result.detail, int(now))
    label = VOL_LABELS[bucket] + (f"/{TREND_LABELS[trend]}" if spec.trend_in_label else "")
    return RegimeState(timestamp=int(now), label=label, engine=spec.engine, spec_hash=spec.spec_hash,
                       model_hash=spec.spec_hash, vol_bucket=bucket, vol_atr_fraction=atr_fraction,
                       trend=trend, trend_score=score, source_bar=vol_start)


# ----------------------------------------------------------------------------- hmm


_FEATURE_REASONS = {1: "insufficient_history", 2: "gap", 3: "invalid_bar", 4: "flat_range", 5: "flat_volume"}


def _rolling_sum(values: np.ndarray, window: int) -> np.ndarray:
    """Trailing sum in a fixed element order, so a row's value never depends on series length."""
    out = np.full(len(values), np.nan)
    if len(values) >= window:
        acc = np.zeros(len(values) - window + 1)
        for k in range(window):
            acc = acc + values[window - 1 - k:len(values) - k]
        out[window - 1:] = acc
    return out


def hmm_features(spec: HmmSpec, bars: Bars) -> tuple:
    """Finite-window causal features per bar and a per-row reason code (0 = valid).

    Row t uses only bars t - lookback .. t, so its value is identical for any series
    that contains those bars (prefix and slice invariant)."""
    n = len(bars)
    lookback = spec.lookback
    codes = np.zeros(n, dtype=np.int8)
    features = np.full((n, len(HMM_FEATURES)), np.nan)
    if n == 0:
        return features, codes
    stamps = bars.timestamp
    invalid = bars.invalid()
    invalid_cumsum = np.r_[0, np.cumsum(invalid)]
    close, high, low, volume = bars.close, bars.high, bars.low, bars.volume
    with np.errstate(divide="ignore", invalid="ignore"):
        previous = np.r_[np.nan, close[:-1]]
        log_return = np.log(close / previous)
        true_range = np.maximum(high - low, np.maximum(np.abs(high - previous), np.abs(low - previous)))
        atr = _rolling_sum(true_range, spec.feature_window) / spec.feature_window
        path = _rolling_sum(np.abs(close - previous), spec.feature_window)
        net = close - np.r_[np.full(spec.feature_window, np.nan), close[:-spec.feature_window]]
        efficiency = net / path
        volume_mean = _rolling_sum(volume, spec.volume_window) / spec.volume_window
        features[:, 0] = log_return
        features[:, 1] = atr / close
        features[:, 2] = efficiency
        features[:, 3] = volume / volume_mean
    index = np.arange(n)
    codes[stamps - lookback * BASE_SECONDS < stamps[0]] = 1
    codes[(codes == 0) & (index < lookback)] = 2  # lookback reaches before the first row through a gap
    later = index >= lookback
    span = np.zeros(n, dtype=np.int64)
    span[later] = stamps[later] - stamps[index[later] - lookback]
    codes[(codes == 0) & later & (span != lookback * BASE_SECONDS)] = 2
    bad = np.zeros(n, dtype=bool)
    bad[later] = invalid_cumsum[index[later] + 1] - invalid_cumsum[index[later] - lookback] > 0
    codes[(codes == 0) & bad] = 3
    codes[(codes == 0) & ~(path > 0)] = 4
    codes[(codes == 0) & ~(volume_mean > 0)] = 5
    codes[(codes == 0) & ~np.isfinite(features).all(axis=1)] = 3
    features[codes != 0] = np.nan
    return features, codes


@dataclass(frozen=True)
class RegimeModel:
    """A fitted HMM regime model: immutable, content-hashed and JSON round-trippable."""
    spec_hash: str
    boundary: int
    seed: int
    log_likelihood: float
    iterations: int
    startprob: tuple
    transmat: tuple
    means: tuple
    variances: tuple
    clip_low: tuple
    clip_high: tuple
    scale_mean: tuple
    scale_std: tuple
    labels: tuple
    occupancy: tuple

    def to_dict(self) -> dict:
        def plain(value):
            if isinstance(value, tuple):
                return [plain(item) for item in value]
            return value
        return {"schema": SCHEMA, **{f.name: plain(getattr(self, f.name)) for f in fields(self)}}

    @cached_property
    def model_hash(self) -> str:
        return _digest(self.to_dict())

    def to_json(self) -> str:
        return _canonical(self.to_dict())

    @classmethod
    def from_json(cls, raw: str) -> "RegimeModel":
        from hummingbot.strategy_v2.components.strict_json import strict_json

        value = strict_json(raw)
        if value.pop("schema", None) != SCHEMA:
            raise ValueError("regime model schema differs")
        names = [f.name for f in fields(cls)]
        if set(value) != set(names):
            raise ValueError("regime model fields differ")

        def frozen(item):
            return tuple(frozen(v) for v in item) if isinstance(item, list) else item
        return cls(**{name: frozen(value[name]) for name in names})


def refit_boundary(spec: HmmSpec, now: int) -> int:
    """The UTC refit boundary that governs decisions at ``now``."""
    return (int(now) // spec.refit_seconds) * spec.refit_seconds


def _minute_grid(bars: Bars, features: np.ndarray, codes: np.ndarray, start: int, stop: int) -> tuple:
    """Features on every minute in [start, stop); missing minutes carry the ``gap`` code."""
    minutes = np.arange(start, stop, BASE_SECONDS, dtype=np.int64)
    grid = np.full((len(minutes), features.shape[1]), np.nan)
    grid_codes = np.full(len(minutes), 2, dtype=np.int8)
    lo = int(np.searchsorted(bars.timestamp, start))
    hi = int(np.searchsorted(bars.timestamp, stop))
    position = (bars.timestamp[lo:hi] - start) // BASE_SECONDS
    grid[position] = features[lo:hi]
    grid_codes[position] = codes[lo:hi]
    return minutes, grid, grid_codes


def _scale(model: RegimeModel, rows: np.ndarray) -> np.ndarray:
    low, high = np.asarray(model.clip_low), np.asarray(model.clip_high)
    return (np.clip(rows, low, high) - np.asarray(model.scale_mean)) / np.asarray(model.scale_std)


def _log_emission(model: RegimeModel, z: np.ndarray) -> np.ndarray:
    """Diagonal Gaussian log densities, summed over features in a fixed order."""
    means, variances = np.asarray(model.means), np.asarray(model.variances)
    out = np.full((len(z), len(means)), -0.5 * len(HMM_FEATURES) * math.log(2 * math.pi))
    for d in range(len(HMM_FEATURES)):
        out = out - 0.5 * (np.log(variances[:, d])[None, :]
                           + (z[:, d, None] - means[None, :, d]) ** 2 / variances[None, :, d])
    return out


def _logsumexp(values: np.ndarray, axis: int) -> np.ndarray:
    peak = values.max(axis=axis, keepdims=True)
    return (peak + np.log(np.exp(values - peak).sum(axis=axis, keepdims=True))).squeeze(axis)


def filter_step(model: RegimeModel, log_alpha: Optional[np.ndarray], log_emission: Optional[np.ndarray]) -> np.ndarray:
    """One forward-filter step; ``log_emission=None`` is a missing observation (predict only).

    Returns the normalized log filtered posterior. Uses only the past and the current row."""
    if log_alpha is None:
        prior = np.log(np.asarray(model.startprob) + 1e-300)
    else:
        prior = _logsumexp(log_alpha[:, None] + np.log(np.asarray(model.transmat) + 1e-300), axis=0)
    posterior = prior if log_emission is None else prior + log_emission
    return posterior - _logsumexp(posterior, axis=0)


def _hmm_decision(model: RegimeModel, spec: HmmSpec, log_alpha: np.ndarray, now: int, source: int) -> RegimeResult:
    masses: dict = {}
    for label, mass in zip(model.labels, np.exp(log_alpha)):
        masses[label] = masses.get(label, 0.0) + float(mass)
    ordered = sorted(masses)
    top = max(ordered, key=lambda name: masses[name])  # ties -> first in sorted order
    if top == UNASSIGNED:
        return Unavailable("unassigned_state", f"posterior {masses[top]:.3f}", int(now))
    return RegimeState(timestamp=int(now), label=top, engine=spec.engine, spec_hash=spec.spec_hash,
                       model_hash=model.model_hash, confidence=masses[top],
                       probabilities=tuple((name, masses[name]) for name in ordered), source_bar=int(source))


def canonical_order(means, anchors) -> np.ndarray:
    """Hungarian assignment of fitted states to the spec's fixed anchor slots.

    Returns ``order`` with ``order[slot]`` = fitted state index. Anchors are fixed in the
    spec (never the previous fit), so state identity does not depend on fit history."""
    from scipy.optimize import linear_sum_assignment

    means, anchors = np.asarray(means, dtype=float), np.asarray(anchors, dtype=float)
    cost = ((means[:, None, :] - anchors[None, :, :]) ** 2).sum(axis=2)
    states, slots = linear_sum_assignment(cost)
    return states[np.argsort(slots)]


def fit(spec: HmmSpec, bars: Bars, boundary: int) -> Union[RegimeModel, Unavailable]:
    """Fit the HMM for ``boundary`` using only bars with open time in [b - W*60, b)."""
    if boundary % spec.refit_seconds:
        raise ValueError("boundary must lie on the spec's UTC refit grid")
    start = boundary - spec.fit_bars * BASE_SECONDS
    lookback_start = start - spec.lookback * BASE_SECONDS
    window = bars.rows(int(np.searchsorted(bars.timestamp, lookback_start)),
                       int(np.searchsorted(bars.timestamp, boundary)))
    if not len(window) or window.timestamp[0] > start:
        return Unavailable("insufficient_history", f"fit window starts {start}", boundary)
    features, codes = hmm_features(spec, window)
    _, rows, row_codes = _minute_grid(window, features, codes, start, boundary)
    bad = row_codes != 0
    if bad.mean() > spec.max_invalid_fit_fraction:
        reason = _FEATURE_REASONS[int(np.bincount(row_codes[bad]).argmax())]
        return Unavailable(reason, f"{int(bad.sum())} of {len(rows)} fit rows invalid", boundary)
    train = rows[~bad]
    low, high = (np.percentile(train, q, axis=0) for q in spec.clip_quantiles)
    clipped = np.clip(train, low, high)
    mean, std = clipped.mean(axis=0), clipped.std(axis=0)
    if np.any(~(std > 1e-12)):
        names = [HMM_FEATURES[i] for i in np.flatnonzero(~(std > 1e-12))]
        return Unavailable("degenerate_feature", ",".join(names), boundary)
    z = (clipped - mean) / std

    from hmmlearn.hmm import GaussianHMM

    best = None
    for seed in spec.seeds:
        candidate = GaussianHMM(n_components=spec.n_states, covariance_type="diag", n_iter=spec.n_iter,
                                tol=spec.tol, random_state=seed)
        candidate.fit(z)
        # hmmlearn reports converged=True at the iteration cap; a cap hit is not convergence.
        if candidate.monitor_.iter >= spec.n_iter or not candidate.monitor_.converged:
            continue
        score = float(candidate.score(z))
        if math.isfinite(score) and (best is None or score > best[0]):
            best = (score, seed, candidate)
    if best is None:
        return Unavailable("nonconverged_fit", f"no seed of {list(spec.seeds)} converged in {spec.n_iter}", boundary)
    score, seed, hmm = best
    order = canonical_order(hmm.means_, spec.anchors)
    decoded = np.argsort(order)[hmm.predict(z)]  # Viterbi on the (past) fit window, in slot ids
    raw_atr, raw_return = clipped[:, 1], clipped[:, 0]
    median_atr = float(np.median(raw_atr))
    labels, occupancy = [], []
    for slot in range(spec.n_states):
        member = decoded == slot
        share = float(member.mean())
        occupancy.append(share)
        if member.sum() < 2 or share < spec.min_occupancy:
            labels.append(UNASSIGNED)
            continue
        values = raw_return[member]
        spread = float(values.std(ddof=1))
        t_stat = float(values.mean() / (spread / math.sqrt(len(values)))) if spread > 0 else 0.0
        volatility = "HV" if float(raw_atr[member].mean()) > median_atr else "LV"
        direction = "Range" if abs(t_stat) < spec.trend_t else "Trend_Up" if t_stat > 0 else "Trend_Down"
        labels.append(f"{volatility}_{direction}")
    unassigned = sum(share for label, share in zip(labels, occupancy) if label == UNASSIGNED)
    if unassigned > spec.max_unassigned:
        return Unavailable("label_gate", f"unassigned occupancy {unassigned:.3f}", boundary)
    as_tuple = lambda array: tuple(float(v) for v in array)  # noqa: E731
    return RegimeModel(
        spec_hash=spec.spec_hash, boundary=int(boundary), seed=int(seed), log_likelihood=score,
        iterations=int(hmm.monitor_.iter), startprob=as_tuple(hmm.startprob_[order]),
        transmat=tuple(as_tuple(row) for row in hmm.transmat_[order][:, order]),
        means=tuple(as_tuple(row) for row in hmm.means_[order]),
        variances=tuple(as_tuple(row) for row in hmm._covars_[order]),
        clip_low=as_tuple(low), clip_high=as_tuple(high), scale_mean=as_tuple(mean), scale_std=as_tuple(std),
        labels=tuple(labels), occupancy=tuple(occupancy))


def _hmm_segment(spec: HmmSpec, model: RegimeModel, bars: Bars, features, codes, boundary: int, stop: int):
    """Filter from ``boundary - warm`` over every minute before ``stop``; yields
    (minute, log_alpha or None, code) for each minute at or after ``boundary - 60``."""
    start = boundary - spec.warm_bars * BASE_SECONDS
    minutes, rows, row_codes = _minute_grid(bars, features, codes, start, stop)
    emission = np.full((len(rows), spec.n_states), np.nan)
    ok = row_codes == 0
    if ok.any():
        emission[ok] = _log_emission(model, _scale(model, rows[ok]))
    log_alpha = None
    for index, minute in enumerate(minutes):
        log_alpha = filter_step(model, log_alpha, emission[index] if ok[index] else None)
        if minute >= boundary - BASE_SECONDS:
            yield int(minute), log_alpha, int(row_codes[index])


def _check_model(spec: HmmSpec, model: RegimeModel, boundary: int) -> Optional[Unavailable]:
    if model.spec_hash != spec.spec_hash or model.boundary != boundary:
        return Unavailable("model_mismatch", f"model {model.boundary} for boundary {boundary}", boundary)
    return None


# -------------------------------------------------------------------------- public


def evaluate(spec: RegimeSpec, bars: Bars, now: int, model: Optional[RegimeModel] = None) -> RegimeResult:
    """The regime available at ``now`` from bars complete at ``now``.

    For the HMM engine, ``model`` must be the fit for ``refit_boundary(spec, now)``; when
    omitted it is fitted deterministically from the same window (restart independence)."""
    now = int(now)
    bars = bars.before(now)
    if isinstance(spec, HtfVolSpec):
        return _htf_state(spec, bars, np.r_[0, np.cumsum(bars.invalid())], now)
    boundary = refit_boundary(spec, now)
    if model is None:
        model = fit(spec, bars, boundary)
    if isinstance(model, Unavailable):
        return Unavailable(model.reason, model.detail, now)
    mismatch = _check_model(spec, model, boundary)
    if mismatch is not None:
        return Unavailable(mismatch.reason, mismatch.detail, now)
    start = boundary - (spec.warm_bars + spec.lookback) * BASE_SECONDS
    window = bars.rows(int(np.searchsorted(bars.timestamp, start)), len(bars))
    features, codes = hmm_features(spec, window)
    last = None
    for minute, log_alpha, code in _hmm_segment(spec, model, window, features, codes, boundary, now):
        last = (minute, log_alpha, code)
    if last is None or last[0] != now - BASE_SECONDS:  # the grid always ends at now - 60
        raise AssertionError("regime filter grid does not end at the latest minute")
    if last[2]:
        return Unavailable(_FEATURE_REASONS[last[2]], "latest minute features", now)
    return _hmm_decision(model, spec, last[1], now, last[0])


@dataclass(frozen=True)
class Replay:
    """Per-row regime for a bar series: row ``i`` is the result available at ``timestamp[i]``."""
    timestamp: np.ndarray
    results: tuple  # RegimeResult per row
    models: tuple   # hmm: (boundary, RegimeModel | Unavailable) per refit; htf_vol: ()

    def labels(self) -> np.ndarray:
        return np.array([r.label if isinstance(r, RegimeState) else None for r in self.results], dtype=object)

    def reasons(self) -> np.ndarray:
        return np.array([r.reason if isinstance(r, Unavailable) else None for r in self.results], dtype=object)

    def digest(self) -> str:
        return _digest([[int(t), asdict(r) if isinstance(r, RegimeState) else {"reason": r.reason, "detail": r.detail}]
                        for t, r in zip(self.timestamp, self.results)])


def replay(spec: RegimeSpec, bars: Bars, models: Optional[dict] = None) -> Replay:
    """Causal replay with the exact evaluate code path.

    ``models`` optionally supplies fitted models by boundary (for example deserialized
    from a live run); missing boundaries are fitted deterministically."""
    stamps = bars.timestamp
    if isinstance(spec, HtfVolSpec):
        invalid_cumsum = np.r_[0, np.cumsum(bars.invalid())]
        cache: dict = {}
        results = []
        for now in stamps:
            # A window for ``now`` reads only bars with open time + 60 <= now (see _htf_window),
            # so the full series gives the same window as evaluate's ``bars.before(now)``.
            results.append(_htf_state(spec, bars, invalid_cumsum, int(now), cache))
        return Replay(timestamp=stamps, results=tuple(results), models=())
    features, codes = hmm_features(spec, bars)
    results: list = [None] * len(stamps)
    fitted = []
    if len(stamps):
        for boundary in range(refit_boundary(spec, int(stamps[0])), int(stamps[-1]) + 1, spec.refit_seconds):
            lo = int(np.searchsorted(stamps, boundary))
            hi = int(np.searchsorted(stamps, boundary + spec.refit_seconds))
            if lo == hi:
                continue
            model = (models or {}).get(boundary)
            if model is None:
                model = fit(spec, bars.before(boundary), boundary)
            fitted.append((boundary, model))
            if isinstance(model, Unavailable):
                for i in range(lo, hi):
                    results[i] = Unavailable(model.reason, model.detail, int(stamps[i]))
                continue
            mismatch = _check_model(spec, model, boundary)
            if mismatch is not None:
                for i in range(lo, hi):
                    results[i] = Unavailable(mismatch.reason, mismatch.detail, int(stamps[i]))
                continue
            posterior = {minute: (log_alpha, code) for minute, log_alpha, code in
                         _hmm_segment(spec, model, bars, features, codes, boundary, int(stamps[hi - 1]))}
            for i in range(lo, hi):
                now = int(stamps[i])
                entry = posterior[now - BASE_SECONDS]  # every minute of the segment is on the grid
                if entry[1]:
                    results[i] = Unavailable(_FEATURE_REASONS[entry[1]], "latest minute features", now)
                else:
                    results[i] = _hmm_decision(model, spec, entry[0], now, now - BASE_SECONDS)
    return Replay(timestamp=stamps, results=tuple(results), models=tuple(fitted))
