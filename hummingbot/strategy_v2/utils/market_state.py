"""
Market State Layer utilities.

Provides forward-looking state signals that complement RSI:
- Kalman local-linear trend with uncertainty
- Efficiency ratio and permutation entropy (trendiness vs chop)
- Return/volume tail detection and capitulation flag
"""
import logging
import math
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class MarketStateSnapshot:
    """
    Snapshot of computed market state signals.

    Attributes:
        timestamp: Unix timestamp for the snapshot.
        price: Latest price used to compute state.
        slope: Estimated trend slope from Kalman filter.
        slope_var: Variance of slope estimate.
        p_slope_up: Probability that slope > 0.
        p_slope_down: Probability that slope < 0.
        efficiency_ratio: ER value (0-1).
        permutation_entropy: Normalized permutation entropy (0-1).
        return_zscore: Z-score of latest return.
        volume_zscore: Z-score of latest volume.
        capitulation: Whether capitulation conditions are met.
        capitulation_score: Strength score (0-1).
    """
    timestamp: float
    price: Optional[float] = None
    slope: Optional[float] = None
    slope_var: Optional[float] = None
    p_slope_up: Optional[float] = None
    p_slope_down: Optional[float] = None
    efficiency_ratio: Optional[float] = None
    permutation_entropy: Optional[float] = None
    return_zscore: Optional[float] = None
    volume_zscore: Optional[float] = None
    capitulation: bool = False
    capitulation_score: Optional[float] = None

    def to_dict(self) -> dict:
        """Return a serializable dict for logging/status.

        Returns:
            Dictionary of snapshot fields.
        """
        return {
            "timestamp": self.timestamp,
            "price": self.price,
            "slope": self.slope,
            "slope_var": self.slope_var,
            "p_slope_up": self.p_slope_up,
            "p_slope_down": self.p_slope_down,
            "efficiency_ratio": self.efficiency_ratio,
            "permutation_entropy": self.permutation_entropy,
            "return_zscore": self.return_zscore,
            "volume_zscore": self.volume_zscore,
            "capitulation": self.capitulation,
            "capitulation_score": self.capitulation_score,
        }


def _normal_cdf(x: float) -> float:
    """Compute the standard normal CDF for a value.

    Args:
        x: Standardized value.

    Returns:
        CDF value in [0, 1].
    """
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _prob_positive(mean: float, var: float) -> float:
    """Compute probability that a normal variable is > 0.

    Args:
        mean: Normal mean.
        var: Normal variance.

    Returns:
        Probability that the variable is positive.
    """
    if var <= 0.0:
        if mean > 0.0:
            return 1.0
        if mean < 0.0:
            return 0.0
        return 0.5
    z = mean / math.sqrt(var)
    return float(_normal_cdf(z))


class KalmanTrendFilter:
    """
    Local-linear Kalman filter for trend estimation.

    State vector: [level, slope]
    Observation: price = level + noise
    """

    def __init__(self, q_level: float = 0.0001, q_slope: float = 0.00001, r: float = 0.001):
        """
        Initialize the filter.

        Args:
            q_level: Process noise for level.
            q_slope: Process noise for slope.
            r: Observation noise.
        """
        self._q_level = float(q_level)
        self._q_slope = float(q_slope)
        self._r = float(r)
        self._x: Optional[np.ndarray] = None
        self._p: Optional[np.ndarray] = None

    def update(self, price: float) -> Tuple[float, float, float, float]:
        """
        Update the filter with a new price observation.

        Args:
            price: Latest price.

        Returns:
            slope, slope_var, p_slope_up, p_slope_down
        """
        if price is None or not np.isfinite(price):
            return 0.0, 0.0, 0.5, 0.5

        if self._x is None:
            self._x = np.array([[float(price)], [0.0]], dtype=float)
            self._p = np.eye(2, dtype=float)

        f = np.array([[1.0, 1.0], [0.0, 1.0]], dtype=float)
        h = np.array([[1.0, 0.0]], dtype=float)
        q = np.array([[self._q_level, 0.0], [0.0, self._q_slope]], dtype=float)
        r = np.array([[self._r]], dtype=float)

        # Predict
        x_pred = f @ self._x
        p_pred = f @ self._p @ f.T + q

        # Update
        y = np.array([[float(price)]], dtype=float) - (h @ x_pred)
        s = h @ p_pred @ h.T + r
        if s[0, 0] <= 0.0:
            self._x = x_pred
            self._p = p_pred
        else:
            k = p_pred @ h.T @ np.linalg.inv(s)
            self._x = x_pred + k @ y
            self._p = (np.eye(2) - k @ h) @ p_pred

        slope = float(self._x[1, 0])
        slope_var = float(max(self._p[1, 1], 0.0))
        p_slope_up = _prob_positive(slope, slope_var)
        p_slope_down = 1.0 - p_slope_up
        return slope, slope_var, p_slope_up, p_slope_down


def calculate_efficiency_ratio(prices: Sequence[float], lookback: int) -> float:
    """
    Calculate the efficiency ratio (ER).

    ER = |price_t - price_{t-n}| / sum(|delta_price| over n)

    Args:
        prices: Sequence of prices.
        lookback: Lookback window length.

    Returns:
        Efficiency ratio in [0, 1]. Returns 0 if insufficient data or zero volatility.
    """
    if lookback <= 0:
        return 0.0
    if prices is None or len(prices) < lookback + 1:
        return 0.0
    window = np.asarray(prices[-(lookback + 1):], dtype=float)
    if np.any(~np.isfinite(window)):
        return 0.0
    direction = abs(window[-1] - window[0])
    volatility = float(np.sum(np.abs(np.diff(window))))
    if volatility <= 0.0:
        return 0.0
    return float(direction / volatility)


def calculate_permutation_entropy(prices: Sequence[float], m: int = 3, tau: int = 1) -> float:
    """
    Calculate normalized permutation entropy (0-1).

    Args:
        prices: Price series.
        m: Embedding dimension.
        tau: Delay between samples.

    Returns:
        Normalized entropy in [0, 1].
    """
    if prices is None or m < 2 or tau < 1:
        return 0.0
    series = np.asarray(prices, dtype=float)
    window = (m - 1) * tau + 1
    if len(series) < window:
        return 0.0

    pattern_counts = {}
    for i in range(0, len(series) - window + 1):
        subseq = series[i:i + window:tau]
        if np.any(~np.isfinite(subseq)):
            continue
        pattern = tuple(np.argsort(subseq))
        pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1

    if not pattern_counts:
        return 0.0

    counts = np.asarray(list(pattern_counts.values()), dtype=float)
    probs = counts / counts.sum()
    entropy = -float(np.sum(probs * np.log(probs)))
    max_entropy = math.log(math.factorial(m))
    if max_entropy <= 0.0:
        return 0.0
    return float(entropy / max_entropy)


def _calculate_zscore(values: Sequence[float], lookback: int) -> float:
    """Calculate the z-score of the latest value over a lookback window.

    Args:
        values: Sequence of values.
        lookback: Lookback window length.

    Returns:
        Z-score of the latest value, or 0.0 if insufficient data/variance.
    """
    if values is None or lookback < 2 or len(values) < lookback:
        return 0.0
    window = np.asarray(values[-lookback:], dtype=float)
    if np.any(~np.isfinite(window)):
        return 0.0
    mean = float(np.mean(window))
    std = float(np.std(window, ddof=0))
    if std <= 0.0:
        return 0.0
    return float((window[-1] - mean) / std)


def calculate_return_zscore(returns: Sequence[float], lookback: int) -> float:
    """
    Calculate z-score of the latest return over a lookback window.

    Args:
        returns: Sequence of returns.
        lookback: Lookback window length.

    Returns:
        Z-score of the latest return.
    """
    return _calculate_zscore(returns, lookback)


def calculate_volume_zscore(volumes: Sequence[float], lookback: int) -> float:
    """
    Calculate z-score of the latest volume over a lookback window.

    Args:
        volumes: Sequence of volumes.
        lookback: Lookback window length.

    Returns:
        Z-score of the latest volume.
    """
    return _calculate_zscore(volumes, lookback)


def detect_capitulation(
    return_z: Optional[float],
    volume_z: Optional[float],
    fragility: Optional[float] = None,
    return_z_threshold: float = -2.0,
    volume_z_threshold: float = 2.0,
    fragility_threshold: Optional[float] = None,
    require_fragility: bool = False,
) -> Tuple[bool, float]:
    """
    Detect capitulation using return tail + volume spike (+ optional fragility).

    Args:
        return_z: Return z-score (negative tail expected).
        volume_z: Volume z-score (positive spike expected).
        fragility: Optional fragility metric (e.g., VPIN or Kyle lambda proxy).
        return_z_threshold: Trigger threshold for return tail.
        volume_z_threshold: Trigger threshold for volume spike.
        fragility_threshold: Optional threshold for fragility.
        require_fragility: If True, fragility must be provided and pass threshold (if set).

    Returns:
        (capitulation_flag, score)
    """
    if return_z is None or volume_z is None:
        return False, 0.0
    return_tail = return_z <= return_z_threshold
    volume_spike = volume_z >= volume_z_threshold

    fragility_ok = True
    if require_fragility:
        if fragility is None:
            fragility_ok = False
        elif fragility_threshold is not None:
            fragility_ok = fragility >= fragility_threshold

    if not (return_tail and volume_spike and fragility_ok):
        return False, 0.0

    return_score = 0.0
    if return_z_threshold != 0.0:
        return_score = min(1.0, abs(return_z) / abs(return_z_threshold))
    volume_score = 0.0
    if volume_z_threshold != 0.0:
        volume_score = min(1.0, volume_z / volume_z_threshold)

    score = (return_score + volume_score) / 2.0
    if require_fragility and fragility is not None and fragility_threshold not in (None, 0.0):
        fragility_score = min(1.0, fragility / fragility_threshold)
        score = (score * 2.0 + fragility_score) / 3.0

    return True, float(score)


def _tick_rule_signs(prices: Sequence[float]) -> np.ndarray:
    """Compute tick-rule signs from a price series.

    Args:
        prices: Sequence of trade prices.

    Returns:
        Array of signs (-1, 0, 1) aligned to prices.
    """
    signs = []
    prev_price = None
    prev_sign = 0
    for price in prices:
        if prev_price is None:
            signs.append(0)
            prev_price = price
            continue
        diff = price - prev_price
        if diff > 0:
            sign = 1
        elif diff < 0:
            sign = -1
        else:
            sign = prev_sign
        signs.append(sign)
        prev_sign = sign
        prev_price = price
    return np.asarray(signs, dtype=float)


def calculate_kyle_lambda(
    trade_prices: Sequence[float],
    trade_sizes: Sequence[float],
    window: int,
) -> Tuple[Optional[float], int]:
    """
    Estimate Kyle's lambda from trade data using tick-rule signed flow.

    Args:
        trade_prices: Sequence of trade prices.
        trade_sizes: Sequence of trade sizes.
        window: Number of trades to use in the rolling regression.

    Returns:
        (lambda, sample_count) where lambda is impact per unit signed flow.
        Falls back to mean(return) / mean(flow) if flow variance is zero.
    """
    if window <= 1:
        return None, 0
    if trade_prices is None or trade_sizes is None:
        return None, 0
    if len(trade_prices) < 2 or len(trade_sizes) < 2:
        return None, 0
    if len(trade_prices) != len(trade_sizes):
        return None, 0

    n = min(window, len(trade_prices))
    prices = np.asarray(trade_prices[-n:], dtype=float)
    sizes = np.asarray(trade_sizes[-n:], dtype=float)
    if np.any(~np.isfinite(prices)) or np.any(~np.isfinite(sizes)):
        return None, 0

    signs = _tick_rule_signs(prices)
    signed_flow = signs * sizes

    returns = np.diff(np.log(prices))
    flow = signed_flow[1:]
    if len(returns) < 2 or len(flow) < 2:
        return None, len(returns)

    flow_var = float(np.var(flow))
    if flow_var <= 0.0:
        mean_flow = float(np.mean(flow))
        mean_return = float(np.mean(returns))
        if mean_flow != 0.0 and mean_return != 0.0:
            return float(mean_return / mean_flow), len(returns)
        return None, len(returns)

    cov = float(np.mean((returns - np.mean(returns)) * (flow - np.mean(flow))))
    kyle_lambda = cov / flow_var
    return float(kyle_lambda), len(returns)


def calculate_vpin_lite(
    trade_prices: Sequence[float],
    trade_sizes: Sequence[float],
    bucket_vol: float,
    window_buckets: int,
) -> Tuple[Optional[float], int]:
    """
    Approximate VPIN using sequential volume buckets.

    Args:
        trade_prices: Sequence of trade prices.
        trade_sizes: Sequence of trade sizes.
        bucket_vol: Target volume per bucket.
        window_buckets: Number of buckets to average.

    Returns:
        (vpin, bucket_count) where vpin is mean imbalance over last buckets.
    """
    if bucket_vol <= 0 or window_buckets <= 0:
        return None, 0
    if trade_prices is None or trade_sizes is None:
        return None, 0
    if len(trade_prices) < 2 or len(trade_sizes) < 2:
        return None, 0
    if len(trade_prices) != len(trade_sizes):
        return None, 0

    prices = np.asarray(trade_prices, dtype=float)
    sizes = np.asarray(trade_sizes, dtype=float)
    if np.any(~np.isfinite(prices)) or np.any(~np.isfinite(sizes)):
        return None, 0

    signs = _tick_rule_signs(prices)
    bucket_imbalances = []
    buy_vol = 0.0
    sell_vol = 0.0
    total_vol = 0.0

    for sign, size in zip(signs, sizes):
        if sign >= 0:
            buy_vol += size
        else:
            sell_vol += size
        total_vol += size
        if total_vol >= bucket_vol:
            imbalance = abs(buy_vol - sell_vol) / bucket_vol
            bucket_imbalances.append(min(1.0, imbalance))
            buy_vol = 0.0
            sell_vol = 0.0
            total_vol = 0.0

    if len(bucket_imbalances) < window_buckets:
        return None, len(bucket_imbalances)

    vpin = float(np.mean(bucket_imbalances[-window_buckets:]))
    return vpin, len(bucket_imbalances)


class LiquidityFragilityTracker:
    """
    Tracks rolling Kyle lambda and VPIN-lite values.

    Designed for trade-level data streams with tick-rule signing.
    """

    def __init__(
        self,
        kyle_window_trades: int = 100,
        vpin_bucket_vol: float = 1000.0,
        vpin_window_buckets: int = 20,
        max_allowed_kyle_lambda: float = 0.01,
        max_allowed_vpin: float = 0.7,
    ):
        self._kyle_window_trades = max(2, int(kyle_window_trades))
        self._vpin_bucket_vol = float(vpin_bucket_vol)
        self._vpin_window_buckets = max(1, int(vpin_window_buckets))
        self._max_allowed_kyle_lambda = float(max_allowed_kyle_lambda)
        self._max_allowed_vpin = float(max_allowed_vpin)

        maxlen = max(self._kyle_window_trades, self._vpin_window_buckets * 50)
        self._prices = []
        self._sizes = []
        self._maxlen = maxlen

        self.kyle_lambda: Optional[float] = None
        self.kyle_sample_count: int = 0
        self.vpin: Optional[float] = None
        self.vpin_bucket_count: int = 0

    def update(self, trade_price: float, trade_size: float) -> None:
        """Append a trade and refresh metrics."""
        if trade_price is None or trade_size is None:
            return
        if not np.isfinite(trade_price) or not np.isfinite(trade_size):
            return
        if trade_size <= 0:
            return

        self._prices.append(float(trade_price))
        self._sizes.append(float(trade_size))
        if len(self._prices) > self._maxlen:
            self._prices = self._prices[-self._maxlen:]
            self._sizes = self._sizes[-self._maxlen:]

        self.kyle_lambda, self.kyle_sample_count = calculate_kyle_lambda(
            self._prices,
            self._sizes,
            window=self._kyle_window_trades,
        )
        self.vpin, self.vpin_bucket_count = calculate_vpin_lite(
            self._prices,
            self._sizes,
            bucket_vol=self._vpin_bucket_vol,
            window_buckets=self._vpin_window_buckets,
        )

    def is_fragile(self) -> bool:
        """Return True if either lambda or VPIN exceeds thresholds."""
        if self.kyle_lambda is not None and self.kyle_lambda >= self._max_allowed_kyle_lambda:
            return True
        if self.vpin is not None and self.vpin >= self._max_allowed_vpin:
            return True
        return False

    def to_dict(self) -> dict:
        """Return a serializable dict for status output."""
        return {
            "kyle_lambda": self.kyle_lambda,
            "kyle_sample_count": self.kyle_sample_count,
            "vpin": self.vpin,
            "vpin_bucket_count": self.vpin_bucket_count,
            "fragile": self.is_fragile(),
        }


class AnchoredVWAP:
    """
    Anchored VWAP calculator with z-score.

    Anchor modes:
    - utc_day: anchor at start of current UTC day
    - last_regime_change: anchor at last detected regime change
    - last_swing: anchor at most recent swing high/low
    """

    def __init__(self, anchor_mode: str = "utc_day", swing_window: int = 3):
        self.anchor_mode = anchor_mode
        self.swing_window = max(1, int(swing_window))

    @staticmethod
    def _anchor_utc_day(timestamps: Sequence[float]) -> Optional[int]:
        if timestamps is None or len(timestamps) == 0:
            return None
        try:
            import datetime as _dt
            last_ts = float(timestamps[-1])
            last_day = _dt.datetime.fromtimestamp(last_ts, tz=_dt.timezone.utc).date()
            for i, ts in enumerate(timestamps):
                if _dt.datetime.fromtimestamp(float(ts), tz=_dt.timezone.utc).date() == last_day:
                    return i
        except Exception:
            return None
        return None

    def _anchor_last_swing(self, prices: Sequence[float]) -> Optional[int]:
        if prices is None or len(prices) < (self.swing_window * 2 + 1):
            return None
        window = self.swing_window
        last_idx = None
        for i in range(window, len(prices) - window):
            local = prices[i - window:i + window + 1]
            if prices[i] == max(local) or prices[i] == min(local):
                last_idx = i
        return last_idx

    @staticmethod
    def _anchor_last_regime_change(regime_labels: Sequence[Optional[str]]) -> Optional[int]:
        if regime_labels is None or len(regime_labels) == 0:
            return None
        last_label = regime_labels[-1]
        for i in range(len(regime_labels) - 1, -1, -1):
            if regime_labels[i] != last_label:
                return i + 1
        return 0

    @staticmethod
    def _compute_from_index(
        prices: Sequence[float],
        volumes: Sequence[float],
        anchor_index: int,
    ) -> Tuple[Optional[float], Optional[float]]:
        if prices is None or volumes is None:
            return None, None
        if len(prices) == 0 or len(prices) != len(volumes):
            return None, None
        if anchor_index is None or anchor_index < 0 or anchor_index >= len(prices):
            return None, None

        prices_arr = np.asarray(prices[anchor_index:], dtype=float)
        volumes_arr = np.asarray(volumes[anchor_index:], dtype=float)
        if np.any(~np.isfinite(prices_arr)) or np.any(~np.isfinite(volumes_arr)):
            return None, None
        total_vol = float(np.sum(volumes_arr))
        if total_vol <= 0.0:
            return None, None

        avwap = float(np.sum(prices_arr * volumes_arr) / total_vol)
        devs = prices_arr - avwap
        std = float(np.std(devs, ddof=0))
        if std <= 0.0:
            zscore = 0.0
        else:
            zscore = float((prices_arr[-1] - avwap) / std)
        return avwap, zscore

    def compute(
        self,
        prices: Sequence[float],
        volumes: Sequence[float],
        timestamps: Optional[Sequence[float]] = None,
        regime_labels: Optional[Sequence[Optional[str]]] = None,
        anchor_index: Optional[int] = None,
    ) -> Tuple[Optional[float], Optional[float], Optional[int]]:
        """
        Compute AVWAP and z-score.

        Args:
            prices: Sequence of prices.
            volumes: Sequence of volumes.
            timestamps: Optional timestamps for utc_day anchor.
            regime_labels: Optional regime labels for last_regime_change anchor.
            anchor_index: Optional explicit anchor index.

        Returns:
            (avwap, zscore, anchor_index)
        """
        idx = anchor_index
        if idx is None:
            if self.anchor_mode == "utc_day":
                idx = self._anchor_utc_day(timestamps or [])
            elif self.anchor_mode == "last_regime_change":
                idx = self._anchor_last_regime_change(regime_labels or [])
            elif self.anchor_mode == "last_swing":
                idx = self._anchor_last_swing(prices)

        if idx is None:
            return None, None, None

        avwap, zscore = self._compute_from_index(prices, volumes, idx)
        return avwap, zscore, idx


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, quantile: float) -> float:
    """Compute a weighted quantile."""
    if len(values) == 0:
        return float("nan")
    order = np.argsort(values)
    values_sorted = values[order]
    weights_sorted = weights[order]
    cumulative = np.cumsum(weights_sorted)
    if cumulative[-1] <= 0:
        return float("nan")
    threshold = quantile * cumulative[-1]
    idx = int(np.searchsorted(cumulative, threshold))
    idx = min(max(idx, 0), len(values_sorted) - 1)
    return float(values_sorted[idx])


def bin_value(value: Optional[float], bins: Sequence[float]) -> Optional[int]:
    """Bin a numeric value into index based on thresholds."""
    if value is None:
        return None
    try:
        numeric = float(value)
    except (ValueError, TypeError):
        return None
    for idx, bound in enumerate(bins):
        if numeric <= bound:
            return idx
    return len(bins)


def regime_to_bin(regime_label: Optional[str]) -> Optional[int]:
    """Map regime label to a coarse bin."""
    if regime_label is None:
        return None
    if "Trend_Down" in regime_label:
        return 0
    if "Range" in regime_label:
        return 1
    if "Trend_Up" in regime_label:
        return 2
    return None


def build_state_key(
    regime_label: Optional[str],
    rsi: Optional[float],
    efficiency_ratio: Optional[float],
    realized_vol: Optional[float],
    impact_metric: Optional[float],
    avwap_z: Optional[float],
) -> Tuple[int, int, int, int, int, int]:
    """
    Build a discretized state key using default bins.

    State key = (regime_bin, rsi_bin, er_bin, vol_bin, impact_bin, avwap_z_bin)
    """
    rsi_bins = [20, 30, 40, 50, 60, 70, 80]
    er_bins = [0.2, 0.4, 0.6, 0.8]
    vol_bins = [0.001, 0.002, 0.003, 0.004, 0.006]
    impact_bins = [0.2, 0.4, 0.6, 0.8]
    avwap_bins = [-2.0, -1.0, 0.0, 1.0, 2.0]

    regime_bin = regime_to_bin(regime_label)
    rsi_bin = bin_value(rsi, rsi_bins)
    er_bin = bin_value(efficiency_ratio, er_bins)
    vol_bin = bin_value(realized_vol, vol_bins)
    impact_bin = bin_value(impact_metric, impact_bins)
    avwap_bin = bin_value(avwap_z, avwap_bins)

    return (
        regime_bin if regime_bin is not None else -1,
        rsi_bin if rsi_bin is not None else -1,
        er_bin if er_bin is not None else -1,
        vol_bin if vol_bin is not None else -1,
        impact_bin if impact_bin is not None else -1,
        avwap_bin if avwap_bin is not None else -1,
    )


class ForwardReturnLearner:
    """
    Online learner for conditional forward returns with no-lookahead labeling.
    """

    def __init__(
        self,
        horizon_seconds: int,
        max_samples_per_state: int = 1000,
        decay_half_life_seconds: Optional[float] = None,
    ):
        self._horizon_seconds = max(1, int(horizon_seconds))
        self._max_samples_per_state = max(10, int(max_samples_per_state))
        self._decay_half_life_seconds = decay_half_life_seconds
        self._decay_lambda = None
        if decay_half_life_seconds and decay_half_life_seconds > 0:
            self._decay_lambda = math.log(2.0) / float(decay_half_life_seconds)
        self._max_age_seconds = None
        if decay_half_life_seconds and decay_half_life_seconds > 0:
            self._max_age_seconds = float(decay_half_life_seconds) * 4.0
        self._buffer = []  # [(timestamp, state_key, price)]
        self._state_returns = {}

    def record(self, state_key, price: float, timestamp: float) -> None:
        """Record a state/price snapshot and label matured samples."""
        if state_key is None or price is None or timestamp is None:
            return
        if not np.isfinite(price) or not np.isfinite(timestamp):
            return
        self._buffer.append((float(timestamp), state_key, float(price)))
        self._flush(float(timestamp), float(price))

    def _flush(self, now_ts: float, current_price: float) -> None:
        """Assign forward returns once horizon has passed."""
        new_buffer = []
        for ts, state_key, price in self._buffer:
            if now_ts - ts >= self._horizon_seconds:
                if price > 0:
                    fwd_ret = (current_price - price) / price
                    self._add_sample(state_key, fwd_ret, now_ts)
            else:
                new_buffer.append((ts, state_key, price))
        self._buffer = new_buffer

    def _add_sample(self, state_key, fwd_return: float, timestamp: float) -> None:
        samples = self._state_returns.get(state_key)
        if samples is None:
            samples = []
            self._state_returns[state_key] = samples
        samples.append((timestamp, float(fwd_return)))
        if len(samples) > self._max_samples_per_state:
            samples[:] = samples[-self._max_samples_per_state:]
        if self._max_age_seconds is not None:
            cutoff = timestamp - self._max_age_seconds
            samples[:] = [s for s in samples if s[0] >= cutoff]

    def _compute_stats(
        self,
        state_key,
        now_ts: float,
        target_x: float,
        target_y: float,
    ) -> Optional[dict]:
        samples = self._state_returns.get(state_key)
        if not samples:
            return None
        returns = np.asarray([r for _, r in samples], dtype=float)
        timestamps = np.asarray([t for t, _ in samples], dtype=float)
        if len(returns) == 0:
            return None
        if self._decay_lambda is None:
            weights = np.ones_like(returns, dtype=float)
        else:
            ages = np.maximum(0.0, now_ts - timestamps)
            weights = np.exp(-self._decay_lambda * ages)
        total_w = float(np.sum(weights))
        if total_w <= 0.0:
            return None
        p_bounce = float(np.sum(weights[returns >= target_x]) / total_w)
        p_dump = float(np.sum(weights[returns <= -target_y]) / total_w)
        q10 = _weighted_quantile(returns, weights, 0.10)
        return {
            "p_bounce": p_bounce,
            "p_dump": p_dump,
            "q10": q10,
            "sample_count": int(len(returns)),
        }

    def get_odds(
        self,
        state_key,
        target_x: float,
        target_y: float,
        min_samples_per_state: int,
        now_ts: float,
        backoff_keys: Optional[Sequence] = None,
    ) -> Optional[dict]:
        """
        Get odds for a state with optional backoff keys.

        Returns:
            Dict with p_bounce, p_dump, q10, sample_count, state_key.
        """
        keys = [state_key]
        if backoff_keys:
            keys.extend(backoff_keys)
        for key in keys:
            stats = self._compute_stats(key, now_ts, target_x, target_y)
            if stats is None:
                continue
            if stats["sample_count"] < min_samples_per_state:
                continue
            stats["state_key"] = key
            return stats
        return None


class MarketStateManager:
    """
    Facade for Market State Layer calculations.
    """

    def __init__(
        self,
        er_lookback: int = 20,
        pe_m: int = 3,
        pe_tau: int = 1,
        return_z_lookback: int = 50,
        volume_z_lookback: int = 50,
        capitulation_return_z: float = -2.0,
        capitulation_volume_z: float = 2.0,
        capitulation_require_fragility: bool = False,
        kalman_filter: Optional[KalmanTrendFilter] = None,
        fragility_tracker: Optional[LiquidityFragilityTracker] = None,
        anchored_vwap: Optional[AnchoredVWAP] = None,
        forward_learner: Optional[ForwardReturnLearner] = None,
    ):
        self.er_lookback = er_lookback
        self.pe_m = pe_m
        self.pe_tau = pe_tau
        self.return_z_lookback = return_z_lookback
        self.volume_z_lookback = volume_z_lookback
        self.capitulation_return_z = capitulation_return_z
        self.capitulation_volume_z = capitulation_volume_z
        self.capitulation_require_fragility = capitulation_require_fragility

        self.kalman_filter = kalman_filter or KalmanTrendFilter()
        self.fragility_tracker = fragility_tracker or LiquidityFragilityTracker()
        self.anchored_vwap = anchored_vwap or AnchoredVWAP()
        self.forward_learner = forward_learner

        self.latest_kyle_lambda: Optional[float] = None
        self.latest_vpin: Optional[float] = None
        self.latest_avwap: Optional[float] = None
        self.latest_avwap_z: Optional[float] = None
        self.latest_avwap_anchor_idx: Optional[int] = None
        self.latest_forward_odds: Optional[dict] = None

    def update_trades(self, trade_price: float, trade_size: float) -> None:
        """Update fragility metrics from trade data."""
        self.fragility_tracker.update(trade_price, trade_size)
        self.latest_kyle_lambda = self.fragility_tracker.kyle_lambda
        self.latest_vpin = self.fragility_tracker.vpin

    def update_from_candles(
        self,
        df,
        timestamp: float,
        use_kalman: bool = True,
        use_efficiency_ratio: bool = True,
        use_permutation_entropy: bool = False,
        use_capitulation: bool = False,
        use_avwap: bool = False,
        avwap_anchor_index: Optional[int] = None,
    ) -> Tuple[MarketStateSnapshot, dict]:
        """
        Compute market state metrics from candle data.

        Returns:
            (snapshot, extras) where extras contains AVWAP/fragility/forward details.
        """
        prices = df["close"].astype(float).tolist() if "close" in df.columns else []
        volumes = df["volume"].astype(float).tolist() if "volume" in df.columns else []
        timestamps = df["timestamp"].astype(float).tolist() if "timestamp" in df.columns else None
        last_price = prices[-1] if prices else None

        slope = slope_var = p_up = p_down = None
        if use_kalman and last_price is not None:
            slope, slope_var, p_up, p_down = self.kalman_filter.update(last_price)

        er = None
        if use_efficiency_ratio:
            er = calculate_efficiency_ratio(prices, self.er_lookback)

        pe = None
        if use_permutation_entropy:
            pe = calculate_permutation_entropy(prices, m=self.pe_m, tau=self.pe_tau)

        ret_z = None
        vol_z = None
        cap_flag = False
        cap_score = None
        if use_capitulation:
            returns = np.diff(prices) / prices[:-1] if len(prices) > 1 else []
            ret_z = calculate_return_zscore(returns, self.return_z_lookback)
            vol_z = calculate_volume_zscore(volumes, self.volume_z_lookback)
            cap_flag, cap_score = detect_capitulation(
                return_z=ret_z,
                volume_z=vol_z,
                fragility=self.fragility_tracker.vpin,
                return_z_threshold=self.capitulation_return_z,
                volume_z_threshold=self.capitulation_volume_z,
                fragility_threshold=None,
                require_fragility=self.capitulation_require_fragility,
            )

        if use_avwap:
            avwap, avwap_z, anchor_idx = self.anchored_vwap.compute(
                prices,
                volumes,
                timestamps=timestamps,
                anchor_index=avwap_anchor_index,
            )
            self.latest_avwap = avwap
            self.latest_avwap_z = avwap_z
            self.latest_avwap_anchor_idx = anchor_idx

        snapshot = MarketStateSnapshot(
            timestamp=timestamp,
            price=last_price,
            slope=slope,
            slope_var=slope_var,
            p_slope_up=p_up,
            p_slope_down=p_down,
            efficiency_ratio=er,
            permutation_entropy=pe,
            return_zscore=ret_z,
            volume_zscore=vol_z,
            capitulation=cap_flag,
            capitulation_score=cap_score,
        )

        extras = {
            "kyle_lambda": self.fragility_tracker.kyle_lambda,
            "vpin": self.fragility_tracker.vpin,
            "avwap": self.latest_avwap,
            "avwap_z": self.latest_avwap_z,
            "avwap_anchor_idx": self.latest_avwap_anchor_idx,
            "forward_odds": self.latest_forward_odds,
        }
        return snapshot, extras

    def update_forward_odds(
        self,
        state_key,
        price: float,
        timestamp: float,
        target_x: float,
        target_y: float,
        min_samples: int,
        backoff_keys: Optional[Sequence] = None,
    ) -> Optional[dict]:
        """Update and query forward odds if a learner is configured."""
        if self.forward_learner is None:
            return None
        self.forward_learner.record(state_key, price, timestamp)
        odds = self.forward_learner.get_odds(
            state_key=state_key,
            target_x=target_x,
            target_y=target_y,
            min_samples_per_state=min_samples,
            now_ts=timestamp,
            backoff_keys=backoff_keys,
        )
        self.latest_forward_odds = odds
        return odds
