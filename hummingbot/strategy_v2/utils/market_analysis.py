"""
Market Analysis Module.

Provides comprehensive market analysis utilities including:
- HMM-based market regime detection with rule-based fallback
- Generic price-indicator divergence detection
- Volume analysis and confirmation
- Support/resistance level detection
- Market condition scoring
- Multi-timeframe trend alignment (ADX/DI)

Enhanced in v1.1 to provide shared utilities for strategy controllers.
"""
import logging
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from hmmlearn.hmm import GaussianHMM
except ImportError:
    GaussianHMM = None

logger = logging.getLogger(__name__)


class StrictHMMRequiredError(RuntimeError):
    """Raised when strict HMM mode cannot use a real GaussianHMM backend."""


# =============================================================================
# ENUMS AND DATA CLASSES
# =============================================================================

class DivergenceType(Enum):
    """Types of price-indicator divergence patterns."""
    NONE = "none"
    BULLISH_REGULAR = "bullish_regular"      # Price lower low, indicator higher low
    BULLISH_HIDDEN = "bullish_hidden"        # Price higher low, indicator lower low (trend continuation)
    BEARISH_REGULAR = "bearish_regular"      # Price higher high, indicator lower high
    BEARISH_HIDDEN = "bearish_hidden"        # Price lower high, indicator higher high (trend continuation)


class VolumeCondition(Enum):
    """Volume condition classifications."""
    VERY_LOW = "very_low"      # < 0.5x average
    LOW = "low"                # 0.5x - 0.8x average
    NORMAL = "normal"          # 0.8x - 1.2x average
    HIGH = "high"              # 1.2x - 2.0x average
    VERY_HIGH = "very_high"    # > 2.0x average
    CLIMAX = "climax"          # > 3.0x average (potential exhaustion)


class TrendDirection(Enum):
    """Trend direction classification."""
    UP = "up"
    DOWN = "down"
    RANGE = "range"

    def to_int(self) -> int:
        """Convert direction to numeric value."""
        if self == TrendDirection.UP:
            return 1
        if self == TrendDirection.DOWN:
            return -1
        return 0


@dataclass
class MarketRegime:
    """
    Market regime classification result.

    Attributes:
        timestamp: Detection timestamp
        regime_label: One of: LV_Range, HV_Range, LV_Trend_Up, HV_Trend_Up, LV_Trend_Down, HV_Trend_Down
        confidence: Detection confidence (0-1)
        volatility_level: low/medium/high
        is_trending: Whether market is trending
        trend_direction: 1 (up), -1 (down), 0 (range)
        trend_strength: Normalized ADX (0-1)
        state_probabilities: HMM state probabilities if available
    """
    timestamp: float
    regime_label: str
    confidence: float = 1.0
    volatility_level: str = "medium"
    is_trending: bool = False
    trend_direction: int = 0
    trend_strength: float = 0.0
    state_probabilities: Optional[Dict[str, float]] = None

    def __str__(self):
        t = pd.Timestamp(self.timestamp, unit='s').strftime('%H:%M:%S')
        td = "UP" if self.trend_direction == 1 else "DOWN" if self.trend_direction == -1 else "NO"
        return (f"Regime({self.regime_label}@{t} | Vol:{self.volatility_level} | "
                f"Trend:{td} | Str:{self.trend_strength:.2f} | Conf:{self.confidence:.2f})")


@dataclass
class DivergenceResult:
    """
    Result of divergence detection.

    Attributes:
        divergence_type: Type of divergence detected
        strength: Divergence strength (0-1)
        price_point_1: First price extremum
        price_point_2: Second price extremum
        indicator_point_1: First indicator extremum
        indicator_point_2: Second indicator extremum
        bars_between: Number of bars between extrema
    """
    divergence_type: DivergenceType = DivergenceType.NONE
    strength: float = 0.0
    price_point_1: Optional[float] = None
    price_point_2: Optional[float] = None
    indicator_point_1: Optional[float] = None
    indicator_point_2: Optional[float] = None
    bars_between: int = 0

    def is_bullish(self) -> bool:
        """Check if divergence is bullish (regular or hidden)."""
        return self.divergence_type in (DivergenceType.BULLISH_REGULAR, DivergenceType.BULLISH_HIDDEN)

    def is_bearish(self) -> bool:
        """Check if divergence is bearish (regular or hidden)."""
        return self.divergence_type in (DivergenceType.BEARISH_REGULAR, DivergenceType.BEARISH_HIDDEN)

    def to_dict(self) -> dict:
        return {
            "type": self.divergence_type.value,
            "strength": self.strength,
            "price_1": self.price_point_1,
            "price_2": self.price_point_2,
            "indicator_1": self.indicator_point_1,
            "indicator_2": self.indicator_point_2,
            "bars_between": self.bars_between,
        }


@dataclass
class VolumeAnalysis:
    """
    Comprehensive volume analysis result.

    Attributes:
        current_volume: Current bar volume
        average_volume: Moving average volume
        volume_ratio: current / average
        condition: Volume condition classification
        is_increasing: Volume trend direction
        accumulation_score: Accumulation/distribution score (-1 to 1)
    """
    current_volume: float = 0.0
    average_volume: float = 0.0
    volume_ratio: float = 1.0
    condition: VolumeCondition = VolumeCondition.NORMAL
    is_increasing: bool = False
    accumulation_score: float = 0.0  # Positive = accumulation, Negative = distribution

    def confirms_buy(self) -> bool:
        """Volume confirms buy signal (above average, accumulating)."""
        return self.volume_ratio >= 1.0 and self.accumulation_score > 0

    def confirms_sell(self) -> bool:
        """Volume confirms sell signal (above average, distributing)."""
        return self.volume_ratio >= 1.0 and self.accumulation_score < 0

    def to_dict(self) -> dict:
        return {
            "current": self.current_volume,
            "average": self.average_volume,
            "ratio": self.volume_ratio,
            "condition": self.condition.value,
            "increasing": self.is_increasing,
            "accumulation": self.accumulation_score,
        }


@dataclass
class SupportResistance:
    """
    Support and resistance levels.

    Attributes:
        supports: List of support levels (price, strength)
        resistances: List of resistance levels (price, strength)
        nearest_support: Nearest support below current price
        nearest_resistance: Nearest resistance above current price
        price_position: Where price is relative to S/R (0=at support, 1=at resistance)
    """
    supports: List[Tuple[float, float]] = field(default_factory=list)
    resistances: List[Tuple[float, float]] = field(default_factory=list)
    nearest_support: Optional[float] = None
    nearest_resistance: Optional[float] = None
    price_position: float = 0.5  # 0 = at support, 1 = at resistance

    def distance_to_support_pct(self, current_price: float) -> Optional[float]:
        """Distance to nearest support as percentage."""
        if self.nearest_support is None or current_price <= 0:
            return None
        return (current_price - self.nearest_support) / current_price

    def distance_to_resistance_pct(self, current_price: float) -> Optional[float]:
        """Distance to nearest resistance as percentage."""
        if self.nearest_resistance is None or current_price <= 0:
            return None
        return (self.nearest_resistance - current_price) / current_price


@dataclass
class MarketCondition:
    """
    Comprehensive market condition assessment.

    Combines regime, volume, divergence, and S/R analysis
    into a single tradability score.
    """
    timestamp: float = 0.0
    regime: Optional[MarketRegime] = None
    volume: Optional[VolumeAnalysis] = None
    divergence: Optional[DivergenceResult] = None
    support_resistance: Optional[SupportResistance] = None

    # Composite scores
    buy_score: float = 0.0      # 0-1, higher = better buy opportunity
    sell_score: float = 0.0    # 0-1, higher = better sell opportunity
    tradability: float = 0.5   # 0-1, overall market tradability

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "regime": self.regime.regime_label if self.regime else None,
            "volume_condition": self.volume.condition.value if self.volume else None,
            "divergence": self.divergence.divergence_type.value if self.divergence else None,
            "buy_score": self.buy_score,
            "sell_score": self.sell_score,
            "tradability": self.tradability,
        }


@dataclass
class TrendState:
    """
    Trend state for a single timeframe.

    Attributes:
        timeframe: Timeframe label (e.g., short/medium/long or 1m/15m/1h)
        direction: Trend direction classification
        strength: Trend strength (0-1) based on ADX normalization
        adx: Raw ADX value
        plus_di: +DI value
        minus_di: -DI value
        is_valid: Whether trend could be computed from data
    """
    timeframe: str
    direction: TrendDirection = TrendDirection.RANGE
    strength: float = 0.0
    adx: float = 0.0
    plus_di: float = 0.0
    minus_di: float = 0.0
    is_valid: bool = False

    def to_dict(self) -> dict:
        return {
            "timeframe": self.timeframe,
            "direction": self.direction.value,
            "strength": self.strength,
            "adx": self.adx,
            "plus_di": self.plus_di,
            "minus_di": self.minus_di,
            "is_valid": self.is_valid,
        }


@dataclass
class MultiTimeframeTrend:
    """
    Multi-timeframe trend summary.

    Attributes:
        states: Mapping of timeframe labels to TrendState
        alignment_score: Weighted alignment score (-1 to 1)
        direction: Aggregate trend direction
        confidence: Absolute alignment score (0-1)
    """
    states: Dict[str, TrendState] = field(default_factory=dict)
    alignment_score: float = 0.0
    direction: TrendDirection = TrendDirection.RANGE
    confidence: float = 0.0

    def to_dict(self) -> dict:
        return {
            "alignment_score": self.alignment_score,
            "direction": self.direction.value,
            "confidence": self.confidence,
            "states": {k: v.to_dict() for k, v in self.states.items()},
        }


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def find_local_extrema(
    series: pd.Series,
    window: int = 5,
    max_extrema: int = 5
) -> Tuple[List[int], List[int]]:
    """
    Find local maxima and minima in a series.

    Args:
        series: Price or indicator series
        window: Lookback window for extrema detection
        max_extrema: Maximum number of extrema to return

    Returns:
        Tuple of (maxima_indices, minima_indices)
    """
    if len(series) < window * 2:
        return [], []

    maxima_idx = []
    minima_idx = []

    values = series.values
    for i in range(window, len(values) - window):
        # Check for local maximum
        is_max = all(values[i] >= values[i - j] for j in range(1, window + 1))
        is_max = is_max and all(values[i] >= values[i + j] for j in range(1, window + 1))
        if is_max:
            maxima_idx.append(i)

        # Check for local minimum
        is_min = all(values[i] <= values[i - j] for j in range(1, window + 1))
        is_min = is_min and all(values[i] <= values[i + j] for j in range(1, window + 1))
        if is_min:
            minima_idx.append(i)

    return maxima_idx[-max_extrema:], minima_idx[-max_extrema:]


def detect_divergence(
    df: pd.DataFrame,
    indicator_col: str,
    price_col: str = "close",
    lookback: int = 30,
    min_bars_between: int = 3,
    tolerance: float = 0.02,
    oversold_level: float = 30.0,
    overbought_level: float = 70.0,
) -> DivergenceResult:
    """
    Generic divergence detection between price and any oscillator.

    Works with RSI, Stochastic, CCI, Williams %R, etc.

    Bullish Regular Divergence:
        - Price makes LOWER LOW
        - Indicator makes HIGHER LOW
        - Best when indicator is in oversold zone

    Bearish Regular Divergence:
        - Price makes HIGHER HIGH
        - Indicator makes LOWER HIGH
        - Best when indicator is in overbought zone

    Args:
        df: DataFrame with indicator and price columns
        indicator_col: Name of oscillator column (e.g., 'RSI_14', 'STOCH_K')
        price_col: Name of price column
        lookback: Number of bars to look back
        min_bars_between: Minimum bars between extrema
        tolerance: Price tolerance for "equal" levels (as fraction)
        oversold_level: Indicator oversold threshold (for strength bonus)
        overbought_level: Indicator overbought threshold (for strength bonus)

    Returns:
        DivergenceResult with detection details
    """
    result = DivergenceResult()

    if df is None or len(df) < lookback:
        return result

    if indicator_col not in df.columns or price_col not in df.columns:
        return result

    # Get recent data
    recent = df.tail(lookback).copy()
    price = recent[price_col].astype(float)
    indicator = recent[indicator_col].astype(float)

    if price.isna().all() or indicator.isna().all():
        return result

    # Find local extrema
    price_highs, price_lows = find_local_extrema(price, window=3, max_extrema=4)
    ind_highs, ind_lows = find_local_extrema(indicator, window=3, max_extrema=4)

    # Check for Bullish Divergence (at lows)
    if len(price_lows) >= 2 and len(ind_lows) >= 2:
        p_low_1 = price.iloc[price_lows[-1]]
        p_low_2 = price.iloc[price_lows[-2]]
        i_low_1 = indicator.iloc[ind_lows[-1]]
        i_low_2 = indicator.iloc[ind_lows[-2]]
        bars = abs(price_lows[-1] - price_lows[-2])

        if bars >= min_bars_between:
            # Bullish Regular: Price lower low, Indicator higher low
            if p_low_1 < p_low_2 * (1 - tolerance) and i_low_1 > i_low_2:
                # Calculate strength
                ind_diff = i_low_1 - i_low_2
                oversold_bonus = max(0, (oversold_level - i_low_1) / oversold_level) if i_low_1 < oversold_level else 0
                strength = min(1.0, (ind_diff / 15) + oversold_bonus * 0.4)

                result.divergence_type = DivergenceType.BULLISH_REGULAR
                result.strength = round(max(0.0, strength), 3)
                result.price_point_1 = float(p_low_1)
                result.price_point_2 = float(p_low_2)
                result.indicator_point_1 = float(i_low_1)
                result.indicator_point_2 = float(i_low_2)
                result.bars_between = bars
                return result

            # Bullish Hidden: Price higher low, Indicator lower low
            elif p_low_1 > p_low_2 * (1 + tolerance) and i_low_1 < i_low_2:
                ind_diff = abs(i_low_2 - i_low_1)
                strength = min(1.0, ind_diff / 15) * 0.7

                result.divergence_type = DivergenceType.BULLISH_HIDDEN
                result.strength = round(max(0.0, strength), 3)
                result.price_point_1 = float(p_low_1)
                result.price_point_2 = float(p_low_2)
                result.indicator_point_1 = float(i_low_1)
                result.indicator_point_2 = float(i_low_2)
                result.bars_between = bars
                return result

    # Check for Bearish Divergence (at highs)
    if len(price_highs) >= 2 and len(ind_highs) >= 2:
        p_high_1 = price.iloc[price_highs[-1]]
        p_high_2 = price.iloc[price_highs[-2]]
        i_high_1 = indicator.iloc[ind_highs[-1]]
        i_high_2 = indicator.iloc[ind_highs[-2]]
        bars = abs(price_highs[-1] - price_highs[-2])

        if bars >= min_bars_between:
            # Bearish Regular: Price higher high, Indicator lower high
            if p_high_1 > p_high_2 * (1 + tolerance) and i_high_1 < i_high_2:
                ind_diff = i_high_2 - i_high_1
                overbought_bonus = max(0, (i_high_1 - overbought_level) / (100 - overbought_level)) if i_high_1 > overbought_level else 0
                strength = min(1.0, (ind_diff / 15) + overbought_bonus * 0.4)

                result.divergence_type = DivergenceType.BEARISH_REGULAR
                result.strength = round(max(0.0, strength), 3)
                result.price_point_1 = float(p_high_1)
                result.price_point_2 = float(p_high_2)
                result.indicator_point_1 = float(i_high_1)
                result.indicator_point_2 = float(i_high_2)
                result.bars_between = bars
                return result

            # Bearish Hidden: Price lower high, Indicator higher high
            elif p_high_1 < p_high_2 * (1 - tolerance) and i_high_1 > i_high_2:
                ind_diff = abs(i_high_1 - i_high_2)
                strength = min(1.0, ind_diff / 15) * 0.7

                result.divergence_type = DivergenceType.BEARISH_HIDDEN
                result.strength = round(max(0.0, strength), 3)
                result.price_point_1 = float(p_high_1)
                result.price_point_2 = float(p_high_2)
                result.indicator_point_1 = float(i_high_1)
                result.indicator_point_2 = float(i_high_2)
                result.bars_between = bars
                return result

    return result


def analyze_volume(
    df: pd.DataFrame,
    lookback: int = 20,
    volume_col: Optional[str] = None,
) -> VolumeAnalysis:
    """
    Comprehensive volume analysis.

    Calculates:
    - Volume ratio (current vs average)
    - Volume condition classification
    - Volume trend direction
    - Accumulation/Distribution score

    Args:
        df: DataFrame with OHLCV data
        lookback: Bars for moving average
        volume_col: Volume column name (auto-detects if None)

    Returns:
        VolumeAnalysis with all metrics
    """
    result = VolumeAnalysis()

    if df is None or len(df) < lookback:
        return result

    # Auto-detect volume column
    vol_cols = ["quote_volume", "volume", "base_volume"]
    vol_series = None

    if volume_col and volume_col in df.columns:
        vol_series = df[volume_col].astype(float)
    else:
        for col in vol_cols:
            if col in df.columns:
                try:
                    vol_series = df[col].astype(float)
                    break
                except (ValueError, TypeError):
                    continue

    if vol_series is None or vol_series.empty:
        return result

    # Basic metrics
    result.current_volume = float(vol_series.iloc[-1])
    result.average_volume = float(vol_series.tail(lookback).mean())

    if result.average_volume > 0:
        result.volume_ratio = round(result.current_volume / result.average_volume, 3)

    # Classify condition
    ratio = result.volume_ratio
    if ratio < 0.5:
        result.condition = VolumeCondition.VERY_LOW
    elif ratio < 0.8:
        result.condition = VolumeCondition.LOW
    elif ratio < 1.2:
        result.condition = VolumeCondition.NORMAL
    elif ratio < 2.0:
        result.condition = VolumeCondition.HIGH
    elif ratio < 3.0:
        result.condition = VolumeCondition.VERY_HIGH
    else:
        result.condition = VolumeCondition.CLIMAX

    # Volume trend
    if len(vol_series) >= 5:
        recent_avg = vol_series.tail(5).mean()
        prior_avg = vol_series.tail(lookback).head(lookback - 5).mean()
        result.is_increasing = recent_avg > prior_avg

    # Accumulation/Distribution score
    if all(col in df.columns for col in ["high", "low", "close"]):
        try:
            high = df["high"].astype(float)
            low = df["low"].astype(float)
            close = df["close"].astype(float)

            # Money Flow Multiplier: ((close - low) - (high - close)) / (high - low)
            hl_range = high - low
            mfm = ((close - low) - (high - close)) / hl_range.replace(0, np.nan)
            mfm = mfm.fillna(0)

            # Recent accumulation vs prior
            recent_mfm = mfm.tail(5).mean()
            prior_mfm = mfm.tail(lookback).head(lookback - 5).mean()

            result.accumulation_score = round(float(recent_mfm - prior_mfm), 3)
        except Exception:
            pass

    return result


def detect_support_resistance(
    df: pd.DataFrame,
    lookback: int = 100,
    num_levels: int = 3,
    clustering_pct: float = 0.005,
) -> SupportResistance:
    """
    Detect support and resistance levels using swing points.

    Args:
        df: DataFrame with OHLCV data
        lookback: Number of bars to analyze
        num_levels: Maximum number of S/R levels to return
        clustering_pct: Percentage for clustering nearby levels

    Returns:
        SupportResistance with detected levels
    """
    result = SupportResistance()

    if df is None or len(df) < 20:
        return result

    recent = df.tail(lookback).copy()

    if "high" not in recent.columns or "low" not in recent.columns:
        return result

    high = recent["high"].astype(float)
    low = recent["low"].astype(float)
    close = recent["close"].astype(float)
    current_price = float(close.iloc[-1])

    # Find swing highs and lows
    _, swing_lows = find_local_extrema(low, window=5, max_extrema=10)
    swing_highs, _ = find_local_extrema(high, window=5, max_extrema=10)

    # Extract price levels
    support_levels = [float(low.iloc[i]) for i in swing_lows if i < len(low)]
    resistance_levels = [float(high.iloc[i]) for i in swing_highs if i < len(high)]

    # Cluster nearby levels
    def cluster_levels(levels: List[float], threshold: float) -> List[Tuple[float, float]]:
        if not levels:
            return []

        levels = sorted(levels)
        clusters = []
        current_cluster = [levels[0]]

        for level in levels[1:]:
            if abs(level - current_cluster[-1]) / current_cluster[-1] < threshold:
                current_cluster.append(level)
            else:
                avg_level = sum(current_cluster) / len(current_cluster)
                strength = len(current_cluster) / len(levels)
                clusters.append((avg_level, round(strength, 2)))
                current_cluster = [level]

        # Add last cluster
        if current_cluster:
            avg_level = sum(current_cluster) / len(current_cluster)
            strength = len(current_cluster) / len(levels)
            clusters.append((avg_level, round(strength, 2)))

        return clusters

    # Get clustered levels
    support_clusters = cluster_levels(support_levels, clustering_pct)
    resistance_clusters = cluster_levels(resistance_levels, clustering_pct)

    # Filter: supports below price, resistances above price
    result.supports = [(p, s) for p, s in support_clusters if p < current_price][-num_levels:]
    result.resistances = [(p, s) for p, s in resistance_clusters if p > current_price][:num_levels]

    # Find nearest levels
    if result.supports:
        result.nearest_support = max(p for p, _ in result.supports)
    if result.resistances:
        result.nearest_resistance = min(p for p, _ in result.resistances)

    # Calculate price position
    if result.nearest_support and result.nearest_resistance:
        sr_range = result.nearest_resistance - result.nearest_support
        if sr_range > 0:
            result.price_position = (current_price - result.nearest_support) / sr_range

    return result


def calculate_market_condition(
    df: pd.DataFrame,
    regime: Optional[MarketRegime] = None,
    indicator_col: Optional[str] = None,
    oversold_level: float = 30.0,
    overbought_level: float = 70.0,
) -> MarketCondition:
    """
    Calculate comprehensive market condition.

    Combines regime, volume, divergence, and S/R analysis
    into composite scores for trading decisions.

    Args:
        df: DataFrame with OHLCV and indicator data
        regime: Pre-calculated regime (optional)
        indicator_col: Oscillator column for divergence (e.g., 'RSI_14')
        oversold_level: Oscillator oversold threshold
        overbought_level: Oscillator overbought threshold

    Returns:
        MarketCondition with all analysis and scores
    """
    condition = MarketCondition(timestamp=time.time())

    if df is None or df.empty:
        return condition

    # Regime
    condition.regime = regime

    # Volume analysis
    condition.volume = analyze_volume(df)

    # Divergence (if indicator provided)
    if indicator_col and indicator_col in df.columns:
        condition.divergence = detect_divergence(
            df, indicator_col,
            oversold_level=oversold_level,
            overbought_level=overbought_level
        )

    # Support/Resistance
    condition.support_resistance = detect_support_resistance(df)

    # Calculate composite scores
    buy_factors = []
    sell_factors = []
    tradability_factors = []

    # Regime contribution
    if regime:
        if "Trend_Up" in regime.regime_label:
            buy_factors.append(0.3 * regime.confidence)
        elif "Trend_Down" in regime.regime_label:
            sell_factors.append(0.3 * regime.confidence)

        if regime.volatility_level == "low":
            tradability_factors.append(0.6)  # Low vol = safer
        elif regime.volatility_level == "high":
            tradability_factors.append(0.4)  # High vol = riskier
        else:
            tradability_factors.append(0.5)

    # Volume contribution
    if condition.volume:
        vol = condition.volume
        if vol.condition in (VolumeCondition.HIGH, VolumeCondition.VERY_HIGH):
            tradability_factors.append(0.7)  # Good volume = tradable
            if vol.accumulation_score > 0.1:
                buy_factors.append(0.2)
            elif vol.accumulation_score < -0.1:
                sell_factors.append(0.2)
        elif vol.condition in (VolumeCondition.VERY_LOW, VolumeCondition.LOW):
            tradability_factors.append(0.3)  # Low volume = risky
        else:
            tradability_factors.append(0.5)

    # Divergence contribution
    if condition.divergence and condition.divergence.divergence_type != DivergenceType.NONE:
        div = condition.divergence
        if div.is_bullish():
            buy_factors.append(0.4 * div.strength)
        elif div.is_bearish():
            sell_factors.append(0.4 * div.strength)

    # S/R contribution
    if condition.support_resistance:
        sr = condition.support_resistance
        if sr.price_position < 0.2:  # Near support
            buy_factors.append(0.2)
        elif sr.price_position > 0.8:  # Near resistance
            sell_factors.append(0.2)

    # Compute final scores
    condition.buy_score = round(sum(buy_factors) / max(1, len(buy_factors)) if buy_factors else 0.0, 3)
    condition.sell_score = round(sum(sell_factors) / max(1, len(sell_factors)) if sell_factors else 0.0, 3)
    condition.tradability = round(sum(tradability_factors) / max(1, len(tradability_factors)) if tradability_factors else 0.5, 3)

    return condition


# =============================================================================
# TREND ANALYSIS (MULTI-TIMEFRAME)
# =============================================================================

def _compute_adx_di(df: pd.DataFrame, length: int = 14) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Compute ADX and directional indicators (+DI, -DI).

    Args:
        df: DataFrame with high, low, close columns
        length: ADX length

    Returns:
        Tuple of (adx, plus_di, minus_di) series
    """
    d = df.copy()
    for col in ["high", "low", "close"]:
        d[col] = pd.to_numeric(d[col], errors="coerce")
    d.dropna(subset=["high", "low", "close"], inplace=True)
    if len(d) < max(30, length + 2):
        return pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float)

    high = d["high"]
    low = d["low"]
    close = d["close"]

    tr = np.maximum(
        high - low,
        np.maximum((high - close.shift(1)).abs(), (low - close.shift(1)).abs())
    )
    atr_w = tr.ewm(alpha=1 / length, adjust=False).mean()

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    plus_di = 100 * (
        pd.Series(plus_dm, index=d.index).ewm(alpha=1 / length, adjust=False).mean() / atr_w
    ).replace([np.inf, -np.inf], 0).fillna(0)
    minus_di = 100 * (
        pd.Series(minus_dm, index=d.index).ewm(alpha=1 / length, adjust=False).mean() / atr_w
    ).replace([np.inf, -np.inf], 0).fillna(0)

    dx = (100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))).fillna(0)
    adx = dx.ewm(alpha=1 / length, adjust=False).mean()

    return adx, plus_di, minus_di


def calculate_trend_state(
    df: pd.DataFrame,
    timeframe: str,
    adx_length: int = 14,
    adx_threshold: float = 18.0
) -> TrendState:
    """
    Calculate trend state for a single timeframe using ADX/DI.

    Args:
        df: DataFrame with OHLCV data
        timeframe: Label for the timeframe (e.g., "1m", "15m")
        adx_length: ADX calculation length
        adx_threshold: Minimum ADX to classify a trend

    Returns:
        TrendState result
    """
    req = {"high", "low", "close"}
    if df is None or df.empty or not req.issubset(set(df.columns)):
        return TrendState(timeframe=timeframe, is_valid=False)

    adx, plus_di, minus_di = _compute_adx_di(df, length=adx_length)
    if adx.empty or plus_di.empty or minus_di.empty:
        return TrendState(timeframe=timeframe, is_valid=False)

    last_adx = float(adx.iloc[-1]) if np.isfinite(adx.iloc[-1]) else 0.0
    last_plus = float(plus_di.iloc[-1]) if np.isfinite(plus_di.iloc[-1]) else 0.0
    last_minus = float(minus_di.iloc[-1]) if np.isfinite(minus_di.iloc[-1]) else 0.0

    direction = TrendDirection.RANGE
    if last_adx >= adx_threshold:
        if last_plus > last_minus:
            direction = TrendDirection.UP
        elif last_minus > last_plus:
            direction = TrendDirection.DOWN

    strength = round(float(np.clip(last_adx / 50.0, 0.0, 1.0)), 3)

    return TrendState(
        timeframe=timeframe,
        direction=direction,
        strength=strength,
        adx=round(last_adx, 3),
        plus_di=round(last_plus, 3),
        minus_di=round(last_minus, 3),
        is_valid=True
    )


def analyze_multi_timeframe_trend(
    dfs: Dict[str, pd.DataFrame],
    weights: Optional[Dict[str, float]] = None,
    adx_length: int = 14,
    adx_threshold: float = 18.0,
    neutral_band: float = 0.05
) -> MultiTimeframeTrend:
    """
    Analyze trend alignment across multiple timeframes.

    Args:
        dfs: Mapping of timeframe label to OHLCV DataFrame
        weights: Optional weights per timeframe label
        adx_length: ADX calculation length
        adx_threshold: Minimum ADX to classify a trend
        neutral_band: Alignment score range treated as range

    Returns:
        MultiTimeframeTrend summary
    """
    states: Dict[str, TrendState] = {}
    valid_states: Dict[str, TrendState] = {}

    for tf, df in (dfs or {}).items():
        state = calculate_trend_state(df, timeframe=tf, adx_length=adx_length, adx_threshold=adx_threshold)
        states[tf] = state
        if state.is_valid:
            valid_states[tf] = state

    if not valid_states:
        return MultiTimeframeTrend(states=states)

    if weights is None:
        if {"short", "medium", "long"}.issubset(valid_states.keys()):
            weights = {"short": 0.2, "medium": 0.3, "long": 0.5}
        else:
            weights = {tf: 1.0 for tf in valid_states.keys()}

    weight_sum = sum(float(weights.get(tf, 0.0)) for tf in valid_states.keys())
    if weight_sum <= 0:
        norm_weights = {tf: 1.0 / len(valid_states) for tf in valid_states.keys()}
    else:
        norm_weights = {tf: float(weights.get(tf, 0.0)) / weight_sum for tf in valid_states.keys()}

    alignment = 0.0
    for tf, state in valid_states.items():
        alignment += norm_weights.get(tf, 0.0) * state.direction.to_int() * state.strength

    alignment = round(float(np.clip(alignment, -1.0, 1.0)), 3)
    if alignment > neutral_band:
        direction = TrendDirection.UP
    elif alignment < -neutral_band:
        direction = TrendDirection.DOWN
    else:
        direction = TrendDirection.RANGE

    return MultiTimeframeTrend(
        states=states,
        alignment_score=alignment,
        direction=direction,
        confidence=round(abs(alignment), 3),
    )


# =============================================================================
# MARKET REGIME DETECTOR (Original + Enhanced)
# =============================================================================

class MarketRegimeDetector:
    """
    HMM-based market regime detection with optional strict no-fallback mode.

    Identifies six regimes:
    - LV_Range: Low volatility, ranging market
    - HV_Range: High volatility, ranging market
    - LV_Trend_Up: Low volatility uptrend
    - HV_Trend_Up: High volatility uptrend
    - LV_Trend_Down: Low volatility downtrend
    - HV_Trend_Down: High volatility downtrend
    """

    REGIME_STATES = {
        0: "LV_Range",
        1: "HV_Range",
        2: "LV_Trend_Up",
        3: "HV_Trend_Up",
        4: "LV_Trend_Down",
        5: "HV_Trend_Down"
    }

    def __init__(
        self,
        n_hmm_states: int = 6,
        features: Optional[List[str]] = None,
        vol_ema_period: int = 21,
        vol_low_q: float = 0.35,
        vol_high_q: float = 0.65,
        trend_adx_threshold: float = 18.0,
        smoothing_window: int = 7,
        htf_confirm: bool = True,
        hmm_covariance_type: str = "diag",
        hmm_n_iter: int = 100,
        use_hmm: bool = True,
        random_state: int = 42,
        require_hmm: bool = False,
        allow_rule_based_fallback: bool = True,
        min_hmm_samples: int = 200,
    ):
        """
        Initialize the MarketRegimeDetector.

        Args:
            n_hmm_states: Number of HMM hidden states
            features: Feature columns for HMM (default: log_return, volatility, adx, volume_ratio)
            vol_ema_period: EMA period for volatility smoothing
            vol_low_q: Quantile threshold for low volatility
            vol_high_q: Quantile threshold for high volatility
            trend_adx_threshold: ADX threshold for trend detection
            smoothing_window: Window for regime smoothing
            htf_confirm: Whether to confirm trends with higher timeframe
            hmm_covariance_type: HMM covariance type
            hmm_n_iter: HMM fitting iterations
            use_hmm: Whether to use HMM (False = rule-based only)
            random_state: Random seed for reproducibility
            require_hmm: When True, fail closed if a real HMM backend cannot be used
            allow_rule_based_fallback: When False, do not silently downgrade to rule-based detection
            min_hmm_samples: Minimum prepared samples required to fit/predict with strict HMM mode
        """
        default_features = ["log_return", "volatility", "adx", "volume_ratio"]
        self.n_states = n_hmm_states
        self.features = list(features) if features is not None else default_features
        self.vol_ema_period = vol_ema_period
        self.vol_low_q = vol_low_q
        self.vol_high_q = vol_high_q
        self.trend_adx_thr = trend_adx_threshold
        self.smoothing_window = smoothing_window
        self.htf_confirm = htf_confirm
        self.require_hmm = bool(require_hmm)
        self.allow_rule_based_fallback = bool(allow_rule_based_fallback)
        self.min_hmm_samples = max(60, int(min_hmm_samples))
        self.use_hmm = use_hmm and (GaussianHMM is not None)

        if self.require_hmm and not use_hmm:
            raise StrictHMMRequiredError("Strict HMM mode requires use_hmm=True.")
        if self.require_hmm and not self.use_hmm:
            raise StrictHMMRequiredError(
                "Strict HMM mode requires hmmlearn/GaussianHMM. Install `hmmlearn` and `scikit-learn`."
            )

        self.model = None
        if self.use_hmm:
            self.model = GaussianHMM(
                n_components=self.n_states,
                covariance_type=hmm_covariance_type,
                n_iter=hmm_n_iter,
                random_state=random_state
            )

        self._is_fit = False
        self._fit_lock = threading.Lock()
        self._last: Optional[MarketRegime] = None
        self._hist = deque(maxlen=max(3, smoothing_window))
        self._vol_low = 0.005
        self._vol_high = 0.02
        self._scaler_mu = None
        self._scaler_sigma = None
        self._state2label: Dict[int, str] = {
            i: self.REGIME_STATES.get(i, f"S{i}")
            for i in range(self.n_states)
        }

    def fit(self, df: pd.DataFrame) -> None:
        """
        Fit the HMM model on historical data.

        Args:
            df: DataFrame with OHLCV data (minimum 200 rows recommended)
        """
        with self._fit_lock:
            df_ta, X = self._prepare(df, fit_scaler=True)
            if X is None or len(X) < self.min_hmm_samples:
                self._is_fit = False
                if self.require_hmm:
                    raise StrictHMMRequiredError(
                        f"Strict HMM mode needs at least {self.min_hmm_samples} prepared samples; "
                        f"received {0 if X is None else len(X)}."
                    )
                return

            self._update_vol_thresholds(df_ta['volatility'])

            if self.model:
                self.model.fit(X)
                self._is_fit = True
                states = self.model.predict(X)
                self._state2label = self._auto_label_states(states, df_ta)
            else:
                self._is_fit = False
                if self.require_hmm:
                    raise StrictHMMRequiredError("Strict HMM mode could not initialize a GaussianHMM model.")

    def detect(
        self,
        df: pd.DataFrame,
        df_htf: Optional[pd.DataFrame] = None
    ) -> Optional[MarketRegime]:
        """
        Detect current market regime.

        Args:
            df: DataFrame with recent OHLCV data
            df_htf: Optional higher timeframe data for trend confirmation

        Returns:
            MarketRegime with detection results
        """
        if df is None or df.empty:
            if self.require_hmm and self._last is None:
                raise StrictHMMRequiredError("Strict HMM mode requires non-empty candle data.")
            return self._last

        df_ta, X = self._prepare(df, fit_scaler=False)
        if df_ta is None:
            if self.require_hmm:
                raise StrictHMMRequiredError("Strict HMM mode could not prepare regime features from candle data.")
            return self._last

        last = df_ta.iloc[-1]
        vol = float(last.get('volatility', np.nan))
        adx = float(last.get('adx', 0))
        dmp = float(last.get('+di', 0))
        dmn = float(last.get('-di', 0))

        # Determine volatility level
        vol_lvl = "medium"
        if np.isfinite(vol):
            if vol < self._vol_low:
                vol_lvl = "low"
            elif vol > self._vol_high:
                vol_lvl = "high"

        # Determine trend
        is_trend = adx > self.trend_adx_thr
        if dmp > dmn and is_trend:
            tdir = 1
        elif dmn > dmp and is_trend:
            tdir = -1
        else:
            tdir = 0
        tstr = float(np.clip(adx / 50.0, 0, 1))

        # Try HMM prediction
        raw = None
        conf = 1.0
        prob_dict = None

        if self._is_fit and self.model and X is not None and len(X) > 0:
            try:
                pb = self.model.predict_proba(X)[-1]
                prob_dict = self._probs_to_labels(pb)
                raw, conf = self._top_label(prob_dict)
            except (ValueError, IndexError) as e:
                if self.require_hmm or not self.allow_rule_based_fallback:
                    raise StrictHMMRequiredError(f"HMM prediction failed: {type(e).__name__}: {e}") from e
                logger.debug(f"HMM prediction failed, falling back to rule-based: {e}")
                raw = None
        elif self.require_hmm:
            raise StrictHMMRequiredError("Strict HMM mode requires a fitted GaussianHMM before detection.")

        # Fallback to rule-based
        if raw is None:
            if self.require_hmm or not self.allow_rule_based_fallback:
                raise StrictHMMRequiredError("Strict HMM mode disallows rule-based regime fallback.")
            if is_trend:
                if tdir == 1:
                    raw = "HV_Trend_Up" if vol_lvl == "high" else "LV_Trend_Up"
                elif tdir == -1:
                    raw = "HV_Trend_Down" if vol_lvl == "high" else "LV_Trend_Down"
                else:
                    raw = "HV_Range" if vol_lvl == "high" else "LV_Range"
            else:
                raw = "HV_Range" if vol_lvl == "high" else "LV_Range"
            conf = 1.0
            prob_dict = None

        # Higher timeframe confirmation
        if self.htf_confirm and ("Trend" in raw) and df_htf is not None and len(df_htf) >= 50:
            want_dir = 1 if "Up" in raw else -1
            if not self._confirm_trend(df_htf, want_dir=want_dir):
                raw = "HV_Range" if vol_lvl == "high" else "LV_Range"

        # Smooth transitions
        self._hist.append(raw)
        final = self._smoothed_label(raw)

        # Build result
        regime = MarketRegime(
            timestamp=time.time(),
            regime_label=final,
            confidence=conf,
            volatility_level=vol_lvl,
            is_trending=("Trend" in final),
            trend_direction=(1 if "Up" in final else -1 if "Down" in final else 0),
            trend_strength=round(tstr, 3),
            state_probabilities=prob_dict if prob_dict else None
        )

        self._last = regime
        return regime

    def analyze_market(
        self,
        df: pd.DataFrame,
        indicator_col: Optional[str] = None,
        oversold_level: float = 30.0,
        overbought_level: float = 70.0,
    ) -> MarketCondition:
        """
        Comprehensive market analysis combining regime with other factors.

        Convenience method that detects regime and calculates full market condition.

        Args:
            df: DataFrame with OHLCV and optional indicator data
            indicator_col: Oscillator column for divergence detection
            oversold_level: Oscillator oversold threshold
            overbought_level: Oscillator overbought threshold

        Returns:
            MarketCondition with comprehensive analysis
        """
        regime = self.detect(df)
        return calculate_market_condition(
            df,
            regime=regime,
            indicator_col=indicator_col,
            oversold_level=oversold_level,
            overbought_level=overbought_level,
        )

    def to_dict(self) -> Dict:
        """Serialize detector state to dictionary."""
        return {
            "vol_low": self._vol_low,
            "vol_high": self._vol_high,
            "mu": self._scaler_mu.tolist() if self._scaler_mu is not None else None,
            "sigma": self._scaler_sigma.tolist() if self._scaler_sigma is not None else None,
            "state2label": self._state2label,
            "is_fit": self._is_fit,
            "model": None if not (self.model and self._is_fit) else {
                "startprob_": self.model.startprob_.tolist(),
                "transmat_": self.model.transmat_.tolist(),
                "means_": getattr(self.model, "means_", None).tolist() if hasattr(self.model, "means_") else None,
                "covars_": getattr(self.model, "covars_", None).tolist() if hasattr(self.model, "covars_") else None,
                "covariance_type": self.model.covariance_type,
                "n_components": self.model.n_components
            }
        }

    def from_dict(self, d: Dict) -> None:
        """Load detector state from dictionary."""
        self._vol_low = float(d.get("vol_low", self._vol_low))
        self._vol_high = float(d.get("vol_high", self._vol_high))
        self._scaler_mu = self._as_np(d.get("mu"))
        self._scaler_sigma = self._as_np(d.get("sigma"))
        self._state2label = d.get("state2label", self._state2label)
        self._is_fit = bool(d.get("is_fit", False))

        m = d.get("model")
        if m and self.use_hmm:
            self.model = GaussianHMM(
                n_components=m.get("n_components", self.n_states),
                covariance_type=m.get("covariance_type", "diag")
            )
            self.model.startprob_ = np.array(m["startprob_"])
            self.model.transmat_ = np.array(m["transmat_"])
            if m.get("means_") is not None:
                self.model.means_ = np.array(m["means_"])
            if m.get("covars_") is not None:
                self.model.covars_ = np.array(m["covars_"])

    # -------------------------------------------------------------------------
    # Private Methods
    # -------------------------------------------------------------------------

    def _prepare(
        self,
        df: pd.DataFrame,
        fit_scaler: bool
    ) -> Tuple[Optional[pd.DataFrame], Optional[np.ndarray]]:
        """Prepare data for HMM."""
        req = {"open", "high", "low", "close", "volume"}
        if not req.issubset(set(df.columns)) or len(df) < 60:
            return None, None

        x = df.copy()
        for c in ["open", "high", "low", "close", "volume"]:
            x[c] = pd.to_numeric(x[c], errors="coerce")
        x.dropna(subset=["open", "high", "low", "close", "volume"], inplace=True)

        if len(x) < 60:
            return None, None

        ta = self._indicators(x)
        if ta is None:
            return None, None

        if fit_scaler:
            Z = ta[self.features].to_numpy()
            self._scaler_mu = Z.mean(axis=0)
            self._scaler_sigma = Z.std(axis=0)
            self._scaler_sigma[self._scaler_sigma < 1e-9] = 1.0

        Z = ta[self.features].to_numpy()
        if self._scaler_mu is None or self._scaler_sigma is None:
            return ta, None

        Z = (Z - self._scaler_mu) / self._scaler_sigma
        Z[np.isnan(Z)] = 0
        Z[~np.isfinite(Z)] = 0

        return ta, Z

    def _indicators(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Calculate technical indicators for regime detection."""
        d = df.copy()

        # Log returns
        d["log_return"] = np.log(d["close"] / d["close"].shift(1))

        # True Range and ATR
        tr = np.maximum(
            d["high"] - d["low"],
            np.maximum(
                (d["high"] - d["close"].shift(1)).abs(),
                (d["low"] - d["close"].shift(1)).abs()
            )
        )
        length = 14
        atr = tr.ewm(alpha=1 / length, adjust=False).mean()

        # Volatility (ATR as % of price, smoothed)
        d["volatility"] = ((atr / d["close"]).clip(lower=0) * 100.0).ewm(
            span=self.vol_ema_period, adjust=False
        ).mean()

        # Directional Movement
        up_move = d["high"].diff()
        down_move = -d["low"].diff()
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

        atr_w = tr.ewm(alpha=1 / length, adjust=False).mean()
        plus_di = 100 * (
            pd.Series(plus_dm, index=d.index).ewm(alpha=1 / length, adjust=False).mean() / atr_w
        ).replace([np.inf, -np.inf], 0).fillna(0)
        minus_di = 100 * (
            pd.Series(minus_dm, index=d.index).ewm(alpha=1 / length, adjust=False).mean() / atr_w
        ).replace([np.inf, -np.inf], 0).fillna(0)

        # ADX
        dx = (100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))).fillna(0)
        adx = dx.ewm(alpha=1 / length, adjust=False).mean()

        d["adx"] = adx
        d["+di"] = plus_di
        d["-di"] = minus_di

        # Volume ratio
        vol_ma_len = 20
        d["volume_ma"] = d["volume"].rolling(vol_ma_len).mean()
        d["volume_ratio"] = (d["volume"] / (d["volume_ma"] + 1e-9)).replace([np.inf, -np.inf], 0).fillna(0)

        d.dropna(inplace=True)

        if len(d) < 50:
            return None

        # Clip outliers
        for col in ["log_return", "volatility", "adx", "+di", "-di", "volume_ratio"]:
            q1, q99 = np.nanpercentile(d[col], 1), np.nanpercentile(d[col], 99)
            d[col] = d[col].clip(q1, q99)

        return d

    def _update_vol_thresholds(self, vol: pd.Series) -> None:
        """Update volatility thresholds from data."""
        v = vol.dropna()
        if len(v) >= 100:
            self._vol_low = float(v.quantile(self.vol_low_q))
            self._vol_high = float(v.quantile(self.vol_high_q))

    def _auto_label_states(
        self,
        states: np.ndarray,
        df_ta: pd.DataFrame
    ) -> Dict[int, str]:
        """Automatically label HMM states based on characteristics."""
        lab = {}
        s = pd.Series(states, index=df_ta.index)

        for st in range(self.n_states):
            idx = s[s == st].index
            if len(idx) == 0:
                lab[st] = self.REGIME_STATES.get(st, f"S{st}")
                continue

            sub = df_ta.loc[idx]
            m_vol = float(sub["volatility"].mean())
            m_adx = float(sub["adx"].mean())
            m_lr = float(sub["log_return"].mean())

            hv = m_vol > self._vol_high
            lv = m_vol < self._vol_low

            if m_adx > self.trend_adx_thr:
                if m_lr > 0:
                    lab[st] = "HV_Trend_Up" if hv else "LV_Trend_Up"
                elif m_lr < 0:
                    lab[st] = "HV_Trend_Down" if hv else "LV_Trend_Down"
                else:
                    lab[st] = "HV_Range" if hv else "LV_Range"
            else:
                lab[st] = "HV_Range" if hv else ("LV_Range" if lv else "LV_Range")

        return lab

    def _probs_to_labels(self, pb: np.ndarray) -> Dict[str, float]:
        """Convert state probabilities to label probabilities."""
        out = defaultdict(float)
        for i, p in enumerate(pb):
            out[self._state2label.get(i, f"S{i}")] += float(p)
        return dict(out)

    def _top_label(self, label_probs: Dict[str, float]) -> Tuple[str, float]:
        """Get top label and its probability."""
        if not label_probs:
            return "LV_Range", 1.0
        k = max(label_probs, key=label_probs.get)
        return k, float(label_probs[k])

    def _confirm_trend(self, df_htf: pd.DataFrame, want_dir: int) -> bool:
        """Confirm trend direction with higher timeframe."""
        ta, _ = self._prepare(df_htf, fit_scaler=False)
        if ta is None or len(ta) < 30:
            return True  # No HTF data, assume confirmed

        adx = float(ta["adx"].iloc[-1])
        dmp = float(ta["+di"].iloc[-1])
        dmn = float(ta["-di"].iloc[-1])

        if adx <= max(self.trend_adx_thr, 20):
            return False

        if dmp > dmn:
            dir_htf = 1
        elif dmn > dmp:
            dir_htf = -1
        else:
            dir_htf = 0

        return dir_htf == want_dir

    def _smoothed_label(self, raw: str) -> str:
        """Smooth regime transitions to avoid whipsaws."""
        if self._last is None:
            return raw
        if raw == self._last.regime_label:
            return raw

        counts = defaultdict(int)
        for r in self._hist:
            counts[r] += 1

        top = max(counts, key=counts.get)
        freq = counts[top] / len(self._hist)

        return raw if (top == raw and freq >= 0.6) else self._last.regime_label

    def _as_np(self, x):
        """Convert to numpy array if not None."""
        if x is None:
            return None
        return np.array(x, dtype=float)
