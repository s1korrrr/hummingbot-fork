"""
RSI Signal Analysis Utilities.

Provides RSI-specific signal analysis built on top of the general
market analysis utilities. Includes:
- RSI-specific divergence detection with oversold/overbought bonuses
- RSI mean reversion scoring
- RSI signal strength calculation
- Adaptive trailing stop parameters
- Multi-timeframe trend confirmation wrapper

Note: Generic utilities (divergence detection, volume analysis, etc.) are now
in `hummingbot/strategy_v2/utils/market_analysis.py` for reuse across strategies.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Tuple

import pandas as pd

from hummingbot.strategy_v2.utils.market_analysis import (
    DivergenceResult,
    DivergenceType,
    MultiTimeframeTrend,
    TrendDirection,
    TrendState,
    analyze_multi_timeframe_trend,
    analyze_volume,
    detect_divergence,
)

__all__ = [
    "DivergenceType",
    "DivergenceResult",
    "RSISignalContext",
    "TrendDirection",
    "TrendState",
    "MultiTimeframeTrend",
    "detect_rsi_divergence",
    "calculate_volume_ratio",
    "calculate_rsi_mean_reversion_score",
    "calculate_signal_strength",
    "get_adaptive_trailing_params",
    "analyze_rsi_trend_confirmation",
    "analyze_rsi_signal",
]


@dataclass
class RSISignalContext:
    """
    Context data for RSI signal analysis.

    Attributes:
        rsi: Current RSI value
        rsi_prev: Previous RSI value
        rsi_smoothed: Smoothed RSI (EMA)
        close: Current close price
        volume_ratio: Current volume / average volume
        divergence: Detected divergence type
        divergence_strength: How strong the divergence is (0-1)
        mean_reversion_score: Probability of mean reversion (0-1)
        signal_strength: Overall signal strength (0-1)
    """

    rsi: Optional[float] = None
    rsi_prev: Optional[float] = None
    rsi_smoothed: Optional[float] = None
    close: Optional[float] = None
    volume_ratio: Optional[float] = None
    divergence: DivergenceType = DivergenceType.NONE
    divergence_strength: float = 0.0
    mean_reversion_score: float = 0.0
    signal_strength: float = 0.0

    def to_dict(self) -> dict:
        """Convert to dictionary for logging/display."""
        return {
            "rsi": self.rsi,
            "rsi_prev": self.rsi_prev,
            "rsi_smoothed": self.rsi_smoothed,
            "close": self.close,
            "volume_ratio": self.volume_ratio,
            "divergence": self.divergence.value,
            "divergence_strength": self.divergence_strength,
            "mean_reversion_score": self.mean_reversion_score,
            "signal_strength": self.signal_strength,
        }


def detect_rsi_divergence(
    df: pd.DataFrame,
    rsi_col: str = "rsi",
    price_col: str = "close",
    lookback: int = 20,
    min_bars_between: int = 3,
    tolerance: float = 0.02,
) -> Tuple[DivergenceType, float]:
    """
    Detect RSI divergence patterns.

    Wrapper around the generic detect_divergence() with RSI-specific defaults.
    """
    result = detect_divergence(
        df=df,
        indicator_col=rsi_col,
        price_col=price_col,
        lookback=lookback,
        min_bars_between=min_bars_between,
        tolerance=tolerance,
        oversold_level=30.0,
        overbought_level=70.0,
    )
    return result.divergence_type, result.strength


def calculate_volume_ratio(
    df: pd.DataFrame,
    lookback: int = 20,
    volume_col: str = "volume",
) -> Optional[float]:
    """
    Calculate current volume relative to recent average.

    Wrapper around analyze_volume() for backward compatibility.
    """
    result = analyze_volume(df, lookback=lookback, volume_col=volume_col)
    return result.volume_ratio if result.volume_ratio != 1.0 or result.current_volume > 0 else None


def calculate_rsi_mean_reversion_score(rsi: float) -> float:
    """
    Calculate probability score for RSI mean reversion.

    Returns a 0-1 score where 1 means highest reversion probability.
    """
    if rsi is None or pd.isna(rsi):
        return 0.0

    distance = abs(rsi - 50)
    score = (distance / 50) ** 1.5
    return round(min(1.0, score), 3)


def calculate_signal_strength(
    rsi: float,
    threshold: float,
    divergence_strength: float = 0.0,
    volume_ratio: Optional[float] = None,
    regime_confidence: float = 0.5,
) -> float:
    """
    Calculate overall RSI signal strength combining multiple factors.
    """
    if rsi is None or pd.isna(rsi):
        return 0.0

    if rsi < 50:
        rsi_strength = max(0, (threshold - rsi) / threshold)
    else:
        rsi_strength = max(0, (rsi - threshold) / (100 - threshold))

    weights = {
        "rsi": 0.4,
        "divergence": 0.3,
        "volume": 0.15,
        "regime": 0.15,
    }

    score = weights["rsi"] * rsi_strength
    score += weights["divergence"] * divergence_strength

    if volume_ratio is not None and volume_ratio > 0:
        vol_score = min(1.0, (volume_ratio - 1.0) / 1.0)
        vol_score = max(0.0, vol_score)
        score += weights["volume"] * vol_score
    else:
        score += weights["volume"] * 0.5

    score += weights["regime"] * regime_confidence
    return round(min(1.0, max(0.0, score)), 3)


def get_adaptive_trailing_params(
    atr_pct: float,
    base_activation: Decimal = Decimal("0.01"),
    base_delta: Decimal = Decimal("0.004"),
    min_multiplier: float = 0.5,
    max_multiplier: float = 2.5,
    baseline_atr: float = 0.002,
) -> Tuple[Decimal, Decimal]:
    """
    Calculate adaptive trailing stop parameters based on volatility.
    """
    if atr_pct is None or atr_pct <= 0:
        return base_activation, base_delta

    multiplier = atr_pct / baseline_atr
    multiplier = max(min_multiplier, min(max_multiplier, multiplier))

    activation = base_activation * Decimal(str(multiplier))
    delta = base_delta * Decimal(str(multiplier))

    activation = max(Decimal("0.003"), min(Decimal("0.05"), activation))
    delta = max(Decimal("0.001"), min(Decimal("0.02"), delta))

    return activation, delta


def analyze_rsi_signal(
    df: pd.DataFrame,
    rsi_length: int = 14,
    volume_lookback: int = 20,
) -> RSISignalContext:
    """
    Comprehensive RSI signal analysis.
    """
    context = RSISignalContext()

    if df is None or df.empty:
        return context

    rsi_col = f"RSI_{rsi_length}"

    if rsi_col in df.columns:
        if pd.notna(df[rsi_col].iloc[-1]):
            context.rsi = float(df[rsi_col].iloc[-1])
        if len(df) > 1 and pd.notna(df[rsi_col].iloc[-2]):
            context.rsi_prev = float(df[rsi_col].iloc[-2])

        rsi_smoothed = df[rsi_col].ewm(span=3, adjust=False).mean()
        if pd.notna(rsi_smoothed.iloc[-1]):
            context.rsi_smoothed = float(rsi_smoothed.iloc[-1])

    if "close" in df.columns and pd.notna(df["close"].iloc[-1]):
        context.close = float(df["close"].iloc[-1])

    context.volume_ratio = calculate_volume_ratio(df, lookback=volume_lookback)

    if rsi_col in df.columns:
        context.divergence, context.divergence_strength = detect_rsi_divergence(
            df,
            rsi_col=rsi_col,
            lookback=min(50, len(df)),
        )

    if context.rsi is not None:
        context.mean_reversion_score = calculate_rsi_mean_reversion_score(context.rsi)

    return context


def analyze_rsi_trend_confirmation(
    short_df: pd.DataFrame,
    medium_df: Optional[pd.DataFrame] = None,
    long_df: Optional[pd.DataFrame] = None,
    adx_length: int = 14,
    adx_threshold: float = 18.0,
) -> MultiTimeframeTrend:
    """
    Analyze multi-timeframe trend alignment for RSI signals.
    """
    dfs = {}
    if short_df is not None:
        dfs["short"] = short_df
    if medium_df is not None:
        dfs["medium"] = medium_df
    if long_df is not None:
        dfs["long"] = long_df
    return analyze_multi_timeframe_trend(
        dfs,
        adx_length=adx_length,
        adx_threshold=adx_threshold,
    )
