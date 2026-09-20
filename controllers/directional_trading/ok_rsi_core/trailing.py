"""Backward-compatible OK RSI import path for the shared trailing policy."""

from controllers.shared.trailing import ContinuousTrailingSelection, calculate_continuous_adaptive_trailing

__all__ = [
    "ContinuousTrailingSelection",
    "calculate_continuous_adaptive_trailing",
]
