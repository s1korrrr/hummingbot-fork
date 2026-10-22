"""Strategy adapter configuration; reusable policies are canonical engine components."""
from decimal import Decimal
from typing import Literal

from pydantic import Field, StrictBool, model_validator

from hummingbot.strategy_v2.components.config import AcquisitionPolicy, PolicyModel, RiskPolicy, TrailingPolicy

__all__ = ['PolicyModel', 'AcquisitionPolicy', 'RiskPolicy', 'TrailingPolicy', 'SignalPolicy']


class SignalPolicy(PolicyModel):
    """Entry signal on closed candles.

    `rsi_rebound`: RSI closes above `oversold` after closing at or below it.
    `breakout`: close above the prior `breakout_lookback` high, optionally in an uptrend.
    `rsi_turn`: the OK RSI reversal port. RSI turns up from a rolling-window minimum that
    was at or below `oversold`, while still near the bottom, debounced over four windows,
    and the confluence score (RSI at or below `oversold`, Bollinger lower touch, MACD
    histogram turn, RSI extremeness) reaches `minimum_score`.

    The ATR band applies to every kind when set: no entry while the Wilder ATR fraction
    of the close is below `min_atr_fraction` or above `max_atr_fraction`.
    """
    kind: Literal['rsi_rebound', 'breakout', 'rsi_turn'] = 'rsi_rebound'
    rsi_length: int = Field(default=14, ge=2, le=200, strict=True)
    oversold: Decimal = Field(default=Decimal('30'), gt=0, lt=100)
    breakout_lookback: int = Field(default=20, ge=2, le=500, strict=True)
    require_uptrend: StrictBool = True
    near_bottom_multiplier: Decimal = Field(default=Decimal('1.3'), ge=1, le=3)
    minimum_score: int = Field(default=1, ge=1, le=4, strict=True)
    bb_length: int = Field(default=20, ge=2, le=500, strict=True)
    bb_std: Decimal = Field(default=Decimal('2'), gt=0, le=10)
    macd_fast: int = Field(default=12, ge=2, le=500, strict=True)
    macd_slow: int = Field(default=26, ge=3, le=1000, strict=True)
    macd_signal: int = Field(default=9, ge=2, le=500, strict=True)
    mean_reversion_minimum: Decimal = Field(default=Decimal('.5'), ge=0, le=1)
    atr_length: int = Field(default=14, ge=2, le=500, strict=True)
    min_atr_fraction: Decimal | None = Field(default=None, ge=0, lt=1)
    max_atr_fraction: Decimal | None = Field(default=None, gt=0, lt=1)

    @model_validator(mode='after')
    def consistent_bands(self):
        if self.macd_fast >= self.macd_slow:
            raise ValueError('MACD fast length must be below the slow length')
        if (self.min_atr_fraction is not None and self.max_atr_fraction is not None
                and self.min_atr_fraction >= self.max_atr_fraction):
            raise ValueError('ATR band minimum must be below its maximum')
        return self

    @property
    def reversal_window(self):
        return max(3, self.rsi_length // 2)

    @property
    def required_history(self):
        """Closed candles every feature needs before a decision; the controller keeps one more."""
        return max(60, 2 * self.rsi_length + 2, self.breakout_lookback + 2, self.bb_length + 2,
                   self.macd_slow + self.macd_signal + 2, self.atr_length + 2,
                   self.rsi_length + 5 * self.reversal_window + 2)
