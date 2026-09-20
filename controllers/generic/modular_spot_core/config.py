"""Strategy adapter configuration; reusable policies are canonical engine components."""
from decimal import Decimal
from typing import Literal

from pydantic import Field, StrictBool

from hummingbot.strategy_v2.components.config import AcquisitionPolicy, PolicyModel, RiskPolicy, TrailingPolicy

__all__ = ['PolicyModel', 'AcquisitionPolicy', 'RiskPolicy', 'TrailingPolicy', 'SignalPolicy']


class SignalPolicy(PolicyModel):
    kind: Literal['rsi_rebound', 'breakout'] = 'rsi_rebound'
    rsi_length: int = Field(default=14, ge=2, le=200, strict=True)
    oversold: Decimal = Field(default=Decimal('30'), gt=0, lt=100)
    breakout_lookback: int = Field(default=20, ge=2, le=500, strict=True)
    require_uptrend: StrictBool = True
