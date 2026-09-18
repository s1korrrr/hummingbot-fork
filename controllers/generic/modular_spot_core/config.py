from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, model_validator


class PolicyModel(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, allow_inf_nan=False, validate_default=True)


class SignalPolicy(PolicyModel):
    kind: Literal['rsi_rebound', 'breakout'] = 'rsi_rebound'
    rsi_length: int = Field(default=14, ge=2, le=200, strict=True)
    oversold: Decimal = Field(default=Decimal('30'), gt=0, lt=100)
    breakout_lookback: int = Field(default=20, ge=2, le=500, strict=True)
    require_uptrend: StrictBool = True


class AcquisitionPolicy(PolicyModel):
    fractions: tuple[Decimal, ...] = (Decimal('1'),)
    minimum_drop: Decimal = Field(default=Decimal('.02'), gt=0, lt=1)
    cooldown_seconds: int = Field(default=300, ge=1, strict=True)
    entry_timeout_seconds: int = Field(default=60, ge=1, le=3600, strict=True)

    @model_validator(mode='after')
    def bounded_schedule(self):
        if not 1 <= len(self.fractions) <= 5:
            raise ValueError('one to five acquisition stages are supported')
        if any(not x.is_finite() or x <= 0 for x in self.fractions) or sum(self.fractions) != Decimal('1'):
            raise ValueError('positive acquisition fractions must sum exactly to one')
        return self


class TrailingPolicy(PolicyModel):
    activation: Decimal = Field(default=Decimal('.015'), gt=0, lt=1)
    delta: Decimal = Field(default=Decimal('.005'), gt=0, lt=1)

    @model_validator(mode='after')
    def positive_locked_profit(self):
        if self.activation <= self.delta:
            raise ValueError('trailing activation must exceed delta')
        return self

    @property
    def minimum_profit(self):
        return self.activation - self.delta


class RiskPolicy(PolicyModel):
    max_drawdown_fraction: Decimal = Field(default=Decimal('.10'), gt=0, lt=1)
    max_daily_loss_fraction: Decimal = Field(default=Decimal('.05'), gt=0, lt=1)
    max_spread_fraction: Decimal = Field(default=Decimal('.003'), gt=0, lt=1)
    fee_reserve_fraction: Decimal = Field(default=Decimal('.005'), gt=0, lt=1)
    cash_reserve_quote: Decimal = Field(default=Decimal('0'), ge=0)
