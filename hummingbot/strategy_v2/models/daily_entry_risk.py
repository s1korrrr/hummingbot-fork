"""Durable UTC entry pause from native reported PnL; no orders or accounting ledger."""
import math
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, model_validator


class DailyEntryRisk(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)
    schema_version: Literal[1] = 1
    limit_quote: Decimal = Field(gt=0)
    utc_day: int = Field(ge=0, strict=True)
    baseline_quote: Decimal
    last_pnl_quote: Decimal
    last_timestamp: float = Field(ge=0)
    paused: StrictBool = False
    breach_day: int | None = Field(default=None, ge=0, strict=True)

    @model_validator(mode='after')
    def coherent(self):
        if (self.utc_day != int(self.last_timestamp // 86400)
                or self.paused != (self.breach_day is not None)
                or self.breach_day is not None and self.breach_day > self.utc_day):
            raise ValueError('Daily entry-risk state is inconsistent')
        return self

    @classmethod
    def new(cls, limit, timestamp):
        timestamp = cls._clock(timestamp)
        return cls(limit_quote=limit, utc_day=int(timestamp // 86400), baseline_quote=0,
                   last_pnl_quote=0, last_timestamp=timestamp)

    @classmethod
    def restore(cls, state, limit):
        result = cls.model_validate(state)
        if result.limit_quote != limit:
            raise ValueError('Daily loss limit changed; explicit reconciliation required')
        return result

    @staticmethod
    def _clock(timestamp):
        if type(timestamp) not in (int, float) or not math.isfinite(timestamp) or timestamp < 0:
            raise ValueError('Invalid daily-risk clock')
        return float(timestamp)

    def observe(self, timestamp, pnl):
        timestamp = self._clock(timestamp)
        pnl = Decimal(str(pnl))
        if timestamp < self.last_timestamp or not pnl.is_finite():
            raise ValueError('Daily-risk clock regressed or native PnL unavailable')
        day = int(timestamp // 86400)
        if day > self.utc_day:
            # Retain losses through downtime; never invent a midnight mark.
            self.baseline_quote = self.last_pnl_quote
            self.utc_day = day
        self.last_timestamp, self.last_pnl_quote = timestamp, pnl
        if self.baseline_quote - pnl >= self.limit_quote:
            self.paused, self.breach_day = True, day
        return self.paused

    def acknowledge(self, timestamp, pnl):
        self.observe(timestamp, pnl)
        if self.paused and self.utc_day <= self.breach_day:
            return False
        self.paused, self.breach_day = False, None
        return True

    def snapshot(self):
        return self.model_dump(mode='json')
