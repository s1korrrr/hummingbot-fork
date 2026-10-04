"""Explicit prospective enrollment, separate from restoration of historical risk.

Only an approved stopped-owner migration may install a pending enrollment.
Normal startup never creates one. The first fresh complete native PnL report
establishes the baseline; the operator entry fence remains in place afterward.
"""
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator, model_validator

from hummingbot.strategy_v2.models.daily_entry_risk import DailyEntryRisk


class EntryRiskEnrollment(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)
    schema_version: Literal[1]
    command_id: str = Field(min_length=1, max_length=128, pattern=r'^[A-Za-z0-9_.:-]+$')
    limit_quote: Decimal = Field(gt=0)
    requested_at: float = Field(ge=0)
    historical_peaks_available: StrictBool
    initialized_at: float | None = Field(default=None, ge=0)
    initial_pnl_quote: Decimal | None = None

    @field_validator('schema_version', mode='before')
    @classmethod
    def strict_version(cls, value):
        if type(value) is not int:
            raise ValueError('Enrollment version must be an integer')
        return value

    @field_validator('limit_quote', 'requested_at', 'initialized_at', 'initial_pnl_quote', mode='before')
    @classmethod
    def reject_boolean_numbers(cls, value):
        if isinstance(value, bool):
            raise ValueError('Enrollment values must be numbers, not booleans')
        return value

    @model_validator(mode='after')
    def coherent(self):
        if ((self.initialized_at is None) != (self.initial_pnl_quote is None)
                or self.initialized_at is not None and self.initialized_at < self.requested_at):
            raise ValueError('Entry risk enrollment is inconsistent')
        return self

    @property
    def pending(self):
        return self.initialized_at is None

    def start(self, timestamp, pnl):
        timestamp = DailyEntryRisk._clock(timestamp)
        pnl = Decimal(str(pnl))
        if not self.pending or timestamp < self.requested_at or not pnl.is_finite():
            raise ValueError('Fresh native PnL is required for pending entry risk enrollment')
        risk = DailyEntryRisk(limit_quote=self.limit_quote, utc_day=int(timestamp // 86400),
                              baseline_quote=pnl, last_pnl_quote=pnl, last_timestamp=timestamp)
        completed = self.model_copy(update={'initialized_at': timestamp, 'initial_pnl_quote': pnl})
        return risk, completed

    def snapshot(self):
        return self.model_dump(mode='json')

    def validate_daily_state(self, risk):
        if (self.pending or risk.limit_quote != self.limit_quote
                or risk.last_timestamp < self.initialized_at
                or (risk.utc_day == int(self.initialized_at // 86400)
                    and risk.baseline_quote != self.initial_pnl_quote)):
            raise ValueError('Daily risk state conflicts with its native enrollment baseline')
