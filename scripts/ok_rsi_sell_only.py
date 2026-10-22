import os
from decimal import Decimal
from typing import List

from pydantic import Field, field_validator, model_validator

from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.okx_spot_candles.constants import INTERVALS as OKX_CANDLE_INTERVALS
from scripts import ok_rsi_script_common
from scripts.v2_with_controllers import V2WithControllers, V2WithControllersConfig


class OKRSISellOnlyConfig(V2WithControllersConfig):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = Field(default_factory=list, exclude=True)
    connector_name: str = "okx"
    trading_pairs: List[str]
    leverage: int = Field(default=1, ge=1, le=1)
    interval: str = "1m"
    trend_confirmation_enabled: bool = True
    trend_confirmation_medium_interval: str = "5m"
    trend_confirmation_long_interval: str = "15m"
    derived_higher_timeframe_max_records: int = Field(default=100, ge=20, le=2000)
    strict_hmm_mode: bool = True
    total_amount_quote: Decimal = Field(default=Decimal("500"), gt=Decimal("0"), allow_inf_nan=False)
    usd_per_entry: Decimal = Field(default=Decimal("25"), gt=Decimal("0"), allow_inf_nan=False)
    max_total_position_usd: Decimal = Field(default=Decimal("500"), gt=Decimal("0"), allow_inf_nan=False)
    max_executors_per_side: int = Field(default=1, ge=1)
    sell_rsi_overbought: float = Field(default=76.0, ge=50.0, le=100.0, allow_inf_nan=False)
    sell_only_inventory_fraction: Decimal = Field(
        default=Decimal("0.10"),
        gt=Decimal("0"),
        le=Decimal("1"),
        allow_inf_nan=False,
    )
    sell_all_below_quote: Decimal = Field(default=Decimal("20"), ge=Decimal("0"), allow_inf_nan=False)

    @field_validator("connector_name", mode="before")
    @classmethod
    def validate_connector(cls, value):
        connector_name = str(value or "okx").strip().lower()
        if connector_name not in {"okx", "okx_paper_trade"}:
            raise ValueError("OK RSI sell-only requires an OKX spot connector")
        return connector_name

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, value):
        if value not in OKX_CANDLE_INTERVALS:
            raise ValueError(f"unsupported OKX candle interval: {value}")
        return value

    @field_validator("trading_pairs", mode="before")
    @classmethod
    def normalize_pairs(cls, value):
        return ok_rsi_script_common.normalize_okx_usdc_pairs(value)

    @model_validator(mode="after")
    def validate_derived_intervals(self):
        base_seconds = CandlesBase.interval_to_seconds[self.interval]
        for interval in (self.trend_confirmation_medium_interval, self.trend_confirmation_long_interval):
            if interval not in OKX_CANDLE_INTERVALS:
                raise ValueError(f"unsupported OKX candle interval: {interval}")
            target_seconds = CandlesBase.interval_to_seconds[interval]
            if target_seconds <= base_seconds or target_seconds % base_seconds != 0:
                raise ValueError("trend intervals must be larger whole multiples of the base interval")
        return self

    @model_validator(mode="after")
    def validate_risk_budget(self):
        if self.usd_per_entry > self.max_total_position_usd:
            raise ValueError("usd_per_entry must not exceed max_total_position_usd")
        if self.max_total_position_usd > self.total_amount_quote:
            raise ValueError("max_total_position_usd must not exceed total_amount_quote")
        if self.max_global_drawdown_quote is not None and self.max_global_drawdown_quote <= 0:
            raise ValueError("max_global_drawdown_quote must be positive when configured")
        if self.max_controller_drawdown_quote is not None and self.max_controller_drawdown_quote <= 0:
            raise ValueError("max_controller_drawdown_quote must be positive when configured")
        if (
            self.max_global_drawdown_quote is not None
            and self.max_controller_drawdown_quote is not None
            and self.max_controller_drawdown_quote > self.max_global_drawdown_quote
        ):
            raise ValueError("max_controller_drawdown_quote must not exceed max_global_drawdown_quote")
        return self

    def load_controller_configs(self, reload_changed_modules: bool = False):
        del reload_changed_modules
        return ok_rsi_script_common.build_ok_rsi_controller_configs(self, "sell_only")


class OKRSISellOnly(V2WithControllers):
    """Independent lifecycle for the uniform OKX sell-only controller group."""
