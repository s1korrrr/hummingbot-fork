"""Public OK RSI configuration, kept separate from runtime orchestration."""

import math
from decimal import Decimal
from typing import List, Optional

from pydantic import field_validator, model_validator

from controllers.directional_trading import hl_rsi
from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.core.data_type.common import OrderType, PositionMode
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.okx_spot_candles.constants import INTERVALS as OKX_CANDLE_INTERVALS


class OKRSIControllerConfig(hl_rsi.HLRSIControllerConfig):
    def get_controller_class(self):
        # ControllerConfigBase otherwise searches this helper module, while
        # the public controller remains in directional_trading.ok_rsi.
        from controllers.directional_trading.ok_rsi import OKRSIController
        return OKRSIController

    controller_name: str = "ok_rsi"
    controller_type: str = "directional_trading"
    connector_name: str = hl_rsi.Field(
        default="okx",
        client_data=ClientFieldData(
            prompt=lambda mi: "Connector:",
            prompt_on_new=True,
            is_updatable=False,
        ),
    )
    trading_pair: str = hl_rsi.Field(
        default="BTC-USDC",
        client_data=ClientFieldData(
            prompt=lambda mi: "Trading pair:",
            prompt_on_new=True,
            is_updatable=False,
        ),
    )
    leverage: int = 1
    position_mode: PositionMode = PositionMode.ONEWAY
    legacy_controller_ids: List[str] = hl_rsi.Field(default_factory=list)
    episode_enabled: bool = hl_rsi.Field(default=False)
    episode_fee_rate: Decimal = hl_rsi.Field(default=Decimal("0.001"), ge=0, lt=Decimal("0.1"))
    episode_assignment_id: Optional[str] = None
    episode_initial_base: Decimal = hl_rsi.Field(default=Decimal("0"), ge=0)
    episode_initial_cost: Optional[Decimal] = hl_rsi.Field(default=None, gt=0)
    episode_budget_quote: Optional[Decimal] = hl_rsi.Field(default=None, gt=0)
    episode_sequential_scale_ins: bool = False
    episode_require_scale_in_signal: bool = False
    episode_scale_in_stabilization_bars: int = hl_rsi.Field(default=2, ge=1, le=240)
    episode_hostile_risk_latch: bool = False
    episode_recovery_exit_enabled: bool = False

    derived_higher_timeframe_max_records: int = hl_rsi.Field(
        default=100,
        ge=20,
        le=2000,
        client_data=ClientFieldData(
            prompt=lambda mi: "Derived higher-timeframe records:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )

    sell_only_inventory_fraction: Decimal = hl_rsi.Field(
        default=Decimal("0.10"),
        gt=Decimal("0"),
        le=Decimal("1"),
        client_data=ClientFieldData(
            prompt=lambda mi: "Sell-only inventory fraction per signal:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    sell_all_below_quote: Decimal = hl_rsi.Field(
        default=Decimal("20"),
        ge=Decimal("0"),
        client_data=ClientFieldData(
            prompt=lambda mi: "Sell all below quote value:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )

    use_trailing_exit: bool = hl_rsi.Field(default=True)
    trailing_activation_pct: float = hl_rsi.Field(default=0.015, gt=0.0)
    trailing_delta_pct: float = hl_rsi.Field(default=0.0035, gt=0.0)
    entry_limit_max_age_seconds: int = hl_rsi.Field(default=30, ge=1)
    use_adaptive_trailing: bool = hl_rsi.Field(default=True)
    trailing_activation_min_pct: float = hl_rsi.Field(default=0.003, gt=0.0)
    trailing_activation_max_pct: float = hl_rsi.Field(default=0.05, gt=0.0)
    trailing_delta_min_pct: float = hl_rsi.Field(default=0.001, gt=0.0)
    trailing_delta_max_pct: float = hl_rsi.Field(default=0.02, gt=0.0)
    trailing_min_locked_profit_pct: float = hl_rsi.Field(default=0.004, gt=0.0)
    trailing_activation_volatility_exponent: float = hl_rsi.Field(default=0.70, gt=0.0, le=2.0)
    trailing_delta_volatility_exponent: float = hl_rsi.Field(default=0.35, gt=0.0, le=2.0)
    trailing_min_multiplier: float = hl_rsi.Field(default=0.5, gt=0.0)
    trailing_max_multiplier: float = hl_rsi.Field(default=2.5, gt=0.0)

    @field_validator("connector_name", mode="before")
    @classmethod
    def validate_hyperliquid_spot_connector(cls, value):
        """Override the inherited validator under the same Pydantic field-validator key."""
        connector_name = str(value or "okx").strip().lower()
        if connector_name not in {"okx", "okx_paper_trade"}:
            raise ValueError("OK RSI requires an OKX spot connector")
        return connector_name

    @field_validator("trading_pair", mode="before")
    @classmethod
    def validate_trading_pair(cls, value):
        trading_pair = str(value or "").strip().upper()
        parts = trading_pair.split("-")
        if len(parts) != 2 or not all(parts):
            raise ValueError("OK RSI trading_pair must use BASE-QUOTE format")
        return trading_pair

    @field_validator("legacy_controller_ids", mode="before")
    @classmethod
    def validate_legacy_controller_ids(cls, value):
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("legacy_controller_ids must be a list")
        normalized = [str(controller_id).strip() for controller_id in value]
        if any(not controller_id for controller_id in normalized):
            raise ValueError("legacy_controller_ids cannot contain blank values")
        if len(normalized) != len(set(normalized)):
            raise ValueError("legacy_controller_ids must be unique")
        return normalized

    @model_validator(mode="after")
    def validate_okx_spot_contract(self):
        candles_connector = str(self.candles_connector or "okx").strip().lower()
        if candles_connector == "okx_paper_trade":
            candles_connector = "okx"
        if candles_connector != "okx":
            raise ValueError("OK RSI requires OKX spot candles")
        object.__setattr__(self, "candles_connector", candles_connector)

        candles_trading_pair = str(self.candles_trading_pair or self.trading_pair).strip().upper()
        if candles_trading_pair != self.trading_pair:
            raise ValueError("OK RSI candles must use the same trading pair as execution")
        object.__setattr__(self, "candles_trading_pair", candles_trading_pair)

        for field_name in (
            "interval",
            "trend_confirmation_medium_interval",
            "trend_confirmation_long_interval",
        ):
            interval = getattr(self, field_name)
            if interval is not None and interval not in OKX_CANDLE_INTERVALS:
                raise ValueError(f"unsupported OKX candle interval for {field_name}: {interval}")

        if self.trend_confirmation_enabled:
            base_seconds = CandlesBase.interval_to_seconds[self.interval]
            for field_name in ("trend_confirmation_medium_interval", "trend_confirmation_long_interval"):
                interval = getattr(self, field_name)
                if interval is None:
                    continue
                target_seconds = CandlesBase.interval_to_seconds[interval]
                if target_seconds <= base_seconds or target_seconds % base_seconds != 0:
                    raise ValueError(f"{field_name} must be a larger whole multiple of the base interval")

        for field_name in type(self).model_fields:
            value = getattr(self, field_name, None)
            if isinstance(value, Decimal) and not value.is_finite():
                raise ValueError(f"{field_name} must be finite")
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError(f"{field_name} must be finite")

        if self.leverage != 1 or self.position_mode != PositionMode.ONEWAY:
            raise ValueError("OKX spot requires leverage=1 and position_mode=ONEWAY")
        if self.episode_enabled and self.interval != "1m":
            raise ValueError("episode mode requires the 1m completed-candle interval")
        if self.episode_enabled and self.trade_mode == "sell_only":
            if not self.episode_assignment_id or not self.episode_assignment_id.strip() or self.episode_initial_base <= 0:
                raise ValueError("sell_only episode requires explicit assignment ID and positive initial base")
            if self.sell_only_if_profitable and self.episode_initial_cost is None:
                raise ValueError("profitable sell_only episode assignment requires initial cost")
            if self.episode_recovery_exit_enabled and self.episode_initial_cost is None:
                raise ValueError("sell_only recovery exit requires assigned cost")
        if self.episode_enabled and self.trade_mode == "buy_sell" and self.episode_initial_base:
            raise ValueError("buy_sell episode requires fresh inventory")
        if self.trade_mode == "buy_sell" and not self.use_trailing_exit and not self.episode_enabled:
            raise ValueError("buy_sell mode requires the trailing exit because no other ordinary exit is enabled")

        unsupported_exit_settings = {
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "time_limit": self.time_limit,
            "take_profit_pct": self.take_profit_pct if self.take_profit_pct > 0 else None,
            "max_hold_time_seconds": self.max_hold_time_seconds if self.max_hold_time_seconds > 0 else None,
        }
        configured_unsupported_exits = [
            name for name, value in unsupported_exit_settings.items() if value is not None
        ]
        if configured_unsupported_exits:
            raise ValueError(
                "unsupported exit settings are not wired to OK RSI executors: "
                + ", ".join(configured_unsupported_exits)
            )

        activation = Decimal(str(self.trailing_activation_pct))
        delta = Decimal(str(self.trailing_delta_pct))
        locked_profit = Decimal(str(self.trailing_min_locked_profit_pct))
        if delta >= activation:
            raise ValueError("trailing delta must be below trailing activation")
        if activation - delta < locked_profit:
            raise ValueError("trailing anchors must preserve the minimum locked profit")
        if self.trailing_activation_min_pct >= self.trailing_activation_max_pct:
            raise ValueError("trailing activation minimum must be below maximum")
        if self.trailing_delta_min_pct >= self.trailing_delta_max_pct:
            raise ValueError("trailing delta minimum must be below maximum")
        if self.trailing_min_multiplier > self.trailing_max_multiplier:
            raise ValueError("trailing minimum multiplier must not exceed maximum multiplier")
        if Decimal(str(self.trailing_activation_max_pct)) - Decimal(str(self.trailing_delta_max_pct)) < locked_profit:
            raise ValueError("trailing maximum bounds must preserve the minimum locked profit")
        if self.trailing_stop_order_type == OrderType.MARKET and self.trailing_stop_limit_max_age_seconds != 0:
            raise ValueError("MARKET trailing requires a zero limit fallback timeout")
        return self
