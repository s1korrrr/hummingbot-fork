"""Public OK RSI configuration, kept separate from runtime orchestration."""

import math
from decimal import Decimal
from typing import List, Literal, Optional

from pydantic import field_validator, model_validator

from controllers.directional_trading import hl_rsi
from controllers.directional_trading.ok_rsi_core.down_only_entry import DownOnlyMakerParameters
from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.core.data_type.common import OrderType, PositionMode
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.okx_spot_candles.constants import INTERVALS as OKX_CANDLE_INTERVALS
from hummingbot.strategy_v2.components.data_reader import DataPolicy
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy


class OKRSIControllerConfig(hl_rsi.HLRSIControllerConfig):
    def get_controller_class(self):
        # ControllerConfigBase otherwise searches this helper module, while
        # the public controller remains in directional_trading.ok_rsi.
        from controllers.directional_trading.ok_rsi import OKRSIController
        return OKRSIController

    @property
    def uses_candle_service(self) -> bool:
        return self.data is not None

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
    durable_candle_watermark_enabled: bool = hl_rsi.Field(default=False)
    # Opt-in: read confirmed 1m candles from the shared candle service (same keys as V2's `data`). Omitted, the native
    # OKX feed is used unchanged. `candles_connector` stays the `okx` identity namespace either way.
    data: Optional[DataPolicy] = hl_rsi.Field(default=None, json_schema_extra={"is_updatable": False})
    episode_enabled: bool = hl_rsi.Field(default=False)
    episode_adaptive_entry_pricing: bool = False
    episode_adaptive_reserved_entry: bool = False
    episode_adaptive_reference_fallback: bool = False
    episode_thin_scout_release: bool = False
    episode_thin_scout_rearm: bool = False
    episode_profitable_rearm: bool = False
    episode_deep_recovery_capital_release: bool = False
    episode_deep_recovery_rotation_exit: bool = False
    episode_deep_recovery_strength_runner: bool = False
    episode_scout_probe_rebound: bool = False
    episode_scout_probe_fraction: Decimal = hl_rsi.Field(
        default=Decimal("0.5"), gt=Decimal("0"), lt=Decimal("1")
    )
    episode_scout_rebound_confirmation: bool = False
    episode_recovery_aware_scout: bool = False
    episode_recovery_rebound_scout: bool = False
    episode_structure_held_runner: bool = False
    episode_markup_release_runner: bool = False
    episode_bounded_markup_runner: bool = False
    episode_stage_preserving_runner: bool = False
    episode_next_stage_confirmation: bool = False
    episode_earned_stage_risk_skim: bool = False
    episode_earned_stage_risk_skim_fraction: Decimal = hl_rsi.Field(
        default=Decimal("0.1"), gt=Decimal("0"), le=Decimal("1")
    )
    episode_fee_rate: Decimal = hl_rsi.Field(default=Decimal("0.001"), ge=0, lt=Decimal("0.1"))
    episode_assignment_id: Optional[str] = None
    episode_initial_base: Decimal = hl_rsi.Field(default=Decimal("0"), ge=0)
    episode_initial_cost: Optional[Decimal] = hl_rsi.Field(default=None, gt=0)
    episode_budget_quote: Optional[Decimal] = hl_rsi.Field(default=None, gt=0)
    episode_sequential_scale_ins: bool = False
    episode_require_scale_in_signal: bool = False
    episode_scale_in_setup_bars: int = hl_rsi.Field(default=10, ge=1, le=600)
    episode_scale_in_stabilization_bars: int = hl_rsi.Field(default=2, ge=1, le=240)
    episode_rebound_atr: Decimal = hl_rsi.Field(default=Decimal(".25"), gt=0)
    episode_level_fractions: List[Decimal] = hl_rsi.Field(
        default_factory=lambda: list(map(Decimal, (".10", ".15", ".20", ".25", ".30")))
    )
    episode_dip_atr_levels: List[Decimal] = hl_rsi.Field(
        default_factory=lambda: list(map(Decimal, ("2", "3", "4", "5", "6")))
    )
    episode_basis_improvement_atr: Decimal = hl_rsi.Field(default=Decimal(".05"), ge=0)
    episode_last_fill_gap_atr: Decimal = hl_rsi.Field(default=Decimal(".25"), ge=0)
    episode_distribution_atr_levels: List[Decimal] = hl_rsi.Field(
        default_factory=lambda: list(map(Decimal, ("4", "7", "10")))
    )
    episode_remaining_fractions: List[Decimal] = hl_rsi.Field(
        default_factory=lambda: list(map(Decimal, (".8", ".6", ".4")))
    )
    episode_trail_atr: Decimal = hl_rsi.Field(default=Decimal("2"), gt=0)
    episode_trail_min: Decimal = hl_rsi.Field(default=Decimal(".005"), gt=0, lt=1)
    episode_trail_max: Decimal = hl_rsi.Field(default=Decimal(".04"), gt=0, lt=1)
    episode_hostile_risk_latch: bool = False
    episode_recovery_exit_enabled: bool = False

    derived_higher_timeframe_max_records: int = hl_rsi.Field(
        default=100,
        ge=20,
        le=2000,
        client_data=ClientFieldData(
            prompt=lambda mi: "Derived higher-timeframe records:",
            prompt_on_new=False,
            is_updatable=False,
        ),
    )

    sell_only_strict_fraction: bool = hl_rsi.Field(
        default=False,
        description="Never uplift a sell-only fraction or sweep its dust remainder; skip below venue minimums.",
        json_schema_extra={"is_updatable": False},
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
    sell_remainder_sweep_max_quote: Decimal = hl_rsi.Field(
        default=Decimal("5"),
        ge=Decimal("0"),
        description="Maximum extra same-controller owned inventory included in a partial SELL exit.",
        json_schema_extra={"is_updatable": False},
    )

    use_trailing_exit: bool = hl_rsi.Field(default=True, json_schema_extra={"is_updatable": True})
    trailing_activation_pct: float = hl_rsi.Field(default=0.015, gt=0.0, json_schema_extra={"is_updatable": True})
    trailing_delta_pct: float = hl_rsi.Field(default=0.0035, gt=0.0, json_schema_extra={"is_updatable": True})
    entry_acquisition_policy: Literal[
        "legacy_single_attempt", "down_only_maker_v1"
    ] = "legacy_single_attempt"
    entry_acquisition_shadow_policy: Optional[Literal["down_only_maker_v1"]] = None
    entry_limit_max_age_seconds: int = hl_rsi.Field(
        default=30, ge=1, le=300, json_schema_extra={"is_updatable": True}
    )
    entry_total_max_age_seconds: int = hl_rsi.Field(default=300, ge=30, le=900)
    entry_activation_bound_pct: Decimal = hl_rsi.Field(default=Decimal("0.0005"), gt=0, lt=1)
    entry_activation_bound_up_pct: Optional[Decimal] = hl_rsi.Field(default=None)
    entry_upward_bound_hysteresis_candles: int = hl_rsi.Field(default=0, ge=0, le=2)
    entry_max_downward_reprices: int = hl_rsi.Field(default=3, ge=1, le=10)
    entry_reprice_cooldown_seconds: int = hl_rsi.Field(default=5, ge=1, le=30)
    entry_reprice_min_ticks: int = hl_rsi.Field(default=3, ge=1, le=100)
    entry_reprice_atr_fraction: Decimal = hl_rsi.Field(default=Decimal("0.05"), gt=0, le=1)
    use_adaptive_trailing: bool = hl_rsi.Field(default=True, json_schema_extra={"is_updatable": True})
    trailing_activation_min_pct: float = hl_rsi.Field(default=0.003, gt=0.0, json_schema_extra={"is_updatable": True})
    trailing_activation_max_pct: float = hl_rsi.Field(default=0.05, gt=0.0, json_schema_extra={"is_updatable": True})
    trailing_delta_min_pct: float = hl_rsi.Field(default=0.001, gt=0.0, json_schema_extra={"is_updatable": True})
    trailing_delta_max_pct: float = hl_rsi.Field(default=0.02, gt=0.0, json_schema_extra={"is_updatable": True})
    trailing_min_locked_profit_pct: float = hl_rsi.Field(default=0.004, gt=0.0, json_schema_extra={"is_updatable": True})
    trailing_activation_volatility_exponent: float = hl_rsi.Field(default=0.70, gt=0.0, le=2.0, json_schema_extra={"is_updatable": True})
    trailing_delta_volatility_exponent: float = hl_rsi.Field(default=0.35, gt=0.0, le=2.0, json_schema_extra={"is_updatable": True})
    trailing_min_multiplier: float = hl_rsi.Field(default=0.5, gt=0.0, json_schema_extra={"is_updatable": True})
    trailing_max_multiplier: float = hl_rsi.Field(default=2.5, gt=0.0, json_schema_extra={"is_updatable": True})

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
        down_only_active = self.entry_acquisition_policy == "down_only_maker_v1"
        down_only_shadow = self.entry_acquisition_shadow_policy == "down_only_maker_v1"
        if down_only_active and down_only_shadow:
            raise ValueError("active and shadow entry acquisition policies are mutually exclusive")
        if self.episode_enabled and (down_only_active or down_only_shadow):
            raise ValueError("down-only maker entry is incompatible with episode mode")
        if down_only_active and self.trade_mode == "sell_only":
            raise ValueError("active down-only maker entry is unavailable in sell_only mode")
        if self.durable_candle_watermark_enabled and self.episode_enabled:
            raise ValueError(
                "durable candle watermark currently guards only the legacy action path"
            )
        candles_connector = str(self.candles_connector or "okx").strip().lower()
        if candles_connector == "okx_paper_trade":
            candles_connector = "okx"
        service_requested = candles_connector == "okx_service"
        if service_requested:
            # Selector alias, normalized like okx_paper_trade: the namespace of watermarks and recorder rows stays okx.
            candles_connector = "okx"
        if candles_connector != "okx":
            raise ValueError("OK RSI requires OKX spot candles")
        object.__setattr__(self, "candles_connector", candles_connector)
        if self.data is not None and (
            self.data.mode != "shared"
            or self.data.transport != "stream"
            or self.data.source_profile != "native"
            or self.data.symbols
        ):
            raise ValueError(
                "OK RSI data must be omitted or use mode shared, transport stream, native profile and no symbols"
            )
        if service_requested and self.data is None:
            raise ValueError("candles_connector okx_service requires a shared stream data block (origin, token_file)")

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

        if self.entry_activation_bound_up_pct is not None:
            up_pct = self.entry_activation_bound_up_pct
            if up_pct <= 0 or up_pct >= 1:
                raise ValueError("entry_activation_bound_up_pct must be finite and between 0 and 1")
            if up_pct < self.entry_activation_bound_pct:
                raise ValueError("entry_activation_bound_up_pct must be at least entry_activation_bound_pct")

        if (not self.episode_enabled and self.sell_only_if_profitable
                and self.recovery_sell_execution_strategy == ExecutionStrategy.MARKET):
            raise ValueError('Profitable-only recovery requires a limit execution strategy')
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
        if self.episode_markup_release_runner and not self.episode_structure_held_runner:
            raise ValueError("markup release runner requires structure held runner")
        if (
            self.episode_bounded_markup_runner
            and not self.episode_markup_release_runner
        ):
            raise ValueError("bounded markup runner requires markup release runner")
        if (
            self.episode_stage_preserving_runner
            and not self.episode_bounded_markup_runner
        ):
            raise ValueError("stage preserving runner requires bounded markup runner")
        if (
            self.episode_next_stage_confirmation
            and not self.episode_stage_preserving_runner
        ):
            raise ValueError("next stage confirmation requires stage preserving runner")
        if (
            self.episode_earned_stage_risk_skim
            and not self.episode_next_stage_confirmation
        ):
            raise ValueError("earned stage risk skim requires next stage confirmation")
        if self.episode_adaptive_reserved_entry and not self.episode_adaptive_entry_pricing:
            raise ValueError("reserved adaptive entry requires adaptive entry pricing")
        if self.episode_adaptive_reference_fallback and not self.episode_adaptive_reserved_entry:
            raise ValueError("reference fallback requires reserved adaptive entry")
        if self.episode_thin_scout_rearm and not self.episode_thin_scout_release:
            raise ValueError("thin scout rearm requires thin scout release")
        if (
            self.episode_deep_recovery_rotation_exit
            and not self.episode_deep_recovery_capital_release
        ):
            raise ValueError(
                "deep recovery rotation requires deep recovery capital release"
            )
        if (
            self.episode_deep_recovery_strength_runner
            and not self.episode_deep_recovery_capital_release
        ):
            raise ValueError(
                "deep recovery strength runner requires deep recovery capital release"
            )
        if (
            self.episode_deep_recovery_strength_runner
            and self.episode_deep_recovery_rotation_exit
        ):
            raise ValueError(
                "deep recovery strength runner and rotation are mutually exclusive"
            )
        if self.episode_scout_probe_rebound and (
            self.episode_scout_rebound_confirmation
            or self.episode_recovery_aware_scout
            or self.episode_recovery_rebound_scout
        ):
            raise ValueError("scout probe treatment cannot be combined")
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

    def down_only_maker_parameters(self) -> DownOnlyMakerParameters:
        hysteresis_seconds = 0
        if self.entry_upward_bound_hysteresis_candles:
            hysteresis_seconds = (
                int(self.entry_upward_bound_hysteresis_candles)
                * int(CandlesBase.interval_to_seconds[self.interval])
            )
        return DownOnlyMakerParameters(
            total_max_age_seconds=self.entry_total_max_age_seconds,
            max_reprices=self.entry_max_downward_reprices,
            cooldown_seconds=self.entry_reprice_cooldown_seconds,
            min_ticks=self.entry_reprice_min_ticks,
            atr_fraction=self.entry_reprice_atr_fraction,
            activation_bound_pct=self.entry_activation_bound_pct,
            activation_bound_up_pct=self.entry_activation_bound_up_pct,
            upward_hysteresis_seconds=hysteresis_seconds,
        )
