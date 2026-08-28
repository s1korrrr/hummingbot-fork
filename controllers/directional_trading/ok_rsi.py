"""OKX spot specialization of the shared RSI v5 controller.

The strategy, regime, inventory, recovery, DCA, and status logic lives in
``hl_rsi``. This module deliberately contains only the venue contract,
sell-only sizing, continuous trailing selection, and strict executor-boundary
checks that differ for OKX spot.
"""

import logging
import math
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional

import pandas as pd
from pydantic import field_validator, model_validator

from controllers.directional_trading import hl_rsi
from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.okx_spot_candles.constants import INTERVALS as OKX_CANDLE_INTERVALS
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)

_DECIMAL_CONVERSION_ERRORS = (ArithmeticError, TypeError, ValueError)
_EXECUTION_INPUT_ERRORS = (ArithmeticError, AttributeError, KeyError, RuntimeError, TypeError, ValueError)

_CANDLE_VALUE_COLUMNS = ("open", "high", "low", "close")
_CANDLE_SUM_COLUMNS = (
    "volume",
    "quote_asset_volume",
    "n_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
)


class ExecutorInputUnavailable(ValueError):
    """Raised when an executor cannot be constructed from valid exchange inputs."""


def derive_higher_timeframe_candles(
    base_candles: pd.DataFrame,
    *,
    base_interval: str,
    target_interval: str,
    current_time: float,
    max_records: int,
) -> pd.DataFrame:
    """Aggregate complete, consecutive base candles into UTC-aligned higher-timeframe bars."""
    output_columns = [
        "timestamp",
        *[column for column in (*_CANDLE_VALUE_COLUMNS, *_CANDLE_SUM_COLUMNS) if column in base_candles.columns],
    ]
    if base_candles.empty or not {"timestamp", *_CANDLE_VALUE_COLUMNS}.issubset(base_candles.columns):
        return pd.DataFrame(columns=output_columns)

    base_seconds = CandlesBase.interval_to_seconds[base_interval]
    target_seconds = CandlesBase.interval_to_seconds[target_interval]
    if target_seconds <= base_seconds or target_seconds % base_seconds != 0:
        raise ValueError("target interval must be a larger whole multiple of the base interval")

    frame = base_candles[output_columns].copy()
    frame["timestamp"] = pd.to_numeric(frame["timestamp"], errors="coerce")
    numeric_columns = [column for column in output_columns if column != "timestamp"]
    frame[numeric_columns] = frame[numeric_columns].apply(pd.to_numeric, errors="coerce")
    frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp")
    frame["timestamp"] = frame["timestamp"].astype("int64")
    frame["bucket_timestamp"] = (frame["timestamp"] // target_seconds) * target_seconds
    frame = frame[frame["bucket_timestamp"] + target_seconds <= int(current_time)]

    expected_count = target_seconds // base_seconds
    records = []
    for bucket_timestamp, bucket in frame.groupby("bucket_timestamp", sort=True):
        expected_timestamps = list(
            range(int(bucket_timestamp), int(bucket_timestamp) + target_seconds, base_seconds)
        )
        if bucket["timestamp"].tolist() != expected_timestamps or len(bucket) != expected_count:
            continue
        if bucket[numeric_columns].isna().any().any():
            continue
        if not bucket[numeric_columns].map(math.isfinite).all().all():
            continue
        record = {
            "timestamp": int(bucket_timestamp),
            "open": bucket["open"].iloc[0],
            "high": bucket["high"].max(),
            "low": bucket["low"].min(),
            "close": bucket["close"].iloc[-1],
        }
        for column in _CANDLE_SUM_COLUMNS:
            if column in bucket.columns:
                record[column] = bucket[column].sum()
        else:
            records.append(record)

    return pd.DataFrame(records, columns=output_columns).tail(max_records).reset_index(drop=True)


@dataclass(frozen=True)
class ContinuousTrailingSelection:
    activation: Decimal
    delta: Decimal
    volatility_ratio: Decimal
    initial_locked_profit: Decimal
    policy: str = "adaptive_continuous"


def calculate_continuous_adaptive_trailing(
    *,
    atr_pct: float,
    baseline_atr_pct: float,
    base_activation: Decimal,
    base_delta: Decimal,
    activation_min: Decimal,
    activation_max: Decimal,
    delta_min: Decimal,
    delta_max: Decimal,
    min_locked_profit: Decimal,
    activation_exponent: float,
    delta_exponent: float,
    min_multiplier: float,
    max_multiplier: float,
) -> ContinuousTrailingSelection:
    numeric_values = (
        atr_pct,
        baseline_atr_pct,
        activation_exponent,
        delta_exponent,
        min_multiplier,
        max_multiplier,
    )
    decimal_values = (
        base_activation,
        base_delta,
        activation_min,
        activation_max,
        delta_min,
        delta_max,
        min_locked_profit,
    )
    if not all(math.isfinite(float(value)) for value in numeric_values):
        raise ValueError("continuous trailing inputs must be finite")
    if not all(value.is_finite() for value in decimal_values):
        raise ValueError("continuous trailing inputs must be finite")
    if atr_pct <= 0 or baseline_atr_pct <= 0:
        raise ValueError("continuous trailing ATR inputs must be positive")
    if min_multiplier <= 0 or max_multiplier < min_multiplier:
        raise ValueError("continuous trailing multiplier bounds are invalid")
    if not (Decimal("0") < activation_min < activation_max):
        raise ValueError("continuous trailing activation bounds are invalid")
    if not (Decimal("0") < delta_min < delta_max):
        raise ValueError("continuous trailing delta bounds are invalid")
    if base_activation <= 0 or base_delta <= 0 or min_locked_profit <= 0:
        raise ValueError("continuous trailing price distances must be positive")

    ratio = atr_pct / baseline_atr_pct
    activation_multiplier = max(min_multiplier, min(max_multiplier, ratio ** activation_exponent))
    delta_multiplier = max(min_multiplier, min(max_multiplier, ratio ** delta_exponent))
    activation = base_activation * Decimal(str(activation_multiplier))
    delta = base_delta * Decimal(str(delta_multiplier))
    activation = max(activation_min, min(activation_max, activation))
    delta = max(delta_min, min(delta_max, delta))
    activation = max(activation, delta + min_locked_profit)
    if activation > activation_max or activation - delta < min_locked_profit:
        raise ValueError("continuous trailing bounds cannot preserve minimum locked profit")
    return ContinuousTrailingSelection(
        activation=activation,
        delta=delta,
        volatility_ratio=Decimal(str(ratio)),
        initial_locked_profit=activation - delta,
    )


class OKRSIControllerConfig(hl_rsi.HLRSIControllerConfig):
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
        if self.trade_mode == "buy_sell" and not self.use_trailing_exit:
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


class OKRSIController(hl_rsi.HLRSIController):
    """Shared RSI v5 behavior with OKX spot execution and trailing contracts."""

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
            cls._logger.setLevel(logging.INFO)
        return cls._logger

    def __init__(self, config: OKRSIControllerConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self._last_trailing_selection: Optional[ContinuousTrailingSelection] = None

    def _derived_base_max_records(self) -> int:
        if not self.config.trend_confirmation_enabled:
            return self.max_records
        base_seconds = CandlesBase.interval_to_seconds[self.config.interval]
        ratios = [1]
        for interval in (
            self.config.trend_confirmation_medium_interval,
            self.config.trend_confirmation_long_interval,
        ):
            if interval:
                ratios.append(CandlesBase.interval_to_seconds[interval] // base_seconds)
        return max(self.max_records, max(ratios) * self.config.derived_higher_timeframe_max_records)

    def _build_default_candles_config(self) -> List[hl_rsi.CandlesConfig]:
        return [
            hl_rsi.CandlesConfig(
                connector=self.config.candles_connector or self.config.connector_name,
                trading_pair=self.config.candles_trading_pair or self.config.trading_pair,
                interval=self.config.interval,
                max_records=self._derived_base_max_records(),
            )
        ]

    def _get_interval_candles_df(self, interval: Optional[str]) -> Optional[pd.DataFrame]:
        if not interval:
            return None
        try:
            base_candles = self.market_data_provider.get_candles_df(
                connector_name=self.config.candles_connector or self.config.connector_name,
                trading_pair=self.config.candles_trading_pair or self.config.trading_pair,
                interval=self.config.interval,
                max_records=self._derived_base_max_records(),
            )
        except _EXECUTION_INPUT_ERRORS:
            return None
        if base_candles is None or base_candles.empty:
            return None
        if interval == self.config.interval:
            return base_candles.copy()
        derived = derive_higher_timeframe_candles(
            base_candles,
            base_interval=self.config.interval,
            target_interval=interval,
            current_time=self.market_data_provider.time(),
            max_records=self.config.derived_higher_timeframe_max_records,
        )
        return derived if not derived.empty else None

    def _get_mid_price(self) -> Optional[Decimal]:
        try:
            value = self.market_data_provider.get_price_by_type(
                self.config.connector_name,
                self.config.trading_pair,
                PriceType.MidPrice,
            )
            price = Decimal(str(value)) if value is not None else None
            return price if price is not None and price.is_finite() and price > 0 else None
        except (AttributeError, InvalidOperation, ValueError, TypeError):
            return None

    def _get_available_asset_balance(self, asset: Optional[str]) -> Optional[Decimal]:
        """Use only exchange-reported available balance; total balance may include reservations."""
        if asset is None:
            return None
        try:
            raw_available = self.market_data_provider.get_available_balance(
                self.config.connector_name,
                asset,
            )
        except _EXECUTION_INPUT_ERRORS:
            return None
        return self._decimal_or_none(raw_available)

    def get_custom_info(self) -> dict:
        custom_info = super().get_custom_info()
        custom_info["trailing_policy"] = self._trailing_policy_snapshot()
        return custom_info

    def get_status_summary(self) -> Dict[str, object]:
        summary = super().get_status_summary()
        summary["trailing_policy"] = self._trailing_policy_snapshot()
        return summary

    def _trailing_policy_snapshot(self) -> Dict[str, object]:
        selection = self._last_trailing_selection
        return {
            "policy": selection.policy if selection is not None else (
                "adaptive_continuous" if self.config.use_adaptive_trailing else "fixed"
            ),
            "anchor_activation_pct": self.config.trailing_activation_pct,
            "anchor_delta_pct": self.config.trailing_delta_pct,
            "selected_activation_pct": float(selection.activation) if selection is not None else None,
            "selected_delta_pct": float(selection.delta) if selection is not None else None,
            "initial_locked_profit_pct": float(selection.initial_locked_profit) if selection is not None else None,
            "volatility_ratio": float(selection.volatility_ratio) if selection is not None else None,
            "atr_pct": self._latest_atr_pct,
            "atr_baseline_pct": self.config.trailing_atr_baseline,
        }

    def _has_usable_trading_rule(self, rule: Optional[TradingRule]) -> bool:
        return bool(
            rule is not None
            and self._rule_decimal(rule, "min_price_increment") > 0
            and self._base_amount_increment(rule) > 0
        )

    def _require_trading_rule(self) -> TradingRule:
        rule = self._get_trading_rule()
        if not self._has_usable_trading_rule(rule):
            raise ExecutorInputUnavailable(
                f"usable trading rule is unavailable for {self.config.connector_name} "
                f"{self.config.trading_pair}"
            )
        return rule

    def _quantize_amount(self, amount: Decimal) -> Decimal:
        try:
            quantized = Decimal(
                str(
                    self.market_data_provider.quantize_order_amount(
                        self.config.connector_name,
                        self.config.trading_pair,
                        amount,
                    )
                )
            )
        except _EXECUTION_INPUT_ERRORS as exc:
            raise ExecutorInputUnavailable(
                f"unable to quantize order amount for {self.config.connector_name} {self.config.trading_pair}"
            ) from exc
        if not quantized.is_finite() or quantized <= 0:
            raise ExecutorInputUnavailable(
                f"amount quantization produced a non-positive value for "
                f"{self.config.connector_name} {self.config.trading_pair}"
            )
        return quantized

    def _shade_limit_maker_price(self, *, trade_type: TradeType, fallback_price: Decimal) -> Decimal:
        try:
            price = super()._shade_limit_maker_price(
                trade_type=trade_type,
                fallback_price=fallback_price,
            )
        except _EXECUTION_INPUT_ERRORS as exc:
            raise ExecutorInputUnavailable(
                f"unable to resolve executable {trade_type.name} price for "
                f"{self.config.connector_name} {self.config.trading_pair}"
            ) from exc
        if not price.is_finite() or price <= 0:
            raise ExecutorInputUnavailable(
                f"executable {trade_type.name} price is unavailable for "
                f"{self.config.connector_name} {self.config.trading_pair}"
            )
        return price

    def can_create_executor(self, signal: int) -> bool:
        if signal == 0:
            return False
        target_side = TradeType.BUY if signal > 0 else TradeType.SELL
        if signal > 0 and self.config.use_adaptive_trailing:
            atr_pct = self._latest_atr_pct
            if atr_pct is None or not math.isfinite(float(atr_pct)) or atr_pct <= 0:
                self._record_gate_reason(target_side, "trailing_input_unavailable")
                return False
        if self._get_mid_price() is None:
            self._record_gate_reason(target_side, "price_unavailable")
            return False
        if not self._has_usable_trading_rule(self._get_trading_rule()):
            self._record_gate_reason(target_side, "trading_rule_unavailable")
            return False
        try:
            return super().can_create_executor(signal)
        except ExecutorInputUnavailable:
            self._record_gate_reason(target_side, "execution_input_unavailable")
            return False

    def _sell_only_target_amount(self, position_amount: Decimal, price: Decimal) -> Decimal:
        if position_amount <= 0 or price <= 0:
            return Decimal("0")
        if position_amount * price < self.config.sell_all_below_quote:
            return position_amount
        return position_amount * self.config.sell_only_inventory_fraction

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        if trade_type == TradeType.BUY and self.config.trade_mode == "sell_only":
            raise ValueError("BUY executor construction is forbidden when trade_mode='sell_only'")
        try:
            price = Decimal(str(price))
        except _DECIMAL_CONVERSION_ERRORS as exc:
            raise ExecutorInputUnavailable("executor construction requires a positive finite price") from exc
        if not price.is_finite() or price <= 0:
            raise ExecutorInputUnavailable("executor construction requires a positive finite price")
        self._require_trading_rule()
        state = (self.processed_data or {}).get("signal_state", {})

        if trade_type == TradeType.SELL:
            ref_price = self._shade_limit_maker_price(
                trade_type=TradeType.SELL,
                fallback_price=price,
            )
            position_amount = self._get_sellable_inventory_amount()
            if not position_amount.is_finite() or position_amount <= 0:
                raise ExecutorInputUnavailable(
                    "SELL executor construction requires positive sellable inventory"
                )
            if self.config.trade_mode == "sell_only":
                target_amount = self._sell_only_target_amount(position_amount, ref_price)
            else:
                target_amount = position_amount
                slice_multiplier = Decimal(str(self.config.signal_sell_inventory_slice_multiplier))
                if slice_multiplier > 0:
                    target_quote = self.config.usd_per_entry * slice_multiplier
                    target_amount = min(position_amount, target_quote / ref_price)
            sell_amount = self._resolve_recovery_sell_amount(
                position_amount=position_amount,
                target_amount=target_amount,
                price=ref_price,
                available_base=position_amount,
            )
            if not sell_amount.is_finite() or sell_amount <= 0:
                raise ExecutorInputUnavailable(
                    "SELL executor construction could not produce a valid sell amount"
                )
            return OrderExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=trade_type,
                amount=sell_amount,
                price=ref_price,
                execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                position_action=PositionAction.CLOSE,
                leverage=self.config.leverage,
                level_id="signal_exit",
            )

        planned_usd, effective_usd, entry_price, entry_amount_base, uplifted = self._resolve_buy_entry_plan(
            price,
            state,
        )
        if (
            not entry_price.is_finite()
            or entry_price <= 0
            or not entry_amount_base.is_finite()
            or entry_amount_base <= 0
        ):
            raise ExecutorInputUnavailable(
                "BUY executor construction could not produce a positive finite price and amount"
            )
        if uplifted:
            self.logger().debug(
                f"BUY min-notional uplift | {self._controller_pair_log_prefix()} "
                f"role={str(state.get('buy_entry_role', 'full') or 'full')} "
                f"planned={planned_usd:.6f} effective={effective_usd:.6f} entry_price={entry_price:.6f}"
            )

        base_activation = Decimal(str(self.config.trailing_activation_pct))
        base_delta = Decimal(str(self.config.trailing_delta_pct))
        if self.config.use_adaptive_trailing:
            if self._latest_atr_pct is None:
                raise ExecutorInputUnavailable("continuous adaptive trailing ATR is unavailable")
            selection = calculate_continuous_adaptive_trailing(
                atr_pct=self._latest_atr_pct,
                baseline_atr_pct=self.config.trailing_atr_baseline,
                base_activation=base_activation,
                base_delta=base_delta,
                activation_min=Decimal(str(self.config.trailing_activation_min_pct)),
                activation_max=Decimal(str(self.config.trailing_activation_max_pct)),
                delta_min=Decimal(str(self.config.trailing_delta_min_pct)),
                delta_max=Decimal(str(self.config.trailing_delta_max_pct)),
                min_locked_profit=Decimal(str(self.config.trailing_min_locked_profit_pct)),
                activation_exponent=self.config.trailing_activation_volatility_exponent,
                delta_exponent=self.config.trailing_delta_volatility_exponent,
                min_multiplier=self.config.trailing_min_multiplier,
                max_multiplier=self.config.trailing_max_multiplier,
            )
        else:
            selection = ContinuousTrailingSelection(
                activation=base_activation,
                delta=base_delta,
                volatility_ratio=Decimal("1"),
                initial_locked_profit=base_activation - base_delta,
                policy="fixed",
            )
        self._last_trailing_selection = selection

        activation_bounds_cfg: Optional[List[Decimal]] = None
        gap_bound = self._effective_price_gap()
        if gap_bound is not None and gap_bound > Decimal("0"):
            activation_bounds_cfg = [gap_bound]

        triple_barrier = TripleBarrierConfig(
            stop_loss=None,
            take_profit=None,
            time_limit=None,
            trailing_stop=TrailingStop(
                activation_price=selection.activation,
                trailing_delta=selection.delta,
            ),
            open_order_type=OrderType.LIMIT_MAKER,
            open_order_limit_max_age_seconds=self.config.entry_limit_max_age_seconds,
            take_profit_order_type=self.config.take_profit_order_type,
            trailing_stop_order_type=self.config.trailing_stop_order_type,
            trailing_stop_limit_max_age_seconds=self.config.trailing_stop_limit_max_age_seconds,
            stop_loss_order_type=OrderType.MARKET,
            time_limit_order_type=OrderType.MARKET,
        )
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            side=trade_type,
            entry_price=entry_price,
            amount=entry_amount_base,
            triple_barrier_config=triple_barrier,
            leverage=self.config.leverage,
            activation_bounds=activation_bounds_cfg,
            level_id=(
                str(state.get("buy_entry_role"))
                if str(state.get("buy_entry_role")) in {"scout", "runner"}
                else None
            ),
        )
