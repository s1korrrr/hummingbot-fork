"""OKX spot specialization of the shared RSI v5 controller.

Legacy signal, inventory and DCA behavior remains in ``hl_rsi``. Configuration,
candle aggregation and pure trailing calculations are re-exported from
``ok_rsi_core``. The opt-in episode runtime owns its entire action path for both
buy/sell and explicitly assigned sell-only inventory.
"""

import logging
import math
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional

import pandas as pd

from controllers.directional_trading import hl_rsi
from controllers.directional_trading.ok_rsi_core.candles import derive_higher_timeframe_candles
from controllers.directional_trading.ok_rsi_core.config import OKRSIControllerConfig
from controllers.directional_trading.ok_rsi_core.trailing import (
    ContinuousTrailingSelection,
    calculate_continuous_adaptive_trailing,
)
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)

_DECIMAL_CONVERSION_ERRORS = (ArithmeticError, TypeError, ValueError)
_EXECUTION_INPUT_ERRORS = (ArithmeticError, AttributeError, KeyError, RuntimeError, TypeError, ValueError)


class ExecutorInputUnavailable(ValueError):
    """Raised when an executor cannot be constructed from valid exchange inputs."""


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
        episode_store = kwargs.pop("episode_store", None)
        super().__init__(config, *args, **kwargs)
        self._last_trailing_selection: Optional[ContinuousTrailingSelection] = None
        self._episode_runtime = None
        if config.episode_enabled:
            from controllers.directional_trading.ok_rsi_core.runtime import ClosedCandleProvider, EpisodeRuntime
            self.market_data_provider = ClosedCandleProvider(self.market_data_provider)
            self._episode_runtime = EpisodeRuntime(self, episode_store)
        else:
            from controllers.directional_trading.ok_rsi_core.runtime import guard_disabled_restore
            guard_disabled_restore(config, episode_store)

    def determine_executor_actions(self):
        if self._episode_runtime is not None:
            return self._episode_runtime.determine_actions()
        return super().determine_executor_actions()

    def stop_actions_proposal(self):
        if self._episode_runtime is not None:
            return []
        return super().stop_actions_proposal()

    def get_executor_class_for_config(self, config):
        runtime = self._episode_runtime
        if runtime is None:
            return super().get_executor_class_for_config(config)
        from controllers.directional_trading.ok_rsi_core.execution import (
            EPISODE_LEVEL_PREFIX,
            EpisodeOrderExecutor,
            EpisodeOrderExecutorConfig,
        )
        if not isinstance(config, EpisodeOrderExecutorConfig):
            raise ValueError("episode request requires its bounded native executor configuration")
        progress = runtime.state.orders.get(config.id)
        claimed = getattr(self, "_episode_factory_claimed_ids", set())
        if progress is None or progress.done or progress.seen or config.id in claimed:
            raise ValueError("episode request is unknown, settled, or already dispatched")
        request = progress.request
        if runtime.hold_reason or runtime.state.fault or self.config.manual_kill_switch:
            raise ValueError("episode executor construction is on hold")
        if request.side not in {"BUY", "SELL"} or (request.side == "BUY" and self.config.trade_mode == "sell_only"):
            raise ValueError("episode mode forbids this executor side")
        expected_timestamp = request.expires_at - runtime.config.setup_bars * runtime.config.interval_seconds
        expected_action = PositionAction.OPEN if request.side == "BUY" else PositionAction.CLOSE
        if (request.episode_id != runtime.state.episode_id
                or config.type != "order_executor"
                or config.execution_strategy is not ExecutionStrategy.LIMIT_MAKER
                or config.chaser_config is not None
                or config.amount != request.amount or config.price != request.price
                or config.side is not TradeType[request.side] or config.position_action is not expected_action
                or type(config.leverage) is not int or config.leverage != 1
                or type(config.episode_level) is not int or type(config.episode_revision) is not int
                or type(config.episode_expires_at) is not int
                or config.episode_intent != request.intent
                or config.episode_setup_policy != request.setup_policy
                or config.episode_setup_started_at != request.setup_started_at
                or config.episode_setup_confirmation_started_at
                != request.setup_confirmation_started_at
                or config.episode_setup_low != request.setup_low
                or config.episode_setup_rebound != request.setup_rebound
                or config.episode_setup_no_new_low != request.setup_no_new_low
                or config.episode_exit_policy != request.exit_policy
                or config.episode_exit_hourly_recovered != request.exit_hourly_recovered
                or config.episode_exit_risk_clear != request.exit_risk_clear
                or config.episode_exit_trail_breach != request.exit_trail_breach
                or config.episode_exit_target_base != request.exit_target_base
                or config.episode_exit_target_revision != request.exit_target_revision
                or config.timestamp != expected_timestamp
                or config.controller_id != self.config.id
                or config.connector_name != self.config.connector_name or config.trading_pair != self.config.trading_pair
                or config.episode_id != request.episode_id or config.episode_level != request.level
                or config.episode_revision != request.revision or config.episode_expires_at != request.expires_at
                or config.level_id != f"{EPISODE_LEVEL_PREFIX}{request.episode_id}:{request.level}"):
            raise ValueError("executor parameters differ from durable episode authorization")
        # The orchestrator resolves immediately before constructing and starting
        # the executor. A constructor failure remains reserved and is not retried
        # implicitly. Restored unknown requests are never re-emitted by runtime.
        self._episode_factory_claimed_ids = claimed | {config.id}
        return EpisodeOrderExecutor

    def validate_runtime_state_transfer(self, source):
        previous = getattr(source, "_episode_runtime", None)
        if previous is None:
            return
        unresolved = previous.state.base > 0 or any(not order.done for order in previous.state.orders.values())
        if unresolved and (self._episode_runtime is None or self._episode_runtime.config.identity != previous.config.identity):
            raise ValueError("cannot disable or reconfigure an unresolved OK RSI episode")
        if self._episode_runtime is not None and self._episode_runtime.sequence < previous.sequence:
            raise ValueError("episode reload checkpoint is older than current runtime")

    def update_config(self, new_config):
        if self._episode_runtime is not None:
            from controllers.directional_trading.ok_rsi_core.runtime import policy_config
            if not new_config.episode_enabled or policy_config(new_config).identity != self._episode_runtime.config.identity:
                raise ValueError("episode policy changes require explicit reconciled restart")
        super().update_config(new_config)

    def _derived_base_max_records(self) -> int:
        if self.config.episode_enabled:
            from controllers.directional_trading.ok_rsi_core.context import HISTORY_RECORDS
            return max(self.max_records, HISTORY_RECORDS)
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
        if self._episode_runtime is not None:
            custom_info["episode"] = self._episode_runtime.status()
        return custom_info

    def get_status_summary(self) -> Dict[str, object]:
        summary = super().get_status_summary()
        summary["trailing_policy"] = self._trailing_policy_snapshot()
        if self._episode_runtime is not None:
            summary["episode"] = self._episode_runtime.status()
        return summary

    def _trailing_policy_snapshot(self) -> Dict[str, object]:
        if self._episode_runtime is not None:
            state = self._episode_runtime.state
            return {"policy": "episode_aggregate", "peak": str(state.peak),
                    "floor": str(state.floor), "target_base": str(state.target_base)}
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
        if self._episode_runtime is not None:
            return False  # Only the durable episode action path may reserve inventory.
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
        if self._episode_runtime is not None:
            raise ExecutorInputUnavailable("episode orders require a durable episode authorization")
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
