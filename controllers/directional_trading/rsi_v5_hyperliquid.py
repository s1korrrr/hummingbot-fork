from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple

from pydantic import model_validator

from controllers.directional_trading.rsi_v5 import Field, RSIv5Controller, RSIv5ControllerConfig
from controllers.directional_trading.rsi_v5_runtime import (
    EpisodeOutcome,
    EpisodeTracker,
    ExecutorExposure,
    ExposureSnapshot,
    build_exit_plan,
    calculate_exposure,
    update_loss_streak,
)
from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.core.data_type.common import OrderType, PositionMode, TradeType
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.executors.rsi_position_executor import RSIPositionExecutor, RSIPositionExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import ExecutorAction, StopExecutorAction

_ZERO = Decimal("0")
_POSITION_EXECUTOR_TYPES = {"position_executor"}


class RSIv5HyperliquidControllerConfig(RSIv5ControllerConfig):
    """Fail-closed Hyperliquid-perpetual safety profile for RSI v5.

    Active entry executors own their residual exposure. Deterministic exits stop
    each owning executor; the controller never creates an aggregate opposite-side
    SELL executor against exposure still managed by a PositionExecutor.
    """

    controller_name: str = "rsi_v5_hyperliquid"
    connector_name: str = Field(
        default="hyperliquid_perpetual",
        client_data=ClientFieldData(
            prompt=lambda mi: "Connector:",
            prompt_on_new=True,
            is_updatable=False,
        ),
    )
    trading_pair: str = Field(
        default="BTC-USD",
        client_data=ClientFieldData(
            prompt=lambda mi: "Trading pair:",
            prompt_on_new=True,
            is_updatable=False,
        ),
    )
    leverage: int = Field(
        default=1,
        ge=1,
        client_data=ClientFieldData(
            prompt=lambda mi: "Leverage:",
            prompt_on_new=True,
            is_updatable=False,
        ),
    )
    position_mode: PositionMode = PositionMode.ONEWAY

    collateral_asset: str = Field(
        default="USDC",
        client_data=ClientFieldData(
            prompt=lambda mi: "Collateral asset:",
            prompt_on_new=False,
            is_updatable=False,
        ),
    )
    perpetual_max_leverage: int = Field(
        default=2,
        ge=1,
        client_data=ClientFieldData(
            prompt=lambda mi: "Maximum permitted leverage:",
            prompt_on_new=False,
            is_updatable=False,
        ),
    )
    perpetual_hard_stop_loss_pct: Decimal = Field(
        default=Decimal("0.08"),
        gt=Decimal("0"),
        le=Decimal("0.25"),
        client_data=ClientFieldData(
            prompt=lambda mi: "Hard stop loss pct:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )
    perpetual_allow_position_hold: bool = Field(
        default=False,
        client_data=ClientFieldData(
            prompt=lambda mi: "Allow POSITION_HOLD on perpetuals?:",
            prompt_on_new=False,
            is_updatable=False,
        ),
    )
    executor_exit_retry_seconds: int = Field(
        default=5,
        ge=1,
        le=60,
        client_data=ClientFieldData(
            prompt=lambda mi: "Executor exit retry seconds:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )

    use_bag_freeze: bool = Field(
        default=False,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enable perpetual bag freeze?:",
            prompt_on_new=False,
            is_updatable=False,
        ),
    )
    early_stop_keep_position: bool = Field(
        default=False,
        client_data=ClientFieldData(
            prompt=lambda mi: "Keep perpetual position after early stop?:",
            prompt_on_new=False,
            is_updatable=False,
        ),
    )
    early_stop_drawdown_pct: float = Field(
        default=0.08,
        ge=0.0,
        le=0.25,
        client_data=ClientFieldData(
            prompt=lambda mi: "Controller emergency drawdown stop:",
            prompt_on_new=False,
            is_updatable=True,
        ),
    )

    @model_validator(mode="after")
    def validate_hyperliquid_perpetual_safety(self):
        if self.connector_name != "hyperliquid_perpetual":
            raise ValueError(
                "rsi_v5_hyperliquid requires connector_name=hyperliquid_perpetual"
            )
        if self.position_mode != PositionMode.ONEWAY:
            raise ValueError("rsi_v5_hyperliquid requires ONEWAY position mode")
        if self.leverage > self.perpetual_max_leverage:
            raise ValueError("leverage must not exceed perpetual_max_leverage")
        if not self.collateral_asset.strip():
            raise ValueError("collateral_asset must not be empty")
        if not self.perpetual_allow_position_hold:
            if self.use_bag_freeze:
                raise ValueError(
                    "use_bag_freeze requires perpetual_allow_position_hold=true"
                )
            if self.early_stop_keep_position:
                raise ValueError(
                    "early_stop_keep_position requires perpetual_allow_position_hold=true"
                )
        return self


class RSIv5HyperliquidController(RSIv5Controller):
    """RSI v5 adapter with executor-owned perpetual exits and episode accounting."""

    def __init__(self, config: RSIv5HyperliquidControllerConfig, *args, **kwargs):
        self._episode_tracker = EpisodeTracker()
        self._last_episode_outcome: Optional[EpisodeOutcome] = None
        self._exit_request_timestamps: Dict[str, float] = {}
        super().__init__(config, *args, **kwargs)

    @property
    def config(self) -> RSIv5HyperliquidControllerConfig:
        return self._config

    @config.setter
    def config(self, value: RSIv5HyperliquidControllerConfig) -> None:
        self._config = value

    def get_executor_class_for_config(self, config):
        if isinstance(config, RSIPositionExecutorConfig):
            return RSIPositionExecutor
        return super().get_executor_class_for_config(config)

    @staticmethod
    def _decimal(value: object, default: Decimal = _ZERO) -> Decimal:
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return default
        return parsed if parsed.is_finite() else default

    @staticmethod
    def _side_name(value: object) -> str:
        if hasattr(value, "name"):
            return str(value.name)
        return str(value or "").replace("TradeType.", "").upper()

    @staticmethod
    def _executor_is_done(executor: object) -> bool:
        explicit = getattr(executor, "is_done", None)
        if isinstance(explicit, bool):
            return explicit
        return getattr(executor, "status", None) == RunnableStatus.TERMINATED

    def _matches_buy_position_executor(self, executor: object) -> bool:
        return bool(
            getattr(executor, "type", None) in _POSITION_EXECUTOR_TYPES
            and getattr(executor, "connector_name", None) == self.config.connector_name
            and getattr(executor, "trading_pair", None) == self.config.trading_pair
            and getattr(executor, "side", None) == TradeType.BUY
        )

    def _owned_buy_position_executors(self) -> List[object]:
        """Executors that still own open or closing exposure.

        ``ExecutorInfo.is_active`` becomes false as soon as an executor enters
        ``SHUTTING_DOWN``. Its filled amount remains economic exposure until the
        executor terminates, so risk, cost basis, and episode flatness use this
        broader ownership view.
        """
        return self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda executor: (
                self._matches_buy_position_executor(executor)
                and not self._executor_is_done(executor)
            ),
        )

    def _active_buy_position_executors(self) -> List[object]:
        """Executors that can still accept a StopExecutorAction."""
        return self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda executor: (
                self._matches_buy_position_executor(executor)
                and bool(getattr(executor, "is_active", False))
            ),
        )

    def _executor_exposure(self, executor: object) -> ExecutorExposure:
        custom_info = getattr(executor, "custom_info", None) or {}
        executor_config = getattr(executor, "config", None)
        configured_base = self._decimal(
            getattr(executor_config, "amount", _ZERO)
        )
        reference_price = self._decimal(
            custom_info.get("current_position_average_price")
            or custom_info.get("entry_price")
            or getattr(executor_config, "entry_price", None)
            or self._get_mid_price()
        )
        filled_base = self._decimal(
            custom_info.get("open_order_filled_amount_base", _ZERO)
        )
        filled_quote = self._decimal(
            custom_info.get("open_order_filled_amount_quote", _ZERO)
        )
        if filled_quote <= _ZERO:
            filled_quote = self._decimal(
                getattr(executor, "filled_amount_quote", _ZERO)
            )
        if filled_base <= _ZERO and filled_quote > _ZERO and reference_price > _ZERO:
            filled_base = filled_quote / reference_price
        if filled_quote <= _ZERO and filled_base > _ZERO:
            filled_quote = filled_base * max(reference_price, _ZERO)

        done = self._executor_is_done(executor)
        is_stoppable = bool(getattr(executor, "is_active", False))
        if not is_stoppable and not done:
            # Entry cancellation has begun. Preserve only filled exposure; the
            # unfilled remainder is no longer a valid pending-risk reservation.
            configured_base = filled_base

        return ExecutorExposure(
            executor_id=str(getattr(executor, "id", "")),
            side=self._side_name(getattr(executor, "side", None)),
            executor_type=str(getattr(executor, "type", "")),
            active=not done,
            configured_base=max(configured_base, _ZERO),
            filled_base=max(filled_base, _ZERO),
            filled_quote=max(filled_quote, _ZERO),
            reference_price=max(reference_price, _ZERO),
            net_pnl_quote=self._decimal(
                getattr(executor, "net_pnl_quote", _ZERO)
            ),
            done=done,
        )

    def _active_executor_exposures(self) -> Tuple[ExecutorExposure, ...]:
        # Compatibility name retained for the existing RSI v5 status surface.
        return tuple(
            self._executor_exposure(executor)
            for executor in self._owned_buy_position_executors()
        )

    def _active_buy_filled_amount(self) -> Decimal:
        return sum(
            (exposure.filled_base for exposure in self._active_executor_exposures()),
            _ZERO,
        )

    def _base_quote_assets(self) -> Tuple[Optional[str], Optional[str]]:
        parts = self.config.trading_pair.split("-")
        base = parts[0] if parts else None
        return base, self.config.collateral_asset

    def _get_base_balance(self) -> Optional[Decimal]:
        # Perpetual close capacity is the position amount, not a spot-wallet base
        # balance. The execution connector owns final reduce-only validation.
        return self._position_amount(TradeType.BUY)

    def _has_buy_inventory(self) -> bool:
        return (
            self._active_buy_filled_amount() > _ZERO
            or super()._has_buy_inventory()
        )

    def _get_position_cost_basis(self) -> Optional[Decimal]:
        held = self._aggregate_held_inventory(TradeType.BUY)
        total_amount = held.total_amount
        weighted_quote = _ZERO
        if held.total_amount > _ZERO:
            if held.cost_basis is None or held.cost_basis <= _ZERO:
                return None
            weighted_quote += held.total_amount * held.cost_basis

        for exposure in self._active_executor_exposures():
            if exposure.filled_base <= _ZERO or exposure.reference_price <= _ZERO:
                continue
            total_amount += exposure.filled_base
            weighted_quote += exposure.filled_base * exposure.reference_price

        if total_amount <= _ZERO:
            return None
        return weighted_quote / total_amount

    def _get_active_buy_executor_position_value_usd(self) -> Decimal:
        mark_price = self._get_mid_price() or _ZERO
        return sum(
            (
                exposure.filled_notional(mark_price)
                for exposure in self._active_executor_exposures()
            ),
            _ZERO,
        )

    def _get_pending_buy_executor_notional_usd(self) -> Decimal:
        mark_price = self._get_mid_price() or _ZERO
        return sum(
            (
                exposure.pending_notional(mark_price)
                for exposure in self._active_executor_exposures()
            ),
            _ZERO,
        )

    def _exposure_snapshot(
        self,
        proposed_notional: Decimal = _ZERO,
    ) -> ExposureSnapshot:
        mark_price = self._get_mid_price() or _ZERO
        held_notional = self._position_amount(TradeType.BUY) * mark_price
        return calculate_exposure(
            executors=self._active_executor_exposures(),
            mark_price=mark_price,
            held_notional=held_notional,
            proposed_notional=proposed_notional,
        )

    def _get_total_position_value_usd(self) -> Decimal:
        return self._exposure_snapshot().total_notional

    def _inventory_net_pnl_pct(
        self,
        current_price: Optional[Decimal],
    ) -> Optional[Decimal]:
        if current_price is None or current_price <= _ZERO:
            return None
        cost_basis = self._get_position_cost_basis()
        if cost_basis is None or cost_basis <= _ZERO:
            return None
        held = self._aggregate_held_inventory(TradeType.BUY)
        total_amount = held.total_amount + self._active_buy_filled_amount()
        if total_amount <= _ZERO:
            return None
        fees = held.cum_fees_quote + sum(
            (
                self._decimal(getattr(executor, "cum_fees_quote", _ZERO))
                for executor in self._owned_buy_position_executors()
            ),
            _ZERO,
        )
        cost_quote = total_amount * cost_basis
        if cost_quote <= _ZERO:
            return None
        return (((current_price - cost_basis) * total_amount) - fees) / cost_quote

    def _sync_episode_tracker(self) -> None:
        owned_executors = self._owned_buy_position_executors()
        held_amount = self._position_amount(TradeType.BUY)
        if not owned_executors and held_amount <= _ZERO:
            return

        if not self._episode_tracker.is_open:
            timestamps = [
                float(
                    getattr(
                        executor,
                        "timestamp",
                        self.market_data_provider.time(),
                    )
                )
                for executor in owned_executors
            ]
            self._episode_tracker.open(
                timestamp=min(
                    timestamps,
                    default=self.market_data_provider.time(),
                )
            )

        for executor in owned_executors:
            exposure = self._executor_exposure(executor)
            planned_notional = (
                exposure.configured_base * max(exposure.reference_price, _ZERO)
            )
            self._episode_tracker.register_executor(
                exposure.executor_id,
                planned_notional,
            )

    def _register_created_executor(
        self,
        config: PositionExecutorConfig,
    ) -> None:
        if not self._episode_tracker.is_open:
            self._episode_tracker.open(
                timestamp=float(
                    config.timestamp or self.market_data_provider.time()
                )
            )
        planned_notional = (
            max(config.amount, _ZERO)
            * max(config.entry_price or _ZERO, _ZERO)
        )
        self._episode_tracker.register_executor(
            config.id,
            planned_notional,
        )

    def _track_executor_result(self, executor) -> None:
        executor_id = getattr(executor, "id", None)
        if executor_id is None:
            return
        self._episode_tracker.record_executor_result(
            str(executor_id),
            self._decimal(getattr(executor, "net_pnl_quote", _ZERO)),
        )

    def _finalize_episode_if_flat(
        self,
        timestamp: Optional[float] = None,
    ) -> Optional[EpisodeOutcome]:
        now = float(
            timestamp
            if timestamp is not None
            else self.market_data_provider.time()
        )
        is_flat = (
            not self._owned_buy_position_executors()
            and self._position_amount(TradeType.BUY) <= _ZERO
        )
        outcome = self._episode_tracker.close_if_flat(
            is_flat=is_flat,
            timestamp=now,
        )
        if outcome is None:
            return None

        self._consecutive_losses = update_loss_streak(
            current_streak=self._consecutive_losses,
            outcome=outcome,
        )
        if outcome.net_pnl_quote < _ZERO:
            self._last_loss_timestamp = now
        elif outcome.net_pnl_quote > _ZERO:
            self._last_loss_timestamp = None
        self._last_episode_outcome = outcome
        self.logger().info(
            f"Episode closed | {self._controller_pair_log_prefix()} "
            f"episode={outcome.episode_id} "
            f"executors={len(outcome.executor_ids)} "
            f"pnl={outcome.net_pnl_quote} "
            f"loss_streak={self._consecutive_losses}"
        )
        return outcome

    def can_create_executor(self, signal: int) -> bool:
        if signal < 0:
            self._record_gate_reason(
                TradeType.SELL,
                "executor_owned_exit",
            )
            return False
        if signal > 0 and any(
            not bool(getattr(executor, "is_active", False))
            for executor in self._owned_buy_position_executors()
        ):
            self._record_gate_reason(TradeType.BUY, "exit_pending")
            return False
        if (
            signal > 0
            and not self.config.perpetual_allow_position_hold
            and self._position_amount(TradeType.BUY) > _ZERO
        ):
            self._record_gate_reason(
                TradeType.BUY,
                "unexpected_held_position",
            )
            return False
        return super().can_create_executor(signal)

    def get_executor_config(
        self,
        trade_type: TradeType,
        price: Decimal,
        amount: Decimal,
    ):
        if trade_type == TradeType.SELL:
            raise RuntimeError(
                "Hyperliquid RSI exits must stop the owning PositionExecutor; "
                "controller-created SELL executors are forbidden"
            )

        executor_config = super().get_executor_config(
            trade_type,
            price,
            amount,
        )
        if not isinstance(executor_config, PositionExecutorConfig):
            raise TypeError(
                "rsi_v5_hyperliquid BUY entries require PositionExecutorConfig"
            )

        triple_barrier = executor_config.triple_barrier_config.model_copy(
            update={
                "stop_loss": Decimal(
                    str(self.config.perpetual_hard_stop_loss_pct)
                ),
                "stop_loss_order_type": OrderType.MARKET,
            }
        )
        payload = executor_config.model_dump()
        payload.update(
            {
                "type": "position_executor",
                "triple_barrier_config": triple_barrier,
                "leverage": self.config.leverage,
            }
        )
        executor_config = RSIPositionExecutorConfig.model_validate(payload)
        self._register_created_executor(executor_config)
        return executor_config

    def _check_bag_freeze_trigger(
        self,
        current_price: Decimal,
        now_ts: float,
    ) -> List[ExecutorAction]:
        if not self.config.perpetual_allow_position_hold:
            return []
        return super()._check_bag_freeze_trigger(
            current_price,
            now_ts,
        )

    def _executor_owned_exit_actions(
        self,
        *,
        now: float,
        existing_stop_ids: set[str],
        reason: str,
    ) -> List[StopExecutorAction]:
        stoppable_executors = self._active_buy_position_executors()
        stoppable_ids = {
            str(getattr(executor, "id", ""))
            for executor in stoppable_executors
        }
        self._exit_request_timestamps = {
            executor_id: timestamp
            for executor_id, timestamp in self._exit_request_timestamps.items()
            if executor_id in stoppable_ids
        }
        plan = build_exit_plan(
            (
                self._executor_exposure(executor)
                for executor in stoppable_executors
            ),
            reason=reason,
        )
        actions: List[StopExecutorAction] = []
        retry_seconds = float(self.config.executor_exit_retry_seconds)
        for executor_id in plan.executor_ids:
            if executor_id in existing_stop_ids:
                continue
            previous_request = self._exit_request_timestamps.get(executor_id)
            if (
                previous_request is not None
                and (now - previous_request) < retry_seconds
            ):
                continue
            self._exit_request_timestamps[executor_id] = now
            actions.append(
                StopExecutorAction(
                    executor_id=executor_id,
                    controller_id=self.config.id,
                    keep_position=False,
                )
            )
        if actions:
            self.logger().info(
                f"Executor-owned exit | {self._controller_pair_log_prefix()} "
                f"reason={reason} "
                f"executors={[action.executor_id for action in actions]}"
            )
        return actions

    def stop_actions_proposal(self) -> List[ExecutorAction]:
        self._sync_episode_tracker()
        actions = super().stop_actions_proposal()
        now = float(self.market_data_provider.time())
        signal = int((self.processed_data or {}).get("signal", 0) or 0)
        if signal < 0:
            state = (self.processed_data or {}).get("signal_state", {})
            reason = str(state.get("sell_reason") or "rsi-reversal")
            existing_stop_ids = {
                str(action.executor_id)
                for action in actions
                if isinstance(action, StopExecutorAction)
            }
            actions.extend(
                self._executor_owned_exit_actions(
                    now=now,
                    existing_stop_ids=existing_stop_ids,
                    reason=reason,
                )
            )
        self._finalize_episode_if_flat(timestamp=now)
        return actions

    def get_custom_info(self) -> dict:
        info = super().get_custom_info()
        exposure = self._exposure_snapshot()
        info.update(
            {
                "runtime_profile": (
                    "hyperliquid-perpetual-executor-owned-v1"
                ),
                "episode_id": self._episode_tracker.episode_id,
                "exposure": {
                    "held_notional": float(exposure.held_notional),
                    "filled_notional": float(exposure.filled_notional),
                    "pending_notional": float(exposure.pending_notional),
                    "total_notional": float(exposure.total_notional),
                },
                "last_episode": (
                    {
                        "episode_id": self._last_episode_outcome.episode_id,
                        "net_pnl_quote": float(
                            self._last_episode_outcome.net_pnl_quote
                        ),
                        "executor_count": len(
                            self._last_episode_outcome.executor_ids
                        ),
                    }
                    if self._last_episode_outcome is not None
                    else None
                ),
            }
        )
        return info
