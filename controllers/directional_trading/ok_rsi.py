"""OKX spot specialization of the shared RSI v5 controller.

Legacy signal, inventory and DCA behavior remains in ``hl_rsi``. Configuration,
candle aggregation and pure trailing calculations are re-exported from
``ok_rsi_core``. The opt-in episode runtime owns its entire action path for both
buy/sell and explicitly assigned sell-only inventory.
"""

import logging
import math
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from controllers.directional_trading import hl_rsi
from controllers.directional_trading.ok_rsi_core.candles import derive_higher_timeframe_candles
from controllers.directional_trading.ok_rsi_core.config import OKRSIControllerConfig
from controllers.directional_trading.ok_rsi_core.down_only_entry_shadow import DownOnlyMakerShadowTracker
from controllers.directional_trading.ok_rsi_core.trailing import (
    ContinuousTrailingSelection,
    calculate_continuous_adaptive_trailing,
)
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.logger import HummingbotLogger
from hummingbot.model.controller_candle_watermark import (
    CONTROLLER_CANDLE_WATERMARK_SCHEMA_VERSION,
    ControllerCandleWatermarkNamespace,
    ControllerCandleWatermarkOutcome,
    ControllerCandleWatermarkResult,
    ControllerCandleWatermarkState,
)
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)

_DECIMAL_CONVERSION_ERRORS = (ArithmeticError, TypeError, ValueError)
_EXECUTION_INPUT_ERRORS = (ArithmeticError, AttributeError, KeyError, RuntimeError, TypeError, ValueError)
_RECONSTRUCTION_INPUT_ERRORS = (*_EXECUTION_INPUT_ERRORS, OSError)
_OWNER_RECONSTRUCTION_MIN_CANDLES = 220


class ExecutorInputUnavailable(ValueError):
    """Raised when an executor cannot be constructed from valid exchange inputs."""


class _ReconstructionMarketDataProvider:
    """Controller-local causal view used to reconstruct one committed candle."""

    def __init__(
        self,
        delegate: object,
        *,
        candle_connector_name: str,
        price_connector_name: str,
        trading_pair: str,
        interval: str,
        candles: pd.DataFrame,
        current_time: float,
        persisted_close: Decimal,
    ) -> None:
        self._delegate = delegate
        self._candle_connector_name = candle_connector_name
        self._price_connector_name = price_connector_name
        self._trading_pair = trading_pair
        self._interval = interval
        self._candles = candles.copy(deep=True)
        self._current_time = current_time
        self._persisted_close = persisted_close

    def time(self) -> float:
        return self._current_time

    def get_candles_df(
        self,
        connector_name: str,
        trading_pair: str,
        interval: str,
        max_records: int,
    ) -> pd.DataFrame:
        if (
            connector_name != self._candle_connector_name
            or trading_pair != self._trading_pair
            or interval != self._interval
        ):
            raise ValueError("owner reconstruction requested an unexpected candle namespace")
        if isinstance(max_records, bool) or not isinstance(max_records, int) or max_records <= 0:
            raise ValueError("owner reconstruction max_records must be a positive integer")
        return self._candles.tail(max_records).copy(deep=True)

    def get_price_by_type(
        self,
        connector_name: str,
        trading_pair: str,
        price_type: PriceType,
    ) -> Decimal:
        if connector_name != self._price_connector_name or trading_pair != self._trading_pair:
            raise ValueError("owner reconstruction requested an unexpected price namespace")
        return self._persisted_close

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)


class OKRSIController(hl_rsi.HLRSIController):
    """Shared RSI v5 behavior with OKX spot execution and trailing contracts."""

    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        directory = Path(__file__).parent
        return [Path(__file__), Path(hl_rsi.__file__), *(directory / "ok_rsi_core").rglob("*.py")]
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
            cls._logger.setLevel(logging.INFO)
        return cls._logger

    def __init__(self, config: OKRSIControllerConfig, *args, **kwargs):
        episode_store = kwargs.pop("episode_store", None)
        self._entry_decision_store = kwargs.pop("entry_decision_store", None)
        self._entry_decision_gate = None
        self._entry_decision_error = None
        self._entry_acquisition_policy = config.entry_acquisition_policy
        self._entry_acquisition_shadow_policy = config.entry_acquisition_shadow_policy
        self._entry_acquisition_shadow = (
            DownOnlyMakerShadowTracker(config.down_only_maker_parameters())
            if config.entry_acquisition_shadow_policy == "down_only_maker_v1"
            else None
        )
        super().__init__(config, *args, **kwargs)
        self._last_trailing_selection: Optional[ContinuousTrailingSelection] = None
        self._watermark_namespace: Optional[ControllerCandleWatermarkNamespace] = None
        self._watermark_state: Optional[ControllerCandleWatermarkState] = None
        self._watermark_result: Optional[ControllerCandleWatermarkResult] = None
        self._watermark_recorder: Optional[MarketsRecorder] = None
        self._watermark_permanent_hold = False
        self._watermark_proposal_grant = False
        self._watermark_reconstruction_target: Optional[int] = None
        self._watermark_initialization_required = False
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
            from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction

            # The episode runtime must continue reconciliation and exit/stop work.
            # Its BUY requests have not yet adopted the normal entry decision gate.
            actions = self._episode_runtime.determine_actions()
            permitted = [action for action in actions if not (
                isinstance(action, CreateExecutorAction) and action.executor_config.side == TradeType.BUY
            )]
            if len(permitted) != len(actions):
                self._entry_admission_failed("Episode BUY requires explicit single-attempt decision support")
            return permitted
        self._advance_entry_acquisition_shadow()
        actions = super().determine_executor_actions()
        self._observe_entry_acquisition_shadow(actions)
        return actions

    def _entry_acquisition_shadow_inputs(self):
        now = Decimal(str(self.market_data_provider.time()))
        best_bid = Decimal(str(self.market_data_provider.get_price_by_type(
            self.config.connector_name,
            self.config.trading_pair,
            PriceType.BestBid,
        )))
        best_ask = Decimal(str(self.market_data_provider.get_price_by_type(
            self.config.connector_name,
            self.config.trading_pair,
            PriceType.BestAsk,
        )))
        trading_rule = self.market_data_provider.get_trading_rules(
            self.config.connector_name,
            self.config.trading_pair,
        )
        price_step = Decimal(str(trading_rule.min_price_increment))
        atr_pct = Decimal(str(self._latest_atr_pct))
        atr_1m = atr_pct * best_bid
        values = (now, best_bid, best_ask, price_step, atr_1m)
        if any(not value.is_finite() for value in values):
            raise ValueError("shadow inputs must be finite")
        if now < 0 or any(value <= 0 for value in (best_bid, best_ask, price_step, atr_1m)):
            raise ValueError("shadow market inputs must be positive")
        if best_bid > best_ask:
            raise ValueError("shadow market book is crossed")
        return now, best_bid, best_ask, price_step, atr_1m

    def _shadow_configuration_changed(self) -> bool:
        return (
            self.config.entry_acquisition_policy != self._entry_acquisition_policy
            or self.config.entry_acquisition_shadow_policy != self._entry_acquisition_shadow_policy
        )

    def _advance_entry_acquisition_shadow(self) -> None:
        tracker = self._entry_acquisition_shadow
        if tracker is None or not tracker.is_active:
            return
        try:
            now = Decimal(str(self.market_data_provider.time()))
        except _EXECUTION_INPUT_ERRORS:
            now = Decimal("0")
        if self._shadow_configuration_changed():
            tracker.terminate(now=now, reason="configuration_changed")
            return
        try:
            now, best_bid, best_ask, _, _ = self._entry_acquisition_shadow_inputs()
            signal = (self.processed_data or {}).get("signal", 0)
            tracker.advance(
                now=now,
                best_bid=best_bid,
                best_ask=best_ask,
                opposing_signal=bool(signal < 0),
                risk_allowed=not bool(getattr(self.config, "manual_kill_switch", False)),
            )
        except _EXECUTION_INPUT_ERRORS:
            tracker.terminate(now=now, reason="input_unavailable")

    def _observe_entry_acquisition_shadow(self, actions) -> None:
        tracker = self._entry_acquisition_shadow
        if tracker is None or tracker.is_active or self._shadow_configuration_changed():
            return
        buy_actions = [
            action for action in actions
            if getattr(getattr(action, "executor_config", None), "side", None) is TradeType.BUY
        ]
        if not buy_actions:
            return
        action = buy_actions[0]
        config = action.executor_config
        try:
            now, best_bid, best_ask, price_step, atr_1m = self._entry_acquisition_shadow_inputs()
            tracker.start(
                intent_id=f"shadow:{config.id}",
                now=now,
                initial_price=Decimal(str(config.entry_price)),
                best_bid=best_bid,
                best_ask=best_ask,
                atr_1m=atr_1m,
                price_step=price_step,
                authorized_base=Decimal(str(config.amount)),
            )
        except _EXECUTION_INPUT_ERRORS:
            try:
                now = Decimal(str(self.market_data_provider.time()))
            except _EXECUTION_INPUT_ERRORS:
                now = Decimal("0")
            tracker.record_unavailable(now=now)

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
                or config.episode_entry_parent_request_id != request.parent_request_id
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

    def _durable_watermark_snapshot(self) -> Dict[str, object]:
        result = self._watermark_result
        state = self._watermark_state
        return {
            "enabled": self.config.durable_candle_watermark_enabled,
            "schema": "CONTROLLER_CANDLE_WATERMARK_V1",
            "guarantee": (
                "AT_MOST_ONCE_COMPLETED_CANDLE_PROPOSAL_ADMISSION_WITH_OWNER_RECONSTRUCTION_V1"
            ),
            "outcome": result.outcome.value if result is not None else None,
            "reason": result.reason if result is not None else None,
            "permanent_hold": self._watermark_permanent_hold,
            "proposal_eligible": bool(
                self._watermark_proposal_grant
                and result is not None
                and result.proposal_eligible
            ),
            "candle_open_timestamp_s": (
                state.candle_open_timestamp_s if state is not None else None
            ),
            "revision": state.revision if state is not None else None,
            "initialization_required": self._watermark_initialization_required,
            "owner_reconstruction_pending": (
                self._watermark_reconstruction_target is not None
            ),
            "restart_required_after_model_registration": True,
        }

    def _neutralize_for_watermark(self, reason: str) -> None:
        previous = dict(self.processed_data or {})
        previous.update(
            {
                "signal": 0,
                "durable_watermark_hold": self._watermark_permanent_hold,
                "signal_suppression_reason": reason,
            }
        )
        self.processed_data = previous

    def _enter_watermark_hold(self, result: ControllerCandleWatermarkResult) -> None:
        self._watermark_result = result
        self._watermark_permanent_hold = True
        self._watermark_proposal_grant = False
        self._neutralize_for_watermark(
            f"durable_watermark_{result.outcome.value.lower()}"
        )
        self.logger().error(
            f"Durable candle watermark hold | {self._controller_pair_log_prefix()} "
            f"outcome={result.outcome.value} reason={result.reason or 'n/a'}"
        )

    @staticmethod
    def _is_missing_watermark(result: ControllerCandleWatermarkResult) -> bool:
        return (
            result.outcome is ControllerCandleWatermarkOutcome.INVALID_STATE
            and result.state is None
            and result.reason
            == "watermark namespace row is missing; explicit research seed required"
        )

    def _load_durable_watermark_before_evaluation(self) -> bool:
        try:
            namespace = ControllerCandleWatermarkNamespace(
                controller_id=self.config.id,
                connector_name=(
                    self.config.candles_connector or self.config.connector_name
                ),
                trading_pair=(
                    self.config.candles_trading_pair or self.config.trading_pair
                ),
                interval=self.config.interval,
            )
        except (TypeError, ValueError) as exc:
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    reason=f"invalid controller watermark namespace: {exc}",
                )
            )
            return False

        recorder = MarketsRecorder.get_existing_instance()
        if recorder is None:
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.STORE_UNAVAILABLE,
                    reason="MarketsRecorder is unavailable before first evaluation",
                )
            )
            return False
        try:
            result = recorder.load_controller_candle_watermark(namespace)
        except Exception as exc:
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.STORE_UNAVAILABLE,
                    reason=f"watermark load raised {type(exc).__name__}",
                )
            )
            return False

        self._watermark_namespace = namespace
        self._watermark_recorder = recorder
        self._watermark_result = result
        if self._is_missing_watermark(result):
            self._watermark_initialization_required = True
            return False
        if (
            result.outcome is not ControllerCandleWatermarkOutcome.ALREADY_COMMITTED
            or result.state is None
            or result.state.namespace != namespace
        ):
            self._enter_watermark_hold(result)
            return False

        self._watermark_state = result.state
        self._watermark_reconstruction_target = result.state.candle_open_timestamp_s
        return True

    def _validated_evaluated_candle_timestamp(self) -> Optional[int]:
        candidate_timestamp = self._last_evaluated_candle_timestamp
        data_health = (self.processed_data or {}).get("data_health")
        if not isinstance(data_health, dict) or not data_health.get("ready"):
            return None
        if candidate_timestamp is None:
            return None
        try:
            candidate_float = float(candidate_timestamp)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(candidate_float) or not candidate_float.is_integer():
            return None
        return int(candidate_float)

    async def _initialize_durable_watermark(self) -> None:
        """Seed the latest owner-evaluated closed candle without admitting its signal."""
        await super().update_processed_data()
        candidate_timestamp = self._validated_evaluated_candle_timestamp()
        if candidate_timestamp is None:
            self._watermark_proposal_grant = False
            return
        try:
            desired_state = ControllerCandleWatermarkState(
                namespace=self._watermark_namespace,
                candle_open_timestamp_s=candidate_timestamp,
                revision=0,
                schema_version=CONTROLLER_CANDLE_WATERMARK_SCHEMA_VERSION,
                updated_at_ms=max(1, int(self.market_data_provider.time() * 1000)),
            )
            result = self._watermark_recorder.seed_controller_candle_watermark(
                desired_state
            )
        except Exception as exc:
            result = ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.STORE_UNAVAILABLE,
                reason=f"watermark initialization raised {type(exc).__name__}",
            )
        if result.state is None or result.state.namespace != self._watermark_namespace:
            self._enter_watermark_hold(result)
            return
        if result.outcome not in {
            ControllerCandleWatermarkOutcome.SEEDED,
            ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
        }:
            self._enter_watermark_hold(result)
            return

        self._watermark_state = result.state
        self._watermark_result = result
        self._watermark_initialization_required = False
        self._watermark_proposal_grant = False
        if result.state.candle_open_timestamp_s != candidate_timestamp:
            self._watermark_reconstruction_target = (
                result.state.candle_open_timestamp_s
            )
        self._neutralize_for_watermark("durable_watermark_initialized")

    def _persisted_candle_reconstruction_frame(
        self,
        target_timestamp: int,
    ) -> Optional[tuple[pd.DataFrame, Decimal]]:
        try:
            frame = self.market_data_provider.get_candles_df(
                connector_name=(
                    self.config.candles_connector or self.config.connector_name
                ),
                trading_pair=(
                    self.config.candles_trading_pair or self.config.trading_pair
                ),
                interval=self.config.interval,
                max_records=self._derived_base_max_records(),
            )
        except _RECONSTRUCTION_INPUT_ERRORS as exc:
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    state=self._watermark_state,
                    reason=(
                        "owner reconstruction candle read raised "
                        f"{type(exc).__name__}"
                    ),
                )
            )
            return None
        if (
            not isinstance(frame, pd.DataFrame)
            or frame.empty
            or "timestamp" not in frame.columns
        ):
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    state=self._watermark_state,
                    reason=(
                        "owner reconstruction requires a nonempty timestamped "
                        "candle frame"
                    ),
                )
            )
            return None

        frame = frame.copy(deep=True)
        numeric_timestamps = pd.to_numeric(frame["timestamp"], errors="coerce")
        if numeric_timestamps.isna().any():
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    state=self._watermark_state,
                    reason="owner reconstruction candle timestamps must be finite integers",
                )
            )
            return None
        normalized_timestamps = numeric_timestamps.map(
            lambda value: self._timestamp_to_seconds(float(value))
        )
        if not normalized_timestamps.map(
            lambda value: math.isfinite(float(value))
            and float(value).is_integer()
        ).all():
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    state=self._watermark_state,
                    reason="owner reconstruction candle timestamps must be finite integers",
                )
            )
            return None

        target_matches = normalized_timestamps.eq(float(target_timestamp))
        if int(target_matches.sum()) != 1:
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    state=self._watermark_state,
                    reason="owner reconstruction requires exactly one persisted candle",
                )
            )
            return None
        target_position = target_matches.tolist().index(True)
        causal_frame = frame.iloc[: target_position + 1].copy(deep=True)
        causal_frame["timestamp"] = (
            normalized_timestamps.iloc[: target_position + 1]
            .astype("int64")
            .to_numpy()
        )
        if len(causal_frame) < _OWNER_RECONSTRUCTION_MIN_CANDLES:
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    state=self._watermark_state,
                    reason=(
                        "owner reconstruction requires at least "
                        f"{_OWNER_RECONSTRUCTION_MIN_CANDLES} causal candles"
                    ),
                )
            )
            return None
        interval_seconds = self._watermark_state.namespace.interval_seconds
        health = self._build_data_health(
            causal_frame,
            now_ts=float(target_timestamp + interval_seconds),
        )
        if (
            not health.get("ready")
            or health.get("last_candle_ts") != float(target_timestamp)
        ):
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    state=self._watermark_state,
                    reason=(
                        "owner reconstruction causal frame is invalid: "
                        f"{health.get('reason')}"
                    ),
                )
            )
            return None

        try:
            persisted_close = Decimal(str(causal_frame["close"].iloc[-1]))
        except (InvalidOperation, KeyError, TypeError, ValueError):
            persisted_close = Decimal("NaN")
        if not persisted_close.is_finite() or persisted_close <= Decimal("0"):
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    state=self._watermark_state,
                    reason=(
                        "owner reconstruction persisted close must be positive and finite"
                    ),
                )
            )
            return None
        return causal_frame, persisted_close

    async def _reconstruct_persisted_owner_candle(
        self, target_timestamp: int
    ) -> bool:
        reconstruction = self._persisted_candle_reconstruction_frame(
            target_timestamp
        )
        if reconstruction is None:
            return False
        causal_frame, persisted_close = reconstruction
        interval_seconds = self._watermark_state.namespace.interval_seconds
        original_provider = self.market_data_provider
        self.market_data_provider = _ReconstructionMarketDataProvider(
            original_provider,
            candle_connector_name=(
                self.config.candles_connector or self.config.connector_name
            ),
            price_connector_name=self.config.connector_name,
            trading_pair=(
                self.config.candles_trading_pair or self.config.trading_pair
            ),
            interval=self.config.interval,
            candles=causal_frame,
            current_time=float(target_timestamp + interval_seconds),
            persisted_close=persisted_close,
        )
        try:
            await hl_rsi.HLRSIController.update_processed_data(self)
        except Exception as exc:
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    state=self._watermark_state,
                    reason=f"owner reconstruction raised {type(exc).__name__}",
                )
            )
            return False
        finally:
            self.market_data_provider = original_provider

        data_health = (self.processed_data or {}).get("data_health")
        observed_timestamp = (self.processed_data or {}).get("candle_timestamp")
        if (
            not isinstance(data_health, dict)
            or not data_health.get("ready")
            or not getattr(self._regime_detector, "_is_fit", False)
            or observed_timestamp != float(target_timestamp)
            or self._last_evaluated_candle_timestamp != float(target_timestamp)
        ):
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    state=self._watermark_state,
                    reason="owner reconstruction did not evaluate the persisted candle",
                )
            )
            return False

        self._watermark_reconstruction_target = None
        self._watermark_proposal_grant = False
        self._neutralize_for_watermark(
            "durable_watermark_owner_reconstruction"
        )
        return True

    async def update_processed_data(self):
        if not self.config.durable_candle_watermark_enabled:
            await super().update_processed_data()
            return
        if self._watermark_permanent_hold:
            self._neutralize_for_watermark("durable_watermark_permanent_hold")
            return
        if self._watermark_state is None:
            loaded = self._load_durable_watermark_before_evaluation()
            if self._watermark_initialization_required:
                await self._initialize_durable_watermark()
                return
            if not loaded:
                return
        if self._watermark_reconstruction_target is not None:
            await self._reconstruct_persisted_owner_candle(
                self._watermark_reconstruction_target
            )
            return

        prior_state = self._watermark_state
        await super().update_processed_data()
        candidate_timestamp = self._last_evaluated_candle_timestamp
        data_health = (self.processed_data or {}).get("data_health")
        inherited_suppression = (self.processed_data or {}).get(
            "signal_suppression_reason"
        )
        if (
            isinstance(data_health, dict) and data_health.get("ready") is False
        ) or (
            candidate_timestamp == float(prior_state.candle_open_timestamp_s)
            and inherited_suppression is None
        ):
            self._watermark_proposal_grant = False
            return
        if inherited_suppression == "non_advancing_candle":
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.REGRESSION,
                    state=prior_state,
                    reason="owner observed a candle below the loaded durable watermark",
                )
            )
            return
        if candidate_timestamp is None:
            self._watermark_proposal_grant = False
            return
        if not math.isfinite(float(candidate_timestamp)) or not float(
            candidate_timestamp
        ).is_integer():
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    state=prior_state,
                    reason="evaluated candle timestamp must be a finite integer",
                )
            )
            return

        desired_timestamp = int(candidate_timestamp)
        if desired_timestamp < prior_state.candle_open_timestamp_s:
            self._enter_watermark_hold(
                ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.REGRESSION,
                    state=prior_state,
                    reason="evaluation regressed below loaded durable watermark",
                )
            )
            return
        if desired_timestamp == prior_state.candle_open_timestamp_s:
            self._watermark_result = ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
                state=prior_state,
            )
            self._watermark_proposal_grant = False
            return

        try:
            updated_at_ms = max(1, int(self.market_data_provider.time() * 1000))
            result = self._watermark_recorder.compare_and_advance_controller_candle_watermark(
                prior_state,
                desired_candle_open_timestamp_s=desired_timestamp,
                updated_at_ms=updated_at_ms,
            )
        except Exception as exc:
            result = ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.STORE_UNAVAILABLE,
                state=prior_state,
                reason=f"watermark compare-and-swap raised {type(exc).__name__}",
            )
        if (
            result.outcome is ControllerCandleWatermarkOutcome.ALREADY_COMMITTED
            and result.state is not None
        ):
            if (
                result.state.namespace != prior_state.namespace
                or result.state.candle_open_timestamp_s < desired_timestamp
            ):
                self._enter_watermark_hold(
                    ControllerCandleWatermarkResult(
                        outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                        state=result.state,
                        reason=(
                            "already-committed watermark did not cover the evaluated candle"
                        ),
                    )
                )
                return
            self._watermark_result = result
            self._watermark_state = result.state
            self._watermark_proposal_grant = False
            if result.state.candle_open_timestamp_s > desired_timestamp:
                self._enter_watermark_hold(
                    ControllerCandleWatermarkResult(
                        outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                        state=result.state,
                        reason=(
                            "already-committed watermark is ahead of the evaluated "
                            "candle; owner state cannot be reconstructed safely"
                        ),
                    )
                )
            else:
                self._last_evaluated_candle_timestamp = float(
                    result.state.candle_open_timestamp_s
                )
                self._neutralize_for_watermark(
                    "durable_watermark_already_committed"
                )
            return
        if not result.proposal_eligible or result.state is None:
            self._enter_watermark_hold(result)
            return

        self._watermark_result = result
        self._watermark_state = result.state
        self._watermark_proposal_grant = True

    def _entry_admission_failed(self, reason):
        if reason != self._entry_decision_error:
            self.logger().error(f"Entry decision admission blocked | {self.config.id} | {reason}")
        self._entry_decision_error = reason

    def _get_entry_decision_gate(self):
        from controllers.directional_trading.ok_rsi_core.entry_decisions import (
            EntryDecisionGate,
            MetadataEntryDecisionStore,
        )
        if self._entry_decision_gate is None:
            store = self._entry_decision_store
            if store is None:
                recorder = MarketsRecorder.get_existing_instance()
                if recorder is None:
                    raise RuntimeError("Production recorder unavailable for durable entry decisions")
                store = MetadataEntryDecisionStore(
                    recorder._sql_manager, recorder._config_file_path, self.config.id,
                    self.config.connector_name, self.config.trading_pair, self.config.interval,
                )
            self._entry_decision_gate = EntryDecisionGate(store)
        return self._entry_decision_gate

    def create_actions_proposal(self):
        if self._episode_runtime is not None:
            self._entry_admission_failed("Episode execution requires explicit single-attempt decision support")
            return []
        if self.config.durable_candle_watermark_enabled:
            if (not self._watermark_proposal_grant or self._watermark_result is None
                    or not self._watermark_result.proposal_eligible):
                return []
            self._watermark_proposal_grant = False
        data = self.processed_data or {}
        signal = data.get("signal", 0)
        # Only the owner's completed-candle evaluation may clear or consume a setup.
        candle = data.get("candle_timestamp")
        try:
            now = self.market_data_provider.time()
            seconds = CandlesBase.interval_to_seconds[self.config.interval]
            evaluated = (isinstance(candle, (int, float)) and not isinstance(candle, bool)
                         and math.isfinite(candle) and candle == self._last_evaluated_candle_timestamp
                         and isinstance(now, (int, float)) and math.isfinite(now)
                         and candle + seconds <= now)
            gate = self._get_entry_decision_gate()
            eligible = gate.observe(data) if evaluated else False
            # SELL management remains independent of BUY admission storage.
            if signal < 0:
                return super().create_actions_proposal()
            if not eligible:
                return []
            actions = super().create_actions_proposal()
            from controllers.directional_trading.ok_rsi_core.entry_decisions import executor_fingerprint
            admitted = []
            for action in actions:
                config = action.executor_config
                if config.side != TradeType.BUY:
                    admitted.append(action)
                elif gate.reserve(config.id, executor_fingerprint(config), int(data["candle_timestamp"])):
                    admitted.append(action)
            self._entry_decision_error = None
            return admitted
        except Exception as exc:
            self._entry_admission_failed(f"{type(exc).__name__}: {exc}")
            return super().create_actions_proposal() if signal < 0 else []

    def _select_signal_candles(self, df: pd.DataFrame, now_ts: float) -> pd.DataFrame:
        if not self._build_data_health(df, now_ts)["ready"]:
            return df
        seconds = CandlesBase.interval_to_seconds[self.config.interval]
        timestamps = pd.to_numeric(df["timestamp"]).map(self._timestamp_to_seconds)
        return df.loc[timestamps + seconds <= now_ts].copy()

    def validate_executor_config(self, config) -> bool:
        if self._episode_runtime is not None:
            if config.side != TradeType.SELL:
                self._entry_admission_failed("Episode BUY requires explicit single-attempt decision support")
                return False
            # Restore the episode's own boundary validation for queued exits.
            from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
            return (ControllerBase.validate_executor_config(self, config)
                    and config.connector_name == self.config.connector_name
                    and config.trading_pair == self.config.trading_pair
                    and not self._episode_runtime.hold_reason
                    and not self._episode_runtime.state.fault)
        if not super().validate_executor_config(config):
            return False
        if config.side != TradeType.BUY:
            if self.config.trade_mode == "sell_only" and self.config.sell_only_strict_fraction:
                ceiling = self._get_sellable_inventory_amount() * self.config.sell_only_inventory_fraction
                if config.amount > ceiling:
                    self._record_gate_reason(TradeType.SELL, "sell_fraction_exceeded")
                    return False
            return True
        try:
            from controllers.directional_trading.ok_rsi_core.entry_decisions import executor_fingerprint
            candle = (self.processed_data or {}).get("candle_timestamp")
            return self._get_entry_decision_gate().dispatch(config.id, executor_fingerprint(config), candle)
        except Exception as exc:
            self._entry_admission_failed(f"{type(exc).__name__}: {exc}")
            return False

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

    def build_chart_snapshot_rows(self, bot_name: str):
        from controllers.directional_trading.ok_rsi_core.chart_snapshots import (
            MAX_BATCH_ROWS,
            MAX_CACHED_CANDLES,
            build_chart_rows,
            chart_projection_signature,
            pending_chart_rows,
        )

        intervals = [self.config.interval]
        if self.config.trend_confirmation_enabled:
            for interval in (self.config.trend_confirmation_medium_interval, self.config.trend_confirmation_long_interval):
                if interval and interval not in intervals:
                    intervals.append(interval)
        processed = self.processed_data or {}
        rows = []
        now = self.market_data_provider.time()
        raw = self._get_interval_candles_df(self.config.interval)
        projections = getattr(self, "_chart_projection_cache", {})
        for interval in intervals:
            frame = raw
            features = processed.get("features") if interval == self.config.interval else None
            if isinstance(features, pd.DataFrame) and not features.empty:
                frame = pd.concat([frame, features], ignore_index=True) if isinstance(frame, pd.DataFrame) else features
            seconds = self._interval_to_seconds(interval)
            if not isinstance(frame, pd.DataFrame) or "timestamp" not in frame or not seconds:
                continue
            timestamps = pd.to_numeric(frame["timestamp"], errors="coerce")
            # Higher-frame cache keys include only fully closed target buckets.
            closed_at = timestamps + seconds if interval == self.config.interval else (timestamps // seconds + 1) * seconds
            frame = frame[closed_at <= now].sort_values("timestamp", kind="stable")
            if interval == self.config.interval:
                frame = frame.drop_duplicates("timestamp", keep="last")
            frame = frame.tail(MAX_CACHED_CANDLES)
            signature = chart_projection_signature(self.config, bot_name, interval, frame, processed, now, seconds)
            cached = projections.get(interval)
            if cached is None or cached[0] != signature:
                if interval != self.config.interval:
                    frame = derive_higher_timeframe_candles(
                        frame, base_interval=self.config.interval, target_interval=interval,
                        current_time=now, max_records=self.config.derived_higher_timeframe_max_records,
                    )
                projected_rows = build_chart_rows(self.config, bot_name, interval, frame, processed,
                                                  now=now, interval_seconds=seconds)
                projections[interval] = (signature, projected_rows)
            rows.extend(projections[interval][1])
        self._chart_projection_cache = {interval: projections[interval] for interval in intervals if interval in projections}
        return pending_chart_rows(rows, getattr(self, "_chart_snapshot_cache", {}), MAX_BATCH_ROWS)

    def chart_snapshot_rows_persisted(self, rows):
        from controllers.directional_trading.ok_rsi_core.chart_snapshots import acknowledge_chart_rows

        cache = getattr(self, "_chart_snapshot_cache", {})
        acknowledge_chart_rows(cache, rows)
        self._chart_snapshot_cache = cache

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
        custom_info["entry_decision_admission"] = {
            "policy": "single_attempt_after_clear", "error": self._entry_decision_error,
            "episode_buy_supported": False,
        }
        custom_info["durable_evaluation_watermark"] = (
            self._durable_watermark_snapshot()
        )
        if self._entry_acquisition_shadow is not None:
            custom_info["entry_acquisition_shadow"] = self._entry_acquisition_shadow.snapshot()
        if self._episode_runtime is not None:
            custom_info["episode"] = self._episode_runtime.status()
        return custom_info

    def _compute_plan_fields(self) -> Dict[str, str]:
        fields = super()._compute_plan_fields()
        base, quote = self._base_quote_assets()
        if self.config.trade_mode == "sell_only" and self.config.sell_only_strict_fraction:
            threshold = (self.processed_data or {}).get("thresholds", {}).get("rsi_sell", self.config.sell_rsi_overbought)
            fields.update(
                plan_mode="EXITS", plan_target=f"rsi≥{float(threshold):.2f}",
                plan_anchor="profit check off" if not self.config.sell_only_if_profitable else "profitable only",
                plan_next="RSI and reversal checks",
                plan_slice=f"{(self.config.sell_only_inventory_fraction * 100).normalize():f}% of available {base}",
            )
        elif self.config.trade_mode == "buy_sell" and fields.get("plan_mode") in {"EXITS", "TRAIL"}:
            target = self.config.usd_per_entry * self.config.signal_sell_inventory_slice_multiplier
            fields["plan_slice"] = f"target {target:g} {quote}, limited to sellable inventory"
        return fields

    def get_status_summary(self) -> Dict[str, object]:
        summary = super().get_status_summary()
        summary["trailing_policy"] = self._trailing_policy_snapshot()
        summary["entry_decision_admission"] = {
            "policy": "single_attempt_after_clear", "error": self._entry_decision_error,
            "episode_buy_supported": False,
        }
        summary["durable_evaluation_watermark"] = (
            self._durable_watermark_snapshot()
        )
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
            allowed = super().can_create_executor(signal)
            if allowed and signal < 0 and self.config.trade_mode == "sell_only" and self.config.sell_only_strict_fraction:
                inventory = self._get_sellable_inventory_amount()
                price = self._shade_limit_maker_price(trade_type=TradeType.SELL, fallback_price=self._get_mid_price())
                return self._resolve_recovery_sell_amount(
                    position_amount=inventory, target_amount=self._sell_only_target_amount(inventory, price),
                    price=price, available_base=inventory,
                ) > 0
            return allowed
        except ExecutorInputUnavailable:
            self._record_gate_reason(target_side, "execution_input_unavailable")
            return False

    def _sell_only_target_amount(self, position_amount: Decimal, price: Decimal) -> Decimal:
        if position_amount <= 0 or price <= 0:
            return Decimal("0")
        if not self.config.sell_only_strict_fraction and position_amount * price < self.config.sell_all_below_quote:
            return position_amount
        return position_amount * self.config.sell_only_inventory_fraction

    def _resolve_recovery_sell_amount(
        self, *, position_amount: Decimal, target_amount: Decimal, price: Decimal,
        available_base: Optional[Decimal] = None,
    ) -> Decimal:
        if self.config.trade_mode != "sell_only" or not self.config.sell_only_strict_fraction:
            return super()._resolve_recovery_sell_amount(
                position_amount=position_amount, target_amount=target_amount, price=price,
                available_base=available_base,
            )
        ceiling = min(position_amount, target_amount)
        if available_base is not None:
            ceiling = min(ceiling, available_base)
        if ceiling <= 0 or price <= 0:
            return Decimal("0")
        rule = self._require_trading_rule()
        minimum = self._minimum_entry_amount(price, rule)
        if ceiling < minimum:
            self._record_gate_reason(TradeType.SELL, "sell_fraction_below_minimum")
            return Decimal("0")
        amount = self._quantize_amount(ceiling)
        if amount > ceiling or amount < minimum or amount * price < self._minimum_entry_quote(price, rule):
            self._record_gate_reason(TradeType.SELL, "sell_fraction_below_minimum")
            return Decimal("0")
        return amount

    def _signal_sell_target_amount(self, position_amount: Decimal, price: Decimal) -> Decimal:
        if self.config.trade_mode == "sell_only":
            return self._sell_only_target_amount(position_amount, price)
        multiplier = Decimal(str(self.config.signal_sell_inventory_slice_multiplier))
        if multiplier > 0:
            return min(position_amount, self.config.usd_per_entry * multiplier / price)
        return position_amount

    def _recovery_sell_target_amount(self, position_amount: Decimal, price: Decimal) -> Decimal:
        # The pair-wide configured target also bounds each owned recovery position.
        return min(position_amount, self._signal_sell_target_amount(self._get_sellable_inventory_amount(), price))

    def _recovery_sell_proposal_limit(self) -> Optional[int]:
        return 1

    def _owned_sell_cost_floor(self) -> tuple[Decimal, Decimal]:
        """Return cost-covered quantity and the aggregate floor before estimated exit fees."""
        total_amount = Decimal("0")
        cost_quote = Decimal("0")
        fees_quote = Decimal("0")
        for position in self._held_positions(TradeType.BUY):
            amount = self._decimal_or_none(position.amount)
            basis = self._decimal_or_none(getattr(position, "breakeven_price", None))
            if amount is None or amount <= 0 or basis is None or basis <= 0:
                raise ExecutorInputUnavailable("SELL owned cost basis is unavailable")
            if (getattr(position, "remaining_fees_quote", None) is None
                    and getattr(position, "cum_fees_quote", None) is None):
                raise ExecutorInputUnavailable("SELL owned position fees are unavailable")
            try:
                fees = self._decimal_or_none(self._remaining_position_fees(position))
            except _DECIMAL_CONVERSION_ERRORS as exc:
                raise ExecutorInputUnavailable("SELL owned position fees are unavailable") from exc
            if fees is None or fees < 0:
                raise ExecutorInputUnavailable("SELL owned position fees are unavailable")
            total_amount += amount
            cost_quote += amount * basis
            fees_quote += fees
        if total_amount <= 0:
            raise ExecutorInputUnavailable("SELL owned cost basis is unavailable")
        return total_amount, (
            cost_quote * (Decimal("1") + Decimal(str(self.config.min_profit_pct_for_sell))) + fees_quote
        ) / total_amount

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
            min_sell_price = None
            if self.config.sell_only_if_profitable:
                owned_amount, min_sell_price = self._owned_sell_cost_floor()
                position_amount = min(position_amount, owned_amount)
            target_amount = self._signal_sell_target_amount(position_amount, ref_price)
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
                min_sell_price=min_sell_price,
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
            entry_order_policy="single_attempt",
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
