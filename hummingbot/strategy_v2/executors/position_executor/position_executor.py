import asyncio
import copy
import hashlib
import json
import logging
import time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from typing import Dict, List, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1, validate_acquisition_chain
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.order_accounting import in_flight_base_fee, in_flight_base_fee_quote
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    parse_position_config,
)
from hummingbot.strategy_v2.executors.restart_continuity import (
    RESTART_ENTRY_STOP_REASON,
    RESTART_RESTORE_KEY,
    RESTART_SUSPEND_KEY,
    TERMINAL_STATES,
    ContinuityPlan,
    RestartContinuityError,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


def validate_planned_entry_reconciliation(config: PositionExecutorConfig, order: InFlightOrder, receipt: Dict) -> Optional[Dict]:
    """Seal only the stopped executor's terminal proof for a fully filled entry."""
    error = "Planned terminal entry reconciliation differs from the sealed entry"
    if receipt is None:
        if config.require_terminal_entry_reconciliation:
            raise ValueError(error)
        return None
    if not isinstance(receipt, dict):
        raise ValueError(error)
    if receipt.get("source") == "native_terminal_entry_set_v1":
        if (set(receipt) != {"schema", "source", "orders"} or type(receipt["schema"]) is not int
                or receipt["schema"] != 1 or not isinstance(receipt["orders"], list)
                or len(receipt["orders"]) != 1):
            raise ValueError(error)
        proof = receipt["orders"][0]
    else:
        proof = receipt
    fields = {"schema", "source", "order_id", "exchange_order_id", "trading_pair",
              "side", "amount", "filled_base", "state", "trade_ids"}
    if not isinstance(proof, dict) or set(proof) != fields or type(proof["schema"]) is not int or proof["schema"] != 1:
        raise ValueError(error)
    try:
        amount = Decimal(str(proof["amount"]))
        filled = Decimal(str(proof["filled_base"]))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(error) from None
    expected_source = {"okx": "okx_terminal_status_v1",
                       "okx_paper_trade": "native_paper_terminal_v1"}.get(config.connector_name)
    if (not amount.is_finite() or not filled.is_finite() or amount <= 0
            or expected_source is None or proof["source"] != expected_source
            or proof["state"] != "FILLED" or proof["side"] != "BUY"
            or proof["order_id"] != order.client_order_id
            or proof["exchange_order_id"] != order.exchange_order_id
            or proof["trading_pair"] != config.trading_pair
            or amount != order.amount or filled != order.executed_amount_base
            or proof["trade_ids"] != sorted(str(key) for key in order.order_fills)):
        raise ValueError(error)
    return copy.deepcopy(receipt)


class PositionExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @staticmethod
    def _fmt_decimal(value: Optional[Decimal], precision: int = 5) -> str:
        if value is None:
            return "n/a"
        try:
            return f"{Decimal(str(value)):.{precision}f}"
        except Exception:
            return "n/a"

    @staticmethod
    def _to_decimal(value) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            decimal_value = Decimal(str(value))
            return decimal_value if decimal_value.is_finite() else None
        except Exception:
            return None

    def _maker_price_buffer(self, candidate_price: Decimal, touch_price: Decimal) -> Decimal:
        try:
            tick_size = Decimal(str(getattr(self.trading_rules, "min_price_increment", "0")))
        except Exception:
            tick_size = Decimal("0")

        # Ignore placeholder/default quanta from incomplete rule objects.
        if tick_size <= Decimal("0") or tick_size < Decimal("1e-18"):
            return candidate_price

        rounding = ROUND_FLOOR if self.config.side == TradeType.BUY else ROUND_CEILING
        aligned_price = (candidate_price / tick_size).to_integral_value(rounding=rounding) * tick_size

        if self.config.side == TradeType.BUY and aligned_price >= touch_price:
            adjusted_price = aligned_price - tick_size
            return adjusted_price if adjusted_price > Decimal("0") else aligned_price
        if self.config.side == TradeType.SELL and aligned_price <= touch_price:
            return aligned_price + tick_size
        return aligned_price

    def _marketable_limit_close_price(self) -> Decimal:
        if self.close_order_side == TradeType.SELL:
            touch_price = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.BestBid)
            return touch_price
        touch_price = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.BestAsk)
        return touch_price

    def _close_order_type_for(self, close_type: CloseType) -> OrderType:
        if close_type == CloseType.TRAILING_STOP:
            if getattr(self, "_trailing_limit_fallback_pending", False):
                return OrderType.MARKET
            return self.config.triple_barrier_config.trailing_stop_order_type
        return OrderType.MARKET

    def _should_fallback_trailing_limit_close(self) -> bool:
        if self.close_type != CloseType.TRAILING_STOP or self._close_order is None:
            return False
        if getattr(self, "_close_order_type", None) != OrderType.LIMIT:
            return False

        max_age_seconds = max(0, int(self.config.triple_barrier_config.trailing_stop_limit_max_age_seconds))
        if max_age_seconds <= 0:
            return False

        reference_ts = self._close_order.creation_timestamp or self.close_timestamp
        if reference_ts is None:
            return False

        return (self._strategy.current_timestamp - reference_ts) >= max_age_seconds

    def _open_order_age_seconds(self) -> Optional[float]:
        if self._open_order is None or self._open_order.order is None:
            return None
        reference_ts = self._open_order.order.creation_timestamp or self._open_order.last_update_timestamp
        if reference_ts is None:
            return None
        return max(0.0, float(self._strategy.current_timestamp - reference_ts))

    def _should_cancel_stale_unfilled_open_limit_order(self) -> bool:
        if self._open_order is None or self._open_order.order is None:
            return False
        if not self.config.triple_barrier_config.open_order_type.is_limit_type():
            return False
        if not self._open_order.order.is_open:
            return False
        if self.config.entry_order_policy != "single_attempt" and self._open_order.is_filled:
            return False

        max_age_seconds = max(0, int(self.config.triple_barrier_config.open_order_limit_max_age_seconds))
        if max_age_seconds <= 0:
            return False

        age_seconds = self._open_order_age_seconds()
        if age_seconds is None:
            return False
        return age_seconds >= max_age_seconds

    def __init__(self, strategy: StrategyV2Base, config: PositionExecutorConfig,
                 update_interval: float = 1.0, max_retries: int = 10):
        """
        Initialize the PositionExecutor instance.

        :param strategy: The strategy to be used by the PositionExecutor.
        :param config: The configuration for the PositionExecutor, subclass of PositionExecutoConfig.
        :param update_interval: The interval at which the PositionExecutor should be updated, defaults to 1.0.
        :param max_retries: The maximum number of retries for the PositionExecutor, defaults to 5.
        """
        if config.triple_barrier_config.time_limit_order_type != OrderType.MARKET or \
                config.triple_barrier_config.stop_loss_order_type != OrderType.MARKET:
            error = (
                "Only market orders are supported for time_limit and stop_loss "
                f"| executor={config.id} connector={config.connector_name} "
                f"pair={config.trading_pair} side={config.side.name}"
            )
            self.logger().error(error)
            raise ValueError(error)
        super().__init__(strategy=strategy, config=config, connectors=[config.connector_name],
                         update_interval=update_interval)
        if not config.entry_price:
            open_order_price_type = PriceType.BestBid if config.side == TradeType.BUY else PriceType.BestAsk
            config.entry_price = self.get_price(config.connector_name, config.trading_pair,
                                                price_type=open_order_price_type)
        self.config: PositionExecutorConfig = config
        self.trading_rules = self.get_trading_rules(self.config.connector_name, self.config.trading_pair)

        # Order tracking
        self._open_order_entry_expired = False
        self._open_order_expiry_reconciled = False
        self._entry_expiry_reconciliation_task: Optional[asyncio.Task] = None
        self._entry_expiry_reconciliation_retry_at: float = 0.0
        self._open_order: Optional[TrackedOrder] = None
        self._close_order: Optional[TrackedOrder] = None
        self._take_profit_limit_order: Optional[TrackedOrder] = None
        self._completed_open_orders: List[TrackedOrder] = []
        self._completed_close_orders: List[TrackedOrder] = []
        self._failed_orders: List[TrackedOrder] = []
        self._trailing_stop_trigger_pct: Optional[Decimal] = None
        self._trailing_stop_move_count: int = 0
        self._trailing_stop_armed_timestamp: Optional[float] = None
        self._trailing_stop_last_update_timestamp: Optional[float] = None
        self._close_order_type: Optional[OrderType] = None
        self._trailing_limit_fallback_pending: bool = False
        self._trailing_floor_hold: bool = False
        self._resume_entry_sealed = False
        self._planned_resume_id = None
        self._planned_resume_compatibility = None
        self._resume_conflicting_fill = None
        self._entry_submitted: bool = False
        self._entry_stop_reason: Optional[str] = None
        self._entry_cancel_last_attempt_at: Optional[float] = None
        self._entry_reconciliation = None
        self._funding_entry_reconciliation_task = None
        self._entry_failure_is_final: bool = False
        self._entry_lifecycle: Dict = {"state": "awaiting_submission", "stop_reason": None}
        self._close_quantity: Dict = {}
        self._exit_trigger_reason: Optional[str] = None
        # Restart continuity: a planned restart suspends (row stays RUNNING);
        # a restored executor resumes its persisted lifecycle exactly.
        self._restart_suspend_requested: bool = False
        self._restart_suspended: Optional[Dict] = None
        self._restart_restored: Optional[Dict] = None
        self._restart_stop_entry_pending: bool = False
        self._restart_target_status: Optional[RunnableStatus] = None
        self._restart_target_close_type: Optional[CloseType] = None
        self._restart_demote_to_hold: Optional[str] = None

        self._total_executed_amount_backup: Decimal = Decimal("0")
        self._current_retries = 0
        self._max_retries = max_retries

    def hydrate_planned_position(self, position: Dict, manifest_id: str):
        """Restore the original terminal receipt; caller must atomically claim before start."""
        if self.status != RunnableStatus.NOT_STARTED or self._open_order is not None:
            raise ValueError("Position must be new and unstarted before hydration")
        resume_config = position.get("resume_config", position["config"])
        if self.config != parse_position_config(resume_config):
            raise ValueError("Planned position configuration differs")
        chain = position.get("acquisition_chain")
        if isinstance(chain, dict):
            if self.config.entry_chain_contract != ACQUISITION_CHAIN_V1:
                raise ValueError("Planned acquisition chain configuration differs")
            orders = [InFlightOrder.from_json(row) for row in position.get("entry_orders", [])]
            if not orders or orders[-1].client_order_id != position["entry"]["client_order_id"]:
                raise ValueError("Planned acquisition chain entry order differs")
            if (orders[-1].current_state != OrderState.FILLED
                    or orders[-1].executed_amount_base != orders[-1].amount
                    or orders[-1].amount != self.config.amount
                    or any(order.current_state != OrderState.CANCELED or order.executed_amount_base != 0
                           for order in orders[:-1])):
                raise ValueError("Planned acquisition chain must be zero-fill cancellations followed by one full fill")
            custom = {
                "acquisition_chain": chain,
                "execution_order_snapshots": [order.to_json() for order in orders],
                "order_ids": [order.client_order_id for order in orders],
                "entry_acquisition": {
                    "intent_id": self.config.id,
                    "policy": self.config.entry_acquisition_policy,
                    "order_ids": [order.client_order_id for order in orders],
                },
            }
            validate_acquisition_chain(self.config, custom, position.get("entry_reconciliation"), require_closed=True)
            if any(order.trade_type is not TradeType.BUY for order in orders):
                raise ValueError("Planned acquisition chain may contain BUY children only")
            tracked = []
            for order in orders:
                item = TrackedOrder(order_id=order.client_order_id)
                item.order = order
                tracked.append(item)
            self._completed_open_orders = tracked[:-1]
            self._open_order = tracked[-1]
            self._entry_reconciliation = copy.deepcopy(position.get("entry_reconciliation"))
        else:
            order = InFlightOrder.from_json(position["entry"])
            if order.current_state != OrderState.FILLED or order.executed_amount_base != order.amount:
                raise ValueError("Planned position entry must be fully filled")
            terminal_receipt = validate_planned_entry_reconciliation(
                self.config, order, position.get("entry_reconciliation"))
            self._open_order = TrackedOrder(order_id=order.client_order_id)
            self._open_order.order = order
            self._entry_reconciliation = terminal_receipt
        self._resume_entry_sealed = True
        self._planned_resume_id = manifest_id
        self._planned_resume_compatibility = copy.deepcopy(position.get("planned_position_resume_compatibility_v1"))
        if self._planned_resume_compatibility is None and position.get("legacy_transition"):
            chain = position["acquisition_chain"]
            self._planned_resume_compatibility = {
                "schema": 1, "kind": "legacy_single_post_only_repair_v1",
                "manifest_id": manifest_id, "executor_id": self.config.id,
                "original_config_sha256": position["legacy_transition"]["original_config_sha256"],
                "resume_config_sha256": hashlib.sha256(
                    json.dumps(position["resume_config"], sort_keys=True, separators=(",", ":"),
                               allow_nan=False).encode()).hexdigest(),
                "acquisition_chain_sha256": hashlib.sha256(
                    json.dumps(chain, sort_keys=True, separators=(",", ":"),
                               allow_nan=False).encode()).hexdigest(),
                "entry_order_ids": [child["order_id"] for child in chain["children"]],
            }
        self._entry_submitted = True
        # A resume restores the owner's own terminal facts; the manifest id is
        # already recorded separately, so the original stop reason survives.
        archived_lifecycle = position.get("entry_lifecycle") if isinstance(position.get("entry_lifecycle"), dict) else {}
        self._entry_lifecycle = {"state": "managing_fill",
                                 "stop_reason": archived_lifecycle.get("stop_reason") or "planned_resume_sealed_entry",
                                 "resume_sealed_entry": True}
        trailing = position["trailing"]
        trigger = trailing["trailing_stop_trigger_pct"]
        self._trailing_stop_trigger_pct = Decimal(str(trigger)) if trigger is not None else None
        self._trailing_stop_move_count = trailing["trailing_move_count"]
        self._trailing_stop_armed_timestamp = trailing["trailing_armed_timestamp"]
        self._trailing_stop_last_update_timestamp = trailing["trailing_last_update_timestamp"]
        if self.amount_to_close != Decimal(position["net_base"]):
            raise ValueError("Hydrated owned inventory differs from sealed receipt")

    def _continuity_tracked(self, order: InFlightOrder) -> TrackedOrder:
        """Bind a persisted order to the connector's live tracker object.

        A non-terminal snapshot is handed back to the tracker so venue status and
        fill polling reconcile what happened while the process was down.
        """
        tracked = TrackedOrder(order_id=order.client_order_id)
        live = self.get_in_flight_order(self.config.connector_name, order.client_order_id)
        if live is not None:
            if (live.trading_pair != order.trading_pair or live.trade_type != order.trade_type
                    or (order.exchange_order_id and live.exchange_order_id
                        and live.exchange_order_id != order.exchange_order_id)):
                raise RestartContinuityError("Tracked order identity differs from executor snapshot")
            if live.executed_amount_base < order.executed_amount_base:
                raise RestartContinuityError("Tracked order has fewer fills than executor snapshot")
            tracked.order = live
            return tracked
        if order.current_state not in TERMINAL_STATES:
            tracker = getattr(self.connectors[self.config.connector_name], "_order_tracker", None)
            if tracker is None or not callable(getattr(tracker, "start_tracking_order", None)):
                raise RestartContinuityError("Open order cannot be handed back to the connector tracker")
            tracker.start_tracking_order(order)
        tracked.order = order
        return tracked

    def hydrate_restart_continuity(self, plan: ContinuityPlan):
        """Rebuild this unstarted executor from its persisted row (planned or crash restart)."""
        if self.status != RunnableStatus.NOT_STARTED or self._open_order is not None:
            raise RestartContinuityError("Executor must be new and unstarted before continuity hydration")
        if plan.executor_id != self.config.id:
            raise RestartContinuityError("Continuity plan belongs to another executor")
        custom = plan.custom_info
        entries = [self._continuity_tracked(order) for order in plan.entry_orders]
        exits = [self._continuity_tracked(order) for order in plan.exit_orders]
        self._completed_open_orders = entries[:-1]
        self._open_order = entries[-1] if entries else None
        open_exit = plan.open_exit_order
        self._completed_close_orders = [item for item in exits
                                        if open_exit is None or item.order_id != open_exit.client_order_id]
        self._close_order = next((item for item in exits
                                  if open_exit is not None and item.order_id == open_exit.client_order_id), None)
        if self._close_order is not None and plan.close_type is None:
            # A resting exit while RUNNING is the take-profit limit order, not a triggered close.
            self._take_profit_limit_order, self._close_order = self._close_order, None
        self._entry_submitted = bool(entries)
        lifecycle = custom.get("entry_lifecycle")
        if isinstance(lifecycle, dict):
            self._entry_lifecycle = {key: value for key, value in copy.deepcopy(lifecycle).items() if key != "policy"}
            self._entry_lifecycle.setdefault("state", "managing_fill")
            self._entry_stop_reason = lifecycle.get("stop_reason")
        self._entry_reconciliation = copy.deepcopy(custom.get("entry_reconciliation"))
        self._open_order_entry_expired = bool(custom.get("open_order_entry_expired"))
        self._open_order_expiry_reconciled = bool(custom.get("open_order_expiry_reconciled"))
        self._planned_resume_id = custom.get("planned_position_resume")
        self._planned_resume_compatibility = copy.deepcopy(custom.get("planned_position_resume_compatibility_v1"))
        retries = custom.get("current_retries") or 0
        self._current_retries = retries if isinstance(retries, int) and not isinstance(retries, bool) else 0
        self._exit_trigger_reason = custom.get("exit_trigger_reason")
        self._close_quantity = copy.deepcopy(custom.get("close_quantity") or {})
        self._held_position_orders = copy.deepcopy(custom.get("held_position_orders") or [])
        trailing = plan.trailing
        self._trailing_stop_trigger_pct = trailing["trailing_stop_trigger_pct"]
        self._trailing_stop_move_count = trailing["trailing_move_count"]
        self._trailing_stop_armed_timestamp = trailing["trailing_armed_timestamp"]
        self._trailing_stop_last_update_timestamp = trailing["trailing_last_update_timestamp"]
        last_entry = self._open_order.order if self._open_order is not None else None
        entry_fully_filled = (last_entry is not None and last_entry.current_state == OrderState.FILLED
                              and last_entry.executed_amount_base == last_entry.amount)
        if entry_fully_filled and all(item.order.current_state in TERMINAL_STATES for item in entries):
            # Terminal entry facts are sealed: late tracker events cannot rewrite them.
            self._resume_entry_sealed = True
            self._entry_lifecycle["state"] = "managing_fill"
        elif not plan.entry_terminal or not entries:
            # An open entry, or a row checkpointed before its entry order was recorded
            # (crash window): never submit a new entry for this intent.
            self._restart_stop_entry_pending = True
        self._restart_target_status = plan.status
        self._restart_target_close_type = plan.close_type
        self._restart_restored = {
            "suspended": plan.suspended,
            "prior_status": plan.status.name,
            "prior_close_type": plan.close_type.name if plan.close_type is not None else None,
            "restored_at": self._strategy.current_timestamp,
            "trailing_stop_trigger_pct": (str(self._trailing_stop_trigger_pct)
                                          if self._trailing_stop_trigger_pct is not None else None),
            "notes": list(plan.notes),
        }
        persisted_remaining = custom.get("remaining_position_amount_base")
        if persisted_remaining is not None and plan.entry_terminal and plan.open_exit_order is None:
            if Decimal(str(persisted_remaining)) != self.amount_to_close - self._closed_residual_dust():
                raise RestartContinuityError("Rebuilt inventory differs from persisted remaining position")

    def demote_restored_to_hold(self, reason: str):
        """Hand an unprovable restored position to the held ledger on its first tick."""
        self._restart_demote_to_hold = reason

    def stop_entry_acquisition(self, reason: str):
        """Stop buying without touching the filled position or its exit barriers."""
        if self.is_closed or self.status == RunnableStatus.SHUTTING_DOWN:
            return
        if self._open_order is None and self._entry_submitted is False and self.open_filled_amount <= 0:
            # Nothing was ever recorded for this intent: end it without an order.
            self._end_entry_acquisition(reason)
            self._entry_lifecycle["state"] = "ended_no_fill"
            self.close_type = CloseType.EXPIRED
            self.stop()
            return
        if self._entry_terminal_confirmed():
            return  # nothing left to acquire; the filled position keeps its exit barriers
        self._end_entry_acquisition(reason)
        if self.config.entry_order_policy != "single_attempt":
            self._open_order_entry_expired = True  # retry policy cancels the resting entry and never replaces it

    def suspend_for_restart(self) -> bool:
        """Request a planned-restart suspension; the row stays RUNNING for exact restore."""
        if self.is_closed or self.status != RunnableStatus.RUNNING:
            return False
        self._restart_suspend_requested = True
        return True

    @property
    def is_restart_suspended(self) -> bool:
        return self._restart_suspended is not None

    def _advance_restart_suspend(self):
        if not self._restart_suspend_requested or self.status != RunnableStatus.RUNNING or self.is_closed:
            return
        if not self._entry_terminal_confirmed():
            self.stop_entry_acquisition(RESTART_ENTRY_STOP_REASON)
            return  # wait for the venue to acknowledge the entry cancel (or fill)
        if self._close_order is not None and not self._close_order.is_done:
            return  # an exit in flight settles before suspension
        self._restart_suspended = {
            "timestamp": self._strategy.current_timestamp,
            "wall_timestamp": time.time(),
            "reason": "planned_restart",
            "remaining_position_amount_base": str(self.amount_to_close),
        }
        MarketsRecorder.get_instance().store_or_update_executor(self)
        self.logger().info(self._log_message(
            f"Suspended for restart remaining_base={self.amount_to_close} "
            f"trailing_trigger={self._trailing_stop_trigger_pct} moves={self._trailing_stop_move_count}"
        ))
        self.terminated.set()
        self.unregister_events()

    def stop(self):
        task = self._funding_entry_reconciliation_task
        self._funding_entry_reconciliation_task = None
        if task is not None:
            if not task.done():
                task.cancel()
            elif not task.cancelled():
                task.exception()
        self._cancel_entry_expiry_reconciliation()
        if (self.config.entry_order_policy == "single_attempt" or self.config.require_terminal_entry_reconciliation) and not self.is_closed:
            # Detach the terminal receipt from mutable connector cache objects.
            # Later fills belong to recorder/held reconciliation after listeners stop.
            for tracked in self._unique_orders([*self._entry_orders(), *self._exit_orders()]):
                if tracked.order is not None:
                    tracked.order = InFlightOrder.from_json(tracked.order.to_json())
        if self.is_restart_suspended and self.close_type is None and self.status == RunnableStatus.RUNNING:
            self.terminated.set()
            self.unregister_events()
            return
        super().stop()

    def _control_loop_finished(self, task):
        if (task is self._control_loop_task and self.is_restart_suspended
                and self.close_type is None and self.status == RunnableStatus.RUNNING):
            if task.cancelled() or task.exception() is None:
                self.terminated.set()
                return
            error = task.exception()
            self.logger().error("Suspended executor control loop failed",
                                exc_info=(type(error), error, error.__traceback__))
        super()._control_loop_finished(task)

    @property
    def is_perpetual(self) -> bool:
        """
        Check if the exchange connector is perpetual.

        :return: True if the exchange connector is perpetual, False otherwise.
        """
        return self.is_perpetual_connector(self.config.connector_name)

    @property
    def is_trading(self):
        """
        Check if the position is trading.

        :return: True if the position is trading, False otherwise.
        """
        return self.status == RunnableStatus.RUNNING and self.open_filled_amount > Decimal("0")

    @property
    def open_filled_amount(self) -> Decimal:
        """
        Get the filled amount of the open order.

        :return: The filled amount of the open order if it exists, otherwise 0.
        """
        base_asset = self.config.trading_pair.split("-")[0]
        amount = sum((order.executed_amount_base - (
            in_flight_base_fee(order.order, base_asset) if order.order is not None and self.config.side == TradeType.BUY else Decimal("0")
        ) for order in self._entry_orders()), Decimal("0"))
        return amount

    @staticmethod
    def _unique_orders(orders):
        return list({order.order_id: order for order in orders if order is not None}.values())

    def _entry_orders(self):
        return self._unique_orders([*self._completed_open_orders, self._open_order])

    def _exit_orders(self):
        return self._unique_orders([*self._completed_close_orders, self._close_order, self._take_profit_limit_order])

    @property
    def amount_to_close(self) -> Decimal:
        """
        Get the amount to close the position.

        :return: The amount to close the position.
        """
        return self.open_filled_amount - self.close_filled_amount

    def _closed_residual_dust(self) -> Decimal:
        """
        Base residue finer than one exchange lot that remains after every exit order completed.

        Base-denominated entry fees leave net inventory that is not a lot multiple; the exit
        sells the largest valid lot and the sub-lot remainder can never be ordered. It is not
        held inventory: the round trip is closed and the executor keeps its accounting.

        :return: The sub-lot residue, or 0 when the residue is tradeable or the exit is not complete.
        """
        remaining = self.amount_to_close
        if (remaining <= Decimal("0") or self.is_perpetual or self.close_order_side != TradeType.SELL
                or self.close_type == CloseType.POSITION_HOLD  # the held ledger owns that accounting
                or self.close_filled_amount <= Decimal("0") or not self.all_orders_completed()):
            return Decimal("0")
        quantum = self._to_decimal(getattr(self.trading_rules, "min_base_amount_increment", None))
        if quantum is None or quantum <= Decimal("0") or remaining >= quantum:
            return Decimal("0")
        return remaining

    def _exit_complete(self) -> bool:
        """The whole tradeable position is closed exactly; at most sub-lot fee dust remains."""
        return (self.close_filled_amount > Decimal("0")
                and self.amount_to_close - self._closed_residual_dust() == Decimal("0"))

    def _close_order_amount(self) -> Decimal:
        close_amount = self.amount_to_close
        if close_amount <= Decimal("0") or self.is_perpetual or self.close_order_side != TradeType.SELL:
            return close_amount
        if self.config.entry_order_policy == "single_attempt" or self._resume_entry_sealed:
            return self._single_attempt_spot_close_amount()

        base_asset = self.config.trading_pair.split("-")[0]
        previous_reason = getattr(self, "_close_quantity", {}).get("blocked_reason")
        available_base = None

        def retain_unavailable(detail: str) -> Decimal:
            self._close_quantity = {
                "owned_base": close_amount,
                "available_base": available_base,
                "order_amount": Decimal("0"),
                "residual_base": close_amount,
                "blocked_reason": "available_balance_unavailable",
            }
            if previous_reason != "available_balance_unavailable":
                self.logger().warning(self._log_message(detail))
            return Decimal("0")

        try:
            available_base = self._to_decimal(self.get_available_balance(self.config.connector_name, base_asset))
        except Exception as exc:
            return retain_unavailable(
                f"Close available balance read failed; retaining position "
                f"amount_to_close={close_amount} error={type(exc).__name__}"
            )
        if available_base is None or available_base < Decimal("0"):
            return retain_unavailable(
                f"Close available balance unavailable; retaining position "
                f"amount_to_close={close_amount} available_{base_asset}={available_base}"
            )

        capped_amount = min(close_amount, available_base)

        def retain_quantization(reason: str, detail: str) -> Decimal:
            self._close_quantity = {
                "owned_base": close_amount,
                "available_base": available_base,
                "order_amount": Decimal("0"),
                "residual_base": close_amount,
                "blocked_reason": reason,
            }
            if previous_reason != reason:
                self.logger().error(self._log_message(detail))
            return Decimal("0")

        try:
            quantized_amount = self._to_decimal(
                self.connectors[self.config.connector_name].quantize_order_amount(
                    trading_pair=self.config.trading_pair,
                    amount=capped_amount,
                )
            )
        except Exception as exc:
            return retain_quantization(
                "quantity_quantization_failed",
                f"Close order amount quantization failed; retaining position "
                f"amount_to_close={close_amount} available_{base_asset}={available_base} "
                f"error={type(exc).__name__}",
            )
        if (
            quantized_amount is None
            or quantized_amount <= Decimal("0")
            or quantized_amount > capped_amount
        ):
            return retain_quantization(
                "quantity_quantization_invalid",
                f"Close order amount quantization invalid; retaining position "
                f"amount_to_close={close_amount} available_{base_asset}={available_base} "
                f"quantized_amount={quantized_amount}",
            )
        capped_amount = quantized_amount
        if available_base < close_amount:
            self.logger().warning(
                self._log_message(
                    f"Capping close order amount to available base balance "
                    f"amount_to_close={close_amount} available_{base_asset}={available_base} "
                    f"order_amount={capped_amount}"
                )
            )
        elif capped_amount < close_amount:
            self.logger().debug(self._log_message(
                f"Quantizing close order amount amount_to_close={close_amount} order_amount={capped_amount}"))
        self._close_quantity = {
            "owned_base": close_amount,
            "available_base": available_base,
            "order_amount": capped_amount,
            "residual_base": close_amount - capped_amount,
            "blocked_reason": None,
        }
        return capped_amount

    def _single_attempt_spot_close_amount(self) -> Decimal:
        """Sell the largest valid owned lot; never borrow unrelated wallet inventory."""
        owned = self._to_decimal(self.amount_to_close)
        quantum = self._to_decimal(self.trading_rules.min_base_amount_increment)
        minimum = self._to_decimal(self.trading_rules.min_order_size)
        notional = self._to_decimal(self.trading_rules.min_notional_size)
        price = self._to_decimal(self.current_market_price)
        previous_reason = self._close_quantity.get("blocked_reason")
        receipt = {"owned_base": owned, "quantity_increment": quantum, "order_amount": Decimal("0"),
                   "residual_base": owned, "blocked_reason": None}

        def blocked(reason):
            receipt["blocked_reason"] = reason
            self._close_quantity = receipt
            if reason != previous_reason:
                self.logger().warning(self._log_message(
                    f"Spot close quantity blocked reason={reason} owned_base={owned} quantum={quantum}"
                ))
            return Decimal("0")

        if (owned is None or quantum is None or minimum is None or notional is None or price is None
                or owned < 0 or quantum <= 0 or minimum < 0 or notional < 0 or price <= 0):
            return blocked("quantity_rules_or_price_unavailable")
        owned_lot = (owned / quantum).to_integral_value(rounding=ROUND_FLOOR) * quantum
        if owned_lot <= 0 or owned_lot < minimum or owned_lot * price < notional:
            return blocked("owned_residual_below_exchange_minimum")
        base = self.config.trading_pair.split("-")[0]
        try:
            available = self._to_decimal(self.get_available_balance(self.config.connector_name, base))
        except Exception:
            return blocked("available_balance_unavailable")
        receipt["available_base"] = available
        if available is None or available < 0:
            return blocked("available_balance_unavailable")
        capped = (min(owned, available) / quantum).to_integral_value(rounding=ROUND_FLOOR) * quantum
        if capped <= 0 or capped < minimum or capped * price < notional:
            return blocked("available_balance_below_minimum")
        try:
            quantity = self._to_decimal(self.connectors[self.config.connector_name].quantize_order_amount(
                trading_pair=self.config.trading_pair, amount=capped,
            ))
        except Exception:
            return blocked("quantity_quantization_failed")
        if (quantity is None or quantity <= 0 or quantity > capped or quantity % quantum != 0
                or quantity < minimum or quantity * price < notional):
            return blocked("quantity_quantization_invalid")
        receipt.update(order_amount=quantity, residual_base=owned - quantity)
        self._close_quantity = receipt
        return quantity

    @property
    def open_filled_amount_quote(self) -> Decimal:
        """
        Get the filled amount of the open order in quote currency.

        :return: The filled amount of the open order in quote currency.
        """
        # Pair fee-net inventory with a per-fill fee-net quote basis. Valuing all
        # base fees at the gross order VWAP changes cash PnL when fee rates differ.
        base = self.config.trading_pair.split("-")[0]
        return sum((order.executed_amount_quote - (
            in_flight_base_fee_quote(order.order, base)
            if order.order is not None and self.config.side == TradeType.BUY else Decimal("0")
        ) for order in self._entry_orders()), Decimal("0"))

    @property
    def _accounting_entry_price(self) -> Decimal:
        """Fee-net basis per owned unit; entry_price remains the gross fill VWAP."""
        amount = self.open_filled_amount
        return self.open_filled_amount_quote / amount if amount > Decimal("0") else self.entry_price

    @property
    def close_filled_amount(self) -> Decimal:
        """
        Get the filled amount of the close order.

        :return: The filled amount of the close order if it exists, otherwise 0.
        """
        return sum((order.executed_amount_base for order in self._exit_orders()), Decimal("0"))

    @property
    def close_filled_amount_quote(self) -> Decimal:
        return sum((order.executed_amount_quote for order in self._exit_orders()), Decimal("0"))

    @property
    def filled_amount(self) -> Decimal:
        """
        Get the filled amount of the position.
        """
        return self.open_filled_amount + self.close_filled_amount

    @property
    def filled_amount_quote(self) -> Decimal:
        """
        Get the filled amount of the position in quote currency.
        """
        return self.open_filled_amount_quote + self.close_filled_amount_quote if self.close_type != CloseType.POSITION_HOLD else Decimal("0")

    @property
    def is_expired(self) -> bool:
        """
        Check if the position is expired.

        :return: True if the position is expired, False otherwise.
        """
        return self.end_time and self.end_time <= self._strategy.current_timestamp

    @property
    def current_market_price(self) -> Decimal:
        """
        This method is responsible for getting the current market price to be used as a reference for control barriers.

        :return: The current market price.
        """
        price_type = PriceType.BestBid if self.config.side == TradeType.BUY else PriceType.BestAsk
        return self.get_price(self.config.connector_name, self.config.trading_pair, price_type=price_type)

    @property
    def entry_price(self) -> Decimal:
        """
        This method is responsible for getting the entry price. If the open order is done, it returns the average executed price.
        If the entry price is set in the configuration, it returns the entry price from the configuration.
        Otherwise, it returns the best ask price for buy orders and the best bid price for sell orders.

        :return: The entry price.
        """
        filled = sum((order.executed_amount_base for order in self._entry_orders()), Decimal("0"))
        if filled > Decimal("0"):
            return sum((order.executed_amount_quote for order in self._entry_orders()), Decimal("0")) / filled
        elif self.config.triple_barrier_config.open_order_type == OrderType.LIMIT_MAKER:
            if self.config.side == TradeType.BUY:
                best_bid = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.BestBid)
                candidate_price = min(self.config.entry_price, best_bid)
            else:
                best_ask = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.BestAsk)
                candidate_price = max(self.config.entry_price, best_ask)
            touch_price = best_bid if self.config.side == TradeType.BUY else best_ask
            return self._maker_price_buffer(candidate_price, touch_price)
        else:
            return self.config.entry_price

    @property
    def close_price(self) -> Decimal:
        """
        This method is responsible for getting the close price. If the close order is done, it returns the average executed price.
        Otherwise, it returns the current market price.

        :return: The close price.
        """
        if self.close_filled_amount > Decimal("0"):
            remaining = max(self.amount_to_close - self._closed_residual_dust(), Decimal("0"))
            if remaining == Decimal("0"):
                return self.close_filled_amount_quote / self.close_filled_amount
            return (self.close_filled_amount_quote + remaining * self.current_market_price) / (self.close_filled_amount + remaining)
        elif self._close_order and self._close_order.is_done:
            return self._close_order.average_executed_price
        else:
            return self.current_market_price

    @property
    def close_order_side(self):
        return TradeType.BUY if self.config.side == TradeType.SELL else TradeType.SELL

    @property
    def trailing_stop_config(self) -> Optional[TrailingStop]:
        return self.config.triple_barrier_config.trailing_stop

    def _known_fee_pct(self) -> Decimal:
        return self.cum_fees_quote / self.open_filled_amount_quote if self.open_filled_amount_quote != Decimal("0") else Decimal("0")

    def _trailing_requires_no_loss_floor(self) -> bool:
        return (
            not self.is_perpetual
            and self.config.side == TradeType.BUY
            and self.open_filled_amount > Decimal("0")
        )

    def _expected_close_amount(self) -> Optional[Decimal]:
        remaining = self._to_decimal(self.amount_to_close)
        if remaining is None or remaining <= Decimal("0"):
            return None
        try:
            parsed = self._to_decimal(self._close_order_amount())
        except Exception:
            return None
        if parsed is None or parsed <= Decimal("0") or parsed > remaining:
            return None
        return parsed

    def _expected_close_fee_rate(self) -> Optional[Decimal]:
        assets = self.config.trading_pair.split("-")
        if len(assets) < 2:
            return None
        close_amount = self._expected_close_amount()
        if close_amount is None:
            return None
        close_order_type = self._close_order_type_for(CloseType.TRAILING_STOP)
        is_maker = close_order_type == OrderType.LIMIT_MAKER
        fee = None
        connector = self.connectors.get(self.config.connector_name)
        get_fee = getattr(connector, "get_fee", None) if connector is not None else None
        if callable(get_fee):
            candidate = get_fee(
                base_currency=assets[0],
                quote_currency=assets[1],
                order_type=close_order_type,
                order_side=self.close_order_side,
                amount=close_amount,
                price=self.entry_price,
                is_maker=is_maker,
            )
            if isinstance(candidate, TradeFeeBase):
                fee = candidate
        if fee is None:
            try:
                fee = build_trade_fee(
                    self.config.connector_name,
                    is_maker,
                    assets[0],
                    assets[1],
                    close_order_type,
                    self.close_order_side,
                    close_amount,
                    self.entry_price,
                )
            except Exception:
                # ponytail: TradeFeeSchemaLoader raises Exception for unknown connectors; hold rather than sell
                return None
        return self._fee_rate_from_trade_fee(fee, close_amount, self.entry_price)

    def _fee_rate_from_trade_fee(
        self, fee: TradeFeeBase, amount: Decimal, price: Decimal
    ) -> Optional[Decimal]:
        if amount <= Decimal("0") or price <= Decimal("0"):
            return None
        assets = self.config.trading_pair.split("-")
        if len(assets) < 2:
            return None
        percent = getattr(fee, "percent", None)
        if percent is None:
            return None
        try:
            rate = percent if isinstance(percent, Decimal) else Decimal(str(percent))
            extra = Decimal("0")
            for flat in getattr(fee, "flat_fees", None) or ():
                flat_amount = flat.amount if isinstance(flat.amount, Decimal) else Decimal(str(flat.amount))
                if not flat_amount.is_finite() or flat_amount < Decimal("0"):
                    return None
                if flat.token == assets[1]:
                    extra += flat_amount
                elif flat.token == assets[0]:
                    extra += flat_amount * price
                else:
                    # ponytail: third-token flats need a rate oracle; hold rather than understate the floor
                    return None
            rate = rate + extra / (amount * price)
        except (InvalidOperation, ValueError, TypeError, AttributeError, ZeroDivisionError):
            return None
        if not rate.is_finite() or rate < Decimal("0") or rate >= Decimal("1"):
            return None
        return rate

    def _min_no_loss_sell_price(self) -> Optional[Decimal]:
        sell_fee_rate = self._expected_close_fee_rate()
        if sell_fee_rate is None:
            return None
        entry = self._accounting_entry_price
        fee_pct = self._known_fee_pct()
        if not isinstance(entry, Decimal) or not isinstance(fee_pct, Decimal) or entry <= Decimal("0"):
            return None
        net_entry = entry * (Decimal("1") + fee_pct)
        return net_entry / (Decimal("1") - sell_fee_rate)

    def _min_no_loss_trigger_pct(self) -> Optional[Decimal]:
        min_sell = self._min_no_loss_sell_price()
        entry = self._accounting_entry_price
        if min_sell is None or not isinstance(entry, Decimal) or entry <= Decimal("0"):
            return None
        return min_sell / entry - Decimal("1") - self._known_fee_pct()

    def _floored_trailing_trigger_pct(self, trigger_pct: Decimal) -> Decimal:
        if not self._trailing_requires_no_loss_floor():
            return trigger_pct
        floor_pct = self._min_no_loss_trigger_pct()
        if floor_pct is None:
            return trigger_pct
        return max(trigger_pct, floor_pct)

    def _should_hold_trailing_close(self) -> bool:
        if not self._trailing_requires_no_loss_floor():
            return False
        min_sell = self._min_no_loss_sell_price()
        current = self._current_trailing_reference_price()
        if min_sell is None or current is None:
            return True
        return current < min_sell

    def _price_for_net_pnl_pct(self, target_net_pnl_pct: Optional[Decimal]) -> Optional[Decimal]:
        if target_net_pnl_pct is None:
            return None
        entry = self._accounting_entry_price
        if entry <= Decimal("0"):
            return None
        target_pct = Decimal(str(target_net_pnl_pct))
        fee_pct = self._known_fee_pct()
        if self.config.side == TradeType.BUY:
            return entry * (Decimal("1") + fee_pct + target_pct)
        else:
            return entry * (Decimal("1") - fee_pct - target_pct)

    def _current_trailing_reference_price(self) -> Optional[Decimal]:
        try:
            return self._to_decimal(self.current_market_price)
        except Exception:
            try:
                return self._to_decimal(self.entry_price)
            except Exception:
                return None

    def _distance_to_trailing_activation_pct(
        self, activation_price: Optional[Decimal], current_price: Optional[Decimal]
    ) -> Optional[Decimal]:
        if activation_price is None or current_price is None or current_price <= Decimal("0"):
            return None
        if self.config.side == TradeType.BUY:
            return max(activation_price - current_price, Decimal("0")) / current_price
        else:
            return max(current_price - activation_price, Decimal("0")) / current_price

    def _distance_to_trailing_trigger_pct(
        self, trigger_price: Optional[Decimal], current_price: Optional[Decimal]
    ) -> Optional[Decimal]:
        if trigger_price is None or current_price is None or current_price <= Decimal("0"):
            return None
        if self.config.side == TradeType.BUY:
            return max(current_price - trigger_price, Decimal("0")) / current_price
        else:
            return max(trigger_price - current_price, Decimal("0")) / current_price

    def _trailing_status_snapshot(self) -> Dict:
        trailing_stop = self.trailing_stop_config
        if trailing_stop is None:
            return {
                "trailing_state": "disabled",
                "trailing_activation_pct": None,
                "trailing_delta_pct": None,
                "trailing_activation_price": None,
                "distance_to_trailing_activation_pct": None,
                "trailing_stop_trigger_pct": None,
                "trailing_trigger_price": None,
                "distance_to_trailing_trigger_pct": None,
                "trailing_move_count": 0,
                "trailing_armed_timestamp": None,
                "trailing_last_update_timestamp": None,
                "trailing_current_price": None,
            }

        current_price = self._current_trailing_reference_price()
        activation_price = self._price_for_net_pnl_pct(trailing_stop.activation_price)
        trigger_pct = self._trailing_stop_trigger_pct
        trigger_price = self._price_for_net_pnl_pct(trigger_pct) if trigger_pct is not None else None

        if self.close_type == CloseType.TRAILING_STOP:
            trailing_state = "triggered"
        elif trigger_pct is not None:
            trailing_state = "armed"
        elif self.open_filled_amount <= Decimal("0"):
            trailing_state = "pending"
        else:
            trailing_state = "waiting"

        return {
            "trailing_state": trailing_state,
            "trailing_activation_pct": trailing_stop.activation_price,
            "trailing_delta_pct": trailing_stop.trailing_delta,
            "trailing_activation_price": activation_price,
            "distance_to_trailing_activation_pct": self._distance_to_trailing_activation_pct(activation_price, current_price),
            "trailing_stop_trigger_pct": trigger_pct,
            "trailing_trigger_price": trigger_price,
            "distance_to_trailing_trigger_pct": self._distance_to_trailing_trigger_pct(trigger_price, current_price),
            "trailing_move_count": self._trailing_stop_move_count,
            "trailing_armed_timestamp": self._trailing_stop_armed_timestamp,
            "trailing_last_update_timestamp": self._trailing_stop_last_update_timestamp,
            "trailing_current_price": current_price,
        }

    @property
    def trade_pnl_pct(self) -> Decimal:
        """
        Calculate the trade pnl (Pure pnl without fees)

        :return: The trade pnl percentage.
        """
        if self.open_filled_amount != Decimal("0") and self.close_type not in [CloseType.FAILED, CloseType.POSITION_HOLD]:
            if self._exit_complete() and self.open_filled_amount_quote != Decimal("0"):
                return self._realized_trade_pnl_quote() / self.open_filled_amount_quote
            if self.config.side == TradeType.BUY:
                return (self.close_price - self._accounting_entry_price) / self._accounting_entry_price
            else:
                return (self._accounting_entry_price - self.close_price) / self._accounting_entry_price
        else:
            return Decimal("0")

    @property
    def trade_pnl_quote(self) -> Decimal:
        """
        Calculate the trade pnl in quote asset

        :return: The trade pnl in quote asset.
        """
        if (self.open_filled_amount != Decimal("0") and self.close_type not in [CloseType.FAILED, CloseType.POSITION_HOLD]
                and self._exit_complete()):
            return self._realized_trade_pnl_quote()
        return self.trade_pnl_pct * self.open_filled_amount_quote

    def _realized_trade_pnl_quote(self) -> Decimal:
        """
        Trade pnl of a completed exit from the actual fills: quote received against the
        fee-net cost basis. Sub-lot fee dust is written off at zero, so with the fees in
        cum_fees_quote this equals quote received minus quote paid minus the exit fee.
        """
        if self.config.side == TradeType.BUY:
            return self.close_filled_amount_quote - self.open_filled_amount_quote
        return self.open_filled_amount_quote - self.close_filled_amount_quote

    def get_net_pnl_quote(self) -> Decimal:
        """
        Calculate the net pnl in quote asset

        :return: The net pnl in quote asset.
        """
        return self.trade_pnl_quote - self.cum_fees_quote

    def get_cum_fees_quote(self) -> Decimal:
        """
        Calculate the cumulative fees in quote asset

        :return: The cumulative fees in quote asset.
        """
        orders = self._unique_orders([*self._entry_orders(), *self._exit_orders()])
        return sum([order.cum_fees_quote for order in orders if order])

    def get_net_pnl_pct(self) -> Decimal:
        """
        Calculate the net pnl percentage

        :return: The net pnl percentage.
        """
        return self.net_pnl_quote / self.open_filled_amount_quote if self.open_filled_amount_quote != Decimal("0") else Decimal("0")

    @property
    def end_time(self) -> Optional[float]:
        """
        Calculate the end time of the position based on the time limit

        :return: The end time of the position.
        """
        if not self.config.triple_barrier_config.time_limit:
            return None
        return self.config.timestamp + self.config.triple_barrier_config.time_limit

    @property
    def take_profit_price(self):
        """
        This method is responsible for calculating the take profit price to place the take profit limit order.

        :return: The take profit price.
        """
        if self.config.side == TradeType.BUY:
            take_profit_price = self.entry_price * (1 + self.config.triple_barrier_config.take_profit)
            if self.config.triple_barrier_config.take_profit_order_type == OrderType.LIMIT_MAKER:
                take_profit_price = max(take_profit_price,
                                        self.get_price(self.config.connector_name, self.config.trading_pair,
                                                       PriceType.BestAsk))
        else:
            take_profit_price = self.entry_price * (1 - self.config.triple_barrier_config.take_profit)
            if self.config.triple_barrier_config.take_profit_order_type == OrderType.LIMIT_MAKER:
                take_profit_price = min(take_profit_price,
                                        self.get_price(self.config.connector_name, self.config.trading_pair,
                                                       PriceType.BestBid))
        return take_profit_price

    async def control_task(self):
        """
        This method is responsible for controlling the task based on the status of the executor.

        :return: None
        """
        if self._resume_conflicting_fill is not None or self._stack_recovery_required():
            return  # Keep ownership; only explicit reconciliation can resolve a sealed receipt conflict.
        if (self._restart_demote_to_hold is not None
                and self.status in (RunnableStatus.RUNNING, RunnableStatus.SHUTTING_DOWN)):
            self.logger().warning(self._log_message(
                f"Restored position handed to held ledger reason={self._restart_demote_to_hold}"))
            self._restart_demote_to_hold = None
            self.early_stop(keep_position=True)
        if self._restart_stop_entry_pending and self.status == RunnableStatus.RUNNING:
            self._restart_stop_entry_pending = False
            self.stop_entry_acquisition(RESTART_ENTRY_STOP_REASON)
        if self.status == RunnableStatus.RUNNING:
            self.control_open_order()
            await self._reconcile_expired_entry()
            if (self.status == RunnableStatus.RUNNING
                    and self._entry_lifecycle["state"] != "reconciliation_required"
                    and (not self._open_order_entry_expired or self.open_filled_amount > Decimal("0"))):
                self.control_barriers()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()
        self._advance_restart_suspend()
        if not self.terminated.is_set():
            self.evaluate_max_retries()

    def all_orders_completed(self):
        """
        This method is responsible for checking if the open orders are completed.

        :return: True if the open orders are completed, False otherwise.
        """
        open_order_condition = self._entry_terminal_confirmed()
        take_profit_condition = not self._take_profit_limit_order or self._take_profit_limit_order.is_done
        close_order_condition = not self._close_order or self._close_order.is_done
        return open_order_condition and take_profit_condition and close_order_condition

    async def control_shutdown_process(self):
        """
        This method is responsible for controlling the shutdown process of the executor.

        :return: None
        """
        if self.close_timestamp is None:
            self.close_timestamp = self._strategy.current_timestamp
        await self._reconcile_expired_entry()
        if (self._open_order_entry_expired and not self._open_order_expiry_reconciled
                and self.open_filled_amount == Decimal("0")):
            # An early-stop request changes exit intent, not proof that entry fills
            # are absent. Keep the executor pending for strict shutdown/restart.
            self.cancel_open_orders()
            await self._sleep(1.0)
            return
        funding_ready = await self._reconcile_funding_entries()
        entry_done = self._entry_terminal_confirmed()
        tp_done = not self._take_profit_limit_order or self._take_profit_limit_order.is_done
        if not entry_done or not tp_done:
            self.cancel_open_orders()
        elif self.close_type == CloseType.POSITION_HOLD:
            if self._close_order and not self._close_order.is_done:
                self._strategy.cancel(connector_name=self.config.connector_name,
                                      trading_pair=self.config.trading_pair, order_id=self._close_order.order_id)
            elif funding_ready:
                self._held_position_orders = [order.order.to_json() for order in
                                              self._unique_orders([*self._entry_orders(), *self._exit_orders()])
                                              if order.order and order.executed_amount_base > Decimal("0")]
                if not self._held_position_orders:
                    self.close_type = CloseType.EARLY_STOP
                self.stop()
        elif self.open_and_close_volume_match():
            if funding_ready:
                self.stop()
        else:
            await self.control_close_order()
        await self._sleep(1.0)

    def open_and_close_volume_match(self):
        return self.amount_to_close - self._closed_residual_dust() <= Decimal("0") and self.all_orders_completed()

    async def control_close_order(self):
        """
        This method is responsible for controlling the close order. If the close order is filled and the open orders are
        completed, it stops the executor. If the close order is not placed, it places the close order. If the close order
        is not filled, it waits for the close order to be filled and requests the order information to the connector.
        """
        if self._close_order and self._close_order.is_done and self.amount_to_close > Decimal("0"):
            self._completed_close_orders.append(self._close_order)
            self._close_order = None
            self._close_order_type = None

        if self._close_order:
            in_flight_order = self.get_in_flight_order(self.config.connector_name,
                                                       self._close_order.order_id) if not self._close_order.order else self._close_order.order
            if in_flight_order:
                self._close_order.order = in_flight_order
                if self._should_fallback_trailing_limit_close() and in_flight_order.is_open:
                    self.logger().warning(
                        self._log_message(
                            f"Trailing limit close stale order_id={self._close_order.order_id} "
                            f"age={self._strategy.current_timestamp - (self._close_order.creation_timestamp or self.close_timestamp):.1f}s "
                            f"falling_back=MARKET"
                        )
                    )
                    self._trailing_limit_fallback_pending = True
                    self._strategy.cancel(
                        connector_name=self.config.connector_name,
                        trading_pair=self.config.trading_pair,
                        order_id=self._close_order.order_id,
                    )
                    return
                connector = self.connectors[self.config.connector_name]
                await connector._update_orders_with_error_handler(
                    orders=[in_flight_order],
                    error_handler=connector._handle_update_error_for_lost_order)
                self.logger().info(
                    self._log_message(
                        f"Waiting for close order fill order_id={self._close_order.order_id} "
                        f"close_type={self.close_type.name if self.close_type else 'UNKNOWN'}"
                    )
                )
            else:
                self.logger().error(self._log_message("Close order state unavailable; retaining reservation until reconciled"))
        else:
            remaining = self.amount_to_close
            if (not self.is_perpetual and remaining > 0 and self.all_orders_completed()
                    and (remaining < self.trading_rules.min_order_size
                         or remaining * self.current_market_price < self.trading_rules.min_notional_size)):
                self.logger().warning(self._log_message('Untradeable residual retained as held inventory'))
                self.early_stop(keep_position=True)
                return
            self.place_close_order_and_cancel_open_orders(close_type=self.close_type)

    def evaluate_max_retries(self):
        """
        This method is responsible for evaluating the maximum number of retries to place an order and stop the executor
        if the maximum number of retries is reached.

        :return: None
        """
        if (self._current_retries > self._max_retries and self.open_filled_amount == Decimal("0")
                and self.all_orders_completed()
                and (not self._open_order_entry_expired or self._open_order_expiry_reconciled)):
            self.close_type = CloseType.FAILED
            if self.config.require_terminal_entry_reconciliation:
                self._status = RunnableStatus.SHUTTING_DOWN
            else:
                self.stop()

    async def on_start(self):
        """
        This method is responsible for starting the executor and validating if the position is expired. The base method
        validates if there is enough balance to place the open order.

        :return: None
        """
        if self._restart_restored is not None:
            if self._restart_target_status == RunnableStatus.SHUTTING_DOWN:
                self.close_type = self._restart_target_close_type
                self._status = RunnableStatus.SHUTTING_DOWN
            return  # The entry was submitted before the restart; its balance check already ran.
        if self._resume_entry_sealed:
            return  # Native barriers retain original end_time; entry balance/expiry checks do not apply.
        await super().on_start()
        if self.is_expired:
            self.close_type = CloseType.EXPIRED
            self.stop()

    def control_open_order(self):
        """
        This method is responsible for controlling the open order. It checks if the open order is not placed and if the
        close price is within the activation bounds to place the open order.

        :return: None
        """
        if self._resume_entry_sealed:
            return
        if self.config.entry_order_policy == "single_attempt":
            self._control_single_attempt_entry()
            return
        if self._open_order_entry_expired:
            if self._open_order and self._open_order.order and self._open_order.order.is_open:
                self.cancel_open_order()
            return
        if not self._open_order:
            if self._is_within_activation_bounds(self.config.entry_price, self.config.side,
                                                 self.config.triple_barrier_config.open_order_type):
                self.place_open_order()
        else:
            if self._should_cancel_stale_unfilled_open_limit_order():
                self.logger().warning(
                    self._log_message(
                        f"Open limit entry stale order_id={self._open_order.order_id} "
                        f"age={self._open_order_age_seconds():.1f}s "
                        f"max_age={self.config.triple_barrier_config.open_order_limit_max_age_seconds}s "
                        f"filled_amount={self.open_filled_amount}"
                    )
                )
                self._open_order_entry_expired = True
                self.cancel_open_order()
                return
            if self._open_order.order and not self._open_order.is_filled and \
                    not self._is_within_activation_bounds(self.config.entry_price, self.config.side,
                                                          self.config.triple_barrier_config.open_order_type):
                self.cancel_open_order()

    def _end_entry_acquisition(self, reason: str):
        """Latch an entry decision closed without authorizing a position exit."""
        if self._entry_stop_reason is None:
            self._entry_stop_reason = reason
            self._entry_lifecycle.update({
                "stop_reason": reason,
                "stop_requested_timestamp": self._strategy.current_timestamp,
                "stop_observed_wall_timestamp": time.time(),
            })
            self.logger().info(self._log_message(
                f"Entry acquisition stopped reason={reason} policy=single_attempt "
                f"filled_base={self.open_filled_amount} replacement_allowed=False"
            ))

    def _entry_terminal_confirmed(self) -> bool:
        if self.config.entry_order_policy != "single_attempt":
            return not self._open_order or self._open_order.is_done
        if self._open_order is None:
            return not self._entry_submitted
        order = self._open_order.order
        return order is not None and (
            (order.current_state == OrderState.CANCELED and self._open_order_expiry_reconciled)
            or (order.current_state == OrderState.FILLED and order.executed_amount_base == order.amount)
            or (order.is_failure and self._entry_failure_is_final)
        )

    def _control_single_attempt_entry(self):
        if self.is_closed:
            return
        tracked = self._open_order
        if tracked is None:
            if self._entry_submitted or self._entry_stop_reason is not None:
                self._entry_lifecycle["state"] = "reconciliation_required"
            elif self._is_within_activation_bounds(
                    self.config.entry_price, self.config.side,
                    self.config.triple_barrier_config.open_order_type):
                self.place_open_order()
            return

        self._entry_submitted = True
        self.update_tracked_orders_with_order_id(tracked.order_id)
        order = tracked.order
        if order is None:
            self._entry_lifecycle["state"] = "reconciliation_required"
            return
        created = order.creation_timestamp
        max_age = self.config.triple_barrier_config.open_order_limit_max_age_seconds
        self._entry_lifecycle.update({
            "order_id": tracked.order_id,
            "creation_timestamp": created,
            "deadline_timestamp": created + max_age if max_age > 0 else None,
            "max_age_seconds": max_age,
        })
        if ((order.is_failure and not self._entry_failure_is_final)
                or (order.current_state == OrderState.FILLED and order.executed_amount_base != order.amount)):
            self._end_entry_acquisition("entry_failure")
            self._entry_lifecycle["state"] = "reconciliation_required"
            return
        if order.is_cancelled and not self._open_order_expiry_reconciled:
            self._open_order_entry_expired = True
            self._end_entry_acquisition("exchange_canceled")
            self._entry_lifecycle["state"] = "reconciliation_required"
            return
        if not self._entry_terminal_confirmed():
            if self._should_cancel_stale_unfilled_open_limit_order():
                self._end_entry_acquisition("entry_deadline")
            elif not self._is_within_activation_bounds(
                    self.config.entry_price, self.config.side,
                    self.config.triple_barrier_config.open_order_type):
                self._end_entry_acquisition("outside_activation_bounds")
            if self._entry_stop_reason is not None:
                cancel_observed = self._entry_lifecycle.get("cancel_event_timestamp") is not None
                self._entry_lifecycle["state"] = "reconciliation_required" if cancel_observed else "cancel_pending"
                now = self._strategy.current_timestamp
                if (not cancel_observed and
                        (self._entry_cancel_last_attempt_at is None or now - self._entry_cancel_last_attempt_at >= 5)):
                    self._entry_cancel_last_attempt_at = now
                    self._entry_lifecycle.setdefault("cancel_requested_timestamp", now)
                    self.cancel_open_order()
            else:
                self._entry_lifecycle["state"] = "open"
            return

        # A transport/lost-order failure is not proof the exchange accepted no fill.
        # Retain ownership and reservation until a terminal exchange event reconciles it.
        if order.is_cancelled:
            self._end_entry_acquisition("exchange_canceled")
        self._entry_lifecycle.setdefault("terminal_exchange_timestamp", order.last_update_timestamp)
        self._entry_lifecycle.setdefault("terminal_observed_timestamp", self._strategy.current_timestamp)
        if self.open_filled_amount > Decimal("0"):
            self._entry_lifecycle["state"] = "managing_fill"
            # Once the exit consumed the tradeable lot, sub-lot fee dust is not an unsellable acquisition.
            if not self.is_perpetual and self.config.side == TradeType.BUY and not self._exit_complete():
                quantum = self._to_decimal(self.trading_rules.min_base_amount_increment)
                price = self._to_decimal(self.current_market_price)
                if quantum is not None and quantum > 0 and price is not None and price > 0:
                    lot = (self.amount_to_close / quantum).to_integral_value(rounding=ROUND_FLOOR) * quantum
                    if (lot <= 0 or lot < self.trading_rules.min_order_size
                            or lot * price < self.trading_rules.min_notional_size):
                        self._close_quantity = {
                            "owned_base": self.amount_to_close, "quantity_increment": quantum,
                            "order_amount": Decimal("0"), "residual_base": self.amount_to_close,
                            "blocked_reason": "owned_residual_below_exchange_minimum",
                        }
                        self._entry_lifecycle["state"] = "held_residual"
                        self.early_stop(keep_position=True)
            return
        self._entry_lifecycle["state"] = "ended_no_fill"
        self.close_type = CloseType.FAILED if self._entry_failure_is_final else CloseType.EXPIRED
        self.stop()

    def _cancel_entry_expiry_reconciliation(self):
        task = self._entry_expiry_reconciliation_task
        self._entry_expiry_reconciliation_task = None
        if task is not None:
            if task.done():
                if not task.cancelled():
                    task.exception()  # Consume any obsolete result after a fill or stop.
            else:
                task.cancel()

    def _entry_cancel_observed(self):
        return self._entry_lifecycle.get("cancel_event_timestamp") is not None

    async def _reconcile_expired_entry(self):
        if (self.config.entry_order_policy == "single_attempt" and self._open_order
                and self._open_order.order
                and (self._open_order.order.is_cancelled
                     or self._entry_cancel_observed())
                and not self._open_order_expiry_reconciled):
            self._open_order_entry_expired = True
            self._end_entry_acquisition("exchange_canceled")
        if not self._open_order_entry_expired or self._open_order_expiry_reconciled:
            return
        # A fill can arrive while REST is pending. Never block its exit barriers.
        if self.open_filled_amount > Decimal("0") and self.config.entry_order_policy != "single_attempt":
            self._cancel_entry_expiry_reconciliation()
            return
        task = self._entry_expiry_reconciliation_task
        if task is None:
            if self._strategy.current_timestamp < self._entry_expiry_reconciliation_retry_at:
                return
            orders = self._entry_orders()
            cancel_observed = (self.config.entry_order_policy == "single_attempt"
                               and self._entry_cancel_observed())
            if orders and (all(tracked.is_done for tracked in orders) or cancel_observed):
                self._entry_expiry_reconciliation_task = asyncio.create_task(
                    self._request_expired_entry_reconciliation(orders))
            return
        if not task.done():
            return
        self._entry_expiry_reconciliation_task = None
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            self._entry_expiry_reconciliation_retry_at = self._strategy.current_timestamp + 10.0
            self.logger().warning(self._log_message(
                f"Expired entry reconciliation pending; retaining ownership: {type(exc).__name__}: {exc}"))
            return
        self._open_order_expiry_reconciled = True
        if self.config.entry_order_policy == "single_attempt" and self.status == RunnableStatus.RUNNING:
            self._control_single_attempt_entry()
            return
        if self.status == RunnableStatus.RUNNING and self.open_filled_amount == Decimal("0"):
            self.close_type = CloseType.EXPIRED
            self.close_timestamp = self._strategy.current_timestamp
            self.stop()

    async def _request_expired_entry_reconciliation(self, orders: List[TrackedOrder]):
        connector = self.connectors[self.config.connector_name]
        # A generic terminal OrderUpdate does not prove that all fills arrived.
        # Opt-in connectors must reconcile authoritative cumulative fill totals.
        if getattr(connector, "supports_terminal_fill_reconciliation", False) is not True:
            raise ValueError("Connector does not support authoritative terminal fill reconciliation")
        reconciled = []
        for tracked in orders:
            order = tracked.order
            rejected = getattr(order, 'terminal_reconciliation', None)
            if (order is not None and order.is_failure and isinstance(rejected, dict)
                    and rejected.get('source') == 'okx_submission_rejected_v1'):
                reconciled.append(copy.deepcopy(rejected))
                continue
            if order is None or order.exchange_order_id is None:
                raise ValueError("Expired entry order identity is unavailable")
            update = await asyncio.wait_for(connector._request_order_status(tracked_order=order), timeout=10.0)
            if (not isinstance(update, OrderUpdate)
                    or update.new_state not in {OrderState.CANCELED, OrderState.FILLED}
                    or update.client_order_id != order.client_order_id
                    or update.exchange_order_id != order.exchange_order_id):
                raise ValueError("Expired entry terminal order state is unavailable or mismatched")
            if (update.new_state == OrderState.FILLED and order.executed_amount_base != order.amount
                    or order.current_state == OrderState.FILLED and update.new_state != OrderState.FILLED):
                raise ValueError("Expired entry terminal order state contradicts reconciled fills")
            order.update_with_order_update(update)
            self.update_tracked_orders_with_order_id(order.client_order_id)
            proof = (update.misc_updates or {}).get('terminal_reconciliation')
            if proof is not None:
                reconciled.append(copy.deepcopy(proof))
        if len(orders) == len(reconciled) and reconciled:
            self._store_entry_reconciliation(orders, reconciled)

    def _store_entry_reconciliation(self, orders, reconciled):
        self._entry_reconciliation = (reconciled[0] if len(reconciled) == 1 else
                                      {'schema': 1, 'source': 'native_terminal_entry_set_v1', 'orders': reconciled})

    async def _reconcile_funding_entries(self):
        """Opt-in shutdown proof; native exits keep running while reads are pending."""
        if not self.config.require_terminal_entry_reconciliation:
            return True
        orders = self._entry_orders()
        if not orders or any(order.order is None or not order.is_done for order in orders):
            return False
        proofs = self._entry_reconciliation
        proofs = proofs.get('orders', [proofs]) if isinstance(proofs, dict) else []
        quantities = {order.order_id: order.executed_amount_base for order in orders}
        if (len(proofs) == len(orders)
                and {proof['order_id'] for proof in proofs} == set(quantities)
                and all(Decimal(proof['filled_base']) == quantities[proof['order_id']] for proof in proofs)):
            return True
        task = self._funding_entry_reconciliation_task
        if task is None:
            self._funding_entry_reconciliation_task = asyncio.create_task(self._request_expired_entry_reconciliation(orders))
            return False
        if not task.done():
            return False
        self._funding_entry_reconciliation_task = None
        try:
            task.result()
        except asyncio.CancelledError:
            return False
        except Exception as exc:
            self.logger().warning(self._log_message(f"Funding entry reconciliation pending: {type(exc).__name__}: {exc}"))
            return False
        # Re-check next cycle against any fill received while the GET was pending.
        return False

    def _is_within_activation_bounds(self, order_price: Decimal, side: TradeType, order_type: OrderType) -> bool:
        """
        This method is responsible for checking if the close price is within the activation bounds to place the open
        order. If the activation bounds are not set, it returns True. This makes the executor more capital efficient.

        :param close_price: The close price to be checked.
        :return: True if the close price is within the activation bounds, False otherwise.
        """
        activation_bounds = self.config.activation_bounds
        mid_price = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        if activation_bounds:
            if order_type.is_limit_type():
                if side == TradeType.BUY:
                    return order_price >= mid_price * (1 - activation_bounds[0])
                else:
                    return order_price <= mid_price * (1 + activation_bounds[0])
            else:
                if side == TradeType.BUY:
                    min_price_to_buy = order_price * (1 - activation_bounds[0])
                    max_price_to_buy = order_price * (1 + activation_bounds[1])
                    return min_price_to_buy <= mid_price <= max_price_to_buy
                else:
                    min_price_to_sell = order_price * (1 - activation_bounds[1])
                    max_price_to_sell = order_price * (1 + activation_bounds[0])
                    return min_price_to_sell <= mid_price <= max_price_to_sell
        else:
            return True

    def place_open_order(self):
        """
        This method is responsible for placing the open order.

        :return: None
        """
        if self._resume_entry_sealed:
            return
        if self.config.entry_order_policy == "single_attempt":
            if self.is_closed or self._entry_submitted or self._entry_stop_reason is not None or self._open_order:
                return
            # Latch before calling the connector boundary: an exception must not cause a resubmission.
            self._entry_submitted = True
            self._entry_lifecycle["submission_timestamp"] = self._strategy.current_timestamp
        self._submit_open_order(self.entry_price)
        self.logger().debug(self._log_message(f"Placing open order order_id={self._open_order.order_id}"))

    def _submit_open_order(self, price: Decimal, amount: Optional[Decimal] = None) -> None:
        remaining = amount if amount is not None else max(
            Decimal("0"),
            self.config.amount - sum(
                (order.executed_amount_base for order in self._entry_orders()),
                Decimal("0"),
            ),
        )
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=self.config.triple_barrier_config.open_order_type,
            amount=remaining,
            price=price,
            side=self.config.side,
            position_action=PositionAction.OPEN,
        )
        self._open_order = TrackedOrder(order_id=order_id)

    def control_barriers(self):
        """
        This method is responsible for controlling the barriers. It controls the stop loss, take profit, time limit and
        trailing stop.

        :return: None
        """
        if self.status == RunnableStatus.RUNNING and self.open_filled_amount >= self.trading_rules.min_order_size \
                and self.open_filled_amount_quote >= self.trading_rules.min_notional_size:
            self.control_stop_loss()
            if self.status == RunnableStatus.RUNNING:
                self.control_trailing_stop()
            if self.status == RunnableStatus.RUNNING:
                self.control_take_profit()
        self.control_time_limit()

    def place_close_order_and_cancel_open_orders(self, close_type: CloseType, price: Decimal = Decimal("NaN")):
        """
        This method is responsible for placing the close order and canceling the open orders. If the difference between
        the open filled amount and the close filled amount is greater than the minimum order size, it places the close
        order. It also cancels the open orders.

        :param close_type: The type of the close order.
        :param price: The price to be used in the close order.
        :return: None
        """
        if self._resume_conflicting_fill is not None:
            return
        if self.config.entry_order_policy == "single_attempt" and close_type == CloseType.EXPIRED:
            # Defense in depth: an entry deadline is never an exit authorization.
            self._end_entry_acquisition("entry_deadline")
            return
        if self._exit_trigger_reason is None and close_type != CloseType.POSITION_HOLD:
            self._exit_trigger_reason = close_type.name
        self.close_type = close_type
        if self.close_timestamp is None:
            self.close_timestamp = self._strategy.current_timestamp
        self._status = RunnableStatus.SHUTTING_DOWN
        self.cancel_open_orders()
        if self._close_order or not self._entry_terminal_confirmed() or (
                self._take_profit_limit_order and not self._take_profit_limit_order.is_done):
            return
        if self._exit_complete():
            return  # Only sub-lot fee dust remains; the shutdown loop finalizes the executor.
        close_amount = self._close_order_amount()
        if (self._close_quantity.get("blocked_reason") == "owned_residual_below_exchange_minimum"
                and self.all_orders_completed()):
            self.early_stop(keep_position=True)
            return
        if close_amount > Decimal("0") and close_amount >= self.trading_rules.min_order_size and close_type != CloseType.POSITION_HOLD:
            order_type = self._close_order_type_for(close_type)
            if close_type == CloseType.TRAILING_STOP and self._trailing_requires_no_loss_floor():
                min_sell = self._min_no_loss_sell_price()
                current = self._current_trailing_reference_price()
                if min_sell is None or current is None or current < min_sell:
                    self.logger().warning(
                        self._log_message(
                            f"Trailing close blocked below fee floor "
                            f"min_sell={self._fmt_decimal(min_sell)} "
                            f"current_price={self._fmt_decimal(current)}"
                        )
                    )
                    self.early_stop(keep_position=True)
                    return
                if order_type.is_limit_type():
                    if not price.is_finite():
                        price = self._marketable_limit_close_price()
                    price = max(price, min_sell)
            elif order_type.is_limit_type() and not price.is_finite():
                price = self._marketable_limit_close_price()
            order_id = self.place_order(
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                order_type=order_type,
                amount=close_amount,
                price=price,
                side=self.close_order_side,
                position_action=PositionAction.CLOSE,
            )
            self._close_order = TrackedOrder(order_id=order_id)
            self._close_order_type = order_type
            if close_type == CloseType.TRAILING_STOP and order_type == OrderType.MARKET:
                self._trailing_limit_fallback_pending = False
            self.logger().debug(
                self._log_message(
                    f"Placing close order order_id={order_id} "
                    f"close_type={close_type.name} filled_amount={self.open_filled_amount}"
                )
            )
        self.close_type = close_type
        self.close_timestamp = self._strategy.current_timestamp
        self._status = RunnableStatus.SHUTTING_DOWN

    def cancel_open_orders(self):
        """
        This method is responsible for canceling the open orders.

        :return: None
        """
        if self._open_order and self._open_order.order and self._open_order.order.is_open:
            self.cancel_open_order()
        if self._take_profit_limit_order and self._take_profit_limit_order.order and self._take_profit_limit_order.order.is_open:
            self.cancel_take_profit()

    def control_stop_loss(self):
        """
        This method is responsible for controlling the stop loss. If the net pnl percentage is less than the stop loss
        percentage, it places the close order and cancels the open orders.

        :return: None
        """
        if self.config.triple_barrier_config.stop_loss:
            if self.net_pnl_pct <= -self.config.triple_barrier_config.stop_loss:
                self.logger().info(
                    self._log_message(
                        f"Stop loss triggered net_pnl={self.net_pnl_pct:.5f} "
                        f"threshold={self.config.triple_barrier_config.stop_loss:.5f}"
                    )
                )
                self.place_close_order_and_cancel_open_orders(close_type=CloseType.STOP_LOSS)

    def control_take_profit(self):
        """
        This method is responsible for controlling the take profit. If the net pnl percentage is greater than the take
        profit percentage, it places the close order and cancels the open orders. If the take profit order type is limit,
        it places the take profit limit order. If the amount of the take profit order is different than the total amount
        executed in the open order, it renews the take profit order (can happen with partial fills).

        :return: None
        """
        if self.config.triple_barrier_config.take_profit:
            if self.config.triple_barrier_config.take_profit_order_type.is_limit_type():
                is_within_activation_bounds = self._is_within_activation_bounds(
                    self.take_profit_price, self.close_order_side,
                    self.config.triple_barrier_config.take_profit_order_type)
                if not self._take_profit_limit_order:
                    if is_within_activation_bounds:
                        self.place_take_profit_limit_order()
                else:
                    if self._take_profit_limit_order.is_open and not self._take_profit_limit_order.is_filled and \
                            not is_within_activation_bounds:
                        self.cancel_take_profit()
            elif self.net_pnl_pct >= self.config.triple_barrier_config.take_profit:
                self.logger().info(
                    self._log_message(
                        f"Take profit triggered net_pnl={self.net_pnl_pct:.5f} "
                        f"threshold={self.config.triple_barrier_config.take_profit:.5f}"
                    )
                )
                self.place_close_order_and_cancel_open_orders(close_type=CloseType.TAKE_PROFIT)

    def control_time_limit(self):
        """
        This method is responsible for controlling the time limit. If the position is expired, it places the close order
        and cancels the open orders.

        :return: None
        """
        if self.is_expired:
            self.logger().info(self._log_message("Time limit triggered"))
            self.place_close_order_and_cancel_open_orders(close_type=CloseType.TIME_LIMIT)

    def place_take_profit_limit_order(self):
        """
        This method is responsible for placing the take profit limit order.

        :return: None
        """
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            amount=self.amount_to_close,
            price=self.take_profit_price,
            order_type=self.config.triple_barrier_config.take_profit_order_type,
            position_action=PositionAction.CLOSE,
            side=self.close_order_side,
        )
        self._take_profit_limit_order = TrackedOrder(order_id=order_id)
        self.logger().debug(self._log_message(f"Placing take profit order order_id={order_id}"))

    def renew_take_profit_order(self):
        """
        This method is responsible for renewing the take profit order.

        :return: None
        """
        self.cancel_take_profit()
        self.place_take_profit_limit_order()
        self.logger().debug(self._log_message("Renewing take profit order"))

    def cancel_take_profit(self):
        """
        This method is responsible for canceling the take profit order.

        :return: None
        """
        self._strategy.cancel(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_id=self._take_profit_limit_order.order_id
        )
        self.logger().debug(
            self._log_message(f"Removing take profit order order_id={self._take_profit_limit_order.order_id}")
        )

    def cancel_open_order(self):
        """
        This method is responsible for canceling the open order.

        :return: None
        """
        self._strategy.cancel(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_id=self._open_order.order_id
        )
        self.logger().debug(self._log_message(f"Removing open order order_id={self._open_order.order_id}"))

    def early_stop(self, keep_position: bool = False):
        """
        This method allows strategy to stop the executor early.

        :return: None
        """
        # Queued controller actions can arrive after the control loop terminated,
        # before the orchestrator archives this executor. Never reopen its state
        # or replace the terminal close reason when no control loop can drain it.
        if self.is_closed:
            return
        self.close_type = CloseType.POSITION_HOLD if keep_position else CloseType.EARLY_STOP
        self._status = RunnableStatus.SHUTTING_DOWN

    def update_tracked_orders_with_order_id(self, order_id: str):
        """
        This method is responsible for updating the tracked orders with the information from the InFlightOrder, using
        the order_id as a reference.

        :param order_id: The order_id to be used as a reference.
        :return: None
        """
        if self._resume_entry_sealed and self._open_order and order_id == self._open_order.order_id:
            return
        in_flight_order = self.get_in_flight_order(self.config.connector_name, order_id)
        if in_flight_order is not None:
            for order in self._unique_orders([*self._entry_orders(), *self._exit_orders()]):
                if order.order_id == order_id:
                    if order.order is not None and order.order.is_done and not in_flight_order.is_done:
                        # Keep the terminal receipt, but a lagging tracker may still carry a real late fill.
                        terminal = order.order
                        if (terminal.client_order_id != in_flight_order.client_order_id
                                or terminal.exchange_order_id != in_flight_order.exchange_order_id
                                or terminal.trading_pair != in_flight_order.trading_pair):
                            self._mark_fill_reconciliation_required("Conflicting tracker order identity")
                            continue
                        for trade_id, fill in in_flight_order.order_fills.items():
                            known = terminal.order_fills.get(trade_id)
                            if known is not None and known != fill:
                                self._mark_fill_reconciliation_required("Conflicting tracker fill")
                                break
                            if (fill.client_order_id != terminal.client_order_id
                                    or fill.exchange_order_id != terminal.exchange_order_id
                                    or fill.trading_pair != terminal.trading_pair):
                                self._mark_fill_reconciliation_required("Conflicting tracker fill identity")
                                break
                        else:
                            for trade_id, fill in in_flight_order.order_fills.items():
                                if trade_id not in terminal.order_fills:
                                    terminal.update_with_trade_update(fill)
                        continue
                    order.order = in_flight_order

    def _mark_fill_reconciliation_required(self, reason: str):
        self._resume_conflicting_fill = {"reason": reason}
        self._entry_lifecycle["state"] = "reconciliation_required"
        recovery = getattr(getattr(self._strategy, "executor_orchestrator", None), "recovery_required", None)
        if isinstance(recovery, dict):
            identities = recovery.setdefault(self.config.controller_id, [])
            if self.config.id not in identities:
                identities.append(self.config.id)
        self.logger().error(self._log_message(f"{reason}; execution requires reconciliation"))

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        """
        This method is responsible for processing the order created event. Here we will update the TrackedOrder with the
        order_id.
        """
        if self._resume_entry_sealed and self._open_order and event.order_id == self._open_order.order_id:
            return
        self.update_tracked_orders_with_order_id(event.order_id)

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        """
        This method is responsible for processing the order filled event. Here we will update the value of
        _total_executed_amount_backup, that can be used if the InFlightOrder
        is not available.
        """
        if self._resume_entry_sealed and self._open_order and event.order_id == self._open_order.order_id:
            known = self._open_order.order.order_fills.get(event.exchange_trade_id)
            if (known is None or event.trading_pair != self.config.trading_pair or event.trade_type != TradeType.BUY
                    or event.amount != known.fill_base_amount or event.price != known.fill_price
                    or event.trade_fee.to_json() != known.fee.to_json()
                    or event.exchange_order_id not in ("", self._open_order.order.exchange_order_id)):
                self._resume_conflicting_fill = {"trade_id": event.exchange_trade_id,
                                                 "amount": str(event.amount), "price": str(event.price)}
                self._entry_lifecycle["state"] = "reconciliation_required"
                recovery = getattr(getattr(self._strategy, "executor_orchestrator", None), "recovery_required", None)
                if isinstance(recovery, dict):
                    identities = recovery.setdefault(self.config.controller_id, [])
                    if self.config.id not in identities:
                        identities.append(self.config.id)
                self.logger().error(self._log_message("Sealed entry fill conflict; execution requires reconciliation"))
            return
        self.update_tracked_orders_with_order_id(event.order_id)

    def process_order_completed_event(self, _, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        """
        This method is responsible for processing the order completed event. Here we will check if the id is one of the
        tracked orders and update the state
        """
        if self._resume_entry_sealed and self._open_order and event.order_id == self._open_order.order_id:
            return
        self._total_executed_amount_backup += event.base_asset_amount
        self.update_tracked_orders_with_order_id(event.order_id)

        if self._take_profit_limit_order and self._take_profit_limit_order.order_id == event.order_id:
            self.close_type = CloseType.TAKE_PROFIT
            self._close_order = self._take_profit_limit_order
            self._status = RunnableStatus.SHUTTING_DOWN
        elif self._close_order and self._close_order.order_id == event.order_id:
            self._close_order_type = None
            self._trailing_limit_fallback_pending = False

    def process_order_canceled_event(self, _, market: ConnectorBase, event: OrderCancelledEvent):
        """
        This method is responsible for processing the order canceled event
        """
        if self._resume_entry_sealed and self._open_order and event.order_id == self._open_order.order_id:
            return
        self.update_tracked_orders_with_order_id(event.order_id)
        if self._close_order and event.order_id == self._close_order.order_id:
            self._failed_orders.append(self._close_order)
            self._completed_close_orders.append(self._close_order)
            self._close_order = None
            self._close_order_type = None
        if self._open_order and event.order_id == self._open_order.order_id:
            if self.config.entry_order_policy == "single_attempt":
                self._end_entry_acquisition("exchange_canceled")
                self._entry_lifecycle.update({
                    "cancel_event_timestamp": event.timestamp,
                    "cancel_event_observed_timestamp": self._strategy.current_timestamp,
                    "cancel_event_observed_wall_timestamp": time.time(),
                })
                # Keep the exact tracked object: terminal reconciliation and late fills
                # must update the same owner rather than enabling a replacement order.
                return
            self._failed_orders.append(self._open_order)
            self._completed_open_orders.append(self._open_order)
            self._open_order = None
        if self._take_profit_limit_order and event.order_id == self._take_profit_limit_order.order_id:
            self._failed_orders.append(self._take_profit_limit_order)
            self._completed_close_orders.append(self._take_profit_limit_order)
            self._take_profit_limit_order = None

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        """
        This method is responsible for processing the order failed event. Here we will add the InFlightOrder to the
        failed orders list.
        """
        if self._resume_entry_sealed and self._open_order and event.order_id == self._open_order.order_id:
            return
        self.update_tracked_orders_with_order_id(event.order_id)
        if self._open_order and event.order_id == self._open_order.order_id:
            if self.config.entry_order_policy == "single_attempt":
                rejected = getattr(self._open_order.order, 'terminal_reconciliation', None)
                if (isinstance(rejected, dict) and rejected.get('source') == 'okx_submission_rejected_v1'
                        and rejected.get('order_id') == event.order_id and rejected.get('filled_base') == '0'):
                    self._entry_reconciliation = copy.deepcopy(rejected)
                self._entry_failure_is_final = self._is_permanent_validation_failure(event) or self._entry_reconciliation is not None
                self._end_entry_acquisition("entry_rejected" if self._entry_failure_is_final else "entry_failure")
                self._entry_lifecycle["failure_event_timestamp"] = event.timestamp
                return
            self._failed_orders.append(self._open_order)
            self._completed_open_orders.append(self._open_order)
            self._open_order = None
            if self._open_order_entry_expired:
                self.logger().warning(self._log_message(
                    f"Expired entry failed order_id={event.order_id}; retaining ownership pending reconciliation"))
                return
            if self._is_permanent_validation_failure(event):
                self.logger().error(
                    self._log_message(
                        f"Open order failed permanently order_id={event.order_id}. "
                        f"error_type={getattr(event, 'error_type', 'unknown')} "
                        f"error_message={getattr(event, 'error_message', 'n/a')}"
                    )
                )
                if self.open_filled_amount > 0:
                    self.early_stop(keep_position=True)
                else:
                    self.close_type = CloseType.FAILED
                    self.stop()
                return
            self.logger().error(
                self._log_message(
                    f"Open order failed order_id={event.order_id}. "
                    f"Retrying {self._current_retries}/{self._max_retries}"
                )
            )
            self._current_retries += 1
        elif self._close_order and event.order_id == self._close_order.order_id:
            self._failed_orders.append(self._close_order)
            self._completed_close_orders.append(self._close_order)
            self._close_order = None
            if self.close_type == CloseType.TRAILING_STOP and self._close_order_type == OrderType.LIMIT:
                self._trailing_limit_fallback_pending = True
            self._close_order_type = None
            self.logger().error(
                self._log_message(
                    f"Close order failed order_id={event.order_id}. "
                    f"Retrying {self._current_retries}/{self._max_retries}"
                )
            )
            self._current_retries += 1
        elif self._take_profit_limit_order and event.order_id == self._take_profit_limit_order.order_id:
            self._failed_orders.append(self._take_profit_limit_order)
            self._completed_close_orders.append(self._take_profit_limit_order)
            self._take_profit_limit_order = None
            self.logger().error(
                self._log_message(
                    f"Take profit order failed order_id={event.order_id}. "
                    f"Retrying {self._current_retries}/{self._max_retries}"
                )
            )

    @staticmethod
    def _is_permanent_validation_failure(event: MarketOrderFailureEvent) -> bool:
        error_type = str(getattr(event, "error_type", "") or "").lower()
        error_message = str(getattr(event, "error_message", "") or "").lower()
        if not error_message:
            return False
        permanent_markers = (
            "minimum notional size",
            "minimum order size",
            "lower than minimum",
            "order value is lower than minimum",
        )
        if error_type and error_type != "valueerror":
            return False
        return any(marker in error_message for marker in permanent_markers)

    def get_custom_info(self) -> Dict:
        level_id = self.config.level_id
        role = level_id if level_id in {"scout", "runner"} else None
        trailing_snapshot = self._trailing_status_snapshot()
        # Sub-lot fee dust after a complete exit is written off: it is not remaining inventory.
        residual_dust = self._closed_residual_dust()
        remaining_base = self.amount_to_close - residual_dust
        custom_info = {
            "planned_position_resume": self._planned_resume_id,
            "planned_position_resume_compatibility_v1": copy.deepcopy(self._planned_resume_compatibility),
            "resume_entry_sealed": self._resume_entry_sealed,
            "resume_conflicting_fill": self._resume_conflicting_fill,
            RESTART_SUSPEND_KEY: copy.deepcopy(self._restart_suspended),
            RESTART_RESTORE_KEY: copy.deepcopy(self._restart_restored),
            "level_id": level_id,
            "role": role,
            "current_position_average_price": self.entry_price,
            "entry_price": self.entry_price,
            "side": self.config.side,
            "current_retries": self._current_retries,
            "max_retries": self._max_retries,
            "close_price": self.close_price,
            "current_market_price": trailing_snapshot.get("trailing_current_price"),
            "open_order_last_update": self._open_order.last_update_timestamp if self._open_order else None,
            "open_order_type": self.config.triple_barrier_config.open_order_type.name,
            "open_order_limit_max_age_seconds": self.config.triple_barrier_config.open_order_limit_max_age_seconds,
            "open_order_age_seconds": self._open_order_age_seconds(),
            "open_order_entry_expired": self._open_order_entry_expired,
            "open_order_expiry_reconciled": self._open_order_expiry_reconciled,
            "open_order_filled_amount_base": self.open_filled_amount,
            "open_order_filled_amount_quote": self.open_filled_amount_quote,
            "open_order_state": (
                None if self._open_order is None or self._open_order.order is None
                else self._open_order.order.current_state.name
            ),
            "order_ids": [order.order_id for order in self._unique_orders([*self._entry_orders(), *self._exit_orders()])],
            "close_order_filled_amount_base": self.close_filled_amount,
            "remaining_position_amount_base": remaining_base,
            "held_position_orders": self._held_position_orders,
            "entry_reconciliation": copy.deepcopy(self._entry_reconciliation),
            "entry_lifecycle": {"policy": self.config.entry_order_policy, **self._entry_lifecycle},
            "exit_trigger_reason": self._exit_trigger_reason,
            "close_quantity": self._close_quantity,
            # Execution receipts remain meaningful after accounting transfers to PositionHold.
            # Do not change scalar PnL/volume semantics: that would double-count the held ledger.
            "execution_receipt": {
                "entry_gross_base": sum((o.executed_amount_base for o in self._entry_orders()), Decimal("0")),
                "entry_gross_quote": sum((o.executed_amount_quote for o in self._entry_orders()), Decimal("0")),
                "exit_gross_base": self.close_filled_amount,
                "exit_gross_quote": self.close_filled_amount_quote,
                "fees_quote": self.cum_fees_quote,
                "remaining_owned_base": remaining_base,
                "residual_dust_base": residual_dust,
                "accounting_owner": "held_position" if self.close_type == CloseType.POSITION_HOLD else "executor",
            },
            "entry_order_snapshot": self._open_order.order.to_json()
            if self.config.entry_order_policy == "single_attempt" and self._open_order and self._open_order.order else None,
            "execution_order_snapshots": [o.order.to_json() for o in
                                          self._unique_orders([*self._entry_orders(), *self._exit_orders()])
                                          if o.order is not None]
            if self.config.entry_order_policy == "single_attempt" or self.config.require_terminal_entry_reconciliation else None,
        }
        custom_info.update(trailing_snapshot)
        return custom_info

    def to_format_status(self, scale=1.0):
        lines = []
        current_price = self.get_price(self.config.connector_name, self.config.trading_pair)
        amount_in_quote = self.entry_price * (self.open_filled_amount if self.open_filled_amount > Decimal("0") else self.config.amount)
        quote_asset = self.config.trading_pair.split("-")[1]
        if self.is_closed:
            lines.extend([f"""
| 🎯 Trading Pair: {self.config.trading_pair} | 🏦 Exchange: {self.config.connector_name} | ↔️ Side: {self.config.side}
| 📍 Entry price: {self.entry_price:.6f} | 🚪 Close price: {self.close_price:.6f} | 📦 Amount: {amount_in_quote:.4f} {quote_asset}
| 💰 Realized PNL: {self.trade_pnl_quote:.6f} {quote_asset} | 🧾 Total Fee: {self.cum_fees_quote:.6f} {quote_asset}
| 📊 PNL (%): {self.net_pnl_pct * 100:.2f}% | 💵 PNL (abs): {self.net_pnl_quote:.6f} {quote_asset} | 🏁 Close Type: {self.close_type}
"""])
        else:
            lines.extend([f"""
| 🎯 Trading Pair: {self.config.trading_pair} | 🏦 Exchange: {self.config.connector_name} | ↔️ Side: {self.config.side} |
| 📍 Entry price: {self.entry_price:.6f} | 🚪 Close price: {self.close_price:.6f} | 📦 Amount: {amount_in_quote:.4f} {quote_asset}
| 💹 Unrealized PNL: {self.trade_pnl_quote:.6f} {quote_asset} | 🧾 Total Fee: {self.cum_fees_quote:.6f} {quote_asset}
| 📊 PNL (%): {self.net_pnl_pct * 100:.2f}% | 💵 PNL (abs): {self.net_pnl_quote:.6f} {quote_asset} | 🏁 Close Type: {self.close_type}
        """])

        if self.is_trading:
            progress = 0
            if self.config.triple_barrier_config.time_limit:
                time_scale = int(scale * 60)
                seconds_remaining = (self.end_time - self._strategy.current_timestamp)
                time_progress = (self.config.triple_barrier_config.time_limit - seconds_remaining) / self.config.triple_barrier_config.time_limit
                time_bar = "".join(['*' if i < time_scale * time_progress else '-' for i in range(time_scale)])
                lines.extend([f"⏳ Time limit: {time_bar}"])

            if self.config.triple_barrier_config.take_profit and self.config.triple_barrier_config.stop_loss:
                price_scale = int(scale * 60)
                stop_loss_price = self.entry_price * (1 - self.config.triple_barrier_config.stop_loss) if self.config.side == TradeType.BUY \
                    else self.entry_price * (1 + self.config.triple_barrier_config.stop_loss)
                take_profit_price = self.entry_price * (1 + self.config.triple_barrier_config.take_profit) if self.config.side == TradeType.BUY \
                    else self.entry_price * (1 - self.config.triple_barrier_config.take_profit)
                if self.config.side == TradeType.BUY:
                    price_range = take_profit_price - stop_loss_price
                    progress = (current_price - stop_loss_price) / price_range
                elif self.config.side == TradeType.SELL:
                    price_range = stop_loss_price - take_profit_price
                    progress = (stop_loss_price - current_price) / price_range
                price_bar = [f'--{current_price:.5f}--' if i == int(price_scale * progress) else '-' for i in range(price_scale)]
                price_bar.insert(0, f"SL:{stop_loss_price:.5f}")
                price_bar.append(f"TP:{take_profit_price:.5f}")
                lines.extend(["".join(price_bar)])
            if self.config.triple_barrier_config.trailing_stop:
                trailing_snapshot = self._trailing_status_snapshot()
                trailing_state = trailing_snapshot.get("trailing_state")
                activation_pct = trailing_snapshot.get("trailing_activation_pct")
                activation_price = trailing_snapshot.get("trailing_activation_price")
                activation_gap = trailing_snapshot.get("distance_to_trailing_activation_pct")
                trigger_pct = trailing_snapshot.get("trailing_stop_trigger_pct")
                trigger_price = trailing_snapshot.get("trailing_trigger_price")
                trigger_gap = trailing_snapshot.get("distance_to_trailing_trigger_pct")
                move_count = trailing_snapshot.get("trailing_move_count", 0)
                lines.extend([
                    "🧵 Trailing stop: "
                    f"{trailing_state} | "
                    f"arm {self._fmt_decimal(activation_pct)} @ {self._fmt_decimal(activation_price)} | "
                    f"delta {self._fmt_decimal(trailing_snapshot.get('trailing_delta_pct'))}"
                ])
                if trailing_state in ("pending", "waiting"):
                    lines.extend([
                        f"🧵 Trailing distance to arm: {self._fmt_decimal(activation_gap * Decimal('100') if activation_gap is not None else None, 2)}%"
                    ])
                else:
                    lines.extend([
                        "🧵 Trailing trigger: "
                        f"{self._fmt_decimal(trigger_pct)} @ {self._fmt_decimal(trigger_price)} | "
                        f"buffer {self._fmt_decimal(trigger_gap * Decimal('100') if trigger_gap is not None else None, 2)}% | "
                        f"moves {move_count}"
                    ])
            lines.extend(["-----------------------------------------------------------------------------------------------------------"])
        return lines

    def control_trailing_stop(self):
        if self._resume_conflicting_fill is not None:
            return
        if self.config.triple_barrier_config.trailing_stop:
            net_pnl_pct = self.get_net_pnl_pct()
            if self._trailing_stop_trigger_pct is None:
                if net_pnl_pct > self.config.triple_barrier_config.trailing_stop.activation_price:
                    persisted = self._trailing_state()
                    self._trailing_stop_trigger_pct = self._floored_trailing_trigger_pct(
                        net_pnl_pct - self.config.triple_barrier_config.trailing_stop.trailing_delta
                    )
                    self._trailing_stop_move_count = 0
                    self._trailing_floor_hold = False
                    self._trailing_stop_armed_timestamp = self._strategy.current_timestamp
                    self._trailing_stop_last_update_timestamp = self._strategy.current_timestamp
                    if not self._checkpoint_trailing_state(persisted):
                        return
                    trailing_snapshot = self._trailing_status_snapshot()
                    self.logger().info(
                        self._log_message(
                            f"Trailing stop armed activation={self.config.triple_barrier_config.trailing_stop.activation_price:.5f} "
                            f"activation_price={self._fmt_decimal(trailing_snapshot.get('trailing_activation_price'))} "
                            f"delta={self.config.triple_barrier_config.trailing_stop.trailing_delta:.5f} "
                            f"trigger={self._trailing_stop_trigger_pct:.5f} "
                            f"trigger_price={self._fmt_decimal(trailing_snapshot.get('trailing_trigger_price'))} "
                            f"current_price={self._fmt_decimal(trailing_snapshot.get('trailing_current_price'))} "
                            f"net_pnl={net_pnl_pct:.5f}"
                        )
                    )
            else:
                if net_pnl_pct < self._trailing_stop_trigger_pct:
                    trailing_snapshot = self._trailing_status_snapshot()
                    if self._should_hold_trailing_close():
                        if not self._trailing_floor_hold:
                            self.logger().info(
                                self._log_message(
                                    f"Trailing stop hold below fee floor "
                                    f"trigger={self._trailing_stop_trigger_pct:.5f} "
                                    f"trigger_price={self._fmt_decimal(trailing_snapshot.get('trailing_trigger_price'))} "
                                    f"min_sell={self._fmt_decimal(self._min_no_loss_sell_price())} "
                                    f"current_price={self._fmt_decimal(trailing_snapshot.get('trailing_current_price'))} "
                                    f"net_pnl={net_pnl_pct:.5f}"
                                )
                            )
                            self._trailing_floor_hold = True
                    else:
                        self._trailing_floor_hold = False
                        self.logger().info(
                            self._log_message(
                                f"Trailing stop triggered trigger={self._trailing_stop_trigger_pct:.5f} "
                                f"trigger_price={self._fmt_decimal(trailing_snapshot.get('trailing_trigger_price'))} "
                                f"current_price={self._fmt_decimal(trailing_snapshot.get('trailing_current_price'))} "
                                f"net_pnl={net_pnl_pct:.5f}"
                            )
                        )
                        self.place_close_order_and_cancel_open_orders(close_type=CloseType.TRAILING_STOP)
                new_trigger = self._floored_trailing_trigger_pct(
                    net_pnl_pct - self.config.triple_barrier_config.trailing_stop.trailing_delta
                )
                if new_trigger > self._trailing_stop_trigger_pct:
                    persisted = self._trailing_state()
                    previous_trigger = self._trailing_stop_trigger_pct
                    previous_trigger_price = self._price_for_net_pnl_pct(previous_trigger)
                    self._trailing_stop_trigger_pct = new_trigger
                    self._trailing_stop_move_count += 1
                    self._trailing_stop_last_update_timestamp = self._strategy.current_timestamp
                    if not self._checkpoint_trailing_state(persisted):
                        return
                    trailing_snapshot = self._trailing_status_snapshot()
                    self.logger().info(
                        self._log_message(
                            f"Trailing stop moved trigger={previous_trigger:.5f}->{self._trailing_stop_trigger_pct:.5f} "
                            f"trigger_price={self._fmt_decimal(previous_trigger_price)}->{self._fmt_decimal(trailing_snapshot.get('trailing_trigger_price'))} "
                            f"current_price={self._fmt_decimal(trailing_snapshot.get('trailing_current_price'))} "
                            f"net_pnl={net_pnl_pct:.5f} moves={self._trailing_stop_move_count}"
                        )
                    )

    def _trailing_state(self) -> tuple:
        return (self._trailing_stop_trigger_pct, self._trailing_stop_move_count, self._trailing_floor_hold,
                self._trailing_stop_armed_timestamp, self._trailing_stop_last_update_timestamp)

    def _checkpoint_trailing_state(self, persisted: tuple) -> bool:
        """Commit a changed high-water before another tick can use it to authorize an exit.

        A failed write restores the last persisted trailing state, so only durable state can
        authorize an exit, and the next tick retries. It is not a fill conflict: latching one
        would stop this position's exits until a manual repair.
        """
        if self.open_filled_amount <= Decimal("0"):
            return True
        try:
            MarketsRecorder.get_instance().store_or_update_executor(self)
        except Exception as error:
            (self._trailing_stop_trigger_pct, self._trailing_stop_move_count, self._trailing_floor_hold,
             self._trailing_stop_armed_timestamp, self._trailing_stop_last_update_timestamp) = persisted
            failure = f"{type(error).__name__}: {error}"
            if getattr(self, "_trailing_checkpoint_failure", None) != failure:
                self.logger().error(self._log_message(
                    f"Trailing checkpoint failed: {failure}; keeping the persisted trailing state and retrying each tick"))
            self._trailing_checkpoint_failure = failure
            return False
        if getattr(self, "_trailing_checkpoint_failure", None) is not None:
            self.logger().info(self._log_message(
                f"Trailing checkpoint recovered after: {self._trailing_checkpoint_failure}"))
            self._trailing_checkpoint_failure = None
        return True

    async def validate_sufficient_balance(self):
        if self.is_perpetual:
            order_candidate = PerpetualOrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=self.config.triple_barrier_config.open_order_type.is_limit_type(),
                order_type=self.config.triple_barrier_config.open_order_type,
                order_side=self.config.side,
                amount=self.config.amount,
                price=self.entry_price,
                leverage=Decimal(self.config.leverage),
            )
        else:
            order_candidate = OrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=self.config.triple_barrier_config.open_order_type.is_limit_type(),
                order_type=self.config.triple_barrier_config.open_order_type,
                order_side=self.config.side,
                amount=self.config.amount,
                price=self.entry_price,
            )
        adjusted_order_candidates = self.adjust_order_candidates(self.config.connector_name, [order_candidate])
        if adjusted_order_candidates[0].amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error(self._log_message("Not enough budget to open position"))
            self.stop()

    async def _sleep(self, delay: float):
        """
        This method is responsible for sleeping the executor for a specific time.

        :param delay: The time to sleep.
        :return: None
        """
        await asyncio.sleep(delay)
