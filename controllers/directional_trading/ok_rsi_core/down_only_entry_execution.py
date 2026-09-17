import asyncio
import json
from decimal import ROUND_FLOOR, Decimal
from typing import Literal, Optional

from pydantic import Field, model_validator

from controllers.directional_trading.ok_rsi_core.down_only_entry import (
    DownOnlyMakerAction,
    DownOnlyMakerDecision,
    DownOnlyMakerParameters,
    DownOnlyMakerReason,
    DownOnlyMakerSnapshot,
    decide_down_only_maker,
)
from controllers.directional_trading.ok_rsi_core.execution import actual_fees
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType

OKX_POST_ONLY_CANCEL_SOURCE = "31"
OKX_STP_CANCEL_SOURCE = "32"
ENTRY_REPLACEMENT_DOWNWARD_REPRICE = "downward_reprice"
ENTRY_REPLACEMENT_POST_ONLY_REPAIR = "post_only_repair"


class DownOnlyMakerPositionExecutorConfig(PositionExecutorConfig):
    entry_acquisition_policy: Literal["down_only_maker_v1"] = "down_only_maker_v1"
    decision_best_bid: Decimal
    decision_best_ask: Decimal
    decision_atr_1m: Decimal
    decision_price_step: Decimal
    maximum_acceptable_price: Decimal
    entry_total_max_age_seconds: int = Field(ge=30, le=900)
    entry_max_downward_reprices: int = Field(ge=1, le=10)
    entry_reprice_cooldown_seconds: int = Field(ge=1, le=30)
    entry_reprice_min_ticks: int = Field(ge=1, le=100)
    entry_reprice_atr_fraction: Decimal = Field(gt=0, le=1)
    entry_activation_bound_up_pct: Decimal | None = None
    entry_upward_bound_hysteresis_seconds: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_down_only_maker_contract(self):
        if self.side is not TradeType.BUY:
            raise ValueError("down-only maker entry requires BUY side")
        if self.leverage != 1:
            raise ValueError("down-only maker entry requires leverage=1")
        if self.triple_barrier_config.open_order_type is not OrderType.LIMIT_MAKER:
            raise ValueError("down-only maker entry requires LIMIT_MAKER orders")
        if self.entry_order_policy != "single_attempt":
            raise ValueError("down-only maker entry requires single_attempt reconciliation")
        if self.entry_price is None:
            raise ValueError("down-only maker entry requires an entry_price")

        positive_decimals = {
            "entry_price": self.entry_price,
            "amount": self.amount,
            "decision_best_bid": self.decision_best_bid,
            "decision_best_ask": self.decision_best_ask,
            "decision_atr_1m": self.decision_atr_1m,
            "decision_price_step": self.decision_price_step,
            "maximum_acceptable_price": self.maximum_acceptable_price,
            "entry_reprice_atr_fraction": self.entry_reprice_atr_fraction,
        }
        for name, value in positive_decimals.items():
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        if self.decision_best_bid > self.decision_best_ask:
            raise ValueError("down-only maker entry rejects a crossed decision book")
        if self.decision_best_bid == self.decision_best_ask:
            raise ValueError("down-only maker entry rejects a crossed or locked decision book")
        if self.entry_price > self.maximum_acceptable_price:
            raise ValueError("entry_price exceeds the maximum acceptable price")
        if self.triple_barrier_config.open_order_limit_max_age_seconds != 0:
            raise ValueError("down-only maker entry requires a zero native open-order age")
        if self.activation_bounds is None or len(self.activation_bounds) != 1:
            raise ValueError("down-only maker entry requires exactly one activation_bounds value")
        activation_bound = self.activation_bounds[0]
        if not activation_bound.is_finite() or activation_bound <= 0 or activation_bound >= 1:
            raise ValueError("down-only maker activation bound must be finite and between 0 and 1")
        up_pct = self.entry_activation_bound_up_pct
        if up_pct is not None:
            if not up_pct.is_finite() or up_pct <= 0 or up_pct >= 1:
                raise ValueError("entry_activation_bound_up_pct must be finite and between 0 and 1")
            if up_pct < activation_bound:
                raise ValueError("entry_activation_bound_up_pct must be at least the downward activation bound")
        return self


class DownOnlyMakerPositionExecutor(PositionExecutor):
    """Cancel/acknowledge/replace executor for one bounded maker-only BUY intent."""

    def __init__(self, strategy, config: DownOnlyMakerPositionExecutorConfig, *args, **kwargs):
        super().__init__(strategy, config, *args, **kwargs)
        self.config: DownOnlyMakerPositionExecutorConfig = config
        self._entry_intent_created_at = min(
            float(config.timestamp),
            float(self._strategy.current_timestamp),
        )
        self._entry_current_price = config.entry_price
        self._entry_order_created_at = self._entry_intent_created_at
        self._entry_last_reprice_at = None
        self._entry_reprice_count = 0
        self._entry_pending_target = None
        self._entry_cancel_reason = None
        self._entry_child_submission_authorized = False
        self._entry_cancel_order_id = None
        self._entry_reprice_cancel_confirmed = False
        self._entry_last_event_signature = None
        self._entry_upward_bound_first_breached_at = None
        self._entry_replacement_of_order_id = None
        self._entry_replacement_purpose = None

    @property
    def _entry_terminal_reason(self) -> Optional[str]:
        return self._entry_stop_reason

    def _activation_bound_down_pct(self) -> Decimal:
        return self.config.activation_bounds[0]

    def _activation_bound_up_pct(self) -> Decimal:
        if self.config.entry_activation_bound_up_pct is None:
            return self._activation_bound_down_pct()
        return self.config.entry_activation_bound_up_pct

    @property
    def _down_only_parameters(self) -> DownOnlyMakerParameters:
        return DownOnlyMakerParameters(
            total_max_age_seconds=self.config.entry_total_max_age_seconds,
            max_reprices=self.config.entry_max_downward_reprices,
            cooldown_seconds=self.config.entry_reprice_cooldown_seconds,
            min_ticks=self.config.entry_reprice_min_ticks,
            atr_fraction=self.config.entry_reprice_atr_fraction,
            activation_bound_pct=self._activation_bound_down_pct(),
            activation_bound_up_pct=self._activation_bound_up_pct(),
            upward_hysteresis_seconds=self.config.entry_upward_bound_hysteresis_seconds,
        )

    def _acquisition_permitted(self) -> bool:
        return (
            self._entry_stop_reason is None
            and self.status == RunnableStatus.RUNNING
            and not self.is_closed
        )

    def _live_entry_permission(self) -> tuple[bool, bool]:
        controllers = getattr(self._strategy, "controllers", None)
        if not isinstance(controllers, dict):
            return False, False
        controller = controllers.get(self.config.controller_id)
        if controller is None:
            return False, False
        processed = getattr(controller, "processed_data", None) or {}
        opposing_signal = bool(processed.get("signal", 0) < 0)
        kill = bool(getattr(getattr(controller, "config", None), "manual_kill_switch", False))
        pause = bool(getattr(controller, "_operator_entry_paused", False))
        risk_allowed = not (kill or pause)
        status = getattr(controller, "status", None)
        if status != RunnableStatus.RUNNING:
            risk_allowed = False
        data_health = getattr(controller, "data_health", None)
        if data_health is None and isinstance(processed, dict):
            data_health = processed.get("data_health")
        if data_health is None:
            risk_allowed = False
        else:
            ready = data_health.get("ready") if isinstance(data_health, dict) else getattr(data_health, "ready", None)
            if not ready:
                risk_allowed = False
        return opposing_signal, risk_allowed

    def _revocation_or_deadline_reason(self) -> Optional[str]:
        opposing_signal, risk_allowed = self._live_entry_permission()
        if not risk_allowed:
            return "risk_blocked"
        if opposing_signal:
            return "opposing_signal"
        if self._intent_age_expired():
            return "total_age_expired"
        return None

    def _replacement_bound_to(self, order_id: str) -> bool:
        return (
            self._entry_replacement_of_order_id == order_id
            and self._entry_replacement_purpose in {
                ENTRY_REPLACEMENT_DOWNWARD_REPRICE,
                ENTRY_REPLACEMENT_POST_ONLY_REPAIR,
            }
        )

    @staticmethod
    def _is_confirmed_post_only_cancel(order) -> bool:
        return (
            order is not None
            and order.is_cancelled
            and getattr(order, "cancel_source", None) == OKX_POST_ONLY_CANCEL_SOURCE
        )

    def _order_awaits_exchange_update(self, order) -> bool:
        if order.exchange_order_id in (None, ""):
            return True
        return order.current_state in {
            OrderState.PENDING_CREATE,
            OrderState.PENDING_CANCEL,
        }

    def _order_is_live_for_price_decision(self, order) -> bool:
        if order.exchange_order_id in (None, ""):
            return False
        return order.current_state in {OrderState.OPEN, OrderState.PARTIALLY_FILLED}

    def control_open_order(self):
        if self._resume_entry_sealed or self.is_closed:
            return
        if self._entry_child_submission_authorized:
            self._submit_reprice_child()
            return

        tracked = self._open_order
        if tracked is None:
            if not self._entry_submitted and self._acquisition_permitted():
                self._submit_initial_maker()
            elif self._entry_submitted:
                # Losing the local handle is not evidence that submission failed.
                reason = self._revocation_or_deadline_reason()
                if reason is not None:
                    self._clear_replacement()
                    self._end_entry_acquisition(reason)
                self._entry_lifecycle["state"] = "reconciliation_required"
            return

        self._entry_submitted = True
        self.update_tracked_orders_with_order_id(tracked.order_id)
        order = tracked.order
        if order is None:
            # The client identity remains reserved while the receipt is unknown.
            reason = self._revocation_or_deadline_reason()
            if reason is not None:
                self._request_terminal_cancel(reason)
            else:
                self._entry_lifecycle["state"] = "awaiting_order_update"
            return
        if order.current_state == OrderState.FILLED:
            self._clear_replacement()
            self._cancel_entry_expiry_reconciliation()
            super()._control_single_attempt_entry()
            return
        if order.is_cancelled:
            if self._replacement_bound_to(tracked.order_id):
                self._open_order_entry_expired = True
                self._entry_reprice_cancel_confirmed = True
                if self._entry_lifecycle.get("state") != "reconciliation_required":
                    self._entry_lifecycle["state"] = "reconciliation_pending"
                return
            if self._cumulative_entry_filled_base() > Decimal("0"):
                self._entry_lifecycle["state"] = "managing_fill"
                return
            super()._control_single_attempt_entry()
            return
        if order.is_failure:
            super()._control_single_attempt_entry()
            return
        if self._entry_stop_reason is not None:
            super()._control_single_attempt_entry()
            return

        awaiting = self._order_awaits_exchange_update(order)
        live = self._order_is_live_for_price_decision(order)
        book = self._live_book()
        if awaiting or not live or book is None:
            reason = self._revocation_or_deadline_reason()
            if reason is not None:
                self._request_terminal_cancel(reason)
                return
            if self._entry_pending_target is not None:
                self._entry_lifecycle["state"] = "cancel_pending"
                self._request_current_cancel()
                return
            self._entry_lifecycle["state"] = (
                "cancel_pending" if order.is_pending_cancel_confirmation else "awaiting_order_update"
            )
            return

        try:
            decision, best_bid, best_ask, now = self._live_maker_decision(self._entry_current_price)
        except Exception as exc:
            self.logger().error(self._log_message(
                f"Down-only entry inputs unavailable; canceling acquisition: {type(exc).__name__}: {exc}"
            ))
            self._request_terminal_cancel("input_unavailable")
            return

        if best_bid is None or best_ask is None:
            if decision.action is DownOnlyMakerAction.CANCEL_TERMINAL:
                self._request_terminal_cancel(decision.reason.value)
            elif self._entry_pending_target is not None:
                self._request_current_cancel()
            else:
                self._entry_lifecycle["state"] = "awaiting_order_update"
            return

        if self._entry_pending_target is not None:
            self._entry_lifecycle["state"] = "cancel_pending"
            if decision.action is DownOnlyMakerAction.CANCEL_TERMINAL:
                self._request_terminal_cancel(decision.reason.value)
            else:
                self._request_current_cancel()
            self._emit_entry_decision(
                decision=decision,
                state_before="cancel_pending",
                best_bid=best_bid,
                best_ask=best_ask,
                now=now,
                order_id=tracked.order_id,
            )
            return

        state_before = self._entry_lifecycle.get("state")
        if decision.reason is DownOnlyMakerReason.UPWARD_BOUND_PENDING:
            if self._entry_upward_bound_first_breached_at is None:
                self._entry_upward_bound_first_breached_at = now
        elif decision.reason is not DownOnlyMakerReason.UPWARD_BOUND_BREACHED:
            self._entry_upward_bound_first_breached_at = None
        if decision.action is DownOnlyMakerAction.CANCEL_FOR_REPRICE:
            self._bind_replacement(
                tracked.order_id,
                ENTRY_REPLACEMENT_DOWNWARD_REPRICE,
                pending_target=decision.target_price,
            )
            self._entry_cancel_reason = decision.reason.value
            self._request_current_cancel()
        elif decision.action is DownOnlyMakerAction.CANCEL_TERMINAL:
            self._request_terminal_cancel(decision.reason.value)
        else:
            self._entry_lifecycle["state"] = decision.reason.value
        self._emit_entry_decision(
            decision=decision,
            state_before=state_before,
            best_bid=best_bid,
            best_ask=best_ask,
            now=now,
            order_id=tracked.order_id,
        )

    def _submit_initial_maker(self) -> None:
        if not self._acquisition_permitted():
            return
        price = self._maker_safe_price(self._entry_current_price)
        if price is None:
            reason = self._revocation_or_deadline_reason()
            if reason is not None:
                self._request_terminal_cancel(reason)
            else:
                self._entry_lifecycle["state"] = "awaiting_order_update"
            return
        if not self._intent_allows_price(price):
            return
        if not self._acquisition_permitted():
            return
        self._entry_submitted = True
        self._entry_lifecycle["submission_timestamp"] = self._strategy.current_timestamp
        self._submit_open_order(price)
        self._entry_current_price = price
        self._entry_order_created_at = float(self._strategy.current_timestamp)
        self._entry_last_reprice_at = self._entry_order_created_at
        self._entry_lifecycle["state"] = "awaiting_order_update"

    def early_stop(self, keep_position: bool = False):
        if not self.is_closed:
            self._clear_replacement()
            self._entry_child_submission_authorized = False
            reason = "external_invalidation" if keep_position else "early_stop"
            self._end_entry_acquisition(reason)
            if self._cumulative_entry_filled_base() > Decimal("0"):
                self._entry_lifecycle["state"] = "managing_fill"
        super().early_stop(keep_position=keep_position)

    def _bind_replacement(self, order_id: str, purpose: str, pending_target=None) -> None:
        if not self._acquisition_permitted():
            return
        self._entry_replacement_of_order_id = order_id
        self._entry_replacement_purpose = purpose
        if pending_target is not None:
            self._entry_pending_target = pending_target

    def _clear_replacement(self) -> None:
        self._entry_pending_target = None
        self._entry_replacement_of_order_id = None
        self._entry_replacement_purpose = None
        self._entry_child_submission_authorized = False
        self._entry_reprice_cancel_confirmed = False

    def _request_current_cancel(self) -> None:
        if self._open_order is None:
            self._entry_lifecycle["state"] = "reconciliation_required"
            return
        self._entry_lifecycle.update({
            "state": "cancel_pending",
            "cancel_reason": self._entry_cancel_reason,
            "pending_target": str(self._entry_pending_target) if self._entry_pending_target is not None else None,
            "replacement_purpose": self._entry_replacement_purpose,
            "replacement_of_order_id": self._entry_replacement_of_order_id,
        })
        now = self._strategy.current_timestamp
        same_order = self._entry_cancel_order_id == self._open_order.order_id
        last_attempt = self._entry_cancel_last_attempt_at
        if same_order and last_attempt is not None and now - last_attempt < 5:
            return
        self._entry_cancel_order_id = self._open_order.order_id
        self._entry_cancel_last_attempt_at = now
        self._entry_lifecycle.setdefault("cancel_requested_timestamp", now)
        self.cancel_open_order()

    def _request_terminal_cancel(self, reason: str) -> None:
        self._clear_replacement()
        self._entry_cancel_reason = reason
        self._end_entry_acquisition(reason)
        if self._open_order is None:
            if self._cumulative_entry_filled_base() > Decimal("0"):
                self._entry_lifecycle["state"] = "managing_fill"
                return
            self._entry_lifecycle["state"] = "ended_no_fill"
            if self.close_type is None:
                self.close_type = CloseType.EXPIRED
            self.stop()
            return
        self._request_current_cancel()

    def process_order_canceled_event(self, _, market, event):
        tracked = self._open_order
        if tracked is None or event.order_id != tracked.order_id:
            super().process_order_canceled_event(_, market, event)
            return
        self.update_tracked_orders_with_order_id(event.order_id)
        order = tracked.order
        if order is None:
            self._entry_lifecycle["state"] = "reconciliation_required"
            return
        order.current_state = OrderState.CANCELED
        cancel_source = getattr(order, "cancel_source", None)
        # A pending local reprice cannot override an explicit venue veto.
        # Source 1 also covers API cancellations; it is not automatic-repair authority.
        if cancel_source not in (None, "", "1", OKX_POST_ONLY_CANCEL_SOURCE):
            self._clear_replacement()
            super().process_order_canceled_event(_, market, event)
            return
        if self._replacement_bound_to(tracked.order_id):
            self._open_order_entry_expired = True
            self._entry_reprice_cancel_confirmed = True
            self._entry_lifecycle.update({
                "state": "reconciliation_pending",
                "cancel_event_timestamp": event.timestamp,
                "cancel_event_observed_timestamp": self._strategy.current_timestamp,
                "cancel_source": getattr(order, "cancel_source", None),
                "replacement_purpose": self._entry_replacement_purpose,
                "replacement_of_order_id": self._entry_replacement_of_order_id,
            })
            return
        if self._acquisition_permitted() and self._is_confirmed_post_only_cancel(order):
            self._bind_replacement(tracked.order_id, ENTRY_REPLACEMENT_POST_ONLY_REPAIR)
            self._open_order_entry_expired = True
            self._entry_reprice_cancel_confirmed = True
            self._entry_lifecycle.update({
                "state": "reconciliation_pending",
                "cancel_event_timestamp": event.timestamp,
                "cancel_event_observed_timestamp": self._strategy.current_timestamp,
                "cancel_source": OKX_POST_ONLY_CANCEL_SOURCE,
                "replacement_purpose": ENTRY_REPLACEMENT_POST_ONLY_REPAIR,
                "replacement_of_order_id": tracked.order_id,
            })
            return
        super().process_order_canceled_event(_, market, event)

    async def _reconcile_expired_entry(self):
        if self._open_order is not None and self._open_order.order is not None:
            if self._open_order.order.current_state == OrderState.FILLED:
                self.control_open_order()
                return
        if self._entry_replacement_purpose is None:
            await super()._reconcile_expired_entry()
            return
        if not self._entry_reprice_cancel_confirmed:
            return
        tracked = self._open_order
        if tracked is None or tracked.order is None or not tracked.order.is_cancelled:
            self._block_replacement("reconciliation_required")
            return
        if not self._replacement_bound_to(tracked.order_id):
            self._block_replacement("reconciliation_required")
            return
        task = self._entry_expiry_reconciliation_task
        if task is None:
            self._entry_expiry_reconciliation_task = asyncio.create_task(
                self._request_expired_entry_reconciliation([tracked])
            )
            return
        if not task.done():
            return
        self._entry_expiry_reconciliation_task = None
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            self.logger().error(self._log_message(
                f"Down-only cancel reconciliation failed: {type(exc).__name__}: {exc}"
            ))
            self._block_replacement("reconciliation_required")
            return

        self._open_order_expiry_reconciled = True
        if self._cumulative_entry_filled_base() > Decimal("0"):
            self._clear_replacement()
            self._end_entry_acquisition("partial_fill")
            self._entry_lifecycle["state"] = "managing_fill"
            return
        if not self._acquisition_permitted():
            self._entry_child_submission_authorized = False
            return

        if tracked not in self._completed_open_orders:
            self._completed_open_orders.append(tracked)
        self._open_order = None
        self._open_order_entry_expired = False
        self._open_order_expiry_reconciled = False
        self._entry_reprice_cancel_confirmed = False
        self._entry_child_submission_authorized = True
        self._entry_lifecycle["state"] = "child_authorized"

    def _block_replacement(self, reason: str) -> None:
        self._entry_child_submission_authorized = False
        self._entry_lifecycle["state"] = "reconciliation_required"
        self._entry_lifecycle["reconciliation_reason"] = reason

    def _submit_reprice_child(self) -> None:
        if not self._acquisition_permitted():
            self._entry_child_submission_authorized = False
            return
        purpose = self._entry_replacement_purpose
        previous_price = self._entry_current_price
        if (
            purpose not in {ENTRY_REPLACEMENT_DOWNWARD_REPRICE, ENTRY_REPLACEMENT_POST_ONLY_REPAIR}
            or self.config.triple_barrier_config.open_order_type is not OrderType.LIMIT_MAKER
            or self._open_order is not None
            or self._entry_reprice_count >= self.config.entry_max_downward_reprices
        ):
            self._entry_child_submission_authorized = False
            if purpose is not None and self._entry_reprice_count >= self.config.entry_max_downward_reprices:
                self._request_terminal_cancel("reprice_budget_exhausted")
            else:
                self._block_replacement("reconciliation_required")
            return

        gross_filled = sum((order.executed_amount_base for order in self._entry_orders()), Decimal("0"))
        if gross_filled > 0:
            self._open_order = self._completed_open_orders.pop()
            self._open_order_entry_expired = True
            self._open_order_expiry_reconciled = True
            self._clear_replacement()
            self._end_entry_acquisition("partial_fill")
            self._entry_lifecycle["state"] = "managing_fill"
            return

        if self._live_book() is None:
            reason = self._revocation_or_deadline_reason()
            if reason is not None:
                self._entry_child_submission_authorized = False
                self._request_terminal_cancel(reason)
            else:
                self._entry_lifecycle["state"] = "awaiting_order_update"
            return
        target = self._maker_safe_price(previous_price)
        if not self._intent_allows_price(target if target is not None else previous_price):
            if not self._acquisition_permitted():
                self._entry_child_submission_authorized = False
            return
        if (
            target is None
            or (purpose == ENTRY_REPLACEMENT_DOWNWARD_REPRICE and target >= previous_price)
            or (purpose == ENTRY_REPLACEMENT_POST_ONLY_REPAIR and target > previous_price)
        ):
            self._entry_lifecycle["state"] = "awaiting_order_update"
            return
        if not self._acquisition_permitted():
            self._entry_child_submission_authorized = False
            return

        remaining = self.config.amount - gross_filled
        try:
            remaining = self.connectors[self.config.connector_name].quantize_order_amount(
                trading_pair=self.config.trading_pair,
                amount=remaining,
            )
        except Exception:
            self._block_replacement("reconciliation_required")
            return
        if (
            remaining <= 0
            or remaining > self.config.amount - self.open_filled_amount
            or remaining < self.trading_rules.min_order_size
            or remaining * target < self.trading_rules.min_notional_size
        ):
            self._clear_replacement()
            self._end_entry_acquisition("remaining_below_minimum")
            self._entry_lifecycle["state"] = "ended_no_fill"
            self.close_type = None
            self.stop()
            return

        if not self._acquisition_permitted():
            self._entry_child_submission_authorized = False
            return
        now = float(self._strategy.current_timestamp)
        last = self._entry_last_reprice_at
        if last is not None and now - float(last) < float(self.config.entry_reprice_cooldown_seconds):
            self._entry_lifecycle["state"] = "awaiting_order_update"
            return
        try:
            self._submit_open_order(target, amount=remaining)
        except Exception as exc:
            self.logger().error(self._log_message(
                f"Down-only child submission failed without retry: {type(exc).__name__}: {exc}"
            ))
            self._block_replacement("reconciliation_required")
            return
        if self._open_order is None or not self._open_order.order_id:
            self._block_replacement("reconciliation_required")
            return

        self._entry_current_price = target
        self._entry_order_created_at = now
        self._entry_last_reprice_at = now
        self._entry_reprice_count += 1
        self._entry_child_submission_authorized = False
        self._entry_pending_target = None
        self._entry_replacement_of_order_id = None
        self._entry_replacement_purpose = None
        self._entry_cancel_reason = None
        self._entry_cancel_order_id = None
        self._entry_lifecycle.update({
            "state": "awaiting_order_update",
            "order_id": self._open_order.order_id,
            "reprice_count": self._entry_reprice_count,
            "replacement_purpose": purpose,
        })

    def _intent_age_expired(self) -> bool:
        try:
            now = Decimal(str(self._strategy.current_timestamp))
            created = Decimal(str(self._entry_intent_created_at))
            return now - created >= Decimal(self.config.entry_total_max_age_seconds)
        except Exception:
            return False

    def _tick_size(self) -> Optional[Decimal]:
        tick = self._to_decimal(self.trading_rules.min_price_increment)
        if tick is None or tick <= 0:
            return None
        return tick

    def _live_book(self) -> Optional[tuple[Decimal, Decimal]]:
        connector = self.connectors.get(self.config.connector_name)
        if connector is None or getattr(connector, "ready", None) is False:
            return None
        tracker = getattr(connector, "order_book_tracker", None)
        if tracker is not None and getattr(tracker, "ready", None) is False:
            return None
        provider = getattr(self._strategy, "market_data_provider", None)
        if provider is not None and getattr(provider, "ready", None) is False:
            return None
        controllers = getattr(self._strategy, "controllers", None)
        if isinstance(controllers, dict):
            controller = controllers.get(self.config.controller_id)
            controller_provider = getattr(controller, "market_data_provider", None) if controller is not None else None
            if controller_provider is not None and getattr(controller_provider, "ready", None) is False:
                return None
        try:
            bid = self._to_decimal(self.get_price(
                self.config.connector_name, self.config.trading_pair, PriceType.BestBid
            ))
            ask = self._to_decimal(self.get_price(
                self.config.connector_name, self.config.trading_pair, PriceType.BestAsk
            ))
        except (ArithmeticError, AttributeError, KeyError, RuntimeError, TypeError, ValueError):
            return None
        if bid is None or ask is None or bid <= 0 or ask <= 0 or bid >= ask:
            return None
        return bid, ask

    def _maker_safe_price(self, previous: Decimal) -> Optional[Decimal]:
        book = self._live_book()
        tick = self._tick_size()
        ceiling = self.config.maximum_acceptable_price
        if book is None or tick is None or previous is None:
            return None
        if not previous.is_finite() or previous <= 0 or not ceiling.is_finite() or ceiling <= 0:
            return None
        bid, ask = book
        raw = min(previous, bid, ask - tick, ceiling)
        if raw <= 0:
            return None
        price = (raw / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
        if price <= 0 or price >= ask or price > ceiling:
            return None
        return price

    def _live_maker_decision(self, current_order_price: Decimal, *, use_intent_clock: bool = False):
        now = Decimal(str(self._strategy.current_timestamp))
        opposing_signal, risk_allowed = self._live_entry_permission()
        book = self._live_book()
        if book is None:
            if not risk_allowed:
                decision = DownOnlyMakerDecision(DownOnlyMakerAction.CANCEL_TERMINAL, DownOnlyMakerReason.RISK_BLOCKED)
            elif opposing_signal:
                decision = DownOnlyMakerDecision(DownOnlyMakerAction.CANCEL_TERMINAL, DownOnlyMakerReason.OPPOSING_SIGNAL)
            elif self._intent_age_expired():
                decision = DownOnlyMakerDecision(DownOnlyMakerAction.CANCEL_TERMINAL, DownOnlyMakerReason.TOTAL_AGE_EXPIRED)
            else:
                decision = DownOnlyMakerDecision(DownOnlyMakerAction.HOLD, DownOnlyMakerReason.RESTING)
            return decision, None, None, now
        best_bid, best_ask = book
        order_created_at = (
            Decimal(str(self._entry_intent_created_at))
            if use_intent_clock
            else Decimal(str(self._entry_order_created_at))
        )
        decision = decide_down_only_maker(
            self._down_only_parameters,
            DownOnlyMakerSnapshot(
                now=now,
                intent_created_at=Decimal(str(self._entry_intent_created_at)),
                order_created_at=order_created_at,
                last_reprice_at=(
                    None if use_intent_clock else (
                        Decimal(str(self._entry_last_reprice_at))
                        if self._entry_last_reprice_at is not None else None
                    )
                ),
                initial_best_bid=self.config.decision_best_bid,
                initial_best_ask=self.config.decision_best_ask,
                best_bid=best_bid,
                best_ask=best_ask,
                current_order_price=current_order_price,
                decision_atr_1m=self.config.decision_atr_1m,
                price_step=self.config.decision_price_step,
                maximum_acceptable_price=self.config.maximum_acceptable_price,
                authorized_base=self.config.amount,
                cumulative_filled_base=self._cumulative_entry_filled_base(),
                reprice_count=self._entry_reprice_count,
                opposing_signal=opposing_signal,
                risk_allowed=risk_allowed,
                upward_bound_first_breached_at=self._entry_upward_bound_first_breached_at,
            ),
        )
        return decision, best_bid, best_ask, now

    def _intent_allows_price(self, target: Decimal) -> bool:
        try:
            decision, best_bid, best_ask, now = self._live_maker_decision(target, use_intent_clock=True)
        except Exception as exc:
            self.logger().error(self._log_message(f"Entry submission inputs unavailable: {type(exc).__name__}: {exc}"))
            self._request_terminal_cancel("input_unavailable")
            return False
        if decision.reason is DownOnlyMakerReason.UPWARD_BOUND_PENDING:
            if self._entry_upward_bound_first_breached_at is None:
                self._entry_upward_bound_first_breached_at = now
        elif decision.reason is not DownOnlyMakerReason.UPWARD_BOUND_BREACHED:
            self._entry_upward_bound_first_breached_at = None
        if decision.action is DownOnlyMakerAction.CANCEL_TERMINAL:
            self._request_terminal_cancel(decision.reason.value)
            return False
        # A non-terminal HOLD is not submission permission. Recheck the latest
        # observed ask; wait and recompute without consuming a new intent.
        if best_bid is None or best_ask is None or target >= best_ask:
            self._entry_lifecycle["state"] = "awaiting_order_update"
            return False
        return True

    def get_custom_info(self):
        custom_info = super().get_custom_info()
        cumulative_filled = self._cumulative_entry_filled_base()
        remaining = max(self.config.amount - cumulative_filled, Decimal("0"))
        fee_errors = self._entry_fee_errors()
        open_native = self._open_order.order if self._open_order is not None else None
        custom_info["entry_acquisition"] = {
            "policy": self.config.entry_acquisition_policy,
            "intent_id": self.config.id,
            "state": self._entry_lifecycle.get("state"),
            "terminal_reason": self._entry_stop_reason,
            "initial_price": str(self.config.entry_price),
            "current_order_price": str(self._entry_current_price),
            "maximum_acceptable_price": str(self.config.maximum_acceptable_price),
            "decision_atr_1m": str(self.config.decision_atr_1m),
            "authorized_base": str(self.config.amount),
            "cumulative_filled_base": str(cumulative_filled),
            "remaining_base": str(remaining),
            "reprice_count": self._entry_reprice_count,
            "activation_bound_pct": str(self._activation_bound_down_pct()),
            "activation_bound_up_pct": str(self._activation_bound_up_pct()),
            "activation_reference": "book_midpoint",
            "total_max_age_seconds": self.config.entry_total_max_age_seconds,
            "order_ids": [tracked.order_id for tracked in self._entry_orders()],
            "queue_fill_status": "UNAVAILABLE",
            "fee_economics_status": "UNAVAILABLE" if fee_errors else "AVAILABLE",
            "fee_errors": fee_errors,
            "native_order_state": open_native.current_state.name if open_native is not None else None,
            "cancel_source": getattr(open_native, "cancel_source", None) if open_native is not None else None,
            "replacement_purpose": self._entry_replacement_purpose,
            "replacement_of_order_id": self._entry_replacement_of_order_id,
        }
        return custom_info

    def _cumulative_entry_filled_base(self) -> Decimal:
        return sum((order.executed_amount_base for order in self._entry_orders()), Decimal("0"))

    def _entry_fee_errors(self) -> list[str]:
        errors = []
        for tracked in self._entry_orders():
            if tracked.order is not None:
                _, _, order_errors = actual_fees(tracked.order)
                errors.extend(order_errors)
        return sorted(set(errors))

    def _emit_entry_decision(self, *, decision, state_before, best_bid, best_ask, now, order_id) -> None:
        state_after = self._entry_lifecycle.get("state")
        signature = (
            decision.action.value,
            decision.reason.value,
            state_after,
            str(decision.target_price),
            order_id,
            self._entry_reprice_count,
        )
        if signature == self._entry_last_event_signature:
            return
        self._entry_last_event_signature = signature
        cumulative_filled = self._cumulative_entry_filled_base()
        remaining = max(self.config.amount - cumulative_filled, Decimal("0"))
        order = self._open_order.order if self._open_order is not None else None
        payload = {
            "action": decision.action.value,
            "authorized_base": str(self.config.amount),
            "best_ask": str(best_ask),
            "best_bid": str(best_bid),
            "mid_price": str((best_bid + best_ask) / Decimal("2")),
            "activation_reference": "book_midpoint",
            "candidate_price": str(decision.target_price) if decision.target_price is not None else None,
            "connector": self.config.connector_name,
            "controller": self.config.controller_id,
            "cooldown_seconds": self.config.entry_reprice_cooldown_seconds,
            "cumulative_filled_base": str(cumulative_filled),
            "current_order_price": str(self._entry_current_price),
            "activation_bound_pct": str(self._activation_bound_down_pct()),
            "activation_bound_up_pct": str(self._activation_bound_up_pct()),
            "activation_lower_price": str(
                self.config.maximum_acceptable_price * (Decimal("1") - self._activation_bound_down_pct())
            ),
            "activation_upper_price": str(
                self.config.maximum_acceptable_price * (Decimal("1") + self._activation_bound_up_pct())
            ),
            "upward_hysteresis_seconds": self.config.entry_upward_bound_hysteresis_seconds,
            "decision_atr_1m": str(self.config.decision_atr_1m),
            "intent_age_seconds": str(now - Decimal(str(self._entry_intent_created_at))),
            "intent_id": self.config.id,
            "order_age_seconds": str(now - Decimal(str(self._entry_order_created_at))),
            "order_id": order_id,
            "queue_fill_status": "UNAVAILABLE",
            "reason": decision.reason.value,
            "remaining_base": str(remaining),
            "reprice_count": self._entry_reprice_count,
            "spread": str(best_ask - best_bid),
            "state_after": state_after,
            "state_before": state_before,
            "trading_pair": self.config.trading_pair,
            "native_order_state": order.current_state.name if order is not None else None,
            "stop_reason": self._entry_stop_reason,
            "cancel_source": getattr(order, "cancel_source", None) if order is not None else None,
            "replacement_purpose": self._entry_replacement_purpose,
            "replacement_of_order_id": self._entry_replacement_of_order_id,
        }
        self.logger().info(
            "ENTRY_ACQUISITION_DECISION " + json.dumps(payload, sort_keys=True, separators=(",", ":"))
        )
