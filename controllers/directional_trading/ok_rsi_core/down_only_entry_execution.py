import asyncio
import json
from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator

from controllers.directional_trading.ok_rsi_core.down_only_entry import (
    DownOnlyMakerAction,
    DownOnlyMakerParameters,
    DownOnlyMakerSnapshot,
    decide_down_only_maker,
)
from controllers.directional_trading.ok_rsi_core.execution import actual_fees
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor


class DownOnlyMakerPositionExecutorConfig(PositionExecutorConfig):
    entry_acquisition_policy: Literal["down_only_maker_v1"] = "down_only_maker_v1"
    decision_best_bid: Decimal
    decision_best_ask: Decimal
    decision_atr_1m: Decimal
    decision_price_step: Decimal
    maximum_acceptable_price: Decimal
    entry_rest_seconds: int = Field(ge=1, le=300)
    entry_total_max_age_seconds: int = Field(ge=30, le=900)
    entry_max_downward_reprices: int = Field(ge=1, le=10)
    entry_reprice_cooldown_seconds: int = Field(ge=1, le=30)
    entry_reprice_min_ticks: int = Field(ge=1, le=100)
    entry_reprice_atr_fraction: Decimal = Field(gt=0, le=1)
    entry_max_downward_atr: Decimal = Field(gt=0, le=10)

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
            "entry_max_downward_atr": self.entry_max_downward_atr,
        }
        for name, value in positive_decimals.items():
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        if self.decision_best_bid > self.decision_best_ask:
            raise ValueError("down-only maker entry rejects a crossed decision book")
        if self.entry_price > self.maximum_acceptable_price:
            raise ValueError("entry_price exceeds the maximum acceptable price")
        if self.entry_total_max_age_seconds < self.entry_rest_seconds:
            raise ValueError("entry total age must not be shorter than the rest lease")
        if self.triple_barrier_config.open_order_limit_max_age_seconds != self.entry_rest_seconds:
            raise ValueError("entry rest lease must match the native open-order lease")
        return self


class DownOnlyMakerPositionExecutor(PositionExecutor):
    """Cancel/acknowledge/replace executor for one bounded maker-only BUY intent."""

    def __init__(self, strategy, config: DownOnlyMakerPositionExecutorConfig, *args, **kwargs):
        super().__init__(strategy, config, *args, **kwargs)
        self.config: DownOnlyMakerPositionExecutorConfig = config
        self._entry_intent_created_at = float(config.timestamp)
        self._entry_current_price = config.entry_price
        self._entry_order_created_at = float(config.timestamp)
        self._entry_last_reprice_at = None
        self._entry_reprice_count = 0
        self._entry_pending_target = None
        self._entry_cancel_reason = None
        self._entry_terminal_reason = None
        self._entry_child_submission_authorized = False
        self._entry_cancel_order_id = None
        self._entry_reprice_cancel_confirmed = False
        self._entry_last_event_signature = None

    @property
    def _down_only_parameters(self) -> DownOnlyMakerParameters:
        return DownOnlyMakerParameters(
            rest_seconds=self.config.entry_rest_seconds,
            total_max_age_seconds=self.config.entry_total_max_age_seconds,
            max_reprices=self.config.entry_max_downward_reprices,
            cooldown_seconds=self.config.entry_reprice_cooldown_seconds,
            min_ticks=self.config.entry_reprice_min_ticks,
            atr_fraction=self.config.entry_reprice_atr_fraction,
            max_downward_atr=self.config.entry_max_downward_atr,
        )

    def control_open_order(self):
        if self._resume_entry_sealed or self.is_closed:
            return
        if self._entry_terminal_reason == "reconciliation_required":
            return
        if self._entry_child_submission_authorized:
            self._submit_reprice_child()
            return

        tracked = self._open_order
        if tracked is None:
            if not self._entry_submitted and self._entry_stop_reason is None:
                self._entry_submitted = True
                self._entry_lifecycle["submission_timestamp"] = self._strategy.current_timestamp
                self._submit_open_order(self._entry_current_price)
                self._entry_order_created_at = float(self._strategy.current_timestamp)
                self._entry_lifecycle["state"] = "awaiting_order_ack"
            elif self._entry_terminal_reason is None:
                self._entry_lifecycle["state"] = "reconciliation_required"
            return

        self._entry_submitted = True
        self.update_tracked_orders_with_order_id(tracked.order_id)
        order = tracked.order
        if order is None:
            self._entry_lifecycle["state"] = "reconciliation_required"
            return
        if order.is_cancelled and self._entry_pending_target is not None:
            self._open_order_entry_expired = True
            self._entry_reprice_cancel_confirmed = True
            self._entry_lifecycle["state"] = "reconciliation_pending"
            return
        if self._entry_pending_target is not None:
            self._entry_lifecycle["state"] = "cancel_pending"
            return
        if order.is_failure:
            super()._control_single_attempt_entry()
            return
        if self._entry_terminal_reason is not None:
            super()._control_single_attempt_entry()
            return

        try:
            now = Decimal(str(self._strategy.current_timestamp))
            best_bid = Decimal(str(self.get_price(
                self.config.connector_name, self.config.trading_pair, PriceType.BestBid
            )))
            best_ask = Decimal(str(self.get_price(
                self.config.connector_name, self.config.trading_pair, PriceType.BestAsk
            )))
            decision = decide_down_only_maker(
                self._down_only_parameters,
                DownOnlyMakerSnapshot(
                    now=now,
                    intent_created_at=Decimal(str(self._entry_intent_created_at)),
                    order_created_at=Decimal(str(order.creation_timestamp or self._entry_order_created_at)),
                    last_reprice_at=(
                        Decimal(str(self._entry_last_reprice_at))
                        if self._entry_last_reprice_at is not None else None
                    ),
                    initial_best_bid=self.config.decision_best_bid,
                    initial_best_ask=self.config.decision_best_ask,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    current_order_price=self._entry_current_price,
                    decision_atr_1m=self.config.decision_atr_1m,
                    price_step=self.config.decision_price_step,
                    maximum_acceptable_price=self.config.maximum_acceptable_price,
                    authorized_base=self.config.amount,
                    cumulative_filled_base=sum(
                        (item.executed_amount_base for item in self._entry_orders()), Decimal("0")
                    ),
                    reprice_count=self._entry_reprice_count,
                    opposing_signal=False,
                    risk_allowed=True,
                ),
            )
        except Exception as exc:
            self.logger().error(self._log_message(
                f"Down-only entry inputs unavailable; canceling acquisition: {type(exc).__name__}: {exc}"
            ))
            self._request_terminal_cancel("input_unavailable")
            return

        state_before = self._entry_lifecycle.get("state")
        if decision.action is DownOnlyMakerAction.CANCEL_FOR_REPRICE:
            self._entry_pending_target = decision.target_price
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

    def early_stop(self, keep_position: bool = False):
        if keep_position and not self.is_closed:
            self._entry_pending_target = None
            self._entry_child_submission_authorized = False
            self._entry_terminal_reason = self._entry_terminal_reason or "external_invalidation"
            self._entry_cancel_reason = self._entry_cancel_reason or "external_invalidation"
            self._end_entry_acquisition(self._entry_terminal_reason)
        super().early_stop(keep_position=keep_position)

    def _request_current_cancel(self) -> None:
        if self._open_order is None:
            self._entry_lifecycle["state"] = "reconciliation_required"
            self._entry_terminal_reason = "reconciliation_required"
            self._entry_pending_target = None
            return
        self._entry_lifecycle.update({
            "state": "cancel_pending",
            "cancel_reason": self._entry_cancel_reason,
            "pending_target": str(self._entry_pending_target),
        })
        if self._entry_cancel_order_id == self._open_order.order_id:
            return
        self._entry_cancel_order_id = self._open_order.order_id
        self.cancel_open_order()

    def _request_terminal_cancel(self, reason: str) -> None:
        self._entry_pending_target = None
        self._entry_terminal_reason = reason
        self._entry_cancel_reason = reason
        self._end_entry_acquisition(reason)
        self._request_current_cancel()

    def process_order_canceled_event(self, _, market, event):
        tracked = self._open_order
        if tracked is not None and event.order_id == tracked.order_id and self._entry_pending_target is not None:
            self.update_tracked_orders_with_order_id(event.order_id)
            order = tracked.order
            if order is None:
                self._entry_pending_target = None
                self._entry_terminal_reason = "reconciliation_required"
                self._entry_lifecycle["state"] = "reconciliation_required"
                return
            order.current_state = OrderState.CANCELED
            self._open_order_entry_expired = True
            self._entry_reprice_cancel_confirmed = True
            self._entry_lifecycle.update({
                "state": "reconciliation_pending",
                "cancel_event_timestamp": event.timestamp,
                "cancel_event_observed_timestamp": self._strategy.current_timestamp,
            })
            return
        super().process_order_canceled_event(_, market, event)

    async def _reconcile_expired_entry(self):
        if self._entry_pending_target is None:
            await super()._reconcile_expired_entry()
            return
        if not self._entry_reprice_cancel_confirmed:
            return
        tracked = self._open_order
        if tracked is None or tracked.order is None or not tracked.order.is_cancelled:
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
        if self.open_filled_amount > Decimal("0"):
            self._entry_pending_target = None
            self._entry_terminal_reason = "partial_fill"
            self._entry_cancel_reason = "partial_fill"
            self._end_entry_acquisition("partial_fill")
            self._entry_lifecycle["state"] = "managing_fill"
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
        self._entry_pending_target = None
        self._entry_child_submission_authorized = False
        self._entry_terminal_reason = reason
        self._entry_lifecycle["state"] = "reconciliation_required"

    def _submit_reprice_child(self) -> None:
        target = self._entry_pending_target
        previous_price = self._entry_current_price
        self._entry_child_submission_authorized = False
        if (
            target is None
            or self.config.triple_barrier_config.open_order_type is not OrderType.LIMIT_MAKER
            or target >= previous_price
            or target > self.config.maximum_acceptable_price
            or self._open_order is not None
            or self._entry_reprice_count >= self.config.entry_max_downward_reprices
        ):
            self._block_replacement("reconciliation_required")
            return

        gross_filled = sum((order.executed_amount_base for order in self._entry_orders()), Decimal("0"))
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
            self._entry_pending_target = None
            self._entry_terminal_reason = "remaining_below_minimum"
            self._entry_lifecycle["state"] = "ended_no_fill"
            self.close_type = None
            self.stop()
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

        now = float(self._strategy.current_timestamp)
        self._entry_current_price = target
        self._entry_order_created_at = now
        self._entry_last_reprice_at = now
        self._entry_reprice_count += 1
        self._entry_pending_target = None
        self._entry_cancel_reason = None
        self._entry_cancel_order_id = None
        self._entry_lifecycle.update({
            "state": "awaiting_order_ack",
            "order_id": self._open_order.order_id,
            "reprice_count": self._entry_reprice_count,
        })

    def get_custom_info(self):
        custom_info = super().get_custom_info()
        cumulative_filled = self._cumulative_entry_filled_base()
        remaining = max(self.config.amount - cumulative_filled, Decimal("0"))
        fee_errors = self._entry_fee_errors()
        custom_info["entry_acquisition"] = {
            "policy": self.config.entry_acquisition_policy,
            "intent_id": self.config.id,
            "state": self._entry_lifecycle.get("state"),
            "terminal_reason": self._entry_terminal_reason,
            "initial_price": str(self.config.entry_price),
            "current_order_price": str(self._entry_current_price),
            "maximum_acceptable_price": str(self.config.maximum_acceptable_price),
            "decision_atr_1m": str(self.config.decision_atr_1m),
            "authorized_base": str(self.config.amount),
            "cumulative_filled_base": str(cumulative_filled),
            "remaining_base": str(remaining),
            "reprice_count": self._entry_reprice_count,
            "rest_seconds": self.config.entry_rest_seconds,
            "total_max_age_seconds": self.config.entry_total_max_age_seconds,
            "order_ids": [order.order_id for order in self._entry_orders()],
            "queue_fill_status": "UNAVAILABLE",
            "fee_economics_status": "UNAVAILABLE" if fee_errors else "AVAILABLE",
            "fee_errors": fee_errors,
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
        payload = {
            "action": decision.action.value,
            "authorized_base": str(self.config.amount),
            "best_ask": str(best_ask),
            "best_bid": str(best_bid),
            "candidate_price": str(decision.target_price) if decision.target_price is not None else None,
            "connector": self.config.connector_name,
            "controller": self.config.controller_id,
            "cooldown_seconds": self.config.entry_reprice_cooldown_seconds,
            "cumulative_filled_base": str(cumulative_filled),
            "current_order_price": str(self._entry_current_price),
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
        }
        self.logger().info(
            "ENTRY_ACQUISITION_DECISION " + json.dumps(payload, sort_keys=True, separators=(",", ":"))
        )
