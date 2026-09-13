from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from controllers.directional_trading.ok_rsi_core.down_only_entry import (
    DownOnlyMakerAction,
    DownOnlyMakerParameters,
    DownOnlyMakerSnapshot,
    decide_down_only_maker,
)


@dataclass
class _ShadowState:
    intent_id: str
    intent_created_at: Decimal
    order_created_at: Decimal
    last_reprice_at: Optional[Decimal]
    initial_best_bid: Decimal
    initial_best_ask: Decimal
    current_order_price: Decimal
    decision_atr_1m: Decimal
    price_step: Decimal
    maximum_acceptable_price: Decimal
    authorized_base: Decimal
    reprice_count: int = 0
    terminal_reason: Optional[str] = None


class DownOnlyMakerShadowTracker:
    """Virtual entry tracker that never receives a strategy or connector."""

    def __init__(self, parameters: DownOnlyMakerParameters):
        self._parameters = parameters
        self._state: Optional[_ShadowState] = None
        self._last_event: Optional[dict] = None
        self._event_count = 0

    @property
    def is_active(self) -> bool:
        return self._state is not None and self._state.terminal_reason is None

    def start(
        self,
        *,
        intent_id: str,
        now: Decimal,
        initial_price: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
        atr_1m: Decimal,
        price_step: Decimal,
        authorized_base: Decimal,
    ) -> None:
        if self.is_active:
            raise ValueError("an active shadow intent already exists")
        if not intent_id:
            raise ValueError("shadow intent_id must not be blank")
        candidate = _ShadowState(
            intent_id=intent_id,
            intent_created_at=now,
            order_created_at=now,
            last_reprice_at=None,
            initial_best_bid=best_bid,
            initial_best_ask=best_ask,
            current_order_price=initial_price,
            decision_atr_1m=atr_1m,
            price_step=price_step,
            maximum_acceptable_price=initial_price,
            authorized_base=authorized_base,
        )
        decide_down_only_maker(
            self._parameters,
            self._decision_snapshot(candidate, now, best_bid, best_ask, False, True),
        )
        self._state = candidate
        self._record_event(now=now, action="start", reason="legacy_buy_observed")

    def advance(
        self,
        *,
        now: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
        opposing_signal: bool,
        risk_allowed: bool,
    ) -> tuple[dict, ...]:
        state = self._state
        if state is None or state.terminal_reason is not None:
            return ()
        if best_ask <= state.current_order_price:
            state.terminal_reason = "touch_proxy"
            return (self._record_event(now=now, action="terminal", reason="touch_proxy"),)

        decision = decide_down_only_maker(
            self._parameters,
            self._decision_snapshot(state, now, best_bid, best_ask, opposing_signal, risk_allowed),
        )
        if decision.action is DownOnlyMakerAction.CANCEL_FOR_REPRICE:
            state.current_order_price = decision.target_price
            state.order_created_at = now
            state.last_reprice_at = now
            state.reprice_count += 1
            action = "replace_lower"
        elif decision.action is DownOnlyMakerAction.CANCEL_TERMINAL:
            state.terminal_reason = decision.reason.value
            action = "cancel_terminal"
        else:
            action = "hold"
        return (
            self._record_event(
                now=now,
                action=action,
                reason=decision.reason.value,
                target_price=decision.target_price,
            ),
        )

    def abandon_for_restart(self, *, now: Decimal) -> dict:
        if not self.is_active:
            raise ValueError("no active shadow intent to abandon")
        self._state.terminal_reason = "restart_abandoned"
        return self._record_event(now=now, action="cancel_terminal", reason="restart_abandoned")

    def terminate(self, *, now: Decimal, reason: str) -> dict:
        if not self.is_active:
            raise ValueError("no active shadow intent to terminate")
        self._state.terminal_reason = reason
        return self._record_event(now=now, action="cancel_terminal", reason=reason)

    def record_unavailable(self, *, now: Decimal, reason: str = "input_unavailable") -> dict:
        event = {
            "intent_id": None,
            "at": str(now),
            "action": "hold",
            "reason": reason,
            "current_order_price": None,
            "target_price": None,
            "reprice_count": 0,
            "queue_fill_status": "UNAVAILABLE",
            "cumulative_filled_base": "UNAVAILABLE",
        }
        self._last_event = event
        self._event_count += 1
        return event

    def snapshot(self) -> dict:
        state = self._state
        if state is None:
            return {
                "active": False,
                "queue_fill_status": "UNAVAILABLE",
                "cumulative_filled_base": "UNAVAILABLE",
                "event_count": self._event_count,
                "last_event": self._last_event,
            }
        return {
            "active": state.terminal_reason is None,
            "intent_id": state.intent_id,
            "intent_created_at": str(state.intent_created_at),
            "order_created_at": str(state.order_created_at),
            "last_reprice_at": str(state.last_reprice_at) if state.last_reprice_at is not None else None,
            "initial_best_bid": str(state.initial_best_bid),
            "initial_best_ask": str(state.initial_best_ask),
            "current_order_price": str(state.current_order_price),
            "decision_atr_1m": str(state.decision_atr_1m),
            "price_step": str(state.price_step),
            "maximum_acceptable_price": str(state.maximum_acceptable_price),
            "authorized_base": str(state.authorized_base),
            "cumulative_filled_base": "UNAVAILABLE",
            "queue_fill_status": "UNAVAILABLE",
            "reprice_count": state.reprice_count,
            "terminal_reason": state.terminal_reason,
            "event_count": self._event_count,
            "last_event": self._last_event,
        }

    @staticmethod
    def _decision_snapshot(
        state: _ShadowState,
        now: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
        opposing_signal: bool,
        risk_allowed: bool,
    ) -> DownOnlyMakerSnapshot:
        return DownOnlyMakerSnapshot(
            now=now,
            intent_created_at=state.intent_created_at,
            order_created_at=state.order_created_at,
            last_reprice_at=state.last_reprice_at,
            initial_best_bid=state.initial_best_bid,
            initial_best_ask=state.initial_best_ask,
            best_bid=best_bid,
            best_ask=best_ask,
            current_order_price=state.current_order_price,
            decision_atr_1m=state.decision_atr_1m,
            price_step=state.price_step,
            maximum_acceptable_price=state.maximum_acceptable_price,
            authorized_base=state.authorized_base,
            cumulative_filled_base=Decimal("0"),
            reprice_count=state.reprice_count,
            opposing_signal=opposing_signal,
            risk_allowed=risk_allowed,
        )

    def _record_event(
        self,
        *,
        now: Decimal,
        action: str,
        reason: str,
        target_price: Optional[Decimal] = None,
    ) -> dict:
        state = self._state
        event = {
            "intent_id": state.intent_id,
            "at": str(now),
            "action": action,
            "reason": reason,
            "current_order_price": str(state.current_order_price),
            "target_price": str(target_price) if target_price is not None else None,
            "reprice_count": state.reprice_count,
            "queue_fill_status": "UNAVAILABLE",
            "cumulative_filled_base": "UNAVAILABLE",
        }
        self._last_event = event
        self._event_count += 1
        return event
