from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Iterable

_ZERO = Decimal("0")


@dataclass(frozen=True)
class LifecycleEvent:
    event_id: str
    kind: str
    amount: Decimal = _ZERO
    quote_amount: Decimal = _ZERO

    def __post_init__(self) -> None:
        amount = Decimal(str(self.amount))
        quote_amount = Decimal(str(self.quote_amount))
        if not amount.is_finite() or not quote_amount.is_finite():
            raise ValueError("lifecycle event amounts must be finite")
        event_id = str(self.event_id).strip()
        if not event_id:
            raise ValueError("lifecycle event id is required")
        object.__setattr__(self, "event_id", event_id)
        object.__setattr__(self, "kind", str(self.kind).upper())
        object.__setattr__(self, "amount", amount)
        object.__setattr__(self, "quote_amount", quote_amount)


@dataclass(frozen=True)
class LifecycleState:
    executor_id: str
    events: tuple[LifecycleEvent, ...] = ()
    filled_base: Decimal = _ZERO
    filled_quote: Decimal = _ZERO
    close_filled_base: Decimal = _ZERO
    close_filled_quote: Decimal = _ZERO
    fee_adjustment_quote: Decimal = _ZERO
    entry_cancelled: bool = False
    terminal: bool = False
    reconciliation_required: bool = False
    conflicting_event_ids: tuple[str, ...] = ()

    @classmethod
    def initial(cls, executor_id: str) -> LifecycleState:
        return cls(executor_id=str(executor_id))


_LIFECYCLE_EVENT_KINDS = {
    "ENTRY_FILL",
    "CLOSE_FILL",
    "FEE_ADJUSTMENT",
    "CANCEL_ACK",
    "ENTRY_CANCEL_ACK",
    "CLOSE_CANCEL_ACK",
    "TAKE_PROFIT_CANCEL_ACK",
    "ENTRY_COMPLETED",
    "CLOSE_COMPLETED",
    "ENTRY_FAILURE",
    "CLOSE_FAILURE",
    "TAKE_PROFIT_FAILURE",
    "TIMEOUT",
    "FAILURE",
    "TERMINAL_CONFIRMATION",
}


def reduce_lifecycle_event(
    state: LifecycleState,
    event: LifecycleEvent,
) -> LifecycleState:
    """Reduce one connector fact without depending on callback arrival order."""
    existing_by_id = {existing.event_id: existing for existing in state.events}
    existing = existing_by_id.get(event.event_id)
    if existing == event:
        return state
    if existing is not None:
        conflicts = tuple(sorted(set(state.conflicting_event_ids) | {event.event_id}))
        return replace(
            state,
            reconciliation_required=True,
            conflicting_event_ids=conflicts,
        )

    existing_by_id[event.event_id] = event
    events = tuple(sorted(existing_by_id.values(), key=lambda item: item.event_id))
    filled_base = sum((item.amount for item in events if item.kind == "ENTRY_FILL"), _ZERO)
    filled_quote = sum((item.quote_amount for item in events if item.kind == "ENTRY_FILL"), _ZERO)
    close_filled_base = sum((item.amount for item in events if item.kind == "CLOSE_FILL"), _ZERO)
    close_filled_quote = sum((item.quote_amount for item in events if item.kind == "CLOSE_FILL"), _ZERO)
    fee_adjustment_quote = sum(
        (item.quote_amount or item.amount for item in events if item.kind == "FEE_ADJUSTMENT"),
        _ZERO,
    )
    reconciliation_required = bool(state.conflicting_event_ids) or any(
        item.kind not in _LIFECYCLE_EVENT_KINDS
        or item.kind in {"TIMEOUT", "FAILURE"}
        or (
            item.kind in {"ENTRY_FILL", "CLOSE_FILL"}
            and (item.amount < _ZERO or item.quote_amount < _ZERO)
        )
        for item in events
    )
    return LifecycleState(
        executor_id=state.executor_id,
        events=events,
        filled_base=filled_base,
        filled_quote=filled_quote,
        close_filled_base=close_filled_base,
        close_filled_quote=close_filled_quote,
        fee_adjustment_quote=fee_adjustment_quote,
        entry_cancelled=any(
            item.kind in {"CANCEL_ACK", "ENTRY_CANCEL_ACK"}
            for item in events
        ),
        terminal=any(
            item.kind in {"TERMINAL_CONFIRMATION", "CLOSE_COMPLETED"}
            for item in events
        ),
        reconciliation_required=reconciliation_required,
        conflicting_event_ids=state.conflicting_event_ids,
    )


def reduce_lifecycle_events(
    state: LifecycleState,
    events: Iterable[LifecycleEvent],
) -> LifecycleState:
    for event in events:
        state = reduce_lifecycle_event(state, event)
    return state
