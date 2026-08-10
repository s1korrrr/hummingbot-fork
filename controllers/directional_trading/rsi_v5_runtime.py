from __future__ import annotations

from dataclasses import dataclass, field, replace
from decimal import Decimal
from enum import Enum
from typing import Iterable, Optional
from uuid import uuid4

_ZERO = Decimal("0")
_POSITION_EXECUTOR_TYPES = {"position_executor", "rsi_position_executor"}


def _non_negative(value: Decimal) -> Decimal:
    return max(Decimal(str(value)), _ZERO)


@dataclass(frozen=True)
class ExecutorExposure:
    executor_id: str
    side: str
    executor_type: str
    active: bool
    configured_base: Decimal
    filled_base: Decimal
    filled_quote: Decimal
    reference_price: Decimal
    net_pnl_quote: Decimal = _ZERO
    done: bool = False

    @property
    def pending_base(self) -> Decimal:
        return _non_negative(self.configured_base - self.filled_base)

    def filled_notional(self, mark_price: Decimal) -> Decimal:
        if (
            not self.active
            or self.side != "BUY"
            or self.executor_type not in _POSITION_EXECUTOR_TYPES
        ):
            return _ZERO
        if self.filled_base > _ZERO and mark_price > _ZERO:
            return self.filled_base * mark_price
        return _non_negative(self.filled_quote)

    def pending_notional(self, mark_price: Decimal) -> Decimal:
        if (
            not self.active
            or self.side != "BUY"
            or self.executor_type not in _POSITION_EXECUTOR_TYPES
        ):
            return _ZERO
        conservative_price = max(
            _non_negative(mark_price),
            _non_negative(self.reference_price),
        )
        return self.pending_base * conservative_price


@dataclass(frozen=True)
class ExitPlan:
    executor_ids: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class ExposureSnapshot:
    held_notional: Decimal
    filled_notional: Decimal
    pending_notional: Decimal
    proposed_notional: Decimal

    @property
    def total_notional(self) -> Decimal:
        return (
            self.held_notional
            + self.filled_notional
            + self.pending_notional
            + self.proposed_notional
        )


@dataclass(frozen=True)
class SignedExposureSnapshot:
    held_long_base: Decimal
    held_short_base: Decimal
    owned_long_base: Decimal

    @property
    def net_base(self) -> Decimal:
        return self.held_long_base + self.owned_long_base - self.held_short_base

    @property
    def is_flat(self) -> bool:
        return (
            self.held_long_base <= _ZERO
            and self.held_short_base <= _ZERO
            and self.owned_long_base <= _ZERO
        )

    @property
    def has_unexpected_short(self) -> bool:
        return self.held_short_base > _ZERO

    @property
    def has_orphan_long(self) -> bool:
        return self.held_long_base > _ZERO and self.owned_long_base <= _ZERO


@dataclass(frozen=True)
class EpisodeOutcome:
    episode_id: str
    started_at: float
    ended_at: float
    executor_ids: tuple[str, ...]
    net_pnl_quote: Decimal
    total_planned_notional: Decimal


class SettlementStatus(str, Enum):
    PROVISIONAL = "PROVISIONAL"
    FINAL = "FINAL"


class SettlementMutation(str, Enum):
    INSERTED = "INSERTED"
    REPLACED = "REPLACED"
    IDEMPOTENT = "IDEMPOTENT"


class RuntimeHoldReason(str, Enum):
    ORPHAN_EXPOSURE = "orphan_exposure"
    UNEXPECTED_SHORT = "unexpected_short"
    SETTLEMENT_CONFLICT = "settlement_conflict"
    UNOWNED_SETTLEMENT = "unowned_settlement"


class SettlementConflict(RuntimeError):
    """Raised when an executor reports incompatible authoritative results."""


class SettlementRevisionError(SettlementConflict):
    """Raised when a provisional settlement revision does not advance."""


class UnknownExecutorSettlement(SettlementConflict):
    """Raised when settlement ownership cannot be tied to the open episode."""


@dataclass(frozen=True)
class ExecutorSettlement:
    executor_id: str
    status: SettlementStatus
    net_pnl_quote: Decimal
    revision: int

    def __post_init__(self) -> None:
        net_pnl_quote = Decimal(str(self.net_pnl_quote))
        if not net_pnl_quote.is_finite():
            raise ValueError("executor settlement PnL must be finite")
        revision = int(self.revision)
        if revision < 0:
            raise ValueError("executor settlement revision must be non-negative")
        object.__setattr__(self, "executor_id", str(self.executor_id))
        object.__setattr__(self, "status", SettlementStatus(self.status))
        object.__setattr__(self, "net_pnl_quote", net_pnl_quote)
        object.__setattr__(self, "revision", revision)


@dataclass
class _OpenEpisode:
    episode_id: str
    started_at: float
    planned_notional_by_executor: dict[str, Decimal] = field(default_factory=dict)
    settlements_by_executor: dict[str, ExecutorSettlement] = field(default_factory=dict)


class EpisodeTracker:
    def __init__(self) -> None:
        self._episode: Optional[_OpenEpisode] = None

    @property
    def is_open(self) -> bool:
        return self._episode is not None

    @property
    def episode_id(self) -> Optional[str]:
        return self._episode.episode_id if self._episode else None

    def open(self, timestamp: float, episode_id: Optional[str] = None) -> str:
        if self._episode is None:
            self._episode = _OpenEpisode(
                episode_id=episode_id or str(uuid4()),
                started_at=float(timestamp),
            )
        return self._episode.episode_id

    def register_executor(
        self,
        executor_id: str,
        planned_notional: Decimal,
    ) -> None:
        if self._episode is None:
            raise RuntimeError(
                "cannot register an executor without an open episode"
            )
        self._episode.planned_notional_by_executor.setdefault(
            str(executor_id),
            _non_negative(Decimal(str(planned_notional))),
        )

    def open_and_register_executor(
        self,
        *,
        timestamp: float,
        executor_id: str,
        planned_notional: Decimal,
        episode_id: Optional[str] = None,
    ) -> str:
        executor_id = str(executor_id)
        planned_notional = _non_negative(Decimal(str(planned_notional)))
        if self._episode is None:
            self._episode = _OpenEpisode(
                episode_id=episode_id or str(uuid4()),
                started_at=float(timestamp),
                planned_notional_by_executor={executor_id: planned_notional},
            )
        else:
            self.register_executor(executor_id, planned_notional)
        return self._episode.episode_id

    def settlement_for_executor(
        self,
        executor_id: str,
    ) -> Optional[ExecutorSettlement]:
        if self._episode is None:
            return None
        return self._episode.settlements_by_executor.get(str(executor_id))

    def record_executor_result(
        self,
        executor_id: str,
        net_pnl_quote: Decimal,
    ) -> bool:
        if self._episode is None:
            return False
        executor_id = str(executor_id)
        if executor_id not in self._episode.planned_notional_by_executor:
            return False
        existing = self._episode.settlements_by_executor.get(executor_id)
        self.record_executor_settlement(
            ExecutorSettlement(
                executor_id=executor_id,
                status=SettlementStatus.FINAL,
                net_pnl_quote=net_pnl_quote,
                revision=(existing.revision + 1) if existing else 1,
            )
        )
        return True

    def record_executor_settlement(
        self,
        settlement: ExecutorSettlement,
    ) -> SettlementMutation:
        episode = self._episode
        if episode is None:
            raise UnknownExecutorSettlement("cannot settle without an open episode")
        if settlement.executor_id not in episode.planned_notional_by_executor:
            raise UnknownExecutorSettlement(
                f"executor {settlement.executor_id!r} is not registered to the open episode"
            )

        existing = episode.settlements_by_executor.get(settlement.executor_id)
        if existing is None:
            episode.settlements_by_executor[settlement.executor_id] = settlement
            return SettlementMutation.INSERTED
        if existing == settlement:
            return SettlementMutation.IDEMPOTENT
        if existing.status is SettlementStatus.FINAL:
            if (
                settlement.status is SettlementStatus.FINAL
                and settlement.net_pnl_quote == existing.net_pnl_quote
            ):
                return SettlementMutation.IDEMPOTENT
            raise SettlementConflict(
                f"executor {settlement.executor_id!r} final settlement changed"
            )
        if settlement.revision <= existing.revision:
            raise SettlementRevisionError(
                f"executor {settlement.executor_id!r} settlement revision "
                f"{settlement.revision} did not advance beyond {existing.revision}"
            )

        episode.settlements_by_executor[settlement.executor_id] = settlement
        return SettlementMutation.REPLACED

    def close_if_flat(
        self,
        *,
        is_flat: bool,
        timestamp: float,
    ) -> Optional[EpisodeOutcome]:
        episode = self._episode
        if episode is None or not is_flat:
            return None
        registered_ids = set(episode.planned_notional_by_executor)
        if not registered_ids:
            return None
        if registered_ids != set(episode.settlements_by_executor):
            return None
        if any(
            settlement.status is not SettlementStatus.FINAL
            for settlement in episode.settlements_by_executor.values()
        ):
            return None
        outcome = EpisodeOutcome(
            episode_id=episode.episode_id,
            started_at=episode.started_at,
            ended_at=float(timestamp),
            executor_ids=tuple(sorted(registered_ids)),
            net_pnl_quote=sum(
                (
                    settlement.net_pnl_quote
                    for settlement in episode.settlements_by_executor.values()
                ),
                _ZERO,
            ),
            total_planned_notional=sum(
                episode.planned_notional_by_executor.values(),
                _ZERO,
            ),
        )
        self._episode = None
        return outcome


@dataclass(frozen=True)
class LifecycleEvent:
    event_id: str
    kind: str
    amount: Decimal = _ZERO

    def __post_init__(self) -> None:
        amount = Decimal(str(self.amount))
        if not amount.is_finite():
            raise ValueError("lifecycle event amount must be finite")
        object.__setattr__(self, "event_id", str(self.event_id))
        object.__setattr__(self, "kind", str(self.kind).upper())
        object.__setattr__(self, "amount", amount)


@dataclass(frozen=True)
class LifecycleState:
    executor_id: str
    events: tuple[LifecycleEvent, ...] = ()
    filled_base: Decimal = _ZERO
    close_filled_base: Decimal = _ZERO
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
    "CANCEL_ACK",
    "CLOSE_FILL",
    "FEE_ADJUSTMENT",
    "TIMEOUT",
    "FAILURE",
    "TERMINAL_CONFIRMATION",
}


def reduce_lifecycle_event(
    state: LifecycleState,
    event: LifecycleEvent,
) -> LifecycleState:
    """Reduce a callback into an order-independent executor lifecycle state."""
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
    filled_base = sum(
        (item.amount for item in events if item.kind == "ENTRY_FILL"),
        _ZERO,
    )
    close_filled_base = sum(
        (item.amount for item in events if item.kind == "CLOSE_FILL"),
        _ZERO,
    )
    fee_adjustment_quote = sum(
        (item.amount for item in events if item.kind == "FEE_ADJUSTMENT"),
        _ZERO,
    )
    reconciliation_required = bool(state.conflicting_event_ids) or any(
        item.kind not in _LIFECYCLE_EVENT_KINDS
        or item.kind in {"TIMEOUT", "FAILURE"}
        or (item.kind in {"ENTRY_FILL", "CLOSE_FILL"} and item.amount < _ZERO)
        for item in events
    )
    if close_filled_base > filled_base:
        reconciliation_required = True

    return LifecycleState(
        executor_id=state.executor_id,
        events=events,
        filled_base=filled_base,
        close_filled_base=close_filled_base,
        fee_adjustment_quote=fee_adjustment_quote,
        entry_cancelled=any(item.kind == "CANCEL_ACK" for item in events),
        terminal=any(item.kind == "TERMINAL_CONFIRMATION" for item in events),
        reconciliation_required=reconciliation_required,
        conflicting_event_ids=state.conflicting_event_ids,
    )


def build_exit_plan(
    executors: Iterable[ExecutorExposure],
    reason: str,
) -> ExitPlan:
    executor_ids = tuple(
        executor.executor_id
        for executor in executors
        if executor.active
        and executor.side == "BUY"
        and executor.executor_type in _POSITION_EXECUTOR_TYPES
    )
    return ExitPlan(
        executor_ids=executor_ids,
        reason=str(reason),
    )


def calculate_exposure(
    *,
    executors: Iterable[ExecutorExposure],
    mark_price: Decimal,
    held_notional: Decimal = _ZERO,
    proposed_notional: Decimal = _ZERO,
) -> ExposureSnapshot:
    executor_list = tuple(executors)
    return ExposureSnapshot(
        held_notional=_non_negative(Decimal(str(held_notional))),
        filled_notional=sum(
            (
                executor.filled_notional(Decimal(str(mark_price)))
                for executor in executor_list
            ),
            _ZERO,
        ),
        pending_notional=sum(
            (
                executor.pending_notional(Decimal(str(mark_price)))
                for executor in executor_list
            ),
            _ZERO,
        ),
        proposed_notional=_non_negative(
            Decimal(str(proposed_notional))
        ),
    )


def update_loss_streak(
    *,
    current_streak: int,
    outcome: Optional[EpisodeOutcome],
) -> int:
    if outcome is None:
        return max(0, int(current_streak))
    if outcome.net_pnl_quote < _ZERO:
        return max(0, int(current_streak)) + 1
    if outcome.net_pnl_quote > _ZERO:
        return 0
    return max(0, int(current_streak))
