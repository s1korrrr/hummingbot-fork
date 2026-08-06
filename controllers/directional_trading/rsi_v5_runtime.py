from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable, Optional
from uuid import uuid4

_ZERO = Decimal("0")


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
        if not self.active or self.side != "BUY" or self.executor_type != "position_executor":
            return _ZERO
        if self.filled_base > _ZERO and mark_price > _ZERO:
            return self.filled_base * mark_price
        return _non_negative(self.filled_quote)

    def pending_notional(self, mark_price: Decimal) -> Decimal:
        if not self.active or self.side != "BUY" or self.executor_type != "position_executor":
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
class EpisodeOutcome:
    episode_id: str
    started_at: float
    ended_at: float
    executor_ids: tuple[str, ...]
    net_pnl_quote: Decimal
    total_planned_notional: Decimal


@dataclass
class _OpenEpisode:
    episode_id: str
    started_at: float
    planned_notional_by_executor: dict[str, Decimal] = field(default_factory=dict)
    net_pnl_by_executor: dict[str, Decimal] = field(default_factory=dict)


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
        self._episode.net_pnl_by_executor.setdefault(
            executor_id,
            Decimal(str(net_pnl_quote)),
        )
        return True

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
        if registered_ids != set(episode.net_pnl_by_executor):
            return None
        outcome = EpisodeOutcome(
            episode_id=episode.episode_id,
            started_at=episode.started_at,
            ended_at=float(timestamp),
            executor_ids=tuple(sorted(registered_ids)),
            net_pnl_quote=sum(
                episode.net_pnl_by_executor.values(),
                _ZERO,
            ),
            total_planned_notional=sum(
                episode.planned_notional_by_executor.values(),
                _ZERO,
            ),
        )
        self._episode = None
        return outcome


def build_exit_plan(
    executors: Iterable[ExecutorExposure],
    reason: str,
) -> ExitPlan:
    executor_ids = tuple(
        executor.executor_id
        for executor in executors
        if executor.active
        and executor.side == "BUY"
        and executor.executor_type == "position_executor"
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
