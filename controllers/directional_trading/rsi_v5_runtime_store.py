from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from math import isfinite
from typing import Any, Iterable, Optional

from controllers.directional_trading.rsi_v5_runtime import (
    EpisodeOutcome,
    EpisodeTracker,
    ExecutorSettlement,
    RuntimeHoldReason,
    SettlementStatus,
    update_loss_streak,
)


class RSIV5RuntimeReplayError(RuntimeError):
    """Raised when persisted RSI runtime history is not self-consistent."""


def _canonical_json(payload: dict[str, Any]) -> str:
    try:
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"runtime event payload is not canonical JSON: {exc}") from exc


@dataclass(frozen=True)
class RSIV5RuntimeEvent:
    controller_id: str
    event_id: str
    episode_id: str
    event_type: str
    payload: dict[str, Any]
    created_at: float
    sequence: Optional[int] = None
    payload_json: str = field(init=False)

    def __post_init__(self) -> None:
        controller_id = str(self.controller_id).strip()
        event_id = str(self.event_id).strip()
        episode_id = str(self.episode_id).strip()
        event_type = str(self.event_type).strip().upper()
        if not controller_id or not event_id or not episode_id or not event_type:
            raise ValueError("runtime event identity fields must not be empty")
        created_at = float(self.created_at)
        if not isfinite(created_at):
            raise ValueError("runtime event created_at must be finite")
        sequence = None if self.sequence is None else int(self.sequence)
        if sequence is not None and sequence < 1:
            raise ValueError("runtime event sequence must be positive")
        payload_json = _canonical_json(dict(self.payload))
        object.__setattr__(self, "controller_id", controller_id)
        object.__setattr__(self, "event_id", event_id)
        object.__setattr__(self, "episode_id", episode_id)
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(self, "payload", json.loads(payload_json))
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "sequence", sequence)
        object.__setattr__(self, "payload_json", payload_json)

    def model_copy(self, *, update: Optional[dict[str, Any]] = None) -> RSIV5RuntimeEvent:
        values = {
            "controller_id": self.controller_id,
            "event_id": self.event_id,
            "episode_id": self.episode_id,
            "event_type": self.event_type,
            "payload": self.payload,
            "created_at": self.created_at,
            "sequence": self.sequence,
        }
        values.update(update or {})
        return type(self)(**values)

    def same_fact(self, other: RSIV5RuntimeEvent) -> bool:
        return (
            self.controller_id == other.controller_id
            and self.event_id == other.event_id
            and self.episode_id == other.episode_id
            and self.event_type == other.event_type
            and self.payload_json == other.payload_json
        )


@dataclass
class RSIV5RuntimeReplay:
    episode_tracker: EpisodeTracker = field(default_factory=EpisodeTracker)
    exit_request_timestamps: dict[str, float] = field(default_factory=dict)
    last_episode_outcome: Optional[EpisodeOutcome] = None
    consecutive_losses: int = 0
    last_loss_timestamp: Optional[float] = None
    runtime_holds: set[RuntimeHoldReason] = field(default_factory=set)
    historical_final_results: dict[str, Decimal] = field(default_factory=dict)


class RSIV5RuntimeStore:
    """Append-only RSI event store with deterministic replay semantics."""

    def __init__(self, recorder=None):
        self._recorder = recorder
        self._memory_events: list[RSIV5RuntimeEvent] = []

    @classmethod
    def in_memory(cls) -> RSIV5RuntimeStore:
        return cls(recorder=None)

    def append(self, event: RSIV5RuntimeEvent) -> RSIV5RuntimeEvent:
        return self.append_many([event])[0]

    def append_many(self, events: Iterable[RSIV5RuntimeEvent]) -> list[RSIV5RuntimeEvent]:
        events = list(events)
        if not events:
            return []
        controller_ids = {event.controller_id for event in events}
        if len(controller_ids) != 1:
            raise ValueError("one append batch cannot span controllers")
        if self._recorder is not None:
            rows = self._recorder.append_rsi_v5_runtime_events(
                [
                    {
                        "controller_id": event.controller_id,
                        "event_id": event.event_id,
                        "episode_id": event.episode_id,
                        "event_type": event.event_type,
                        "payload_json": event.payload_json,
                        "created_at": event.created_at,
                        "sequence": event.sequence,
                    }
                    for event in events
                ]
            )
            return [self._from_row(row) for row in rows]

        working_events = list(self._memory_events)
        appended: list[RSIV5RuntimeEvent] = []
        for event in events:
            existing = next(
                (item for item in working_events if item.event_id == event.event_id),
                None,
            )
            if existing is not None:
                if not existing.same_fact(event):
                    raise RSIV5RuntimeReplayError(
                        f"event_id {event.event_id!r} already records a different fact"
                    )
                appended.append(existing)
                continue
            next_sequence = 1 + max(
                (
                    item.sequence or 0
                    for item in working_events
                    if item.controller_id == event.controller_id
                ),
                default=0,
            )
            if event.sequence is not None and event.sequence != next_sequence:
                raise RSIV5RuntimeReplayError(
                    f"sequence {event.sequence} conflicts with next sequence {next_sequence}"
                )
            stored = event.model_copy(update={"sequence": next_sequence})
            working_events.append(stored)
            appended.append(stored)
        self._memory_events = working_events
        return appended

    def load(self, controller_id: str) -> list[RSIV5RuntimeEvent]:
        if self._recorder is not None:
            return [
                self._from_row(row)
                for row in self._recorder.get_rsi_v5_runtime_events(str(controller_id))
            ]
        return sorted(
            (
                event
                for event in self._memory_events
                if event.controller_id == str(controller_id)
            ),
            key=lambda event: event.sequence or 0,
        )

    @staticmethod
    def _from_row(row) -> RSIV5RuntimeEvent:
        return RSIV5RuntimeEvent(
            controller_id=row.controller_id,
            event_id=row.event_id,
            episode_id=row.episode_id,
            event_type=row.event_type,
            payload=json.loads(row.payload_json),
            created_at=row.created_at,
            sequence=row.sequence,
        )

    def replay(self, controller_id: str) -> RSIV5RuntimeReplay:
        replay = RSIV5RuntimeReplay()
        events = self.load(controller_id)
        expected_sequence = 1
        try:
            for event in events:
                if event.sequence != expected_sequence:
                    raise RSIV5RuntimeReplayError(
                        f"runtime sequence gap: expected {expected_sequence}, got {event.sequence}"
                    )
                expected_sequence += 1
                self._apply(replay, event)
        except RSIV5RuntimeReplayError:
            raise
        except Exception as exc:
            event_label = events[expected_sequence - 2].event_type if events else "UNKNOWN"
            raise RSIV5RuntimeReplayError(
                f"invalid {event_label} runtime event: {exc}"
            ) from exc
        return replay

    @staticmethod
    def _apply(replay: RSIV5RuntimeReplay, event: RSIV5RuntimeEvent) -> None:
        payload = event.payload
        tracker = replay.episode_tracker
        if event.event_type == "EPISODE_OPENED":
            if tracker.is_open:
                raise RSIV5RuntimeReplayError("EPISODE_OPENED while another episode is open")
            tracker.open(float(payload["started_at"]), episode_id=event.episode_id)
            return
        if event.event_type == "EXECUTOR_REGISTERED":
            if tracker.episode_id != event.episode_id:
                raise RSIV5RuntimeReplayError("EXECUTOR_REGISTERED outside its episode")
            tracker.register_executor(
                str(payload["executor_id"]),
                Decimal(str(payload["planned_notional"])),
            )
            return
        if event.event_type == "EXIT_REQUESTED":
            executor_id = str(payload["executor_id"])
            if tracker.settlement_for_executor(executor_id) is not None:
                raise RSIV5RuntimeReplayError("EXIT_REQUESTED after executor settlement")
            if tracker.episode_id != event.episode_id:
                raise RSIV5RuntimeReplayError("EXIT_REQUESTED outside its episode")
            # register_executor is deliberately not called: unknown ownership is corruption.
            if not tracker.owns_executor(executor_id):
                raise RSIV5RuntimeReplayError("EXIT_REQUESTED for an unowned executor")
            if not bool(payload.get("keep_position", False)):
                replay.exit_request_timestamps[executor_id] = float(payload["requested_at"])
            return
        if event.event_type == "EXECUTOR_SETTLED":
            if tracker.episode_id != event.episode_id:
                raise RSIV5RuntimeReplayError("EXECUTOR_SETTLED outside its episode")
            tracker.record_executor_settlement(
                ExecutorSettlement(
                    executor_id=str(payload["executor_id"]),
                    status=SettlementStatus(str(payload["status"])),
                    net_pnl_quote=Decimal(str(payload["net_pnl_quote"])),
                    revision=int(payload["revision"]),
                )
            )
            return
        if event.event_type == "EPISODE_CLOSED":
            if tracker.episode_id != event.episode_id:
                raise RSIV5RuntimeReplayError("EPISODE_CLOSED outside its episode")
            final_results = {
                executor_id: tracker.settlement_for_executor(executor_id)
                for executor_id in tracker.registered_executor_ids
            }
            outcome = tracker.close_if_flat(
                is_flat=True,
                timestamp=float(payload["ended_at"]),
            )
            if outcome is None:
                raise RSIV5RuntimeReplayError("EPISODE_CLOSED before final settlement")
            expected_ids = tuple(sorted(str(item) for item in payload["executor_ids"]))
            if (
                outcome.executor_ids != expected_ids
                or outcome.net_pnl_quote != Decimal(str(payload["net_pnl_quote"]))
                or outcome.total_planned_notional
                != Decimal(str(payload["total_planned_notional"]))
            ):
                raise RSIV5RuntimeReplayError("EPISODE_CLOSED outcome does not match ledger")
            replay.consecutive_losses = update_loss_streak(
                current_streak=replay.consecutive_losses,
                outcome=outcome,
            )
            if outcome.net_pnl_quote < Decimal("0"):
                replay.last_loss_timestamp = outcome.ended_at
            elif outcome.net_pnl_quote > Decimal("0"):
                replay.last_loss_timestamp = None
            replay.last_episode_outcome = outcome
            for executor_id in outcome.executor_ids:
                settlement = final_results.get(executor_id)
                if settlement is not None:
                    replay.historical_final_results[executor_id] = settlement.net_pnl_quote
            replay.exit_request_timestamps.clear()
            return
        if event.event_type == "RUNTIME_HOLD":
            replay.runtime_holds.add(RuntimeHoldReason(str(payload["reason"])))
            return
        raise RSIV5RuntimeReplayError(f"unknown runtime event type {event.event_type!r}")
