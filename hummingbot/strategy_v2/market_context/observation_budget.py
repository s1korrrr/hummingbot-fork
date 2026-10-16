"""Optional pre-commit acquisition filter for a separately bound native owner.

The proxy can refuse a new reservation before the native owner writes its
claim. Every other budget operation remains with the native BudgetStore.
"""

import json
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
from typing import Callable

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from .observation_entry import EntryObservation


@dataclass(frozen=True)
class NativeGateState:
    proposal_id: str
    trading_pair: str
    first_seen_ms: int
    decision_candle_ms: int
    deadline_ms: int
    native_deadline_ms: int
    last_clock_ms: int
    phase: str
    source_epoch: str | None = None
    observation_sha256: str | None = None
    observation_expires_at_ms: int | None = None

    def __post_init__(self):
        if (
            len(self.proposal_id) != 64
            or any(x not in "0123456789abcdef" for x in self.proposal_id)
            or not self.trading_pair.endswith("-USDC")
            or not 0 <= self.first_seen_ms <= self.last_clock_ms
            or not 0 <= self.decision_candle_ms <= self.first_seen_ms
            or not self.first_seen_ms <= self.deadline_ms <= self.native_deadline_ms
            or self.phase not in {"WAIT", "QUEUED", "DISPATCHED", "EXPIRED"}
        ):
            raise ValueError("invalid native observation gate state")


class ObservationGateStore:
    """CAS checkpoint in the recorder Metadata table, scoped by native owner key."""

    def __init__(self, recorder, native_key: str, *, metadata_model=None):
        if metadata_model is None:
            from hummingbot.model.metadata import Metadata

            metadata_model = Metadata
        self.recorder = recorder
        self.model = metadata_model
        self.key = native_key + ":observations-v1"

    def load(self):
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                row = session.get(self.model, self.key)
                raw = None if row is None else row.value
        except SQLAlchemyError as exc:
            raise RuntimeError("observation checkpoint unavailable") from exc
        if raw is None:
            return {}, 0, None
        try:
            data = json.loads(raw)
            if (
                not isinstance(data, dict)
                or set(data) != {"schema_version", "max_clock_ms", "states"}
                or type(data["schema_version"]) is not int
                or data["schema_version"] != 1
                or type(data["max_clock_ms"]) is not int
                or data["max_clock_ms"] < 0
                or not isinstance(data["states"], dict)
                or len(data["states"]) > 10_000
            ):
                raise ValueError("invalid checkpoint envelope")
            states = {key: NativeGateState(**value) for key, value in data["states"].items()}
            if any(
                key != value.proposal_id or value.last_clock_ms > data["max_clock_ms"] for key, value in states.items()
            ):
                raise ValueError("invalid checkpoint state identity")
            return states, data["max_clock_ms"], raw
        except (TypeError, ValueError) as exc:
            raise RuntimeError("invalid observation checkpoint") from exc

    def compare_and_swap(self, expected, states: dict[str, NativeGateState], max_clock_ms: int) -> bool:
        payload = json.dumps(
            {
                "schema_version": 1,
                "max_clock_ms": max_clock_ms,
                "states": {key: asdict(value) for key, value in sorted(states.items())},
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                with session.begin():
                    if expected is None:
                        session.add(self.model(key=self.key, value=payload))
                        session.flush()
                        return True
                    result = session.execute(
                        update(self.model)
                        .where(self.model.key == self.key, self.model.value == expected)
                        .values(value=payload)
                    )
                    return result.rowcount == 1
        except IntegrityError:
            return False
        except SQLAlchemyError as exc:
            raise RuntimeError("observation checkpoint write unavailable") from exc


class NativeEntryGate:
    def __init__(
        self,
        *,
        max_wait_ms: int,
        source_profile: str = "okx_eea_public_v1",
        max_states: int = 10_000,
        checkpoint: ObservationGateStore | None = None,
    ):
        if (
            type(max_wait_ms) is not int
            or not 0 < max_wait_ms <= 1_200_000
            or type(max_states) is not int
            or not 0 < max_states <= 10_000
            or source_profile not in {"okx_eea_public_v1", "okx_global_public_v1"}
        ):
            raise ValueError("bounded native entry-gate policy is required")
        self.max_wait_ms = max_wait_ms
        self.max_states = max_states
        self.source_profile = source_profile
        self.checkpoint = checkpoint
        self.state: dict[str, NativeGateState] = {}
        self.max_clock_ms = 0
        self._eligible_attempts: set[str] = set()
        self.reason = "passive"

    def refresh(self):
        if self.checkpoint is None:
            return None
        self.state, self.max_clock_ms, raw = self.checkpoint.load()
        return raw

    def _save(self, raw, now_ms: int) -> bool:
        if self.checkpoint is not None and not self.checkpoint.compare_and_swap(
            raw, self.state, max(now_ms, self.max_clock_ms)
        ):
            self.reason = "checkpoint_conflict"
            return False
        self.max_clock_ms = max(now_ms, self.max_clock_ms)
        return True

    def _prune(self, now_ms: int, decision_candle_ms: int):
        self.state = {
            key: value
            for key, value in self.state.items()
            if value.phase not in {"EXPIRED", "DISPATCHED"}
            or now_ms < value.native_deadline_ms
            or decision_candle_ms <= value.decision_candle_ms
        }

    def evaluate(
        self, reservation, observation: EntryObservation | None, *, now_ms: int, decision_candle_ms: int = 0
    ) -> bool:
        identifier = getattr(reservation, "executor_id", None)
        pair = getattr(reservation, "trading_pair", None)
        expiry = getattr(reservation, "expires_at", None)
        self._eligible_attempts.discard(identifier)
        if (
            not isinstance(identifier, str)
            or len(identifier) != 64
            or any(x not in "0123456789abcdef" for x in identifier)
            or not isinstance(pair, str)
            or not pair.endswith("-USDC")
            or not isinstance(expiry, (Decimal, int))
            or isinstance(expiry, bool)
            or type(now_ms) is not int
            or now_ms < 0
            or type(decision_candle_ms) is not int
            or not 0 <= decision_candle_ms <= now_ms
        ):
            self.reason = "invalid_native_proposal"
            return False
        try:
            native_deadline = int(expiry * 1000)
        except (ArithmeticError, ValueError, TypeError):
            self.reason = "invalid_native_proposal"
            return False
        raw = self.refresh()
        if now_ms < self.max_clock_ms:
            self.reason = "gate_clock_regressed"
            return False
        self._prune(now_ms, decision_candle_ms)
        state = self.state.get(identifier)
        if state is None:
            if len(self.state) >= self.max_states:
                self.reason = "gate_capacity_exceeded"
                self._save(raw, now_ms)
                return False
            deadline = min(native_deadline, now_ms + self.max_wait_ms)
            if deadline < now_ms:
                self.reason = "opportunity_expired"
                self._save(raw, now_ms)
                return False
            state = NativeGateState(
                identifier,
                pair,
                now_ms,
                decision_candle_ms,
                deadline,
                native_deadline,
                now_ms,
                "EXPIRED" if deadline == now_ms else "WAIT",
            )
        elif (
            state.trading_pair != pair or state.decision_candle_ms != decision_candle_ms or now_ms < state.last_clock_ms
        ):
            self.reason = "proposal_scope_or_clock_changed"
            return False
        elif native_deadline < state.native_deadline_ms:
            state = replace(
                state, native_deadline_ms=native_deadline, deadline_ms=min(state.deadline_ms, native_deadline)
            )
        if state.phase != "WAIT" or now_ms >= state.deadline_ms:
            self.state[identifier] = replace(
                state, phase="EXPIRED" if state.phase == "WAIT" else state.phase, last_clock_ms=now_ms
            )
            self.reason = "opportunity_expired" if state.phase == "WAIT" else "already_consumed"
            self._save(raw, now_ms)
            return False
        state = replace(state, last_clock_ms=now_ms)
        if isinstance(observation, EntryObservation) and observation.opportunity_id == identifier:
            if state.source_epoch is not None and state.source_epoch != observation.source_epoch:
                self.reason = "source_epoch_changed"
                return False
            state = replace(
                state,
                source_epoch=observation.source_epoch,
                observation_sha256=observation.content_sha256,
                observation_expires_at_ms=observation.expires_at_ms,
            )
        self.state[identifier] = state
        if not self._save(raw, now_ms):
            return False
        if (
            not isinstance(observation, EntryObservation)
            or observation.opportunity_id != identifier
            or observation.instrument_id != f"okx:spot:{pair}"
            or observation.source_profile != self.source_profile
        ):
            self.reason = "observation_scope_mismatch"
            return False
        if observation.available_at_ms > now_ms or now_ms >= observation.expires_at_ms:
            self.reason = "observation_unavailable"
            return False
        if not observation.qualified:
            self.reason = "observation_unqualified"
            return False
        if observation.adverse_flow:
            self.reason = "adverse_flow_veto"
            return False
        self._eligible_attempts.add(identifier)
        self.reason = "released_for_native_reservation"
        return True

    def mark_queued(self, proposal_id: str, *, now_ms: int) -> bool:
        raw = self.refresh()
        state = self.state.get(proposal_id)
        if (
            proposal_id not in self._eligible_attempts
            or state is None
            or state.phase != "WAIT"
            or type(now_ms) is not int
            or now_ms < state.last_clock_ms
        ):
            self.reason = "unproved_native_create"
            return False
        self.state[proposal_id] = replace(state, phase="QUEUED", last_clock_ms=now_ms)
        if not self._save(raw, now_ms):
            return False
        self._eligible_attempts.remove(proposal_id)
        self.reason = "queued_for_native_validation"
        return True

    def dispatch_permitted(
        self, proposal_id: str, observation: EntryObservation | None, *, pair: str, now_ms: int
    ) -> bool:
        raw = self.refresh()
        state = self.state.get(proposal_id)
        if type(now_ms) is not int or now_ms < self.max_clock_ms:
            self.reason = "gate_clock_regressed"
            return False
        if (
            state is None
            or state.phase != "QUEUED"
            or state.trading_pair != pair
            or now_ms >= min(state.deadline_ms, state.native_deadline_ms)
            or not isinstance(observation, EntryObservation)
            or observation.opportunity_id != proposal_id
            or observation.instrument_id != f"okx:spot:{pair}"
            or observation.source_profile != self.source_profile
            or observation.source_epoch != state.source_epoch
            or observation.content_sha256 != state.observation_sha256
            or observation.expires_at_ms != state.observation_expires_at_ms
            or observation.available_at_ms > now_ms
            or now_ms >= observation.expires_at_ms
            or not observation.qualified
            or observation.adverse_flow
        ):
            self.reason = "observation_dispatch_hold"
            return False
        if not self._save(raw, now_ms):
            return False
        self.reason = "observation_dispatch_eligible"
        return True

    def mark_dispatched(self, proposal_id: str, *, now_ms: int) -> bool:
        raw = self.refresh()
        state = self.state.get(proposal_id)
        if (
            state is None
            or state.phase != "QUEUED"
            or now_ms < state.last_clock_ms
            or now_ms >= min(state.deadline_ms, state.native_deadline_ms)
        ):
            self.reason = "dispatch_checkpoint_unavailable"
            return False
        self.state[proposal_id] = replace(state, phase="DISPATCHED", last_clock_ms=now_ms)
        if not self._save(raw, now_ms):
            return False
        self.reason = "dispatched_to_native_executor"
        return True


class ObservationBudgetProxy:
    def __init__(self, native_budget, admission: Callable[[object], bool]):
        self.native_budget = native_budget
        self.admission = admission
        self.last_error: str | None = None

    def reserve(self, reservation, *, native_executors) -> bool:
        try:
            admitted = self.admission(reservation)
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}:{exc}"[:240]
            return False
        if type(admitted) is not bool:
            self.last_error = "admission callback returned a non-boolean"
            return False
        self.last_error = None
        if not admitted:
            return False
        return self.native_budget.reserve(reservation, native_executors=native_executors)

    def __getattr__(self, name):
        return getattr(self.native_budget, name)
