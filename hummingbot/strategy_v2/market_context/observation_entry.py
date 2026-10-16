"""Offline entry-filter contract over an existing opportunity and trace.

The caller retains all order, reservation, exit and reconciliation authority.
The release result means only that native admission may be attempted once.
This reducer is not wired into the current modular spot owner: its pre-claim
proposal seam must preserve native bag management before it can be used there.
"""

from dataclasses import dataclass
from typing import Callable

from hummingbot.strategy_v2.components.decision_protocol import DecisionTrace, Opportunity
from hummingbot.strategy_v2.components.recipe_contract import digest


@dataclass(frozen=True)
class EntryPolicy:
    enabled: bool = False
    max_wait_ms: int = 60_000

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool or type(self.max_wait_ms) is not int or not 0 < self.max_wait_ms <= 1_200_000:
            raise ValueError("entry observation policy requires explicit bounded values")


@dataclass(frozen=True)
class EntryObservation:
    opportunity_id: str
    content_sha256: str
    instrument_id: str
    source_profile: str
    source_epoch: str
    available_at_ms: int
    expires_at_ms: int
    qualified: bool
    adverse_flow: bool

    def __post_init__(self) -> None:
        if (
            len(self.opportunity_id) != 64
            or len(self.content_sha256) != 64
            or not isinstance(self.instrument_id, str)
            or not self.instrument_id.startswith("okx:spot:")
            or self.source_profile not in {"okx_eea_public_v1", "okx_global_public_v1"}
            or not self.source_epoch
            or len(self.source_epoch) > 128
            or type(self.available_at_ms) is not int
            or self.available_at_ms < 0
            or type(self.expires_at_ms) is not int
            or self.expires_at_ms <= self.available_at_ms
            or type(self.qualified) is not bool
            or type(self.adverse_flow) is not bool
        ):
            raise ValueError("invalid bounded entry observation")
        for value in (self.opportunity_id, self.content_sha256):
            if any(char not in "0123456789abcdef" for char in value):
                raise ValueError("entry observation requires lowercase SHA-256 identities")


@dataclass(frozen=True)
class EntryGateState:
    opportunity_id: str
    consumption_key: str
    first_seen_ms: int
    deadline_ms: int
    last_clock_ms: int
    phase: str
    source_epoch: str | None = None
    observation_sha256: str | None = None

    def __post_init__(self) -> None:
        if self.phase not in {"WAIT", "RELEASED", "EXPIRED"}:
            raise ValueError("invalid entry gate phase")
        if not self.first_seen_ms <= self.last_clock_ms or self.deadline_ms < self.first_seen_ms:
            raise ValueError("invalid entry gate clock")


@dataclass(frozen=True)
class EntryGateResult:
    state: EntryGateState | None
    trace: DecisionTrace


def _trace(
    opportunity: Opportunity, now_ms: int, reason: str, observation: EntryObservation | None, eligible: bool = False
) -> DecisionTrace:
    evidence = observation.content_sha256 if observation is not None else None
    return DecisionTrace(
        opportunity_id=opportunity.opportunity_id,
        consumption_key=opportunity.consumption_key,
        decision_time_ms=now_ms,
        status="ELIGIBLE_FOR_NATIVE_ADMISSION" if eligible else "HOLD",
        reason=reason,
        blockers=() if eligible else (reason,),
        prerequisite_sha256=digest(["okx-observation-entry-v1", reason, evidence]),
    )


def evaluate_entry(
    state: EntryGateState | None,
    opportunity: Opportunity,
    baseline_trace: DecisionTrace,
    *,
    now_ms: int,
    policy: EntryPolicy,
    observation: EntryObservation | None,
    native_recheck: Callable[[], bool],
    pending_commitment: bool = False,
) -> EntryGateResult:
    """Evaluate one already native-eligible entry. No orders or capital effects.

    ``native_recheck`` must perform the owner's current admission check at release.
    An existing pending commitment can only be cleared by native reconciliation.
    """
    if not isinstance(policy, EntryPolicy):
        raise ValueError("entry observation policy is required")
    if not policy.enabled:
        return EntryGateResult(state, baseline_trace)
    if not isinstance(opportunity, Opportunity) or not isinstance(baseline_trace, DecisionTrace):
        raise ValueError("native opportunity and decision trace are required")
    if (
        baseline_trace.opportunity_id != opportunity.opportunity_id
        or baseline_trace.consumption_key != opportunity.consumption_key
    ):
        raise ValueError("native trace does not belong to the opportunity")
    if type(now_ms) is not int or now_ms < opportunity.created_at_ms or now_ms < baseline_trace.decision_time_ms:
        raise ValueError("decision clock regressed")
    if type(pending_commitment) is not bool:
        raise ValueError("pending commitment must be a native boolean")
    if opportunity.action != "acquire" or baseline_trace.status != "ELIGIBLE_FOR_NATIVE_ADMISSION":
        return EntryGateResult(state, baseline_trace)
    if state is None:
        deadline = min(opportunity.expires_at_ms, now_ms + policy.max_wait_ms)
        state = EntryGateState(
            opportunity_id=opportunity.opportunity_id,
            consumption_key=opportunity.consumption_key,
            first_seen_ms=now_ms,
            deadline_ms=max(now_ms, deadline),
            last_clock_ms=now_ms,
            phase="EXPIRED" if deadline <= now_ms else "WAIT",
        )
    elif (
        state.opportunity_id != opportunity.opportunity_id
        or state.consumption_key != opportunity.consumption_key
        or now_ms < state.last_clock_ms
    ):
        raise ValueError("entry gate scope or clock regressed")
    if observation is not None:
        if (
            not isinstance(observation, EntryObservation)
            or observation.opportunity_id != opportunity.opportunity_id
            or observation.instrument_id != f"okx:spot:{opportunity.scope.trading_pair}"
        ):
            raise ValueError("observation does not belong to the opportunity")
        if state.source_epoch is not None and observation.source_epoch != state.source_epoch:
            raise ValueError("observation source epoch changed during a held opportunity")
    if state.phase == "RELEASED":
        return EntryGateResult(state, _trace(opportunity, now_ms, "already_released", observation))
    if state.phase == "EXPIRED" or now_ms >= state.deadline_ms or now_ms >= opportunity.expires_at_ms:
        expired = EntryGateState(
            opportunity.opportunity_id,
            opportunity.consumption_key,
            state.first_seen_ms,
            state.deadline_ms,
            now_ms,
            "EXPIRED",
            state.source_epoch,
            state.observation_sha256,
        )
        return EntryGateResult(
            expired,
            _trace(
                opportunity, now_ms, "pending_commitment" if pending_commitment else "opportunity_expired", observation
            ),
        )
    next_state = EntryGateState(
        opportunity.opportunity_id,
        opportunity.consumption_key,
        state.first_seen_ms,
        state.deadline_ms,
        now_ms,
        "WAIT",
        observation.source_epoch if observation else state.source_epoch,
        observation.content_sha256 if observation else state.observation_sha256,
    )
    if pending_commitment:
        return EntryGateResult(next_state, _trace(opportunity, now_ms, "pending_commitment", observation))
    if observation is None or observation.available_at_ms > now_ms or now_ms >= observation.expires_at_ms:
        return EntryGateResult(next_state, _trace(opportunity, now_ms, "observation_unavailable", observation))
    if not observation.qualified:
        return EntryGateResult(next_state, _trace(opportunity, now_ms, "observation_unqualified", observation))
    if observation.adverse_flow:
        return EntryGateResult(next_state, _trace(opportunity, now_ms, "adverse_flow_veto", observation))
    if not callable(native_recheck):
        raise ValueError("native admission recheck is required")
    admitted = native_recheck()
    if type(admitted) is not bool:
        raise ValueError("native admission recheck must return a boolean")
    if not admitted:
        return EntryGateResult(next_state, _trace(opportunity, now_ms, "native_recheck_hold", observation))
    released = EntryGateState(
        opportunity.opportunity_id,
        opportunity.consumption_key,
        state.first_seen_ms,
        state.deadline_ms,
        now_ms,
        "RELEASED",
        observation.source_epoch,
        observation.content_sha256,
    )
    return EntryGateResult(
        released, _trace(opportunity, now_ms, "released_for_native_admission", observation, eligible=True)
    )
