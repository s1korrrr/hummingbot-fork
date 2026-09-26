"""Single-owner native trailing view, exit resolution and a restart seam. No seller, ledger or I/O."""
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import Field, StrictBool, field_validator, model_serializer, model_validator

from controllers.generic.spot_recipe_v2_core.spot_lifecycle_v2 import (
    Amount,
    NativeScopeV2,
    NativeSnapshotV2,
    checked_snapshot,
    validate_continuity,
)
from hummingbot.strategy_v2.components.config import PolicyModel, TrailingPolicy
from hummingbot.strategy_v2.components.decision_protocol import HASH, NAME
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.exit_policy import (
    ExitAssessment,
    ExitProposal,
    NativeExitView,
    NativeProfitFloor,
)
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.components.strict_json import strict_json
from hummingbot.strategy_v2.components.trailing import protected_sell_price

from .native_views import assess_exit

Price = Annotated[Amount, Field(gt=0)]
RequestId = Annotated[str, Field(pattern=NAME)]


class NativeTrailingViewV2(PolicyModel):
    owner_id: str = Field(pattern=NAME)
    executor_id: str = Field(pattern=NAME)
    receipt_sha256: str = Field(pattern=HASH)
    observed_at_ms: int = Field(ge=0, le=2 ** 63 - 1, strict=True)
    activated: StrictBool
    peak_price: Price | None = None
    threshold_price: Price
    exit_owner: Literal['native_executor', 'recipe_proposal']
    # PositionExecutor persists a net-PnL trigger, not an immutable quote price.
    # Older/recipe views omit this evidence and retain the quote-price guard.
    native_trigger_pct: Decimal | None = None
    native_move_count: int | None = Field(default=None, ge=0, strict=True)
    native_armed_timestamp: Decimal | None = Field(default=None, ge=0)
    native_last_update_timestamp: Decimal | None = Field(default=None, ge=0)

    @model_serializer(mode='wrap')
    def serialize_native_evidence(self, handler):
        data = handler(self)
        if self.native_trigger_pct is None:
            # Legacy checkpoint hashes include the older null fields, but not these new fields.
            for key in ('native_trigger_pct', 'native_move_count', 'native_armed_timestamp',
                        'native_last_update_timestamp'):
                data.pop(key, None)
        return data

    @model_validator(mode='after')
    def ratcheted_below_peak(self):
        if self.peak_price is not None and self.threshold_price > self.peak_price:
            raise ValueError('native trailing threshold exceeds the observed peak')
        evidence = (self.native_trigger_pct, self.native_move_count,
                    self.native_armed_timestamp, self.native_last_update_timestamp)
        if any(value is not None for value in evidence):
            if any(value is None for value in evidence) or self.exit_owner != 'native_executor' or not self.activated:
                raise ValueError('complete activated native trailing evidence required')
            if self.native_last_update_timestamp < self.native_armed_timestamp:
                raise ValueError('native trailing update precedes activation')
        return self


class BoundRequestV2(PolicyModel):
    request_id: RequestId
    action: Literal['acquire', 'reduce']


class LifecycleCheckpointV2(PolicyModel):
    schema_version: Literal[1] = 1
    scope: NativeScopeV2
    receipt_sha256: str = Field(pattern=HASH)
    sequence: int = Field(ge=1, le=2 ** 63 - 1, strict=True)
    cursor_sha256: str | None = Field(default=None, pattern=HASH)
    outstanding: tuple[BoundRequestV2, ...] = Field(default=(), max_length=64)
    trailing: NativeTrailingViewV2
    execution_authorized: Literal[False] = False

    @field_validator('schema_version', mode='before')
    @classmethod
    def strict_schema(cls, value):
        if type(value) is not int:
            raise ValueError('schema version must be an integer')
        return value

    @property
    def outstanding_request_ids(self) -> tuple[str, ...]:
        return tuple(item.request_id for item in self.outstanding)

    @model_validator(mode='after')
    def bound_identities(self):
        if len(set(self.outstanding_request_ids)) != len(self.outstanding):
            raise ValueError('duplicate outstanding native request identity')
        if (self.trailing.owner_id, self.trailing.executor_id, self.trailing.receipt_sha256) != (
                self.scope.owner_id, self.scope.executor_id, self.receipt_sha256):
            raise ValueError('checkpoint trailing state belongs to another lot or receipt')
        return self

    @property
    def checkpoint_sha256(self):
        return digest(self.model_dump(mode='json'))


class RestoreDecisionV2(PolicyModel):
    status: Literal['BOUND_REQUEST', 'UNCERTAINTY_HOLD']
    reason: str = Field(pattern=NAME)
    checkpoint_sha256: str = Field(pattern=HASH)
    snapshot_sha256: str = Field(pattern=HASH)
    request_ids: tuple[RequestId, ...] = Field(default=(), max_length=64)
    trailing: NativeTrailingViewV2
    execution_authorized: Literal[False] = False


def bind_trailing_view(snapshot: NativeSnapshotV2, views: tuple[NativeTrailingViewV2, ...], *, now_ms: int,
                       max_age_ms: int, previous: NativeTrailingViewV2 | None) -> NativeTrailingViewV2:
    """Bind the one native-owned trailing record to a snapshot; never compute or loosen a trail.

    `previous` has no default. A ratchet whose guard is opt-in is not a ratchet, so a caller with
    no prior view has to say `previous=None` and mean it.
    """
    snapshot = checked_snapshot(snapshot)
    if type(views) is not tuple or len(views) > 4 or any(type(item) is not NativeTrailingViewV2 for item in views):
        raise PolicyError('bounded canonical native trailing evidence required')
    if (type(now_ms) is not int or now_ms < 0 or type(max_age_ms) is not int
            or not 1 <= max_age_ms <= 60000):
        raise PolicyError('invalid trailing clock or native observation age limit')
    if len(views) != 1:
        raise PolicyError('exactly one native trailing owner is required')
    view = NativeTrailingViewV2.model_validate(views[0].model_dump(mode='python'))
    if (view.owner_id, view.executor_id, view.receipt_sha256) != (
            snapshot.scope.owner_id, snapshot.scope.executor_id, snapshot.receipt_sha256):
        raise PolicyError('native trailing state belongs to another lot or receipt')
    if view.observed_at_ms != snapshot.observed_at_ms:
        raise PolicyError('native trailing observation differs from the native snapshot')
    if not 0 <= now_ms - view.observed_at_ms <= max_age_ms:
        raise PolicyError('native trailing observation is stale')
    if previous is not None:
        if type(previous) is not NativeTrailingViewV2:
            raise PolicyError('canonical previous native trailing state required')
        previous = NativeTrailingViewV2.model_validate(previous.model_dump(mode='python'))
        if (previous.owner_id, previous.executor_id) != (view.owner_id, view.executor_id):
            raise PolicyError('native trailing lineage belongs to another lot')
        if previous.exit_owner != view.exit_owner:
            raise PolicyError('native trailing exit owner changed')
        if previous.observed_at_ms > view.observed_at_ms:
            raise PolicyError('native trailing observation clock regressed')
        if previous.native_trigger_pct is not None:
            if view.native_trigger_pct is None:
                raise PolicyError('native trailing evidence disappeared')
            if view.native_armed_timestamp != previous.native_armed_timestamp:
                raise PolicyError('native trailing arm lineage changed')
            if view.native_last_update_timestamp < previous.native_last_update_timestamp:
                raise PolicyError('native trailing update clock regressed')
            if (view.native_trigger_pct < previous.native_trigger_pct
                    or view.native_move_count < previous.native_move_count):
                raise PolicyError('native trailing ratchet loosened')
            if ((view.native_trigger_pct > previous.native_trigger_pct)
                    != (view.native_move_count > previous.native_move_count)):
                raise PolicyError('native trailing move lineage inconsistent')
        elif view.threshold_price < previous.threshold_price:
            raise PolicyError('native trailing ratchet loosened')
        if previous.peak_price is not None:
            if view.peak_price is None:
                raise PolicyError('native trailing peak evidence disappeared')
            if view.peak_price < previous.peak_price:
                raise PolicyError('native trailing ratchet loosened')
        if previous.activated and not view.activated:
            raise PolicyError('native trailing activation withdrawn')
    return view


def protective_exit_price(view: NativeTrailingViewV2, native_no_loss_price, *, policy: TrailingPolicy,
                          price_step) -> Decimal:
    if type(view) is not NativeTrailingViewV2 or type(policy) is not TrailingPolicy:
        raise PolicyError('canonical native trailing state and trailing policy required')
    view = NativeTrailingViewV2.model_validate(view.model_dump(mode='python'))
    policy = TrailingPolicy.model_validate(policy.model_dump(mode='python'))
    return max(view.threshold_price, protected_sell_price(native_no_loss_price, policy, price_step))


def resolve_exit(snapshot: NativeSnapshotV2, trailing: tuple[NativeTrailingViewV2, ...], view: NativeExitView,
                 proposals: tuple[ExitProposal, ...], floor: NativeProfitFloor | None, *, now_ms: int,
                 max_age_ms: int, policy: TrailingPolicy, previous_trailing: NativeTrailingViewV2 | None,
                 ) -> ExitAssessment:
    """Guard native trailing ownership and the ratchet, then delegate quantity rules to assess_exit."""
    snapshot = checked_snapshot(snapshot)
    if (type(trailing) is not tuple or not 1 <= len(trailing) <= 4
            or any(type(item) is not NativeTrailingViewV2 for item in trailing)):
        raise PolicyError('bounded canonical native trailing evidence required')
    if (type(view) is not NativeExitView or type(proposals) is not tuple or len(proposals) > 64
            or any(type(item) is not ExitProposal for item in proposals)):
        raise PolicyError('bounded canonical native exit evidence required')
    if type(policy) is not TrailingPolicy:
        raise PolicyError('canonical trailing policy required to enforce the ratchet')
    source = digest([snapshot, trailing, view, sorted(proposals, key=lambda item: item.policy_id),
                     floor, now_ms, max_age_ms, policy])
    if len(trailing) != 1:
        return ExitAssessment(status='HOLD', reason='duplicate_native_trailing_owner', input_sha256=source)
    if trailing[0].exit_owner != 'native_executor':
        return ExitAssessment(status='HOLD', reason='native_trailing_submission_not_owned', input_sha256=source)
    bound = bind_trailing_view(snapshot, trailing, now_ms=now_ms, max_age_ms=max_age_ms, previous=previous_trailing)
    assessment = assess_exit(snapshot, view, proposals, floor, now_ms=now_ms, max_age_ms=max_age_ms)
    if assessment.status != 'PROPOSED_REDUCTION':
        return assessment
    protective = protective_exit_price(bound, None if floor is None else floor.price,
                                       policy=policy, price_step=view.price_step)
    # Enforce the current native price projection after validating its persisted percentage ratchet.
    if assessment.limit_price < protective:
        return ExitAssessment(status='HOLD', reason='native_trailing_floor_not_met', input_sha256=source)
    return assessment


def capture_checkpoint(snapshot: NativeSnapshotV2, trailing: tuple[NativeTrailingViewV2, ...], *, now_ms: int,
                       max_age_ms: int, previous: NativeTrailingViewV2 | None,
                       cursor_sha256: str | None = None) -> LifecycleCheckpointV2:
    snapshot = checked_snapshot(snapshot)
    view = bind_trailing_view(snapshot, trailing, now_ms=now_ms, max_age_ms=max_age_ms, previous=previous)
    return LifecycleCheckpointV2(
        scope=snapshot.scope, receipt_sha256=snapshot.receipt_sha256, sequence=snapshot.sequence,
        cursor_sha256=cursor_sha256, trailing=view,
        outstanding=tuple(BoundRequestV2(request_id=item.request_id, action=item.action)
                          for item in sorted(snapshot.commitments, key=lambda item: item.request_id)))


def encode_checkpoint(checkpoint: LifecycleCheckpointV2) -> str:
    if type(checkpoint) is not LifecycleCheckpointV2:
        raise PolicyError('exact lifecycle checkpoint required')
    raw = LifecycleCheckpointV2.model_validate(checkpoint.model_dump(mode='python')).model_dump_json()
    if len(raw.encode('utf-8')) > 131072:
        raise PolicyError('checkpoint exceeds byte bound')
    return raw


def decode_checkpoint(raw: str) -> LifecycleCheckpointV2:
    if type(raw) is not str or len(raw) > 131072 or len(raw.encode('utf-8')) > 131072:
        raise PolicyError('bounded UTF-8 checkpoint text required')
    return LifecycleCheckpointV2.model_validate(strict_json(raw))


def restore_checkpoint(raw: str, snapshot: NativeSnapshotV2, *,
                       previous: NativeSnapshotV2 | None = None) -> RestoreDecisionV2:
    """Rebind the original outstanding request or hold; a restart never produces a second request."""
    checkpoint = decode_checkpoint(raw)
    snapshot = checked_snapshot(snapshot)
    if snapshot.scope != checkpoint.scope:
        raise PolicyError('native scope or recipe changed since the checkpoint; explicit reconciliation required')
    if snapshot.sequence < checkpoint.sequence:
        raise PolicyError('native sequence regressed since the checkpoint')
    if snapshot.sequence == checkpoint.sequence and snapshot.receipt_sha256 != checkpoint.receipt_sha256:
        raise PolicyError('native receipt changed at the checkpoint sequence')
    if previous is not None:
        previous = checked_snapshot(previous)
        if (previous.sequence, previous.receipt_sha256) != (checkpoint.sequence, checkpoint.receipt_sha256):
            raise PolicyError('previous native snapshot does not match the checkpoint')
        validate_continuity(previous, snapshot)
    bound = set(checkpoint.outstanding_request_ids)
    current = {item.request_id: item for item in snapshot.commitments}
    ended = {item.request_id for item in snapshot.terminal_requests}
    # An id is not an identity: the same id carrying a different action is a different request.
    if any(current[item.request_id].action != item.action
           for item in checkpoint.outstanding if item.request_id in current):
        raise PolicyError('native request action changed since the checkpoint')

    def decision(status, reason, request_ids=()):
        # The checkpoint trailing lineage is returned so the caller binds the next view against it.
        return RestoreDecisionV2(status=status, reason=reason, request_ids=tuple(sorted(request_ids)),
                                 checkpoint_sha256=checkpoint.checkpoint_sha256,
                                 snapshot_sha256=snapshot.snapshot_sha256, trailing=checkpoint.trailing)
    if set(current) - bound:
        return decision('UNCERTAINTY_HOLD', 'unbound_native_request_present', bound & set(current))
    if any(item.status == 'UNCERTAIN' for item in current.values()):
        return decision('UNCERTAINTY_HOLD', 'native_commitment_uncertain', set(current))
    unresolved = bound - set(current) - ended
    if unresolved:
        return decision('UNCERTAINTY_HOLD', 'bound_native_request_unresolved', unresolved)
    if bound & set(current):
        return decision('BOUND_REQUEST', 'native_request_rebound', bound & set(current))
    return decision('UNCERTAINTY_HOLD', 'no_bound_native_request')
