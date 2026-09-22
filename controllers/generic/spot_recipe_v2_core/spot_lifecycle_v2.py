"""Bounded read-only spot lifecycle projections. Never an order or inventory ledger."""
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal

from pydantic import BeforeValidator, Field, StrictBool, field_validator, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import HASH, NAME
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, digest
from hummingbot.strategy_v2.components.strict_json import strict_json


def _amount(value):
    if type(value) not in (str, int, Decimal) or len(str(value)) > 80:
        raise ValueError('exact bounded decimal required; floats and booleans are not amounts')
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise ValueError('invalid decimal amount') from error
    if not result.is_finite() or result < 0:
        raise ValueError('nonnegative finite amount required')
    parts = result.as_tuple()
    if len(parts.digits) > 38 or abs(parts.exponent) > 36:
        raise ValueError('amount precision exceeds contract capacity')
    return result


Amount = Annotated[Decimal, BeforeValidator(_amount)]


class NativeScopeV2(PolicyModel):
    program_id: str = Field(pattern=NAME)
    owner_id: str = Field(pattern=NAME)
    executor_id: str = Field(pattern=NAME)
    connector: Literal['okx', 'okx_paper_trade']
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$', max_length=80)
    execution_mode: Literal['observation', 'shadow', 'paper', 'live']
    capital_model: Literal['SPOT_UNLEVERAGED'] = 'SPOT_UNLEVERAGED'
    recipe_binding: RecipeBinding

    @field_validator('recipe_binding', mode='before')
    @classmethod
    def checked_binding(cls, value):
        if isinstance(value, RecipeBinding):
            value = value.model_dump(mode='python')
        return RecipeBinding.model_validate(value)

    @model_validator(mode='after')
    def market_scope(self):
        base, quote = self.trading_pair.split('-')
        if base == quote or (self.connector == 'okx_paper_trade') != (self.execution_mode == 'paper'):
            raise ValueError('incompatible spot instrument or paper/live scope')
        return self


class NativeCommitmentV2(PolicyModel):
    request_id: str = Field(pattern=NAME)
    action: Literal['acquire', 'reduce']
    status: Literal['PENDING', 'UNCERTAIN']
    remaining_quote: Amount
    remaining_base: Amount

    @model_validator(mode='after')
    def liability(self):
        if self.action == 'acquire' and self.remaining_quote <= 0:
            raise ValueError('acquisition requires positive native quote liability')
        if self.action == 'reduce' and (self.remaining_base <= 0 or self.remaining_quote != 0):
            raise ValueError('reduction requires positive base liability and no quote reservation')
        return self


class NativeTerminalV2(PolicyModel):
    request_id: str = Field(pattern=NAME)
    receipt_sha256: str = Field(pattern=HASH)


class NativeSnapshotV2(PolicyModel):
    schema_version: Literal[2] = 2
    scope: NativeScopeV2
    receipt_sha256: str = Field(pattern=HASH)
    sequence: int = Field(ge=1, le=2 ** 63 - 1, strict=True)
    observed_at_ms: int = Field(ge=0, le=2 ** 63 - 1, strict=True)
    available_at_ms: int = Field(ge=0, le=2 ** 63 - 1, strict=True)
    remaining_base: Amount
    entry_gross_quote: Amount
    exit_gross_base: Amount
    entry_terminal: StrictBool
    terminal: StrictBool
    reconciled: StrictBool
    fees_complete: StrictBool
    accounting_owner: Literal['executor', 'held_position']
    commitments: tuple[NativeCommitmentV2, ...] = Field(default=(), max_length=64)
    terminal_requests: tuple[NativeTerminalV2, ...] = Field(default=(), max_length=64)

    @field_validator('schema_version', mode='before')
    @classmethod
    def strict_schema(cls, value):
        if type(value) is not int:
            raise ValueError('schema version must be an integer')
        return value

    @model_validator(mode='after')
    def native_consistency(self):
        if self.available_at_ms < self.observed_at_ms:
            raise ValueError('availability precedes native observation')
        if (self.remaining_base > 0 or self.exit_gross_base > 0) and self.entry_gross_quote <= 0:
            raise ValueError('inventory without native acquisition provenance')
        if self.terminal and not self.entry_terminal:
            raise ValueError('terminal executor with nonterminal acquisition')
        current = {item.request_id for item in self.commitments}
        ended = {item.request_id for item in self.terminal_requests}
        if len(current) != len(self.commitments) or len(ended) != len(self.terminal_requests) or current & ended:
            raise ValueError('duplicate or conflicting native request identities')
        return self

    @property
    def snapshot_sha256(self):
        return digest(self.model_dump(mode='json'))


class LifecycleProjectionV2(PolicyModel):
    phase: Literal['READY', 'ACQUIRING', 'ACQUISITION_UNCERTAIN', 'MANAGING', 'EXIT_PENDING',
                   'EXIT_UNCERTAIN', 'RETAINED', 'CLOSED', 'RECONCILIATION_REQUIRED']
    snapshot_sha256: str = Field(pattern=HASH)
    management_required: StrictBool
    blockers: tuple[str, ...]
    execution_authorized: Literal[False] = False


def checked_snapshot(value: NativeSnapshotV2) -> NativeSnapshotV2:
    if type(value) is not NativeSnapshotV2:
        raise PolicyError('exact native snapshot required')
    # Revalidate copied/constructed nested models at every public assessment boundary.
    return NativeSnapshotV2.model_validate(value.model_dump(mode='python'))


def project_lifecycle(view: NativeSnapshotV2, *, now_ms: int, max_age_ms: int) -> LifecycleProjectionV2:
    view = checked_snapshot(view)
    if type(now_ms) is not int or not 0 <= now_ms < 2 ** 63 or type(max_age_ms) is not int or not 1 <= max_age_ms <= 60000:
        raise PolicyError('invalid clock or native snapshot age limit')
    blockers = []
    if now_ms < view.available_at_ms:
        blockers.append('native_snapshot_not_available')
    if now_ms - view.observed_at_ms > max_age_ms:
        blockers.append('native_snapshot_stale')
    if not view.reconciled:
        blockers.append('native_reconciliation_required')
    if not view.fees_complete:
        blockers.append('native_fees_incomplete')
    actions = {item.action for item in view.commitments}
    uncertain = any(item.status == 'UNCERTAIN' for item in view.commitments)
    if len(actions) > 1:
        blockers.append('conflicting_native_commitments')
    if uncertain:
        blockers.append('native_commitment_uncertain')
    if 'reduce' in actions:
        phase = 'EXIT_UNCERTAIN' if uncertain else 'EXIT_PENDING'
    elif 'acquire' in actions:
        phase = 'ACQUISITION_UNCERTAIN' if uncertain else 'ACQUIRING'
    elif view.remaining_base > 0:
        phase = 'RETAINED' if view.terminal or view.accounting_owner == 'held_position' else 'MANAGING'
        if not view.entry_terminal:
            phase = 'ACQUIRING'
    elif blockers:
        phase = 'RECONCILIATION_REQUIRED'
    elif view.terminal:
        phase = 'CLOSED'
    elif view.entry_gross_quote == 0 and not view.entry_terminal:
        phase = 'READY'
    else:
        phase = 'RECONCILIATION_REQUIRED'
        blockers.append('native_terminal_required')
    management = bool(view.remaining_base or view.commitments or blockers or phase not in ('READY', 'CLOSED'))
    return LifecycleProjectionV2(phase=phase, snapshot_sha256=view.snapshot_sha256,
                                 management_required=management, blockers=tuple(blockers))


def validate_continuity(previous: NativeSnapshotV2, current: NativeSnapshotV2) -> None:
    previous, current = checked_snapshot(previous), checked_snapshot(current)
    if current.scope != previous.scope:
        raise PolicyError('native scope or recipe changed; explicit reconciliation required')
    if current.sequence < previous.sequence:
        raise PolicyError('native sequence regressed')
    if current.sequence == previous.sequence:
        if current != previous:
            raise PolicyError('native content changed at the same sequence')
        return
    if current.observed_at_ms < previous.observed_at_ms or current.available_at_ms < previous.available_at_ms:
        raise PolicyError('native observation clock regressed')
    removed = {item.request_id for item in previous.commitments} - {item.request_id for item in current.commitments}
    ended = {item.request_id for item in current.terminal_requests}
    if not removed <= ended:
        raise PolicyError('native terminal receipt required before removing a commitment')
    before = {item.request_id: item for item in previous.commitments}
    for item in current.commitments:
        if item.request_id in before and item.action != before[item.request_id].action:
            raise PolicyError('native request action changed')


def encode_snapshot(view: NativeSnapshotV2) -> str:
    raw = checked_snapshot(view).model_dump_json()
    if len(raw.encode('utf-8')) > 131072:
        raise PolicyError('snapshot exceeds byte bound')
    return raw


def decode_snapshot(raw: str) -> NativeSnapshotV2:
    if type(raw) is not str or len(raw) > 131072 or len(raw.encode('utf-8')) > 131072:
        raise PolicyError('bounded UTF-8 snapshot text required')
    return NativeSnapshotV2.model_validate(strict_json(raw))


def management_pairs(views: tuple[NativeSnapshotV2, ...], *, now_ms: int, max_age_ms: int) -> tuple[str, ...]:
    if type(views) is not tuple or len(views) > 1024:
        raise PolicyError('bounded immutable native snapshot collection required')
    seen, pairs = set(), set()
    for view in views:
        view = checked_snapshot(view)
        key = (view.scope.owner_id, view.scope.executor_id)
        if key in seen:
            raise PolicyError('duplicate native scope in management projection')
        seen.add(key)
        if project_lifecycle(view, now_ms=now_ms, max_age_ms=max_age_ms).management_required:
            pairs.add(view.scope.trading_pair)
    return tuple(sorted(pairs))
