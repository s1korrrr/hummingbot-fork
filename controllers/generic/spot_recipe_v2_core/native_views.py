"""Read native receipts and qualify new exit proposals; no orders, I/O or cost ledger."""
import math
from decimal import Decimal

from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.exit_policy import (
    ExitAssessment, ExitProposal, NativeExitView, NativeProfitFloor, arbitrate_exit,
)
from hummingbot.strategy_v2.components.recipe_contract import canonical_json, digest
from hummingbot.strategy_v2.components.spot_lifecycle_v2 import (
    NativeScopeV2, NativeSnapshotV2, checked_snapshot, project_lifecycle,
)


def _bounded_receipt(value, *, depth=0, budget=None):
    if budget is None:
        budget = [2048]
    budget[0] -= 1
    if depth > 8 or budget[0] < 0:
        raise PolicyError('native receipt exceeds structure bound')
    if type(value) is dict:
        if len(value) > 128 or any(type(k) is not str or len(k) > 128 for k in value):
            raise PolicyError('invalid native receipt keys')
        return {k: _bounded_receipt(v, depth=depth + 1, budget=budget) for k, v in value.items()}
    if type(value) in (tuple, list):
        if len(value) > 128:
            raise PolicyError('native receipt sequence exceeds bound')
        return [_bounded_receipt(v, depth=depth + 1, budget=budget) for v in value]
    if value is None or type(value) is bool:
        return value
    if type(value) is str and len(value) <= 1024:
        return value
    if type(value) is int and value.bit_length() <= 256:
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if type(value) is Decimal and value.is_finite():
        if len(value.as_tuple().digits) <= 38 and abs(value.as_tuple().exponent) <= 36:
            return value
    raise PolicyError('unsupported or unbounded native receipt value')


def capture_native_snapshot(info, *, scope: NativeScopeV2, sequence: int, observed_at_ms: int,
                            available_at_ms: int, reconciled: bool, fees_complete: bool,
                            commitments=(), terminal_requests=()) -> NativeSnapshotV2:
    """Caller supplies owner-qualified clocks, reconciliation and pending/terminal evidence.

    This adapter supports the existing entry-terminal PositionExecutor receipt shape.
    It cannot discover missing orders or certify fee completeness from gross totals.
    """
    if type(scope) is not NativeScopeV2:
        raise PolicyError('exact native scope required')
    scope = NativeScopeV2.model_validate(scope.model_dump(mode='python'))
    try:
        config = info.config
        if (info.id != scope.executor_id or info.controller_id != scope.owner_id
                or config.controller_id != scope.owner_id or config.connector_name != scope.connector
                or config.trading_pair != scope.trading_pair or config.side.name != 'BUY'):
            raise PolicyError('native executor scope mismatch')
        if type(info.is_done) is not bool or type(info.is_active) is not bool or info.is_active == info.is_done:
            raise PolicyError('unsupported native executor lifecycle flags')
        receipt = _bounded_receipt(info.custom_info['execution_receipt'])
        if type(receipt) is not dict:
            raise PolicyError('native execution receipt must be an object')
        entry_state = info.custom_info['entry_lifecycle']['state']
        if entry_state not in ('managing_fill', 'ended_no_fill', 'held_residual'):
            raise PolicyError('native entry lifecycle unsupported; entry-terminal evidence required')
        raw = canonical_json([scope.model_dump(mode='json'), info.id, info.is_done, info.is_active,
                              entry_state, receipt])
        if len(raw.encode('utf-8')) > 131072:
            raise PolicyError('native receipt exceeds byte bound')
        return NativeSnapshotV2(
            scope=scope, receipt_sha256=digest(raw), sequence=sequence,
            observed_at_ms=observed_at_ms, available_at_ms=available_at_ms,
            remaining_base=receipt['remaining_owned_base'], entry_gross_quote=receipt['entry_gross_quote'],
            exit_gross_base=receipt['exit_gross_base'], accounting_owner=receipt['accounting_owner'],
            entry_terminal=True, terminal=info.is_done, reconciled=reconciled, fees_complete=fees_complete,
            commitments=commitments, terminal_requests=terminal_requests)
    except (AttributeError, KeyError, TypeError) as error:
        raise PolicyError('native execution receipt unavailable') from error


def assess_exit(snapshot: NativeSnapshotV2, view: NativeExitView, proposals: tuple[ExitProposal, ...],
                floor: NativeProfitFloor | None, *, now_ms: int, max_age_ms: int) -> ExitAssessment:
    """Assess only a NEW reduction. Never replace the native management/action loop."""
    snapshot = checked_snapshot(snapshot)
    if type(view) is not NativeExitView or type(proposals) is not tuple or len(proposals) > 64:
        raise PolicyError('bounded canonical native exit evidence required')
    view = NativeExitView.model_validate(view.model_dump(mode='python'))
    if any(type(p) is not ExitProposal for p in proposals):
        raise PolicyError('canonical exit proposals required')
    proposals = tuple(ExitProposal.model_validate(p.model_dump(mode='python')) for p in proposals)
    if floor is not None:
        if type(floor) is not NativeProfitFloor:
            raise PolicyError('canonical native profit floor required')
        floor = NativeProfitFloor.model_validate(floor.model_dump(mode='python'))
    projection = project_lifecycle(snapshot, now_ms=now_ms, max_age_ms=max_age_ms)
    source = digest([snapshot, view, sorted(proposals, key=lambda p: p.policy_id), floor, now_ms, max_age_ms])

    def hold(reason):
        return ExitAssessment(status='HOLD', reason=reason, input_sha256=source)

    scope = snapshot.scope
    if (view.owner_id, view.executor_id, view.connector, view.trading_pair, view.receipt_sha256,
        view.observed_at_ms, view.remaining_base, view.reconciled, view.fees_complete) != (
            scope.owner_id, scope.executor_id, scope.connector, scope.trading_pair, snapshot.receipt_sha256,
            snapshot.observed_at_ms, snapshot.remaining_base, snapshot.reconciled, snapshot.fees_complete):
        return hold('native_exit_snapshot_mismatch')
    if projection.blockers:
        return hold(projection.blockers[0])
    if snapshot.commitments:
        return hold('native_request_pending')
    if not snapshot.entry_terminal:
        return hold('native_acquisition_pending')
    if projection.phase == 'RETAINED':
        return hold('retained_inventory_adapter_required')
    if view.reserved_base != 0:
        return hold('native_commitment_projection_mismatch')
    result = arbitrate_exit(view, proposals, floor, now_ms=now_ms, max_age_ms=max_age_ms)
    return ExitAssessment.model_validate(result.model_dump(mode='python') | {'input_sha256': source})
