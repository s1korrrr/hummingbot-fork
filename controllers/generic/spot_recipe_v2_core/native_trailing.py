"""Adapt PositionExecutor's read-only trailing projection into the isolated v2 lifecycle contract."""
import math
from decimal import Decimal, InvalidOperation

from controllers.generic.spot_recipe_v2_core.lifecycle import NativeTrailingViewV2
from controllers.generic.spot_recipe_v2_core.native_views import capture_native_snapshot
from controllers.generic.spot_recipe_v2_core.spot_lifecycle_v2 import NativeSnapshotV2, checked_snapshot
from hummingbot.strategy_v2.components.domain import PolicyError


_UNARMED_STATES = {'disabled', 'pending', 'waiting'}
_ARMED_STATES = {'armed', 'triggered'}


def _decimal(value, name, *, positive=False):
    if type(value) not in (str, int, Decimal) or len(str(value)) > 80:
        raise PolicyError(f'exact native {name} required')
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError, TypeError):
        raise PolicyError(f'exact native {name} required') from None
    if not result.is_finite() or (positive and result <= 0):
        raise PolicyError(f'valid native {name} required')
    return result


def _timestamp(value, name):
    if type(value) not in (int, float, Decimal) or type(value) is bool:
        raise PolicyError(f'native {name} required')
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        raise PolicyError(f'native {name} required') from None
    if not math.isfinite(numeric) or numeric < 0:
        raise PolicyError(f'valid native {name} required')
    return numeric


def capture_native_trailing_view(info, snapshot: NativeSnapshotV2) -> NativeTrailingViewV2 | None:
    """Bind PositionExecutor's persisted ratchet threshold to the exact execution snapshot.

    The executor remains the only trailing owner. Unarmed state returns ``None`` rather than
    inventing a threshold. PositionExecutor persists the trigger/threshold but not its historical
    peak, so the adapter reports that peak as unavailable instead of reconstructing unsupported
    evidence. The threshold is the native value that actually gates the executor's trailing exit.
    """
    snapshot = checked_snapshot(snapshot)
    recaptured = capture_native_snapshot(
        info, scope=snapshot.scope, sequence=snapshot.sequence,
        observed_at_ms=snapshot.observed_at_ms, available_at_ms=snapshot.available_at_ms,
        reconciled=snapshot.reconciled, fees_complete=snapshot.fees_complete,
        commitments=snapshot.commitments, terminal_requests=snapshot.terminal_requests)
    if recaptured != snapshot:
        raise PolicyError('native trailing projection differs from the bound execution snapshot')
    custom = getattr(info, 'custom_info', None)
    if type(custom) is not dict:
        raise PolicyError('native trailing projection unavailable')
    try:
        state = custom['trailing_state']
    except KeyError as error:
        raise PolicyError('native trailing projection unavailable') from error
    if type(state) is not str:
        raise PolicyError('native trailing state required')
    if state in _UNARMED_STATES:
        return None
    if state not in _ARMED_STATES:
        raise PolicyError('unsupported native trailing state')
    try:
        _decimal(custom['trailing_delta_pct'], 'trailing delta', positive=True)
        _decimal(custom['trailing_stop_trigger_pct'], 'trailing trigger')
        threshold = _decimal(custom['trailing_trigger_price'], 'trailing trigger price', positive=True)
        move_count = custom['trailing_move_count']
        armed_at = custom['trailing_armed_timestamp']
        updated_at = custom['trailing_last_update_timestamp']
    except KeyError as error:
        raise PolicyError('native trailing projection unavailable') from error
    if type(move_count) is not int or type(move_count) is bool or move_count < 0:
        raise PolicyError('native trailing move count is invalid')
    armed_at = _timestamp(armed_at, 'trailing armed timestamp')
    updated_at = _timestamp(updated_at, 'trailing update timestamp')
    if updated_at < armed_at:
        raise PolicyError('native trailing update clock regressed')
    return NativeTrailingViewV2(
        owner_id=snapshot.scope.owner_id,
        executor_id=snapshot.scope.executor_id,
        receipt_sha256=snapshot.receipt_sha256,
        observed_at_ms=snapshot.observed_at_ms,
        activated=True,
        peak_price=None,
        threshold_price=threshold,
        exit_owner='native_executor')
