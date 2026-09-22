"""Lifecycle-only acquisition prerequisite; native sizing, budgets and dispatch stay native."""
from typing import Literal

from pydantic import Field

from controllers.generic.spot_recipe_v2_core.spot_lifecycle_v2 import (
    LifecycleProjectionV2,
    NativeSnapshotV2,
    checked_snapshot,
    project_lifecycle,
)
from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import (
    HASH,
    DecisionCursor,
    DecisionTrace,
    Opportunity,
    Prerequisite,
    decide,
)
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import digest


class AcquisitionAssessmentV2(PolicyModel):
    purpose: Literal['initial_entry', 'dca']
    snapshot_sha256: str = Field(pattern=HASH)
    native_reason: str
    lifecycle: LifecycleProjectionV2
    cursor: DecisionCursor
    trace: DecisionTrace
    economic_backtest: Literal[False] = False
    execution_authorized: Literal[False] = False


def evaluate_acquisition(snapshot: NativeSnapshotV2, previous: DecisionCursor | None,
                         opportunity: Opportunity, *, purpose: Literal['initial_entry', 'dca'],
                         now_ms: int, max_age_ms: int,
                         prerequisites: tuple[Prerequisite, ...]) -> AcquisitionAssessmentV2:
    """Return a canonical cursor to persist, never mutate or reset an existing cursor.

    This only supplies lifecycle eligibility. A passing trace still requires native
    sizing, fee reserves, risk and capital admission, and an admitted runtime adapter.
    """
    snapshot = checked_snapshot(snapshot)
    if previous is not None and type(previous) is not DecisionCursor:
        raise PolicyError('canonical decision cursor required')
    if type(opportunity) is not Opportunity or purpose not in ('initial_entry', 'dca'):
        raise PolicyError('explicit supported acquisition purpose and canonical opportunity required')
    opportunity = Opportunity.model_validate(opportunity.model_dump(mode='python'))
    if opportunity.action != 'acquire':
        raise PolicyError('reductions must not pass through acquisition prerequisites')
    if type(prerequisites) is not tuple or len(prerequisites) > 63:
        raise PolicyError('at most 63 immutable external prerequisites are supported')
    if any(type(p) is not Prerequisite or p.name == 'spot_lifecycle_v2' for p in prerequisites):
        raise PolicyError('canonical prerequisites must not replace the native lifecycle prerequisite')
    scope, proposed = snapshot.scope, opportunity.scope
    if (proposed.program_id, proposed.owner_id, proposed.connector, proposed.trading_pair,
        proposed.recipe_sha256) != (scope.program_id, scope.owner_id, scope.connector, scope.trading_pair,
                                    scope.recipe_binding.recipe_sha256):
        raise PolicyError('opportunity and native scope differ')
    lifecycle = project_lifecycle(snapshot, now_ms=now_ms, max_age_ms=max_age_ms)
    reason = 'eligible'
    if lifecycle.blockers:
        reason = lifecycle.blockers[0]
    elif snapshot.commitments:
        reason = 'native_request_pending'
    elif purpose == 'initial_entry' and lifecycle.phase != 'READY':
        reason = 'initial_entry_requires_ready_scope'
    elif purpose == 'dca' and (lifecycle.phase != 'MANAGING' or not snapshot.entry_terminal):
        reason = 'dca_requires_managing_inventory'
    elif purpose == 'dca' and snapshot.exit_gross_base > 0:
        reason = 'acquisition_after_liquidation_unsupported'
    native = Prerequisite(name='spot_lifecycle_v2', status='PASS' if reason == 'eligible' else 'HOLD',
                          evidence_sha256=digest([purpose, snapshot, reason, now_ms, max_age_ms]))
    cursor, trace = decide(previous, opportunity, now_ms=now_ms, prerequisites=(*prerequisites, native))
    return AcquisitionAssessmentV2(purpose=purpose, snapshot_sha256=snapshot.snapshot_sha256,
                                   native_reason=reason, lifecycle=lifecycle, cursor=cursor, trace=trace)
