"""Isolated v2 recipe binding, universe composition and one deterministic decision pass."""
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, StrictBool, model_validator

from hummingbot.strategy_v2.components.capital_policy import CapitalLimits, CapitalSnapshot
from hummingbot.strategy_v2.components.config import AcquisitionPolicy, PolicyModel, RiskPolicy, TrailingPolicy
from hummingbot.strategy_v2.components.decision_protocol import HASH, NAME, DecisionCursor, Opportunity, Prerequisite
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.exit_policy import (
    ExitAssessment,
    ExitProposal,
    NativeExitView,
    NativeProfitFloor,
)
from hummingbot.strategy_v2.components.recipe_contract import (
    RecipeBinding,
    configuration_digest,
    digest,
    source_digest,
    verify_binding,
)
from hummingbot.strategy_v2.components.spot_lifecycle_v2 import (
    LifecycleProjectionV2,
    NativeSnapshotV2,
    checked_snapshot,
    management_pairs,
    project_lifecycle,
)
from hummingbot.strategy_v2.components.universe import UniversePolicy, UniverseSelection, pairs, select_universe

from .acquisition import evaluate_acquisition
from .capital import (
    AcquisitionLedgerV2,
    CapitalAdmissionV2,
    RecorderScopeEvidence,
    StageInputs,
    admit_acquisition,
    size_stage,
)
from .lifecycle import NativeTrailingViewV2, resolve_exit

HOST = 'spot_recipe_v2'


def source_root() -> Path:
    return Path(__file__).resolve().parent.parent


def source_files() -> list[Path]:
    root = source_root()
    return [root / f'{HOST}.py', *sorted((root / f'{HOST}_core').rglob('*.py'))]


class PolicyDependency(PolicyModel):
    policy_id: str = Field(pattern=NAME)
    action: Literal['acquire', 'reduce']
    requires_loss_liquidation: StrictBool = False


class SpotRecipeV2(PolicyModel):
    schema_version: Literal[1] = 1
    recipe_id: str = Field(pattern=r'^[A-Za-z0-9_-]{1,32}$')
    mandate: Literal['profit_only', 'loss_permitted']
    connector: Literal['okx', 'okx_paper_trade']
    execution_mode: Literal['observation', 'shadow', 'paper', 'live']
    interval: str = Field(pattern=r'^[0-9]{1,3}[smhd]$')
    observation_epoch: str = Field(pattern=NAME)
    data_revision: int = Field(ge=1, le=2 ** 31 - 1, strict=True)
    model_identity: str = Field(pattern=NAME)
    capital_capability: Literal['RECORDER_SCOPED'] = 'RECORDER_SCOPED'
    exit_owner: Literal['native_executor'] = 'native_executor'
    quote_budget: Decimal = Field(gt=0)
    universe: UniversePolicy
    acquisition: AcquisitionPolicy
    trailing: TrailingPolicy
    risk: RiskPolicy
    policies: tuple[PolicyDependency, ...] = Field(min_length=1, max_length=64)
    execution_enabled: Literal[False] = False

    @model_validator(mode='after')
    def compatible_mandate_and_owners(self):
        if (self.connector == 'okx_paper_trade') != (self.execution_mode == 'paper'):
            raise ValueError('paper connector and paper execution mode must agree')
        if len({item.policy_id for item in self.policies}) != len(self.policies):
            raise ValueError('duplicate policy identity')
        if self.mandate == 'profit_only' and any(item.requires_loss_liquidation for item in self.policies):
            raise ValueError('a profit-only mandate cannot admit a hard-loss liquidation policy')
        if not any(item.action == 'reduce' for item in self.policies):
            raise ValueError('a spot recipe requires at least one reduction policy')
        pairs(self.universe.symbols)
        return self

    @property
    def recipe_sha256(self) -> str:
        return digest(self.model_dump(mode='json'))

    @property
    def acquisition_allowed(self) -> bool:
        return self.universe.mode != 'sell_only'


def bind_recipe(recipe: SpotRecipeV2, payload) -> RecipeBinding:
    if type(recipe) is not SpotRecipeV2:
        raise PolicyError('exact isolated v2 recipe required')
    recipe = SpotRecipeV2.model_validate(recipe.model_dump(mode='python'))
    if type(payload) is not dict:
        raise PolicyError('exact configuration payload mapping required')
    # configuration_digest excludes CONTROL_FIELDS, so a binding can never prove execution is off.
    if payload.get('execution_enabled') is not False or payload.get('initial_positions'):
        raise PolicyError('an isolated v2 binding requires disabled execution and no adopted inventory')
    binding = RecipeBinding(recipe_id=recipe.recipe_id, recipe_sha256=recipe.recipe_sha256,
                            source_sha256=source_digest(source_root(), source_files()),
                            configuration_sha256=configuration_digest(payload))
    verify_binding(binding, payload, source_sha256=binding.source_sha256)
    return binding


def render_recipe(recipe: SpotRecipeV2, payload) -> dict:
    binding = bind_recipe(recipe, payload)
    return {'recipe': recipe.model_dump(mode='json'), 'recipe_binding': binding.model_dump(mode='json'),
            'receipt_kind': 'spot_recipe_v2_composition', 'source_scope': 'spot_recipe_v2+strategy_components',
            'native_validation': 'NOT_RUN', 'native_runtime_validation': 'NOT_RUN',
            'capital_authority': 'NOT_PROVEN', 'capital_capability': recipe.capital_capability,
            'execution_enabled': False, 'economic_backtest': False}


def compose_universes(recipe: SpotRecipeV2, views: tuple[NativeSnapshotV2, ...], *, available, tradable, scores,
                      blocked, previous, now_ms: int, max_age_ms: int) -> UniverseSelection:
    """Attributed inventory stays in management even when entry is de-selected or paused."""
    if type(recipe) is not SpotRecipeV2 or type(views) is not tuple:
        raise PolicyError('exact isolated v2 recipe and bounded native snapshots required')
    managed = management_pairs(views, now_ms=now_ms, max_age_ms=max_age_ms)
    policy = recipe.universe if recipe.acquisition_allowed else recipe.universe.model_copy(
        update={'mode': 'sell_only', 'observe_all': False})
    selection = select_universe(policy, available=available, tradable=tradable, scores=scores,
                                managed=managed, pending=(), blocked=blocked, previous=previous)
    if not set(managed) <= set(selection.management):
        raise PolicyError('native management universe dropped attributed inventory')
    return selection


class SpotRecipeDecisionV2(PolicyModel):
    recipe_sha256: str = Field(pattern=HASH)
    snapshot_sha256: str = Field(pattern=HASH)
    lifecycle: LifecycleProjectionV2
    exit_reason: str = Field(pattern=NAME)
    exit_assessment: ExitAssessment | None = None
    acquisition_reason: str = Field(pattern=NAME)
    acquisition: CapitalAdmissionV2 | None = None
    execution_authorized: Literal[False] = False
    economic_backtest: Literal[False] = False

    @model_validator(mode='after')
    def managed_inventory_is_always_assessed(self):
        """Nothing may report a managed native position without an exit assessment for it."""
        if self.lifecycle.management_required and self.exit_assessment is None:
            raise ValueError('a managed native position requires an exit assessment')
        return self


def decide_spot_recipe(recipe: SpotRecipeV2, snapshot: NativeSnapshotV2,
                       trailing: tuple[NativeTrailingViewV2, ...], exit_view: NativeExitView,
                       proposals: tuple[ExitProposal, ...], floor: NativeProfitFloor | None, *,
                       now_ms: int, max_age_ms: int, acquisition: dict | None = None,
                       previous_trailing: NativeTrailingViewV2 | None) -> SpotRecipeDecisionV2:
    """Management and exit are assessed first and never suppressed by an entry pause."""
    if type(recipe) is not SpotRecipeV2:
        raise PolicyError('exact isolated v2 recipe required')
    snapshot = checked_snapshot(snapshot)
    if snapshot.scope.connector != recipe.connector or snapshot.scope.execution_mode != recipe.execution_mode:
        raise PolicyError('native scope venue or execution mode differs from the recipe')
    if snapshot.scope.recipe_binding.recipe_sha256 != recipe.recipe_sha256:
        raise PolicyError('native scope is bound to another recipe revision')
    lifecycle = project_lifecycle(snapshot, now_ms=now_ms, max_age_ms=max_age_ms)
    if lifecycle.management_required or proposals:
        assessment = resolve_exit(snapshot, trailing, exit_view, proposals, floor, now_ms=now_ms,
                                  max_age_ms=max_age_ms, policy=recipe.trailing,
                                  previous_trailing=previous_trailing)
        exit_reason = assessment.reason
    else:
        assessment, exit_reason = None, 'no_native_position_to_reduce'
    result = dict(recipe_sha256=recipe.recipe_sha256, snapshot_sha256=snapshot.snapshot_sha256,
                  lifecycle=lifecycle, exit_assessment=assessment, exit_reason=exit_reason)
    if not recipe.acquisition_allowed:
        return SpotRecipeDecisionV2(**result, acquisition_reason='acquisition_paused')
    if acquisition is None:
        return SpotRecipeDecisionV2(**result, acquisition_reason='acquisition_inputs_unavailable')
    admission = _admit(recipe, snapshot, acquisition, now_ms=now_ms, max_age_ms=max_age_ms)
    return SpotRecipeDecisionV2(**result, acquisition_reason=admission.reason, acquisition=admission)


def _admit(recipe: SpotRecipeV2, snapshot: NativeSnapshotV2, inputs: dict, *,
           now_ms: int, max_age_ms: int) -> CapitalAdmissionV2:
    required = {'opportunity', 'cursor', 'prerequisites', 'purpose', 'ledger', 'predecessor_ledger',
                'stage_inputs', 'recorder_evidence', 'capital_snapshot', 'capital_limits'}
    if type(inputs) is not dict or set(inputs) != required:
        raise PolicyError('exact acquisition evidence set required')
    if type(inputs['opportunity']) is not Opportunity or type(inputs['ledger']) is not AcquisitionLedgerV2:
        raise PolicyError('canonical opportunity and admitted ledger required')
    if (type(inputs['stage_inputs']) is not StageInputs
            or type(inputs['recorder_evidence']) is not RecorderScopeEvidence
            or type(inputs['capital_snapshot']) is not CapitalSnapshot
            or type(inputs['capital_limits']) is not CapitalLimits):
        raise PolicyError('canonical stage, recorder and capital evidence required')
    cursor, checks = inputs['cursor'], inputs['prerequisites']
    if cursor is not None and type(cursor) is not DecisionCursor:
        raise PolicyError('canonical decision cursor required')
    if type(checks) is not tuple or any(type(item) is not Prerequisite for item in checks):
        raise PolicyError('canonical prerequisites required')
    primary = inputs['opportunity'].primary
    if (primary.epoch, primary.revision) != (recipe.observation_epoch, recipe.data_revision):
        raise PolicyError('opportunity observation epoch or data revision differs from the recipe')
    eligibility = evaluate_acquisition(snapshot, cursor, inputs['opportunity'], purpose=inputs['purpose'],
                                       now_ms=now_ms, max_age_ms=max_age_ms, prerequisites=checks)
    stage = size_stage(eligibility, snapshot, inputs['ledger'], recipe.acquisition, recipe.risk,
                       inputs['stage_inputs'], predecessor=inputs['predecessor_ledger'])
    return admit_acquisition(stage, inputs['recorder_evidence'], inputs['capital_snapshot'],
                             inputs['capital_limits'], recipe.risk,
                             capability=recipe.capital_capability, now_ms=now_ms)


__all__ = ['HOST', 'PolicyDependency', 'SpotRecipeDecisionV2', 'SpotRecipeV2', 'bind_recipe', 'compose_universes',
           'decide_spot_recipe', 'render_recipe', 'source_files', 'source_root']
