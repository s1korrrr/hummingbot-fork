"""Bounded operator projection. An unknown amount is unavailable, never zero.

`bid` and `floor_price` are caller-supplied market context, republished as context and not vouched
for as native evidence. Everything else is either derived here or carried from validated records.
"""
from decimal import Decimal
from typing import Literal

from pydantic import Field, StrictBool, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import HASH, NAME
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import source_digest
from hummingbot.strategy_v2.components.spot_lifecycle_v2 import (
    LifecycleProjectionV2,
    NativeSnapshotV2,
    checked_snapshot,
)

from .recipe import SpotRecipeDecisionV2, SpotRecipeV2, source_files, source_root

NativePhase = LifecycleProjectionV2.model_fields['phase'].annotation


class OperatorProjectionV2(PolicyModel):
    schema_version: Literal[1] = 1
    available: StrictBool
    unavailable_reason: str | None = Field(default=None, pattern=NAME)
    owner_id: str = Field(pattern=NAME)
    executor_id: str = Field(pattern=NAME)
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$', max_length=80)
    execution_mode: Literal['observation', 'shadow', 'paper', 'live']
    recipe_sha256: str | None = Field(default=None, pattern=HASH)
    source_sha256: str | None = Field(default=None, pattern=HASH)
    capital_capability: Literal['RECORDER_SCOPED'] = 'RECORDER_SCOPED'
    native_phase: NativePhase | None = None
    evidence_age_ms: int | None = Field(default=None, ge=0)
    retained_base: Decimal | None = Field(default=None, ge=0)
    requested_base: Decimal | None = Field(default=None, gt=0)
    admitted_quote: Decimal | None = Field(default=None, gt=0)
    floor_price: Decimal | None = Field(default=None, gt=0)
    bid: Decimal | None = Field(default=None, gt=0)
    realized_pnl_quote: None = None
    unrealized_pnl_quote: None = None
    reason_chain: tuple[str, ...] = Field(default=(), max_length=32)
    execution_authorized: Literal[False] = False

    @model_validator(mode='after')
    def unavailable_carries_no_numbers(self):
        if self.available != (self.unavailable_reason is None):
            raise ValueError('an unavailable projection requires exactly one reason')
        numbers = (self.native_phase, self.evidence_age_ms, self.retained_base, self.requested_base,
                   self.admitted_quote, self.floor_price, self.bid)
        if not self.available and any(item is not None for item in numbers):
            raise ValueError('an unavailable projection cannot publish native quantities')
        return self


def project_operator_view(recipe: SpotRecipeV2, decision: SpotRecipeDecisionV2, snapshot: NativeSnapshotV2, *,
                          bid=None, floor_price=None, now_ms: int,
                          max_age_ms: int) -> OperatorProjectionV2:
    if type(recipe) is not SpotRecipeV2 or type(decision) is not SpotRecipeDecisionV2:
        raise PolicyError('exact isolated v2 recipe and decision required')
    snapshot = checked_snapshot(snapshot)
    # model_copy skips model validators, so a decision has to be revalidated at the publication
    # boundary or a managed position can be published with no exit assessment.
    decision = SpotRecipeDecisionV2.model_validate(decision.model_dump(mode='python'))
    if type(now_ms) is not int or type(max_age_ms) is not int or not 1 <= max_age_ms <= 60000:
        raise PolicyError('invalid projection clock or age limit')
    scope = snapshot.scope
    identity = dict(owner_id=scope.owner_id, executor_id=scope.executor_id, trading_pair=scope.trading_pair,
                    execution_mode=scope.execution_mode, capital_capability=recipe.capital_capability)

    def unavailable(reason):
        return OperatorProjectionV2(available=False, unavailable_reason=reason, **identity)

    if decision.snapshot_sha256 != snapshot.snapshot_sha256:
        return unavailable('decision_snapshot_mismatch')
    if decision.recipe_sha256 != recipe.recipe_sha256:
        return unavailable('recipe_mismatch')
    # No venue check here: a decision can only carry both digests if decide_spot_recipe already
    # agreed the scope and the recipe name the same venue and mode.
    age = now_ms - snapshot.observed_at_ms
    if not 0 <= age <= max_age_ms:
        return unavailable('native_evidence_stale')
    reasons = (*decision.lifecycle.blockers, decision.exit_reason, decision.acquisition_reason)
    admission, assessment = decision.acquisition, decision.exit_assessment
    return OperatorProjectionV2(
        available=True, recipe_sha256=recipe.recipe_sha256,
        source_sha256=source_digest(source_root(), source_files()),
        native_phase=decision.lifecycle.phase, evidence_age_ms=age, retained_base=snapshot.remaining_base,
        requested_base=None if assessment is None else assessment.amount_base,
        admitted_quote=None if admission is None else admission.admitted_quote,
        floor_price=floor_price, bid=bid, reason_chain=reasons[:32], **identity)
