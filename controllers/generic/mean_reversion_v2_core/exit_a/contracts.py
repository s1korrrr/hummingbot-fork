"""V2-local immutable envelopes around the canonical native contracts."""
from decimal import Decimal
from math import isfinite
from typing import Literal

from pydantic import Field, StrictBool, field_validator, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import (
    HASH, DecisionCursor, DecisionScope, DecisionTrace, ObservationRef,
)
from hummingbot.strategy_v2.components.exit_policy import ExitProposal, NativeExitView
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, digest


class ExitAContextV2(PolicyModel):
    recipe: RecipeBinding
    scope: DecisionScope
    observation: ObservationRef
    native: NativeExitView
    inventory_revision: int = Field(ge=1, strict=True)
    full_configuration_sha256: str = Field(pattern=HASH)
    decision_time_ms: int = Field(ge=0, strict=True)
    activated: StrictBool
    native_close_pending: StrictBool
    activation_cost_quote: Decimal = Field(gt=0)
    feature_names: tuple[str, ...] = Field(min_length=1, max_length=32)
    feature_values: tuple[float, ...] = Field(min_length=1, max_length=32)

    @field_validator('activation_cost_quote', mode='before')
    @classmethod
    def not_boolean(cls, value):
        if isinstance(value, bool):
            raise ValueError('boolean is not a cost')
        return value

    @field_validator('feature_values', mode='before')
    @classmethod
    def numeric_features(cls, values):
        try:
            valid = isinstance(values, (tuple, list)) and all(type(v) in (int, float) and isfinite(v) for v in values)
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError('finite numeric features required')
        return values

    @model_validator(mode='after')
    def coherent(self):
        if len(self.feature_names) != len(self.feature_values) or len(set(self.feature_names)) != len(self.feature_names):
            raise ValueError('ordered unique feature names required')
        if any(not n or len(n) > 64 or not n.isascii() for n in self.feature_names):
            raise ValueError('bounded feature names required')
        if self.observation.available_time_ms > self.decision_time_ms:
            raise ValueError('future observation')
        if self.native.observed_at_ms > self.decision_time_ms:
            raise ValueError('future native evidence')
        if (self.scope.owner_id, self.scope.connector, self.scope.trading_pair) != (
            self.native.owner_id, self.native.connector, self.native.trading_pair
        ) or self.scope.recipe_sha256 != self.recipe.recipe_sha256:
            raise ValueError('inconsistent native/recipe scope')
        if self.scope.connector not in ('okx', 'okx_paper_trade') or not self.scope.trading_pair.endswith('-USDC'):
            raise ValueError('only the explicit OKX USDC spot lane is supported')
        return self

    @property
    def feature_sha256(self):
        return digest([self.feature_names, self.feature_values])

    @property
    def context_sha256(self):
        return digest(self)

    @property
    def setup_key(self):
        return 'exit_a:' + digest([self.scope.program_id, self.native.owner_id, self.native.executor_id,
                                  self.inventory_revision, self.recipe, 'exit_a_v2'])


class ExitAParametersV2(PolicyModel):
    enabled: StrictBool = False
    recipe: RecipeBinding
    full_configuration_sha256: str = Field(pattern=HASH)
    model_sha256: str = Field(pattern=HASH)
    training_manifest_sha256: str = Field(pattern=HASH)
    margin_bps: Decimal = Field(ge=0)
    max_feature_age_ms: int = Field(ge=1, le=86400000, strict=True)
    max_native_age_ms: int = Field(ge=1, le=60000, strict=True)
    lifetime_ms: int = Field(ge=1, le=86400000, strict=True)

    @field_validator('margin_bps', mode='before')
    @classmethod
    def numeric_margin(cls, value):
        if isinstance(value, bool):
            raise ValueError('boolean is not a margin')
        return value


class ContinuationEstimateV2(PolicyModel):
    context_sha256: str = Field(pattern=HASH)
    feature_sha256: str = Field(pattern=HASH)
    model_sha256: str = Field(pattern=HASH)
    training_manifest_sha256: str = Field(pattern=HASH)
    available: StrictBool
    value_bps: Decimal | None = None
    label_cutoff_ms: int = Field(ge=0, strict=True)
    trained_at_ms: int = Field(ge=0, strict=True)
    expires_at_ms: int = Field(ge=0, strict=True)
    reason: str

    @model_validator(mode='after')
    def valid(self):
        if self.available != (self.value_bps is not None):
            raise ValueError('availability and estimate disagree')
        if self.label_cutoff_ms > self.trained_at_ms or self.trained_at_ms >= self.expires_at_ms:
            raise ValueError('invalid model availability interval')
        return self


class ExitAResultV2(PolicyModel):
    status: Literal['NO_PROPOSAL', 'PROPOSE_REDUCTION'] = 'NO_PROPOSAL'
    reason: str
    cursor: DecisionCursor | None = None
    trace: DecisionTrace | None = None
    proposal: ExitProposal | None = None
    execution_authorized: Literal[False] = False

    @model_validator(mode='after')
    def coherent(self):
        if (self.status == 'PROPOSE_REDUCTION') != (self.proposal is not None):
            raise ValueError('proposal and result status disagree')
        return self
