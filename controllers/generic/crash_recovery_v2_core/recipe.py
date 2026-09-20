"""Versioned closed-candle acquisition overlay, not a deployable controller config."""
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Literal

from pydantic import Field, StrictBool, field_validator, model_validator

from hummingbot.strategy_v2.components.candle_data import Dataset
from hummingbot.strategy_v2.components.config import AcquisitionPolicy, PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import HASH, NAME, DecisionScope
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.market_stress_v1 import StressParameters, bounded_decimal, checked
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, configuration_digest, digest, verify_binding
from hummingbot.strategy_v2.components.recovery_evidence_v1 import RecoveryParameters

REQUIRED_ACQUISITION_CHECKS = ('data', 'funding', 'native_admission', 'ownership', 'risk')


class CrashRecoveryRecipe(PolicyModel):
    schema_version: Literal[1]
    recipe_id: str = Field(pattern=r'^[A-Za-z0-9_-]{1,32}$')
    program_id: str = Field(pattern=NAME)
    owner_id: str = Field(pattern=NAME)
    dataset: Dataset
    baseline_recipe_sha256: str = Field(pattern=HASH)
    exit_policy_sha256: str = Field(pattern=HASH)
    capital_model: Literal['spot_cash_inventory_1x']
    quote_budget: Decimal = Field(gt=0)
    mode: Literal['off', 'stress_only', 'recovery']
    stress: StressParameters
    recovery: RecoveryParameters
    acquisition: AcquisitionPolicy
    lifetime_ms: int = Field(ge=1, le=86_400_000, strict=True)
    execution_enabled: StrictBool

    _amount = field_validator('quote_budget', mode='before')(bounded_decimal)

    @field_validator('schema_version', mode='before')
    @classmethod
    def integer_schema(cls, value):
        if type(value) is not int:
            raise ValueError('integer schema version required')
        return value

    @model_validator(mode='after')
    def supported(self):
        if self.execution_enabled:
            raise ValueError('native execution adapter is not implemented')
        if (self.dataset.connector != 'okx' or self.dataset.source_profile != 'native'
                or self.dataset.interval != '1m' or self.dataset.trading_pair.split('-')[1] != 'USDC'):
            raise ValueError('only explicit native OKX one-minute USDC spot is supported')
        if self.dataset.history > self.stress.max_history_rows:
            raise ValueError('dataset exceeds stress capacity')
        if self.lifetime_ms > min(self.stress.max_age_ms, self.recovery.eligibility_ttl_ms):
            raise ValueError('opportunity lifetime exceeds evidence bounds')
        fractions = [Fraction(bounded_decimal(x)) for x in self.acquisition.fractions]
        if not 1 <= len(fractions) <= 5 or any(x <= 0 for x in fractions) or sum(fractions) != 1:
            raise ValueError('exact bounded acquisition fractions required')
        return self

    @property
    def fingerprint(self):
        return digest(self.model_dump(mode='json'))

    @property
    def scope(self):
        return DecisionScope(program_id=self.program_id, owner_id=self.owner_id,
                             connector=self.dataset.connector, trading_pair=self.dataset.trading_pair,
                             recipe_sha256=self.fingerprint, acquisition_allowed=True)


def validate_recipe(recipe):
    # Canonical AcquisitionPolicy uses Decimal sum. Isolate its validation context.
    with localcontext() as context:
        context.prec = 100
        return checked(CrashRecoveryRecipe, recipe)


def bind_recipe(recipe: CrashRecoveryRecipe, source_sha256: str) -> RecipeBinding:
    """Bind caller-verified source identity; this does not inspect installed bytes."""
    recipe = validate_recipe(recipe)
    return RecipeBinding(recipe_id=recipe.recipe_id, recipe_sha256=recipe.fingerprint,
                         source_sha256=source_sha256,
                         configuration_sha256=configuration_digest(recipe.model_dump(mode='json')))


def validate_binding(recipe, binding, expected_source_sha256):
    recipe = validate_recipe(recipe)
    binding = checked(RecipeBinding, binding)
    if binding.recipe_id != recipe.recipe_id or binding.recipe_sha256 != recipe.fingerprint:
        raise PolicyError('recipe identity differs from binding')
    verify_binding(binding, recipe.model_dump(mode='json'), source_sha256=expected_source_sha256)
    return recipe, binding
