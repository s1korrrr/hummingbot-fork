"""Versioned policy binding only; not a native controller compiler or export approval."""
from typing import Annotated, Literal

from pydantic import Field, model_validator

from hummingbot.strategy_v2.components.recipe_contract import (
    RecipeBinding, configuration_digest, digest, verify_binding,
)

from .snapshot import ContextModel, ContextRequirementsV2, Fraction, Hash, Name, Nonnegative, Number, checked


class ReductionRuleV2(ContextModel):
    name: Name
    return_5m_at_most: Annotated[Number, Field(ge=-1)] | None = None
    breadth_at_most: Fraction | None = None
    volatility_at_least: Nonnegative | None = None
    multiplier: Fraction

    @model_validator(mode='after')
    def has_predicate(self):
        if all(value is None for value in
               (self.return_5m_at_most, self.breadth_at_most, self.volatility_at_least)):
            raise ValueError('a rule needs an explicit economic predicate')
        return self

    @property
    def predicates(self):
        return (('return_5m_fraction', self.return_5m_at_most, 'le'),
                ('breadth_fraction', self.breadth_at_most, 'le'),
                ('realized_vol_15m_fraction', self.volatility_at_least, 'ge'))


class ContextRecipeV2(ContextModel):
    schema_version: Literal['mr_context.recipe.v2'] = 'mr_context.recipe.v2'
    recipe_id: Annotated[str, Field(strict=True, pattern=r'^[A-Za-z0-9_-]{1,32}$')]
    revision: Annotated[int, Field(strict=True, ge=1)]
    baseline_recipe_sha256: Hash
    acquisition_kind: Literal['initial_entry'] = 'initial_entry'
    mode: Literal['off', 'shadow', 'restrict'] = 'off'
    requirements: ContextRequirementsV2
    rules: tuple[ReductionRuleV2, ...] = Field(default=(), max_length=8)

    @model_validator(mode='after')
    def explicit_rules(self):
        if self.mode != 'off' and not self.rules:
            raise ValueError('shadow/restrict requires explicit preregistered rules')
        if len({item.name for item in self.rules}) != len(self.rules):
            raise ValueError('duplicate reduction rule identity')
        return self

    @property
    def recipe_sha256(self) -> str:
        return digest(self.model_dump(mode='json'))


def bind_policy(recipe: ContextRecipeV2, *, source_sha256: str) -> RecipeBinding:
    """Bind a policy envelope to supplied source evidence, never a live configuration."""
    recipe = checked(recipe, ContextRecipeV2)
    return RecipeBinding(recipe_id=recipe.recipe_id, recipe_sha256=recipe.recipe_sha256,
                         source_sha256=source_sha256,
                         configuration_sha256=configuration_digest(recipe.model_dump(mode='json')))


def verify_policy_binding(recipe: ContextRecipeV2, binding: RecipeBinding, *, source_sha256: str) -> None:
    recipe = checked(recipe, ContextRecipeV2)
    binding = checked(binding, RecipeBinding)
    if (binding.recipe_id, binding.recipe_sha256) != (recipe.recipe_id, recipe.recipe_sha256):
        raise ValueError('recipe identity does not match its binding')
    verify_binding(binding, recipe.model_dump(mode='json'), source_sha256=source_sha256)
