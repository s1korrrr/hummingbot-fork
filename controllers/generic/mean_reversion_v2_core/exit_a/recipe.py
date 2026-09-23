"""Isolated v2 program/owner/recipe binding. Compatibility profiles are not hosts."""
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import HASH
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import (
    RecipeBinding,
    configuration_digest,
    digest,
    source_digest,
    source_records,
    verify_binding,
)

FORBIDDEN = ('ok_rsi', 'rsi_v5', 'rsi_modular', 'okx_spot_dca')
CONTROLLER_MODULE = 'controllers.generic.mean_reversion_v2_core'
RECIPE_ID = 'exit_a_v2'


def package_root() -> Path:
    return Path(__file__).resolve().parents[1]


def controllers_root() -> Path:
    return Path(__file__).resolve().parents[3]


def implementation_files() -> tuple[Path, ...]:
    root = package_root()
    paths = tuple(sorted(path for path in root.rglob('*.py') if path.is_file() and not path.is_symlink()))
    if not paths:
        raise PolicyError('mean_reversion_v2_core implementation source is empty')
    return paths


def _reject_legacy(value: str, field: str) -> None:
    if value == 'r11' or any(token in value for token in FORBIDDEN):
        raise ValueError(f'{field} cannot inherit a legacy or compatibility identity')


class ExitARecipeV2(PolicyModel):
    schema_version: Literal[1] = 1
    recipe_id: Literal['exit_a_v2'] = RECIPE_ID
    program_id: str = Field(pattern=r'^[A-Za-z0-9_.:-]{1,128}$')
    owner_id: str = Field(pattern=r'^[A-Za-z0-9_.:-]{1,128}$')
    controller_module: Literal['controllers.generic.mean_reversion_v2_core'] = CONTROLLER_MODULE
    connector: Literal['okx', 'okx_paper_trade'] = 'okx'
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-USDC$')
    baseline_recipe_sha256: str = Field(pattern=HASH)
    model_sha256: str = Field(pattern=HASH)
    training_manifest_sha256: str = Field(pattern=HASH)
    execution_enabled: Literal[False] = False

    @field_validator('schema_version', mode='before')
    @classmethod
    def strict_version(cls, value):
        if type(value) is not int:
            raise ValueError('schema version must be an integer')
        return value

    @model_validator(mode='after')
    def isolated(self):
        for field in ('program_id', 'owner_id', 'recipe_id'):
            _reject_legacy(getattr(self, field), field)
        return self


class ExitAOwnerBindingV2(PolicyModel):
    recipe: ExitARecipeV2
    binding: RecipeBinding
    full_configuration_sha256: str = Field(pattern=HASH)
    source_records: tuple[tuple[str, str], ...]

    @model_validator(mode='after')
    def local_sources_are_isolated(self):
        local = [path for path, _ in self.source_records if not path.startswith('@')]
        if not local or any(not path.startswith('generic/mean_reversion_v2_core/') for path in local):
            raise ValueError('implementation source must stay inside mean_reversion_v2_core')
        banned = FORBIDDEN + ('modular_spot_core',)
        if any(token in path for path in local for token in banned):
            raise ValueError('legacy or compatibility sources cannot enter this binding')
        return self


def bind_exit_a_v2(recipe: ExitARecipeV2) -> ExitAOwnerBindingV2:
    recipe = ExitARecipeV2.model_validate(recipe.model_dump(mode='json'))
    root = controllers_root()
    files = implementation_files()
    records = tuple((path, digest) for path, digest in source_records(root, files))
    payload = recipe.model_dump(mode='json')
    source_sha256 = source_digest(root, files)
    binding = RecipeBinding(recipe_id=recipe.recipe_id, recipe_sha256=digest(payload),
                            source_sha256=source_sha256, configuration_sha256=configuration_digest(payload))
    verify_binding(binding, payload, source_sha256=source_sha256)
    return ExitAOwnerBindingV2(recipe=recipe, binding=binding, full_configuration_sha256=digest(payload),
                               source_records=records)
