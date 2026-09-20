"""Recipes for complete existing native controller policies, without synthetic policy aliases."""
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from .config import PolicyModel
from .data_reader import DataPolicy
from .domain import PolicyError
from .recipe_contract import canonical_json, digest, source_records


class NativePolicyRecipe(PolicyModel):
    schema_version: Literal[1] = 1
    name: str = Field(pattern=r'^[A-Za-z0-9_-]{1,32}$')
    family: Literal['ok_rsi', 'rsi_v5']
    controller_id: str = Field(pattern=r'^modular_spot_[A-Za-z0-9_-]{1,100}$')
    parameters: dict[str, Any] = Field(default_factory=dict, max_length=300)
    data: DataPolicy = Field(default_factory=DataPolicy)
    shared_history: int | None = Field(default=None, ge=500, le=10000, strict=True)

    @field_validator('schema_version', mode='before')
    @classmethod
    def strict_version(cls, value):
        if type(value) is not int:
            raise ValueError('schema version must be an integer')
        return value

    @model_validator(mode='after')
    def complete(self):
        reserved = {'profile', 'id', 'controller_name', 'controller_type', 'execution_enabled', 'data', 'shared_history',
                    'execution_mode', 'recipe_binding', 'initial_positions', 'legacy_controller_ids'}
        if reserved & set(self.parameters):
            raise ValueError('native recipe parameters cannot override identity, transport, authority or inventory')
        if len(canonical_json(self.model_dump(mode='json'))) > 65536:
            raise ValueError('native recipe exceeds the bounded configuration size')
        return self

    @property
    def digest(self):
        return digest(self.model_dump(mode='json'))


def render_native_recipe(recipe):
    from controllers.generic import rsi_modular as native

    from .candle_data import Dataset
    from .native_composition import required_shared_history
    recipe = NativePolicyRecipe.model_validate(recipe.model_dump(mode='json'))
    config_type = native.get_config_class(recipe.family)
    config = config_type.model_validate({**recipe.parameters, 'profile': recipe.family, 'id': recipe.controller_id,
                                        'execution_enabled': False, 'data': recipe.data,
                                         'shared_history': recipe.shared_history})
    if config.execution_enabled or config.initial_positions:
        raise PolicyError('native recipe must remain disabled without inventory adoption')
    datasets = []
    if config.data.mode == 'shared':
        datasets.append(Dataset(connector='okx', trading_pair=config.trading_pair,
                                history=config.shared_history or required_shared_history(config, recipe.family)))
    root = native.source_root()
    manifest = dict(source_records(root, native.source_files(recipe.family)))
    return {'schema_version': 1, 'controllers': [config.model_dump(mode='json')],
            'candle_manifest': {'schema_version': 1, 'datasets': [spec.model_dump(mode='json') for spec in datasets]},
            'recipe': recipe.model_dump(mode='json'), 'recipe_sha256': recipe.digest,
            'receipt_kind': 'native_policy_composition_v2',
            'source_scope': 'controllers+strategy_components', 'source_files': manifest,
            'native_validation': 'NOT_RUN', 'native_runtime_validation': 'NOT_RUN',
            'capital_authority': 'NOT_PROVEN', 'account_topology': 'single_native_owner',
            'inventory_adoption_supported': False, 'execution_authorized': False,
            'sell_only_handoff': 'UNSUPPORTED'}
