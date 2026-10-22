"""Isolated non-executing v2 spot host. Native Hummingbot keeps every order and fill."""
from typing import Literal

from pydantic import Field, StrictBool, field_validator

from controllers.generic.spot_recipe_v2_core.recipe import SpotRecipeV2, source_files, source_root
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase

CONFIG_CLASS = 'SpotRecipeV2ControllerConfig'

__all__ = ['CONFIG_CLASS', 'SpotRecipeV2Controller', 'SpotRecipeV2ControllerConfig', 'source_files', 'source_root']


class SpotRecipeV2ControllerConfig(ControllerConfigBase):
    controller_name: Literal['spot_recipe_v2'] = 'spot_recipe_v2'
    controller_type: Literal['generic'] = 'generic'
    id: str = Field(pattern=r'^spot_recipe_v2_[A-Za-z0-9_-]+$')
    connector_name: str = Field(min_length=1)
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$')
    recipe: SpotRecipeV2
    execution_enabled: Literal[False] = False
    acquisition_enabled: StrictBool = Field(default=False, json_schema_extra={'is_updatable': True})

    @field_validator('execution_enabled', mode='before')
    @classmethod
    def unforgeable_disabled_execution(cls, value):
        if value is not False:
            raise ValueError('this isolated v2 host cannot be enabled for execution')
        return value


class SpotRecipeV2Controller(ControllerBase):
    """Publishes decision diagnostics only. It never returns an executor action."""

    async def update_processed_data(self):
        self.processed_data = {'controller_id': self.config.id, 'recipe_sha256': self.config.recipe.recipe_sha256,
                               'execution_authorized': False, 'executor_actions': 0}

    def determine_executor_actions(self):
        return []

    def validate_executor_config(self, config) -> bool:
        return False

    def to_format_status(self):
        return [f'{self.config.id} spot_recipe_v2 non-executing, recipe {self.config.recipe.recipe_sha256[:12]}']
