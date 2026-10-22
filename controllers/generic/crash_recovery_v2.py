"""Isolated non-executing crash-recovery host. Native Hummingbot keeps every order and fill."""
from pathlib import Path
from typing import Literal

from pydantic import Field, StrictBool, field_validator, model_validator
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from controllers.generic.crash_recovery_v2_core.recipe import CrashRecoveryRecipe, bind_recipe
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import source_digest
from hummingbot.strategy_v2.components.source_identity import component_source_files
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase

HOST = 'crash_recovery_v2'
CONFIG_CLASS = 'CrashRecoveryV2ControllerConfig'

__all__ = ['CONFIG_CLASS', 'CheckpointStore', 'CrashRecoveryV2Controller', 'CrashRecoveryV2ControllerConfig',
           'HOST', 'bind_host_recipe', 'source_files', 'source_root']


def source_root() -> Path:
    return Path(__file__).resolve().parent


def source_files() -> list[Path]:
    root = source_root()
    return [root / f'{HOST}.py', *sorted((root / f'{HOST}_core').rglob('*.py'))]


def bind_host_recipe(recipe: CrashRecoveryRecipe):
    """Hash installed host+core bytes. Core bind_recipe still accepts a caller-supplied digest."""
    return bind_recipe(recipe, source_digest(source_root(), source_files()))


class CheckpointStore:
    """Recorder Metadata CAS for overlay decision text. Not a funding or inventory ledger."""

    def __init__(self, recorder, owner_id: str, *, metadata_model=None):
        if type(owner_id) is not str or not owner_id.startswith('crash_recovery_v2_'):
            raise PolicyError('checkpoint owner is not this host')
        if metadata_model is None:
            metadata_model = Metadata
        self.model = metadata_model
        self.recorder = recorder
        self.key = 'crash-recovery-v2:' + owner_id

    def load(self):
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                row = session.get(self.model, self.key)
                raw = None if row is None else row.value
            return raw
        except SQLAlchemyError as exc:
            raise PolicyError('crash recovery checkpoint store unavailable') from exc

    def compare_and_swap(self, expected: str | None, raw: str) -> bool:
        if type(raw) is not str or not raw:
            raise PolicyError('bounded checkpoint text required')
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                with session.begin():
                    if expected is None:
                        session.add(self.model(key=self.key, value=raw))
                        session.flush()
                        return True
                    changed = session.execute(update(self.model).where(
                        self.model.key == self.key, self.model.value == expected).values(value=raw))
                    return changed.rowcount == 1
        except IntegrityError:
            return False
        except SQLAlchemyError as exc:
            raise PolicyError('crash recovery checkpoint persistence failed') from exc


class CrashRecoveryV2ControllerConfig(ControllerConfigBase):
    controller_name: Literal['crash_recovery_v2'] = 'crash_recovery_v2'
    controller_type: Literal['generic'] = 'generic'
    id: str = Field(pattern=r'^crash_recovery_v2_[A-Za-z0-9_-]+$')
    connector_name: str = Field(min_length=1)
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$')
    recipe: CrashRecoveryRecipe
    execution_enabled: Literal[False] = False
    acquisition_enabled: StrictBool = Field(default=False, json_schema_extra={'is_updatable': True})

    @field_validator('execution_enabled', mode='before')
    @classmethod
    def unforgeable_disabled_execution(cls, value):
        if value is not False:
            raise ValueError('this isolated v2 host cannot be enabled for execution')
        return value

    @model_validator(mode='after')
    def isolated_market(self):
        if self.initial_positions:
            raise ValueError('unverified inventory adoption is not supported')
        if self.connector_name != self.recipe.dataset.connector:
            raise ValueError('controller connector differs from recipe dataset')
        if self.trading_pair != self.recipe.dataset.trading_pair:
            raise ValueError('controller pair differs from recipe dataset')
        return self


class CrashRecoveryV2Controller(ControllerBase):
    """Publishes overlay diagnostics only. It never returns an executor action."""

    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        return [*source_files(), *component_source_files()]

    async def update_processed_data(self):
        self.processed_data = {'controller_id': self.config.id, 'recipe_sha256': self.config.recipe.fingerprint,
                               'execution_authorized': False, 'executor_actions': 0}

    def determine_executor_actions(self):
        return []

    def validate_executor_config(self, config) -> bool:
        return False

    def entry_permission(self, config, check_funds=True) -> bool:
        return False

    def to_format_status(self):
        return [f'{self.config.id} crash_recovery_v2 non-executing, recipe {self.config.recipe.fingerprint[:12]}']
