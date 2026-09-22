"""Canonical B0/C1 observation host. Native paper execution remains unqualified."""
import math
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator

from controllers.generic.momentum_fusion_v2_core.features import FeaturePipeline
from controllers.generic.momentum_fusion_v2_core.recipe import MomentumRecipe, bind_recipe, scope_for
from controllers.generic.momentum_fusion_v2_core.setups import step
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.components.data_reader import DataPolicy
from hummingbot.strategy_v2.components.domain import DataUnavailable, PolicyError
from hummingbot.strategy_v2.components.recipe_contract import source_digest
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase


def source_root():
    return Path(__file__).resolve().parent


def source_files():
    root = source_root()
    return [Path(__file__).resolve(), *sorted((root / 'momentum_fusion_v2_core').glob('*.py')),
            *sorted((root / 'momentum_fusion_v2_runtime').glob('*.py'))]


class MomentumFusionV2Config(ControllerConfigBase):
    controller_name: Literal['momentum_fusion_v2'] = 'momentum_fusion_v2'
    controller_type: Literal['generic'] = 'generic'
    id: str = Field(pattern=r'^momentum_v2_[A-Za-z0-9_-]+$')
    connector_name: Literal['okx', 'okx_paper_trade'] = 'okx'
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-USDC$')
    execution_mode: Literal['observation', 'shadow', 'paper'] = 'observation'
    execution_enabled: Literal[False] = False
    recipe: MomentumRecipe
    data: DataPolicy = Field(default_factory=DataPolicy)

    @field_validator('execution_enabled', mode='before')
    @classmethod
    def disabled_only(cls, value):
        if value is not False:
            raise ValueError('momentum native paper admission remains unqualified')
        return value

    @model_validator(mode='after')
    def bound_scope(self):
        expected = 'okx_paper_trade' if self.execution_mode == 'paper' else 'okx'
        if self.connector_name != expected or self.trading_pair != self.recipe.symbol:
            raise ValueError('momentum mode/connector/recipe binding differs')
        if self.initial_positions:
            raise ValueError('momentum cannot adopt inventory before native qualification')
        return self

    def get_controller_class(self):
        return MomentumFusionV2Controller


class MomentumFusionV2Controller(ControllerBase):
    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        from hummingbot.strategy_v2.components.source_identity import component_source_files
        return [*source_files(), *component_source_files()]

    def __init__(self, config, market_data_provider, actions_queue, **kwargs):
        config = MomentumFusionV2Config.model_validate(config.model_dump(mode='json'))
        super().__init__(config, market_data_provider, actions_queue, **kwargs)
        self._source_sha256 = source_digest(source_root(), source_files())
        self._binding = bind_recipe(config.recipe, source_sha256=self._source_sha256)
        self._pipeline = FeaturePipeline(config.recipe)
        self._state = None
        self._observed_at = None

    def get_candles_config(self):
        return [CandlesConfig(connector=d.connector, trading_pair=d.trading_pair,
                              interval=d.interval, max_records=d.history) for d in self.config.recipe.datasets]

    async def update_processed_data(self):
        self._observed_at = None
        self.processed_data = {'data_health': {'ready': False}, 'action_reason': 'native_paper_admission_unqualified'}
        try:
            now = self.market_data_provider.time()
            if type(now) not in (int, float) or not math.isfinite(now) or now < 0:
                raise DataUnavailable('native observation clock unavailable')
            self._observed_at = now
            if source_digest(source_root(), source_files()) != self._source_sha256:
                raise PolicyError('momentum source identity changed')
            getter = getattr(self.market_data_provider, 'get_candle_snapshot', None)
            if getter is None:
                raise DataUnavailable('native publication-bound candle snapshots unavailable')
            fixture = getattr(self, '_paper_fixture', None)
            if fixture is not None:
                fixture.observe()
                return
            snapshots = {d.key: getter(d) for d in self.config.recipe.datasets}
            observed = self._pipeline.evaluate(snapshots, now_ms=int(now * 1000))
            # Paper mode can observe the canonical shadow policy but is never an
            # admitted paper execution adapter. That limitation is projected below.
            mode = 'shadow' if self.config.execution_mode == 'paper' else self.config.execution_mode
            scope = scope_for(self.config.recipe, program_id='momentum_v2_reference', owner_id=self.config.id, mode=mode)
            self._state, decision = step(self.config.recipe, self._state, observed, scope=scope,
                                         now_ms=int(now * 1000), prerequisites=())
            self.processed_data.update(data_health={'ready': True}, decision=decision.model_dump(mode='json'),
                                       signal=0, observation_mode=mode)
        except (ValueError, TypeError, AttributeError, KeyError, DataUnavailable, PolicyError) as error:
            self.processed_data['data_health']['reason'] = str(error)

    def determine_executor_actions(self):
        fixture = getattr(self, '_paper_fixture', None)
        if fixture is not None:
            return fixture.actions()
        self.processed_data['action_reason'] = 'native_paper_admission_unqualified'
        return []

    def validate_executor_config(self, config):
        fixture = getattr(self, '_paper_fixture', None)
        return fixture is not None and fixture.validate(config)

    def commit_entry_executor(self, config):
        fixture = getattr(self, '_paper_fixture', None)
        if fixture is None:
            raise PolicyError('native offline fixture admission required')
        fixture.committed(config)

    def get_custom_info(self):
        current = source_digest(source_root(), source_files())
        return {'momentum_fusion_v2': {
            'controller_name': self.config.controller_name, 'controller_id': self.config.id,
            'trading_pair': self.config.trading_pair, 'execution_mode': self.config.execution_mode,
            'execution_enabled': False, 'observed_at': self._observed_at,
            'offline_fixture': getattr(self, '_paper_fixture', None) is not None,
            'source_sha256': current if current == self._source_sha256 else None,
            'recipe_sha256': self._binding.recipe_sha256,
            'data_ready': self.processed_data.get('data_health', {}).get('ready') is True,
            'data_reason': self.processed_data.get('data_health', {}).get('reason'),
            'recovery_hold': getattr(self, '_recovery_hold', None),
            'action_reason': 'native_paper_admission_unqualified', 'execution_authorized': False}}

    def get_status_summary(self):
        return self.get_custom_info()['momentum_fusion_v2']
