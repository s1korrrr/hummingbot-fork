"""Standalone spot composition using native Hummingbot execution ownership."""
import hashlib
from decimal import Decimal
from pathlib import Path
import time
from typing import Literal

from pydantic import ConfigDict, Field, StrictBool, model_validator

from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.models.base import RunnableStatus

from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy, TrailingPolicy
from controllers.generic.modular_spot_core.data_reader import DataPolicy, attach_pool, dataset_for
from controllers.generic.modular_spot_core.domain import identity
from controllers.generic.modular_spot_core.program_budget import BudgetPolicy
from controllers.generic.modular_spot_core.recipe_contract import RecipeBinding, source_digest, verify_binding
from controllers.generic.modular_spot_core.runtime import ModularRuntime
from controllers.generic.modular_spot_core.universe import UniversePolicy


def source_files():
    return [Path(__file__), *sorted((Path(__file__).parent / 'modular_spot_core').rglob('*.py'))]


class ModularSpotControllerConfig(ControllerConfigBase):
    controller_name: Literal['modular_spot'] = 'modular_spot'
    controller_type: Literal['generic'] = 'generic'
    id: str = Field(pattern=r'^modular_spot_[A-Za-z0-9_-]+$')
    connector_name: str = Field(min_length=1)
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$')
    total_amount_quote: Decimal = Field(default=Decimal('100'), gt=0, json_schema_extra={'is_updatable': False})
    execution_enabled: StrictBool = False
    acquisition_enabled: StrictBool = Field(default=True, json_schema_extra={'is_updatable': True})
    interval: str = '1m'
    max_records: int = Field(default=300, ge=60, le=3000, strict=True)
    max_candle_age_seconds: int = Field(default=120, ge=1, strict=True)
    max_native_report_age_seconds: int = Field(default=5, ge=1, le=60, strict=True)
    signal: SignalPolicy = Field(default_factory=SignalPolicy)
    acquisition: AcquisitionPolicy = Field(default_factory=AcquisitionPolicy)
    trailing: TrailingPolicy = Field(default_factory=TrailingPolicy)
    risk: RiskPolicy = Field(default_factory=RiskPolicy)
    data: DataPolicy = Field(default_factory=DataPolicy)
    universe: UniversePolicy = Field(default_factory=UniversePolicy)
    budget: BudgetPolicy | None = None
    recipe_binding: RecipeBinding | None = None
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False, validate_default=True)

    @model_validator(mode='after')
    def validate_composition(self):
        if self.initial_positions:
            raise ValueError('unverified inventory adoption is not supported')
        if self.interval not in CandlesBase.interval_to_seconds:
            raise ValueError('unsupported native candle interval')
        required = max(60, 2 * self.signal.rsi_length + 2, self.signal.breakout_lookback + 2)
        if self.max_records < required + 1:
            raise ValueError('insufficient feature history plus open candle')
        if self.signal.kind == 'breakout' and len(self.acquisition.fractions) != 1:
            raise ValueError('breakout profile does not permit dip averaging')
        if self.budget is not None and self.total_amount_quote > self.budget.max_quote:
            raise ValueError('pair allocation exceeds program allocation')
        if self.universe.mode == 'sell_only' and self.acquisition_enabled:
            raise ValueError('sell-only requires acquisition_enabled=false')
        if self.data.mode == 'shared':
            dataset_for(self)
        if self.universe.mode == 'impulse':
            if self.data.mode != 'shared' or self.budget is None or self.signal.kind != 'breakout':
                raise ValueError('impulse requires shared data, a program budget and breakout policy')
            if not self.data.symbols or not self.universe.symbols:
                raise ValueError('observation and acquisition universes must be explicitly admitted')
            if not set(self.universe.symbols) <= set(self.data.symbols):
                raise ValueError('acquisition universe exceeds observed instruments')
            if self.trading_pair not in self.universe.symbols:
                raise ValueError('pair is outside the configured acquisition universe')
        if self.recipe_binding is not None:
            verify_binding(self.recipe_binding, self.model_dump(mode='json'),
                           source_sha256=source_digest(Path(__file__).parent, source_files()))
        return self

    def interval_seconds(self):
        return CandlesBase.interval_to_seconds[self.interval]

    def fingerprint(self):
        values = self.model_dump(mode='json', exclude={'manual_kill_switch', 'execution_enabled', 'acquisition_enabled'})
        sources = [(p.relative_to(Path(__file__).parent).as_posix(), hashlib.sha256(p.read_bytes()).hexdigest())
                   for p in source_files()]
        return identity({'configuration': values, 'sources': sources, 'policy_version': 2})

    def update_markets(self, markets):
        return markets.add_or_update(self.connector_name, self.trading_pair)


class ModularSpotController(ControllerBase):
    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        return source_files()

    @property
    def performance_report(self):
        return getattr(self, '_native_report', None)

    @performance_report.setter
    def performance_report(self, value):
        self._native_report = value
        self._native_report_received_at = time.monotonic()

    def native_report_age(self):
        return time.monotonic() - self._native_report_received_at

    def __init__(self, config: ModularSpotControllerConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.runtime = ModularRuntime(self)

    def start(self):
        if self.config.data.mode == 'shared' and self.runtime.pool is None:
            self.runtime.pool = attach_pool(self.market_data_provider, self.config)
        super().start()

    def stop(self):
        super().stop()
        task = self.runtime.close_data()
        if task is not None:
            task.add_done_callback(self._data_closed)

    def _data_closed(self, task):
        if not task.cancelled() and task.exception() is not None:
            self.logger().error('Modular candle reader did not close cleanly: %s', task.exception())

    def get_candles_config(self):
        if self.config.data.mode == 'shared':
            return []  # Never also start the native upstream candle subscription.
        return [CandlesConfig(connector=self.config.connector_name, trading_pair=self.config.trading_pair,
                              interval=self.config.interval, max_records=self.config.max_records)]

    async def update_processed_data(self):
        self.runtime.start_data()
        market = self.runtime.observe()
        self.processed_data = {'modular_status': self.runtime.reason,
                               'candle_open': None if market is None else market.candle_open,
                               'execution_enabled': self.config.execution_enabled,
                               'acquisition_enabled': self.config.acquisition_enabled,
                               'universe_mode': self.config.universe.mode,
                               'universe_coverage': self.runtime.coverage,
                               'data_source': self.config.data.mode,
                               'recipe_binding': None if self.config.recipe_binding is None
                               else self.config.recipe_binding.model_dump(mode='json')}

    def determine_executor_actions(self):
        if self.status != RunnableStatus.RUNNING:
            return []
        actions = self.runtime.actions()
        self.processed_data['modular_status'] = self.runtime.reason
        return actions

    def entry_permission(self, config, *, check_funds=True):
        return super().validate_executor_config(config) and self.runtime.entry_permission(config, check_funds=check_funds)

    def validate_executor_config(self, config):
        return config is not None and config.id not in self.runtime.dispatched and self.entry_permission(config)

    def get_executor_class_for_config(self, config):
        if not super().validate_executor_config(config):
            raise ValueError('controller is stopped; executor creation forbidden')
        return self.runtime.executor_class(config)

    def to_format_status(self):
        return [f'Modular spot | {self.config.id} | {self.config.connector_name}:{self.config.trading_pair}',
                f'Execution: {self.config.execution_enabled} | Acquisitions: {self.config.acquisition_enabled}',
                f'Data: {self.config.data.mode} | {self.runtime.reason} | Coverage: {self.runtime.coverage}',
                'Native per-tranche ownership; held inventory requires explicit reconciliation.']
