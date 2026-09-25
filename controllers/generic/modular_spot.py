"""Standalone spot composition using native Hummingbot execution ownership."""
import asyncio
import time
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, Field, StrictBool, model_validator

from controllers.generic.modular_spot_core.bag import AdaptiveTrailingConfig, BagConfig, BuyGapConfig, LiveCapitalConfig
from controllers.generic.modular_spot_core.candle_data import FIRST_DELIVERY
from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy, TrailingPolicy
from controllers.generic.modular_spot_core.data_reader import DataPolicy, attach_pool, dataset_for
from controllers.generic.modular_spot_core.domain import identity
from controllers.generic.modular_spot_core.program_budget import BudgetPolicy
from controllers.generic.modular_spot_core.recipe_contract import (
    RecipeBinding,
    source_digest,
    source_records,
    verify_binding,
)
from controllers.generic.modular_spot_core.runtime import ModularRuntime
from controllers.generic.modular_spot_core.universe import UniversePolicy
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.components.source_identity import component_source_files
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.models.base import RunnableStatus

CONFIG_CLASS = 'ModularSpotControllerConfig'


def source_root():
    return Path(__file__).parent


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
    account: str = Field(default='unassigned', pattern=r'^[A-Za-z0-9_-]{1,80}$')
    program_revision: str = Field(default='0', pattern=r'^[A-Za-z0-9._-]{1,64}$')
    policy_revision: str = Field(default='0', pattern=r'^[A-Za-z0-9._-]{1,64}$')
    recipe_binding: RecipeBinding | None = None
    # Live OK RSI parity (opt-in): held-inventory bags and continuous adaptive trailing.
    bag: BagConfig | None = None
    adaptive_trailing: AdaptiveTrailingConfig | None = None
    capital: LiveCapitalConfig | None = None  # live OK RSI capital model; requires bag
    buy_gap: BuyGapConfig | None = None  # live ATR-scaled buy gap; requires capital
    # 'protected' keeps the modular no-loss floor raised by activation - delta; 'live_no_loss'
    # uses the plain after-fee no-loss price like live OK RSI.
    exit_floor: Literal['protected', 'live_no_loss'] = 'protected'
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False, validate_default=True)

    @model_validator(mode='after')
    def validate_composition(self):
        if self.initial_positions:
            raise ValueError('unverified inventory adoption is not supported')
        if self.interval not in CandlesBase.interval_to_seconds:
            raise ValueError('unsupported native candle interval')
        required = self.signal.required_history
        if self.max_records < required + 1:
            raise ValueError('insufficient feature history plus open candle')
        if self.capital is not None and self.bag is None:
            raise ValueError('live capital requires the bag lifecycle')
        if self.buy_gap is not None and self.capital is None:
            raise ValueError('the live buy gap requires live capital')
        if self.bag is not None or self.adaptive_trailing is not None:
            if self.signal.kind not in {'rsi_rebound', 'rsi_turn'} or self.universe.mode != 'fixed':
                raise ValueError('bag and adaptive trailing require a fixed-universe RSI entry profile')
            if self.interval != '1m':
                raise ValueError('live OK RSI parity features are defined on 1m candles')
            records = (self.bag or BagConfig()).required_records()
            if self.max_records < records + 1:
                raise ValueError(f'bag/adaptive features need max_records >= {records + 1}')
        if self.signal.kind == 'breakout' and len(self.acquisition.fractions) != 1:
            raise ValueError('breakout profile does not permit dip averaging')
        if self.budget is not None and self.total_amount_quote > self.budget.max_quote:
            raise ValueError('pair allocation exceeds program allocation')
        if self.universe.mode == 'sell_only' and self.acquisition_enabled:
            raise ValueError('sell-only requires acquisition_enabled=false')
        if self.data.mode == 'shared':
            if (self.interval != FIRST_DELIVERY['interval'] or self.connector_name != FIRST_DELIVERY['connector']):
                raise ValueError('shared first delivery is confirmed 1m OKX spot')
            dataset_for(self)
            if self.data.transport == 'stream':
                symbols = (self.trading_pair, *self.universe.symbols, *self.universe.benchmarks, *self.data.symbols)
                if self.data.source_profile != 'native' or any(not pair.endswith('-USDC') for pair in symbols):
                    raise ValueError('stream transport requires native OKX USDC spot datasets')
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
        universe = dict(values['universe'])
        universe['symbols'] = []
        universe['benchmarks'] = []
        values['universe'] = universe
        data = dict(values['data'])
        data['symbols'] = []
        values['data'] = data
        return identity({'configuration': values, 'sources': self.source_manifest(), 'policy_version': 2})

    def source_manifest(self):
        """Source records bound into the decision fingerprint; a binding module extends this closure."""
        return source_records(source_root(), source_files())

    def update_markets(self, markets):
        return markets.add_or_update(self.connector_name, self.trading_pair)


class ModularSpotController(ControllerBase):
    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        return [*source_files(), *component_source_files()]

    @property
    def performance_report(self):
        return getattr(self, '_native_report', None)

    @performance_report.setter
    def performance_report(self, value):
        self._native_report = value
        self._native_report_received_at = time.monotonic()

    def native_report_age(self):
        received = getattr(self, '_native_report_received_at', None)
        if received is None:
            return float('inf')
        return time.monotonic() - received

    def __init__(self, config: ModularSpotControllerConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.runtime = ModularRuntime(self, bindings=self._runtime_bindings())
        self._operator_entry_paused = False
        self._operator_state = {'entry_paused': False, 'reason': None, 'scope': None,
                                'updated_at': None, 'last_command_id': None}

    def _runtime_bindings(self):
        """Native effect bindings for the runtime; None selects the deployment defaults."""
        return None

    def set_operator_entry_pause(self, enabled, *, reason=None, scope=None, command_id=None, updated_at=None):
        if type(enabled) is not bool:
            raise ValueError('Operator pause must be a boolean')
        self._operator_entry_paused = enabled
        self._operator_state = {'entry_paused': enabled, 'reason': reason, 'scope': scope,
                                'updated_at': updated_at if updated_at is not None else time.time(),
                                'last_command_id': command_id}

    def get_operator_state(self):
        return dict(self._operator_state)

    def start(self):
        if self.config.data.mode == 'shared' and self.runtime.pool is None:
            self.runtime.pool = attach_pool(self.market_data_provider, self.config)
        super().start()

    def stop(self):
        super().stop()
        task = self.runtime.close_data()
        if task is not None:
            task.add_done_callback(self._data_closed)

    async def control_loop(self):
        try:
            await super().control_loop()
        finally:
            task = self.runtime.close_data()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)

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
                               'data_health': {'ready': market is not None, 'reason': self.runtime.reason},
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

    def get_custom_info(self) -> dict:
        """Bounded, read-only metadata for the native performance-report publisher."""
        return {
            'operator': self.get_operator_state(),
            'modular_status': self.runtime.reason.split(':', 1)[0][:80],
            'candle_open': self.processed_data.get('candle_open'),
            'execution_enabled': self.config.execution_enabled,
            'acquisition_enabled': self.config.acquisition_enabled,
            'universe_mode': self.config.universe.mode,
            'universe_coverage': None if self.runtime.coverage is None else str(self.runtime.coverage),
            'data_source': self.config.data.mode,
            'recipe_binding': None if self.config.recipe_binding is None
            else self.config.recipe_binding.model_dump(mode='json'),
            'bag': self.runtime.bag_status,
        }

    def entry_permission(self, config, *, check_funds=True):
        return super().validate_executor_config(config) and self.runtime.entry_permission(config, check_funds=check_funds)

    def validate_executor_config(self, config):
        if config is None or config.id in self.runtime.dispatched:
            return False
        from controllers.generic.modular_spot_core.bag import is_recovery_sell
        if is_recovery_sell(config):
            return super().validate_executor_config(config) and self.runtime.recovery_permission(config)
        if self.config.universe.mode == 'sell_only':
            return super().validate_executor_config(config) and self.runtime.reduction_permission(config)
        return self.entry_permission(config)

    def get_executor_class_for_config(self, config):
        if not super().validate_executor_config(config):
            raise ValueError('controller is stopped; executor creation forbidden')
        return self.runtime.executor_class(config)

    def to_format_status(self):
        return [f'Modular spot | {self.config.id} | {self.config.connector_name}:{self.config.trading_pair}',
                f'Execution: {self.config.execution_enabled} | Acquisitions: {self.config.acquisition_enabled}',
                f'Data: {self.config.data.mode} | {self.runtime.reason} | Coverage: {self.runtime.coverage}',
                'Native per-tranche ownership; held inventory requires explicit reconciliation.'
                if self.config.bag is None else f'Bag: {self.runtime.bag_status}']
