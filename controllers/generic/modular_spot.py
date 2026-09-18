"""Standalone spot composition; no existing strategy controller is inherited."""
import hashlib
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, Field, StrictBool, model_validator

from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.models.base import RunnableStatus

from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy, TrailingPolicy
from controllers.generic.modular_spot_core.domain import identity
from controllers.generic.modular_spot_core.runtime import ModularRuntime


def source_files():
    return [Path(__file__), *sorted((Path(__file__).parent / 'modular_spot_core').rglob('*.py'))]


class ModularSpotControllerConfig(ControllerConfigBase):
    # Native discovery resolves the controller in this configuration's module.
    controller_name: Literal['modular_spot'] = 'modular_spot'
    controller_type: Literal['generic'] = 'generic'
    id: str = Field(pattern=r'^modular_spot_[A-Za-z0-9_-]+$')
    connector_name: str = Field(min_length=1)
    trading_pair: str = Field(pattern=r'^[A-Z0-9]+-[A-Z0-9]+$')
    total_amount_quote: Decimal = Field(default=Decimal('100'), gt=0, json_schema_extra={'is_updatable': False})
    execution_enabled: StrictBool = False
    interval: str = '1m'
    max_records: int = Field(default=300, ge=60, le=3000, strict=True)
    max_candle_age_seconds: int = Field(default=120, ge=1, strict=True)
    signal: SignalPolicy = Field(default_factory=SignalPolicy)
    acquisition: AcquisitionPolicy = Field(default_factory=AcquisitionPolicy)
    trailing: TrailingPolicy = Field(default_factory=TrailingPolicy)
    risk: RiskPolicy = Field(default_factory=RiskPolicy)
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False, validate_default=True)

    @model_validator(mode='after')
    def validate_composition(self):
        if self.initial_positions:
            raise ValueError('inventory adoption is not supported')
        if self.interval not in CandlesBase.interval_to_seconds:
            raise ValueError('unsupported native candle interval')
        required = max(60, 2 * self.signal.rsi_length + 2, self.signal.breakout_lookback + 2)
        if self.max_records < required + 1:
            raise ValueError('insufficient feature history plus open candle')
        if self.signal.kind == 'breakout' and len(self.acquisition.fractions) != 1:
            raise ValueError('breakout profile does not permit dip averaging')
        return self

    def interval_seconds(self):
        return CandlesBase.interval_to_seconds[self.interval]

    def fingerprint(self):
        values = self.model_dump(mode='json', exclude={'manual_kill_switch', 'execution_enabled'})
        sources = [(p.relative_to(Path(__file__).parent).as_posix(),
                    hashlib.sha256(p.read_bytes()).hexdigest()) for p in source_files()]
        return identity({'configuration': values, 'sources': sources, 'policy_version': 1})

    def update_markets(self, markets):
        return markets.add_or_update(self.connector_name, self.trading_pair)


class ModularSpotController(ControllerBase):
    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        return source_files()

    def __init__(self, config: ModularSpotControllerConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.runtime = ModularRuntime(self)

    def get_candles_config(self):
        return [CandlesConfig(connector=self.config.connector_name, trading_pair=self.config.trading_pair,
                              interval=self.config.interval, max_records=self.config.max_records)]

    async def update_processed_data(self):
        market = self.runtime.observe()
        self.processed_data = {'modular_status': self.runtime.reason,
                               'candle_open': None if market is None else market.candle_open,
                               'execution_enabled': self.config.execution_enabled}

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
                f'Execution enabled: {self.config.execution_enabled} | {self.runtime.reason}',
                'Native per-tranche ownership; held inventory requires explicit reconciliation.']
