"""Composition seams only: native controllers remain the policy and execution owners."""
import ast
import asyncio
import importlib
from copy import deepcopy
from pathlib import Path
from typing import ClassVar, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, StrictBool, model_validator

from controllers.directional_trading.ok_rsi_core.candles import derive_higher_timeframe_candles
from hummingbot.core.data_type.common import PositionMode, TradeType
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig

from .candle_data import Dataset
from .data_reader import DataPolicy, DataPool
from .domain import DataUnavailable, PolicyError
from .native_recovery import NativeRecoveryStore
from .recipe_contract import RecipeBinding, configuration_digest, source_digest, verify_binding

FAMILIES = {'ok_rsi': 'modular_ok_rsi', 'rsi_v5': 'modular_rsi_v5'}


def source_root():
    return Path(__file__).parents[2]


def source_files(family):
    """Controller-source closure, including relative imports and package initializers.

    Native Hummingbot engine dependencies outside controllers are bound by the
    deployment image identity, not misrepresented as part of this source seal.
    """
    root = source_root()
    pending = [root / 'generic' / (FAMILIES[family] + '.py'),
               root / 'directional_trading' / (family + '.py'),
               *sorted((root / 'generic' / 'modular_spot_core').rglob('*.py'))]
    seen = set()

    def enqueue(module):
        if module and module[0] == 'controllers':
            path = root.joinpath(*module[1:])
            for candidate in (path.with_suffix('.py'), path / '__init__.py'):
                if candidate.is_file() and candidate not in seen:
                    pending.append(candidate)

    while pending:
        path = pending.pop()
        if path in seen:
            continue
        seen.add(path)
        if len(seen) > 1000:
            raise PolicyError('native controller dependency closure exceeds bound')
        relative = path.relative_to(root)
        package = ('controllers', *relative.parts[:-1])
        for count in range(1, len(package) + 1):
            initializer = root.joinpath(*package[1:count]) / '__init__.py'
            if initializer.is_file() and initializer not in seen:
                pending.append(initializer)
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    enqueue(tuple(alias.name.split('.')))
            elif isinstance(node, ast.ImportFrom):
                suffix = tuple(node.module.split('.')) if node.module else ()
                module = (*package[:len(package) - node.level + 1], *suffix) if node.level else suffix
                enqueue(module)
                for alias in node.names:
                    enqueue((*module, alias.name))
            elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                  and node.func.attr == 'import_module' and node.args
                  and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)):
                enqueue(tuple(node.args[0].value.split('.')))
    return sorted(seen)


def base_records(config):
    return max(500, config.bb_length * 3, config.macd_slow * 3, config.rsi_length * 5)


def native_candle_configs(config, family):
    records = base_records(config)
    intervals = [config.interval]
    if config.trend_confirmation_enabled:
        intervals.extend(value for value in (config.trend_confirmation_medium_interval,
                                             config.trend_confirmation_long_interval)
                         if value and value not in intervals)
    if family == 'ok_rsi':
        if config.episode_enabled:
            from controllers.directional_trading.ok_rsi_core.context import HISTORY_RECORDS
            records = max(records, HISTORY_RECORDS)
        elif config.trend_confirmation_enabled:
            ratio = max(CandlesBase.interval_to_seconds[value] // CandlesBase.interval_to_seconds[config.interval]
                        for value in intervals)
            records = max(records, ratio * config.derived_higher_timeframe_max_records)
        intervals = [config.interval]
    return [CandlesConfig(connector=config.candles_connector or config.connector_name,
                          trading_pair=config.candles_trading_pair or config.trading_pair,
                          interval=interval, max_records=records) for interval in dict.fromkeys(intervals)]


def required_shared_history(config, family):
    required = 0
    for feed in native_candle_configs(config, family):
        seconds = CandlesBase.interval_to_seconds[feed.interval]
        if seconds < 60 or seconds % 60:
            raise ValueError('shared native composition requires whole-minute candle intervals')
        ratio = seconds // 60
        required = max(required, feed.max_records * ratio + ratio - 1)
    return required


class NativeCompositionFields(BaseModel):
    """Additional fields; all strategy parameters come from the actual native schema."""
    _native_family: ClassVar[str]
    execution_mode: Literal['live', 'paper'] = 'live'
    execution_enabled: StrictBool = Field(default=False, json_schema_extra={'is_updatable': True})
    data: DataPolicy = Field(default_factory=DataPolicy)
    shared_history: int | None = Field(default=None, ge=500, le=10000, strict=True)
    recipe_binding: RecipeBinding | None = None
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False, validate_default=True)

    @model_validator(mode='after')
    def validate_native_composition(self):
        expected_connector = 'okx_paper_trade' if self.execution_mode == 'paper' else 'okx'
        if (self.connector_name != expected_connector or self.leverage != 1 or self.position_mode != PositionMode.ONEWAY
                or not self.trading_pair.endswith('-USDC')):
            raise ValueError('native modular composition admits OKX USDC spot at 1x only')
        if self.execution_mode == 'paper' and self.candles_connector != 'okx':
            raise ValueError('paper execution requires the explicit public OKX candle owner')
        controls = {'mode', 'transport', 'origin', 'token_file'}
        if self.data.model_dump(exclude=controls) != DataPolicy().model_dump(exclude=controls):
            raise ValueError('native composition does not consume generic screening or source-profile settings')
        if self.initial_positions or getattr(self, 'legacy_controller_ids', ()):
            raise ValueError('native modular composition does not adopt another owner inventory')
        if not self.id.startswith('modular_spot_'):
            raise ValueError('a separate modular_spot_ native owner identity is required')
        if not self.candles_config:
            self.candles_config = native_candle_configs(self, self._native_family)
        if self.data.mode == 'shared':
            if (self.interval != '1m' or self.data.source_profile != 'native'
                    or (self.candles_connector or self.connector_name) != 'okx'
                    or (self.candles_trading_pair or self.trading_pair) != self.trading_pair
                    or self.data.symbols):
                raise ValueError('shared native composition requires one native OKX 1m base dataset')
            needed = required_shared_history(self, self._native_family)
            history = self.shared_history or needed
            if history < needed or history > 10000:
                raise ValueError(f'shared history must cover all native windows: required={needed}, maximum=10000')
            if self.data.transport == 'snapshot' and history > 5000:
                raise ValueError('HTTP snapshot transport admits at most 5000 candles; use stream')
        elif self.shared_history is not None:
            raise ValueError('shared_history requires shared data mode')
        if self.recipe_binding is not None:
            verify_binding(self.recipe_binding, self.model_dump(mode='json'),
                           source_sha256=source_digest(source_root(), source_files(self._native_family)))
        return self


class NativeCandleProvider:
    """Replace candle reads only; prices, balances and native connectivity delegate unchanged."""

    def __init__(self, delegate, config, family):
        self.delegate = delegate
        self.config = config
        self.spec = Dataset(connector='okx', trading_pair=config.trading_pair,
                            history=config.shared_history or required_shared_history(config, family))
        self.pool = DataPool(config.data, clock=delegate.time)
        self.pool.attach((self.spec,))

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def initialize_candles_feed(self, candles_config):
        self._scope(candles_config.connector, candles_config.trading_pair, candles_config.interval)
        return None

    def _scope(self, connector_name, trading_pair, interval):
        seconds = CandlesBase.interval_to_seconds.get(interval)
        if connector_name != 'okx' or trading_pair != self.spec.trading_pair or not seconds or seconds % 60:
            raise DataUnavailable('unsubscribed native candle namespace')
        return seconds

    def get_candles_df(self, connector_name, trading_pair, interval, max_records):
        seconds = self._scope(connector_name, trading_pair, interval)
        if type(max_records) is not int or max_records < 1:
            raise DataUnavailable('invalid native candle history request')
        now = self.delegate.time()
        max_age = max(1, int(self.config.max_candle_age_intervals * 60 - 60))
        snapshot = self.pool.snapshot(self.spec, now=now, max_age=max_age)
        frame = snapshot.frame(now=now, max_age=max_age)
        if seconds != 60:
            frame = derive_higher_timeframe_candles(frame, base_interval='1m', target_interval=interval,
                                                    current_time=now, max_records=max_records)
        if len(frame) < max_records:
            raise DataUnavailable('shared native candle history is warming')
        return frame.tail(max_records).copy(deep=True)

    def entry_ready(self):
        try:
            self.get_candles_df('okx', self.spec.trading_pair, '1m', self.spec.history)
            return self.pool.entry_ready(self.spec)
        except (DataUnavailable, PolicyError):
            return False


class NativeComposition:
    """MRO mixin: native signal, sizing, execution, stop and recovery methods are inherited."""
    _native_family = None

    def __init__(self, config, market_data_provider, actions_queue, **kwargs):
        # Revalidate before a native constructor may use any strategy parameter.
        config = type(config).model_validate(config.model_dump(mode='json'))
        if config.execution_mode == 'paper':
            from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
            connector = getattr(market_data_provider, 'connectors', {}).get(config.connector_name)
            if not is_native_paper_connector(connector):
                raise ValueError('paper composition requires the isolated native paper connector')
        self.shared_candles = None
        if config.data.mode == 'shared':
            self.shared_candles = NativeCandleProvider(market_data_provider, config, self._native_family)
            market_data_provider = self.shared_candles
        super().__init__(config, market_data_provider, actions_queue, **kwargs)
        self._composition_digest = configuration_digest(config.model_dump(mode='json'))
        self._recovery_store = None
        self._recovery_hold = None
        self._recovery_fault = None
        self._native_source_hash = source_digest(source_root(), source_files(self._native_family))

    @classmethod
    def implementation_source_files(cls):
        return source_files(cls._native_family)

    def _composition_unchanged(self):
        return configuration_digest(self.config.model_dump(mode='json')) == self._composition_digest

    def initialize_candles(self):
        if self.shared_candles is not None:
            self.shared_candles.pool.start()
        super().initialize_candles()

    def on_stop(self):
        if self.shared_candles is not None:
            asyncio.create_task(self.shared_candles.pool.close(), name='modular-native-candle-close')
        super().on_stop()

    async def update_processed_data(self):
        try:
            await super().update_processed_data()
        except DataUnavailable as exc:
            self.processed_data = {'signal': 0, 'features': pd.DataFrame(),
                                   'data_health': {'ready': False, 'reason': type(exc).__name__}}

    def determine_executor_actions(self):
        if self._recovery_fault:
            return []
        before_trails = None
        try:
            if self._recovery_store is None:
                from hummingbot.connector.markets_recorder import MarketsRecorder
                recorder = MarketsRecorder.get_existing_instance()
                if recorder is None:
                    raise DataUnavailable('native recorder required for recovery-safe action dispatch')
                self._recovery_store = NativeRecoveryStore(self, recorder, self._native_source_hash)
            positions = self._recovery_store.prepare()
            before_trails = deepcopy(self._recovery_trails)
            actions = super().determine_executor_actions()
            self._recovery_store.persist(positions, actions)
            self._recovery_hold = None
            return actions
        except DataUnavailable as exc:
            if before_trails is not None:
                self._recovery_trails = before_trails
            self._recovery_hold = str(exc)
            return []
        except (PolicyError, ValueError, TypeError, KeyError, ArithmeticError, OSError, RuntimeError) as exc:
            if before_trails is not None:
                self._recovery_trails = before_trails
            self._recovery_fault = str(exc)
            self._recovery_hold = self._recovery_fault
            return []

    def can_create_executor(self, signal):
        if signal > 0 and (not self.config.execution_enabled or not self._composition_unchanged()
                           or self.shared_candles is not None and not self.shared_candles.entry_ready()):
            return False
        return super().can_create_executor(signal)

    def validate_executor_config(self, config):
        if self._recovery_hold or self._recovery_fault:
            return False
        if config.side == TradeType.BUY and (not self.config.execution_enabled or not self._composition_unchanged()
                                             or self.shared_candles is not None and not self.shared_candles.entry_ready()):
            return False
        return super().validate_executor_config(config)

    def get_custom_info(self):
        info = super().get_custom_info()
        info['modular_composition'] = {'native_family': self._native_family,
                                       'execution_mode': self.config.execution_mode,
                                       'data_mode': self.config.data.mode,
                                       'transport': self.config.data.transport,
                                       'execution_enabled': self.config.execution_enabled,
                                       'recovery_hold': self._recovery_hold}
        return info


def module_for(family):
    if family not in FAMILIES:
        raise PolicyError('unsupported native composition family')
    return importlib.import_module('controllers.generic.' + FAMILIES[family])
