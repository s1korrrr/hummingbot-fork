"""Composition seams only: native controllers remain the policy and execution owners."""
import ast
import asyncio
import hashlib
import importlib
import inspect
from copy import deepcopy
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from typing import ClassVar, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, StrictBool, model_validator
from sqlalchemy.exc import SQLAlchemyError

from hummingbot.core.data_type.common import PositionMode, TradeType
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.components.candle_aggregation import derive_higher_timeframe_candles
from hummingbot.strategy_v2.components.candle_data import Dataset
from hummingbot.strategy_v2.components.data_reader import READINESS_SCHEMA, DataPolicy, DataPool
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1
from hummingbot.strategy_v2.models.base import RunnableStatus

from .domain import DataUnavailable, PolicyError
from .native_funding import CandidateFundingPolicy, NativeFundingStore
from .native_recovery import NativeRecoveryStore
from .recipe_contract import RecipeBinding, configuration_digest, source_digest, verify_binding

FAMILIES = {'ok_rsi': 'modular_ok_rsi', 'rsi_v5': 'modular_rsi_v5'}


def source_root():
    return Path(__file__).parents[2]


def source_files(family, *, public_module=None):
    """Controller-source closure, including relative imports and package initializers.

    Native Hummingbot engine dependencies outside controllers are bound by the
    deployment image identity, not misrepresented as part of this source seal.
    """
    root = source_root()
    pending = [root / 'generic' / ((public_module or FAMILIES[family]) + '.py'),
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
        for reference in import_references(path.read_text()):
            if reference[0] == 'from':
                _, level, suffix, names = reference
                module = (*package[:len(package) - level + 1], *suffix) if level else suffix
                enqueue(module)
                for name in names:
                    enqueue((*module, name))
            else:
                enqueue(reference[1])
    return sorted(seen)


_IMPORT_REFERENCES: dict[str, tuple] = {}
_IMPORT_REFERENCE_CAPACITY = 4096


def import_references(text):
    """Import statements of one source text, in ``ast.walk`` order.

    Parsed once per distinct content: the strategy reloads controller configs every
    few seconds and re-derives this closure for every controller, and re-parsing the
    unchanged sources each time stalled the event loop for seconds. Relative imports
    stay unresolved here because their meaning depends on the importing file.
    """
    key = hashlib.sha256(text.encode()).hexdigest()
    cached = _IMPORT_REFERENCES.get(key)
    if cached is not None:
        return cached
    references = []
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.Import):
            references.extend(('import', tuple(alias.name.split('.'))) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            suffix = tuple(node.module.split('.')) if node.module else ()
            references.append(('from', node.level, suffix, tuple(alias.name for alias in node.names)))
        elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
              and node.func.attr == 'import_module' and node.args
              and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)):
            references.append(('import', tuple(node.args[0].value.split('.'))))
    if len(_IMPORT_REFERENCES) >= _IMPORT_REFERENCE_CAPACITY:
        _IMPORT_REFERENCES.clear()
    cached = _IMPORT_REFERENCES[key] = tuple(references)
    return cached


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
    _public_module: ClassVar[str | None] = None

    @property
    def uses_candle_service(self) -> bool:
        # Shared composition candles are owned by NativeCandleProvider, not the
        # direct OK RSI service route inherited through its native controller.
        return False

    execution_mode: Literal['live', 'paper'] = 'live'
    execution_enabled: StrictBool = Field(default=False, json_schema_extra={'is_updatable': True})
    data: DataPolicy = Field(default_factory=DataPolicy)
    shared_history: int | None = Field(default=None, ge=500, le=10000, strict=True)
    recipe_binding: RecipeBinding | None = None
    funding: CandidateFundingPolicy | None = None
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False, validate_default=True)

    @model_validator(mode='after')
    def validate_native_composition(self):
        expected_connector = 'okx_paper_trade' if self.execution_mode == 'paper' else 'okx'
        if (self.connector_name != expected_connector or self.leverage != 1 or self.position_mode != PositionMode.ONEWAY
                or not self.trading_pair.endswith('-USDC')):
            raise ValueError('native modular composition admits OKX USDC spot at 1x only')
        if self.execution_mode == 'paper' and self.candles_connector != 'okx':
            raise ValueError('paper execution requires the explicit public OKX candle owner')
        if self.execution_mode == 'live' and self.execution_enabled and self.funding is None:
            raise ValueError('Enabled live native candidate requires explicit funding allocation and fee bound')
        if self.funding is not None:
            if self.trading_pair.split('-')[0] == 'SUI' or getattr(self, 'trade_mode', 'buy_sell') != 'buy_sell':
                raise ValueError('Initial funded candidate excludes wallet-wide seller assets and sell-only adoption')
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
                           source_sha256=source_digest(source_root(), source_files(self._native_family, public_module=self._public_module)))
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

    def _max_age(self):
        return max(1, int(self.config.max_candle_age_intervals * 60 - 60))

    def _frame_gate_detail(self, now):
        """O(1) mirror of the ``get_candles_df`` failures inside ``entry_ready``; parity is tested.

        Status publication must not rebuild a full candle frame on every report.
        """
        client = self.pool.client
        if client is None:
            return 'shared candle client has not started'
        replica = client.replica
        value = replica.snapshots.get(self.spec.key)
        if not replica.connected or value is None:
            return 'candle reader has not synchronized'
        if not 0 <= now - value.published_at <= replica.max_transport_age:
            return 'candle transport is stale'
        if now < value.published_at or now - (value.rows[-1].open_time + value.dataset.seconds) > self._max_age():
            return 'future or stale candle snapshot'
        if len(value.rows) < max(value.dataset.history, self.spec.history):
            return 'candle history is warming'
        return None

    def readiness(self):
        """Observation only: ``entry_ready``'s two checks with reasons, on the provider clock."""
        now = self.delegate.time()
        return now, [self.pool.readiness(self.spec, now=now, candles_error=self._frame_gate_detail(now))]


class NativeComposition:
    """MRO mixin: native signal, sizing, execution, stop and recovery methods are inherited."""
    _native_family = None
    _public_module = None

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
        self._reader_close_task = None
        self._reader_close_error = None
        super().__init__(config, market_data_provider, actions_queue, **kwargs)
        self._composition_digest = configuration_digest(config.model_dump(mode='json'))
        self._funding_store = None
        self._funding_hold = None
        self._funding_assessment_hold = None
        self._recovery_store = None
        self._recovery_hold = None
        self._recovery_fault = None
        self._native_source_hash = source_digest(source_root(), source_files(
            self._native_family, public_module=self._public_module))

    @classmethod
    def implementation_source_files(cls):
        from hummingbot.strategy_v2.components.source_identity import component_source_files

        return [*source_files(cls._native_family, public_module=cls._public_module), *component_source_files()]

    def _composition_unchanged(self):
        return configuration_digest(self.config.model_dump(mode='json')) == self._composition_digest

    def initialize_candles(self):
        if self.shared_candles is not None:
            self.shared_candles.pool.start()
        super().initialize_candles()

    def start(self):
        if self.status != RunnableStatus.RUNNING:
            self._assert_control_loop_stopped()
            task = getattr(self, '_reader_close_task', None)
            if task is not None and not task.done():
                raise RuntimeError('Modular native candle reader is still stopping; retry after it finishes')
            error = getattr(self, '_reader_close_error', None)
            if error is None and task is not None:
                error = RuntimeError('reader cleanup was cancelled') if task.cancelled() else task.exception()
            if error is not None:
                raise RuntimeError('Modular native candle reader cleanup failed; process restart required') from error
            self._reader_close_task = None
        super().start()

    def on_stop(self):
        try:
            if self.shared_candles is not None:
                close = self.shared_candles.pool.close()
                try:
                    task = asyncio.create_task(close, name='modular-native-candle-close')
                except BaseException:
                    if inspect.getcoroutinestate(close) == inspect.CORO_CREATED:
                        close.close()
                    raise
                self._reader_close_task = task
                task.add_done_callback(self._reader_closed)
        except BaseException as error:
            self._reader_close_error = error
            self.logger().error('Modular native candle reader release failed: %s', error)
            raise
        finally:
            super().on_stop()

    async def control_loop(self):
        try:
            await super().control_loop()
        finally:
            task = getattr(self, '_reader_close_task', None)
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)

    def _reader_closed(self, task):
        error = RuntimeError('reader cleanup was cancelled') if task.cancelled() else task.exception()
        if error is not None:
            self._reader_close_error = error
            self.logger().error('Modular native candle reader did not close cleanly: %s', error)

    async def update_processed_data(self):
        try:
            await super().update_processed_data()
        except DataUnavailable as exc:
            self.processed_data = {'signal': 0, 'features': pd.DataFrame(),
                                   'data_health': {'ready': False, 'reason': type(exc).__name__}}

    def _refresh_funding_assessment(self):
        """Prepare the runtime source binding, then inspect funding before a signal."""
        if self.config.funding is None:
            return
        now = self.market_data_provider.time()
        previous = getattr(self, '_funding_assessed_at', None)
        if previous is not None and 0 <= now - previous < 5:
            return
        self._funding_assessed_at = now
        try:
            if self._funding_store is None:
                from hummingbot.connector.markets_recorder import MarketsRecorder
                recorder = MarketsRecorder.get_existing_instance()
                if recorder is None:
                    raise DataUnavailable('Native recorder required for funding assessment')
                self._funding_store = NativeFundingStore(self, recorder)
            self._funding_store.prepare_runtime_binding()
            self._funding_store.status = self._funding_store.assess()
            self._funding_store.status['observed_at'] = now
            self._funding_assessment_hold = None
        except (PolicyError, ValueError, TypeError, KeyError, ArithmeticError, OSError, RuntimeError, SQLAlchemyError) as error:
            self._funding_assessment_hold = str(error) or type(error).__name__

    STARTUP_RESTORE_GRACE_SECONDS: ClassVar[float] = 60.0

    def _awaiting_startup_restore(self):
        """True only within the startup grace after this controller's first action pass."""
        now = self.market_data_provider.time()
        first = getattr(self, '_first_action_pass_at', None)
        if first is None:
            return False
        return 0 <= now - first < self.STARTUP_RESTORE_GRACE_SECONDS

    def determine_executor_actions(self):
        if getattr(self, '_first_action_pass_at', None) is None:
            self._first_action_pass_at = self.market_data_provider.time()
        self._refresh_funding_assessment()
        if self._recovery_fault:
            self.logger().warning('native recovery fault holds actions: %s', self._recovery_fault)
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
            if not positions and self._held_unsellable_dust():
                self._release_unsellable_entry_fuse()
            before_trails = deepcopy(self._recovery_trails)
            actions = super().determine_executor_actions()
            self._recovery_store.persist(positions, actions)
            self._recovery_hold = None
            return actions
        except DataUnavailable as exc:
            if before_trails is not None:
                self._recovery_trails = before_trails
            self._recovery_hold = str(exc)
            if self._awaiting_startup_restore():
                # The orchestrator restores held positions only once connectors are ready,
                # after controllers start: the hold is correct, the alarm is not.
                self.logger().info('native recovery hold while startup positions restore: %s', self._recovery_hold)
            else:
                self.logger().warning('native recovery hold: %s', self._recovery_hold)
            return []
        except (PolicyError, ValueError, TypeError, KeyError, ArithmeticError, OSError, RuntimeError) as exc:
            if before_trails is not None:
                self._recovery_trails = before_trails
            self._recovery_fault = str(exc)
            self._recovery_hold = self._recovery_fault
            self.logger().warning('native recovery fault holds actions: %s', self._recovery_fault)
            return []

    def _held_unsellable_dust(self):
        for item in self.positions_held or ():
            amount = getattr(item, 'amount', 0)
            if amount is None or amount <= 0:
                continue
            return True
        return False

    def _release_unsellable_entry_fuse(self):
        getter = getattr(self, '_get_entry_decision_gate', None)
        if not callable(getter):
            return
        getter().abandon()

    def _effective_buy_entry_usd(self, state=None, *, entry_fraction=None):
        planned = super()._effective_buy_entry_usd(state, entry_fraction=entry_fraction)
        if self.config.funding is None or not self.config.funding.resize_to_available_quote:
            return planned
        planned = min(planned, self.config.usd_per_entry)
        try:
            if self._funding_store is None:
                from hummingbot.connector.markets_recorder import MarketsRecorder
                recorder = MarketsRecorder.get_existing_instance()
                if recorder is None:
                    raise PolicyError('Native recorder required for candidate funding sizing')
                self._funding_store = NativeFundingStore(self, recorder)
            self._funding_store.prepare_runtime_binding()
            ceiling = self._funding_store.entry_notional_ceiling().quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)
            self._funding_size = {'requested_quote': str(planned), 'ceiling_quote': str(ceiling),
                                  'effective_quote': str(min(planned, ceiling))}
            self._funding_hold = None
            return min(planned, ceiling)
        except (PolicyError, ValueError, TypeError, KeyError, ArithmeticError, OSError, RuntimeError, SQLAlchemyError) as error:
            self._funding_hold = str(error) or type(error).__name__
            return Decimal(0)

    def get_executor_config(self, *args, **kwargs):
        config = super().get_executor_config(*args, **kwargs)
        if self.config.funding is not None and config.side == TradeType.BUY:
            if (self._native_family == 'ok_rsi'
                    and getattr(config, 'entry_acquisition_policy', None) == 'down_only_maker_v1'):
                if 'entry_chain_contract' not in type(config).model_fields:
                    raise PolicyError('V2 bounded acquisition executor contract is unavailable')
                config.entry_chain_contract = ACQUISITION_CHAIN_V1
            if self.config.funding.resize_to_available_quote and config.amount * config.entry_price > self.config.usd_per_entry:
                rule = self._get_trading_rule()
                increment = self._base_amount_increment(rule)
                raw_amount = self.config.usd_per_entry / config.entry_price
                amount = ((raw_amount / increment).to_integral_value(rounding=ROUND_DOWN) * increment
                          if increment > 0 else raw_amount.quantize(Decimal('0.00000001'), rounding=ROUND_DOWN))
                if (amount <= 0 or amount < self._minimum_entry_amount(config.entry_price, rule)
                        or amount * config.entry_price < self._minimum_entry_quote(config.entry_price, rule)):
                    self._funding_hold = 'Native minimum exceeds funded per-entry cap'
                else:
                    config.amount = amount
                    if getattr(self, '_funding_size', None) is not None:
                        self._funding_size['effective_quote'] = str(amount * config.entry_price)
            config.require_terminal_entry_reconciliation = True
            pending = getattr(self, '_down_only_pending_configs', None)
            if isinstance(pending, dict) and config.id in pending:
                pending[config.id] = config.model_copy(deep=True)
        return config

    def update_config(self, new_config):
        if type(new_config) is not type(self.config):
            raise ValueError('Controller profile/type change requires restart and explicit state reconciliation')
        return super().update_config(new_config)

    def can_create_executor(self, signal):
        if signal <= 0:
            return super().can_create_executor(signal)
        if not self.config.execution_enabled:
            reason = 'execution_enabled'
        elif not self._composition_unchanged():
            reason = 'composition_unchanged'
        elif self.shared_candles is not None and not self.shared_candles.entry_ready():
            reason = 'entry_ready'
        else:
            return super().can_create_executor(signal)
        self.logger().warning('native executor create refused: %s', reason)
        return False

    def create_action_refused(self, action):
        """The engine dropped this queued create before calling validate_executor_config.

        Every refusal inside the validator already releases the reserved entry decision;
        this covers the refusals that never reach it. The release re-arms only when no
        executor row exists.
        """
        if getattr(getattr(action, 'executor_config', None), 'side', None) == TradeType.BUY:
            release = getattr(self, '_release_entry_decision', None)
            if callable(release):
                release()

    def validate_executor_config(self, config):
        if (config.side == TradeType.BUY and self.config.funding is not None
                and self._native_family == 'ok_rsi'
                and getattr(config, 'entry_acquisition_policy', None) == 'down_only_maker_v1'):
            try:
                from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import (
                    DownOnlyMakerPositionExecutor,
                    DownOnlyMakerPositionExecutorConfig,
                )
                from hummingbot.strategy_v2.executors import planned_position_resume
                from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
                if (type(config) is not DownOnlyMakerPositionExecutorConfig
                        or config.entry_chain_contract != ACQUISITION_CHAIN_V1
                        or not issubclass(DownOnlyMakerPositionExecutor, PositionExecutor)
                        or ACQUISITION_CHAIN_V1 not in
                        planned_position_resume.SUPPORTED_ACQUISITION_CHAIN_CONTRACTS):
                    raise PolicyError('V2 bounded acquisition execution/funding/recovery contract differs')
            except (AttributeError, ImportError, PolicyError) as error:
                self._funding_hold = str(error) or type(error).__name__
                release = getattr(self, '_release_entry_decision', None)
                if callable(release):
                    release()
                return False
        if (config.side == TradeType.BUY and self.config.funding is not None
                and self.config.funding.resize_to_available_quote
                and not self._funding_hold
                and (getattr(config, 'amount', None) is None or getattr(config, 'entry_price', None) is None
                     or config.amount * config.entry_price > self.config.usd_per_entry)):
            self._funding_hold = 'Funded per-entry cap exceeded'
        sizing_hold = (config.side == TradeType.BUY and self.config.funding is not None
                       and self.config.funding.resize_to_available_quote and self._funding_hold)
        assessment_hold = config.side == TradeType.BUY and self._funding_assessment_hold
        if self._recovery_hold or self._recovery_fault or sizing_hold or assessment_hold:
            if config.side == TradeType.BUY:
                release = getattr(self, '_release_entry_decision', None)
                if callable(release):
                    release()
            return False
        if config.side == TradeType.BUY and (not self.config.execution_enabled or not self._composition_unchanged()
                                             or self.shared_candles is not None and not self.shared_candles.entry_ready()):
            release = getattr(self, '_release_entry_decision', None)
            if callable(release):
                release()
            return False
        if not super().validate_executor_config(config):
            if config.side == TradeType.BUY:
                release = getattr(self, '_release_entry_decision', None)
                if callable(release):
                    release()
            return False
        if config.side == TradeType.BUY and self.config.funding is not None:
            try:
                if self._funding_store is None:
                    from hummingbot.connector.markets_recorder import MarketsRecorder
                    recorder = MarketsRecorder.get_existing_instance()
                    if recorder is None:
                        raise PolicyError('Native recorder required for candidate funding')
                    self._funding_store = NativeFundingStore(self, recorder)
                committed = getattr(self, '_committed_create_intent', None)
                self._funding_store.admit(
                    config,
                    committed_intent_config=(config if isinstance(committed, tuple)
                                             and len(committed) == 2
                                             and committed[0] == config.id
                                             and committed[1] is config else None))
                self._funding_hold = None
            except Exception as error:
                self._funding_hold = str(error) or type(error).__name__
                self.logger().warning('Funding admission refused %s: %s', config.id, self._funding_hold)
                release = getattr(self, '_release_entry_decision', None)
                if callable(release):
                    release()
                return False
        return True

    def _entry_admission_status(self):
        for label, reason in (('recovery', self._recovery_fault or self._recovery_hold),
                              ('funding', self._funding_assessment_hold or self._funding_hold),
                              ('entry_decision', getattr(self, '_entry_decision_error', None))):
            if reason:
                return {'state': 'blocked', 'reason': f'{label}: {reason}'}
        if not self.config.execution_enabled:
            return {'state': 'blocked', 'reason': 'execution_disabled'}
        if not self._composition_unchanged():
            return {'state': 'blocked', 'reason': 'composition_changed'}
        if self.config.funding is not None and (self._funding_store is None or not self._funding_store.status):
            return {'state': 'unverified', 'reason': 'funding_unverified'}
        return {'state': 'conditional', 'reason': None}

    def get_status_summary(self):
        summary = super().get_status_summary()
        admission = self._entry_admission_status()
        if admission['reason']:
            summary['gate'] = admission['reason']
        summary['entry_admission'] = admission
        return summary

    def get_custom_info(self):
        info = super().get_custom_info()
        info['entry_admission'] = self._entry_admission_status()
        info['native_buy_gate'] = info.get('buy_gate')
        if info['entry_admission']['reason']:
            info['buy_gate'] = info['entry_admission']['reason']
        info['modular_composition'] = {'native_family': self._native_family,
                                       'execution_mode': self.config.execution_mode,
                                       'data_mode': self.config.data.mode,
                                       'transport': self.config.data.transport,
                                       'execution_enabled': self.config.execution_enabled,
                                       'recovery_hold': self._recovery_hold,
                                       'funding_hold': self._funding_hold,
                                       'funding_assessment_hold': self._funding_assessment_hold,
                                       'funding_sizing': getattr(self, '_funding_size', None),
                                       'funding': self._funding_store.status if self._funding_store else None}
        if hasattr(self.config, 'profile'):
            info['modular_composition']['profile'] = self.config.profile
            info['modular_composition']['controller_name'] = self.config.controller_name
        info['candle_readiness'] = self._candle_readiness()
        return info

    def _candle_readiness(self):
        """Projection of the shared-candle BUY gate; never raises and never changes a decision."""
        projection = {'schema': READINESS_SCHEMA, 'status': 'available', 'reason': None,
                      'data_mode': self.config.data.mode, 'transport': self.config.data.transport,
                      'observed_at': None, 'clock': 'controller_market_data_provider_time', 'datasets': []}
        if self.shared_candles is None:
            projection.update(status='not_applicable', reason='native_candles_not_replica_gated')
            return projection
        try:
            projection['observed_at'], projection['datasets'] = self.shared_candles.readiness()
        except Exception as error:
            projection.update(status='unavailable', reason=f'projection_error: {type(error).__name__}',
                              observed_at=None, datasets=[])
        return projection


def module_for(family):
    if family not in FAMILIES:
        raise PolicyError('unsupported native composition family')
    return importlib.import_module('controllers.generic.' + FAMILIES[family])
