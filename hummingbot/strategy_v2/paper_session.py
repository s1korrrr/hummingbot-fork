"""Explicitly stepped native controller/executor session with no networking.

An execution fixture, not an economic backtester: callers must supply separately
observed candles, book depth and public trades with their availability timestamps.
The normal native orchestrator remains responsible for executor and held inventory
accounting. Unfinished executor recovery keeps the native reconciliation hold.
"""
import asyncio
import copy
import fcntl
import hashlib
import json
import os
import platform
from pathlib import Path

import numpy as np
import pandas as pd

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.order_executor.data_types import OrderExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction, StoreExecutorAction


class _InjectedRates(RateSourceBase):
    @property
    def name(self):
        return 'native_paper_injected'

    async def get_prices(self, quote_token=None):
        raise RuntimeError('Paper conversion rates must be explicitly injected; no fetch path')


class InjectedPaperMarketData(MarketDataProvider):
    """Native market-data methods, with candle reads and clock explicitly injected."""

    def __init__(self, connector):
        if not is_native_paper_connector(connector):
            raise ValueError('Exact native paper connector required')
        super().__init__({connector.name: connector})
        self._paper_connector = connector
        self._frames = {}
        self._requested = {}

    def time(self):
        return self._paper_connector.current_timestamp

    def initialize_rate_sources(self, connector_pairs):
        if any(p.connector_name != self._paper_connector.name for p in connector_pairs):
            raise ValueError('Unexpected paper rate source')

    def _create_non_trading_connector(self, connector_name):
        raise RuntimeError('Paper session never constructs another connector')

    def initialize_candles_feed(self, config):
        if config.connector != 'okx' or config.trading_pair not in self._paper_connector.trading_pairs:
            raise ValueError('Unexpected paper candle source')
        self._requested[(config.connector, config.trading_pair, config.interval)] = config.max_records

    def validate_candles_input(self, connector_name, trading_pair, interval, frame, *, available_at, current_timestamp=None):
        now = self.time() if current_timestamp is None else current_timestamp
        if connector_name != 'okx' or trading_pair not in self._paper_connector.trading_pairs:
            raise ValueError('Unexpected paper candle source')
        if not np.isfinite(available_at) or available_at > now or available_at < 0:
            raise ValueError('Candle availability is future or invalid')
        seconds = CandlesBase.interval_to_seconds.get(interval)
        if seconds is None or seconds < 60 or seconds % 60:
            raise ValueError('Whole-minute native candle interval required')
        required = {'timestamp', 'open', 'high', 'low', 'close', 'volume'}
        if not isinstance(frame, pd.DataFrame) or not required.issubset(frame.columns) or frame.empty:
            raise ValueError('Native candle columns are required')
        numeric = frame[list(required)].astype(float)
        timestamps = numeric['timestamp']
        if (not np.isfinite(numeric.to_numpy()).all() or not timestamps.is_monotonic_increasing
                or timestamps.duplicated().any() or (timestamps + seconds > available_at).any()
                or (timestamps < 0).any() or (timestamps % seconds != 0).any()
                or (timestamps.diff().dropna() != seconds).any()
                or (numeric[['open', 'high', 'low', 'close']] <= 0).any().any()
                or (numeric['volume'] < 0).any()
                or (numeric['low'] > numeric[['open', 'close']].min(axis=1)).any()
                or (numeric['high'] < numeric[['open', 'close']].max(axis=1)).any()):
            raise ValueError('Non-finite, unordered, duplicated, or unavailable candles')
        if 'confirmed' in frame and not frame['confirmed'].map(lambda value: isinstance(value, (bool, np.bool_)) and bool(value)).all():
            raise ValueError('Only confirmed candles are admitted to paper replay')
        for field in ('quote_asset_volume', 'n_trades', 'taker_buy_base_volume', 'taker_buy_quote_volume'):
            if field in frame:
                values = pd.to_numeric(frame[field], errors='raise')
                if not np.isfinite(values.to_numpy()).all() or (values < 0).any():
                    raise ValueError('Invalid native candle volume/count')
        return frame.copy(deep=True)

    def set_candles(self, connector_name, trading_pair, interval, frame, *, available_at):
        validated = self.validate_candles_input(connector_name, trading_pair, interval, frame, available_at=available_at)
        key = (connector_name, trading_pair, interval)
        self._frames[key] = validated
        if not hasattr(self, '_frame_publications'):
            self._frame_publications = {}
        prior = self._frame_publications.get(key, (0, 0))
        self._frame_publications[key] = (prior[0] + 1, available_at)

    def get_candle_snapshot(self, dataset):
        from hummingbot.strategy_v2.components.candle_data import Candle, CandleSnapshot
        frame = self.get_candles_df(dataset.connector, dataset.trading_pair, dataset.interval, dataset.history)
        key = (dataset.connector, dataset.trading_pair, dataset.interval)
        sequence, available = self._frame_publications[key]
        return CandleSnapshot(dataset=dataset, epoch='injected-paper', sequence=sequence, published_at=available,
                              rows=tuple(Candle(open_time=int(row.timestamp), open=row.open, high=row.high,
                                                low=row.low, close=row.close, volume=row.volume)
                                         for row in frame.itertuples(index=False)))

    def get_candles_df(self, connector_name, trading_pair, interval, max_records=500):
        from controllers.generic.modular_spot_core.domain import DataUnavailable
        frame = self._frames.get((connector_name, trading_pair, interval))
        if frame is None or len(frame) < max_records:
            raise DataUnavailable('Injected native candle history is unavailable')
        return frame.tail(max_records).copy(deep=True)


def _runtime_identity():
    root = Path(__file__).parents[2]
    digest = hashlib.sha256(platform.python_version().encode())
    # Complete engine/controller Python source plus actual native extension bytes.
    # A different binary or dirty implementation cannot silently resume this DB.
    for directory in (root / 'hummingbot', root / 'controllers'):
        for path in sorted(directory.rglob('*')):
            if path.is_file() and path.suffix in {'.py', '.pyx', '.pxd', '.so', '.pyd'}:
                digest.update(str(path.relative_to(root)).encode())
                digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


class NativePaperSession:
    """Own a disposable native recorder, full controllers and real V2 orchestrator.

    `step(timestamp, books=(), trades=(), candles=())` accepts dictionary arguments
    for the connector/provider's explicit input methods. No controller or executor
    background loop is started. A live process with an existing recorder cannot
    instantiate this session. Existing state requires `restore=True`; unfinished
    executors remain under native reconciliation holds rather than replaying BUYs.
    """

    def __init__(self, connector, controller_configs, db_path, *, restore=False, momentum_fixture_policy=None):
        # Acquire ownership before SQL construction or any persisted state read.
        # Resolving the path also prevents symlink aliases from bypassing the lease.
        self._owner_lease = None
        try:
            if momentum_fixture_policy is not None:
                from hummingbot.strategy_v2.components.domain import PolicyError
                lease = Path(db_path).resolve().with_suffix('.momentum-owner.lock').open('a')
                try:
                    fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BaseException:
                    lease.close()
                    raise PolicyError('another process owns this native momentum paper account')
                self._owner_lease = lease
            self._initialize(connector, controller_configs, db_path, restore=restore,
                             momentum_fixture_policy=momentum_fixture_policy)
        except BaseException:
            self._release_owner_lease()
            raise

    def _release_owner_lease(self):
        if self._owner_lease is not None:
            self._owner_lease.close()
            self._owner_lease = None

    def _initialize(self, connector, controller_configs, db_path, *, restore, momentum_fixture_policy):
        from controllers.generic.modular_ok_rsi import ModularOKRSIControllerConfig
        from controllers.generic.modular_rsi_v5 import ModularRSIV5ControllerConfig
        if not is_native_paper_connector(connector):
            raise ValueError('Exact native paper connector required')
        if MarketsRecorder.get_existing_instance() is not None:
            raise RuntimeError('Paper session requires an isolated process without another recorder')
        prior_oracle = RateOracle._shared_instance
        if prior_oracle is not None and (prior_oracle.started or prior_oracle._fetch_price_task is not None):
            raise RuntimeError('Paper session refuses an existing active rate oracle')
        configs = list(controller_configs)
        from controllers.generic.component_breakout import ComponentBreakoutConfig
        from controllers.generic.modular_spot_research import ModularSpotResearchConfig
        from controllers.generic.momentum_fusion_v2 import MomentumFusionV2Config
        from controllers.generic.rsi_modular import CONFIG_PROFILES
        allowed = (ModularOKRSIControllerConfig, ModularRSIV5ControllerConfig, *CONFIG_PROFILES.values(),
                   ComponentBreakoutConfig, MomentumFusionV2Config, ModularSpotResearchConfig)
        if not configs or len({c.id for c in configs}) != len(configs):
            raise ValueError('Distinct native paper controller identities required')
        for config in configs:
            if (type(config) not in allowed or getattr(config, 'execution_mode', None) != 'paper'
                    or config.connector_name != connector.name or config.data.mode != 'native'
                    or config.trading_pair not in connector.trading_pairs):
                raise ValueError('Only explicitly paper-bound native modular controllers are admitted')
        path = Path(db_path)
        marker = path.with_suffix(path.suffix + '.paper-session.json')
        identity = {'schema': 'native_paper_session_v1', 'connector_contract': connector.tracking_states['contract'],
                    'source_runtime_sha256': _runtime_identity(),
                    'initial_balances_sha256': hashlib.sha256(json.dumps(
                        connector.tracking_states['balances'], sort_keys=True).encode()).hexdigest(),
                    'controllers_sha256': hashlib.sha256(json.dumps(
                        [c.model_dump(mode='json') for c in configs], sort_keys=True).encode()).hexdigest()}
        if momentum_fixture_policy is not None:
            from controllers.generic.momentum_fusion_v2_runtime.paper_lifecycle import MomentumPaperPolicy
            if type(momentum_fixture_policy) is not MomentumPaperPolicy:
                raise ValueError('exact explicit offline momentum fixture policy required')
            identity['momentum_fixture_policy'] = momentum_fixture_policy.model_dump(mode='json')
        if path.exists() != restore or not path.parent.is_dir() or marker.exists() != restore:
            raise ValueError('New paper database or explicit existing-state restore required')
        if restore and json.loads(marker.read_text()) != identity:
            raise ValueError('Paper session marker/config identity mismatch')
        self.connectors = {connector.name: connector}
        self.markets = {connector.name: set(connector.trading_pairs)}
        self.market_data_provider = InjectedPaperMarketData(connector)
        self.controllers = {c.id: c.get_controller_class()(c, self.market_data_provider, asyncio.Queue()) for c in configs}
        from controllers.generic.modular_spot_core.native_funding import bind_funding_scope
        bind_funding_scope(self.controllers)
        for controller in self.controllers.values():
            if restore and type(controller.config) is ComponentBreakoutConfig:
                controller.recovery_hold = "reference_restart_requires_inventory_reconciliation"
            controller._status = RunnableStatus.RUNNING
            controller.initialize_candles()
        self._connector = connector
        self._initial_balances = dict(connector.get_all_balances())
        self._closed = False
        self._step_lock = asyncio.Lock()
        self._failure_path = path.with_suffix(path.suffix + '.paper-step.json')
        self._failure = None
        if self._failure_path.exists():
            if not restore:
                raise ValueError('Existing interrupted paper session requires explicit reconciliation')
            # A corrupt receipt is also an interrupted run, never permission to resume.
            self._failure = {'status': 'interrupted_or_failed_step'}
        self._identity = identity
        self._started = []
        self._controller_reports_valid = True
        self._is_stop_triggered = False
        if not restore:
            with marker.open('x') as stream:
                json.dump(identity, stream, sort_keys=True)
        self._sql = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                         db_path=str(path))
        self.recorder = None
        self._prior_oracle = prior_oracle
        self._oracle = None
        try:
            self.recorder = MarketsRecorder(self._sql, [connector], 'native-paper-session', 'v2_with_controllers',
                                            MarketDataCollectionConfigMap(market_data_collection_enabled=False))
            if restore:
                self.recorder.restore_market_states('native-paper-session', connector)
            self.recorder.start()
            self._prior_oracle = prior_oracle
            self._oracle = RateOracle(source=_InjectedRates())
            RateOracle._shared_instance = self._oracle
            for order in connector._order_tracker.all_orders.values():
                for fill in order.order_fills.values():
                    self._oracle.set_price(order.trading_pair, fill.fill_price)
            self.momentum_fixture = None
            if momentum_fixture_policy is not None:
                from controllers.generic.momentum_fusion_v2_runtime.paper_lifecycle import MomentumPaperLifecycle
                self.momentum_fixture = MomentumPaperLifecycle(self, momentum_fixture_policy)
            self.executor_orchestrator = ExecutorOrchestrator(self, executor_start=self._start_executor,
                                                              executor_restore=self.momentum_fixture.restore_executor if restore and self.momentum_fixture else None)
        except BaseException:
            if self.recorder is not None:
                self.recorder.stop()
                if MarketsRecorder.get_existing_instance() is self.recorder:
                    MarketsRecorder._shared_instance = None
            if self._oracle is not None and RateOracle._shared_instance is self._oracle:
                RateOracle._shared_instance = prior_oracle
            self._sql.engine.dispose()
            self._closed = True
            raise

    @property
    def current_timestamp(self):
        return self._connector.current_timestamp

    def _start_executor(self, executor):
        executor.register_events()
        executor._status = RunnableStatus.RUNNING
        self._started.append(executor)

    def buy(self, connector_name, trading_pair, amount, order_type, price, position_action):
        if self._failure is not None:
            raise RuntimeError('Paper session HELD after failed or interrupted step')
        self._check_connector(connector_name)
        return self._connector.buy(trading_pair, amount, order_type, price, position_action=position_action)

    def sell(self, connector_name, trading_pair, amount, order_type, price, position_action):
        if self._failure is not None:
            raise RuntimeError('Paper session HELD after failed or interrupted step')
        self._check_connector(connector_name)
        return self._connector.sell(trading_pair, amount, order_type, price, position_action=position_action)

    def cancel(self, connector_name, trading_pair, order_id):
        self._check_connector(connector_name)
        return self._connector.cancel(trading_pair, order_id)

    def _check_connector(self, name):
        if self._closed or name != self._connector.name or not is_native_paper_connector(self._connector):
            raise RuntimeError('Unexpected or closed paper execution route')

    def _refresh_reports(self, *, controller_ids=None):
        reports = self.executor_orchestrator.get_all_reports(controller_ids=controller_ids)
        for key, controller in self.controllers.items():
            if controller_ids is not None and key not in controller_ids:
                continue
            report = reports.get(key, {})
            controller.executors_info = report.get('executors', [])
            controller.positions_held = report.get('positions', [])
            controller.performance_report = report.get('performance')
        return reports

    def _validate_actions(self, actions):
        from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import (
            DownOnlyMakerPositionExecutorConfig,
        )
        for action in actions:
            if type(action) not in (CreateExecutorAction, StopExecutorAction, StoreExecutorAction) or action.controller_id not in self.controllers:
                raise ValueError('Unexpected paper executor action')
            if isinstance(action, CreateExecutorAction):
                config = action.executor_config
                if type(config) not in (PositionExecutorConfig, OrderExecutorConfig, DownOnlyMakerPositionExecutorConfig):
                    raise ValueError('Paper session admits native PositionExecutor and OrderExecutor only')
                self._check_connector(config.connector_name)
                if config.trading_pair not in self._connector.trading_pairs:
                    raise ValueError('Unexpected paper executor pair')

    def _write_step_receipt(self, receipt):
        temporary = self._failure_path.with_suffix('.tmp')
        with temporary.open('w') as stream:
            json.dump(receipt, stream, sort_keys=True, default=str)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self._failure_path)
        directory = os.open(str(self._failure_path.parent), os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def _preflight(self, timestamp, books, trades, candles):
        timestamp = float(timestamp)
        if not np.isfinite(timestamp) or timestamp < self.current_timestamp:
            raise ValueError('Paper clock must be finite and monotonic')
        books, trades, candles = copy.deepcopy((list(books), list(trades), list(candles)))
        sequences = dict(self._connector._book_sequence)
        book_times = dict(self._connector._book_time)
        for book in books:
            pair = book.get('trading_pair')
            self._connector.validate_order_book_input(**book, current_timestamp=timestamp,
                                                      previous_sequence=sequences.get(pair, -1), previous_timestamp=book_times.get(pair, 0))
            sequences[pair], book_times[pair] = book['sequence'], float(book['timestamp'])
        for candle in candles:
            self.market_data_provider.validate_candles_input(**candle, current_timestamp=timestamp)
        for trade in trades:
            self._connector.validate_trade_input(**trade, current_timestamp=timestamp,
                                                 book_timestamp=book_times.get(trade.get('trading_pair'), float('-inf')))
        return timestamp, books, trades, candles

    async def _persist_failure(self, receipt, error):
        self._failure = receipt
        receipt.update(status='failed', error_type=type(error).__name__, error=str(error)[:500], persistence_errors=[])
        # Settle accepted native operations; never undo fills or replace native cash.
        try:
            await self._connector.drain()
        except BaseException as failure:
            receipt['persistence_errors'].append('drain:' + type(failure).__name__)
        for executors in self.executor_orchestrator.active_executors.values():
            for executor in executors:
                try:
                    self.recorder.store_or_update_executor(executor)
                except BaseException as failure:
                    receipt['persistence_errors'].append('executor:' + type(failure).__name__)
        try:
            with self._sql.get_new_session() as session, session.begin():
                self.recorder.save_market_states('native-paper-session', self._connector, session)
        except BaseException as failure:
            receipt['persistence_errors'].append('market_state:' + type(failure).__name__)
        receipt['market_state'] = self._connector.tracking_states
        try:
            self._write_step_receipt(receipt)
        except BaseException as failure:
            # The pre-effect fsynced journal remains; restore must still hold.
            receipt['persistence_errors'].append('failure_receipt:' + type(failure).__name__)

    async def step(self, timestamp, *, books=(), trades=(), candles=()):
        async with self._step_lock:
            self._check_connector(self._connector.name)
            if self._failure is not None:
                raise RuntimeError('Paper session HELD after failed or interrupted step; explicit reconciliation required')
            timestamp, books, trades, candles = self._preflight(timestamp, books, trades, candles)

            def encode(value):
                return value.to_json(orient='split', double_precision=15) if isinstance(value, pd.DataFrame) else str(value)
            receipt = {'schema': 'native_paper_step_v1', 'status': 'in_progress', 'identity': self._identity,
                       'clock_before': self.current_timestamp, 'timestamp': timestamp,
                       'input_sha256': hashlib.sha256(json.dumps(
                           [timestamp, books, trades, candles], sort_keys=True, default=encode).encode()).hexdigest()}
            # Persist interruption evidence before any clock, liquidity or fill mutation.
            self._write_step_receipt(receipt)
            try:
                result = await self._apply_step(timestamp, books=books, trades=trades, candles=candles)
                self._failure_path.unlink()
                return result
            except BaseException as error:
                await self._persist_failure(receipt, error)
                raise

    async def _apply_step(self, timestamp, *, books, trades, candles):
        self._connector.tick(timestamp)
        for book in books:
            self._connector.set_order_book(**book)
            self._oracle.set_price(book['trading_pair'], self._connector.get_mid_price(book['trading_pair']))
        for frame in candles:
            self.market_data_provider.set_candles(**frame)
        fresh_ids = {key for key, controller in self.controllers.items()
                     if self._connector.pair_ready(controller.config.trading_pair)}
        for trade in trades:
            await self._connector.process_trade(**trade)
        self._refresh_reports(controller_ids=fresh_ids)
        actions = []
        for key, controller in self.controllers.items():
            if key not in fresh_ids:
                controller.processed_data = {'signal': 0, 'data_health': {'ready': False,
                                                                          'reason': 'public_book_unavailable'}}
                continue
            await controller.update_processed_data()
            actions.extend(controller.determine_executor_actions())
        self._validate_actions(actions)
        self.executor_orchestrator.execute_actions(actions)
        started, self._started = self._started, []
        for executor in started:
            await executor.on_start()
        await self._drive_executors(fresh_ids)
        self.executor_orchestrator.sync_live_state_to_db(controller_ids=fresh_ids)
        reports = self._refresh_reports(controller_ids=fresh_ids)
        reasons = []
        if not self._connector.ready:
            reasons.append('fresh_book_required')
        reasons.extend('exposed_pair_book_unavailable:' + p for p in self.unavailable_exposure())
        for key, controller in self.controllers.items():
            recovery_hold = getattr(controller, '_recovery_hold', getattr(controller, 'recovery_hold', ''))
            if recovery_hold:
                reasons.append(f'{key}: {recovery_hold}')
            if not controller.processed_data.get('data_health', {}).get('ready'):
                reasons.append(f'{key}: native_candles_unavailable')
        for key, pending in self.executor_orchestrator.recovery_required.items():
            if pending:
                reasons.append(f'{key}: unfinished_native_executors_require_reconciliation')
        return {'actions': actions, 'reports': reports, 'market_state': self._connector.tracking_states,
                'readiness': {'ready': not reasons, 'reasons': reasons}}

    async def _control_open_executors(self, fresh_ids):
        for key, executors in self.executor_orchestrator.active_executors.items():
            if key not in fresh_ids:
                continue
            for executor in list(executors):
                if not executor.is_closed:
                    await executor.control_task()
                    await self._connector.drain()

    async def _drive_executors(self, fresh_ids):
        await self._control_open_executors(fresh_ids)
        pending = []
        follow_up = {}
        for key, executors in self.executor_orchestrator.active_executors.items():
            if key not in fresh_ids:
                continue
            for executor in executors:
                for name in ('_entry_expiry_reconciliation_task', '_funding_entry_reconciliation_task'):
                    task = getattr(executor, name, None)
                    if task is not None and not task.done():
                        pending.append(task)
                        follow_up.setdefault(id(executor), (key, executor))
        if not pending:
            return
        await asyncio.gather(*pending, return_exceptions=True)
        # Only task owners need another transition; unrelated executors keep
        # the same single-pass semantics as a step without reconciliation.
        for key, executor in follow_up.values():
            active = self.executor_orchestrator.active_executors.get(key, ())
            if executor.is_closed or not any(item is executor for item in active):
                continue
            await executor.control_task()
            await self._connector.drain()

    def unavailable_exposure(self):
        pairs = {executor.config.trading_pair for executors in self.executor_orchestrator.active_executors.values()
                 for executor in executors if not executor.is_closed}
        pairs.update(position.trading_pair for positions in self.executor_orchestrator.positions_held.values()
                     for position in positions)
        return sorted(pair for pair in pairs if not self._connector.pair_ready(pair))

    async def suspend(self):
        """Persist and detach a qualified offline fixture without changing exits.

        This is neither normal shutdown nor live recovery. Interrupted writes
        retain the existing step journal and therefore require reconciliation.
        """
        if self.momentum_fixture is None:
            raise ValueError('Explicit momentum qualification fixture required')
        async with self._step_lock:
            self._check_connector(self._connector.name)
            if self._failure is not None or self.unavailable_exposure():
                raise ValueError('Fresh reconciled offline checkpoint required')
            receipt = {'schema': 'native_paper_step_v1', 'status': 'suspending', 'identity': self._identity}
            self._write_step_receipt(receipt)
            try:
                await self._connector.drain()
                for executors in self.executor_orchestrator.active_executors.values():
                    for executor in executors:
                        if any(getattr(executor, name, None) is not None and not getattr(executor, name).done()
                               for name in ('_entry_expiry_reconciliation_task', '_funding_entry_reconciliation_task')):
                            raise ValueError('Pending native reconciliation cannot be checkpointed')
                        self.recorder.store_or_update_executor(executor)
                with self._sql.get_new_session() as db, db.begin():
                    self.recorder.save_market_states('native-paper-session', self._connector, db)
                self._failure_path.unlink()
            except BaseException as error:
                await self._persist_failure(receipt, error)
                raise
            finally:
                for executors in self.executor_orchestrator.active_executors.values():
                    for executor in executors:
                        executor.unregister_events()
                for controller in self.controllers.values():
                    controller._status = RunnableStatus.TERMINATED
                self.recorder.stop()
                if MarketsRecorder.get_existing_instance() is self.recorder:
                    MarketsRecorder._shared_instance = None
                self._sql.engine.dispose()
                if RateOracle._shared_instance is self._oracle:
                    RateOracle._shared_instance = self._prior_oracle
                self._closed = True
                self._release_owner_lease()

    async def close(self):
        if self._closed:
            return
        self._is_stop_triggered = True
        try:
            if self.unavailable_exposure():
                # A current mark cannot be invented merely to close a session.
                # Save exact tracker/cash state, leave prior executor/position
                # receipts intact and require explicit reconciliation on restore.
                if self._failure is None:
                    self._failure = {'status': 'shutdown_without_fresh_exposed_book', 'identity': self._identity}
                    self._write_step_receipt(self._failure)
                await self._connector.cancel_all(5)
                with self._sql.get_new_session() as session, session.begin():
                    self.recorder.save_market_states('native-paper-session', self._connector, session)
                for executors in self.executor_orchestrator.active_executors.values():
                    for executor in executors:
                        tasks = [getattr(executor, name, None) for name in
                                 ('_entry_expiry_reconciliation_task', '_funding_entry_reconciliation_task')]
                        for task in tasks:
                            if task is not None:
                                task.cancel()
                        await asyncio.gather(*(task for task in tasks if task is not None), return_exceptions=True)
                        executor.unregister_events()
                return
            await self._connector.cancel_all(5)
            for executors in self.executor_orchestrator.active_executors.values():
                for executor in executors:
                    # No new disposal SELL authority at session shutdown.
                    if not executor.is_closed:
                        executor.early_stop(keep_position=True)
                        for _ in range(3):
                            await executor.control_task()
                            await self._connector.drain()
                            tasks = [getattr(executor, name, None) for name in
                                     ('_entry_expiry_reconciliation_task', '_funding_entry_reconciliation_task')]
                            await asyncio.gather(*(task for task in tasks if task is not None), return_exceptions=True)
                            if executor.is_closed:
                                break
                    task = getattr(executor, '_entry_expiry_reconciliation_task', None)
                    if task is not None:
                        executor._cancel_entry_expiry_reconciliation()
                        await asyncio.gather(task, return_exceptions=True)
                    task = getattr(executor, '_funding_entry_reconciliation_task', None)
                    if task is not None:
                        executor._funding_entry_reconciliation_task = None
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                    executor.unregister_events()
            if self._failure is None:
                self._refresh_reports()
                self.executor_orchestrator.sync_live_state_to_db()
                self._refresh_reports()
            else:
                # A held restart may intentionally have no book. Save native
                # terminal state without valuing inventory or certifying recovery.
                for executors in self.executor_orchestrator.active_executors.values():
                    for executor in executors:
                        self.recorder.store_or_update_executor(executor)
            # An orderly simulator stop can create native held inventory. Bind
            # that recorder-backed identity to an already restored checkpoint,
            # preserving every existing trail field. No policy decision, new
            # peak, reduction intent, or recovery-hold bypass is introduced.
            for controller in self.controllers.values():
                store = getattr(controller, '_recovery_store', None)
                if (self._failure is None and store is not None and store.restored and not controller._recovery_fault
                        and not controller._recovery_hold and controller._composition_unchanged()):
                    positions = store.prepare()  # Also checks pending reduction receipts.
                    store.persist(positions)
            with self._sql.get_new_session() as session, session.begin():
                self.recorder.save_market_states('native-paper-session', self._connector, session)
        finally:
            for controller in self.controllers.values():
                controller._status = RunnableStatus.TERMINATED
            self.recorder.stop()
            if MarketsRecorder.get_existing_instance() is self.recorder:
                MarketsRecorder._shared_instance = None
            self._sql.engine.dispose()
            if RateOracle._shared_instance is self._oracle:
                RateOracle._shared_instance = self._prior_oracle
            self._closed = True
            self._release_owner_lease()
