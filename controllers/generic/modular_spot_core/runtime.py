"""Native read/decision adapter. Persistence contains claims, never fill accounting."""
from hashlib import sha256
from pathlib import Path

import pandas as pd

from .data_reader import attach_pool, dataset_for, release_pool
from .domain import DataUnavailable, PolicyError, decimal, identity
from .inventory import inventory_view
from .market import features
from .planner import plan
from .program_budget import BudgetStore, Reservation
from .risk import risk_update
from .selection_state import SelectionStore
from .state import DecisionStore
from .universe import select_universe

ERRORS = (ValueError, TypeError, ArithmeticError, RuntimeError, KeyError, AttributeError, OSError)


class ModularRuntime:
    def __init__(self, controller, bindings=None):
        if bindings is None:
            from .native_bindings import NativeBindings
            bindings = NativeBindings()
        self.bindings = bindings
        self.controller = controller
        self.fingerprint = controller.config.fingerprint()
        self.feature_implementation = sha256(Path(__file__).with_name("market.py").read_bytes()).hexdigest()
        self.store = self.budget = self.selection_store = None
        self.market = self.input_identity = None
        self.pending, self.pending_inputs = {}, {}
        self.dispatched = set()
        self.fault = None
        self.reason = 'execution_disabled'
        self.coverage = None
        self.bindings.guard_resume(controller, self.bindings.recorder())
        self.pool = attach_pool(controller.market_data_provider, controller.config)

    def start_data(self):
        if self.pool is not None:
            self.pool.start()

    def close_data(self):
        pool, self.pool = self.pool, None
        return release_pool(self.controller.market_data_provider, pool)

    def ensure_store(self):
        if self.store is None:
            recorder = self.bindings.recorder()
            if recorder is None:
                raise PolicyError('initialized native recorder required')
            self.bindings.guard_resume(self.controller, recorder)
            c = self.controller.config
            self.store = DecisionStore(recorder, c.id, c.connector_name, c.trading_pair,
                                       metadata_model=self.bindings.metadata_model)
            if c.budget is not None:
                self.budget = BudgetStore(recorder, c.budget, connector=c.connector_name,
                                          quote=c.trading_pair.split('-')[1], metadata_model=self.bindings.metadata_model)
        return self.store

    def observe(self):
        c, p = self.controller.config, self.controller.market_data_provider
        self.market = self.input_identity = None
        try:
            now = p.time()
            def compute(frame):
                return features(frame, now=now, interval=c.interval_seconds(), max_age=c.max_candle_age_seconds,
                                policy=c.signal, trend_analyzer=self.bindings.trend)
            if c.data.mode == 'shared':
                if self.pool is None:
                    raise DataUnavailable('shared candle reader is stopped')
                snapshot = self.pool.snapshot(dataset_for(c), now=now, max_age=c.max_candle_age_seconds)
                self.market = self.pool.features.get(snapshot, spec=c.signal.model_dump(mode='json'),
                    implementation='modular-closed-features-v1:' + self.feature_implementation, now=now,
                    max_age=c.max_candle_age_seconds, compute=compute)
                self.input_identity = snapshot.digest
            else:
                frame = p.get_candles_df(c.connector_name, c.trading_pair, c.interval, c.max_records)
                self.market = compute(frame)
                closed = frame.loc[frame.timestamp + c.interval_seconds() <= now,
                                   ['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                self.input_identity = identity(pd.util.hash_pandas_object(closed, index=False).astype(str).tolist())
            self.reason = 'observed' if c.execution_enabled else 'execution_disabled'
        except ERRORS as exc:
            self.reason = f'market_unavailable:{type(exc).__name__}:{exc}'
        return self.market

    def snapshot(self):
        return self.bindings.snapshot(self.controller)

    def inventory(self):
        c = self.controller.config
        recorder = self.ensure_store().recorder
        kwargs = dict(controller_id=c.id, connector=c.connector_name, pair=c.trading_pair)
        saved = inventory_view(recorder.get_executors_by_controller(c.id), recorder.get_positions_by_controller(c.id), **kwargs)
        current = inventory_view(self.controller.executors_info, self.controller.positions_held, **kwargs)
        for key, value in current.items():
            old = saved.get(key)
            if old and (value.entry_quote < old.entry_quote or value.sold_base < old.sold_base
                        or (old.terminal and not value.terminal)):
                raise PolicyError('native execution receipt regressed')
        return {**saved, **current}

    def acquisition_permitted(self):
        c = self.controller.config
        if not c.acquisition_enabled or c.universe.mode == 'sell_only':
            self.reason = 'sell_only_no_acquisition'
            return False
        if c.data.mode == 'shared' and (self.pool is None or not self.pool.entry_ready(dataset_for(c))):
            self.reason = 'shared_data_resynchronization'
            return False
        if c.universe.mode == 'fixed':
            permitted = c.universe.symbols or (c.trading_pair,)
            return c.trading_pair in permitted
        self.ensure_store()
        scores, self.coverage = self.pool.screen(c, now=self.controller.market_data_provider.time())
        policy_hash = identity([c.universe.model_dump(mode='json'), c.data.model_dump(mode='json'),
                                c.connector_name, c.interval])
        if self.selection_store is None:
            self.selection_store = SelectionStore(self.store.recorder, group=c.budget.group,
                policy_hash=policy_hash, metadata_model=self.bindings.metadata_model)
        now = self.controller.market_data_provider.time()
        candle = int(now // c.interval_seconds()) * c.interval_seconds() - c.interval_seconds()
        evidence_hash = identity([candle, [(pair, str(score)) for pair, score in sorted(scores.items())], str(self.coverage)])
        entries = self.selection_store.choose(candle=candle, evidence_hash=evidence_hash,
            select=lambda previous: select_universe(c.universe, available=c.data.symbols,
                tradable=c.universe.symbols, scores=scores, managed=(), pending=(), blocked=(), previous=previous).entries)
        if c.trading_pair not in entries:
            self.reason = 'not_selected_or_incomplete_universe'
            return False
        return True

    def reservation(self, config):
        c = self.controller.config
        return Reservation(executor_id=config.id, controller_id=c.id, trading_pair=c.trading_pair,
                           reserved_quote=config.amount * config.entry_price * (1 + c.risk.fee_reserve_fraction))

    def actions(self):
        c = self.controller.config
        if not c.execution_enabled or c.manual_kill_switch or self.fault:
            return []
        try:
            if c.fingerprint() != self.fingerprint:
                raise PolicyError('implementation/configuration changed')
            if self.observe() is None:
                return []
            allowed = self.acquisition_permitted()
            permission_reason = self.reason
            store = self.ensure_store()
            state, raw = store.load()
            revised, proposal, reason = plan(
                state, self.inventory(), fingerprint=self.fingerprint, budget=c.total_amount_quote,
                now=self.controller.market_data_provider.time(), market=self.market,
                signal=c.signal, acquisition=c.acquisition, risk=c.risk,
                allow_acquisition=allowed, **self.snapshot())
            config = self.bindings.config(c, proposal) if proposal is not None else None
            if config is not None and self.budget is not None:
                if not self.budget.reserve(self.reservation(config), native_executors=store.recorder.get_all_executors()):
                    self.reason = 'program_capital_or_asset_limit'
                    return []
            # A group claim remains reserved if this second persistence boundary fails.
            if revised != state and not store.compare_and_swap(raw, revised):
                raise PolicyError('decision CAS conflict; no dispatch')
            self.reason = permission_reason if reason == 'acquisition_disabled' else reason
            self.pending = {key: value for key, value in self.pending.items() if key in revised.claims}
            self.pending_inputs = {key: value for key, value in self.pending_inputs.items() if key in revised.claims}
            self.dispatched.intersection_update(revised.claims)
            if config is None:
                return []
            self.pending[config.id] = config.model_copy(deep=True)
            self.pending_inputs[config.id] = self.input_identity
            return [self.bindings.action(c.id, config)]
        except DataUnavailable as exc:
            self.reason = f'data_hold:{exc}'
            return []
        except ERRORS as exc:
            self.fault = f'{type(exc).__name__}:{exc}'
            self.reason = f'admission_hold:{self.fault}'
            self.controller.logger().error(self.reason)
            return []

    def entry_permission(self, config, *, check_funds=True):
        c = self.controller.config
        if (not c.execution_enabled or c.manual_kill_switch or self.fault
                or config is None or self.pending.get(config.id) != config):
            return False
        try:
            if c.fingerprint() != self.fingerprint:
                raise PolicyError('implementation/configuration changed')
            store = self.ensure_store()
            state, raw = store.load()
            if state is None or state.fingerprint != self.fingerprint or config.id not in state.claims:
                return False
            if self.budget is not None and not self.budget.contains(
                    self.reservation(config), native_executors=store.recorder.get_all_executors()):
                raise PolicyError('group capital reservation is unavailable')
            snapshot = self.snapshot()
            now = decimal(self.controller.market_data_provider.time(), 'now')
            timestamp = decimal(config.timestamp, 'entry timestamp')
            if now < timestamp or now - timestamp > c.acquisition.entry_timeout_seconds:
                return False
            if self.observe() is None or not self.acquisition_permitted():
                return False
            if self.pending_inputs.get(config.id) != self.input_identity:
                self.reason = 'entry_input_revision_changed'
                return False
            assessment = risk_update(c.risk, budget=c.total_amount_quote, pnl=snapshot['pnl'], day=int(now // 86400),
                                     peak=state.risk_peak, previous_day=state.risk_day,
                                     day_start=state.risk_day_start, paused=state.risk_paused)
            revised = state.model_copy(update={'risk_peak': assessment.peak, 'risk_day': assessment.day,
                                              'risk_day_start': assessment.day_start, 'risk_paused': assessment.paused})
            if revised != state and not store.compare_and_swap(raw, revised):
                raise PolicyError('entry risk CAS conflict')
            bid, ask = snapshot['bid'], snapshot['ask']
            if assessment.paused or ask < bid or (ask - bid) / bid > c.risk.max_spread_fraction:
                return False
            required = config.amount * config.entry_price * (1 + c.risk.fee_reserve_fraction)
            if check_funds and required + c.risk.cash_reserve_quote > snapshot['available_quote']:
                return False
            return self.bindings.fees_permit(self.controller, config)
        except DataUnavailable as exc:
            self.reason = f'data_hold:{exc}'
            return False
        except ERRORS as exc:
            self.fault = f'{type(exc).__name__}:{exc}'
            self.reason = f'entry_permission_hold:{self.fault}'
            self.controller.logger().error(self.reason)
            return False

    def executor_class(self, config):
        if config is None:
            raise PolicyError('missing modular executor request')
        if config.id in self.dispatched or not self.entry_permission(config):
            raise PolicyError('ungranted or already consumed modular executor request')
        self.dispatched.add(config.id)
        return self.bindings.executor_class()
