"""Native read/decision adapter. Persistence contains claims, never fill accounting."""
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType

from . import sell_only
from .data_reader import attach_pool, dataset_for, release_pool
from .domain import DataUnavailable, PolicyError, decimal, identity
from .inventory import inventory_view
from .market import features
from .planner import plan
from .program_budget import BudgetStore, Reservation
from .risk import risk_update
from .screener import CapitalAuthority, NativeMarketReadiness, admit_candidates, screen
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
        self.closed_rows = {}
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
                                          quote=c.trading_pair.split('-')[1], account=c.account,
                                          metadata_model=self.bindings.metadata_model)
        return self.store

    def decision_inputs(self, candle_open):
        """Digests of the closed candles a decision at `candle_open` consumed, keyed by open time."""
        return {stamp: digest for stamp, digest in self.closed_rows.items() if stamp <= candle_open}

    def inputs_revised(self, recorded):
        """True when the candles a pending entry decided on no longer read the same.

        `recorded` is the `decision_inputs` mapping stored at proposal time; the current
        view is `decision_inputs(recorded candle)`. Later candles are new observations, not
        revisions: a resting entry outlives them until its own timeout.
        """
        if not isinstance(recorded, dict) or 'candle' not in recorded or 'rows' not in recorded:
            return True
        current = self.decision_inputs(recorded['candle'])
        rows = recorded['rows']
        if any(stamp not in rows for stamp in current):
            return True  # a backfilled candle the decision never saw
        # Candles that scrolled out of the bounded window cannot be re-read; only re-readable ones count.
        return any(current[stamp] != digest for stamp, digest in rows.items() if stamp in current)

    def observe(self):
        c, p = self.controller.config, self.controller.market_data_provider
        self.market = self.input_identity = None
        self.closed_rows = {}
        try:
            now = p.time()

            def compute(frame):
                return features(frame, now=now, interval=c.interval_seconds(), max_age=c.max_candle_age_seconds,
                                policy=c.signal, trend_analyzer=self.bindings.trend)
            if c.data.mode == 'shared':
                if self.pool is None:
                    raise DataUnavailable('shared candle reader is stopped')
                snapshot = self.pool.snapshot(dataset_for(c), now=now, max_age=c.max_candle_age_seconds)
                self.market = self.pool.features.get(
                    snapshot, spec=c.signal.model_dump(mode='json'),
                    implementation='modular-closed-features-v1:' + self.feature_implementation,
                    initialization='closed-bar-complete-window', now=now,
                    max_age=c.max_candle_age_seconds, compute=compute)
                self.input_identity = snapshot.digest
                frame = snapshot.frame(now=now, max_age=c.max_candle_age_seconds)
            else:
                # A research binding may read public candles under a separate owner namespace.
                frame = p.get_candles_df(getattr(c, 'candles_connector', None) or c.connector_name,
                                         c.trading_pair, c.interval, c.max_records)
                self.market = compute(frame)
            closed = frame.loc[frame.timestamp + c.interval_seconds() <= now,
                               ['timestamp', 'open', 'high', 'low', 'close', 'volume']]
            self.closed_rows = dict(zip(closed.timestamp.astype(int).tolist(),
                                        pd.util.hash_pandas_object(closed, index=False).astype(str).tolist()))
            if c.data.mode != 'shared':
                self.input_identity = identity(list(self.closed_rows.values()))
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

    def _managed_pairs(self):
        c = self.controller.config
        managed = {c.trading_pair for view in self.inventory().values()
                   if view.remaining_base > 0 or not view.terminal}
        managed.update(item.trading_pair for item in self.pending.values())
        if self.budget is not None:
            managed.update(self.budget.occupied_pairs(
                native_executors=self.store.recorder.get_all_executors()))
        return tuple(sorted(managed))

    def _listed_execution(self, admitted):
        connector = (getattr(self.controller.market_data_provider, 'connectors', None) or {}).get(
            self.controller.config.connector_name)
        rules = getattr(connector, 'trading_rules', None) if connector is not None else None
        if not isinstance(rules, dict):
            return tuple(admitted)
        return tuple(pair for pair in admitted if pair in rules)

    def _unready_execution(self, listed):
        provider = self.controller.market_data_provider
        connector = (getattr(provider, 'connectors', None) or {}).get(self.controller.config.connector_name)
        if connector is None:
            return ()
        getter = getattr(provider, 'get_price_by_type', None)
        unready = []
        for pair in listed:
            if getattr(connector, 'ready', True) is not True:
                unready.append(pair)
                continue
            if getter is None:
                continue
            try:
                bid = getter(self.controller.config.connector_name, pair, PriceType.BestBid)
                ask = getter(self.controller.config.connector_name, pair, PriceType.BestAsk)
                decimal(bid, 'bid', positive=True)
                decimal(ask, 'ask', positive=True)
            except ERRORS:
                unready.append(pair)
        return tuple(unready)

    def acquisition_permitted(self):
        c = self.controller.config
        if getattr(self.controller, '_operator_entry_paused', False):
            self.reason = 'operator_entry_pause'
            return False
        if not c.acquisition_enabled or c.universe.mode == 'sell_only':
            self.reason = 'sell_only_no_acquisition'
            return False
        if c.data.mode == 'shared' and (self.pool is None or not self.pool.entry_ready(dataset_for(c))):
            self.reason = 'shared_data_resynchronization'
            return False
        if c.budget is None or c.account == 'unassigned':
            self.reason = 'topology_unproven'
            return False
        try:
            self.ensure_store()
            occupied = self._managed_pairs()
            managed = tuple(pair for pair in occupied if pair != c.trading_pair)
            pending_pairs = tuple(sorted({
                item.trading_pair for item in self.pending.values() if item.trading_pair != c.trading_pair}))
            tradable = c.universe.symbols or (c.trading_pair,)
            available = c.data.symbols or tuple(dict.fromkeys((*tradable, *c.universe.benchmarks, *occupied)))
            policy = c.universe if c.universe.symbols else c.universe.model_copy(update={'symbols': tradable})
            impulse_reason = None
            if c.universe.mode == 'impulse':
                ranked = screen(self.pool, c, SimpleNamespace(now=self.controller.market_data_provider.time()))
                self.coverage = ranked.coverage
                policy_hash = identity([c.universe.model_dump(mode='json'), c.data.model_dump(mode='json'),
                                        c.connector_name, c.interval])
                if self.selection_store is None:
                    self.selection_store = SelectionStore(
                        self.store.recorder, group=c.budget.group,
                        policy_hash=policy_hash, metadata_model=self.bindings.metadata_model)
                listed = self._listed_execution(tradable)
                admitted = admit_candidates(ranked, NativeMarketReadiness(
                    available=available, tradable=listed, unready=self._unready_execution(listed)),
                    CapitalAuthority(
                        universe=policy, managed=managed, pending=pending_pairs,
                        selection_store=self.selection_store, budget=self.budget,
                        native_executors=self.store.recorder.get_all_executors(),
                        fingerprint=self.fingerprint, account=c.account))
                entries = admitted.entries
                impulse_reason = admitted.reason
            else:
                entries = select_universe(
                    policy, available=available, tradable=tradable, scores={},
                    managed=managed, pending=pending_pairs, blocked=(), previous=()).entries
            if c.trading_pair not in entries:
                if c.universe.mode != 'impulse':
                    self.reason = 'no_budget_slot'
                elif impulse_reason not in (None, 'admitted'):
                    self.reason = impulse_reason
                else:
                    self.reason = 'not_selected_or_incomplete_universe'
                return False
            return True
        except PolicyError as exc:
            if 'foreign' in str(exc) or 'topology' in str(exc):
                self.reason = 'ownership_conflict'
                return False
            raise

    def reservation(self, config):
        c = self.controller.config
        fee = c.risk.fee_reserve_fraction
        timeout = decimal(c.acquisition.entry_timeout_seconds, 'entry timeout')
        return Reservation(
            executor_id=config.id, controller_id=c.id, trading_pair=c.trading_pair,
            reserved_quote=config.amount * config.entry_price * (1 + fee),
            reserved_base=config.amount * (1 + fee),
            account=c.account, connector=c.connector_name, program=c.budget.group,
            slot=c.trading_pair.split('-')[0],
            expires_at=decimal(config.timestamp, 'entry timestamp') + timeout)

    def actions(self):
        c = self.controller.config
        if not c.execution_enabled or c.manual_kill_switch or self.fault:
            return []
        if c.universe.mode == 'sell_only':
            return self._sell_only_actions()
        try:
            if c.fingerprint() != self.fingerprint:
                raise PolicyError('implementation/configuration changed')
            market = self.observe()
            if market is None:
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
            if config is not None:
                if self.budget is None:
                    self.reason = 'topology_unproven'
                    return []
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
            group = c.budget.group if c.budget is not None else c.id
            freezer = self.selection_store
            if freezer is None:
                freezer = SelectionStore(store.recorder, group=group, policy_hash=self.fingerprint,
                                         metadata_model=self.bindings.metadata_model)
            freezer.freeze(executor_id=config.id, trading_pair=c.trading_pair, policy_hash=self.fingerprint)
            self.pending[config.id] = config.model_copy(deep=True)
            self.pending_inputs[config.id] = dict(candle=market.candle_open, rows=self.decision_inputs(market.candle_open))
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
            if self.budget is None:
                return False
            if not self.budget.contains(
                    self.reservation(config), native_executors=store.recorder.get_all_executors()):
                raise PolicyError('group capital reservation is unavailable')
            snapshot = self.snapshot()
            now = decimal(self.controller.market_data_provider.time(), 'now')
            timestamp = decimal(config.timestamp, 'entry timestamp')
            if now < timestamp or now - timestamp > c.acquisition.entry_timeout_seconds:
                return False
            if self.observe() is None or not self.acquisition_permitted():
                return False
            if self.inputs_revised(self.pending_inputs.get(config.id)):
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

    def reduction_permission(self, config):
        c = self.controller.config
        if (c.universe.mode != 'sell_only' or not c.execution_enabled or c.manual_kill_switch
                or self.fault or config is None or self.pending.get(config.id) != config):
            return False
        return sell_only.is_limit_reduce(config)

    def executor_class(self, config):
        if config is None:
            raise PolicyError('missing modular executor request')
        if config.id in self.dispatched:
            raise PolicyError('ungranted or already consumed modular executor request')
        if self.controller.config.universe.mode == 'sell_only':
            if not self.reduction_permission(config):
                raise PolicyError('sell-only factory refuses buy, short, or ungranted requests')
            self.dispatched.add(config.id)
            return sell_only.ModularReduceExecutor
        if not self.entry_permission(config):
            raise PolicyError('ungranted or already consumed modular executor request')
        self.dispatched.add(config.id)
        return self.bindings.executor_class()

    def _sell_only_store(self):
        c = self.controller.config
        recorder = self.ensure_store().recorder
        return sell_only.SellOnlyStore(recorder, c.id, c.connector_name, c.trading_pair,
                                       metadata_model=self.bindings.metadata_model)

    def _unique_native(self, *groups):
        seen, rows = set(), []
        for group in groups:
            for info in group or ():
                ident = getattr(info, 'id', None)
                if ident is not None and ident in seen:
                    continue
                if ident is not None:
                    seen.add(ident)
                rows.append(info)
        return rows

    def admit_inventory(self, handoff, native_reconciliation):
        c = self.controller.config
        recorder = self.ensure_store().recorder
        supplied = () if native_reconciliation is None else tuple(native_reconciliation)
        scope = sell_only.admit_inventory(handoff, self._unique_native(supplied, recorder.get_all_executors()))
        lots = tuple(lot for lot in scope.lots if lot.trading_pair == c.trading_pair and lot.new_owner == c.id)
        if any(lot.account != c.account or lot.connector != c.connector_name for lot in lots):
            raise PolicyError('inventory handoff account or connector differs from controller')
        if not lots:
            return sell_only.ManagementScope(())
        if not self._sell_only_store().replace(lots):
            raise PolicyError('sell-only persistence failed')
        return sell_only.ManagementScope(lots)

    def _pending_sell_evidence(self):
        rows = []
        for config in self.pending.values():
            if getattr(config, 'type', None) != 'order_executor' or getattr(config.side, 'name', None) != 'SELL':
                continue
            rows.append(SimpleNamespace(
                id=config.id, controller_id=self.controller.config.id, is_done=False,
                is_active=True, config=config, custom_info={'executed_amount_base': 0}))
        return rows

    def _reduction_evidence(self):
        store = self.ensure_store()
        saved = {info.id: info for info in store.recorder.get_all_executors()}
        for info in self.controller.executors_info:
            old = saved.get(info.id)
            if old is not None:
                previous = decimal(old.custom_info.get('executed_amount_base', 0), 'persisted sold base')
                current = decimal(info.custom_info.get('executed_amount_base', 0), 'current sold base')
                if current < previous or (old.is_done and not info.is_done):
                    raise PolicyError('native reduction receipt regressed')
            saved[info.id] = info
        rows = list(saved.values())
        owned = {info.id for info in rows if getattr(info, 'id', None) is not None}
        self.pending = {key: value for key, value in self.pending.items() if key not in owned}
        rows.extend(self._pending_sell_evidence())
        return rows

    def _sell_only_actions(self):
        c = self.controller.config
        try:
            if c.fingerprint() != self.fingerprint:
                raise PolicyError('implementation/configuration changed')
            if self.observe() is None:
                return []
            loaded, _ = self._sell_only_store().load()
            if loaded is None or not loaded.lots:
                self.reason = 'no_admitted_inventory'
                return []
            snapshot = self.snapshot()
            connector = self.controller.market_data_provider.connectors[c.connector_name]
            rules = connector.trading_rules[c.trading_pair]
            base, quote = c.trading_pair.split('-')
            fee = connector.get_fee(
                base_currency=base, quote_currency=quote, order_type=OrderType.LIMIT,
                order_side=TradeType.SELL, amount=loaded.lots[0].available_base, price=snapshot['bid'], is_maker=False)
            # Native quantity reservation has no separate base-fee cap. Until that
            # contract is supported, admit quote percentage fees only.
            if not sell_only.supported_reduction_fee(fee, quote):
                self.reason = 'unsupported_fees_or_book'
                return []
            fee_base = decimal('0', 'sell base fee')
            observation = sell_only.ExitObservation(
                trading_pair=c.trading_pair, bid=snapshot['bid'], ask=snapshot['ask'], sell_signal=True,
                amount_step=snapshot['amount_step'], minimum_amount=snapshot['minimum_amount'],
                minimum_notional=snapshot['minimum_notional'], price_step=rules.min_price_increment,
                sell_fee_rate=decimal(fee.percent, 'sell fee'), sell_fee_base=fee_base,
                trailing=c.trailing, timestamp=float(self.controller.market_data_provider.time()))
            reduction = sell_only.propose_reduction(loaded, observation, self._reduction_evidence())
            self.reason = reduction.reason
            if reduction.action != 'sell' or reduction.config is None:
                return []
            config = reduction.config
            self.pending[config.id] = config.model_copy(deep=True)
            return [self.bindings.action(c.id, config)]
        except DataUnavailable as exc:
            self.reason = f'data_hold:{exc}'
            return []
        except ERRORS as exc:
            self.fault = f'{type(exc).__name__}:{exc}'
            self.reason = f'admission_hold:{self.fault}'
            self.controller.logger().error(self.reason)
            return []
