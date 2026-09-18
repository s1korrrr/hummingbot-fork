"""Native read/decision adapter. Persistence contains claims, never fill accounting."""
from decimal import Decimal

from hummingbot.client.settings import AllConnectorSettings, ConnectorType
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy_v2.executors.planned_position_resume import pending_manifest
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.utils.market_analysis import calculate_trend_state

from .domain import DataUnavailable, PolicyError, decimal
from .execution import ModularPositionExecutor, native_config
from .inventory import inventory_view
from .market import features
from .planner import plan
from .risk import risk_update
from .state import DecisionStore

ERRORS = (ValueError, TypeError, ArithmeticError, RuntimeError, KeyError, AttributeError)


class ModularRuntime:
    def __init__(self, controller):
        self.controller = controller
        self.fingerprint = controller.config.fingerprint()
        self.store = None
        self.market = None
        self.pending = {}
        self.dispatched = set()
        self.fault = None
        self.reason = 'execution_disabled'
        self.guard_resume()

    def guard_resume(self):
        recorder = MarketsRecorder.get_existing_instance()
        if recorder is not None:
            path = recorder.sql_manager.db_path
            manifest = pending_manifest(path) if path else None
            if manifest and any(p['controller_id'] == self.controller.config.id for p in manifest['positions']):
                raise PolicyError('native planned resume bypasses local executor selection; not supported')

    def ensure_store(self):
        if self.store is None:
            self.guard_resume()
            recorder = MarketsRecorder.get_existing_instance()
            if recorder is None:
                raise PolicyError('initialized native recorder required')
            c = self.controller.config
            self.store = DecisionStore(recorder, c.id, c.connector_name, c.trading_pair)
        return self.store

    def observe(self):
        c = self.controller.config
        p = self.controller.market_data_provider
        self.market = None
        try:
            self.market = features(p.get_candles_df(c.connector_name, c.trading_pair, c.interval, c.max_records),
                                   now=p.time(), interval=c.interval_seconds(), max_age=c.max_candle_age_seconds,
                                   policy=c.signal, trend_analyzer=calculate_trend_state)
            self.reason = 'observed' if c.execution_enabled else 'execution_disabled'
        except ERRORS as exc:
            self.reason = f'market_unavailable:{type(exc).__name__}:{exc}'
        return self.market

    def snapshot(self):
        c = self.controller.config
        p = self.controller.market_data_provider
        setting = AllConnectorSettings.get_connector_settings().get(c.connector_name)
        if setting is None or setting.type != ConnectorType.Exchange:
            raise PolicyError('only native centralized spot connectors are supported')
        connector = p.connectors[c.connector_name]
        if p.ready is not True or connector.ready is not True:
            raise DataUnavailable('native market/account provider not ready')
        if getattr(connector, 'supports_terminal_fill_reconciliation', False) is not True:
            raise PolicyError('authoritative terminal fill reconciliation is required')
        report = self.controller.performance_report
        if report is None or report.pnl_available is not True or report.global_pnl_quote is None:
            raise DataUnavailable('native marked performance unavailable')
        pnl = decimal(report.global_pnl_quote, 'native marked pnl', minimum=Decimal('-Infinity'))
        bid = decimal(p.get_price_by_type(c.connector_name, c.trading_pair, PriceType.BestBid), 'bid', positive=True)
        ask = decimal(p.get_price_by_type(c.connector_name, c.trading_pair, PriceType.BestAsk), 'ask', positive=True)
        quote = c.trading_pair.split('-')[1]
        available = decimal(connector.get_available_balance(quote), 'available quote')
        rules = connector.trading_rules[c.trading_pair]
        return dict(bid=bid, ask=ask, available_quote=available, pnl=pnl,
                    amount_step=rules.min_base_amount_increment, minimum_amount=rules.min_order_size,
                    minimum_notional=rules.min_notional_size)

    def inventory(self):
        c = self.controller.config
        recorder = self.ensure_store().recorder
        kwargs = dict(controller_id=c.id, connector=c.connector_name, pair=c.trading_pair)
        saved = inventory_view(recorder.get_executors_by_controller(c.id),
                               recorder.get_positions_by_controller(c.id), **kwargs)
        current = inventory_view(self.controller.executors_info, self.controller.positions_held, **kwargs)
        for key, value in current.items():
            old = saved.get(key)
            if old and (value.entry_quote < old.entry_quote or value.sold_base < old.sold_base
                        or (old.terminal and not value.terminal)):
                raise PolicyError('native execution receipt regressed')
        return {**saved, **current}

    def actions(self):
        c = self.controller.config
        if not c.execution_enabled or c.manual_kill_switch or self.fault or self.market is None:
            return []
        try:
            if c.fingerprint() != self.fingerprint:
                raise PolicyError('implementation/configuration changed')
            store = self.ensure_store()
            state, raw = store.load()
            revised, proposal, reason = plan(
                state, self.inventory(), fingerprint=self.fingerprint, budget=c.total_amount_quote,
                now=self.controller.market_data_provider.time(), market=self.market,
                signal=c.signal, acquisition=c.acquisition, risk=c.risk, **self.snapshot())
            # Before even a single CreateExecutorAction can leave this controller.
            if revised != state and not store.compare_and_swap(raw, revised):
                raise PolicyError('decision CAS conflict; no dispatch')
            self.reason = reason
            self.pending = {key: value for key, value in self.pending.items() if key in revised.claims}
            self.dispatched.intersection_update(revised.claims)
            if proposal is None:
                return []
            config = native_config(c, identifier=proposal.identifier, timestamp=proposal.timestamp,
                                   price=proposal.price, amount=proposal.amount, cycle=proposal.cycle, stage=proposal.stage)
            self.pending[config.id] = config.model_copy(deep=True)
            return [CreateExecutorAction(controller_id=c.id, executor_config=config)]
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
            snapshot = self.snapshot()
            now = decimal(self.controller.market_data_provider.time(), 'now')
            if now < config.timestamp or now - Decimal(str(config.timestamp)) > c.acquisition.entry_timeout_seconds:
                return False
            if self.observe() is None:
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
            connector = self.controller.market_data_provider.connectors[c.connector_name]
            base, quote = c.trading_pair.split('-')
            fee = connector.get_fee(base_currency=base, quote_currency=quote, order_type=OrderType.LIMIT,
                                    order_side=TradeType.BUY, amount=config.amount, price=config.entry_price, is_maker=False)
            if fee.percent and fee.percent_token not in (None, base, quote):
                return False
            rate = decimal(fee.percent, 'entry fee')
            for item in fee.flat_fees:
                amount = decimal(item.amount, 'flat entry fee')
                if item.token not in (base, quote):
                    return False
                rate += amount * (config.entry_price if item.token == base else 1) / (config.amount * config.entry_price)
            return rate <= c.risk.fee_reserve_fraction
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
        return ModularPositionExecutor
