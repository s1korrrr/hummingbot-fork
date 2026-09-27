"""Meridian v3 unified: one native controller, five symbol contexts, one portfolio.

Symbol contexts run the frozen p23 lifecycle (features, bags, trailing exits).
Only the portfolio admits acquisitions, through native grants in this owner's
recorder. Exits and held-inventory management never wait for portfolio alpha.
Live spending additionally requires an explicit exclusive capital-domain binding.
"""
import asyncio
import time

from controllers.generic.meridian_v3_unified_core.config import PAIRS, RECIPE_ID, MeridianUnifiedConfig, source_files
from controllers.generic.meridian_v3_unified_core.portfolio import MeridianPortfolio
from controllers.generic.meridian_v3_unified_core.symbols import SymbolContext, SymbolRuntime
from controllers.generic.modular_spot_core.bag import is_recovery_sell
from controllers.generic.modular_spot_core.data_reader import attach_pool
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
from hummingbot.core.data_type.common import TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.components.source_identity import component_source_files
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
from hummingbot.strategy_v2.models.base import RunnableStatus

CONFIG_CLASS = 'MeridianUnifiedConfig'
__all__ = ['MeridianUnifiedConfig', 'MeridianUnifiedController']


class MeridianUnifiedController(ControllerBase):
    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        return [*source_files(), *component_source_files()]

    def __init__(self, config, market_data_provider, actions_queue, **kwargs):
        if type(config) is not MeridianUnifiedConfig:
            raise ValueError('Exact Meridian unified configuration required')
        config = MeridianUnifiedConfig.model_validate(config.model_dump(mode='json'))
        paper = config.execution_mode == 'paper'
        if paper and not is_native_paper_connector(market_data_provider.connectors.get(config.connector_name)):
            raise ValueError('Meridian paper requires the exact native paper connector')
        super().__init__(config, market_data_provider, actions_queue, **kwargs)
        self._symbol_source = self._symbol_configs = None
        self.operator_entry_paused = False
        self._operator_state = {'entry_paused': False, 'reason': None, 'scope': None,
                                'updated_at': None, 'last_command_id': None}
        self.portfolio = MeridianPortfolio(self)
        self.contexts = {pair: SymbolContext(self, pair) for pair in PAIRS}
        for context in self.contexts.values():
            context.runtime = SymbolRuntime(context, self.portfolio, paper=paper)
        self.fault = None

    # ------------------------------------------------------------------ native report
    @property
    def performance_report(self):
        return getattr(self, '_native_report', None)

    @performance_report.setter
    def performance_report(self, value):
        self._native_report = value
        self._native_report_received_at = time.monotonic()

    def native_report_age(self):
        received = getattr(self, '_native_report_received_at', None)
        return float('inf') if received is None else time.monotonic() - received

    # ------------------------------------------------------------------ configuration
    def symbol_config(self, pair):
        """Per-symbol native configuration, rebuilt only when the owner config object changes."""
        if self._symbol_source is not self.config:
            self._symbol_configs, self._symbol_source = self.config.symbol_configs(), self.config
        return self._symbol_configs[pair]

    def set_operator_entry_pause(self, enabled, *, reason=None, scope=None, command_id=None, updated_at=None):
        """Stop new acquisitions only; positions, bags and exits keep running."""
        if type(enabled) is not bool:
            raise ValueError('Operator pause must be a boolean')
        self.operator_entry_paused = enabled
        self._operator_state = {'entry_paused': enabled, 'reason': reason, 'scope': scope,
                                'updated_at': updated_at if updated_at is not None else time.time(),
                                'last_command_id': command_id}

    def get_operator_state(self):
        return dict(self._operator_state)

    # ------------------------------------------------------------------ lifecycle
    def start(self):
        if self.config.data.mode == 'shared':
            for context in self.contexts.values():
                if context.runtime.pool is None:
                    context.runtime.pool = attach_pool(self.market_data_provider, context.config)
        super().start()

    def _close_data(self):
        return [task for task in (c.runtime.close_data() for c in self.contexts.values()) if task is not None]

    def stop(self):
        super().stop()
        for task in self._close_data():
            task.add_done_callback(self._data_closed)

    async def control_loop(self):
        try:
            await super().control_loop()
        finally:
            tasks = self._close_data()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    def _data_closed(self, task):
        if not task.cancelled() and task.exception() is not None:
            self.logger().error('Meridian candle reader did not close cleanly: %s', task.exception())

    def get_candles_config(self):
        if self.config.data.mode == 'shared':
            return []  # One shared reader serves all five symbols; no native upstream feed.
        return [CandlesConfig(connector='okx', trading_pair=pair, interval='1m', max_records=1500) for pair in PAIRS]

    # ------------------------------------------------------------------ decisions
    async def update_processed_data(self):
        symbols = {}
        for pair, context in self.contexts.items():
            context.runtime.start_data()
            market = context.runtime.observe()
            symbols[pair] = {'ready': market is not None, 'reason': context.runtime.reason,
                             'candle_open': None if market is None else market.candle_open}
        self.processed_data = {'symbols': symbols, 'execution_mode': self.config.execution_mode,
                               'data_health': {'ready': any(s['ready'] for s in symbols.values()),
                                               'reason': ','.join(sorted({s['reason'] for s in symbols.values()}))}}

    def determine_executor_actions(self):
        if self.status != RunnableStatus.RUNNING:
            return []
        self.portfolio.begin_tick()
        markets = {pair: c.runtime.market for pair, c in self.contexts.items()}
        try:
            self.portfolio.evaluate(markets)
            self.fault = None
        except Exception as exc:  # Portfolio failure holds entries only; management continues below.
            self.fault = f'{type(exc).__name__}:{exc}'[:300]
            self.portfolio.status = {'reason': 'portfolio_fault', 'hold': self.fault}
            self.logger().error(f'Meridian portfolio evaluation held: {self.fault}')
        actions = []
        for context in self.contexts.values():
            actions.extend(context.runtime.actions())
        try:
            self.portfolio.withdraw_unused()
        except Exception as exc:
            self.logger().error(f'Meridian unused grant withdrawal deferred: {type(exc).__name__}:{exc}')
        return actions

    def _context(self, config):
        pair = getattr(config, 'trading_pair', None)
        if pair not in self.contexts or getattr(config, 'controller_id', self.config.id) != self.config.id:
            return None
        return self.contexts[pair]

    def entry_permission(self, config, *, check_funds=True):
        context = self._context(config)
        return (context is not None and super().validate_executor_config(config)
                and context.runtime.entry_permission(config, check_funds=check_funds))

    def validate_executor_config(self, config):
        context = self._context(config)
        if context is None or config.id in context.runtime.dispatched:
            return False
        if is_recovery_sell(config):
            return super().validate_executor_config(config) and context.runtime.recovery_permission(config)
        return self.entry_permission(config)

    def restart_continuity_executor_class(self, config):
        """Restore persisted p23 tranches with their exit floor and limit-only close policy."""
        from hummingbot.strategy_v2.executors.restart_continuity import RestartContinuityError
        if (self._context(config) is None or getattr(config, 'side', None) != TradeType.BUY
                or not str(getattr(config, 'level_id', '')).startswith('modular_spot:v1:')):
            raise RestartContinuityError('Persisted executor is not a Meridian p23 tranche')
        return ModularPositionExecutor

    def get_executor_class_for_config(self, config):
        context = self._context(config)
        if context is None or not super().validate_executor_config(config):
            raise ValueError('Meridian executor request is outside its symbols or the controller is stopped')
        return context.runtime.executor_class(config)

    # ------------------------------------------------------------------ reporting
    def get_custom_info(self) -> dict:
        """One portfolio tile with five symbol rows; values trace to native state or are None."""
        target, admission = self.portfolio.last_target, self.portfolio.last_admission
        entries = {s.trading_pair: s.entry_enabled for s in self.config.symbols}
        rows = {}
        for pair, context in self.contexts.items():
            change = None
            if target is not None:
                change = next(str(q.change) for q in target.quantities if q.symbol == pair)
            rows[pair] = {'entry_enabled': entries[pair], 'status': context.runtime.reason.split(':', 1)[0][:80],
                          'bag': context.runtime.bag_status, 'target_change': change,
                          'priority': self.config.priority.index(pair) + 1}
        return {
            'strategy_name': 'Meridian v3 unified', 'recipe_version': RECIPE_ID,
            'execution_mode': self.config.execution_mode, 'execution_enabled': self.config.execution_enabled,
            'operator': self.get_operator_state(), 'portfolio': dict(self.portfolio.status),
            'fault': self.fault, 'writer_epoch': self.portfolio.epoch,
            'limits': {'order_quote_including_fees': '20', 'asset_quote': '100', 'envelope_quote': '500'},
            'capital_scope': ('PAPER_WALLET' if self.config.execution_mode == 'paper'
                              else 'SHARED_WALLET_BOUNDED' if self.config.live_capital else 'CAPITAL_DOMAIN_UNPROVEN'),
            'loss_rails': 'none',
            'target': None if target is None else {'status': target.status, 'reasons': list(target.reasons),
                                                   'sha256': target.identity, 'decision_time_ms': target.decision_time_ms,
                                                   'requested_quote': str(target.requested_acquisition_quote)},
            'admission': None if admission is None else admission.model_dump(mode='json'),
            'symbols': rows,
        }

    def to_format_status(self):
        lines = [f'Meridian v3 unified | {RECIPE_ID} | {self.config.execution_mode} | '
                 f'execution={self.config.execution_enabled} | portfolio={self.portfolio.status.get("reason")}']
        for pair, context in self.contexts.items():
            lines.append(f'  {pair}: {context.runtime.reason} | bag={context.runtime.bag_status}')
        return lines
