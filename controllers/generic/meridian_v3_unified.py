"""Meridian v3 unified: one native controller, five symbol contexts, one portfolio.

Symbol contexts run the frozen p23 lifecycle (features, bags, trailing exits).
Only the portfolio admits acquisitions, through native grants in this owner's
recorder. Exits and held-inventory management never wait for portfolio alpha.
Live spending additionally requires an explicit bounded shared-wallet capital binding.

Operator vocabulary. None of these controls sells, liquidates, or demotes a position:

* Entry pause (`set_operator_entry_pause`, `*_pause_entries` commands): no new
  acquisition; a resting unfilled entry ends at its next permission check. Filled
  tranches keep trailing, held bags keep their recovery sells.
* Kill switch (`manual_kill_switch`): the same entry hold, persisted in the config.
  The V2 script also stops every acquiring BUY executor with `acquisition_only`, so
  a partial fill keeps its filled part as an active trailing tranche. The owner is
  not stopped, so its management continues. Clearing the switch resumes entries.
* Cancel pending entries (`bot_cancel_pending_entries`): one-off `acquisition_only`
  stop of resting entries; no hold on later entries.
* Suspend (planned restart, `owner_restart.py`, shutdown): executors persist exactly
  and resume on start; not a trading control.
* Demotion to a held bag (`keep_position=True` without `acquisition_only`) is strategy
  management only (the -3 % bag line, unprovable restart state), never an operator stop.
"""
import asyncio
import time
from contextlib import nullcontext

from controllers.generic.meridian_v3_unified_core.config import PAIRS, RECIPE_ID, MeridianUnifiedConfig, source_files
from controllers.generic.meridian_v3_unified_core.market_context import MarketContextObserver
from controllers.generic.meridian_v3_unified_core.portfolio import MeridianPortfolio
from controllers.generic.meridian_v3_unified_core.symbols import SymbolContext, SymbolRuntime
from controllers.generic.modular_spot_core.bag import is_recovery_sell
from controllers.generic.modular_spot_core.data_reader import attach_pool
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
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
    # A stale or unavailable symbol holds its own acquisitions (and every new acquisition
    # while the portfolio cannot mark it); the other symbols' exits and bags keep running.
    per_symbol_book_freshness = True
    # The generic kill switch stops and demotes every executor; this owner's stops buying only.
    kill_switch_stops_acquisition_only = True

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
        self.market_context = MarketContextObserver(config.market_context, clock_ms=self.portfolio.now_ms,
                                                    logger=self.logger())

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
        """Per-symbol native configuration, rebuilt only when the owner config object changes.

        The p23 runtime's own kill switch would stop exits too; this owner applies the
        switch as an entry hold (`entries_held`), so symbols never see it.
        """
        if self._symbol_source is not self.config:
            configs = self.config.symbol_configs()
            self._symbol_configs = {p: c.model_copy(update={'manual_kill_switch': False}) for p, c in configs.items()}
            self._symbol_source = self.config
        return self._symbol_configs[pair]

    def entries_held(self):
        """Why no symbol may acquire now, or None: the kill switch, then the entry pause."""
        if self.config.manual_kill_switch:
            return 'kill_switch_entry_hold'
        if self.operator_entry_paused:
            return 'operator_entry_pause'
        return None

    def _running(self):
        return self.status == RunnableStatus.RUNNING

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
        tasks = [c.runtime.close_data() for c in self.contexts.values()] + [self.market_context.close()]
        return [task for task in tasks if task is not None]

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
        # One synchronous decision over five symbols reads the Executors table about 25 times;
        # they share one read, cleared by any Executors write (see MarketsRecorder).
        recorder = NativeBindings.recorder()
        with nullcontext() if recorder is None else recorder.shared_executor_reads():
            return self._determine_executor_actions()

    def _determine_executor_actions(self):
        self.portfolio.begin_tick()
        markets = {pair: c.runtime.market for pair, c in self.contexts.items()}
        try:
            identity_hold = self.portfolio.check_identity()  # records its own hold status
            if identity_hold is None and self.config.manual_kill_switch:
                self.portfolio.status = {'reason': 'kill_switch_entry_hold', 'hold': None}
            elif identity_hold is None:
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
        self.market_context.after_cycle(self.portfolio)  # observe-only; never raises, never read back
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

    def quote_permission(self, trading_pair):
        """Price-dependent management requires this pair's current applied book.

        This is independent of acquisition switches and performance reports. Native
        order/fill reconciliation and already-resting profit exits keep running.
        """
        context = self.contexts.get(trading_pair)
        if context is None:
            return False
        try:
            context.runtime.bindings.fresh_book(context)
            return True
        except (ValueError, TypeError, AttributeError, KeyError, RuntimeError):
            return False

    def validate_executor_config(self, config):
        context = self._context(config)
        if context is None or config.id in context.runtime.dispatched:
            return False
        if is_recovery_sell(config):
            # An exit of held inventory: neither the kill switch nor an entry pause stops it.
            return self._running() and context.runtime.recovery_permission(config)
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
        allowed = self._running() if is_recovery_sell(config) else super().validate_executor_config(config)
        if context is None or not allowed:
            raise ValueError('Meridian executor request is outside its symbols or the controller is stopped')
        selected = context.runtime.executor_class(config)
        if is_recovery_sell(config):
            from controllers.generic.meridian_v3_unified_core.execution import MeridianRecoveryExecutor
            return MeridianRecoveryExecutor
        return selected

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
            'fault': self.fault, 'writer_epoch': self.portfolio.epoch, 'clock': self.portfolio.clock,
            'identity_hold': self.portfolio.identity_hold,
            'capital_stage': self.config.capital_stage,
            'limits': {'order_quote_including_fees': str(self.config.stage().order_quote),
                       'asset_quote': str(self.config.stage().per_asset_quote),
                       'envelope_quote': str(self.config.stage().envelope_quote)},
            'capital_scope': ('PAPER_WALLET' if self.config.execution_mode == 'paper'
                              else 'SHARED_WALLET_BOUNDED' if self.config.live_capital else 'CAPITAL_DOMAIN_UNPROVEN'),
            'loss_rails': 'none',
            'target': None if target is None else {'status': target.status, 'reasons': list(target.reasons),
                                                   'sha256': target.identity, 'decision_time_ms': target.decision_time_ms,
                                                   'requested_quote': str(target.requested_acquisition_quote)},
            'admission': None if admission is None else admission.model_dump(mode='json'),
            'symbols': rows,
            'market_context': self.market_context.custom_info(),
        }

    def to_format_status(self):
        lines = [f'Meridian v3 unified | {RECIPE_ID} | {self.config.execution_mode} | {self.config.capital_stage} | '
                 f'execution={self.config.execution_enabled} | portfolio={self.portfolio.status.get("reason")}',
                 f'  {self.market_context.status_line()}']
        for pair, context in self.contexts.items():
            lines.append(f'  {pair}: {context.runtime.reason} | bag={context.runtime.bag_status}')
        return lines
