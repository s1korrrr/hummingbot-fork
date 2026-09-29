"""Five symbol contexts inside one native owner, each running the unchanged p23 runtime.

A context sees only its own instrument: executors, held positions and recorder
rows of the portfolio owner are filtered by trading pair, because the native
inventory and bag readers treat any other pair as a scope violation. Other
owners' rows pass through unchanged so foreign-writer checks still see them.

Capital never comes from the symbol: its budget is the portfolio grant store.
A symbol can bind a grant the portfolio admitted; it cannot create one.
"""
from decimal import Decimal

from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
from controllers.generic.modular_spot_core.runtime import ModularRuntime
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector


def _pair(row):
    config = getattr(row, 'config', None)
    return getattr(config, 'trading_pair', None) if config is not None else getattr(row, 'trading_pair', None)


class PairScopedRecorder:
    """Native recorder reads for one symbol of one owner. Writes are delegated unchanged."""

    def __init__(self, recorder, owner_id, pair):
        self._recorder, self._owner, self._pair = recorder, owner_id, pair

    def __getattr__(self, name):
        return getattr(self._recorder, name)

    def _keep(self, row):
        return getattr(row, 'controller_id', None) != self._owner or _pair(row) == self._pair

    def get_all_executors(self):
        return [row for row in self._recorder.get_all_executors() if self._keep(row)]

    def get_executors_by_controller(self, controller_id):
        rows = self._recorder.get_executors_by_controller(controller_id)
        return rows if controller_id != self._owner else [row for row in rows if _pair(row) == self._pair]

    def get_positions_by_controller(self, controller_id):
        rows = self._recorder.get_positions_by_controller(controller_id)
        return rows if controller_id != self._owner else [row for row in rows if _pair(row) == self._pair]


class SymbolBindings(NativeBindings):
    def __init__(self, owner_id, pair):
        self.owner_id, self.pair = owner_id, pair

    def snapshot(self, controller):
        # Meridian has no loss rail: the p23 risk update receives no PnL, so drawdown
        # and daily-loss pauses can never trigger. Freshness checks of the native
        # report, book and account still apply before any entry.
        return {**super().snapshot(controller), 'pnl': Decimal('0')}

    def recorder(self):
        recorder = NativeBindings.recorder()
        return None if recorder is None else PairScopedRecorder(recorder, self.owner_id, self.pair)


class PaperSymbolBindings(SymbolBindings):
    @staticmethod
    def spot_connector(controller):
        connector = controller.market_data_provider.connectors.get(controller.config.connector_name)
        if not is_native_paper_connector(connector):
            raise PolicyError('Meridian paper requires the exact native paper connector')
        return connector


class SymbolGrantBudget:
    """The runtime's program-budget interface, answered by portfolio grants for one pair."""

    def __init__(self, portfolio, pair):
        self.portfolio, self.pair = portfolio, pair
        self.key = portfolio.store().key + ':' + pair
        # The claim SymbolRuntime.entry_permission proved bound by a fresh read earlier in the
        # same synchronous call; the base check's repeat of that read is answered from it.
        self.verified = None

    def _claim(self, claim):
        owner = self.portfolio.owner.config
        if (claim.trading_pair != self.pair or claim.controller_id != owner.id
                or claim.account != owner.account_id or claim.connector != owner.connector_name):
            raise PolicyError('symbol reservation scope differs from the portfolio owner')
        return claim

    # Entry-path failures deny the entry. They must not fault the symbol runtime,
    # because a faulted runtime also stops its bag and exit management.
    def reserve(self, claim, *, native_executors=()):
        try:
            return self.portfolio.bind_grant(self.pair, self._claim(claim))
        except PolicyError as exc:
            self.portfolio.owner.logger().warning(f'Meridian {self.pair} grant bind held: {exc}')
            return False

    def contains(self, claim, *, native_executors=()):
        try:
            claim = self._claim(claim)
            if self.verified is not None and self.verified == claim:
                return True
            return self.portfolio.grant_bound(self.pair, claim)
        except PolicyError as exc:
            self.portfolio.owner.logger().warning(f'Meridian {self.pair} grant check held: {exc}')
            return False

    def occupied_pairs(self, *, native_executors=()):
        # Cross-symbol capital is portfolio-owned; a symbol's own occupancy never gates itself.
        return ()

    def released(self):
        return self.portfolio.store().released()

    def release_held(self, controller_id, executor_ids, *, native_executors=()):
        if controller_id != self.portfolio.owner.config.id:
            raise PolicyError('release requires the portfolio owner')
        return self.portfolio.release_held(self.pair, executor_ids)


class SymbolContext:
    """The controller surface one p23 runtime reads, restricted to one instrument."""

    def __init__(self, owner, pair):
        self.owner, self.pair = owner, pair
        self.runtime = None

    @property
    def config(self):
        return self.owner.symbol_config(self.pair)

    @property
    def market_data_provider(self):
        return self.owner.market_data_provider

    def logger(self):
        return self.owner.logger()

    @property
    def executors_info(self):
        return [info for info in self.owner.executors_info if _pair(info) == self.pair]

    @property
    def positions_held(self):
        return [position for position in self.owner.positions_held if _pair(position) == self.pair]

    @property
    def performance_report(self):
        return self.owner.performance_report

    def native_report_age(self):
        return self.owner.native_report_age()

    @property
    def _operator_entry_paused(self):
        return self.owner.operator_entry_paused

    def entry_permission(self, config, *, check_funds=True):
        return self.owner.entry_permission(config, check_funds=check_funds)


class SymbolRuntime(ModularRuntime):
    """The p23 runtime with portfolio admission as its only acquisition authority."""

    def __init__(self, context, portfolio, *, paper):
        self.portfolio = portfolio
        bindings = (PaperSymbolBindings if paper else SymbolBindings)(context.owner.config.id, context.pair)
        super().__init__(context, bindings=bindings)

    def actions(self):
        # Checkpoints bound to another code/config identity: no entry, recovery sell or
        # checkpoint write until the operator migration; protective bag stops continue.
        hold = self.portfolio.identity_hold
        if hold is not None:
            self.reason = hold[:200]
            return self._bag_stops_only()
        return super().actions()

    def ensure_store(self):
        store = super().ensure_store()
        if type(self.budget) is not SymbolGrantBudget:
            self.budget = SymbolGrantBudget(self.portfolio, self.controller.pair)
        return store

    def entry_permission(self, config, *, check_funds=True):
        # Check the portfolio grant first: a missing or unreadable grant denies this
        # entry. Inside the base check the same condition would fault the runtime and
        # stop its exits.
        if config is None:
            return False
        try:
            self.ensure_store()
            claim = self.reservation(config)
            bound = self.portfolio.grant_bound(self.controller.pair, claim)
        except PolicyError as exc:
            self.reason = f'portfolio_grant_hold:{exc}'[:200]
            return False
        if not bound:
            self.reason = 'portfolio_grant_required'
            return False
        self.budget.verified = claim  # nothing awaits before the base check reads it
        try:
            return super().entry_permission(config, check_funds=check_funds)
        finally:
            self.budget.verified = None

    def _release_bag(self, held, mid, snapshot):
        # An unprovable portfolio release keeps the capital reserved; it never faults exits.
        try:
            return super()._release_bag(held, mid, snapshot)
        except PolicyError as exc:
            self.controller.logger().warning(f'Meridian {self.controller.pair} held release deferred: {exc}')
            return 'unproven:portfolio_grant_hold'

    def acquisition_permitted(self):
        if not super().acquisition_permitted():
            return False
        try:
            if self.portfolio.acquisition_open(self.controller.pair):
                return True
            self.reason = 'portfolio_grant_required'
        except PolicyError as exc:
            self.reason = f'portfolio_grant_hold:{exc}'[:200]
        return False
