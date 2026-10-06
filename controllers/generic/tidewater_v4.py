"""Tidewater (V4): Meridian V3's unified runtime with paced capital.

Same five symbol contexts, p23 lifecycle, native grants, protected exits and held bags as
Meridian V3 (`meridian_v3_unified.py`, whose operator vocabulary applies unchanged). Tidewater
changes admission only:

* tide gate: buys of one pair spread out as portfolio occupancy rises;
* per-pair limit: one pair holds at most its stage limit (350 of a 1,000 envelope);
* decision receipts for every refusal, admission and resting-entry revocation;
* `resting_entry='stable'` (opt-in): a submitted entry is revoked only for hard reasons.

None of these cancels an exit, blocks a recovery sell or demotes a position.
"""
import time
from decimal import Decimal

from controllers.generic.meridian_v3_unified import MeridianUnifiedController
from controllers.generic.meridian_v3_unified_core.config import RECIPE_ID as MERIDIAN_RECIPE_ID
from controllers.generic.meridian_v3_unified_core.market_context import MarketContextObserver
from controllers.generic.meridian_v3_unified_core.symbols import SymbolContext, SymbolRuntime
from controllers.generic.modular_spot_core.domain import DataUnavailable, decimal
from controllers.generic.tidewater_v4_core.config import PAIRS, RECIPE_ID, TidewaterConfig, source_files
from controllers.generic.tidewater_v4_core.portfolio import TidewaterPortfolio
from controllers.generic.tidewater_v4_core.receipts import REVOKED
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
from hummingbot.strategy_v2.components.source_identity import component_source_files
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase

CONFIG_CLASS = 'TidewaterConfig'
__all__ = ['TidewaterConfig', 'TidewaterController']
ZERO = Decimal('0')
_ABSENT = object()
# Entry lifecycle states with no open entry order left: a denial there cancels nothing.
AFTER_ENTRY = frozenset({'managing_fill', 'ended_no_fill'})


class TidewaterController(MeridianUnifiedController):

    @classmethod
    def implementation_source_files(cls):
        return [*source_files(), *component_source_files()]

    def __init__(self, config, market_data_provider, actions_queue, **kwargs):
        # Meridian's constructor admits only its exact configuration type; Tidewater wires the
        # same parts around its own configuration and portfolio.
        if type(config) is not TidewaterConfig:
            raise ValueError('Exact Tidewater configuration required')
        config = TidewaterConfig.model_validate(config.model_dump(mode='json'))
        paper = config.execution_mode == 'paper'
        if paper and not is_native_paper_connector(market_data_provider.connectors.get(config.connector_name)):
            raise ValueError('Tidewater paper requires the exact native paper connector')
        ControllerBase.__init__(self, config, market_data_provider, actions_queue, **kwargs)
        self._symbol_source = self._symbol_configs = None
        self.operator_entry_paused = False
        self._operator_state = {'entry_paused': False, 'reason': None, 'scope': None,
                                'updated_at': None, 'last_command_id': None}
        self.portfolio = TidewaterPortfolio(self)
        self.contexts = {pair: SymbolContext(self, pair) for pair in PAIRS}
        for context in self.contexts.values():
            context.runtime = SymbolRuntime(context, self.portfolio, paper=paper)
        self.fault = None
        self.market_context = MarketContextObserver(config.market_context, clock_ms=self.portfolio.now_ms,
                                                    logger=self.logger())
        self._data_close_tasks = set()
        self._data_close_error = None
        self._last_denial = None  # the latest strict denial predicate, for reporting

    # ------------------------------------------------------------------ resting entries (T7)
    def _stable_hold(self, context, config):
        """The hard reason a submitted entry must end, or None to keep it resting.

        Spread, book freshness, candle availability and shared-data resynchronization are
        submit-time conditions; they do not end an order that is already resting at its limit.
        """
        runtime = context.runtime
        c = runtime.controller.config
        if not ControllerBase.validate_executor_config(self, config):
            return 'controller_stopped_or_kill_switch'
        held = self.entries_held()
        if held is not None:
            return held
        if self.portfolio.identity_hold is not None:
            return 'identity_hold'
        if self.fault:
            return 'portfolio_fault'
        if not c.execution_enabled or runtime.fault:
            return 'symbol_fault_or_execution_disabled'
        if runtime.pending.get(config.id) != config:
            return 'claim_not_pending'
        if c.fingerprint() != runtime.fingerprint:
            return 'configuration_changed'
        state, _ = runtime.ensure_store().load()
        if state is None or state.fingerprint != runtime.fingerprint or config.id not in state.claims:
            return 'claim_not_recorded'
        if not self.portfolio.grant_bound(context.pair, runtime.reservation(config)):
            return 'grant_not_bound'
        now = decimal(self.market_data_provider.time(), 'now')
        timestamp = decimal(config.timestamp, 'entry timestamp')
        if now < timestamp or now - timestamp > c.acquisition.entry_timeout_seconds:
            return 'entry_timeout'
        if runtime.inputs_revised(runtime.pending_inputs.get(config.id)):
            return 'entry_input_revision_changed'
        return None

    # Methods Meridian's permission check reads, in its order (`SymbolRuntime.entry_permission`
    # then `ModularRuntime.entry_permission`). Their results attribute a denial to its branch.
    OBSERVED = ('grant_bound', 'snapshot', 'observe', 'acquisition_permitted', 'inputs_revised', 'fees_permit')

    def _observed_permission(self, context, config, check_funds):
        """Meridian's own permission decision, with the values it read recorded on the side.

        Each read is wrapped on the instance for this one synchronous call and restored after.
        The wrappers return exactly what the original returned and re-raise what it raised.
        """
        runtime = context.runtime
        targets = {'grant_bound': self.portfolio, 'snapshot': runtime, 'observe': runtime,
                   'acquisition_permitted': runtime, 'inputs_revised': runtime, 'fees_permit': runtime.bindings}
        seen = {}

        def wrap(name, original):
            def recorded(*args, **kwargs):
                try:
                    result = original(*args, **kwargs)
                except BaseException as exc:
                    seen.setdefault(name, ('raised', exc))
                    raise
                seen.setdefault(name, ('returned', result))
                return result
            return recorded

        installed = []
        fault, reason = runtime.fault, runtime.reason
        try:
            for name in self.OBSERVED:
                target = targets[name]
                previous = target.__dict__.get(name, _ABSENT)
                setattr(target, name, wrap(name, getattr(target, name)))
                installed.append((target, name, previous))
            allowed = super().entry_permission(config, check_funds=check_funds)
        finally:
            for target, name, previous in reversed(installed):
                if previous is _ABSENT:
                    delattr(target, name)
                else:
                    setattr(target, name, previous)
        return allowed, seen, fault, reason

    def _attribute(self, context, config, check_funds, seen, fault_before, reason_before):
        """The branch that denied, from the values Meridian's check actually read. No re-reads."""
        runtime = context.runtime
        c = runtime.controller.config
        for name in self.OBSERVED:  # a read that raised ended Meridian's check right there
            outcome, value = seen.get(name, (None, None))
            if outcome == 'raised':
                kind = 'data_hold' if isinstance(value, DataUnavailable) else 'entry_permission_hold'
                if name == 'grant_bound':
                    kind = 'portfolio_grant_hold'
                return f'{kind}:{name}:{type(value).__name__}:{value}'
        if runtime.fault and runtime.fault != fault_before:  # a read outside the observed set raised
            return f'entry_permission_hold:{runtime.fault}'
        if 'grant_bound' not in seen:
            if not ControllerBase.validate_executor_config(self, config):
                return 'controller_stopped_or_kill_switch'
            return 'claim_missing'
        if seen['grant_bound'][1] is not True:
            return 'portfolio_grant_required'
        if 'snapshot' not in seen:
            if not c.execution_enabled:
                return 'execution_disabled'
            if c.manual_kill_switch:
                return 'kill_switch'
            if fault_before:
                return f'symbol_fault:{fault_before}'
            if runtime.pending.get(config.id) != config:
                return 'claim_not_pending'
            return 'checkpoint_claim_or_budget'
        if 'observe' not in seen:
            return 'entry_timeout'  # the only denial between the snapshot and observation
        if seen['observe'] == ('returned', None):
            return f'data_unavailable:{runtime.reason}'
        if seen.get('acquisition_permitted') == ('returned', False):
            return f'acquisition_not_permitted:{runtime.reason}'
        if seen.get('inputs_revised') == ('returned', True):
            return 'entry_input_revision_changed'
        if 'fees_permit' not in seen:
            snapshot = seen['snapshot'][1]
            bid, ask = snapshot['bid'], snapshot['ask']
            if ask < bid or (ask - bid) / bid > c.risk.max_spread_fraction:
                return 'spread_limit'
            required = config.amount * config.entry_price * (1 + c.risk.fee_reserve_fraction)
            if check_funds and required + c.risk.cash_reserve_quote > snapshot['available_quote']:
                return 'insufficient_funds'
            return 'risk_paused'
        if seen['fees_permit'] == ('returned', False):
            return 'fee_reserve_exceeded'
        return 'unattributed'

    def entry_permission(self, config, *, check_funds=True):
        context = self._context(config)
        if context is None:
            return super().entry_permission(config, check_funds=check_funds)
        submitted = not check_funds  # the executor stops checking funds once its order is submitted
        if submitted and self.config.resting_entry == 'stable':
            try:
                predicate = self._stable_hold(context, config)
            except Exception as exc:  # an unprovable hard check ends the entry; it never raises into the executor
                predicate = f'stable_check_error:{type(exc).__name__}:{exc}'[:200]
            allowed = predicate is None
        else:
            allowed, seen, fault, reason = self._observed_permission(context, config, check_funds)
            predicate = None
            if not allowed:
                try:
                    predicate = 'strict:' + self._attribute(context, config, check_funds, seen, fault, reason)
                except Exception as exc:  # attribution explains a denial; it can never change or delay it
                    predicate = f'strict:unattributed:{type(exc).__name__}:{exc}'
                predicate = predicate[:200]
            self._last_denial = predicate
        if not allowed and self._entry_state(config.id) not in AFTER_ENTRY:
            self._record_denial(context.pair, config, predicate, 'submitted' if submitted else 'pre_submission')
        return allowed

    def _entry_state(self, executor_id):
        """The executor's own entry lifecycle state, from the in-memory executor reports.

        After a full fill the executor still asks permission each tick; past the entry window
        the answer is no, and the native stop is a no-op. That is not a revocation, so it gets
        no receipt (Meridian labels those executors `modular_entry_permission_revoked`).
        """
        for info in self.executors_info:
            if info.id == executor_id:
                lifecycle = (info.custom_info or {}).get('entry_lifecycle') or {}
                return lifecycle.get('state')
        return None

    def _record_denial(self, pair, config, predicate, phase):
        written = self.portfolio.receipts.record(
            REVOKED, pair=pair, cut=config.id, code=phase, dedupe=predicate,
            payload={'executor_id': config.id, 'phase': phase, 'predicate': predicate,
                     'resting_entry': self.config.resting_entry,
                     'entry_price': str(config.entry_price), 'amount': str(config.amount)},
            now=self.market_data_provider.time())
        if written:
            self.logger().info(f'Tidewater {pair} entry permission denied phase={phase} executor={config.id} '
                               f'predicate={predicate}')

    # ------------------------------------------------------------------ reporting (T6)
    def _inventory_view(self):
        now = self.market_data_provider.time()
        rows = {}
        for pair, context in self.contexts.items():
            active = [i for i in context.executors_info if i.is_active]
            held = [p for p in context.positions_held]
            rows[pair] = {
                'active_tranches': len(active),
                'active_cost_quote': str(sum((Decimal(str(i.filled_amount_quote or 0)) for i in active), ZERO)),
                'oldest_active_age_s': None if not active else round(now - min(i.timestamp for i in active)),
                'held_amount': str(sum((p.amount for p in held), ZERO)),
                'held_cost_quote': str(sum((p.amount * p.breakeven_price for p in held), ZERO)),
                # Held positions carry no native acquisition time; bag age is not reported.
                'held_age_s': None,
            }
        return rows

    def get_custom_info(self) -> dict:
        info = super().get_custom_info()
        info.update(strategy_name='Tidewater v4', recipe_version=RECIPE_ID, tidewater={
            'tide_gate': None if self.config.tide_gate is None else self.config.tide_gate.model_dump(mode='json'),
            'pair_limit_quote': str(self.config.pair_limit_quote()),
            'resting_entry': self.config.resting_entry,
            'occupancy': self.portfolio.occupancy,
            'pacing': dict(self.portfolio.pacing),
            'inventory': self._inventory_view(),
            'receipts': {'written': self.portfolio.receipts.written, 'fault': self.portfolio.receipts.fault},
            'observed_at': time.time()})
        return info

    def to_format_status(self):
        lines = super().to_format_status()
        occupancy = self.portfolio.occupancy or {}
        lines[0] = lines[0].replace('Meridian v3 unified', 'Tidewater v4').replace(MERIDIAN_RECIPE_ID, RECIPE_ID)
        lines.insert(1, f'  occupancy={occupancy.get("u")} multiplier={occupancy.get("multiplier")} '
                        f'pair_limit={self.config.pair_limit_quote()} resting_entry={self.config.resting_entry}'
                        f' receipt_fault={bool(self.portfolio.receipts.fault)}')
        return lines
