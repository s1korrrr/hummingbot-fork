"""The single strategic writer: native snapshot -> joint P0 target -> native grants.

Evaluation runs once per new confirmed candle cut in which a symbol shows fresh
p23 eligibility. The first cut a symbol observes after start only seeds it, so a
reconnect cannot turn an already-closed crossing into a catch-up entry.

Hummingbot runs controllers on one event loop and this evaluation has no await,
so native fills cannot interleave between the snapshot and admission. Grants are
still rechecked against durable usage inside one compare-and-swap write.
"""
from decimal import Decimal

from controllers.generic import meridian_v3_recipe as recipe
from controllers.generic.meridian_v3_portfolio import BNBMRFeatures, BNBMRObservation, MeridianPortfolioRecipe
from controllers.generic.modular_spot_core.bag import HeldBag
from controllers.generic.modular_spot_core.domain import DataUnavailable, PolicyError, decimal, identity
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
from controllers.generic.modular_spot_core.native_funding import funding_projection
from controllers.generic.modular_spot_core.signals import signal_decision
from controllers.generic.modular_spot_core.terminal_dust import filled_base, terminal_dust
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
from hummingbot.strategy_v2.components.decision_protocol import DecisionScope, ObservationRef, Opportunity
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.controllers.native_portfolio_bridge_v3 import PortfolioGrantStore
from hummingbot.strategy_v2.controllers.portfolio_target_store_v3 import NativeTargetStore
from hummingbot.strategy_v2.portfolio_v3.contracts import AssetState, Commitment, PortfolioSnapshot
from hummingbot.strategy_v2.portfolio_v3.runtime_contracts_v1 import (
    CAPITAL_DOMAIN_UNPROVEN,
    EXECUTION_CAPABILITY_UNBOUND,
    GRANT_STORE_HOLD,
    NATIVE_INVENTORY_MISMATCH,
    NATIVE_SNAPSHOT_UNAVAILABLE,
)

from .config import PORTFOLIO_ID, PRODUCER
from .entry_preview import preview_entry
from .migration import IDENTITY_UNVERIFIED, IdentityGuard, epoch, grant_policy, proposal_writer

ZERO = Decimal('0')
ERRORS = (ValueError, TypeError, ArithmeticError, KeyError, AttributeError, OSError)


class MeridianPortfolio:
    def __init__(self, owner):
        self.owner = owner
        config = owner.config
        self.recipe = MeridianPortfolioRecipe(config.mandate(), config.p0_policy())
        self.epoch = epoch(config, self.recipe)
        self.fence = 1
        self.identity = IdentityGuard(config)
        self.identity_hold = None  # explicit reason while recorder state is bound to another identity
        self.source_sha256 = digest(config.source_manifest())
        self._store = self._targets = None
        self._seen = {}  # pair -> first cut observed by this process
        self._evaluated = {}  # pair -> last cut evaluated for admission
        self._admitted = ()  # grant ids from the current evaluation, withdrawn if unused
        self._tick = None  # (evidence, live grants) read once per controller tick
        self.last_target = self.last_admission = None
        self.status = {'reason': 'no_evaluation', 'hold': None}
        self.clock = {'candles': {}}  # timestamps of the last evaluation, each under its real meaning

    # ------------------------------------------------------------------ native owners
    def recorder(self):
        recorder = NativeBindings.recorder()
        if recorder is None:
            raise PolicyError('initialized native recorder required')
        return recorder

    def store(self):
        if self._store is None:
            self._store = PortfolioGrantStore(self.recorder(), grant_policy(self.owner.config, self.recipe))
        return self._store

    def target_store(self):
        if self._targets is None:
            self._targets = NativeTargetStore(self.recorder().sql_manager.get_new_session,
                                              proposal_writer(self.owner.config, self.recipe, fence=self.fence))
        return self._targets

    def check_identity(self):
        """Hold every acquisition while recorder state is bound to another code/config identity.

        Returns None once bound; the reason otherwise. Symbol runtimes keep bag stops and
        recovery sells, never buys, until an operator applies the identity migration.
        """
        try:
            recorder = self.recorder()
            hold = self.identity.check(recorder.sql_manager.get_new_session, recorder.config_file_path)
        except (PolicyError, *ERRORS) as exc:
            hold = f'{IDENTITY_UNVERIFIED}:{type(exc).__name__}:{exc}'[:300]
        if hold != self.identity_hold and hold is not None:
            self.owner.logger().error(f'Meridian entries held until identity migration: {hold}')
        self.identity_hold = hold
        if hold is not None:
            self.status = {'reason': hold.split(':', 1)[0], 'hold': hold}
        return hold

    def evidence(self):
        """Every recorded executor of this recorder, with current active state preferred."""
        if self._tick is not None:
            return self._tick[0]
        rows = {}
        for info in self.recorder().get_all_executors():
            if not isinstance(info.id, str) or not info.id or info.id in rows:
                raise PolicyError('duplicate or missing recorded executor identity')
            rows[info.id] = info
        current_ids = set()
        for info in self.owner.executors_info:
            if not isinstance(info.id, str) or not info.id or info.id in current_ids:
                raise PolicyError('duplicate or missing current executor identity')
            current_ids.add(info.id)
            recorded = rows.get(info.id)
            if recorded is not None:
                recorded_custom = recorded.custom_info or {}
                recorded_receipt = recorded_custom.get('execution_receipt') or {}
                recorded_dust = decimal(recorded_receipt.get('residual_dust_base', '0'), 'recorded terminal dust')
                if recorded_dust > 0:
                    current_custom = info.custom_info or {}
                    current_receipt = current_custom.get('execution_receipt') or {}
                    current_dust = decimal(current_receipt.get('residual_dust_base', '0'), 'current terminal dust')
                    if current_dust != recorded_dust:
                        raise PolicyError('current executor erased recorded terminal dust')

                    def terminal_proof(row, custom):
                        config = getattr(row, 'config', None)
                        if config is None or getattr(config, 'side', None) is None:
                            raise PolicyError('terminal dust configuration unavailable')
                        receipt = custom.get('execution_receipt')
                        if not isinstance(receipt, dict):
                            raise PolicyError('terminal dust receipt unavailable')
                        receipt = dict(receipt)
                        for field in ('residual_dust_base', 'remaining_owned_base', 'entry_gross_base',
                                      'entry_gross_quote', 'exit_gross_base', 'exit_gross_quote'):
                            receipt[field] = decimal(receipt.get(field), f'terminal dust {field}').normalize()
                        close_quantity = custom.get('close_quantity')
                        if not isinstance(close_quantity, dict):
                            raise PolicyError('terminal dust lot unavailable')
                        close_quantity = dict(close_quantity)
                        close_quantity['quantity_increment'] = decimal(
                            close_quantity.get('quantity_increment'), 'terminal dust lot', positive=True).normalize()
                        return digest({
                            'controller_id': row.controller_id,
                            'config_controller_id': config.controller_id,
                            'connector_name': config.connector_name,
                            'trading_pair': config.trading_pair,
                            'side': config.side.name,
                            'level_id': config.level_id,
                            'close_type': None if row.close_type is None else row.close_type.value,
                            'close_timestamp': decimal(row.close_timestamp, 'terminal dust close time',
                                                       positive=True).normalize(),
                            'is_done': row.is_done,
                            'is_active': row.is_active,
                            'receipt': receipt,
                            'order_ids': custom.get('order_ids'),
                            'orders': custom.get('execution_order_snapshots'),
                            'lot': close_quantity,
                            'entry_state': (custom.get('entry_lifecycle') or {}).get('state'),
                        })
                    if terminal_proof(recorded, recorded_custom) != terminal_proof(info, current_custom):
                        raise PolicyError('current executor changed recorded terminal dust proof')
            rows[info.id] = info
        return tuple(rows.values())

    def begin_tick(self):
        self._tick = None

    def _invalidate(self):
        self._tick = None

    def now_ms(self):
        return int(decimal(self.owner.market_data_provider.time(), 'now') * 1000)

    def execution_capable(self):
        """Whether admission may create spending authority; each reason is explicit."""
        config = self.owner.config
        if config.execution_mode == 'observation' or not config.execution_enabled or config.recipe_binding is None:
            return False, EXECUTION_CAPABILITY_UNBOUND
        if config.execution_mode == 'paper':
            connector = self.owner.market_data_provider.connectors.get(config.connector_name)
            return (True, None) if is_native_paper_connector(connector) else (False, CAPITAL_DOMAIN_UNPROVEN)
        if config.live_capital is None:
            return False, CAPITAL_DOMAIN_UNPROVEN
        return True, None

    # ------------------------------------------------------------------ symbol-facing grant API
    def _live(self):
        if self._tick is None:
            evidence = self.evidence()
            self._tick = (evidence, self.store().grants(now_ms=self.now_ms(), native_executors=evidence))
        return self._tick[1]

    def acquisition_open(self, pair):
        return any(g.symbol == pair for g in self._live())

    def bind_grant(self, pair, claim):
        self._invalidate()
        try:
            return self.store().bind(executor_id=claim.executor_id, symbol=pair, reserved_quote=claim.reserved_quote,
                                     now_ms=self.now_ms(), native_executors=self.evidence())
        finally:
            self._invalidate()

    def grant_bound(self, pair, claim):
        """The last native dispatch boundary: always a fresh read, never the tick cache."""
        self._invalidate()
        return self.store().contains(executor_id=claim.executor_id, symbol=pair, reserved_quote=claim.reserved_quote,
                                     native_executors=self.evidence(), now_ms=self.now_ms())

    def release_held(self, pair, executor_ids):
        self._invalidate()
        try:
            return self.store().release_held(symbol=pair, executor_ids=executor_ids,
                                             native_executors=self.evidence(), now_ms=self.now_ms())
        finally:
            self._invalidate()

    def withdraw_unused(self):
        admitted, self._admitted = self._admitted, ()
        if admitted:
            self._invalidate()
            self.store().withdraw(grant_ids=admitted, now_ms=self.now_ms(), native_executors=self.evidence())
            self._invalidate()

    # ------------------------------------------------------------------ evaluation
    def candidates(self, markets):
        """New p23 setups with complete executable planner proposals, in stable order."""
        signal = recipe.strategy_parameters()['signal']
        entries = {s.trading_pair: s.entry_enabled for s in self.owner.config.symbols}
        result, holds, previews = [], {}, {}
        now = self.owner.market_data_provider.time()
        entry_hold = self.owner.entries_held()
        for pair in sorted(markets):
            market = markets[pair]
            if market is None:
                continue
            cut = market.candle_open
            first = self._seen.setdefault(pair, cut)
            if cut <= first or cut <= self._evaluated.get(pair, first) or not entries[pair]:
                continue
            decision = signal_decision(signal, previous_rsi=market.previous_rsi, rsi=market.rsi,
                                       atr_fraction=market.atr_fraction, breakout=False, trend_up=False)
            if not decision.enter:
                continue
            try:
                proposal, reason = preview_entry(self.owner.contexts[pair].runtime, market,
                                                 now=now, first_observed_cut=first, entry_hold=entry_hold)
            except (DataUnavailable, PolicyError, RuntimeError, *ERRORS) as exc:
                proposal, reason = None, f'preview_unavailable:{type(exc).__name__}:{exc}'[:200]
            if proposal is not None:
                result.append(pair)
                previews[pair] = reason
            else:
                holds[pair] = reason
        self.status = {**self.status, 'candidate_holds': holds, 'candidate_previews': previews}
        return tuple(result)

    def observation(self, pair, market, runtime, now_ms):
        """The closed candle's open and close, and when this owner read its features.

        The features are final at the candle close, so that is the observation event;
        a close after the owner's clock is not a closed candle and fails validation.
        """
        features = BNBMRFeatures(previous_rsi=market.previous_rsi, rsi=market.rsi, atr_fraction=market.atr_fraction)
        open_ms = int(market.candle_open) * 1000
        closed_ms = open_ms + self.owner.symbol_config(pair).interval_seconds() * 1000
        lineage = runtime.input_identity
        if not isinstance(lineage, str):
            raise DataUnavailable('closed candle lineage unavailable')
        ref = ObservationRef(dataset_sha256=identity(['okx', pair, '1m', 'closed', lineage]), epoch='closed_1m',
                             revision=1, content_sha256=features.identity, event_time_ms=closed_ms,
                             available_time_ms=now_ms)
        opportunity = Opportunity(
            scope=DecisionScope(program_id=PORTFOLIO_ID, owner_id=PORTFOLIO_ID, connector='okx', trading_pair=pair,
                                recipe_sha256=self.recipe.recipe_sha256),
            setup=PRODUCER, action='acquire', primary=ref, created_at_ms=now_ms,
            lifetime_ms=self.owner.config.target_lifetime_ms)
        self.clock['candles'][pair] = {'open_ms': open_ms, 'closed_at_ms': closed_ms, 'read_at_ms': now_ms}
        return BNBMRObservation(opportunity=opportunity, features=features, candle_open_ms=open_ms,
                                feature_closed_at_ms=closed_ms)

    def evaluate(self, markets):
        pairs = self.candidates(markets)
        if not pairs:
            entry_hold = self.owner.entries_held()
            if entry_hold is not None:
                self.status = {**self.status, 'reason': 'ENTRY_HOLD', 'hold': entry_hold}
            return None
        now_ms = self.now_ms()
        self.clock = {'decision_cutoff_ms': now_ms, 'candles': {}}
        try:
            snapshot = self.snapshot(now_ms)
            observations = tuple(self.observation(pair, markets[pair], self.owner.contexts[pair].runtime, now_ms)
                                 for pair in pairs)
        except (DataUnavailable, PolicyError, *ERRORS) as exc:
            # Transient: this cut may be retried while it is still the current cut.
            self.status = {'reason': NATIVE_SNAPSHOT_UNAVAILABLE, 'hold': f'{type(exc).__name__}:{exc}'[:300]}
            return None
        target = self.recipe.propose(snapshot, observations, epoch=self.epoch, fence=self.fence)
        stored = self.target_store().head()
        self.target_store().publish(target, previous_sha256=None if stored is None else stored['target_sha256'],
                                    now_ms=now_ms)
        self.last_target = target
        capable, reason = self.execution_capable()
        self._invalidate()
        try:
            outcome = self.store().admit(
                target, priority=self.owner.config.priority,
                setup_ids={pair: f'{PRODUCER}:{markets[pair].candle_open * 1000}' for pair in pairs},
                now_ms=now_ms, submit_deadline_ms=now_ms + self.owner.config.target_lifetime_ms,
                native_executors=self.evidence(), execution_capable=capable)
        except PolicyError as exc:
            self.status = {'reason': GRANT_STORE_HOLD, 'hold': str(exc)[:300],
                           'snapshot_holds': self.status.get('snapshot_holds', ())}
            return target
        self._invalidate()
        self.last_admission = outcome
        # A failed publication/admission may retry; preview alone never consumes a cut.
        for pair in pairs:
            self._evaluated[pair] = markets[pair].candle_open
        self._admitted = outcome.admitted
        hold = reason if not capable else None
        self.status = {**self.status, 'reason': 'admitted' if outcome.admitted else (hold or target.status.lower()), 'hold': hold,
                       'snapshot_holds': self.status.get('snapshot_holds', ())}
        return target

    # ------------------------------------------------------------------ native snapshot
    def terminal_dust(self, views, evidence, fills, pair, amount_step):
        return terminal_dust(views, evidence, fills, pair, amount_step,
                             connector=self.owner.config.connector_name)

    def snapshot(self, now_ms):
        """Attributed fixed-envelope state from native fills, executors, positions and books.

        Books are read at the owner clock `now_ms`, but marks retain the validated
        source-event and local-application clocks of each pair's native book.
        The current read time never refreshes the book's availability or age.
        """
        config = self.owner.config
        mandate = self.recipe.mandate
        fee = config.risk.fee_reserve_fraction
        recorder = self.recorder()
        fills = recorder.get_trades_for_config(recorder.config_file_path)
        evidence = self.evidence()
        holds = []
        try:
            grants = self.store().grants(now_ms=now_ms, native_executors=evidence)
        except PolicyError as exc:  # foreign or unexplained writer: entries hold, targets stay observable
            grants = ()
            holds.append(f'{CAPITAL_DOMAIN_UNPROVEN}:{exc}')
        stored = self.target_store().head()
        floor = 0 if stored is None else stored['sequence']
        book_read_ms = now_ms
        self.clock.update(native_book_read_ms=book_read_ms, venue_book_event_ms=None,
                          venue_book_event='PER_PAIR_APPLIED_BOOK', books={})
        assets, commitments, consumed, wallet = [], [], ZERO, None
        entries = tuple(s.trading_pair for s in config.symbols if s.entry_enabled)
        for pair in mandate.pairs:
            context = self.owner.contexts[pair]
            runtime = context.runtime
            book = runtime.snapshot()
            observed = runtime.bindings.quote_observation
            event_ms = int(observed.event_timestamp * 1000)
            applied_ms = int(observed.applied_timestamp * 1000)
            self.clock['books'][pair] = {'event_ms': event_ms, 'applied_ms': applied_ms,
                                         'read_ms': book_read_ms,
                                         'source': 'paper_injected' if config.execution_mode == 'paper' else 'venue'}
            wallet = book['available_quote'] if wallet is None else min(wallet, book['available_quote'])
            held = HeldBag.from_positions(context.positions_held, connector=config.connector_name, pair=pair)
            views = runtime.inventory()
            pair_fills = [f for f in fills if f.symbol == pair]
            projection = funding_projection(pair_fills, config.connector_name, pair)
            consumed += projection['cash_used']
            loose = {key: v for key, v in views.items() if not v.held}
            dust, dust_basis = self.terminal_dust(loose, evidence, fills, pair, book['amount_step'])
            quantity = held.amount + sum((v.remaining_base for v in loose.values()), ZERO) + dust
            difference = abs(filled_base(pair_fills, pair) - quantity)
            if difference > (ZERO if dust > 0 else book['amount_step']):
                holds.append(f'{NATIVE_INVENTORY_MISMATCH}:{pair}')
            basis = (held.amount * held.breakeven_price + held.remaining_fees_quote + dust_basis
                     + sum((v.remaining_base * v.entry_price for v in loose.values()), ZERO))
            for key, view in loose.items():
                unfilled = max(view.planned_quote - view.entry_quote, ZERO)
                if not view.entry_terminal and unfilled > 0:
                    commitments.append(Commitment(native_id=key, symbol=pair, side='buy',
                                                  remaining_quantity=unfilled / view.entry_price,
                                                  quote_authority=unfilled * (1 + fee), venue_encumbered_quote=0))
            mark = (book['bid'] + book['ask']) / 2
            for grant in grants:
                if grant.symbol == pair and (grant.status == 'ADMITTED' or grant.executor_id not in views):
                    native_id = 'grant:' + grant.grant_id if grant.status == 'ADMITTED' else grant.executor_id
                    commitments.append(Commitment(native_id=native_id, symbol=pair, side='buy',
                                                  remaining_quantity=grant.reserved_quote / mark,
                                                  quote_authority=grant.reserved_quote, venue_encumbered_quote=0))
            assets.append(AssetState(
                symbol=pair, quantity=quantity, basis_quote=basis, net_cash_consumed_quote=projection['cash_used'],
                mark=mark, bid=book['bid'], ask=book['ask'], lot=book['amount_step'],
                minimum_quantity=book['minimum_amount'], minimum_notional=book['minimum_notional'],
                mark_event_ms=event_ms, available_at_ms=applied_ms, revision=len(fills) + len(evidence) + 1,
                sellable_quantity=0, minimum_sell_price=held.breakeven_price if held.amount > 0 else mark,
                acquisition_allowed=pair in entries, reduction_allowed=False))
        cash_total = max(ZERO, mandate.reference_quote - consumed)
        self.status = {**self.status, 'snapshot_holds': tuple(h[:200] for h in holds)}
        return PortfolioSnapshot(
            portfolio_id=PORTFOLIO_ID, account_id=config.account_id, mandate_sha256=mandate.identity,
            source_sha256=self.source_sha256, sequence=max(len(fills) + len(evidence), floor), cutoff_ms=now_ms,
            native_available_at_ms=book_read_ms, cash_total_quote=cash_total,
            cash_available_quote=min(cash_total, wallet), attributed_nav_quote=cash_total + sum(
                (a.quantity * a.mark for a in assets), ZERO),
            assets=tuple(assets), commitments=tuple(commitments), entry_universe=entries,
            complete=len(assets) == len(mandate.pairs),
            reconciled=not any(h.startswith(NATIVE_INVENTORY_MISMATCH) for h in holds),
            all_writers_accounted=not any(h.startswith(CAPITAL_DOMAIN_UNPROVEN) for h in holds),
            risk_paused=self.owner.operator_entry_paused)
