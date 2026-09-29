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
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
from hummingbot.strategy_v2.components.decision_protocol import DecisionScope, ObservationRef, Opportunity
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.controllers.native_portfolio_bridge_v3 import PortfolioGrantStore
from hummingbot.strategy_v2.controllers.portfolio_target_store_v3 import NativeTargetStore, ProposalWriter
from hummingbot.strategy_v2.portfolio_v3.contracts import AssetState, Commitment, PortfolioSnapshot
from hummingbot.strategy_v2.portfolio_v3.runtime_contracts_v1 import (
    CAPITAL_DOMAIN_UNPROVEN,
    EXECUTION_CAPABILITY_UNBOUND,
    GRANT_STORE_HOLD,
    NATIVE_INVENTORY_MISMATCH,
    NATIVE_SNAPSHOT_UNAVAILABLE,
    GrantPolicy,
)

from .config import PORTFOLIO_ID, PRODUCER

ZERO = Decimal('0')
ERRORS = (ValueError, TypeError, ArithmeticError, KeyError, AttributeError, OSError)


def filled_base(fills, pair):
    """Owned base implied by native fills, net of fees charged in the base asset."""
    base = pair.split('-')[0]
    total = ZERO
    for fill in fills:
        amount = decimal(fill.amount_decimal, 'fill amount')
        fee = sum((decimal(item['amount'], 'fill fee') for item in fill.trade_fee['flat_fees']
                   if item['token'] == base), ZERO)
        total += amount - fee if fill.trade_type == 'BUY' else -amount - fee
    return total


class MeridianPortfolio:
    def __init__(self, owner):
        self.owner = owner
        config = owner.config
        self.recipe = MeridianPortfolioRecipe(config.mandate(), config.p0_policy())
        self.epoch = f'{config.execution_mode}.{self.recipe.recipe_sha256[:16]}'
        self.fence = 1
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
            config, mandate = self.owner.config, self.recipe.mandate
            self._store = PortfolioGrantStore(self.recorder(), GrantPolicy(
                portfolio_id=PORTFOLIO_ID, account_id=config.account_id, controller_id=config.id,
                connector=config.connector_name, mandate_sha256=mandate.identity,
                policy_sha256=self.recipe.policy.identity, pairs=mandate.pairs,
                envelope_quote=mandate.funding_envelope_quote, per_asset_quote=mandate.per_asset_quote,
                order_quote=mandate.order_quote))
        return self._store

    def target_store(self):
        if self._targets is None:
            config = self.owner.config
            self._targets = NativeTargetStore(self.recorder().sql_manager.get_new_session, ProposalWriter(
                portfolio_id=PORTFOLIO_ID, account_id=config.account_id, writer_id=config.id,
                epoch=self.epoch, fence=self.fence, mandate_sha256=self.recipe.mandate.identity,
                recipe_sha256=self.recipe.recipe_sha256))
        return self._targets

    def evidence(self):
        """Every recorded executor of this recorder, with current active state preferred."""
        if self._tick is not None:
            return self._tick[0]
        rows = {info.id: info for info in self.recorder().get_all_executors()}
        rows.update({info.id: info for info in self.owner.executors_info})
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
        """Symbols whose new confirmed cut shows fresh p23 eligibility, in stable order."""
        signal = recipe.strategy_parameters()['signal']
        entries = {s.trading_pair: s.entry_enabled for s in self.owner.config.symbols}
        result = []
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
            if decision.enter:
                result.append(pair)
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
        for pair in pairs:
            self._evaluated[pair] = markets[pair].candle_open
        target = self.recipe.propose(snapshot, observations, epoch=self.epoch, fence=self.fence)
        stored = self.target_store().read()
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
        self._admitted = outcome.admitted
        hold = reason if not capable else None
        self.status = {'reason': 'admitted' if outcome.admitted else (hold or target.status.lower()), 'hold': hold,
                       'snapshot_holds': self.status.get('snapshot_holds', ())}
        return target

    # ------------------------------------------------------------------ native snapshot
    def snapshot(self, now_ms):
        """Attributed fixed-envelope state from native fills, executors, positions and books.

        Books are read from the native connector at the owner clock `now_ms`. The native
        adapter exposes no venue event time for a book, so each mark's event and
        availability are that read time, and the clock records the venue book event
        time as unavailable rather than inventing one.
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
        stored = self.target_store().read()
        floor = 0 if stored is None else stored['target']['sequence']
        book_read_ms = now_ms  # every native book below is read at this owner clock
        self.clock.update(native_book_read_ms=book_read_ms, venue_book_event_ms=None,
                          venue_book_event='UNAVAILABLE_FROM_NATIVE_ADAPTER')
        assets, commitments, consumed, wallet = [], [], ZERO, None
        entries = tuple(s.trading_pair for s in config.symbols if s.entry_enabled)
        for pair in mandate.pairs:
            context = self.owner.contexts[pair]
            runtime = context.runtime
            book = runtime.snapshot()
            wallet = book['available_quote'] if wallet is None else min(wallet, book['available_quote'])
            held = HeldBag.from_positions(context.positions_held, connector=config.connector_name, pair=pair)
            views = runtime.inventory()
            pair_fills = [f for f in fills if f.symbol == pair]
            projection = funding_projection(pair_fills, config.connector_name, pair)
            consumed += projection['cash_used']
            loose = {key: v for key, v in views.items() if not v.held}
            quantity = held.amount + sum((v.remaining_base for v in loose.values()), ZERO)
            if abs(filled_base(pair_fills, pair) - quantity) > book['amount_step']:
                holds.append(f'{NATIVE_INVENTORY_MISMATCH}:{pair}')
            basis = (held.amount * held.breakeven_price + held.remaining_fees_quote
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
                mark_event_ms=book_read_ms, available_at_ms=book_read_ms, revision=len(fills) + len(evidence) + 1,
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
