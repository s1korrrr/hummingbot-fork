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
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.strategy_v2.components.decision_protocol import DecisionScope, ObservationRef, Opportunity
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.controllers.native_portfolio_bridge_v3 import PortfolioGrantStore
from hummingbot.strategy_v2.controllers.portfolio_target_store_v3 import NativeTargetStore
from hummingbot.strategy_v2.models.executors import CloseType
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
        """Retain completed tranche dust in owned quantity and cost, never sell authority.

        The native executor reports dust separately from its managed remainder.
        Individual sublot residuals can accumulate above one lot across exits.
        Inventory ownership is already validated by the symbol inventory reader;
        the fill comparison below still rejects any unexplained aggregate amount.
        """
        by_id = {info.id: info for info in evidence}
        pair_fills = [fill for fill in fills if getattr(fill, 'symbol', None) == pair]
        global_trade_ids = set()
        for fill in fills:
            trade_id = getattr(fill, 'exchange_trade_id', None)
            symbol = getattr(fill, 'symbol', None)
            market = getattr(fill, 'market', None)
            if trade_id in (None, '') or symbol in (None, '') or market in (None, ''):
                raise PolicyError('native fill identity unavailable')
            identity_key = (market, symbol, str(trade_id))
            if identity_key in global_trade_ids:
                raise PolicyError('duplicate native exchange trade identity')
            global_trade_ids.add(identity_key)
        amount, basis, used_orders, used_trades = ZERO, ZERO, set(), set()
        for key, view in views.items():
            info = by_id[key]
            custom = info.custom_info or {}
            receipt = custom.get('execution_receipt') or {}
            dust = decimal(receipt.get('residual_dust_base', '0'), 'terminal dust')
            if dust == 0:
                continue
            step = decimal((custom.get('close_quantity') or {}).get('quantity_increment'),
                           'dust lot', positive=True)
            lifecycle = custom.get('entry_lifecycle')
            if (not view.terminal or info.is_active or not info.is_done or view.held
                    or view.remaining_base != 0 or receipt['accounting_owner'] != 'executor'
                    or decimal(receipt['remaining_owned_base'], 'dust remainder') != 0
                    or step != amount_step or dust >= step or view.entry_quote <= 0
                    or info.close_type in (None, CloseType.POSITION_HOLD)
                    or decimal(info.close_timestamp, 'dust close time', positive=True) <= 0):
                raise PolicyError('unverified native terminal dust')
            if (not isinstance(lifecycle, dict) or lifecycle.get('state') != 'managing_fill'
                    or lifecycle.get('state') == 'reconciliation_required'):
                raise PolicyError('terminal dust entry lifecycle is unresolved')
            order_ids = custom.get('order_ids')
            snapshots = custom.get('execution_order_snapshots')
            if (not isinstance(order_ids, list) or not order_ids
                    or any(not isinstance(key, str) or not key for key in order_ids)
                    or len(set(order_ids)) != len(order_ids) or used_orders.intersection(order_ids)
                    or not isinstance(snapshots, list) or len(snapshots) != len(order_ids)):
                raise PolicyError('terminal dust order identity unavailable')
            order_by_id = {}
            exchange_order_ids = set()
            snapshot_trade_ids = set()
            for order in snapshots:
                if not isinstance(order, dict):
                    raise PolicyError('terminal dust order snapshot invalid')
                order_id = order.get('client_order_id')
                exchange_order_id = order.get('exchange_order_id')
                trade_type = order.get('trade_type')
                try:
                    state = OrderState(int(str(order.get('last_state'))))
                except (TypeError, ValueError):
                    raise PolicyError('terminal dust order state unavailable')
                if (order_id not in order_ids or order_id in order_by_id
                        or not isinstance(exchange_order_id, str) or not exchange_order_id
                        or exchange_order_id in exchange_order_ids
                        or order.get('trading_pair') != pair or trade_type not in {'BUY', 'SELL'}
                        or state not in {OrderState.CANCELED, OrderState.FILLED, OrderState.FAILED}):
                    raise PolicyError('terminal dust order identity invalid')
                exchange_order_ids.add(exchange_order_id)
                order_by_id[order_id] = order
                order_fills = order.get('order_fills')
                if not isinstance(order_fills, dict):
                    raise PolicyError('terminal dust order fills unavailable')
                for trade_id, order_fill in order_fills.items():
                    if (not isinstance(trade_id, str) or not trade_id or trade_id in snapshot_trade_ids
                            or not isinstance(order_fill, dict) or order_fill.get('trade_id') != trade_id
                            or order_fill.get('client_order_id') != order_id
                            or order_fill.get('exchange_order_id') != exchange_order_id
                            or order_fill.get('trading_pair') != pair):
                        raise PolicyError('terminal dust snapshot fill identity invalid')
                    snapshot_trade_ids.add(trade_id)
            if set(order_by_id) != set(order_ids):
                raise PolicyError('terminal dust order snapshots incomplete')
            # A native order or fill cannot be attributed to both this completed
            # executor and another active, held, or terminal executor.
            for other in evidence:
                if getattr(other, 'id', None) == getattr(info, 'id', None):
                    continue
                other_custom = getattr(other, 'custom_info', None) or {}
                other_ids = other_custom.get('order_ids')
                if other_ids is not None:
                    if (not isinstance(other_ids, list)
                            or any(not isinstance(order_id, str) or not order_id for order_id in other_ids)):
                        raise PolicyError('native executor order identity is malformed')
                    if set(order_ids).intersection(other_ids):
                        raise PolicyError('terminal dust order is attributed to multiple executors')
                other_orders = []
                for snapshot_key in ('execution_order_snapshots', 'held_position_orders'):
                    snapshots_for_key = other_custom.get(snapshot_key) or []
                    if not isinstance(snapshots_for_key, list):
                        raise PolicyError('native order snapshot collection is malformed')
                    other_orders.extend(snapshots_for_key)
                entry_snapshot = other_custom.get('entry_order_snapshot')
                if entry_snapshot is not None:
                    other_orders.append(entry_snapshot)
                for other_order in other_orders:
                    if not isinstance(other_order, dict):
                        raise PolicyError('native order snapshot is malformed')
                    if (other_order.get('client_order_id') in order_by_id
                            or (other_order.get('exchange_order_id') is not None
                                and other_order.get('exchange_order_id') in exchange_order_ids)):
                        raise PolicyError('terminal dust order is attributed to multiple executors')
                    other_fills = other_order.get('order_fills') or {}
                    if not isinstance(other_fills, dict):
                        raise PolicyError('native order snapshot fills are malformed')
                    if snapshot_trade_ids.intersection(other_fills):
                        raise PolicyError('terminal dust fill is attributed to multiple executors')
            for fill in fills:
                if fill.order_id in order_ids and getattr(fill, 'symbol', None) != pair:
                    raise PolicyError('terminal dust order has a foreign-market fill')
            selected = [fill for fill in pair_fills if fill.order_id in order_ids]
            if {fill.order_id for fill in selected} - set(order_ids):
                raise PolicyError('terminal dust native fill order mismatch')
            fills_by_order = {}
            for fill in selected:
                trade_id = str(fill.exchange_trade_id)
                order_id = str(fill.order_id)
                if trade_id in used_trades:
                    raise PolicyError('terminal dust fill identity is duplicated')
                fills_by_order.setdefault(order_id, {})
                if trade_id in fills_by_order[order_id]:
                    raise PolicyError('terminal dust native fill identity duplicated')
                fills_by_order[order_id][trade_id] = fill
            totals = {'BUY': [ZERO, ZERO], 'SELL': [ZERO, ZERO]}
            for order_id in order_ids:
                order = order_by_id[order_id]
                native_fills = fills_by_order.get(order_id, {})
                snapshot_fills = order['order_fills']
                state = OrderState(int(str(order['last_state'])))
                executed_base = decimal(order.get('executed_amount_base'), 'dust order executed base')
                executed_quote = decimal(order.get('executed_amount_quote'), 'dust order executed quote')
                order_amount = decimal(order.get('amount'), 'dust order amount', positive=True)
                if (executed_base < 0 or executed_quote < 0 or executed_base > order_amount
                        or state == OrderState.FILLED and executed_base != order_amount):
                    raise PolicyError('terminal dust order execution totals invalid')
                if not native_fills:
                    if (snapshot_fills or state != OrderState.CANCELED or executed_base != 0 or executed_quote != 0):
                        raise PolicyError('empty terminal dust order lacks native zero-fill cancellation proof')
                    continue
                if set(snapshot_fills) != set(native_fills):
                    raise PolicyError('terminal dust native fills differ from order snapshot')
                fill_base = fill_quote = ZERO
                for trade_id, fill in native_fills.items():
                    raw = snapshot_fills[trade_id]
                    if (str(fill.trade_type).upper() != order['trade_type']
                            or str(fill.order_id) != order_id
                            or getattr(fill, 'market', None) != self.owner.config.connector_name
                            or getattr(fill, 'symbol', None) != pair):
                        raise PolicyError('terminal dust fill-to-order attribution mismatch')
                    filled = decimal(fill.amount_decimal, 'dust fill amount', positive=True)
                    price = decimal(fill.price_decimal, 'dust fill price', positive=True)
                    quote = filled * price
                    if (decimal(raw.get('fill_base_amount'), 'snapshot fill base') != filled
                            or decimal(raw.get('fill_price'), 'snapshot fill price') != price
                            or decimal(raw.get('fill_quote_amount'), 'snapshot fill quote') != quote
                            or raw.get('fee') != fill.trade_fee):
                        raise PolicyError('terminal dust exact fill receipt mismatch')
                    fill_base += filled
                    fill_quote += quote
                    totals[order['trade_type']][0] += filled
                    totals[order['trade_type']][1] += quote
                    used_trades.add(trade_id)
                if fill_base != executed_base or fill_quote != executed_quote:
                    raise PolicyError('terminal dust native order totals mismatch')
            if not totals['BUY'][0] or not totals['SELL'][0]:
                raise PolicyError('terminal dust needs native entry and exit fills')
            if (totals['BUY'][0] <= 0 or totals['SELL'][0] <= 0
                    or totals['BUY'] != [decimal(receipt['entry_gross_base'], 'dust entry base'),
                                         decimal(receipt['entry_gross_quote'], 'dust entry quote')]
                    or totals['SELL'] != [decimal(receipt['exit_gross_base'], 'dust exit base'),
                                          decimal(receipt['exit_gross_quote'], 'dust exit quote')]
                    or filled_base(selected, pair) != dust):
                raise PolicyError('terminal dust native fill arithmetic mismatch')
            try:
                exact_projection = funding_projection(selected, self.owner.config.connector_name, pair)
            except (ValueError, TypeError, ArithmeticError, KeyError) as exc:
                raise PolicyError('terminal dust native cost projection unavailable') from exc
            if (exact_projection['inventory_base'] != dust or not exact_projection['inventory_cost'].is_finite()
                    or exact_projection['inventory_cost'] < 0):
                raise PolicyError('terminal dust native cost projection mismatch')
            used_orders.update(order_ids)
            amount += dust
            basis += exact_projection['inventory_cost']
        return amount, basis

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
