"""Tidewater portfolio: Meridian V3's single strategic writer plus paced admission.

The evaluation is Meridian's. Tidewater paces the previewed candidates against the native
snapshot before Meridian builds observations, the P0 target and grant admissions:

    candidates (native executable preview) -> **pace (own snapshot)** -> snapshot -> observations
    -> P0 target -> grant admission

`pace` refuses a candidate that would break the per-pair limit or the tide gate. A refused
pair is removed before setup ids are issued and before its cut is marked evaluated, so the
refusal does not consume the cut: later ticks of the same cut re-evaluate it with a fresh
snapshot. Candidates are paced in the configured priority order with occupancy updated by
each accepted candidate, so one batch cannot cross the gate's limits unnoticed.

Occupancy is the allocator's own usage (`committed_usage`) over the envelope: no second
accounting truth. The gap is the pair's live buy gap, the value its runtime applies, so the
gate is never looser than dispatch.

Research candidates (opt-in; none is active by default):

* With any candidate enabled, each first-entry candidate (no inventory, no pending claim, no open
  commitment) gets a census receipt before any treatment. The order is then
  pair_limit -> entry veto -> tide gate.
* `entry_veto='e0'` refuses a first-entry BUY whose frozen E0 probability exceeds its threshold.
  Like every pacing refusal it does not consume the cut. A shadow receipt records what the tide
  gate would have said; it is never read by a decision.
* `capitulation_override` is applied by the symbol runtime (`runtime.py`) to the market the
  planner reads; pacing records a receipt for each candidate it let through the flash pause.
"""
from decimal import Decimal

from sqlalchemy.exc import SQLAlchemyError

from controllers.generic.meridian_v3_unified_core.migration import IDENTITY_UNVERIFIED, IdentityGuard
from controllers.generic.meridian_v3_unified_core.portfolio import ERRORS, MeridianPortfolio
from controllers.generic.modular_spot_core.bag import HeldBag, live_buy_gap
from controllers.generic.modular_spot_core.domain import DataUnavailable, PolicyError, decimal, identity
from hummingbot.strategy_v2.components.decision_protocol import DecisionScope, Opportunity
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.controllers.native_portfolio_bridge_v3 import PortfolioGrantStore
from hummingbot.strategy_v2.controllers.portfolio_target_store_v3 import NativeTargetStore
from hummingbot.strategy_v2.portfolio_v3.capital import committed_usage
from hummingbot.strategy_v2.portfolio_v3.contracts import PortfolioSnapshot
from hummingbot.strategy_v2.portfolio_v3.runtime_contracts_v1 import NATIVE_SNAPSHOT_UNAVAILABLE

from . import entry_veto
from .capitulation import drawdown
from .config import PORTFOLIO_ID
from .receipts import ADMITTED, CAPITULATION, CENSUS, REFUSED, SHADOW_TIDE_GATE, ReceiptWriter
from .recipe import TidewaterPortfolioRecipe, epoch, grant_policy, owner_identity, proposal_writer
from .tide_gate import multiplier, tide_gate

ZERO = Decimal('0')
# Pacing one pair reads its decision checkpoint and recorder positions; any failure of those
# reads (including database errors) refuses that pair only and never faults the portfolio.
PACING_ERRORS = (DataUnavailable, PolicyError, RuntimeError, SQLAlchemyError, *ERRORS)
VETO_CODES = frozenset({'entry_veto_e0', 'entry_veto_unavailable'})
_UNREAD = object()  # the pair's tide-gate reference has not been read this pass


class TidewaterPortfolio(MeridianPortfolio):
    def __init__(self, owner):
        # Meridian's constructor binds Meridian's recipe; Tidewater binds its own and
        # otherwise sets the same state.
        self.owner = owner
        config = owner.config
        self.recipe = TidewaterPortfolioRecipe(config.mandate(), config.p0_policy())
        self.epoch = epoch(config, self.recipe)
        self.fence = 1
        # Bound lazily, as Meridian does: an identity that cannot be computed holds entries while
        # exits and bags keep running, instead of preventing the controller from starting.
        self.identity = IdentityGuard(config)
        self.identity_hold = None
        self.source_sha256 = digest(config.source_manifest())
        self._store = self._targets = None
        self._seen = {}
        self._evaluated = {}
        self._admitted = ()
        self._tick = None
        self.last_target = self.last_admission = None
        self.status = {'reason': 'no_evaluation', 'hold': None}
        self.clock = {'candles': {}}
        self.receipts = ReceiptWriter(config.id)
        self.pacing = {}  # pair -> last pacing decision, for reporting
        self.occupancy = None
        self._paced = ()

    # ------------------------------------------------------------------ Tidewater identity
    def check_identity(self):
        """Meridian's identity hold, bound to Tidewater's owner identity (never Meridian's)."""
        if self.identity.current is None:
            try:
                self.identity.current = owner_identity(self.owner.config, self.recipe)
            except (PolicyError, *ERRORS) as exc:
                hold = f'{IDENTITY_UNVERIFIED}:{type(exc).__name__}:{exc}'[:300]
                if hold != self.identity_hold:
                    self.owner.logger().error(f'Tidewater entries held: owner identity unavailable: {hold}')
                self.identity_hold = hold
                self.status = {'reason': IDENTITY_UNVERIFIED, 'hold': hold}
                return hold
        return super().check_identity()

    def store(self):
        if self._store is None:
            self._store = PortfolioGrantStore(self.recorder(), grant_policy(self.owner.config, self.recipe))
        return self._store

    def target_store(self):
        if self._targets is None:
            self._targets = NativeTargetStore(self.recorder().sql_manager.get_new_session,
                                              proposal_writer(self.owner.config, self.recipe, fence=self.fence))
        return self._targets

    def observation(self, pair, market, runtime, now_ms):
        observation = super().observation(pair, market, runtime, now_ms)
        o = observation.opportunity
        scope = DecisionScope(**{**o.scope.model_dump(mode='json'), 'program_id': PORTFOLIO_ID, 'owner_id': PORTFOLIO_ID})
        opportunity = Opportunity.model_validate({**o.model_dump(mode='json'), 'scope': scope.model_dump(mode='json')})
        return type(observation).model_validate({**observation.model_dump(mode='json'),
                                                 'opportunity': opportunity.model_dump(mode='json')})

    def snapshot(self, now_ms):
        """Meridian's native snapshot under Tidewater's portfolio id."""
        snapshot = super().snapshot(now_ms)
        return PortfolioSnapshot.model_validate({**snapshot.model_dump(mode='json'), 'portfolio_id': PORTFOLIO_ID})

    # ------------------------------------------------------------------ pacing
    def _reference(self, pair):
        """Last active acquired tranche of the pair, else its held-bag breakeven, else None."""
        context = self.owner.contexts[pair]
        runtime = context.runtime
        config = self.owner.symbol_config(pair)
        state, _ = runtime.ensure_store().load()
        if state is not None:
            inventory = runtime.inventory()
            active = [inventory[x] for x in state.claims if x in inventory and not inventory[x].terminal]
            acquired = [v for v in active if v.entry_quote > 0]
            if acquired:
                return acquired[-1].entry_price
        held = HeldBag.from_positions(context.positions_held, connector=config.connector_name, pair=pair)
        return held.breakeven_price if held.amount > 0 else None

    def entry_state(self, pair, snapshot=None):
        """('holds' | 'first' | 'pending', evidence, reference) for one pair.

        'holds': the pair has an acquired active tranche or a held bag (`_reference` is set).
        'first': no inventory, no unresolved claim (an executor not yet created, or active with
        nothing filled), nothing pending creation and, when a snapshot is given, no open commitment.
        'pending': no inventory but an acquisition is under way; it is not a first entry.
        """
        runtime = self.owner.contexts[pair].runtime
        reference = self._reference(pair)
        state, _ = runtime.ensure_store().load()
        inventory = runtime.inventory()
        claims = () if state is None else tuple(state.claims)
        unresolved = [x for x in claims
                      if x not in inventory or (not inventory[x].terminal and inventory[x].entry_quote <= 0)]
        commitments = ZERO if snapshot is None else sum(
            (c.quote_authority for c in snapshot.commitments if c.symbol == pair), ZERO)
        evidence = {'reference': None if reference is None else str(reference), 'unresolved_claims': len(unresolved),
                    'runtime_pending': len(runtime.pending), 'commitments_quote': str(commitments)}
        if reference is not None:
            return 'holds', evidence, reference
        if unresolved or runtime.pending or commitments > 0:
            return 'pending', evidence, reference
        return 'first', evidence, reference

    def capitulation(self, pair, market, override):
        """Whether the capitulation override lifts the native flash pause for this market, or None
        when the pause is not active. Reads only the market's own closed-candle features and the
        pair's inventory state; a read failure leaves the pause in force."""
        pause = getattr(market, 'flash_pause_until', None)
        now = decimal(self.owner.market_data_provider.time(), 'now')
        if pause is None or not now < decimal(pause, 'flash pause'):  # the planner's own check
            return None
        decision = {'cut': market.candle_open, 'native_flash_pause_until': pause, 'close': str(market.close),
                    'depth': str(override.depth), 'applied': False}
        features = getattr(market, 'bag', None)
        recent_high = None if features is None else features.recent_high
        if recent_high is None:
            return {**decision, 'reason': 'recent_high_unavailable'}
        try:
            decision.update(recent_high=str(recent_high), drawdown=str(drawdown(close=market.close, recent_high=recent_high)))
            if not override.holds(close=market.close, recent_high=recent_high):
                return {**decision, 'reason': 'not_deep_enough'}
            state, evidence, _ = self.entry_state(pair)
        except PACING_ERRORS as exc:
            return {**decision, 'reason': f'unavailable:{type(exc).__name__}:{exc}'[:200]}
        decision.update(entry_state=state, entry_evidence=evidence)
        if state == 'pending':
            return {**decision, 'reason': 'acquisition_pending'}
        return {**decision, 'applied': True, 'reason': 'capitulation'}

    def _census(self, pair, market, evidence, native_pause, now_ms):
        """One record per first-entry candidate, before any treatment. Deterministic id and payload."""
        cut = market.candle_open
        config = self.owner.config
        self.receipts.record(CENSUS, pair=pair, cut=cut, code='first_entry', now=now_ms / 1000, payload={
            'census_id': identity(['tidewater_first_entry_census', config.id, pair, cut]),
            'close': str(market.close), 'native_flash_pause_until': native_pause, 'evidence': evidence,
            'variants': config.variants()})

    def _tide(self, pair, market, u, bid, reference):
        features = getattr(market, 'bag', None)
        gap = live_buy_gap(self.owner.symbol_config(pair).buy_gap, None if features is None else features.atr_pct)
        if reference is _UNREAD:
            reference = self._reference(pair)
        return gap, reference, tide_gate(self.owner.config.tide_gate, gap=gap, occupancy=u, reference=reference, bid=bid)

    def _shadow_tide_gate(self, pair, market, u, bid, reference, code, now_ms):
        """What the tide gate would have said about a candidate the entry veto refused. Report only."""
        if self.owner.config.tide_gate is None:
            shadow = {'tide_gate': None, 'would_allow': True, 'reason': 'tide_gate_disabled'}
        else:
            try:
                gap, reference, gate = self._tide(pair, market, u, bid, reference)
            except PACING_ERRORS as exc:
                shadow = {'would_allow': None, 'reason': 'pacing_unavailable', 'error': f'{type(exc).__name__}:{exc}'[:200]}
            else:
                shadow = {'would_allow': gate.allowed, 'reason': gate.reason, 'gap': str(gap),
                          'multiplier': str(gate.multiplier),
                          'required_price': None if gate.required_price is None else str(gate.required_price)}
        self.receipts.record(SHADOW_TIDE_GATE, pair=pair, cut=market.candle_open, code=code, now=now_ms / 1000,
                             payload={**shadow, 'occupancy': str(u), 'bid': str(bid), 'refused_by': code})

    def _pair_usage(self, snapshot, pair):
        asset = next(a for a in snapshot.assets if a.symbol == pair)
        pending = sum((c.quote_authority for c in snapshot.commitments if c.symbol == pair), ZERO)
        return max(asset.basis_quote, asset.net_cash_consumed_quote, ZERO) + pending

    def pace(self, pairs, markets, snapshot, now_ms):
        """Candidates allowed by the per-pair limit and the tide gate, in priority order."""
        config = self.owner.config
        stage = config.stage()
        increment = stage.order_quote * (1 + config.risk.fee_reserve_fraction)
        veto = config.entry_veto_model()
        variants = veto is not None or config.capitulation_override is not None
        used = committed_usage(snapshot)
        added = {}
        allowed = []
        for pair in (p for p in config.priority if p in pairs):
            market = markets[pair]
            u = used / stage.envelope_quote
            pair_usage = self._pair_usage(snapshot, pair) + added.get(pair, ZERO)
            bid = next(a.bid for a in snapshot.assets if a.symbol == pair)
            decision = {'occupancy': str(u), 'pair_usage': str(pair_usage), 'bid': str(bid),
                        'pair_limit': str(config.pair_limit_quote())}
            code = None
            entry, reference = None, _UNREAD
            if variants:
                try:
                    entry, evidence, reference = self.entry_state(pair, snapshot)
                except PACING_ERRORS as exc:
                    decision['error'] = f'{type(exc).__name__}:{exc}'[:200]
                    code = 'pacing_unavailable'
                else:
                    decision['entry_state'] = entry
                    lifted = self._lifted(pair, market)
                    if entry == 'first':
                        native = market.flash_pause_until if lifted is None else lifted['native_flash_pause_until']
                        self._census(pair, market, evidence, native, now_ms)
                    if lifted is not None:
                        decision['capitulation'] = lifted
                        self.receipts.record(CAPITULATION, pair=pair, cut=market.candle_open, code='flash_pause_lifted',
                                             now=now_ms / 1000, payload={
                                                 **lifted, 'shadow_native_flash_pause': {
                                                     'would_refuse': True, 'reason': 'flash_crash_pause',
                                                     'until': lifted['native_flash_pause_until']}})
            if code is not None:
                pass
            elif pair_usage + increment > config.pair_limit_quote():
                code = 'pair_limit'
            else:
                if veto is not None and entry == 'first':
                    try:
                        verdict = entry_veto.evaluate(veto, self.owner.contexts[pair].runtime, market)
                    except PACING_ERRORS as exc:
                        decision['entry_veto'] = {'error': f'{type(exc).__name__}:{exc}'[:200]}
                        code = 'entry_veto_unavailable'
                    else:
                        decision['entry_veto'] = verdict
                        if verdict['vetoed']:
                            code = 'entry_veto_e0'
                if code is None and config.tide_gate is not None:
                    try:
                        gap, reference, gate = self._tide(pair, market, u, bid, reference)
                    except PACING_ERRORS as exc:
                        # This pair cannot be paced now; it waits. Other pairs are paced normally.
                        decision['error'] = f'{type(exc).__name__}:{exc}'[:200]
                        code = 'pacing_unavailable'
                    else:
                        decision.update(gap=str(gap), multiplier=str(gate.multiplier),
                                        reference=None if reference is None else str(reference),
                                        required_price=None if gate.required_price is None else str(gate.required_price))
                        if not gate.allowed:
                            code = gate.reason
            self.pacing[pair] = {**decision, 'outcome': code or 'paced_ok', 'cut': market.candle_open}
            if code is not None:
                self.receipts.record(REFUSED, pair=pair, cut=market.candle_open, code=code, payload=decision,
                                     now=now_ms / 1000)
                if code in VETO_CODES:
                    self._shadow_tide_gate(pair, market, u, bid, reference, code, now_ms)
                continue
            allowed.append(pair)
            used += increment
            added[pair] = added.get(pair, ZERO) + increment
        self.occupancy = {'u': str(committed_usage(snapshot) / stage.envelope_quote),
                          'multiplier': None if config.tide_gate is None else str(
                              multiplier(config.tide_gate, committed_usage(snapshot) / stage.envelope_quote))}
        return tuple(p for p in pairs if p in allowed)

    def _lifted(self, pair, market):
        """The capitulation decision that lifted this market's flash pause, else None."""
        lifted = getattr(self.owner.contexts[pair].runtime, 'capitulation', None)
        if (lifted is None or not lifted['applied'] or lifted['cut'] != market.candle_open
                or market.flash_pause_until is not None):
            return None
        return lifted

    # ------------------------------------------------------------------ evaluation
    def candidates(self, markets):
        """Meridian's previewed candidates, paced against the native snapshot.

        The gate reads its own native snapshot. Meridian's `evaluate()` then reads the snapshot it
        passes to P0, at its own decision time, as in Meridian. Both reads happen in one synchronous
        evaluation with no await, so no fill or order event can change native state between them;
        only the clock can move, and each read keeps its own consistent time (a reused snapshot
        would carry an earlier cutoff than the observations, and P0 would reject them).
        """
        pairs = super().candidates(markets)
        self._paced = ()
        if not pairs:
            return pairs
        now_ms = self.now_ms()
        try:
            snapshot = self.snapshot(now_ms)
        except (DataUnavailable, PolicyError, *ERRORS) as exc:
            self.status = {**self.status, 'reason': NATIVE_SNAPSHOT_UNAVAILABLE, 'hold': f'{type(exc).__name__}:{exc}'[:300]}
            return ()
        paced = self.pace(pairs, markets, snapshot, now_ms)
        if not paced:
            self.status = {**self.status, 'reason': 'PACED', 'hold': None}
        self._paced = paced
        return paced

    def evaluate(self, markets):
        """Meridian's evaluation; an admission receipt follows each committed target."""
        self._paced = ()
        target = super().evaluate(markets)
        if target is None or target is not self.last_target or self.last_admission is None:
            return target
        outcome = self.last_admission
        try:  # admitted grant ids -> their symbols, so each pair's receipt says what it received
            wanted = set(outcome.admitted)
            granted = {g.symbol for g in self.store().grants(now_ms=self.now_ms(), native_executors=self.evidence())
                       if g.grant_id in wanted} if wanted else set()
        except PACING_ERRORS:
            granted = None
        for pair in self._paced:
            if self._evaluated.get(pair) != markets[pair].candle_open:
                continue  # admission did not complete for this cut; nothing was committed for it
            code = 'admission_unresolved' if granted is None else ('admitted' if pair in granted else 'not_admitted')
            self.receipts.record(ADMITTED, pair=pair, cut=markets[pair].candle_open, code=code,
                                 payload={'target_sha256': target.identity, 'target_status': target.status,
                                          'admitted': sorted(str(g) for g in outcome.admitted),
                                          'pacing': self.pacing.get(pair)},
                                 now=self.now_ms() / 1000)
        return target
