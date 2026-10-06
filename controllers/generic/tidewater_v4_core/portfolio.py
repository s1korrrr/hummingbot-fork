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
"""
from decimal import Decimal

from sqlalchemy.exc import SQLAlchemyError

from controllers.generic.meridian_v3_unified_core.migration import IdentityGuard
from controllers.generic.meridian_v3_unified_core.portfolio import ERRORS, MeridianPortfolio
from controllers.generic.modular_spot_core.bag import HeldBag, live_buy_gap
from controllers.generic.modular_spot_core.domain import DataUnavailable, PolicyError
from hummingbot.strategy_v2.components.decision_protocol import DecisionScope, Opportunity
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.controllers.native_portfolio_bridge_v3 import PortfolioGrantStore
from hummingbot.strategy_v2.controllers.portfolio_target_store_v3 import NativeTargetStore
from hummingbot.strategy_v2.portfolio_v3.capital import committed_usage
from hummingbot.strategy_v2.portfolio_v3.contracts import PortfolioSnapshot
from hummingbot.strategy_v2.portfolio_v3.runtime_contracts_v1 import NATIVE_SNAPSHOT_UNAVAILABLE

from .config import PORTFOLIO_ID
from .receipts import ADMITTED, REFUSED, ReceiptWriter
from .recipe import TidewaterPortfolioRecipe, epoch, grant_policy, owner_identity, proposal_writer
from .tide_gate import multiplier, tide_gate

ZERO = Decimal('0')
# Pacing one pair reads its decision checkpoint and recorder positions; any failure of those
# reads (including database errors) refuses that pair only and never faults the portfolio.
PACING_ERRORS = (DataUnavailable, PolicyError, RuntimeError, SQLAlchemyError, *ERRORS)


class TidewaterPortfolio(MeridianPortfolio):
    def __init__(self, owner):
        # Meridian's constructor binds Meridian's recipe; Tidewater binds its own and
        # otherwise sets the same state.
        self.owner = owner
        config = owner.config
        self.recipe = TidewaterPortfolioRecipe(config.mandate(), config.p0_policy())
        self.epoch = epoch(config, self.recipe)
        self.fence = 1
        self.identity = IdentityGuard(config)
        self.identity.current = owner_identity(config, self.recipe)
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

    def _pair_usage(self, snapshot, pair):
        asset = next(a for a in snapshot.assets if a.symbol == pair)
        pending = sum((c.quote_authority for c in snapshot.commitments if c.symbol == pair), ZERO)
        return max(asset.basis_quote, asset.net_cash_consumed_quote, ZERO) + pending

    def pace(self, pairs, markets, snapshot, now_ms):
        """Candidates allowed by the per-pair limit and the tide gate, in priority order."""
        config = self.owner.config
        stage = config.stage()
        increment = stage.order_quote * (1 + config.risk.fee_reserve_fraction)
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
            if pair_usage + increment > config.pair_limit_quote():
                code = 'pair_limit'
            elif config.tide_gate is not None:
                try:
                    features = getattr(market, 'bag', None)
                    gap = live_buy_gap(self.owner.symbol_config(pair).buy_gap,
                                       None if features is None else features.atr_pct)
                    reference = self._reference(pair)
                    gate = tide_gate(config.tide_gate, gap=gap, occupancy=u, reference=reference, bid=bid)
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
                continue
            allowed.append(pair)
            used += increment
            added[pair] = added.get(pair, ZERO) + increment
        self.occupancy = {'u': str(committed_usage(snapshot) / stage.envelope_quote),
                          'multiplier': None if config.tide_gate is None else str(
                              multiplier(config.tide_gate, committed_usage(snapshot) / stage.envelope_quote))}
        return tuple(p for p in pairs if p in allowed)

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
        for pair in self._paced:
            if self._evaluated.get(pair) != markets[pair].candle_open:
                continue  # admission did not complete for this cut; nothing was committed for it
            self.receipts.record(ADMITTED, pair=pair, cut=markets[pair].candle_open, code='admission',
                                 payload={'target_sha256': target.identity, 'target_status': target.status,
                                          'admitted': sorted(str(g) for g in outcome.admitted),
                                          'pacing': self.pacing.get(pair)},
                                 now=self.now_ms() / 1000)
        return target
