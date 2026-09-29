"""Scripted-feature scenarios on the real native paper engine for Meridian unified.

Orders, fills, fees, executors, held positions, the recorder, grants and restart are
native. Only each symbol's closed-candle features are scripted per minute, so a test
can reach exact RSI, ATR and depth states; the feature math has its own tests.
"""
from decimal import Decimal as D
from test.meridian_v3.test_native_paper import paper
from test.meridian_v3.test_unified_runtime import OWNER, unified

from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.modular_spot_core.bag import BagFeatures
from controllers.generic.modular_spot_core.market import MarketFeatures
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.paper_session import NativePaperSession

START = 1_800_000_000
LINEAGE = 'c' * 64


class Scenario:
    def __init__(self, directory, *, cash='500', restore=False, **config):
        self.directory, self.cash = directory, cash
        self.config = unified(execution_enabled=True, **config)
        self.session = NativePaperSession(paper(PAIRS, cash), [self.config], directory / 'meridian.sqlite',
                                          restore=restore)
        self.controller = self.session.controllers[OWNER]
        self.features = {}
        self.minute = 0
        self.sequence = {pair: 1 for pair in PAIRS}
        self.trade_sequence = {pair: 0 for pair in PAIRS}
        self.mids = {pair: D('100') for pair in PAIRS}
        self._script()

    def _script(self):
        for pair, context in self.controller.contexts.items():
            runtime = context.runtime

            def observe(runtime=runtime, pair=pair):
                market = self.features.get(pair)
                runtime.market, runtime.closed_rows = market, {}
                runtime.input_identity = LINEAGE
                runtime.reason = 'observed' if market is not None else 'market_unavailable:scripted'
                return market
            runtime.observe = observe

    @property
    def now(self):
        return START + 60 * self.minute

    def feature(self, pair, *, prev, rsi, recent_high=None, atr_fraction='.005', atr_pct='.002'):
        mid = self.mids[pair]
        # The last closed one-minute candle opened a minute before the owner clock.
        self.features[pair] = MarketFeatures(
            self.now - 60, mid, D(str(prev)), D(str(rsi)), False, False, atr_fraction=D(atr_fraction),
            bag=BagFeatures(D(str(prev)), mid, D('0'), D('0'), D(atr_pct), 'range', D('0'),
                            D(str(recent_high)) if recent_high is not None else mid))

    def book(self, pair):
        mid = self.mids[pair]
        self.sequence[pair] += 1
        return dict(trading_pair=pair, sequence=self.sequence[pair], timestamp=self.now,
                    bids=[(float(mid - D('0.01')), 50)], asks=[(float(mid + D('0.01')), 50)])

    def trade(self, pair, side, price, amount):
        self.trade_sequence[pair] += 1
        return dict(trading_pair=pair, sequence=self.trade_sequence[pair], timestamp=self.now, side=side,
                    price=D(str(price)), amount=D(str(amount)))

    async def step(self, *, minutes=1, mids=None, rsi=None, trades=(), stale=(), seconds=0):
        """Advance, publish books for fresh pairs and scripted features, then run one native step."""
        self.minute += minutes
        for pair, mid in (mids or {}).items():
            self.mids[pair] = D(str(mid))
        for pair in PAIRS:
            prev, current = (rsi or {}).get(pair, (50, 50))
            self.feature(pair, prev=prev, rsi=current)
        now = self.now + seconds
        books = [dict(self.book(pair), timestamp=now) for pair in PAIRS if pair not in stale]
        trades = [dict(t, timestamp=now) for t in trades]
        receipt = await self.session.step(now, books=books, trades=trades)
        faults = {pair: c.runtime.fault for pair, c in self.controller.contexts.items() if c.runtime.fault}
        assert not faults, faults
        short = [(p.trading_pair, p.amount) for p in self.session.recorder.get_positions_by_controller(OWNER)
                 if p.side != 'BUY' and D(str(p.amount)) > 0]
        assert not short, f'spot owner persisted an opposite-side held position: {short}'
        return receipt

    async def fill(self, pair, amount='1', *, below='0.02'):
        """A market SELL print below our resting bid fills our buy (full participation)."""
        return await self.step(minutes=0, seconds=1, trades=[
            self.trade(pair, TradeType.SELL, self.mids[pair] - D(below), amount)])

    async def lift(self, pair, amount='1', *, above='0.02'):
        """A market BUY print above our resting ask fills our sell."""
        return await self.step(minutes=0, seconds=1, trades=[
            self.trade(pair, TradeType.BUY, self.mids[pair] + D(above), amount)])

    def executors(self, pair=None):
        return [e for e in self.session.executor_orchestrator.active_executors.get(OWNER, [])
                if pair is None or e.config.trading_pair == pair]

    def grants(self):
        portfolio = self.controller.portfolio
        portfolio.begin_tick()
        return portfolio.store().grants(now_ms=portfolio.now_ms(), native_executors=portfolio.evidence())

    def held(self, pair):
        return sum((p.amount for p in self.controller.positions_held if p.trading_pair == pair), D('0'))

    def balance(self, asset):
        return self.session._connector.get_balance(asset)

    async def close(self):
        await self.session.close()


def rebound():
    """RSI crosses the p23 oversold level (29) upward on this closed candle."""
    return (28, 31)


def creates(receipt, pair=None):
    from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
    return [a for a in receipt['actions'] if isinstance(a, CreateExecutorAction)
            and (pair is None or a.executor_config.trading_pair == pair)]


def stops(receipt, pair=None):
    from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction
    return [a for a in receipt['actions'] if isinstance(a, StopExecutorAction)]
