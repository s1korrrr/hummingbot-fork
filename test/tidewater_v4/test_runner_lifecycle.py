"""Tidewater protected runner (`exit_runner`) on the real native paper engine.

Orders, fills, fees, executors, grants, receipts and the recorder are native. As in the Meridian
lifecycle harness, each symbol's closed-candle features are scripted per minute; the runner's one-minute
frame is scripted the same way (one closed bar per minute at the scripted mid unless a test sets its
high). Daily bars older than the frame are stored as RUNNER_DAY records before the first tick, exactly as
an earlier run of the runner would have stored them. Economics are not claimed.
"""
import logging
import tempfile
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.lifecycle_harness import LINEAGE, START, creates, rebound
from test.meridian_v3.test_native_paper import paper
from test.tidewater_v4.test_tidewater_candidates import CONFIGS, OWNER_0E4811F71
from test.tidewater_v4.test_tidewater_runtime import tidewater
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import pandas as pd
import pytest

from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.modular_spot_core.bag import BagFeatures
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
from controllers.generic.modular_spot_core.market import MarketFeatures
from controllers.generic.tidewater_v4_core.config import RUNNER_ITER5_B, TidewaterConfig
from controllers.generic.tidewater_v4_core.receipts import RUNNER, RUNNER_DAY, ReceiptWriter, read_receipts
from controllers.generic.tidewater_v4_core.runner import DAY_SECONDS, RunnerConfig
from controllers.generic.tidewater_v4_core.runner_book import EXIT, OPEN, RunnerBook
from controllers.generic.tidewater_v4_core.runner_execution import (
    RUNNER_CANCEL_MAX_REQUESTS,
    RUNNER_CANCEL_RETRY_SECONDS,
    TidewaterRunnerExecutor,
)
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.strategy_v2.executors.restart_continuity import continuity_plan
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.paper_session import NativePaperSession

BTC = 'BTC-USDC'
OWNER = 'modular_spot_tidewater_v4_paper'
ATR = D('0.02')  # the seeded daily bars: true range 2 on a close of 100 every day
LOT, TICK = D('.001'), D('.01')


def paper_base_fee(pairs, cash):
    """The native paper exchange of `paper` with base-denominated buy fees: owned inventory is not a lot
    multiple (fee dust), as on a venue that takes the buy fee from the bought asset."""
    rules = {pair: TradingRule(pair, min_order_size=LOT, min_price_increment=TICK, min_base_amount_increment=LOT,
                               min_notional_size=D('1')) for pair in pairs}
    market = NativePaperTradeExchange(rules, fee_schema=TradeFeeSchema(
        maker_percent_fee_decimal=D('.001'), taker_percent_fee_decimal=D('.002'),
        buy_percent_fee_deducted_from_returns=True), trade_participation=D('1'))
    for pair in pairs:
        market.set_balance(pair.split('-')[0], D('0'))
    market.set_balance('USDC', D(cash))
    market.tick(1000)
    for pair in pairs:
        market.set_order_book(pair, 1, 1000, [(99.5, 10)], [(99.51, 10)])
    return market


def lot(amount):
    return (amount / LOT).to_integral_value(rounding=ROUND_FLOOR) * LOT


def up_to_tick(price):
    return (price / TICK).to_integral_value(rounding=ROUND_CEILING) * TICK


# ---------------------------------------------------------------------- configuration and identity
def test_runner_iter5_b_is_the_lead_setting():
    assert RUNNER_ITER5_B == RunnerConfig(runner_fraction=D('0.5'), floor_margin=D('0.01'), atr_multiplier=D('2'),
                                          atr_days=14)


@pytest.mark.parametrize('name', sorted(CONFIGS))
def test_default_configurations_keep_their_recipe_and_configuration_digests(name):
    overrides = dict(CONFIGS[name])
    if overrides.pop('mode', None) == 'observation':
        config = tidewater(mode='observation', **overrides)
    else:
        config = tidewater(**overrides)
    assert config.exit_runner is None and 'exit_runner' not in config.sealed_payload()
    assert (config.recipe_digest(), config.seal().configuration_sha256) == OWNER_0E4811F71[name]


def test_the_runner_is_sealed_only_when_set():
    base, runner = tidewater(), tidewater(exit_runner=RUNNER_ITER5_B)
    assert runner.sealed_payload()['exit_runner'] == RUNNER_ITER5_B.model_dump(mode='json')
    assert runner.recipe_digest() != base.recipe_digest()
    assert runner.seal().configuration_sha256 != base.seal().configuration_sha256
    other = tidewater(exit_runner=RUNNER_ITER5_B.model_copy(update={'atr_days': 5}))
    assert other.recipe_digest() != runner.recipe_digest()
    sealed = tidewater(execution_enabled=True, exit_runner=RUNNER_ITER5_B)
    for change in ({'exit_runner': None}, {'exit_runner': {**RUNNER_ITER5_B.model_dump(mode='json'),
                                                           'runner_fraction': '0.35'}}):
        with pytest.raises(ValueError):
            TidewaterConfig(**{**sealed.model_dump(mode='json'), **change})
    unsealed = tidewater(execution_enabled=True)
    with pytest.raises(ValueError):
        TidewaterConfig(**{**unsealed.model_dump(mode='json'), 'exit_runner': RUNNER_ITER5_B.model_dump(mode='json')})


def test_the_runner_is_a_research_option():
    with pytest.raises(ValueError, match='research option'):
        tidewater(mode='live', exit_runner=RUNNER_ITER5_B)
    assert tidewater(mode='observation', exit_runner=RUNNER_ITER5_B).exit_runner == RUNNER_ITER5_B


@pytest.mark.parametrize('bad', [{'runner_fraction': '1'}, {'runner_fraction': '0'}, {'floor_margin': '-0.01'},
                                 {'atr_multiplier': '0'}, {'atr_days': 1}, {'extra': 1}])
def test_invalid_runner_settings_are_rejected(bad):
    with pytest.raises(ValueError):
        tidewater(exit_runner={**RUNNER_ITER5_B.model_dump(mode='json'), **bad})


# ---------------------------------------------------------------------- scripted native scenario
class RunnerScenario:
    """The Meridian lifecycle harness (lifecycle_harness.Scenario) for Tidewater with the runner on."""

    def __init__(self, directory, *, cash='1000', runner=RUNNER_ITER5_B, restore=False, seed_days=14,
                 base_fee=False, **options):
        self.directory = directory
        self.config = tidewater(execution_enabled=True, exit_runner=runner, **options)
        market = (paper_base_fee if base_fee else paper)(PAIRS, cash)
        self.session = NativePaperSession(market, [self.config], directory / 'tidewater.sqlite', restore=restore)
        self.controller = self.session.controllers[OWNER]
        self.features, self.bars = {}, {pair: [] for pair in PAIRS}
        self.minute = 0
        self.sequence = {pair: 1 for pair in PAIRS}
        self.trade_sequence = {pair: 0 for pair in PAIRS}
        self.mids = {pair: D('100') for pair in PAIRS}
        self.highs = {}
        self.atr = D('.002')  # the closed-candle ATR fraction the planner's live buy gap reads
        self._script()
        if seed_days:
            self.seed_days(BTC, seed_days)

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
        self.controller.portfolio._candles = self.frame

    def seed_days(self, pair, count):
        """Daily bars an earlier run stored: high 102, low 100, close 100 (true range 2, ATR 2%)."""
        writer = ReceiptWriter(OWNER)
        first_day = START // DAY_SECONDS * DAY_SECONDS
        for k in range(count, 0, -1):
            start = first_day - k * DAY_SECONDS
            assert writer.persist(RUNNER_DAY, pair=pair, cut=start, code='day', now=START, payload={
                'start': start, 'high': '102', 'low': '100', 'close': '100', 'previous_close': '100'},
                event_id=f'tidewater:runner_day:{OWNER}:{pair}:{start}')

    def frame(self, pair):
        rows = self.bars[pair][-1500:]
        return pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    @property
    def now(self):
        return START + 60 * self.minute

    def feature(self, pair, *, prev, rsi):
        mid = self.mids[pair]
        self.features[pair] = MarketFeatures(
            self.now - 60, mid, D(str(prev)), D(str(rsi)), False, False, atr_fraction=D('.005'),
            bag=BagFeatures(D(str(prev)), mid, D('0'), D('0'), self.atr, 'range', D('0'), mid))

    def book(self, pair, depth=50):
        mid = self.mids[pair]
        self.sequence[pair] += 1
        return dict(trading_pair=pair, sequence=self.sequence[pair], timestamp=self.now,
                    bids=[(float(mid - D('0.01')), float(depth))], asks=[(float(mid + D('0.01')), 50)])

    def trade(self, pair, side, price, amount):
        self.trade_sequence[pair] += 1
        return dict(trading_pair=pair, sequence=self.trade_sequence[pair], timestamp=self.now, side=side,
                    price=D(str(price)), amount=D(str(amount)))

    async def step(self, *, minutes=1, mids=None, highs=None, rsi=None, trades=(), seconds=0, depth=None):
        """Advance; each new minute closes one bar per pair at its mid (high from `highs` when given).
        `depth` sets a pair's bid size in this step's book (a thin bid leaves a sell resting)."""
        for _ in range(minutes):
            self.minute += 1
            for pair, mid in (mids or {}).items():
                self.mids[pair] = D(str(mid))
            for pair in PAIRS:
                close = self.mids[pair]
                high = max(close, D(str((highs or {}).get(pair, close))))
                self.bars[pair].append((self.now - 60, float(close), float(high), float(close), float(close), 10.0))
        for pair in PAIRS:
            prev, current = (rsi or {}).get(pair, (50, 50))
            self.feature(pair, prev=prev, rsi=current)
        now = self.now + seconds
        books = [dict(self.book(pair, (depth or {}).get(pair, 50)), timestamp=now) for pair in PAIRS]
        receipt = await self.session.step(now, books=books, trades=[dict(t, timestamp=now) for t in trades])
        faults = {pair: c.runtime.fault for pair, c in self.controller.contexts.items() if c.runtime.fault}
        assert not faults, faults
        return receipt

    async def lift(self, pair, amount='5', *, above='0.02'):
        """A market BUY print above our resting ask fills our sell (full participation)."""
        return await self.step(minutes=0, seconds=1, trades=[
            self.trade(pair, TradeType.BUY, self.mids[pair] + D(above), amount)])

    async def lift_at(self, pair, price, amount='5'):
        return await self.step(minutes=0, seconds=1, trades=[self.trade(pair, TradeType.BUY, price, amount)])

    async def fill(self, pair, amount='1', *, below='0.02'):
        return await self.step(minutes=0, seconds=1, trades=[
            self.trade(pair, TradeType.SELL, self.mids[pair] - D(below), amount)])

    def executors(self, pair=None):
        return [e for e in self.session.executor_orchestrator.active_executors.get(OWNER, [])
                if pair is None or e.config.trading_pair == pair]

    def grants(self):
        portfolio = self.controller.portfolio
        portfolio.begin_tick()
        return portfolio.store().grants(now_ms=portfolio.now_ms(), native_executors=portfolio.evidence())

    def balance(self, asset):
        return self.session._connector.get_balance(asset)

    def records(self, code=None):
        return [r for r in read_receipts(self.session.recorder, OWNER)
                if r['kind'] == RUNNER and (code is None or r['code'] == code)]

    async def close(self):
        await self.session.close()


class RunnerLifecycle(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    async def asyncSetUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.directory = Path(self._directory.name)
        self.scenarios = []

    async def asyncTearDown(self):
        for scenario in self.scenarios:
            if not scenario.session._closed:
                await scenario.close()
        self._directory.cleanup()

    def scenario(self, **kwargs):
        scenario = RunnerScenario(self.directory, **kwargs)
        self.scenarios.append(scenario)
        return scenario

    async def entered(self, s, amount='1'):
        await s.step()  # the first observed cut only seeds each symbol
        receipt = await s.step(rsi={BTC: rebound()})
        [create] = creates(receipt, BTC)
        await s.fill(BTC, amount)
        [executor] = [e for e in s.executors(BTC) if e.config.id == create.executor_config.id]
        return executor

    async def harvested(self, s, depth=None):
        """Enter, arm the native trailing stop and let it fire: the runner split happens here. The harvest
        sale is a limit at the bid; with a thin bid (`depth`) only that much fills at once, the rest rests."""
        executor = await self.entered(s)
        self.assertIs(type(executor), TidewaterRunnerExecutor)
        self.assertIs(s.controller.restart_continuity_executor_class(executor.config), TidewaterRunnerExecutor)
        for mid in ('104.5', '105.5'):
            await s.step(mids={BTC: mid})
        self.assertIsNotNone(executor._trailing_stop_trigger_pct)
        owned = executor.amount_to_close
        await s.step(mids={BTC: '104.9'}, depth=None if depth is None else {BTC: depth})
        return executor, owned

    def runner(self, s, executor):
        return s.controller.runners.lots[executor.config.id]

    async def runner_phase(self, s):
        """Harvest; the harvest sale fills at the bid: the executor owns only its runner, RUNNING."""
        executor, owned = await self.harvested(s)
        await s.step()
        return executor, owned

    # ------------------------------------------------------------------ harvest and runner phase
    async def test_harvest_keeps_half_as_a_runner_and_sells_the_rest_above_the_protected_floor(self):
        s = self.scenario()
        executor, owned = await self.harvested(s)
        runner = self.runner(s, executor)
        self.assertEqual((runner['runner_base'], runner['sold_base']), (D('0.249'), owned - D('0.249')))
        [close] = [o.order for o in executor._exit_orders()]
        self.assertEqual((close.amount, close.order_type.name, close.trade_type), (runner['sold_base'], 'LIMIT',
                                                                                   TradeType.SELL))
        self.assertEqual((close.price, close.executed_amount_base), (D('104.89'), runner['sold_base']))  # at the bid
        self.assertGreaterEqual(close.price, executor._min_no_loss_sell_price())
        # cost floor = gross entry price x (1 + entry fee + expected exit fee + margin), as the research owner
        self.assertEqual(runner['cost_basis'], D('99.99'))
        self.assertEqual(runner['fee_fraction'], D('0.003'))
        self.assertEqual(runner['cost_floor'], D('99.99') * (1 + D('0.003') + D('0.01')))
        self.assertEqual((runner['floor'], runner['high_water'], runner['phase']),
                         (runner['cost_floor'], runner['harvest_price'], OPEN))
        [created] = s.records('created')
        self.assertEqual((created['cut'], created['runner_base'], created['cost_floor']),
                         (executor.config.id, '0.249', str(runner['cost_floor'])))
        self.assertEqual(executor.status, RunnableStatus.SHUTTING_DOWN)
        await s.step(minutes=0, seconds=1)  # the executor's next tick sees the harvest sale complete
        self.assertEqual((executor.status, executor.close_type), (RunnableStatus.RUNNING, None))
        self.assertEqual(executor.amount_to_close, D('0.249'))
        self.assertEqual(s.balance('BTC'), D('0.249'))
        self.assertFalse(executor.is_closed)
        [grant] = s.grants()
        self.assertEqual((grant.status, grant.executor_id), ('BOUND', executor.config.id))  # runner capital stays bound

    async def test_floor_ratchets_with_the_high_water_and_the_runner_exits_at_the_floor(self):
        s = self.scenario()
        executor, _ = await self.runner_phase(s)
        cost_floor = self.runner(s, executor)['cost_floor']
        await s.step(mids={BTC: '106'})
        # a bar's floor uses the high water of earlier bars: 104.9 x (1 - 2 x 2%) < cost floor
        self.assertEqual(self.runner(s, executor)['floor'], cost_floor)
        await s.step(mids={BTC: '108'})
        self.assertEqual(self.runner(s, executor)['floor'], D('106') * (1 - 2 * ATR))
        await s.step(mids={BTC: '110'})
        self.assertEqual(self.runner(s, executor)['floor'], D('108') * (1 - 2 * ATR))
        await s.step(mids={BTC: '107'})  # 107 > 110 x 0.96 = 105.6: still open
        runner = self.runner(s, executor)
        self.assertEqual((runner['floor'], runner['high_water'], runner['phase']), (D('105.6'), D('110'), OPEN))
        self.assertEqual(executor.status, RunnableStatus.RUNNING)
        await s.step(mids={BTC: '105.6'})  # close at the floor: exit at the bid, above the cost floor
        self.assertEqual(self.runner(s, executor)['phase'], EXIT)
        exit_order = executor._close_order.order
        self.assertEqual((exit_order.amount, exit_order.price, exit_order.order_type.name),
                         (D('0.249'), D('105.59'), 'LIMIT'))
        await s.lift(BTC)
        await s.step()
        self.assertTrue(executor.is_closed)
        self.assertEqual(executor.close_type, CloseType.TRAILING_STOP)
        self.assertEqual(s.balance('BTC'), D('0'))
        self.assertEqual(s.grants(), ())
        self.assertEqual(s.controller.runners.lots, {})
        [closed] = s.records('closed')
        self.assertEqual((closed['cut'], closed['close_type'], closed['phase']),
                         (executor.config.id, 'TRAILING_STOP', EXIT))
        floors = [D(r['floor']) for r in s.records() if r['code'] in ('created', 'state')]
        self.assertEqual(floors, sorted(floors))  # monotone
        self.assertTrue(all(f >= cost_floor for f in floors))
        sells = [o for o in executor._exit_orders() if o.order.executed_amount_base > 0]
        self.assertTrue(all(o.order.price >= cost_floor for o in sells))

    async def test_an_exit_below_the_cost_floor_rests_at_it_and_the_bag_line_never_demotes_the_runner(self):
        s = self.scenario()
        executor, _ = await self.runner_phase(s)
        cost_floor = self.runner(s, executor)['cost_floor']
        await s.step(mids={BTC: '100'})  # close below the cost floor: exit signal, bid below the floor
        self.assertEqual(self.runner(s, executor)['phase'], EXIT)
        resting = executor._close_order.order
        self.assertEqual((resting.amount, resting.price), (D('0.249'), D('101.29')))  # cost floor up to the tick
        self.assertGreaterEqual(resting.price, cost_floor)
        [placed] = s.records('exit_placed')
        self.assertEqual((placed['fill_kind'], placed['price'], placed['order_floor']),
                         ('LIMIT', str(cost_floor), str(cost_floor)))
        receipt = await s.step(mids={BTC: '96.5'})  # 3.5 % under the entry: Meridian's bag line fires
        self.assertFalse([a for a in receipt['actions'] if isinstance(a, StopExecutorAction)])
        self.assertGreater(s.controller.runners.kept_stops, 0)
        self.assertFalse(executor.is_closed)
        self.assertIs(executor._close_order.order, resting)  # still resting at the cost floor
        self.assertEqual(sum((p.amount for p in s.controller.positions_held), D('0')), D('0'))
        await s.step(mids={BTC: '101'})
        await s.lift_at(BTC, '101.25')  # below the limit: no fill
        self.assertFalse(executor.is_closed)
        await s.step(mids={BTC: '101.3'})
        await s.lift_at(BTC, '101.40')
        await s.step()
        self.assertTrue(executor.is_closed)
        self.assertEqual((resting.executed_amount_base, resting.price), (D('0.249'), D('101.29')))
        self.assertEqual(s.balance('BTC'), D('0'))

    async def test_partial_fills_of_the_harvest_sale_and_of_the_runner_exit(self):
        s = self.scenario()
        executor, owned = await self.harvested(s, depth='0.1')
        self.assertEqual(executor.status, RunnableStatus.SHUTTING_DOWN)  # harvest sale partly filled, still resting
        self.assertEqual(executor.amount_to_close, owned - D('0.1'))
        await s.lift(BTC, amount='0.149')
        self.assertEqual((executor.status, executor.amount_to_close), (RunnableStatus.RUNNING, D('0.249')))
        await s.step(mids={BTC: '100'})  # exit below the cost floor rests at it
        resting = executor._close_order.order
        await s.step(mids={BTC: '101.5'})
        await s.lift_at(BTC, '101.40', amount='0.1')
        self.assertEqual((resting.executed_amount_base, executor.amount_to_close), (D('0.1'), D('0.149')))
        self.assertFalse(executor.is_closed)
        await s.lift_at(BTC, '101.40')
        await s.step()
        self.assertTrue(executor.is_closed)
        self.assertEqual(s.balance('BTC'), D('0'))

    async def test_a_runner_exit_while_the_harvest_sale_rests_sells_everything_above_both_floors(self):
        s = self.scenario()
        executor, owned = await self.harvested(s, depth='0.05')
        harvest = executor._close_order.order
        self.assertEqual(harvest.executed_amount_base, D('0.05'))
        protected = executor._min_no_loss_sell_price()
        await s.step(mids={BTC: '100'})  # the runner exits before the harvest sale filled
        self.assertEqual(self.runner(s, executor)['phase'], EXIT)
        self.assertEqual(harvest.current_state.name, 'CANCELED')
        await s.step()
        merged = executor._close_order.order
        self.assertEqual(merged.amount, owned - D('0.05'))  # one exit for everything still owned
        self.assertGreaterEqual(merged.price, max(protected, self.runner(s, executor)['cost_floor']))
        await s.step(mids={BTC: '104'})
        await s.lift_at(BTC, merged.price + D('0.05'))
        await s.step()
        self.assertTrue(executor.is_closed)
        self.assertEqual(s.balance('BTC'), D('0'))

    # ------------------------------------------------------------------ base-denominated buy fees (fee dust)
    async def test_with_fee_dust_the_runner_exit_rests_at_the_cost_floor_without_churn(self):
        s = self.scenario(base_fee=True)
        executor, owned = await self.harvested(s)
        self.assertNotEqual(owned, lot(owned))  # the buy fee left sub-lot dust in the owned base
        protected = executor._min_no_loss_sell_price()  # the harvest part's native protected floor
        await s.step()  # the harvest sale filled: runner phase
        runner = self.runner(s, executor)
        self.assertEqual(executor.status, RunnableStatus.RUNNING)
        self.assertEqual(lot(executor.amount_to_close), runner['runner_base'])
        self.assertLess(runner['runner_base'], executor.amount_to_close)  # dust, not an unsold harvest part
        await s.step(mids={BTC: '100'})  # exit signal, bid below the cost floor: a resting limit at the cost floor
        self.assertEqual(self.runner(s, executor)['phase'], EXIT)
        resting = executor._close_order.order
        cost_floor = self.runner(s, executor)['cost_floor']
        self.assertEqual((resting.amount, resting.price), (lot(executor.amount_to_close), up_to_tick(cost_floor)))
        self.assertLess(resting.price, protected)  # the harvest part's floor does not apply to the runner
        for _ in range(6):
            await s.step(minutes=0, seconds=1)
            self.assertIs(executor._close_order.order, resting)
            self.assertEqual(resting.current_state.name, 'OPEN')
        await s.step(mids={BTC: '100.5'})
        self.assertIs(executor._close_order.order, resting)
        [_, exit_order] = executor._exit_orders()  # the harvest sale and one runner exit: no cancel and re-place
        self.assertIs(exit_order.order, resting)
        [placed] = s.records('exit_placed')
        self.assertEqual(placed['order_floor'], str(cost_floor))
        await s.step(mids={BTC: '101.3'})
        await s.lift_at(BTC, '101.40')
        await s.step()
        self.assertTrue(executor.is_closed)
        self.assertEqual(executor.close_type, CloseType.TRAILING_STOP)
        self.assertEqual(resting.executed_amount_base, resting.amount)
        self.assertLess(executor.amount_to_close, LOT)  # only the native sub-lot residue remains
        self.assertEqual(s.balance('BTC'), executor.amount_to_close)

    async def test_with_fee_dust_a_runner_exit_while_the_harvest_rests_sells_the_owned_lot_above_both_floors(self):
        s = self.scenario(base_fee=True)
        executor, owned = await self.harvested(s, depth='0.05')
        harvest = executor._close_order.order
        self.assertEqual(harvest.executed_amount_base, D('0.05'))
        protected = executor._min_no_loss_sell_price()
        await s.step(mids={BTC: '100'})
        self.assertEqual(self.runner(s, executor)['phase'], EXIT)
        self.assertEqual(harvest.current_state.name, 'CANCELED')
        await s.step()
        merged = executor._close_order.order
        self.assertEqual(merged.amount, lot(owned - D('0.05')))  # the largest owned lot, harvest part included
        cost_floor = self.runner(s, executor)['cost_floor']
        self.assertGreater(protected, cost_floor)
        self.assertEqual(merged.price, up_to_tick(protected))  # the unsold harvest part keeps its protected floor
        for _ in range(3):
            await s.step(minutes=0, seconds=1)
            self.assertIs(executor._close_order.order, merged)
        await s.step(mids={BTC: '104'})
        await s.lift_at(BTC, merged.price + D('0.05'))
        await s.step()
        self.assertTrue(executor.is_closed)
        self.assertLess(s.balance('BTC'), LOT)

    async def test_a_lost_cancel_of_the_resting_harvest_is_requested_again_within_a_bound(self):
        s = self.scenario()
        executor, owned = await self.harvested(s, depth='0.05')
        harvest = executor._close_order.order
        requests = []

        def lost(**kwargs):
            requests.append(kwargs['order_id'])  # the request never reaches the venue

        with patch.object(executor._strategy, 'cancel', side_effect=lost):
            await s.step(mids={BTC: '100'})  # runner exit: the resting harvest sale must be cancelled
            self.assertEqual(requests, [harvest.client_order_id])
            await s.step(minutes=0, seconds=int(RUNNER_CANCEL_RETRY_SECONDS) - 2)
            self.assertEqual(len(requests), 1)  # not before the retry interval
            with self.assertLogs(level=logging.WARNING) as logs:
                for k in range(2, RUNNER_CANCEL_MAX_REQUESTS + 3):
                    await s.step(minutes=0, seconds=k * int(RUNNER_CANCEL_RETRY_SECONDS))
            self.assertEqual(requests, [harvest.client_order_id] * RUNNER_CANCEL_MAX_REQUESTS)  # bounded
            self.assertTrue(any('cancel requested again' in line for line in logs.output))
            self.assertTrue(any('cancel unconfirmed' in line for line in logs.output))
            self.assertEqual(s.controller.runners.status[BTC], f'runner_cancel_unconfirmed:{harvest.client_order_id}')
            self.assertTrue(harvest.is_open)  # still resting at its protected floor
            await s.step(minutes=0, seconds=(RUNNER_CANCEL_MAX_REQUESTS + 6) * int(RUNNER_CANCEL_RETRY_SECONDS))
            self.assertEqual(len(requests), RUNNER_CANCEL_MAX_REQUESTS)  # no request after the bound
        self.assertFalse(executor.is_closed)

    async def test_a_re_requested_cancel_lets_the_runner_exit_sell_everything(self):
        s = self.scenario()
        executor, owned = await self.harvested(s, depth='0.05')
        harvest = executor._close_order.order
        with patch.object(executor._strategy, 'cancel', side_effect=lambda **kwargs: None):
            await s.step(mids={BTC: '100'})  # the first cancel request is lost
        self.assertTrue(harvest.is_open)
        await s.step(minutes=0, seconds=int(RUNNER_CANCEL_RETRY_SECONDS))  # requested again: the venue cancels
        self.assertEqual(harvest.current_state.name, 'CANCELED')
        await s.step()
        merged = executor._close_order.order
        self.assertEqual(merged.amount, owned - D('0.05'))
        self.assertGreaterEqual(merged.price, self.runner(s, executor)['cost_floor'])

    async def test_a_split_below_the_minimum_keeps_no_runner_and_the_native_exit_runs(self):
        s = self.scenario(runner=RUNNER_ITER5_B.model_copy(update={'runner_fraction': D('0.001')}))
        executor, owned = await self.harvested(s)
        self.assertEqual(s.controller.runners.lots, {})
        self.assertEqual(s.controller.runners.skipped, {executor.config.id: 'runner_below_min_notional'})
        self.assertEqual(executor._close_order.order.amount, owned)
        await s.lift(BTC)
        self.assertTrue(executor.is_closed)
        self.assertEqual(executor.close_type, CloseType.TRAILING_STOP)

    async def test_without_the_runner_tranches_keep_the_meridian_executor_and_exit(self):
        s = self.scenario(runner=None, seed_days=0)
        self.assertIsNone(s.controller.runners)
        self.assertNotIn('runner', s.controller.get_custom_info()['tidewater'])
        executor = await self.entered(s)
        self.assertIs(type(executor), ModularPositionExecutor)
        self.assertIs(s.controller.restart_continuity_executor_class(executor.config), ModularPositionExecutor)
        for mid in ('104.5', '105.5', '104.9'):
            await s.step(mids={BTC: mid})
        await s.step(minutes=0, seconds=1)
        self.assertTrue(executor.is_closed)  # the whole tranche sold at the native trailing exit
        self.assertEqual((executor.close_type, s.balance('BTC')), (CloseType.TRAILING_STOP, D('0')))
        self.assertEqual(s.records(), [])

    # ------------------------------------------------------------------ restart on the same database
    async def test_a_fresh_book_on_the_same_database_rebuilds_the_runner_exactly(self):
        s = self.scenario()
        executor, _ = await self.runner_phase(s)
        for mid in ('106', '108', '110', '107'):
            await s.step(mids={BTC: mid})
        live = s.controller.runners
        fresh = RunnerBook(s.controller, s.config.exit_runner, ReceiptWriter(OWNER))
        self.assertTrue(fresh.load())
        stored, current = fresh.lots[executor.config.id], live.lots[executor.config.id]
        persisted = ('runner_base', 'cost_basis', 'cost_floor', 'floor', 'high_water', 'phase', 'seq', 'harvest_price')
        self.assertEqual({k: stored[k] for k in persisted}, {k: current[k] for k in persisted})
        self.assertLessEqual(stored['last_minute'], current['last_minute'])  # unchanged minutes are replayed
        self.assertEqual(fresh.days[BTC], live.days[BTC])
        self.assertEqual(fresh.atr_pct(BTC, s.now), ATR)

    async def test_restart_rebuilds_the_runner_executor_and_its_floor_and_exits_at_the_floor(self):
        s = self.scenario()
        executor, _ = await self.runner_phase(s)
        for mid in ('106', '108', '110'):
            await s.step(mids={BTC: mid})
        # Process restart: the executor row and the runner records on disk are all that remain.
        s.session.recorder.store_or_update_executor(executor)
        [row] = [i for i in s.session.recorder.get_all_executors() if i.id == executor.config.id]
        plan = continuity_plan(row)
        cls = s.controller.restart_continuity_executor_class(row.config)
        self.assertIs(cls, TidewaterRunnerExecutor)
        restored = cls(s.session, row.config, 1.0, 10)
        restored.hydrate_restart_continuity(plan)
        executor.unregister_events()
        active = s.session.executor_orchestrator.active_executors[OWNER]
        active[active.index(executor)] = restored
        s.session._start_executor(restored)
        s.controller.runners = RunnerBook(s.controller, s.config.exit_runner, ReceiptWriter(OWNER))
        self.assertEqual(restored.amount_to_close, D('0.249'))
        await s.step(mids={BTC: '107'})
        runner = s.controller.runners.lots[restored.config.id]
        self.assertEqual((runner['floor'], runner['high_water'], runner['phase']), (D('105.6'), D('110'), OPEN))
        self.assertIsNone(restored._close_order)  # no second harvest sale after the restart
        await s.step(mids={BTC: '105.5'})
        exit_order = restored._close_order.order
        self.assertEqual((exit_order.amount, exit_order.price), (D('0.249'), D('105.49')))
        await s.lift(BTC)
        await s.step()
        self.assertTrue(restored.is_closed)
        self.assertEqual(s.balance('BTC'), D('0'))
        self.assertEqual(s.grants(), ())

    async def test_paper_session_shutdown_hands_the_runner_to_the_held_ledger_and_restore_settles_it(self):
        s = self.scenario()
        executor, _ = await self.runner_phase(s)
        minute, sequence, trades = s.minute, dict(s.sequence), dict(s.trade_sequence)
        await s.close()  # native paper shutdown: every active executor keeps its position as held inventory
        r = self.scenario(restore=True, seed_days=0)
        r.minute, r.sequence, r.trade_sequence = minute, sequence, trades
        r.mids[BTC] = D('104.9')
        await r.step()
        self.assertEqual(r.controller.runners.lots, {})
        self.assertEqual(r.controller.runners.closed, {executor.config.id: 'POSITION_HOLD'})
        self.assertEqual(sum((p.amount for p in r.controller.positions_held if p.trading_pair == BTC), D('0')),
                         D('0.249'))
        self.assertEqual(r.controller.runners.days[BTC], s.controller.runners.days[BTC])
