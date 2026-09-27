"""End-to-end order lifecycle of the unified Meridian controller on the native paper engine.

Every order, fill, executor, held position, grant and restart is native; only the
closed-candle features are scripted (see lifecycle_harness). Economics are not claimed.
"""
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.lifecycle_harness import Scenario, creates, rebound, stops
from types import SimpleNamespace as NS
from unittest import IsolatedAsyncioTestCase

from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.models.executors import CloseType

BTC, ETH, SOL = 'BTC-USDC', 'ETH-USDC', 'SOL-USDC'


class Lifecycle(IsolatedAsyncioTestCase):
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
        scenario = Scenario(self.directory, **kwargs)
        self.scenarios.append(scenario)
        return scenario

    async def entered(self, s, pair, *, amount='1'):
        await s.step()  # the first observed cut only seeds each symbol
        receipt = await s.step(rsi={pair: rebound()})
        [create] = creates(receipt, pair)
        await s.fill(pair, amount)
        [executor] = [e for e in s.executors(pair) if e.config.id == create.executor_config.id]
        return executor

    def reason(self, s, pair):
        return s.controller.contexts[pair].runtime.reason

    def bag(self, s, pair):
        return s.controller.contexts[pair].runtime.bag_status or {}

    # ------------------------------------------------------------------ entries
    async def test_entry_is_one_native_limit_tranche_within_twenty_quote(self):
        s = self.scenario()
        await s.step()
        receipt = await s.step(rsi={BTC: rebound()})
        [create] = creates(receipt)
        config = create.executor_config
        self.assertEqual((config.trading_pair, config.side, config.controller_id), (BTC, TradeType.BUY, s.config.id))
        self.assertTrue(config.level_id.startswith('modular_spot:v1:'))
        self.assertLessEqual(config.amount * config.entry_price * D('1.003'), D('20'))
        [executor] = s.executors(BTC)
        self.assertIs(type(executor), ModularPositionExecutor)
        self.assertEqual(executor._open_order.order.price, config.entry_price)  # a resting limit, never a market buy
        [grant] = s.grants()
        self.assertEqual((grant.symbol, grant.status, grant.executor_id), (BTC, 'BOUND', config.id))

    async def test_partial_fill_then_entry_timeout_keeps_the_partial_managed_and_reserved(self):
        s = self.scenario()
        executor = await self.entered(s, BTC, amount='.05')
        self.assertEqual(executor.open_filled_amount, D('.05'))
        await s.step(minutes=6)  # past the 300 s p23 entry timeout
        self.assertEqual(executor._open_order.order.current_state.name, 'CANCELED')
        self.assertTrue(executor.is_active)
        self.assertEqual(s.balance('BTC'), D('.05'))
        [grant] = s.grants()
        self.assertEqual(grant.status, 'BOUND')  # released only when the partial is flat
        await s.step(mids={BTC: '105'})
        await s.step(mids={BTC: '104.4'})
        await s.lift(BTC)
        self.assertEqual(executor.close_type, CloseType.TRAILING_STOP)
        self.assertEqual(s.balance('BTC'), D('0'))
        await s.step()
        self.assertEqual(s.grants(), ())

    # ------------------------------------------------------------------ trailing
    async def test_trailing_round_trip_sells_above_the_protected_floor_and_frees_capital(self):
        s = self.scenario()
        executor = await self.entered(s, BTC)
        cost = executor.open_filled_amount * executor._open_order.order.price
        for mid in ('102', '103.9'):
            await s.step(mids={BTC: mid})
            self.assertIsNone(executor._trailing_stop_trigger_pct)  # below the 4 % activation
        for mid in ('104.5', '105.5'):
            await s.step(mids={BTC: mid})
        self.assertIsNotNone(executor._trailing_stop_trigger_pct)
        floor = executor._min_no_loss_sell_price()
        self.assertGreater(floor, executor._open_order.order.price)
        await s.step(mids={BTC: '104.9'})
        close = executor._close_order.order
        self.assertEqual(executor.close_type, CloseType.TRAILING_STOP)
        self.assertEqual(close.order_type.name, 'LIMIT')
        self.assertGreaterEqual(close.price, floor)
        await s.lift(BTC)
        self.assertTrue(executor.is_closed)
        self.assertGreater(close.price * close.executed_amount_base, cost)
        await s.step()
        self.assertEqual(s.grants(), ())
        receipt = await s.step(rsi={BTC: rebound()})
        self.assertEqual(len(creates(receipt, BTC)), 1)  # capital came back; the symbol re-enters

    async def test_no_trailing_or_bag_sell_ever_realizes_a_loss(self):
        s = self.scenario()
        executor = await self.entered(s, BTC)
        for mid in ('99.5', '99', '98.5'):  # under water, above the -3 % bag line
            receipt = await s.step(mids={BTC: mid}, rsi={BTC: (80, 70)})
            self.assertEqual([a for a in receipt['actions'] if getattr(a, 'executor_config', None)
                              and a.executor_config.trading_pair == BTC], [])
            self.assertIsNone(executor._close_order)
        await s.step(mids={BTC: '96.9'})  # bag at -3 %: keep the position
        await s.step(mids={BTC: '96.9'})
        self.assertEqual(s.held(BTC), executor.open_filled_amount)
        for mid in ('100.5', '101.5', '101'):  # above breakeven, below the +2 % recovery floor
            receipt = await s.step(mids={BTC: mid}, rsi={BTC: (80, 80)})
            self.assertEqual(creates(receipt, BTC), [])
        self.assertEqual(s.balance('BTC'), executor.open_filled_amount)

    # ------------------------------------------------------------------ DCA, bag and recovery
    async def test_dca_bag_recovery_and_release_cycle(self):
        s = self.scenario()
        first = await self.entered(s, ETH)
        receipt = await s.step(mids={ETH: '99'}, rsi={ETH: rebound()})  # >= buy gap below the last buy
        [second_create] = creates(receipt, ETH)
        self.assertEqual(len({g.grant_id for g in s.grants() if g.symbol == ETH}), 2)  # a new grant per stage
        await s.fill(ETH)
        receipt = await s.step(mids={ETH: '96.9'})
        self.assertEqual([(a.executor_id, a.keep_position) for a in stops(receipt)], [(first.config.id, True)])
        await s.step(mids={ETH: '96.8'})
        self.assertEqual(first.close_type, CloseType.POSITION_HOLD)
        self.assertEqual(s.held(ETH), first.open_filled_amount)
        second = next(e for e in s.executors(ETH) if e.config.id == second_create.executor_config.id)
        self.assertTrue(second.is_active)  # the shallower tranche keeps its own trailing
        for mid in ('101', '103', '104'):
            await s.step(mids={ETH: mid}, rsi={ETH: (80, 80)})
        self.assertTrue(self.bag(s, ETH)['trail_armed'])
        receipt = await s.step(mids={ETH: '103.05'}, rsi={ETH: (80, 80)})
        [sell] = [a for a in creates(receipt, ETH) if a.executor_config.type == 'order_executor']
        config = sell.executor_config
        self.assertEqual((config.side, config.amount), (TradeType.SELL, first.open_filled_amount))
        self.assertGreaterEqual(config.min_sell_price, first._open_order.order.price * D('1.02'))
        self.assertTrue(any(type(e) is OrderExecutor for e in s.executors(ETH)))
        await s.lift(ETH, amount='5', above='0.5')
        await s.step()
        self.assertEqual(s.balance('ETH'), D('0'))
        self.assertTrue(self.bag(s, ETH)['release'].startswith('released'))
        self.assertEqual(s.grants(), ())
        receipt = await s.step(rsi={ETH: rebound()})
        self.assertEqual(len(creates(receipt, ETH)), 1)

    async def test_partial_bag_sell_leaves_dust_and_still_releases(self):
        s = self.scenario()
        executor = await self.entered(s, ETH)
        await s.step(mids={ETH: '96.9'})
        await s.step(mids={ETH: '96.8'})
        for mid in ('101', '103', '104'):
            await s.step(mids={ETH: mid}, rsi={ETH: (80, 80)})
        receipt = await s.step(mids={ETH: '103.05'}, rsi={ETH: (80, 80)})
        [sell] = [a for a in creates(receipt, ETH) if a.executor_config.type == 'order_executor']
        await s.lift(ETH, amount=str(sell.executor_config.amount - D('.004')), above='0.5')
        self.assertEqual(s.balance('ETH'), D('.004'))  # ~0.41 USDC: below the 1 USDC minimum notional
        for mid in ('102.5', '101.5'):  # the chaser refreshes; the remainder cannot form an order
            await s.step(mids={ETH: mid}, rsi={ETH: (60, 60)})
        [done] = [e for e in s.session.executor_orchestrator.active_executors[s.config.id] + []
                  if e.config.id == sell.executor_config.id] or [None]
        self.assertTrue(done is None or done.is_closed)
        await s.step()
        self.assertEqual(s.balance('ETH'), D('.004'))
        self.assertIn('released', self.bag(s, ETH).get('release') or '', self.bag(s, ETH))
        self.assertEqual(s.grants(), ())
        receipt = await s.step(rsi={ETH: rebound()})
        self.assertEqual(len(creates(receipt, ETH)), 1)  # dust never pins the symbol
        self.assertIsNotNone(executor)

    async def test_per_asset_ceiling_and_depth_budget_bound_a_falling_symbol(self):
        s = self.scenario()
        await self.entered(s, SOL)
        mid, denied = D('100'), []
        for _ in range(6):
            mid = mid * D('.992')  # above the -3 % bag line of the newest tranche, below the buy gap
            receipt = await s.step(mids={SOL: str(mid.quantize(D('.01')))}, rsi={SOL: rebound()})
            if creates(receipt, SOL):
                await s.fill(SOL)
            else:
                denied.append(self.reason(s, SOL))
        deployed = sum((g.reserved_quote for g in s.grants() if g.symbol == SOL), D('0'))
        self.assertLessEqual(deployed, D('100'))
        self.assertTrue(denied, 'a falling symbol must hit its capital limits')

    # ------------------------------------------------------------------ portfolio isolation
    async def test_one_symbol_bagged_while_another_completes_a_trailing_round_trip(self):
        s = self.scenario()
        await s.step()
        receipt = await s.step(rsi={BTC: rebound(), ETH: rebound()})
        self.assertEqual({a.executor_config.trading_pair for a in creates(receipt)}, {BTC, ETH})
        await s.fill(BTC)
        await s.fill(ETH)
        eth = s.executors(ETH)[0]
        await s.step(mids={BTC: '96.9'})
        await s.step(mids={BTC: '96.8'})
        self.assertGreater(s.held(BTC), 0)
        for mid in ('104.5', '105.5', '104.9'):
            await s.step(mids={ETH: mid})
        await s.lift(ETH)
        self.assertEqual(eth.close_type, CloseType.TRAILING_STOP)
        self.assertEqual(s.balance('ETH'), D('0'))
        self.assertGreater(s.held(BTC), 0)
        self.assertEqual({g.symbol for g in s.grants()}, {BTC})

    async def test_entry_pause_and_disabled_symbol_keep_exits_running(self):
        s = self.scenario()
        executor = await self.entered(s, BTC)
        s.controller.set_operator_entry_pause(True, reason='test')
        changed = s.controller.config.model_dump(mode='json')
        changed['symbols'] = [{'trading_pair': p, 'entry_enabled': p != BTC} for p in PAIRS]
        s.controller.update_config(type(s.controller.config)(**changed))
        receipt = await s.step(rsi={ETH: rebound()})
        self.assertEqual(creates(receipt), [])
        for mid in ('104.5', '105.5', '104.9'):
            await s.step(mids={BTC: mid})
        await s.lift(BTC)
        self.assertEqual(executor.close_type, CloseType.TRAILING_STOP)
        self.assertEqual(s.balance('BTC'), D('0'))

    async def test_foreign_writer_holds_new_entries_but_not_exits(self):
        s = self.scenario()
        executor = await self.entered(s, BTC)
        foreign = NS(id='foreign-buy', controller_id='modular_spot_v2_eth', is_done=False, is_active=True,
                     custom_info={}, config=NS(controller_id='modular_spot_v2_eth', connector_name='okx_paper_trade',
                                               trading_pair='ETH-USDC', type='position_executor',
                                               side=NS(name='BUY'), level_id='x'))
        evidence = s.controller.portfolio.evidence
        s.controller.portfolio.evidence = lambda: (*evidence(), foreign)
        receipt = await s.step(rsi={ETH: rebound()})
        self.assertEqual(creates(receipt), [])
        self.assertTrue(s.controller.portfolio.status.get('snapshot_holds')
                        or s.controller.portfolio.status['reason'] == 'GRANT_STORE_HOLD', s.controller.portfolio.status)
        for mid in ('104.5', '105.5', '104.9'):
            await s.step(mids={BTC: mid})
        await s.lift(BTC)
        self.assertEqual(executor.close_type, CloseType.TRAILING_STOP)

    async def test_stale_book_pauses_the_paper_owner_without_losing_state(self):
        s = self.scenario()
        executor = await self.entered(s, BTC)
        receipt = await s.step(mids={BTC: '104.5'}, stale=('XRP-USDC',))
        self.assertIn('fresh_book_required', receipt['readiness']['reasons'])
        self.assertEqual(receipt['actions'], [])
        self.assertIsNone(executor._trailing_stop_trigger_pct)  # not driven while the owner is paused
        self.assertTrue(executor.is_active)
        for mid in ('104.5', '105.5', '104.9'):  # books fresh again: the same position resumes
            await s.step(mids={BTC: mid})
        await s.lift(BTC)
        self.assertEqual(executor.close_type, CloseType.TRAILING_STOP)
        self.assertEqual(s.grants(), ())

    # ------------------------------------------------------------------ restart
    async def test_restart_with_a_held_bag_recovers_it_without_reentry(self):
        s = self.scenario()
        executor = await self.entered(s, BTC)
        amount = executor.open_filled_amount
        await s.step(mids={BTC: '96.9'})
        await s.step(mids={BTC: '96.8'})
        bound = s.grants()
        minute = s.minute
        await s.close()
        r = self.scenario(restore=True)
        r.minute, r.sequence, r.trade_sequence = minute, dict(s.sequence), dict(s.trade_sequence)
        receipt = await r.step(mids={BTC: '96.8'}, rsi={BTC: rebound()})
        self.assertEqual(creates(receipt, BTC), [])  # the first observed cut after restart only seeds
        self.assertEqual(r.held(BTC), amount)
        self.assertEqual(r.grants(), bound)
        for mid in ('101', '103', '104'):
            await r.step(mids={BTC: mid}, rsi={BTC: (80, 80)})
        receipt = await r.step(mids={BTC: '103.05'}, rsi={BTC: (80, 80)})
        [sell] = creates(receipt, BTC)
        self.assertEqual(sell.executor_config.amount, amount)
        await r.lift(BTC, amount='5', above='0.5')
        await r.step()
        self.assertEqual(r.balance('BTC'), D('0'))
        self.assertEqual(r.grants(), ())
