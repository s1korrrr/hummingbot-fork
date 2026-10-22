"""End-to-end order lifecycle of the unified Meridian controller on the native paper engine.

Every order, fill, executor, held position, grant and restart is native; only the
closed-candle features are scripted (see lifecycle_harness). Economics are not claimed.
"""
import tempfile
from contextlib import nullcontext
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.lifecycle_harness import Scenario, creates, rebound, stops
from test.meridian_v3.test_unified_runtime import OWNER
from types import SimpleNamespace as NS
from unittest import IsolatedAsyncioTestCase

from sqlalchemy import event

from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.meridian_v3_unified_core.execution import MeridianRecoveryExecutor
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.portfolio_v3.runtime_contracts_v1 import NATIVE_SNAPSHOT_UNAVAILABLE

BTC, ETH, SOL, XRP, BNB = 'BTC-USDC', 'ETH-USDC', 'SOL-USDC', 'XRP-USDC', 'BNB-USDC'


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

    def scenario(self, *, directory=None, **kwargs):
        directory = directory or self.directory
        directory.mkdir(exist_ok=True)
        scenario = Scenario(directory, **kwargs)
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

    async def test_finished_executors_are_archived_like_live_and_capital_stays_consistent(self):
        # Live stores done executors beyond `closed_executors_buffer`; the paper step must too, or
        # every finished executor stays in the per-step reports forever. Buffer 0 is the strictest case.
        s = self.scenario()
        s.session.closed_executors_buffer = 0
        finished = []
        for _ in range(3):
            executor = await self.entered(s, BTC)
            for mid in ('104.5', '105.5', '104.9'):
                await s.step(mids={BTC: mid})
            await s.lift(BTC)
            self.assertTrue(executor.is_closed)
            finished.append(executor.config.id)
            await s.step()  # the owner sees the done executor once, then the step archives it
            self.assertEqual(s.executors(BTC), [])
            self.assertEqual(s.grants(), ())
            s.mids[BTC] = D('100')
        stored = {e.id for e in s.session.recorder.get_all_executors() if e.close_type is not None}
        self.assertLessEqual(set(finished), stored)
        self.assertEqual(s.balance('BTC'), D('0'))
        receipt = await s.step(rsi={BTC: rebound()})
        self.assertEqual(len(creates(receipt, BTC)), 1)  # archived history never blocks the next entry

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
        self.assertTrue(any(type(e) is MeridianRecoveryExecutor for e in s.executors(ETH)))
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

    def kill_switch(self, s, enabled):
        """Flip the config switch and run the real V2 script check on this native session."""
        from unittest.mock import MagicMock

        from scripts.v2_with_controllers import V2WithControllers
        changed = s.controller.config.model_dump(mode='json')
        changed['manual_kill_switch'] = enabled
        s.controller.update_config(type(s.controller.config)(**changed))
        orchestrator = s.session.executor_orchestrator
        strategy = getattr(s, 'kill_switch_strategy', None) or NS(
            controllers=s.session.controllers, executor_orchestrator=orchestrator, drawdown_exited_controllers=[],
            _acquisition_kill_switch=set(), logger=MagicMock(return_value=MagicMock()),
            get_executors_by_controller=lambda key: orchestrator.get_executors_report(controller_ids={key}).get(key, []))
        strategy._kill_switch_acquisitions = lambda *args: V2WithControllers._kill_switch_acquisitions(strategy, *args)
        s.kill_switch_strategy = strategy
        V2WithControllers.check_manual_kill_switch(strategy)

    async def test_kill_switch_stops_buying_only_for_filled_partial_unfilled_and_held(self):
        s = self.scenario()
        await s.step()
        receipt = await s.step(rsi={p: rebound() for p in (BTC, ETH, SOL, BNB)})
        self.assertEqual({a.executor_config.trading_pair for a in creates(receipt)}, {BTC, ETH, SOL, BNB})
        await s.fill(BTC)  # filled tranche
        await s.fill(ETH, amount='.05')  # partial fill, remainder still resting
        await s.fill(BNB)
        await s.step(mids={BNB: '96.9'})
        await s.step(mids={BNB: '96.8'})  # held bag
        btc, eth, sol = (s.executors(p)[0] for p in (BTC, ETH, SOL))  # SOL: an unfilled resting entry
        bag = s.held(BNB)
        self.assertGreater(bag, 0)

        self.kill_switch(s, True)
        self.assertEqual(s.controller.status, RunnableStatus.RUNNING)  # the owner keeps managing
        receipt = await s.step(rsi={XRP: rebound()})
        self.assertEqual(creates(receipt), [])
        self.assertEqual(self.reason(s, XRP), 'kill_switch_entry_hold')
        # Unfilled: the entry ends without a fill and its capital is released.
        self.assertEqual(sol._open_order.order.current_state.name, 'CANCELED')
        await s.step()  # the native cancel acknowledgement is reconciled one step later
        self.assertEqual(sol.close_type, CloseType.EXPIRED)
        self.assertEqual(sol.open_filled_amount, D('0'))
        self.assertNotEqual(sol.close_type, CloseType.POSITION_HOLD)
        self.assertEqual(s.balance('SOL'), D('0'))
        self.assertNotIn(SOL, {g.symbol for g in s.grants()})
        # Partial: the remainder stops buying; the filled part stays an active, trailing tranche.
        self.assertEqual(eth._open_order.order.current_state.name, 'CANCELED')
        self.assertTrue(eth.is_active)
        self.assertIsNone(eth.close_type)
        self.assertEqual((eth.open_filled_amount, s.held(ETH)), (D('.05'), D('0')))
        # Filled: never demoted, never sold by the switch.
        self.assertTrue(btc.is_active)
        self.assertIsNone(btc.close_type)
        self.assertEqual(s.held(BTC), D('0'))
        self.kill_switch(s, True)  # every tick re-checks the switch; nothing is stopped twice
        # Management continues: BTC trails out above its floor and the BNB bag recovers.
        for btc_mid, bnb_mid in (('104.5', '101'), ('105.5', '103'), ('104.9', '104')):
            if btc_mid == '104.9':
                floor = btc._min_no_loss_sell_price()
            await s.step(mids={BTC: btc_mid, BNB: bnb_mid}, rsi={BNB: (80, 80)})
        await s.lift(BTC)
        self.assertEqual(btc.close_type, CloseType.TRAILING_STOP)
        self.assertGreaterEqual(btc._close_order.order.price, floor)
        self.assertTrue(self.bag(s, BNB)['trail_armed'])
        receipt = await s.step(mids={BNB: '103.05'}, rsi={BNB: (80, 80)})
        [sell] = creates(receipt, BNB)
        self.assertEqual((sell.executor_config.side, sell.executor_config.amount), (TradeType.SELL, bag))
        self.assertEqual(s.held(BNB), bag)  # the bag was never sold at a loss or liquidated by the switch

        self.kill_switch(s, False)
        receipt = await s.step(rsi={XRP: rebound()})
        self.assertEqual(len(creates(receipt, XRP)), 1)

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

    # ------------------------------------------------------------------ shared-wallet cash contention
    def foreign_bid(self, s, quote='489'):
        """Another owner of the physical wallet rests a bid: native cash is reserved, no Meridian row."""
        return s.session._connector.buy(SOL, D(quote) / D('90'), OrderType.LIMIT, D('90'))

    async def release_foreign(self, s, order_id):
        s.session._connector.cancel(SOL, order_id)
        await s.session._connector.drain()  # the venue acknowledges the cancel; the cash is available again

    def claims(self, s, pair):
        state, _ = s.controller.contexts[pair].runtime.ensure_store().load()
        return () if state is None else state.claims

    def contend(self, s, *, after_create=False):
        """Spend the wallet once, just before (or after) the engine executes this step's creates."""
        orchestrator, spent = s.session.executor_orchestrator, []
        original = orchestrator.execute_actions

        def execute(actions):
            creating = any(isinstance(a, CreateExecutorAction) for a in actions)
            if creating and not after_create and not spent:
                spent.append(self.foreign_bid(s))
            result = original(actions)
            if creating and after_create and not spent:
                spent.append(self.foreign_bid(s))
            return result
        orchestrator.execute_actions = execute
        return spent

    async def test_insufficient_shared_cash_refuses_entry_and_a_later_signal_enters(self):
        s = self.scenario()
        await s.step()
        foreign = self.foreign_bid(s)
        self.assertLess(s.session._connector.get_available_balance('USDC'), D('20'))
        receipt = await s.step(rsi={BTC: rebound()})
        self.assertEqual(creates(receipt), [])
        self.assertEqual((s.grants(), self.claims(s, BTC)), ((), ()))  # nothing reserved, nothing consumed
        await self.release_foreign(s, foreign)
        receipt = await s.step(rsi={BTC: rebound()})
        self.assertEqual(len(creates(receipt, BTC)), 1)

    async def test_cash_taken_before_creation_never_pins_the_symbol_or_its_capital(self):
        s = self.scenario()
        await s.step()
        spent = self.contend(s)
        receipt = await s.step(rsi={BTC: rebound()})
        [create] = creates(receipt, BTC)  # decided and bound, then the engine refused the create
        self.assertEqual(s.executors(BTC), [])
        self.assertNotIn(create.executor_config.id, s.controller.contexts[BTC].runtime.dispatched)
        [grant] = s.grants()
        self.assertEqual((grant.status, grant.executor_id), ('BOUND', create.executor_config.id))
        await s.step(minutes=4)
        self.assertEqual(self.reason(s, BTC), 'execution_receipt_missing')  # the entry window is still open
        self.assertEqual(len(s.grants()), 1)
        await s.step(minutes=2)  # past the 300 s entry window: no executor can be created for that claim
        self.assertEqual((s.grants(), self.claims(s, BTC)), ((), ()))
        self.assertNotIn(create.executor_config.id, s.controller.contexts[BTC].runtime.pending)
        await self.release_foreign(s, spent[0])
        receipt = await s.step(rsi={BTC: rebound()})
        [retry] = creates(receipt, BTC)
        self.assertNotEqual(retry.executor_config.id, create.executor_config.id)
        self.assertEqual([e.config.id for e in s.executors(BTC)], [retry.executor_config.id])

    async def test_cash_taken_after_creation_ends_the_entry_without_an_order(self):
        s = self.scenario()
        await s.step()
        spent = self.contend(s, after_create=True)
        receipt = await s.step(rsi={BTC: rebound()})
        [create] = creates(receipt, BTC)
        [executor] = s.executors(BTC)
        self.assertIsNone(executor._open_order)  # funds re-checked at submission: never sent
        await s.step()
        self.assertTrue(executor.is_closed)
        self.assertEqual((executor.open_filled_amount, s.balance('BTC')), (D('0'), D('0')))
        self.assertEqual(s.grants(), ())
        await self.release_foreign(s, spent[0])
        receipt = await s.step(rsi={BTC: rebound()})
        self.assertEqual(len(creates(receipt, BTC)), 1)
        self.assertNotEqual(creates(receipt, BTC)[0].executor_config.id, create.executor_config.id)

    async def test_partial_fill_then_refused_entry_conserves_capital(self):
        s = self.scenario()
        executor = await self.entered(s, ETH, amount='.05')
        await s.step(minutes=6)  # the remainder expires; the partial stays managed and reserved
        await s.step()  # the native cancel acknowledgement is reconciled one step later
        self.assertTrue(executor._entry_terminal_confirmed())
        entry = executor._open_order.order
        fills = list(entry.order_fills.values())
        self.assertTrue(all(f.fee.percent == 0 and {t.token for t in f.fee.flat_fees} == {'USDC'} for f in fills))
        spent = entry.executed_amount_quote + sum((t.amount for f in fills for t in f.fee.flat_fees), D('0'))
        self.assertEqual(D('500') - s.balance('USDC'), spent)
        [partial] = s.grants()
        self.assertGreaterEqual(partial.bound_quote, spent)
        balances = s.balance('USDC'), s.balance('ETH')
        foreign = self.foreign_bid(s, quote='480')
        receipt = await s.step(rsi={BTC: rebound()})  # another symbol's entry the wallet cannot fund
        self.assertEqual(creates(receipt), [])
        self.assertEqual(s.grants(), (partial,))  # the partial keeps exactly its own reservation
        self.assertEqual((self.claims(s, ETH), self.claims(s, BTC)), ((executor.config.id,), ()))
        self.assertEqual((s.balance('USDC'), s.balance('ETH')), balances)
        portfolio = s.controller.portfolio
        portfolio.begin_tick()
        snapshot = portfolio.snapshot(portfolio.now_ms())
        [eth] = [a for a in snapshot.assets if a.symbol == ETH]
        self.assertEqual((snapshot.commitments, eth.quantity, eth.basis_quote),
                         ((), executor.open_filled_amount, entry.executed_amount_quote))  # no unfilled remainder counted
        self.assertTrue(executor.is_active)
        self.assertEqual(s.held(ETH), D('0'))
        await self.release_foreign(s, foreign)
        receipt = await s.step(rsi={BTC: rebound()})
        self.assertEqual(len(creates(receipt, BTC)), 1)
        self.assertEqual({g.symbol for g in s.grants()}, {ETH, BTC})

    # ------------------------------------------------------------------ per-symbol freshness
    async def test_stale_symbol_holds_acquisitions_but_never_another_symbols_trailing(self):
        s = self.scenario()
        executor = await self.entered(s, BTC)
        receipt = await s.step(mids={BTC: '104.5'}, rsi={ETH: rebound()}, stale=(XRP,))
        reasons = receipt['readiness']['reasons']
        self.assertIn('fresh_book_required', reasons)
        self.assertIn(f'{OWNER}: stale_symbols_held:{XRP}', reasons)
        self.assertEqual(creates(receipt), [])  # the portfolio cannot mark XRP: no new acquisition anywhere
        self.assertEqual(s.controller.portfolio.status['reason'], NATIVE_SNAPSHOT_UNAVAILABLE)
        self.assertTrue(self.reason(s, XRP).startswith('data_hold:'), self.reason(s, XRP))
        self.assertIsNotNone(executor._trailing_stop_trigger_pct)  # BTC's own book is fresh: it is managed
        for mid in ('105.5', '104.9'):
            await s.step(mids={BTC: mid}, stale=(XRP,))
        await s.lift(BTC, stale=(XRP,))
        self.assertEqual(executor.close_type, CloseType.TRAILING_STOP)
        self.assertEqual(s.balance('BTC'), D('0'))
        await s.step(stale=(XRP,))
        self.assertEqual(s.grants(), ())
        receipt = await s.step(rsi={ETH: rebound()})  # XRP fresh again: acquisitions resume
        self.assertEqual(len(creates(receipt, ETH)), 1)

    async def test_stale_symbols_own_executor_never_acts_on_its_stale_book(self):
        s = self.scenario()
        await s.step()
        receipt = await s.step(rsi={BTC: rebound(), XRP: rebound()})
        self.assertEqual({a.executor_config.trading_pair for a in creates(receipt)}, {BTC, XRP})
        await s.fill(BTC)
        await s.fill(XRP)
        [btc], [xrp] = s.executors(BTC), s.executors(XRP)
        for mid in ('104.5', '105.5'):
            receipt = await s.step(mids={BTC: mid, XRP: mid}, stale=(XRP,))
            self.assertIn(f'reported_at_last_observed_book:{XRP}', receipt['readiness']['reasons'])
            self.assertEqual([a for a in receipt['actions'] if getattr(a, 'executor_config', None) is not None
                              and a.executor_config.trading_pair == XRP], [])
        self.assertIsNotNone(btc._trailing_stop_trigger_pct)
        self.assertIsNone(xrp._trailing_stop_trigger_pct)  # never saw an unpublished price
        self.assertTrue(xrp.is_active)
        self.assertEqual(len([g for g in s.grants() if g.symbol == XRP]), 1)
        self.assertEqual({p.trading_pair for p in s.session.recorder.get_positions_by_controller(OWNER)
                          if D(str(p.amount)) > 0}, set())
        await s.step(mids={XRP: '105.5'})  # fresh again: the same executor resumes from the observed book
        self.assertIsNotNone(xrp._trailing_stop_trigger_pct)

    async def test_stale_symbol_never_stops_another_symbols_bag_recovery(self):
        s = self.scenario()
        executor = await self.entered(s, BTC)
        amount = executor.open_filled_amount
        await s.step(mids={BTC: '96.9'}, stale=(XRP,))
        await s.step(mids={BTC: '96.8'}, stale=(XRP,))
        self.assertEqual(s.held(BTC), amount)  # the bag stop is BTC management, not an acquisition
        for mid in ('101', '103', '104'):
            await s.step(mids={BTC: mid}, rsi={BTC: (80, 80)}, stale=(XRP,))
        self.assertTrue(self.bag(s, BTC)['trail_armed'])
        receipt = await s.step(mids={BTC: '103.05'}, rsi={BTC: (80, 80)}, stale=(XRP,))
        [sell] = creates(receipt, BTC)
        self.assertEqual((sell.executor_config.side, sell.executor_config.amount), (TradeType.SELL, amount))

    async def test_a_stale_symbol_leaves_a_healthy_symbols_trace_unchanged(self):
        async def trace(stale):
            s = self.scenario(directory=self.directory / ('stale' if stale else 'fresh'))
            executor = await self.entered(s, BTC)
            steps = []
            for mid in ('102', '104.5', '105.5', '104.9'):
                receipt = await s.step(mids={BTC: mid}, stale=stale)
                steps.append((mid, [(type(a).__name__, getattr(a, 'executor_id', None)) for a in receipt['actions']],
                              executor._trailing_stop_trigger_pct))
            await s.lift(BTC, stale=stale)
            close = executor._close_order.order
            result = (executor.config.id, steps, executor.close_type, close.price, close.executed_amount_base,
                      s.balance('USDC'), s.balance('BTC'))
            await s.close()  # one native recorder per process
            return result
        self.assertEqual(await trace(()), await trace((XRP,)))

    # ------------------------------------------------------------------ decision-loop reads
    async def test_shared_executor_reads_decide_the_same_tape_with_one_read_per_decision(self):
        async def tape(shared):
            s = self.scenario(directory=self.directory / ('shared' if shared else 'unshared'))
            recorder = s.session.recorder
            recorder._executor_reads = None  # the live read path: no process-lifetime paper cache
            if not shared:
                recorder.shared_executor_reads = nullcontext
            reads, decide = [], s.controller.determine_executor_actions
            selects = {'n': 0}

            def count(conn, cursor, text, *args):
                selects['n'] += text.lstrip().upper().startswith('SELECT') and 'FROM "Executors"' in text

            def measured():
                before = selects['n']
                try:
                    return decide()
                finally:
                    reads.append(selects['n'] - before)
            event.listen(recorder.sql_manager.engine, 'before_cursor_execute', count)
            s.controller.determine_executor_actions = measured
            trace = []

            async def step(**kwargs):
                receipt = await s.step(**kwargs)
                trace.append([(type(a).__name__, getattr(a, 'executor_id', None), getattr(a, 'keep_position', None),
                               *((a.executor_config.id, a.executor_config.trading_pair, str(a.executor_config.amount))
                                 if isinstance(a, CreateExecutorAction) else ())) for a in receipt['actions']])
            await step()
            await step(rsi={BTC: rebound(), ETH: rebound()})
            await s.fill(BTC)
            await s.fill(ETH)
            await step(mids={ETH: '99'}, rsi={ETH: rebound()})
            await s.fill(ETH)
            for btc, eth in (('104.5', '96.9'), ('105.5', '96.8'), ('104.9', '101'), ('104.9', '103'), ('104.9', '104')):
                await step(mids={BTC: btc, ETH: eth}, rsi={ETH: (80, 80)})
            await s.lift(BTC)
            await step(mids={ETH: '103.05'}, rsi={ETH: (80, 80)})
            await s.lift(ETH, amount='5', above='0.5')
            await step()
            result = (trace, [(g.symbol, g.status, g.executor_id, g.bound_quote) for g in s.grants()],
                      {asset: s.balance(asset) for asset in ('USDC', 'BTC', 'ETH')}, self.bag(s, ETH))
            await s.close()
            return result, reads
        (shared, reads), (unshared, unshared_reads) = await tape(True), await tape(False)
        self.assertEqual(shared, unshared)
        self.assertTrue(any(trace for trace in shared[0]))
        self.assertEqual(max(reads), 1)
        self.assertGreater(max(unshared_reads), 10)

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
