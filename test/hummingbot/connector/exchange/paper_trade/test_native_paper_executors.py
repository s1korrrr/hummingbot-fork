"""Native executors, event forwarding and disposable SQL recorder; no OMS doubles."""
import tempfile
from pathlib import Path
from test.hummingbot.connector.exchange.paper_trade.test_native_paper_trade_exchange import PAIR, D, paper
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


class PaperStrategyBridge:
    """Only the StrategyV2Base placement surface; native connector owns all state."""

    def __init__(self, market):
        self.connectors = {market.name: market}
        self.market = market

    @property
    def current_timestamp(self):
        return self.market.current_timestamp

    def buy(self, connector_name, trading_pair, amount, order_type, price, position_action):
        return self.connectors[connector_name].buy(trading_pair, amount, order_type, price,
                                                   position_action=position_action)

    def sell(self, connector_name, trading_pair, amount, order_type, price, position_action):
        return self.connectors[connector_name].sell(trading_pair, amount, order_type, price,
                                                    position_action=position_action)

    def cancel(self, connector_name, trading_pair, order_id):
        return self.connectors[connector_name].cancel(trading_pair, order_id)


class NativePaperExecutorTests(IsolatedAsyncioTestCase):
    def setUp(self):
        for target in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.socket.sendto',
                       'socket.getaddrinfo'):
            guard = patch(target, side_effect=AssertionError('network forbidden in native paper fixture'))
            guard.start()
            self.addCleanup(guard.stop)

    async def test_position_partial_entry_cancel_then_native_market_protective_exit(self):
        market = paper()
        strategy = PaperStrategyBridge(market)
        executor = PositionExecutor(strategy, PositionExecutorConfig(
            id='paper-position', controller_id='paper-test', timestamp=1000,
            connector_name=market.name, trading_pair=PAIR, side=TradeType.BUY,
            amount=D('1'), entry_price=D('99'),
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.LIMIT,
                                                      stop_loss=D('.05'))))
        executor.register_events()
        self.addCleanup(executor.unregister_events)
        executor._status = RunnableStatus.RUNNING
        await executor.validate_sufficient_balance()
        await executor.control_task()
        await market.drain()
        self.assertIs(executor._open_order.order, market._order_tracker.fetch_order(executor._open_order.order_id))
        market.tick(1001)
        await market.process_trade(PAIR, 1, 1001, TradeType.SELL, D('98'), D('.4'))
        self.assertEqual(executor.open_filled_amount, D('.4'))
        entry_order = executor._open_order.order
        executor.place_close_order_and_cancel_open_orders(CloseType.STOP_LOSS)
        await market.drain()
        executor.place_close_order_and_cancel_open_orders(CloseType.STOP_LOSS)
        await market.drain()
        self.assertEqual(executor.close_filled_amount, D('.4'))
        self.assertEqual(market.get_balance('BTC'), D('0'))
        self.assertEqual(market.get_balance('USDC'), D('999.8812'))
        self.assertTrue(entry_order.is_cancelled)
        self.assertEqual(executor._close_order.order.order_type, OrderType.MARKET)
        await market.process_trade(PAIR, 2, 1001, TradeType.SELL, D('98'), D('1'))
        self.assertEqual(executor.open_filled_amount, D('.4'))
        self.assertEqual(executor.close_filled_amount, D('.4'))

    async def test_order_executor_reduces_native_inventory_in_partial_fills(self):
        market = paper()
        market.set_balance('BTC', D('1'))
        executor = OrderExecutor(PaperStrategyBridge(market), OrderExecutorConfig(
            id='paper-reduce', controller_id='paper-test', timestamp=1000,
            connector_name=market.name, trading_pair=PAIR, side=TradeType.SELL,
            amount=D('1'), price=D('102'), execution_strategy=ExecutionStrategy.LIMIT,
            position_action=PositionAction.CLOSE))
        executor.register_events()
        self.addCleanup(executor.unregister_events)
        executor._status = RunnableStatus.RUNNING
        await executor.validate_sufficient_balance()
        await executor.control_task()
        await market.drain()
        market.tick(1001)
        await market.process_trade(PAIR, 1, 1001, TradeType.BUY, D('103'), D('.25'))
        self.assertEqual(executor._order.executed_amount_base, D('.25'))
        self.assertEqual(market.get_balance('BTC'), D('.75'))
        await market.process_trade(PAIR, 2, 1001, TradeType.BUY, D('103'), D('.75'))
        self.assertEqual(executor._order.executed_amount_base, D('1'))
        self.assertEqual(market.get_balance('USDC'), D('1101.898'))
        self.assertEqual(market.get_balance('BTC'), D('0'))
        self.assertTrue(executor._order.order.is_filled)

    async def test_real_recorder_restart_preserves_fills_cash_and_remaining_order(self):
        market = paper()
        previous_recorder = MarketsRecorder._shared_instance
        self.addCleanup(setattr, MarketsRecorder, '_shared_instance', previous_recorder)
        with tempfile.TemporaryDirectory(prefix='native-paper-recorder-') as directory:
            manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                           db_path=str(Path(directory) / 'paper.sqlite'))
            recorder = MarketsRecorder(manager, [market], 'native-paper-fixture', 'v2_with_controllers',
                                       MarketDataCollectionConfigMap(market_data_collection_enabled=False))
            recorder.start()
            try:
                oid = market.buy(PAIR, D('1'), OrderType.LIMIT, D('99'))
                await market.drain()
                market.tick(1001)
                await market.process_trade(PAIR, 1, 1001, TradeType.SELL, D('98'), D('.4'))
                trades = recorder.get_trades_for_config('native-paper-fixture')
                self.assertEqual(len(trades), 1)
                self.assertEqual(D(str(trades[0].amount)), D('.4'))
                restored = paper()
                recorder.restore_market_states('native-paper-fixture', restored)
                self.assertFalse(restored.ready)
                restored.set_order_book(PAIR, 2, 1001, [(99, 10)], [(101, 10)])
                self.assertEqual(restored.get_balance('BTC'), D('.4'))
                self.assertEqual(restored.get_balance('USDC'), D('960.3604'))
                self.assertEqual(restored._order_tracker.fetch_order(oid).executed_amount_base, D('.4'))
                await restored.process_trade(PAIR, 1, 1001, TradeType.SELL, D('98'), D('.4'))
                self.assertEqual(restored.get_balance('BTC'), D('.4'))
                await restored.process_trade(PAIR, 2, 1001, TradeType.SELL, D('98'), D('.6'))
                self.assertEqual(restored.get_balance('BTC'), D('1'))
                self.assertTrue(restored._order_tracker.fetch_order(oid).is_filled)
            finally:
                recorder.stop()
                manager.engine.dispose()

    async def test_native_single_attempt_expiry_reconciles_terminal_partial_fill(self):
        market = paper()
        executor = PositionExecutor(PaperStrategyBridge(market), PositionExecutorConfig(
            id='paper-expiry', timestamp=1000, connector_name=market.name, trading_pair=PAIR,
            side=TradeType.BUY, amount=D('1'), entry_price=D('99'), entry_order_policy='single_attempt',
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.LIMIT,
                                                      open_order_limit_max_age_seconds=1)))
        executor.register_events()
        self.addCleanup(executor.unregister_events)
        executor._status = RunnableStatus.RUNNING
        await executor.control_task()
        await market.drain()
        market.tick(1001)
        await market.process_trade(PAIR, 1, 1001, TradeType.SELL, D('98'), D('.2'))
        await executor.control_task()
        await market.drain()
        await executor.control_task()
        task = executor._entry_expiry_reconciliation_task
        if task is not None:
            await task
        await executor.control_task()
        self.assertTrue(executor._open_order_expiry_reconciled)
        self.assertEqual(executor.open_filled_amount, D('.2'))
        self.assertEqual(market.get_balance('BTC'), D('.2'))
        self.assertEqual(market.in_flight_orders, {})
        await market.process_trade(PAIR, 2, 1001, TradeType.SELL, D('98'), D('1'))
        self.assertEqual(executor.open_filled_amount, D('.2'))
        self.assertEqual(len(executor._entry_orders()), 1)

    async def test_unfilled_single_attempt_expires_after_tracker_cache_eviction(self):
        market = paper()
        executor = PositionExecutor(PaperStrategyBridge(market), PositionExecutorConfig(
            id='paper-empty-expiry', timestamp=1000, connector_name=market.name, trading_pair=PAIR,
            side=TradeType.BUY, amount=D('1'), entry_price=D('99'), entry_order_policy='single_attempt',
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.LIMIT,
                                                      open_order_limit_max_age_seconds=1)))
        executor.register_events()
        self.addCleanup(executor.unregister_events)
        executor._status = RunnableStatus.RUNNING
        await executor.control_task()
        await market.drain()
        market.tick(1002)
        await executor.control_task()
        await market.drain()
        self.assertTrue(executor._open_order.order.is_cancelled)
        self.assertEqual(executor.open_filled_amount, D('0'))
        market._order_tracker._cached_orders.clear()
        await executor.control_task()
        task = executor._entry_expiry_reconciliation_task
        if task is not None:
            await task
        await executor.control_task()
        self.assertTrue(executor._open_order_expiry_reconciled)
        self.assertEqual(executor.close_type, CloseType.EXPIRED)
        self.assertTrue(executor.is_closed)

    async def test_orchestrator_default_start_and_explicit_scheduler_keep_native_class(self):
        from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
        from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
        from hummingbot.strategy_v2.paper_session import InjectedPaperMarketData
        previous = MarketsRecorder._shared_instance
        self.addCleanup(setattr, MarketsRecorder, '_shared_instance', previous)
        with tempfile.TemporaryDirectory() as directory:
            market = paper()
            manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                           db_path=str(Path(directory) / 'paper.sqlite'))
            MarketsRecorder(manager, [market], 'scheduler-fixture', 'v2_with_controllers',
                            MarketDataCollectionConfigMap(market_data_collection_enabled=False))
            strategy = PaperStrategyBridge(market)
            strategy.controllers = {}
            strategy.markets = {market.name: {PAIR}}
            strategy.market_data_provider = InjectedPaperMarketData(market)
            try:
                cfg = PositionExecutorConfig(id='default-start', timestamp=1000, connector_name=market.name,
                                             trading_pair=PAIR, side=TradeType.BUY, amount=D('1'), entry_price=D('99'))
                with patch.object(PositionExecutor, 'start') as start:
                    default = ExecutorOrchestrator(strategy)
                    default.execute_action(CreateExecutorAction(controller_id='owner', executor_config=cfg))
                    start.assert_called_once_with()
                scheduled = []
                stepped = ExecutorOrchestrator(strategy, executor_start=scheduled.append)
                cfg = cfg.model_copy(update={'id': 'callback-start'})
                with patch.object(PositionExecutor, 'start') as start:
                    stepped.execute_action(CreateExecutorAction(controller_id='owner', executor_config=cfg))
                    start.assert_not_called()
                self.assertEqual(len(scheduled), 1)
                self.assertIs(type(scheduled[0]), PositionExecutor)
                self.assertIs(scheduled[0].config, cfg)
            finally:
                manager.engine.dispose()
