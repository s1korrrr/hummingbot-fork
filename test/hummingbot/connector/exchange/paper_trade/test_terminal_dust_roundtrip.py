"""Real paper fills survive native recorder reload as attributed terminal dust."""
import asyncio
import tempfile
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace as NS
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import (
    DownOnlyMakerPositionExecutor,
    DownOnlyMakerPositionExecutorConfig,
)
from controllers.generic.meridian_v3_unified_core.portfolio import MeridianPortfolio
from controllers.generic.modular_spot_core.inventory import inventory_view
from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType

from .test_native_paper_executors import PaperStrategyBridge

PAIR, OWNER = 'BTC-USDC', 'paper-dust-owner'


class NativeTerminalDustRoundtripTests(IsolatedAsyncioTestCase):
    def setUp(self):
        for target in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.socket.sendto', 'socket.getaddrinfo'):
            guard = patch(target, side_effect=AssertionError('network forbidden in native dust fixture'))
            guard.start()
            self.addCleanup(guard.stop)
        previous = MarketsRecorder._shared_instance
        self.addCleanup(setattr, MarketsRecorder, '_shared_instance', previous)
        oracle = RateOracle()
        oracle.set_price(PAIR, D('100'))
        previous_oracle = RateOracle._shared_instance
        RateOracle._shared_instance = oracle
        self.addCleanup(setattr, RateOracle, '_shared_instance', previous_oracle)

    async def test_two_completed_base_fee_exits_reload_with_exact_inventory_and_no_sell_authority(self):
        rule = TradingRule(PAIR, min_order_size=D('.01'), min_price_increment=D('.01'),
                           min_base_amount_increment=D('.01'), min_notional_size=D('1'))
        fee_schema = TradeFeeSchema(
            maker_percent_fee_decimal=D('.001'), taker_percent_fee_decimal=D('.002'),
            buy_percent_fee_deducted_from_returns=True)
        market = NativePaperTradeExchange({PAIR: rule}, fee_schema=fee_schema, trade_participation=D('1'))
        market.set_balance('BTC', D('0'))
        market.set_balance('USDC', D('1000'))
        market.tick(1000)
        market.set_order_book(PAIR, 1, 1000, [(D('99'), D('10'))], [(D('101'), D('10'))])
        with tempfile.TemporaryDirectory(prefix='native-terminal-dust-') as directory:
            manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                           db_path=str(Path(directory) / 'paper.sqlite'))
            recorder = MarketsRecorder(manager, [market], 'native-terminal-dust', 'v2_with_controllers',
                                       MarketDataCollectionConfigMap(market_data_collection_enabled=False))
            recorder.start()
            try:
                for index in range(2):
                    now = 1000 + index * 5
                    market.tick(now)
                    market.set_order_book(PAIR, index + 2, now, [(99, 10)], [(101, 10)])
                    strategy = PaperStrategyBridge(market)
                    strategy.controllers = {OWNER: NS(
                        config=NS(manual_kill_switch=False), _operator_entry_paused=False,
                        status=RunnableStatus.RUNNING, processed_data={'signal': 1, 'data_health': {'ready': True}})}
                    executor = DownOnlyMakerPositionExecutor(strategy, DownOnlyMakerPositionExecutorConfig(
                        id=f'paper-dust-{index}', controller_id=OWNER, timestamp=now,
                        connector_name=market.name, trading_pair=PAIR, side=TradeType.BUY,
                        level_id='modular_spot:v1:terminal-proof', amount=D('1'), entry_price=D('99'),
                        entry_order_policy='single_attempt', activation_bounds=[D('.05')],
                        decision_best_bid=D('99'), decision_best_ask=D('101'), decision_atr_1m=D('1'),
                        decision_price_step=D('.01'), maximum_acceptable_price=D('99'),
                        entry_total_max_age_seconds=300, entry_max_downward_reprices=3,
                        entry_reprice_cooldown_seconds=5, entry_reprice_min_ticks=3,
                        entry_reprice_atr_fraction=D('.05'),
                        triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.LIMIT_MAKER,
                                                                  stop_loss=D('.05'))))
                    executor.register_events()
                    try:
                        executor._status = RunnableStatus.RUNNING
                        await executor.validate_sufficient_balance()
                        await executor.control_task()
                        await market.drain()
                        market.tick(now + 1)
                        await market.process_trade(PAIR, index + 1, now + 1, TradeType.SELL, D('98'), D('1'))
                        self.assertEqual(executor.open_filled_amount, D('.999'))
                        await executor.control_task()
                        market.tick(now + 2)
                        executor.place_close_order_and_cancel_open_orders(CloseType.STOP_LOSS)
                        await market.drain()
                        await executor.control_shutdown_process()
                        self.assertTrue(executor.is_closed)
                        self.assertEqual(executor.get_custom_info()['execution_receipt']['residual_dust_base'], D('.009'))
                        recorder.store_or_update_executor(executor)
                    finally:
                        executor.unregister_events()

                self.assertEqual(market.get_balance('BTC'), D('.018'))
                recorder.stop()
                # Both projections read genuine reloaded ExecutorInfo and detached TradeFill rows.
                history = recorder.get_executors_by_controller(OWNER)
                self.assertEqual(len(history), 2)
                self.assertIs(type(history[0].side), int)
                provider = MagicMock()
                provider.time.return_value = market.current_timestamp
                provider.get_trading_rules.return_value = rule
                provider.get_price_by_type.return_value = D('100')
                controller = OKRSIController(OKRSIControllerConfig(
                    id=OWNER, connector_name=market.name, trading_pair=PAIR, strict_hmm_mode=False),
                    provider, asyncio.Queue())
                controller.positions_held = []
                controller.executors_info = []  # historical dust is outside the bounded recent report
                with self.subTest(owner='V1'):
                    self.assertEqual(controller._get_total_position_value_usd(), D('1.8'))
                    self.assertEqual(controller._buy_executors_for_capital(), [])

                def views():
                    return inventory_view(history, [], controller_id=OWNER, connector=market.name,
                                          pair=PAIR, bag_mode=True)
                runtime = NS(inventory=views, bindings=NS(quote_observation=NS(
                    event_timestamp=market.current_timestamp, applied_timestamp=market.current_timestamp)),
                    snapshot=lambda: dict(available_quote=market.get_available_balance('USDC'),
                                          amount_step=D('.01'), minimum_amount=D('.01'),
                                          minimum_notional=D('1'), bid=D('99'), ask=D('101')))
                portfolio = object.__new__(MeridianPortfolio)
                portfolio.owner = NS(config=NS(risk=NS(fee_reserve_fraction=D('.003')),
                                               connector_name=market.name, account_id=OWNER, execution_mode='paper',
                                               symbols=[NS(trading_pair=PAIR, entry_enabled=True)]), operator_entry_paused=False,
                                     contexts={PAIR: NS(runtime=runtime, positions_held=[])})
                portfolio.clock, portfolio.status, portfolio.source_sha256 = {}, {}, 'a' * 64
                portfolio.recipe = NS(mandate=NS(pairs=(PAIR,), reference_quote=D('1000'), identity='b' * 64))
                portfolio.recorder = lambda: recorder
                portfolio.evidence = lambda: history
                portfolio.store = lambda: NS(grants=lambda **_: ())
                portfolio.target_store = lambda: NS(head=lambda: None)
                snapshot = portfolio.snapshot(int(market.current_timestamp * 1000))
                with self.subTest(owner='V3'):
                    self.assertTrue(snapshot.reconciled)
                    self.assertEqual(snapshot.assets[0].quantity, D('.018'))
                    # Two 99 USDC acquisitions receive .999 BTC each after base fees;
                    # each exit leaves .009 BTC, retaining its fraction of paid cost.
                    expected_basis = 2 * D('99') * D('.009') / D('.999')
                    self.assertLess(abs(snapshot.assets[0].basis_quote - expected_basis), D('1e-24'))
                    self.assertEqual(snapshot.assets[0].sellable_quantity, D('0'))
                    self.assertFalse(snapshot.assets[0].reduction_allowed)
                    self.assertEqual(snapshot.commitments, ())
                    self.assertEqual(portfolio.snapshot(int(market.current_timestamp * 1000)), snapshot)
                self.assertEqual(len(recorder.get_trades_for_config('native-terminal-dust')), 4)

                restored = NativePaperTradeExchange({PAIR: rule}, fee_schema=fee_schema,
                                                    trade_participation=D('1'))
                recorder.restore_market_states('native-terminal-dust', restored)
                self.assertFalse(restored.ready)
                self.assertEqual(restored.get_balance('BTC'), D('.018'))
                self.assertEqual(restored.get_balance('USDC'), market.get_balance('USDC'))
                restored.tick(market.current_timestamp)
                restored.set_order_book(PAIR, 4, market.current_timestamp, [(99, 10)], [(101, 10)])
                await restored.process_trade(PAIR, 2, market.current_timestamp, TradeType.SELL, D('98'), D('1'))
                self.assertEqual(restored.get_balance('BTC'), D('.018'))
                self.assertEqual(len(recorder.get_trades_for_config('native-terminal-dust')), 4)
            finally:
                recorder.stop()
                manager.engine.dispose()
