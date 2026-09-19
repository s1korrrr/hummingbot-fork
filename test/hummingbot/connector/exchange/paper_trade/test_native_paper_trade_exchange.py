import json
from decimal import Decimal as D
from unittest import IsolatedAsyncioTestCase

from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.core.event.events import OrderFilledEvent

PAIR = 'BTC-USDC'


def paper():
    book = OrderBook()
    book.apply_snapshot([OrderBookRow(99, 10, 1)], [OrderBookRow(101, 10, 1)], 1)
    rule = TradingRule(PAIR, min_order_size=D('.01'), min_price_increment=D('.01'),
                       min_base_amount_increment=D('.01'), min_notional_size=D('1'))
    result = NativePaperTradeExchange({PAIR: rule}, fee_schema=TradeFeeSchema(maker_percent_fee_decimal=D('.001'), taker_percent_fee_decimal=D('.002')), trade_participation=D('1'))
    result.set_balance('BTC', D('0'))
    result.set_balance('USDC', D('1000'))
    result.tick(1000)
    result.set_order_book(PAIR, 1, 1000, [(D('99'), D('10'))], [(D('101'), D('10'))])
    return result


class NativePaperTradeTests(IsolatedAsyncioTestCase):
    async def test_partial_fill_charges_fee_and_reserves_remaining_cash(self):
        market = paper()
        oid = market.buy(PAIR, D('1'), OrderType.LIMIT, D('99'))
        await market.drain()
        market.tick(1001)
        await market.process_trade(PAIR, 1, 1001, TradeType.SELL, D('98'), D('.1'))
        order = market._order_tracker.fetch_order(oid)
        self.assertEqual(order.executed_amount_base, D('.1'))
        self.assertEqual(market.get_balance('BTC'), D('.1'))
        self.assertEqual(market.get_balance('USDC'), D('990.0901'))
        self.assertEqual(market.get_available_balance('USDC'), D('900.8119'))
        fills = [e for e in market.event_logs if isinstance(e, OrderFilledEvent)]
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].trade_fee.percent, D('0'))
        self.assertEqual(fills[0].trade_fee.flat_fees[0].token, 'USDC')
        self.assertEqual(fills[0].trade_fee.flat_fees[0].amount, D('.0099'))

    async def test_print_volume_shared_by_orders_and_duplicate_ignored(self):
        market = paper()
        a = market.buy(PAIR, D('1'), OrderType.LIMIT, D('99'))
        b = market.buy(PAIR, D('1'), OrderType.LIMIT, D('98'))
        await market.drain()
        market.tick(1001)
        await market.process_trade(PAIR, 1, 1001, TradeType.SELL, D('97'), D('1.2'))
        self.assertEqual(market._order_tracker.fetch_order(a).executed_amount_base, D('1'))
        self.assertEqual(market._order_tracker.fetch_order(b).executed_amount_base, D('.2'))
        balance = market.get_balance('BTC')
        market.tick(1001)
        await market.process_trade(PAIR, 1, 1001, TradeType.SELL, D('97'), D('1.2'))
        self.assertEqual(market.get_balance('BTC'), balance)

    async def test_cancel_retains_partial_fill_and_restore_deduplicates(self):
        market = paper()
        oid = market.buy(PAIR, D('1'), OrderType.LIMIT, D('99'))
        await market.drain()
        market.tick(1001)
        await market.process_trade(PAIR, 1, 1001, TradeType.SELL, D('98'), D('.2'))
        restored = paper()
        restored.restore_tracking_states(json.loads(json.dumps(market.tracking_states)))
        restored.set_order_book(PAIR, 2, 1001, [(99, 10)], [(101, 10)])
        await restored.process_trade(PAIR, 1, 1001, TradeType.SELL, D('98'), D('.2'))
        self.assertEqual(restored.get_balance('BTC'), D('.2'))
        restored.cancel(PAIR, oid)
        await restored.drain()
        restored.tick(1002)
        await restored.process_trade(PAIR, 2, 1002, TradeType.SELL, D('98'), D('1'))
        self.assertEqual(restored.get_balance('BTC'), D('.2'))
        self.assertTrue(restored._order_tracker.fetch_order(oid).is_cancelled)
        self.assertEqual(restored.get_available_balance('USDC'), restored.get_balance('USDC'))

    async def test_no_overcommit_invalid_inputs_and_market_rejected(self):
        market = paper()
        with self.assertRaises(ValueError):
            market.buy(PAIR, D('1'), OrderType.AMM_SWAP)
        with self.assertRaises(ValueError):
            market.buy(PAIR, D('11'), OrderType.LIMIT, D('99'))
        with self.assertRaises(ValueError):
            market.sell(PAIR, D('1'), OrderType.LIMIT, D('102'))
        with self.assertRaises(ValueError):
            market.buy(PAIR, D('1'), OrderType.LIMIT_MAKER, D('101'))
        with self.assertRaises(ValueError):
            market.buy(PAIR, D('NaN'), OrderType.LIMIT, D('99'))
        market.buy(PAIR, D('10'), OrderType.LIMIT, D('99'))
        with self.assertRaises(ValueError):
            market.buy(PAIR, D('1'), OrderType.LIMIT, D('99'))
        await market.drain()

    async def test_restore_corruption_is_atomic(self):
        market = paper()
        market.buy(PAIR, D('1'), OrderType.LIMIT, D('99'))
        await market.drain()
        state = market.tracking_states
        state['balances']['USDC'] = '-1'
        target = paper()
        with self.assertRaises(ValueError):
            target.restore_tracking_states(state)
        self.assertEqual(target.get_balance('USDC'), D('1000'))
        self.assertEqual(target.in_flight_orders, {})

    async def test_market_consumes_depth_partial_ioc_and_no_reuse(self):
        market = paper()
        market.set_order_book(PAIR, 2, 1000, [(D('99'), D('.3'))], [(D('101'), D('.2')), (D('102'), D('.3'))])
        oid = market.buy(PAIR, D('1'), OrderType.MARKET)
        await market.drain()
        order = market._order_tracker.fetch_order(oid)
        self.assertEqual(order.executed_amount_base, D('.5'))
        self.assertEqual(order.executed_amount_quote, D('50.8'))
        self.assertTrue(order.is_cancelled)
        self.assertEqual(market.get_balance('USDC'), D('949.0984'))
        with self.assertRaises(ValueError):
            market.buy(PAIR, D('.1'), OrderType.MARKET)
        with self.assertRaises(ValueError):
            market.set_order_book(PAIR, 2, 1000, [(99, 1)], [(101, 1)])

    async def test_native_buy_base_fee_and_sell_quote_fee(self):
        market = paper()
        rules = market.trading_rules
        market = NativePaperTradeExchange(rules, fee_schema=TradeFeeSchema(
            maker_percent_fee_decimal=D('.001'), taker_percent_fee_decimal=D('.002'),
            buy_percent_fee_deducted_from_returns=True), trade_participation=D('1'))
        market.set_balance('USDC', D('1000'))
        market.set_balance('BTC', D('0'))
        market.tick(1000)
        market.set_order_book(PAIR, 1, 1000, [(99, 10)], [(101, 10)])
        buy = market.buy(PAIR, D('1'), OrderType.MARKET)
        await market.drain()
        self.assertEqual(market.get_balance('BTC'), D('.998'))
        self.assertEqual(market.get_balance('USDC'), D('899'))
        record = market._order_tracker.fetch_order(buy).to_json()
        from hummingbot.strategy_v2.executors.order_accounting import base_fee_amount
        self.assertEqual(base_fee_amount(record, 'BTC'), D('.002'))
        market.sell(PAIR, D('.99'), OrderType.MARKET)
        await market.drain()
        self.assertEqual(market.get_balance('BTC'), D('.008'))
        self.assertEqual(market.get_balance('USDC'), D('996.81398'))

    async def test_stale_future_inputs_restore_and_participation_bound(self):
        market = paper()
        market.buy(PAIR, D('1'), OrderType.LIMIT, D('99'))
        await market.drain()
        with self.assertRaises(ValueError):
            await market.process_trade(PAIR, 1, 1001, TradeType.SELL, D('98'), D('1'))
        market.tick(1006)
        with self.assertRaises(ValueError):
            market.buy(PAIR, D('1'), OrderType.MARKET)
        with self.assertRaises(ValueError):
            await market.process_trade(PAIR, 1, 1000, TradeType.SELL, D('98'), D('1'))
        target = paper()
        target.restore_tracking_states(market.tracking_states)
        self.assertFalse(target.ready)
        with self.assertRaises(ValueError):
            await target.process_trade(PAIR, 1, 1006, TradeType.SELL, D('98'), D('1'))
        target.set_order_book(PAIR, 2, 1006, [(99, 1)], [(101, 1)])
        self.assertTrue(target.ready)

    async def test_crossing_limit_reserves_taker_fee_before_async_fill(self):
        market = paper()
        market.set_balance('USDC', D('101.151'))
        with self.assertRaises(ValueError):
            market.buy(PAIR, D('1'), OrderType.LIMIT, D('101'))
        market.set_balance('USDC', D('202.404'))
        market.buy(PAIR, D('1'), OrderType.LIMIT, D('101'))
        market.buy(PAIR, D('1'), OrderType.LIMIT, D('101'))
        self.assertEqual(market.get_available_balance('USDC'), D('0'))
        await market.drain()
        self.assertEqual(market.get_balance('USDC'), D('0'))

    async def test_restore_validates_actual_fee_and_preserves_liquidity_role(self):
        market = paper()
        oid = market.buy(PAIR, D('1'), OrderType.LIMIT, D('99'))
        await market.drain()
        market.tick(1001)
        await market.process_trade(PAIR, 1, 1001, TradeType.SELL, D('98'), D('.1'))
        state = market.tracking_states
        restored = paper()
        restored.restore_tracking_states(state)
        fill = next(iter(restored._order_tracker.fetch_order(oid).order_fills.values()))
        self.assertFalse(fill.is_taker)
        raw_fill = next(iter(state['orders'][oid]['order_fills'].values()))
        raw_fill['fee']['flat_fees'][0]['amount'] = '-1'
        target = paper()
        with self.assertRaisesRegex(ValueError, 'actual fee'):
            target.restore_tracking_states(state)
        self.assertEqual(target.get_balance('USDC'), D('1000'))
