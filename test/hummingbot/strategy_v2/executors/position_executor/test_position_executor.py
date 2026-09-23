import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import BuyOrderCompletedEvent, MarketOrderFailureEvent, OrderCancelledEvent
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class TestPositionExecutor(IsolatedAsyncioWrapperTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.strategy = self.create_mock_strategy

    @property
    def create_mock_strategy(self):
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market

        strategy = MagicMock(spec=StrategyV2Base)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="ETH-USDT")
        type(strategy).current_timestamp = PropertyMock(return_value=1234567890)
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2", "OID-BUY-3"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2", "OID-SELL-3"]
        strategy.cancel.return_value = None
        strategy.connectors = {
            "binance": MagicMock(spec=ExchangePyBase),
        }
        strategy.connectors["binance"].get_price_by_type.return_value = Decimal("100")
        strategy.connectors["binance"].get_available_balance.return_value = Decimal("10")
        strategy.connectors["binance"].trading_rules = {"ETH-USDT": TradingRule("ETH-USDT", min_order_size=Decimal("0.01"), min_notional_size=Decimal("1"))}
        strategy.connectors["binance"]._order_tracker = MagicMock()
        strategy.connectors["binance"]._order_tracker.fetch_order.return_value = None
        return strategy

    def get_position_config_market_long(self):
        return PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                      connector_name="binance",
                                      side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1"),
                                      triple_barrier_config=TripleBarrierConfig(
                                          stop_loss=Decimal("0.05"), take_profit=Decimal("0.1"), time_limit=60,
                                          take_profit_order_type=OrderType.LIMIT,
                                          stop_loss_order_type=OrderType.MARKET))

    def get_position_config_market_long_tp_market(self):
        return PositionExecutorConfig(id="test-1", timestamp=1234567890, trading_pair="ETH-USDT",
                                      connector_name="binance",
                                      side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1"),
                                      triple_barrier_config=TripleBarrierConfig(
                                          stop_loss=Decimal("0.05"), take_profit=Decimal("0.1"), time_limit=60,
                                          take_profit_order_type=OrderType.MARKET,
                                          stop_loss_order_type=OrderType.MARKET))

    def get_position_config_market_long_with_trailing(self):
        return PositionExecutorConfig(id="test-trailing", timestamp=1234567890, trading_pair="ETH-USDT",
                                      connector_name="binance",
                                      side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1"),
                                      triple_barrier_config=TripleBarrierConfig(
                                          trailing_stop=TrailingStop(
                                              activation_price=Decimal("0.05"),
                                              trailing_delta=Decimal("0.02"),
                                          ),
                                          trailing_stop_order_type=OrderType.LIMIT,
                                          trailing_stop_limit_max_age_seconds=3,
                                          stop_loss_order_type=OrderType.MARKET,
                                          time_limit_order_type=OrderType.MARKET))

    def get_position_config_market_short(self):
        return PositionExecutorConfig(id="test-2", timestamp=1234567890, trading_pair="ETH-USDT",
                                      connector_name="binance",
                                      side=TradeType.SELL, entry_price=Decimal("100"), amount=Decimal("1"),
                                      triple_barrier_config=TripleBarrierConfig(
                                          stop_loss=Decimal("0.05"), take_profit=Decimal("0.1"), time_limit=60,
                                          take_profit_order_type=OrderType.LIMIT,
                                          stop_loss_order_type=OrderType.MARKET))

    def get_incomplete_position_config(self):
        return PositionExecutorConfig(id="test-3", timestamp=1234567890, trading_pair="ETH-USDT",
                                      connector_name="binance",
                                      side=TradeType.SELL, entry_price=Decimal("100"), amount=Decimal("1"),
                                      triple_barrier_config=TripleBarrierConfig(
                                          take_profit_order_type=OrderType.LIMIT,
                                          stop_loss_order_type=OrderType.MARKET))

    def test_properties(self):
        position_config = self.get_position_config_market_short()
        position_executor = PositionExecutor(self.strategy, position_config)
        self.assertEqual(position_executor.trade_pnl_quote, Decimal("0"))
        position_executor._status = RunnableStatus.TERMINATED
        self.assertTrue(position_executor.is_closed)
        self.assertEqual(position_executor.config.trading_pair, "ETH-USDT")
        self.assertEqual(position_executor.config.connector_name, "binance")
        self.assertEqual(position_executor.config.side, TradeType.SELL)
        self.assertEqual(position_executor.entry_price, Decimal("100"))
        self.assertEqual(position_executor.config.amount, Decimal("1"))
        self.assertEqual(position_executor.take_profit_price, Decimal("90.0"))
        self.assertEqual(position_executor.end_time, 1234567890 + 60)
        self.assertEqual(position_executor.config.triple_barrier_config.take_profit_order_type, OrderType.LIMIT)
        self.assertEqual(position_executor.config.triple_barrier_config.stop_loss_order_type, OrderType.MARKET)
        self.assertEqual(position_executor.config.triple_barrier_config.time_limit_order_type, OrderType.MARKET)
        self.assertEqual(position_executor.open_filled_amount, Decimal("0"))
        self.assertEqual(position_executor.config.triple_barrier_config.trailing_stop, None)
        self.assertIsInstance(position_executor.logger(), HummingbotLogger)

    def get_position_executor_running_from_config(self, position_config):
        position_executor = PositionExecutor(self.strategy, position_config)
        position_executor._status = RunnableStatus.RUNNING
        return position_executor

    def _filled_spot_long_executor(self):
        executor = self.get_position_executor_running_from_config(
            self.get_position_config_market_long_with_trailing()
        )
        executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT_MAKER,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED,
        )
        executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair="ETH-USDT",
                fill_price=Decimal("100"),
                fill_base_amount=Decimal("1"),
                fill_quote_amount=Decimal("100"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        return executor

    @patch.object(PositionExecutor, "get_trading_rules")
    def test_place_open_order_preserves_single_attempt_dispatch_contract(self, trading_rules_mock):
        trading_rules_mock.return_value = TradingRule(
            "ETH-USDT",
            min_order_size=Decimal("0.01"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.01"),
            min_notional_size=Decimal("1"),
        )
        config = self.get_position_config_market_long()
        config.entry_order_policy = "single_attempt"
        config.triple_barrier_config.open_order_type = OrderType.LIMIT_MAKER
        executor = self.get_position_executor_running_from_config(config)
        executor.place_order = MagicMock(return_value="entry-order")

        executor.place_open_order()
        executor.place_open_order()

        executor.place_order.assert_called_once_with(
            connector_name="binance",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT_MAKER,
            amount=Decimal("1"),
            price=Decimal("99.99"),
            side=TradeType.BUY,
            position_action=PositionAction.OPEN,
        )
        self.assertTrue(executor._entry_submitted)
        self.assertEqual(executor._open_order.order_id, "entry-order")

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch.object(PositionExecutor, "get_price")
    async def test_control_position_create_open_order(self, mock_price, trading_rules_mock):
        mock_price.return_value = Decimal("100")
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules_mock.return_value = trading_rules
        position_config = self.get_position_config_market_short()
        position_executor = self.get_position_executor_running_from_config(position_config)
        await position_executor.control_task()
        self.assertEqual(position_executor._open_order.order_id, "OID-SELL-1")

    @patch.object(PositionExecutor, "validate_sufficient_balance")
    @patch.object(PositionExecutor, "get_trading_rules")
    @patch.object(PositionExecutor, "get_price")
    async def test_control_position_not_started_expired(self, mock_price, trading_rules_mock, _):
        mock_price.return_value = Decimal("100")
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules_mock.return_value = trading_rules
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567890 + 61)
        position_config = self.get_position_config_market_short()
        position_executor = PositionExecutor(self.strategy, position_config)
        await position_executor.control_loop()
        self.assertIsNone(position_executor._open_order)
        self.assertEqual(position_executor.close_type, CloseType.EXPIRED)
        self.assertEqual(position_executor.trade_pnl_pct, Decimal("0"))

    @patch.object(PositionExecutor, "get_trading_rules")
    async def test_control_open_order_expiration(self, trading_rules_mock):
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        trading_rules_mock.return_value = trading_rules
        position_config = self.get_position_config_market_short()
        position_executor = self.get_position_executor_running_from_config(position_config)
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567890 + 61)
        self.strategy.connectors["binance"].quantize_order_amount.return_value = Decimal("0")
        position_executor._open_order = TrackedOrder(order_id="OID-SELL-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-SELL-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=position_config.triple_barrier_config.open_order_type,
            trade_type=TradeType.SELL,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1640001112.223,
            initial_state=OrderState.OPEN
        )
        await position_executor.control_task()
        position_executor._strategy.cancel.assert_called_with(
            connector_name="binance",
            trading_pair="ETH-USDT",
            order_id="OID-SELL-1")
        self.assertEqual(position_executor.trade_pnl_pct, Decimal("0"))

    @patch.object(PositionExecutor, "get_trading_rules")
    async def test_control_open_order_cancels_unfilled_stale_limit_entry(self, trading_rules_mock):
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        trading_rules_mock.return_value = trading_rules
        position_config = self.get_position_config_market_long()
        position_config.triple_barrier_config.open_order_type = OrderType.LIMIT
        position_config.triple_barrier_config.open_order_limit_max_age_seconds = 3
        position_executor = self.get_position_executor_running_from_config(position_config)
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567894)
        self.strategy.connectors["binance"].quantize_order_amount.return_value = Decimal("0")
        position_executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1234567890,
            initial_state=OrderState.OPEN,
        )

        await position_executor.control_task()

        position_executor._strategy.cancel.assert_called_with(
            connector_name="binance",
            trading_pair="ETH-USDT",
            order_id="OID-BUY-1",
        )
        self.assertIsNone(position_executor.close_type)
        self.assertEqual(position_executor.status, RunnableStatus.RUNNING)

    @patch.object(PositionExecutor, "get_trading_rules")
    async def test_control_open_order_cancels_partially_filled_stale_limit_entry(self, trading_rules_mock):
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        trading_rules_mock.return_value = trading_rules
        position_config = self.get_position_config_market_long()
        position_config.triple_barrier_config.open_order_type = OrderType.LIMIT
        position_config.triple_barrier_config.open_order_limit_max_age_seconds = 3
        position_executor = self.get_position_executor_running_from_config(position_config)
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567894)
        self.strategy.connectors["binance"].quantize_order_amount.return_value = Decimal("0.2")
        position_executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1234567890,
            initial_state=OrderState.OPEN,
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair=position_config.trading_pair,
                fill_price=position_config.entry_price,
                fill_base_amount=Decimal("0.2"),
                fill_quote_amount=Decimal("20"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.02"))]),
                fill_timestamp=1234567891,
            )
        )

        await position_executor.control_task()

        position_executor._strategy.cancel.assert_called_once()
        self.assertEqual(position_executor.status, RunnableStatus.RUNNING)

    @patch.object(PositionExecutor, "get_trading_rules")
    async def test_control_position_order_placed_not_cancel_open_order(self, trading_rules_mock):
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        trading_rules_mock.return_value = trading_rules
        position_config = self.get_position_config_market_short()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder(order_id="OID-SELL-1")
        await position_executor.control_task()
        position_executor._strategy.cancel.assert_not_called()

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("101"))
    async def test_control_position_active_position_create_take_profit(self, _, trading_rules_mock):
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        trading_rules_mock.return_value = trading_rules
        position_config = self.get_position_config_market_short()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder(order_id="OID-SELL-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-SELL-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=position_config.triple_barrier_config.open_order_type,
            trade_type=TradeType.SELL,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-SELL-1",
                exchange_order_id="EOID4",
                trading_pair=position_config.trading_pair,
                fill_price=position_config.entry_price,
                fill_base_amount=position_config.amount,
                fill_quote_amount=position_config.amount * position_config.entry_price,
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )

        self.strategy.connectors["binance"].quantize_order_amount.return_value = position_config.amount
        await position_executor.control_task()
        self.assertEqual(position_executor._take_profit_limit_order.order_id, "OID-BUY-1")
        self.assertEqual(position_executor.trade_pnl_pct, Decimal("-0.01"))

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("120"))
    async def test_control_position_active_position_close_by_take_profit_market(self, _, trading_rules_mock):
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        trading_rules_mock.return_value = trading_rules
        position_config = self.get_position_config_market_long_tp_market()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=position_config.triple_barrier_config.open_order_type,
            trade_type=TradeType.BUY,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        )

        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair=position_config.trading_pair,
                fill_price=position_config.entry_price,
                fill_base_amount=position_config.amount,
                fill_quote_amount=position_config.amount * position_config.entry_price,
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        self.strategy.connectors["binance"].quantize_order_amount.return_value = position_config.amount
        await position_executor.control_task()
        self.assertEqual(position_executor._close_order.order_id, "OID-SELL-1")
        self.assertEqual(position_executor.close_type, CloseType.TAKE_PROFIT)
        self.assertEqual(position_executor.trade_pnl_pct, Decimal("0.2"))

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("70"))
    async def test_control_position_active_position_close_by_stop_loss(self, _, trading_rules_mock):
        position_config = self.get_position_config_market_long()
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        trading_rules_mock.return_value = trading_rules
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=position_config.triple_barrier_config.open_order_type,
            trade_type=TradeType.BUY,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        )

        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair=position_config.trading_pair,
                fill_price=position_config.entry_price,
                fill_base_amount=position_config.amount,
                fill_quote_amount=position_config.amount * position_config.entry_price,
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        self.strategy.connectors["binance"].quantize_order_amount.return_value = position_config.amount
        await position_executor.control_task()
        self.assertEqual(position_executor._close_order.order_id, "OID-SELL-1")
        self.assertEqual(position_executor.close_type, CloseType.STOP_LOSS)
        self.assertEqual(position_executor.trade_pnl_pct, Decimal("-0.3"))

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("100"))
    async def test_control_position_active_position_close_by_time_limit(self, _, trading_rules_mock):
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        trading_rules_mock.return_value = trading_rules
        position_config = self.get_position_config_market_long()
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567890 + 61)
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=position_config.triple_barrier_config.open_order_type,
            trade_type=TradeType.BUY,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair=position_config.trading_pair,
                fill_price=position_config.entry_price,
                fill_base_amount=position_config.amount,
                fill_quote_amount=position_config.amount * position_config.entry_price,
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        self.strategy.connectors["binance"].quantize_order_amount.return_value = position_config.amount

        await position_executor.control_task()
        self.assertIsNone(position_executor._close_order)
        position_executor.process_order_canceled_event(
            0, None, OrderCancelledEvent(self.strategy.current_timestamp, "OID-SELL-1"))
        await position_executor.control_task()
        self.assertEqual(position_executor._close_order.order_id, "OID-SELL-2")
        self.assertEqual(position_executor.close_type, CloseType.TIME_LIMIT)
        self.assertEqual(position_executor.trade_pnl_pct, Decimal("0.0"))

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("70"))
    async def test_control_position_close_placed_stop_loss_failed(self, _, trading_rules_mock):
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        trading_rules_mock.return_value = trading_rules
        position_config = self.get_position_config_market_long()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=position_config.triple_barrier_config.open_order_type,
            trade_type=TradeType.BUY,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair=position_config.trading_pair,
                fill_price=position_config.entry_price,
                fill_base_amount=position_config.amount,
                fill_quote_amount=position_config.amount * position_config.entry_price,
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )

        position_executor._close_order = TrackedOrder("OID-SELL-FAIL")
        position_executor.close_type = CloseType.STOP_LOSS
        market = MagicMock()
        position_executor.process_order_failed_event(
            "102", market, MarketOrderFailureEvent(
                order_id="OID-SELL-FAIL",
                timestamp=1640001112.223,
                order_type=OrderType.MARKET)
        )
        self.strategy.connectors["binance"].quantize_order_amount.return_value = position_config.amount
        await position_executor.control_task()
        self.assertEqual(position_executor._close_order.order_id, "OID-SELL-1")
        self.assertEqual(position_executor.close_type, CloseType.STOP_LOSS)

    @patch.object(PositionExecutor, "get_in_flight_order")
    def test_process_order_completed_event_open_order(self, in_flight_order_mock):
        order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1640001112.223,
        )
        in_flight_order_mock.return_value = order
        position_config = self.get_position_config_market_long()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder("OID-BUY-1")
        event = BuyOrderCompletedEvent(
            timestamp=1234567890,
            order_id="OID-BUY-1",
            base_asset="ETH",
            quote_asset="USDT",
            base_asset_amount=position_config.amount,
            quote_asset_amount=position_config.amount * position_config.entry_price,
            order_type=position_config.triple_barrier_config.open_order_type,
            exchange_order_id="ED140"
        )
        market = MagicMock()
        position_executor.process_order_completed_event("102", market, event)
        self.assertEqual(position_executor._open_order.order, order)

    @patch.object(PositionExecutor, "get_in_flight_order")
    def test_process_order_completed_event_close_order(self, mock_in_flight_order):
        order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1640001112.223,
        )
        mock_in_flight_order.return_value = order
        position_config = self.get_position_config_market_long()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._close_order = TrackedOrder("OID-BUY-1")
        position_executor.close_type = CloseType.STOP_LOSS
        event = BuyOrderCompletedEvent(
            timestamp=1234567890,
            order_id="OID-BUY-1",
            base_asset="ETH",
            quote_asset="USDT",
            base_asset_amount=position_config.amount,
            quote_asset_amount=position_config.amount * position_config.entry_price,
            order_type=position_config.triple_barrier_config.open_order_type,
            exchange_order_id="ED140"
        )
        market = MagicMock()
        position_executor.process_order_completed_event("102", market, event)
        self.assertEqual(position_executor.close_type, CloseType.STOP_LOSS)
        self.assertEqual(position_executor._close_order.order, order)

    @patch.object(PositionExecutor, "get_in_flight_order")
    def test_process_order_completed_event_take_profit_order(self, in_flight_order_mock):
        order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1640001112.223,
        )
        in_flight_order_mock.return_value = order
        position_config = self.get_position_config_market_long()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._take_profit_limit_order = TrackedOrder("OID-BUY-1")
        event = BuyOrderCompletedEvent(
            timestamp=1234567890,
            order_id="OID-BUY-1",
            base_asset="ETH",
            quote_asset="USDT",
            base_asset_amount=position_config.amount,
            quote_asset_amount=position_config.amount * position_config.entry_price,
            order_type=position_config.triple_barrier_config.open_order_type,
            exchange_order_id="ED140"
        )
        market = MagicMock()
        position_executor.process_order_completed_event("102", market, event)
        self.assertEqual(position_executor.close_type, CloseType.TAKE_PROFIT)
        self.assertEqual(position_executor._take_profit_limit_order.order, order)

    def test_process_order_canceled_event(self):
        position_config = self.get_position_config_market_long()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._close_order = TrackedOrder("OID-BUY-1")
        event = OrderCancelledEvent(
            timestamp=1234567890,
            order_id="OID-BUY-1",
        )
        market = MagicMock()
        position_executor.process_order_canceled_event(102, market, event)
        self.assertEqual(position_executor._close_order, None)

    def test_control_trailing_stop_logs_trading_pair_context(self):
        position_config = self.get_position_config_market_long_with_trailing()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor.get_net_pnl_pct = MagicMock(return_value=Decimal("0.10"))

        with self.assertLogs(position_executor.logger().name, level="INFO") as activation_logs:
            position_executor.control_trailing_stop()

        activation_output = "\n".join(activation_logs.output)
        self.assertIn("Trailing stop armed", activation_output)
        self.assertIn("activation_price=", activation_output)
        self.assertIn("trigger_price=", activation_output)
        self.assertIn("pair=ETH-USDT", activation_output)

        position_executor._trailing_stop_trigger_pct = Decimal("0.08")
        position_executor.get_net_pnl_pct = MagicMock(return_value=Decimal("0.05"))
        position_executor.place_close_order_and_cancel_open_orders = MagicMock()

        with self.assertLogs(position_executor.logger().name, level="INFO") as trigger_logs:
            position_executor.control_trailing_stop()

        trigger_output = "\n".join(trigger_logs.output)
        self.assertIn("Trailing stop triggered", trigger_output)
        self.assertIn("trigger_price=", trigger_output)
        self.assertIn("current_price=", trigger_output)
        self.assertIn("pair=ETH-USDT", trigger_output)

    def test_trailing_arm_and_move_checkpoint_native_executor(self):
        executor, order, _ = self.expired_entry_fixture(Decimal("1"))
        order.current_state = OrderState.FILLED
        recorder = MagicMock()
        with patch.object(MarketsRecorder, "get_instance", return_value=recorder):
            executor.get_net_pnl_pct = MagicMock(return_value=Decimal("0.10"))
            executor.control_trailing_stop()
            self.assertEqual(executor._trailing_stop_trigger_pct, Decimal("0.08"))
            self.assertEqual(recorder.store_or_update_executor.call_count, 1)

            executor.get_net_pnl_pct.return_value = Decimal("0.13")
            executor.control_trailing_stop()
            self.assertEqual(executor._trailing_stop_trigger_pct, Decimal("0.11"))
            self.assertEqual(recorder.store_or_update_executor.call_count, 2)

    def test_trailing_checkpoint_failure_holds_before_sell(self):
        executor, order, _ = self.expired_entry_fixture(Decimal("1"))
        order.current_state = OrderState.FILLED
        recorder = MagicMock()
        recorder.store_or_update_executor.side_effect = IOError("database unavailable")
        with patch.object(MarketsRecorder, "get_instance", return_value=recorder):
            executor.get_net_pnl_pct = MagicMock(return_value=Decimal("0.10"))
            executor.control_trailing_stop()
            executor.get_net_pnl_pct.return_value = Decimal("0.01")
            executor.control_trailing_stop()

        self.assertIsNotNone(executor._resume_conflicting_fill)
        self.strategy.sell.assert_not_called()

    def test_other_unfinished_owner_blocks_direct_position_sell(self):
        executor = self.get_position_executor_running_from_config(
            self.get_position_config_market_long_with_trailing())
        self.strategy.executor_orchestrator = MagicMock()
        self.strategy.executor_orchestrator.recovery_required = {"other-controller": ["unknown-order"]}

        with self.assertRaisesRegex(RuntimeError, "startup reconciliation"):
            executor.place_order(
                connector_name="binance", trading_pair="ETH-USDT", order_type=OrderType.LIMIT,
                side=TradeType.SELL, amount=Decimal("1"), price=Decimal("100"))

        self.strategy.sell.assert_not_called()

    async def test_other_unfinished_owner_freezes_existing_trailing_control(self):
        executor, order, _ = self.expired_entry_fixture(Decimal("1"))
        order.current_state = OrderState.FILLED
        executor._trailing_stop_trigger_pct = Decimal("0.08")
        executor.get_net_pnl_pct = MagicMock(return_value=Decimal("0.01"))
        self.strategy.executor_orchestrator = MagicMock()
        self.strategy.executor_orchestrator.recovery_required = {"other-controller": ["unknown-order"]}

        await executor.control_task()

        self.assertEqual(executor.status, RunnableStatus.RUNNING)
        self.assertIsNone(executor.close_type)
        self.strategy.sell.assert_not_called()

    async def test_unattended_v2_restart_hold_freezes_existing_trailing_control(self):
        executor, order, _ = self.expired_entry_fixture(Decimal("1"))
        order.current_state = OrderState.FILLED
        executor._trailing_stop_trigger_pct = Decimal("0.08")
        executor.get_net_pnl_pct = MagicMock(return_value=Decimal("0.01"))
        self.strategy._unattended_restart_hold = True

        await executor.control_task()

        self.assertEqual(executor.status, RunnableStatus.RUNNING)
        self.assertIsNone(executor.close_type)
        self.strategy.sell.assert_not_called()

    def test_trailing_arm_checkpoint_survives_native_database_reopen(self):
        from tempfile import TemporaryDirectory

        from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
        from hummingbot.client.config.config_helpers import ClientConfigAdapter
        from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType

        executor, order, _ = self.expired_entry_fixture(Decimal("1"))
        order.current_state = OrderState.FILLED
        executor.get_net_pnl_pct = MagicMock(return_value=Decimal("0.10"))
        with TemporaryDirectory() as directory:
            manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                           db_path=directory + "/trailing.sqlite")
            previous_recorder = MarketsRecorder._shared_instance
            recorder = MarketsRecorder(manager, [], "trailing.yml", "v2_with_controllers",
                                       market_data_collection=MarketDataCollectionConfigMap(
                                           market_data_collection_enabled=False))
            try:
                with patch.object(MarketsRecorder, "get_instance", return_value=recorder):
                    executor.control_trailing_stop()
                stored = recorder.get_all_executors()[0]
                self.assertEqual(Decimal(stored.custom_info["trailing_stop_trigger_pct"]), Decimal("0.08"))
                self.assertEqual(stored.custom_info["trailing_state"], "armed")
                executor.get_net_pnl_pct.return_value = Decimal("0.13")
                with patch.object(MarketsRecorder, "get_instance", return_value=recorder):
                    executor.control_trailing_stop()
                moved = recorder.get_all_executors()[0]
                self.assertEqual(Decimal(moved.custom_info["trailing_stop_trigger_pct"]), Decimal("0.11"))
                self.assertEqual(moved.custom_info["trailing_move_count"], 1)
            finally:
                MarketsRecorder._shared_instance = previous_recorder
                manager.engine.dispose()

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price")
    def test_trailing_stop_places_marketable_limit_close_order(self, mock_get_price, mock_get_trading_rules):
        mock_get_price.side_effect = lambda connector, pair, price_type=PriceType.MidPrice: (
            Decimal("109") if price_type == PriceType.BestBid else Decimal("110")
        )
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        mock_get_trading_rules.return_value = trading_rules

        position_executor = self.get_position_executor_running_from_config(self.get_position_config_market_long_with_trailing())
        position_executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT_MAKER,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED,
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair="ETH-USDT",
                fill_price=Decimal("100"),
                fill_base_amount=Decimal("1"),
                fill_quote_amount=Decimal("100"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        connector = self.strategy.connectors["binance"]
        connector.quantize_order_amount.return_value = Decimal("1")
        connector.get_fee.return_value = AddedToCostTradeFee(
            percent=Decimal("0.001"),
            flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.05"))],
        )

        position_executor.place_close_order_and_cancel_open_orders(close_type=CloseType.TRAILING_STOP)

        self.assertEqual(
            self.strategy.sell.call_args.args,
            ("binance", "ETH-USDT", Decimal("1"), OrderType.LIMIT, Decimal("109"), PositionAction.CLOSE),
        )
        self.assertEqual(position_executor._close_order.order_id, "OID-SELL-1")
        self.assertEqual(connector.get_fee.call_args.kwargs["is_maker"], False)
        self.assertEqual(connector.get_fee.call_args.kwargs["order_type"], OrderType.LIMIT)

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price")
    def test_trailing_stop_places_marketable_limit_close_when_max_age_is_zero(self, mock_get_price, mock_get_trading_rules):
        mock_get_price.side_effect = lambda connector, pair, price_type=PriceType.MidPrice: (
            Decimal("109") if price_type == PriceType.BestBid else Decimal("110")
        )
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        mock_get_trading_rules.return_value = trading_rules

        position_config = self.get_position_config_market_long_with_trailing()
        position_config.triple_barrier_config.trailing_stop_limit_max_age_seconds = 0
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT_MAKER,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED,
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair="ETH-USDT",
                fill_price=Decimal("100"),
                fill_base_amount=Decimal("1"),
                fill_quote_amount=Decimal("100"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        connector = self.strategy.connectors["binance"]
        connector.quantize_order_amount.return_value = Decimal("1")
        connector.get_fee.return_value = AddedToCostTradeFee(
            percent=Decimal("0.001"),
            flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.05"))],
        )

        position_executor.place_close_order_and_cancel_open_orders(close_type=CloseType.TRAILING_STOP)

        self.assertEqual(
            self.strategy.sell.call_args.args,
            ("binance", "ETH-USDT", Decimal("1"), OrderType.LIMIT, Decimal("109"), PositionAction.CLOSE),
        )
        self.assertEqual(position_executor._close_order.order_id, "OID-SELL-1")
        self.assertEqual(connector.get_fee.call_args.kwargs["is_maker"], False)
        self.assertEqual(connector.get_fee.call_args.kwargs["order_type"], OrderType.LIMIT)
        self.assertFalse(position_executor._should_fallback_trailing_limit_close())

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price")
    def test_spot_long_close_order_caps_to_available_base_balance(self, mock_get_price, mock_get_trading_rules):
        mock_get_price.side_effect = lambda connector, pair, price_type=PriceType.MidPrice: (
            Decimal("109") if price_type == PriceType.BestBid else Decimal("110")
        )
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        mock_get_trading_rules.return_value = trading_rules

        position_executor = self.get_position_executor_running_from_config(
            self.get_position_config_market_long_with_trailing()
        )
        position_executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT_MAKER,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED,
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair="ETH-USDT",
                fill_price=Decimal("100"),
                fill_base_amount=Decimal("1"),
                fill_quote_amount=Decimal("100"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        self.strategy.connectors["binance"].get_available_balance.return_value = Decimal("0.75")
        self.strategy.connectors["binance"].quantize_order_amount.side_effect = lambda trading_pair, amount: amount

        position_executor.place_close_order_and_cancel_open_orders(close_type=CloseType.TRAILING_STOP)

        self.assertEqual(
            self.strategy.sell.call_args.args,
            ("binance", "ETH-USDT", Decimal("0.75"), OrderType.LIMIT, Decimal("109"), PositionAction.CLOSE),
        )
        self.assertEqual(position_executor._close_order.order_id, "OID-SELL-1")
        position_executor._close_order.order = InFlightOrder(
            client_order_id="OID-SELL-1",
            exchange_order_id="EOID5",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            amount=Decimal("0.75"),
            price=Decimal("109"),
            creation_timestamp=1640001113.223,
            initial_state=OrderState.FILLED,
        )
        position_executor._close_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="2",
                client_order_id="OID-SELL-1",
                exchange_order_id="EOID5",
                trading_pair="ETH-USDT",
                fill_price=Decimal("109"),
                fill_base_amount=Decimal("0.75"),
                fill_quote_amount=Decimal("81.75"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.1"))]),
                fill_timestamp=11,
            )
        )
        self.assertFalse(position_executor.open_and_close_volume_match())

    def _prepare_legacy_spot_close_rules(self, mock_get_price, mock_get_trading_rules):
        mock_get_price.side_effect = lambda connector, pair, price_type=PriceType.MidPrice: (
            Decimal("109") if price_type == PriceType.BestBid else Decimal("110")
        )
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        mock_get_trading_rules.return_value = trading_rules
        connector = self.strategy.connectors["binance"]
        connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
        return self._filled_spot_long_executor(), connector

    def _assert_legacy_spot_close_retained(self, executor):
        self.assertEqual(executor._close_order_amount(), Decimal("0"))
        self.assertEqual(executor._close_quantity.get("blocked_reason"), "available_balance_unavailable")
        executor.place_close_order_and_cancel_open_orders(close_type=CloseType.TRAILING_STOP)
        self.strategy.sell.assert_not_called()
        self.assertIsNone(executor._close_order)
        self.assertEqual(executor.amount_to_close, Decimal("1"))
        self.assertEqual(executor.status, RunnableStatus.SHUTTING_DOWN)
        self.assertEqual(executor.close_type, CloseType.TRAILING_STOP)
        self.assertFalse(executor.is_closed)

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price")
    def test_legacy_spot_close_blocks_when_available_balance_missing(self, mock_get_price, mock_get_trading_rules):
        executor, connector = self._prepare_legacy_spot_close_rules(mock_get_price, mock_get_trading_rules)
        connector.get_available_balance.return_value = None
        self._assert_legacy_spot_close_retained(executor)

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price")
    def test_legacy_spot_close_blocks_when_available_balance_non_finite(self, mock_get_price, mock_get_trading_rules):
        for value in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
            with self.subTest(value=str(value)):
                self.strategy = self.create_mock_strategy
                executor, connector = self._prepare_legacy_spot_close_rules(mock_get_price, mock_get_trading_rules)
                connector.get_available_balance.return_value = value
                self._assert_legacy_spot_close_retained(executor)

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price")
    def test_legacy_spot_close_blocks_when_available_balance_read_raises(self, mock_get_price, mock_get_trading_rules):
        executor, connector = self._prepare_legacy_spot_close_rules(mock_get_price, mock_get_trading_rules)
        connector.get_available_balance.side_effect = RuntimeError("balance feed down")
        self._assert_legacy_spot_close_retained(executor)

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price")
    def test_legacy_spot_close_recovers_when_available_balance_returns(self, mock_get_price, mock_get_trading_rules):
        executor, connector = self._prepare_legacy_spot_close_rules(mock_get_price, mock_get_trading_rules)
        connector.get_available_balance.side_effect = RuntimeError("balance feed down")
        executor.place_close_order_and_cancel_open_orders(close_type=CloseType.TRAILING_STOP)
        self.strategy.sell.assert_not_called()
        self.assertEqual(executor.amount_to_close, Decimal("1"))

        connector.get_available_balance.side_effect = None
        connector.get_available_balance.return_value = Decimal("0.75")
        executor.place_close_order_and_cancel_open_orders(close_type=CloseType.TRAILING_STOP)

        self.assertEqual(
            self.strategy.sell.call_args.args,
            ("binance", "ETH-USDT", Decimal("0.75"), OrderType.LIMIT, Decimal("109"), PositionAction.CLOSE),
        )
        self.assertEqual(executor._close_order.order_id, "OID-SELL-1")
        self.assertIsNone(executor._close_quantity.get("blocked_reason"))
        self.assertEqual(executor.amount_to_close, Decimal("1"))
        self.assertFalse(executor.is_closed)

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price")
    def test_legacy_close_receipt_tracks_each_recovery_stage(self, mock_get_price, mock_get_trading_rules):
        executor, connector = self._prepare_legacy_spot_close_rules(mock_get_price, mock_get_trading_rules)
        connector.get_available_balance.return_value = None
        self.assertEqual(executor._close_order_amount(), Decimal("0"))
        self.assertEqual(executor._close_quantity["blocked_reason"], "available_balance_unavailable")
        self.assertEqual(executor._close_quantity["residual_base"], Decimal("1"))

        connector.get_available_balance.return_value = Decimal("0.75")
        connector.quantize_order_amount.side_effect = RuntimeError("quantizer unavailable")
        self.assertEqual(executor._close_order_amount(), Decimal("0"))
        self.assertEqual(executor._close_quantity["blocked_reason"], "quantity_quantization_failed")
        self.assertEqual(executor._close_quantity["available_base"], Decimal("0.75"))
        self.assertEqual(executor._close_quantity["order_amount"], Decimal("0"))

        connector.quantize_order_amount.side_effect = None
        connector.quantize_order_amount.return_value = Decimal("-0.1")
        self.assertEqual(executor._close_order_amount(), Decimal("0"))
        self.assertEqual(executor._close_quantity["blocked_reason"], "quantity_quantization_invalid")
        self.assertEqual(executor._close_quantity["available_base"], Decimal("0.75"))
        self.assertEqual(executor._close_quantity["residual_base"], Decimal("1"))

        connector.quantize_order_amount.return_value = Decimal("0.7")
        self.assertEqual(executor._close_order_amount(), Decimal("0.7"))
        self.assertIsNone(executor._close_quantity["blocked_reason"])
        self.assertEqual(executor._close_quantity["order_amount"], Decimal("0.7"))
        self.assertEqual(executor._close_quantity["residual_base"], Decimal("0.3"))
        self.strategy.sell.assert_not_called()

    @patch.object(PositionExecutor, "_sleep")
    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price")
    async def test_spot_long_partial_close_continues_closing_remaining_amount(
        self, mock_get_price, mock_get_trading_rules, _
    ):
        mock_get_price.side_effect = lambda connector, pair, price_type=PriceType.MidPrice: (
            Decimal("109") if price_type == PriceType.BestBid else Decimal("110")
        )
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        mock_get_trading_rules.return_value = trading_rules

        position_executor = self.get_position_executor_running_from_config(
            self.get_position_config_market_long_with_trailing()
        )
        position_executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT_MAKER,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED,
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair="ETH-USDT",
                fill_price=Decimal("100"),
                fill_base_amount=Decimal("1"),
                fill_quote_amount=Decimal("100"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        position_executor._close_order = TrackedOrder(order_id="OID-SELL-1")
        position_executor._close_order.order = InFlightOrder(
            client_order_id="OID-SELL-1",
            exchange_order_id="EOID5",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            amount=Decimal("0.75"),
            price=Decimal("109"),
            creation_timestamp=1640001113.223,
            initial_state=OrderState.FILLED,
        )
        position_executor._close_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="2",
                client_order_id="OID-SELL-1",
                exchange_order_id="EOID5",
                trading_pair="ETH-USDT",
                fill_price=Decimal("109"),
                fill_base_amount=Decimal("0.75"),
                fill_quote_amount=Decimal("81.75"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.1"))]),
                fill_timestamp=11,
            )
        )
        position_executor.close_type = CloseType.TRAILING_STOP
        position_executor._status = RunnableStatus.SHUTTING_DOWN
        self.strategy.sell.side_effect = ["OID-SELL-2"]
        self.strategy.connectors["binance"].get_available_balance.return_value = Decimal("0.25")
        self.strategy.connectors["binance"].quantize_order_amount.side_effect = lambda trading_pair, amount: amount

        await position_executor.control_shutdown_process()

        self.assertEqual(
            self.strategy.sell.call_args.args,
            ("binance", "ETH-USDT", Decimal("0.25"), OrderType.LIMIT, Decimal("109"), PositionAction.CLOSE),
        )
        self.assertEqual(position_executor.close_filled_amount, Decimal("0.75"))
        self.assertEqual(position_executor.close_filled_amount_quote, Decimal("81.75"))
        self.assertEqual(position_executor.close_price, Decimal("109"))
        self.assertEqual(position_executor.cum_fees_quote, Decimal("0.3"))
        self.assertEqual(position_executor.amount_to_close, Decimal("0.25"))
        self.assertEqual(position_executor._close_order.order_id, "OID-SELL-2")

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price")
    async def test_trailing_limit_close_falls_back_to_market_after_timeout(self, mock_get_price, mock_get_trading_rules):
        mock_get_price.side_effect = lambda connector, pair, price_type=PriceType.MidPrice: (
            Decimal("109") if price_type == PriceType.BestBid else Decimal("110")
        )
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules.min_notional_size = Decimal("1")
        mock_get_trading_rules.return_value = trading_rules

        position_executor = self.get_position_executor_running_from_config(self.get_position_config_market_long_with_trailing())
        position_executor._open_order = TrackedOrder(order_id="OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT_MAKER,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED,
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair="ETH-USDT",
                fill_price=Decimal("100"),
                fill_base_amount=Decimal("1"),
                fill_quote_amount=Decimal("100"),
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        self.strategy.connectors["binance"].quantize_order_amount.return_value = Decimal("1")
        position_executor.close_type = CloseType.TRAILING_STOP
        position_executor.close_timestamp = 1234567890
        position_executor._close_order = TrackedOrder("OID-SELL-LIMIT")
        position_executor._close_order_type = OrderType.LIMIT
        position_executor._close_order.order = InFlightOrder(
            client_order_id="OID-SELL-LIMIT",
            exchange_order_id="EOID5",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            amount=Decimal("1"),
            price=Decimal("109"),
            creation_timestamp=1234567890,
            initial_state=OrderState.OPEN,
        )
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567894)

        connector = self.strategy.connectors["binance"]
        connector._update_orders_with_error_handler = MagicMock()

        await position_executor.control_close_order()

        self.strategy.cancel.assert_called_with(
            connector_name="binance",
            trading_pair="ETH-USDT",
            order_id="OID-SELL-LIMIT",
        )
        self.assertFalse(self.strategy.sell.called)

        position_executor.process_order_canceled_event(
            "102",
            MagicMock(),
            OrderCancelledEvent(timestamp=1234567894, order_id="OID-SELL-LIMIT"),
        )
        await position_executor.control_close_order()

        sell_args = self.strategy.sell.call_args.args
        self.assertEqual(sell_args[:4], ("binance", "ETH-USDT", Decimal("1"), OrderType.MARKET))
        self.assertFalse(sell_args[4].is_finite())
        self.assertEqual(sell_args[5], PositionAction.CLOSE)
        self.assertFalse(position_executor._trailing_limit_fallback_pending)

    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("110"))
    def test_to_format_status_includes_trailing_activation_and_trigger_details(self, _):
        position_config = self.get_position_config_market_long_with_trailing()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder("OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=position_config.triple_barrier_config.open_order_type,
            trade_type=TradeType.BUY,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair=position_config.trading_pair,
                fill_price=position_config.entry_price,
                fill_base_amount=position_config.amount,
                fill_quote_amount=position_config.amount * position_config.entry_price,
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        self.strategy.connectors["binance"].quantize_order_amount.return_value = position_config.amount
        position_executor._trailing_stop_trigger_pct = Decimal("0.08")
        position_executor._trailing_stop_move_count = 1

        status = "\n".join(position_executor.to_format_status())
        self.assertIn("🧵 Trailing stop: armed", status)
        self.assertIn("arm 0.05000 @ 105.20000", status)
        self.assertIn("🧵 Trailing trigger: 0.08000 @ 108.20000", status)
        self.assertIn("moves 1", status)

    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("101"))
    def test_to_format_status(self, _):
        position_config = self.get_position_config_market_long()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder("OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=position_config.triple_barrier_config.open_order_type,
            trade_type=TradeType.BUY,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair=position_config.trading_pair,
                fill_price=position_config.entry_price,
                fill_base_amount=position_config.amount,
                fill_quote_amount=position_config.amount * position_config.entry_price,
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        self.strategy.connectors["binance"].quantize_order_amount.return_value = position_config.amount

        status = position_executor.to_format_status()
        self.assertIn("🎯 Trading Pair: ETH-USDT", status[0])
        self.assertIn("PNL (%): 0.80%", status[0])

    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("101"))
    def test_to_format_status_is_closed(self, _):
        position_config = self.get_position_config_market_long()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder("OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=position_config.triple_barrier_config.open_order_type,
            trade_type=TradeType.BUY,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair=position_config.trading_pair,
                fill_price=position_config.entry_price,
                fill_base_amount=position_config.amount,
                fill_quote_amount=position_config.amount * position_config.entry_price,
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        self.strategy.connectors["binance"].quantize_order_amount.return_value = position_config.amount
        with patch.object(type(position_executor), "close_price", new_callable=PropertyMock, return_value=Decimal(101)):
            status = position_executor.to_format_status()
        self.assertIn("🎯 Trading Pair: ETH-USDT", status[0])
        self.assertIn("PNL (%): 0.80%", status[0])

    @patch.object(PositionExecutor, 'get_trading_rules')
    @patch.object(PositionExecutor, 'adjust_order_candidates')
    async def test_validate_sufficient_balance(self, mock_adjust_order_candidates, mock_get_trading_rules):
        # Mock trading rules
        trading_rules = TradingRule(trading_pair="ETH-USDT", min_order_size=Decimal("0.1"),
                                    min_price_increment=Decimal("0.1"), min_base_amount_increment=Decimal("0.1"))
        mock_get_trading_rules.return_value = trading_rules
        executor = PositionExecutor(self.strategy, self.get_position_config_market_long())
        # Mock order candidate
        order_candidate = OrderCandidate(
            trading_pair="ETH-USDT",
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100")
        )
        # Test for sufficient balance
        mock_adjust_order_candidates.return_value = [order_candidate]
        await executor.validate_sufficient_balance()
        self.assertNotEqual(executor.close_type, CloseType.INSUFFICIENT_BALANCE)

        # Test for insufficient balance
        order_candidate.amount = Decimal("0")
        mock_adjust_order_candidates.return_value = [order_candidate]
        await executor.validate_sufficient_balance()
        self.assertEqual(executor.close_type, CloseType.INSUFFICIENT_BALANCE)
        self.assertEqual(executor.status, RunnableStatus.TERMINATED)

    def test_get_custom_info(self):
        position_config = self.get_position_config_market_long_with_trailing()
        executor = PositionExecutor(self.strategy, position_config)
        executor._trailing_stop_trigger_pct = Decimal("0.08")
        executor._trailing_stop_move_count = 2
        custom_info = executor.get_custom_info()

        self.assertEqual(custom_info["level_id"], position_config.level_id)
        self.assertIsNone(custom_info["role"])
        self.assertEqual(custom_info["current_position_average_price"], executor.entry_price)
        self.assertEqual(custom_info["side"], position_config.side)
        self.assertEqual(custom_info["current_retries"], executor._current_retries)
        self.assertEqual(custom_info["max_retries"], executor._max_retries)
        self.assertEqual(custom_info["trailing_state"], "armed")
        self.assertEqual(custom_info["trailing_activation_price"], Decimal("105"))
        self.assertEqual(custom_info["trailing_trigger_price"], Decimal("108"))
        self.assertEqual(custom_info["trailing_move_count"], 2)

    def test_get_custom_info_sets_role_for_named_split_legs(self):
        position_config = self.get_position_config_market_long()
        position_config.level_id = "scout"
        executor = PositionExecutor(self.strategy, position_config)

        custom_info = executor.get_custom_info()

        self.assertEqual(custom_info["level_id"], "scout")
        self.assertEqual(custom_info["role"], "scout")

    def test_cancel_close_order_and_process_cancel_event(self):
        position_config = self.get_position_config_market_long()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._close_order = TrackedOrder("OID-BUY-1")
        position_executor.cancel_open_orders()
        event = OrderCancelledEvent(
            timestamp=1234567890,
            order_id="OID-BUY-1",
        )
        market = MagicMock()
        position_executor.process_order_canceled_event("102", market, event)
        self.assertEqual(position_executor.close_type, None)

    def test_process_order_failed_event_fail_closes_permanent_min_notional_open_failure(self):
        position_config = self.get_position_config_market_long()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder("OID-BUY-1")

        market = MagicMock()
        position_executor.process_order_failed_event(
            "102",
            market,
            MarketOrderFailureEvent(
                timestamp=1640001112.223,
                order_id="OID-BUY-1",
                order_type=OrderType.LIMIT_MAKER,
                error_message="Order notional 1.50 is lower than minimum notional size 5.00 for the pair ETH-USDT.",
                error_type="ValueError",
            ),
        )

        self.assertIsNone(position_executor._open_order)
        self.assertEqual(position_executor.close_type, CloseType.FAILED)
        self.assertEqual(position_executor.status, RunnableStatus.TERMINATED)
        self.assertEqual(position_executor._current_retries, 0)

    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("101"))
    def test_position_executor_created_without_entry_price(self, _):
        config = PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                        connector_name="binance",
                                        side=TradeType.BUY, amount=Decimal("1"),
                                        triple_barrier_config=TripleBarrierConfig(
                                            stop_loss=Decimal("0.05"), take_profit=Decimal("0.1"), time_limit=60,
                                            take_profit_order_type=OrderType.LIMIT,
                                            stop_loss_order_type=OrderType.MARKET))

        executor = PositionExecutor(self.strategy, config)
        self.assertEqual(executor.entry_price, Decimal("101"))

    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("101"))
    def test_position_executor_entry_price_updated_with_limit_maker(self, _):
        config = PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                        connector_name="binance",
                                        side=TradeType.BUY, amount=Decimal("1"),
                                        entry_price=Decimal("102"),
                                        triple_barrier_config=TripleBarrierConfig(
                                            open_order_type=OrderType.LIMIT_MAKER,
                                            stop_loss=Decimal("0.05"), take_profit=Decimal("0.1"), time_limit=60,
                                            take_profit_order_type=OrderType.LIMIT,
                                            stop_loss_order_type=OrderType.MARKET))

        executor = PositionExecutor(self.strategy, config)
        self.assertEqual(executor.entry_price, Decimal("101"))

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("101"))
    def test_position_executor_entry_price_limit_maker_buffers_touch_price(self, _, mock_get_trading_rules):
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_price_increment = Decimal("0.1")
        mock_get_trading_rules.return_value = trading_rules
        config = PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                        connector_name="binance",
                                        side=TradeType.BUY, amount=Decimal("1"),
                                        entry_price=Decimal("102"),
                                        triple_barrier_config=TripleBarrierConfig(
                                            open_order_type=OrderType.LIMIT_MAKER,
                                            stop_loss=Decimal("0.05"), take_profit=Decimal("0.1"), time_limit=60,
                                            take_profit_order_type=OrderType.LIMIT,
                                            stop_loss_order_type=OrderType.MARKET))

        executor = PositionExecutor(self.strategy, config)
        self.assertEqual(executor.entry_price, Decimal("100.9"))

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_price",
           return_value=Decimal("101.005"))
    def test_position_executor_entry_price_limit_maker_quantizes_off_grid_touch_price(self, _, mock_get_trading_rules):
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_price_increment = Decimal("0.01")
        mock_get_trading_rules.return_value = trading_rules
        config = PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                        connector_name="binance",
                                        side=TradeType.BUY, amount=Decimal("1"),
                                        entry_price=Decimal("102"),
                                        triple_barrier_config=TripleBarrierConfig(
                                            open_order_type=OrderType.LIMIT_MAKER,
                                            stop_loss=Decimal("0.05"), take_profit=Decimal("0.1"), time_limit=60,
                                            take_profit_order_type=OrderType.LIMIT,
                                            stop_loss_order_type=OrderType.MARKET))

        executor = PositionExecutor(self.strategy, config)
        self.assertEqual(executor.entry_price, Decimal("101.00"))

    @patch.object(PositionExecutor, "_sleep")
    @patch.object(PositionExecutor, "place_close_order_and_cancel_open_orders")
    async def test_control_shutdown_process(self, place_order_mock, _):
        position_config = self.get_position_config_market_long()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor._open_order = TrackedOrder("OID-BUY-1")
        position_executor._open_order.order = InFlightOrder(
            client_order_id="OID-BUY-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=position_config.triple_barrier_config.open_order_type,
            trade_type=TradeType.BUY,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1640001112.223,
            initial_state=OrderState.FILLED
        )
        position_executor._open_order.order.update_with_trade_update(
            TradeUpdate(
                trade_id="1",
                client_order_id="OID-BUY-1",
                exchange_order_id="EOID4",
                trading_pair=position_config.trading_pair,
                fill_price=position_config.entry_price,
                fill_base_amount=position_config.amount,
                fill_quote_amount=position_config.amount * position_config.entry_price,
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
                fill_timestamp=10,
            )
        )
        self.strategy.connectors["binance"].quantize_order_amount.side_effect = lambda trading_pair, amount: amount
        position_executor._status = RunnableStatus.SHUTTING_DOWN
        await position_executor.control_task()
        place_order_mock.assert_called_once()
        position_executor._close_order = TrackedOrder("OID-SELL-1")
        position_executor._close_order.order = InFlightOrder(
            client_order_id="OID-SELL-1",
            exchange_order_id="EOID4",
            trading_pair=position_config.trading_pair,
            order_type=position_config.triple_barrier_config.open_order_type,
            trade_type=TradeType.SELL,
            amount=position_config.amount,
            price=position_config.entry_price,
            creation_timestamp=1640001112.223,
            initial_state=OrderState.OPEN
        )
        await position_executor.control_task()

    @patch.object(PositionExecutor, "get_price")
    def test_is_within_activation_bounds_long(self, mock_price):
        mock_price.return_value = Decimal("100")
        position_config = self.get_position_config_market_long()
        position_config.activation_bounds = [Decimal("0.001"), Decimal("0.01")]
        position_config.triple_barrier_config.open_order_type = OrderType.MARKET
        executor = PositionExecutor(self.strategy, position_config)
        self.assertTrue(executor._is_within_activation_bounds(Decimal("100.0"), TradeType.BUY, OrderType.MARKET))
        self.assertFalse(executor._is_within_activation_bounds(Decimal("101.0"), TradeType.BUY, OrderType.MARKET))
        self.assertTrue(executor._is_within_activation_bounds(Decimal("99.9"), TradeType.BUY, OrderType.LIMIT))
        self.assertFalse(executor._is_within_activation_bounds(Decimal("99.7"), TradeType.BUY, OrderType.LIMIT))

    @patch.object(PositionExecutor, "get_price")
    def test_is_within_activation_bounds_market_short(self, mock_price):
        mock_price.return_value = Decimal("100")
        position_config = self.get_position_config_market_short()
        position_config.activation_bounds = [Decimal("0.001"), Decimal("0.01")]
        position_config.triple_barrier_config.open_order_type = OrderType.MARKET
        executor = PositionExecutor(self.strategy, position_config)
        self.assertTrue(executor._is_within_activation_bounds(Decimal("100"), TradeType.SELL, OrderType.MARKET))
        self.assertFalse(executor._is_within_activation_bounds(Decimal("99.9"), TradeType.SELL, OrderType.MARKET))
        self.assertTrue(executor._is_within_activation_bounds(Decimal("100.1"), TradeType.SELL, OrderType.LIMIT))
        self.assertFalse(executor._is_within_activation_bounds(Decimal("100.3"), TradeType.SELL, OrderType.LIMIT))

    def test_failed_executor_info(self):
        position_config = self.get_position_config_market_short()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor.close_type = CloseType.FAILED
        with patch.object(type(position_executor), "filled_amount_quote", new_callable=PropertyMock,
                          return_value=Decimal("0")):
            executor_info = position_executor.executor_info
        self.assertEqual(executor_info.close_type, CloseType.FAILED)
        self.assertEqual(executor_info.net_pnl_pct, Decimal("0"))

    @patch.object(PositionExecutor, "get_trading_rules")
    @patch.object(PositionExecutor, "get_price")
    def test_early_stop(self, mock_price, trading_rules_mock):
        mock_price.return_value = Decimal("100")
        trading_rules = MagicMock(spec=TradingRule)
        trading_rules.min_order_size = Decimal("0.1")
        trading_rules_mock.return_value = trading_rules
        position_config = self.get_position_config_market_short()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor.early_stop()
        self.assertEqual(position_executor.close_type, CloseType.EARLY_STOP)
        self.assertEqual(position_executor.status, RunnableStatus.SHUTTING_DOWN)
        position_config = self.get_position_config_market_short()
        position_executor = self.get_position_executor_running_from_config(position_config)
        position_executor.early_stop(keep_position=True)
        self.assertEqual(position_executor.close_type, CloseType.POSITION_HOLD)
        self.assertEqual(position_executor.status, RunnableStatus.SHUTTING_DOWN)

    def expired_entry_fixture(self, filled=Decimal("0")):
        config = self.get_position_config_market_long_with_trailing()
        config.triple_barrier_config.open_order_type = OrderType.LIMIT
        config.triple_barrier_config.open_order_limit_max_age_seconds = 30
        executor = self.get_position_executor_running_from_config(config)
        order = InFlightOrder(
            client_order_id="OID-BUY-1", exchange_order_id="EOID4", trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1"),
            price=Decimal("100"), creation_timestamp=1234567890, initial_state=OrderState.OPEN)
        executor._open_order = TrackedOrder(order_id=order.client_order_id)
        executor._open_order.order = order
        connector = self.strategy.connectors["binance"]
        connector._order_tracker.fetch_order.side_effect = lambda client_order_id: order if client_order_id == order.client_order_id else None
        connector.get_available_balance.return_value = Decimal("10")
        connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
        connector.supports_terminal_fill_reconciliation = True
        connector._request_order_status = AsyncMock(return_value=OrderUpdate(
            trading_pair=order.trading_pair, update_timestamp=1234567921,
            new_state=OrderState.CANCELED, client_order_id=order.client_order_id, exchange_order_id=order.exchange_order_id))
        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567920)
        if filled:
            order.update_with_trade_update(self.entry_trade(order, filled))
        return executor, order, connector

    def entry_trade(self, order, amount, trade_id="entry-fill"):
        return TradeUpdate(
            trade_id=trade_id, client_order_id=order.client_order_id, exchange_order_id=order.exchange_order_id,
            trading_pair=order.trading_pair, fill_timestamp=1234567920, fill_price=Decimal("100"),
            fill_base_amount=amount, fill_quote_amount=amount * 100,
            fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="ETH", amount=amount * Decimal("0.001"))]))

    def acknowledge_entry_cancel(self, executor, order):
        order.update_with_order_update(OrderUpdate(
            trading_pair=order.trading_pair, update_timestamp=1234567921,
            new_state=OrderState.CANCELED, client_order_id=order.client_order_id))
        event = OrderCancelledEvent(timestamp=1234567921, order_id=order.client_order_id)
        executor.process_order_canceled_event(None, None, event)
        executor.process_order_canceled_event(None, None, event)

    async def test_expired_entry_keeps_partial_fill_and_original_trailing_owner(self):
        executor, order, connector = self.expired_entry_fixture(Decimal("0.2"))
        await self.settle_expiry_reconciliation(executor)
        self.assertEqual(executor.status, RunnableStatus.RUNNING)
        self.acknowledge_entry_cancel(executor, order)
        await self.settle_expiry_reconciliation(executor)
        await self.settle_expiry_reconciliation(executor)
        self.assertTrue(executor.is_trading)
        connector._request_order_status.assert_not_awaited()
        self.assertIsNone(executor.close_type)
        self.assertEqual(executor.open_filled_amount, Decimal("0.1998"))
        self.assertEqual(executor.get_custom_info()["order_ids"], [order.client_order_id])
        self.assertTrue(executor.get_custom_info()["open_order_entry_expired"])
        self.strategy.buy.assert_not_called()
        self.strategy.sell.assert_not_called()
        connector.get_price_by_type.return_value = Decimal("110")
        with patch.object(MarketsRecorder, "get_instance", return_value=MagicMock()):
            await self.settle_expiry_reconciliation(executor)
        self.assertIsNotNone(executor._trailing_stop_trigger_pct)
        connector.get_price_by_type.return_value = Decimal("106")
        await self.settle_expiry_reconciliation(executor)
        self.assertEqual(executor.close_type, CloseType.TRAILING_STOP)
        self.assertEqual(self.strategy.sell.call_args.args[2], Decimal("0.1998"))
        self.assertEqual(self.strategy.sell.call_args.args[3], OrderType.LIMIT)

    async def test_expired_entry_reconciles_fill_delivered_after_cancel_ack(self):
        executor, order, connector = self.expired_entry_fixture()
        await self.settle_expiry_reconciliation(executor)
        self.acknowledge_entry_cancel(executor, order)
        status = connector._request_order_status.return_value

        async def reconcile(tracked_order):
            fill = self.entry_trade(order, Decimal("1"))
            order.update_with_trade_update(fill)
            order.update_with_trade_update(fill)
            return status
        connector._request_order_status.side_effect = reconcile
        await self.settle_expiry_reconciliation(executor)
        self.assertEqual(executor.status, RunnableStatus.RUNNING)
        self.assertEqual(executor.open_filled_amount, Decimal("0.999"))
        self.strategy.buy.assert_not_called()
        self.strategy.sell.assert_not_called()

    async def test_expired_empty_entry_terminates_only_after_terminal_reconciliation(self):
        executor, order, connector = self.expired_entry_fixture()
        await self.settle_expiry_reconciliation(executor)
        self.assertEqual(executor.status, RunnableStatus.RUNNING)
        connector._request_order_status.assert_not_awaited()
        self.acknowledge_entry_cancel(executor, order)
        await self.settle_expiry_reconciliation(executor)
        connector._request_order_status.assert_awaited_once()
        self.assertEqual(executor.status, RunnableStatus.TERMINATED)
        self.assertEqual(executor.close_type, CloseType.EXPIRED)
        self.strategy.buy.assert_not_called()
        self.strategy.sell.assert_not_called()

    async def test_cancel_event_with_stale_open_snapshot_reconciles_zero_fill(self):
        executor, order, connector = self.expired_entry_fixture()
        executor.config.entry_order_policy = "single_attempt"
        executor.process_order_canceled_event(None, None, OrderCancelledEvent(
            timestamp=1234567921, order_id=order.client_order_id))
        self.assertEqual(order.current_state, OrderState.OPEN)

        await self.settle_expiry_reconciliation(executor)

        connector._request_order_status.assert_awaited_once()
        self.assertEqual(order.current_state, OrderState.CANCELED)
        self.assertEqual(executor.status, RunnableStatus.TERMINATED)
        self.assertEqual(executor.close_type, CloseType.EXPIRED)
        self.strategy.buy.assert_not_called()
        self.strategy.sell.assert_not_called()

    async def test_cancel_reconciliation_keeps_authoritative_snapshot_over_stale_tracker(self):
        executor, order, connector = self.expired_entry_fixture()
        executor.config.entry_order_policy = "single_attempt"
        connector._order_tracker.fetch_order.side_effect = lambda client_order_id: InFlightOrder.from_json(order.to_json())
        executor.process_order_canceled_event(None, None, OrderCancelledEvent(
            timestamp=1234567921, order_id=order.client_order_id))

        await self.settle_expiry_reconciliation(executor)

        self.assertEqual(executor.status, RunnableStatus.TERMINATED)
        self.assertEqual(executor._open_order.order.current_state, OrderState.CANCELED)
        self.strategy.sell.assert_not_called()

    async def test_terminal_entry_keeps_late_fill_from_stale_tracker(self):
        executor, order, connector = self.expired_entry_fixture()
        late_snapshot = InFlightOrder.from_json(order.to_json())
        late_snapshot.update_with_trade_update(self.entry_trade(late_snapshot, Decimal("0.2")))
        late_snapshot.current_state = OrderState.PARTIALLY_FILLED
        order.current_state = OrderState.CANCELED
        connector._order_tracker.fetch_order.return_value = late_snapshot
        connector._order_tracker.fetch_order.side_effect = None

        executor.update_tracked_orders_with_order_id(order.client_order_id)

        self.assertIs(executor._open_order.order, order)
        self.assertEqual(order.current_state, OrderState.CANCELED)
        self.assertEqual(order.executed_amount_base, Decimal("0.2"))
        self.assertEqual(executor.amount_to_close, Decimal("0.1998"))
        self.strategy.sell.assert_not_called()

    async def test_terminal_entry_holds_on_conflicting_tracker_fill(self):
        executor, order, connector = self.expired_entry_fixture(Decimal("0.2"))
        late_snapshot = InFlightOrder.from_json(order.to_json())
        late_snapshot.order_fills["entry-fill"] = self.entry_trade(late_snapshot, Decimal("0.3"))
        late_snapshot.current_state = OrderState.PARTIALLY_FILLED
        order.current_state = OrderState.CANCELED
        connector._order_tracker.fetch_order.return_value = late_snapshot
        connector._order_tracker.fetch_order.side_effect = None

        executor.update_tracked_orders_with_order_id(order.client_order_id)

        self.assertIs(executor._open_order.order, order)
        self.assertEqual(order.executed_amount_base, Decimal("0.2"))
        self.assertEqual(executor._entry_lifecycle["state"], "reconciliation_required")
        self.assertIsNotNone(executor._resume_conflicting_fill)
        self.strategy.sell.assert_not_called()

    async def test_cancel_event_with_stale_open_snapshot_keeps_late_fill_owner(self):
        executor, order, connector = self.expired_entry_fixture()
        executor.config.entry_order_policy = "single_attempt"
        executor.process_order_canceled_event(None, None, OrderCancelledEvent(
            timestamp=1234567921, order_id=order.client_order_id))

        async def terminal_with_fill(tracked_order):
            order.update_with_trade_update(self.entry_trade(order, Decimal("0.2")))
            return connector._request_order_status.return_value

        connector._request_order_status.side_effect = terminal_with_fill
        await self.settle_expiry_reconciliation(executor)

        self.assertEqual(order.current_state, OrderState.CANCELED)
        self.assertEqual(executor.status, RunnableStatus.RUNNING)
        self.assertTrue(executor.is_trading)
        self.assertEqual(executor.amount_to_close, Decimal("0.1998"))
        self.strategy.sell.assert_not_called()

    async def test_cancel_event_with_stale_open_snapshot_retains_on_bad_status(self):
        for response in [IOError("status unavailable"), OrderUpdate(
                trading_pair="ETH-USDT", update_timestamp=1234567921,
                new_state=OrderState.CANCELED, client_order_id="other", exchange_order_id="EOID4")]:
            with self.subTest(response=response):
                executor, order, connector = self.expired_entry_fixture()
                executor.config.entry_order_policy = "single_attempt"
                executor.process_order_canceled_event(None, None, OrderCancelledEvent(
                    timestamp=1234567921, order_id=order.client_order_id))
                if isinstance(response, Exception):
                    connector._request_order_status.side_effect = response
                else:
                    connector._request_order_status.return_value = response

                await self.settle_expiry_reconciliation(executor)

                self.assertEqual(order.current_state, OrderState.OPEN)
                self.assertEqual(executor.status, RunnableStatus.RUNNING)
                self.assertEqual(executor.get_custom_info()["entry_lifecycle"]["state"], "reconciliation_required")
                self.strategy.cancel.assert_not_called()
                self.strategy.buy.assert_not_called()
                self.strategy.sell.assert_not_called()

    async def test_restored_cancel_receipt_reconciles_stale_open_snapshot(self):
        executor, order, connector = self.expired_entry_fixture()
        executor.config.entry_order_policy = "single_attempt"
        executor._entry_stop_reason = "exchange_canceled"
        executor._entry_lifecycle.update({
            "state": "cancel_pending", "stop_reason": "exchange_canceled",
            "cancel_event_timestamp": 1234567921,
        })

        await self.settle_expiry_reconciliation(executor)

        connector._request_order_status.assert_awaited_once()
        self.assertEqual(executor.status, RunnableStatus.TERMINATED)
        self.strategy.cancel.assert_not_called()

    async def test_cancel_reconciliation_failure_waits_before_retrying_status(self):
        executor, order, connector = self.expired_entry_fixture()
        executor.config.entry_order_policy = "single_attempt"
        executor.process_order_canceled_event(None, None, OrderCancelledEvent(
            timestamp=1234567921, order_id=order.client_order_id))
        connector._request_order_status.side_effect = IOError("venue unavailable")

        await self.settle_expiry_reconciliation(executor)
        await executor.control_task()
        if executor._entry_expiry_reconciliation_task is not None:
            await asyncio.gather(executor._entry_expiry_reconciliation_task, return_exceptions=True)
        connector._request_order_status.assert_awaited_once()
        self.strategy.cancel.assert_not_called()

        type(self.strategy).current_timestamp = PropertyMock(return_value=1234567932)
        connector._request_order_status.side_effect = None
        await self.settle_expiry_reconciliation(executor)
        self.assertEqual(connector._request_order_status.await_count, 2)
        self.assertEqual(executor.status, RunnableStatus.TERMINATED)

    async def test_expired_entry_reconciliation_failure_retains_ownership(self):
        for response in [IOError("fill total unavailable"), TimeoutError("status timed out"), None, OrderUpdate(
                trading_pair="ETH-USDT", update_timestamp=1234567921, new_state=OrderState.OPEN,
                client_order_id="OID-BUY-1", exchange_order_id="EOID4")]:
            with self.subTest(response=response):
                executor, order, connector = self.expired_entry_fixture()
                await self.settle_expiry_reconciliation(executor)
                self.acknowledge_entry_cancel(executor, order)
                if isinstance(response, Exception):
                    connector._request_order_status.side_effect = response
                else:
                    connector._request_order_status.return_value = response
                await self.settle_expiry_reconciliation(executor)
                self.assertEqual(executor.status, RunnableStatus.RUNNING)
                self.assertIsNone(executor.close_type)
                self.strategy.buy.assert_not_called()
                self.strategy.sell.assert_not_called()

    async def test_expired_partial_entry_still_obeys_early_stop(self):
        executor, order, connector = self.expired_entry_fixture(Decimal("0.2"))
        await self.settle_expiry_reconciliation(executor)
        executor.early_stop()
        self.acknowledge_entry_cancel(executor, order)
        with patch.object(executor, "_sleep", new_callable=AsyncMock):
            await self.settle_expiry_reconciliation(executor)
        self.assertEqual(executor.close_type, CloseType.EARLY_STOP)
        self.assertEqual(self.strategy.sell.call_args.args[2], Decimal("0.1998"))

    async def test_expired_entry_fill_crossing_cancel_keeps_owner(self):
        executor, order, connector = self.expired_entry_fixture()
        await self.settle_expiry_reconciliation(executor)
        order.update_with_trade_update(self.entry_trade(order, Decimal("1")))
        order.update_with_order_update(OrderUpdate(
            trading_pair=order.trading_pair, update_timestamp=1234567921,
            new_state=OrderState.FILLED, client_order_id=order.client_order_id))
        connector._request_order_status.return_value = OrderUpdate(
            trading_pair=order.trading_pair, update_timestamp=1234567921,
            new_state=OrderState.FILLED, client_order_id=order.client_order_id, exchange_order_id=order.exchange_order_id)
        await self.settle_expiry_reconciliation(executor)
        self.assertTrue(executor.is_trading)
        self.assertIsNone(executor.close_type)
        self.strategy.buy.assert_not_called()
        self.strategy.sell.assert_not_called()

    async def test_expired_partial_entry_obeys_stop_loss_and_time_limit(self):
        for barrier in ["stop_loss", "time_limit"]:
            with self.subTest(barrier=barrier):
                self.strategy = self.create_mock_strategy
                executor, order, connector = self.expired_entry_fixture(Decimal("0.2"))
                await self.settle_expiry_reconciliation(executor)
                self.acknowledge_entry_cancel(executor, order)
                await self.settle_expiry_reconciliation(executor)
                if barrier == "stop_loss":
                    executor.config.triple_barrier_config.stop_loss = Decimal("0.05")
                    connector.get_price_by_type.return_value = Decimal("90")
                else:
                    executor.config.triple_barrier_config.time_limit = 30
                await self.settle_expiry_reconciliation(executor)
                self.assertEqual(executor.close_type, CloseType.STOP_LOSS if barrier == "stop_loss" else CloseType.TIME_LIMIT)
                self.assertEqual(self.strategy.sell.call_args.args[2], Decimal("0.1998"))
                self.assertEqual(self.strategy.sell.call_args.args[3], OrderType.MARKET)

    def test_close_quantity_rounding_does_not_report_balance_shortage(self):
        executor, order, connector = self.expired_entry_fixture(Decimal("0.2"))
        connector.quantize_order_amount.side_effect = None
        connector.quantize_order_amount.return_value = Decimal("0.199")
        with patch.object(executor.logger(), "warning") as warning:
            self.assertEqual(executor._close_order_amount(), Decimal("0.199"))
            warning.assert_not_called()
        connector.get_available_balance.return_value = Decimal("0.1")
        connector.quantize_order_amount.return_value = Decimal("0.1")
        with patch.object(executor.logger(), "warning") as warning:
            self.assertEqual(executor._close_order_amount(), Decimal("0.1"))
            warning.assert_called_once()
            self.assertIn("available base balance", warning.call_args.args[0])

    async def test_okx_terminal_reconciliation_restores_missing_fill_before_expiry(self):
        from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange

        executor, order, _ = self.expired_entry_fixture()
        okx = OkxExchange(
            okx_api_key="test-key", okx_secret_key="test-secret", okx_passphrase="test-passphrase",
            trading_pairs=["ETH-USDT"], trading_required=False)
        self.assertTrue(okx.supports_terminal_fill_reconciliation)
        okx._order_tracker.start_tracking_order(order)
        executor.connectors["binance"] = okx
        executor.register_events()
        terminal = {"code": "0", "data": [{
            "ordId": order.exchange_order_id, "clOrdId": order.client_order_id,
            "state": "canceled", "accFillSz": "0.2", "uTime": "1234567921000"}]}
        fills = {"code": "0", "data": [{
            "tradeId": "late-trade", "ordId": order.exchange_order_id,
            "fillSz": "0.2", "fillPx": "100", "fee": "-0.0002", "feeCcy": "ETH", "ts": "1234567920000"}]}
        with patch.object(okx, "get_price_by_type", return_value=Decimal("100")), \
                patch.object(okx, "_request_order_update", new_callable=AsyncMock, return_value=terminal), \
                patch.object(okx, "_request_order_fills", new_callable=AsyncMock, return_value=fills):
            await self.settle_expiry_reconciliation(executor)
            # Exercise the native tracker cancellation event before its missing fill is visible.
            await okx._order_tracker.process_order_update(OrderUpdate(
                trading_pair=order.trading_pair, update_timestamp=1234567921,
                new_state=OrderState.CANCELED, client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id))
            self.assertIsNone(executor._open_order)
            self.assertEqual(executor.open_filled_amount, Decimal("0"))
            await self.settle_expiry_reconciliation(executor)
            self.assertEqual(executor.open_filled_amount, Decimal("0.1998"))
            self.assertEqual(executor.status, RunnableStatus.RUNNING)
            self.assertIsNone(executor.close_type)
            self.assertEqual(executor.get_custom_info()["order_ids"], [order.client_order_id])
            # Duplicate exchange receipts are deduplicated by the native tracker.
            await okx._request_order_status(tracked_order=order)
            self.assertEqual(executor.open_filled_amount, Decimal("0.1998"))
            self.strategy.buy.assert_not_called()
            self.strategy.sell.assert_not_called()
        executor.unregister_events()
        missing_order = InFlightOrder(
            client_order_id="OID-MISSING", exchange_order_id="EO-MISSING", trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1"),
            price=Decimal("100"), creation_timestamp=1234567890, initial_state=OrderState.OPEN)
        okx._order_tracker.start_tracking_order(missing_order)
        terminal["data"][0].update(ordId="EO-MISSING", clOrdId="OID-MISSING")
        with patch.object(okx, "_request_order_update", new_callable=AsyncMock, return_value=terminal), \
                patch.object(okx, "_request_order_fills", new_callable=AsyncMock, return_value={"code": "0", "data": []}):
            with self.assertRaisesRegex(IOError, "Terminal fill reconciliation pending"):
                await okx._request_order_status(tracked_order=missing_order)
        self.assertEqual(missing_order.executed_amount_base, Decimal("0"))

    async def test_empty_expiry_cannot_bypass_reconciliation_via_time_or_retry_limit(self):
        executor, order, connector = self.expired_entry_fixture()
        await self.settle_expiry_reconciliation(executor)
        self.acknowledge_entry_cancel(executor, order)
        connector._request_order_status.side_effect = IOError("fill total unavailable")
        executor.config.triple_barrier_config.time_limit = 30
        executor._current_retries = executor._max_retries + 1
        await self.settle_expiry_reconciliation(executor)
        self.assertEqual(executor.status, RunnableStatus.RUNNING)
        self.assertIsNone(executor.close_type)
        self.strategy.buy.assert_not_called()
        self.strategy.sell.assert_not_called()

    async def test_entry_failure_after_expiry_does_not_release_unknown_inventory(self):
        executor, order, connector = self.expired_entry_fixture()
        await self.settle_expiry_reconciliation(executor)
        order.update_with_order_update(OrderUpdate(
            trading_pair=order.trading_pair, update_timestamp=1234567921,
            new_state=OrderState.FAILED, client_order_id=order.client_order_id))
        event = MarketOrderFailureEvent(timestamp=1234567921, order_id=order.client_order_id, order_type=OrderType.LIMIT)
        executor.process_order_failed_event(None, None, event)
        connector._request_order_status.side_effect = IOError("terminal state unavailable")
        await self.settle_expiry_reconciliation(executor)
        self.assertEqual(executor.status, RunnableStatus.RUNNING)
        self.assertIsNone(executor.close_type)
        self.strategy.buy.assert_not_called()
        self.strategy.sell.assert_not_called()

    async def test_expiry_status_request_does_not_delay_new_fill_barriers(self):
        executor, order, connector = self.expired_entry_fixture()
        await executor.control_task()
        self.acknowledge_entry_cancel(executor, order)
        started = asyncio.Event()
        blocked = asyncio.Event()

        async def delayed_status(tracked_order):
            started.set()
            await blocked.wait()

        connector._request_order_status.side_effect = delayed_status
        await asyncio.wait_for(executor.control_task(), timeout=0.1)
        await asyncio.wait_for(started.wait(), timeout=0.1)
        pending_task = executor._entry_expiry_reconciliation_task
        order.update_with_trade_update(self.entry_trade(order, Decimal("0.2")))
        connector.get_price_by_type.return_value = Decimal("110")
        await asyncio.wait_for(executor.control_task(), timeout=0.1)
        await asyncio.gather(pending_task, return_exceptions=True)
        self.assertTrue(pending_task.cancelled())
        self.assertIsNotNone(executor._trailing_stop_trigger_pct)
        self.assertTrue(executor.is_trading)
        self.assertIsNone(executor._entry_expiry_reconciliation_task)

    async def test_expiry_terminal_release_requires_connector_fill_proof_capability(self):
        executor, order, connector = self.expired_entry_fixture()
        connector.supports_terminal_fill_reconciliation = False
        await self.settle_expiry_reconciliation(executor)
        self.acknowledge_entry_cancel(executor, order)
        await self.settle_expiry_reconciliation(executor)
        self.assertEqual(executor.status, RunnableStatus.RUNNING)
        self.assertIsNone(executor.close_type)
        connector._request_order_status.assert_not_awaited()

    async def settle_expiry_reconciliation(self, executor):
        await executor.control_task()
        task = executor._entry_expiry_reconciliation_task
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
            await executor.control_task()

    async def test_stop_cancels_pending_expiry_reconciliation(self):
        executor, order, connector = self.expired_entry_fixture()
        await executor.control_task()
        self.acknowledge_entry_cancel(executor, order)
        started = asyncio.Event()

        async def pending_status(tracked_order):
            started.set()
            await asyncio.Event().wait()

        connector._request_order_status.side_effect = pending_status
        await executor.control_task()
        task = executor._entry_expiry_reconciliation_task
        await asyncio.wait_for(started.wait(), timeout=0.1)
        executor.stop()
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.cancelled())
        self.assertIsNone(executor._entry_expiry_reconciliation_task)

    async def test_expired_filled_owner_persists_and_blocks_fresh_execution_after_restart(self):
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace

        from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
        from hummingbot.client.config.config_helpers import ClientConfigAdapter
        from hummingbot.connector.markets_recorder import MarketsRecorder
        from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
        from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
        from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction

        executor, order, connector = self.expired_entry_fixture(Decimal("0.2"))
        executor.config.controller_id = "expiry-controller"
        await executor.control_task()
        self.acknowledge_entry_cancel(executor, order)
        await executor.control_task()
        self.strategy.controllers = {"expiry-controller": SimpleNamespace(config=SimpleNamespace(
            connector_name="binance", trading_pair="ETH-USDT"))}
        self.strategy.markets = {"binance": {"ETH-USDT"}}
        with TemporaryDirectory() as directory:
            manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                           db_path=directory + "/expiry.sqlite")
            previous_recorder = MarketsRecorder._shared_instance
            recorder = MarketsRecorder(manager, [], "expiry.yml", "v2_with_controllers",
                                       market_data_collection=MarketDataCollectionConfigMap(market_data_collection_enabled=False))
            try:
                recorder.store_or_update_executor(executor)
                stored = recorder.get_all_executors()[0]
                self.assertFalse(stored.is_done)
                self.assertTrue(stored.custom_info["open_order_entry_expired"])
                self.assertEqual(Decimal(stored.custom_info["open_order_filled_amount_base"]), Decimal("0.1998"))
                self.assertEqual(stored.custom_info["order_ids"], [order.client_order_id])
                with patch.object(MarketsRecorder, "get_instance", return_value=recorder):
                    restarted = ExecutorOrchestrator(self.strategy)
                    self.assertEqual(restarted.recovery_required, {"expiry-controller": [executor.config.id]})
                    with self.assertRaisesRegex(RuntimeError, "startup reconciliation"):
                        restarted.create_executor(CreateExecutorAction(
                            controller_id="expiry-controller", executor_config=executor.config))
                self.strategy.buy.assert_not_called()
                self.strategy.sell.assert_not_called()
            finally:
                MarketsRecorder._shared_instance = previous_recorder
                manager.engine.dispose()

    async def test_early_stop_cannot_release_empty_expiry_without_terminal_reconciliation(self):
        for keep_position in (False, True):
            with self.subTest(keep_position=keep_position):
                self.strategy = self.create_mock_strategy
                executor, order, connector = self.expired_entry_fixture()
                await executor.control_task()
                self.acknowledge_entry_cancel(executor, order)
                connector.supports_terminal_fill_reconciliation = False
                executor.early_stop(keep_position=keep_position)
                with patch.object(executor, "_sleep", new_callable=AsyncMock):
                    await self.settle_expiry_reconciliation(executor)
                self.assertEqual(executor.status, RunnableStatus.SHUTTING_DOWN)
                self.assertFalse(executor.executor_info.is_done)
                self.assertFalse(executor._open_order_expiry_reconciled)
                self.strategy.buy.assert_not_called()
                self.strategy.sell.assert_not_called()

    async def test_early_stop_empty_expiry_completes_after_authoritative_reconciliation(self):
        for keep_position in (False, True):
            with self.subTest(keep_position=keep_position):
                self.strategy = self.create_mock_strategy
                executor, order, connector = self.expired_entry_fixture()
                await executor.control_task()
                self.acknowledge_entry_cancel(executor, order)
                executor.early_stop(keep_position=keep_position)
                with patch.object(executor, "_sleep", new_callable=AsyncMock):
                    await self.settle_expiry_reconciliation(executor)
                self.assertTrue(executor._open_order_expiry_reconciled)
                self.assertTrue(executor.executor_info.is_done)
                self.assertEqual(executor.close_type, CloseType.EARLY_STOP)
                self.strategy.buy.assert_not_called()
                self.strategy.sell.assert_not_called()

    async def test_late_fill_during_early_stop_reconciliation_still_closes_requested_inventory(self):
        executor, order, connector = self.expired_entry_fixture()
        await executor.control_task()
        self.acknowledge_entry_cancel(executor, order)
        started = asyncio.Event()

        async def pending_status(tracked_order):
            started.set()
            await asyncio.Event().wait()

        connector._request_order_status.side_effect = pending_status
        executor.early_stop()
        with patch.object(executor, "_sleep", new_callable=AsyncMock):
            await executor.control_task()
            await asyncio.wait_for(started.wait(), timeout=.1)
            task = executor._entry_expiry_reconciliation_task
            order.update_with_trade_update(self.entry_trade(order, Decimal("0.2")))
            await asyncio.wait_for(executor.control_task(), timeout=.1)
            await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.cancelled())
        self.assertEqual(executor.status, RunnableStatus.SHUTTING_DOWN)
        self.assertEqual(executor.close_type, CloseType.EARLY_STOP)
        self.assertEqual(self.strategy.sell.call_args.args[2], Decimal("0.1998"))
        self.assertEqual(self.strategy.sell.call_args.args[3], OrderType.MARKET)
