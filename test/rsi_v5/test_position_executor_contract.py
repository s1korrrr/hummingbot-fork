from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import MarketOrderFailureEvent, OrderCancelledEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.executors.rsi_position_executor import RSIPositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class TestRSIv5HyperliquidPositionExecutorContract(IsolatedAsyncioWrapperTestCase):
    connector_name = "hyperliquid_perpetual"
    trading_pair = "BTC-USD"

    def setUp(self) -> None:
        super().setUp()
        connector = MagicMock(spec=ExchangePyBase)
        connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
        connector._update_orders_with_error_handler = AsyncMock()

        self.strategy = MagicMock(spec=StrategyV2Base)
        type(self.strategy).current_timestamp = PropertyMock(return_value=1_700_000_000.0)
        self.strategy.connectors = {self.connector_name: connector}
        self.strategy.buy.return_value = "OPEN-ORDER"
        self.strategy.sell.side_effect = [
            "CLOSE-1",
            "CLOSE-2",
            "CLOSE-3",
            "CLOSE-4",
        ]
        self.strategy.cancel.return_value = None

        self.trading_rule = TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("0.0001"),
            min_price_increment=Decimal("0.1"),
            min_base_amount_increment=Decimal("0.0001"),
            min_notional_size=Decimal("5"),
        )

    def _config(
        self,
        executor_id: str,
        amount: Decimal = Decimal("1"),
    ) -> PositionExecutorConfig:
        return PositionExecutorConfig(
            id=executor_id,
            controller_id="btc-rsi-v5-hyperliquid",
            timestamp=1_700_000_000.0,
            connector_name=self.connector_name,
            trading_pair=self.trading_pair,
            side=TradeType.BUY,
            entry_price=Decimal("100"),
            amount=amount,
            leverage=1,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=Decimal("0.08"),
                open_order_type=OrderType.LIMIT_MAKER,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
        )

    def _executor(
        self,
        executor_id: str,
        amount: Decimal = Decimal("1"),
        max_retries: int = 3,
    ) -> PositionExecutor:
        executor_class = RSIPositionExecutor
        with patch.object(
            executor_class,
            "get_trading_rules",
            return_value=self.trading_rule,
        ), patch.object(
            executor_class,
            "get_price",
            return_value=Decimal("100"),
        ):
            executor = executor_class(
                strategy=self.strategy,
                config=self._config(executor_id, amount),
                max_retries=max_retries,
            )
        executor._status = RunnableStatus.RUNNING
        return executor

    @staticmethod
    def _open_order(
        executor: PositionExecutor,
        *,
        order_id: str,
        configured_amount: Decimal,
        fill_amount: Decimal,
        state: OrderState,
    ) -> None:
        tracked = TrackedOrder(order_id)
        tracked.order = InFlightOrder(
            client_order_id=order_id,
            exchange_order_id=f"EX-{order_id}",
            trading_pair=executor.config.trading_pair,
            order_type=OrderType.LIMIT_MAKER,
            trade_type=TradeType.BUY,
            amount=configured_amount,
            price=Decimal("100"),
            creation_timestamp=1_700_000_000.0,
            initial_state=state,
        )
        if fill_amount > Decimal("0"):
            tracked.order.update_with_trade_update(
                TradeUpdate(
                    trade_id=f"TRADE-{order_id}",
                    client_order_id=order_id,
                    exchange_order_id=f"EX-{order_id}",
                    trading_pair=executor.config.trading_pair,
                    fill_price=Decimal("100"),
                    fill_base_amount=fill_amount,
                    fill_quote_amount=fill_amount * Decimal("100"),
                    fee=AddedToCostTradeFee(
                        flat_fees=[
                            TokenAmount(
                                token="USDC",
                                amount=Decimal("0.01"),
                            )
                        ]
                    ),
                    fill_timestamp=1_700_000_001.0,
                )
            )
        executor._open_order = tracked

    def test_runtime_mapping_preserves_standard_executor_discriminator(self):
        config = self._config("typed")

        self.assertEqual(config.type, "position_executor")
        self.assertIs(
            ExecutorOrchestrator._executor_mapping["position_executor"],
            PositionExecutor,
        )

    def test_standard_executor_config_round_trips_through_executor_info(self):
        config = self._config("round-trip")
        info = ExecutorInfo(
            id=config.id,
            timestamp=float(config.timestamp),
            type=config.type,
            status=RunnableStatus.RUNNING,
            config=config,
            net_pnl_pct=Decimal("0"),
            net_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            filled_amount_quote=Decimal("0"),
            is_active=True,
            is_trading=False,
            custom_info={"side": TradeType.BUY},
            controller_id=config.controller_id,
        )

        restored = ExecutorInfo.model_validate(info.model_dump())

        self.assertEqual(restored.type, "position_executor")
        self.assertEqual(restored.config.type, "position_executor")
        self.assertEqual(restored.config.id, "round-trip")

    async def test_early_stop_unfilled_entry_cancels_without_close_order(self):
        executor = self._executor("unfilled")
        self._open_order(
            executor,
            order_id="OPEN-UNFILLED",
            configured_amount=Decimal("1"),
            fill_amount=Decimal("0"),
            state=OrderState.OPEN,
        )

        executor.early_stop(False)
        with patch.object(executor, "_sleep", new=AsyncMock()):
            await executor.control_task()

        self.strategy.cancel.assert_called_once_with(
            connector_name=self.connector_name,
            trading_pair=self.trading_pair,
            order_id="OPEN-UNFILLED",
        )
        self.strategy.sell.assert_not_called()

        executor.process_order_canceled_event(
            0,
            self.strategy.connectors[self.connector_name],
            OrderCancelledEvent(
                timestamp=1_700_000_002.0,
                order_id="OPEN-UNFILLED",
            ),
        )
        with patch.object(executor, "_sleep", new=AsyncMock()):
            await executor.control_task()

        self.assertEqual(executor.status, RunnableStatus.TERMINATED)
        self.strategy.sell.assert_not_called()

    async def test_early_stop_partial_fill_survives_entry_cancellation_and_closes_residual(self):
        executor = self._executor("partial")
        self._open_order(
            executor,
            order_id="OPEN-PARTIAL",
            configured_amount=Decimal("1"),
            fill_amount=Decimal("0.4"),
            state=OrderState.OPEN,
        )

        executor.early_stop(False)
        with patch.object(executor, "_sleep", new=AsyncMock()):
            await executor.control_task()
        executor.process_order_canceled_event(
            0,
            self.strategy.connectors[self.connector_name],
            OrderCancelledEvent(
                timestamp=1_700_000_002.0,
                order_id="OPEN-PARTIAL",
            ),
        )
        with patch.object(executor, "_sleep", new=AsyncMock()):
            await executor.control_task()

        sell_args = self.strategy.sell.call_args.args
        self.assertEqual(sell_args[0], self.connector_name)
        self.assertEqual(sell_args[1], self.trading_pair)
        self.assertEqual(sell_args[2], Decimal("0.4"))
        self.assertEqual(sell_args[3], OrderType.MARKET)
        self.assertEqual(sell_args[5], PositionAction.CLOSE)
        self.assertEqual(executor.open_filled_amount, Decimal("0.4"))
        self.assertEqual(executor.amount_to_close, Decimal("0.4"))
        self.assertEqual(executor.close_type, CloseType.EARLY_STOP)
        self.assertEqual(
            executor.get_custom_info()["archived_open_order_ids"],
            ["OPEN-PARTIAL"],
        )

    async def test_early_stop_filled_entry_places_owner_close_immediately(self):
        executor = self._executor("filled")
        self._open_order(
            executor,
            order_id="OPEN-FILLED",
            configured_amount=Decimal("1"),
            fill_amount=Decimal("1"),
            state=OrderState.FILLED,
        )

        executor.early_stop(False)
        with patch.object(executor, "_sleep", new=AsyncMock()):
            await executor.control_task()

        sell_args = self.strategy.sell.call_args.args
        self.assertEqual(sell_args[2], Decimal("1"))
        self.assertEqual(sell_args[5], PositionAction.CLOSE)

    async def test_two_oneway_executors_close_only_their_own_filled_amounts(self):
        scout = self._executor("scout", amount=Decimal("0.4"))
        runner = self._executor("runner", amount=Decimal("0.6"))
        self._open_order(
            scout,
            order_id="OPEN-SCOUT",
            configured_amount=Decimal("0.4"),
            fill_amount=Decimal("0.4"),
            state=OrderState.FILLED,
        )
        self._open_order(
            runner,
            order_id="OPEN-RUNNER",
            configured_amount=Decimal("0.6"),
            fill_amount=Decimal("0.6"),
            state=OrderState.FILLED,
        )

        scout.early_stop(False)
        runner.early_stop(False)
        with patch.object(scout, "_sleep", new=AsyncMock()), patch.object(
            runner,
            "_sleep",
            new=AsyncMock(),
        ):
            await scout.control_task()
            await runner.control_task()

        close_amounts = [call.args[2] for call in self.strategy.sell.call_args_list]
        close_actions = [call.args[5] for call in self.strategy.sell.call_args_list]
        self.assertEqual(close_amounts, [Decimal("0.4"), Decimal("0.6")])
        self.assertEqual(
            close_actions,
            [PositionAction.CLOSE, PositionAction.CLOSE],
        )

    async def test_failed_close_is_retried_for_same_residual_amount(self):
        executor = self._executor("retry", max_retries=3)
        self._open_order(
            executor,
            order_id="OPEN-RETRY",
            configured_amount=Decimal("0.25"),
            fill_amount=Decimal("0.25"),
            state=OrderState.FILLED,
        )
        executor.early_stop(False)
        with patch.object(executor, "_sleep", new=AsyncMock()):
            await executor.control_task()

        executor.process_order_failed_event(
            0,
            self.strategy.connectors[self.connector_name],
            MarketOrderFailureEvent(
                timestamp=1_700_000_002.0,
                order_id="CLOSE-1",
                order_type=OrderType.MARKET,
                error_message="temporary close failure",
                error_type="RuntimeError",
            ),
        )
        with patch.object(executor, "_sleep", new=AsyncMock()):
            await executor.control_task()

        close_amounts = [call.args[2] for call in self.strategy.sell.call_args_list]
        self.assertEqual(close_amounts, [Decimal("0.25"), Decimal("0.25")])
        self.assertEqual(executor.close_type, CloseType.EARLY_STOP)
