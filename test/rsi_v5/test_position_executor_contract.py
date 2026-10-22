from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
)
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
        executor.get_in_flight_order = MagicMock(
            side_effect=lambda _, order_id: next(
                (
                    tracked.order
                    for tracked in (
                        executor._open_order,
                        executor._close_order,
                        executor._take_profit_limit_order,
                    )
                    if tracked is not None and tracked.order_id == order_id
                ),
                None,
            )
        )
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

    def _fill_event(
        self,
        *,
        order_id: str,
        trade_id: str,
        amount: Decimal,
        trade_type: TradeType = TradeType.BUY,
    ) -> OrderFilledEvent:
        return OrderFilledEvent(
            timestamp=1_700_000_001.0,
            order_id=order_id,
            trading_pair=self.trading_pair,
            trade_type=trade_type,
            order_type=OrderType.LIMIT_MAKER,
            price=Decimal("100"),
            amount=amount,
            trade_fee=AddedToCostTradeFee(
                flat_fees=[TokenAmount(token="USD", amount=Decimal("0.01"))]
            ),
            exchange_trade_id=trade_id,
            exchange_order_id=f"EX-{order_id}",
        )

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

    def test_duplicate_entry_completion_is_a_callback_noop(self):
        executor = self._executor("duplicate-completion")
        self._open_order(
            executor,
            order_id="OPEN-DUPLICATE",
            configured_amount=Decimal("0.4"),
            fill_amount=Decimal("0.4"),
            state=OrderState.FILLED,
        )
        event = BuyOrderCompletedEvent(
            timestamp=1_700_000_002.0,
            order_id="OPEN-DUPLICATE",
            base_asset="BTC",
            quote_asset="USD",
            base_asset_amount=Decimal("0.4"),
            quote_asset_amount=Decimal("40"),
            order_type=OrderType.LIMIT_MAKER,
            exchange_order_id="EX-OPEN-DUPLICATE",
        )

        executor.process_order_completed_event(0, self.strategy.connectors[self.connector_name], event)
        executor.process_order_completed_event(0, self.strategy.connectors[self.connector_name], event)

        self.assertEqual(executor._total_executed_amount_backup, Decimal("0.4"))
        self.assertEqual(len(executor.lifecycle_state.events), 1)

    def test_cancel_then_late_fill_preserves_reducer_owned_exposure(self):
        executor = self._executor("cancel-late-fill")
        self._open_order(
            executor,
            order_id="OPEN-LATE",
            configured_amount=Decimal("1"),
            fill_amount=Decimal("0"),
            state=OrderState.OPEN,
        )

        executor.process_order_canceled_event(
            0,
            self.strategy.connectors[self.connector_name],
            OrderCancelledEvent(
                timestamp=1_700_000_002.0,
                order_id="OPEN-LATE",
                exchange_order_id="EX-OPEN-LATE",
            ),
        )
        executor.process_order_filled_event(
            0,
            self.strategy.connectors[self.connector_name],
            self._fill_event(
                order_id="OPEN-LATE",
                trade_id="TRADE-LATE",
                amount=Decimal("0.4"),
            ),
        )

        self.assertEqual(executor.lifecycle_state.filled_base, Decimal("0.4"))
        self.assertEqual(executor.lifecycle_state.filled_quote, Decimal("40"))
        self.assertTrue(executor.lifecycle_state.entry_cancelled)
        self.assertEqual(executor.open_filled_amount, Decimal("0.4"))
        self.assertEqual(executor.amount_to_close, Decimal("0.4"))

    def test_entry_fill_and_cancel_callback_order_converges(self):
        states = []
        for cancel_first in (False, True):
            executor = self._executor("entry-cancel-order")
            self._open_order(
                executor,
                order_id="OPEN-ENTRY-CANCEL",
                configured_amount=Decimal("1"),
                fill_amount=Decimal("0"),
                state=OrderState.OPEN,
            )
            fill = self._fill_event(
                order_id="OPEN-ENTRY-CANCEL",
                trade_id="TRADE-ENTRY-CANCEL",
                amount=Decimal("0.4"),
            )
            cancel = OrderCancelledEvent(
                timestamp=1_700_000_002.0,
                order_id="OPEN-ENTRY-CANCEL",
                exchange_order_id="EX-OPEN-ENTRY-CANCEL",
            )
            callbacks = (
                (executor.process_order_canceled_event, cancel),
                (executor.process_order_filled_event, fill),
            ) if cancel_first else (
                (executor.process_order_filled_event, fill),
                (executor.process_order_canceled_event, cancel),
            )

            for callback, event in callbacks:
                callback(0, self.strategy.connectors[self.connector_name], event)
            states.append(executor.lifecycle_state)

        self.assertEqual(states[0], states[1])
        self.assertEqual(states[0].filled_base, Decimal("0.4"))
        self.assertTrue(states[0].entry_cancelled)
        self.assertFalse(states[0].reconciliation_required)

    def test_conflicting_fill_callback_enters_position_hold_without_overwrite(self):
        executor = self._executor("conflicting-fill")
        self._open_order(
            executor,
            order_id="OPEN-CONFLICT",
            configured_amount=Decimal("1"),
            fill_amount=Decimal("0"),
            state=OrderState.OPEN,
        )
        first = self._fill_event(
            order_id="OPEN-CONFLICT",
            trade_id="TRADE-CONFLICT",
            amount=Decimal("0.4"),
        )
        conflicting = first._replace(amount=Decimal("0.6"))

        executor.process_order_filled_event(0, self.strategy.connectors[self.connector_name], first)
        executor.process_order_filled_event(0, self.strategy.connectors[self.connector_name], conflicting)

        self.assertTrue(executor.lifecycle_state.reconciliation_required)
        self.assertEqual(executor.lifecycle_state.filled_base, Decimal("0.4"))
        self.assertEqual(executor.status, RunnableStatus.SHUTTING_DOWN)
        self.assertEqual(executor.close_type, CloseType.POSITION_HOLD)

    async def test_reconciliation_hold_accepts_cancel_bookkeeping_but_keeps_unowned_residual(self):
        executor = self._executor("conflict-cancel")
        self._open_order(
            executor,
            order_id="OPEN-CONFLICT-CANCEL",
            configured_amount=Decimal("1"),
            fill_amount=Decimal("0"),
            state=OrderState.OPEN,
        )
        first = self._fill_event(
            order_id="OPEN-CONFLICT-CANCEL",
            trade_id="TRADE-CONFLICT-CANCEL",
            amount=Decimal("0.4"),
        )
        conflicting = first._replace(amount=Decimal("0.6"))
        executor.process_order_filled_event(0, self.strategy.connectors[self.connector_name], first)
        executor.process_order_filled_event(0, self.strategy.connectors[self.connector_name], conflicting)

        executor.process_order_canceled_event(
            0,
            self.strategy.connectors[self.connector_name],
            OrderCancelledEvent(
                timestamp=1_700_000_003.0,
                order_id="OPEN-CONFLICT-CANCEL",
                exchange_order_id="EX-OPEN-CONFLICT-CANCEL",
            ),
        )
        with patch.object(executor, "_sleep", new=AsyncMock()):
            await executor.control_task()

        self.assertIsNone(executor._open_order)
        self.assertEqual(executor.status, RunnableStatus.SHUTTING_DOWN)
        self.assertEqual(executor.close_type, CloseType.POSITION_HOLD)
        self.assertEqual(executor.open_filled_amount, Decimal("0.4"))
        self.strategy.sell.assert_not_called()

    def test_duplicate_fill_and_fee_callback_is_idempotent(self):
        executor = self._executor("duplicate-fill")
        self._open_order(
            executor,
            order_id="OPEN-FILL-DUPLICATE",
            configured_amount=Decimal("1"),
            fill_amount=Decimal("0"),
            state=OrderState.OPEN,
        )
        event = self._fill_event(
            order_id="OPEN-FILL-DUPLICATE",
            trade_id="TRADE-FILL-DUPLICATE",
            amount=Decimal("0.4"),
        )

        executor.process_order_filled_event(0, self.strategy.connectors[self.connector_name], event)
        executor.process_order_filled_event(0, self.strategy.connectors[self.connector_name], event)

        self.assertEqual(executor.lifecycle_state.filled_base, Decimal("0.4"))
        self.assertEqual(executor.lifecycle_state.fee_adjustment_quote, Decimal("0.01"))
        self.assertEqual(len(executor.lifecycle_state.events), 2)
        self.assertFalse(executor.lifecycle_state.reconciliation_required)

    def test_entry_and_close_fill_callback_order_converges(self):
        states = []
        for close_first in (False, True):
            executor = self._executor("fill-order")
            self._open_order(
                executor,
                order_id="OPEN-ORDERED",
                configured_amount=Decimal("0.4"),
                fill_amount=Decimal("0"),
                state=OrderState.OPEN,
            )
            open_order = executor._open_order
            self._open_order(
                executor,
                order_id="CLOSE-ORDERED",
                configured_amount=Decimal("0.4"),
                fill_amount=Decimal("0"),
                state=OrderState.OPEN,
            )
            executor._close_order = executor._open_order
            executor._open_order = open_order
            entry_fill = self._fill_event(
                order_id="OPEN-ORDERED",
                trade_id="ENTRY-TRADE",
                amount=Decimal("0.4"),
            )
            close_fill = self._fill_event(
                order_id="CLOSE-ORDERED",
                trade_id="CLOSE-TRADE",
                amount=Decimal("0.4"),
                trade_type=TradeType.SELL,
            )
            callbacks = (close_fill, entry_fill) if close_first else (entry_fill, close_fill)

            for callback in callbacks:
                executor.process_order_filled_event(
                    0,
                    self.strategy.connectors[self.connector_name],
                    callback,
                )
            states.append(executor.lifecycle_state)

        self.assertEqual(states[0], states[1])
        self.assertEqual(states[0].filled_base, Decimal("0.4"))
        self.assertEqual(states[0].close_filled_base, Decimal("0.4"))
        self.assertFalse(states[0].reconciliation_required)

    def test_fill_without_exchange_trade_id_preserves_amount_and_holds(self):
        executor = self._executor("missing-trade-id")
        self._open_order(
            executor,
            order_id="OPEN-MISSING-TRADE-ID",
            configured_amount=Decimal("1"),
            fill_amount=Decimal("0"),
            state=OrderState.OPEN,
        )
        event = self._fill_event(
            order_id="OPEN-MISSING-TRADE-ID",
            trade_id="",
            amount=Decimal("0.4"),
        )

        executor.process_order_filled_event(0, self.strategy.connectors[self.connector_name], event)

        self.assertEqual(executor.lifecycle_state.filled_base, Decimal("0.4"))
        self.assertTrue(executor.lifecycle_state.reconciliation_required)
        self.assertEqual(executor.status, RunnableStatus.SHUTTING_DOWN)
        self.assertEqual(executor.close_type, CloseType.POSITION_HOLD)

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
