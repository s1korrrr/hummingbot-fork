from decimal import Decimal
from typing import Dict, List

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.event.events import MarketOrderFailureEvent, OrderCancelledEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.executors.rsi_position_executor.data_types import RSIPositionExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class RSIPositionExecutor(PositionExecutor):
    """PositionExecutor that preserves valid fills from cancelled open orders.

    During an early stop, an entry order may be partially filled and still open.
    Hummingbot first cancels the unfilled remainder. The base executor then drops
    the tracked open order on the cancellation event, which makes the partial
    fill disappear from ``amount_to_close``. This executor archives the filled
    portion before clearing the live order so shutdown can close the exact
    residual exposure owned by this executor.
    """

    def __init__(
        self,
        strategy: StrategyV2Base,
        config: RSIPositionExecutorConfig,
        update_interval: float = 1.0,
        max_retries: int = 10,
    ):
        self._archived_open_orders: List[TrackedOrder] = []
        super().__init__(
            strategy=strategy,
            config=config,
            update_interval=update_interval,
            max_retries=max_retries,
        )

    def _open_orders_with_fills(self) -> List[TrackedOrder]:
        orders = list(self._archived_open_orders)
        if self._open_order is not None:
            orders.append(self._open_order)
        return [order for order in orders if order.executed_amount_base > Decimal("0")]

    def _archive_current_open_order(self) -> bool:
        open_order = self._open_order
        if open_order is None or open_order.executed_amount_base <= Decimal("0"):
            return False
        if not any(order.order_id == open_order.order_id for order in self._archived_open_orders):
            self._archived_open_orders.append(open_order)
        self._open_order = None
        return True

    @property
    def open_filled_amount(self) -> Decimal:
        base_asset = self.config.trading_pair.split("-")[0]
        total = Decimal("0")
        for order in self._open_orders_with_fills():
            amount = order.executed_amount_base
            if order.fee_asset == base_asset:
                amount -= order.cum_fees_base
            total += max(amount, Decimal("0"))
        return self.connectors[self.config.connector_name].quantize_order_amount(
            trading_pair=self.config.trading_pair,
            amount=max(total, Decimal("0")),
        )

    @property
    def open_filled_amount_quote(self) -> Decimal:
        return sum(
            (order.executed_amount_quote for order in self._open_orders_with_fills()),
            Decimal("0"),
        )

    @property
    def entry_price(self) -> Decimal:
        amount = self.open_filled_amount
        quote = self.open_filled_amount_quote
        if amount > Decimal("0") and quote > Decimal("0"):
            return quote / amount
        return super().entry_price

    def get_cum_fees_quote(self) -> Decimal:
        orders = [
            *self._archived_open_orders,
            self._open_order,
            self._close_order,
            *self._completed_close_orders,
        ]
        return sum(
            (order.cum_fees_quote for order in orders if order is not None),
            Decimal("0"),
        )

    async def control_shutdown_process(self):
        if self.all_orders_completed() and self.close_type == CloseType.POSITION_HOLD:
            held_orders = [
                order
                for order in self._open_orders_with_fills()
                if order.order is not None
            ]
            if self._close_order and self._close_order.is_filled and self._close_order.order is not None:
                held_orders.append(self._close_order)
            existing_ids = {
                str(order.get("client_order_id"))
                for order in self._held_position_orders
            }
            for order in held_orders:
                payload = order.order.to_json()
                if str(payload.get("client_order_id")) not in existing_ids:
                    self._held_position_orders.append(payload)
                    existing_ids.add(str(payload.get("client_order_id")))
            if not self._held_position_orders:
                self.close_type = CloseType.EARLY_STOP
            self.stop()
            await self._sleep(5.0)
            return
        await super().control_shutdown_process()

    def process_order_canceled_event(
        self,
        event_tag,
        market: ConnectorBase,
        event: OrderCancelledEvent,
    ):
        if self._open_order and event.order_id == self._open_order.order_id:
            if not self._archive_current_open_order():
                self._failed_orders.append(self._open_order)
                self._open_order = None
        super().process_order_canceled_event(event_tag, market, event)

    def process_order_failed_event(
        self,
        event_tag,
        market,
        event: MarketOrderFailureEvent,
    ):
        if self._open_order and event.order_id == self._open_order.order_id:
            if self._archive_current_open_order():
                if self._is_permanent_validation_failure(event):
                    self.logger().error(
                        self._log_message(
                            f"Partially filled open order failed permanently "
                            f"order_id={event.order_id}; closing archived fill"
                        )
                    )
                    self.close_type = CloseType.EARLY_STOP
                    self._status = RunnableStatus.SHUTTING_DOWN
                    return
                self.logger().error(
                    self._log_message(
                        f"Partially filled open order failed order_id={event.order_id}; "
                        f"preserving fill and closing residual"
                    )
                )
                self.close_type = self.close_type or CloseType.EARLY_STOP
                self._status = RunnableStatus.SHUTTING_DOWN
                self._current_retries += 1
                return
        super().process_order_failed_event(event_tag, market, event)

    def get_custom_info(self) -> Dict:
        info = super().get_custom_info()
        archived_ids = [order.order_id for order in self._archived_open_orders]
        info["archived_open_order_ids"] = archived_ids
        info["order_ids"] = list(dict.fromkeys([*archived_ids, *info.get("order_ids", [])]))
        return info
