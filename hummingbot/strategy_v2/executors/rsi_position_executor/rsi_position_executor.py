from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.executors.rsi_position_executor.data_types import RSIPositionExecutorConfig
from hummingbot.strategy_v2.executors.rsi_position_executor.lifecycle import (
    LifecycleEvent,
    LifecycleState,
    reduce_lifecycle_events,
)
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
        self._order_roles: Dict[str, str] = {}
        self._lifecycle_state = LifecycleState.initial(config.id)
        super().__init__(
            strategy=strategy,
            config=config,
            update_interval=update_interval,
            max_retries=max_retries,
        )

    @property
    def lifecycle_state(self) -> LifecycleState:
        return self._lifecycle_state

    def _remember_order_role(self, order_id: str) -> Optional[str]:
        role = self._order_roles.get(order_id)
        if role is not None:
            return role
        candidates = (
            ("entry", self._open_order),
            ("close", self._close_order),
            ("close", self._take_profit_limit_order),
            *(("entry", order) for order in self._archived_open_orders),
            *(("close", order) for order in self._completed_close_orders),
            *((self._order_roles.get(order.order_id, "unknown"), order) for order in self._failed_orders),
        )
        for candidate_role, tracked_order in candidates:
            if tracked_order is not None and tracked_order.order_id == order_id:
                if candidate_role != "unknown":
                    self._order_roles[order_id] = candidate_role
                    return candidate_role
                return None
        return None

    def _enter_lifecycle_reconciliation_hold(self, event_ids: List[str]) -> None:
        if self.close_type != CloseType.POSITION_HOLD or self.status != RunnableStatus.SHUTTING_DOWN:
            self.logger().error(
                self._log_message(
                    "RSI lifecycle reconciliation hold "
                    f"event_ids={event_ids} "
                    f"conflicts={list(self._lifecycle_state.conflicting_event_ids)}"
                )
            )
        self.close_type = CloseType.POSITION_HOLD
        self._status = RunnableStatus.SHUTTING_DOWN

    def _accept_lifecycle_events(self, events: List[LifecycleEvent]) -> bool:
        previous = self._lifecycle_state
        current = reduce_lifecycle_events(previous, events)
        if current == previous:
            return False
        self._lifecycle_state = current
        new_conflict = current.conflicting_event_ids != previous.conflicting_event_ids
        unsafe_event = any(
            event.kind in {"FAILURE", "TIMEOUT"}
            or (
                event.kind in {"ENTRY_FILL", "CLOSE_FILL"}
                and (event.amount < Decimal("0") or event.quote_amount < Decimal("0"))
            )
            for event in events
        )
        if current.reconciliation_required:
            self._enter_lifecycle_reconciliation_hold([event.event_id for event in events])
        if unsafe_event or new_conflict:
            return False
        return True

    @staticmethod
    def _fill_event_id(event: OrderFilledEvent) -> str:
        trade_id = str(event.exchange_trade_id or "").strip()
        if not trade_id:
            trade_id = ":".join(
                (
                    str(event.timestamp),
                    str(event.price),
                    str(event.amount),
                )
            )
        return f"fill:{event.order_id}:{trade_id}"

    def _fee_lifecycle_event(
        self,
        market: ConnectorBase,
        event: OrderFilledEvent,
        fill_event_id: str,
    ) -> Optional[LifecycleEvent]:
        quote_asset = self.config.trading_pair.split("-")[1]
        try:
            fee_quote = event.trade_fee.fee_amount_in_token(
                trading_pair=event.trading_pair,
                price=event.price,
                order_amount=event.amount,
                token=quote_asset,
                exchange=market,
            )
        except Exception as exc:
            self.logger().warning(
                self._log_message(
                    f"Deferring callback fee conversion event_id={fill_event_id} "
                    f"error={type(exc).__name__}"
                )
            )
            return None
        if fee_quote == Decimal("0"):
            return None
        return LifecycleEvent(
            event_id=fill_event_id.replace("fill:", "fee:", 1),
            kind="FEE_ADJUSTMENT",
            quote_amount=fee_quote,
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
        tracked_total = Decimal("0")
        for order in self._open_orders_with_fills():
            amount = order.executed_amount_base
            if order.fee_asset == base_asset:
                amount -= order.cum_fees_base
            tracked_total += max(amount, Decimal("0"))
        total = max(tracked_total, self._lifecycle_state.filled_base)
        return self.connectors[self.config.connector_name].quantize_order_amount(
            trading_pair=self.config.trading_pair,
            amount=max(total, Decimal("0")),
        )

    @property
    def open_filled_amount_quote(self) -> Decimal:
        tracked_quote = sum(
            (order.executed_amount_quote for order in self._open_orders_with_fills()),
            Decimal("0"),
        )
        return max(tracked_quote, self._lifecycle_state.filled_quote)

    @property
    def close_filled_amount(self) -> Decimal:
        return max(super().close_filled_amount, self._lifecycle_state.close_filled_base)

    @property
    def close_filled_amount_quote(self) -> Decimal:
        return max(super().close_filled_amount_quote, self._lifecycle_state.close_filled_quote)

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
        tracked_fees = sum(
            (order.cum_fees_quote for order in orders if order is not None),
            Decimal("0"),
        )
        return max(tracked_fees, self._lifecycle_state.fee_adjustment_quote)

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
            lifecycle_residual = max(
                self._lifecycle_state.filled_base - self._lifecycle_state.close_filled_base,
                Decimal("0"),
            )
            if lifecycle_residual > Decimal("0") and not self._held_position_orders:
                self.logger().error(
                    self._log_message(
                        "RSI lifecycle hold has known residual exposure without "
                        f"a serializable owner order residual_base={lifecycle_residual}"
                    )
                )
                await self._sleep(5.0)
                return
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
        role = self._remember_order_role(event.order_id)
        kind = {
            "entry": "ENTRY_CANCEL_ACK",
            "close": "CLOSE_CANCEL_ACK",
        }.get(role, "FAILURE")
        if not self._accept_lifecycle_events(
            [LifecycleEvent(event_id=f"cancel:{event.order_id}", kind=kind)]
        ):
            return
        if self._open_order and event.order_id == self._open_order.order_id:
            if not self._archive_current_open_order():
                self._failed_orders.append(self._open_order)
                self._open_order = None
        super().process_order_canceled_event(event_tag, market, event)

    def process_order_created_event(
        self,
        event_tag,
        market,
        event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent],
    ):
        self._remember_order_role(event.order_id)
        super().process_order_created_event(event_tag, market, event)
        self._remember_order_role(event.order_id)

    def process_order_filled_event(
        self,
        event_tag,
        market: ConnectorBase,
        event: OrderFilledEvent,
    ):
        role = self._remember_order_role(event.order_id)
        kind = {
            "entry": "ENTRY_FILL",
            "close": "CLOSE_FILL",
        }.get(role, "FAILURE")
        fill_event_id = self._fill_event_id(event)
        lifecycle_events = [
            LifecycleEvent(
                event_id=fill_event_id,
                kind=kind,
                amount=event.amount,
                quote_amount=event.amount * event.price,
            )
        ]
        fee_event = self._fee_lifecycle_event(market, event, fill_event_id)
        if fee_event is not None:
            lifecycle_events.append(fee_event)
        if not str(event.exchange_trade_id or "").strip():
            lifecycle_events.append(
                LifecycleEvent(
                    event_id=f"missing-trade-id:{event.order_id}:{event.timestamp}",
                    kind="FAILURE",
                )
            )
        if not self._accept_lifecycle_events(lifecycle_events):
            return
        super().process_order_filled_event(event_tag, market, event)

    def process_order_completed_event(
        self,
        event_tag,
        market,
        event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent],
    ):
        role = self._remember_order_role(event.order_id)
        kind = {
            "entry": "ENTRY_COMPLETED",
            "close": "CLOSE_COMPLETED",
        }.get(role, "FAILURE")
        if not self._accept_lifecycle_events(
            [
                LifecycleEvent(
                    event_id=f"completed:{event.order_id}",
                    kind=kind,
                    amount=event.base_asset_amount,
                    quote_amount=event.quote_asset_amount,
                )
            ]
        ):
            return
        super().process_order_completed_event(event_tag, market, event)

    def process_order_failed_event(
        self,
        event_tag,
        market,
        event: MarketOrderFailureEvent,
    ):
        role = self._remember_order_role(event.order_id)
        kind = {
            "entry": "ENTRY_FAILURE",
            "close": "CLOSE_FAILURE",
        }.get(role, "FAILURE")
        if not self._accept_lifecycle_events(
            [LifecycleEvent(event_id=f"failure:{event.order_id}", kind=kind)]
        ):
            return
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
        info["lifecycle_event_count"] = len(self._lifecycle_state.events)
        info["lifecycle_reconciliation_required"] = self._lifecycle_state.reconciliation_required
        info["lifecycle_conflicting_event_ids"] = list(self._lifecycle_state.conflicting_event_ids)
        return info
