"""Bounded native order execution for the opt-in inventory episode policy."""

from decimal import Decimal

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.order_executor.data_types import OrderExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType

ZERO = Decimal("0")
EPISODE_LEVEL_PREFIX = "ok_rsi_episode:"


def actual_fees(native):
    base, quote = native.trading_pair.split("-")
    base_fee = quote_fee = ZERO
    errors = []
    for fill in native.order_fills.values():
        fee = fill.fee
        amounts = [(item.token, item.amount) for item in fee.flat_fees]
        if fee.percent:
            token = fee.percent_token
            if token is None:
                deducted = fee.type_descriptor_for_json() == "DeductedFromReturns"
                token = base if (native.trade_type == TradeType.BUY) == deducted else quote
            if token == base:
                amounts.append((base, fill.fill_base_amount * fee.percent))
            elif token == quote:
                amounts.append((quote, fill.fill_quote_amount * fee.percent))
            else:
                errors.append(f"unsupported_fee_asset:{token}")
        for token, amount in amounts:
            if not amount.is_finite():
                errors.append("nonfinite_fee")
            elif token == base:
                base_fee += amount
            elif token == quote:
                quote_fee += amount
            else:
                errors.append(f"unsupported_fee_asset:{token}")
    return base_fee, quote_fee, errors


class EpisodeOrderExecutorConfig(OrderExecutorConfig):
    episode_id: str
    episode_expires_at: int
    episode_revision: int = 0
    episode_level: int = -1
    episode_intent: str = "UNSPECIFIED"


class EpisodeOrderExecutor(OrderExecutor):
    """One order attempt per durable request, with native fill receipts.

    Cancellation/failure never retries the original amount. A later request is
    issued only by the controller after terminal reconciliation. Existing RSI
    executors continue using the unmodified native implementation.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._episode_orders = {}
        self._episode_order_ids = set()
        self._episode_submitted = False

    def _remember_orders(self):
        for tracked in [self._order, *self._partial_filled_orders, *self._canceled_orders, *self._failed_orders]:
            if tracked is None:
                continue
            self._episode_order_ids.add(tracked.order_id)
            if tracked.order is None:
                native = self.get_in_flight_order(self.config.connector_name, tracked.order_id)
                if native is not None:
                    tracked.order = native
            if tracked.order is not None:
                self._episode_orders[tracked.order_id] = tracked.order

    def place_open_order(self):
        if self._episode_submitted:
            self._status = RunnableStatus.SHUTTING_DOWN
            return
        self._episode_submitted = True
        super().place_open_order()

    async def control_task(self):
        expiry = getattr(self.config, "episode_expires_at", None)
        if expiry is not None and self._strategy.current_timestamp >= expiry:
            self._status = RunnableStatus.SHUTTING_DOWN
        await super().control_task()

    def update_tracked_order_with_order_id(self, order_id):
        super().update_tracked_order_with_order_id(order_id)
        self._remember_orders()
        if order_id in self._episode_orders:
            native = self.get_in_flight_order(self.config.connector_name, order_id)
            if native is not None:
                self._episode_orders[order_id] = native

    def process_order_canceled_event(self, _, market, event):
        self._remember_orders()
        if self._order and event.order_id == self._order.order_id:
            super().process_order_canceled_event(_, market, event)
            self._status = RunnableStatus.SHUTTING_DOWN

    def process_order_failed_event(self, _, market, event):
        self._remember_orders()
        if self._order and event.order_id == self._order.order_id:
            # The native failure path can retry a full amount after a partial
            # fill. Keep the native order object until its status is terminal.
            self._status = RunnableStatus.SHUTTING_DOWN

    def process_order_completed_event(self, _, market, event):
        self.update_tracked_order_with_order_id(event.order_id)
        if event.order_id in self._episode_orders:
            self._status = RunnableStatus.SHUTTING_DOWN

    def _receipt(self):
        self._remember_orders()
        gross_base = gross_quote = base_fee = quote_fee = ZERO
        errors = []
        for native in self._episode_orders.values():
            gross_base += native.executed_amount_base
            gross_quote += native.executed_amount_quote
            fill_base = sum((fill.fill_base_amount for fill in native.order_fills.values()), ZERO)
            fill_quote = sum((fill.fill_quote_amount for fill in native.order_fills.values()), ZERO)
            if fill_base != native.executed_amount_base or fill_quote != native.executed_amount_quote:
                errors.append("incomplete_fill_records")
            native_base_fee, native_quote_fee, native_errors = actual_fees(native)
            base_fee += native_base_fee
            quote_fee += native_quote_fee
            errors.extend(native_errors)
        unresolved = self._episode_order_ids - self._episode_orders.keys()
        errors.extend(f"unresolved_native_order:{order_id}" for order_id in unresolved)
        all_done = all(native.is_done for native in self._episode_orders.values())
        return {
            "gross_base": str(gross_base), "gross_quote": str(gross_quote),
            "base_fee": str(base_fee), "quote_fee": str(quote_fee),
            "intent": getattr(self.config, "episode_intent", "UNSPECIFIED"),
            "fees_known": not errors,
            "final": self.status == RunnableStatus.TERMINATED and all_done and not errors,
            "errors": sorted(set(errors)),
        }

    def _held_records(self):
        records = []
        for native in self._episode_orders.values():
            if native.executed_amount_base <= ZERO:
                continue
            # Preserve actual exchange fill facts. Do not call cumulative_fee_paid:
            # it may silently return partial totals when conversion is unavailable.
            base_fee, quote_fees, errors = actual_fees(native)
            if errors:
                raise ValueError("cannot materialize held inventory with unknown fees")
            records.append({
                "client_order_id": native.client_order_id, "exchange_order_id": native.exchange_order_id,
                "trading_pair": native.trading_pair, "trade_type": native.trade_type.name,
                "order_type": native.order_type.name, "price": str(native.price),
                "amount": str(native.amount), "executed_amount_base": str(native.executed_amount_base),
                "executed_amount_quote": str(native.executed_amount_quote),
                "cumulative_fee_paid_quote": str(quote_fees), "actual_base_fee": str(base_fee),
                "creation_timestamp": native.creation_timestamp,
                "last_update_timestamp": native.last_update_timestamp,
                "episode_intent": getattr(self.config, "episode_intent", "UNSPECIFIED"),
                "order_fills": {key: fill.to_json() for key, fill in native.order_fills.items()},
            })
        return records

    async def control_shutdown_process(self):
        self._remember_orders()
        if self._episode_order_ids - self._episode_orders.keys():
            return
        if any(not native.is_done for native in self._episode_orders.values()):
            if self._order is not None and self._order.is_open:
                self.cancel_order()
            return
        receipt = self._receipt()
        if not receipt["fees_known"]:
            return
        self._held_position_orders = self._held_records()
        self.close_type = CloseType.POSITION_HOLD if self._held_position_orders else CloseType.EARLY_STOP
        self.stop()

    def get_custom_info(self):
        result = super().get_custom_info()
        result["episode_receipt"] = self._receipt()
        return result

    @property
    def filled_amount_quote(self):
        return Decimal(self._receipt()["gross_quote"])

    def get_cum_fees_quote(self):
        self._remember_orders()
        if not self._receipt()["fees_known"]:
            return ZERO  # Explicit fees_known=false is the admission boundary.
        return sum((Decimal(record["cumulative_fee_paid_quote"]) for record in self._held_records()), ZERO)
