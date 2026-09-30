"""Meridian's native bag executor adds only the per-pair quote-age boundary."""
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor


class MeridianRecoveryExecutor(OrderExecutor):
    def _quote_permitted(self):
        owner = self._strategy.controllers.get(self.config.controller_id)
        return owner is not None and owner.quote_permission(self.config.trading_pair) is True

    def control_order(self):
        if not self._quote_permitted():
            # Cancellation already requested must still reconcile on stale data.
            if self._cancel_pending:
                self.cancel_order()
            return
        super().control_order()

    def place_open_order(self):
        if self._quote_permitted():
            super().place_open_order()
