"""Controller-local tightening of the native PositionExecutor, not another OMS."""
from decimal import Decimal

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.executors import CloseType

from .config import TrailingPolicy
from .domain import PolicyError, decimal
from .trailing import protected_sell_price


def native_config(config, *, identifier, timestamp, price, amount, cycle, stage, market=None):
    activation, delta = config.trailing.activation, config.trailing.delta
    adaptive = getattr(config, 'adaptive_trailing', None)
    if adaptive is not None:
        # Live OK RSI selects the tranche geometry from the entry decision candle's ATR.
        if market is None or market.bag is None:
            raise PolicyError('adaptive trailing requires closed-candle ATR')
        activation, delta = adaptive.select(market.bag.atr_pct, activation, delta)
    return PositionExecutorConfig(
        id=identifier, controller_id=config.id, timestamp=timestamp,
        connector_name=config.connector_name, trading_pair=config.trading_pair,
        side=TradeType.BUY, leverage=1, amount=amount, entry_price=price,
        entry_order_policy='single_attempt', level_id=f'modular_spot:v1:{cycle}:{stage}',
        triple_barrier_config=TripleBarrierConfig(
            open_order_type=OrderType.LIMIT,
            open_order_limit_max_age_seconds=config.acquisition.entry_timeout_seconds,
            trailing_stop=TrailingStop(activation_price=activation, trailing_delta=delta),
            trailing_stop_order_type=OrderType.LIMIT, trailing_stop_limit_max_age_seconds=0,
            stop_loss=None, time_limit=None, take_profit=None,
        ),
    )


class ModularPositionExecutor(PositionExecutor):
    def _quote_permitted(self):
        owner = self._strategy.controllers.get(self.config.controller_id)
        check = getattr(owner, 'quote_permission', None)
        return check is None or check(self.config.trading_pair) is True

    def control_open_order(self):
        controller = self._strategy.controllers.get(self.config.controller_id)
        if controller is None or not controller.entry_permission(self.config, check_funds=not self._entry_submitted):
            # The native stop ends an intent that never reached the venue and leaves a
            # terminal entry untouched; a submitted entry is cancelled and reconciled.
            self.stop_entry_acquisition('modular_entry_permission_revoked')
            if self.is_closed:
                return
        super().control_open_order()

    def _min_no_loss_sell_price(self):
        floor = super()._min_no_loss_sell_price()
        if floor is None:
            return None
        strategy = getattr(self, '_strategy', None)
        controller = None if strategy is None else strategy.controllers.get(self.config.controller_id)
        if controller is not None and controller.config.exit_floor == 'live_no_loss':
            return floor  # live OK RSI: the after-fee no-loss price only
        trailing = self.config.triple_barrier_config.trailing_stop
        try:
            return protected_sell_price(floor, TrailingPolicy(
                activation=trailing.activation_price, delta=trailing.trailing_delta),
                self.trading_rules.min_price_increment)
        except (PolicyError, ValueError, AttributeError):
            return None

    def _current_trailing_reference_price(self):
        if not self._quote_permitted():
            return None
        try:
            return decimal(self.get_price(self.config.connector_name, self.config.trading_pair,
                                          PriceType.BestBid), 'executable bid', positive=True)
        except (PolicyError, RuntimeError, KeyError, AttributeError):
            return None

    def control_trailing_stop(self):
        # The native state machine reads marked PnL before consulting the close
        # reference. Gate that earlier read too, so stale prices cannot persist a
        # new arm/high-water mark and change the exit after fresh data returns.
        if self._quote_permitted():
            super().control_trailing_stop()

    def _close_order_type_for(self, close_type):
        return OrderType.LIMIT

    def _should_fallback_trailing_limit_close(self):
        return False

    def _fees_complete(self):
        base, quote = self.config.trading_pair.split('-')
        for tracked in [*self._entry_orders(), *self._exit_orders()]:
            order = tracked.order
            if order is None:
                return False
            fills = list(order.order_fills.values())
            if (sum((f.fill_base_amount for f in fills), Decimal('0')) != order.executed_amount_base
                    or sum((f.fill_quote_amount for f in fills), Decimal('0')) != order.executed_amount_quote):
                return False
            for fill in fills:
                fee = fill.fee
                if fee.percent and fee.percent_token not in (None, base, quote):
                    return False
                if not fee.percent.is_finite() or fee.percent < 0:
                    return False
                if any(f.token not in (base, quote) or not f.amount.is_finite() or f.amount < 0 for f in fee.flat_fees):
                    return False
        return True

    def place_close_order_and_cancel_open_orders(self, close_type, price=Decimal('NaN')):
        if close_type != CloseType.TRAILING_STOP:
            self.early_stop(keep_position=True)
            return
        if not self._quote_permitted():
            return  # Do not turn missing quotes into a position-hold transition.
        if not self._fees_complete():
            self.logger().error('Modular exit held: native fill/fee receipts incomplete')
            return
        super().place_close_order_and_cancel_open_orders(close_type, price)

    def early_stop(self, keep_position=True):
        # An operator stop is not authorization to violate the profit-only mandate.
        super().early_stop(keep_position=True)
