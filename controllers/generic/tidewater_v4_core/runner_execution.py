"""Tidewater protected runner: the tranche executor (opt-in `exit_runner`; see runner_book.py).

`TidewaterRunnerExecutor` is Meridian's `ModularPositionExecutor` (subclassed, never edited) with one
change at the trailing exit. Without a runner it behaves exactly like its parent.

1. Harvest. When the native trailing stop fires, the book decides the split (stored first). The
   executor sells only the harvest part with a native LIMIT close at max(bid, protected floor), as the
   native trailing close prices it.
2. Runner phase. When the harvest part is sold the executor returns to RUNNING owning the runner
   base. Native barriers (trailing, take profit, stop loss) no longer apply to it; the book's floor does.
3. Runner exit. When the book signals an exit the executor closes everything it still owns with a
   native LIMIT order at `runner_exit_fill(bid, cost_floor)`: the bid when it is at or above the cost
   floor, else a resting limit at the cost floor. A harvest sale still resting is cancelled first and
   joins that order, whose floor is then also the native protected floor of the harvest part.

Never below the floor: every runner-path order is a LIMIT at or above the native protected floor
(harvest part, as the native trailing close) or the runner's cost floor (exit), checked immediately
before submission. The runner path never
demotes the position to the held bag; untradeable residue follows the native held-inventory rule.
"""
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

from .runner_book import EXIT, RunnerHold

RUNNER_EXIT_REASON = 'RUNNER_FLOOR'


class TidewaterRunnerExecutor(ModularPositionExecutor):
    def _runner_book(self):
        owner = self._strategy.controllers.get(self.config.controller_id)
        return getattr(owner, 'runners', None)

    def _runner_state(self):
        book = self._runner_book()
        return None if book is None else book.runner(self.config.id)

    def harvest_facts(self, price):
        """The native values the runner split and cost floor read at the trailing trigger."""
        rules = self.trading_rules
        entry_fee, exit_fee = self._known_fee_pct(), self._expected_close_fee_rate()
        orders = self._entry_orders()
        gross_base = sum((o.executed_amount_base for o in orders), Decimal('0'))
        gross_quote = sum((o.executed_amount_quote for o in orders), Decimal('0'))
        return {'executor_id': self.config.id, 'pair': self.config.trading_pair, 'amount': self.amount_to_close,
                'price': price, 'amount_step': rules.min_base_amount_increment,
                'minimum_notional': rules.min_notional_size, 'minimum_order': rules.min_order_size,
                # The research cost basis is the entry price, with both sides' costs in the fee fraction.
                'cost_basis': gross_quote / gross_base if gross_base > 0 and gross_quote > 0 else None,
                'fee_fraction': None if exit_fee is None else entry_fee + exit_fee,
                # The native after-fee no-loss price (not the trailing exit's protected floor).
                'no_loss_floor': PositionExecutor._min_no_loss_sell_price(self)}

    # ------------------------------------------------------------------ native hooks
    def place_close_order_and_cancel_open_orders(self, close_type, price=Decimal('NaN')):
        book = self._runner_book()
        if book is None or close_type != CloseType.TRAILING_STOP:
            # Every runner-path close is a TRAILING_STOP close; other close types keep the native path.
            return super().place_close_order_and_cancel_open_orders(close_type, price)
        runner = book.runner(self.config.id)
        if isinstance(runner, RunnerHold):
            self._runner_hold(runner)
            return
        if runner is None and self.config.id not in book.skipped:
            if not self._quote_permitted() or not self._fees_complete():
                return super().place_close_order_and_cancel_open_orders(close_type, price)  # the native holds
            bid = self._current_trailing_reference_price()
            if bid is None:
                return
            runner = book.harvest(self.harvest_facts(bid))
            if isinstance(runner, RunnerHold):
                self._runner_hold(runner)
                return
        if runner is None:
            return super().place_close_order_and_cancel_open_orders(close_type, price)
        self._runner_close(runner, price)

    def control_barriers(self):
        runner = self._runner_state()
        if runner is None or isinstance(runner, RunnerHold):
            # Without a runner the native barriers apply; while the book is unread a fired trailing
            # stop holds in place_close_order_and_cancel_open_orders, so nothing sells.
            return super().control_barriers()
        if self.status == RunnableStatus.RUNNING and (runner['phase'] == EXIT
                                                      or self.amount_to_close > runner['runner_base']):
            self.place_close_order_and_cancel_open_orders(CloseType.TRAILING_STOP)

    async def control_close_order(self):
        runner = self._runner_state()
        order = None if self._close_order is None else self._close_order.order
        if (isinstance(runner, dict) and runner['phase'] == EXIT and order is not None and order.is_open
                and (order.amount - order.executed_amount_base < self.amount_to_close
                     or order.price < runner['cost_floor'])):
            # The resting harvest sale yields to the runner exit of everything still owned.
            if getattr(self, '_runner_cancel_requested', None) != order.client_order_id:
                self._runner_cancel_requested = order.client_order_id
                self._strategy.cancel(connector_name=self.config.connector_name,
                                      trading_pair=self.config.trading_pair, order_id=order.client_order_id)
            return
        await super().control_close_order()

    # ------------------------------------------------------------------ runner path
    def _runner_hold(self, hold):
        if getattr(self, '_runner_hold_reason', None) != hold.reason:
            self.logger().error(self._log_message(f'Runner exit held: {hold.reason}; position kept, retrying'))
        self._runner_hold_reason = hold.reason

    def _lot(self, amount):
        quantum = self.trading_rules.min_base_amount_increment
        return (amount / quantum).to_integral_value(rounding=ROUND_FLOOR) * quantum

    def _tradeable(self, amount, price):
        rules = self.trading_rules
        return amount > 0 and amount >= rules.min_order_size and amount * price >= rules.min_notional_size

    def _runner_close(self, runner, price):
        if self._resume_conflicting_fill is not None:
            return
        book = self._runner_book()
        bid = self._current_trailing_reference_price()
        if runner['phase'] == EXIT:
            floor = runner['cost_floor']
            if self.amount_to_close > runner['runner_base']:
                # Part of the harvest sale is unsold: the one exit order also keeps its protected floor.
                protected = self._min_no_loss_sell_price()
                if protected is None:
                    return
                floor = max(floor, protected)
            if bid is None:
                return
            _, order_price = book.exit_order(self.config.id, bid, floor)
            self._exit_trigger_reason = RUNNER_EXIT_REASON
            self._runner_place(order_price, floor=floor, cap=None)
            return
        sell = self._lot(self.amount_to_close - runner['runner_base'])
        reference = bid if bid is not None else self.current_market_price
        if not self._tradeable(sell, reference):
            self._enter_runner_phase()  # the harvest part is sold; untradeable residue stays with the runner
            return
        floor = self._min_no_loss_sell_price()
        if bid is None or floor is None:
            return
        order_price = max(price if price.is_finite() else bid, floor)
        if self._exit_trigger_reason is None:
            self._exit_trigger_reason = CloseType.TRAILING_STOP.name
        self._runner_place(order_price, floor=floor, cap=sell)

    def _enter_runner_phase(self):
        if self.status != RunnableStatus.SHUTTING_DOWN or self._close_order is not None:
            return
        self.close_type = None
        self.close_timestamp = None
        self._exit_trigger_reason = None
        self._status = RunnableStatus.RUNNING
        self.logger().info(self._log_message(f'Runner phase: owned_base={self.amount_to_close}'))

    def _runner_place(self, price, *, floor, cap):
        """Submit one native LIMIT close (the native close tail, without its demotion branch)."""
        self.close_type = CloseType.TRAILING_STOP
        if self.close_timestamp is None:
            self.close_timestamp = self._strategy.current_timestamp
        self._status = RunnableStatus.SHUTTING_DOWN
        self.cancel_open_orders()
        if self._close_order or not self._entry_terminal_confirmed() or (
                self._take_profit_limit_order and not self._take_profit_limit_order.is_done):
            return
        if self._exit_complete():
            return
        amount = self._close_order_amount()
        if (self._close_quantity.get('blocked_reason') == 'owned_residual_below_exchange_minimum'
                and self.all_orders_completed()):
            self.early_stop(keep_position=True)  # native: untradeable residue is held inventory
            return
        if cap is not None:
            amount = min(amount, cap)
            self._close_quantity = {**self._close_quantity, 'order_amount': amount,
                                    'residual_base': self.amount_to_close - amount, 'runner_retained': True}
        if amount <= 0 or amount < self.trading_rules.min_order_size:
            return  # the native quantity receipt says why; retried next tick
        if price.is_finite():  # a sell limit rounds up to the price tick, never below its floor
            tick = self.trading_rules.min_price_increment
            price = (price / tick).to_integral_value(rounding=ROUND_CEILING) * tick
        if not price.is_finite() or price < floor:
            self.logger().error(self._log_message(f'Runner order refused below its floor price={price} floor={floor}'))
            return
        order_id = self.place_order(connector_name=self.config.connector_name, trading_pair=self.config.trading_pair,
                                    order_type=OrderType.LIMIT, amount=amount, price=price, side=TradeType.SELL,
                                    position_action=PositionAction.CLOSE)
        self._close_order = TrackedOrder(order_id=order_id)
        self._close_order_type = OrderType.LIMIT
        self.logger().info(self._log_message(f'Runner close order order_id={order_id} amount={amount} price={price} '
                                             f'floor={floor} reason={self._exit_trigger_reason}'))
