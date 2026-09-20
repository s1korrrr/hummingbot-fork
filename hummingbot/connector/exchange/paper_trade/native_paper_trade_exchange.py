"""Opt-in, network-free spot execution using Hummingbot's native order ledger.

The injected book is a consumable snapshot. Resting orders use price/time priority
against a configured fraction of each *new* public trade; real queue position is
unknown. This is an execution test model, never exchange-equivalence evidence.
"""
import asyncio
import copy
import hashlib
import json
import math
from dataclasses import fields
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from hummingbot.connector.budget_checker import BudgetChecker
from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.data_type.terminal_reconciliation import terminal_reconciliation
from hummingbot.core.data_type.trade_fee import TradeFeeBase, TradeFeeSchema
from hummingbot.core.network_iterator import NetworkStatus

ZERO = Decimal('0')


def _decimal(value, *, positive=False):
    result = Decimal(str(value))
    if not result.is_finite() or result < 0 or (positive and result == 0):
        raise ValueError('Expected a finite nonnegative decimal')
    return result


class _PaperInFlightOrder(InFlightOrder):
    def cumulative_fee_paid(self, token, exchange=None):
        if exchange is not None:
            return super().cumulative_fee_paid(token, exchange)
        # Keep the native fee conversion, but bind it to each observed fill price.
        # Native JSON serialization otherwise falls back to a process-wide oracle.
        total = ZERO
        for fill in self.order_fills.values():
            inverse = f'{self.quote_asset}-{self.base_asset}'
            view = SimpleNamespace(order_books={self.trading_pair: True, inverse: True},
                                   get_price_by_type=lambda pair, *_: (Decimal('1') / fill.fill_price
                                                                       if pair == inverse else fill.fill_price))
            total += fill.fee.fee_amount_in_token(self.trading_pair, fill.fill_price,
                                                  fill.fill_base_amount, token, exchange=view)
        return total


class _PaperOrderCandidate(OrderCandidate):
    def _get_fee(self, exchange):
        base, quote = self.trading_pair.split('-')
        return exchange.get_fee(base, quote, self.order_type, self.order_side,
                                self.amount, self.price, is_maker=self.is_maker)


class _PaperBudgetChecker(BudgetChecker):
    """Keep native sizing; bind the explicit local fee schema without global settings."""

    def populate_collateral_entries(self, order_candidate):
        if type(order_candidate) not in (OrderCandidate, _PaperOrderCandidate):
            raise ValueError('Paper connector supports spot OrderCandidate only')
        candidate = _PaperOrderCandidate(**{
            field.name: getattr(order_candidate, field.name)
            for field in fields(OrderCandidate) if field.init
        })
        candidate.populate_collateral_entries(self._exchange)
        return candidate


class NativePaperTradeExchange(ExchangeBase):
    """Explicitly injected spot paper venue; contains no transport or credentials.

    Call ``set_order_book`` with fresh, strictly increasing snapshot sequence IDs,
    ``tick`` with the simulation wall clock, and await ``drain`` after synchronous
    buy/sell/cancel calls. ``process_trade`` accepts monotonic per-pair sequence IDs.
    MARKET orders are IOC: available depth fills, then residual quantity cancels.
    Snapshots emitted through ``tracking_states`` are native recorder compatible.
    """

    def __init__(self, trading_rules, *, fee_schema=None, name='okx_paper_trade',
                 max_data_age=5.0, trade_participation=Decimal('.1')):
        if not name.endswith('_paper_trade') or 'perpetual' in name:
            raise ValueError('An explicit spot paper connector name is required')
        self._paper_name = name
        super().__init__()
        self._rules = dict(trading_rules)
        if not self._rules:
            raise ValueError('Trading rules are required')
        for pair, rule in self._rules.items():
            if pair != rule.trading_pair or len(pair.split('-')) != 2:
                raise ValueError('Invalid trading rule identity')
            for value in (rule.min_price_increment, rule.min_base_amount_increment):
                _decimal(value, positive=True)
        self._fee_policy = copy.deepcopy(fee_schema or TradeFeeSchema())
        self._fee_policy.validate_schema()
        for rate in (self._fee_policy.maker_percent_fee_decimal, self._fee_policy.taker_percent_fee_decimal):
            if _decimal(rate) >= 1:
                raise ValueError('Paper fee rate must be below one')
        if (self._fee_policy.percent_fee_token is not None or self._fee_policy.maker_fixed_fees
                or self._fee_policy.taker_fixed_fees):
            raise ValueError('Only native percentage spot fees are supported; third-token/flat fees require a separate model')
        if not math.isfinite(max_data_age) or max_data_age <= 0:
            raise ValueError('A bounded data age is required')
        self.max_data_age = float(max_data_age)
        self.trade_participation = _decimal(trade_participation, positive=True)
        if self.trade_participation > 1:
            raise ValueError('Trade participation cannot exceed one')
        self._paper_time = 0.0
        self._books = {}
        self._depth = {}
        self._book_sequence = {}
        self._book_time = {}
        self._trade_sequence = {}
        self._order_tracker = ClientOrderTracker(self)
        self._paper_budget_checker = _PaperBudgetChecker(self)
        self._pending = []
        self._operation_lock = asyncio.Lock()
        self._restored = False
        self._execution_started = False
        self._contract = hashlib.sha256(json.dumps({
            'name': name, 'rules': {p: repr(r) for p, r in sorted(self._rules.items())},
            'maker': str(self._fee_policy.maker_percent_fee_decimal),
            'taker': str(self._fee_policy.taker_percent_fee_decimal),
            'buy_base_fee': self._fee_policy.buy_percent_fee_deducted_from_returns,
            'max_data_age': self.max_data_age, 'participation': str(self.trade_participation),
        }, sort_keys=True).encode()).hexdigest()

    @property
    def name(self):
        return self._paper_name

    @property
    def display_name(self):
        return self.name

    @property
    def current_timestamp(self):
        return self._paper_time

    @property
    def budget_checker(self):
        return self._paper_budget_checker

    @property
    def trading_rules(self):
        return self._rules

    @property
    def trading_pairs(self):
        return list(self._rules)

    @property
    def order_books(self):
        return self._books

    @property
    def in_flight_orders(self):
        return self._order_tracker.active_orders

    @property
    def limit_orders(self):
        return [o.to_limit_order() for o in self.in_flight_orders.values()
                if o.order_type.is_limit_type() and not o.is_done]

    @property
    def ready(self):
        return all(self._fresh(p) for p in self._rules)

    @property
    def status_dict(self):
        return {'fresh_injected_books': self.ready}

    def tick(self, timestamp):
        timestamp = float(timestamp)
        if not math.isfinite(timestamp) or timestamp < self._paper_time:
            raise ValueError('Paper clock must be finite and monotonic')
        self._paper_time = timestamp

    def set_balance(self, asset, amount):
        if self._execution_started or self._restored:
            raise ValueError('Balances may only be seeded before execution or restore')
        self._account_balances[asset] = _decimal(amount)

    def get_available_balance(self, currency):
        reserved = ZERO
        for order in self.in_flight_orders.values():
            if not order.is_done:
                candidate = self._candidate(order, order.amount - order.executed_amount_base, order.price,
                                            self._reservation_maker(order))
                reserved += candidate.collateral_dict.get(currency, ZERO)
        return self.get_balance(currency) - reserved

    def get_order_price_quantum(self, trading_pair, price):
        return self._rules[trading_pair].min_price_increment

    def get_order_size_quantum(self, trading_pair, order_size):
        return self._rules[trading_pair].min_base_amount_increment

    def get_order_book(self, trading_pair):
        self._require_fresh(trading_pair)
        return self._books[trading_pair]

    def get_price(self, trading_pair, is_buy):
        book = self.get_order_book(trading_pair)
        return Decimal(str(book.get_price(is_buy)))

    def get_fee(self, base_currency, quote_currency, order_type, order_side, amount,
                price=Decimal('NaN'), is_maker=None):
        maker = order_type != OrderType.MARKET if is_maker is None else is_maker
        rate = (self._fee_policy.maker_percent_fee_decimal if maker
                else self._fee_policy.taker_percent_fee_decimal)
        token = (base_currency if order_side == TradeType.BUY
                 and self._fee_policy.buy_percent_fee_deducted_from_returns else quote_currency)
        return TradeFeeBase.new_spot_fee(self._fee_policy, order_side, percent=rate, percent_token=token)

    @property
    def supports_terminal_fill_reconciliation(self):
        return True

    async def _request_order_status(self, tracked_order):
        await self.drain()
        order = self._order_tracker.fetch_order(tracked_order.client_order_id)
        if (order is None or order is not tracked_order
                or order.exchange_order_id != tracked_order.exchange_order_id):
            raise ValueError('Paper order identity is unavailable for native reconciliation')
        misc = None
        if order.current_state in {OrderState.CANCELED, OrderState.FILLED}:
            misc = {'terminal_reconciliation': terminal_reconciliation(
                order, 'native_paper_terminal_v1', order.current_state.name, order.executed_amount_base)}
        return OrderUpdate(order.trading_pair, self.current_timestamp, order.current_state,
                           order.client_order_id, order.exchange_order_id, misc)

    async def _update_orders_with_error_handler(self, orders, error_handler):
        for order in orders:
            try:
                update = await self._request_order_status(order)
                await self._order_tracker.process_order_update(update)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await error_handler(order, error)

    async def _handle_update_error_for_lost_order(self, order, error):
        # Missing paper authority is an observable hold; never infer cancellation.
        raise RuntimeError(f'Paper order reconciliation unavailable: {order.client_order_id}') from error

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def start_network(self):
        # Deliberately no producer, connector, socket, or background network task.
        return None

    async def stop_network(self):
        await self.drain()

    async def check_network(self):
        return NetworkStatus.CONNECTED if self.ready else NetworkStatus.NOT_CONNECTED

    def _fresh(self, pair):
        age = self.current_timestamp - self._book_time.get(pair, float('-inf'))
        return 0 <= age <= self.max_data_age

    def _require_fresh(self, pair):
        if pair not in self._rules or not self._fresh(pair):
            raise ValueError('Missing, stale, or future paper order book')

    def validate_order_book_input(self, trading_pair, sequence, timestamp, bids, asks, *,
                                  current_timestamp=None, previous_sequence=None, previous_timestamp=None):
        now = self.current_timestamp if current_timestamp is None else current_timestamp
        previous_sequence = self._book_sequence.get(trading_pair, -1) if previous_sequence is None else previous_sequence
        previous_timestamp = self._book_time.get(trading_pair, 0) if previous_timestamp is None else previous_timestamp
        if trading_pair not in self._rules or type(sequence) is not int or sequence <= previous_sequence:
            raise ValueError('Book sequence must strictly increase for a configured pair')
        age = now - float(timestamp)
        if (not math.isfinite(age) or not 0 <= age <= self.max_data_age
                or float(timestamp) < previous_timestamp):
            raise ValueError('Book timestamp is stale, future, or regressing')
        sides = [[[_decimal(p, positive=True), _decimal(q, positive=True)] for p, q in rows]
                 for rows in (bids, asks)]
        if not all(sides) or sides[0][0][0] >= sides[1][0][0]:
            raise ValueError('Two uncrossed book sides are required')
        for rows, reverse in zip(sides, (True, False)):
            prices = [r[0] for r in rows]
            if prices != sorted(set(prices), reverse=reverse):
                raise ValueError('Book levels must be unique and ordered')
        return sides

    def set_order_book(self, trading_pair, sequence, timestamp, bids, asks):
        sides = self.validate_order_book_input(trading_pair, sequence, timestamp, bids, asks)
        book = OrderBook()
        book.apply_snapshot(*[[OrderBookRow(float(p), float(q), sequence) for p, q in rows]
                              for rows in sides], sequence)
        self._books[trading_pair] = book
        self._depth[trading_pair] = sides
        self._book_sequence[trading_pair] = sequence
        self._book_time[trading_pair] = float(timestamp)

    def _reservation_maker(self, order):
        # A LIMIT can execute as maker or taker. Reserve the greater fee until
        # terminal status instead of exposing cash during asynchronous matching.
        return (order.order_type == OrderType.LIMIT_MAKER or
                (order.order_type == OrderType.LIMIT and
                 self._fee_policy.maker_percent_fee_decimal >= self._fee_policy.taker_percent_fee_decimal))

    def _candidate(self, order, amount, price, maker):
        return self._paper_budget_checker.populate_collateral_entries(OrderCandidate(
            order.trading_pair, maker, order.order_type, order.trade_type, amount, price))

    def buy(self, trading_pair, amount, order_type=OrderType.MARKET, price=Decimal('NaN'), **kwargs):
        return self._submit(trading_pair, amount, order_type, price, TradeType.BUY, kwargs)

    def sell(self, trading_pair, amount, order_type=OrderType.MARKET, price=Decimal('NaN'), **kwargs):
        return self._submit(trading_pair, amount, order_type, price, TradeType.SELL, kwargs)

    def _submit(self, pair, amount, order_type, price, side, kwargs):
        self._require_fresh(pair)
        if order_type not in self.supported_order_types() or kwargs.get('leverage', 1) != 1:
            raise ValueError('Only unleveraged spot orders are supported')
        amount = self.quantize_order_amount(pair, _decimal(amount, positive=True))
        rule = self._rules[pair]
        depth = self._depth[pair][1 if side == TradeType.BUY else 0]
        available = [(p, q) for p, q in depth if q > 0]
        if order_type == OrderType.MARKET:
            if not available:
                raise ValueError('No unconsumed paper depth')
            price = max(p for p, _ in available) if side == TradeType.BUY else min(p for p, _ in available)
        else:
            price = self.quantize_order_price(pair, _decimal(price, positive=True))
        if amount <= 0 or amount < rule.min_order_size or amount > rule.max_order_size or amount * price < max(rule.min_notional_size, rule.min_order_value):
            raise ValueError('Order violates native trading rules')
        crosses = available and (price >= available[0][0] if side == TradeType.BUY else price <= available[0][0])
        if order_type == OrderType.LIMIT_MAKER and crosses:
            raise ValueError('Post-only order crosses the injected book')
        order = _PaperInFlightOrder(f'paper-{uuid4().hex}', pair, order_type, side, amount,
                                    self.current_timestamp, price=price,
                                    exchange_order_id=f'paper-{uuid4().hex}',
                                    position=PositionAction.NIL)
        candidate = self._candidate(order, amount, price, self._reservation_maker(order))
        if any(required > self.get_available_balance(asset) for asset, required in candidate.collateral_dict.items()):
            raise ValueError('Insufficient unreserved paper balance including fees')
        self._execution_started = True
        self._order_tracker.start_tracking_order(order)
        self._pending.append(asyncio.create_task(self._open(order)))
        return order.client_order_id

    async def _update(self, order, state):
        await self._order_tracker.process_order_update(OrderUpdate(
            order.trading_pair, self.current_timestamp, state, order.client_order_id, order.exchange_order_id))

    async def _open(self, order):
        async with self._operation_lock:
            await self._update(order, OrderState.OPEN)
            if order.order_type == OrderType.LIMIT_MAKER:
                return
            if not self._fresh(order.trading_pair):
                await self._update(order, OrderState.CANCELED)
                return
            depth = self._depth[order.trading_pair][1 if order.trade_type == TradeType.BUY else 0]
            for index, level in enumerate(depth):
                price, quantity = level
                crosses = price <= order.price if order.trade_type == TradeType.BUY else price >= order.price
                if not crosses or order.executed_amount_base == order.amount:
                    break
                fill = min(quantity, order.amount - order.executed_amount_base)
                if fill > 0:
                    level[1] -= fill
                    await self._fill(order, fill, price, False,
                                     f'book:{self._book_sequence[order.trading_pair]}:{index}:{order.client_order_id}')
            if order.order_type == OrderType.MARKET and order.executed_amount_base < order.amount:
                await self._update(order, OrderState.CANCELED)

    async def drain(self):
        while self._pending:
            pending, self._pending = self._pending, []
            await asyncio.gather(*pending)

    def cancel(self, trading_pair, client_order_id):
        order = self._order_tracker.fetch_tracked_order(client_order_id)
        if order is not None and order.trading_pair != trading_pair:
            raise ValueError('Cancellation pair mismatch')
        if order is not None:
            self._pending.append(asyncio.create_task(self._cancel(order)))
        return client_order_id

    async def _cancel(self, order):
        async with self._operation_lock:
            if not order.is_done:
                await self._update(order, OrderState.CANCELED)

    async def cancel_all(self, timeout_seconds):
        orders = list(self.in_flight_orders.values())
        for order in orders:
            self.cancel(order.trading_pair, order.client_order_id)
        await asyncio.wait_for(self.drain(), timeout_seconds)
        return [CancellationResult(o.client_order_id, o.is_cancelled or o.is_filled) for o in orders]

    def validate_trade_input(self, trading_pair, sequence, timestamp, side, price, amount, *,
                             current_timestamp=None, book_timestamp=None):
        now = self.current_timestamp if current_timestamp is None else current_timestamp
        if trading_pair not in self._rules or type(sequence) is not int or sequence < 0:
            raise ValueError('Invalid paper trade identity')
        if side not in (TradeType.BUY, TradeType.SELL):
            raise ValueError('Invalid public trade side')
        price, amount = _decimal(price, positive=True), _decimal(amount, positive=True)
        age = now - float(timestamp)
        if not math.isfinite(age) or not 0 <= age <= self.max_data_age:
            raise ValueError('Public trade timestamp is stale or future')
        book_timestamp = self._book_time.get(trading_pair, float('-inf')) if book_timestamp is None else book_timestamp
        if not 0 <= now - book_timestamp <= self.max_data_age:
            raise ValueError('Missing, stale, or future paper order book')
        return price, amount

    async def process_trade(self, trading_pair, sequence, timestamp, side, price, amount):
        price, amount = self.validate_trade_input(trading_pair, sequence, timestamp, side, price, amount)
        await self.drain()
        async with self._operation_lock:
            if sequence <= self._trade_sequence.get(trading_pair, -1):
                return
            self._trade_sequence[trading_pair] = sequence
            amount *= self.trade_participation
            orders = [o for o in self.in_flight_orders.values() if o.trading_pair == trading_pair
                      and o.trade_type != side and o.order_type.is_limit_type() and not o.is_done
                      and o.creation_timestamp <= timestamp]
            orders.sort(key=lambda o: ((-o.price if o.trade_type == TradeType.BUY else o.price), o.creation_timestamp))
            for order in orders:
                crosses = price <= order.price if order.trade_type == TradeType.BUY else price >= order.price
                if amount <= 0:
                    break
                if crosses:
                    fill = min(amount, order.amount - order.executed_amount_base)
                    amount -= fill
                    await self._fill(order, fill, order.price, True, f'trade:{sequence}:{order.client_order_id}')

    def _charged_fee(self, order, candidate):
        fee_amount = candidate.percent_fee_value
        return TradeFeeBase.new_spot_fee(self._fee_policy, order.trade_type,
                                         flat_fees=[fee_amount] if fee_amount and fee_amount.amount else [])

    async def _fill(self, order, amount, price, maker, trade_id):
        candidate = self._candidate(order, amount, price, maker)
        balances = dict(self._account_balances)
        for asset, cost in candidate.collateral_dict.items():
            balances[asset] = balances.get(asset, ZERO) - cost
        returns = candidate.potential_returns
        balances[returns.token] = balances.get(returns.token, ZERO) + returns.amount
        if any(value < 0 for value in balances.values()):
            raise ValueError('Paper fill would overdraw native balances')
        # Estimate through native candidate/schema, but emit the actual charged
        # currency/amount like a real connector. Percent estimates are not exact
        # native held-position reconciliation receipts.
        fee = self._charged_fee(order, candidate)
        self._account_balances = balances
        self._order_tracker.process_trade_update(TradeUpdate(
            trade_id, order.client_order_id, order.exchange_order_id, order.trading_pair,
            self.current_timestamp, price, amount, amount * price, fee, is_taker=not maker))
        await self._update(order, OrderState.FILLED if order.executed_amount_base == order.amount else OrderState.PARTIALLY_FILLED)

    @property
    def tracking_states(self):
        return {
            'schema': 'native_spot_paper_v1', 'contract': self._contract,
            'timestamp': self.current_timestamp,
            'balances': {asset: str(value) for asset, value in self._account_balances.items()},
            'orders': {key: value.to_json() for key, value in self._order_tracker.all_orders.items()},
            'trade_sequence': dict(self._trade_sequence), 'book_sequence': dict(self._book_sequence),
        }

    def restore_tracking_states(self, saved_states):
        if self._order_tracker.all_orders or self._pending or self._restored:
            raise ValueError('Restore requires a new unused connector')
        state = copy.deepcopy(saved_states)
        if state.get('schema') != 'native_spot_paper_v1' or state.get('contract') != self._contract:
            raise ValueError('Paper state schema/config identity mismatch')
        balances = {asset: _decimal(value) for asset, value in state['balances'].items()}
        timestamp = float(state['timestamp'])
        if not math.isfinite(timestamp) or timestamp < 0:
            raise ValueError('Invalid paper state timestamp')
        for field in ('trade_sequence', 'book_sequence'):
            if any(p not in self._rules or type(n) is not int or n < 0 for p, n in state[field].items()):
                raise ValueError('Invalid persisted input sequence')
        orders = []
        for key, value in state['orders'].items():
            order = _PaperInFlightOrder.from_json(value)
            if (key != order.client_order_id or order.trading_pair not in self._rules
                    or not key.startswith('paper-') or order.leverage != 1
                    or order.order_type not in self.supported_order_types()
                    or order.trade_type not in (TradeType.BUY, TradeType.SELL)
                    or not math.isfinite(order.creation_timestamp)
                    or not 0 <= order.creation_timestamp <= timestamp
                    or order.current_state not in (OrderState.OPEN, OrderState.PARTIALLY_FILLED, OrderState.FILLED, OrderState.CANCELED)
                    or order.position != PositionAction.NIL):
                raise ValueError('Invalid native paper order identity')
            _decimal(order.amount, positive=True)
            _decimal(order.price, positive=True)
            base, quote = ZERO, ZERO
            for fill_id, fill in order.order_fills.items():
                if (fill.trade_id != fill_id or fill.client_order_id != key or fill.trading_pair != order.trading_pair
                        or fill.exchange_order_id != order.exchange_order_id
                        or not math.isfinite(fill.fill_timestamp)
                        or not order.creation_timestamp <= fill.fill_timestamp <= timestamp):
                    raise ValueError('Native paper fill identity mismatch')
                base += _decimal(fill.fill_base_amount, positive=True)
                quote += _decimal(fill.fill_quote_amount, positive=True)
                if fill.fill_base_amount * _decimal(fill.fill_price, positive=True) != fill.fill_quote_amount:
                    raise ValueError('Native paper fill price mismatch')
                taker = value['order_fills'][fill_id].get('is_taker')
                if type(taker) is not bool:
                    raise ValueError('Native paper fill liquidity identity missing')
                candidate = self._candidate(order, fill.fill_base_amount, fill.fill_price, not taker)
                if fill.fee.to_json() != self._charged_fee(order, candidate).to_json():
                    raise ValueError('Native paper actual fee differs from the sealed schema')
                # Native TradeUpdate.from_json currently omits is_taker. Retain
                # this own wire field without changing the native order ledger.
                order.order_fills[fill_id] = fill._replace(is_taker=taker)
            if order.current_state == OrderState.FILLED and base != order.amount:
                raise ValueError('Native paper filled status lacks complete fills')
            if base != order.executed_amount_base or quote != order.executed_amount_quote or base > order.amount:
                raise ValueError('Native paper cumulative fill mismatch')
            orders.append(order)
        reserved = {}
        for order in orders:
            if not order.is_done:
                candidate = self._candidate(order, order.amount - order.executed_amount_base, order.price,
                                            self._reservation_maker(order))
                for asset, cost in candidate.collateral_dict.items():
                    reserved[asset] = reserved.get(asset, ZERO) + cost
        if any(cost > balances.get(asset, ZERO) for asset, cost in reserved.items()):
            raise ValueError('Restored orders exceed paper balances')
        tracker = ClientOrderTracker(self)
        for order in orders:
            tracker.start_tracking_order(order)
            if order.is_done:
                tracker.stop_tracking_order(order.client_order_id)
        self._account_balances = balances
        self._order_tracker = tracker
        self._paper_time = max(timestamp, self._paper_time)
        self._trade_sequence = dict(state['trade_sequence'])
        self._book_sequence = dict(state['book_sequence'])
        self._books, self._depth, self._book_time = {}, {}, {}
        self._restored = True


def create_native_paper_trade_market(trading_rules, fee_schema, initial_balances, **kwargs):
    """Explicit isolated factory. Never delegates to a live or legacy paper factory."""
    market = NativePaperTradeExchange(trading_rules, fee_schema=fee_schema, **kwargs)
    for asset, amount in initial_balances.items():
        market.set_balance(asset, amount)
    return market


def is_native_paper_connector(connector):
    """Do not authorize an arbitrary object/subclass based on its name string."""
    return type(connector) is NativePaperTradeExchange and connector.name == 'okx_paper_trade'
