"""Explicit emergency-only recovery using native tracking, accounting and SELL execution.

This module never resumes a strategy or closes its historical executor rows.
Native owner state remains under reconciliation HOLD after emergency settlement.
"""
import asyncio
import json
import uuid
from decimal import Decimal

from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.model.market_state import MarketState
from hummingbot.model.metadata import Metadata
from hummingbot.model.order import Order
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator, PositionHold
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus

TERMINAL = {'filled': OrderState.FILLED, 'canceled': OrderState.CANCELED}
STATES = dict(TERMINAL, live=OrderState.OPEN, partially_filled=OrderState.PARTIALLY_FILLED)


def require(value, message):
    if not value:
        raise ValueError(message)


def decimal(value):
    result = Decimal(str(value))
    require(result.is_finite(), 'Nonfinite recovery value')
    return result


def load_native_orders(recorder, owner_pairs):
    """Read genuine persisted tracker/executor snapshots, including PENDING_CREATE."""
    require(recorder.config_file_path == 'rsi_modular_v2.yml', 'Recovery recorder owner differs')
    snapshots = {}

    def identity(order):
        price = order.price
        if order.order_type == OrderType.MARKET and (price is None or price.is_nan()):
            price = None
        else:
            require(price is not None and price.is_finite() and price > 0, 'Invalid native limit price')
        return order.trading_pair, order.trade_type, order.amount, price, order.order_type

    def retain(raw):
        order = InFlightOrder.from_json(raw)
        require(order.trading_pair in owner_pairs.values(), 'Foreign native order pair')
        require(order.amount.is_finite() and order.amount > 0, 'Invalid native order amount')
        identity(order)
        previous = snapshots.get(order.client_order_id)
        if previous is not None:
            require(identity(order) == identity(previous),
                    'Native order snapshot identity conflict')
            if order.executed_amount_base < previous.executed_amount_base:
                return
        snapshots[order.client_order_id] = order
    infos = recorder.get_all_executors()
    for info in infos:
        require(info.controller_id in owner_pairs and info.connector_name == 'okx'
                and info.trading_pair == owner_pairs[info.controller_id], 'Foreign executor ownership')
        custom = info.custom_info or {}
        for raw in [*(custom.get('execution_order_snapshots') or []), *(custom.get('held_position_orders') or [])]:
            retain(raw)
    with recorder._sql_manager.get_new_session() as session:
        for row in session.query(Metadata).filter(Metadata.key.like('v2-submitted-order:%')).all():
            value = json.loads(row.value)
            require(value['owner'] == recorder.config_file_path, 'Foreign submission owner')
            retain(value['order'])
        for row in session.query(Metadata).filter(Metadata.key.like('v2-supervised-orders:%')).all():
            value = json.loads(row.value)
            require(value['owner'] == recorder.config_file_path, 'Foreign recovery snapshot owner')
            for raw in value['orders']:
                retain(raw)
        for state in session.query(MarketState).all():
            require(state.config_file_path == recorder.config_file_path and state.market == 'okx', 'Foreign tracker owner')
            for raw in state.saved_state.values():
                retain(raw)
        fills = session.query(TradeFill).all()
        require(all(fill.config_file_path == recorder.config_file_path and fill.market == 'okx'
                    and fill.symbol in owner_pairs.values() for fill in fills), 'Foreign native fill scope')
        require({fill.order_id for fill in fills} <= set(snapshots), 'Native fill lacks exact order snapshot')
        orders = session.query(Order).all()
        require(all(row.config_file_path == recorder.config_file_path and row.market == 'okx'
                    and row.symbol in owner_pairs.values() for row in orders), 'Foreign recorded order')
        require({row.id for row in orders} <= set(snapshots), 'Recorded order lacks exact native tracker snapshot')
        for row in orders:
            order = snapshots[row.id]
            require(row.symbol == order.trading_pair and (not row.exchange_order_id
                    or order.exchange_order_id in (None, row.exchange_order_id)), 'Recorded native order identity differs')
    require(all({key[2] for key in ExecutorOrchestrator._owned_order_keys(info)} <= set(snapshots) for info in infos),
            'Executor order history is incomplete')
    require(len(snapshots) <= 500, 'Recovery order history exceeds bounded scope')
    return snapshots


def reconciled_order(original, raw, fills):
    """Strict raw venue evidence applied through native InFlightOrder operations."""
    order = InFlightOrder.from_json(original.to_json())
    require(raw.get('clOrdId') == order.client_order_id and raw.get('instId') == order.trading_pair
            and raw.get('tdMode') == 'cash' and raw.get('side') == order.trade_type.name.lower()
            and raw.get('state') in STATES and str(raw.get('ordId', '')).isdecimal()
            and order.exchange_order_id in (None, str(raw['ordId']))
            and decimal(raw['sz']) == order.amount, 'Venue recovery order identity differs')
    require(raw.get('ordType') == {OrderType.LIMIT_MAKER: 'post_only', OrderType.LIMIT: 'limit',
                                   OrderType.MARKET: 'market'}.get(order.order_type)
            and (order.order_type == OrderType.MARKET or decimal(raw['px']) == order.price),
            'Venue native order type/price differs')
    require(isinstance(fills, list) and len(fills) < 100, 'Complete bounded raw fills required')
    ids = [str(fill['tradeId']) for fill in fills]
    require(len(ids) == len(set(ids)), 'Duplicate venue fill')
    require(set(order.order_fills) <= set(ids), 'Venue omitted a native fill')
    order.update_exchange_order_id(str(raw['ordId']))
    base, quote = order.trading_pair.split('-')
    updates = []
    for fill in fills:
        amount, price, fee_paid = decimal(fill['fillSz']), decimal(fill['fillPx']), -decimal(fill['fee'])
        require(str(fill['ordId']) == order.exchange_order_id and fill['instId'] == order.trading_pair
                and fill['side'] == order.trade_type.name.lower() and amount > 0 and price > 0
                and fee_paid >= 0 and fill['feeCcy'] in (base, quote), 'Invalid venue recovery fill')
        # The same native actual-fee representation used by the OKX connector.
        from hummingbot.connector.exchange.okx import okx_utils
        fee = TradeFeeBase.new_spot_fee(okx_utils.DEFAULT_FEES, order.trade_type,
                                        percent_token=fill['feeCcy'],
                                        flat_fees=[TokenAmount(fill['feeCcy'], fee_paid)])
        update = TradeUpdate(str(fill['tradeId']), order.client_order_id, order.exchange_order_id,
                             order.trading_pair, float(decimal(fill['ts']) / 1000), price, amount, amount * price, fee)
        old = order.order_fills.get(update.trade_id)
        require(old is None or (old.fill_base_amount, old.fill_quote_amount, old.fee.to_json())
                == (update.fill_base_amount, update.fill_quote_amount, update.fee.to_json()), 'Native/venue fill conflict')
        order.update_with_trade_update(update)
        updates.append(update)
    require(sum((update.fill_base_amount for update in updates), Decimal(0)) == decimal(raw['accFillSz'])
            == order.executed_amount_base <= order.amount, 'Venue cumulative recovery fill differs')
    require(raw['state'] != 'filled' or order.executed_amount_base == order.amount, 'Incomplete FILLED recovery order')
    order.update_with_order_update(OrderUpdate(order.trading_pair, float(decimal(raw['uTime']) / 1000),
                                               STATES[raw['state']], order.client_order_id, order.exchange_order_id))
    return order, updates


async def observe_order(connector, original):
    require(type(connector) is OkxExchange, 'Genuine native OKX connector required')
    response = await connector._api_get(path_url='/api/v5/trade/order', is_auth_required=True,
                                        params={'instId': original.trading_pair, 'clOrdId': original.client_order_id})
    require(response.get('code') == '0' and len(response.get('data', [])) == 1, 'Order lookup unavailable; retain HOLD')
    raw = response['data'][0]
    require(str(raw.get('ordId', '')).isdecimal(), 'Venue order identity unavailable')
    response = await connector._api_get(path_url='/api/v5/trade/fills', is_auth_required=True,
                                        params={'instType': 'SPOT', 'instId': original.trading_pair, 'ordId': raw['ordId']})
    require(response.get('code') == '0', 'Venue fills unavailable')
    order, updates = reconciled_order(original, raw, response.get('data'))
    return order, updates, {'order': raw, 'fills': response['data']}


def inventory(orders, owner_pairs):
    """Reuse native PositionHold fee/inventory semantics; never read wallet as ownership."""
    positions = {owner: PositionHold('okx', pair, TradeType.BUY) for owner, pair in owner_pairs.items()}
    by_pair = {pair: owner for owner, pair in owner_pairs.items()}
    require(len(by_pair) == len(owner_pairs), 'Ambiguous recovery inventory ownership')
    for order in orders:
        require(order.is_done, 'Pending native order remains; liquidation blocked')
        require(order.trading_pair in by_pair, 'Foreign inventory pair')
        positions[by_pair[order.trading_pair]].add_order_record(order.to_json())
    result = {}
    for owner, position in positions.items():
        amount = position.buy_amount_base - position.sell_amount_base
        require(amount >= 0, 'Native recovery is oversold; manual reconciliation required')
        result[owner] = amount
    return result


async def settle_owned_orders(connector, recorder, orders, *, deadline, now):
    """Explicit emergency capability caller only; cancel exact proven native IDs."""
    require(type(connector) is OkxExchange and recorder.config_file_path == 'rsi_modular_v2.yml'
            and connector in recorder._markets, 'Native recovery connector/recorder mismatch')
    settled = []
    for original in orders.values():
        require(now() < deadline, 'Recovery deadline exceeded')
        order, updates, proof = await observe_order(connector, original)
        # Rehydrate the native tracker with saved receipts. New fill events flow
        # through its normal recorder listeners; terminal updates follow fills.
        tracked = InFlightOrder.from_json(original.to_json())
        tracked.current_state = OrderState.OPEN
        tracked.update_exchange_order_id(order.exchange_order_id)
        connector._order_tracker.start_tracking_order(tracked)
        # GET proves the venue accepted this exact native client ID. Replaying
        # native creation is idempotent and repairs the pre-response crash gap.
        connector._order_tracker._trigger_created_event(tracked)
        for update in updates:
            connector._order_tracker.process_trade_update(update)
        if not order.is_done:
            await connector._execute_cancel(order.trading_pair, order.client_order_id)
            while now() < deadline:
                order, updates, proof = await observe_order(connector, original)
                for update in updates:
                    connector._order_tracker.process_trade_update(update)
                if order.is_done:
                    break
                await asyncio.sleep(.2)
            require(order.is_done, 'Cancellation unconfirmed; liquidation blocked')
        await connector._order_tracker.process_order_update(OrderUpdate(order.trading_pair, order.last_update_timestamp,
                                                                        order.current_state, order.client_order_id, order.exchange_order_id))
        native_fills = {str(fill.exchange_trade_id): fill for fill in recorder.get_trades_for_config(recorder.config_file_path)
                        if fill.order_id == order.client_order_id}
        require(set(native_fills) == set(order.order_fills), 'Native recorder did not retain exact recovery fills')
        for trade_id, fill in native_fills.items():
            record = ExecutorOrchestrator._trade_fill_order_record(fill)
            native = order.order_fills[trade_id]
            require(record['executed_amount_base'] == native.fill_base_amount
                    and record['price'] == native.fill_price and fill.trade_fee == native.fee.to_json(),
                    'Recorded recovery fill/fee differs')
        settled.append(order)
        retain_recovery_orders(recorder, [order])
    return settled


def retain_recovery_orders(recorder, orders):
    """Append native order snapshots for a later interrupted emergency attempt."""
    payload = {'owner': recorder.config_file_path, 'orders': [order.to_json() for order in orders]}
    with recorder._sql_manager.get_new_session() as session:
        with session.begin():
            session.add(Metadata(key='v2-supervised-orders:' + str(uuid.uuid4()),
                                 value=json.dumps(payload, sort_keys=True, allow_nan=False)))


class EmergencySellStrategy:
    """Minimal native execution host: no controllers and no BUY implementation."""

    def __init__(self, connector, limits):
        require(type(connector) is OkxExchange or is_native_paper_connector(connector), 'Untrusted recovery connector')
        self.connectors = {connector.name: connector}
        self.connector = connector
        self.remaining = dict(limits)

    @property
    def current_timestamp(self):
        return self.connector.current_timestamp

    def buy(self, *args, **kwargs):
        raise RuntimeError('Emergency recovery never creates BUY orders')

    def sell(self, connector_name, trading_pair, amount, order_type, price, position_action):
        require(connector_name == self.connector.name and position_action == PositionAction.CLOSE
                and order_type == OrderType.MARKET and 0 < amount <= self.remaining.get(trading_pair, Decimal(0)),
                'Emergency sale exceeds exact native ownership')
        self.remaining[trading_pair] -= amount  # Submission uncertainty never grants a second sale.
        return self.connector.sell(trading_pair, amount, order_type, price, position_action=position_action)

    def cancel(self, connector_name, trading_pair, order_id):
        require(connector_name == self.connector.name, 'Foreign cancellation route')
        return self.connector.cancel(trading_pair, order_id)


def liquidation_executor(strategy, owner, pair, amount):
    """One native market SELL with no retry after ambiguous submission; retain dust."""
    connector = strategy.connector
    price = decimal(connector.get_price_by_type(pair, PriceType.BestBid))
    require(price > 0, 'Fresh native liquidation price unavailable')
    amount = connector.quantize_order_amount(pair, amount / Decimal('1.003'))
    rule = connector.trading_rules[pair]
    if amount < rule.min_order_size or amount * price < rule.min_notional_size:
        return None
    require(amount <= decimal(connector.get_available_balance(pair.split('-')[0])), 'Owned base is unavailable')
    config = OrderExecutorConfig(timestamp=strategy.current_timestamp, controller_id=owner,
                                 connector_name=connector.name, trading_pair=pair, side=TradeType.SELL, amount=amount,
                                 position_action=PositionAction.CLOSE, execution_strategy=ExecutionStrategy.MARKET)
    executor = OrderExecutor(strategy, config, max_retries=0)
    executor.register_events()
    executor._status = RunnableStatus.RUNNING
    return executor
