"""Terminal paper proof from committed native recorder rows, without a second ledger."""
from decimal import Decimal
from types import SimpleNamespace

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.model.exact_order_values import OrderExact, TradeFillExact, recorded_values
from hummingbot.model.order import Order
from hummingbot.model.order_status import OrderStatus
from hummingbot.model.trade_fill import TradeFill


def recorded_terminal_order(recorder, market, tracked):
    """Return proof only when exact native order/fill rows cover the caller's order.

    A missing/stale caller fill cannot be treated as canceled inventory. Native
    reconciliation must first recover that fill; this reader fails closed.
    """
    with recorder.sql_manager.get_new_session() as session:
        row = session.get(Order, tracked.client_order_id)
        if row is None or row.config_file_path != recorder.config_file_path or row.market != market.display_name:
            return None
        state = {'OrderCancelled': OrderState.CANCELED, 'BuyOrderCompleted': OrderState.FILLED,
                 'SellOrderCompleted': OrderState.FILLED}.get(row.last_status)
        if state is None:
            return None
        exact = session.get(OrderExact, row.id)
        if exact is None:
            raise ValueError('Native paper terminal order lacks exact creation receipt')
        amount, price = Decimal(exact.amount), Decimal(exact.price)
        statuses = {s.status for s in session.query(OrderStatus).filter_by(order_id=row.id)}
        sides = {side for event, side in (('BuyOrderCreated', TradeType.BUY), ('SellOrderCreated', TradeType.SELL))
                 if event in statuses}
        if (len(sides) != 1 or row.last_status not in statuses or row.exchange_order_id != tracked.exchange_order_id
                or row.symbol != tracked.trading_pair or amount != tracked.amount or price != tracked.price
                or sides != {tracked.trade_type} or row.order_type != tracked.order_type.name):
            raise ValueError('Native paper terminal order identity differs')
        fills = session.query(TradeFill).filter_by(order_id=row.id).all()
        proof_fills = {}
        for fill in fills:
            if fill.market != row.market or fill.config_file_path != row.config_file_path or fill.symbol != row.symbol:
                raise ValueError('Native paper terminal fill scope differs')
            values = recorded_values(fill, session.get(TradeFillExact, (fill.market, fill.order_id, fill.exchange_trade_id)))
            expected = tracked.order_fills.get(fill.exchange_trade_id)
            if (values.precision != 'event_exact' or expected is None
                    or values.amount != expected.fill_base_amount or values.price != expected.fill_price
                    or values.amount * values.price != expected.fill_quote_amount
                    or expected.trade_id != fill.exchange_trade_id or expected.client_order_id != row.id
                    or expected.exchange_order_id != row.exchange_order_id or expected.trading_pair != row.symbol
                    or int(expected.fill_timestamp * 1000) != fill.timestamp
                    or fill.trade_fee != expected.fee.to_json() or fill.trade_type != tracked.trade_type.name):
                raise ValueError('Native paper terminal fill receipt differs')
            proof_fills[fill.exchange_trade_id] = SimpleNamespace(fill_base_amount=values.amount,
                                                                  fill_quote_amount=values.amount * values.price)
        cumulative = sum((f.fill_base_amount for f in proof_fills.values()), Decimal('0'))
        cumulative_quote = sum((f.fill_quote_amount for f in proof_fills.values()), Decimal('0'))
        if (set(proof_fills) != set(tracked.order_fills) or cumulative != tracked.executed_amount_base
                or cumulative_quote != tracked.executed_amount_quote
                or cumulative < 0 or cumulative > amount or (state == OrderState.FILLED and cumulative != amount)):
            raise ValueError('Native paper terminal fill coverage differs')
        return SimpleNamespace(client_order_id=row.id, exchange_order_id=row.exchange_order_id,
                               trading_pair=row.symbol, trade_type=tracked.trade_type, amount=amount,
                               current_state=state, executed_amount_base=cumulative,
                               executed_amount_quote=cumulative_quote, order_fills=proof_fills)
