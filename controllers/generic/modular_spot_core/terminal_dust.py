"""Pure native terminal-residue proof shared by portfolio and recovery readers.

It creates no ownership store, order, grant or sell authority. Callers supply
complete immutable native evidence and an independently bound lot increment.
"""
from decimal import Decimal

from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.strategy_v2.models.executors import CloseType

from .domain import PolicyError, decimal
from .native_funding import funding_projection

ZERO = Decimal('0')


def filled_base(fills, pair):
    """Owned base implied by native fills, net of fees charged in the base asset."""
    base = pair.split('-')[0]
    total = ZERO
    for fill in fills:
        amount = decimal(fill.amount_decimal, 'fill amount')
        fee = sum((decimal(item['amount'], 'fill fee', minimum=None) for item in fill.trade_fee['flat_fees']
                   if item['token'] == base), ZERO)
        total += amount - fee if fill.trade_type == 'BUY' else -amount - fee
    return total


def terminal_dust(views, evidence, fills, pair, amount_step, *, connector):
    """Retain completed tranche dust in owned quantity and cost, never sell authority.

    The native executor reports dust separately from its managed remainder.
    Individual sublot residuals can accumulate above one lot across exits.
    Inventory ownership is already validated by the symbol inventory reader;
    the fill comparison below still rejects any unexplained aggregate amount.
    """
    by_id = {info.id: info for info in evidence}
    pair_fills = [fill for fill in fills if getattr(fill, 'symbol', None) == pair]
    global_trade_ids = set()
    for fill in fills:
        trade_id = getattr(fill, 'exchange_trade_id', None)
        symbol = getattr(fill, 'symbol', None)
        market = getattr(fill, 'market', None)
        if trade_id in (None, '') or symbol in (None, '') or market in (None, ''):
            raise PolicyError('native fill identity unavailable')
        identity_key = (market, symbol, str(trade_id))
        if identity_key in global_trade_ids:
            raise PolicyError('duplicate native exchange trade identity')
        global_trade_ids.add(identity_key)
    amount, basis, used_orders, used_trades = ZERO, ZERO, set(), set()
    for key, view in views.items():
        info = by_id[key]
        custom = info.custom_info or {}
        receipt = custom.get('execution_receipt') or {}
        dust = decimal(receipt.get('residual_dust_base', '0'), 'terminal dust')
        if dust == 0:
            continue
        step = decimal((custom.get('close_quantity') or {}).get('quantity_increment'),
                       'dust lot', positive=True)
        lifecycle = custom.get('entry_lifecycle')
        if (not view.terminal or info.is_active or not info.is_done or view.held
                or view.remaining_base != 0 or receipt['accounting_owner'] != 'executor'
                or decimal(receipt['remaining_owned_base'], 'dust remainder') != 0
                or step != amount_step or dust >= step or view.entry_quote <= 0
                or info.close_type in (None, CloseType.POSITION_HOLD)
                or decimal(info.close_timestamp, 'dust close time', positive=True) <= 0):
            raise PolicyError('unverified native terminal dust')
        if not isinstance(lifecycle, dict) or lifecycle.get('state') != 'managing_fill':
            raise PolicyError('terminal dust entry lifecycle is unresolved')
        order_ids = custom.get('order_ids')
        snapshots = custom.get('execution_order_snapshots')
        if (not isinstance(order_ids, list) or not order_ids
                or any(not isinstance(key, str) or not key for key in order_ids)
                or len(set(order_ids)) != len(order_ids) or used_orders.intersection(order_ids)
                or not isinstance(snapshots, list) or len(snapshots) != len(order_ids)):
            raise PolicyError('terminal dust order identity unavailable')
        order_by_id = {}
        exchange_order_ids = set()
        snapshot_trade_ids = set()
        for order in snapshots:
            if not isinstance(order, dict):
                raise PolicyError('terminal dust order snapshot invalid')
            order_id = order.get('client_order_id')
            exchange_order_id = order.get('exchange_order_id')
            trade_type = order.get('trade_type')
            try:
                state = OrderState(int(str(order.get('last_state'))))
            except (TypeError, ValueError):
                raise PolicyError('terminal dust order state unavailable')
            if (order_id not in order_ids or order_id in order_by_id
                    or not isinstance(exchange_order_id, str) or not exchange_order_id
                    or exchange_order_id in exchange_order_ids
                    or order.get('trading_pair') != pair or trade_type not in {'BUY', 'SELL'}
                    or state not in {OrderState.CANCELED, OrderState.FILLED}):
                raise PolicyError('terminal dust order identity invalid')
            exchange_order_ids.add(exchange_order_id)
            order_by_id[order_id] = order
            order_fills = order.get('order_fills')
            if not isinstance(order_fills, dict):
                raise PolicyError('terminal dust order fills unavailable')
            for trade_id, order_fill in order_fills.items():
                if (not isinstance(trade_id, str) or not trade_id or trade_id in snapshot_trade_ids
                        or not isinstance(order_fill, dict) or order_fill.get('trade_id') != trade_id
                        or order_fill.get('client_order_id') != order_id
                        or order_fill.get('exchange_order_id') != exchange_order_id
                        or order_fill.get('trading_pair') != pair):
                    raise PolicyError('terminal dust snapshot fill identity invalid')
                snapshot_trade_ids.add(trade_id)
        if set(order_by_id) != set(order_ids):
            raise PolicyError('terminal dust order snapshots incomplete')
        # A native order or fill cannot be attributed to both this completed
        # executor and another active, held, or terminal executor.
        for other in evidence:
            if getattr(other, 'id', None) == getattr(info, 'id', None):
                continue
            other_custom = getattr(other, 'custom_info', None) or {}
            other_ids = other_custom.get('order_ids')
            if other_ids is not None:
                if (not isinstance(other_ids, list)
                        or any(not isinstance(order_id, str) or not order_id for order_id in other_ids)):
                    raise PolicyError('native executor order identity is malformed')
                if set(order_ids).intersection(other_ids):
                    raise PolicyError('terminal dust order is attributed to multiple executors')
            other_orders = []
            for snapshot_key in ('execution_order_snapshots', 'held_position_orders'):
                snapshots_for_key = other_custom.get(snapshot_key) or []
                if not isinstance(snapshots_for_key, list):
                    raise PolicyError('native order snapshot collection is malformed')
                other_orders.extend(snapshots_for_key)
            entry_snapshot = other_custom.get('entry_order_snapshot')
            if entry_snapshot is not None:
                other_orders.append(entry_snapshot)
            for other_order in other_orders:
                if not isinstance(other_order, dict):
                    raise PolicyError('native order snapshot is malformed')
                if (other_order.get('client_order_id') in order_by_id
                        or (other_order.get('exchange_order_id') is not None
                            and other_order.get('exchange_order_id') in exchange_order_ids)):
                    raise PolicyError('terminal dust order is attributed to multiple executors')
                other_fills = other_order.get('order_fills') or {}
                if not isinstance(other_fills, dict):
                    raise PolicyError('native order snapshot fills are malformed')
                if snapshot_trade_ids.intersection(other_fills):
                    raise PolicyError('terminal dust fill is attributed to multiple executors')
        for fill in fills:
            if fill.order_id in order_ids and getattr(fill, 'symbol', None) != pair:
                raise PolicyError('terminal dust order has a foreign-market fill')
        selected = [fill for fill in pair_fills if fill.order_id in order_ids]
        fills_by_order = {}
        for fill in selected:
            trade_id = str(fill.exchange_trade_id)
            order_id = str(fill.order_id)
            if trade_id in used_trades:
                raise PolicyError('terminal dust fill identity is duplicated')
            fills_by_order.setdefault(order_id, {})
            if trade_id in fills_by_order[order_id]:
                raise PolicyError('terminal dust native fill identity duplicated')
            fills_by_order[order_id][trade_id] = fill
        totals = {'BUY': [ZERO, ZERO], 'SELL': [ZERO, ZERO]}
        for order_id in order_ids:
            order = order_by_id[order_id]
            native_fills = fills_by_order.get(order_id, {})
            snapshot_fills = order['order_fills']
            state = OrderState(int(str(order['last_state'])))
            executed_base = decimal(order.get('executed_amount_base'), 'dust order executed base')
            executed_quote = decimal(order.get('executed_amount_quote'), 'dust order executed quote')
            order_amount = decimal(order.get('amount'), 'dust order amount', positive=True)
            if (executed_base < 0 or executed_quote < 0 or executed_base > order_amount
                    or state == OrderState.FILLED and executed_base != order_amount):
                raise PolicyError('terminal dust order execution totals invalid')
            if not native_fills:
                if (snapshot_fills or state != OrderState.CANCELED or executed_base != 0 or executed_quote != 0):
                    raise PolicyError('empty terminal dust order lacks native zero-fill cancellation proof')
                continue
            if set(snapshot_fills) != set(native_fills):
                raise PolicyError('terminal dust native fills differ from order snapshot')
            fill_base = fill_quote = ZERO
            for trade_id, fill in native_fills.items():
                raw = snapshot_fills[trade_id]
                if (str(fill.trade_type).upper() != order['trade_type']
                        or str(fill.order_id) != order_id
                        or getattr(fill, 'market', None) != connector
                        or getattr(fill, 'symbol', None) != pair):
                    raise PolicyError('terminal dust fill-to-order attribution mismatch')
                filled = decimal(fill.amount_decimal, 'dust fill amount', positive=True)
                price = decimal(fill.price_decimal, 'dust fill price', positive=True)
                quote = filled * price
                if (decimal(raw.get('fill_base_amount'), 'snapshot fill base') != filled
                        or decimal(raw.get('fill_price'), 'snapshot fill price') != price
                        or decimal(raw.get('fill_quote_amount'), 'snapshot fill quote') != quote
                        or raw.get('fee') != fill.trade_fee):
                    raise PolicyError('terminal dust exact fill receipt mismatch')
                fill_base += filled
                fill_quote += quote
                totals[order['trade_type']][0] += filled
                totals[order['trade_type']][1] += quote
                used_trades.add(trade_id)
            if fill_base != executed_base or fill_quote != executed_quote:
                raise PolicyError('terminal dust native order totals mismatch')
        if not totals['BUY'][0] or not totals['SELL'][0]:
            raise PolicyError('terminal dust needs native entry and exit fills')
        if (totals['BUY'][0] <= 0 or totals['SELL'][0] <= 0
                or totals['BUY'] != [decimal(receipt['entry_gross_base'], 'dust entry base'),
                                     decimal(receipt['entry_gross_quote'], 'dust entry quote')]
                or totals['SELL'] != [decimal(receipt['exit_gross_base'], 'dust exit base'),
                                      decimal(receipt['exit_gross_quote'], 'dust exit quote')]
                or filled_base(selected, pair) != dust):
            raise PolicyError('terminal dust native fill arithmetic mismatch')
        try:
            exact_projection = funding_projection(selected, connector, pair)
        except (ValueError, TypeError, ArithmeticError, KeyError) as exc:
            raise PolicyError('terminal dust native cost projection unavailable') from exc
        if (exact_projection['inventory_base'] != dust or not exact_projection['inventory_cost'].is_finite()
                or exact_projection['inventory_cost'] < 0):
            raise PolicyError('terminal dust native cost projection mismatch')
        used_orders.update(order_ids)
        amount += dust
        basis += exact_projection['inventory_cost']
    return amount, basis
