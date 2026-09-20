"""Immutable evidence from a connector-authoritative terminal status reconciliation.

Calling this serializer is not reconciliation: producers must first verify terminal
venue state and cumulative fills, or prove that submission was rejected.
"""
from decimal import Decimal


def terminal_reconciliation(order, source, state, cumulative):
    cumulative = Decimal(str(cumulative))
    if (not cumulative.is_finite() or cumulative < 0 or cumulative > order.amount
            or cumulative != order.executed_amount_base
            or sum((fill.fill_base_amount for fill in order.order_fills.values()), Decimal('0')) != cumulative):
        raise ValueError('Terminal reconciliation fill coverage differs')
    return {'schema': 1, 'source': source, 'order_id': order.client_order_id,
            'exchange_order_id': order.exchange_order_id, 'trading_pair': order.trading_pair,
            'side': order.trade_type.name, 'amount': str(order.amount), 'filled_base': str(cumulative),
            'state': state, 'trade_ids': sorted(str(key) for key in order.order_fills)}
