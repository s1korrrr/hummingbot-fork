"""Asset-denominated fee accounting from actual fills (never converted fee totals)."""
from decimal import Decimal


def base_fee_amount(order_record, base_asset):
    total = Decimal('0')
    for fill in order_record.get('order_fills', {}).values():
        fee = fill.get('fee') or {}
        total += sum((Decimal(str(item['amount'])) for item in fee.get('flat_fees', [])
                      if item['token'] == base_asset), Decimal('0'))
        token = fee.get('percent_token')
        if token == base_asset or (token is None and fee.get('fee_type') == 'DeductedFromReturns'
                                   and order_record.get('trade_type') == 'BUY'):
            total += Decimal(str(fee.get('percent', '0'))) * Decimal(str(fill['fill_base_amount']))
    if not total.is_finite():
        raise ValueError('Non-finite base fee')
    return total


def in_flight_base_fee(order, base_asset):
    """`base_fee_amount` of a live in-flight order from exactly the fields it reads.

    `InFlightOrder.to_json()` also converts every fill fee into base and quote totals, which
    this accounting never uses; building only the trade type, each fill's fee record and
    its base amount gives the same result without that work.
    """
    return base_fee_amount({
        'trade_type': order.trade_type.name,
        'order_fills': {key: {'fee': fill.fee.to_json(), 'fill_base_amount': str(fill.fill_base_amount)}
                        for key, fill in order.order_fills.items()},
    }, base_asset)
