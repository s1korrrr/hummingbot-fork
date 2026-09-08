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
