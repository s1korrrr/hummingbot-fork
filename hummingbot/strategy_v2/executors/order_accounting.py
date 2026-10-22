"""Asset-denominated fee accounting from actual fills (never converted fee totals)."""
from decimal import Decimal, InvalidOperation


def base_fee_amount(order_record, base_asset):
    total = Decimal('0')
    for fill in order_record.get('order_fills', {}).values():
        fee = fill.get('fee') or {}
        total += sum((Decimal(str(item['amount'])) for item in fee.get('flat_fees', [])
                      if item['token'] == base_asset), Decimal('0'))
        token = fee.get('percent_token')
        if token == base_asset or (token is None and (
                fee.get('fee_type') == 'DeductedFromReturns' and order_record.get('trade_type') == 'BUY'
                or fee.get('fee_type') == 'AddedToCost' and order_record.get('trade_type') == 'SELL')):
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


def base_fee_quote_amount(order_record, base_asset):
    """Value only the base-asset fee at its own fill price.

    Gross order VWAP cannot value this offset: partial fills may have different
    fee rates/assets. This is the cost-basis offset paired with net base quantity;
    total valued fees still get charged once by the accounting owner. Other-token
    fees never enter this offset or require conversion here.
    """
    total = Decimal('0')
    for fill in order_record.get('order_fills', {}).values():
        amount = base_fee_amount({'trade_type': order_record.get('trade_type'),
                                  'order_fills': {'fill': fill}}, base_asset)
        if amount == 0:
            continue
        try:
            price = Decimal(str(fill.get('fill_price')))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError('Base fee requires an exact fill price') from exc
        if not price.is_finite() or price <= 0:
            raise ValueError('Base fee requires a finite positive fill price')
        total += amount * price
    if not total.is_finite():
        raise ValueError('Non-finite base fee quote value')
    return total


def in_flight_base_fee_quote(order, base_asset):
    return base_fee_quote_amount({
        'trade_type': order.trade_type.name,
        'order_fills': {key: {'fee': fill.fee.to_json(), 'fill_base_amount': str(fill.fill_base_amount),
                              'fill_price': str(fill.fill_price)}
                        for key, fill in order.order_fills.items()},
    }, base_asset)
