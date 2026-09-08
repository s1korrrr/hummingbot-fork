"""Authoritative, read-only OKX SPOT order reconciliation for opt-in native shutdown.

Protocol: https://my.okx.com/docs-v5/en/#order-book-trading-trade-get-order-list
and #order-book-trading-algo-trading-get-algo-order-list (checked 2026-09-08).
"""
from hummingbot.connector.exchange.okx import okx_constants as CONSTANTS

SPOT_ALGO_TYPES = ('conditional', 'oco', 'trigger', 'move_order_stop', 'iceberg', 'twap', 'smart_iceberg')


async def _pending_records(connector, path, params, id_key):
    records = []
    cursors = set()
    for _ in range(100):
        response = await connector._api_get(path_url=path, params=dict(params), is_auth_required=True)
        if not isinstance(response, dict) or response.get('code') != '0' or not isinstance(response.get('data'), list):
            raise ValueError('Invalid pending-order response')
        page = response['data']
        if len(page) > 100:
            raise ValueError('Unexpected pending-order page size')
        for order in page:
            if (not isinstance(order, dict) or order.get('instId') != params['instId']
                    or not isinstance(order.get(id_key), str) or not order[id_key].isdecimal()):
                raise ValueError('Unrecognized pending-order identity')
        records.extend(page)
        if len(page) < 100:
            return records
        cursor = page[-1][id_key]
        if cursor in cursors:
            raise ValueError('Non-progressing pending-order pagination')
        cursors.add(cursor)
        params['after'] = cursor
    raise ValueError('Pending-order pagination exceeded reconciliation bound')


async def verify_no_open_orders(connector):
    try:
        if not connector.trading_pairs:
            return False
        for pair in connector.trading_pairs:
            symbol = await connector.exchange_symbol_associated_to_pair(trading_pair=pair)
            params = {'instType': 'SPOT', 'instId': symbol, 'limit': '100'}
            if await _pending_records(connector, CONSTANTS.OKX_PENDING_ORDERS_PATH, dict(params), 'ordId'):
                return False
            for order_type in SPOT_ALGO_TYPES:
                if await _pending_records(connector, CONSTANTS.OKX_PENDING_ALGO_ORDERS_PATH,
                                          dict(params, ordType=order_type), 'algoId'):
                    return False
        return True
    except Exception:
        connector.logger().exception('Native OKX open-order reconciliation unavailable')
        return False
