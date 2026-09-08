from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from hummingbot.connector.exchange.okx.native_reconciliation import verify_no_open_orders


@pytest.mark.asyncio
async def test_empty_regular_and_every_spot_algo_type_are_required():
    connector = SimpleNamespace(trading_pairs=['BTC-USDC'],
                                exchange_symbol_associated_to_pair=AsyncMock(return_value='BTC-USDC'),
                                _api_get=AsyncMock(return_value={'code': '0', 'data': []}), logger=Mock())
    assert await verify_no_open_orders(connector) is True
    calls = connector._api_get.await_args_list
    assert len(calls) == 8
    assert {call.kwargs['params'].get('ordType') for call in calls} == {
        None, 'conditional', 'oco', 'trigger', 'move_order_stop', 'iceberg', 'twap', 'smart_iceberg'}
    assert all(call.kwargs['is_auth_required'] for call in calls)
    assert all(call.kwargs['params']['instId'] == 'BTC-USDC' for call in calls)


@pytest.mark.asyncio
@pytest.mark.parametrize('response', [{'code': '1', 'data': []}, {'code': '0', 'data': {}},
                                      {'code': '0', 'data': [{'ordId': '1', 'instId': 'BTC-USDC'}]}])
async def test_unknown_orders_or_invalid_response_never_reconcile(response):
    connector = SimpleNamespace(trading_pairs=['BTC-USDC'],
                                exchange_symbol_associated_to_pair=AsyncMock(return_value='BTC-USDC'),
                                _api_get=AsyncMock(return_value=response), logger=Mock())
    assert await verify_no_open_orders(connector) is False


@pytest.mark.asyncio
async def test_full_page_uses_after_cursor_and_network_failure_is_unavailable():
    page = [{'ordId': str(200 - i), 'instId': 'BTC-USDC'} for i in range(100)]
    connector = SimpleNamespace(trading_pairs=['BTC-USDC'],
                                exchange_symbol_associated_to_pair=AsyncMock(return_value='BTC-USDC'),
                                _api_get=AsyncMock(side_effect=[{'code': '0', 'data': page}, {'code': '0', 'data': []}]), logger=Mock())
    assert await verify_no_open_orders(connector) is False
    assert connector._api_get.await_args_list[1].kwargs['params']['after'] == '101'
    connector._api_get.side_effect = RuntimeError('network unavailable')
    assert await verify_no_open_orders(connector) is False
