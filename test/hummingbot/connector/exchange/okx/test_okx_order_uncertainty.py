import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.core.data_type.common import OrderType, TradeType


@pytest.mark.asyncio
@pytest.mark.parametrize('error', [IOError('HTTP 503'), asyncio.TimeoutError(), ValueError('malformed response')])
async def test_transport_errors_do_not_fail_active_orders_or_release_reservations(error):
    exchange = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDT'])
    exchange.start_tracking_order(order_id='uncertain', exchange_order_id='venue', trading_pair='ETH-USDT',
                                  trade_type=TradeType.BUY, price=Decimal('100'), amount=Decimal('1'),
                                  order_type=OrderType.LIMIT)
    order = exchange.in_flight_orders['uncertain']
    for _ in range(5):
        await exchange._handle_update_error_for_active_order(order, error)
    assert 'uncertain' in exchange.in_flight_orders
    assert not order.is_failure
    assert exchange._order_tracker._order_not_found_records.get('uncertain', 0) == 0


@pytest.mark.asyncio
async def test_cancel_timeout_retains_active_order():
    exchange = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDT'])
    exchange.start_tracking_order(order_id='uncertain', exchange_order_id='venue', trading_pair='ETH-USDT',
                                  trade_type=TradeType.BUY, price=Decimal('100'), amount=Decimal('1'),
                                  order_type=OrderType.LIMIT)
    order = exchange.in_flight_orders['uncertain']
    exchange._execute_order_cancel_and_process_update = AsyncMock(side_effect=asyncio.TimeoutError())
    for _ in range(5):
        await exchange._execute_order_cancel(order)
    assert 'uncertain' in exchange.in_flight_orders
    assert not order.is_failure


@pytest.mark.asyncio
async def test_submission_timeout_retains_client_order_identity_for_status_recovery():
    from hummingbot.connector.trading_rule import TradingRule
    from hummingbot.core.data_type.in_flight_order import OrderState

    exchange = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDT'])
    exchange._trading_rules['ETH-USDT'] = TradingRule('ETH-USDT', min_order_size=Decimal('.01'),
                                                      min_price_increment=Decimal('.01'),
                                                      min_base_amount_increment=Decimal('.01'))
    exchange._place_order = AsyncMock(side_effect=asyncio.TimeoutError())
    await exchange._create_order(TradeType.BUY, 'uncertain', 'ETH-USDT', Decimal('1'), OrderType.LIMIT, Decimal('100'))
    await asyncio.sleep(0)
    assert 'uncertain' in exchange.in_flight_orders
    order = exchange.in_flight_orders['uncertain']
    assert not order.is_failure
    exchange._request_order_update = AsyncMock(return_value={'data': [{'state': 'live', 'ordId': 'venue', 'uTime': '1000000'}]})
    update = await exchange._request_order_status(order)
    exchange._order_tracker.process_order_update(update)
    await asyncio.sleep(0)
    assert order.exchange_order_id == 'venue'
    assert order.current_state == OrderState.OPEN
    exchange._place_order.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('fills_available', [True, False])
async def test_cancellation_waits_for_reported_partial_fills(fills_available):
    from hummingbot.core.data_type.in_flight_order import TradeUpdate
    from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee

    exchange = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDT'])
    exchange.start_tracking_order(order_id='partial', exchange_order_id='venue', trading_pair='ETH-USDT',
                                  trade_type=TradeType.BUY, price=Decimal('100'), amount=Decimal('1'), order_type=OrderType.LIMIT)
    order = exchange.in_flight_orders['partial']
    exchange._request_order_update = AsyncMock(return_value={'data': [{'state': 'canceled', 'accFillSz': '.4',
                                                                       'ordId': 'venue', 'uTime': '1000000'}]})
    trade = TradeUpdate(trade_id='fill', client_order_id='partial', exchange_order_id='venue', trading_pair='ETH-USDT',
                        fill_timestamp=999, fill_price=Decimal('100'), fill_base_amount=Decimal('.4'),
                        fill_quote_amount=Decimal('40'), fee=AddedToCostTradeFee())
    exchange._all_trade_updates_for_order = AsyncMock(return_value=[trade] if fills_available else [])
    if fills_available:
        update = await exchange._request_order_status(order)
        assert order.executed_amount_base == Decimal('.4')
        exchange._order_tracker.process_order_update(update)
        await asyncio.sleep(0)
        assert order.is_cancelled
    else:
        with pytest.raises(IOError, match='fill'):
            await exchange._request_order_status(order)
        assert not order.is_done


@pytest.mark.asyncio
async def test_explicit_negative_venue_ack_is_a_real_rejection():
    from hummingbot.connector.exchange.okx.okx_exchange import OkxOrderRejectedError
    from hummingbot.connector.trading_rule import TradingRule
    exchange = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDT'])
    exchange._trading_rules['ETH-USDT'] = TradingRule('ETH-USDT', min_order_size=Decimal('.01'),
                                                      min_price_increment=Decimal('.01'),
                                                      min_base_amount_increment=Decimal('.01'))
    exchange._place_order = AsyncMock(side_effect=OkxOrderRejectedError('sCode=51008: insufficient balance'))
    await exchange._create_order(TradeType.BUY, 'rejected', 'ETH-USDT', Decimal('1'), OrderType.LIMIT, Decimal('100'))
    await asyncio.sleep(0)
    assert 'rejected' not in exchange.in_flight_orders
    assert exchange._order_tracker.fetch_order('rejected').is_failure
