import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType


@pytest.mark.asyncio
async def test_unattended_restart_hold_rejects_order_before_durable_commit(monkeypatch):
    monkeypatch.setenv('RSIBOT_V2_UNATTENDED_RESTART_HOLD', '1')
    monkeypatch.setenv('RSIBOT_V2_DURABLE_ORDER_TRACKING', '1')
    market = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=['ETH-USDC'])
    market.start_tracking_order('owned-client', None, 'ETH-USDC', TradeType.SELL,
                                Decimal('100'), Decimal('.09'), OrderType.LIMIT_MAKER)
    with patch.object(MarketsRecorder, 'get_existing_instance') as recorder, \
            patch.object(market, '_place_order', new_callable=AsyncMock) as place:
        with pytest.raises(RuntimeError, match='unattended restart hold'):
            await market._place_order_and_process_update(market.in_flight_orders['owned-client'])
    recorder.assert_not_called()
    place.assert_not_awaited()


@pytest.mark.asyncio
async def test_unattended_restart_hold_rejects_direct_okx_place(monkeypatch):
    monkeypatch.setenv('RSIBOT_V2_UNATTENDED_RESTART_HOLD', '1')
    market = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=['ETH-USDC'])
    with patch.object(market, '_api_request', new_callable=AsyncMock) as request:
        with pytest.raises(RuntimeError, match='unattended restart hold'):
            await market._place_order('owned-client', 'ETH-USDC', Decimal('.09'), TradeType.SELL,
                                      OrderType.LIMIT_MAKER, Decimal('100'))
    request.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('kind', [OrderType.LIMIT_MAKER, OrderType.MARKET])
async def test_native_market_state_is_durable_before_venue_submission(tmp_path, monkeypatch, enabled, kind):
    monkeypatch.setenv('RSIBOT_V2_DURABLE_ORDER_TRACKING', '1' if enabled else '0')
    market = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=['ETH-USDC'])
    sql = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                               db_path=str(tmp_path / 'rsi_modular_v2.sqlite'))
    prior = MarketsRecorder._shared_instance
    recorder = MarketsRecorder(sql, [market], 'rsi_modular_v2.yml', 'v2_with_controllers',
                               MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    market.start_tracking_order('owned-client', None, 'ETH-USDC', TradeType.SELL,
                                Decimal('NaN') if kind == OrderType.MARKET else Decimal('100'), Decimal('.09'), kind)
    order = market.in_flight_orders['owned-client']

    async def crash_after_submission(**kwargs):
        with sql.get_new_session() as session:
            stored = recorder.get_market_states('rsi_modular_v2.yml', market, session)
        if enabled:
            assert stored.saved_state['owned-client']['last_state'] == '0'
            assert stored.saved_state['owned-client']['exchange_order_id'] is None
        else:
            assert stored is None
        raise asyncio.CancelledError()
    try:
        with patch.object(market, '_place_order', side_effect=crash_after_submission):
            with pytest.raises(asyncio.CancelledError):
                await market._place_order_and_process_update(order)
        replacement = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=['ETH-USDC'])
        recorder.restore_market_states('rsi_modular_v2.yml', replacement)
        assert ('owned-client' in replacement.in_flight_orders) == enabled
        if enabled:
            from hummingbot.strategy_v2.executors.supervised_recovery import load_native_orders
            native_orders = load_native_orders(recorder, {'modular_spot_v2_eth': 'ETH-USDC'})
            assert native_orders['owned-client'].order_type == kind
            market._order_tracker.stop_tracking_order('owned-client')
            with sql.get_new_session() as session:
                with session.begin():
                    recorder.save_market_states(recorder.config_file_path, market, session)
            # A later tracker checkpoint cannot erase the immutable submitted identity.
            assert 'owned-client' in load_native_orders(recorder, {'modular_spot_v2_eth': 'ETH-USDC'})
    finally:
        MarketsRecorder._shared_instance = prior
        sql.engine.dispose()


@pytest.mark.asyncio
async def test_opted_in_submission_rejects_missing_or_foreign_recorder(monkeypatch):
    monkeypatch.setenv('RSIBOT_V2_DURABLE_ORDER_TRACKING', '1')
    market = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=['ETH-USDC'])
    market.start_tracking_order('owned-client', None, 'ETH-USDC', TradeType.BUY, Decimal('100'),
                                Decimal('.09'), OrderType.LIMIT_MAKER)
    with patch.object(MarketsRecorder, '_shared_instance', None), patch.object(market, '_place_order', new_callable=AsyncMock) as place:
        with pytest.raises(ValueError, match='durable V2'):
            await market._place_order_and_process_update(market.in_flight_orders['owned-client'])
        place.assert_not_awaited()
