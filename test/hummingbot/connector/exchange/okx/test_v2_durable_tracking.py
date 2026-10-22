import asyncio
import json
import threading
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.model.metadata import Metadata
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
@pytest.mark.parametrize('kind', [OrderType.LIMIT_MAKER, OrderType.MARKET])
@pytest.mark.parametrize('side', [TradeType.BUY, TradeType.SELL])
async def test_meridian_submission_commits_native_identity_before_venue_io(tmp_path, monkeypatch, kind, side):
    monkeypatch.setenv('RSIBOT_V2_DURABLE_ORDER_TRACKING', '1')
    market = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=['ETH-USDC'])
    market._set_current_timestamp(1790764800)
    sql = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                               db_path=str(tmp_path / 'meridian_v3.sqlite'))
    prior = MarketsRecorder._shared_instance
    recorder = MarketsRecorder(sql, [market], 'meridian_v3.yml', 'v2_with_controllers',
                               MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    market.start_tracking_order('meridian-client', None, 'ETH-USDC', side,
                                Decimal('NaN') if kind == OrderType.MARKET else Decimal('100'), Decimal('.09'), kind)
    order = market.in_flight_orders['meridian-client']

    async def uncertain_venue_submission(**kwargs):
        # A separate connection must see both identities before any venue I/O.
        with sql.get_new_session() as session:
            stored = recorder.get_market_states('meridian_v3.yml', market, session)
            assert stored.saved_state['meridian-client']['last_state'] == '0'
            receipt = json.loads(session.get(Metadata, 'v2-submitted-order:meridian-client').value)
            assert receipt['owner'] == 'meridian_v3.yml'
            assert receipt['order'] == order.to_json()
        raise asyncio.CancelledError()

    try:
        with patch.object(market, '_place_order', side_effect=uncertain_venue_submission) as place:
            with pytest.raises(asyncio.CancelledError):
                await market._place_order_and_process_update(order)
            place.assert_awaited_once()
            # Transport uncertainty never permits a second submission of this ID.
            with pytest.raises(ValueError, match='already submitted'):
                await market._place_order_and_process_update(order)
            place.assert_awaited_once()
        replacement = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=['ETH-USDC'])
        recorder.restore_market_states('meridian_v3.yml', replacement)
        assert replacement.in_flight_orders['meridian-client'].to_json() == order.to_json()
    finally:
        MarketsRecorder._shared_instance = prior
        sql.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize('recorder_kind', ['missing', 'foreign-owner', 'foreign-market'])
async def test_opted_in_submission_rejects_missing_or_foreign_recorder(tmp_path, monkeypatch, recorder_kind):
    monkeypatch.setenv('RSIBOT_V2_DURABLE_ORDER_TRACKING', '1')
    market = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=['ETH-USDC'])
    market.start_tracking_order('owned-client', None, 'ETH-USDC', TradeType.BUY, Decimal('100'),
                                Decimal('.09'), OrderType.LIMIT_MAKER)
    prior = MarketsRecorder._shared_instance
    sql = None
    try:
        MarketsRecorder._shared_instance = None
        if recorder_kind != 'missing':
            sql = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                       db_path=str(tmp_path / 'foreign.sqlite'))
            other_market = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=['ETH-USDC'])
            MarketsRecorder(sql, [other_market if recorder_kind == 'foreign-market' else market],
                            'meridian_v3.yml' if recorder_kind == 'foreign-market' else 'unapproved.yml',
                            'v2_with_controllers',
                            MarketDataCollectionConfigMap(market_data_collection_enabled=False))
        with patch.object(market, '_place_order', new_callable=AsyncMock) as place:
            with pytest.raises(ValueError, match='matching durable'):
                await market._place_order_and_process_update(market.in_flight_orders['owned-client'])
            place.assert_not_awaited()
    finally:
        MarketsRecorder._shared_instance = prior
        if sql is not None:
            sql.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize('owner', ['rsi_modular_v2.yml', 'meridian_v3.yml'])
async def test_failed_durable_commit_blocks_venue_submission(tmp_path, monkeypatch, owner):
    monkeypatch.setenv('RSIBOT_V2_DURABLE_ORDER_TRACKING', '1')
    market = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=['ETH-USDC'])
    sql = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                               db_path=str(tmp_path / 'failed.sqlite'))
    prior = MarketsRecorder._shared_instance
    recorder = MarketsRecorder(sql, [market], owner, 'v2_with_controllers',
                               MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    market.start_tracking_order('owned-client', None, 'ETH-USDC', TradeType.BUY, Decimal('100'),
                                Decimal('.09'), OrderType.LIMIT_MAKER)
    try:
        with patch.object(recorder, '_store_submitted_order_checkpoint', side_effect=IOError('durable storage unavailable')), \
                patch.object(market, '_place_order', new_callable=AsyncMock) as place:
            with pytest.raises(IOError, match='storage unavailable'):
                await market._place_order_and_process_update(market.in_flight_orders['owned-client'])
            place.assert_not_awaited()
        with sql.get_new_session() as session:
            assert session.get(Metadata, 'v2-submitted-order:owned-client') is None
            assert recorder.get_market_states(owner, market, session) is None
    finally:
        MarketsRecorder._shared_instance = prior
        sql.engine.dispose()


@pytest.mark.asyncio
async def test_pre_submit_checkpoint_waits_off_loop_for_writer_ack(tmp_path, monkeypatch):
    monkeypatch.setenv('RSIBOT_V2_DURABLE_ORDER_TRACKING', '1')
    market = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=['ETH-USDC'])
    sql = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                               db_path=str(tmp_path / 'held-writer.sqlite'))
    prior = MarketsRecorder._shared_instance
    recorder = MarketsRecorder(sql, [market], 'rsi_modular_v2.yml', 'v2_with_controllers',
                               MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    market.start_tracking_order('held-client', None, 'ETH-USDC', TradeType.BUY, Decimal('100'),
                                Decimal('.09'), OrderType.LIMIT_MAKER)
    entered, release = threading.Event(), threading.Event()
    place = AsyncMock(return_value=('exchange-held-client', 1.0))
    try:
        blocker = asyncio.create_task(sql.write_queue.write(
            lambda: (entered.set(), release.wait(3))))
        assert await asyncio.to_thread(entered.wait, 1)
        submission = asyncio.create_task(
            market._place_order_and_process_update(market.in_flight_orders['held-client']))
        # The event loop remains responsive while the owner writer is blocked,
        # and venue I/O cannot begin before the checkpoint ACK.
        await asyncio.sleep(.03)
        assert not submission.done()
        with patch.object(market, '_place_order', place):
            release.set()
            await blocker
            await submission
        place.assert_awaited_once()
        with sql.get_new_session() as session:
            checkpoint = recorder.get_market_states(recorder.config_file_path, market, session)
            assert checkpoint.saved_state['held-client']['last_state'] == '0'
            assert session.get(Metadata, 'v2-submitted-order:held-client') is not None
    finally:
        release.set()
        MarketsRecorder._shared_instance = prior
        await sql.write_queue.close()
        sql.engine.dispose()
