from decimal import Decimal as D
from unittest.mock import patch

import pytest
from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.strategy_v2.executors.supervised_recovery import (
    EmergencySellStrategy,
    inventory,
    liquidation_executor,
    load_native_orders,
    reconciled_order,
    settle_owned_orders,
)

PAIR = 'BTC-USDC'
OWNERS = {'modular_spot_v2_btc': PAIR}


def order(oid='entry', *, side=TradeType.BUY, amount='.09'):
    return InFlightOrder(oid, PAIR, OrderType.LIMIT_MAKER if side == TradeType.BUY else OrderType.LIMIT,
                         side, D(amount), 1000, price=D('100'), exchange_order_id='123')


def raw(original, state='canceled', filled='.04', trade='1', fee='-.00004', fee_ccy='BTC'):
    venue = {'clOrdId': original.client_order_id, 'instId': PAIR, 'ordId': original.exchange_order_id,
             'tdMode': 'cash', 'side': original.trade_type.name.lower(), 'state': state,
             'sz': str(original.amount), 'accFillSz': filled, 'uTime': '1001000', 'px': str(original.price),
             'ordType': 'post_only' if original.order_type == OrderType.LIMIT_MAKER else 'limit'}
    fills = [] if D(filled) == 0 else [{'tradeId': trade, 'ordId': original.exchange_order_id, 'instId': PAIR,
                                        'side': original.trade_type.name.lower(), 'fillSz': filled, 'fillPx': '100', 'fee': fee,
                                        'feeCcy': fee_ccy, 'ts': '1001000'}]
    return venue, fills


def test_native_inventory_includes_reprices_partial_exit_and_base_fees():
    first, _ = reconciled_order(order('entry-one'), *raw(order('entry-one')))
    second, _ = reconciled_order(order('entry-two'), *raw(order('entry-two'), filled='.03', fee='-.00003'))
    exit_order = order('exit', side=TradeType.SELL, amount='.06')
    exit_order, _ = reconciled_order(exit_order, *raw(exit_order, filled='.02', fee='-.00002'))
    assert inventory([first, second, exit_order], OWNERS) == {'modular_spot_v2_btc': D('.04991')}
    with pytest.raises(ValueError, match='Pending'):
        inventory([order()], OWNERS)


@pytest.mark.parametrize('damage', ['owner', 'size', 'missing_fill', 'fee', 'overfill'])
def test_raw_recovery_evidence_rejects_ambiguity(damage):
    original = order()
    venue, fills = raw(original)
    prior, _ = reconciled_order(original, venue, fills)
    if damage == 'owner':
        venue['clOrdId'] = 'V1-order'
    elif damage == 'size':
        venue['sz'] = '.2'
    elif damage == 'missing_fill':
        fills = []
    elif damage == 'fee':
        fills[0]['fee'] = '-99'
    else:
        venue['accFillSz'] = '.1'
    with pytest.raises(ValueError):
        reconciled_order(prior, venue, fills)


@pytest.mark.asyncio
async def test_actual_native_cancel_late_fill_recorder_and_owned_scope(tmp_path):
    market = OkxExchange('fixture', 'fixture', 'fixture', trading_pairs=[PAIR])
    market._set_current_timestamp(1002)
    market._set_trading_pair_symbol_map(bidict({PAIR: PAIR}))
    sql = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                               db_path=str(tmp_path / 'rsi_modular_v2.sqlite'))
    prior = MarketsRecorder._shared_instance
    recorder = MarketsRecorder(sql, [market], 'rsi_modular_v2.yml', 'v2_with_controllers',
                               MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    original = order()
    original.exchange_order_id = None  # Crash after request, before accepted response.
    market._order_tracker.start_tracking_order(original)
    with sql.get_new_session() as session:
        with session.begin():
            recorder.save_market_states(recorder.config_file_path, market, session)
    loaded = load_native_orders(recorder, OWNERS)
    assert loaded['entry'].exchange_order_id is None
    # The same wallet has an unrelated V1 order. It must never be touched.
    foreign = order('V1-order')
    market._order_tracker.start_tracking_order(foreign)
    canceled = False

    async def get(*, path_url, params, is_auth_required):
        assert is_auth_required
        known = order()
        venue, fills = raw(known, 'canceled' if canceled else 'partially_filled',
                           filled='.05' if canceled else '.04', fee='-.00005' if canceled else '-.00004')
        # A new trade, not mutation of an existing venue trade ID, arrives during cancellation.
        if canceled:
            _, earlier = raw(known)
            fills[0]['fillSz'], fills[0]['fee'], fills[0]['tradeId'] = '.01', '-.00001', '2'
            fills = earlier + fills
        return {'code': '0', 'data': [venue] if path_url.endswith('/order') else fills}

    async def post(*, path_url, data, **kwargs):
        nonlocal canceled
        assert path_url.endswith('/cancel-order') and data['clOrdId'] == 'entry'
        canceled = True
        return {'code': '0', 'data': [{'sCode': '0', 'ordId': '123', 'clOrdId': 'entry'}]}
    recorder.start()
    try:
        with patch.object(market, '_api_get', side_effect=get), patch.object(market, '_api_post', side_effect=post), \
                patch.object(recorder, 'append_to_csv'):
            settled = await settle_owned_orders(market, recorder, loaded, deadline=2000, now=lambda: 1002)
        assert inventory(settled, OWNERS)['modular_spot_v2_btc'] == D('.04995')
        assert 'V1-order' in market.in_flight_orders
        trades = recorder.get_trades_for_config('rsi_modular_v2.yml')
        assert {trade.exchange_trade_id for trade in trades} == {'1', '2'}
        assert all(trade.has_exact_receipt for trade in trades)
        restored = load_native_orders(recorder, OWNERS)
        assert restored['entry'].executed_amount_base == D('.05')
        assert restored['entry'].is_cancelled
        from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount
        from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
        with patch.object(recorder, 'append_to_csv'):
            market.trigger_event(MarketEvent.OrderFilled, OrderFilledEvent(
                1002, 'missing-sell-snapshot', PAIR, TradeType.SELL, OrderType.LIMIT,
                D('100'), D('.01'), DeductedFromReturnsTradeFee(flat_fees=[TokenAmount('USDC', D('.001'))]),
                exchange_trade_id='orphan-sell-fill'))
        with pytest.raises(ValueError, match='fill lacks exact order snapshot'):
            load_native_orders(recorder, OWNERS)
    finally:
        recorder.stop()
        MarketsRecorder._shared_instance = prior
        sql.engine.dispose()


@pytest.mark.asyncio
async def test_genuine_native_paper_liquidation_only_sells_owned_residual():
    from test.hummingbot.connector.exchange.paper_trade.test_native_paper_trade_exchange import paper
    market = paper()
    market.set_balance('BTC', D('10.09'))  # Ten BTC belongs to another owner.
    strategy = EmergencySellStrategy(market, {PAIR: D('.09')})
    executor = liquidation_executor(strategy, 'modular_spot_v2_btc', PAIR, D('.09'))
    try:
        assert executor.config.amount == D('.08')  # Native lot quantization plus conservative base-fee reserve.
        await executor.control_task()
        await market.drain()
        assert market.get_balance('BTC') == D('10.01')
        assert executor.executor_info.custom_info['held_position_orders'][0]['executed_amount_quote'] == '7.92'
        with pytest.raises(ValueError):
            strategy.sell(market.name, PAIR, D('.09'), OrderType.MARKET, D('NaN'), PositionAction.CLOSE)
        with pytest.raises(RuntimeError, match='never'):
            strategy.buy()
        assert liquidation_executor(strategy, 'modular_spot_v2_btc', PAIR, D('.001')) is None
    finally:
        executor.unregister_events()
