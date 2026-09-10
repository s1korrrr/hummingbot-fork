"""Recorder receipts retain event precision without rewriting legacy columns."""
import asyncio
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, inspect, text

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import BuyOrderCreatedEvent, MarketEvent, OrderFilledEvent
from hummingbot.model.order import Order
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.model.trade_fill import TradeFill


@pytest.fixture
def recorder():
    previous_recorder = MarketsRecorder._shared_instance
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    engine = create_engine('sqlite:///:memory:')
    with patch('hummingbot.model.sql_connection_manager.create_engine', return_value=engine):
        manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS)
    market = SimpleNamespace(display_name='test', tracking_states={},
                             add_exchange_order_ids_from_market_recorder=lambda value: None,
                             add_trade_fills_from_market_recorder=lambda value: None)
    rec = MarketsRecorder(manager, [], 'test.yml', 'test', MarketDataCollectionConfigMap())
    try:
        yield rec, market, manager
    finally:
        MarketsRecorder._shared_instance = previous_recorder
        engine.dispose()
        loop.close()
        asyncio.set_event_loop(None)


def create(rec, market):
    rec._did_create_order(MarketEvent.BuyOrderCreated.value, market,
                          BuyOrderCreatedEvent(1, OrderType.LIMIT_MAKER, 'BTC-USDC', D('0.000210987654'),
                                               D('78240.12345678'), 'order', 1, exchange_order_id='exchange'))


def fill(rec, market):
    rec._did_fill_order(MarketEvent.OrderFilled.value, market,
                        OrderFilledEvent(2, 'order', 'BTC-USDC', TradeType.BUY, OrderType.LIMIT_MAKER,
                                         D('78240.12345678'), D('0.000210987654'),
                                         AddedToCostTradeFee(flat_fees=[TokenAmount('USDC', D('0.000000123456'))]),
                                         exchange_trade_id='fill'))


def test_recorder_retains_exact_small_fill_and_legacy_columns(recorder):
    rec, market, manager = recorder
    create(rec, market)
    fill(rec, market)
    assert 'TradeFillExact' in inspect(manager.engine).get_table_names()
    from hummingbot.model.exact_order_values import read_exact_values
    with manager.get_new_session() as session:
        trade = session.query(TradeFill).one()
        order = session.query(Order).one()
        assert trade.amount == D('0.000210')
        values = read_exact_values(session, trade)
        assert values.amount == D('0.000210987654')
        assert values.price == D('78240.12345678')
        assert values.fee_in_quote == D('0.000000123456')
        assert values.precision == 'event_exact'
        assert read_exact_values(session, order).amount == D('0.000210987654')


def test_position_reconciliation_uses_exact_receipt_without_rewriting_compatibility_rows(recorder):
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator

    rec, market, manager = recorder
    create(rec, market)
    fill(rec, market)
    trades = rec.get_trades_for_config('test.yml')
    record = ExecutorOrchestrator._trade_fill_order_record(trades[0])
    assert record['executed_amount_base'] == D('0.000210987654')
    assert record['cumulative_fee_paid_quote'] == D('0.000000123456')
    assert record['precision'] == 'event_exact'
    with manager.get_new_session() as session:
        assert session.query(TradeFill).one().amount == D('0.000210')


def test_legacy_rows_are_explicitly_precision_limited(recorder):
    rec, market, manager = recorder
    create(rec, market)
    fill(rec, market)
    assert 'TradeFillExact' in inspect(manager.engine).get_table_names()
    from hummingbot.model.exact_order_values import read_exact_values
    with manager.engine.begin() as conn:
        conn.execute(text('DELETE FROM "TradeFillExact"'))
        conn.execute(text('UPDATE "TradeFill" SET exact_amount = NULL, exact_price = NULL, '
                          'exact_trade_fee_in_quote = NULL, exact_receipt_source = NULL'))
        conn.execute(text('DELETE FROM "OrderExact"'))
    with manager.get_new_session() as session:
        for row in [session.query(TradeFill).one(), session.query(Order).one()]:
            values = read_exact_values(session, row)
            assert values.precision == 'legacy_6dp'
            assert values.amount == D('0.000210')


def test_duplicate_events_do_not_duplicate_rows_or_status(recorder):
    rec, market, manager = recorder
    create(rec, market)
    fill(rec, market)
    create(rec, market)
    fill(rec, market)
    with manager.engine.connect() as conn:
        for table in ['Order', 'TradeFill', 'OrderExact', 'TradeFillExact']:
            assert conn.execute(text(f'SELECT count(*) FROM "{table}"')).scalar() == 1
        assert conn.execute(text('SELECT count(*) FROM "OrderStatus"')).scalar() == 2


def test_receipt_and_legacy_record_roll_back_together(recorder):
    rec, market, manager = recorder
    create(rec, market)
    with patch.object(rec, 'save_market_states', side_effect=RuntimeError('forced rollback')):
        with pytest.raises(RuntimeError, match='forced rollback'):
            fill(rec, market)
    assert 'TradeFillExact' in inspect(manager.engine).get_table_names()
    with manager.engine.connect() as conn:
        for table in ['TradeFill', 'TradeFillExact']:
            assert conn.execute(text(f'SELECT count(*) FROM "{table}"')).scalar() == 0


def test_old_database_without_companion_tables_reads_without_schema_writes(recorder):
    rec, market, manager = recorder
    create(rec, market)
    fill(rec, market)
    from hummingbot.model.exact_order_values import read_exact_values
    with manager.engine.begin() as conn:
        conn.execute(text('DROP TABLE "TradeFillExact"'))
        conn.execute(text('UPDATE "TradeFill" SET exact_amount = NULL, exact_price = NULL, '
                          'exact_trade_fee_in_quote = NULL, exact_receipt_source = NULL'))
        conn.execute(text('DROP TABLE "OrderExact"'))
    with manager.get_new_session() as session:
        assert read_exact_values(session, session.query(TradeFill).one()).precision == 'legacy_6dp'
    assert 'TradeFillExact' not in inspect(manager.engine).get_table_names()


def test_fee_conversion_failure_remains_unavailable_in_exact_receipt(recorder):
    rec, market, manager = recorder
    create(rec, market)
    with patch.object(AddedToCostTradeFee, 'fee_amount_in_token', side_effect=ValueError('no conversion')):
        fill(rec, market)
    from hummingbot.model.exact_order_values import read_exact_values
    with manager.get_new_session() as session:
        trade = session.query(TradeFill).one()
        assert trade.trade_fee_in_quote is None  # Preserve published unavailable-fee semantics.
        assert read_exact_values(session, trade).fee_in_quote is None


def test_conflicting_duplicate_is_rejected_without_overwriting_receipt(recorder):
    rec, market, manager = recorder
    create(rec, market)
    fill(rec, market)
    with pytest.raises(ValueError, match='Conflicting'):
        rec._did_fill_order(MarketEvent.OrderFilled.value, market,
                            OrderFilledEvent(2, 'order', 'BTC-USDC', TradeType.BUY, OrderType.LIMIT_MAKER,
                                             D('78240.12345678'), D('0.000210999999'),
                                             AddedToCostTradeFee(), exchange_trade_id='fill'))
    from hummingbot.model.exact_order_values import read_exact_values
    with manager.get_new_session() as session:
        assert read_exact_values(session, session.query(TradeFill).one()).amount == D('0.000210987654')


def test_order_receipt_rolls_back_and_conflicting_duplicate_preserves_original(recorder):
    rec, market, manager = recorder
    with patch.object(rec, 'save_market_states', side_effect=RuntimeError('forced rollback')):
        with pytest.raises(RuntimeError, match='forced rollback'):
            create(rec, market)
    with manager.engine.connect() as conn:
        for table in ['Order', 'OrderExact']:
            assert conn.execute(text(f'SELECT count(*) FROM "{table}"')).scalar() == 0
    create(rec, market)
    with pytest.raises(ValueError, match='Conflicting'):
        rec._did_create_order(MarketEvent.BuyOrderCreated.value, market,
                              BuyOrderCreatedEvent(1, OrderType.LIMIT_MAKER, 'BTC-USDC', D('0.000211'),
                                                   D('78240.12345678'), 'order', 1, exchange_order_id='exchange'))
    from hummingbot.model.exact_order_values import read_exact_values
    with manager.get_new_session() as session:
        assert read_exact_values(session, session.query(Order).one()).amount == D('0.000210987654')


def test_native_exact_receipt_survives_absent_companion_and_is_not_downgraded(recorder):
    rec, market, manager = recorder
    create(rec, market)
    fill(rec, market)
    from hummingbot.model.exact_order_values import read_exact_values
    with manager.engine.begin() as conn:
        conn.execute(text('DROP TABLE "TradeFillExact"'))
    with manager.get_new_session() as session:
        values = read_exact_values(session, session.query(TradeFill).one())
        assert values.precision == 'event_exact'
        assert values.amount == D('0.000210987654')
        assert values.fee_in_quote == D('0.000000123456')


def test_native_exact_and_companion_disagreement_fails_closed(recorder):
    rec, market, manager = recorder
    create(rec, market)
    fill(rec, market)
    from hummingbot.model.exact_order_values import read_exact_values
    with manager.engine.begin() as conn:
        conn.execute(text('UPDATE "TradeFillExact" SET amount = :amount'), {'amount': '0.000211'})
    with manager.get_new_session() as session:
        with pytest.raises(ValueError, match='conflict'):
            read_exact_values(session, session.query(TradeFill).one())
