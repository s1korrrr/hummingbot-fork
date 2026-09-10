import asyncio
import sqlite3
from decimal import Decimal as D
from unittest.mock import MagicMock

import pytest

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.model.trade_fill import TradeFill


@pytest.fixture
def manager(tmp_path):
    result = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                  db_path=str(tmp_path / 'receipts.sqlite'))
    yield result
    result.engine.dispose()


def trade(**changes):
    values = dict(config_file_path='test.yml', strategy='test', market='okx', symbol='BTC-USDC',
                  base_asset='BTC', quote_asset='USDC', timestamp=1789021246000, order_id='order',
                  trade_type='BUY', order_type='LIMIT', price=D('78207.512345678901'),
                  amount=D('.00002824123456789'), leverage=1, trade_fee={},
                  trade_fee_in_quote=D('.001766123456789'), exchange_trade_id='fill', position='NIL')
    values.update(changes)
    return TradeFill(**values)


def recorder(manager):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = MarketsRecorder(manager, [], 'test.yml', 'test', MarketDataCollectionConfigMap())
    loop.close()
    market = MagicMock()
    market.display_name = 'okx'
    market.tracking_states = {}
    return result, market


def event():
    return OrderFilledEvent(timestamp=1789021246, order_id='order', trading_pair='BTC-USDC',
                            trade_type=TradeType.BUY, order_type=OrderType.LIMIT,
                            price=D('78207.512345678901'), amount=D('.00002824123456789'),
                            trade_fee=AddedToCostTradeFee(flat_fees=[TokenAmount('USDC', D('.001766123456789'))]),
                            exchange_trade_id='fill')


def test_recorder_preserves_exact_decimals_after_reopen_and_deduplicates(manager):
    writer, market = recorder(manager)
    fill = event()
    writer._did_fill_order(MarketEvent.OrderFilled.value, market, fill)
    writer._did_fill_order(MarketEvent.OrderFilled.value, market, fill)
    manager.engine.dispose()
    with manager.get_new_session() as session:
        result = session.query(TradeFill).one()
        assert result.amount_decimal == fill.amount
        assert result.price_decimal == fill.price
        assert result.trade_fee_in_quote_decimal == D('.001766123456789')
        assert result.exact_receipt_source == 'order_filled_event_v1'
        assert set(result.receipt_precision.values()) == {'exact_decimal'}
        assert result.amount == D('.000028')
    with sqlite3.connect(manager.db_path) as raw:
        assert raw.execute('select amount, exact_amount from TradeFill').fetchone() == (28, str(fill.amount))
        assert raw.execute('select count(*) from OrderStatus').fetchone()[0] == 1


def test_legacy_receipts_remain_explicitly_precision_limited(manager):
    with manager.get_new_session() as session:
        session.add(trade())
        session.commit()
    with manager.get_new_session() as session:
        result = session.query(TradeFill).one()
        assert result.amount_decimal == D('.000028')
        assert result.receipt_precision == dict.fromkeys(('amount', 'price', 'trade_fee_in_quote'), 'legacy_6dp')
        assert result.exact_receipt_source is None


def test_failed_fee_conversion_remains_unavailable(manager):
    writer, market = recorder(manager)
    fill = event()
    fill.trade_fee.fee_amount_in_token = MagicMock(side_effect=ValueError('missing conversion'))
    writer._did_fill_order(MarketEvent.OrderFilled.value, market, fill)
    with manager.get_new_session() as session:
        result = session.query(TradeFill).one()
        assert result.amount_decimal == fill.amount
        assert result.trade_fee_in_quote_decimal is None
        assert result.receipt_precision['trade_fee_in_quote'] == 'unavailable'


@pytest.mark.parametrize('value', ['NaN', 'Infinity', 'bad'])
def test_invalid_exact_values_never_fall_back_to_legacy(value):
    result = trade()
    result.exact_amount = value
    result.exact_receipt_source = 'order_filled_event_v1'
    with pytest.raises(ValueError):
        _ = result.amount_decimal


def test_existing_database_migrates_without_reinterpreting_legacy_numbers(manager):
    with manager.get_new_session() as session:
        session.add(trade())
        session.commit()
    manager.engine.dispose()
    with sqlite3.connect(manager.db_path) as raw:
        for name in ('exact_amount', 'exact_price', 'exact_trade_fee_in_quote', 'exact_receipt_source'):
            raw.execute(f'ALTER TABLE TradeFill DROP COLUMN {name}')
        raw.execute("UPDATE Metadata SET value='20260327' WHERE key='local_db_version'")
        columns = [column[1] for column in raw.execute('PRAGMA table_info(TradeFill)')]
        values = list(raw.execute('SELECT * FROM TradeFill').fetchone())
        values[columns.index('exchange_trade_id')] = 'wal-fill'
        raw.execute(f"INSERT INTO TradeFill VALUES ({','.join('?' for _ in values)})", values)
    # Keep this idle connection open so committed schema and fill pages remain in WAL.
    reopened = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                    db_path=manager.db_path)
    with reopened.get_new_session() as session:
        assert session.query(TradeFill).count() == 2
        result = session.query(TradeFill).filter_by(exchange_trade_id='wal-fill').one()
        assert result.amount_decimal == D('.000028')
        assert result.exact_amount is None
        assert result.receipt_precision['amount'] == 'legacy_6dp'
        assert reopened.get_local_db_version(session).value == SQLConnectionManager.LOCAL_DB_VERSION_VALUE
    reopened.engine.dispose()
    raw.close()
    from pathlib import Path
    backup = next(Path(manager.db_path).parent.glob('*.backup_*'))
    with sqlite3.connect(backup) as snapshot:
        assert snapshot.execute('SELECT count(*) FROM TradeFill').fetchone()[0] == 2
        assert snapshot.execute("SELECT value FROM Metadata WHERE key='local_db_version'").fetchone()[0] == '20260327'


def test_pandas_and_bounty_read_exact_values():
    result = trade()
    result.exact_amount = '.00002824123456789'
    result.exact_price = '78207.512345678901'
    result.exact_receipt_source = 'order_filled_event_v1'
    result.amount = D('.000028')
    result.price = D('78207.512345')
    assert TradeFill.to_pandas([result]).iloc[0]['Amount'] == D(result.exact_amount)
    assert TradeFill.to_bounty_api_json(result)['quantity'] == format(D(result.exact_amount), 'f')


def test_conflicting_duplicate_preserves_first_receipt_and_reports_error(manager):
    writer, market = recorder(manager)
    fill = event()
    writer._did_fill_order(MarketEvent.OrderFilled.value, market, fill)
    changed = fill._replace(amount=fill.amount + D('.000000001'))
    with pytest.raises(ValueError, match='Conflicting'):
        writer._did_fill_order(MarketEvent.OrderFilled.value, market, changed)
    with manager.get_new_session() as session:
        assert session.query(TradeFill).one().amount_decimal == fill.amount


def test_nonfinite_fee_conversion_is_unavailable_without_losing_fill(manager):
    writer, market = recorder(manager)
    fill = event()
    fill.trade_fee.fee_amount_in_token = MagicMock(return_value=D('NaN'))
    writer._did_fill_order(MarketEvent.OrderFilled.value, market, fill)
    with manager.get_new_session() as session:
        result = session.query(TradeFill).one()
        assert result.amount_decimal == fill.amount
        assert result.trade_fee_in_quote_decimal is None


def test_csv_uses_exact_values_and_retains_precision_metadata(manager, tmp_path, monkeypatch):
    import csv
    import json

    import hummingbot.connector.markets_recorder as recorder_module

    writer, market = recorder(manager)
    fill = event()
    writer._did_fill_order(MarketEvent.OrderFilled.value, market, fill)
    monkeypatch.setattr(recorder_module, 'data_path', lambda: str(tmp_path))
    with manager.get_new_session() as session:
        writer.append_to_csv(session.query(TradeFill).one())
    with (tmp_path / 'trades_test.csv').open() as exported:
        rows = list(csv.DictReader(exported))
    assert D(rows[0]['amount']) == fill.amount
    assert json.loads(rows[0]['receipt_precision'])['amount'] == 'exact_decimal'
    assert rows[0]['exact_receipt_source'] == 'order_filled_event_v1'


def test_wal_reader_keeps_its_snapshot_during_in_place_migration(manager):
    from hummingbot.model.db_migration.migrator import Migrator

    with manager.engine.begin() as connection:
        connection.exec_driver_sql("INSERT INTO Metadata VALUES ('probe', 'before')")
    reader = sqlite3.connect(manager.db_path)
    reader.execute('BEGIN')
    reader.execute('SELECT * FROM Metadata').fetchall()
    with manager.engine.begin() as connection:
        connection.exec_driver_sql("UPDATE Metadata SET value='after' WHERE key='probe'")
    with manager.engine.begin() as connection:
        connection.exec_driver_sql("UPDATE Metadata SET value='20260327' WHERE key='local_db_version'")
    try:
        assert Migrator().migrate_db_to_version(ClientConfigAdapter(ClientConfigMap()), manager, 20260327, 20260910)
        assert reader.execute("SELECT value FROM Metadata WHERE key='probe'").fetchone()[0] == 'before'
    finally:
        reader.close()
    with sqlite3.connect(manager.db_path) as raw:
        assert raw.execute("SELECT value FROM Metadata WHERE key='probe'").fetchone()[0] == 'after'


def test_performance_volumes_use_exact_amount_and_price():
    from hummingbot.client.performance import PerformanceMetrics

    fill = trade()
    fill.exact_amount = str(fill.amount)
    fill.exact_price = str(fill.price)
    fill.exact_receipt_source = 'order_filled_event_v1'
    fill.amount = D('.000028')
    fill.price = D('78207.512345')
    metrics = PerformanceMetrics()
    metrics._preprocess_trades_and_group_by_type([fill])
    assert metrics.b_vol_base == D(fill.exact_amount)
    assert metrics.b_vol_quote == -D(fill.exact_amount) * D(fill.exact_price)


def test_derivative_aggregation_keeps_exact_receipts_immutable():
    from hummingbot.client.performance import PerformanceMetrics

    first = trade(exact_amount='.00002824123456789', exact_price='100.00000001',
                  exact_receipt_source='order_filled_event_v1')
    second = trade(exact_amount='.00001111111111111', exact_price='102.00000003', exchange_trade_id='second',
                   exact_receipt_source='order_filled_event_v1')
    aggregated = PerformanceMetrics.aggregate_orders([first, second])[0]
    assert not isinstance(aggregated, TradeFill)
    assert aggregated.amount == D('.00003935234567900')
    assert aggregated.price == D('101.00000002')
    assert first.amount == D('.00002824123456789')
    assert first.price == D('78207.512345678901')


@pytest.mark.asyncio
async def test_performance_uses_exact_recorded_quote_fee():
    from hummingbot.client.performance import PerformanceMetrics

    fill = trade(exact_trade_fee_in_quote='.001766123456789', exact_receipt_source='order_filled_event_v1')
    metrics = PerformanceMetrics()
    await metrics._calculate_fees('USDC', [fill])
    assert metrics.fee_in_quote == D('.001766123456789')


@pytest.mark.asyncio
async def test_performance_missing_exact_fee_is_unavailable():
    from hummingbot.client.performance import PerformanceMetrics

    fill = trade(trade_fee_in_quote=None, exact_receipt_source='order_filled_event_v1')
    metrics = PerformanceMetrics()
    await metrics._calculate_fees('USDC', [fill])
    assert metrics.fee_in_quote.is_nan()


def test_monitor_formats_unavailable_pnl_truthfully():
    from hummingbot.client.ui.interface_utils import _format_total_pnls_by_quote

    assert _format_total_pnls_by_quote({'USDC': D('NaN')}) == 'UNAVAILABLE USDC'


def test_partial_exact_receipt_never_falls_back_to_legacy_amount():
    fill = trade(exact_price='100', exact_receipt_source='order_filled_event_v1')
    with pytest.raises(ValueError, match='Missing exact'):
        _ = fill.amount_decimal
    assert fill.receipt_precision['amount'] == 'unavailable'


def test_missing_exact_fee_never_falls_back_to_legacy_zero():
    fill = trade(exact_amount='.1', exact_price='100', trade_fee_in_quote=D(0),
                 exact_receipt_source='order_filled_event_v1')
    assert fill.trade_fee_in_quote_decimal is None
    assert fill.receipt_precision['trade_fee_in_quote'] == 'unavailable'


@pytest.mark.parametrize('field,value', [('amount', '0'), ('price', '-1')])
def test_exact_amount_and_price_must_be_positive(field, value):
    fill = trade(exact_amount='.1', exact_price='100', exact_receipt_source='order_filled_event_v1')
    setattr(fill, f'exact_{field}', value)
    with pytest.raises(ValueError):
        getattr(fill, f'{field}_decimal')


@pytest.mark.parametrize('source', [None, '', 'unverified_import_v1'])
@pytest.mark.parametrize('field', ['amount', 'price', 'trade_fee_in_quote'])
def test_exact_accessors_reject_missing_or_unsupported_provenance(source, field):
    fill = trade(exact_amount='.00002824', exact_price='78207.5',
                 exact_trade_fee_in_quote='.001766', exact_receipt_source=source)
    with pytest.raises(ValueError, match='provenance'):
        getattr(fill, f'{field}_decimal')
    assert set(fill.receipt_precision.values()) == {'unavailable'}


def test_precision_annotation_does_not_claim_exact_for_invalid_numeric_receipt():
    fill = trade(exact_amount='NaN', exact_price='78207.5',
                 exact_trade_fee_in_quote='0', exact_receipt_source='order_filled_event_v1')
    assert fill.receipt_precision['amount'] == 'unavailable'
    assert fill.receipt_precision['price'] == 'exact_decimal'
    assert fill.receipt_precision['trade_fee_in_quote'] == 'exact_decimal'


def test_legacy_aggregation_does_not_forge_exact_receipt_provenance():
    from hummingbot.client.performance import PerformanceMetrics

    first = trade(amount=D('.000028'), price=D('100'))
    second = trade(amount=D('.000011'), price=D('102'), exchange_trade_id='second')
    aggregated = PerformanceMetrics.aggregate_orders([first, second])[0]
    assert not isinstance(aggregated, TradeFill)
    assert aggregated.amount == D('.000039')
    assert aggregated.price == D('101')
    assert not first.has_exact_receipt
    assert not second.has_exact_receipt
    assert first.receipt_precision['amount'] == 'legacy_6dp'
