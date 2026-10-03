"""Native recovery must cover physical residue, not only managed positions."""
import copy
import hashlib
import json
import sqlite3
from decimal import Decimal
from test.hummingbot.strategy_v2.executors.test_planned_position_resume import (
    make_executor,
    stopped_fixture,
    write_fleet_owner_snapshot,
)
from test.meridian_v3.test_terminal_dust_snapshot import PAIR, snapshot_case

import pytest
from sqlalchemy import create_engine

from hummingbot.model.executors import Executors
from hummingbot.model.order import Order
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.executors import planned_position_resume as resume


def test_missing_native_fill_history_cannot_certify_fleet_ownership(tmp_path):
    path = write_fleet_owner_snapshot(tmp_path / 'v3.sqlite', 'meridian_v3', active_amount='.2')
    with sqlite3.connect(path) as connection:
        connection.execute('DROP TABLE IF EXISTS TradeFill')
    with pytest.raises(ValueError, match='native.*fill|TradeFill'):
        resume.inspect_fleet_owner_snapshot(path, 'meridian_v3')


def dust_snapshot(path):
    """Persist two independently known residuals through the native SQLite types."""
    _, rows, fills, _ = snapshot_case()
    engine = create_engine(f'sqlite:///{path}')
    Executors.__table__.create(engine)
    Order.__table__.create(engine)
    TradeFill.__table__.create(engine)
    with engine.begin() as connection:
        for row in rows:
            executor = make_executor()
            executor.config.id = row.id
            executor.config.controller_id = row.controller_id
            executor.config.trading_pair = PAIR
            executor.config.amount = row.config.amount
            executor.config.entry_price = row.config.entry_price
            custom = copy.deepcopy(row.custom_info)
            custom['remaining_position_amount_base'] = '0'
            connection.execute(Executors.__table__.insert().values(
                id=row.id, controller_id=row.controller_id, type='position_executor',
                timestamp=1, close_timestamp=1000, close_type=row.close_type.value,
                status=4, is_active=False, is_trading=False, net_pnl_pct=0,
                net_pnl_quote=0, cum_fees_quote=0, filled_amount_quote=51,
                config=executor.config.model_dump(mode='json'), custom_info=custom))
        for fill in fills:
            connection.execute(Order.__table__.insert().values(
                id=fill.order_id, config_file_path='meridian_v3.yml', strategy='v2_with_controllers',
                market='okx', symbol=PAIR, base_asset='BTC', quote_asset='USDC',
                creation_timestamp=1, order_type='LIMIT', amount=fill.amount_decimal,
                price=fill.price_decimal, leverage=1, last_status='BuyOrderCompleted'
                if fill.trade_type == 'BUY' else 'SellOrderCompleted', last_update_timestamp=1000,
                exchange_order_id=f'exchange-{fill.order_id}', position='NIL'))
            values = {column.key: getattr(fill, column.key) for column in TradeFill.__table__.columns}
            values['config_file_path'] = 'meridian_v3.yml'
            connection.execute(TradeFill.__table__.insert().values(**values))
    engine.dispose()
    with sqlite3.connect(path) as connection:
        connection.execute('CREATE TABLE Metadata (key TEXT PRIMARY KEY, value TEXT)')
        connection.execute('CREATE TABLE PositionHoldLedger (id TEXT PRIMARY KEY, controller_id TEXT, '
                           'executor_id TEXT, connector_name TEXT, trading_pair TEXT, order_side TEXT, '
                           'client_order_id TEXT, order_payload TEXT)')
        connection.execute('CREATE TABLE Position (id TEXT PRIMARY KEY, connector_name TEXT, '
                           'trading_pair TEXT, amount BIGINT)')
    return path


def inspect(path, lots=None):
    return resume.inspect_fleet_owner_snapshot(path, 'meridian_v3',
                                               quantity_increments={PAIR: '0.00000001'} if lots is None else lots)


def test_completed_dust_above_one_lot_is_owned_and_read_only(tmp_path):
    path = dust_snapshot(tmp_path / 'native.sqlite')
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    first = inspect(path)
    assert first['owned_residual_by_asset'] == {'BTC': '0.000000011454'}
    assert inspect(path) == first
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize('lots', [{}, {PAIR: '0.0000001'}, {PAIR: 'NaN'}, {PAIR: '0'}, []])
def test_positive_dust_needs_independent_bound_native_lot(tmp_path, lots):
    path = dust_snapshot(tmp_path / 'native.sqlite')
    with pytest.raises(ValueError):
        inspect(path, lots)


@pytest.mark.parametrize('change', ['erased_dust', 'deleted_executor', 'deleted_fill', 'legacy_fill',
                                    'foreign_fill', 'negative_dust', 'active_dust', 'unknown_close',
                                    'orphan_order', 'foreign_order', 'forged_venue_id', 'fee_drift',
                                    'unexplained_sublot', 'scaled_conflict'])
def test_incomplete_or_conflicting_native_history_cannot_certify_ownership(tmp_path, change):
    path = dust_snapshot(tmp_path / 'native.sqlite')
    with sqlite3.connect(path) as connection:
        row = connection.execute('SELECT custom_info FROM Executors WHERE id=?', ('terminal-0',)).fetchone()
        custom = json.loads(row[0])
        if change == 'erased_dust':
            custom['execution_receipt']['residual_dust_base'] = '0'
        elif change == 'negative_dust':
            custom['execution_receipt']['residual_dust_base'] = '-0.1'
        elif change == 'deleted_executor':
            connection.execute('DELETE FROM Executors WHERE id=?', ('terminal-0',))
        elif change == 'deleted_fill':
            connection.execute('DELETE FROM TradeFill WHERE order_id=?', ('exit-0',))
        elif change == 'legacy_fill':
            connection.execute('UPDATE TradeFill SET exact_amount=NULL, exact_price=NULL, '
                               'exact_trade_fee_in_quote=NULL, exact_receipt_source=NULL')
        elif change == 'foreign_fill':
            connection.execute('UPDATE TradeFill SET config_file_path=? WHERE order_id=?', ('foreign', 'entry-0'))
        elif change == 'active_dust':
            connection.execute('UPDATE Executors SET status=2,is_active=1 WHERE id=?', ('terminal-0',))
        elif change == 'unknown_close':
            connection.execute('UPDATE Executors SET close_type=NULL WHERE id=?', ('terminal-0',))
        elif change == 'orphan_order':
            connection.execute('DELETE FROM "Order" WHERE id=?', ('entry-0',))
        elif change == 'foreign_order':
            connection.execute('UPDATE "Order" SET symbol=? WHERE id=?', ('ETH-USDC', 'entry-0'))
        elif change == 'forged_venue_id':
            snapshot = custom['execution_order_snapshots'][0]
            snapshot['exchange_order_id'] = 'foreign'
            for fill in snapshot['order_fills'].values():
                fill['exchange_order_id'] = 'foreign'
        elif change == 'fee_drift':
            connection.execute('UPDATE TradeFill SET trade_fee=? WHERE order_id=?',
                               (json.dumps({'percent': '0', 'flat_fees': [{'token': 'BTC', 'amount': '.000000001'}]}),
                                'entry-0'))
        elif change == 'unexplained_sublot':
            # Each dust receipt remains plausible in isolation; only full conservation catches lost ownership.
            custom['execution_receipt']['residual_dust_base'] = '0'
            custom['execution_receipt']['remaining_owned_base'] = '0'
            custom['execution_order_snapshots'] = []
        else:
            connection.execute('UPDATE TradeFill SET amount=6000 WHERE order_id=?', ('entry-0',))
        connection.execute('UPDATE Executors SET custom_info=? WHERE id=?', (json.dumps(custom), 'terminal-0'))
    with pytest.raises(ValueError):
        inspect(path)


@pytest.mark.parametrize('change', ['conflict', 'orphan'])
def test_native_exact_companion_conflict_or_orphan_is_rejected(tmp_path, change):
    path = dust_snapshot(tmp_path / 'native.sqlite')
    with sqlite3.connect(path) as connection:
        connection.execute('CREATE TABLE TradeFillExact (market TEXT, order_id TEXT, exchange_trade_id TEXT, '
                           'amount TEXT, price TEXT, fee_in_quote TEXT, '
                           'PRIMARY KEY(market, order_id, exchange_trade_id))')
        connection.execute('INSERT INTO TradeFillExact VALUES (?, ?, ?, ?, ?, ?)',
                           ('okx', 'entry-0' if change == 'conflict' else 'orphan', 'buy-0', '.0007', '85000', '0'))
    with pytest.raises(ValueError, match='conflict|no TradeFill parent'):
        inspect(path)


def test_exact_companion_without_additive_fill_columns_preserves_its_exact_values(tmp_path):
    from hummingbot.strategy_v2.executors.recovery_inventory import native_inventory_proof

    path = write_fleet_owner_snapshot(tmp_path / 'native.sqlite', 'rsi_modular_v2')
    with sqlite3.connect(path) as connection:
        connection.execute('UPDATE TradeFill SET exact_amount=NULL, exact_price=NULL, '
                           'exact_trade_fee_in_quote=NULL, exact_receipt_source=NULL')
    with resume.connect(path) as connection:
        _, totals, fills = native_inventory_proof(connection, 'rsi_modular_v2.yml', {})
    assert totals == {'BNB': Decimal('.249')}
    assert fills[0].amount_decimal == Decimal('.25')
    assert fills[0].has_exact_receipt


def _legacy_held_fill_snapshot(path, *, remove_exact=True, exact_amount='.25', exact_price='100'):
    """Turn the native fixture into the old TradeFill schema plus exact held-order event."""
    with sqlite3.connect(path) as connection:
        for column, kind in (('position_side', 'TEXT'), ('timestamp', 'BIGINT'),
                             ('exchange_order_id', 'TEXT'), ('price', 'BIGINT'), ('amount', 'BIGINT'),
                             ('executed_amount_base', 'BIGINT'), ('executed_amount_quote', 'BIGINT'),
                             ('cumulative_fee_paid_quote', 'BIGINT')):
            connection.execute(f'ALTER TABLE PositionHoldLedger ADD COLUMN {column} {kind}')
        fill = connection.execute(
            'SELECT market, order_id, exchange_trade_id, symbol, timestamp, trade_fee, amount, price '
            'FROM TradeFill').fetchone()
        order = connection.execute(
            'SELECT exchange_order_id FROM "Order" WHERE id=?', (fill[1],)).fetchone()
        ledger = connection.execute(
            'SELECT order_payload FROM PositionHoldLedger WHERE client_order_id=?', (fill[1],)).fetchone()
        payload = json.loads(ledger[0])
        exact = payload['order_fills']['v2-trade']
        amount, price = Decimal(exact_amount), Decimal(exact_price)
        quote_amount = amount * price
        fee = exact['fee']
        quote_fee = sum((Decimal(item['amount']) * (price if item['token'] == 'BNB' else 1)
                         for item in fee['flat_fees']), Decimal(0))
        exact.update({
            'trade_id': fill[2], 'client_order_id': fill[1], 'exchange_order_id': order[0],
            'trading_pair': fill[3], 'fill_timestamp': 10,
            'fill_base_amount': str(amount), 'fill_price': str(price),
            'fill_quote_amount': str(quote_amount),
        })
        payload.update({
            'client_order_id': fill[1], 'exchange_order_id': order[0],
            'trading_pair': fill[3], 'amount': '.3', 'price': '100', 'trade_type': 'BUY',
            'creation_timestamp': 9000, 'last_update_timestamp': 11000,
            'executed_amount_base': str(amount), 'executed_amount_quote': str(quote_amount),
            'cumulative_fee_paid_quote': str(quote_fee),
        })
        connection.execute('UPDATE PositionHoldLedger SET order_payload=? WHERE client_order_id=?',
                           (json.dumps(payload, sort_keys=True), fill[1]))
        connection.execute('UPDATE PositionHoldLedger SET exchange_order_id=?,executed_amount_base=?, '
                           'executed_amount_quote=?,cumulative_fee_paid_quote=?,amount=?,price=?,timestamp=? '
                           'WHERE client_order_id=?',
                           (order[0], int(amount * 1_000_000), int(quote_amount * 1_000_000),
                            int(quote_fee * 1_000_000), 300000, 100000000, 10000, fill[1]))
        connection.execute('UPDATE "Order" SET amount=?,price=?,creation_timestamp=9000,last_update_timestamp=11000 '
                           'WHERE id=?', (300000, 100000000, fill[1]))
        connection.execute('UPDATE TradeFill SET timestamp=10000 WHERE order_id=?', (fill[1],))
        if remove_exact:
            connection.execute('UPDATE TradeFill SET exact_amount=NULL, exact_price=NULL, '
                               'exact_trade_fee_in_quote=NULL, exact_receipt_source=NULL WHERE order_id=?',
                               (fill[1],))
            connection.execute('DELETE FROM TradeFillExact WHERE market=? AND order_id=? AND exchange_trade_id=?',
                               (fill[0], fill[1], fill[2]))
    return path


def test_legacy_tradefill_uses_complete_exact_native_held_order_receipt_without_writing(tmp_path):
    from hummingbot.strategy_v2.executors.recovery_inventory import native_inventory_proof

    path = _legacy_held_fill_snapshot(write_fleet_owner_snapshot(tmp_path / 'v2.sqlite', 'rsi_modular_v2'))
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    connection = sqlite3.connect(f'file:{path}?mode=ro&immutable=1', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        dust, totals, fills = native_inventory_proof(connection, 'rsi_modular_v2.yml', {})
    finally:
        connection.close()
    assert dust == {}
    assert totals == {'BNB': Decimal('.249')}
    assert len(fills) == 1
    assert fills[0].amount_decimal == Decimal('.25')
    assert fills[0].price_decimal == Decimal('100')
    assert fills[0].native_receipt_source == 'position_hold_order_snapshot_v1'
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
    assert ExecutorOrchestrator._trade_fill_order_record(fills[0])['precision'] == 'position_hold_order_snapshot_v1'
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_legacy_held_order_snapshot_preserves_sub_six_decimal_fill_precision(tmp_path):
    from hummingbot.strategy_v2.executors.recovery_inventory import native_inventory_proof

    path = _legacy_held_fill_snapshot(write_fleet_owner_snapshot(tmp_path / 'v2.sqlite', 'rsi_modular_v2'),
                                      exact_amount='.25000041', exact_price='100.00000039')
    connection = sqlite3.connect(f'file:{path}?mode=ro&immutable=1', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        _, totals, fills = native_inventory_proof(connection, 'rsi_modular_v2.yml', {})
    finally:
        connection.close()
    assert totals == {'BNB': Decimal('.24900041')}
    assert fills[0].amount_decimal == Decimal('.25000041')
    assert fills[0].price_decimal == Decimal('100.00000039')


def test_exact_event_receipt_rejects_conflicting_complete_held_snapshot(tmp_path):
    from hummingbot.strategy_v2.executors.recovery_inventory import native_inventory_proof

    path = _legacy_held_fill_snapshot(write_fleet_owner_snapshot(tmp_path / 'v2.sqlite', 'rsi_modular_v2'),
                                      remove_exact=False)
    with sqlite3.connect(path) as connection:
        payload = json.loads(connection.execute('SELECT order_payload FROM PositionHoldLedger').fetchone()[0])
        payload['order_fills']['v2-trade']['fill_base_amount'] = '.251'
        payload['order_fills']['v2-trade']['fill_quote_amount'] = '25.1'
        connection.execute('UPDATE PositionHoldLedger SET order_payload=?', (json.dumps(payload),))
    connection = sqlite3.connect(f'file:{path}?mode=ro&immutable=1', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        with pytest.raises(ValueError, match='conflicts|differs'):
            native_inventory_proof(connection, 'rsi_modular_v2.yml', {})
    finally:
        connection.close()


@pytest.mark.parametrize('mutation', [
    'missing_trade', 'wrong_pair', 'wrong_exchange_order', 'wrong_fee', 'wrong_quote',
    'wrong_timestamp', 'duplicate_trade', 'legacy_projection_conflict', 'wrong_order_aggregate',
    'wrong_ledger_fee_projection', 'third_fee_currency', 'percent_fee', 'wrong_side',
])
def test_legacy_held_order_fallback_rejects_incomplete_or_conflicting_provenance(tmp_path, mutation):
    from hummingbot.strategy_v2.executors.recovery_inventory import native_inventory_proof

    path = _legacy_held_fill_snapshot(write_fleet_owner_snapshot(tmp_path / 'v2.sqlite', 'rsi_modular_v2'))
    with sqlite3.connect(path) as connection:
        row = connection.execute('SELECT order_payload FROM PositionHoldLedger').fetchone()
        payload = json.loads(row[0])
        fill = payload['order_fills']['v2-trade']
        if mutation == 'missing_trade':
            payload['order_fills'] = {}
        elif mutation == 'wrong_pair':
            fill['trading_pair'] = 'BTC-USDC'
        elif mutation == 'wrong_exchange_order':
            fill['exchange_order_id'] = 'other-order'
        elif mutation == 'wrong_fee':
            fill['fee']['flat_fees'][0]['amount'] = '.002'
        elif mutation == 'wrong_quote':
            fill['fill_quote_amount'] = '24.99'
        elif mutation == 'wrong_timestamp':
            fill['fill_timestamp'] = 999
        elif mutation == 'duplicate_trade':
            payload['order_fills']['duplicate'] = dict(fill)
        elif mutation == 'legacy_projection_conflict':
            connection.execute('UPDATE TradeFill SET amount=249000 WHERE order_id=?',
                               (payload['client_order_id'],))
        elif mutation == 'wrong_order_aggregate':
            payload['executed_amount_quote'] = '26'
        elif mutation == 'wrong_ledger_fee_projection':
            connection.execute('UPDATE PositionHoldLedger SET cumulative_fee_paid_quote=99999')
        elif mutation == 'third_fee_currency':
            fill['fee']['flat_fees'][0]['token'] = 'ETH'
        elif mutation == 'percent_fee':
            fill['fee']['percent'] = '.001'
        elif mutation == 'wrong_side':
            payload['trade_type'] = 'SELL'
        connection.execute('UPDATE PositionHoldLedger SET order_payload=?', (json.dumps(payload),))
    connection = sqlite3.connect(f'file:{path}?mode=ro&immutable=1', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        with pytest.raises(ValueError):
            native_inventory_proof(connection, 'rsi_modular_v2.yml', {})
    finally:
        connection.close()


def test_same_asset_totals_do_not_allow_a_forged_held_fill_identity(tmp_path):
    path = write_fleet_owner_snapshot(tmp_path / 'v2.sqlite', 'rsi_modular_v2')
    with sqlite3.connect(path) as connection:
        payload = json.loads(connection.execute('SELECT order_payload FROM PositionHoldLedger').fetchone()[0])
        payload['order_fills']['forged'] = payload['order_fills'].pop('v2-trade')
        connection.execute('UPDATE PositionHoldLedger SET order_payload=?', (json.dumps(payload),))
    with pytest.raises(ValueError, match='native history'):
        resume.inspect_fleet_owner_snapshot(path, 'rsi_modular_v2')


@pytest.mark.parametrize('field', ['trade_id', 'client_order_id', 'exchange_order_id', 'trading_pair'])
def test_held_fill_explicit_identity_cannot_disagree_with_native_key(tmp_path, field):
    path = write_fleet_owner_snapshot(tmp_path / 'v2.sqlite', 'rsi_modular_v2')
    with sqlite3.connect(path) as connection:
        payload = json.loads(connection.execute('SELECT order_payload FROM PositionHoldLedger').fetchone()[0])
        payload['order_fills']['v2-trade'][field] = 'foreign'
        connection.execute('UPDATE PositionHoldLedger SET order_payload=?', (json.dumps(payload),))
    with pytest.raises(ValueError, match='identity conflicts|native fill pair is inconsistent'):
        resume.inspect_fleet_owner_snapshot(path, 'rsi_modular_v2')


def test_native_base_rebate_in_held_inventory_is_not_rejected_as_a_negative_fee(tmp_path):
    from hummingbot.model.exact_order_values import TradeFillExact
    from hummingbot.model.position import Position
    path = write_fleet_owner_snapshot(tmp_path / 'v2.sqlite', 'rsi_modular_v2')
    fee = {'percent': '0', 'flat_fees': [{'token': 'BNB', 'amount': '-.001'}]}
    with sqlite3.connect(path) as connection:
        payload = json.loads(connection.execute('SELECT order_payload FROM PositionHoldLedger').fetchone()[0])
        payload['order_fills']['v2-trade']['fee'] = fee
        connection.execute('UPDATE PositionHoldLedger SET order_payload=?', (json.dumps(payload),))
    engine = create_engine(f'sqlite:///{path}')
    with engine.begin() as connection:
        connection.execute(TradeFill.__table__.update().values(
            trade_fee=fee, trade_fee_in_quote=Decimal('-.1'), exact_trade_fee_in_quote='-.1'))
        connection.execute(TradeFillExact.__table__.update().values(fee_in_quote='-.1'))
        connection.execute(Position.__table__.update().values(amount=Decimal('.251')))
    engine.dispose()
    assert resume.inspect_fleet_owner_snapshot(path, 'rsi_modular_v2')['owned_residual_by_asset'] == {'BNB': '0.251'}


def test_conservation_is_per_asset_and_has_no_sublot_tolerance():
    from hummingbot.strategy_v2.executors.recovery_inventory import require_inventory_conservation
    with pytest.raises(ValueError):
        require_inventory_conservation({'BTC': Decimal('1')}, {'ETH': Decimal('1')})
    with pytest.raises(ValueError):
        require_inventory_conservation({'BTC': Decimal('1')}, {'BTC': Decimal('1.000000001')})


@pytest.mark.asyncio
@pytest.mark.parametrize('source', [
    'hummingbot/model/executors.py', 'hummingbot/model/decimal_type_decorator.py',
    'hummingbot/strategy_v2/executors/recovery_inventory.py',
    'controllers/generic/meridian_v3_unified_core/terminal_dust.py',
])
async def test_prepared_claim_is_invalid_after_native_inventory_parser_drift(tmp_path, source):
    db, preflight, root, _, _, evidence = await stopped_fixture(tmp_path)
    resume.prepare(db, preflight, evidence, now=1000, root=root)
    path = root / source
    path.write_text(path.read_text() + '\n# changed parser identity\n')
    with pytest.raises(ValueError, match='source/config binding changed'):
        resume.pending_manifest(db, now=1001, root=root)


def test_signed_base_rebate_stays_owned_and_quote_fee_does_not_remove_base():
    from test.meridian_v3.test_terminal_dust_snapshot import native_fill

    from controllers.generic.meridian_v3_unified_core.terminal_dust import filled_base
    buy = native_fill('entry', 'buy', 'BUY', '1', '100',
                      {'percent': '0', 'flat_fees': [{'token': 'BTC', 'amount': '-.001'}]})
    sell = native_fill('exit', 'sell', 'SELL', '1', '100',
                       {'percent': '0', 'flat_fees': [{'token': 'USDC', 'amount': '.1'}]})
    assert filled_base([buy, sell], PAIR) == Decimal('.001')


def test_unselected_v1_zero_fill_terminal_still_owns_an_unresolved_order(tmp_path):
    path = dust_snapshot(tmp_path / 'native.sqlite')
    with sqlite3.connect(path) as connection:
        custom = {'remaining_position_amount_base': '0', 'order_ids': ['unknown-outcome-order']}
        connection.execute('UPDATE Executors SET close_type=NULL,custom_info=? WHERE id=?',
                           (json.dumps(custom), 'terminal-0'))
        connection.execute('DELETE FROM TradeFill WHERE order_id IN (?, ?)', ('entry-0', 'exit-0'))
    with resume.connect(path) as connection, pytest.raises(ValueError, match='unresolved lifecycle ownership'):
        resume.validate_v1_terminal_inventory_population(connection, set())
