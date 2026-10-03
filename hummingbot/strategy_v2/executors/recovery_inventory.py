"""Read-only physical inventory proof for sealed stopped-owner recovery.

Native model result processors retain SQLite scaling and exact receipt provenance.
No recorder is initialized and no second connection, schema write or network call
is made. All reads use the caller's existing snapshot transaction.
"""
import json
from collections import defaultdict
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy.dialects.sqlite import dialect

from hummingbot.core.data_type.common import TradeType
from hummingbot.model.exact_order_values import TradeFillExact, recorded_values
from hummingbot.model.executors import Executors
from hummingbot.model.trade_fill import TradeFill

ZERO = Decimal('0')


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _model_row(model, row):
    """Use the same declared SQLite conversion as the native ORM, without a writer/session."""
    native = dict(row)
    sqlite = dialect()
    values = {}
    for column in model.__table__.columns:
        if column.key not in native:
            _require(column.nullable, f'native {model.__tablename__} schema lacks {column.key}')
            continue
        processor = column.type.dialect_impl(sqlite).result_processor(sqlite, None)
        value = native[column.key]
        values[column.key] = processor(value) if processor else value
    return model(**values)


class _VerifiedHeldFill:
    """In-memory fill view backed by one exact native held-order snapshot."""

    def __init__(self, native, *, amount, price, fee_in_quote, exchange_order_id):
        self._native = native
        self.amount_decimal = amount
        self.price_decimal = price
        self.trade_fee_in_quote_decimal = fee_in_quote
        self.native_order_exchange_id = exchange_order_id
        self.native_receipt_source = 'position_hold_order_snapshot_v1'
        self.has_exact_receipt = True

    def __getattr__(self, name):
        return getattr(self._native, name)


def _quote_fee_from_snapshot(fee, base_asset, quote_asset, price, validate_fee):
    """Value exact base/quote flat fees at this fill, rejecting unvalued fee currencies."""
    validate_fee(fee)
    _require(Decimal(str(fee['percent'])) == 0, 'native fallback percent fee is unsupported')
    quote_fee = Decimal(0)
    for item in fee['flat_fees']:
        amount = Decimal(str(item['amount']))
        if not amount.is_finite():
            raise ValueError('native fallback fee is non-finite')
        if item['token'] == base_asset:
            quote_fee += amount * price
        elif item['token'] == quote_asset:
            quote_fee += amount
        else:
            raise ValueError('native fallback fee currency cannot be valued exactly')
    if not quote_fee.is_finite():
        raise ValueError('native fallback quote fee is non-finite')
    return quote_fee


def _order_snapshot_receipts(connection, script_config, fills_by_order, validate_fee):
    """Validate complete held order snapshots for orders containing legacy fills.

    InFlightOrder timestamps are seconds; recorder Order/TradeFill timestamps are
    milliseconds. Each event time must lie within the recorded native order
    lifecycle. Event receipt time and exchange fill time need not be identical.
    """
    from hummingbot.model.position_hold_ledger import PositionHoldLedger

    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'PositionHoldLedger' not in tables:
        return {}
    result = {}
    for order_id, native_fills in fills_by_order.items():
        needs_fallback = any(values.precision != 'event_exact' for _, _, values in native_fills)
        ledger_rows = connection.execute(
            'SELECT * FROM PositionHoldLedger WHERE client_order_id=?', (order_id,)).fetchall()
        if not ledger_rows:
            _require(not needs_fallback, 'legacy native fill lacks one unique held order snapshot')
            continue
        if len(ledger_rows) != 1:
            _require(not needs_fallback, 'legacy native fill lacks one unique held order snapshot')
            continue
        ledger = ledger_rows[0]
        order = connection.execute('SELECT * FROM "Order" WHERE id=?', (order_id,)).fetchone()
        if order is None:
            _require(not needs_fallback, 'legacy held snapshot lacks its native Order')
            continue
        try:
            payload = json.loads(ledger['order_payload']) if isinstance(ledger['order_payload'], str) else ledger['order_payload']
        except (TypeError, ValueError):
            _require(not needs_fallback, 'legacy held order snapshot is malformed')
            continue
        required_top = {'client_order_id', 'exchange_order_id', 'trading_pair', 'trade_type', 'order_fills',
                        'executed_amount_base', 'executed_amount_quote'}
        required_fill = {'trade_id', 'client_order_id', 'exchange_order_id', 'trading_pair',
                         'fill_timestamp', 'fill_base_amount', 'fill_price', 'fill_quote_amount', 'fee'}
        complete_snapshot = (isinstance(payload, dict) and required_top <= payload.keys()
                             and isinstance(payload.get('order_fills'), dict)
                             and all(isinstance(receipt, dict) and required_fill <= receipt.keys()
                                     for receipt in payload['order_fills'].values()))
        if not needs_fallback and not complete_snapshot:
            continue
        _require(complete_snapshot, 'legacy held order snapshot is incomplete')
        _require(order is not None and order['config_file_path'] == script_config
                 and order['market'] == 'okx' and order['symbol'] == native_fills[0][0].symbol
                 and order['base_asset'] == native_fills[0][0].base_asset
                 and order['quote_asset'] == native_fills[0][0].quote_asset
                 and order['leverage'] == 1 and order['position'] in (None, '', 'NIL')
                 and isinstance(order['exchange_order_id'], str) and bool(order['exchange_order_id']),
                 'legacy native fill order owner or exchange identity differs')
        pair = order['symbol']
        base, quote = order['base_asset'], order['quote_asset']
        _require(ledger['connector_name'] == 'okx' and ledger['trading_pair'] == pair
                 and ledger['order_side'] in ('BUY', 'SELL')
                 and ledger['client_order_id'] == order_id
                 and ledger['exchange_order_id'] == order['exchange_order_id'],
                 'legacy held order ledger identity differs')
        _require(payload.get('client_order_id') == order_id
                 and payload.get('exchange_order_id') == order['exchange_order_id']
                 and payload.get('trading_pair') == pair
                 and payload.get('trade_type') == ledger['order_side']
                 and isinstance(payload.get('order_fills'), dict),
                 'legacy held order snapshot owner is incomplete')
        by_trade_id = {str(fill.exchange_trade_id): (fill, companion, values)
                       for fill, companion, values in native_fills}
        snapshots = payload['order_fills']
        _require(set(snapshots) == set(by_trade_id) and len(snapshots) == len(by_trade_id),
                 'legacy held order fill set differs from native TradeFill history')
        _require(order['creation_timestamp'] > 0
                 and order['last_update_timestamp'] >= order['creation_timestamp'],
                 'legacy native order lifecycle timestamp is invalid')
        aggregate_base = Decimal(0)
        aggregate_quote = Decimal(0)
        aggregate_fee_quote = Decimal(0)
        for trade_id, (fill, companion, values) in by_trade_id.items():
            receipt = snapshots[trade_id]
            required = {'trade_id', 'client_order_id', 'exchange_order_id', 'trading_pair',
                        'fill_timestamp', 'fill_base_amount', 'fill_price', 'fill_quote_amount', 'fee'}
            _require(isinstance(receipt, dict) and required <= receipt.keys()
                     and str(receipt['trade_id']) == trade_id
                     and receipt['client_order_id'] == order_id
                     and receipt['exchange_order_id'] == order['exchange_order_id']
                     and receipt['trading_pair'] == pair,
                     'legacy held fill identity is incomplete or conflicting')
            amount = Decimal(str(receipt['fill_base_amount']))
            price = Decimal(str(receipt['fill_price']))
            quote_amount = Decimal(str(receipt['fill_quote_amount']))
            fill_timestamp = Decimal(str(receipt['fill_timestamp']))
            _require(all(value.is_finite() and value > 0
                         for value in (amount, price, quote_amount, fill_timestamp))
                     and amount * price == quote_amount,
                     'legacy held fill amount, price, quote, or timestamp is invalid')
            _require(order['creation_timestamp'] <= fill_timestamp * 1000 <= order['last_update_timestamp']
                     and order['creation_timestamp'] <= fill.timestamp <= order['last_update_timestamp'],
                     'legacy held fill timestamp is outside its native order lifecycle')
            _require(fill.trade_type == ledger['order_side'] and fill.trade_fee == receipt['fee']
                     and Decimal(fill.amount) == Decimal(int(amount * 1_000_000)) / 1_000_000
                     and Decimal(fill.price) == Decimal(int(price * 1_000_000)) / 1_000_000,
                     'legacy held fill conflicts with its six-decimal native TradeFill projection')
            fee_quote = _quote_fee_from_snapshot(receipt['fee'], base, quote, price, validate_fee)
            _require(fill.trade_fee_in_quote is not None
                     and Decimal(fill.trade_fee_in_quote) == Decimal(int(fee_quote * 1_000_000)) / 1_000_000,
                     'legacy held exact quote fee conflicts with its native six-decimal projection')
            if values.precision == 'event_exact':
                _require(values.amount == amount and values.price == price
                         and (values.fee_in_quote is None or values.fee_in_quote == fee_quote),
                         'native exact fill companion conflicts with held order snapshot')
            aggregate_base += amount
            aggregate_quote += quote_amount
            aggregate_fee_quote += fee_quote
            if values.precision != 'event_exact':
                result[(fill.market, pair, order_id, trade_id)] = _VerifiedHeldFill(
                    fill, amount=amount, price=price, fee_in_quote=fee_quote,
                    exchange_order_id=order['exchange_order_id'])
        for field, expected in (('executed_amount_base', aggregate_base),
                                ('executed_amount_quote', aggregate_quote)):
            try:
                actual = Decimal(str(payload[field]))
            except (KeyError, ArithmeticError, TypeError, ValueError):
                raise ValueError('legacy held order aggregate is incomplete') from None
            _require(actual.is_finite() and actual == expected,
                     'legacy held order aggregate conflicts with exact fills')
        native_ledger = _model_row(PositionHoldLedger, ledger)
        _require(Decimal(native_ledger.executed_amount_base)
                 == Decimal(int(aggregate_base * 1_000_000)) / 1_000_000
                 and Decimal(native_ledger.executed_amount_quote)
                 == Decimal(int(aggregate_quote * 1_000_000)) / 1_000_000
                 and Decimal(native_ledger.cumulative_fee_paid_quote)
                 == Decimal(int(aggregate_fee_quote * 1_000_000)) / 1_000_000,
                 'legacy held ledger aggregate conflicts with exact fills')
    return result


def native_inventory_proof(connection, script_config, quantity_increments=None):
    """Return proven terminal dust and all fee-net fills, each grouped by base asset.

    The lot map is an explicit input sealed with recovery evidence; it is never
    inferred from a dust receipt. Missing lots hold any positive dust. Callers
    must compare total held/active/selected/dust ownership with the fill totals.
    """
    # Local imports avoid the native funding/orchestrator startup import cycle.
    from controllers.generic.meridian_v3_unified_core.terminal_dust import terminal_dust
    from controllers.generic.modular_spot_core.domain import decimal
    from controllers.generic.modular_spot_core.inventory import InventoryView
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator

    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    _require('TradeFill' in tables, 'complete native TradeFill history is required')
    if 'TradeFillExact' in tables:
        orphan = connection.execute(
            'SELECT 1 FROM TradeFillExact e LEFT JOIN TradeFill f '
            'ON f.market=e.market AND f.order_id=e.order_id AND f.exchange_trade_id=e.exchange_trade_id '
            'WHERE f.order_id IS NULL LIMIT 1').fetchone()
        _require(orphan is None, 'native exact fill companion has no TradeFill parent')
    lots = {} if quantity_increments is None else quantity_increments
    _require(isinstance(lots, dict), 'native lot evidence must be a pair map')
    lots = {pair: decimal(value, 'native lot evidence', positive=True) for pair, value in lots.items()}
    fills, fill_totals, seen = [], defaultdict(Decimal), set()
    exact_fill_groups = defaultdict(list)
    native_rows = [_model_row(TradeFill, raw) for raw in connection.execute('SELECT * FROM TradeFill')]
    for fill in native_rows:
        companion = None
        if 'TradeFillExact' in tables:
            exact = connection.execute('SELECT * FROM TradeFillExact WHERE market=? AND order_id=? '
                                       'AND exchange_trade_id=?',
                                       (fill.market, fill.order_id, fill.exchange_trade_id)).fetchone()
            if exact is not None:
                companion = _model_row(TradeFillExact, exact)
        values = recorded_values(fill, companion)
        exact_fill_groups[fill.order_id].append((fill, companion, values))
    held_snapshot_fills = _order_snapshot_receipts(
        connection, script_config, exact_fill_groups, ExecutorOrchestrator._validate_reconciliation_fee)

    for fill in native_rows:
        _require(fill.config_file_path == script_config and fill.market == 'okx'
                 and fill.symbol == f'{fill.base_asset}-{fill.quote_asset}'
                 and fill.leverage == 1 and fill.position in (None, '', 'NIL')
                 and fill.trade_type in ('BUY', 'SELL'), 'native fill owner or spot scope differs')
        order = connection.execute('SELECT config_file_path, market, symbol, base_asset, quote_asset, leverage, position, '
                                   'exchange_order_id '
                                   'FROM "Order" WHERE id=?', (fill.order_id,)).fetchone()
        _require(order is not None and order['config_file_path'] == script_config
                 and order['market'] == fill.market and order['symbol'] == fill.symbol
                 and order['base_asset'] == fill.base_asset and order['quote_asset'] == fill.quote_asset
                 and order['leverage'] == 1 and order['position'] in (None, '', 'NIL'),
                 'native fill lacks its owner Order')
        _require(isinstance(order['exchange_order_id'], str) and bool(order['exchange_order_id']),
                 'native fill Order exchange identity is unavailable')
        fill.native_order_exchange_id = order['exchange_order_id']
        key = (fill.market, fill.symbol, str(fill.exchange_trade_id))
        _require(fill.exchange_trade_id not in (None, '') and key not in seen,
                 'native exchange fill identity is missing or duplicated')
        seen.add(key)
        companion = next(companion for grouped_fill, companion, _ in exact_fill_groups[fill.order_id]
                         if grouped_fill.exchange_trade_id == fill.exchange_trade_id)
        values = recorded_values(fill, companion)
        if values.precision == 'event_exact':
            receipt_source = 'order_filled_event_v1'
            # Companions may be the only exact representation in older schemas.
            # These are detached models: retain proven event values without
            # writing or rounding the historical recorder columns.
            fill.exact_amount = str(values.amount)
            fill.exact_price = str(values.price)
            fill.exact_trade_fee_in_quote = None if values.fee_in_quote is None else str(values.fee_in_quote)
            fill.exact_receipt_source = receipt_source
            exact_fill = fill
            amount = values.amount
        else:
            key = (fill.market, fill.symbol, fill.order_id, str(fill.exchange_trade_id))
            exact_fill = held_snapshot_fills.get(key)
            _require(exact_fill is not None,
                     'native fill lacks exact event or complete held-order snapshot provenance')
            receipt_source = exact_fill.native_receipt_source
            amount = exact_fill.amount_decimal
        if exact_fill is not fill:
            exact_fill.native_order_exchange_id = order['exchange_order_id']
            # Keep the event source distinct; this record is only a scalar/fee
            # contract check and is never persisted or advertised as event_exact.
            ExecutorOrchestrator._trade_fill_order_record(exact_fill)
        else:
            fill.native_receipt_source = receipt_source
            ExecutorOrchestrator._trade_fill_order_record(fill)
        base_fee = sum((decimal(item['amount'], 'native base fee', minimum=None) for item in fill.trade_fee['flat_fees']
                        if item['token'] == fill.base_asset), ZERO)
        fill_totals[fill.base_asset] += (amount if fill.trade_type == 'BUY' else -amount) - base_fee
        fills.append(exact_fill)

    rows = list(connection.execute('SELECT * FROM Executors ORDER BY id'))
    evidence, selected = [], defaultdict(dict)
    for raw in rows:
        custom = json.loads(raw['custom_info'] or '{}')
        _require(isinstance(custom, dict), 'native executor custom state is malformed')
        receipt = custom.get('execution_receipt')
        receipt = {} if receipt is None else receipt
        _require(isinstance(receipt, dict), 'native executor receipt is malformed')
        dust = decimal(receipt.get('residual_dust_base', 0), 'native terminal dust')
        if dust == 0:
            evidence.append(SimpleNamespace(id=raw['id'], custom_info=custom))
            continue
        info = _model_row(Executors, raw).to_executor_info()
        config = info.config
        _require(info.type == 'position_executor' and config.id == info.id
                 and config.controller_id == info.controller_id and config.connector_name == 'okx'
                 and config.side == TradeType.BUY and config.leverage == 1
                 and decimal(custom.get('remaining_position_amount_base'), 'managed remainder') == 0,
                 'native terminal dust owner or managed remainder differs')
        pair = config.trading_pair
        _require(pair in lots, 'positive native terminal dust requires bound lot evidence')
        snapshots = custom.get('execution_order_snapshots')
        _require(isinstance(snapshots, list), 'native dust order snapshots are unavailable')
        for snapshot in snapshots:
            _require(isinstance(snapshot, dict), 'native dust order snapshot is malformed')
            order = connection.execute(
                'SELECT config_file_path, market, symbol, exchange_order_id FROM "Order" WHERE id=?',
                (snapshot.get('client_order_id'),)).fetchone()
            _require(order is not None and order['config_file_path'] == script_config
                     and order['market'] == 'okx' and order['symbol'] == pair
                     and order['exchange_order_id'] == snapshot.get('exchange_order_id'),
                     'terminal dust snapshot conflicts with native Order identity')
        view = InventoryView(
            remaining_base=decimal(receipt['remaining_owned_base'], 'receipt remainder'),
            entry_quote=decimal(receipt['entry_gross_quote'], 'native entry quote'),
            entry_price=decimal(custom['entry_price'], 'native entry price', positive=True),
            sold_base=decimal(receipt['exit_gross_base'], 'native exit amount'),
            terminal=info.is_done, entry_terminal=info.is_done,
            held=receipt['accounting_owner'] == 'held_position')
        evidence.append(info)
        selected[pair][info.id] = view

    dust_totals = defaultdict(Decimal)
    for pair, views in selected.items():
        amount, _ = terminal_dust(views, evidence, fills, pair, lots[pair], connector='okx')
        dust_totals[pair.split('-')[0]] += amount
    _require(all(value >= 0 for value in fill_totals.values()), 'native fill history sells unowned inventory')
    return dict(dust_totals), dict(fill_totals), fills


def corroborate_order_receipt(receipt, pair, client_order_id, side, fills):
    """Bind each claimed held or active order to its complete native exact fills."""
    from controllers.generic.modular_spot_core.domain import decimal

    native = {str(fill.exchange_trade_id): fill for fill in fills if fill.order_id == client_order_id}
    claimed = receipt.get('order_fills')
    _require(isinstance(claimed, dict) and claimed and set(claimed) == set(native),
             'claimed order fills differ from complete native history')
    for trade_id, raw in claimed.items():
        fill = native[trade_id]
        _require(isinstance(raw, dict) and fill.symbol == pair and fill.trade_type == side
                 and decimal(raw.get('fill_base_amount'), 'claimed fill amount', positive=True) == fill.amount_decimal
                 and decimal(raw.get('fill_price'), 'claimed fill price', positive=True) == fill.price_decimal
                 and decimal(raw.get('fill_quote_amount'), 'claimed fill quote', positive=True)
                 == fill.amount_decimal * fill.price_decimal and raw.get('fee') == fill.trade_fee,
                 'claimed order receipt conflicts with native exact fill')
        # Historical held-ledger payloads can omit duplicated identity fields.
        # Their containing native order/key still binds identity; any explicit
        # payload identity must agree, including the venue order identity.
        identities = {'trade_id': trade_id, 'client_order_id': client_order_id,
                      'exchange_order_id': fill.native_order_exchange_id, 'trading_pair': pair}
        _require(all(key not in raw or raw[key] == value for key, value in identities.items()),
                 'claimed fill identity conflicts with native order')
        _require(all(key not in receipt or receipt[key] == value for key, value in identities.items()
                     if key != 'trade_id'), 'claimed receipt identity conflicts with native order')


def require_inventory_conservation(owned, fills):
    """No tolerance: many individually small physical residuals can exceed a lot."""
    _require(all(Decimal(str(owned.get(asset, 0))) == amount for asset, amount in fills.items())
             and all(amount == fills.get(asset, ZERO) for asset, amount in owned.items()),
             'complete native fill inventory differs from proved recovery ownership')
