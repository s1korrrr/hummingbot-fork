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
    for raw in connection.execute('SELECT * FROM TradeFill'):
        fill = _model_row(TradeFill, raw)
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
        companion = None
        if 'TradeFillExact' in tables:
            exact = connection.execute('SELECT * FROM TradeFillExact WHERE market=? AND order_id=? '
                                       'AND exchange_trade_id=?',
                                       (fill.market, fill.order_id, fill.exchange_trade_id)).fetchone()
            if exact is not None:
                companion = _model_row(TradeFillExact, exact)
        values = recorded_values(fill, companion)
        _require(values.precision == 'event_exact', 'native fill lacks exact event precision for recovery')
        # Keep the original six-decimal columns intact. Attach proven exact event
        # values only to this detached in-memory model for the shared proof reader.
        fill.exact_amount = str(values.amount)
        fill.exact_price = str(values.price)
        fill.exact_trade_fee_in_quote = None if values.fee_in_quote is None else str(values.fee_in_quote)
        fill.exact_receipt_source = 'order_filled_event_v1'
        ExecutorOrchestrator._trade_fill_order_record(fill)  # validates native fee and exact scalar contracts
        base_fee = sum((decimal(item['amount'], 'native base fee', minimum=None) for item in fill.trade_fee['flat_fees']
                        if item['token'] == fill.base_asset), ZERO)
        fill_totals[fill.base_asset] += (fill.amount_decimal if fill.trade_type == 'BUY'
                                         else -fill.amount_decimal) - base_fee
        fills.append(fill)

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
