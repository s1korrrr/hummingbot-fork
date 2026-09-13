"""Proof checks for cost-unknown spot wallet disposals; never synthesize acquired inventory."""
from decimal import Decimal


def legacy_value(value):
    return Decimal(int(value * 1000000)) / 1000000


def validate_wallet_sale_summary(summary, sales):
    """Recognize the old SELL-only held summary using its retained native receipt prefix."""
    sales = [sale for sale in sales if sale['timestamp'] <= summary.timestamp]
    amount = sum((sale['amount_base'] for sale in sales), Decimal('0'))
    quote = sum((sale['proceeds_quote'] for sale in sales), Decimal('0'))
    fees = sum((sale['fees_quote'] for sale in sales), Decimal('0'))
    # The pre-repair owner marks its synthetic SELL summary at the shutdown
    # midpoint. This finite historical projection is not wallet economic PnL.
    unrealized = Decimal(str(summary.unrealized_pnl_quote))
    if (amount <= 0 or summary.side != 'SELL' or not unrealized.is_finite()
            or any(Decimal(str(getattr(summary, field))) != legacy_value(value) for field, value in (
                ('amount', amount), ('breakeven_price', quote / amount),
                ('volume_traded_quote', quote), ('cum_fees_quote', fees),
                ('realized_pnl_quote', Decimal('0'))))):
        raise ValueError('Stored wallet sale summary conflicts with exact sale receipts')


def validate_completed_wallet_order(info, payload, ledger, native, exact, config_file_path):
    """Only a fully filled, corroborated order can resolve a stale saved shutdown."""
    if native is None or exact is None or ledger is None:
        raise ValueError('Completed wallet sale lacks native order, exact order, or ledger proof')
    amount, price = Decimal(str(exact.amount)), Decimal(str(exact.price))
    if (not amount.is_finite() or not price.is_finite() or amount <= 0 or price <= 0
            or native.id != payload.get('client_order_id') or native.exchange_order_id != payload.get('exchange_order_id')
            or native.last_status != 'SellOrderCompleted' or payload.get('last_state') != '5'
            or native.config_file_path != config_file_path or native.market != info.connector_name
            or native.symbol != info.trading_pair or native.base_asset != info.trading_pair.split('-')[0]
            or native.quote_asset != info.trading_pair.split('-')[1] or native.leverage != 1
            or native.position not in (None, 'NIL') or payload.get('trade_type') != 'SELL'
            or amount != Decimal(str(payload.get('executed_amount_base')))
            or amount != Decimal(str(payload.get('amount'))) or price != Decimal(str(payload.get('price')))
            or native.amount != legacy_value(amount) or native.price != legacy_value(price)
            or ledger.executor_id != info.id or ledger.order_side != 'SELL'
            or ledger.client_order_id != native.id or ledger.exchange_order_id != native.exchange_order_id
            or ledger.order_payload.get('order_fills') != payload.get('order_fills')
            or Decimal(str(ledger.order_payload.get('executed_amount_base'))) != amount
            or Decimal(str(ledger.order_payload.get('executed_amount_quote'))) !=
            Decimal(str(payload.get('executed_amount_quote')))
            or Decimal(str(ledger.order_payload.get('cumulative_fee_paid_quote'))) !=
            Decimal(str(payload.get('cumulative_fee_paid_quote')))):
        raise ValueError('Completed wallet sale order proof conflicts')
    return amount
