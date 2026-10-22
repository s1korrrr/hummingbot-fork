"""Additive event receipts; legacy six-decimal columns remain unchanged."""
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional, Union

from sqlalchemy import Column, Text, inspect
from sqlalchemy.orm import Session

from hummingbot.model import HummingbotBase
from hummingbot.model.order import Order
from hummingbot.model.trade_fill import TradeFill


class OrderExact(HummingbotBase):
    __tablename__ = "OrderExact"

    order_id = Column(Text, primary_key=True)
    amount = Column(Text, nullable=False)
    price = Column(Text, nullable=False)


class TradeFillExact(HummingbotBase):
    __tablename__ = "TradeFillExact"

    market = Column(Text, primary_key=True)
    order_id = Column(Text, primary_key=True)
    exchange_trade_id = Column(Text, primary_key=True)
    amount = Column(Text, nullable=False)
    price = Column(Text, nullable=False)
    fee_in_quote = Column(Text, nullable=True)


def decimal_text(value) -> str:
    value = Decimal(str(value))
    if not value.is_finite():
        raise ValueError("Exact receipt requires finite decimal values")
    return str(value)


@dataclass(frozen=True)
class RecordedValues:
    amount: Decimal
    price: Decimal
    fee_in_quote: Optional[Decimal]
    precision: str


def recorded_values(row: Union[Order, TradeFill], receipt=None) -> RecordedValues:
    """Decode an already joined receipt without performing I/O."""
    is_fill = isinstance(row, TradeFill)
    if is_fill and row.has_exact_receipt:
        native = RecordedValues(row.amount_decimal, row.price_decimal,
                                row.trade_fee_in_quote_decimal, "event_exact")
        if receipt is not None:
            companion = RecordedValues(Decimal(decimal_text(receipt.amount)), Decimal(decimal_text(receipt.price)),
                                       Decimal(decimal_text(receipt.fee_in_quote))
                                       if receipt.fee_in_quote is not None else None, "event_exact")
            if native != companion:
                raise ValueError("Native exact fill and companion receipt conflict")
        return native
    if receipt is not None:
        return RecordedValues(Decimal(receipt.amount), Decimal(receipt.price),
                              Decimal(receipt.fee_in_quote) if is_fill and receipt.fee_in_quote is not None else None,
                              "event_exact")
    return RecordedValues(row.amount, row.price,
                          row.trade_fee_in_quote if is_fill and row.trade_fee_in_quote else None,
                          "legacy_6dp")


def read_exact_values(session: Session, row: Union[Order, TradeFill]) -> RecordedValues:
    """Read event precision where retained, disclosing legacy precision otherwise.

    Missing quote fees remain unavailable; this helper performs no conversion or
    exchange lookup. A legacy zero fee is not evidence that conversion succeeded.
    Supports read-only old databases without the additive tables.
    """
    is_fill = isinstance(row, TradeFill)
    model = TradeFillExact if is_fill else OrderExact
    key = (row.market, row.order_id, row.exchange_trade_id) if is_fill else row.id
    receipt = None
    if inspect(session.connection()).has_table(model.__tablename__):
        receipt = session.get(model, key)
    return recorded_values(row, receipt)


def trade_fills_for_config(session: Session, config_file_path: str, number_of_rows: Optional[int] = None) -> List[TradeFill]:
    """One recorder config's fills, newest first, with immutable exact values attached, in `session`."""
    has_exact = inspect(session.connection()).has_table(TradeFillExact.__tablename__)
    query = (session.query(TradeFill)
             .filter(TradeFill.config_file_path == config_file_path)
             .order_by(TradeFill.timestamp.desc()))
    if has_exact:
        query = query.add_entity(TradeFillExact).outerjoin(TradeFillExact, (
            (TradeFill.market == TradeFillExact.market)
            & (TradeFill.order_id == TradeFillExact.order_id)
            & (TradeFill.exchange_trade_id == TradeFillExact.exchange_trade_id)
        ))
    if number_of_rows is not None:
        query = query.limit(number_of_rows)
    trades = []
    for result in query.all():
        trade, receipt = result if has_exact else (result, None)
        # Attach immutable exact values; never rewrite mapped legacy columns.
        trade.recorded_values = recorded_values(trade, receipt)
        trades.append(trade)
    return trades
