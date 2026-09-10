import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import JSON, BigInteger, Column, ForeignKey, Index, Integer, Text
from sqlalchemy.orm import Session, relationship

from hummingbot.core.event.events import PositionAction
from hummingbot.model import HummingbotBase
from hummingbot.model.decimal_type_decorator import SqliteDecimal


class TradeFill(HummingbotBase):
    __tablename__ = "TradeFill"
    __table_args__ = (Index("tf_config_timestamp_index",
                            "config_file_path", "timestamp"),
                      Index("tf_market_trading_pair_timestamp_index",
                            "market", "symbol", "timestamp"),
                      Index("tf_market_base_asset_timestamp_index",
                            "market", "base_asset", "timestamp"),
                      Index("tf_market_quote_asset_timestamp_index",
                            "market", "quote_asset", "timestamp")
                      )

    config_file_path = Column(Text, nullable=False)
    strategy = Column(Text, nullable=False)
    market = Column(Text, primary_key=True, nullable=False)
    symbol = Column(Text, nullable=False)
    base_asset = Column(Text, nullable=False)
    quote_asset = Column(Text, nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    order_id = Column(Text, ForeignKey("Order.id"), primary_key=True, nullable=False)
    trade_type = Column(Text, nullable=False)
    order_type = Column(Text, nullable=False)
    price = Column(SqliteDecimal(6), nullable=False)
    amount = Column(SqliteDecimal(6), nullable=False)
    leverage = Column(Integer, nullable=False, default=1)
    trade_fee = Column(JSON, nullable=False)
    trade_fee_in_quote = Column(SqliteDecimal(6))
    # Additive exact receipts: legacy integers retain their original six-decimal scale.
    exact_amount = Column(Text, nullable=True)
    exact_price = Column(Text, nullable=True)
    exact_trade_fee_in_quote = Column(Text, nullable=True)
    exact_receipt_source = Column(Text, nullable=True)
    exchange_trade_id = Column(Text, primary_key=True, nullable=False)
    position = Column(Text, nullable=True, default=PositionAction.NIL.value)
    order = relationship("Order", back_populates="trade_fills")

    @staticmethod
    def decimal_text(value) -> Optional[str]:
        if value is None:
            return None
        try:
            decimal = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as error:
            raise ValueError("Invalid trade fill decimal") from error
        if not decimal.is_finite():
            raise ValueError("Non-finite trade fill decimal")
        return str(decimal)

    @property
    def has_exact_receipt(self) -> bool:
        return self.exact_receipt_source is not None or any(
            getattr(self, f"exact_{field}") is not None for field in ("amount", "price", "trade_fee_in_quote"))

    def _decimal_value(self, field: str) -> Optional[Decimal]:
        if self.has_exact_receipt and self.exact_receipt_source != "order_filled_event_v1":
            raise ValueError("Missing or unsupported exact trade fill provenance")
        value = getattr(self, f"exact_{field}") if self.has_exact_receipt else getattr(self, field)
        text = self.decimal_text(value)
        if text is None:
            if field in ("amount", "price"):
                raise ValueError(f"Missing exact trade fill {field}" if self.has_exact_receipt
                                 else f"Missing legacy trade fill {field}")
            return None
        result = Decimal(text)
        if field in ("amount", "price") and result <= 0:
            raise ValueError(f"Trade fill {field} must be positive")
        return result

    @property
    def amount_decimal(self) -> Decimal:
        return self._decimal_value("amount")

    @property
    def price_decimal(self) -> Decimal:
        return self._decimal_value("price")

    @property
    def trade_fee_in_quote_decimal(self) -> Optional[Decimal]:
        return self._decimal_value("trade_fee_in_quote")

    @property
    def receipt_precision(self) -> Dict[str, str]:
        precision = {}
        for field in ("amount", "price", "trade_fee_in_quote"):
            try:
                value = self._decimal_value(field)
            except ValueError:
                value = None
            precision[field] = ("unavailable" if value is None else
                                "exact_decimal" if self.has_exact_receipt else "legacy_6dp")
        return precision

    def value_for_file_export(self, field: str):
        if field in ("amount", "price", "trade_fee_in_quote"):
            return self._decimal_value(field)
        if field == "receipt_precision":
            return json.dumps(self.receipt_precision, sort_keys=True)
        return getattr(self, field)

    def __repr__(self) -> str:
        return f"TradeFill(config_file_path='{self.config_file_path}', strategy='{self.strategy}', " \
               f"market='{self.market}', symbol='{self.symbol}', base_asset='{self.base_asset}', " \
               f"quote_asset='{self.quote_asset}', timestamp={self.timestamp}, order_id='{self.order_id}', " \
               f"trade_type='{self.trade_type}', order_type='{self.order_type}', price={self.price}, " \
               f"amount={self.amount}, leverage={self.leverage}, trade_fee={self.trade_fee}, " \
               f"exchange_trade_id={self.exchange_trade_id}, position={self.position})"

    @staticmethod
    def get_trades(sql_session: Session,
                   strategy: str = None,
                   market: str = None,
                   trading_pair: str = None,
                   base_asset: str = None,
                   quote_asset: str = None,
                   trade_type: str = None,
                   order_type: str = None,
                   start_time: int = None,
                   end_time: int = None,
                   ) -> Optional[List["TradeFill"]]:
        filters = []
        if strategy is not None:
            filters.append(TradeFill.strategy == strategy)
        if market is not None:
            filters.append(TradeFill.market == market)
        if trading_pair is not None:
            filters.append(TradeFill.symbol == trading_pair)
        if base_asset is not None:
            filters.append(TradeFill.base_asset == base_asset)
        if quote_asset is not None:
            filters.append(TradeFill.quote_asset == quote_asset)
        if trade_type is not None:
            filters.append(TradeFill.trade_type == trade_type)
        if order_type is not None:
            filters.append(TradeFill.order_type == order_type)
        if start_time is not None:
            filters.append(TradeFill.timestamp >= start_time)
        if end_time is not None:
            filters.append(TradeFill.timestamp <= end_time)

        trades: Optional[List[TradeFill]] = (sql_session
                                             .query(TradeFill)
                                             .filter(*filters)
                                             .order_by(TradeFill.timestamp.asc())
                                             .all())
        return trades

    @classmethod
    def to_pandas(cls, trades: List):
        columns: List[str] = ["Id",
                              "Timestamp",
                              "Exchange",
                              "Market",
                              "Order_type",
                              "Side",
                              "Price",
                              "Amount",
                              "Leverage",
                              "Position",
                              "Age"]
        data = []
        for trade in trades:

            if trade.order is None:  # order creation update has not arrived yet
                age = pd.Timestamp(0, unit='s').strftime('%H:%M:%S')
            else:
                age = pd.Timestamp(int(trade.timestamp / 1e3 - trade.order.creation_timestamp / 1e3),
                                   unit='s').strftime('%H:%M:%S')
            data.append([
                trade.exchange_trade_id,
                datetime.fromtimestamp(int(trade.timestamp / 1e3)).strftime("%Y-%m-%d %H:%M:%S"),
                trade.market,
                trade.symbol,
                trade.order_type.lower(),
                trade.trade_type.lower(),
                trade.price_decimal,
                trade.amount_decimal,
                trade.leverage,
                trade.position,
                age,
            ])
        df = pd.DataFrame(data=data, columns=columns)
        df.set_index('Id', inplace=True)

        return df

    @staticmethod
    def to_bounty_api_json(trade_fill: "TradeFill") -> Dict[str, Any]:
        return {
            "market": trade_fill.market,
            "trade_id": trade_fill.exchange_trade_id,
            "price": format(trade_fill.price_decimal, "f"),
            "quantity": format(trade_fill.amount_decimal, "f"),
            "symbol": trade_fill.symbol,
            "trade_timestamp": trade_fill.timestamp,
            "trade_type": trade_fill.trade_type,
            "base_asset": trade_fill.base_asset,
            "quote_asset": trade_fill.quote_asset,
            "raw_json": {
                "trade_fee": trade_fill.trade_fee,
            }
        }

    @staticmethod
    def attribute_names_for_file_export():

        return [
            "exchange_trade_id",  # Keep the key attribute first in the list
            "config_file_path",
            "strategy",
            "market",
            "symbol",
            "base_asset",
            "quote_asset",
            "timestamp",
            "order_id",
            "trade_type",
            "order_type",
            "price",
            "amount",
            "leverage",
            "trade_fee",
            "trade_fee_in_quote",
            "position",
            "exact_receipt_source",
            "receipt_precision", ]
