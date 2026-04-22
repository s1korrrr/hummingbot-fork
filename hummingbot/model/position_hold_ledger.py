from typing import Any, Dict

from sqlalchemy import JSON, BigInteger, Column, Index, Text

from . import HummingbotBase
from .decimal_type_decorator import SqliteDecimal


class PositionHoldLedger(HummingbotBase):
    """
    Durable ledger of held-order events used to rebuild controller bag state after restart.
    """

    __tablename__ = "PositionHoldLedger"
    __table_args__ = (
        Index("phl_controller_timestamp_index", "controller_id", "timestamp"),
        Index("phl_connector_pair_timestamp_index", "connector_name", "trading_pair", "timestamp"),
        Index("phl_controller_pair_side_timestamp_index", "controller_id", "trading_pair", "position_side", "timestamp"),
    )

    id = Column(Text, primary_key=True, nullable=False)
    controller_id = Column(Text, nullable=False)
    executor_id = Column(Text, nullable=False)
    connector_name = Column(Text, nullable=False)
    trading_pair = Column(Text, nullable=False)
    position_side = Column(Text, nullable=True)
    order_side = Column(Text, nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    client_order_id = Column(Text, nullable=False)
    exchange_order_id = Column(Text, nullable=True)
    price = Column(SqliteDecimal(6), nullable=False)
    amount = Column(SqliteDecimal(6), nullable=False)
    executed_amount_base = Column(SqliteDecimal(6), nullable=False)
    executed_amount_quote = Column(SqliteDecimal(6), nullable=False)
    cumulative_fee_paid_quote = Column(SqliteDecimal(6), nullable=False)
    order_payload = Column(JSON, nullable=False)

    def to_order_record(self) -> Dict[str, Any]:
        payload = dict(self.order_payload or {})
        payload.setdefault("client_order_id", self.client_order_id)
        payload.setdefault("exchange_order_id", self.exchange_order_id)
        payload.setdefault("trade_type", self.order_side)
        payload.setdefault("price", str(self.price))
        payload.setdefault("amount", str(self.amount))
        payload.setdefault("executed_amount_base", str(self.executed_amount_base))
        payload.setdefault("executed_amount_quote", str(self.executed_amount_quote))
        payload.setdefault("cumulative_fee_paid_quote", str(self.cumulative_fee_paid_quote))
        payload.setdefault("last_update_timestamp", self.timestamp)
        return payload
