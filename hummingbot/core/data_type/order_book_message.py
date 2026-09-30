from collections import namedtuple
from enum import Enum
from typing import Dict, List, Optional

from hummingbot.core.data_type.order_book_row import OrderBookRow


class OrderBookMessageType(Enum):
    SNAPSHOT = 1
    DIFF = 2
    TRADE = 3


class OrderBookMessage(namedtuple("_OrderBookMessage", "type, content, timestamp")):
    type: OrderBookMessageType
    content: Dict[str, any]
    timestamp: float

    def __new__(
        cls,
        message_type: OrderBookMessageType,
        content: Dict[str, any],
        timestamp: Optional[float] = None,
        *args,
        **kwargs,
    ):
        return super(OrderBookMessage, cls).__new__(cls, message_type, content, timestamp, *args, **kwargs)

    @property
    def update_id(self) -> int:
        if self.type in [OrderBookMessageType.DIFF, OrderBookMessageType.SNAPSHOT]:
            return self.content["update_id"]
        else:
            return -1

    @property
    def first_update_id(self) -> int:
        if self.type is OrderBookMessageType.DIFF:
            return self.content.get("first_update_id", self.update_id)
        else:
            return -1

    @property
    def trade_id(self) -> int:
        if self.type is OrderBookMessageType.TRADE:
            return self.content["trade_id"]
        return -1

    @property
    def trading_pair(self) -> str:
        return self.content["trading_pair"]

    @property
    def asks(self) -> List[OrderBookRow]:
        return [
            OrderBookRow(float(price), float(amount), self.update_id) for price, amount, *trash in self.content["asks"]
        ]

    @property
    def bids(self) -> List[OrderBookRow]:
        return [
            OrderBookRow(float(price), float(amount), self.update_id) for price, amount, *trash in self.content["bids"]
        ]

    @property
    def has_update_id(self) -> bool:
        return self.type in {OrderBookMessageType.DIFF, OrderBookMessageType.SNAPSHOT}

    @property
    def has_trade_id(self) -> bool:
        return self.type == OrderBookMessageType.TRADE

    def __eq__(self, other: "OrderBookMessage") -> bool:
        if not isinstance(other, OrderBookMessage):
            return NotImplemented
        if self.type != other.type:
            return False
        return self.update_id == other.update_id if self.has_update_id else self.trade_id == other.trade_id

    def __ne__(self, other: "OrderBookMessage") -> bool:
        # namedtuple supplies tuple.__ne__, which otherwise disagrees with ID-based equality.
        equal = self.__eq__(other)
        return NotImplemented if equal is NotImplemented else not equal

    def __le__(self, other: "OrderBookMessage") -> bool:
        less = self.__lt__(other)
        return NotImplemented if less is NotImplemented else less or self == other

    def __gt__(self, other: "OrderBookMessage") -> bool:
        less_or_equal = self.__le__(other)
        return NotImplemented if less_or_equal is NotImplemented else not less_or_equal

    def __ge__(self, other: "OrderBookMessage") -> bool:
        less = self.__lt__(other)
        return NotImplemented if less is NotImplemented else not less

    def __hash__(self):
        return hash((self.type, self.update_id if self.has_update_id else self.trade_id))

    def __lt__(self, other: "OrderBookMessage") -> bool:
        """Order each ID domain independently; mixed domains retain timestamp compatibility.

        Book IDs determine snapshot replay. A diff at the snapshot's ID is already
        represented and sorts before that snapshot. Mixed book/trade comparisons use
        timestamps, with books first on ties. This does not promise a general mixed
        sort order when unrelated exchange IDs and clocks disagree.
        """
        if not isinstance(other, OrderBookMessage):
            return NotImplemented
        if self.has_update_id and other.has_update_id:
            if self.update_id != other.update_id:
                return self.update_id < other.update_id
            return self.type is OrderBookMessageType.DIFF and other.type is OrderBookMessageType.SNAPSHOT
        if self.has_trade_id and other.has_trade_id:
            return self.trade_id < other.trade_id
        if self.timestamp != other.timestamp:
            return self.timestamp < other.timestamp
        return self.has_update_id
