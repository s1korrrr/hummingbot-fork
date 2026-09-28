from decimal import Decimal as D

import pytest

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, DeductedFromReturnsTradeFee, TokenAmount
from hummingbot.strategy_v2.executors.order_accounting import base_fee_amount, in_flight_base_fee

FEES = {
    "flat_base": lambda: AddedToCostTradeFee(flat_fees=[TokenAmount("ETH", D("0.0004"))]),
    "flat_quote": lambda: AddedToCostTradeFee(flat_fees=[TokenAmount("USDC", D("0.3"))]),
    "flat_both": lambda: AddedToCostTradeFee(flat_fees=[TokenAmount("ETH", D("0.0001")), TokenAmount("USDC", D("0.1"))]),
    "percent_base": lambda: AddedToCostTradeFee(percent=D("0.001"), percent_token="ETH"),
    "percent_quote": lambda: AddedToCostTradeFee(percent=D("0.001"), percent_token="USDC"),
    "deducted_untokened": lambda: DeductedFromReturnsTradeFee(percent=D("0.0008")),
}


def order(side, fees):
    result = InFlightOrder("oid", "ETH-USDC", OrderType.LIMIT, side, D("0.3"), 1.0, D("2000"), "ex")
    for index, fee in enumerate(fees):
        result.order_fills[f"t{index}"] = TradeUpdate(
            trade_id=f"t{index}", client_order_id="oid", exchange_order_id="ex", trading_pair="ETH-USDC",
            fill_timestamp=1.0 + index, fill_price=D("2000") + index, fill_base_amount=D("0.1"),
            fill_quote_amount=D("200"), fee=fee)
    return result


@pytest.mark.parametrize("side", [TradeType.BUY, TradeType.SELL])
@pytest.mark.parametrize("kinds", [[], *[[kind] for kind in FEES], list(FEES)])
def test_in_flight_base_fee_equals_the_full_order_record(side, kinds):
    live = order(side, [FEES[kind]() for kind in kinds])
    assert in_flight_base_fee(live, "ETH") == base_fee_amount(live.to_json(), "ETH")
