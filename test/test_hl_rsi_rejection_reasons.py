"""Every executor rejection by the HL RSI validator names its gate reason (observability only)."""
from decimal import Decimal
from types import SimpleNamespace

import pytest

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.models.base import RunnableStatus


def controller(*, mid=Decimal("100"), ready=True, regime_error=False, same_side=False, can_create=True,
               kill=False, status=RunnableStatus.RUNNING):
    from controllers.directional_trading.hl_rsi import HLRSIController
    instance = object.__new__(HLRSIController)
    instance._status = status
    instance.config = SimpleNamespace(connector_name="okx", trading_pair="BTC-USDC", strict_hmm_mode=True,
                                      allocation_mode="bounded",
                                      max_total_position_usd=Decimal("1000"), manual_kill_switch=kill)
    instance.market_data_provider = SimpleNamespace(time=lambda: 1000.0)
    instance.update_interval = 1.0
    instance.processed_data = {"data_health": {"ready": ready}, "regime_error": regime_error}
    instance._get_mid_price = lambda: mid
    instance.can_create_executor = lambda signal: can_create
    instance._filter_same_side = lambda side, active_only=False: [object()] if same_side else []
    instance._get_total_position_value_usd = lambda: Decimal("0")
    instance._get_sellable_inventory_amount = lambda: Decimal("10")
    instance.recorded = []
    instance._record_gate_reason = lambda side, reason: instance.recorded.append((side, reason))
    return instance


def executor(side=TradeType.BUY, *, pair="BTC-USDC", timestamp=999.5, amount=Decimal("1"), entry=Decimal("100")):
    return SimpleNamespace(connector_name="okx", trading_pair=pair, timestamp=timestamp, side=side,
                           amount=amount, entry_price=entry)


@pytest.mark.parametrize("make, config, reason", [
    (lambda: controller(kill=True), executor(), "manual_kill_switch"),
    (lambda: controller(status=RunnableStatus.TERMINATED), executor(), "controller_stopped"),
    (lambda: controller(), executor(pair="ETH-USDC"), "executor_scope_mismatch"),
    (lambda: controller(), executor(timestamp=980.0), "stale_action"),
    (lambda: controller(ready=False), executor(), "data_unavailable"),
    (lambda: controller(mid=None), executor(), "price_unavailable"),
    (lambda: controller(), executor(amount=Decimal("0")), "invalid_amount"),
    (lambda: controller(), executor(entry=None), "invalid_entry_price"),
    (lambda: controller(regime_error=True), executor(TradeType.SELL), "regime_error"),
    (lambda: controller(same_side=True), executor(TradeType.SELL), "sell_active"),
])
def test_rejection_records_its_gate_reason(make, config, reason):
    instance = make()
    assert instance.validate_executor_config(config) is False
    assert instance.recorded[-1] == (config.side, reason)


def test_accepted_executor_records_no_rejection_reason():
    instance = controller()
    assert instance.validate_executor_config(executor()) is True
    assert instance.recorded == []
