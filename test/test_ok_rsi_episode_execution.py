import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


def order(*, side=TradeType.BUY, fee_token="ETH", fee="0.001", state=OrderState.PARTIALLY_FILLED):
    result = InFlightOrder(
        client_order_id="order-1", exchange_order_id="exchange-1", trading_pair="ETH-USDT",
        order_type=OrderType.LIMIT_MAKER, trade_type=side, price=Decimal("100"),
        amount=Decimal("1"), creation_timestamp=1, initial_state=state,
    )
    result.update_with_trade_update(TradeUpdate(
        trade_id="fill-1", client_order_id="order-1", exchange_order_id="exchange-1",
        trading_pair="ETH-USDT", fill_timestamp=2, fill_price=Decimal("100"),
        fill_base_amount=Decimal("0.4"), fill_quote_amount=Decimal("40"),
        fee=AddedToCostTradeFee(flat_fees=[TokenAmount(fee_token, Decimal(fee))]),
    ))
    return result


def executor():
    from controllers.directional_trading.ok_rsi_core.execution import EpisodeOrderExecutor
    strategy = MagicMock()
    strategy.connectors = {"okx": MagicMock()}
    strategy.current_timestamp = 3
    config = OrderExecutorConfig(
        id="request-1", timestamp=1, side=TradeType.BUY, connector_name="okx",
        trading_pair="ETH-USDT", amount=Decimal("1"), price=Decimal("100"),
        execution_strategy=ExecutionStrategy.LIMIT_MAKER, level_id="ok_rsi_episode:test:0",
    )
    instance = EpisodeOrderExecutor(strategy, config)
    instance._status = RunnableStatus.RUNNING
    instance._sleep = AsyncMock()
    instance._order = TrackedOrder(order_id="order-1")
    return instance


def test_fill_receipt_keeps_actual_fee_currency_and_duplicate_identity():
    instance = executor()
    instance._order.order = order()
    receipt = instance.get_custom_info()["episode_receipt"]
    assert receipt["gross_base"] == "0.4"
    assert receipt["gross_quote"] == "40"
    assert receipt["base_fee"] == "0.001"
    assert receipt["quote_fee"] == "0"
    assert receipt["fees_known"] is True
    assert instance.filled_amount_quote == Decimal("40")
    assert receipt == instance.get_custom_info()["episode_receipt"]


def test_fill_receipt_preserves_native_episode_intent():
    from controllers.directional_trading.ok_rsi_core.execution import EpisodeOrderExecutorConfig

    instance = executor()
    instance.config = EpisodeOrderExecutorConfig(
        **instance.config.model_dump(),
        episode_id="episode-1",
        episode_expires_at=60,
        episode_intent="SCOUT_BUY",
    )
    instance._order.order = order()

    assert instance.get_custom_info()["episode_receipt"]["intent"] == "SCOUT_BUY"


def test_fill_receipt_preserves_native_structure_exit_authorization():
    from controllers.directional_trading.ok_rsi_core.execution import EpisodeOrderExecutorConfig

    instance = executor()
    instance.config = EpisodeOrderExecutorConfig(
        **instance.config.model_dump(),
        episode_id="episode-1",
        episode_expires_at=60,
        episode_intent="DISTRIBUTION_SELL",
        episode_exit_policy="STRUCTURE_HELD_RUNNER_V1",
        episode_exit_hourly_recovered=True,
        episode_exit_risk_clear=True,
        episode_exit_trail_breach="PRICE_AND_EQUITY",
        episode_exit_target_base=Decimal("0.4"),
        episode_exit_target_revision=2,
    )
    instance._order.order = order(side=TradeType.SELL)

    receipt = instance.get_custom_info()["episode_receipt"]
    assert receipt["exit_policy"] == "STRUCTURE_HELD_RUNNER_V1"
    assert receipt["exit_hourly_recovered"] is True
    assert receipt["exit_risk_clear"] is True
    assert receipt["exit_trail_breach"] == "PRICE_AND_EQUITY"
    assert receipt["exit_target_base"] == "0.4"
    assert receipt["exit_target_revision"] == 2


def test_fill_receipt_preserves_markup_release_authorization():
    from controllers.directional_trading.ok_rsi_core.execution import EpisodeOrderExecutorConfig

    instance = executor()
    instance.config = EpisodeOrderExecutorConfig(
        **instance.config.model_dump(),
        episode_id="episode-1",
        episode_expires_at=60,
        episode_intent="TRAILING_PROFIT_EXIT",
        episode_exit_policy="MARKUP_RELEASED_RUNNER_V1",
        episode_exit_hourly_recovered=True,
        episode_exit_risk_clear=True,
        episode_exit_trail_breach="MARKUP_LOSS",
        episode_exit_target_base=Decimal("0"),
        episode_exit_target_revision=2,
    )
    instance._order.order = order(side=TradeType.SELL)

    receipt = instance.get_custom_info()["episode_receipt"]
    assert receipt["exit_policy"] == "MARKUP_RELEASED_RUNNER_V1"
    assert receipt["exit_trail_breach"] == "MARKUP_LOSS"
    assert receipt["exit_target_base"] == "0"
    assert receipt["exit_target_revision"] == 2


def test_unknown_third_asset_fee_is_visible_and_not_zero_cost():
    instance = executor()
    instance._order.order = order(fee_token="BNB")
    receipt = instance.get_custom_info()["episode_receipt"]
    assert receipt["fees_known"] is False
    assert "unsupported_fee_asset:BNB" in receipt["errors"]


def test_cancelled_partial_preserves_native_hold_and_never_retries():
    instance = executor()
    native = order(state=OrderState.CANCELED)
    instance._order.order = native
    instance.process_order_canceled_event(None, None, SimpleNamespace(order_id="order-1"))
    asyncio.run(instance.control_task())
    assert instance.status == RunnableStatus.TERMINATED
    assert instance.close_type == CloseType.POSITION_HOLD
    assert len(instance.get_custom_info()["held_position_orders"]) == 1
    assert instance.get_custom_info()["held_position_orders"][0]["executed_amount_base"] == "0.4"
    assert instance.get_custom_info()["episode_receipt"]["final"] is True
    instance._strategy.buy.assert_not_called()


def test_unacknowledged_cancel_keeps_reservation_nonterminal():
    instance = executor()
    instance._order.order = order()
    instance.early_stop()
    asyncio.run(instance.control_task())
    assert instance.status == RunnableStatus.SHUTTING_DOWN
    assert instance.get_custom_info()["episode_receipt"]["final"] is False


def test_completed_order_missing_fill_details_is_not_final_receipt():
    instance = executor()
    native = order(state=OrderState.FILLED)
    native.executed_amount_base = Decimal("1")
    native.executed_amount_quote = Decimal("100")
    instance._order.order = native
    instance._status = RunnableStatus.SHUTTING_DOWN
    asyncio.run(instance.control_task())
    receipt = instance.get_custom_info()["episode_receipt"]
    assert receipt["final"] is False
    assert receipt["fees_known"] is False
    assert "incomplete_fill_records" in receipt["errors"]


def test_native_position_hold_accounts_base_fee_once_and_closes_net_inventory():
    from hummingbot.strategy_v2.executors.executor_orchestrator import PositionHold
    instance = executor()
    instance._order.order = order(state=OrderState.CANCELED)
    instance._status = RunnableStatus.SHUTTING_DOWN
    asyncio.run(instance.control_task())
    record = instance.get_custom_info()["held_position_orders"][0]
    hold = PositionHold("okx", "ETH-USDT", TradeType.BUY)
    hold.add_order_record(record)
    hold.add_order_record(record)
    assert hold.buy_amount_base == Decimal(".399")
    assert hold.cum_fees_quote == Decimal("0")
    hold.add_order_record(dict(client_order_id="sell-1", trade_type="SELL",
                               executed_amount_base=".399", executed_amount_quote="41", cumulative_fee_paid_quote=".041"))
    assert hold.get_position_summary(Decimal("100")).amount == 0
    assert hold.get_position_summary(Decimal("100")).realized_pnl_quote.quantize(Decimal(".00000001")) == Decimal("1")


def test_episode_order_expiry_cancels_without_new_submission():
    from controllers.directional_trading.ok_rsi_core.execution import EpisodeOrderExecutorConfig
    instance = executor()
    instance.config = EpisodeOrderExecutorConfig(**instance.config.model_dump(), episode_id="episode-1", episode_expires_at=3)
    instance._order.order = order()
    asyncio.run(instance.control_task())
    assert instance.status == RunnableStatus.SHUTTING_DOWN
    instance._strategy.buy.assert_not_called()


def test_cancelled_unknown_native_order_retains_reservation():
    instance = executor()
    instance.get_in_flight_order = MagicMock(return_value=None)
    instance.process_order_canceled_event(None, None, SimpleNamespace(order_id="order-1"))
    asyncio.run(instance.control_task())
    receipt = instance.get_custom_info()["episode_receipt"]
    assert instance.status == RunnableStatus.SHUTTING_DOWN
    assert receipt["final"] is False
    assert receipt["fees_known"] is False
    assert "unresolved_native_order:order-1" in receipt["errors"]


def test_late_native_partial_record_after_unknown_cancel_is_reconciled():
    instance = executor()
    instance.get_in_flight_order = MagicMock(return_value=None)
    instance.process_order_canceled_event(None, None, SimpleNamespace(order_id="order-1"))
    asyncio.run(instance.control_task())
    assert instance.status == RunnableStatus.SHUTTING_DOWN
    native = order(state=OrderState.CANCELED)
    instance.get_in_flight_order.return_value = native
    asyncio.run(instance.control_task())
    receipt = instance.get_custom_info()["episode_receipt"]
    assert receipt["gross_base"] == "0.4"
    assert receipt["base_fee"] == "0.001"
    assert receipt["final"] is True


def test_failed_unknown_native_order_retains_reservation():
    instance = executor()
    instance.get_in_flight_order = MagicMock(return_value=None)
    instance.process_order_failed_event(None, None, SimpleNamespace(order_id="order-1"))
    asyncio.run(instance.control_task())
    assert instance.status == RunnableStatus.SHUTTING_DOWN
    receipt = instance.get_custom_info()["episode_receipt"]
    assert receipt["final"] is False
    assert "unresolved_native_order:order-1" in receipt["errors"]
