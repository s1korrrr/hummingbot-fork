"""Native fixture regressions for the follow-up review of consolidated PR #52."""
import asyncio
from decimal import Decimal
from test.test_ok_rsi_admission_safety import _hold_done, controller as okx_controller
from test.test_ok_rsi_down_only_entry_execution import (
    bind_controller,
    finish_reconciliation,
    install_terminal_reconciliation,
    make_config,
    make_executor,
    request_reprice,
    tracked_order,
)
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine

from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import OrderCancelledEvent
from hummingbot.model.position_hold_ledger import PositionHoldLedger
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.strategy_v2.executors.executor_orchestrator import PositionHold
from hummingbot.strategy_v2.models.base import RunnableStatus


def test_missing_receipt_deadline_keeps_submitted_identity():
    executor, _, _ = make_executor()
    executor.control_open_order()
    original = executor._open_order
    assert original is not None and original.order is None
    executor._strategy.current_timestamp = 900

    executor.control_open_order()

    assert executor._entry_stop_reason == "total_age_expired"
    assert executor._open_order is original
    assert executor.status is RunnableStatus.RUNNING
    assert executor.place_order.call_count == 1


def test_missing_receipt_pause_keeps_submitted_identity():
    executor, _, _ = make_executor()
    executor.control_open_order()
    bind_controller(executor, pause=True)

    executor.control_open_order()

    assert executor._entry_stop_reason == "risk_blocked"
    assert executor._open_order is not None
    assert executor.status is RunnableStatus.RUNNING
    assert executor.place_order.call_count == 1


def test_missing_local_handle_is_not_a_no_fill_outcome():
    executor, _, _ = make_executor(strategy_timestamp=900)
    executor._entry_submitted = True
    executor._open_order = None

    executor.control_open_order()

    assert executor._entry_stop_reason == "total_age_expired"
    assert executor._entry_lifecycle["state"] == "reconciliation_required"
    assert executor.status is RunnableStatus.RUNNING
    executor.place_order.assert_not_called()


def test_quote_lookup_error_does_not_skip_pending_deadline():
    executor, _, _ = make_executor(strategy_timestamp=900)
    executor._entry_submitted = True
    executor._open_order = tracked_order(state=OrderState.PENDING_CREATE)
    executor.get_price.side_effect = KeyError("book unavailable")

    executor.control_open_order()

    assert executor._entry_stop_reason == "total_age_expired"
    assert executor._open_order is not None
    assert executor.status is RunnableStatus.RUNNING
    executor.place_order.assert_not_called()


def test_initial_missing_book_does_not_hide_entry_pause():
    executor, _, prices = make_executor()
    bind_controller(executor, pause=True)
    prices[PriceType.BestAsk] = None

    executor.control_open_order()

    assert executor._entry_stop_reason == "risk_blocked"
    executor.place_order.assert_not_called()


def test_book_disappearance_during_control_does_not_report_resting():
    executor, _, prices = make_executor()
    executor._entry_submitted = True
    executor._open_order = tracked_order()
    reads = 0

    def quote(connector, pair, price_type):
        nonlocal reads
        reads += 1
        return prices[price_type] if reads <= 2 else None

    executor.get_price.side_effect = quote
    executor.control_open_order()

    assert executor._entry_lifecycle["state"] == "awaiting_order_update"
    assert executor._entry_stop_reason is None
    executor.place_order.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_source", ["0", "3", "27", "999"])
async def test_explicit_venue_cancel_veto_precedes_local_reprice(cancel_source):
    executor, connector, prices = make_executor()
    request_reprice(executor, prices)
    parent = executor._open_order
    parent.order.current_state = OrderState.CANCELED
    parent.order.cancel_source = cancel_source
    install_terminal_reconciliation(connector, parent)

    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    await finish_reconciliation(executor)
    executor.control_open_order()

    assert executor._entry_stop_reason is not None
    assert executor._entry_replacement_purpose is None
    executor.place_order.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("ask_after", [Decimal("99.90"), None])
async def test_final_child_book_check_defers_without_consuming_intent(ask_after):
    executor, connector, prices = make_executor(bid=Decimal("99.90"))
    executor._open_order = tracked_order(state=OrderState.CANCELED)
    executor._open_order.order.cancel_source = "31"
    executor._entry_submitted = True
    executor.place_order.side_effect = ["child-after-repair"]
    install_terminal_reconciliation(connector, executor._open_order)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    await finish_reconciliation(executor)
    reads = 0

    def quote(connector_name, pair, price_type):
        nonlocal reads
        reads += 1
        if reads <= 4:
            return prices[price_type]
        return Decimal("99.80") if price_type is PriceType.BestBid else ask_after

    executor.get_price.side_effect = quote
    executor.control_open_order()

    executor.place_order.assert_not_called()
    assert executor._entry_stop_reason is None
    assert executor._entry_child_submission_authorized

    executor.get_price.side_effect = lambda connector_name, pair, price_type: prices[price_type]
    executor.control_open_order()

    assert executor.place_order.call_count == 1
    assert executor.place_order.call_args.kwargs["price"] == Decimal("99.90")
    assert executor._open_order.order_id == "child-after-repair"


def _capital_controller():
    provider = MagicMock()
    provider.time.return_value = 1000
    provider.get_price_by_type.return_value = Decimal("100")
    provider.get_available_balance.return_value = Decimal("1000")
    provider.get_trading_rules.return_value = SimpleNamespace(
        min_price_increment=Decimal(".01"), min_order_size=Decimal(".001"),
        min_base_amount_increment=Decimal(".001"), min_notional_size=Decimal("1"), min_order_value=Decimal("1"))
    provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
    config = OKRSIControllerConfig(
        id="controller", connector_name="okx", trading_pair="ETH-USDC",
        use_adaptive_trailing=False, strict_hmm_mode=False,
        usd_per_entry=Decimal("25"), max_total_position_usd=Decimal("100"),
        max_executors_per_side=6,
    )
    result = OKRSIController(config, provider, asyncio.Queue())
    result.processed_data = {
        "timestamp": 1000, "signal": 1, "data_health": {"ready": True},
        "signal_state": {"condition_ok": True, "size_multiplier": 1},
    }
    result._status = RunnableStatus.RUNNING
    return result


def _assert_parent_commitment_held(instance, executor, *, authorized=Decimal("1")):
    info = executor.executor_info
    instance.executors_info = [info]
    outstanding = instance._executor_outstanding_base(info)
    assert outstanding == authorized
    assert instance._get_total_position_value_usd() == authorized * Decimal("100")
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] != "ready"


def _assert_resolved_remainder_released(instance, executor):
    info = executor.executor_info
    instance.executors_info = [info]
    assert instance._executor_outstanding_base(info) == Decimal("0")
    assert instance._get_total_position_value_usd().is_finite()


@pytest.mark.asyncio
async def test_post_only_child_cancel_keeps_parent_commitment_through_replacement():
    executor, connector, prices = make_executor(config=make_config(entry_reprice_cooldown_seconds=30))
    instance = _capital_controller()
    executor.control_open_order()
    _assert_parent_commitment_held(instance, executor)

    executor._strategy.current_timestamp = 1
    parent = tracked_order(state=OrderState.CANCELED)
    parent.order.cancel_source = "31"
    executor._open_order = parent
    install_terminal_reconciliation(connector, parent)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    assert executor.get_custom_info()["entry_acquisition"]["state"] == "reconciliation_pending"
    assert executor._entry_stop_reason is None
    _assert_parent_commitment_held(instance, executor)

    await finish_reconciliation(executor)
    _assert_parent_commitment_held(instance, executor)

    executor.control_open_order()
    assert executor.place_order.call_count == 1
    assert executor._entry_child_submission_authorized is True
    _assert_parent_commitment_held(instance, executor)

    executor._strategy.current_timestamp = 29
    executor.control_open_order()
    executor.place_order.assert_called_once()
    _assert_parent_commitment_held(instance, executor)

    executor._strategy.current_timestamp = 30
    executor.control_open_order()
    assert executor.place_order.call_count == 2
    _assert_parent_commitment_held(instance, executor)


@pytest.mark.asyncio
async def test_bound_downward_reprice_keeps_parent_commitment_through_replacement():
    executor, connector, prices = make_executor()
    instance = _capital_controller()
    request_reprice(executor, prices)
    parent = executor._open_order
    parent.order.current_state = OrderState.CANCELED
    install_terminal_reconciliation(connector, parent)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    assert executor.get_custom_info()["entry_acquisition"]["state"] == "reconciliation_pending"
    _assert_parent_commitment_held(instance, executor)

    await finish_reconciliation(executor)
    _assert_parent_commitment_held(instance, executor)

    executor._strategy.current_timestamp = 20
    executor.control_open_order()
    assert executor.place_order.call_count == 1
    _assert_parent_commitment_held(instance, executor)


@pytest.mark.asyncio
async def test_revocation_during_reconciliation_keeps_commitment_until_authority_ends():
    executor, connector, prices = make_executor()
    instance = _capital_controller()
    request_reprice(executor, prices)
    parent = executor._open_order
    parent.order.current_state = OrderState.CANCELED
    install_terminal_reconciliation(connector, parent)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, "parent"))
    bind_controller(executor, pause=True)
    _assert_parent_commitment_held(instance, executor)

    await finish_reconciliation(executor)
    _assert_parent_commitment_held(instance, executor)

    executor.control_open_order()
    executor.place_order.assert_not_called()
    _assert_resolved_remainder_released(instance, executor)
    assert executor._entry_stop_reason is not None


def test_fee_bearing_fills_keep_finite_outstanding_then_reject_malformed_gross():
    executor, _, _ = make_executor()
    instance = _capital_controller()
    executor._open_order = tracked_order(
        fill="0.4",
        state=OrderState.PARTIALLY_FILLED,
        fee=AddedToCostTradeFee(flat_fees=[TokenAmount("ETH", Decimal("0.04"))]),
    )
    executor._entry_submitted = True
    info = executor.executor_info
    custom = info.custom_info
    receipt = custom["execution_receipt"]
    net = instance._executor_owned_base(info)
    gross = Decimal(str(receipt["entry_gross_base"]))
    acquired = Decimal(str(custom["entry_acquisition"]["cumulative_filled_base"]))
    assert net != gross
    assert acquired == gross
    instance.executors_info = [info]
    assert instance._executor_outstanding_base(info) == Decimal("1") - gross
    assert instance._get_total_position_value_usd().is_finite()

    custom["entry_acquisition"]["cumulative_filled_base"] = "NaN"
    instance.executors_info = [info]
    assert instance._executor_outstanding_base(info) is None
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == "exposure_unavailable"

    custom["entry_acquisition"]["cumulative_filled_base"] = str(gross)
    receipt["entry_gross_base"] = Decimal("0.9")
    instance.executors_info = [info]
    assert instance._executor_outstanding_base(info) is None
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == "exposure_unavailable"


def _held_order(order_id, base, quote, fee="0", trade_type="BUY"):
    payload = {
        "client_order_id": order_id,
        "price": "100",
        "amount": str(base),
        "executed_amount_base": str(base),
        "executed_amount_quote": str(quote),
        "cumulative_fee_paid_quote": str(fee),
        "actual_base_fee": "0",
        "last_update_timestamp": 1000,
    }
    if Decimal(str(base)) > 0:
        payload["order_fills"] = {
            f"fill-{order_id}": {
                "fill_base_amount": str(base),
                "fill_quote_amount": str(quote),
                "fill_price": "100",
            }
        }
    if trade_type is not None:
        payload["trade_type"] = trade_type
    return payload


def _memory_recorder():
    engine = create_engine("sqlite:///:memory:")
    with patch("hummingbot.model.sql_connection_manager.create_engine", return_value=engine):
        manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_name="pr52-hold"
        )
    return MarketsRecorder(
        sql=manager,
        markets=[],
        config_file_path="test.yml",
        strategy_name="test",
        market_data_collection=MarketDataCollectionConfigMap(
            market_data_collection_enabled=False,
            market_data_collection_interval=60,
            market_data_collection_depth=20,
        ),
    )


def _clear_hold_pass(instance):
    if hasattr(instance, "_hold_ledger_pass"):
        delattr(instance, "_hold_ledger_pass")


@pytest.mark.asyncio
async def test_native_recorder_requires_complete_fills_and_survives_disposal():
    instance = okx_controller()
    previous = MarketsRecorder._shared_instance
    recorder = _memory_recorder()
    try:
        buy1 = _held_order("buy-1", "0.1", "10", "0.01")
        buy2 = _held_order("buy-2", "0.3", "30", "0.02")
        canceled = _held_order("buy-0", "0", "0")
        missing_side = dict(buy2)
        missing_side.pop("trade_type")
        info = _hold_done(executor_id="held-ex")
        info.custom_info["held_position_orders"] = [buy1, buy2, canceled]
        instance.executors_info = [info]

        recorder.store_or_update_position_hold_ledger_entries(
            controller_id=instance.config.id,
            executor_id="held-ex",
            connector_name="okx",
            trading_pair="ETH-USDT",
            held_position_orders=[buy1, missing_side, canceled],
        )
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            _clear_hold_pass(instance)
            assert instance._executor_hold_released_to_bag(info) is False

        stale = dict(buy2, executed_amount_base="0.1", executed_amount_quote="10")
        recorder.store_or_update_position_hold_ledger_entries(
            controller_id=instance.config.id,
            executor_id="held-ex",
            connector_name="okx",
            trading_pair="ETH-USDT",
            held_position_orders=[buy1, stale],
        )
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            _clear_hold_pass(instance)
            assert instance._executor_hold_released_to_bag(info) is False

        recorder.store_or_update_position_hold_ledger_entries(
            controller_id=instance.config.id,
            executor_id="held-ex",
            connector_name="okx",
            trading_pair="ETH-USDT",
            held_position_orders=[buy1, buy2],
        )
        rows = recorder.get_position_hold_ledger_entries_by_controller(instance.config.id)
        bag = PositionHold("okx", "ETH-USDT", TradeType.BUY)
        for row in rows:
            if row.executor_id == "held-ex":
                bag.add_order_record(row.to_order_record())
        assert bag.buy_amount_base == Decimal("0.4")
        instance.positions_held = [SimpleNamespace(
            connector_name="okx", trading_pair="ETH-USDT", side=TradeType.BUY,
            amount=Decimal("0.3"), breakeven_price=Decimal("100"), cum_fees_quote=Decimal("0"),
        )]
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            _clear_hold_pass(instance)
            assert instance._executor_hold_released_to_bag(info)
            assert instance._executor_owned_base(info) == Decimal("0")
            assert instance._get_total_position_value_usd() == Decimal("30")

        instance.config.max_total_position_usd = Decimal("1000")
        instance.positions_held = []
        instance.executors_info = [info]
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            _clear_hold_pass(instance)
            assert instance._executor_hold_released_to_bag(info)
            assert instance._executor_owned_base(info) == Decimal("0")
            admitted = instance.can_create_executor(1)
            assert admitted, instance._last_gate_reason
            assert instance._last_gate_reason[TradeType.BUY] != "exposure_unavailable"

        instance.executors_info = []
        instance.positions_held = [SimpleNamespace(
            connector_name="okx", trading_pair="ETH-USDT", side=TradeType.BUY,
            amount=Decimal("0.3"), breakeven_price=Decimal("100"), cum_fees_quote=Decimal("0"),
        )]
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            _clear_hold_pass(instance)
            assert instance._get_total_position_value_usd() == Decimal("30")
            instance.can_create_executor(1)
            assert instance._last_gate_reason[TradeType.BUY] != "exposure_unavailable"
    finally:
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_native_recorder_accepts_exact_payload_after_scaled_column_round_trip():
    instance = okx_controller()
    previous = MarketsRecorder._shared_instance
    recorder = _memory_recorder()
    try:
        raw = _held_order("order-1", "0.02800601", "66.64391778", "0.06664391778")
        info = _hold_done(executor_id="held-ex")
        info.custom_info["held_position_orders"] = [raw]
        instance.executors_info = [info]
        recorder.store_or_update_position_hold_ledger_entries(
            controller_id=instance.config.id,
            executor_id="held-ex",
            connector_name="okx",
            trading_pair="ETH-USDT",
            held_position_orders=[raw],
        )
        rows = recorder.get_position_hold_ledger_entries_by_controller(instance.config.id)
        row = next(item for item in rows if item.client_order_id == "order-1")
        assert row.executed_amount_base != Decimal("0.02800601")
        assert row.executed_amount_quote != Decimal("66.64391778")
        assert row.cumulative_fee_paid_quote != Decimal("0.06664391778")
        assert row.order_payload["executed_amount_quote"] == "66.64391778"
        bag = PositionHold("okx", "ETH-USDT", TradeType.BUY)
        bag.add_order_record(row.to_order_record())
        assert bag.buy_amount_base == Decimal("0.02800601")
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            _clear_hold_pass(instance)
            assert instance._executor_hold_released_to_bag(info) is True
            assert instance._executor_owned_base(info) == Decimal("0")
    finally:
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_native_recorder_rejects_payload_that_disagrees_with_row():
    instance = okx_controller()
    previous = MarketsRecorder._shared_instance
    recorder = _memory_recorder()
    try:
        raw = _held_order("order-1", "0.028006", "66.644", "0.01")
        info = _hold_done(executor_id="held-ex")
        info.custom_info["held_position_orders"] = [raw]
        instance.executors_info = [info]
        recorder.store_or_update_position_hold_ledger_entries(
            controller_id=instance.config.id,
            executor_id="held-ex",
            connector_name="okx",
            trading_pair="ETH-USDT",
            held_position_orders=[raw],
        )
        with recorder.sql_manager.get_new_session() as session:
            row = session.query(PositionHoldLedger).filter_by(client_order_id="order-1").one()
            payload = dict(row.order_payload)
            payload["executed_amount_base"] = "0.9"
            row.order_payload = payload
            session.commit()
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            _clear_hold_pass(instance)
            assert instance._executor_hold_released_to_bag(info) is False

        with recorder.sql_manager.get_new_session() as session:
            row = session.query(PositionHoldLedger).filter_by(client_order_id="order-1").one()
            payload = dict(row.order_payload)
            payload["executed_amount_base"] = "0.028006"
            payload["client_order_id"] = "different-order"
            row.order_payload = payload
            session.commit()
        with patch.object(MarketsRecorder, "get_existing_instance", return_value=recorder):
            _clear_hold_pass(instance)
            assert instance._executor_hold_released_to_bag(info) is False
    finally:
        MarketsRecorder._shared_instance = previous
