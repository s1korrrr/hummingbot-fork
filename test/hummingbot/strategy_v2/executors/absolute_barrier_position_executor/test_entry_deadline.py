from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

from pydantic import ValidationError

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.event.events import OrderCancelledEvent
from hummingbot.strategy_v2.executors.absolute_barrier_position_executor.absolute_barrier_position_executor import AbsoluteBarrierPositionExecutor
from hummingbot.strategy_v2.executors.absolute_barrier_position_executor.data_types import AbsoluteBarrierConfig, AbsoluteBarrierPositionExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import TrackedOrder


class TestAbsoluteBarrierEntryDeadline(TestCase):
    @staticmethod
    def make_config(entry_deadline: datetime | None = None) -> AbsoluteBarrierPositionExecutorConfig:
        return AbsoluteBarrierPositionExecutorConfig(
            id="test-absolute-deadline",
            timestamp=100.0,
            controller_id="geoshock:test-absolute-deadline",
            connector_name="hyperliquid_perpetual_testnet",
            trading_pair="BTC-USD",
            side=TradeType.SELL,
            entry_price=Decimal("100"),
            entry_deadline=entry_deadline or datetime.fromtimestamp(110, tz=UTC),
            amount=Decimal("1"),
            triple_barrier_config=AbsoluteBarrierConfig(
                stop_loss_price=Decimal("102"),
                take_profit_price=Decimal("96"),
                time_limit=3600,
                open_order_limit_max_age_seconds=60,
            ),
        )

    @classmethod
    def make_executor(cls, current_timestamp: float = 111.0) -> AbsoluteBarrierPositionExecutor:
        executor = object.__new__(AbsoluteBarrierPositionExecutor)
        executor.config = cls.make_config()
        executor._strategy = SimpleNamespace(current_timestamp=current_timestamp, cancel=MagicMock())
        executor._open_order = None
        executor._close_order = None
        executor._take_profit_limit_order = None
        executor._completed_close_orders = []
        executor._failed_orders = []
        executor._entry_remainder_cancel_pending = False
        executor._entry_remainder_cancelled = False
        executor.close_type = None
        executor.close_timestamp = None
        executor._status = RunnableStatus.RUNNING
        return executor

    def test_config_requires_timezone_aware_utc_deadline_after_creation(self) -> None:
        config = self.make_config()
        self.assertEqual(datetime.fromtimestamp(110, tz=UTC), config.entry_deadline)
        with self.assertRaises(ValidationError):
            self.make_config(entry_deadline=datetime(2026, 8, 6, 10, 0, 0))
        with self.assertRaises(ValidationError):
            self.make_config(entry_deadline=datetime.fromtimestamp(110, tz=timezone(timedelta(hours=2))))
        with self.assertRaises(ValidationError):
            self.make_config(entry_deadline=datetime.fromtimestamp(99, tz=UTC))

    def test_no_fill_at_deadline_cancels_entry_and_expires(self) -> None:
        executor = self.make_executor()
        open_order = TrackedOrder("OID-ENTRY")
        open_order.order = MagicMock(is_open=True, is_filled=False, creation_timestamp=100.0)
        executor._open_order = open_order
        executor.cancel_open_order = MagicMock()
        executor._is_within_activation_bounds = MagicMock(return_value=True)
        with patch.object(AbsoluteBarrierPositionExecutor, "open_filled_amount", new_callable=PropertyMock, return_value=Decimal("0")):
            executor.control_open_order()
        executor.cancel_open_order.assert_called_once_with()
        self.assertEqual(RunnableStatus.SHUTTING_DOWN, executor.status)
        self.assertEqual("EXPIRED", executor.close_type.name)

    def test_partial_fill_at_deadline_cancels_only_remainder(self) -> None:
        executor = self.make_executor()
        open_order = TrackedOrder("OID-ENTRY")
        open_order.order = MagicMock(is_open=True, is_filled=False, creation_timestamp=100.0)
        executor._open_order = open_order
        executor.cancel_open_order = MagicMock()
        executor._is_within_activation_bounds = MagicMock(return_value=True)
        with patch.object(AbsoluteBarrierPositionExecutor, "open_filled_amount", new_callable=PropertyMock, return_value=Decimal("0.25")):
            executor.control_open_order()
        executor.cancel_open_order.assert_called_once_with()
        self.assertEqual(RunnableStatus.RUNNING, executor.status)
        self.assertTrue(executor._entry_remainder_cancel_pending)
        self.assertIs(open_order, executor._open_order)

    def test_cancel_event_retains_partially_filled_order(self) -> None:
        executor = self.make_executor()
        open_order = TrackedOrder("OID-ENTRY")
        open_order.order = MagicMock(is_open=False, is_filled=False, creation_timestamp=100.0)
        executor._open_order = open_order
        executor._entry_remainder_cancel_pending = True
        with patch.object(AbsoluteBarrierPositionExecutor, "open_filled_amount", new_callable=PropertyMock, return_value=Decimal("0.25")):
            executor.process_order_canceled_event(0, MagicMock(), OrderCancelledEvent(timestamp=111.0, order_id="OID-ENTRY"))
        self.assertIs(open_order, executor._open_order)
        self.assertFalse(executor._entry_remainder_cancel_pending)
        self.assertTrue(executor._entry_remainder_cancelled)

    def test_custom_info_contains_signed_deadline(self) -> None:
        executor = self.make_executor()
        executor._entry_remainder_cancelled = True
        with patch("hummingbot.strategy_v2.executors.position_executor.position_executor.PositionExecutor.get_custom_info", return_value={}):
            custom_info = executor.get_custom_info()
        self.assertEqual("1970-01-01T00:01:50Z", custom_info["entry_deadline"])
        self.assertTrue(custom_info["entry_remainder_cancelled"])
