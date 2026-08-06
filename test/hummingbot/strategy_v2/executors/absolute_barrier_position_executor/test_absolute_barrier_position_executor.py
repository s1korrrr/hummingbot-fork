from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.absolute_barrier_position_executor.absolute_barrier_position_executor import (
    AbsoluteBarrierPositionExecutor,
)
from hummingbot.strategy_v2.executors.absolute_barrier_position_executor.data_types import (
    AbsoluteBarrierConfig,
    AbsoluteBarrierPositionExecutorConfig,
)
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.executors import CloseType


class TestAbsoluteBarrierPositionExecutor(TestCase):
    @staticmethod
    def make_config(
        side: TradeType = TradeType.SELL,
        entry: str = "100",
        stop: str = "102",
        take_profit: str = "96",
    ) -> AbsoluteBarrierPositionExecutorConfig:
        return AbsoluteBarrierPositionExecutorConfig(
            id="test-absolute-barrier",
            timestamp=1,
            controller_id="geoshock:test",
            connector_name="hyperliquid_perpetual_testnet",
            trading_pair="BTC-USD",
            side=side,
            entry_price=Decimal(entry),
            entry_deadline=datetime.fromtimestamp(61, tz=UTC),
            amount=Decimal("1"),
            triple_barrier_config=AbsoluteBarrierConfig(
                stop_loss_price=Decimal(stop),
                take_profit_price=Decimal(take_profit),
                time_limit=3600,
                open_order_limit_max_age_seconds=60,
            ),
        )

    @classmethod
    def make_executor(
        cls,
        side: TradeType = TradeType.SELL,
        entry: str = "100",
        stop: str = "102",
        take_profit: str = "96",
        tick: str = "0.5",
    ) -> AbsoluteBarrierPositionExecutor:
        executor = object.__new__(AbsoluteBarrierPositionExecutor)
        executor.config = cls.make_config(side=side, entry=entry, stop=stop, take_profit=take_profit)
        executor.trading_rules = SimpleNamespace(
            min_price_increment=Decimal(tick),
            min_order_size=Decimal("0.1"),
            min_notional_size=Decimal("1"),
        )
        executor._strategy = SimpleNamespace(current_timestamp=1)
        executor._open_order = MagicMock()
        executor._entry_remainder_cancel_pending = False
        executor._entry_remainder_cancelled = False
        return executor

    def test_constructor_keeps_absolute_config_type(self):
        config = self.make_config()
        with patch.object(PositionExecutor, "__init__", return_value=None), patch.object(
            AbsoluteBarrierPositionExecutor, "_validate_tick_aligned_prices"
        ) as validate:
            executor = AbsoluteBarrierPositionExecutor(MagicMock(), config)
        self.assertIs(config, executor.config)
        self.assertFalse(executor._entry_remainder_cancel_pending)
        self.assertFalse(executor._entry_remainder_cancelled)
        validate.assert_called_once_with()

    def test_tick_alignment_fails_closed(self):
        self.make_executor()._validate_tick_aligned_prices()
        with self.assertRaisesRegex(ValueError, "entry_price"):
            self.make_executor(entry="100.1")._validate_tick_aligned_prices()
        with self.assertRaisesRegex(ValueError, "stop_loss_price"):
            self.make_executor(stop="102.1")._validate_tick_aligned_prices()
        with self.assertRaisesRegex(ValueError, "take_profit_price"):
            self.make_executor(take_profit="96.1")._validate_tick_aligned_prices()
        with self.assertRaisesRegex(ValueError, "minimum price increment"):
            self.make_executor(tick="0")._validate_tick_aligned_prices()

    def test_partial_fill_above_market_minimum_is_protectable(self):
        executor = self.make_executor()
        with patch.object(
            AbsoluteBarrierPositionExecutor,
            "open_filled_amount",
            new_callable=PropertyMock,
            return_value=Decimal("0.2"),
        ), patch.object(
            AbsoluteBarrierPositionExecutor,
            "open_filled_amount_quote",
            new_callable=PropertyMock,
            return_value=Decimal("20"),
        ):
            self.assertTrue(executor._has_protectable_exposure())

    def test_dust_partial_fill_is_not_marked_protectable(self):
        executor = self.make_executor()
        with patch.object(
            AbsoluteBarrierPositionExecutor,
            "open_filled_amount",
            new_callable=PropertyMock,
            return_value=Decimal("0.01"),
        ), patch.object(
            AbsoluteBarrierPositionExecutor,
            "open_filled_amount_quote",
            new_callable=PropertyMock,
            return_value=Decimal("1"),
        ):
            self.assertFalse(executor._has_protectable_exposure())

    def test_short_stop_uses_exact_absolute_price(self):
        executor = self.make_executor()
        executor._has_protectable_exposure = MagicMock(return_value=True)
        executor._validated_market_price = MagicMock(return_value=Decimal("102"))
        executor.place_close_order_and_cancel_open_orders = MagicMock()
        executor.control_time_limit = MagicMock()
        executor.control_barriers()
        executor.place_close_order_and_cancel_open_orders.assert_called_once_with(close_type=CloseType.STOP_LOSS)
        executor.control_time_limit.assert_not_called()

    def test_short_take_profit_uses_exact_absolute_price(self):
        executor = self.make_executor()
        executor._has_protectable_exposure = MagicMock(return_value=True)
        executor._validated_market_price = MagicMock(return_value=Decimal("96"))
        executor.place_close_order_and_cancel_open_orders = MagicMock()
        executor.control_time_limit = MagicMock()
        executor.control_barriers()
        executor.place_close_order_and_cancel_open_orders.assert_called_once_with(close_type=CloseType.TAKE_PROFIT)
        executor.control_time_limit.assert_not_called()

    def test_long_absolute_barriers_have_correct_direction(self):
        executor = self.make_executor(side=TradeType.BUY, stop="98", take_profit="104")
        self.assertTrue(executor._absolute_stop_triggered(Decimal("98")))
        self.assertFalse(executor._absolute_stop_triggered(Decimal("99")))
        self.assertTrue(executor._absolute_take_profit_triggered(Decimal("104")))
        self.assertFalse(executor._absolute_take_profit_triggered(Decimal("103")))

    def test_fill_price_never_rebases_barriers(self):
        executor = self.make_executor()
        with patch.object(
            AbsoluteBarrierPositionExecutor,
            "entry_price",
            new_callable=PropertyMock,
            return_value=Decimal("99.25"),
        ):
            self.assertEqual(Decimal("102"), executor.stop_loss_price)
            self.assertEqual(Decimal("96"), executor.take_profit_price)

    def test_no_trigger_or_invalid_price_runs_time_limit_control(self):
        executor = self.make_executor()
        executor._has_protectable_exposure = MagicMock(return_value=True)
        executor.place_close_order_and_cancel_open_orders = MagicMock()
        executor.control_time_limit = MagicMock()
        executor._validated_market_price = MagicMock(return_value=Decimal("99"))
        executor.control_barriers()
        executor._validated_market_price = MagicMock(return_value=None)
        executor.control_barriers()
        executor.place_close_order_and_cancel_open_orders.assert_not_called()
        self.assertEqual(2, executor.control_time_limit.call_count)

    def test_custom_info_exposes_exact_barriers(self):
        executor = self.make_executor()
        with patch.object(PositionExecutor, "get_custom_info", return_value={"existing": True}):
            custom_info = executor.get_custom_info()
        self.assertEqual("ABSOLUTE", custom_info["barrier_mode"])
        self.assertEqual(Decimal("102"), custom_info["absolute_stop_loss_price"])
        self.assertEqual(Decimal("96"), custom_info["absolute_take_profit_price"])
        self.assertEqual("1970-01-01T00:01:01Z", custom_info["entry_deadline"])
        self.assertTrue(custom_info["existing"])
