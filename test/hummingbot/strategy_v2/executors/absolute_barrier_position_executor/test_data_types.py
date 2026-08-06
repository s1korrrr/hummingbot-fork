from decimal import Decimal
from unittest import TestCase

from pydantic import ValidationError

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.absolute_barrier_position_executor.data_types import (
    AbsoluteBarrierConfig,
    AbsoluteBarrierPositionExecutorConfig,
)


class TestAbsoluteBarrierPositionExecutorConfig(TestCase):
    @staticmethod
    def make_config(
        side: TradeType = TradeType.SELL,
        entry: str = "100",
        stop: str = "102",
        take_profit: str = "96",
        **barrier_overrides,
    ) -> AbsoluteBarrierPositionExecutorConfig:
        barrier = {
            "stop_loss_price": Decimal(stop),
            "take_profit_price": Decimal(take_profit),
            "time_limit": 3600,
            "open_order_limit_max_age_seconds": 60,
        }
        barrier.update(barrier_overrides)
        return AbsoluteBarrierPositionExecutorConfig(
            id="test-absolute-barrier",
            timestamp=1,
            controller_id="geoshock:test",
            connector_name="hyperliquid_perpetual_testnet",
            trading_pair="BTC-USD",
            side=side,
            entry_price=Decimal(entry),
            amount=Decimal("1"),
            triple_barrier_config=AbsoluteBarrierConfig(**barrier),
        )

    def test_valid_short_and_long_geometry(self):
        short = self.make_config()
        long = self.make_config(side=TradeType.BUY, stop="98", take_profit="104")
        self.assertEqual("absolute_barrier_position_executor", short.type)
        self.assertEqual(1, short.leverage)
        self.assertEqual(Decimal("102"), short.triple_barrier_config.stop_loss_price)
        self.assertEqual(Decimal("104"), long.triple_barrier_config.take_profit_price)

    def test_invalid_short_geometry_fails_closed(self):
        with self.assertRaisesRegex(ValidationError, "geometry"):
            self.make_config(stop="99")
        with self.assertRaisesRegex(ValidationError, "geometry"):
            self.make_config(take_profit="101")

    def test_invalid_long_geometry_fails_closed(self):
        with self.assertRaisesRegex(ValidationError, "geometry"):
            self.make_config(side=TradeType.BUY, stop="101", take_profit="104")
        with self.assertRaisesRegex(ValidationError, "geometry"):
            self.make_config(side=TradeType.BUY, stop="98", take_profit="99")

    def test_non_positive_or_non_finite_barriers_are_rejected(self):
        with self.assertRaises(ValidationError):
            self.make_config(take_profit_price=Decimal("0"))
        with self.assertRaises(ValidationError):
            self.make_config(stop_loss_price=Decimal("NaN"))

    def test_only_limit_entry_and_market_exits_are_supported(self):
        with self.assertRaises(ValidationError):
            self.make_config(open_order_type=OrderType.MARKET)
        with self.assertRaises(ValidationError):
            self.make_config(take_profit_order_type=OrderType.LIMIT)
        with self.assertRaises(ValidationError):
            self.make_config(stop_loss_order_type=OrderType.LIMIT)
        with self.assertRaises(ValidationError):
            self.make_config(time_limit_order_type=OrderType.LIMIT)

    def test_time_and_entry_age_are_bounded(self):
        with self.assertRaises(ValidationError):
            self.make_config(time_limit=None)
        with self.assertRaises(ValidationError):
            self.make_config(open_order_limit_max_age_seconds=0)
        with self.assertRaises(ValidationError):
            self.make_config(open_order_limit_max_age_seconds=301)

    def test_activation_bounds_are_forbidden(self):
        payload = self.make_config().model_dump()
        payload["activation_bounds"] = [Decimal("0.01"), Decimal("0.01")]
        with self.assertRaises(ValidationError):
            AbsoluteBarrierPositionExecutorConfig.model_validate(payload)

    def test_volatility_adjustment_never_mutates_absolute_prices(self):
        barrier = self.make_config().triple_barrier_config
        adjusted = barrier.new_instance_with_adjusted_volatility(3.0)
        self.assertEqual(barrier, adjusted)
        self.assertIsNot(barrier, adjusted)

    def test_round_trip_preserves_absolute_prices(self):
        config = self.make_config()
        restored = AbsoluteBarrierPositionExecutorConfig.model_validate(config.model_dump())
        self.assertEqual(config, restored)

    def test_unknown_fields_are_rejected(self):
        with self.assertRaises(ValidationError):
            AbsoluteBarrierConfig(
                stop_loss_price=Decimal("102"),
                take_profit_price=Decimal("96"),
                time_limit=3600,
                open_order_limit_max_age_seconds=60,
                unknown=True,
            )
