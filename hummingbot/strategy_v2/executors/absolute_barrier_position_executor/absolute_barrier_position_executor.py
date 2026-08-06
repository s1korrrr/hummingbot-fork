from __future__ import annotations

from decimal import Decimal
from typing import Dict

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.absolute_barrier_position_executor.data_types import (
    AbsoluteBarrierPositionExecutorConfig,
)
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.executors import CloseType


class AbsoluteBarrierPositionExecutor(PositionExecutor):
    def __init__(
        self,
        strategy,
        config: AbsoluteBarrierPositionExecutorConfig,
        update_interval: float = 1.0,
        max_retries: int = 10,
    ):
        super().__init__(strategy=strategy, config=config, update_interval=update_interval, max_retries=max_retries)
        self.config: AbsoluteBarrierPositionExecutorConfig = config
        self._validate_tick_aligned_prices()

    @property
    def stop_loss_price(self) -> Decimal:
        return self.config.triple_barrier_config.stop_loss_price

    @property
    def take_profit_price(self) -> Decimal:
        return self.config.triple_barrier_config.take_profit_price

    @property
    def barrier_mode(self) -> str:
        return "ABSOLUTE"

    def _minimum_price_increment(self) -> Decimal:
        try:
            increment = Decimal(str(self.trading_rules.min_price_increment))
        except Exception as exc:
            raise ValueError("absolute barrier executor requires a valid minimum price increment") from exc
        if not increment.is_finite() or increment <= Decimal("0") or increment < Decimal("1e-18"):
            raise ValueError("absolute barrier executor requires a valid minimum price increment")
        return increment

    def _validate_tick_aligned_prices(self) -> None:
        increment = self._minimum_price_increment()
        for field_name, price in (
            ("entry_price", self.config.entry_price),
            ("stop_loss_price", self.stop_loss_price),
            ("take_profit_price", self.take_profit_price),
        ):
            if price % increment != Decimal("0"):
                raise ValueError(f"{field_name}={price} is not aligned to minimum price increment {increment}")

    def _has_protectable_exposure(self) -> bool:
        if self._open_order is None or self.open_filled_amount <= Decimal("0"):
            return False
        return (
            self.open_filled_amount >= self.trading_rules.min_order_size
            and self.open_filled_amount_quote >= self.trading_rules.min_notional_size
        )

    def _absolute_stop_triggered(self, market_price: Decimal) -> bool:
        if self.config.side == TradeType.BUY:
            return market_price <= self.stop_loss_price
        return market_price >= self.stop_loss_price

    def _absolute_take_profit_triggered(self, market_price: Decimal) -> bool:
        if self.config.side == TradeType.BUY:
            return market_price >= self.take_profit_price
        return market_price <= self.take_profit_price

    def _validated_market_price(self) -> Decimal | None:
        try:
            market_price = Decimal(str(self.current_market_price))
        except Exception:
            return None
        if not market_price.is_finite() or market_price <= Decimal("0"):
            return None
        return market_price

    def control_barriers(self):
        if self._has_protectable_exposure():
            market_price = self._validated_market_price()
            if market_price is not None:
                if self._absolute_stop_triggered(market_price):
                    self.logger().info(
                        self._log_message(
                            f"Absolute stop loss triggered market_price={market_price} stop_price={self.stop_loss_price}"
                        )
                    )
                    self.place_close_order_and_cancel_open_orders(close_type=CloseType.STOP_LOSS)
                    return
                if self._absolute_take_profit_triggered(market_price):
                    self.logger().info(
                        self._log_message(
                            "Absolute take profit triggered "
                            f"market_price={market_price} take_profit_price={self.take_profit_price}"
                        )
                    )
                    self.place_close_order_and_cancel_open_orders(close_type=CloseType.TAKE_PROFIT)
                    return
        self.control_time_limit()

    def control_stop_loss(self):
        if not self._has_protectable_exposure():
            return False
        market_price = self._validated_market_price()
        if market_price is not None and self._absolute_stop_triggered(market_price):
            self.place_close_order_and_cancel_open_orders(close_type=CloseType.STOP_LOSS)
            return True
        return False

    def control_take_profit(self):
        if not self._has_protectable_exposure():
            return False
        market_price = self._validated_market_price()
        if market_price is not None and self._absolute_take_profit_triggered(market_price):
            self.place_close_order_and_cancel_open_orders(close_type=CloseType.TAKE_PROFIT)
            return True
        return False

    def get_custom_info(self) -> Dict:
        custom_info = super().get_custom_info()
        custom_info.update(
            {
                "barrier_mode": self.barrier_mode,
                "absolute_stop_loss_price": self.stop_loss_price,
                "absolute_take_profit_price": self.take_profit_price,
            }
        )
        return custom_info
