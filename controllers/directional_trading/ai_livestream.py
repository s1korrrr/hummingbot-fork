from decimal import Decimal
from typing import List

import pandas_ta as ta  # noqa: F401
from pydantic import Field

from controllers.directional_trading.ai_signal_validation import validate_prediction
from hummingbot.core.data_type.common import TradeType
from hummingbot.remote_iface.mqtt import ExternalTopicFactory
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig


class AILivestreamControllerConfig(DirectionalTradingControllerConfigBase):
    controller_name: str = "ai_livestream"
    long_threshold: float = Field(
        default=0.5, ge=0, le=1, allow_inf_nan=False, json_schema_extra={"is_updatable": True}
    )
    short_threshold: float = Field(
        default=0.5, ge=0, le=1, allow_inf_nan=False, json_schema_extra={"is_updatable": True}
    )
    topic: str = "hbot/predictions"
    external_signal_execution_enabled: bool = False
    signal_max_age_seconds: float = Field(default=30.0, gt=0, allow_inf_nan=False)
    signal_max_future_skew_seconds: float = Field(default=1.0, ge=0, allow_inf_nan=False)
    signal_max_target_pct: float = Field(default=0.1, gt=0, le=1, allow_inf_nan=False)


class AILivestreamController(DirectionalTradingControllerBase):
    def __init__(self, config: AILivestreamControllerConfig, *args, **kwargs):
        self.config = config
        super().__init__(config, *args, **kwargs)
        self._prediction = None
        self._last_signal_timestamp = None
        self._signal_binding = None
        self.processed_data.update(signal=0, features={}, signal_status="awaiting_prediction")
        # Start ML signal listener
        self._init_ml_signal_listener()

    def _init_ml_signal_listener(self):
        """Initialize a listener for ML signals from the MQTT broker"""
        try:
            normalized_pair = self.config.trading_pair.replace("-", "_").lower()
            topic = f"{self.config.topic}/{normalized_pair}/ML_SIGNALS"
            self._ml_signal_listener = ExternalTopicFactory.create_async(
                topic=topic,
                callback=self._handle_ml_signal,
                use_bot_prefix=False,
            )
            self.logger().info("ML signal listener initialized successfully")
        except Exception as e:
            self.logger().error(f"Failed to initialize ML signal listener: {str(e)}")
            self._ml_signal_listener = None

    def _expected_topic(self):
        pair = self.config.trading_pair.replace("-", "_").lower()
        return f"{self.config.topic}/{pair}/ML_SIGNALS"

    def _clear_signal(self, reason):
        self._prediction = None
        self.processed_data.update(signal=0, features={}, signal_status=reason)

    def _handle_ml_signal(self, signal: dict, topic: str):
        """Reject invalid input without retaining the previous new-risk signal."""
        self._clear_signal("invalid_prediction")
        try:
            if topic != self._expected_topic():
                raise ValueError("unexpected prediction topic")
            prediction = validate_prediction(
                signal, now=self.market_data_provider.time(),
                max_age=self.config.signal_max_age_seconds,
                max_future_skew=self.config.signal_max_future_skew_seconds,
                max_target_pct=self.config.signal_max_target_pct,
                last_timestamp=self._last_signal_timestamp,
            )
        except (TypeError, ValueError, OverflowError):
            self.logger().warning("Rejected invalid, stale, or replayed external prediction")
            return
        self._prediction = prediction
        self._last_signal_timestamp = prediction.timestamp
        self._signal_binding = (self.config.connector_name, self._expected_topic())
        self.processed_data["features"] = {
            "timestamp": prediction.timestamp, "probabilities": prediction.probabilities,
            "target_pct": prediction.target_pct,
        }
        self._refresh_signal()

    def _refresh_signal(self):
        prediction = self._prediction
        self.processed_data["signal"] = 0
        if prediction is None:
            return False
        if not prediction.is_fresh(self.market_data_provider.time()) or self._signal_binding != (
            self.config.connector_name, self._expected_topic()
        ):
            self._clear_signal("expired_or_rebound")
            return False
        if not self.config.external_signal_execution_enabled:
            self.processed_data["signal_status"] = "execution_disabled"
            return False
        short, _, long = prediction.probabilities
        short_active = short > self.config.short_threshold
        long_active = long > self.config.long_threshold
        if short_active != long_active:
            self.processed_data["signal"] = -1 if short_active else 1
        self.processed_data["signal_status"] = "validated"
        return self.processed_data["signal"] != 0

    async def update_processed_data(self):
        self._refresh_signal()

    def create_actions_proposal(self):
        if not self._refresh_signal():
            return []
        actions = super().create_actions_proposal()
        if actions:
            self._clear_signal("consumed")
        return actions

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        """
        Get the executor config based on the trade_type, price and amount. This method can be overridden by the
        subclasses if required.
        """
        if not self._refresh_signal():
            raise ValueError("external prediction is not authorized for new risk")
        expected_side = TradeType.BUY if self.processed_data["signal"] > 0 else TradeType.SELL
        if trade_type != expected_side:
            raise ValueError("executor side does not match the validated prediction")
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=self.config.triple_barrier_config.new_instance_with_adjusted_volatility(
                volatility_factor=self.processed_data["features"].get("target_pct", 0.01)),
            leverage=self.config.leverage,
        )

    def to_format_status(self) -> List[str]:
        lines = []
        lines.append(f"External prediction: {self.processed_data.get('signal_status', 'unavailable')}")
        features = self.processed_data.get("features", {})
        lines.append(f"🎯 Signal: {self.processed_data.get('signal', 'N/A')}")
        lines.append(f"🕒 Timestamp: {features.get('timestamp', 'N/A')}")
        lines.append(f"📊 Probabilities: {features.get('probabilities', 'N/A')}")
        lines.append(f"🎯 Target Pct: {features.get('target_pct', 'N/A')}")
        return lines
