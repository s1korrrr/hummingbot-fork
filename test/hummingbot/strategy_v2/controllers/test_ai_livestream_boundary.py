import asyncio
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from controllers.directional_trading.ai_livestream import AILivestreamController, AILivestreamControllerConfig
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import DirectionalTradingControllerBase


@pytest.fixture
def controller():
    value = object.__new__(AILivestreamController)
    value.config = SimpleNamespace(
        connector_name="hyperliquid", trading_pair="ETH-USDC", topic="hbot/predictions",
        signal_max_age_seconds=30.0, signal_max_future_skew_seconds=1.0,
        signal_max_target_pct=0.1, external_signal_execution_enabled=True,
        long_threshold=0.5, short_threshold=0.5,
    )
    value.processed_data = {}
    value.market_data_provider = SimpleNamespace(time=Mock(return_value=1000.0))
    value._prediction = None
    value._last_signal_timestamp = None
    value._signal_binding = None
    with patch.object(AILivestreamController, "logger", return_value=Mock()):
        yield value


def send(controller, **changes):
    payload = {"timestamp": 1000.0, "probabilities": [0.1, 0.1, 0.8], "target_pct": 0.02}
    payload.update(changes)
    controller._handle_ml_signal(payload, "hbot/predictions/eth_usdc/ML_SIGNALS")
    return payload


def test_external_execution_is_disabled_by_default():
    assert AILivestreamControllerConfig.model_fields["external_signal_execution_enabled"].default is False


def test_no_prediction_and_disabled_execution_never_reach_parent(controller):
    with patch.object(DirectionalTradingControllerBase, "create_actions_proposal") as parent:
        assert controller.create_actions_proposal() == []
        controller.config.external_signal_execution_enabled = False
        send(controller)
        assert controller.processed_data["signal"] == 0
        assert controller.create_actions_proposal() == []
        parent.assert_not_called()


@pytest.mark.parametrize("change", [
    {"timestamp": 0}, {"timestamp": 2000}, {"probabilities": [0.1]},
    {"probabilities": [0.0, 0.0, 2.0]}, {"target_pct": float("nan")},
])
def test_invalid_message_clears_prior_valid_signal(controller, change):
    send(controller)
    assert controller.processed_data["signal"] == 1
    send(controller, **change)
    assert controller.processed_data["signal"] == 0
    assert controller.create_actions_proposal() == []


def test_expiry_is_checked_without_a_new_message(controller):
    send(controller)
    controller.market_data_provider.time.return_value = 1030.0
    asyncio.run(controller.update_processed_data())
    assert controller.processed_data["signal"] == 0
    assert controller.create_actions_proposal() == []


def test_prediction_can_only_propose_one_action_batch(controller):
    send(controller)
    action = object()
    with patch.object(DirectionalTradingControllerBase, "create_actions_proposal",
                      return_value=[action]) as parent:
        assert controller.create_actions_proposal() == [action]
        assert controller.create_actions_proposal() == []
        parent.assert_called_once()
    send(controller)  # Same timestamp cannot restore a consumed prediction.
    assert controller.processed_data["signal"] == 0


def test_wrong_topic_and_rebound_instrument_clear_signal(controller):
    send(controller)
    controller._handle_ml_signal({"timestamp": 1001, "probabilities": [0, 0, 1]}, "wrong")
    assert controller.processed_data["signal"] == 0
    send(controller, timestamp=1001)
    controller.config.trading_pair = "BTC-USDC"
    assert controller.create_actions_proposal() == []


def test_source_payload_mutation_cannot_rewrite_prediction(controller):
    payload = send(controller)
    payload["probabilities"][:] = [1, 0, 0]
    controller._refresh_signal()
    assert controller.processed_data["signal"] == 1


def test_ambiguous_thresholds_abstain(controller):
    controller.config.short_threshold = controller.config.long_threshold = 0.1
    send(controller, probabilities=[0.4, 0.2, 0.4])
    assert controller.create_actions_proposal() == []
