from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from controllers.directional_trading.ok_rsi import OKRSIControllerConfig
from controllers.directional_trading.ok_rsi_core.down_only_entry import DownOnlyMakerParameters
from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import DownOnlyMakerPositionExecutorConfig
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig

MAIN_CONTROLLER_CONFIGS = (
    "ok_rsi_btc.yml",
    "ok_rsi_eth.yml",
    "ok_rsi_sol.yml",
    "ok_rsi_xrp.yml",
    "ok_rsi_bnb.yml",
)


def executor_config(**changes) -> DownOnlyMakerPositionExecutorConfig:
    values = {
        "id": "entry-intent",
        "timestamp": 1_000.0,
        "controller_id": "ok-rsi",
        "connector_name": "okx",
        "trading_pair": "BTC-USDC",
        "side": TradeType.BUY,
        "entry_price": Decimal("100"),
        "amount": Decimal("1"),
        "leverage": 1,
        "triple_barrier_config": TripleBarrierConfig(
            open_order_type=OrderType.LIMIT_MAKER,
            open_order_limit_max_age_seconds=0,
        ),
        "activation_bounds": [Decimal("0.005")],
        "entry_order_policy": "single_attempt",
        "decision_best_bid": Decimal("100"),
        "decision_best_ask": Decimal("100.01"),
        "decision_atr_1m": Decimal("1"),
        "decision_price_step": Decimal("0.01"),
        "maximum_acceptable_price": Decimal("100"),
        "entry_total_max_age_seconds": 300,
        "entry_max_downward_reprices": 3,
        "entry_reprice_cooldown_seconds": 5,
        "entry_reprice_min_ticks": 3,
        "entry_reprice_atr_fraction": Decimal("0.05"),
    }
    values.update(changes)
    return DownOnlyMakerPositionExecutorConfig(**values)


def test_entry_acquisition_defaults_preserve_legacy_runtime() -> None:
    config = OKRSIControllerConfig(id="legacy")
    assert config.entry_acquisition_policy == "legacy_single_attempt"
    assert config.entry_acquisition_shadow_policy is None
    assert config.entry_limit_max_age_seconds == 30


def test_main_controller_yamls_enable_bound_driven_entry_without_thirty_second_lease() -> None:
    config_root = Path(__file__).parents[1] / "conf" / "controllers"
    for filename in MAIN_CONTROLLER_CONFIGS:
        payload = yaml.safe_load((config_root / filename).read_text())
        config = OKRSIControllerConfig(**payload)
        assert config.entry_acquisition_policy == "down_only_maker_v1"
        assert config.entry_total_max_age_seconds == 300
        assert config.entry_activation_bound_pct == Decimal("0.0005")
        assert "entry_limit_max_age_seconds" not in payload
        if filename == "ok_rsi_eth.yml":
            assert payload["id"] == "ok-rsi-eth"
            assert payload["trading_pair"] == "ETH-USDC"
            assert config.rsi_buy_threshold == 31.0
            assert config.entry_activation_bound_up_pct == Decimal("0.001")
            assert config.entry_upward_bound_hysteresis_candles == 2
            assert config.down_only_maker_parameters().activation_bound_up_pct == Decimal("0.001")
            assert config.down_only_maker_parameters().upward_hysteresis_seconds == 120
        else:
            assert "entry_activation_bound_up_pct" not in payload
            assert "entry_upward_bound_hysteresis_candles" not in payload
            assert config.entry_activation_bound_up_pct is None
            assert config.entry_upward_bound_hysteresis_candles == 0
            assert config.down_only_maker_parameters().activation_bound_up_pct is None
            assert config.down_only_maker_parameters().upward_hysteresis_seconds == 0


def test_shadow_candidate_materializes_frozen_parameters() -> None:
    config = OKRSIControllerConfig(id="shadow", entry_acquisition_shadow_policy="down_only_maker_v1")
    assert config.down_only_maker_parameters() == DownOnlyMakerParameters(
        total_max_age_seconds=300,
        max_reprices=3,
        cooldown_seconds=5,
        min_ticks=3,
        atr_fraction=Decimal("0.05"),
        activation_bound_pct=Decimal("0.0005"),
    )


@pytest.mark.parametrize(
    "values,message",
    [
        (
            {
                "entry_acquisition_policy": "down_only_maker_v1",
                "entry_acquisition_shadow_policy": "down_only_maker_v1",
            },
            "mutually exclusive",
        ),
        ({"entry_acquisition_policy": "down_only_maker_v1", "episode_enabled": True}, "episode"),
        ({"entry_acquisition_shadow_policy": "down_only_maker_v1", "episode_enabled": True}, "episode"),
        ({"entry_acquisition_policy": "down_only_maker_v1", "trade_mode": "sell_only"}, "sell_only"),
        ({"entry_total_max_age_seconds": 29}, "entry_total_max_age_seconds"),
        ({"entry_activation_bound_pct": 0}, "entry_activation_bound_pct"),
        ({"entry_activation_bound_pct": 1}, "entry_activation_bound_pct"),
        ({"entry_activation_bound_pct": "NaN"}, "entry_activation_bound_pct"),
        ({"entry_activation_bound_up_pct": 0}, "entry_activation_bound_up_pct"),
        ({"entry_activation_bound_up_pct": 1}, "entry_activation_bound_up_pct"),
        ({"entry_activation_bound_up_pct": "NaN"}, "entry_activation_bound_up_pct"),
        ({"entry_activation_bound_up_pct": "0.0004"}, "entry_activation_bound_up_pct"),
        ({"entry_upward_bound_hysteresis_candles": -1}, "entry_upward_bound_hysteresis_candles"),
        ({"entry_upward_bound_hysteresis_candles": 3}, "entry_upward_bound_hysteresis_candles"),
        ({"entry_total_max_age_seconds": 901}, "entry_total_max_age_seconds"),
        ({"entry_reprice_cooldown_seconds": 0}, "entry_reprice_cooldown_seconds"),
        ({"entry_reprice_cooldown_seconds": 31}, "entry_reprice_cooldown_seconds"),
        ({"entry_max_downward_reprices": 0}, "entry_max_downward_reprices"),
        ({"entry_max_downward_reprices": 11}, "entry_max_downward_reprices"),
        ({"entry_reprice_min_ticks": 0}, "entry_reprice_min_ticks"),
        ({"entry_reprice_min_ticks": 101}, "entry_reprice_min_ticks"),
        ({"entry_reprice_atr_fraction": Decimal("0")}, "entry_reprice_atr_fraction"),
        ({"entry_reprice_atr_fraction": Decimal("1.01")}, "entry_reprice_atr_fraction"),
        ({"entry_acquisition_policy": "chase_anywhere"}, "entry_acquisition_policy"),
        ({"entry_acquisition_shadow_policy": "legacy_single_attempt"}, "entry_acquisition_shadow_policy"),
    ],
)
def test_rejects_invalid_controller_policy_configuration(values, message) -> None:
    with pytest.raises(ValidationError, match=message):
        OKRSIControllerConfig(id="invalid", **values)


@pytest.mark.parametrize("rest", [0, 301])
def test_rejects_rest_lease_outside_bounds(rest) -> None:
    with pytest.raises(ValidationError, match="entry_limit_max_age_seconds"):
        OKRSIControllerConfig(id="invalid", entry_limit_max_age_seconds=rest)


def test_executor_config_retains_exact_frozen_envelope_and_identity() -> None:
    config = executor_config()
    assert config.id == "entry-intent"
    assert config.entry_acquisition_policy == "down_only_maker_v1"
    assert config.decision_best_bid == Decimal("100")
    assert config.activation_bounds == [Decimal("0.005")]
    assert config.triple_barrier_config.open_order_limit_max_age_seconds == 0


@pytest.mark.parametrize(
    "changes,message",
    [
        ({"side": TradeType.SELL}, "BUY"),
        ({"leverage": 2}, "leverage"),
        ({"entry_order_policy": "retry"}, "single_attempt"),
        (
            {"triple_barrier_config": TripleBarrierConfig(open_order_type=OrderType.LIMIT)},
            "LIMIT_MAKER",
        ),
        ({"decision_best_bid": Decimal("100.02")}, "crossed"),
        ({"decision_best_bid": Decimal("100.01")}, "crossed or locked"),
        ({"entry_price": Decimal("100.01")}, "maximum acceptable"),
        ({"decision_atr_1m": Decimal("NaN")}, "decision_atr_1m"),
        ({"activation_bounds": None}, "activation_bounds"),
        ({"activation_bounds": []}, "activation_bounds"),
        ({"activation_bounds": [Decimal("0")]}, "activation bound"),
        ({"activation_bounds": [Decimal("1")]}, "activation bound"),
        ({"triple_barrier_config": TripleBarrierConfig(
            open_order_type=OrderType.LIMIT_MAKER,
            open_order_limit_max_age_seconds=30,
        )}, "zero native open-order age"),
    ],
)
def test_executor_config_rejects_unsafe_envelope(changes, message) -> None:
    with pytest.raises(ValidationError, match=message):
        executor_config(**changes)
