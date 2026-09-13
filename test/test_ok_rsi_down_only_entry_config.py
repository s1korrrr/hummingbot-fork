from decimal import Decimal

import pytest
from pydantic import ValidationError

from controllers.directional_trading.ok_rsi import OKRSIControllerConfig
from controllers.directional_trading.ok_rsi_core.down_only_entry import DownOnlyMakerParameters
from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import DownOnlyMakerPositionExecutorConfig
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig


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
            open_order_limit_max_age_seconds=30,
        ),
        "entry_order_policy": "single_attempt",
        "decision_best_bid": Decimal("100"),
        "decision_best_ask": Decimal("100.01"),
        "decision_atr_1m": Decimal("1"),
        "decision_price_step": Decimal("0.01"),
        "maximum_acceptable_price": Decimal("100"),
        "entry_rest_seconds": 30,
        "entry_total_max_age_seconds": 300,
        "entry_max_downward_reprices": 3,
        "entry_reprice_cooldown_seconds": 5,
        "entry_reprice_min_ticks": 3,
        "entry_reprice_atr_fraction": Decimal("0.05"),
        "entry_max_downward_atr": Decimal("1"),
    }
    values.update(changes)
    return DownOnlyMakerPositionExecutorConfig(**values)


def test_entry_acquisition_defaults_preserve_legacy_runtime() -> None:
    config = OKRSIControllerConfig(id="legacy")
    assert config.entry_acquisition_policy == "legacy_single_attempt"
    assert config.entry_acquisition_shadow_policy is None
    assert config.entry_limit_max_age_seconds == 30


def test_shadow_candidate_materializes_frozen_parameters() -> None:
    config = OKRSIControllerConfig(id="shadow", entry_acquisition_shadow_policy="down_only_maker_v1")
    assert config.down_only_maker_parameters() == DownOnlyMakerParameters(
        rest_seconds=30,
        total_max_age_seconds=300,
        max_reprices=3,
        cooldown_seconds=5,
        min_ticks=3,
        atr_fraction=Decimal("0.05"),
        max_downward_atr=Decimal("1.0"),
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
        ({"entry_total_max_age_seconds": 901}, "entry_total_max_age_seconds"),
        ({"entry_reprice_cooldown_seconds": 0}, "entry_reprice_cooldown_seconds"),
        ({"entry_reprice_cooldown_seconds": 31}, "entry_reprice_cooldown_seconds"),
        ({"entry_max_downward_reprices": 0}, "entry_max_downward_reprices"),
        ({"entry_max_downward_reprices": 11}, "entry_max_downward_reprices"),
        ({"entry_reprice_min_ticks": 0}, "entry_reprice_min_ticks"),
        ({"entry_reprice_min_ticks": 101}, "entry_reprice_min_ticks"),
        ({"entry_reprice_atr_fraction": Decimal("0")}, "entry_reprice_atr_fraction"),
        ({"entry_reprice_atr_fraction": Decimal("1.01")}, "entry_reprice_atr_fraction"),
        ({"entry_max_downward_atr": Decimal("0")}, "entry_max_downward_atr"),
        ({"entry_max_downward_atr": Decimal("10.01")}, "entry_max_downward_atr"),
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


def test_rejects_total_age_shorter_than_rest_lease() -> None:
    with pytest.raises(ValidationError, match="total age"):
        OKRSIControllerConfig(
            id="invalid",
            entry_limit_max_age_seconds=60,
            entry_total_max_age_seconds=30,
        )


def test_executor_config_retains_exact_frozen_envelope_and_identity() -> None:
    config = executor_config()
    assert config.id == "entry-intent"
    assert config.entry_acquisition_policy == "down_only_maker_v1"
    assert config.decision_best_bid == Decimal("100")
    assert config.entry_rest_seconds == 30


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
        ({"entry_price": Decimal("100.01")}, "maximum acceptable"),
        ({"decision_atr_1m": Decimal("NaN")}, "decision_atr_1m"),
        ({"entry_total_max_age_seconds": 30, "entry_rest_seconds": 31}, "total age"),
    ],
)
def test_executor_config_rejects_unsafe_envelope(changes, message) -> None:
    with pytest.raises(ValidationError, match=message):
        executor_config(**changes)
