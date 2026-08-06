from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from controllers.directional_trading.rsi_v5_core.canonical import canonical_json, stable_hash
from controllers.directional_trading.rsi_v5_core.domain import (
    DataHealth,
    DataHealthStatus,
    ExecutorPhase,
    ExecutorSnapshot,
    MarketSnapshot,
    Side,
    StrategyConfig,
)


def market(**overrides):
    payload = dict(
        event_id="evt-1",
        sequence=1,
        event_time_ms=1_000,
        received_time_ms=1_010,
        connector="hyperliquid_perpetual",
        trading_pair="BTC-USD",
        mark_price=Decimal("100.10"),
        best_bid=Decimal("100.00"),
        best_ask=Decimal("100.20"),
        candle_time_ms=960,
        rsi=Decimal("29.5"),
        signal_score=3,
        raw_buy_reversal=True,
        buy_confirmation_ready=True,
        sell_reversal_confirmed=False,
        condition_ok=True,
        condition_reason="ok",
        data_health=DataHealth(status=DataHealthStatus.READY, age_ms=40, max_age_ms=120_000),
    )
    payload.update(overrides)
    return MarketSnapshot(**payload)


def test_contracts_are_frozen_and_reject_unknown_fields():
    snapshot = market()
    with pytest.raises(ValidationError):
        snapshot.mark_price = Decimal("1")
    with pytest.raises(ValidationError):
        MarketSnapshot(**{**snapshot.model_dump(), "unknown": True})


def test_canonical_json_is_order_independent_and_preserves_decimal_strings():
    left = {"b": Decimal("2.00"), "a": [Decimal("1.20"), "x"]}
    right = {"a": [Decimal("1.20"), "x"], "b": Decimal("2.00")}

    assert canonical_json(left) == canonical_json(right)
    assert canonical_json(left) == '{"a":["1.20","x"],"b":"2.00"}'
    assert stable_hash(left) == stable_hash(right)


def test_canonical_json_requires_timezone_aware_datetimes():
    assert "2026-08-06T12:00:00Z" in canonical_json(
        {"at": datetime(2026, 8, 6, 12, tzinfo=timezone.utc)}
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        canonical_json({"at": datetime(2026, 8, 6, 12)})


def test_closing_executor_retains_filled_exposure_but_has_no_pending_base():
    executor = ExecutorSnapshot(
        executor_id="closing",
        side=Side.BUY,
        phase=ExecutorPhase.SHUTTING_DOWN,
        configured_base=Decimal("1"),
        filled_base=Decimal("0.4"),
        filled_quote=Decimal("40"),
        reference_price=Decimal("100"),
    )

    assert executor.owns_exposure is True
    assert executor.pending_base == Decimal("0")


def test_strategy_config_hash_is_stable_and_secret_free():
    config = StrategyConfig()
    first = config.identity_hash
    second = StrategyConfig.model_validate(config.model_dump()).identity_hash
    assert first == second
    assert len(first) == 64
