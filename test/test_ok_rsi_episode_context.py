from decimal import Decimal

import pandas as pd
import pytest


def candles():
    return pd.DataFrame([
        dict(timestamp=1_800_000_000 + i * 60, open=100 + i / 1000,
             high=101 + i / 1000, low=99 + i / 1000, close=100 + i / 1000, volume=1)
        for i in range(3601)
    ])


def test_context_ignores_forming_candle_and_uses_prior_high():
    from controllers.directional_trading.ok_rsi_core.context import completed_market_context
    data = candles()
    now = int(data.timestamp.iloc[-1]) + 30
    result = completed_market_context(data, now)
    data.loc[data.index[-1], ["close", "high", "low"]] = [100000, 100000, 0]
    assert result == completed_market_context(data, now)
    assert result["timestamp"] == now - 30
    assert result["anchor"] < Decimal("105")
    assert result["recovery"] is True
    assert result["hourly_recovered"] is True


def test_context_rejects_latest_gap_and_short_history():
    from controllers.directional_trading.ok_rsi_core.context import ContextUnavailable, completed_market_context
    data = candles()
    now = int(data.timestamp.iloc[-1]) + 30
    with pytest.raises(ContextUnavailable):
        completed_market_context(data.drop(index=data.index[-3]), now)
    with pytest.raises(ContextUnavailable):
        completed_market_context(data.tail(100), now)


def test_candidate_configuration_is_opt_in_and_sell_only_assignment_explicit():
    from controllers.directional_trading.ok_rsi import OKRSIControllerConfig
    legacy = OKRSIControllerConfig(id="legacy")
    assert legacy.episode_enabled is False
    assert legacy.episode_budget_quote is None
    assert legacy.episode_sequential_scale_ins is False
    assert legacy.episode_require_scale_in_signal is False
    assert legacy.episode_scale_in_setup_bars == 10
    assert legacy.episode_scale_in_stabilization_bars == 2
    assert legacy.episode_dip_atr_levels == [Decimal("2"), Decimal("3"), Decimal("4"), Decimal("5"), Decimal("6")]
    assert legacy.episode_distribution_atr_levels == [Decimal("4"), Decimal("7"), Decimal("10")]
    assert legacy.episode_trail_atr == Decimal("2")
    assert legacy.episode_hostile_risk_latch is False
    assert legacy.episode_recovery_exit_enabled is False
    assert legacy.episode_adaptive_entry_pricing is False
    assert legacy.episode_adaptive_reserved_entry is False
    assert legacy.episode_adaptive_reference_fallback is False
    assert legacy.episode_thin_scout_release is False
    assert legacy.episode_scout_probe_rebound is False
    assert legacy.episode_scout_probe_fraction == Decimal("0.5")
    assert legacy.episode_structure_held_runner is False
    assert legacy.episode_markup_release_runner is False
    assert legacy.episode_bounded_markup_runner is False
    assert legacy.episode_stage_preserving_runner is False
    assert legacy.episode_next_stage_confirmation is False
    assert legacy.episode_earned_stage_risk_skim is False
    assert legacy.episode_earned_stage_risk_skim_fraction == Decimal("0.1")
    assert legacy.episode_scout_rebound_confirmation is False
    assert legacy.episode_recovery_aware_scout is False
    assert legacy.entry_acquisition_policy == "legacy_single_attempt"
    assert legacy.entry_acquisition_shadow_policy is None
    with pytest.raises(ValueError, match="assignment"):
        OKRSIControllerConfig(id="candidate", episode_enabled=True, trade_mode="sell_only")

    with pytest.raises(ValueError, match="reserved adaptive"):
        OKRSIControllerConfig(id="candidate", episode_adaptive_reserved_entry=True)

    with pytest.raises(ValueError, match="reference fallback"):
        OKRSIControllerConfig(
            id="candidate",
            episode_adaptive_entry_pricing=True,
            episode_adaptive_reference_fallback=True,
        )


def test_candidate_episode_budget_must_be_positive_and_finite():
    from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

    for value in (Decimal("0"), Decimal("-1"), Decimal("NaN")):
        with pytest.raises(ValueError):
            OKRSIControllerConfig(id="candidate", episode_enabled=True, episode_budget_quote=value)


def test_profitable_sell_only_recovery_requires_assigned_cost_basis():
    from controllers.directional_trading.ok_rsi import OKRSIControllerConfig

    with pytest.raises(ValueError, match="cost"):
        OKRSIControllerConfig(
            id="candidate",
            episode_enabled=True,
            episode_recovery_exit_enabled=True,
            trade_mode="sell_only",
            episode_assignment_id="assigned",
            episode_initial_base=Decimal("1"),
            sell_only_if_profitable=False,
        )
