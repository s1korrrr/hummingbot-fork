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
    assert OKRSIControllerConfig(id="legacy").episode_enabled is False
    with pytest.raises(ValueError, match="assignment"):
        OKRSIControllerConfig(id="candidate", episode_enabled=True, trade_mode="sell_only")
