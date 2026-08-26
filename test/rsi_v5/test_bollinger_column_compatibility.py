from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pandas as pd

from controllers.directional_trading.rsi_v5 import RSIv5Controller, RSIv5ControllerConfig
from hummingbot.core.data_type.common import PriceType


class BollingerCandleProvider:
    def __init__(self, candles: pd.DataFrame, now: float) -> None:
        self.candles = candles
        self.now = now

    def initialize_rate_sources(self, _sources: list[Any]) -> None:
        return None

    def time(self) -> float:
        return self.now

    def get_candles_df(self, **_kwargs: Any) -> pd.DataFrame:
        return self.candles.copy()

    def get_price_by_type(self, _connector: str, _pair: str, _price_type: PriceType) -> Decimal:
        return Decimal(str(self.candles["close"].iloc[-1]))


def _controller(provider: BollingerCandleProvider) -> RSIv5Controller:
    return RSIv5Controller(
        config=RSIv5ControllerConfig(
            id="rsi-v5-bollinger-compatibility",
            connector_name="binance",
            trading_pair="BNB-USDT",
            interval="1m",
            strict_hmm_mode=False,
            trend_confirmation_enabled=False,
            rsi_length=4,
            bb_length=20,
            bb_std=2.0,
            macd_slow=50,
            min_signal_score=4,
            buy_confirmation_mode="none",
            dynamic_position_sizing=False,
            flash_crash_drop_pct=1.0,
        ),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )


def _raw_candles() -> tuple[pd.DataFrame, float]:
    now = 1_800_000_000.0
    closes = [100.0] * 35 + [100.0, 99.0, 98.0, 97.0, 96.0]
    candles = pd.DataFrame(
        {
            "timestamp": [now - (len(closes) - 1 - index) * 60 for index in range(len(closes))],
            "open": closes,
            "high": [close + 0.2 for close in closes],
            "low": [close - 0.2 for close in closes],
            "close": closes,
            "volume": [100.0] * len(closes),
        }
    )
    return candles, now + 1


def _last_backtest_score(**bollinger_columns: list[float]) -> int:
    candles = pd.DataFrame(
        {
            "timestamp": [1.0, 2.0, 3.0, 4.0],
            "open": [100.0, 99.0, 98.0, 96.0],
            "high": [100.2, 99.2, 98.2, 96.2],
            "low": [99.8, 98.8, 97.8, 95.8],
            "close": [100.0, 99.0, 98.0, 96.0],
            "volume": [100.0] * 4,
            "RSI_4": [30.0, 20.0, 0.0, 0.0],
            **bollinger_columns,
        }
    )
    controller = _controller(BollingerCandleProvider(candles, now=5.0))

    frame = controller._build_backtest_signal_frame(
        candles,
        rsi_buy_eff=40.0,
        rsi_sell_eff=76.0,
    )

    return int(frame["signal_state"].iloc[-1]["signal_score"])


def test_public_update_consumes_installed_pandas_ta_bollinger_suffix() -> None:
    candles, now = _raw_candles()
    controller = _controller(BollingerCandleProvider(candles, now))

    asyncio.run(controller.update_processed_data())

    generated_lower_bands = [
        column for column in controller.processed_data["features"].columns if column.startswith("BBL_20_2.0")
    ]
    generated_middle_bands = [
        column for column in controller.processed_data["features"].columns if column.startswith("BBM_20_2.0")
    ]
    generated_upper_bands = [
        column for column in controller.processed_data["features"].columns if column.startswith("BBU_20_2.0")
    ]
    assert generated_lower_bands == ["BBL_20_2.0_2.0"]
    assert generated_middle_bands == ["BBM_20_2.0_2.0"]
    assert generated_upper_bands == ["BBU_20_2.0_2.0"]
    assert controller.processed_data["indicators"]["bb_lower"] is not None
    assert controller.processed_data["indicators"]["bb_mid"] is not None
    assert controller.processed_data["indicators"]["bb_upper"] is not None
    assert controller.processed_data["signal_state"]["score_bb_touch"] is True
    assert controller.processed_data["signal_state"]["signal_score"] == 3


def test_backtest_score_preserves_historical_exact_bollinger_name() -> None:
    score = _last_backtest_score(
        **{
            "BBL_20_2.0": [97.0] * 4,
            "BBL_20_2.0_2.0": [95.0] * 4,
        }
    )

    assert score == 3


def test_backtest_score_selects_only_finite_bollinger_prefix_match() -> None:
    score = _last_backtest_score(
        **{
            "BBL_20_2.0_1.0": [97.0] * 4,
            "BBL_20_2.0_2.0": [float("nan")] * 4,
        }
    )

    assert score == 3


def test_backtest_score_fails_closed_for_ambiguous_or_missing_bollinger_match() -> None:
    ambiguous = _last_backtest_score(
        **{
            "BBL_20_2.0_1.0": [97.0] * 4,
            "BBL_20_2.0_2.0": [97.0] * 4,
        }
    )
    missing = _last_backtest_score()

    assert ambiguous == 2
    assert missing == 2


def test_backtest_score_ignores_unique_nonnumeric_bollinger_lookalike() -> None:
    score = _last_backtest_score(**{"BBL_20_2.0_SIGNAL": [97.0] * 4})

    assert score == 2


def test_backtest_score_fails_closed_for_infinite_bollinger_values() -> None:
    positive_infinity = _last_backtest_score(**{"BBL_20_2.0_2.0": [float("inf")] * 4})
    negative_infinity = _last_backtest_score(**{"BBL_20_2.0_2.0": [float("-inf")] * 4})

    assert positive_infinity == 2
    assert negative_infinity == 2
