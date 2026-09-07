# --- controllers/directional_trading/hl_rsi.py ---
"""
HL RSI Controller - Hyperliquid spot RSI v5 strategy.

Core Strategy:
- BUY when multiple indicators agree: RSI oversold + Bollinger Band lower touch + MACD turning positive
- SELL when RSI overbought AND position is profitable (or held bag recovery target reached)
- Trailing take profit as primary exit mechanism
- Bag Freeze: when all executor slots are underwater, freeze positions and start fresh DCA cycle

Key Features:
- Multi-indicator signal scoring (RSI + BB + MACD)
- Bag Freeze system: detect DCA traps, freeze via POSITION_HOLD, recover at breakeven+target
- Flash crash protection: block entries during cascading drops
- ATR-adaptive cooldown and entry gap spacing
- Regime-aware RSI threshold adjustments (HMM-based)
- No time limit on executors - hold until trailing stop or bag freeze
"""
import logging
import time as _time
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from typing import Dict, List, Literal, Optional, Tuple

import pandas as pd
from pydantic import Field as PydanticField, field_validator, model_validator
from pydantic_core.core_schema import ValidationInfo

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.order_executor.data_types import (
    ExecutionStrategy,
    LimitChaserConfig,
    OrderExecutorConfig,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.utils.common import parse_enum_value
from hummingbot.strategy_v2.utils.market_analysis import (
    MarketRegime,
    MarketRegimeDetector,
    MultiTimeframeTrend,
    StrictHMMRequiredError,
    TrendDirection,
)

from .pandas_compat import pandas_copy_on_write_warning_category
from .rsi_signals import (
    analyze_rsi_trend_confirmation,
    calculate_rsi_mean_reversion_score,
    calculate_signal_strength,
    calculate_volume_ratio,
    get_adaptive_trailing_params,
)

_PANDAS_COPY_ON_WRITE_WARNING = pandas_copy_on_write_warning_category(pd.errors)
with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        category=_PANDAS_COPY_ON_WRITE_WARNING,
        message=".*mode.copy_on_write.*",
    )
    import pandas_ta as ta  # noqa: F401

# ---------------------------------------------------------------------------
# Pydantic Field helper (same as rsi_v1 for prompt/client_data compat)
# ---------------------------------------------------------------------------
_CLIENT_FIELD_SCHEMA_KEYS = ("prompt", "prompt_on_new", "is_secure", "is_connect_key", "is_updatable")
_DECIMAL_CONVERSION_ERRORS = (ArithmeticError, TypeError, ValueError)
_DECIMAL_ATTRIBUTE_ERRORS = (AttributeError, ArithmeticError, TypeError, ValueError)
_MARKET_DATA_ERRORS = (AttributeError, KeyError, RuntimeError, TypeError, ValueError, ArithmeticError)
_PROVIDER_ERRORS = (AttributeError, KeyError, RuntimeError, TypeError, ValueError)
_INDICATOR_ERRORS = (AttributeError, KeyError, IndexError, RuntimeError, TypeError, ValueError, ArithmeticError)
_STATUS_FORMAT_ERRORS = (AttributeError, KeyError, OSError, TypeError, ValueError, ArithmeticError)


def _extract_client_field_value(client_data: Optional[ClientFieldData], key: str):
    if client_data is None:
        return None
    if hasattr(client_data, key):
        return getattr(client_data, key)
    kwargs = getattr(client_data, "kwargs", None)
    if isinstance(kwargs, dict):
        return kwargs.get(key)
    return None


def _flatten_client_field_data(client_data: Optional[ClientFieldData]) -> Dict[str, object]:
    flattened: Dict[str, object] = {}
    for key in _CLIENT_FIELD_SCHEMA_KEYS:
        value = _extract_client_field_value(client_data, key)
        if value is not None:
            flattened[key] = value
    return flattened


def Field(*args, client_data: Optional[ClientFieldData] = None, json_schema_extra: Optional[Dict[str, object]] = None, **kwargs):
    schema_extra = dict(json_schema_extra or {})
    if client_data is not None:
        schema_extra.update(_flatten_client_field_data(client_data))
    if schema_extra:
        kwargs["json_schema_extra"] = schema_extra
    return PydanticField(*args, **kwargs)


# ---------------------------------------------------------------------------
# SignalState dataclass
# ---------------------------------------------------------------------------


@dataclass
class SignalState:
    """Compact signal state for rsi_v5."""

    timestamp: float
    close: Optional[float]
    rsi: Optional[float]
    rsi_prev: Optional[float]
    atr: Optional[float]
    atr_pct: Optional[float]
    ema_fast: Optional[float]
    ema_slow: Optional[float]
    regime_label: Optional[str]
    regime_confidence: float
    bb_lower: Optional[float]
    bb_upper: Optional[float]
    bb_mid: Optional[float]
    macd_hist: Optional[float]
    macd_hist_prev: Optional[float]
    signal_score: int
    condition_ok: bool
    condition_reason: str
    size_multiplier: float
    consecutive_losses: int = 0
    volume_ratio: Optional[float] = None
    mean_reversion_score: float = 0.0
    signal_strength_score: float = 0.0
    rsi_reversal: bool = False
    raw_buy_candidate: bool = False
    raw_reversal_prev_was_min: bool = False
    raw_reversal_turning_up: bool = False
    raw_reversal_was_oversold: bool = False
    raw_reversal_near_bottom: bool = False
    score_rsi_oversold: bool = False
    score_bb_touch: bool = False
    score_macd_turn: bool = False
    score_mean_reversion: bool = False
    buy_decision: str = "idle"
    buy_reason: str = "none"
    buy_confirmation_active: bool = False
    buy_confirmation_rebound_delta: Optional[float] = None
    buy_confirmation_rebound_target: Optional[float] = None
    buy_confirmation_price_rebounded: Optional[bool] = None
    buy_entry_role: str = "none"
    buy_entry_fraction: float = 0.0
    buy_context_bias: str = "neutral"
    sell_decision: str = "idle"
    sell_reason: str = "none"
    sell_has_inventory: bool = False
    sell_pnl_pct: Optional[float] = None
    sell_profitability_ok: bool = False
    sell_rsi_rollover: bool = False
    sell_price_below_ema: bool = False
    sell_macd_rollover: bool = False
    sell_reversal_confirmed: bool = False
    sell_trend_hold: bool = False

    def as_dict(self) -> Dict[str, object]:
        return {f.name: getattr(self, f.name) for f in self.__dataclass_fields__.values()}


@dataclass
class BuyConfirmationSetup:
    armed_timestamp: float
    trough_rsi: float
    trough_price: float
    armed_signal_score: int = 0
    armed_min_signal_score: int = 0

    def score_qualified(self) -> bool:
        if self.armed_min_signal_score <= 0:
            return True
        return self.armed_signal_score >= self.armed_min_signal_score


@dataclass
class RecoveryTrailState:
    position_key: str
    armed_timestamp: float
    arm_price: Decimal
    peak_price: Decimal
    tracked_amount: Decimal
    peak_rsi: Optional[float]
    target_profit_pct: Decimal
    last_reason: str = "armed"
    partial_exit_done: bool = False

    def as_dict(self) -> Dict[str, object]:
        return {
            "position_key": self.position_key,
            "armed_timestamp": self.armed_timestamp,
            "arm_price": float(self.arm_price),
            "peak_price": float(self.peak_price),
            "tracked_amount": float(self.tracked_amount),
            "peak_rsi": self.peak_rsi,
            "target_profit_pct": float(self.target_profit_pct),
            "last_reason": self.last_reason,
            "partial_exit_done": self.partial_exit_done,
        }


@dataclass
class AggregatedInventoryState:
    positions: List[object]
    total_amount: Decimal
    bag_count: int
    cost_basis: Optional[Decimal]
    cum_fees_quote: Decimal = Decimal("0")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class HLRSIControllerConfig(DirectionalTradingControllerConfigBase):
    controller_name: str = "hl_rsi"
    controller_type: str = "directional_trading"
    connector_name: str = Field(default="hyperliquid", client_data=ClientFieldData(prompt=lambda mi: "Connector:", prompt_on_new=True, is_updatable=False))
    trading_pair: str = Field(default="HYPE-USDC", client_data=ClientFieldData(prompt=lambda mi: "Trading pair:", prompt_on_new=True, is_updatable=False))
    trade_mode: Literal["buy_sell", "sell_only"] = Field(
        default="buy_sell",
        client_data=ClientFieldData(
            prompt=lambda mi: "Trade mode (buy_sell/sell_only):",
            prompt_on_new=True,
            is_updatable=False,
        ),
    )
    leverage: int = 1
    position_mode: PositionMode = PositionMode.ONEWAY

    # Override parent defaults — V5 uses trailing stop only, no SL/TP/TL
    max_executors_per_side: int = Field(default=6, json_schema_extra={"prompt": "Max executors per side:", "prompt_on_new": True, "is_updatable": True})
    stop_loss: Optional[Decimal] = Field(default=None, json_schema_extra={"prompt": "Stop loss (None=disabled):", "prompt_on_new": False, "is_updatable": True})
    take_profit: Optional[Decimal] = Field(default=None, json_schema_extra={"prompt": "Take profit (None=disabled):", "prompt_on_new": False, "is_updatable": True})
    time_limit: Optional[int] = Field(default=None, json_schema_extra={"prompt": "Time limit (None=disabled):", "prompt_on_new": False, "is_updatable": True})

    candles_connector: str = Field(default=None, client_data=ClientFieldData(prompt=lambda mi: "Candles connector:", prompt_on_new=False, is_updatable=False))
    candles_trading_pair: str = Field(default=None, client_data=ClientFieldData(prompt=lambda mi: "Candles trading pair:", prompt_on_new=False, is_updatable=False))
    candles_config: List[CandlesConfig] = []
    interval: str = Field(default="1m", client_data=ClientFieldData(prompt=lambda mi: "Candle interval:", prompt_on_new=True, is_updatable=False))
    max_candle_age_intervals: float = Field(default=2.5, gt=0.0, client_data=ClientFieldData(prompt=lambda mi: "Max candle age in intervals:", prompt_on_new=False, is_updatable=True))
    strict_hmm_mode: bool = Field(default=True, client_data=ClientFieldData(prompt=lambda mi: "Require real HMM regime detector?:", prompt_on_new=False, is_updatable=False))
    trend_confirmation_enabled: bool = Field(default=True, client_data=ClientFieldData(prompt=lambda mi: "Use multi-timeframe trend confirmation?:", prompt_on_new=False, is_updatable=True))
    trend_confirmation_medium_interval: Optional[str] = Field(default="5m", client_data=ClientFieldData(prompt=lambda mi: "Trend medium interval:", prompt_on_new=False, is_updatable=True))
    trend_confirmation_long_interval: Optional[str] = Field(default="15m", client_data=ClientFieldData(prompt=lambda mi: "Trend long interval:", prompt_on_new=False, is_updatable=True))
    trend_confirmation_adx_threshold: float = Field(default=18.0, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "Trend confirmation ADX threshold:", prompt_on_new=False, is_updatable=True))
    sell_trend_hold_min_confidence: float = Field(default=0.20, ge=0.0, le=1.0, client_data=ClientFieldData(prompt=lambda mi: "Sell trend-hold confidence:", prompt_on_new=False, is_updatable=True))
    trend_sell_threshold_boost: float = Field(default=4.0, ge=0.0, le=10.0, client_data=ClientFieldData(prompt=lambda mi: "HTF uptrend sell-threshold boost:", prompt_on_new=False, is_updatable=True))
    sell_rsi_rollover_delta: float = Field(default=0.5, ge=0.0, le=10.0, client_data=ClientFieldData(prompt=lambda mi: "Sell RSI rollover delta:", prompt_on_new=False, is_updatable=True))
    recovery_trail_pullback_pct: float = Field(default=0.0035, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "Recovery trailing pullback pct:", prompt_on_new=False, is_updatable=True))
    recovery_rsi_rollover_delta: float = Field(default=2.0, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "Recovery RSI rollover delta:", prompt_on_new=False, is_updatable=True))
    recovery_cancel_stale_order_pct: float = Field(default=0.0025, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "Recovery stale-order cancel pct:", prompt_on_new=False, is_updatable=True))
    recovery_sell_execution_strategy: ExecutionStrategy = Field(default=ExecutionStrategy.LIMIT_CHASER, client_data=ClientFieldData(prompt=lambda mi: "Recovery sell execution strategy (LIMIT/LIMIT_MAKER/LIMIT_CHASER/MARKET):", prompt_on_new=False, is_updatable=True))
    recovery_limit_chaser_distance_pct: Decimal = Field(default=Decimal("0.002"), gt=Decimal("0"), client_data=ClientFieldData(prompt=lambda mi: "Recovery limit chaser distance pct:", prompt_on_new=False, is_updatable=True))
    recovery_limit_chaser_refresh_threshold_pct: Decimal = Field(default=Decimal("0.003"), gt=Decimal("0"), client_data=ClientFieldData(prompt=lambda mi: "Recovery limit chaser refresh threshold pct:", prompt_on_new=False, is_updatable=True))
    recovery_partial_exit_fraction: float = Field(default=0.5, gt=0.0, le=1.0, client_data=ClientFieldData(prompt=lambda mi: "Recovery partial exit fraction:", prompt_on_new=False, is_updatable=True))

    # --- Core indicators ---
    rsi_length: int = Field(default=10, ge=2, client_data=ClientFieldData(prompt=lambda mi: "RSI length:", is_updatable=False))
    atr_length: int = Field(default=14, ge=2, client_data=ClientFieldData(prompt=lambda mi: "ATR length:", is_updatable=False))
    ema_fast_length: int = Field(default=9, ge=2, client_data=ClientFieldData(prompt=lambda mi: "EMA fast:", prompt_on_new=False, is_updatable=False))
    ema_slow_length: int = Field(default=21, ge=2, client_data=ClientFieldData(prompt=lambda mi: "EMA slow:", prompt_on_new=False, is_updatable=False))
    rsi_buy_threshold: float = Field(default=31.0, gt=0.0, lt=100.0, client_data=ClientFieldData(prompt=lambda mi: "RSI buy threshold:", prompt_on_new=False, is_updatable=True))
    rsi_sell_threshold: float = Field(default=75.0, gt=0.0, lt=100.0, client_data=ClientFieldData(prompt=lambda mi: "RSI sell threshold:", prompt_on_new=False, is_updatable=True))

    # --- Bollinger Bands ---
    bb_length: int = Field(default=20, ge=5, client_data=ClientFieldData(prompt=lambda mi: "BB length:", prompt_on_new=False, is_updatable=False))
    bb_std: float = Field(default=2.0, ge=0.5, client_data=ClientFieldData(prompt=lambda mi: "BB std:", prompt_on_new=False, is_updatable=True))

    # --- MACD ---
    macd_fast: int = Field(default=12, ge=2, client_data=ClientFieldData(prompt=lambda mi: "MACD fast:", prompt_on_new=False, is_updatable=False))
    macd_slow: int = Field(default=26, ge=5, client_data=ClientFieldData(prompt=lambda mi: "MACD slow:", prompt_on_new=False, is_updatable=False))
    macd_signal: int = Field(default=9, ge=2, client_data=ClientFieldData(prompt=lambda mi: "MACD signal:", prompt_on_new=False, is_updatable=False))

    # --- Multi-indicator signal scoring ---
    min_signal_score: int = Field(default=2, ge=1, le=4, client_data=ClientFieldData(prompt=lambda mi: "Min indicators that must agree (1-4):", prompt_on_new=False, is_updatable=True))
    dca_score_boost: int = Field(default=1, ge=0, le=2, client_data=ClientFieldData(prompt=lambda mi: "Extra score required when DCA-ing into loss:", prompt_on_new=False, is_updatable=True))
    buy_confirmation_mode: str = Field(default="none", client_data=ClientFieldData(prompt=lambda mi: "BUY confirmation mode (none/rebound_confirm):", prompt_on_new=False, is_updatable=True))
    buy_confirmation_rsi_delta: float = Field(default=1.0, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "BUY confirmation RSI rebound delta:", prompt_on_new=False, is_updatable=True))
    buy_confirmation_max_wait_seconds: int = Field(default=75, ge=0, client_data=ClientFieldData(prompt=lambda mi: "BUY confirmation max wait seconds (0=no timeout):", prompt_on_new=False, is_updatable=True))
    use_split_entries: bool = Field(default=False, client_data=ClientFieldData(prompt=lambda mi: "Enable scout/runner BUY entries?:", prompt_on_new=False, is_updatable=True))
    scout_entry_fraction: float = Field(default=0.35, gt=0.0, lt=1.0, client_data=ClientFieldData(prompt=lambda mi: "Scout BUY fraction of usd_per_entry:", prompt_on_new=False, is_updatable=True))
    hostile_trend_scout_fraction: float = Field(default=0.15, ge=0.0, le=1.0, client_data=ClientFieldData(prompt=lambda mi: "Scout fraction in hostile trend:", prompt_on_new=False, is_updatable=True))

    # --- Selling rules ---
    sell_only_if_profitable: bool = Field(default=True, client_data=ClientFieldData(prompt=lambda mi: "Only sell if profitable?:", prompt_on_new=False, is_updatable=True))
    sell_rsi_overbought: float = Field(default=76.0, ge=50.0, le=100.0, client_data=ClientFieldData(prompt=lambda mi: "RSI overbought trigger for sell:", prompt_on_new=False, is_updatable=True))
    min_profit_pct_for_sell: Decimal = Field(default=Decimal("0.002"), client_data=ClientFieldData(prompt=lambda mi: "Min profit pct for sell:", prompt_on_new=False, is_updatable=True))
    signal_sell_inventory_slice_multiplier: Decimal = Field(default=Decimal("0"), ge=Decimal("0"), client_data=ClientFieldData(prompt=lambda mi: "Signal SELL inventory slice multiplier (0=all):", prompt_on_new=False, is_updatable=True))
    recent_trades_lookback: int = Field(default=15, client_data=ClientFieldData(prompt=lambda mi: "Recent trades lookback:", prompt_on_new=False, is_updatable=False))

    # --- Position sizing & risk limits ---
    usd_per_entry: Decimal = Field(default=Decimal("30"), gt=Decimal("0"), client_data=ClientFieldData(prompt=lambda mi: "USD per entry:", prompt_on_new=True, is_updatable=True))
    max_total_position_usd: Decimal = Field(default=Decimal("500"), gt=Decimal("0"), client_data=ClientFieldData(prompt=lambda mi: "Max total position USD:", prompt_on_new=True, is_updatable=True))

    # --- Trailing take profit ---
    use_trailing_exit: bool = Field(default=True, client_data=ClientFieldData(prompt=lambda mi: "Use trailing exit?:", prompt_on_new=False, is_updatable=True))
    trailing_activation_pct: float = Field(default=0.047, client_data=ClientFieldData(prompt=lambda mi: "Trailing activation pct:", prompt_on_new=False, is_updatable=True))
    trailing_delta_pct: float = Field(default=0.034, client_data=ClientFieldData(prompt=lambda mi: "Trailing delta pct:", prompt_on_new=False, is_updatable=True))
    trailing_stop_order_type: OrderType = Field(default=OrderType.LIMIT, client_data=ClientFieldData(prompt=lambda mi: "Trailing close order type (LIMIT/MARKET):", prompt_on_new=False, is_updatable=True))
    trailing_stop_limit_max_age_seconds: int = Field(default=3, ge=0, client_data=ClientFieldData(prompt=lambda mi: "Trailing limit fallback seconds (0=disabled):", prompt_on_new=False, is_updatable=True))
    use_adaptive_trailing: bool = Field(default=False, client_data=ClientFieldData(prompt=lambda mi: "Volatility-adaptive trailing?:", prompt_on_new=False, is_updatable=True))
    trailing_atr_baseline: float = Field(default=0.002, ge=0.0001, client_data=ClientFieldData(prompt=lambda mi: "Trailing ATR baseline:", prompt_on_new=False, is_updatable=True))

    # --- Take profit (optional fixed target, 0 = disabled) ---
    take_profit_pct: float = Field(default=0.0, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "Take profit pct (0=disabled):", prompt_on_new=False, is_updatable=True))

    # --- Max hold time (0 = no limit) ---
    max_hold_time_seconds: int = Field(default=0, ge=0, client_data=ClientFieldData(prompt=lambda mi: "Max hold time seconds (0=disabled):", prompt_on_new=False, is_updatable=True))

    # --- Entry spacing (DCA ladder) ---
    executor_price_gap_threshold: Decimal = Field(default=Decimal("0.0045"), client_data=ClientFieldData(prompt=lambda mi: "Min price gap between entries:", prompt_on_new=False, is_updatable=True))
    sell_executor_price_gap_threshold: Decimal = Field(default=Decimal("0.01"), ge=Decimal("0"), client_data=ClientFieldData(prompt=lambda mi: "Min price gap between sell executors:", prompt_on_new=False, is_updatable=True))
    cooldown_time: int = Field(default=90, ge=15, client_data=ClientFieldData(prompt=lambda mi: "Base cooldown seconds:", prompt_on_new=False, is_updatable=True))

    # --- Adaptive controls ---
    use_adaptive_filters: bool = Field(default=False, client_data=ClientFieldData(prompt=lambda mi: "Enable adaptive parameters?:", prompt_on_new=False, is_updatable=True))
    min_cooldown_time: int = Field(default=45, ge=0, client_data=ClientFieldData(prompt=lambda mi: "Min cooldown (high vol):", prompt_on_new=False, is_updatable=True))
    cooldown_atr_low_pct: float = Field(default=0.0012, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "ATR low threshold:", prompt_on_new=False, is_updatable=True))
    cooldown_atr_high_pct: float = Field(default=0.0045, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "ATR high threshold:", prompt_on_new=False, is_updatable=True))
    min_executor_price_gap_threshold: Decimal = Field(default=Decimal("0.005"), client_data=ClientFieldData(prompt=lambda mi: "Min gap:", prompt_on_new=False, is_updatable=True))
    max_executor_price_gap_threshold: Decimal = Field(default=Decimal("0.02"), client_data=ClientFieldData(prompt=lambda mi: "Max gap:", prompt_on_new=False, is_updatable=True))
    price_gap_atr_multiplier: float = Field(default=3.0, client_data=ClientFieldData(prompt=lambda mi: "ATR multiplier for gap:", prompt_on_new=False, is_updatable=True))

    # --- Market condition filters ---
    min_atr_pct_to_trade: float = Field(default=0.0002, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "Min ATR pct:", prompt_on_new=False, is_updatable=True))
    max_atr_pct_to_trade: float = Field(default=0.02, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "Max ATR pct:", prompt_on_new=False, is_updatable=True))

    # --- Bag Freeze ---
    use_bag_freeze: bool = Field(default=True, client_data=ClientFieldData(prompt=lambda mi: "Enable bag freeze?:", prompt_on_new=False, is_updatable=True))
    bag_freeze_distance_pct: float = Field(default=0.04, ge=0.005, client_data=ClientFieldData(prompt=lambda mi: "Bag freeze distance pct:", prompt_on_new=False, is_updatable=True))
    bag_freeze_min_age_seconds: int = Field(default=600, ge=60, client_data=ClientFieldData(prompt=lambda mi: "Bag freeze min age seconds:", prompt_on_new=False, is_updatable=True))
    bag_freeze_require_full_slots: bool = Field(default=True, client_data=ClientFieldData(prompt=lambda mi: "Require full slots for freeze?:", prompt_on_new=False, is_updatable=True))

    # --- Bag Recovery ---
    bag_recovery_target_pct: float = Field(default=0.003, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "Bag recovery target pct:", prompt_on_new=False, is_updatable=True))
    bag_max_concurrent_holds: int = Field(default=2, ge=1, client_data=ClientFieldData(prompt=lambda mi: "Max concurrent held bags:", prompt_on_new=False, is_updatable=True))

    # --- Flash Crash Protection ---
    flash_crash_lookback: int = Field(default=5, ge=2, client_data=ClientFieldData(prompt=lambda mi: "Flash crash lookback candles:", prompt_on_new=False, is_updatable=True))
    flash_crash_drop_pct: float = Field(default=0.03, ge=0.005, client_data=ClientFieldData(prompt=lambda mi: "Flash crash drop pct:", prompt_on_new=False, is_updatable=True))
    flash_crash_cooldown: int = Field(default=300, ge=30, client_data=ClientFieldData(prompt=lambda mi: "Flash crash cooldown seconds:", prompt_on_new=False, is_updatable=True))

    # --- Dynamic position sizing ---
    dynamic_position_sizing: bool = Field(default=True, client_data=ClientFieldData(prompt=lambda mi: "Dynamic sizing?:", prompt_on_new=False, is_updatable=True))
    min_position_size_multiplier: float = Field(default=0.5, ge=0.1, client_data=ClientFieldData(prompt=lambda mi: "Min size multiplier:", prompt_on_new=False, is_updatable=True))
    max_position_size_multiplier: float = Field(default=1.5, ge=0.5, client_data=ClientFieldData(prompt=lambda mi: "Max size multiplier:", prompt_on_new=False, is_updatable=True))

    # --- Early stop ---
    early_stop_drawdown_pct: float = Field(default=0.08, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "Early stop drawdown pct:", prompt_on_new=False, is_updatable=True))
    early_stop_keep_position: bool = Field(default=True, client_data=ClientFieldData(prompt=lambda mi: "Keep position on early stop? (True=hold position):", prompt_on_new=False, is_updatable=True))

    # --- Consecutive loss protection ---
    max_consecutive_losses: int = Field(default=5, ge=0, client_data=ClientFieldData(prompt=lambda mi: "Max consecutive losses before pause (0=disabled):", prompt_on_new=False, is_updatable=True))
    consecutive_loss_cooldown: int = Field(default=3600, ge=0, client_data=ClientFieldData(prompt=lambda mi: "Cooldown seconds after max losses:", prompt_on_new=False, is_updatable=True))

    @field_validator("connector_name", mode="before")
    @classmethod
    def validate_hyperliquid_spot_connector(cls, value):
        connector_name = str(value or "hyperliquid").strip().lower()
        if connector_name not in {"hyperliquid", "hyperliquid_paper_trade"}:
            raise ValueError("HL RSI requires a Hyperliquid spot connector")
        return connector_name

    @field_validator("candles_connector", mode="before")
    @classmethod
    def set_candles_connector(cls, v, validation_info: ValidationInfo):
        if v is None or v == "":
            return validation_info.data.get("connector_name")
        return v

    @field_validator("candles_trading_pair", mode="before")
    @classmethod
    def set_candles_trading_pair(cls, v, validation_info: ValidationInfo):
        if v is None or v == "":
            return validation_info.data.get("trading_pair")
        return v

    @field_validator("buy_confirmation_mode", mode="before")
    @classmethod
    def validate_buy_confirmation_mode(cls, v):
        if v is None:
            return "none"
        normalized = str(v).strip().lower()
        allowed = {"none", "rebound_confirm"}
        if normalized not in allowed:
            raise ValueError(f"buy_confirmation_mode must be one of {sorted(allowed)}")
        return normalized

    @field_validator("trend_confirmation_medium_interval", "trend_confirmation_long_interval", mode="before")
    @classmethod
    def normalize_optional_interval(cls, v):
        if v is None:
            return None
        normalized = str(v).strip()
        return normalized or None

    @field_validator("trailing_stop_order_type", mode="before")
    @classmethod
    def validate_trailing_stop_order_type(cls, v) -> OrderType:
        if v is None:
            return OrderType.LIMIT
        if isinstance(v, str):
            v = v.replace("OrderType.", "")
        parsed = parse_enum_value(OrderType, v, "trailing_stop_order_type")
        if parsed == OrderType.LIMIT_MAKER:
            raise ValueError("trailing_stop_order_type must be LIMIT or MARKET")
        return parsed

    @field_validator("recovery_sell_execution_strategy", mode="before")
    @classmethod
    def validate_recovery_sell_execution_strategy(cls, v) -> ExecutionStrategy:
        if v is None:
            return ExecutionStrategy.LIMIT_CHASER
        if isinstance(v, str):
            v = v.replace("ExecutionStrategy.", "")
        return parse_enum_value(ExecutionStrategy, v, "recovery_sell_execution_strategy")

    @model_validator(mode="after")
    def harmonize_sell_threshold_fields(self):
        default_rsi_sell = float(type(self).model_fields["rsi_sell_threshold"].default)
        default_sell_overbought = float(type(self).model_fields["sell_rsi_overbought"].default)
        rsi_sell = float(self.rsi_sell_threshold)
        sell_overbought = float(self.sell_rsi_overbought)

        rsi_sell_custom = abs(rsi_sell - default_rsi_sell) > 1e-9
        sell_overbought_custom = abs(sell_overbought - default_sell_overbought) > 1e-9

        if not rsi_sell_custom and not sell_overbought_custom:
            object.__setattr__(self, "rsi_sell_threshold", default_sell_overbought)
            object.__setattr__(self, "sell_rsi_overbought", default_sell_overbought)
        elif sell_overbought_custom and not rsi_sell_custom:
            object.__setattr__(self, "rsi_sell_threshold", sell_overbought)
        elif rsi_sell_custom and not sell_overbought_custom:
            object.__setattr__(self, "sell_rsi_overbought", rsi_sell)
        elif sell_overbought_custom and rsi_sell_custom and abs(rsi_sell - sell_overbought) > 1e-9:
            object.__setattr__(self, "rsi_sell_threshold", sell_overbought)
        else:
            object.__setattr__(self, "sell_rsi_overbought", rsi_sell)

        if float(self.rsi_buy_threshold) >= float(self.sell_rsi_overbought):
            raise ValueError("rsi_buy_threshold must be below the effective RSI sell threshold")
        if self.usd_per_entry > self.max_total_position_usd:
            raise ValueError("usd_per_entry must be less than or equal to max_total_position_usd")
        if float(self.min_position_size_multiplier) > float(self.max_position_size_multiplier):
            raise ValueError("min_position_size_multiplier must be less than or equal to max_position_size_multiplier")

        return self

# ---------------------------------------------------------------------------
# Controller
# ---------------------------------------------------------------------------


class HLRSIController(DirectionalTradingControllerBase):
    """
    All-Weather DCA Controller.

    Entry: Multi-indicator score (RSI + BB + MACD + mean reversion) >= min_signal_score
    Exit: Trailing stop (primary) | RSI overbought sell | Held bag recovery sell
    Risk: Bag freeze on DCA trap | Flash crash protection | max_total_position_usd cap
    """

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
            cls._logger.setLevel(logging.INFO)
        return cls._logger

    def __init__(self, config: HLRSIControllerConfig, *args, **kwargs):
        self.config = config

        self._latest_atr_pct: Optional[float] = None
        self._current_gap_threshold: Decimal = self.config.executor_price_gap_threshold
        self._current_cooldown: int = self.config.cooldown_time
        self._last_gate_reason: Dict[TradeType, Optional[str]] = {TradeType.BUY: None, TradeType.SELL: None}
        self._last_signal_log_signature: Dict[str, Optional[str]] = {}
        self._last_processed_timestamp: Optional[float] = None
        self._last_evaluated_candle_timestamp: Optional[float] = None
        self._last_buy_signal_candle_ts: Optional[float] = None
        self._last_buy_signal_role: Optional[str] = None
        self._pending_buy_confirmation: Optional[BuyConfirmationSetup] = None
        self._recovery_trails: Dict[str, RecoveryTrailState] = {}
        self._flash_crash_until: float = 0.0
        self._bag_freeze_count: int = 0

        self._cached_cost_basis: Optional[Decimal] = None
        self._cost_basis_cache_ts: Optional[float] = None
        self._consecutive_losses: int = 0
        self._last_loss_timestamp: Optional[float] = None
        self._tracked_executor_result_ids: set = set()
        self._operator_entry_paused: bool = False
        self._operator_pause_reason: Optional[str] = None
        self._operator_pause_scope: Optional[str] = None
        self._operator_pause_updated_at: Optional[float] = None
        self._operator_last_command_id: Optional[str] = None
        # Indicator max_records must cover the slowest indicator
        self.max_records = max(500, config.bb_length * 3, config.macd_slow * 3, config.rsi_length * 5)
        if not getattr(self.config, "candles_config", None) or len(self.config.candles_config) == 0:
            self.config.candles_config = self._build_default_candles_config()

        self._regime_detector: MarketRegimeDetector = MarketRegimeDetector(
            use_hmm=True,
            require_hmm=bool(self.config.strict_hmm_mode),
            allow_rule_based_fallback=False,
            htf_confirm=False,
        )

        super().__init__(config, *args, **kwargs)

    # -----------------------------------------------------------------------
    # Small helpers
    # -----------------------------------------------------------------------

    def _build_default_candles_config(self) -> List[CandlesConfig]:
        intervals = [self.config.interval]
        if self.config.trend_confirmation_enabled:
            for interval in (
                self.config.trend_confirmation_medium_interval,
                self.config.trend_confirmation_long_interval,
            ):
                if interval and interval not in intervals:
                    intervals.append(interval)
        return [
            CandlesConfig(
                connector=self.config.candles_connector or self.config.connector_name,
                trading_pair=self.config.candles_trading_pair or self.config.trading_pair,
                interval=interval,
                max_records=self.max_records,
            )
            for interval in intervals
        ]

    def get_candles_config(self) -> List[CandlesConfig]:
        return self._build_default_candles_config()

    def _filter_same_side(self, side: TradeType, active_only: bool = False):
        return self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x, s=side, a=active_only: (
                (x.is_active if a else True)
                and x.connector_name == self.config.connector_name
                and x.trading_pair == self.config.trading_pair
                and x.side == s
            ),
        )

    def _get_mid_price(self) -> Optional[Decimal]:
        try:
            val = self.market_data_provider.get_price_by_type(
                self.config.connector_name, self.config.trading_pair, PriceType.MidPrice,
            )
            return Decimal(str(val)) if val is not None else None
        except (AttributeError, InvalidOperation, ValueError, TypeError):
            return None

    def _get_interval_candles_df(self, interval: Optional[str]) -> Optional[pd.DataFrame]:
        if not interval:
            return None
        try:
            candles_df = self.market_data_provider.get_candles_df(
                connector_name=self.config.candles_connector or self.config.connector_name,
                trading_pair=self.config.candles_trading_pair or self.config.trading_pair,
                interval=interval,
                max_records=self.max_records,
            )
        except _MARKET_DATA_ERRORS:
            return None
        if candles_df is None or candles_df.empty:
            return None
        return candles_df.copy()

    def set_operator_entry_pause(
        self,
        enabled: bool,
        *,
        reason: Optional[str] = None,
        scope: Optional[str] = None,
        command_id: Optional[str] = None,
        updated_at: Optional[float] = None,
    ) -> None:
        self._operator_entry_paused = enabled
        self._operator_pause_reason = reason
        self._operator_pause_scope = scope
        self._operator_pause_updated_at = updated_at or _time.time()
        self._operator_last_command_id = command_id
        log_fn = self.logger().warning if enabled else self.logger().info
        log_fn(
            f"Operator entry pause {'enabled' if enabled else 'disabled'} | "
            f"pair={self.config.trading_pair} scope={scope or 'n/a'} reason={reason or 'n/a'}"
        )

    def get_operator_state(self) -> dict:
        return {
            "entry_paused": self._operator_entry_paused,
            "reason": self._operator_pause_reason,
            "scope": self._operator_pause_scope,
            "updated_at": self._operator_pause_updated_at,
            "last_command_id": self._operator_last_command_id,
        }

    def get_custom_info(self) -> dict:
        mid_price = self._get_mid_price()
        return {
            "pair": self.config.trading_pair,
            "signal": (self.processed_data or {}).get("signal", 0),
            "mid_price": float(mid_price) if mid_price is not None else None,
            "buy_gate": self._last_gate_reason.get(TradeType.BUY),
            "sell_gate": self._last_gate_reason.get(TradeType.SELL),
            "operator": self.get_operator_state(),
        }

    def _split_entries_enabled(self) -> bool:
        return bool(
            getattr(self.config, "use_split_entries", False)
            and self.config.buy_confirmation_mode == "rebound_confirm"
            and 0.0 < float(self.config.scout_entry_fraction) < 1.0
        )

    @staticmethod
    def _executor_level_id(executor) -> Optional[str]:
        custom_info = getattr(executor, "custom_info", None) or {}
        level_id = custom_info.get("level_id") or custom_info.get("role") or getattr(executor, "level_id", None)
        if level_id in (None, ""):
            return None
        return str(level_id)

    def _has_active_buy_leg(self, level_id: str) -> bool:
        return any(
            self._executor_level_id(executor) == level_id
            for executor in self._filter_same_side(TradeType.BUY, active_only=True)
        )

    def _classify_buy_context(
        self,
        *,
        regime_label: Optional[str],
        trend_confirmation: Optional[MultiTimeframeTrend],
    ) -> str:
        if trend_confirmation is not None:
            confidence = float(trend_confirmation.confidence or 0.0)
            if (
                trend_confirmation.direction == TrendDirection.DOWN
                and confidence >= float(self.config.sell_trend_hold_min_confidence)
            ):
                return "hostile"
            if (
                trend_confirmation.direction == TrendDirection.UP
                and confidence >= float(self.config.sell_trend_hold_min_confidence)
            ):
                return "supportive"

        if regime_label:
            if "Trend_Down" in regime_label:
                return "hostile"
            if "Range" in regime_label or "Trend_Up" in regime_label:
                return "supportive"
        return "neutral"

    def _effective_scout_fraction(self, context_bias: Optional[str]) -> Decimal:
        scout_fraction = Decimal(str(self.config.scout_entry_fraction))
        hostile_fraction = Decimal(str(self.config.hostile_trend_scout_fraction))
        if context_bias == "hostile":
            scout_fraction = min(scout_fraction, hostile_fraction)
        return max(Decimal("0"), min(Decimal("1"), scout_fraction))

    def _is_flat_for_new_buy_entry(self) -> bool:
        return not self._has_buy_inventory() and not self._filter_same_side(TradeType.BUY, active_only=True)

    def _planned_buy_entry_fraction(self, state: Optional[Dict[str, object]] = None) -> Decimal:
        state = state or (self.processed_data or {}).get("signal_state", {})
        role = str(state.get("buy_entry_role", "none") or "none").lower()
        context_bias = str(state.get("buy_context_bias", "neutral") or "neutral")
        explicit_fraction = state.get("buy_entry_fraction")
        try:
            if explicit_fraction is not None and Decimal(str(explicit_fraction)) > 0:
                return max(Decimal("0"), min(Decimal("1"), Decimal(str(explicit_fraction))))
        except _DECIMAL_CONVERSION_ERRORS:
            pass
        if role == "scout":
            return self._effective_scout_fraction(context_bias)
        if role == "runner":
            return max(Decimal("0"), Decimal("1") - self._effective_scout_fraction(context_bias))
        if role == "none":
            return Decimal("1")
        return Decimal("1")

    def _effective_buy_entry_usd(
        self,
        state: Optional[Dict[str, object]] = None,
        *,
        entry_fraction: Optional[Decimal] = None,
    ) -> Decimal:
        state = state or (self.processed_data or {}).get("signal_state", {})
        fraction = entry_fraction if entry_fraction is not None else self._planned_buy_entry_fraction(state)
        usd_budget = self.config.usd_per_entry * max(Decimal("0"), min(Decimal("1"), fraction))
        if self.config.dynamic_position_sizing:
            mult = Decimal(str(state.get("size_multiplier", 1.0) or 1.0))
            usd_budget = usd_budget * mult
        return max(usd_budget.quantize(Decimal("1e-8")), Decimal("0"))

    def _planned_buy_entry_usd(self, state: Optional[Dict[str, object]] = None) -> Decimal:
        return self._effective_buy_entry_usd(state)

    def _get_trading_rule(self) -> Optional[TradingRule]:
        try:
            return self.market_data_provider.get_trading_rules(self.config.connector_name, self.config.trading_pair)
        except KeyError:
            self.logger().warning(
                f"Trading rule unavailable | {self._controller_pair_log_prefix()} "
                f"connector={self.config.connector_name}"
            )
        except _PROVIDER_ERRORS as e:
            self.logger().warning(
                f"Trading rule lookup failed | {self._controller_pair_log_prefix()} "
                f"error={type(e).__name__}: {e}"
            )
        return None

    @staticmethod
    def _rule_decimal(rule: Optional[TradingRule], attr_name: str) -> Decimal:
        if rule is None:
            return Decimal("0")
        try:
            value = getattr(rule, attr_name, Decimal("0")) or Decimal("0")
            decimal_value = Decimal(str(value))
            return decimal_value if decimal_value.is_finite() and decimal_value > 0 else Decimal("0")
        except _DECIMAL_ATTRIBUTE_ERRORS:
            return Decimal("0")

    def _quantize_amount(self, amount: Decimal) -> Decimal:
        try:
            return Decimal(
                str(
                    self.market_data_provider.quantize_order_amount(
                        self.config.connector_name, self.config.trading_pair, amount
                    )
                )
            )
        except _MARKET_DATA_ERRORS:
            return amount

    @staticmethod
    def _ceil_to_increment(value: Decimal, increment: Decimal) -> Decimal:
        if increment <= 0:
            return value
        return (value / increment).to_integral_value(rounding=ROUND_CEILING) * increment

    def _base_amount_increment(self, rule: Optional[TradingRule]) -> Decimal:
        increment = self._rule_decimal(rule, "min_base_amount_increment")
        if increment <= 0:
            increment = self._rule_decimal(rule, "min_order_size")
        return increment

    def _minimum_entry_quote(self, price: Decimal, rule: Optional[TradingRule] = None) -> Decimal:
        if price <= 0:
            return Decimal("0")
        rule = rule or self._get_trading_rule()
        min_base = self._rule_decimal(rule, "min_order_size")
        min_notional = self._rule_decimal(rule, "min_notional_size")
        min_order_value = self._rule_decimal(rule, "min_order_value")
        min_quote_from_base = min_base * price if min_base > 0 else Decimal("0")
        return max(min_quote_from_base, min_notional, min_order_value)

    def _minimum_entry_amount(self, price: Decimal, rule: Optional[TradingRule] = None) -> Decimal:
        if price <= 0:
            return Decimal("0")
        rule = rule or self._get_trading_rule()
        min_base = self._rule_decimal(rule, "min_order_size")
        min_quote = self._minimum_entry_quote(price, rule)
        min_amount = max(min_base, (min_quote / price) if min_quote > 0 else Decimal("0"))
        increment = self._base_amount_increment(rule)
        if increment > 0:
            min_amount = self._ceil_to_increment(min_amount, increment)
        return max(min_amount, Decimal("0"))

    def _resolve_recovery_sell_amount(
        self,
        *,
        position_amount: Decimal,
        target_amount: Decimal,
        price: Decimal,
        available_base: Optional[Decimal] = None,
    ) -> Decimal:
        if position_amount <= 0 or target_amount <= 0 or price <= 0:
            return Decimal("0")

        max_sellable = position_amount
        if available_base is not None:
            max_sellable = min(max_sellable, available_base)
        if max_sellable <= 0:
            return Decimal("0")

        rule = self._get_trading_rule()
        min_quote = self._minimum_entry_quote(price, rule)
        min_amount = self._minimum_entry_amount(price, rule)
        if max_sellable < min_amount or (min_quote > 0 and max_sellable * price < min_quote):
            return Decimal("0")

        sell_amount = min(target_amount, max_sellable)
        if sell_amount < min_amount or (min_quote > 0 and sell_amount * price < min_quote):
            sell_amount = min_amount

        remaining_amount = max_sellable - sell_amount
        if remaining_amount > 0 and (
            remaining_amount < min_amount or (min_quote > 0 and remaining_amount * price < min_quote)
        ):
            sell_amount = max_sellable

        sell_amount = min(sell_amount, max_sellable)
        sell_amount = self._quantize_amount(sell_amount)
        if sell_amount > max_sellable:
            sell_amount = max_sellable
        if sell_amount <= 0:
            return Decimal("0")
        if sell_amount < min_amount or (min_quote > 0 and sell_amount * price < min_quote):
            if max_sellable >= min_amount and (min_quote <= 0 or max_sellable * price >= min_quote):
                return max_sellable
            return Decimal("0")
        return sell_amount

    def _resolve_buy_entry_plan(
        self,
        price: Decimal,
        state: Optional[Dict[str, object]] = None,
    ) -> Tuple[Decimal, Decimal, Decimal, Decimal, bool]:
        min_step = Decimal("1e-8")
        state = state or (self.processed_data or {}).get("signal_state", {})
        planned_usd = self._planned_buy_entry_usd(state)
        entry_price = self._shade_limit_maker_price(
            trade_type=TradeType.BUY,
            fallback_price=price,
        )
        if entry_price <= 0:
            entry_price = price if price > 0 else min_step

        rule = self._get_trading_rule()
        min_quote = self._minimum_entry_quote(entry_price, rule)
        effective_usd = max(planned_usd, min_quote)
        min_amount = self._minimum_entry_amount(entry_price, rule)
        sizing_price = price if price > 0 else entry_price
        amount_base = (effective_usd / sizing_price) if sizing_price > 0 else min_step
        amount_base = max(amount_base, min_amount, min_step)
        quantized_amount = self._quantize_amount(amount_base)
        amount_base = quantized_amount if quantized_amount > 0 else amount_base

        if amount_base < min_amount:
            increment = self._base_amount_increment(rule)
            amount_base = self._ceil_to_increment(min_amount, increment) if increment > 0 else min_amount

        if min_quote > 0 and (amount_base * entry_price) < min_quote:
            increment = self._base_amount_increment(rule)
            required_amount = max(min_amount, (min_quote / entry_price))
            amount_base = self._ceil_to_increment(required_amount, increment) if increment > 0 else required_amount

        amount_base = max(amount_base, min_step).quantize(min_step)
        effective_usd = (amount_base * entry_price).quantize(min_step)
        uplifted = effective_usd > planned_usd
        return planned_usd, effective_usd, entry_price, amount_base, uplifted

    def _trend_confirmation_dict(self, trend: Optional[MultiTimeframeTrend]) -> Dict[str, object]:
        if trend is None:
            return {
                "direction": "unavailable",
                "confidence": 0.0,
                "alignment_score": 0.0,
                "states": {},
            }
        return trend.to_dict()

    def _processed_trend_confirmation(self) -> Dict[str, object]:
        trend = (self.processed_data or {}).get("trend_confirmation")
        return trend if isinstance(trend, dict) else {}

    def _apply_trend_confirmation_to_thresholds(
        self,
        *,
        rsi_buy_eff: float,
        rsi_sell_eff: float,
        trend_confirmation: Optional[MultiTimeframeTrend],
    ) -> Tuple[float, float]:
        if not self.config.trend_confirmation_enabled or trend_confirmation is None:
            return rsi_buy_eff, rsi_sell_eff

        confidence = max(0.0, min(1.0, float(trend_confirmation.confidence or 0.0)))
        boost = float(self.config.trend_sell_threshold_boost) * confidence

        if trend_confirmation.direction == TrendDirection.UP:
            rsi_sell_eff = min(88.0, rsi_sell_eff + boost)
        elif trend_confirmation.direction == TrendDirection.DOWN:
            rsi_buy_eff = max(20.0, rsi_buy_eff - (boost * 0.4))
            rsi_sell_eff = max(60.0, rsi_sell_eff - (boost * 0.5))

        return float(rsi_buy_eff), float(rsi_sell_eff)

    def _is_strong_bullish_trend(self) -> bool:
        trend = self._processed_trend_confirmation()
        return bool(
            self.config.trend_confirmation_enabled
            and trend.get("direction") == TrendDirection.UP.value
            and float(trend.get("confidence", 0.0) or 0.0) >= float(self.config.sell_trend_hold_min_confidence)
        )

    @staticmethod
    def _recovery_position_key(position) -> str:
        connector = getattr(position, "connector_name", "unknown")
        pair = getattr(position, "trading_pair", "unknown")
        side = getattr(getattr(position, "side", None), "name", str(getattr(position, "side", "unknown")))
        try:
            breakeven = Decimal(str(getattr(position, "breakeven_price", "0"))).quantize(Decimal("1e-8"))
        except _DECIMAL_CONVERSION_ERRORS:
            breakeven = Decimal("0")
        return f"{connector}|{pair}|{side}|{breakeven}"

    def _build_sell_reversal_state(
        self,
        *,
        last_rsi: Optional[float],
        rsi_prev: Optional[float],
        current_price: Optional[Decimal],
        ema_fast: Optional[float],
        macd_hist: Optional[float],
        macd_hist_prev: Optional[float],
    ) -> Dict[str, bool]:
        rsi_rollover = bool(
            last_rsi is not None
            and rsi_prev is not None
            and (rsi_prev - last_rsi) >= float(self.config.sell_rsi_rollover_delta)
        )
        price_below_ema = bool(
            current_price is not None
            and ema_fast is not None
            and current_price < Decimal(str(ema_fast))
        )
        macd_rollover = bool(
            macd_hist is not None
            and macd_hist_prev is not None
            and macd_hist < macd_hist_prev
        )
        reversal_confirmed = bool((rsi_rollover and price_below_ema) or macd_rollover)
        return {
            "sell_rsi_rollover": rsi_rollover,
            "sell_price_below_ema": price_below_ema,
            "sell_macd_rollover": macd_rollover,
            "sell_reversal_confirmed": reversal_confirmed,
        }

    def _sync_recovery_trails(self):
        active_keys = {
            self._recovery_position_key(position)
            for position in self.positions_held
            if position.connector_name == self.config.connector_name
            and position.trading_pair == self.config.trading_pair
            and position.side == TradeType.BUY
            and position.amount > 0
        }
        for key in list(self._recovery_trails.keys()):
            if key not in active_keys:
                self._recovery_trails.pop(key, None)

    def _recovery_reversal_details(
        self,
        trail: RecoveryTrailState,
        *,
        current_price: Optional[Decimal],
        current_rsi: Optional[float],
        ema_fast: Optional[float],
    ) -> Tuple[bool, str, Dict[str, object]]:
        if current_price is None or current_price <= 0 or trail.peak_price <= 0:
            return False, "no-price", {"pullback_pct": None, "rsi_rollover": False, "price_below_ema": False}

        pullback_pct = max((trail.peak_price - current_price) / trail.peak_price, Decimal("0"))
        pullback_ready = pullback_pct >= Decimal(str(self.config.recovery_trail_pullback_pct))
        rsi_rollover = (
            current_rsi is not None
            and trail.peak_rsi is not None
            and current_rsi <= (trail.peak_rsi - float(self.config.recovery_rsi_rollover_delta))
        )
        price_below_ema = (
            ema_fast is not None
            and current_price < Decimal(str(ema_fast))
        )
        reversal_ready = bool(pullback_ready or (rsi_rollover and price_below_ema))
        reason = "pullback-trail" if pullback_ready else "rsi-rollover" if reversal_ready else "trend-hold"
        return reversal_ready, reason, {
            "pullback_pct": float(pullback_pct),
            "rsi_rollover": rsi_rollover,
            "price_below_ema": price_below_ema,
        }

    def _held_bag_recovery_sell_rsi_threshold(self) -> float:
        return float(self.config.sell_rsi_overbought)

    def _held_bag_recovery_is_overbought(self, current_rsi: Optional[float]) -> bool:
        threshold = self._held_bag_recovery_sell_rsi_threshold()
        return current_rsi is not None and current_rsi >= threshold

    def _recovery_sell_chaser_config(self) -> Optional[LimitChaserConfig]:
        if self.config.recovery_sell_execution_strategy != ExecutionStrategy.LIMIT_CHASER:
            return None
        return LimitChaserConfig(
            distance=Decimal(str(self.config.recovery_limit_chaser_distance_pct)),
            refresh_threshold=Decimal(str(self.config.recovery_limit_chaser_refresh_threshold_pct)),
        )

    def _sell_trend_hold_active(
        self,
        current_price: Optional[Decimal] = None,
        *,
        current_rsi: Optional[float] = None,
        rsi_prev: Optional[float] = None,
        ema_fast: Optional[float] = None,
    ) -> Tuple[bool, str]:
        if not self._is_strong_bullish_trend():
            return False, "trend-neutral"

        state = (self.processed_data or {}).get("signal_state", {})
        current_rsi = current_rsi if current_rsi is not None else state.get("rsi")
        rsi_prev = rsi_prev if rsi_prev is not None else state.get("rsi_prev")
        ema_fast = ema_fast if ema_fast is not None else state.get("ema_fast")
        if current_price is None:
            current_price = self._get_mid_price()

        rsi_rollover = (
            current_rsi is not None
            and rsi_prev is not None
            and (rsi_prev - current_rsi) >= float(self.config.sell_rsi_rollover_delta)
        )
        price_below_ema = ema_fast is not None and current_price is not None and current_price < Decimal(str(ema_fast))
        recovery_ready = any(
            self._recovery_reversal_details(
                trail,
                current_price=current_price,
                current_rsi=current_rsi,
                ema_fast=ema_fast,
            )[0]
            for trail in self._recovery_trails.values()
        )

        if recovery_ready or (rsi_rollover and price_below_ema):
            return False, "reversal-confirmed"
        return True, "trend-hold"

    def _active_recovery_sell_executors(self):
        return self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda ex: (
                ex.is_active
                and ex.connector_name == self.config.connector_name
                and ex.trading_pair == self.config.trading_pair
                and ex.side == TradeType.SELL
                and getattr(ex, "type", "") == "order_executor"
                and str((ex.custom_info or {}).get("level_id") or "").startswith("recovery_exit:")
            ),
        )

    def _active_signal_sell_executors(self):
        return self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda ex: (
                ex.is_active
                and ex.connector_name == self.config.connector_name
                and ex.trading_pair == self.config.trading_pair
                and ex.side == TradeType.SELL
                and self._executor_level_id(ex) == "signal_exit"
            ),
        )

    def _active_sell_executors(self):
        return self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda ex: (
                ex.is_active
                and ex.connector_name == self.config.connector_name
                and ex.trading_pair == self.config.trading_pair
                and ex.side == TradeType.SELL
            ),
        )

    def _maybe_cancel_stale_recovery_sell_executors(self, current_price: Decimal) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        should_hold, hold_reason = self._sell_trend_hold_active(current_price=current_price)
        if not should_hold:
            return actions

        stale_pct = Decimal(str(self.config.recovery_cancel_stale_order_pct))
        if stale_pct <= 0:
            return actions

        for executor in self._active_recovery_sell_executors():
            order_price = getattr(executor.config, "price", None)
            try:
                order_price_decimal = Decimal(str(order_price)) if order_price is not None else None
            except _DECIMAL_CONVERSION_ERRORS:
                order_price_decimal = None
            if order_price_decimal is None or order_price_decimal <= 0:
                continue
            if current_price >= order_price_decimal * (Decimal("1") + stale_pct):
                actions.append(
                    StopExecutorAction(
                        executor_id=executor.id,
                        controller_id=self.config.id,
                        keep_position=True,
                    )
                )
                self.logger().debug(
                    f"Cancel stale recovery sell | {self._controller_pair_log_prefix()} "
                    f"executor={executor.id} reason={hold_reason} "
                    f"order_price={order_price_decimal} current={current_price}"
                )
        return actions

    def _maybe_cancel_stale_signal_sell_executors(
        self,
        current_price: Decimal,
        *,
        current_signal: int,
    ) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        stale_pct = Decimal(str(self.config.recovery_cancel_stale_order_pct))
        if stale_pct <= 0:
            return actions

        should_hold, hold_reason = self._sell_trend_hold_active(current_price=current_price)
        sell_signal_active = current_signal < 0

        for executor in self._active_signal_sell_executors():
            executor_config = getattr(executor, "config", None)
            order_price = getattr(executor_config, "price", None)
            try:
                order_price_decimal = Decimal(str(order_price)) if order_price is not None else None
            except _DECIMAL_CONVERSION_ERRORS:
                order_price_decimal = None
            if order_price_decimal is None or order_price_decimal <= 0:
                continue

            cancel_reason: Optional[str] = None
            if should_hold and current_price >= order_price_decimal * (Decimal("1") + stale_pct):
                cancel_reason = hold_reason
            elif not sell_signal_active:
                distance_pct = abs(current_price - order_price_decimal) / order_price_decimal
                if distance_pct >= stale_pct:
                    cancel_reason = "signal-stale"
            elif current_price <= order_price_decimal * (Decimal("1") - stale_pct):
                cancel_reason = "reprice"

            if cancel_reason is None:
                continue

            actions.append(
                StopExecutorAction(
                    executor_id=executor.id,
                    controller_id=self.config.id,
                    keep_position=True,
                )
            )
            self.logger().debug(
                f"Cancel stale signal sell | {self._controller_pair_log_prefix()} "
                f"executor={executor.id} reason={cancel_reason} "
                f"order_price={order_price_decimal} current={current_price}"
            )
        return actions

    def _held_positions(self, side: TradeType = TradeType.BUY) -> List[object]:
        return [
            position for position in self.positions_held
            if position.connector_name == self.config.connector_name
            and position.trading_pair == self.config.trading_pair
            and position.side == side
            and position.amount > 0
        ]

    @staticmethod
    def _decimal_or_none(value: object) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None
        return decimal_value if decimal_value.is_finite() else None

    def _get_available_asset_balance(self, asset: Optional[str]) -> Optional[Decimal]:
        if asset is None:
            return None
        try:
            raw_available = self.market_data_provider.get_available_balance(self.config.connector_name, asset)
            available = self._decimal_or_none(raw_available)
            if available is not None or raw_available is None:
                return available
        except AttributeError:
            pass
        except _PROVIDER_ERRORS:
            pass
        try:
            raw_balance = self.market_data_provider.get_balance(self.config.connector_name, asset)
            return self._decimal_or_none(raw_balance)
        except _PROVIDER_ERRORS:
            return None

    def _aggregate_held_inventory(self, side: TradeType = TradeType.BUY) -> AggregatedInventoryState:
        positions = self._held_positions(side)
        total_amount = Decimal("0")
        weighted_cost_quote = Decimal("0")
        cum_fees_quote = Decimal("0")
        complete_cost_basis = True

        for position in positions:
            try:
                amount = Decimal(str(position.amount))
            except (InvalidOperation, TypeError, ValueError):
                continue
            if amount <= 0:
                continue
            total_amount += amount
            try:
                breakeven = Decimal(str(getattr(position, "breakeven_price", None)))
            except (InvalidOperation, TypeError, ValueError):
                breakeven = None
            if breakeven is None or breakeven <= 0:
                complete_cost_basis = False
                continue
            weighted_cost_quote += amount * breakeven
            position_fees = self._decimal_or_none(getattr(position, "cum_fees_quote", Decimal("0")))
            if position_fees is not None and position_fees > 0:
                cum_fees_quote += position_fees

        cost_basis: Optional[Decimal] = None
        if total_amount > 0 and complete_cost_basis:
            cost_basis = weighted_cost_quote / total_amount

        return AggregatedInventoryState(
            positions=positions,
            total_amount=total_amount,
            bag_count=len(positions),
            cost_basis=cost_basis,
            cum_fees_quote=cum_fees_quote,
        )

    def _check_active_executor_drawdown_stop(
        self,
        current_price: Decimal,
        existing_executor_ids: Optional[set] = None,
    ) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        threshold = Decimal(str(self.config.early_stop_drawdown_pct))
        if threshold <= 0 or current_price <= 0:
            return actions

        stopped_executor_ids = existing_executor_ids if existing_executor_ids is not None else set()
        keep_position = self.config.early_stop_keep_position
        for side in (TradeType.BUY, TradeType.SELL):
            for executor in self._filter_same_side(side, active_only=True):
                executor_id = getattr(executor, "id", None)
                if executor_id is None or executor_id in stopped_executor_ids:
                    continue
                ref_price = self._executor_ref_price(executor)
                if ref_price is None or ref_price <= 0:
                    continue
                pnl_pct = (
                    (current_price - ref_price) / ref_price
                    if side == TradeType.BUY
                    else (ref_price - current_price) / ref_price
                )
                if pnl_pct > -threshold:
                    continue

                actions.append(
                    StopExecutorAction(
                        executor_id=executor_id,
                        controller_id=self.config.id,
                        keep_position=keep_position,
                    )
                )
                stopped_executor_ids.add(executor_id)
                self.logger().warning(
                    f"Early stop | {self._controller_pair_log_prefix()} executor={executor_id} "
                    f"side={'BUY' if side == TradeType.BUY else 'SELL'} "
                    f"ref={ref_price} current={current_price} pnl={pnl_pct} keep={keep_position}"
                )
        return actions

    def _get_base_balance(self) -> Optional[Decimal]:
        base_asset, _ = self._base_quote_assets()
        return self._get_available_asset_balance(base_asset)

    def _get_sellable_inventory_amount(self) -> Decimal:
        if self.config.trade_mode == "sell_only":
            return max(self._get_base_balance() or Decimal("0"), Decimal("0"))
        inventory = self._aggregate_held_inventory(TradeType.BUY)
        if inventory.total_amount <= 0:
            return Decimal("0")
        base_balance = self._get_base_balance()
        if base_balance is not None:
            return min(inventory.total_amount, base_balance)
        return Decimal("0")

    def _position_amount(self, side: TradeType) -> Decimal:
        return self._aggregate_held_inventory(side).total_amount

    def _get_position_cost_basis(self) -> Optional[Decimal]:
        return self._aggregate_held_inventory(TradeType.BUY).cost_basis

    def _inventory_balance_shortfall_amount(
        self,
        inventory: Optional[AggregatedInventoryState] = None,
    ) -> Optional[Decimal]:
        inventory_state = inventory if inventory is not None else self._aggregate_held_inventory(TradeType.BUY)
        if inventory_state.total_amount <= 0:
            return Decimal("0")
        base_balance = self._get_base_balance()
        if base_balance is None:
            return None
        return max(inventory_state.total_amount - base_balance, Decimal("0"))

    def _inventory_balance_shortfall_value(
        self,
        current_price: Optional[Decimal],
        inventory: Optional[AggregatedInventoryState] = None,
    ) -> Optional[Decimal]:
        if current_price is None or current_price <= 0:
            return None
        shortfall_amount = self._inventory_balance_shortfall_amount(inventory)
        if shortfall_amount is None:
            return None
        return shortfall_amount * current_price

    def _inventory_net_pnl_pct(self, current_price: Optional[Decimal]) -> Optional[Decimal]:
        if current_price is None or current_price <= 0:
            return None
        inventory = self._aggregate_held_inventory(TradeType.BUY)
        if inventory.total_amount <= 0 or inventory.cost_basis is None or inventory.cost_basis <= 0:
            return None
        cost_quote = inventory.total_amount * inventory.cost_basis
        if cost_quote <= 0:
            return None
        gross_pnl_quote = (current_price - inventory.cost_basis) * inventory.total_amount
        return (gross_pnl_quote - inventory.cum_fees_quote) / cost_quote

    def _position_net_pnl_pct(self, position: object, current_price: Decimal) -> Optional[Decimal]:
        if current_price <= 0:
            return None
        amount = self._decimal_or_none(getattr(position, "amount", None))
        breakeven = self._decimal_or_none(getattr(position, "breakeven_price", None))
        if amount is None or amount <= 0 or breakeven is None or breakeven <= 0:
            return None
        cost_quote = amount * breakeven
        if cost_quote <= 0:
            return None
        fees_quote = self._decimal_or_none(getattr(position, "cum_fees_quote", Decimal("0"))) or Decimal("0")
        gross_pnl_quote = (current_price - breakeven) * amount
        return (gross_pnl_quote - fees_quote) / cost_quote

    def _base_quote_assets(self) -> Tuple[Optional[str], Optional[str]]:
        parts = self.config.trading_pair.split("-")
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, None

    def _controller_pair_log_prefix(self) -> str:
        return f"controller={self.config.id} pair={self.config.trading_pair}"

    def _held_bag_count(self, side: TradeType = TradeType.BUY) -> int:
        return self._aggregate_held_inventory(side).bag_count

    def _position_log_context(self) -> str:
        mid_price = self._get_mid_price()
        avg_entry = self._get_position_cost_basis()
        context_parts = [
            f"pos_util={self._get_position_utilization():.0%}",
            f"active_buy_execs={len(self._filter_same_side(TradeType.BUY, active_only=True))}",
            f"held_bags={self._held_bag_count()}",
        ]
        if mid_price is not None and mid_price > 0:
            context_parts.append(f"mid={mid_price:.6f}")
        if avg_entry is not None and avg_entry > 0:
            context_parts.append(f"avg_entry={avg_entry:.6f}")
        return " ".join(context_parts)

    def _get_active_buy_executor_position_value_usd(self) -> Decimal:
        total_value = Decimal("0")
        mid_price = self._get_mid_price()
        for executor in self._filter_same_side(TradeType.BUY, active_only=True):
            try:
                filled_quote = Decimal(str(getattr(executor, "filled_amount_quote", Decimal("0")) or Decimal("0")))
                if filled_quote <= 0:
                    continue
                ref_price = self._executor_ref_price(executor)
                if mid_price is not None and mid_price > 0 and ref_price is not None and ref_price > 0:
                    base_amount = filled_quote / ref_price
                    total_value += base_amount * mid_price
                else:
                    total_value += filled_quote
            except (InvalidOperation, ValueError, TypeError, AttributeError):
                continue
        return total_value

    def _get_total_position_value_usd(self) -> Decimal:
        try:
            amount = self._position_amount(TradeType.BUY)
            held_value = Decimal("0")
            if amount > 0:
                price = self.market_data_provider.get_price_by_type(
                    self.config.connector_name, self.config.trading_pair, PriceType.MidPrice,
                )
                if price is not None and price > 0:
                    held_value = amount * Decimal(str(price))
            return held_value + self._get_active_buy_executor_position_value_usd()
        except (InvalidOperation, ValueError, TypeError, AttributeError):
            return Decimal("0")

    def _get_position_utilization(self) -> float:
        max_pos = getattr(self.config, "max_total_position_usd", Decimal("0"))
        if max_pos <= 0:
            return 0.0
        return float(min(Decimal("1"), self._get_total_position_value_usd() / max_pos))

    @staticmethod
    def _normalize(val, low, high) -> Optional[float]:
        if val is None or low is None or high is None or high <= low:
            return None
        clamped = max(low, min(high, val))
        return (clamped - low) / (high - low)

    @staticmethod
    def _fmt(value, precision: int = 4) -> str:
        if value is None:
            return "n/a"
        try:
            numeric = float(value)
            if numeric != numeric or numeric in (float("inf"), float("-inf")):
                return "n/a"
            return f"{numeric:.{precision}f}"
        except (ValueError, TypeError):
            return str(value)

    def _record_gate_reason(self, side: TradeType, reason: Optional[str]):
        try:
            prev = self._last_gate_reason.get(side)
            if prev != reason:
                self._last_gate_reason[side] = reason
                side_label = "BUY" if side == TradeType.BUY else "SELL"
                normalized_reason = self._normalize_reason(reason)
                state = (self.processed_data or {}).get("signal_state", {})
                score = int(state.get("signal_score", 0) or 0)
                current_signal = int((self.processed_data or {}).get("signal", 0) or 0)
                self._log_signal_transition(
                    key=f"gate_{side_label.lower()}",
                    signature=f"{normalized_reason}|{current_signal}|{score}",
                    message=(
                        f"{side_label} gate | {self._controller_pair_log_prefix()} "
                        f"signal={current_signal} score={score}/{self.config.min_signal_score} "
                        f"reason={normalized_reason} {self._position_log_context()}"
                    ),
                )
        except _STATUS_FORMAT_ERRORS:
            pass

    @staticmethod
    def _compact_gate_reason(reason: Optional[str]) -> str:
        if reason in (None, "ok", "ready"):
            return "ready"
        mapping = {
            "consecutive_loss_pause": "loss-pause",
            "cooldown": "cooldown",
            "ib_cooldown": "ib-cd",
            "flash_crash": "flash-cd",
            "price_gap": "gap",
            "capacity": "capacity",
            "dca_score_low": "score-low",
            "max_position": "max-pos",
            "price_unavailable": "no-price",
            "balance_low": "quote-low",
            "base_balance_low": "base-low",
            "trend_hold": "trend-hold",
            "strict_hmm": "strict-hmm",
        }
        return mapping.get(reason, str(reason).replace("_", "-"))

    @staticmethod
    def _normalize_reason(reason: Optional[str]) -> str:
        if reason in (None, "", "ok", "ready"):
            return "ready"
        return str(reason).replace("_", "-")

    @staticmethod
    def _bool_label(value: Optional[bool]) -> str:
        if value is None:
            return "n/a"
        return "yes" if value else "no"

    def _log_signal_transition(self, key: str, signature: str, message: str, *, level: int = logging.DEBUG):
        previous_signature = self._last_signal_log_signature.get(key)
        if previous_signature != signature:
            self._last_signal_log_signature[key] = signature
            logger = self.logger()
            if level >= logging.WARNING:
                logger.warning(message)
            elif level >= logging.INFO:
                logger.info(message)
            else:
                logger.debug(message)

    @staticmethod
    def _summary_signal_label(signal: int) -> str:
        signal_map = {
            1: "BUY",
            -1: "SELL",
            0: "NEUTRAL",
        }
        return signal_map.get(signal, "UNKNOWN")

    @staticmethod
    def _summary_pct(value: Optional[Decimal], precision: int = 1) -> str:
        if value is None:
            return "n/a"
        try:
            return f"{float(value) * 100:.{precision}f}%"
        except (TypeError, ValueError):
            return "n/a"

    @staticmethod
    def _summary_price(value: Optional[Decimal]) -> str:
        if value is None:
            return "n/a"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "n/a"
        if numeric != numeric or numeric in (float("inf"), float("-inf")):
            return "n/a"
        if abs(numeric) >= 100:
            precision = 2
        elif abs(numeric) >= 1:
            precision = 4
        else:
            precision = 6
        return f"{numeric:.{precision}f}".rstrip("0").rstrip(".")

    @staticmethod
    def _summary_timestamp_label(value: Optional[object]) -> str:
        try:
            numeric = float(value)
        except (TypeError, ValueError, OverflowError):
            return "n/a"
        if numeric != numeric or numeric in (float("inf"), float("-inf")):
            return "n/a"
        try:
            return datetime.fromtimestamp(numeric, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except (OSError, OverflowError, ValueError):
            return "n/a"

    @staticmethod
    def _short_regime_label(regime_label: Optional[str]) -> str:
        if not regime_label:
            return "detecting"

        if "HV_" in regime_label:
            vol = "HV"
        elif "LV_" in regime_label:
            vol = "LV"
        else:
            vol = "MV"

        if "Trend_Up" in regime_label:
            trend = "up"
        elif "Trend_Down" in regime_label:
            trend = "down"
        elif "Range" in regime_label:
            trend = "range"
        else:
            trend = regime_label.replace("_", "-")

        return f"{vol}-{trend}"

    def get_status_summary(self) -> Dict[str, object]:
        if not self.config:
            return {}

        processed = self.processed_data or {}
        indicators = processed.get("indicators", {})
        state = processed.get("signal_state", {})
        current_signal = processed.get("signal", 0)
        regime_label = processed.get("regime")
        cost_basis = self._decimal_or_none(indicators.get("cost_basis"))
        if cost_basis is None:
            cost_basis = self._get_position_cost_basis()
        buy_role = str(state.get("buy_entry_role", "none") or "none")

        mid_price = self._get_mid_price()
        pos_usd = self._get_total_position_value_usd()
        util = self._get_position_utilization()
        active_buys = self._filter_same_side(TradeType.BUY, active_only=True)
        active_sells = self._filter_same_side(TradeType.SELL, active_only=True)
        held_bags = [
            position
            for position in self.positions_held
            if position.connector_name == self.config.connector_name
            and position.trading_pair == self.config.trading_pair
            and position.side == TradeType.BUY
            and position.amount > 0
        ]

        unrealized_pnl_pct: Optional[Decimal] = None
        profit_headroom_pct: Optional[Decimal] = None
        if cost_basis is not None and mid_price is not None and cost_basis > 0:
            unrealized_pnl_pct = (mid_price - cost_basis) / cost_basis
            profit_headroom_pct = unrealized_pnl_pct - self.config.min_profit_pct_for_sell

        now_ts = self.market_data_provider.time() if self.market_data_provider else 0.0
        loss_pause_active = self._check_consecutive_loss_pause()
        flash_crash_active = self._is_flash_crash_cooldown(now_ts)
        buy_gate = self._compact_gate_reason(self._last_gate_reason.get(TradeType.BUY))
        sell_gate = self._compact_gate_reason(self._last_gate_reason.get(TradeType.SELL))

        if loss_pause_active:
            state_label = "PAUSED"
        elif flash_crash_active:
            state_label = "FLASH-CD"
        elif len(active_sells) > 0:
            state_label = "EXITING"
        elif pos_usd > 0 and len(active_buys) > 0:
            state_label = "BUILDING"
        elif pos_usd > 0 or len(held_bags) > 0:
            state_label = "HOLDING"
        elif len(active_buys) > 0:
            state_label = "ENTERING"
        elif current_signal > 0 and buy_role == "scout":
            state_label = "SCOUT-ARMED"
        elif current_signal > 0 and buy_role == "runner":
            state_label = "RUNNER-ARMED"
        elif current_signal > 0:
            state_label = "ARMED-BUY"
        elif current_signal < 0:
            state_label = "SELL-WATCH"
        else:
            state_label = "FLAT"

        if loss_pause_active:
            note = "loss pause"
        elif flash_crash_active:
            note = "flash cooldown"
        elif len(active_sells) > 0:
            note = f"{len(active_sells)} sell active"
        elif len(active_buys) > 0:
            note = f"{len(active_buys)} buy active"
        elif sell_gate == "trend-hold":
            note = "trend hold"
        elif current_signal > 0 and buy_gate != "ready":
            role_prefix = buy_role if buy_role in {"scout", "runner"} else "buy"
            note = f"{role_prefix} blocked {buy_gate}"
        elif current_signal < 0 and sell_gate != "ready":
            note = f"sell blocked {sell_gate}"
        elif pos_usd > 0 and profit_headroom_pct is not None:
            if profit_headroom_pct < 0:
                note = f"sell gap {self._summary_pct(abs(profit_headroom_pct))}"
            else:
                note = f"sell buffer {self._summary_pct(profit_headroom_pct)}"
        elif len(held_bags) > 0:
            note = f"held bags {len(held_bags)}"
        elif current_signal != 0:
            note = "signal armed"
        else:
            note = "monitoring"

        attention_score = 0
        if loss_pause_active:
            attention_score += 120
        if flash_crash_active:
            attention_score += 100
        if len(active_sells) > 0:
            attention_score += 90
        if pos_usd > 0:
            attention_score += 80
        if len(active_buys) > 0:
            attention_score += 70
        if current_signal != 0:
            attention_score += 40
        if (current_signal > 0 and buy_gate != "ready") or (current_signal < 0 and sell_gate != "ready"):
            attention_score += 20
        attention_score += int(util * 10)

        relevant_gate = sell_gate if (pos_usd > 0 or current_signal < 0 or len(active_sells) > 0) else buy_gate
        exposure_text = (
            f"${float(pos_usd):.0f}/${float(self.config.max_total_position_usd):.0f} ({util:.0%})"
        )

        rsi_value = indicators.get("rsi")
        rsi_buy_threshold = float(self.config.rsi_buy_threshold)
        rsi_sell_threshold = float(self.config.sell_rsi_overbought)
        has_inventory = pos_usd > 0 or len(held_bags) > 0

        recovery_target_pct = Decimal(str(self.config.bag_recovery_target_pct))
        if self.config.sell_only_if_profitable:
            recovery_target_pct = max(
                recovery_target_pct, Decimal(str(self.config.min_profit_pct_for_sell))
            )

        rsi_proximity_label, rsi_text = self._rsi_proximity_label(
            rsi_value=rsi_value,
            rsi_sell_threshold=rsi_sell_threshold,
            rsi_buy_threshold=rsi_buy_threshold,
            has_inventory=has_inventory,
        )
        buy_gauge = self._buy_gauge_label(rsi_value, rsi_buy_threshold)
        sell_gauge = self._sell_gauge_label(
            unrealized_pnl_pct=unrealized_pnl_pct,
            target_pct=recovery_target_pct,
            has_inventory=has_inventory,
        )
        gates_mask = self._gates_mask_label(
            has_inventory=has_inventory,
            rsi_value=rsi_value,
            rsi_sell_threshold=rsi_sell_threshold,
            unrealized_pnl_pct=unrealized_pnl_pct,
            recovery_target_pct=recovery_target_pct,
            sell_reversal_confirmed=bool(state.get("sell_reversal_confirmed")),
            buy_gate=buy_gate,
        )

        plan_fields = self._compute_plan_fields()

        return {
            "pair": self.config.trading_pair,
            "price": self._summary_price(mid_price),
            "timestamp": self._summary_timestamp_label(processed.get("timestamp") or processed.get("candle_timestamp")),
            "controller_id": self.config.id,
            "state": state_label,
            "signal": self._summary_signal_label(current_signal),
            "score": f"{state.get('signal_score', 0)}/{self.config.min_signal_score}",
            "regime": self._short_regime_label(regime_label),
            "trend": self._processed_trend_confirmation().get("direction", "unavailable"),
            "avg_buy": self._summary_price(cost_basis),
            "exposure": exposure_text,
            "execs": f"B{len(active_buys)} S{len(active_sells)} H{len(held_bags)}",
            "u_pnl": self._summary_pct(unrealized_pnl_pct),
            "u_pnl_pct": self._summary_pct(unrealized_pnl_pct),
            "rsi": rsi_text,
            "rsi_proximity": rsi_proximity_label,
            "buy_gauge": buy_gauge,
            "sell_gauge": sell_gauge,
            "gates_mask": gates_mask,
            "gate": relevant_gate,
            "note": note,
            "focus": note,
            "attention_score": attention_score,
            **plan_fields,
        }

    def _compute_plan_fields(self) -> Dict[str, str]:
        """Build the structured ``plan_*`` fields consumed by the parent ``PLANS`` summary table.

        The parent strategy renders one row per controller. Cells adapt per ``plan_mode``:

        - ``EXITS``   → ``target`` recovery %, ``anchor`` = min-profit price + price gap,
          ``next`` = recovery arm price + price gap.
        - ``TRAIL``   → ``anchor`` = peak price, ``next`` = pullback % + partial-done flag.
        - ``ENTRIES`` → ``target`` = ``rsi≤<buy_threshold>``, ``anchor`` = scout slice,
          ``next`` = runner slice.

        ``sellable`` / ``held`` / ``active`` / ``free`` / ``managed`` feed the
        compact inventory reconciliation table rendered next to the action plan.
        """
        if not self.config:
            return {
                "plan_mode": "—",
                "plan_target": "—",
                "plan_anchor": "—",
                "plan_next": "—",
                "plan_slice": "—",
                "plan_liquidity": "—",
                "plan_sellable": "—",
                "plan_held": "—",
                "plan_active": "—",
                "plan_untracked": "—",
                "plan_free": "—",
                "plan_managed": "—",
            }

        processed = self.processed_data or {}
        thresholds = processed.get("thresholds", {})
        rsi_buy = float(thresholds.get("rsi_buy", self.config.rsi_buy_threshold))
        mid_price = self._get_mid_price()
        inventory = self._aggregate_held_inventory(TradeType.BUY)
        has_inventory = inventory.total_amount > 0
        recovery_target_pct = Decimal(str(self.config.bag_recovery_target_pct))
        if self.config.sell_only_if_profitable:
            recovery_target_pct = max(
                recovery_target_pct, Decimal(str(self.config.min_profit_pct_for_sell))
            )

        base_asset, _ = self._base_quote_assets()
        base_suffix = f" {base_asset}" if base_asset else ""
        idle_compact = self._idle_wallet_label(mid_price).replace("idle wallet ", "idle ", 1)

        if not has_inventory:
            try:
                usd_per_entry = Decimal(str(getattr(self.config, "usd_per_entry", Decimal("0")) or Decimal("0")))
            except (InvalidOperation, TypeError, ValueError):
                usd_per_entry = Decimal("0")
            try:
                scout_fraction = Decimal(str(getattr(self.config, "scout_entry_fraction", Decimal("0")) or Decimal("0")))
            except (InvalidOperation, TypeError, ValueError):
                scout_fraction = Decimal("0")
            scout_fraction = max(Decimal("0"), min(Decimal("1"), scout_fraction))
            runner_fraction = max(Decimal("0"), Decimal("1") - scout_fraction)
            scout_quote = usd_per_entry * scout_fraction if usd_per_entry > 0 else None
            runner_quote = usd_per_entry * runner_fraction if usd_per_entry > 0 else None
            held_value, active_value, untracked_value = self._inventory_reconciliation_values(mid_price)
            managed_value = (held_value or Decimal("0")) + active_value

            plan_anchor = (
                f"scout {self._status_money_label(scout_quote)} ({float(scout_fraction) * 100:.0f}%)"
                if scout_quote is not None
                else "scout —"
            )
            plan_next = (
                f"runner {self._status_money_label(runner_quote)} ({float(runner_fraction) * 100:.0f}%)"
                if runner_quote is not None
                else "runner —"
            )
            return {
                "plan_mode": "ENTRIES",
                "plan_target": f"rsi≤{rsi_buy:.2f}",
                "plan_anchor": plan_anchor,
                "plan_next": plan_next,
                "plan_slice": "—",
                "plan_liquidity": idle_compact,
                "plan_sellable": "—",
                "plan_held": self._status_compact_money_label(held_value),
                "plan_active": self._status_compact_money_label(active_value),
                "plan_untracked": self._status_compact_money_label(untracked_value),
                "plan_free": self._status_compact_money_label(untracked_value),
                "plan_managed": self._status_compact_money_label(managed_value),
            }

        breakeven = inventory.cost_basis
        min_sell_price: Optional[Decimal] = None
        recovery_arm_price: Optional[Decimal] = None
        if breakeven is not None and breakeven > 0:
            min_sell_price = breakeven * (Decimal("1") + Decimal(str(self.config.min_profit_pct_for_sell)))
            recovery_arm_price = breakeven * (Decimal("1") + recovery_target_pct)

        next_fraction = Decimal(str(self.config.recovery_partial_exit_fraction))
        next_amount = inventory.total_amount * next_fraction if inventory.total_amount > 0 else Decimal("0")
        next_quote = next_amount * recovery_arm_price if recovery_arm_price is not None else None

        sellable_amount = self._get_sellable_inventory_amount()
        held_amount = inventory.total_amount
        if held_amount > 0 and sellable_amount < held_amount:
            sellable_label = f"{sellable_amount:.4f}/{held_amount:.4f}{base_suffix}"
        else:
            sellable_label = f"{held_amount:.4f}{base_suffix}"
        held_value, active_value, untracked_value = self._inventory_reconciliation_values(mid_price)
        managed_value = (held_value or Decimal("0")) + active_value
        plan_liquidity = f"sell {sellable_label} · {self._inventory_reconciliation_label(mid_price)}"

        plan_slice = (
            f"{float(next_fraction) * 100:.0f}% ≈ {next_amount:.4f}{base_suffix} "
            f"({self._status_money_label(next_quote)})"
        )
        plan_target = self._status_pct_label(recovery_target_pct)

        trail_snapshots = self._status_recovery_trails()
        if trail_snapshots:
            top_trail = max(
                trail_snapshots,
                key=lambda trail: float(trail.get("armed_timestamp") or 0.0),
            )
            peak_price = top_trail.get("peak_price")
            peak_decimal: Optional[Decimal]
            pullback_pct: Optional[Decimal]
            try:
                peak_decimal = Decimal(str(peak_price)) if peak_price is not None else None
                if mid_price is not None and peak_decimal is not None and peak_decimal > 0:
                    pullback_pct = max((peak_decimal - mid_price) / peak_decimal, Decimal("0"))
                else:
                    pullback_pct = None
            except (InvalidOperation, TypeError, ValueError):
                peak_decimal = None
                pullback_pct = None

            partial_done = bool(top_trail.get("partial_exit_done"))
            partial_label = "partial-done" if partial_done else "full-stack"
            pullback_text = (
                f"pullback {self._status_pct_label(pullback_pct)}"
                if pullback_pct is not None
                else "pullback n/a"
            )
            return {
                "plan_mode": "TRAIL",
                "plan_target": plan_target,
                "plan_anchor": f"peak {self._summary_price(peak_decimal)}",
                "plan_next": f"{pullback_text} · {partial_label}",
                "plan_slice": plan_slice,
                "plan_liquidity": plan_liquidity,
                "plan_sellable": sellable_label,
                "plan_held": self._status_compact_money_label(held_value),
                "plan_active": self._status_compact_money_label(active_value),
                "plan_untracked": self._status_compact_money_label(untracked_value),
                "plan_free": self._status_compact_money_label(untracked_value),
                "plan_managed": self._status_compact_money_label(managed_value),
            }

        return {
            "plan_mode": "EXITS",
            "plan_target": plan_target,
            "plan_anchor": (
                f"min {self._summary_price(min_sell_price)} "
                f"({self._status_price_gap_label(min_sell_price, mid_price)})"
            ),
            "plan_next": (
                f"arm {self._summary_price(recovery_arm_price)} "
                f"({self._status_price_gap_label(recovery_arm_price, mid_price)})"
            ),
            "plan_slice": plan_slice,
            "plan_liquidity": plan_liquidity,
            "plan_sellable": sellable_label,
            "plan_held": self._status_compact_money_label(held_value),
            "plan_active": self._status_compact_money_label(active_value),
            "plan_untracked": self._status_compact_money_label(untracked_value),
            "plan_free": self._status_compact_money_label(untracked_value),
            "plan_managed": self._status_compact_money_label(managed_value),
        }

    @staticmethod
    def _clamp_progress(value: Optional[float]) -> float:
        if value is None:
            return 0.0
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return 0.0
        if numeric != numeric or numeric in (float("inf"), float("-inf")):
            return 0.0
        return max(0.0, min(1.0, numeric))

    @classmethod
    def _progress_bar(cls, progress: float, width: int = 12) -> str:
        progress = cls._clamp_progress(progress)
        filled = max(0, min(width, int(round(progress * width))))
        return f"[{'#' * filled}{'.' * (width - filled)}]"

    @classmethod
    def _progress_gauge(cls, progress: float, width: int = 12) -> str:
        progress = cls._clamp_progress(progress)
        filled = max(0, min(width, int(round(progress * width))))
        return f"{'█' * filled}{'░' * (width - filled)}"

    @classmethod
    def _gauge_summary(
        cls,
        progress: Optional[float],
        *,
        width: int = 10,
        idle_label: str = "—",
        ready_label: str = "FIRE",
    ) -> str:
        """Compact ASCII gauge: ``██░░░░░░░░ 24%``, ``░░░░░░░░░░ —``, or ready label."""
        if progress is None:
            return f"{'░' * width} {idle_label:>4}"
        clamped = cls._clamp_progress(progress)
        bar = cls._progress_gauge(clamped, width)
        if clamped >= 1.0:
            return f"{bar} {ready_label:>4}"
        return f"{bar} {int(round(clamped * 100)):>3}%"

    @staticmethod
    def _rsi_proximity_label(
        *,
        rsi_value: Optional[object],
        rsi_sell_threshold: float,
        rsi_buy_threshold: float,
        has_inventory: bool,
    ) -> Tuple[str, str]:
        """Return ``("42.5/79.7", "42.46")`` for the CONTROLLERS SUMMARY ``rsi`` column."""
        try:
            numeric = float(rsi_value) if rsi_value is not None else None
        except (TypeError, ValueError):
            numeric = None
        if numeric is None:
            return ("n/a", "n/a")
        target = rsi_sell_threshold if has_inventory else rsi_buy_threshold
        return (f"{numeric:.1f}/{target:.1f}", f"{numeric:.2f}")

    @classmethod
    def _buy_gauge_label(
        cls,
        rsi_value: Optional[object],
        rsi_buy_threshold: float,
    ) -> str:
        """Progress toward the buy trigger.

        ``progress = clamp((50 - rsi) / (50 - rsi_buy_threshold), 0, 1)`` so the gauge
        only fills as RSI crosses below the neutral midline. Above 50 the bar is empty.
        """
        try:
            numeric = float(rsi_value) if rsi_value is not None else None
        except (TypeError, ValueError):
            numeric = None
        if numeric is None:
            return cls._gauge_summary(None)
        denom = 50.0 - float(rsi_buy_threshold)
        if denom <= 0:
            progress = 1.0 if numeric <= float(rsi_buy_threshold) else 0.0
        else:
            progress = (50.0 - numeric) / denom
        return cls._gauge_summary(progress, ready_label="BUY")

    @classmethod
    def _sell_gauge_label(
        cls,
        *,
        unrealized_pnl_pct: Optional[Decimal],
        target_pct: Decimal,
        has_inventory: bool,
    ) -> str:
        """Progress toward the bag-recovery profit target. ``—`` when flat / no basis."""
        if not has_inventory or unrealized_pnl_pct is None:
            return cls._gauge_summary(None)
        try:
            target_value = float(target_pct)
            if target_value <= 0:
                return cls._gauge_summary(None)
            progress = float(unrealized_pnl_pct) / target_value
        except (InvalidOperation, TypeError, ValueError, ZeroDivisionError):
            return cls._gauge_summary(None)
        return cls._gauge_summary(max(0.0, progress), ready_label="SELL")

    @classmethod
    def _gates_mask_label(
        cls,
        *,
        has_inventory: bool,
        rsi_value: Optional[object],
        rsi_sell_threshold: float,
        unrealized_pnl_pct: Optional[Decimal],
        recovery_target_pct: Decimal,
        sell_reversal_confirmed: bool,
        buy_gate: Optional[str],
    ) -> str:
        """Compact gate truthtable for the CONTROLLERS SUMMARY ``gates`` column.

        HOLDING → ``P{✓|✗} R{✓|✗} V{✓|✗} T{·|▲}`` for profit / RSI / reversal / trail.
        FLAT    → ``buy:{compact_reason}`` (e.g. ``buy:ok``, ``buy:trd``, ``buy:ib-cd``).
        """
        if has_inventory:
            try:
                profit_ready = (
                    unrealized_pnl_pct is not None
                    and Decimal(str(unrealized_pnl_pct)) >= Decimal(str(recovery_target_pct))
                )
            except (InvalidOperation, TypeError, ValueError):
                profit_ready = False
            try:
                rsi_ready = rsi_value is not None and float(rsi_value) >= float(rsi_sell_threshold)
            except (TypeError, ValueError):
                rsi_ready = False
            check = lambda flag: "✓" if flag else "✗"  # noqa: E731 - tiny inline closure
            return (
                f"P{check(profit_ready)}"
                f" R{check(rsi_ready)}"
                f" V{check(sell_reversal_confirmed)}"
            )
        compact = (buy_gate or "ready").lower()
        compact = "ok" if compact == "ready" else compact
        return f"buy:{compact}"

    @staticmethod
    def _lane_badge(side: str, progress: float, ready: bool) -> str:
        if ready:
            return "🟢" if side == "buy" else "🔴"
        if progress >= 0.85:
            return "🟡"
        if progress >= 0.45:
            return "🟠"
        return "⚪"

    @staticmethod
    def _flag_icon(value: Optional[bool]) -> str:
        if value is None:
            return "·"
        return "✓" if value else "✗"

    @classmethod
    def _threshold_progress(cls, value: Optional[float], threshold: float, *, direction: str, window: float) -> float:
        if value is None or window <= 0:
            return 0.0
        if direction == "down":
            if value <= threshold:
                return 1.0
            return cls._clamp_progress(1.0 - ((value - threshold) / window))
        if value >= threshold:
            return 1.0
        return cls._clamp_progress(1.0 - ((threshold - value) / window))

    def _buy_signal_status_line(
        self,
        *,
        current_signal: int,
        last_rsi: Optional[float],
        last_close: Optional[float],
        rsi_buy: float,
        rsi_window: float,
        signal_score: int,
        raw_reversal: bool,
        condition_ok: bool,
        condition_reason: Optional[str],
        buy_decision: str,
        buy_reason: str,
        raw_prev_was_min: bool,
        raw_turning_up: bool,
        raw_was_oversold: bool,
        raw_near_bottom: bool,
        score_rsi_oversold: bool,
        score_bb_touch: bool,
        score_macd_turn: bool,
        score_mean_reversion: bool,
        buy_confirmation_active: bool,
        buy_confirmation_rebound_delta: Optional[float],
        buy_confirmation_rebound_target: Optional[float],
        buy_confirmation_price_rebounded: Optional[bool],
        buy_entry_role: str,
        buy_entry_fraction: float,
        buy_context_bias: str,
    ) -> str:
        min_score = max(1, int(self.config.min_signal_score))
        score_progress = self._clamp_progress(signal_score / min_score)
        rsi_progress = self._threshold_progress(last_rsi, rsi_buy, direction="down", window=rsi_window)
        gate = self._compact_gate_reason(self._last_gate_reason.get(TradeType.BUY))
        normalized_buy_reason = self._normalize_reason(buy_reason)
        condition_label = self._normalize_reason(condition_reason if not condition_ok else "ready")

        ready = current_signal > 0
        if ready:
            progress = 1.0
        else:
            if self.config.buy_confirmation_mode == "rebound_confirm" and buy_confirmation_active:
                rebound_target = max(float(buy_confirmation_rebound_target or 0.0), 0.0)
                rebound_delta = max(0.0, float(buy_confirmation_rebound_delta or 0.0))
                rebound_progress = 1.0 if rebound_target == 0 else self._clamp_progress(rebound_delta / rebound_target)
                price_rebounded = bool(buy_confirmation_price_rebounded)
                price_progress = 1.0 if price_rebounded else 0.0
                progress = (score_progress + rebound_progress + price_progress) / 3.0
            else:
                reversal_progress = 1.0 if raw_reversal else 0.0
                progress = (rsi_progress + score_progress + reversal_progress) / 3.0

        rsi_gap = max(0.0, (last_rsi - rsi_buy)) if last_rsi is not None else None
        need_parts: List[str] = []
        if rsi_gap is not None and rsi_gap > 0:
            need_parts.append(f"rsi {rsi_gap:.2f}")
        if signal_score < min_score:
            need_parts.append(f"score {min_score - signal_score}")
        if self.config.buy_confirmation_mode == "rebound_confirm" and buy_confirmation_active:
            rebound_target = max(float(buy_confirmation_rebound_target or 0.0), 0.0)
            rebound_delta = max(0.0, float(buy_confirmation_rebound_delta or 0.0))
            if rebound_target > rebound_delta:
                need_parts.append(f"rebound {rebound_target - rebound_delta:.2f}")
            if buy_confirmation_price_rebounded is False:
                need_parts.append("price")
        elif not raw_reversal and not ready:
            need_parts.append("reversal")
        if not condition_ok or condition_label != "ready":
            need_parts.append(condition_label)

        badge = self._lane_badge("buy", progress, ready)
        gauge = self._progress_gauge(progress)
        headline = normalized_buy_reason if normalized_buy_reason != "ready" else buy_decision
        details: List[str] = [
            f"{badge} {gauge} {progress * 100:.0f}%",
            f"🎯 {headline}",
            f"📉 RSI {self._fmt(last_rsi, 2)}/{rsi_buy:.2f}",
        ]
        if need_parts and not ready:
            details.append(f"⏳ {', '.join(need_parts)}")
        elif signal_score < min_score:
            details.append(f"⏳ score {min_score - signal_score}")
        if ready and buy_entry_role not in ("", "none"):
            details.append(f"🧩 {buy_entry_role} {buy_entry_fraction * 100:.0f}%")
        if ready and buy_context_bias not in ("", "neutral"):
            details.append(f"🧭 {buy_context_bias}")
        if buy_confirmation_active and not ready:
            details.append(
                f"🔁 {self._fmt(buy_confirmation_rebound_delta, 2)}/{self._fmt(buy_confirmation_rebound_target, 2)} {self._flag_icon(buy_confirmation_price_rebounded)}"
            )
        if gate != "ready":
            details.append(f"🧱 {gate}")
        return self._status_row("BUY", *details)

    def _sell_signal_status_line(
        self,
        *,
        current_signal: int,
        last_rsi: Optional[float],
        rsi_sell: float,
        rsi_window: float,
        has_inventory: bool,
        cost_basis: Optional[Decimal],
        mid_price: Optional[Decimal],
        sell_decision: str,
        sell_reason: str,
        sell_profitability_ok: bool,
        sell_rsi_rollover: bool,
        sell_price_below_ema: bool,
        sell_macd_rollover: bool,
        sell_reversal_confirmed: bool,
        sell_trend_hold: bool,
    ) -> str:
        rsi_progress = self._threshold_progress(last_rsi, rsi_sell, direction="up", window=rsi_window)
        gate = self._compact_gate_reason(self._last_gate_reason.get(TradeType.SELL))
        normalized_sell_reason = self._normalize_reason(sell_reason)

        pnl_pct: Optional[Decimal] = None
        profit_progress = 1.0
        profit_target = self.config.min_profit_pct_for_sell
        if has_inventory and cost_basis is not None and mid_price is not None and cost_basis > 0:
            pnl_pct = (mid_price - cost_basis) / cost_basis
        if has_inventory and self.config.sell_only_if_profitable:
            target_float = float(profit_target)
            current_float = float(pnl_pct) if pnl_pct is not None else float("-inf")
            if target_float <= 0:
                profit_progress = 1.0 if current_float >= 0 else 0.0
            else:
                profit_progress = self._clamp_progress(current_float / target_float)
        reversal_progress = 1.0 if sell_reversal_confirmed else 0.0

        if current_signal < 0:
            progress = 1.0
        elif has_inventory and self.config.sell_only_if_profitable:
            progress = (rsi_progress + profit_progress + reversal_progress) / 3.0
        else:
            progress = (rsi_progress + reversal_progress) / 2.0

        badge = self._lane_badge("sell", progress, current_signal < 0)
        gauge = self._progress_gauge(progress)
        headline = normalized_sell_reason if normalized_sell_reason != "ready" else sell_decision
        details: List[str] = [
            f"{badge} {gauge} {progress * 100:.0f}%",
            f"🎯 {headline}",
            f"📈 RSI {self._fmt(last_rsi, 2)}/{rsi_sell:.2f}",
        ]
        rsi_gap = max(0.0, (rsi_sell - last_rsi)) if last_rsi is not None else None
        need_parts: List[str] = []
        if current_signal >= 0 and rsi_gap is not None and rsi_gap > 0:
            need_parts.append(f"rsi {rsi_gap:.2f}")

        if has_inventory:
            if pnl_pct is not None:
                details.append(f"💰 {float(pnl_pct) * 100:+.2f}%/{float(profit_target) * 100:.2f}%")
                if self.config.sell_only_if_profitable and pnl_pct < profit_target:
                    need_parts.append(f"pnl {float(profit_target - pnl_pct) * 100:.2f}%")
            else:
                details.append("💰 n/a")
        else:
            details.append("📦 flat")
        if need_parts and current_signal >= 0:
            details.append(f"⏳ {', '.join(need_parts)}")
        if sell_trend_hold:
            details.append("🌿 trend-hold")
        if sell_reversal_confirmed and current_signal >= 0:
            details.append("✅ reversal-ready")
        if gate != "ready":
            details.append(f"🧱 {gate}")
        return self._status_row("SELL", *details)

    # -----------------------------------------------------------------------
    # Consecutive loss tracking (from v1)
    # -----------------------------------------------------------------------

    def _check_consecutive_loss_pause(self) -> bool:
        max_losses = self.config.max_consecutive_losses
        if max_losses <= 0:
            return False
        if self._consecutive_losses >= max_losses:
            cooldown = self.config.consecutive_loss_cooldown
            now = self.market_data_provider.time()
            if self._last_loss_timestamp is not None:
                elapsed = now - self._last_loss_timestamp
                if elapsed < cooldown:
                    return True
                else:
                    self._consecutive_losses = 0
                    self._last_loss_timestamp = None
        return False

    def _track_executor_result(self, executor) -> None:
        if not hasattr(executor, "net_pnl_quote") or executor.net_pnl_quote is None:
            return
        try:
            net_pnl_quote = Decimal(str(executor.net_pnl_quote))
        except (InvalidOperation, TypeError, ValueError):
            return
        if net_pnl_quote < Decimal("0"):
            self._consecutive_losses += 1
            self._last_loss_timestamp = self.market_data_provider.time()
        elif net_pnl_quote > Decimal("0"):
            self._consecutive_losses = 0
            self._last_loss_timestamp = None

    # -----------------------------------------------------------------------
    # ATR computation (Welles Wilder, from v1)
    # -----------------------------------------------------------------------

    def _compute_atr_series(self, df: pd.DataFrame, length: int) -> Tuple[Optional[pd.Series], Optional[float]]:
        required_cols = {"high", "low", "close"}
        if not required_cols.issubset(df.columns):
            return None, None
        if len(df) < 2:
            return None, None
        try:
            h = df["high"].astype(float)
            low_series = df["low"].astype(float)
            c = df["close"].astype(float)
            prev_c = c.shift(1)
            tr = pd.concat([h - low_series, (h - prev_c).abs(), (low_series - prev_c).abs()], axis=1).max(axis=1)
            atr = tr.ewm(alpha=1.0 / length, min_periods=length, adjust=False).mean()
            last_val = float(atr.iloc[-1]) if pd.notna(atr.iloc[-1]) else None
            return atr, last_val
        except _INDICATOR_ERRORS:
            return None, None

    # -----------------------------------------------------------------------
    # Adaptive cooldown (from v1, simplified - no inventory spacing)
    # -----------------------------------------------------------------------

    def _effective_cooldown_time(self) -> int:
        base_cd = self.config.cooldown_time or 0
        if base_cd <= 0:
            self._current_cooldown = 0
            return 0
        if not self.config.use_adaptive_filters:
            self._current_cooldown = base_cd
            return base_cd
        atr_pct = self._latest_atr_pct
        min_cd = max(0, min(int(self.config.min_cooldown_time), base_cd))
        low = self.config.cooldown_atr_low_pct or 0.0
        high = self.config.cooldown_atr_high_pct or (low + 1e-6)
        effective = float(base_cd)
        if atr_pct is not None and high > 0 and high > low:
            clamped = min(max(atr_pct, low), high)
            span = high - low if high != low else 1.0
            ratio = max(0.0, min(1.0, (clamped - low) / span))
            effective = base_cd - (base_cd - min_cd) * ratio
        effective = max(min_cd, min(base_cd, effective))
        self._current_cooldown = int(round(effective))
        return self._current_cooldown

    # -----------------------------------------------------------------------
    # Adaptive price gap (from v1, simplified - no trend/inventory multipliers)
    # -----------------------------------------------------------------------

    def _effective_price_gap(self) -> Decimal:
        base_gap = self.config.executor_price_gap_threshold or Decimal("0")
        if not self.config.use_adaptive_filters:
            self._current_gap_threshold = base_gap
            return base_gap
        atr_pct = self._latest_atr_pct
        min_gap = self.config.min_executor_price_gap_threshold
        max_gap = self.config.max_executor_price_gap_threshold
        multiplier = Decimal(str(self.config.price_gap_atr_multiplier))
        dynamic_gap: Optional[Decimal] = None
        if atr_pct is not None and atr_pct > 0 and multiplier > 0:
            try:
                dynamic_gap = Decimal(str(atr_pct)) * multiplier
            except (InvalidOperation, ValueError):
                dynamic_gap = None
        gap = dynamic_gap if dynamic_gap is not None and dynamic_gap > 0 else base_gap
        if gap is None:
            gap = Decimal("0")
        if min_gap is not None and min_gap > 0:
            gap = max(gap, min_gap)
        if max_gap is not None and max_gap > 0:
            gap = min(gap, max_gap)
        if (gap is None or gap <= 0) and base_gap > 0:
            gap = base_gap
        self._current_gap_threshold = gap
        return gap

    def _has_sufficient_price_gap(self, side: TradeType, new_price: Optional[Decimal]) -> bool:
        gap_thr = (
            Decimal(str(self.config.sell_executor_price_gap_threshold or Decimal("0")))
            if side == TradeType.SELL
            else self._effective_price_gap()
        )
        if gap_thr <= 0:
            return True
        if new_price is None or new_price <= 0:
            return False
        ref_price = self._last_same_side_ref_price(side)
        if ref_price is None or ref_price <= 0:
            return True
        pct_gap = abs(new_price - ref_price) / ref_price
        return pct_gap >= gap_thr

    def _has_sufficient_avg_entry_gap_for_buy(self, new_price: Optional[Decimal]) -> bool:
        gap_thr = self._effective_price_gap()
        if gap_thr <= 0:
            return True
        if new_price is None or new_price <= 0:
            return False
        avg_entry = self._get_position_cost_basis()
        if avg_entry is None or avg_entry <= 0:
            return True
        pct_gap = abs(new_price - avg_entry) / avg_entry
        return pct_gap >= gap_thr

    def _last_same_side_ref_price(self, side: TradeType) -> Optional[Decimal]:
        if side == TradeType.SELL:
            candidates = self.filter_executors(
                executors=self.executors_info,
                filter_func=lambda ex: (
                    ex.connector_name == self.config.connector_name
                    and ex.trading_pair == self.config.trading_pair
                    and ex.side == side
                    and (ex.is_active or self._executor_has_fill(ex))
                ),
            )
        else:
            candidates = self._filter_same_side(side, active_only=True)
        if not candidates:
            return None
        latest = max(
            candidates,
            key=lambda x: float(getattr(x, "close_timestamp", None) or getattr(x, "timestamp", 0.0) or 0.0),
        )
        return self._executor_ref_price(latest)

    def _executor_ref_price(self, executor_info) -> Optional[Decimal]:
        try:
            config = getattr(executor_info, "config", None)
            ci = executor_info.custom_info or {}
            for candidate in (
                getattr(config, "price", None),
                getattr(config, "entry_price", None),
                ci.get("order_price"),
                ci.get("entry_price"),
                ci.get("current_position_average_price"),
            ):
                if candidate is not None and float(candidate) > 0:
                    return Decimal(str(candidate))
        except _DECIMAL_ATTRIBUTE_ERRORS:
            pass
        return None

    def _get_min_price_increment(self) -> Optional[Decimal]:
        try:
            rule = self.market_data_provider.get_trading_rules(
                self.config.connector_name,
                self.config.trading_pair,
            )
        except _PROVIDER_ERRORS:
            return None
        try:
            tick_size = getattr(rule, "min_price_increment", None)
            tick = Decimal(str(tick_size)) if tick_size is not None else None
            if tick is not None and tick > 0:
                return tick
        except _DECIMAL_CONVERSION_ERRORS:
            return None
        return None

    def _align_limit_maker_price(
        self,
        *,
        trade_type: TradeType,
        candidate_price: Decimal,
        touch_price: Decimal,
        tick: Decimal,
    ) -> Decimal:
        if tick <= 0:
            return candidate_price

        rounding = ROUND_FLOOR if trade_type == TradeType.BUY else ROUND_CEILING
        aligned_price = (candidate_price / tick).to_integral_value(rounding=rounding) * tick

        if trade_type == TradeType.BUY and aligned_price >= touch_price:
            adjusted_price = aligned_price - tick
            return adjusted_price if adjusted_price > Decimal("0") else aligned_price
        if trade_type == TradeType.SELL and aligned_price <= touch_price:
            return aligned_price + tick
        return aligned_price

    def _shade_limit_maker_price(
        self,
        *,
        trade_type: TradeType,
        fallback_price: Decimal,
    ) -> Decimal:
        price_type = PriceType.BestBid if trade_type == TradeType.BUY else PriceType.BestAsk
        try:
            ref_price_val = self.market_data_provider.get_price_by_type(
                self.config.connector_name,
                self.config.trading_pair,
                price_type,
            )
            ref_price = Decimal(str(ref_price_val)) if ref_price_val is not None else fallback_price
        except _MARKET_DATA_ERRORS:
            ref_price = fallback_price

        tick = self._get_min_price_increment()
        if tick is None or tick <= 0:
            return ref_price

        candidate_price = min(fallback_price, ref_price) if trade_type == TradeType.BUY else max(fallback_price, ref_price)
        shaded_price = self._align_limit_maker_price(
            trade_type=trade_type,
            candidate_price=candidate_price,
            touch_price=ref_price,
            tick=tick,
        )
        return shaded_price if shaded_price > 0 else ref_price

    def _has_buy_inventory(self) -> bool:
        if self.config.trade_mode == "sell_only":
            return self._get_sellable_inventory_amount() > 0
        return self._position_amount(TradeType.BUY) > 0

    def _should_emit_live_buy_reversal(self, buy_signal_on_bar: bool, candle_timestamp: Optional[float]) -> bool:
        if not buy_signal_on_bar:
            return False
        if candle_timestamp is None:
            return True
        if (
            self._last_buy_signal_candle_ts is not None
            and candle_timestamp <= self._last_buy_signal_candle_ts
        ):
            return False
        self._last_buy_signal_candle_ts = candle_timestamp
        return True

    def _compute_raw_buy_reversal_components(self, rsi: pd.Series, buy_thr: float) -> Dict[str, pd.Series]:
        reversal_window = max(3, self.config.rsi_length // 2)
        rsi_prev = rsi.shift(1)
        prev_was_min = rsi_prev == rsi.rolling(window=reversal_window, min_periods=2).min()
        turning_up = rsi > rsi_prev
        was_oversold = rsi.rolling(window=reversal_window, min_periods=1).min() <= buy_thr
        near_bottom = rsi <= buy_thr * 1.3
        raw_reversal = prev_was_min & turning_up & was_oversold & near_bottom

        debounce_window = reversal_window * 4
        prev_fire = raw_reversal.shift(1).rolling(
            window=debounce_window, min_periods=1,
        ).max().fillna(0).astype(bool)
        debounced_reversal = raw_reversal & ~prev_fire

        return {
            "prev_was_min": prev_was_min.fillna(False),
            "turning_up": turning_up.fillna(False),
            "was_oversold": was_oversold.fillna(False),
            "near_bottom": near_bottom.fillna(False),
            "raw_reversal": raw_reversal.fillna(False),
            "debounced_reversal": debounced_reversal.fillna(False),
        }

    def _compute_raw_buy_reversal_series(self, rsi: pd.Series, buy_thr: float) -> pd.Series:
        return self._compute_raw_buy_reversal_components(rsi, buy_thr)["debounced_reversal"]

    def _compute_signal_score_details(
        self,
        *,
        last_rsi: Optional[float],
        last_close: Optional[float],
        bb_lower: Optional[float],
        macd_hist: Optional[float],
        macd_hist_prev: Optional[float],
        rsi_buy_threshold: float,
        mean_reversion_score: float,
    ) -> Dict[str, object]:
        score_rsi_oversold = last_rsi is not None and last_rsi <= rsi_buy_threshold
        score_bb_touch = (
            score_rsi_oversold
            and last_close is not None
            and bb_lower is not None
            and last_close <= bb_lower
        )
        if macd_hist is not None and macd_hist_prev is not None:
            score_macd_turn = score_rsi_oversold and macd_hist > macd_hist_prev and (macd_hist > 0 or macd_hist_prev < 0)
        else:
            score_macd_turn = score_rsi_oversold and macd_hist is not None and macd_hist > 0
        score_mean_reversion = score_rsi_oversold and mean_reversion_score >= 0.5

        score = 0
        if score_rsi_oversold:
            score = 1
            if score_bb_touch:
                score += 1
            if score_macd_turn:
                score += 1
            if score_mean_reversion:
                score += 1

        return {
            "score": score,
            "score_rsi_oversold": score_rsi_oversold,
            "score_bb_touch": score_bb_touch,
            "score_macd_turn": score_macd_turn,
            "score_mean_reversion": score_mean_reversion,
        }

    def _build_buy_decision_state(
        self,
        *,
        current_signal: int,
        last_rsi: Optional[float],
        last_close: Optional[float],
        signal_score: int,
        raw_buy_reversal: bool,
        condition_ok: bool,
        condition_reason: str,
        buy_entry_role: str = "none",
        buy_entry_fraction: float = 0.0,
        buy_context_bias: str = "neutral",
    ) -> Dict[str, object]:
        min_score = max(1, int(self.config.min_signal_score))
        pending_setup = self._pending_buy_confirmation
        decision = "watch"
        reason = "monitoring"
        rebound_delta: Optional[float] = None
        rebound_target: Optional[float] = None
        price_rebounded: Optional[bool] = None

        if last_rsi is None:
            decision = "blocked"
            reason = "no-rsi"
        elif current_signal > 0:
            decision = "ready"
            reason = "signal-generated"
        elif not condition_ok:
            decision = "blocked"
            reason = condition_reason or "filter"
        elif pending_setup is not None and self.config.buy_confirmation_mode == "rebound_confirm":
            decision = "armed"
            rebound_target = max(float(self.config.buy_confirmation_rsi_delta), 0.0)
            rebound_delta = max(0.0, last_rsi - pending_setup.trough_rsi)
            price_rebounded = (
                last_close is not None
                and pending_setup.trough_price > 0
                and last_close > pending_setup.trough_price
            )
            if not pending_setup.score_qualified():
                reason = "need-score"
            elif rebound_target > rebound_delta:
                reason = "need-rebound"
            elif not price_rebounded:
                reason = "need-price-rebound"
            else:
                reason = "await-next-candle"
        elif not raw_buy_reversal:
            decision = "blocked"
            reason = "no-reversal"
        elif signal_score < min_score:
            decision = "blocked"
            reason = "need-score"
        else:
            decision = "watch"
            reason = "await-confirmation"

        return {
            "buy_decision": decision,
            "buy_reason": self._normalize_reason(reason),
            "buy_confirmation_active": pending_setup is not None,
            "buy_confirmation_rebound_delta": rebound_delta,
            "buy_confirmation_rebound_target": rebound_target,
            "buy_confirmation_price_rebounded": price_rebounded,
            "buy_entry_role": buy_entry_role,
            "buy_entry_fraction": buy_entry_fraction,
            "buy_context_bias": buy_context_bias,
        }

    def _build_sell_decision_state(
        self,
        *,
        current_signal: int,
        last_rsi: Optional[float],
        rsi_prev: Optional[float],
        rsi_sell_eff: float,
        cost_basis: Optional[Decimal],
        mid_price: Optional[Decimal],
        ema_fast: Optional[float],
        macd_hist: Optional[float],
        macd_hist_prev: Optional[float],
        has_inventory: bool,
    ) -> Dict[str, object]:
        decision = "watch"
        reason = "monitoring"
        pnl_pct: Optional[float] = None
        profitability_ok = False
        reversal_state = self._build_sell_reversal_state(
            last_rsi=last_rsi,
            rsi_prev=rsi_prev,
            current_price=mid_price,
            ema_fast=ema_fast,
            macd_hist=macd_hist,
            macd_hist_prev=macd_hist_prev,
        )
        trend_hold_active = False

        net_pnl_pct = self._inventory_net_pnl_pct(mid_price)
        if net_pnl_pct is not None:
            pnl_pct = float(net_pnl_pct)
        elif cost_basis is not None and mid_price is not None and cost_basis > 0:
            pnl_pct = float((mid_price - cost_basis) / cost_basis)

        if current_signal < 0:
            decision = "ready"
            reason = "signal-generated"
            profitability_ok = True
        elif last_rsi is None:
            decision = "blocked"
            reason = "no-rsi"
        elif not has_inventory:
            decision = "blocked"
            reason = "inventory-flat"
        elif last_rsi < rsi_sell_eff:
            decision = "watch"
            reason = "need-rsi"
        elif not reversal_state["sell_reversal_confirmed"]:
            decision = "watch"
            reason = "need-reversal"
        elif self.config.trend_confirmation_enabled:
            trend_hold_active, _ = self._sell_trend_hold_active(
                current_price=mid_price,
                current_rsi=last_rsi,
                rsi_prev=rsi_prev,
                ema_fast=ema_fast,
            )
            if trend_hold_active:
                decision = "watch"
                reason = "trend-hold"
            elif not self.config.sell_only_if_profitable:
                decision = "watch"
                reason = "await-sell-trigger"
                profitability_ok = True
            elif cost_basis is None or cost_basis <= 0:
                decision = "blocked"
                reason = "no-cost-basis"
            elif mid_price is None or mid_price <= 0:
                decision = "blocked"
                reason = "no-mid-price"
            else:
                profitability_ok = pnl_pct is not None and pnl_pct >= float(self.config.min_profit_pct_for_sell)
                if profitability_ok:
                    decision = "watch"
                    reason = "await-sell-trigger"
                else:
                    decision = "blocked"
                    reason = "need-profit"
        elif not self.config.sell_only_if_profitable:
            decision = "watch"
            reason = "await-sell-trigger"
            profitability_ok = True
        elif cost_basis is None or cost_basis <= 0:
            decision = "blocked"
            reason = "no-cost-basis"
        elif mid_price is None or mid_price <= 0:
            decision = "blocked"
            reason = "no-mid-price"
        else:
            profitability_ok = pnl_pct is not None and pnl_pct >= float(self.config.min_profit_pct_for_sell)
            if profitability_ok:
                decision = "watch"
                reason = "await-sell-trigger"
            else:
                decision = "blocked"
                reason = "need-profit"

        return {
            "sell_decision": decision,
            "sell_reason": self._normalize_reason(reason),
            "sell_has_inventory": has_inventory,
            "sell_pnl_pct": pnl_pct,
            "sell_profitability_ok": profitability_ok,
            **reversal_state,
            "sell_trend_hold": trend_hold_active,
        }

    @staticmethod
    def _is_buy_log_zone(
        *,
        last_rsi: Optional[float],
        rsi_buy_eff: float,
        raw_buy_reversal: bool,
        signal_score: int,
        pending_setup_active: bool,
        current_signal: int,
    ) -> bool:
        return bool(
            current_signal > 0
            or pending_setup_active
            or raw_buy_reversal
            or signal_score > 0
            or (last_rsi is not None and last_rsi <= rsi_buy_eff)
        )

    @staticmethod
    def _is_sell_log_zone(
        *,
        last_rsi: Optional[float],
        rsi_sell_eff: float,
        current_signal: int,
    ) -> bool:
        return bool(
            current_signal < 0
            or (last_rsi is not None and last_rsi >= rsi_sell_eff)
        )

    def _evaluate_buy_confirmation_step(
        self,
        *,
        timestamp: float,
        last_rsi: Optional[float],
        last_close: Optional[float],
        signal_score: int,
        raw_buy_candidate: bool,
        pending_setup: Optional[BuyConfirmationSetup],
    ) -> Tuple[bool, Optional[BuyConfirmationSetup], bool]:
        if self.config.buy_confirmation_mode == "none":
            return raw_buy_candidate, None, False

        if last_rsi is None or last_close is None:
            return False, None, False

        setup = pending_setup
        max_wait_seconds = max(0, int(self.config.buy_confirmation_max_wait_seconds))
        if setup is not None and max_wait_seconds > 0 and (timestamp - setup.armed_timestamp) > max_wait_seconds:
            setup = None

        armed = False
        if raw_buy_candidate and setup is None:
            setup = BuyConfirmationSetup(
                armed_timestamp=timestamp,
                trough_rsi=last_rsi,
                trough_price=last_close,
                armed_signal_score=signal_score,
                armed_min_signal_score=max(1, int(self.config.min_signal_score)),
            )
            armed = True
            return False, setup, armed

        if setup is None:
            return False, None, armed

        if signal_score > setup.armed_signal_score:
            setup.armed_signal_score = signal_score
        if setup.armed_min_signal_score <= 0:
            setup.armed_min_signal_score = max(1, int(self.config.min_signal_score))
        if last_rsi < setup.trough_rsi:
            setup.trough_rsi = last_rsi
        if last_close < setup.trough_price:
            setup.trough_price = last_close

        rsi_rebounded = last_rsi >= (setup.trough_rsi + self.config.buy_confirmation_rsi_delta)
        price_rebounded = last_close > setup.trough_price
        confirmed = setup.score_qualified() and rsi_rebounded and price_rebounded
        if confirmed:
            return True, None, armed

        return False, setup, armed

    def _restore_buy_confirmation_after_veto(self, previous_setup: Optional[BuyConfirmationSetup]):
        if (
            self.config.buy_confirmation_mode == "rebound_confirm"
            and previous_setup is not None
            and self._pending_buy_confirmation is None
        ):
            self._pending_buy_confirmation = previous_setup

    # -----------------------------------------------------------------------
    # Regime-aware RSI thresholds (from v1)
    # -----------------------------------------------------------------------

    def _get_dynamic_rsi_thresholds(self, regime_label: Optional[str], confidence: float) -> Tuple[float, float]:
        base_buy = float(self.config.rsi_buy_threshold)
        base_sell = float(self.config.sell_rsi_overbought)
        if regime_label is None:
            return base_buy, base_sell
        c = max(0.0, min(1.0, float(confidence)))
        max_adjust = 3.0 * c
        if "Range" in regime_label:
            buy_eff = base_buy - max_adjust
            sell_eff = base_sell + max_adjust
        elif "Trend_Up" in regime_label:
            buy_eff = base_buy
            sell_eff = base_sell + (max_adjust * 0.7)
        elif "Trend_Down" in regime_label:
            buy_eff = base_buy - (max_adjust * 0.5)
            sell_eff = base_sell - max_adjust
        else:
            return base_buy, base_sell
        buy_eff = max(20.0, min(40.0, buy_eff))
        sell_eff = max(60.0, min(80.0, sell_eff))
        return float(buy_eff), float(sell_eff)

    # -----------------------------------------------------------------------
    # Flash crash detection
    # -----------------------------------------------------------------------

    def _detect_flash_crash(self, df: pd.DataFrame) -> bool:
        """Return True if a flash crash is detected in the recent candle window."""
        lookback = self.config.flash_crash_lookback
        if df is None or len(df) < lookback + 1:
            return False
        try:
            recent_high = float(df["high"].iloc[-(lookback + 1)])
            recent_low = float(df["low"].iloc[-1])
            if recent_high <= 0:
                return False
            drop_pct = (recent_high - recent_low) / recent_high
            return drop_pct >= self.config.flash_crash_drop_pct
        except (KeyError, IndexError, ValueError, TypeError):
            return False

    def _is_flash_crash_cooldown(self, now_ts: float) -> bool:
        return now_ts < self._flash_crash_until

    # -----------------------------------------------------------------------
    # Signal pipeline
    # -----------------------------------------------------------------------

    @staticmethod
    def _interval_to_seconds(interval: Optional[str]) -> Optional[float]:
        if interval is None:
            return None
        text = str(interval).strip().lower()
        if not text:
            return None
        unit = text[-1]
        try:
            value = float(text[:-1])
        except ValueError:
            return None
        multipliers = {
            "s": 1.0,
            "m": 60.0,
            "h": 3600.0,
            "d": 86400.0,
            "w": 604800.0,
        }
        multiplier = multipliers.get(unit)
        if multiplier is None or value <= 0:
            return None
        return value * multiplier

    @staticmethod
    def _timestamp_to_seconds(timestamp: float) -> float:
        return timestamp / 1000.0 if timestamp > 10_000_000_000 else timestamp

    @staticmethod
    def _finite_numeric_series(series: pd.Series) -> bool:
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.isna().any():
            return False
        return bool(numeric.map(lambda value: float("-inf") < float(value) < float("inf")).all())

    def _build_data_health(self, df: pd.DataFrame, now_ts: float) -> Dict[str, object]:
        health: Dict[str, object] = {
            "ready": False,
            "reason": "unknown",
            "row_count": int(len(df)) if df is not None else 0,
            "last_candle_ts": None,
            "candle_age_seconds": None,
            "interval_seconds": self._interval_to_seconds(self.config.interval),
        }
        if df is None or df.empty:
            health["reason"] = "no_candles"
            return health
        if "timestamp" not in df.columns:
            health["reason"] = "missing_timestamp"
            return health

        timestamps = pd.to_numeric(df["timestamp"], errors="coerce")
        if timestamps.isna().any():
            health["reason"] = "invalid_timestamp"
            return health
        normalized_ts = timestamps.map(lambda value: self._timestamp_to_seconds(float(value)))
        if normalized_ts.duplicated().any():
            health["reason"] = "duplicate_timestamp"
            return health
        if not normalized_ts.is_monotonic_increasing:
            health["reason"] = "out_of_order_timestamp"
            return health

        interval_seconds = health["interval_seconds"]
        if interval_seconds is not None:
            timestamp_deltas = normalized_ts.diff().iloc[1:]
            if timestamp_deltas.gt(float(interval_seconds)).any():
                health["reason"] = "missing_interval"
                return health
            if timestamp_deltas.ne(float(interval_seconds)).any():
                health["reason"] = "unexpected_interval"
                return health

        last_ts = float(normalized_ts.iloc[-1])
        health["last_candle_ts"] = last_ts
        candle_age = float(now_ts) - last_ts
        health["candle_age_seconds"] = candle_age
        if interval_seconds is not None:
            max_age = float(interval_seconds) * float(self.config.max_candle_age_intervals)
            if candle_age > max_age:
                health["reason"] = "stale_candle"
                return health

        required_cols = ("open", "high", "low", "close")
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            health["reason"] = f"missing_{missing_cols[0]}"
            return health
        for col in required_cols:
            if not self._finite_numeric_series(df[col]):
                health["reason"] = f"invalid_{col}"
                return health

        health["ready"] = True
        health["reason"] = "ok"
        return health

    async def update_processed_data(self):
        if not self.market_data_provider:
            self.logger().warning(
                f"Market data provider unavailable | {self._controller_pair_log_prefix()}"
            )
            return

        now_ts = self.market_data_provider.time()
        self._last_processed_timestamp = now_ts

        df = self.market_data_provider.get_candles_df(
            connector_name=self.config.candles_connector or self.config.connector_name,
            trading_pair=self.config.candles_trading_pair or self.config.trading_pair,
            interval=self.config.interval,
            max_records=self.max_records,
        )
        if df is None or df.empty:
            self.processed_data = {
                "signal": 0,
                "features": pd.DataFrame(),
                "data_health": {
                    "ready": False,
                    "reason": "no_candles",
                    "row_count": 0,
                    "last_candle_ts": None,
                    "candle_age_seconds": None,
                    "interval_seconds": self._interval_to_seconds(self.config.interval),
                },
            }
            return
        df = df.copy()
        signal_series: Optional[pd.Series] = None
        last_candle_ts: Optional[float] = None
        data_health = self._build_data_health(df, now_ts)
        if data_health.get("last_candle_ts") is not None:
            last_candle_ts = float(data_health["last_candle_ts"])
        if not bool(data_health.get("ready")):
            self._restore_buy_confirmation_after_veto(self._pending_buy_confirmation)
            self.processed_data = {
                "signal": 0,
                "features": df,
                "data_health": data_health,
                "timestamp": now_ts,
                "candle_timestamp": last_candle_ts,
                "signal_state": {
                    "condition_ok": False,
                    "condition_reason": data_health.get("reason", "data_unhealthy"),
                },
            }
            self._log_signal_transition(
                key="data_health",
                signature=f"{data_health.get('reason')}|{data_health.get('last_candle_ts')}",
                message=(
                    f"Data not ready | {self._controller_pair_log_prefix()} "
                    f"reason={data_health.get('reason')} "
                    f"last_candle_ts={data_health.get('last_candle_ts')} "
                    f"age={self._fmt(data_health.get('candle_age_seconds'), 2)}s"
                ),
                level=logging.WARNING,
            )
            return
        try:
            if last_candle_ts is None and "timestamp" in df.columns and pd.notna(df["timestamp"].iloc[-1]):
                last_candle_ts = float(df["timestamp"].iloc[-1])
        except _INDICATOR_ERRORS:
            last_candle_ts = None

        if (
            last_candle_ts is not None
            and self._last_evaluated_candle_timestamp is not None
            and last_candle_ts <= self._last_evaluated_candle_timestamp
        ):
            suppression_reason = (
                "same_candle"
                if last_candle_ts == self._last_evaluated_candle_timestamp
                else "non_advancing_candle"
            )
            previous_data = dict(self.processed_data or {})
            previous_data.update(
                {
                    "signal": 0,
                    "timestamp": now_ts,
                    "candle_timestamp": last_candle_ts,
                    "data_health": data_health,
                    "signal_suppression_reason": suppression_reason,
                }
            )
            self.processed_data = previous_data
            self._log_signal_transition(
                key="candle_identity",
                signature=f"{suppression_reason}|{last_candle_ts}",
                message=(
                    f"Signal evaluation suppressed | {self._controller_pair_log_prefix()} "
                    f"reason={suppression_reason} candle_timestamp={last_candle_ts}"
                ),
            )
            return

        last_close: Optional[float] = None
        last_rsi: Optional[float] = None
        rsi_prev: Optional[float] = None
        atr_pct: Optional[float] = None
        last_atr: Optional[float] = None
        ema_fast: Optional[float] = None
        ema_slow: Optional[float] = None
        bb_lower: Optional[float] = None
        bb_upper: Optional[float] = None
        bb_mid: Optional[float] = None
        macd_hist: Optional[float] = None
        macd_hist_prev: Optional[float] = None

        # --- RSI ---
        try:
            last_close = float(df["close"].iloc[-1]) if "close" in df.columns else None
            df.ta.rsi(length=self.config.rsi_length, append=True)
            rsi_col = f"RSI_{self.config.rsi_length}"
            if rsi_col in df.columns:
                last_rsi = float(df[rsi_col].iloc[-1]) if pd.notna(df[rsi_col].iloc[-1]) else None
                if len(df) > 1 and pd.notna(df[rsi_col].iloc[-2]):
                    rsi_prev = float(df[rsi_col].iloc[-2])
        except _INDICATOR_ERRORS as e:
            self.logger().error(
                f"RSI error | {self._controller_pair_log_prefix()} error={type(e).__name__}: {e}",
                exc_info=True,
            )

        # --- ATR ---
        try:
            atr_series, atr_value = self._compute_atr_series(df, self.config.atr_length)
            if atr_series is not None:
                df[f"ATR_{self.config.atr_length}"] = atr_series
            if atr_value is not None:
                last_atr = atr_value
                if last_close and last_close > 0:
                    atr_pct = atr_value / last_close
        except _INDICATOR_ERRORS:
            pass

        # --- EMAs ---
        try:
            df.ta.ema(length=self.config.ema_fast_length, append=True)
            df.ta.ema(length=self.config.ema_slow_length, append=True)
            ema_fast_col = f"EMA_{self.config.ema_fast_length}"
            ema_slow_col = f"EMA_{self.config.ema_slow_length}"
            if ema_fast_col in df.columns and pd.notna(df[ema_fast_col].iloc[-1]):
                ema_fast = float(df[ema_fast_col].iloc[-1])
            if ema_slow_col in df.columns and pd.notna(df[ema_slow_col].iloc[-1]):
                ema_slow = float(df[ema_slow_col].iloc[-1])
        except _INDICATOR_ERRORS:
            pass

        # --- Bollinger Bands ---
        try:
            df.ta.bbands(length=self.config.bb_length, std=self.config.bb_std, append=True)
            bbl_col = f"BBL_{self.config.bb_length}_{self.config.bb_std}"
            bbu_col = f"BBU_{self.config.bb_length}_{self.config.bb_std}"
            bbm_col = f"BBM_{self.config.bb_length}_{self.config.bb_std}"
            if bbl_col in df.columns and pd.notna(df[bbl_col].iloc[-1]):
                bb_lower = float(df[bbl_col].iloc[-1])
            if bbu_col in df.columns and pd.notna(df[bbu_col].iloc[-1]):
                bb_upper = float(df[bbu_col].iloc[-1])
            if bbm_col in df.columns and pd.notna(df[bbm_col].iloc[-1]):
                bb_mid = float(df[bbm_col].iloc[-1])
        except _INDICATOR_ERRORS:
            pass

        # --- MACD ---
        try:
            df.ta.macd(fast=self.config.macd_fast, slow=self.config.macd_slow, signal=self.config.macd_signal, append=True)
            hist_col = f"MACDh_{self.config.macd_fast}_{self.config.macd_slow}_{self.config.macd_signal}"
            if hist_col in df.columns and pd.notna(df[hist_col].iloc[-1]):
                macd_hist = float(df[hist_col].iloc[-1])
                if len(df) > 1 and pd.notna(df[hist_col].iloc[-2]):
                    macd_hist_prev = float(df[hist_col].iloc[-2])
        except _INDICATOR_ERRORS:
            pass

        self._latest_atr_pct = atr_pct

        # --- Flash crash detection ---
        if self._detect_flash_crash(df):
            self._flash_crash_until = now_ts + self.config.flash_crash_cooldown
            self.logger().warning(
                f"Flash crash detected | {self._controller_pair_log_prefix()} "
                f"cooldown until {self._flash_crash_until:.0f} "
                f"({self.config.flash_crash_cooldown}s)"
            )

        trend_confirmation: Optional[MultiTimeframeTrend] = None
        if self.config.trend_confirmation_enabled:
            trend_confirmation = analyze_rsi_trend_confirmation(
                df,
                medium_df=self._get_interval_candles_df(self.config.trend_confirmation_medium_interval),
                long_df=self._get_interval_candles_df(self.config.trend_confirmation_long_interval),
                adx_threshold=self.config.trend_confirmation_adx_threshold,
            )

        # --- Regime ---
        regime: Optional[MarketRegime] = None
        regime_error: Optional[str] = None
        try:
            if not getattr(self._regime_detector, "_is_fit", False) and len(df) >= 220:
                self._regime_detector.fit(df)
            regime = self._regime_detector.detect(df)
        except StrictHMMRequiredError as e:
            regime_error = str(e)
            self._log_signal_transition(
                key="regime_error",
                signature=regime_error,
                message=(
                    f"Regime unavailable | {self._controller_pair_log_prefix()} "
                    f"reason={regime_error}"
                ),
                level=logging.WARNING,
            )
            regime = None
        except _INDICATOR_ERRORS as e:
            regime_error = f"{type(e).__name__}: {e}"
            self._log_signal_transition(
                key="regime_error",
                signature=regime_error,
                message=(
                    f"Regime detection error | {self._controller_pair_log_prefix()} "
                    f"reason={regime_error}"
                ),
                level=logging.WARNING,
            )
            regime = None

        regime_label = regime.regime_label if regime is not None else None
        regime_conf = float(regime.confidence) if regime is not None and regime.confidence is not None else 0.0
        rsi_buy_eff, rsi_sell_eff = self._get_dynamic_rsi_thresholds(regime_label, regime_conf)
        rsi_buy_eff, rsi_sell_eff = self._apply_trend_confirmation_to_thresholds(
            rsi_buy_eff=rsi_buy_eff,
            rsi_sell_eff=rsi_sell_eff,
            trend_confirmation=trend_confirmation,
        )
        buy_context_bias = self._classify_buy_context(
            regime_label=regime_label,
            trend_confirmation=trend_confirmation,
        )

        # --- Volume & mean reversion analysis ---
        volume_ratio: Optional[float] = None
        mean_reversion_score: float = 0.0
        try:
            volume_ratio = calculate_volume_ratio(df)
        except _INDICATOR_ERRORS:
            pass
        if last_rsi is not None:
            try:
                mean_reversion_score = calculate_rsi_mean_reversion_score(last_rsi)
            except (TypeError, ValueError, ArithmeticError):
                pass

        # --- BUY reversal detection ---
        raw_buy_reversal = False
        raw_reversal_components: Dict[str, bool] = {
            "prev_was_min": False,
            "turning_up": False,
            "was_oversold": False,
            "near_bottom": False,
        }
        try:
            rsi_col = f"RSI_{self.config.rsi_length}"
            if rsi_col in df.columns:
                rsi_series = pd.to_numeric(df[rsi_col], errors="coerce")
                raw_reversal_series = self._compute_raw_buy_reversal_components(rsi_series, rsi_buy_eff)
                raw_buy_reversal = bool(
                    not raw_reversal_series["debounced_reversal"].empty
                    and raw_reversal_series["debounced_reversal"].iloc[-1]
                )
                raw_reversal_components = {
                    "prev_was_min": bool(raw_reversal_series["prev_was_min"].iloc[-1]),
                    "turning_up": bool(raw_reversal_series["turning_up"].iloc[-1]),
                    "was_oversold": bool(raw_reversal_series["was_oversold"].iloc[-1]),
                    "near_bottom": bool(raw_reversal_series["near_bottom"].iloc[-1]),
                }
        except _INDICATOR_ERRORS:
            raw_buy_reversal = False

        # --- Signal scoring (RSI is mandatory gate) ---
        score_details = self._compute_signal_score_details(
            last_rsi=last_rsi,
            last_close=last_close,
            bb_lower=bb_lower,
            macd_hist=macd_hist,
            macd_hist_prev=macd_hist_prev,
            rsi_buy_threshold=rsi_buy_eff,
            mean_reversion_score=mean_reversion_score,
        )
        signal_score = int(score_details["score"])

        # --- Determine BUY/SELL ---
        pending_setup_before_signal = self._pending_buy_confirmation
        if regime_error is not None and self.config.strict_hmm_mode:
            signal = 0
        else:
            signal = self._determine_signal(
                last_rsi=last_rsi,
                signal_score=signal_score,
                rsi_buy_eff=rsi_buy_eff,
                rsi_sell_eff=rsi_sell_eff,
                last_close=last_close,
                rsi_prev=rsi_prev,
                ema_fast=ema_fast,
                macd_hist=macd_hist,
                macd_hist_prev=macd_hist_prev,
                raw_buy_reversal=raw_buy_reversal,
                signal_timestamp=now_ts,
                candle_timestamp=last_candle_ts,
            )

        # --- Market condition filter for BUY ---
        condition_ok = True
        condition_reason = "ok"
        if regime_error is not None and self.config.strict_hmm_mode:
            condition_ok = False
            condition_reason = "strict_hmm"
            signal = 0
        elif signal > 0 and atr_pct is not None:
            if atr_pct < self.config.min_atr_pct_to_trade:
                self._restore_buy_confirmation_after_veto(pending_setup_before_signal)
                condition_ok = False
                condition_reason = "low_vol"
                signal = 0
            elif atr_pct > self.config.max_atr_pct_to_trade:
                self._restore_buy_confirmation_after_veto(pending_setup_before_signal)
                condition_ok = False
                condition_reason = "high_vol"
                signal = 0
        if signal <= 0:
            self._last_buy_signal_role = None

        # --- Dynamic sizing using full signal strength ---
        size_mult = 1.0
        sig_strength = 0.0
        if self.config.dynamic_position_sizing and last_rsi is not None and atr_pct is not None:
            sig_strength = calculate_signal_strength(
                rsi=last_rsi, threshold=rsi_buy_eff,
                divergence_strength=0.0,
                volume_ratio=volume_ratio,
                regime_confidence=regime_conf,
            )
            size_mult = self.config.min_position_size_multiplier + (
                (self.config.max_position_size_multiplier - self.config.min_position_size_multiplier) * sig_strength
            )
            size_mult = max(self.config.min_position_size_multiplier, min(self.config.max_position_size_multiplier, size_mult))

        effective_cooldown = self._effective_cooldown_time()
        effective_gap = self._effective_price_gap()
        cost_basis = self._get_position_cost_basis()
        has_inventory = self._has_buy_inventory()
        buy_entry_role = self._last_buy_signal_role or ("full" if signal > 0 else "none")
        buy_entry_fraction = float(self._planned_buy_entry_fraction({
            "buy_entry_role": buy_entry_role,
            "buy_context_bias": buy_context_bias,
        }))
        buy_decision_state = self._build_buy_decision_state(
            current_signal=signal,
            last_rsi=last_rsi,
            last_close=last_close,
            signal_score=signal_score,
            raw_buy_reversal=raw_buy_reversal,
            condition_ok=condition_ok,
            condition_reason=condition_reason,
            buy_entry_role=buy_entry_role,
            buy_entry_fraction=buy_entry_fraction,
            buy_context_bias=buy_context_bias,
        )
        sell_decision_state = self._build_sell_decision_state(
            current_signal=signal,
            last_rsi=last_rsi,
            rsi_prev=rsi_prev,
            rsi_sell_eff=rsi_sell_eff,
            cost_basis=cost_basis,
            mid_price=self._get_mid_price(),
            ema_fast=ema_fast,
            macd_hist=macd_hist,
            macd_hist_prev=macd_hist_prev,
            has_inventory=has_inventory,
        )

        state = SignalState(
            timestamp=now_ts, close=last_close, rsi=last_rsi, rsi_prev=rsi_prev,
            atr=last_atr, atr_pct=atr_pct, ema_fast=ema_fast, ema_slow=ema_slow,
            regime_label=regime_label, regime_confidence=regime_conf,
            bb_lower=bb_lower, bb_upper=bb_upper, bb_mid=bb_mid,
            macd_hist=macd_hist, macd_hist_prev=macd_hist_prev,
            signal_score=signal_score, condition_ok=condition_ok,
            condition_reason=condition_reason, size_multiplier=size_mult,
            consecutive_losses=self._consecutive_losses,
            volume_ratio=volume_ratio,
            mean_reversion_score=mean_reversion_score,
            signal_strength_score=sig_strength,
            rsi_reversal=raw_buy_reversal,
            raw_buy_candidate=raw_buy_reversal and signal_score >= self.config.min_signal_score,
            raw_reversal_prev_was_min=raw_reversal_components["prev_was_min"],
            raw_reversal_turning_up=raw_reversal_components["turning_up"],
            raw_reversal_was_oversold=raw_reversal_components["was_oversold"],
            raw_reversal_near_bottom=raw_reversal_components["near_bottom"],
            score_rsi_oversold=bool(score_details["score_rsi_oversold"]),
            score_bb_touch=bool(score_details["score_bb_touch"]),
            score_macd_turn=bool(score_details["score_macd_turn"]),
            score_mean_reversion=bool(score_details["score_mean_reversion"]),
            buy_decision=str(buy_decision_state["buy_decision"]),
            buy_reason=str(buy_decision_state["buy_reason"]),
            buy_confirmation_active=bool(buy_decision_state["buy_confirmation_active"]),
            buy_confirmation_rebound_delta=buy_decision_state["buy_confirmation_rebound_delta"],
            buy_confirmation_rebound_target=buy_decision_state["buy_confirmation_rebound_target"],
            buy_confirmation_price_rebounded=buy_decision_state["buy_confirmation_price_rebounded"],
            buy_entry_role=str(buy_decision_state["buy_entry_role"]),
            buy_entry_fraction=float(buy_decision_state["buy_entry_fraction"]),
            buy_context_bias=str(buy_decision_state["buy_context_bias"]),
            sell_decision=str(sell_decision_state["sell_decision"]),
            sell_reason=str(sell_decision_state["sell_reason"]),
            sell_has_inventory=bool(sell_decision_state["sell_has_inventory"]),
            sell_pnl_pct=sell_decision_state["sell_pnl_pct"],
            sell_profitability_ok=bool(sell_decision_state["sell_profitability_ok"]),
            sell_rsi_rollover=bool(sell_decision_state["sell_rsi_rollover"]),
            sell_price_below_ema=bool(sell_decision_state["sell_price_below_ema"]),
            sell_macd_rollover=bool(sell_decision_state["sell_macd_rollover"]),
            sell_reversal_confirmed=bool(sell_decision_state["sell_reversal_confirmed"]),
            sell_trend_hold=bool(sell_decision_state["sell_trend_hold"]),
        )

        self.logger().debug(
            f"Signal eval | rsi={self._fmt(last_rsi, 2)} reversal={raw_buy_reversal} "
            f"bb_low={self._fmt(bb_lower)} macd_h={self._fmt(macd_hist)} "
            f"score={signal_score}/{self.config.min_signal_score} "
            f"regime={regime_label} buy_role={buy_entry_role} ctx={buy_context_bias} -> signal={signal}"
        )

        # Per-row signal column for BacktestingEngineBase compatibility
        try:
            if signal_series is None:
                signal_frame = self._build_backtest_signal_frame(
                    df, rsi_buy_eff=rsi_buy_eff, rsi_sell_eff=rsi_sell_eff,
                )
                signal_series = signal_frame["signal"]
                df["signal_state"] = signal_frame["signal_state"]
            df["signal"] = signal_series
        except _INDICATOR_ERRORS:
            if "signal" not in df.columns:
                df["signal"] = 0

        self.processed_data = {
            "signal": signal,
            "features": df,
            "indicators": {
                "rsi": last_rsi, "rsi_prev": rsi_prev, "atr": last_atr, "atr_pct": atr_pct,
                "ema_fast": ema_fast, "ema_slow": ema_slow,
                "bb_lower": bb_lower, "bb_upper": bb_upper, "bb_mid": bb_mid,
                "macd_hist": macd_hist, "cost_basis": cost_basis,
            },
            "regime": regime_label,
            "regime_confidence": regime_conf,
            "regime_error": regime_error,
            "trend_confirmation": self._trend_confirmation_dict(trend_confirmation),
            "timestamp": now_ts,
            "candle_timestamp": last_candle_ts,
            "data_health": data_health,
            "thresholds": {
                "rsi_buy": rsi_buy_eff, "rsi_sell": rsi_sell_eff,
                "cooldown": effective_cooldown,
                "price_gap": float(effective_gap) if effective_gap is not None else None,
            },
            "signal_state": state.as_dict(),
            "recovery_trails": {key: trail.as_dict() for key, trail in self._recovery_trails.items()},
        }
        self._last_evaluated_candle_timestamp = last_candle_ts
        if self._is_buy_log_zone(
            last_rsi=last_rsi,
            rsi_buy_eff=rsi_buy_eff,
            raw_buy_reversal=raw_buy_reversal,
            signal_score=signal_score,
            pending_setup_active=state.buy_confirmation_active,
            current_signal=signal,
        ):
            self._log_signal_transition(
                key="buy_eval",
                signature=(
                    f"{state.buy_decision}|{state.buy_reason}|{state.signal_score}|"
                    f"{state.rsi_reversal}|{state.buy_confirmation_active}|"
                    f"{self._fmt(state.buy_confirmation_rebound_delta, 2)}"
                ),
                message=(
                    f"BUY eval | {self._controller_pair_log_prefix()} signal={signal} "
                    f"decision={state.buy_decision} reason={state.buy_reason} "
                    f"rsi={self._fmt(last_rsi, 2)} raw_rev={self._bool_label(state.rsi_reversal)} "
                    f"raw_parts(prev_min={self._bool_label(state.raw_reversal_prev_was_min)},"
                    f"turn_up={self._bool_label(state.raw_reversal_turning_up)},"
                    f"oversold={self._bool_label(state.raw_reversal_was_oversold)},"
                    f"near_bottom={self._bool_label(state.raw_reversal_near_bottom)}) "
                    f"score={signal_score}/{self.config.min_signal_score} "
                    f"score_parts(rsi={self._bool_label(state.score_rsi_oversold)},"
                    f"bb={self._bool_label(state.score_bb_touch)},"
                    f"macd={self._bool_label(state.score_macd_turn)},"
                    f"mr={self._bool_label(state.score_mean_reversion)}) "
                    f"entry={state.buy_entry_role}@{state.buy_entry_fraction * 100:.0f}% "
                    f"context={state.buy_context_bias} "
                    f"confirm={self._bool_label(state.buy_confirmation_active)} "
                    f"rebound={self._fmt(state.buy_confirmation_rebound_delta, 2)}/"
                    f"{self._fmt(state.buy_confirmation_rebound_target, 2)} "
                    f"price_rebounded={self._bool_label(state.buy_confirmation_price_rebounded)} "
                    f"condition={self._normalize_reason(condition_reason)}"
                ),
            )
        if self._is_sell_log_zone(
            last_rsi=last_rsi,
            rsi_sell_eff=rsi_sell_eff,
            current_signal=signal,
        ):
            self._log_signal_transition(
                key="sell_eval",
                signature=(
                    f"{state.sell_decision}|{state.sell_reason}|{self._bool_label(state.sell_has_inventory)}|"
                    f"{self._fmt(state.sell_pnl_pct, 4)}|{self._bool_label(state.sell_profitability_ok)}"
                ),
                message=(
                    f"SELL eval | {self._controller_pair_log_prefix()} signal={signal} "
                    f"decision={state.sell_decision} reason={state.sell_reason} "
                    f"rsi={self._fmt(last_rsi, 2)} has_inventory={self._bool_label(state.sell_has_inventory)} "
                    f"pnl={self._fmt((state.sell_pnl_pct or 0.0) * 100 if state.sell_pnl_pct is not None else None, 2)}% "
                    f"profit_ok={self._bool_label(state.sell_profitability_ok)} "
                    f"reversal={self._bool_label(state.sell_reversal_confirmed)} "
                    f"trend_hold={self._bool_label(state.sell_trend_hold)}"
                ),
            )

    # -----------------------------------------------------------------------
    # Multi-indicator signal score
    # -----------------------------------------------------------------------

    def _compute_signal_score(
        self,
        last_rsi: Optional[float],
        last_close: Optional[float],
        bb_lower: Optional[float],
        macd_hist: Optional[float],
        macd_hist_prev: Optional[float],
        rsi_buy_threshold: float,
        volume_ratio: Optional[float] = None,
        mean_reversion_score: float = 0.0,
    ) -> int:
        """
        Count how many indicators agree on a BUY condition.

        RSI oversold is MANDATORY — if RSI is not below threshold, score is 0.
        Additional indicators each contribute 0 or 1:
        - bb_score: close <= lower Bollinger Band
        - macd_score: MACD histogram turning positive
        - mean_reversion_score: high probability of mean reversion (RSI extreme)
        """
        score_details = self._compute_signal_score_details(
            last_rsi=last_rsi,
            last_close=last_close,
            bb_lower=bb_lower,
            macd_hist=macd_hist,
            macd_hist_prev=macd_hist_prev,
            rsi_buy_threshold=rsi_buy_threshold,
            mean_reversion_score=mean_reversion_score,
        )
        return int(score_details["score"])

    def _build_backtest_signal_frame(
        self, df: pd.DataFrame, *, rsi_buy_eff: float, rsi_sell_eff: float,
    ) -> pd.DataFrame:
        """Build per-row signal + state payload for BacktestingEngineBase compatibility."""
        frame = pd.DataFrame(index=df.index)
        frame["signal"] = 0
        rsi_col = f"RSI_{self.config.rsi_length}"
        if rsi_col not in df.columns:
            frame["signal_state"] = [{} for _ in df.index]
            return frame

        rsi = pd.to_numeric(df[rsi_col], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        buy_thr = float(rsi_buy_eff)
        sell_thr = float(rsi_sell_eff)
        timestamps = (
            pd.to_numeric(df["timestamp"], errors="coerce")
            if "timestamp" in df.columns
            else pd.Series(range(len(df)), index=df.index, dtype=float)
        )

        rsi_reversal = self._compute_raw_buy_reversal_series(rsi, buy_thr)

        # Multi-indicator BUY score (vectorised)
        score = pd.Series(0, index=df.index, dtype=int)
        rsi_oversold_now = rsi <= buy_thr
        score = score + rsi_oversold_now.astype(int)

        bbl_col = f"BBL_{self.config.bb_length}_{self.config.bb_std}"
        if bbl_col in df.columns:
            bbl = pd.to_numeric(df[bbl_col], errors="coerce")
            score = score + (rsi_oversold_now & (close <= bbl)).astype(int)

        hist_col = f"MACDh_{self.config.macd_fast}_{self.config.macd_slow}_{self.config.macd_signal}"
        ema_fast_col = f"EMA_{self.config.ema_fast_length}"
        macd_h = None
        macd_h_prev = None
        if hist_col in df.columns:
            macd_h = pd.to_numeric(df[hist_col], errors="coerce")
            macd_h_prev = macd_h.shift(1)
            macd_turn = ((macd_h > macd_h_prev) & ((macd_h > 0) | (macd_h_prev < 0))).fillna(False)
            score = score + (rsi_oversold_now & macd_turn).astype(int)

        mean_reversion_score = rsi.apply(calculate_rsi_mean_reversion_score)
        score = score + (rsi_oversold_now & (mean_reversion_score >= 0.5)).astype(int)

        atr_pct = pd.Series(index=df.index, dtype=float)
        try:
            atr_series, _ = self._compute_atr_series(df.copy(), self.config.atr_length)
            if atr_series is not None:
                atr_pct = pd.to_numeric(atr_series, errors="coerce") / close.replace(0, pd.NA)
        except _INDICATOR_ERRORS:
            atr_pct = pd.Series(index=df.index, dtype=float)

        condition_ok = pd.Series(True, index=df.index, dtype=bool)
        condition_reason = pd.Series("ok", index=df.index, dtype=object)
        if not atr_pct.empty:
            low_vol = (atr_pct < float(self.config.min_atr_pct_to_trade)).fillna(False)
            high_vol = (atr_pct > float(self.config.max_atr_pct_to_trade)).fillna(False)
            condition_ok = ~(low_vol | high_vol)
            condition_reason.loc[low_vol] = "low_vol"
            condition_reason.loc[high_vol] = "high_vol"

        # BUY: run the same confirmation state machine used in live mode.
        split_entries_enabled = self._split_entries_enabled()
        scout_fraction = float(self._effective_scout_fraction("neutral"))
        runner_fraction = float(max(Decimal("0"), Decimal("1") - Decimal(str(scout_fraction))))
        pending_setup: Optional[BuyConfirmationSetup] = None
        buy_mask = pd.Series(False, index=df.index, dtype=bool)
        backtest_state: List[Dict[str, object]] = []
        inventory_units = Decimal("0")
        scout_established = False
        runner_established = False
        for row_number, idx in enumerate(df.index):
            row_timestamp = timestamps.loc[idx]
            timestamp = float(row_timestamp) if pd.notna(row_timestamp) else float(row_number)
            row_rsi = rsi.loc[idx]
            row_close = close.loc[idx]
            previous_setup = pending_setup
            row_signal_score = int(score.loc[idx])
            row_raw_candidate = bool(rsi_reversal.loc[idx] and row_signal_score >= int(self.config.min_signal_score))
            buy_signal, pending_setup, _ = self._evaluate_buy_confirmation_step(
                timestamp=timestamp,
                last_rsi=float(row_rsi) if pd.notna(row_rsi) else None,
                last_close=float(row_close) if pd.notna(row_close) else None,
                signal_score=row_signal_score,
                raw_buy_candidate=row_raw_candidate,
                pending_setup=pending_setup,
            )
            row_condition_ok = bool(condition_ok.loc[idx])
            row_condition_reason = str(condition_reason.loc[idx])
            row_state: Dict[str, object] = {
                "timestamp": timestamp,
                "signal_score": row_signal_score,
                "condition_ok": row_condition_ok,
                "condition_reason": row_condition_reason,
                "buy_entry_role": "none",
                "buy_entry_fraction": 0.0,
                "buy_context_bias": "neutral",
            }

            if (
                split_entries_enabled
                and row_raw_candidate
                and inventory_units <= Decimal("0")
                and not scout_established
                and not runner_established
            ):
                if row_condition_ok:
                    buy_mask.loc[idx] = True
                    frame.loc[idx, "signal"] = 1
                    row_state["buy_entry_role"] = "scout"
                    row_state["buy_entry_fraction"] = scout_fraction
                    inventory_units += Decimal(str(scout_fraction))
                    scout_established = True
                backtest_state.append(row_state)
                continue

            if buy_signal and not row_condition_ok:
                pending_setup = previous_setup
                buy_signal = False

            if buy_signal:
                buy_mask.loc[idx] = True
                frame.loc[idx, "signal"] = 1
                if split_entries_enabled and scout_established and not runner_established:
                    row_state["buy_entry_role"] = "runner"
                    row_state["buy_entry_fraction"] = runner_fraction
                    inventory_units += Decimal(str(runner_fraction))
                    runner_established = True
                else:
                    row_state["buy_entry_role"] = "full"
                    row_state["buy_entry_fraction"] = 1.0
                    inventory_units += Decimal("1")

            backtest_state.append(row_state)

        sell_rsi_rollover = (rsi.shift(1) - rsi) >= float(self.config.sell_rsi_rollover_delta)
        sell_price_below_ema = (
            (close < pd.to_numeric(df[ema_fast_col], errors="coerce"))
            if ema_fast_col in df.columns
            else pd.Series(False, index=df.index, dtype=bool)
        )
        sell_macd_rollover = (
            (macd_h < macd_h_prev)
            if macd_h is not None and macd_h_prev is not None
            else pd.Series(False, index=df.index, dtype=bool)
        )
        sell_reversal = ((sell_rsi_rollover & sell_price_below_ema) | sell_macd_rollover).fillna(False)
        sell_candidate = (rsi >= sell_thr) & sell_reversal

        simulated_units = Decimal("0")
        simulated_avg_entry: Optional[Decimal] = None
        for idx in df.index:
            row_close = close.loc[idx]
            if pd.isna(row_close):
                continue
            row_close_decimal = Decimal(str(row_close))
            row_buy = bool(buy_mask.loc[idx])
            row_sell = bool(sell_candidate.loc[idx])

            if row_buy and row_sell:
                continue

            if row_buy:
                previous_units = simulated_units
                row_state = backtest_state[df.index.get_loc(idx)]
                row_fraction = Decimal(str(row_state.get("buy_entry_fraction", 1.0) or 1.0))
                simulated_units += row_fraction
                if simulated_avg_entry is None or previous_units <= 0:
                    simulated_avg_entry = row_close_decimal
                else:
                    simulated_avg_entry = (
                        (simulated_avg_entry * previous_units) + (row_close_decimal * row_fraction)
                    ) / simulated_units
                continue

            if row_sell and simulated_units > 0:
                profitability_ok = True
                if (
                    self.config.sell_only_if_profitable
                    and simulated_avg_entry is not None
                    and simulated_avg_entry > 0
                ):
                    min_exit_price = simulated_avg_entry * (
                        Decimal("1") + Decimal(str(self.config.min_profit_pct_for_sell))
                    )
                    profitability_ok = row_close_decimal >= min_exit_price
                if profitability_ok:
                    frame.loc[idx, "signal"] = -1
                    simulated_units = Decimal("0")
                    simulated_avg_entry = None
                    scout_established = False
                    runner_established = False
                    inventory_units = Decimal("0")

        frame["signal_state"] = backtest_state
        return frame

    def _build_backtest_signal_series(
        self, df: pd.DataFrame, *, rsi_buy_eff: float, rsi_sell_eff: float,
    ) -> pd.Series:
        return self._build_backtest_signal_frame(
            df,
            rsi_buy_eff=rsi_buy_eff,
            rsi_sell_eff=rsi_sell_eff,
        )["signal"]

    # -----------------------------------------------------------------------
    # Determine BUY / SELL signal
    # -----------------------------------------------------------------------

    def _determine_signal(
        self,
        last_rsi: Optional[float],
        signal_score: int,
        rsi_buy_eff: float,
        rsi_sell_eff: float,
        last_close: Optional[float],
        rsi_prev: Optional[float] = None,
        ema_fast: Optional[float] = None,
        macd_hist: Optional[float] = None,
        macd_hist_prev: Optional[float] = None,
        raw_buy_reversal: bool = False,
        signal_timestamp: Optional[float] = None,
        candle_timestamp: Optional[float] = None,
    ) -> int:
        if last_rsi is None:
            return 0

        # --- BUY: raw reversal can either enter immediately or arm a rebound confirmation setup ---
        raw_buy_candidate = raw_buy_reversal and signal_score >= self.config.min_signal_score
        signal_timestamp = signal_timestamp if signal_timestamp is not None else 0.0
        previous_setup = self._pending_buy_confirmation
        self._last_buy_signal_role = None
        buy_signal_ready, next_setup, setup_armed = self._evaluate_buy_confirmation_step(
            timestamp=signal_timestamp,
            last_rsi=last_rsi,
            last_close=float(last_close) if last_close is not None else None,
            signal_score=signal_score,
            raw_buy_candidate=raw_buy_candidate,
            pending_setup=self._pending_buy_confirmation,
        )
        if setup_armed and next_setup is not None:
            self.logger().debug(
                f"BUY setup armed | {self._controller_pair_log_prefix()} "
                f"mode={self.config.buy_confirmation_mode} rsi={last_rsi:.2f} score={signal_score} "
                f"trough_rsi={next_setup.trough_rsi:.2f} wait={self.config.buy_confirmation_max_wait_seconds}s"
            )
        self._pending_buy_confirmation = next_setup

        if buy_signal_ready:
            emitted_buy_signal = self._should_emit_live_buy_reversal(
                buy_signal_on_bar=True,
                candle_timestamp=candle_timestamp,
            )
        else:
            emitted_buy_signal = False

        split_entries_enabled = self._split_entries_enabled()
        active_scout = self._has_active_buy_leg("scout")
        active_runner = self._has_active_buy_leg("runner")
        scout_established = active_scout and self._get_total_position_value_usd() > Decimal("0")
        scout_signal_ready = bool(
            split_entries_enabled
            and raw_buy_candidate
            and not active_scout
            and not active_runner
            and self._is_flat_for_new_buy_entry()
        )

        if scout_signal_ready:
            emitted_scout_signal = self._should_emit_live_buy_reversal(
                buy_signal_on_bar=True,
                candle_timestamp=candle_timestamp,
            )
            if emitted_scout_signal:
                state = (self.processed_data or {}).get("signal_state", {})
                effective_scout_fraction = self._effective_scout_fraction(
                    str(state.get("buy_context_bias", "neutral") or "neutral")
                )
                self.logger().info(
                    f"BUY signal (scout-reversal) | {self._controller_pair_log_prefix()} "
                    f"rsi={last_rsi:.2f} score={signal_score} scout={float(effective_scout_fraction) * 100:.0f}% "
                    f"{self._position_log_context()}"
                )
                self._last_buy_signal_role = "scout"
                return 1

        if emitted_buy_signal:
            buy_signal_role = "runner" if split_entries_enabled and scout_established and not active_runner else "full"
            if self.config.buy_confirmation_mode == "rebound_confirm":
                setup = next_setup or previous_setup
                trough_rsi = setup.trough_rsi if setup is not None else None
                trough_text = f" trough_rsi={trough_rsi:.2f}" if trough_rsi is not None else ""
                label = "BUY signal (runner-confirmed)" if buy_signal_role == "runner" else "BUY signal (rebound-confirmed)"
            else:
                trough_text = ""
                label = "BUY signal (reversal)"
            self.logger().info(
                f"{label} | {self._controller_pair_log_prefix()} "
                f"rsi={last_rsi:.2f}{trough_text} score={signal_score} {self._position_log_context()}"
            )
            self._last_buy_signal_role = buy_signal_role
            self._pending_buy_confirmation = None
            return 1
        else:
            if scout_signal_ready:
                blocked_reason = "same-candle-scout"
            elif raw_buy_candidate and buy_signal_ready:
                blocked_reason = "same-candle"
            else:
                blocked_reason = str(self._build_buy_decision_state(
                    current_signal=0,
                    last_rsi=last_rsi,
                    last_close=last_close,
                    signal_score=signal_score,
                    raw_buy_reversal=raw_buy_reversal,
                    condition_ok=True,
                    condition_reason="ready",
                )["buy_reason"])
            if self._is_buy_log_zone(
                last_rsi=last_rsi,
                rsi_buy_eff=rsi_buy_eff,
                raw_buy_reversal=raw_buy_reversal,
                signal_score=signal_score,
                pending_setup_active=next_setup is not None,
                current_signal=0,
            ):
                self._log_signal_transition(
                    key="buy_determine",
                    signature=(
                        f"{blocked_reason}|{raw_buy_reversal}|{signal_score}|"
                        f"{self._bool_label(next_setup is not None)}|{candle_timestamp}"
                    ),
                    message=(
                        f"BUY not ready | {self._controller_pair_log_prefix()} "
                        f"reason={blocked_reason} rsi={last_rsi:.2f} "
                        f"score={signal_score}/{self.config.min_signal_score} "
                        f"raw_reversal={self._bool_label(raw_buy_reversal)} "
                        f"confirm_active={self._bool_label(next_setup is not None)}"
                    ),
                )

        # --- SELL: RSI must be overbought ---
        if last_rsi >= rsi_sell_eff:
            self._pending_buy_confirmation = None
            has_inventory = self._has_buy_inventory()
            cost_basis = self._get_position_cost_basis() if has_inventory else None
            mid = self._get_mid_price() if has_inventory else None
            sell_reversal_state = self._build_sell_reversal_state(
                last_rsi=last_rsi,
                rsi_prev=rsi_prev,
                current_price=mid,
                ema_fast=ema_fast,
                macd_hist=macd_hist,
                macd_hist_prev=macd_hist_prev,
            )
            sell_state = self._build_sell_decision_state(
                current_signal=0,
                last_rsi=last_rsi,
                rsi_prev=rsi_prev,
                rsi_sell_eff=rsi_sell_eff,
                cost_basis=cost_basis,
                mid_price=mid,
                ema_fast=ema_fast,
                macd_hist=macd_hist,
                macd_hist_prev=macd_hist_prev,
                has_inventory=has_inventory,
            )

            if not has_inventory:
                self._log_signal_transition(
                    key="sell_determine",
                    signature=f"inventory-flat|{rsi_sell_eff:.2f}",
                    message=(
                        f"SELL not ready | {self._controller_pair_log_prefix()} "
                        f"reason=inventory-flat rsi={last_rsi:.2f} thr={rsi_sell_eff:.2f}"
                    ),
                )
                return 0

            sell_reason = str(sell_state["sell_reason"])
            if sell_reason == "no-cost-basis":
                self._log_signal_transition(
                    key="sell_determine",
                    signature=f"no-cost-basis|{rsi_sell_eff:.2f}",
                    message=(
                        f"SELL not ready | {self._controller_pair_log_prefix()} "
                        f"reason=no-cost-basis rsi={last_rsi:.2f} thr={rsi_sell_eff:.2f}"
                    ),
                )
                return 0
            if sell_reason == "no-mid-price":
                self._log_signal_transition(
                    key="sell_determine",
                    signature=f"no-mid-price|{rsi_sell_eff:.2f}",
                    message=(
                        f"SELL not ready | {self._controller_pair_log_prefix()} "
                        f"reason=no-mid-price rsi={last_rsi:.2f} thr={rsi_sell_eff:.2f}"
                    ),
                )
                return 0
            if not sell_reversal_state["sell_reversal_confirmed"]:
                self._log_signal_transition(
                    key="sell_determine",
                    signature=(
                        "need-reversal|"
                        f"{self._bool_label(sell_reversal_state['sell_rsi_rollover'])}|"
                        f"{self._bool_label(sell_reversal_state['sell_price_below_ema'])}|"
                        f"{self._bool_label(sell_reversal_state['sell_macd_rollover'])}"
                    ),
                    message=(
                        f"SELL not ready | {self._controller_pair_log_prefix()} "
                        f"reason=need-reversal rsi={last_rsi:.2f} thr={rsi_sell_eff:.2f} "
                        f"rsi_rollover={self._bool_label(sell_reversal_state['sell_rsi_rollover'])} "
                        f"price_below_ema={self._bool_label(sell_reversal_state['sell_price_below_ema'])} "
                        f"macd_rollover={self._bool_label(sell_reversal_state['sell_macd_rollover'])}"
                    ),
                )
                return 0
            if sell_state["sell_trend_hold"]:
                self._log_signal_transition(
                    key="sell_determine",
                    signature=f"trend-hold|{rsi_sell_eff:.2f}",
                    message=(
                        f"SELL not ready | {self._controller_pair_log_prefix()} "
                        f"reason=trend-hold rsi={last_rsi:.2f} thr={rsi_sell_eff:.2f}"
                    ),
                )
                return 0
            if self.config.sell_only_if_profitable:
                if sell_reason == "need-profit":
                    pnl_pct = sell_state["sell_pnl_pct"]
                    self._log_signal_transition(
                        key="sell_determine",
                        signature=(
                            f"need-profit|{float(pnl_pct or 0.0):.5f}|"
                            f"{float(self.config.min_profit_pct_for_sell):.5f}"
                        ),
                        message=(
                            f"SELL not ready | {self._controller_pair_log_prefix()} "
                            f"reason=need-profit rsi={last_rsi:.2f} thr={rsi_sell_eff:.2f} "
                            f"pnl={float(pnl_pct or 0.0) * 100:.2f}% "
                            f"min={float(self.config.min_profit_pct_for_sell) * 100:.2f}%"
                        ),
                    )
                    return 0
            self.logger().info(
                f"SELL signal | {self._controller_pair_log_prefix()} "
                f"rsi={last_rsi:.2f} thr={rsi_sell_eff:.2f} {self._position_log_context()}"
            )
            return -1

        return 0

    # -----------------------------------------------------------------------
    # Executor gating
    # -----------------------------------------------------------------------

    def can_create_executor(self, signal: int) -> bool:
        if signal == 0:
            return False
        state = (self.processed_data or {}).get("signal_state", {})
        target_side = TradeType.BUY if signal > 0 else TradeType.SELL
        if signal > 0 and self.config.trade_mode == "sell_only":
            self._record_gate_reason(target_side, "trade_mode_sell_only")
            return False
        buy_entry_role = str(state.get("buy_entry_role", "none") or "none")
        runner_entry = signal > 0 and buy_entry_role == "runner"

        if getattr(self.config, "manual_kill_switch", False):
            self._record_gate_reason(target_side, "manual_kill_switch")
            return False

        if signal > 0 and self._operator_entry_paused:
            self._record_gate_reason(target_side, "operator_pause")
            return False

        # Consecutive loss pause
        if signal > 0 and self._check_consecutive_loss_pause():
            self._record_gate_reason(target_side, "consecutive_loss_pause")
            return False

        # Market conditions (live-only; per-row ATR filter handled in backtest signal series)
        if signal > 0 and state and not state.get("condition_ok", True):
            is_live = state.get("timestamp") == (self.processed_data or {}).get("timestamp")
            if is_live:
                self._record_gate_reason(target_side, state.get("condition_reason", "filter"))
                return False

        # Capacity
        active_same_side = self._filter_same_side(target_side, active_only=True)
        if signal < 0 and active_same_side:
            self._record_gate_reason(target_side, "sell_active")
            return False
        if len(active_same_side) >= self.config.max_executors_per_side:
            self._record_gate_reason(target_side, "capacity")
            return False

        # Position-aware DCA gating: require higher score when adding to losing positions
        if signal > 0 and active_same_side and self.config.dca_score_boost > 0:
            score = state.get("signal_score", 0) or 0
            all_underwater = all(
                (getattr(ex, "net_pnl_pct", None) or 0) < 0 for ex in active_same_side
            )
            if all_underwater and not (runner_entry and self._has_active_buy_leg("scout") and not self._has_active_buy_leg("runner")):
                required = self.config.min_signal_score + self.config.dca_score_boost
                if score < required:
                    self._record_gate_reason(target_side, "dca_score_low")
                    return False

        try:
            mp_val = self.market_data_provider.get_price_by_type(
                self.config.connector_name, self.config.trading_pair, PriceType.MidPrice,
            )
            new_price = Decimal(str(mp_val)) if mp_val is not None else None
        except _MARKET_DATA_ERRORS:
            new_price = None
        if new_price is None or new_price <= 0:
            self._record_gate_reason(target_side, "price_unavailable")
            return False

        if signal < 0:
            should_hold, _ = self._sell_trend_hold_active(current_price=new_price)
            if should_hold:
                self._record_gate_reason(target_side, "trend_hold")
                return False
            if self._held_bag_count() > 0:
                self._record_gate_reason(target_side, "held_bag_recovery")
                return False
            sell_price = self._shade_limit_maker_price(
                trade_type=TradeType.SELL,
                fallback_price=new_price,
            )
            if not self._has_sufficient_price_gap(target_side, sell_price):
                self._record_gate_reason(target_side, "price_gap")
                return False

        # Max total position
        if signal > 0:
            _, entry_usd, entry_price, _, _ = self._resolve_buy_entry_plan(new_price, state)
            if not self._has_sufficient_avg_entry_gap_for_buy(entry_price):
                self._record_gate_reason(target_side, "avg_entry_gap")
                return False
            current_pos = self._get_total_position_value_usd()
            if current_pos + entry_usd > self.config.max_total_position_usd:
                self._record_gate_reason(target_side, "max_position")
                return False

        # Flash crash cooldown
        now_ts = self.market_data_provider.time()
        if signal > 0 and self._is_flash_crash_cooldown(now_ts):
            self._record_gate_reason(target_side, "flash_crash")
            return False

        # Cooldown
        if signal > 0 and active_same_side and not runner_entry:
            last_ts = max(e.timestamp for e in active_same_side)
            cd = self._effective_cooldown_time()
            if (now_ts - last_ts) < cd:
                self._record_gate_reason(target_side, "cooldown")
                return False

        # IB cooldown
        if signal > 0:
            ib_executors = self.filter_executors(
                executors=self.executors_info,
                filter_func=lambda x, s=target_side: (
                    x.connector_name == self.config.connector_name
                    and x.trading_pair == self.config.trading_pair
                    and x.side == s and getattr(x, "close_type", None) == CloseType.INSUFFICIENT_BALANCE
                ),
            )
            if ib_executors:
                last_ib_ts = max((ex.close_timestamp or ex.timestamp) for ex in ib_executors)
                if (now_ts - last_ib_ts) < self._effective_cooldown_time():
                    self._record_gate_reason(target_side, "ib_cooldown")
                    return False

        # Price gap
        if signal > 0 and not runner_entry and not self._has_sufficient_price_gap(target_side, new_price):
            self._record_gate_reason(target_side, "price_gap")
            return False

        # Balance check for BUY
        if signal > 0:
            _, quote = self._base_quote_assets()
            if quote is not None:
                balance = self._get_available_asset_balance(quote)
                _, entry_usd_check, _, _, _ = self._resolve_buy_entry_plan(new_price, state)
                if balance is None or balance <= Decimal("0") or balance < entry_usd_check:
                    self._record_gate_reason(target_side, "balance_low")
                    return False

        # Balance check for SELL
        if signal < 0:
            base, _ = self._base_quote_assets()
            if base is not None:
                sellable_amount = self._get_sellable_inventory_amount()
                if sellable_amount <= Decimal("0"):
                    self._record_gate_reason(target_side, "base_balance_low")
                    return False

        self._record_gate_reason(target_side, "ready")
        return True

    # -----------------------------------------------------------------------
    # Executor config generation
    # -----------------------------------------------------------------------

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        if trade_type == TradeType.BUY and self.config.trade_mode == "sell_only":
            raise ValueError("BUY executor construction is forbidden when trade_mode='sell_only'")
        state = (self.processed_data or {}).get("signal_state", {})

        min_step = Decimal("1e-8")

        if trade_type == TradeType.SELL:
            usd_budget = self.config.usd_per_entry
            entry_amount_base = (usd_budget / price).quantize(min_step) if price and price > 0 else min_step
            if entry_amount_base <= 0:
                entry_amount_base = min_step
            ref_price = self._shade_limit_maker_price(
                trade_type=TradeType.SELL,
                fallback_price=price,
            )
            position_amount = self._get_sellable_inventory_amount()
            target_amount = position_amount
            slice_multiplier = Decimal(str(self.config.signal_sell_inventory_slice_multiplier))
            if slice_multiplier > 0 and ref_price > 0:
                target_quote = self.config.usd_per_entry * slice_multiplier
                target_amount = min(position_amount, target_quote / ref_price)
            sell_amount = self._resolve_recovery_sell_amount(
                position_amount=position_amount,
                target_amount=target_amount,
                price=ref_price,
                available_base=position_amount,
            )
            if sell_amount <= 0 and position_amount > 0:
                sell_amount = position_amount.quantize(min_step)
            return OrderExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=trade_type,
                amount=sell_amount,
                price=ref_price,
                execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                position_action=PositionAction.CLOSE,
                leverage=self.config.leverage,
                level_id="signal_exit",
            )

        # BUY: PositionExecutor with trailing stop
        planned_usd, effective_usd, entry_price, entry_amount_base, uplifted = self._resolve_buy_entry_plan(price, state)
        if uplifted:
            self.logger().debug(
                f"BUY min-notional uplift | {self._controller_pair_log_prefix()} "
                f"role={str(state.get('buy_entry_role', 'full') or 'full')} "
                f"planned={planned_usd:.6f} effective={effective_usd:.6f} entry_price={entry_price:.6f}"
            )
        trailing_stop_cfg = None
        if self.config.use_trailing_exit:
            try:
                base_activation = Decimal(str(self.config.trailing_activation_pct))
                base_delta = Decimal(str(self.config.trailing_delta_pct))
                if self.config.use_adaptive_trailing and self._latest_atr_pct is not None:
                    activation, delta = get_adaptive_trailing_params(
                        atr_pct=self._latest_atr_pct,
                        base_activation=base_activation,
                        base_delta=base_delta,
                        baseline_atr=self.config.trailing_atr_baseline,
                    )
                else:
                    activation = base_activation
                    delta = base_delta
                if activation > 0 and delta > 0:
                    trailing_stop_cfg = TrailingStop(activation_price=activation, trailing_delta=delta)
            except (TypeError, ValueError, ArithmeticError) as e:
                self.logger().warning(
                    f"Trailing stop config failed | {self._controller_pair_log_prefix()} "
                    f"error={type(e).__name__}: {e}"
                )
                trailing_stop_cfg = None

        activation_bounds_cfg: Optional[List[Decimal]] = None
        gap_bound = self._effective_price_gap()
        if gap_bound is not None and gap_bound > Decimal("0"):
            activation_bounds_cfg = [gap_bound]

        triple = TripleBarrierConfig(
            stop_loss=None,
            take_profit=None,
            time_limit=None,
            trailing_stop=trailing_stop_cfg,
            open_order_type=OrderType.LIMIT_MAKER,
            take_profit_order_type=self.config.take_profit_order_type,
            trailing_stop_order_type=self.config.trailing_stop_order_type,
            trailing_stop_limit_max_age_seconds=self.config.trailing_stop_limit_max_age_seconds,
            stop_loss_order_type=OrderType.MARKET,
            # PositionExecutor only supports MARKET stop-loss and time-limit exits.
            time_limit_order_type=OrderType.MARKET,
        )
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            side=trade_type,
            entry_price=entry_price,
            amount=entry_amount_base,
            triple_barrier_config=triple,
            leverage=self.config.leverage,
            activation_bounds=activation_bounds_cfg,
            level_id=str(state.get("buy_entry_role")) if str(state.get("buy_entry_role")) in {"scout", "runner"} else None,
        )

    # -----------------------------------------------------------------------
    # Stop actions: Bag Freeze + held bag recovery
    # -----------------------------------------------------------------------

    def stop_actions_proposal(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        try:
            current_price = self._get_mid_price()
            if current_price is None:
                return actions
        except _MARKET_DATA_ERRORS:
            return actions

        now_ts = self.market_data_provider.time()
        self._sync_recovery_trails()

        # --- Bag Freeze ---
        if self.config.use_bag_freeze:
            freeze_actions = self._check_bag_freeze_trigger(current_price, now_ts)
            actions.extend(freeze_actions)

        regime_error = (self.processed_data or {}).get("regime_error")
        current_signal = int((self.processed_data or {}).get("signal", 0) or 0)

        # --- Held Bag Recovery management ---
        if not (self.config.strict_hmm_mode and regime_error):
            signal_cancel_actions = self._maybe_cancel_stale_signal_sell_executors(
                current_price,
                current_signal=current_signal,
            )
            actions.extend(signal_cancel_actions)
            cancel_actions = self._maybe_cancel_stale_recovery_sell_executors(current_price)
            actions.extend(cancel_actions)
            recovery_actions = self._check_held_bag_recovery(
                current_price=current_price,
                now_ts=now_ts,
                current_signal=current_signal,
            )
            actions.extend(recovery_actions)

        # --- Track closed executors for consecutive loss counting ---
        for ex in self.executors_info:
            if not ex.is_active:
                executor_id = getattr(ex, "id", None)
                if executor_id is not None:
                    if executor_id in self._tracked_executor_result_ids:
                        continue
                    self._track_executor_result(ex)
                    self._tracked_executor_result_ids.add(executor_id)
                    continue
                if hasattr(ex, "_v5_tracked"):
                    continue
                self._track_executor_result(ex)
                ex._v5_tracked = True  # type: ignore[attr-defined]

        # --- Early stop on active executor deep drawdown ---
        existing_stop_ids = {
            action.executor_id for action in actions
            if isinstance(action, StopExecutorAction)
        }
        actions.extend(self._check_active_executor_drawdown_stop(current_price, existing_stop_ids))

        return actions

    def _check_bag_freeze_trigger(self, current_price: Decimal, now_ts: float) -> List[ExecutorAction]:
        """Detect DCA trap: all slots full, all underwater beyond threshold, min age met."""
        actions: List[ExecutorAction] = []
        active_buys = self._filter_same_side(TradeType.BUY, active_only=True)

        if self.config.bag_freeze_require_full_slots:
            if len(active_buys) < self.config.max_executors_per_side:
                return actions
        elif len(active_buys) == 0:
            return actions

        # Check concurrent holds limit
        held_count = sum(
            1 for p in self.positions_held
            if p.connector_name == self.config.connector_name
            and p.trading_pair == self.config.trading_pair
            and p.side == TradeType.BUY
            and p.amount > 0
        )
        if held_count >= self.config.bag_max_concurrent_holds:
            return actions

        # All executors must be underwater beyond threshold and old enough
        distance_thr = Decimal(str(self.config.bag_freeze_distance_pct))
        min_age = self.config.bag_freeze_min_age_seconds

        all_qualify = True
        for ex in active_buys:
            ref_price = self._executor_ref_price(ex)
            if ref_price is None or ref_price <= 0:
                all_qualify = False
                break
            underwater_pct = (ref_price - current_price) / ref_price
            if underwater_pct < distance_thr:
                all_qualify = False
                break
            age = now_ts - ex.timestamp
            if age < min_age:
                all_qualify = False
                break

        if not all_qualify:
            return actions

        # Freeze all active BUY executors
        for ex in active_buys:
            actions.append(StopExecutorAction(
                executor_id=ex.id, controller_id=self.config.id, keep_position=True,
            ))
            self.logger().debug(
                f"BAG FREEZE | {self._controller_pair_log_prefix()} executor={ex.id} "
                f"ref={self._executor_ref_price(ex)} current={current_price}"
            )

        self._bag_freeze_count += 1
        self.logger().warning(
            f"BAG FREEZE triggered | {self._controller_pair_log_prefix()} "
            f"freezing {len(active_buys)} executors "
            f"(total freezes: {self._bag_freeze_count})"
        )
        return actions

    def _check_held_bag_recovery(
        self,
        *,
        current_price: Decimal,
        now_ts: float,
        current_signal: int,
    ) -> List[ExecutorAction]:
        """
        Check held bags for recovery.

        New recovery mode arms a local trailing state once the bag is green
        enough, then waits for a reversal signal instead of parking an early
        maker sell while price is still making new highs.
        """
        actions: List[ExecutorAction] = []
        recovery_target = Decimal(str(self.config.bag_recovery_target_pct))

        if self._active_sell_executors():
            return actions

        base_asset, _ = self._base_quote_assets()
        available_base: Optional[Decimal] = None
        if base_asset is not None:
            available_base = self._get_available_asset_balance(base_asset)

        if available_base is not None and available_base <= Decimal("0"):
            return actions

        state = (self.processed_data or {}).get("signal_state", {})
        current_rsi = state.get("rsi")
        ema_fast = state.get("ema_fast")
        recovery_overbought_threshold = self._held_bag_recovery_sell_rsi_threshold()
        effective_target = recovery_target
        if self.config.sell_only_if_profitable:
            effective_target = max(effective_target, Decimal(str(self.config.min_profit_pct_for_sell)))

        for pos in self.positions_held:
            if (pos.connector_name != self.config.connector_name
                    or pos.trading_pair != self.config.trading_pair
                    or pos.side != TradeType.BUY
                    or pos.amount <= Decimal("0")):
                continue

            try:
                breakeven = pos.breakeven_price
                if breakeven is None or breakeven <= 0:
                    continue
            except (AttributeError, TypeError):
                continue

            pnl_pct = self._position_net_pnl_pct(pos, current_price)
            if pnl_pct is None:
                continue
            position_key = self._recovery_position_key(pos)
            if pnl_pct < effective_target:
                self._recovery_trails.pop(position_key, None)
                continue

            trail = self._recovery_trails.get(position_key)
            if trail is None:
                if not self._held_bag_recovery_is_overbought(current_rsi):
                    continue
                trail = RecoveryTrailState(
                    position_key=position_key,
                    armed_timestamp=now_ts,
                    arm_price=current_price,
                    peak_price=current_price,
                    tracked_amount=Decimal(str(pos.amount)),
                    peak_rsi=current_rsi,
                    target_profit_pct=effective_target,
                    last_reason="armed",
                )
                self._recovery_trails[position_key] = trail
                self.logger().debug(
                    f"RECOVERY armed | {self._controller_pair_log_prefix()} "
                    f"breakeven={breakeven} current={current_price} pnl={float(pnl_pct) * 100:.2f}% "
                    f"target={float(effective_target) * 100:.2f}% "
                    f"rsi={self._fmt(current_rsi, 2)} thr={recovery_overbought_threshold:.2f}"
                )
                continue

            current_amount = Decimal(str(pos.amount))
            if current_amount < trail.tracked_amount:
                trail.partial_exit_done = True
            trail.tracked_amount = current_amount
            trail.target_profit_pct = effective_target
            if current_price > trail.peak_price:
                trail.peak_price = current_price
                if current_rsi is not None and (trail.peak_rsi is None or current_rsi > trail.peak_rsi):
                    trail.peak_rsi = current_rsi
                trail.last_reason = "new-peak"
                continue

            reversal_ready, reversal_reason, reversal_metrics = self._recovery_reversal_details(
                trail,
                current_price=current_price,
                current_rsi=current_rsi,
                ema_fast=ema_fast,
            )
            trail.last_reason = reversal_reason

            if not reversal_ready:
                if current_signal < 0:
                    should_hold, hold_reason = self._sell_trend_hold_active(current_price=current_price)
                    if should_hold:
                        trail.last_reason = hold_reason
                        continue
                    reversal_reason = "sell-signal"
                else:
                    continue

            ref_price = self._shade_limit_maker_price(
                trade_type=TradeType.SELL,
                fallback_price=current_price,
            )
            recovery_execution_strategy = self.config.recovery_sell_execution_strategy
            chaser_config = self._recovery_sell_chaser_config()
            sell_price = current_price if recovery_execution_strategy == ExecutionStrategy.MARKET else ref_price
            target_amount = Decimal(str(pos.amount))
            partial_fraction = Decimal(str(self.config.recovery_partial_exit_fraction))
            if Decimal("0") < partial_fraction < Decimal("1"):
                target_amount = target_amount * partial_fraction
                reversal_reason = f"partial-{reversal_reason}"
            sell_amount = self._resolve_recovery_sell_amount(
                position_amount=Decimal(str(pos.amount)),
                target_amount=target_amount,
                price=sell_price,
                available_base=available_base,
            )
            if sell_amount <= Decimal("0"):
                continue
            if not self._has_sufficient_price_gap(TradeType.SELL, sell_price):
                trail.last_reason = "price-gap"
                self.logger().debug(
                    f"RECOVERY TRAIL HOLD | {self._controller_pair_log_prefix()} "
                    f"reason=price-gap current={current_price} sell_price={sell_price} "
                    f"last_sell={self._last_same_side_ref_price(TradeType.SELL)}"
                )
                continue
            sell_config = OrderExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=TradeType.SELL,
                amount=sell_amount,
                price=None if recovery_execution_strategy == ExecutionStrategy.MARKET else ref_price,
                chaser_config=chaser_config,
                execution_strategy=recovery_execution_strategy,
                position_action=PositionAction.CLOSE,
                leverage=self.config.leverage,
                level_id=f"recovery_exit:{position_key}",
            )
            actions.append(CreateExecutorAction(
                executor_config=sell_config,
                controller_id=self.config.id,
            ))
            self.logger().info(
                f"RECOVERY TRAIL SELL | {self._controller_pair_log_prefix()} "
                f"reason={reversal_reason} breakeven={breakeven} current={current_price} "
                f"peak={trail.peak_price} pnl={float(pnl_pct) * 100:.2f}% "
                f"pullback={self._fmt((reversal_metrics.get('pullback_pct') or 0.0) * 100, 2)}% "
                f"amount={sell_amount}"
            )
            trail.arm_price = current_price
            trail.peak_price = current_price
            trail.peak_rsi = current_rsi
            trail.last_reason = reversal_reason
            if available_base is not None:
                available_base -= sell_amount
                if available_base <= Decimal("0"):
                    break

        return actions

    # -----------------------------------------------------------------------
    # Status display
    # -----------------------------------------------------------------------

    @staticmethod
    def _status_age_label(now_ts: float, timestamp: Optional[float]) -> str:
        try:
            age_seconds = max(0.0, float(now_ts) - float(timestamp))
        except (TypeError, ValueError):
            return "n/a"
        total_seconds = int(age_seconds)
        if total_seconds < 60:
            return f"{total_seconds}s"
        minutes, seconds = divmod(total_seconds, 60)
        if minutes < 60:
            return f"{minutes}m{seconds:02d}s"
        hours, minutes = divmod(minutes, 60)
        return f"{hours}h{minutes:02d}m"

    @staticmethod
    def _status_pct_label(value: Optional[object], precision: int = 2) -> str:
        try:
            return f"{float(value) * 100:.{precision}f}%"
        except (TypeError, ValueError):
            return "n/a"

    @staticmethod
    def _status_row(label: str, *parts: object) -> str:
        filtered = [str(part) for part in parts if part not in (None, "")]
        body = " | ".join(filtered) if filtered else "-"
        return f"   {label:<8} {body}"

    @staticmethod
    def _status_money_label(value: Optional[object], precision: int = 2, *, signed: bool = False) -> str:
        try:
            amount = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return "$n/a"
        sign = "+" if signed else ""
        return f"${amount:{sign}.{precision}f}"

    @staticmethod
    def _status_compact_money_label(value: Optional[object]) -> str:
        try:
            amount = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return "$n/a"
        if abs(amount) >= Decimal("1000"):
            return f"${amount / Decimal('1000'):.1f}k"
        return f"${amount:.0f}"

    @staticmethod
    def _status_signed_pct_label(value: Optional[object], precision: int = 2) -> str:
        try:
            return f"{float(value) * 100:+.{precision}f}%"
        except (TypeError, ValueError):
            return "n/a"

    @staticmethod
    def _status_price_gap_label(target_price: Optional[Decimal], current_price: Optional[Decimal]) -> str:
        if target_price is None or current_price is None or current_price <= 0:
            return "n/a"
        if current_price >= target_price:
            return "ready"
        return HLRSIController._status_signed_pct_label((target_price - current_price) / current_price)

    @staticmethod
    def _executor_has_fill(executor) -> bool:
        try:
            return Decimal(str(getattr(executor, "filled_amount_quote", Decimal("0")) or Decimal("0"))) > 0
        except (InvalidOperation, TypeError, ValueError):
            return False

    def _buy_role_executors(self, role: str) -> List[object]:
        return self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda ex, target=role: (
                ex.connector_name == self.config.connector_name
                and ex.trading_pair == self.config.trading_pair
                and ex.side == TradeType.BUY
                and self._executor_level_id(ex) == target
            ),
        )

    def _status_executor_price(self, executor) -> Optional[Decimal]:
        executor_config = getattr(executor, "config", None)
        custom_info = getattr(executor, "custom_info", None) or {}
        for candidate in (
            getattr(executor_config, "price", None),
            custom_info.get("order_price"),
            custom_info.get("entry_price"),
            custom_info.get("current_position_average_price"),
        ):
            try:
                if candidate is None:
                    continue
                price = Decimal(str(candidate))
                if price > 0:
                    return price
            except (InvalidOperation, TypeError, ValueError):
                continue
        return self._executor_ref_price(executor)

    def _status_inventory_line(self) -> str:
        inventory = self._aggregate_held_inventory(TradeType.BUY)
        sellable_amount = self._get_sellable_inventory_amount()
        base_balance = self._get_base_balance()
        base_asset, _ = self._base_quote_assets()
        amount_suffix = f" {base_asset}" if base_asset else ""
        shortfall_amount = self._inventory_balance_shortfall_amount(inventory)
        position_value = self._get_total_position_value_usd()
        try:
            max_position = Decimal(str(getattr(self.config, "max_total_position_usd", Decimal("0"))))
        except (InvalidOperation, TypeError, ValueError):
            max_position = Decimal("0")
        utilization = self._get_position_utilization()
        parts = [
            f"held={inventory.total_amount:.8f}{amount_suffix}",
            f"sellable={sellable_amount:.8f}{amount_suffix}",
        ]
        if shortfall_amount is not None and shortfall_amount > 0:
            parts.append(f"short={shortfall_amount:.8f}{amount_suffix}")
        parts.extend([
            f"bags={inventory.bag_count}",
            f"base={self._fmt(base_balance, 8)}",
            f"cost={self._fmt(inventory.cost_basis, 4)}",
            f"exp=${float(position_value):.0f}/${float(max_position):.0f} ({utilization:.0%})",
        ])
        return self._status_row("position", *parts)

    def _status_exit_owner_line(self, current_signal: int) -> str:
        signal_sells = self._active_signal_sell_executors()
        recovery_sells = self._active_recovery_sell_executors()
        active_sells = self._active_sell_executors()
        active_buys = self._filter_same_side(TradeType.BUY, active_only=True)
        if signal_sells and recovery_sells:
            owner = "mixed"
        elif signal_sells:
            owner = "signal_exit"
        elif recovery_sells:
            owner = "recovery_exit"
        elif current_signal < 0:
            owner = "sell-signal"
        elif self._recovery_trails:
            owner = "recovery-armed"
        else:
            owner = "none"
        return self._status_row(
            "orders",
            f"owner={owner}",
            f"buy={len(active_buys)}",
            f"sell={len(active_sells)}",
            f"recovery={len(self._status_recovery_trails())}",
        )

    def _status_buy_executor_line(self, now_ts: float) -> Optional[str]:
        active_buys = sorted(
            self._filter_same_side(TradeType.BUY, active_only=True),
            key=lambda ex: getattr(ex, "timestamp", 0.0),
            reverse=True,
        )
        if not active_buys:
            return None

        entries: List[str] = []
        for executor in active_buys[:2]:
            custom_info = getattr(executor, "custom_info", None) or {}
            role = str(custom_info.get("role") or self._executor_level_id(executor) or "buy")
            trailing_state = str(custom_info.get("trailing_state") or "n/a")
            entry_price = self._status_executor_price(executor)
            activation_pct = custom_info.get("trailing_activation_pct")
            activation_price = custom_info.get("trailing_activation_price")
            trigger_pct = custom_info.get("trailing_stop_trigger_pct")
            trigger_price = custom_info.get("trailing_trigger_price")
            move_count = custom_info.get("trailing_move_count")
            age = self._status_age_label(now_ts, getattr(executor, "timestamp", None))

            parts = [f"{role} {trailing_state}", f"entry={self._fmt(entry_price, 4)}", f"age={age}"]
            if activation_pct is not None or activation_price is not None:
                parts.append(f"arm={self._status_pct_label(activation_pct)}@{self._fmt(activation_price, 4)}")
            if trigger_pct is not None or trigger_price is not None:
                parts.append(f"trigger={self._status_pct_label(trigger_pct)}@{self._fmt(trigger_price, 4)}")
            if move_count not in (None, ""):
                parts.append(f"moves={move_count}")
            entries.append(" ".join(parts))
        return self._status_row("buy_exec", *entries)

    def _status_sell_executor_line(self, now_ts: float) -> Optional[str]:
        active_sells = sorted(
            self._active_sell_executors(),
            key=lambda ex: getattr(ex, "timestamp", 0.0),
            reverse=True,
        )
        if not active_sells:
            return None

        entries: List[str] = []
        for executor in active_sells[:2]:
            custom_info = getattr(executor, "custom_info", None) or {}
            level_id = self._executor_level_id(executor) or "sell"
            order_price = self._status_executor_price(executor)
            age = self._status_age_label(now_ts, getattr(executor, "timestamp", None))
            retries = custom_info.get("current_retries")
            max_retries = custom_info.get("max_retries")
            last_update = custom_info.get("order_last_update") or custom_info.get("open_order_last_update")

            parts = [f"{level_id}@{self._fmt(order_price, 4)}", f"age={age}"]
            if retries is not None or max_retries is not None:
                parts.append(f"retries={int(retries or 0)}/{int(max_retries or 0)}")
            if last_update is not None:
                parts.append(f"last={self._status_age_label(now_ts, last_update)}")
            entries.append(" ".join(parts))
        return self._status_row("sell_exec", *entries)

    def _status_recovery_trails(self) -> List[Dict[str, object]]:
        processed_trails = (self.processed_data or {}).get("recovery_trails")
        snapshots: List[Dict[str, object]] = []
        if isinstance(processed_trails, dict) and processed_trails:
            for trail in processed_trails.values():
                if isinstance(trail, dict):
                    snapshots.append(trail)
        elif self._recovery_trails:
            snapshots.extend(trail.as_dict() for trail in self._recovery_trails.values())
        return snapshots

    def _status_recovery_line(self, current_price: Optional[Decimal]) -> Optional[str]:
        trail_snapshots = self._status_recovery_trails()
        if not trail_snapshots:
            return None

        top_trail = max(trail_snapshots, key=lambda trail: float(trail.get("armed_timestamp") or 0.0))
        peak_price = top_trail.get("peak_price")
        pullback_pct: Optional[Decimal] = None
        try:
            peak_decimal = Decimal(str(peak_price)) if peak_price is not None else None
            if current_price is not None and peak_decimal is not None and peak_decimal > 0:
                pullback_pct = max((peak_decimal - current_price) / peak_decimal, Decimal("0"))
        except (InvalidOperation, TypeError, ValueError):
            pullback_pct = None

        parts = [
            f"count={len(trail_snapshots)}",
            f"reason={top_trail.get('last_reason', 'n/a')}",
            f"target={self._status_pct_label(top_trail.get('target_profit_pct'))}",
            f"peak={self._fmt(top_trail.get('peak_price'), 4)}",
            f"tracked={self._fmt(top_trail.get('tracked_amount'), 8)}",
            f"partial={self._bool_label(bool(top_trail.get('partial_exit_done')))}",
        ]
        if pullback_pct is not None:
            parts.append(f"pullback={self._status_pct_label(pullback_pct)}")
        return self._status_row("recovery", *parts)

    def to_format_status(self) -> List[str]:
        """Per-controller body for the operator console.

        All controller-level information now lives in the parent strategy's
        ``CONTROLLERS`` / ``SIGNALS`` / ``PLANS`` summary tables (RSI proximity,
        buy/sell gauges, gate truth table, focus notes, exit/entry plan), and
        per-executor / per-position runtime detail lives in the parent's
        ``ACTIVE EXECUTORS`` / ``POSITIONS HELD`` tables. Returning an empty
        list lets the parent suppress the per-controller section entirely
        (header, body, and the aggregate ``active_exec total=...`` line) so a
        multi-pair ``hl_rsi`` desk renders the shared tables only.
        """
        return []

    def _idle_wallet_label(self, current_price: Optional[Decimal]) -> str:
        """Return ``idle wallet $X.YZ`` for the off-controller base-asset wallet headroom."""
        base_balance = self._get_base_balance()
        managed_value = self._get_total_position_value_usd()
        idle_value: Optional[Decimal] = None
        if base_balance is not None and current_price is not None and current_price > 0:
            wallet_value = base_balance * current_price
            idle_value = max(wallet_value - managed_value, Decimal("0"))
        return f"idle wallet {self._status_money_label(idle_value)}"

    def _inventory_reconciliation_values(
        self,
        current_price: Optional[Decimal],
    ) -> Tuple[Optional[Decimal], Decimal, Optional[Decimal]]:
        inventory = self._aggregate_held_inventory(TradeType.BUY)
        held_value: Optional[Decimal] = None
        if current_price is not None and current_price > 0:
            held_value = inventory.total_amount * current_price
        active_value = self._get_active_buy_executor_position_value_usd()
        base_balance = self._get_base_balance()
        untracked_value: Optional[Decimal] = None
        if base_balance is not None and current_price is not None and current_price > 0:
            wallet_value = base_balance * current_price
            managed_value = (held_value or Decimal("0")) + active_value
            untracked_value = max(wallet_value - managed_value, Decimal("0"))
        return held_value, active_value, untracked_value

    def _inventory_reconciliation_label(self, current_price: Optional[Decimal]) -> str:
        inventory = self._aggregate_held_inventory(TradeType.BUY)
        held_value, active_value, untracked_value = self._inventory_reconciliation_values(current_price)
        parts = [
            f"H{self._status_compact_money_label(held_value)}",
            f"A{self._status_compact_money_label(active_value)}",
            f"U{self._status_compact_money_label(untracked_value)}",
        ]
        shortfall_value = self._inventory_balance_shortfall_value(current_price, inventory)
        if shortfall_value is not None and shortfall_value > 0:
            parts.append(f"short {self._status_compact_money_label(shortfall_value)}")
        return " ".join(parts)
