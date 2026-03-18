# --- controllers/directional_trading/rsi_v5.py ---
"""
RSI v5 Controller - All-Weather DCA Strategy.

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
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pandas_ta as ta  # noqa: F401
from pydantic import Field as PydanticField, field_validator
from pydantic_core.core_schema import ValidationInfo

from controllers.directional_trading.rsi_signals import (
    calculate_rsi_mean_reversion_score,
    calculate_signal_strength,
    calculate_volume_ratio,
    get_adaptive_trailing_params,
)
from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.logger import HummingbotLogger
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TrailingStop,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.utils.market_analysis import MarketRegime, MarketRegimeDetector

# ---------------------------------------------------------------------------
# Pydantic Field helper (same as rsi_v1 for prompt/client_data compat)
# ---------------------------------------------------------------------------
_CLIENT_FIELD_SCHEMA_KEYS = ("prompt", "prompt_on_new", "is_secure", "is_connect_key", "is_updatable")


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

    def as_dict(self) -> Dict[str, object]:
        return {f.name: getattr(self, f.name) for f in self.__dataclass_fields__.values()}


@dataclass
class BuyConfirmationSetup:
    armed_timestamp: float
    trough_rsi: float
    trough_price: float


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class RSIv5ControllerConfig(DirectionalTradingControllerConfigBase):
    controller_name: str = "rsi_v5"
    controller_type: str = "directional_trading"
    connector_name: str = Field(default="binance", client_data=ClientFieldData(prompt=lambda mi: "Connector:", prompt_on_new=True, is_updatable=False))
    trading_pair: str = Field(default="BNB-USDC", client_data=ClientFieldData(prompt=lambda mi: "Trading pair:", prompt_on_new=True, is_updatable=False))
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

    # --- Core indicators ---
    rsi_length: int = Field(default=10, ge=2, client_data=ClientFieldData(prompt=lambda mi: "RSI length:", is_updatable=False))
    atr_length: int = Field(default=14, ge=2, client_data=ClientFieldData(prompt=lambda mi: "ATR length:", is_updatable=False))
    ema_fast_length: int = Field(default=9, ge=2, client_data=ClientFieldData(prompt=lambda mi: "EMA fast:", prompt_on_new=False, is_updatable=False))
    ema_slow_length: int = Field(default=21, ge=2, client_data=ClientFieldData(prompt=lambda mi: "EMA slow:", prompt_on_new=False, is_updatable=False))
    rsi_buy_threshold: float = Field(default=31.0, client_data=ClientFieldData(prompt=lambda mi: "RSI buy threshold:", prompt_on_new=False, is_updatable=True))
    rsi_sell_threshold: float = Field(default=75.0, client_data=ClientFieldData(prompt=lambda mi: "RSI sell threshold:", prompt_on_new=False, is_updatable=True))

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

    # --- Selling rules ---
    sell_only_if_profitable: bool = Field(default=True, client_data=ClientFieldData(prompt=lambda mi: "Only sell if profitable?:", prompt_on_new=False, is_updatable=True))
    sell_rsi_overbought: float = Field(default=76.0, ge=50.0, le=100.0, client_data=ClientFieldData(prompt=lambda mi: "RSI overbought trigger for sell:", prompt_on_new=False, is_updatable=True))
    min_profit_pct_for_sell: Decimal = Field(default=Decimal("0.002"), client_data=ClientFieldData(prompt=lambda mi: "Min profit pct for sell:", prompt_on_new=False, is_updatable=True))
    recent_trades_lookback: int = Field(default=15, client_data=ClientFieldData(prompt=lambda mi: "Recent trades lookback:", prompt_on_new=False, is_updatable=False))

    # --- Position sizing & risk limits ---
    usd_per_entry: Decimal = Field(default=Decimal("30"), client_data=ClientFieldData(prompt=lambda mi: "USD per entry:", prompt_on_new=True, is_updatable=True))
    max_total_position_usd: Decimal = Field(default=Decimal("500"), client_data=ClientFieldData(prompt=lambda mi: "Max total position USD:", prompt_on_new=True, is_updatable=True))

    # --- Trailing take profit ---
    use_trailing_exit: bool = Field(default=True, client_data=ClientFieldData(prompt=lambda mi: "Use trailing exit?:", prompt_on_new=False, is_updatable=True))
    trailing_activation_pct: float = Field(default=0.047, client_data=ClientFieldData(prompt=lambda mi: "Trailing activation pct:", prompt_on_new=False, is_updatable=True))
    trailing_delta_pct: float = Field(default=0.034, client_data=ClientFieldData(prompt=lambda mi: "Trailing delta pct:", prompt_on_new=False, is_updatable=True))
    use_adaptive_trailing: bool = Field(default=False, client_data=ClientFieldData(prompt=lambda mi: "Volatility-adaptive trailing?:", prompt_on_new=False, is_updatable=True))
    trailing_atr_baseline: float = Field(default=0.002, ge=0.0001, client_data=ClientFieldData(prompt=lambda mi: "Trailing ATR baseline:", prompt_on_new=False, is_updatable=True))

    # --- Take profit (optional fixed target, 0 = disabled) ---
    take_profit_pct: float = Field(default=0.0, ge=0.0, client_data=ClientFieldData(prompt=lambda mi: "Take profit pct (0=disabled):", prompt_on_new=False, is_updatable=True))

    # --- Max hold time (0 = no limit) ---
    max_hold_time_seconds: int = Field(default=0, ge=0, client_data=ClientFieldData(prompt=lambda mi: "Max hold time seconds (0=disabled):", prompt_on_new=False, is_updatable=True))

    # --- Entry spacing (DCA ladder) ---
    executor_price_gap_threshold: Decimal = Field(default=Decimal("0.0045"), client_data=ClientFieldData(prompt=lambda mi: "Min price gap between entries:", prompt_on_new=False, is_updatable=True))
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

# ---------------------------------------------------------------------------
# Controller
# ---------------------------------------------------------------------------


class RSIv5Controller(DirectionalTradingControllerBase):
    """
    All-Weather DCA Controller.

    Entry: Multi-indicator score (RSI + BB + MACD) >= min_signal_score
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

    def __init__(self, config: RSIv5ControllerConfig, *args, **kwargs):
        self.config = config

        self._latest_atr_pct: Optional[float] = None
        self._current_gap_threshold: Decimal = self.config.executor_price_gap_threshold
        self._current_cooldown: int = self.config.cooldown_time
        self._last_gate_reason: Dict[TradeType, Optional[str]] = {TradeType.BUY: None, TradeType.SELL: None}
        self._last_processed_timestamp: Optional[float] = None
        self._last_buy_signal_candle_ts: Optional[float] = None
        self._pending_buy_confirmation: Optional[BuyConfirmationSetup] = None
        self._flash_crash_until: float = 0.0
        self._bag_freeze_count: int = 0

        self._cached_cost_basis: Optional[Decimal] = None
        self._cost_basis_cache_ts: Optional[float] = None
        self._consecutive_losses: int = 0
        self._last_loss_timestamp: Optional[float] = None
        # Indicator max_records must cover the slowest indicator
        self.max_records = max(500, config.bb_length * 3, config.macd_slow * 3, config.rsi_length * 5)
        if not getattr(self.config, "candles_config", None) or len(self.config.candles_config) == 0:
            self.config.candles_config = [CandlesConfig(
                connector=config.candles_connector or config.connector_name,
                trading_pair=config.candles_trading_pair or config.trading_pair,
                interval=config.interval,
                max_records=self.max_records,
            )]

        self._regime_detector: MarketRegimeDetector = MarketRegimeDetector(use_hmm=True)

        super().__init__(config, *args, **kwargs)

    # -----------------------------------------------------------------------
    # Small helpers
    # -----------------------------------------------------------------------

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

    def _position_amount(self, side: TradeType) -> Decimal:
        pos = next(
            (p for p in self.positions_held
             if p.connector_name == self.config.connector_name
             and p.trading_pair == self.config.trading_pair
             and p.side == side),
            None,
        )
        return Decimal(str(pos.amount)) if pos is not None and pos.amount > 0 else Decimal("0")

    def _get_position_cost_basis(self) -> Optional[Decimal]:
        pos = next(
            (p for p in self.positions_held
             if p.connector_name == self.config.connector_name
             and p.trading_pair == self.config.trading_pair
             and p.side == TradeType.BUY),
            None,
        )
        if pos is not None and hasattr(pos, "breakeven_price") and pos.breakeven_price > 0:
            return Decimal(str(pos.breakeven_price))
        return self._calculate_recent_avg_buy_price()

    def _calculate_recent_avg_buy_price(self) -> Optional[Decimal]:
        try:
            session = MarketsRecorder.get_instance().session
            base, quote = self._base_quote_assets()
            if session is None or base is None:
                return None
            fills = (
                session.query(TradeFill)
                .filter(TradeFill.base_asset == base, TradeFill.quote_asset == quote, TradeFill.trade_type == "BUY")
                .order_by(TradeFill.timestamp.desc())
                .limit(self.config.recent_trades_lookback)
                .all()
            )
            if not fills:
                return None
            total_base = sum(Decimal(str(f.amount)) for f in fills)
            total_quote = sum(Decimal(str(f.amount)) * Decimal(str(f.price)) for f in fills)
            if total_base <= 0:
                return None
            return total_quote / total_base
        except Exception:
            return None

    def _base_quote_assets(self) -> Tuple[Optional[str], Optional[str]]:
        parts = self.config.trading_pair.split("-")
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, None

    def _controller_pair_log_prefix(self) -> str:
        return f"controller={self.config.id} pair={self.config.trading_pair}"

    def _held_bag_count(self, side: TradeType = TradeType.BUY) -> int:
        return sum(
            1 for position in self.positions_held
            if position.connector_name == self.config.connector_name
            and position.trading_pair == self.config.trading_pair
            and position.side == side
            and position.amount > 0
        )

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
                self.logger().debug(f"Gate | side={'BUY' if side == TradeType.BUY else 'SELL'} reason={reason}")
        except Exception:
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
        }
        return mapping.get(reason, str(reason).replace("_", "-"))

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
        cost_basis = indicators.get("cost_basis")

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
        if cost_basis is not None and mid_price is not None and Decimal(str(cost_basis)) > 0:
            basis = Decimal(str(cost_basis))
            unrealized_pnl_pct = (mid_price - basis) / basis
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
        elif current_signal > 0 and buy_gate != "ready":
            note = f"buy blocked {buy_gate}"
        elif current_signal < 0 and sell_gate != "ready":
            note = f"sell blocked {sell_gate}"
        elif pos_usd > 0 and profit_headroom_pct is not None:
            note = f"headroom {self._summary_pct(profit_headroom_pct)}"
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

        return {
            "pair": self.config.trading_pair,
            "controller_id": self.config.id,
            "state": state_label,
            "signal": self._summary_signal_label(current_signal),
            "score": f"{state.get('signal_score', 0)}/{self.config.min_signal_score}",
            "regime": self._short_regime_label(regime_label),
            "exposure": exposure_text,
            "execs": f"B{len(active_buys)} S{len(active_sells)} H{len(held_bags)}",
            "u_pnl": self._summary_pct(unrealized_pnl_pct),
            "gate": relevant_gate,
            "note": note,
            "attention_score": attention_score,
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
    ) -> str:
        min_score = max(1, int(self.config.min_signal_score))
        score_progress = self._clamp_progress(signal_score / min_score)
        rsi_progress = self._threshold_progress(last_rsi, rsi_buy, direction="down", window=rsi_window)
        gate = self._compact_gate_reason(self._last_gate_reason.get(TradeType.BUY))
        blocker = "ready"
        if not condition_ok:
            blocker = str(condition_reason or "filter").replace("_", "-")
        elif gate != "ready":
            blocker = gate

        details: List[str] = []
        ready = current_signal > 0
        if ready:
            progress = 1.0
            details.append("ready-now")
        else:
            pending_setup = self._pending_buy_confirmation
            if self.config.buy_confirmation_mode == "rebound_confirm" and pending_setup is not None:
                rebound_target = max(float(self.config.buy_confirmation_rsi_delta), 0.0)
                rebound_delta = max(0.0, (last_rsi or 0.0) - pending_setup.trough_rsi)
                rebound_progress = 1.0 if rebound_target == 0 else self._clamp_progress(rebound_delta / rebound_target)
                price_rebounded = (
                    last_close is not None
                    and pending_setup.trough_price > 0
                    and last_close > pending_setup.trough_price
                )
                price_progress = 1.0 if price_rebounded else 0.0
                progress = (score_progress + rebound_progress + price_progress) / 3.0
                details.append("setup=armed")
                details.append(
                    f"rebound={rebound_delta:.2f}/{rebound_target:.2f}"
                )
                if rebound_target > rebound_delta:
                    details.append(f"need_rebound={rebound_target - rebound_delta:.2f}")
                if last_close is not None and pending_setup.trough_price > 0:
                    price_gap_pct = ((last_close - pending_setup.trough_price) / pending_setup.trough_price) * 100
                    details.append(f"price_vs_trough={price_gap_pct:+.2f}%")
            else:
                reversal_progress = 1.0 if raw_reversal else 0.0
                progress = (rsi_progress + score_progress + reversal_progress) / 3.0
                details.append(f"reversal={'yes' if raw_reversal else 'no'}")

        rsi_gap = max(0.0, (last_rsi - rsi_buy)) if last_rsi is not None else None
        details.insert(0, f"rsi={self._fmt(last_rsi, 2)}/{rsi_buy:.2f}")
        if rsi_gap is not None and rsi_gap > 0:
            details.append(f"need_rsi={rsi_gap:.2f}")
        details.append(f"score={signal_score}/{min_score}")
        if signal_score < min_score:
            details.append(f"need_score={min_score - signal_score}")
        if blocker != "ready":
            details.append(f"block={blocker}")
        else:
            details.append("exec=ready")
        return f"🟢 BUY  {self._progress_bar(progress)} {progress * 100:>3.0f}% | " + " | ".join(details)

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
    ) -> str:
        rsi_progress = self._threshold_progress(last_rsi, rsi_sell, direction="up", window=rsi_window)
        gate = self._compact_gate_reason(self._last_gate_reason.get(TradeType.SELL))

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

        if current_signal < 0:
            progress = 1.0
        elif has_inventory and self.config.sell_only_if_profitable:
            progress = (rsi_progress + profit_progress) / 2.0
        else:
            progress = rsi_progress

        details: List[str] = [f"rsi={self._fmt(last_rsi, 2)}/{rsi_sell:.2f}"]
        rsi_gap = max(0.0, (rsi_sell - last_rsi)) if last_rsi is not None else None
        if current_signal < 0:
            details.append("ready-now")
        elif rsi_gap is not None and rsi_gap > 0:
            details.append(f"need_rsi={rsi_gap:.2f}")

        if has_inventory:
            if pnl_pct is not None:
                details.append(
                    f"pnl={float(pnl_pct) * 100:+.2f}%/{float(profit_target) * 100:.2f}%"
                )
                if self.config.sell_only_if_profitable and pnl_pct < profit_target:
                    details.append(
                        f"need_pnl={float(profit_target - pnl_pct) * 100:.2f}%"
                    )
            else:
                details.append("pnl=n/a")
        else:
            details.append("inventory=flat")

        if not has_inventory:
            details.append("block=flat")
        elif gate != "ready":
            details.append(f"block={gate}")
        else:
            details.append("exec=ready")
        return f"🔴 SELL {self._progress_bar(progress)} {progress * 100:>3.0f}% | " + " | ".join(details)

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
        if executor.net_pnl_quote < 0:
            self._consecutive_losses += 1
            self._last_loss_timestamp = self.market_data_provider.time()
        else:
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
        except Exception:
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
        gap_thr = self._effective_price_gap()
        if gap_thr <= 0:
            return True
        if new_price is None or new_price <= 0:
            return False
        ref_price = self._last_same_side_ref_price(side)
        if ref_price is None or ref_price <= 0:
            return True
        pct_gap = abs(new_price - ref_price) / ref_price
        return pct_gap >= gap_thr

    def _last_same_side_ref_price(self, side: TradeType) -> Optional[Decimal]:
        active = self._filter_same_side(side, active_only=True)
        if not active:
            return None
        latest = max(active, key=lambda x: x.timestamp)
        return self._executor_ref_price(latest)

    def _executor_ref_price(self, executor_info) -> Optional[Decimal]:
        try:
            ci = executor_info.custom_info or {}
            for key in ("current_position_average_price", "entry_price", "order_price"):
                val = ci.get(key)
                if val is not None and float(val) > 0:
                    return Decimal(str(val))
        except Exception:
            pass
        return None

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

    def _compute_raw_buy_reversal_series(self, rsi: pd.Series, buy_thr: float) -> pd.Series:
        reversal_window = max(3, self.config.rsi_length // 2)
        rsi_prev = rsi.shift(1)
        rsi_prev_was_min = rsi_prev == rsi.rolling(window=reversal_window, min_periods=2).min()
        rsi_turning_up = rsi > rsi_prev
        rsi_was_oversold = rsi.rolling(window=reversal_window, min_periods=1).min() <= buy_thr
        rsi_near_bottom = rsi <= buy_thr * 1.3

        raw_reversal = rsi_was_oversold & rsi_prev_was_min & rsi_turning_up & rsi_near_bottom

        debounce_window = reversal_window * 4
        prev_fire = raw_reversal.shift(1).rolling(
            window=debounce_window, min_periods=1,
        ).max().fillna(0).astype(bool)
        return raw_reversal & ~prev_fire

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
            )
            armed = True
            return False, setup, armed

        if setup is None:
            return False, None, armed

        if last_rsi < setup.trough_rsi:
            setup.trough_rsi = last_rsi
        if last_close < setup.trough_price:
            setup.trough_price = last_close

        rsi_rebounded = last_rsi >= (setup.trough_rsi + self.config.buy_confirmation_rsi_delta)
        price_rebounded = last_close > setup.trough_price
        confirmed = signal_score >= self.config.min_signal_score and rsi_rebounded and price_rebounded
        if confirmed:
            return True, None, armed

        return False, setup, armed

    # -----------------------------------------------------------------------
    # Regime-aware RSI thresholds (from v1)
    # -----------------------------------------------------------------------

    def _get_dynamic_rsi_thresholds(self, regime_label: Optional[str], confidence: float) -> Tuple[float, float]:
        base_buy = float(self.config.rsi_buy_threshold)
        base_sell = float(self.config.rsi_sell_threshold)
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
            self.processed_data = {"signal": 0, "features": pd.DataFrame()}
            return
        df = df.copy()
        signal_series: Optional[pd.Series] = None
        last_candle_ts: Optional[float] = None
        try:
            if "timestamp" in df.columns and pd.notna(df["timestamp"].iloc[-1]):
                last_candle_ts = float(df["timestamp"].iloc[-1])
        except Exception:
            last_candle_ts = None

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
        except Exception as e:
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
        except Exception:
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
        except Exception:
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
        except Exception:
            pass

        # --- MACD ---
        try:
            df.ta.macd(fast=self.config.macd_fast, slow=self.config.macd_slow, signal=self.config.macd_signal, append=True)
            hist_col = f"MACDh_{self.config.macd_fast}_{self.config.macd_slow}_{self.config.macd_signal}"
            if hist_col in df.columns and pd.notna(df[hist_col].iloc[-1]):
                macd_hist = float(df[hist_col].iloc[-1])
                if len(df) > 1 and pd.notna(df[hist_col].iloc[-2]):
                    macd_hist_prev = float(df[hist_col].iloc[-2])
        except Exception:
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

        # --- Regime ---
        regime: Optional[MarketRegime] = None
        try:
            if not getattr(self._regime_detector, "_is_fit", False) and len(df) >= 220:
                self._regime_detector.fit(df)
            regime = self._regime_detector.detect(df)
        except Exception:
            regime = None

        regime_label = regime.regime_label if regime is not None else None
        regime_conf = float(regime.confidence) if regime is not None and regime.confidence is not None else 0.0
        rsi_buy_eff, rsi_sell_eff = self._get_dynamic_rsi_thresholds(regime_label, regime_conf)

        # --- Volume & mean reversion analysis ---
        volume_ratio: Optional[float] = None
        mean_reversion_score: float = 0.0
        try:
            volume_ratio = calculate_volume_ratio(df)
        except Exception:
            pass
        if last_rsi is not None:
            try:
                mean_reversion_score = calculate_rsi_mean_reversion_score(last_rsi)
            except Exception:
                pass

        # --- BUY reversal detection ---
        raw_buy_reversal = False
        try:
            rsi_col = f"RSI_{self.config.rsi_length}"
            if rsi_col in df.columns:
                rsi_series = pd.to_numeric(df[rsi_col], errors="coerce")
                raw_reversal_series = self._compute_raw_buy_reversal_series(rsi_series, rsi_buy_eff)
                raw_buy_reversal = bool(not raw_reversal_series.empty and raw_reversal_series.iloc[-1])
        except Exception:
            raw_buy_reversal = False

        # --- Signal scoring (RSI is mandatory gate) ---
        signal_score = self._compute_signal_score(
            last_rsi=last_rsi, last_close=last_close,
            bb_lower=bb_lower, macd_hist=macd_hist, macd_hist_prev=macd_hist_prev,
            rsi_buy_threshold=rsi_buy_eff,
            volume_ratio=volume_ratio,
            mean_reversion_score=mean_reversion_score,
        )

        # --- Determine BUY/SELL ---
        signal = self._determine_signal(
            last_rsi=last_rsi, signal_score=signal_score,
            rsi_buy_eff=rsi_buy_eff, rsi_sell_eff=rsi_sell_eff,
            last_close=last_close,
            raw_buy_reversal=raw_buy_reversal,
            signal_timestamp=now_ts,
            candle_timestamp=last_candle_ts,
        )

        # --- Market condition filter for BUY ---
        condition_ok = True
        condition_reason = "ok"
        if signal > 0 and atr_pct is not None:
            if atr_pct < self.config.min_atr_pct_to_trade:
                condition_ok = False
                condition_reason = "low_vol"
                signal = 0
            elif atr_pct > self.config.max_atr_pct_to_trade:
                condition_ok = False
                condition_reason = "high_vol"
                signal = 0

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
        )

        self.logger().debug(
            f"Signal eval | rsi={self._fmt(last_rsi, 2)} reversal={raw_buy_reversal} "
            f"bb_low={self._fmt(bb_lower)} macd_h={self._fmt(macd_hist)} "
            f"score={signal_score}/{self.config.min_signal_score} "
            f"regime={regime_label} -> signal={signal}"
        )

        # Per-row signal column for BacktestingEngineBase compatibility
        try:
            if signal_series is None:
                signal_series = self._build_backtest_signal_series(
                    df, rsi_buy_eff=rsi_buy_eff, rsi_sell_eff=rsi_sell_eff,
                )
            df["signal"] = signal_series
        except Exception:
            if "signal" not in df.columns:
                df["signal"] = 0

        self.processed_data = {
            "signal": signal,
            "features": df,
            "indicators": {
                "rsi": last_rsi, "rsi_prev": rsi_prev, "atr": last_atr, "atr_pct": atr_pct,
                "ema_fast": ema_fast, "ema_slow": ema_slow,
                "bb_lower": bb_lower, "bb_upper": bb_upper, "bb_mid": bb_mid,
                "macd_hist": macd_hist, "cost_basis": self._get_position_cost_basis(),
            },
            "regime": regime_label,
            "regime_confidence": regime_conf,
            "timestamp": now_ts,
            "thresholds": {
                "rsi_buy": rsi_buy_eff, "rsi_sell": rsi_sell_eff,
                "cooldown": effective_cooldown,
                "price_gap": float(effective_gap) if effective_gap is not None else None,
            },
            "signal_state": state.as_dict(),
        }

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
        if last_rsi is None or last_rsi > rsi_buy_threshold:
            return 0

        score = 1  # RSI oversold passed (mandatory gate)
        if last_close is not None and bb_lower is not None and last_close <= bb_lower:
            score += 1
        if macd_hist is not None and macd_hist_prev is not None:
            if macd_hist > macd_hist_prev and (macd_hist > 0 or macd_hist_prev < 0):
                score += 1
        elif macd_hist is not None and macd_hist > 0:
            score += 1
        if mean_reversion_score >= 0.5:
            score += 1
        return score

    def _build_backtest_signal_series(
        self, df: pd.DataFrame, *, rsi_buy_eff: float, rsi_sell_eff: float,
    ) -> pd.Series:
        """Build per-row signal series for BacktestingEngineBase compatibility.

        Uses the same BUY confirmation semantics as the live controller path.
        """
        signal = pd.Series(0, index=df.index, dtype=int)
        rsi_col = f"RSI_{self.config.rsi_length}"
        if rsi_col not in df.columns:
            return signal

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
        score = score + rsi_reversal.astype(int)

        rsi_oversold_now = rsi <= buy_thr
        score = score + rsi_oversold_now.astype(int)

        bbl_col = f"BBL_{self.config.bb_length}_{self.config.bb_std}"
        if bbl_col in df.columns:
            bbl = pd.to_numeric(df[bbl_col], errors="coerce")
            score = score + (close <= bbl).astype(int)

        hist_col = f"MACDh_{self.config.macd_fast}_{self.config.macd_slow}_{self.config.macd_signal}"
        if hist_col in df.columns:
            macd_h = pd.to_numeric(df[hist_col], errors="coerce")
            macd_h_prev = macd_h.shift(1)
            macd_turn = ((macd_h > macd_h_prev) & ((macd_h > 0) | (macd_h_prev < 0))).fillna(False)
            score = score + macd_turn.astype(int)

        # BUY: run the same confirmation state machine used in live mode
        pending_setup: Optional[BuyConfirmationSetup] = None
        buy_mask = pd.Series(False, index=df.index, dtype=bool)
        for row_number, idx in enumerate(df.index):
            row_timestamp = timestamps.loc[idx]
            timestamp = float(row_timestamp) if pd.notna(row_timestamp) else float(row_number)
            row_rsi = rsi.loc[idx]
            row_close = close.loc[idx]
            buy_signal, pending_setup, _ = self._evaluate_buy_confirmation_step(
                timestamp=timestamp,
                last_rsi=float(row_rsi) if pd.notna(row_rsi) else None,
                last_close=float(row_close) if pd.notna(row_close) else None,
                signal_score=int(score.loc[idx]),
                raw_buy_candidate=bool(rsi_reversal.loc[idx] and score.loc[idx] >= int(self.config.min_signal_score)),
                pending_setup=pending_setup,
            )
            buy_mask.loc[idx] = buy_signal

        sell_mask = rsi >= sell_thr

        signal = signal.where(~buy_mask, 1)
        signal = signal.where(~sell_mask, -1)
        signal = signal.where(~(buy_mask & sell_mask), 0)
        return signal

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
        buy_signal_ready, next_setup, setup_armed = self._evaluate_buy_confirmation_step(
            timestamp=signal_timestamp,
            last_rsi=last_rsi,
            last_close=float(last_close) if last_close is not None else None,
            signal_score=signal_score,
            raw_buy_candidate=raw_buy_candidate,
            pending_setup=self._pending_buy_confirmation,
        )
        if setup_armed and next_setup is not None:
            self.logger().info(
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

        if emitted_buy_signal:
            if self.config.buy_confirmation_mode == "rebound_confirm":
                setup = next_setup or previous_setup
                trough_rsi = setup.trough_rsi if setup is not None else None
                trough_text = f" trough_rsi={trough_rsi:.2f}" if trough_rsi is not None else ""
                label = "BUY signal (rebound-confirmed)"
            else:
                trough_text = ""
                label = "BUY signal (reversal)"
            self.logger().info(
                f"{label} | {self._controller_pair_log_prefix()} "
                f"rsi={last_rsi:.2f}{trough_text} score={signal_score} {self._position_log_context()}"
            )
            self._pending_buy_confirmation = None
            return 1

        # --- SELL: RSI must be overbought ---
        if last_rsi >= rsi_sell_eff:
            self._pending_buy_confirmation = None
            if self.config.sell_only_if_profitable:
                cost_basis = self._get_position_cost_basis()
                if cost_basis is None or cost_basis <= 0:
                    self.logger().debug("SELL gated: no cost basis")
                    return 0
                mid = self._get_mid_price()
                if mid is None or mid <= 0:
                    return 0
                pnl_pct = (mid - cost_basis) / cost_basis
                if pnl_pct < self.config.min_profit_pct_for_sell:
                    self.logger().debug(
                        f"SELL gated: pnl={float(pnl_pct) * 100:.2f}% < min={float(self.config.min_profit_pct_for_sell) * 100:.2f}%"
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
        if len(active_same_side) >= self.config.max_executors_per_side:
            self._record_gate_reason(target_side, "capacity")
            return False

        # Position-aware DCA gating: require higher score when adding to losing positions
        if signal > 0 and active_same_side and self.config.dca_score_boost > 0:
            score = state.get("signal_score", 0) or 0
            all_underwater = all(
                (getattr(ex, "net_pnl_pct", None) or 0) < 0 for ex in active_same_side
            )
            if all_underwater:
                required = self.config.min_signal_score + self.config.dca_score_boost
                if score < required:
                    self._record_gate_reason(target_side, "dca_score_low")
                    return False

        try:
            mp_val = self.market_data_provider.get_price_by_type(
                self.config.connector_name, self.config.trading_pair, PriceType.MidPrice,
            )
            new_price = Decimal(str(mp_val)) if mp_val is not None else None
        except Exception:
            new_price = None
        if new_price is None or new_price <= 0:
            self._record_gate_reason(target_side, "price_unavailable")
            return False

        # Max total position
        if signal > 0:
            entry_usd = self.config.usd_per_entry
            if self.config.dynamic_position_sizing:
                mult = Decimal(str(state.get("size_multiplier", 1.0) or 1.0))
                entry_usd = entry_usd * mult
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
        if signal > 0 and active_same_side:
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
                    and x.side == s and x.close_type == CloseType.INSUFFICIENT_BALANCE
                ),
            )
            if ib_executors:
                last_ib_ts = max((ex.close_timestamp or ex.timestamp) for ex in ib_executors)
                if (now_ts - last_ib_ts) < self._effective_cooldown_time():
                    self._record_gate_reason(target_side, "ib_cooldown")
                    return False

        # Price gap
        if signal > 0 and not self._has_sufficient_price_gap(target_side, new_price):
            self._record_gate_reason(target_side, "price_gap")
            return False

        # Balance check for BUY (skipped when balance is 0 → backtesting mode)
        if signal > 0:
            _, quote = self._base_quote_assets()
            if quote is not None:
                try:
                    balance = self.market_data_provider.get_balance(self.config.connector_name, quote)
                    if balance is not None and balance > 0:
                        entry_usd_check = self.config.usd_per_entry
                        if Decimal(str(balance)) < entry_usd_check:
                            self._record_gate_reason(target_side, "balance_low")
                            return False
                except Exception:
                    pass

        # Balance check for SELL (skipped when balance is 0 → backtesting mode)
        if signal < 0:
            base, _ = self._base_quote_assets()
            if base is not None:
                try:
                    balance = self.market_data_provider.get_balance(self.config.connector_name, base)
                    if balance is not None and balance > 0:
                        pos_amount = self._position_amount(TradeType.BUY)
                        required = pos_amount if pos_amount > 0 else (self.config.usd_per_entry / new_price)
                        if Decimal(str(balance)) < required:
                            self._record_gate_reason(target_side, "base_balance_low")
                            return False
                except Exception:
                    pass

        self._record_gate_reason(target_side, "ready")
        return True

    # -----------------------------------------------------------------------
    # Executor config generation
    # -----------------------------------------------------------------------

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        state = (self.processed_data or {}).get("signal_state", {})
        usd_budget = self.config.usd_per_entry
        if trade_type == TradeType.BUY and self.config.dynamic_position_sizing:
            mult = Decimal(str(state.get("size_multiplier", 1.0) or 1.0))
            usd_budget = (usd_budget * mult).quantize(Decimal("1e-8"))

        min_step = Decimal("1e-8")
        entry_amount_base = (usd_budget / price).quantize(min_step) if price and price > 0 else min_step
        if entry_amount_base <= 0:
            entry_amount_base = min_step

        if trade_type == TradeType.SELL:
            try:
                ref_price_val = self.market_data_provider.get_price_by_type(
                    self.config.connector_name, self.config.trading_pair, PriceType.BestAsk,
                )
                ref_price = Decimal(str(ref_price_val)) if ref_price_val is not None else price
            except Exception:
                ref_price = price
            position_amount = self._position_amount(TradeType.BUY)
            sell_amount = (position_amount if position_amount > 0 else entry_amount_base).quantize(min_step)
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
            )

        # BUY: PositionExecutor with trailing stop
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
            except Exception as e:
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
            stop_loss_order_type=OrderType.MARKET,
            # PositionExecutor only supports MARKET stop-loss and time-limit exits.
            time_limit_order_type=OrderType.MARKET,
        )
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=entry_amount_base,
            triple_barrier_config=triple,
            leverage=self.config.leverage,
            activation_bounds=activation_bounds_cfg,
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
        except Exception:
            return actions

        now_ts = self.market_data_provider.time()

        # --- Bag Freeze ---
        if self.config.use_bag_freeze:
            freeze_actions = self._check_bag_freeze_trigger(current_price, now_ts)
            actions.extend(freeze_actions)

        # --- Held Bag Recovery Sell ---
        recovery_actions = self._check_held_bag_recovery(current_price)
        actions.extend(recovery_actions)

        # --- Track closed executors for consecutive loss counting ---
        for ex in self.executors_info:
            if not ex.is_active and hasattr(ex, "_v5_tracked"):
                continue
            if not ex.is_active:
                self._track_executor_result(ex)
                ex._v5_tracked = True  # type: ignore[attr-defined]

        # --- Early stop on deep drawdown ---
        threshold = Decimal(str(self.config.early_stop_drawdown_pct))
        keep_pos = self.config.early_stop_keep_position
        if threshold > 0:
            for side in (TradeType.BUY, TradeType.SELL):
                pos = next(
                    (p for p in self.positions_held
                     if p.connector_name == self.config.connector_name
                     and p.trading_pair == self.config.trading_pair
                     and p.side == side),
                    None,
                )
                if pos is None or pos.amount <= Decimal("0"):
                    continue
                try:
                    if side == TradeType.BUY:
                        pnl_pct = (current_price - pos.breakeven_price) / pos.breakeven_price
                    else:
                        pnl_pct = (pos.breakeven_price - current_price) / pos.breakeven_price
                except Exception:
                    continue
                if pnl_pct <= -threshold:
                    active_side = self._filter_same_side(side, active_only=True)
                    for ex in active_side:
                        actions.append(StopExecutorAction(
                            executor_id=ex.id, controller_id=self.config.id, keep_position=keep_pos,
                        ))
                        self.logger().info(
                            f"Early stop | {self._controller_pair_log_prefix()} executor={ex.id} "
                            f"side={'BUY' if side == TradeType.BUY else 'SELL'} pnl={pnl_pct} keep={keep_pos}"
                        )

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
            self.logger().warning(
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

    def _check_held_bag_recovery(self, current_price: Decimal) -> List[ExecutorAction]:
        """
        Check held bags for recovery.

        Normal mode: sell when price > breakeven + recovery_target.
        Overbought mode: when RSI >= sell_rsi_overbought, allow a looser
        recovery trigger to capture local peaks. If sell_only_if_profitable is
        enabled, never relax the held-position recovery target below the
        configured minimum sell profit threshold.
        """
        actions: List[ExecutorAction] = []
        recovery_target = Decimal(str(self.config.bag_recovery_target_pct))

        if self._filter_same_side(TradeType.SELL, active_only=True):
            return actions

        # When RSI is overbought, lower the bar to sell bags at local peaks
        state = (self.processed_data or {}).get("signal_state", {})
        current_rsi = state.get("rsi")
        is_overbought = current_rsi is not None and current_rsi >= self.config.sell_rsi_overbought
        effective_target = Decimal("-0.05") if is_overbought else recovery_target
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

            pnl_pct = (current_price - breakeven) / breakeven
            if pnl_pct >= effective_target:
                min_step = Decimal("1e-8")
                try:
                    ref_price_val = self.market_data_provider.get_price_by_type(
                        self.config.connector_name, self.config.trading_pair, PriceType.BestAsk,
                    )
                    ref_price = Decimal(str(ref_price_val)) if ref_price_val is not None else current_price
                except Exception:
                    ref_price = current_price

                sell_amount = Decimal(str(pos.amount)).quantize(min_step)
                sell_config = OrderExecutorConfig(
                    timestamp=self.market_data_provider.time(),
                    connector_name=self.config.connector_name,
                    trading_pair=self.config.trading_pair,
                    side=TradeType.SELL,
                    amount=sell_amount,
                    price=ref_price,
                    execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                    position_action=PositionAction.CLOSE,
                    leverage=self.config.leverage,
                )
                actions.append(CreateExecutorAction(
                    executor_config=sell_config,
                    controller_id=self.config.id,
                ))
                label = "OVERBOUGHT BAG SELL" if is_overbought and pnl_pct < recovery_target else "BAG RECOVERY SELL"
                self.logger().info(
                    f"{label} | {self._controller_pair_log_prefix()} breakeven={breakeven} current={current_price} "
                    f"pnl={float(pnl_pct) * 100:.2f}% amount={sell_amount}"
                    + (f" rsi={current_rsi:.1f}" if is_overbought else "")
                )

        return actions

    # -----------------------------------------------------------------------
    # Status display
    # -----------------------------------------------------------------------

    def to_format_status(self) -> List[str]:
        """Return a compact signal-distance view for BUY and SELL readiness."""
        if not self.config:
            return ["Configuration not available."]

        processed = self.processed_data or {}
        indicators = processed.get("indicators", {})
        thresholds = processed.get("thresholds", {})
        state = processed.get("signal_state", {})
        mid_price = self._get_mid_price()
        rsi = indicators.get("rsi")
        cost_basis = indicators.get("cost_basis")
        current_signal = processed.get("signal", 0)
        rsi_buy = thresholds.get("rsi_buy", self.config.rsi_buy_threshold)
        rsi_sell = thresholds.get("rsi_sell", self.config.rsi_sell_threshold)
        last_close = state.get("close")
        signal_score = int(state.get("signal_score", 0) or 0)
        raw_reversal = bool(state.get("rsi_reversal"))
        condition_ok = bool(state.get("condition_ok", True))
        condition_reason = state.get("condition_reason", "ok")
        has_inventory = any(
            position.connector_name == self.config.connector_name
            and position.trading_pair == self.config.trading_pair
            and position.side == TradeType.BUY
            and position.amount > 0
            for position in self.positions_held
        ) or self._get_total_position_value_usd() > 0

        rsi_window = max(6.0, min(15.0, abs(float(rsi_sell) - float(rsi_buy)) * 0.35))
        buy_line = self._buy_signal_status_line(
            current_signal=current_signal,
            last_rsi=rsi,
            last_close=float(last_close) if last_close is not None else None,
            rsi_buy=float(rsi_buy),
            rsi_window=rsi_window,
            signal_score=signal_score,
            raw_reversal=raw_reversal,
            condition_ok=condition_ok,
            condition_reason=condition_reason,
        )
        sell_line = self._sell_signal_status_line(
            current_signal=current_signal,
            last_rsi=rsi,
            rsi_sell=float(rsi_sell),
            rsi_window=rsi_window,
            has_inventory=has_inventory,
            cost_basis=Decimal(str(cost_basis)) if cost_basis is not None else None,
            mid_price=mid_price,
        )
        return [buy_line, sell_line]
