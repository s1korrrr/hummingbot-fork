from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from controllers.directional_trading.rsi_v5_core.canonical import stable_hash

_ZERO = Decimal("0")


class StableModel(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_default=True,
        str_strip_whitespace=True,
    )


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class ExecutorPhase(str, Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    TERMINATED = "TERMINATED"


class Lifecycle(str, Enum):
    FLAT = "FLAT"
    ENTERING = "ENTERING"
    OPEN = "OPEN"
    EXITING = "EXITING"
    PAUSED = "PAUSED"
    FAULTED = "FAULTED"


class DataHealthStatus(str, Enum):
    READY = "READY"
    STALE = "STALE"
    INVALID = "INVALID"


class DecisionKind(str, Enum):
    NOOP = "NOOP"
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    PAUSE = "PAUSE"
    FAULT = "FAULT"


class ExitReason(str, Enum):
    HARD_STOP = "HARD_STOP"
    SIGNAL = "SIGNAL"
    OPERATOR = "OPERATOR"
    RISK = "RISK"


class EntryRole(str, Enum):
    FULL = "FULL"
    SCOUT = "SCOUT"
    RUNNER = "RUNNER"


class DataHealth(StableModel):
    status: DataHealthStatus
    age_ms: int = Field(ge=0)
    max_age_ms: int = Field(gt=0)
    reason: str = "ok"

    @property
    def ready(self) -> bool:
        return self.status == DataHealthStatus.READY and self.age_ms <= self.max_age_ms


class MarketSnapshot(StableModel):
    event_id: str = Field(min_length=1, max_length=128)
    sequence: int = Field(ge=1)
    event_time_ms: int = Field(ge=0)
    received_time_ms: int = Field(ge=0)
    connector: str = Field(min_length=1)
    trading_pair: str = Field(min_length=3)
    mark_price: Decimal = Field(gt=_ZERO)
    best_bid: Decimal = Field(gt=_ZERO)
    best_ask: Decimal = Field(gt=_ZERO)
    candle_time_ms: int = Field(ge=0)
    rsi: Optional[Decimal] = Field(default=None, ge=_ZERO, le=Decimal("100"))
    signal_score: int = Field(default=0, ge=0, le=16)
    raw_buy_reversal: bool = False
    buy_confirmation_ready: bool = False
    sell_reversal_confirmed: bool = False
    condition_ok: bool = True
    condition_reason: str = "ok"
    data_health: DataHealth

    @model_validator(mode="after")
    def validate_market_time_and_book(self):
        if self.received_time_ms < self.event_time_ms:
            raise ValueError("received_time_ms must not precede event_time_ms")
        if self.candle_time_ms > self.event_time_ms:
            raise ValueError("candle_time_ms must not exceed event_time_ms")
        if self.best_bid > self.best_ask:
            raise ValueError("best_bid must be less than or equal to best_ask")
        return self


class AccountSnapshot(StableModel):
    collateral_asset: str = Field(min_length=1)
    available_collateral: Decimal = Field(ge=_ZERO)
    equity: Decimal = Field(ge=_ZERO)
    daily_pnl: Decimal = _ZERO
    margin_utilization: Decimal = Field(default=_ZERO, ge=_ZERO, le=Decimal("1"))


class ExecutorSnapshot(StableModel):
    executor_id: str = Field(min_length=1)
    side: Side
    phase: ExecutorPhase
    configured_base: Decimal = Field(ge=_ZERO)
    filled_base: Decimal = Field(ge=_ZERO)
    filled_quote: Decimal = Field(ge=_ZERO)
    reference_price: Decimal = Field(gt=_ZERO)
    net_pnl_quote: Decimal = _ZERO

    @model_validator(mode="after")
    def validate_fill(self):
        if self.filled_base > self.configured_base:
            raise ValueError("filled_base must not exceed configured_base")
        return self

    @property
    def pending_base(self) -> Decimal:
        if self.phase in (ExecutorPhase.SHUTTING_DOWN, ExecutorPhase.TERMINATED):
            return _ZERO
        return max(self.configured_base - self.filled_base, _ZERO)

    @property
    def owns_exposure(self) -> bool:
        return self.phase != ExecutorPhase.TERMINATED and (
            self.filled_base > _ZERO or self.pending_base > _ZERO
        )

    @property
    def stoppable(self) -> bool:
        return self.phase in (ExecutorPhase.PENDING, ExecutorPhase.OPEN)


class PositionSnapshot(StableModel):
    position_id: str = Field(min_length=1)
    side: Side
    amount: Decimal = Field(gt=_ZERO)
    breakeven_price: Decimal = Field(gt=_ZERO)
    cumulative_fees_quote: Decimal = Field(default=_ZERO, ge=_ZERO)


class StrategyConfig(StableModel):
    schema_version: str = "1.0"
    min_signal_score: int = Field(default=2, ge=1, le=16)
    rsi_buy_threshold: Decimal = Field(default=Decimal("31"), gt=_ZERO, lt=Decimal("100"))
    rsi_sell_threshold: Decimal = Field(default=Decimal("76"), gt=_ZERO, lt=Decimal("100"))
    usd_per_entry: Decimal = Field(default=Decimal("30"), gt=_ZERO)
    max_total_position_usd: Decimal = Field(default=Decimal("500"), gt=_ZERO)
    min_profit_pct_for_sell: Decimal = Field(default=Decimal("0.002"), ge=_ZERO)
    hard_stop_loss_pct: Decimal = Field(default=Decimal("0.08"), gt=_ZERO, le=Decimal("0.25"))
    max_daily_loss_quote: Decimal = Field(default=Decimal("100"), gt=_ZERO)
    max_margin_utilization: Decimal = Field(default=Decimal("0.50"), gt=_ZERO, le=Decimal("1"))
    max_leverage: int = Field(default=2, ge=1, le=10)
    leverage: int = Field(default=1, ge=1, le=10)
    sell_only_if_profitable: bool = True

    @model_validator(mode="after")
    def validate_strategy_config(self):
        if self.rsi_buy_threshold >= self.rsi_sell_threshold:
            raise ValueError("rsi_buy_threshold must be below rsi_sell_threshold")
        if self.usd_per_entry > self.max_total_position_usd:
            raise ValueError("usd_per_entry must not exceed max_total_position_usd")
        if self.leverage > self.max_leverage:
            raise ValueError("leverage must not exceed max_leverage")
        return self

    @property
    def identity_hash(self) -> str:
        return stable_hash(self.model_dump(mode="python"))


class StrategyInput(StableModel):
    market: MarketSnapshot
    account: AccountSnapshot
    executors: tuple[ExecutorSnapshot, ...] = ()
    positions: tuple[PositionSnapshot, ...] = ()
    config: StrategyConfig


class ExposureSnapshot(StableModel):
    held_notional: Decimal = Field(ge=_ZERO)
    filled_notional: Decimal = Field(ge=_ZERO)
    pending_notional: Decimal = Field(ge=_ZERO)
    proposed_notional: Decimal = Field(ge=_ZERO)

    @property
    def total_notional(self) -> Decimal:
        return self.held_notional + self.filled_notional + self.pending_notional + self.proposed_notional


class RiskEvaluation(StableModel):
    allowed: bool
    reason_codes: tuple[str, ...] = ()
    exposure: ExposureSnapshot


class EntryIntent(StableModel):
    schema_version: str = "1.0"
    intent_id: str = Field(min_length=64, max_length=64)
    controller_id: str
    event_id: str
    sequence: int
    config_hash: str = Field(min_length=64, max_length=64)
    snapshot_hash: str = Field(min_length=64, max_length=64)
    side: Side = Side.BUY
    role: EntryRole = EntryRole.FULL
    notional: Decimal = Field(gt=_ZERO)
    reference_price: Decimal = Field(gt=_ZERO)
    amount_base: Decimal = Field(gt=_ZERO)
    leverage: int = Field(ge=1)


class ExitIntent(StableModel):
    schema_version: str = "1.0"
    intent_id: str = Field(min_length=64, max_length=64)
    controller_id: str
    event_id: str
    sequence: int
    config_hash: str = Field(min_length=64, max_length=64)
    snapshot_hash: str = Field(min_length=64, max_length=64)
    reason: ExitReason
    executor_ids: tuple[str, ...]
    held_position_ids: tuple[str, ...] = ()


class DecisionRecord(StableModel):
    decision_id: str = Field(min_length=64, max_length=64)
    kind: DecisionKind
    event_id: str
    sequence: int
    reason_codes: tuple[str, ...] = ()


class StrategyState(StableModel):
    controller_id: str
    state_version: int = Field(default=0, ge=0)
    lifecycle: Lifecycle = Lifecycle.FLAT
    last_event_id: Optional[str] = None
    last_sequence: int = Field(default=0, ge=0)
    last_event_time_ms: int = Field(default=0, ge=0)
    config_hash: Optional[str] = None
    episode_id: Optional[str] = None
    consecutive_losses: int = Field(default=0, ge=0)
    entry_paused: bool = False
    pause_reason: Optional[str] = None

    @classmethod
    def initial(cls, controller_id: str) -> "StrategyState":
        return cls(controller_id=controller_id)


class TransitionResult(StableModel):
    state: StrategyState
    decision: DecisionRecord
    exposure: ExposureSnapshot
    entry_intent: Optional[EntryIntent] = None
    exit_intent: Optional[ExitIntent] = None


class ReplayTrace(StableModel):
    initial_state: StrategyState
    results: tuple[TransitionResult, ...]
    trace_hash: str = Field(min_length=64, max_length=64)
