"""Pure, fill-driven spot inventory lifecycle; no exchange, clock, or storage I/O.

Amounts are Decimal in base/quote units. Quote fees exclude converted base fees:
base fees change inventory, quote fees change cash. Each request is one network
attempt; uncertain attempts remain reserved until a native terminal receipt.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any

ZERO = Decimal("0")
ONE = Decimal("1")
PHASES = {
    "WATCH",
    "SCOUT",
    "ACCUMULATE",
    "DEFENSIVE",
    "EXHAUSTED",
    "RECOVER",
    "MARKUP",
    "DISTRIBUTE",
    "EXIT",
    "COOLDOWN",
}


class EpisodeInputError(ValueError):
    """Unreconciled, contradictory, or invalid policy input; do not dispatch."""


def _decimal(value: Any, name: str, *, positive: bool = False) -> Decimal:
    try:
        if isinstance(value, bool):
            raise ValueError("boolean amount")
        result = Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise EpisodeInputError(f"invalid {name}") from exc
    if not result.is_finite() or result < 0 or (positive and result <= 0):
        raise EpisodeInputError(f"invalid {name}: must be finite and {'positive' if positive else 'nonnegative'}")
    return result


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(_json_value(value), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _floor(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def _ceil(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


@dataclass(frozen=True)
class EpisodeConfig:
    controller_id: str
    connector: str
    trading_pair: str
    mode: str
    budget_quote: Decimal
    mark_cap_quote: Decimal
    fee_rate: Decimal = Decimal("0.001")
    min_profit: Decimal = Decimal("0.002")
    level_fractions: tuple[Decimal, ...] = tuple(map(Decimal, (".10", ".15", ".20", ".25", ".30")))
    dip_atr_levels: tuple[Decimal, ...] = tuple(map(Decimal, ("2", "3", "4", "5", "6")))
    distribution_atr_levels: tuple[Decimal, ...] = tuple(map(Decimal, ("4", "7", "10")))
    remaining_fractions: tuple[Decimal, ...] = tuple(map(Decimal, (".8", ".6", ".4")))
    hostile_scout_fraction: Decimal = Decimal(".25")
    rebound_atr: Decimal = Decimal(".25")
    basis_improvement_atr: Decimal = Decimal(".05")
    last_fill_gap_atr: Decimal = Decimal(".25")
    trail_atr: Decimal = Decimal("2")
    trail_min: Decimal = Decimal(".005")
    trail_max: Decimal = Decimal(".04")
    setup_bars: int = 10
    stabilization_bars: int = 2
    interval_seconds: int = 60
    require_profit: bool = True
    assignment_id: str | None = None
    initial_base: Decimal = ZERO
    initial_cost: Decimal | None = None

    def __post_init__(self):
        for name in ("controller_id", "connector", "trading_pair"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise EpisodeInputError(f"invalid {name}")
        if self.mode not in ("buy_sell", "sell_only"):
            raise EpisodeInputError("unsupported mode")
        if type(self.require_profit) is not bool:
            raise EpisodeInputError("invalid profit guard flag")
        for name in ("budget_quote", "mark_cap_quote", "trail_atr", "trail_min", "trail_max"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name, positive=True))
        for name in (
            "fee_rate",
            "min_profit",
            "hostile_scout_fraction",
            "rebound_atr",
            "basis_improvement_atr",
            "last_fill_gap_atr",
            "initial_base",
        ):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        if (
            self.fee_rate >= ONE
            or self.hostile_scout_fraction > ONE
            or not ZERO < self.trail_min <= self.trail_max < ONE
        ):
            raise EpisodeInputError("invalid fee, sizing factor or trailing range")
        for name in ("level_fractions", "dip_atr_levels", "distribution_atr_levels", "remaining_fractions"):
            object.__setattr__(self, name, tuple(_decimal(x, name, positive=True) for x in getattr(self, name)))
        if (
            not 1 <= len(self.level_fractions) <= 20
            or len(self.level_fractions) != len(self.dip_atr_levels)
            or sum(self.level_fractions) > ONE
        ):
            raise EpisodeInputError("invalid level allocations")
        for levels in (self.dip_atr_levels, self.distribution_atr_levels):
            if any(a >= b for a, b in zip(levels, levels[1:])):
                raise EpisodeInputError("levels must strictly increase")
        if (
            not self.remaining_fractions
            or len(self.remaining_fractions) != len(self.distribution_atr_levels)
            or self.remaining_fractions[0] >= ONE
            or any(a <= b for a, b in zip(self.remaining_fractions, self.remaining_fractions[1:]))
        ):
            raise EpisodeInputError("remaining fractions must strictly decrease within (0,1)")
        for name in ("setup_bars", "stabilization_bars", "interval_seconds"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise EpisodeInputError(f"invalid {name}")
        if self.initial_cost is not None:
            object.__setattr__(self, "initial_cost", _decimal(self.initial_cost, "initial_cost", positive=True))
        if self.mode == "sell_only" and (not self.assignment_id or self.initial_base <= ZERO):
            raise EpisodeInputError("sell_only requires an explicit positive inventory assignment")
        if self.mode == "sell_only" and self.require_profit and self.initial_cost is None:
            raise EpisodeInputError("sell_only profit guard requires assigned cost")
        if self.mode == "buy_sell" and (self.initial_base or self.initial_cost is not None or self.assignment_id):
            raise EpisodeInputError("buy_sell starts from an empty attributed episode")

    @property
    def identity(self) -> str:
        return _hash(asdict(self))


@dataclass(frozen=True)
class MarketSnapshot:
    timestamp: int
    bid: Decimal
    ask: Decimal
    low: Decimal
    atr_1m: Decimal
    atr_15m: Decimal
    atr_1h: Decimal
    anchor: Decimal
    quote_available: Decimal
    base_available: Decimal
    min_amount: Decimal
    min_notional: Decimal
    amount_step: Decimal
    price_step: Decimal
    htf_timestamp: int = 0
    valid: bool = True
    entry_signal: bool = False
    recovery: bool = False
    markup: bool = False
    reset: bool = False
    hostile: bool = False
    cascade: bool = False
    buy_allowed: bool = True
    quality_factor: Decimal = ONE

    def __post_init__(self):
        if type(self.timestamp) is not int or self.timestamp < 0 or type(self.htf_timestamp) is not int:
            raise EpisodeInputError("invalid market timestamp")
        if self.htf_timestamp < 0 or self.htf_timestamp > self.timestamp:
            raise EpisodeInputError("future higher-timeframe observation")
        for name in (
            "bid",
            "ask",
            "low",
            "atr_1m",
            "atr_15m",
            "atr_1h",
            "anchor",
            "quote_available",
            "base_available",
            "min_amount",
            "min_notional",
            "amount_step",
            "price_step",
        ):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        if self.amount_step <= 0 or self.price_step <= 0:
            raise EpisodeInputError("quantization steps must be positive")
        object.__setattr__(self, "quality_factor", _decimal(self.quality_factor, "quality_factor"))
        if self.quality_factor > ONE:
            raise EpisodeInputError("quality factor cannot enlarge allocation")
        for name in ("valid", "entry_signal", "recovery", "markup", "reset", "hostile", "cascade", "buy_allowed"):
            if type(getattr(self, name)) is not bool:
                raise EpisodeInputError(f"invalid boolean {name}")
        if self.valid and (
            min(self.bid, self.ask, self.low, self.atr_1m, self.atr_15m, self.atr_1h, self.anchor) <= 0
            or self.bid > self.ask
        ):
            raise EpisodeInputError("invalid prices, ATR or crossed book")


@dataclass(frozen=True)
class OrderRequest:
    id: str
    episode_id: str
    level: int
    side: str
    amount: Decimal
    price: Decimal
    reserved_quote: Decimal
    expires_at: int
    revision: int

    def __post_init__(self):
        for name in ("id", "episode_id"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise EpisodeInputError("invalid request identity")
        for name in ("level", "expires_at", "revision"):
            if type(getattr(self, name)) is not int:
                raise EpisodeInputError("invalid request integer")
        if self.side not in ("BUY", "SELL") or self.expires_at <= 0 or self.revision <= 0:
            raise EpisodeInputError("invalid request boundary")
        if (self.side == "BUY" and self.level < 0) or (self.side == "SELL" and self.level != -1):
            raise EpisodeInputError("invalid request level")
        for name in ("amount", "price", "reserved_quote"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name, positive=name != "reserved_quote"))


@dataclass(frozen=True)
class OrderUpdate:
    request_id: str
    filled_base: Decimal
    filled_quote: Decimal
    fee_quote: Decimal = ZERO
    fee_base: Decimal = ZERO
    done: bool = False
    fees_known: bool = True

    def __post_init__(self):
        for name in ("filled_base", "filled_quote", "fee_quote", "fee_base"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        if type(self.done) is not bool or type(self.fees_known) is not bool:
            raise EpisodeInputError("invalid execution status")
        if not self.request_id or (self.filled_base > 0 and self.filled_quote <= 0):
            raise EpisodeInputError("invalid execution identity or quote amount")


@dataclass
class OrderProgress:
    request: OrderRequest
    filled_base: Decimal = ZERO
    filled_quote: Decimal = ZERO
    fee_quote: Decimal = ZERO
    fee_base: Decimal = ZERO
    done: bool = False
    seen: bool = False

    @property
    def reserved_quote(self) -> Decimal:
        return ZERO if self.done else max(ZERO, self.request.reserved_quote - self.filled_quote - self.fee_quote)

    @property
    def reserved_base(self) -> Decimal:
        return ZERO if self.done or self.request.side != "SELL" else max(ZERO, self.request.amount - self.filled_base)


@dataclass
class EpisodeState:
    schema_version: int = 1
    config_hash: str = ""
    phase: str = "WATCH"
    episode_number: int = 0
    episode_id: str | None = None
    timestamp: int = -1
    levels: list[str] = field(default_factory=list)
    orders: dict[str, OrderProgress] = field(default_factory=dict)
    base: Decimal = ZERO
    cost: Decimal = ZERO
    gross_spent: Decimal = ZERO
    sell_proceeds: Decimal = ZERO
    frozen_anchor: Decimal = ZERO
    frozen_atr: Decimal = ZERO
    last_fill_price: Decimal = ZERO
    last_valid_bid: Decimal = ZERO
    setup_level: int = -1
    setup_low: Decimal = ZERO
    setup_start: int = 0
    no_new_low: int = 0
    distribution_start_base: Decimal = ZERO
    distribution_basis: Decimal = ZERO
    distribution_atr: Decimal = ZERO
    target_base: Decimal = ZERO
    peak: Decimal = ZERO
    floor: Decimal = ZERO
    equity_peak: Decimal | None = None
    equity_floor: Decimal | None = None
    command_revision: int = 0
    cooldown_htf_timestamp: int = 0
    assignment_consumed: bool = False
    cost_known: bool = True
    fault: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return _json_value(asdict(self))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> EpisodeState:
        try:
            values = copy.deepcopy(payload)
            if values.get("schema_version") != 1 or values.get("phase") not in PHASES:
                raise EpisodeInputError("unsupported episode schema/phase")
            for name in ("assignment_consumed", "cost_known"):
                if type(values[name]) is not bool:
                    raise EpisodeInputError(f"invalid checkpoint boolean {name}")
            for name in (
                "schema_version",
                "episode_number",
                "timestamp",
                "setup_level",
                "setup_start",
                "no_new_low",
                "command_revision",
                "cooldown_htf_timestamp",
            ):
                if type(values[name]) is not int or values[name] < (-1 if name in ("timestamp", "setup_level") else 0):
                    raise EpisodeInputError(f"invalid checkpoint integer {name}")
            if any(level not in ("OPEN", "AUTHORIZED", "SKIPPED") for level in values["levels"]):
                raise EpisodeInputError("invalid checkpoint level")
            decimal_fields = (
                "base",
                "cost",
                "gross_spent",
                "frozen_anchor",
                "frozen_atr",
                "last_fill_price",
                "last_valid_bid",
                "setup_low",
                "distribution_start_base",
                "distribution_basis",
                "distribution_atr",
                "target_base",
                "peak",
                "floor",
            )
            for name in decimal_fields:
                values[name] = _decimal(values[name], name)
            for name in ("equity_peak", "equity_floor", "sell_proceeds"):
                if values[name] is not None:
                    values[name] = Decimal(values[name])
                    if not values[name].is_finite():
                        raise EpisodeInputError(f"invalid {name}")
            orders = {}
            for key, raw in values["orders"].items():
                if type(raw["done"]) is not bool or type(raw["seen"]) is not bool:
                    raise EpisodeInputError("invalid checkpoint execution status")
                for name in ("filled_base", "filled_quote", "fee_quote", "fee_base"):
                    raw[name] = _decimal(raw[name], name)
                request = raw["request"]
                for name in ("amount", "price", "reserved_quote"):
                    request[name] = _decimal(request[name], name)
                raw["request"] = OrderRequest(**request)
                if key != raw["request"].id or raw["request"].side not in ("BUY", "SELL"):
                    raise EpisodeInputError("invalid stored order identity")
                if (
                    raw["request"].amount <= 0
                    or raw["request"].price <= 0
                    or raw["filled_base"] > raw["request"].amount
                ):
                    raise EpisodeInputError("invalid stored execution amounts")
                orders[key] = OrderProgress(**raw)
            values["orders"] = orders
            return cls(**values)
        except (KeyError, TypeError, ArithmeticError) as exc:
            raise EpisodeInputError("invalid episode checkpoint") from exc


@dataclass(frozen=True)
class Transition:
    state: EpisodeState
    requests: tuple[OrderRequest, ...] = ()
    cancel_ids: tuple[str, ...] = ()
    reason: str = "watch"


def remaining_budget(config: EpisodeConfig, state: EpisodeState) -> Decimal:
    if config.mode == "sell_only":
        return ZERO
    return (
        config.budget_quote
        - state.gross_spent
        - sum((order.reserved_quote for order in state.orders.values() if order.request.side == "BUY"), ZERO)
    )


def _accounting(config: EpisodeConfig, state: EpisodeState) -> dict[str, Decimal]:
    """Project the one accumulation then liquidation episode from fill truth.

    Delayed acquisition fees restate average basis of both sold and remaining
    inventory. Recomputing also makes checkpoint contradictions observable.
    """
    assigned = config.mode == "sell_only" and state.assignment_consumed
    acquired = config.initial_base if assigned else ZERO
    spent = (config.initial_cost or ZERO) if assigned else ZERO
    sold = proceeds = ZERO
    for order in state.orders.values():
        if order.request.episode_id != state.episode_id:
            if not order.done:
                raise EpisodeInputError("unresolved order outside current episode")
            continue
        if order.request.side == "BUY":
            if config.mode != "buy_sell":
                raise EpisodeInputError("BUY receipt in sell_only assignment")
            acquired += order.filled_base - order.fee_base
            spent += order.filled_quote + order.fee_quote
        else:
            sold += order.filled_base + order.fee_base
            proceeds += order.filled_quote - order.fee_quote
    base = acquired - sold
    if acquired < ZERO or base < ZERO:
        raise EpisodeInputError("receipt accounting exceeds attributed inventory")
    cost = spent * base / acquired if acquired else ZERO
    return dict(base=base, cost=cost, gross_spent=spent, sell_proceeds=proceeds)


def _validate_accounting(config: EpisodeConfig, state: EpisodeState) -> None:
    if any(getattr(state, name) != value for name, value in _accounting(config, state).items()):
        raise EpisodeInputError("checkpoint accounting contradicts execution receipts")
    if state.episode_id and len(state.levels) != len(config.level_fractions):
        raise EpisodeInputError("checkpoint level allocation mismatch")
    current = [order for order in state.orders.values() if order.request.episode_id == state.episode_id]
    for order in current:
        request = order.request
        expected = _hash((state.episode_id, request.revision, request.side, request.level))
        if request.id != expected or request.revision > state.command_revision:
            raise EpisodeInputError("checkpoint authorization identity mismatch")
        reserve = request.amount * request.price * (ONE + config.fee_rate) if request.side == "BUY" else ZERO
        if reserve != request.reserved_quote:
            raise EpisodeInputError("checkpoint reservation mismatch")
        if request.side == "BUY" and (
            request.level >= len(state.levels) or state.levels[request.level] != "AUTHORIZED"
        ):
            raise EpisodeInputError("checkpoint authorization level mismatch")
    if sum(not order.done for order in current) > 1:
        raise EpisodeInputError("multiple unresolved episode requests")


def _apply_update(config: EpisodeConfig, state: EpisodeState, market: MarketSnapshot, update: OrderUpdate) -> None:
    if update.request_id not in state.orders:
        raise EpisodeInputError("unknown execution request")
    if not update.fees_known:
        raise EpisodeInputError("execution fee currency/value unavailable")
    order = state.orders[update.request_id]
    values = ("filled_base", "filled_quote", "fee_quote", "fee_base")
    if any(getattr(update, name) < getattr(order, name) for name in values):
        raise EpisodeInputError("cumulative execution receipt decreased")
    if order.done and not update.done:
        raise EpisodeInputError("terminal order became pending")
    db, dq, fq, fb = (getattr(update, name) - getattr(order, name) for name in values)
    if any((db, dq, fq, fb)) and order.request.episode_id != state.episode_id:
        raise EpisodeInputError("closed episode receipt changed; explicit reconciliation required")
    if update.filled_base > order.request.amount:
        raise EpisodeInputError("fill exceeds authorized base amount")
    if order.request.side == "BUY":
        if db > 0:
            state.last_fill_price = dq / db
        if update.filled_quote + update.fee_quote > order.request.reserved_quote:
            state.fault = "buy_reservation_overrun"
    for name in values:
        setattr(order, name, getattr(update, name))
    order.done = update.done
    order.seen = True
    for name, value in _accounting(config, state).items():
        setattr(state, name, value)
    if db == ZERO and dq == ZERO and (fq or fb):
        # Missing books cannot erase an accounting correction. Use the last
        # explicitly valid causal valuation while preserving actual fee units.
        restatement = fq + fb * state.last_valid_bid * (ONE - config.fee_rate)
        for name in ("equity_peak", "equity_floor"):
            if getattr(state, name) is not None:
                setattr(state, name, getattr(state, name) - restatement)


def _start_episode(config: EpisodeConfig, state: EpisodeState, market: MarketSnapshot) -> None:
    state.episode_number += 1
    state.episode_id = _hash(
        (
            config.controller_id,
            config.connector,
            config.trading_pair,
            state.episode_number,
            market.timestamp,
            config.assignment_id,
        )
    )[:24]
    state.levels = ["OPEN"] * len(config.level_fractions)
    state.frozen_anchor = market.anchor
    state.frozen_atr = market.atr_1h


def _request(
    config: EpisodeConfig,
    state: EpisodeState,
    market: MarketSnapshot,
    *,
    side: str,
    amount: Decimal,
    price: Decimal,
    reserve: Decimal,
    level: int,
) -> OrderRequest:
    state.command_revision += 1
    identity = _hash((state.episode_id, state.command_revision, side, level))
    request = OrderRequest(
        identity,
        state.episode_id or "",
        level,
        side,
        amount,
        price,
        reserve,
        market.timestamp + config.setup_bars * config.interval_seconds,
        state.command_revision,
    )
    state.orders[identity] = OrderProgress(request)
    return request


def _finish_if_flat(state: EpisodeState, market: MarketSnapshot) -> bool:
    current = [order for order in state.orders.values() if order.request.episode_id == state.episode_id]
    if state.episode_id and state.base == 0 and current and all(order.done for order in current):
        state.phase = "COOLDOWN"
        state.cooldown_htf_timestamp = market.htf_timestamp
        return True
    return False


def _sell(config: EpisodeConfig, state: EpisodeState, market: MarketSnapshot) -> Transition:
    if not state.cost_known and config.require_profit:
        return Transition(state, reason="basis_unavailable")
    min_price = (state.cost / state.base) * (ONE + config.min_profit) / (ONE - config.fee_rate) if state.base else ZERO
    if config.require_profit and market.bid < min_price:
        return Transition(state, reason="profit_guard_blocked")
    # Price-bounded limit: no unconstrained market fallback can defeat the floor.
    price = _ceil(max(market.bid, min_price if config.require_profit else ZERO), market.price_step)
    pending = sum((order.reserved_base for order in state.orders.values()), ZERO)
    needed = max(ZERO, state.base - state.target_base - pending)
    amount = _floor(min(needed, market.base_available), market.amount_step)
    if amount <= 0 or amount < market.min_amount or amount * price < market.min_notional:
        return Transition(state, reason="dust" if needed > 0 else "target_satisfied")
    request = _request(config, state, market, side="SELL", amount=amount, price=price, reserve=ZERO, level=-1)
    return Transition(state, (request,), reason="terminal_exit" if state.phase == "EXIT" else "distribution")


def transition(
    config: EpisodeConfig,
    previous: EpisodeState,
    market: MarketSnapshot,
    updates: tuple[OrderUpdate, ...] | list[OrderUpdate] = (),
) -> Transition:
    """Reconcile actual cumulative receipts, then process at most one closed bar.

    Caller must durably store returned state before emitting requests/cancels.
    Existing commands remain reserved on missing acknowledgement or restart.
    """
    state = copy.deepcopy(previous)
    if state.config_hash and state.config_hash != config.identity:
        raise EpisodeInputError("episode configuration changed without migration")
    state.config_hash = config.identity
    if state.phase not in PHASES or state.schema_version != 1:
        raise EpisodeInputError("invalid episode state")
    _validate_accounting(config, state)
    if market.timestamp < state.timestamp:
        raise EpisodeInputError("out-of-order market observation")
    if market.valid:
        state.last_valid_bid = market.bid
    if (
        not market.valid
        or market.cascade
        or market.hostile
        or not market.buy_allowed
        or market.recovery
        or (state.timestamp >= 0 and market.timestamp > state.timestamp + config.interval_seconds)
    ):
        state.setup_level, state.no_new_low = -1, 0
    for update in updates:
        _apply_update(config, state, market, update)
    pending = [order for order in state.orders.values() if not order.done]
    cancellations = tuple(
        order.request.id
        for order in pending
        if not market.valid
        or market.cascade
        or market.timestamp >= order.request.expires_at
        or (
            (market.hostile or not market.buy_allowed or market.markup or market.recovery)
            and order.request.side == "BUY"
        )
    )
    if state.fault or remaining_budget(config, state) < 0:
        state.fault = state.fault or "episode_budget_overrun"
        return Transition(state, cancel_ids=tuple(order.request.id for order in pending), reason=state.fault)
    if not market.valid:
        return Transition(state, cancel_ids=cancellations, reason="data_unavailable")
    if market.timestamp == state.timestamp:
        return Transition(state, cancel_ids=cancellations, reason="same_candle")
    state.timestamp = market.timestamp
    if state.phase == "COOLDOWN":
        if state.base > 0:
            state.phase, state.target_base = "EXIT", ZERO
            if pending:
                return Transition(
                    state, cancel_ids=tuple(order.request.id for order in pending), reason="late_fill_pending"
                )
            return _sell(config, state, market)
        if config.mode == "buy_sell" and market.reset and market.htf_timestamp > state.cooldown_htf_timestamp:
            state = EpisodeState(
                config_hash=config.identity,
                episode_number=state.episode_number,
                timestamp=market.timestamp,
                assignment_consumed=state.assignment_consumed,
                orders=state.orders,
            )
            return Transition(state, reason="cooldown_reset")
        return Transition(state, reason="cooldown")
    if config.mode == "sell_only" and not state.assignment_consumed:
        if market.base_available < config.initial_base:
            return Transition(state, reason="assignment_balance_shortfall")
        _start_episode(config, state, market)
        state.base = config.initial_base
        state.cost = config.initial_cost or ZERO
        state.cost_known = config.initial_cost is not None
        state.gross_spent = config.initial_cost or ZERO
        state.assignment_consumed = True
        state.phase = "RECOVER"
    if _finish_if_flat(state, market):
        return Transition(state, reason="episode_completed")
    if state.phase in ("MARKUP", "DISTRIBUTE", "EXIT"):
        distance = min(config.trail_max, max(config.trail_min, config.trail_atr * market.atr_15m / market.bid))
        state.peak = max(state.peak, market.bid)
        state.floor = max(state.floor, state.peak * (ONE - distance))
        equity = state.sell_proceeds + state.base * market.bid * (ONE - config.fee_rate) - state.gross_spent
        state.equity_peak = max(state.equity_peak if state.equity_peak is not None else equity, equity)
        floor = state.equity_peak - state.base * state.peak * distance
        state.equity_floor = max(state.equity_floor if state.equity_floor is not None else floor, floor)
        if market.bid <= state.floor or equity <= state.equity_floor:
            state.phase, state.target_base = "EXIT", ZERO
        elif state.phase != "EXIT":
            extension = (state.peak - state.distribution_basis) / state.distribution_atr
            for threshold, fraction in zip(config.distribution_atr_levels, config.remaining_fractions):
                if extension >= threshold:
                    state.target_base = min(state.target_base, state.distribution_start_base * fraction)
                    state.phase = "DISTRIBUTE"
        if pending:
            if state.phase == "EXIT":
                cancellations = tuple(order.request.id for order in pending)
            return Transition(state, cancel_ids=cancellations, reason="pending_execution")
        if state.base > state.target_base:
            return _sell(config, state, market)
        return Transition(state, reason="runner")
    if pending:
        return Transition(state, cancel_ids=cancellations, reason="pending_execution")
    # Available balance can exclude other reservations; never adopt the surplus.
    if state.base > market.base_available:
        return Transition(state, reason="inventory_balance_shortfall")
    if state.base and market.markup:
        if config.require_profit and (
            not state.cost_known
            or market.bid * (ONE - config.fee_rate) * state.base < state.cost * (ONE + config.min_profit)
        ):
            return Transition(state, reason="markup_profit_guard")
        state.phase = "MARKUP"
        state.distribution_start_base = state.target_base = state.base
        state.distribution_basis = state.cost / state.base if state.cost_known else market.bid
        state.distribution_atr = market.atr_1h
        state.peak = market.bid
        state.floor = ZERO
        return Transition(state, reason="markup_armed")
    if market.cascade or (market.hostile and state.base > 0):
        if state.base:
            state.phase = "DEFENSIVE"
        return Transition(state, reason="defensive")
    if state.base and market.recovery:
        state.phase = "RECOVER"
        return Transition(state, reason="recovering")
    if config.mode == "sell_only":
        return Transition(state, reason="sell_only_observe")
    if not market.buy_allowed:
        return Transition(state, reason="buy_veto")
    if not state.episode_id:
        if not market.entry_signal or market.ask > market.anchor - config.dip_atr_levels[0] * market.atr_1h:
            return Transition(state, reason="need_dislocation")
        _start_episode(config, state, market)
    open_levels = [index for index, value in enumerate(state.levels) if value == "OPEN"]
    if not open_levels or remaining_budget(config, state) <= 0:
        state.phase = "EXHAUSTED"
        return Transition(state, reason="exhausted")
    eligible = [
        index
        for index in open_levels
        if market.ask <= state.frozen_anchor - config.dip_atr_levels[index] * state.frozen_atr
    ]
    if not eligible:
        state.phase = "ACCUMULATE" if state.base else "WATCH"
        return Transition(state, reason="need_deeper_level")
    # First purchase is always the scout; a gap cannot multiply initial risk.
    level = 0 if state.gross_spent == 0 and state.levels[0] == "OPEN" else max(eligible)
    if level not in eligible:
        return Transition(state, reason="need_scout_level")
    factor = (config.hostile_scout_fraction if market.hostile else ONE) * market.quality_factor
    factor *= min(ONE, state.frozen_atr / market.atr_1h)
    if (market.hostile and level > 0) or factor == 0:
        return Transition(state, reason="zero_allocation" if factor == 0 else "regime_veto")
    if level == 0:
        if not market.entry_signal:
            return Transition(state, reason="need_entry_confirmation")
    else:
        if state.setup_level != level:
            state.setup_level, state.setup_low, state.setup_start, state.no_new_low = (
                level,
                market.low,
                market.timestamp,
                0,
            )
            return Transition(state, reason="level_armed")
        if market.timestamp - state.setup_start >= config.setup_bars * config.interval_seconds:
            state.levels[level] = "SKIPPED"
            state.setup_level = -1
            return Transition(state, reason="setup_expired")
        if market.low < state.setup_low:
            state.setup_low, state.no_new_low = market.low, 0
        else:
            state.no_new_low += 1
        rebound = max(config.rebound_atr * market.atr_1m, 2 * (market.ask - market.bid))
        if state.no_new_low < config.stabilization_bars or market.ask < state.setup_low + rebound:
            return Transition(state, reason="need_stabilization")
        if market.ask > state.last_fill_price - config.last_fill_gap_atr * market.atr_1m:
            return Transition(state, reason="last_fill_gap")
    cap = config.budget_quote * config.level_fractions[level] * factor
    if cap <= 0:
        return Transition(state, reason="zero_allocation")
    if cap > remaining_budget(config, state) or cap > market.quote_available:
        return Transition(state, reason="insufficient_episode_cash")
    price = _floor(market.ask, market.price_step)
    if price <= 0:
        return Transition(state, reason="price_below_tick")
    amount = _floor(cap / (price * (ONE + config.fee_rate)), market.amount_step)
    reserve = amount * price * (ONE + config.fee_rate)
    if amount <= 0 or amount < market.min_amount or amount * price < market.min_notional:
        return Transition(state, reason="below_exchange_minimum")
    if state.base * market.ask + amount * market.ask > config.mark_cap_quote:
        return Transition(state, reason="mark_exposure_cap")
    if state.base > 0:
        before = state.cost / state.base
        after = (state.cost + reserve) / (state.base + amount)
        if before - after < config.basis_improvement_atr * state.frozen_atr:
            return Transition(state, reason="insufficient_basis_improvement")
    for index in range(level):
        if state.levels[index] == "OPEN":
            state.levels[index] = "SKIPPED"
    state.levels[level] = "AUTHORIZED"
    state.setup_level = -1
    state.phase = "SCOUT" if level == 0 else "ACCUMULATE"
    request = _request(config, state, market, side="BUY", amount=amount, price=price, reserve=reserve, level=level)
    return Transition(state, (request,), reason="scout" if level == 0 else "scale_in")
