from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from types import MappingProxyType
from typing import Any

_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_PARITY_FIELDS = (
    "close_notional",
    "unrealized_pnl_at_close",
    "close_fee_cost",
    "close_impact_cost",
    "liquidatable_nav",
)
_AUTHORITY_KEYS = (
    "paper_allowed",
    "canary_allowed",
    "live_allowed",
    "live_orders_allowed",
    "capital_authority",
    "risk_authority",
    "execution_authority",
    "trading_authority",
)


class EpisodeAccountingError(ValueError):
    pass


class TerminalSettlementMode(StrEnum):
    FORCE_FLAT = "force_flat"
    CARRY_STATE = "carry_state"
    MARK_TO_LIQUIDATION = "mark_to_liquidation"


class ParityStatus(StrEnum):
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EpisodeAccountingError(f"{label} must be non-empty text")
    return value


def _decimal(
    value: object,
    label: str,
    *,
    nonnegative: bool = False,
    positive: bool = False,
) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{label} must be Decimal, not {type(value).__name__}")
    if not value.is_finite():
        raise EpisodeAccountingError(f"{label} must be finite")
    if value.is_zero() and value.is_signed():
        raise EpisodeAccountingError(f"{label} must not use negative zero")
    if positive and value <= 0:
        raise EpisodeAccountingError(f"{label} must be positive")
    if nonnegative and value < 0:
        raise EpisodeAccountingError(f"{label} must be non-negative")
    return value


def _rate(value: object, label: str) -> Decimal:
    parsed = _decimal(value, label, nonnegative=True)
    if parsed >= 1:
        raise EpisodeAccountingError(f"{label} must be less than one")
    return parsed


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise EpisodeAccountingError(f"{label} must be a lowercase SHA-256 hash")
    return value


def _false_authority(value: Mapping[str, bool] | None = None) -> Mapping[str, bool]:
    supplied = dict(value or {})
    unknown = set(supplied) - set(_AUTHORITY_KEYS)
    if unknown:
        raise EpisodeAccountingError(f"unknown parity authority keys: {sorted(unknown)}")
    if any(type(item) is not bool for item in supplied.values()):
        raise EpisodeAccountingError("parity authority values must be booleans")
    if any(supplied.values()):
        raise EpisodeAccountingError("parity evidence cannot grant authority")
    return MappingProxyType({key: False for key in _AUTHORITY_KEYS})


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, StrEnum):
        return value.value
    return value


def _content_hash(value: Any) -> str:
    payload = json.dumps(
        _json_value(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class LinearPerpPositionSnapshot:
    instrument_id: str
    quantity: Decimal
    entry_price: Decimal
    executable_bid: Decimal
    executable_ask: Decimal
    close_fee_rate: Decimal
    close_impact_rate: Decimal
    contract_multiplier: Decimal

    def __post_init__(self) -> None:
        _text(self.instrument_id, "instrument_id")
        quantity = _decimal(self.quantity, "quantity")
        if quantity == 0:
            raise EpisodeAccountingError("quantity must be non-zero")
        _decimal(self.entry_price, "entry_price", positive=True)
        bid = _decimal(self.executable_bid, "executable_bid", positive=True)
        ask = _decimal(self.executable_ask, "executable_ask", positive=True)
        if bid > ask:
            raise EpisodeAccountingError("executable bid cannot exceed ask")
        _rate(self.close_fee_rate, "close_fee_rate")
        _rate(self.close_impact_rate, "close_impact_rate")
        _decimal(self.contract_multiplier, "contract_multiplier", positive=True)

    @property
    def close_price(self) -> Decimal:
        return self.executable_bid if self.quantity > 0 else self.executable_ask

    @property
    def close_notional(self) -> Decimal:
        with localcontext() as context:
            context.prec = 50
            return abs(self.quantity) * self.close_price * self.contract_multiplier

    @property
    def unrealized_pnl_at_close(self) -> Decimal:
        with localcontext() as context:
            context.prec = 50
            return (
                self.quantity
                * (self.close_price - self.entry_price)
                * self.contract_multiplier
            )

    @property
    def close_fee_cost(self) -> Decimal:
        with localcontext() as context:
            context.prec = 50
            return self.close_notional * self.close_fee_rate

    @property
    def close_impact_cost(self) -> Decimal:
        with localcontext() as context:
            context.prec = 50
            return self.close_notional * self.close_impact_rate


@dataclass(frozen=True, slots=True)
class EpisodeAccountSnapshot:
    account_id: str
    as_of_ns: int
    collateral_cash: Decimal
    accrued_funding_cashflow: Decimal
    positions: tuple[LinearPerpPositionSnapshot, ...]

    def __post_init__(self) -> None:
        _text(self.account_id, "account_id")
        if (
            isinstance(self.as_of_ns, bool)
            or not isinstance(self.as_of_ns, int)
            or self.as_of_ns < 0
        ):
            raise EpisodeAccountingError("as_of_ns must be a non-negative integer")
        _decimal(self.collateral_cash, "collateral_cash")
        _decimal(self.accrued_funding_cashflow, "accrued_funding_cashflow")
        if not isinstance(self.positions, tuple):
            raise EpisodeAccountingError("positions must be a tuple")
        if any(
            not isinstance(position, LinearPerpPositionSnapshot)
            for position in self.positions
        ):
            raise EpisodeAccountingError("positions contain an invalid item")
        ordered = tuple(sorted(self.positions, key=lambda item: item.instrument_id))
        identifiers = tuple(position.instrument_id for position in ordered)
        if len(identifiers) != len(set(identifiers)):
            raise EpisodeAccountingError("duplicate instrument in positions")
        object.__setattr__(self, "positions", ordered)


@dataclass(frozen=True, slots=True)
class LiquidatableNavBreakdown:
    collateral_cash: Decimal
    accrued_funding_cashflow: Decimal
    unrealized_pnl_at_close: Decimal
    close_notional: Decimal
    close_fee_cost: Decimal
    close_impact_cost: Decimal
    nav: Decimal
    position_count: int
    breakdown_hash: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "breakdown_hash", _content_hash(self.as_dict()))

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "hummingbot.linear_perp_liquidatable_nav.v1",
            "collateral_cash": self.collateral_cash,
            "accrued_funding_cashflow": self.accrued_funding_cashflow,
            "unrealized_pnl_at_close": self.unrealized_pnl_at_close,
            "close_notional": self.close_notional,
            "close_fee_cost": self.close_fee_cost,
            "close_impact_cost": self.close_impact_cost,
            "nav": self.nav,
            "position_count": self.position_count,
        }


@dataclass(frozen=True, slots=True)
class EpisodeContinuationIdentity:
    account_id: str
    policy_checkpoint_hash: str
    risk_state_hash: str
    inventory_hash: str
    next_episode_id: str

    def __post_init__(self) -> None:
        _text(self.account_id, "continuation account_id")
        _sha256(self.policy_checkpoint_hash, "policy_checkpoint_hash")
        _sha256(self.risk_state_hash, "risk_state_hash")
        _sha256(self.inventory_hash, "inventory_hash")
        _text(self.next_episode_id, "next_episode_id")


@dataclass(frozen=True, slots=True)
class TerminalSettlement:
    mode: TerminalSettlementMode
    ending_snapshot: EpisodeAccountSnapshot
    terminal_nav: Decimal
    close_fee_cost: Decimal
    close_impact_cost: Decimal
    continuation: EpisodeContinuationIdentity | None


@dataclass(frozen=True, slots=True)
class EpisodeTransitionAttribution:
    gross_market_pnl: Decimal
    funding_cashflow: Decimal
    fee_cost: Decimal
    spread_cost: Decimal
    slippage_cost: Decimal
    impact_cost: Decimal
    borrow_cost: Decimal
    liquidation_penalty: Decimal
    close_cost_reserve_change: Decimal
    ending_external_cashflow: Decimal

    def __post_init__(self) -> None:
        _decimal(self.gross_market_pnl, "gross_market_pnl")
        _decimal(self.funding_cashflow, "funding_cashflow")
        for name in (
            "fee_cost",
            "spread_cost",
            "slippage_cost",
            "impact_cost",
            "borrow_cost",
            "liquidation_penalty",
        ):
            _decimal(getattr(self, name), name, nonnegative=True)
        _decimal(self.close_cost_reserve_change, "close_cost_reserve_change")
        _decimal(self.ending_external_cashflow, "ending_external_cashflow")

    @property
    def actual_costs(self) -> Decimal:
        with localcontext() as context:
            context.prec = 50
            return (
                self.fee_cost
                + self.spread_cost
                + self.slippage_cost
                + self.impact_cost
                + self.borrow_cost
                + self.liquidation_penalty
            )

    @property
    def net_performance_change(self) -> Decimal:
        with localcontext() as context:
            context.prec = 50
            return (
                self.gross_market_pnl
                + self.funding_cashflow
                - self.actual_costs
                - self.close_cost_reserve_change
            )

    @property
    def total_nav_change(self) -> Decimal:
        with localcontext() as context:
            context.prec = 50
            return self.net_performance_change + self.ending_external_cashflow


@dataclass(frozen=True, slots=True)
class EpisodeNavReward:
    nav_before: Decimal
    nav_after: Decimal
    performance_nav_after: Decimal
    simple_return: Decimal
    log_return: Decimal
    attribution_net_performance_change: Decimal
    reconciliation_error: Decimal


@dataclass(frozen=True, slots=True)
class ParityExpectation:
    close_notional: Decimal
    unrealized_pnl_at_close: Decimal
    close_fee_cost: Decimal
    close_impact_cost: Decimal
    liquidatable_nav: Decimal
    quants_lab_artifact_hash: str
    expectation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name in _PARITY_FIELDS:
            _decimal(getattr(self, name), name)
        _sha256(self.quants_lab_artifact_hash, "quants_lab_artifact_hash")
        object.__setattr__(self, "expectation_hash", _content_hash(self.as_dict()))

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "hummingbot.quants_lab_parity_expectation.v1",
            "close_notional": self.close_notional,
            "unrealized_pnl_at_close": self.unrealized_pnl_at_close,
            "close_fee_cost": self.close_fee_cost,
            "close_impact_cost": self.close_impact_cost,
            "liquidatable_nav": self.liquidatable_nav,
            "quants_lab_artifact_hash": self.quants_lab_artifact_hash,
        }


@dataclass(frozen=True, slots=True)
class QuantsLabParityResult:
    status: ParityStatus
    mismatches: tuple[str, ...]
    actual: LiquidatableNavBreakdown
    expectation: ParityExpectation
    tolerance: Decimal
    authority: Mapping[str, bool] = field(default_factory=dict)
    result_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.status, ParityStatus):
            raise EpisodeAccountingError("parity status is invalid")
        if not isinstance(self.actual, LiquidatableNavBreakdown):
            raise EpisodeAccountingError("parity actual breakdown is invalid")
        if not isinstance(self.expectation, ParityExpectation):
            raise EpisodeAccountingError("parity expectation is invalid")
        if not isinstance(self.mismatches, tuple):
            raise EpisodeAccountingError("parity mismatches must be a tuple")
        if any(name not in _PARITY_FIELDS for name in self.mismatches):
            raise EpisodeAccountingError("parity mismatches contain an invalid field")
        if len(self.mismatches) != len(set(self.mismatches)):
            raise EpisodeAccountingError("parity mismatches must be unique")
        tolerance = _decimal(self.tolerance, "tolerance", nonnegative=True)
        expected_mismatches = self._expected_mismatches(tolerance)
        if self.status is ParityStatus.MATCH and expected_mismatches:
            raise EpisodeAccountingError(
                "MATCH parity result is inconsistent with the numeric comparison"
            )
        if self.status is ParityStatus.MISMATCH and not expected_mismatches:
            raise EpisodeAccountingError(
                "MISMATCH parity result requires a numeric mismatch"
            )
        if self.mismatches != expected_mismatches:
            raise EpisodeAccountingError(
                "parity mismatch list does not match the numeric comparison"
            )
        object.__setattr__(self, "authority", _false_authority(self.authority))
        object.__setattr__(self, "result_hash", _content_hash(self.as_dict()))

    @property
    def execution_authority(self) -> bool:
        return False

    def _expected_mismatches(self, tolerance: Decimal) -> tuple[str, ...]:
        actual_values = {
            "close_notional": self.actual.close_notional,
            "unrealized_pnl_at_close": self.actual.unrealized_pnl_at_close,
            "close_fee_cost": self.actual.close_fee_cost,
            "close_impact_cost": self.actual.close_impact_cost,
            "liquidatable_nav": self.actual.nav,
        }
        expected_values = {
            name: getattr(self.expectation, name) for name in _PARITY_FIELDS
        }
        return tuple(
            name
            for name in _PARITY_FIELDS
            if abs(actual_values[name] - expected_values[name]) > tolerance
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "hummingbot.quants_lab_parity_result.v1",
            "status": self.status,
            "mismatches": self.mismatches,
            "actual": self.actual.as_dict(),
            "expectation": self.expectation.as_dict(),
            "tolerance": self.tolerance,
            "authority": self.authority,
        }


def liquidatable_nav(snapshot: EpisodeAccountSnapshot) -> LiquidatableNavBreakdown:
    if not isinstance(snapshot, EpisodeAccountSnapshot):
        raise TypeError("snapshot must be EpisodeAccountSnapshot")
    with localcontext() as context:
        context.prec = 50
        unrealized_pnl = sum(
            (position.unrealized_pnl_at_close for position in snapshot.positions),
            Decimal("0"),
        )
        close_notional = sum(
            (position.close_notional for position in snapshot.positions),
            Decimal("0"),
        )
        close_fee = sum(
            (position.close_fee_cost for position in snapshot.positions),
            Decimal("0"),
        )
        close_impact = sum(
            (position.close_impact_cost for position in snapshot.positions),
            Decimal("0"),
        )
        nav = (
            snapshot.collateral_cash
            + snapshot.accrued_funding_cashflow
            + unrealized_pnl
            - close_fee
            - close_impact
        )
    if nav <= 0:
        raise EpisodeAccountingError("liquidatable NAV must be positive")
    return LiquidatableNavBreakdown(
        collateral_cash=snapshot.collateral_cash,
        accrued_funding_cashflow=snapshot.accrued_funding_cashflow,
        unrealized_pnl_at_close=unrealized_pnl,
        close_notional=close_notional,
        close_fee_cost=close_fee,
        close_impact_cost=close_impact,
        nav=nav,
        position_count=len(snapshot.positions),
    )


def settle_terminal(
    snapshot: EpisodeAccountSnapshot,
    mode: TerminalSettlementMode,
    *,
    continuation: EpisodeContinuationIdentity | None = None,
) -> TerminalSettlement:
    if not isinstance(mode, TerminalSettlementMode):
        raise EpisodeAccountingError("terminal settlement mode is invalid")
    breakdown = liquidatable_nav(snapshot)
    if mode is TerminalSettlementMode.CARRY_STATE:
        if continuation is None:
            raise EpisodeAccountingError("carry_state requires continuation identity")
        if continuation.account_id != snapshot.account_id:
            raise EpisodeAccountingError("continuation account does not match snapshot")
        return TerminalSettlement(
            mode=mode,
            ending_snapshot=snapshot,
            terminal_nav=breakdown.nav,
            close_fee_cost=breakdown.close_fee_cost,
            close_impact_cost=breakdown.close_impact_cost,
            continuation=continuation,
        )
    if continuation is not None:
        raise EpisodeAccountingError(
            "continuation identity is valid only for carry_state"
        )
    if mode is TerminalSettlementMode.MARK_TO_LIQUIDATION:
        return TerminalSettlement(
            mode=mode,
            ending_snapshot=snapshot,
            terminal_nav=breakdown.nav,
            close_fee_cost=breakdown.close_fee_cost,
            close_impact_cost=breakdown.close_impact_cost,
            continuation=None,
        )
    ending = EpisodeAccountSnapshot(
        account_id=snapshot.account_id,
        as_of_ns=snapshot.as_of_ns,
        collateral_cash=breakdown.nav,
        accrued_funding_cashflow=Decimal("0"),
        positions=(),
    )
    return TerminalSettlement(
        mode=mode,
        ending_snapshot=ending,
        terminal_nav=breakdown.nav,
        close_fee_cost=breakdown.close_fee_cost,
        close_impact_cost=breakdown.close_impact_cost,
        continuation=None,
    )


def liquidatable_nav_reward(
    before: EpisodeAccountSnapshot,
    after: EpisodeAccountSnapshot,
    attribution: EpisodeTransitionAttribution,
    *,
    reconciliation_tolerance: Decimal = Decimal("0.000000000000000001"),
) -> EpisodeNavReward:
    if before.account_id != after.account_id:
        raise EpisodeAccountingError("snapshot account identity changed")
    if after.as_of_ns <= before.as_of_ns:
        raise EpisodeAccountingError("snapshot time must increase monotonically")
    tolerance = _decimal(
        reconciliation_tolerance,
        "reconciliation_tolerance",
        nonnegative=True,
    )
    nav_before = liquidatable_nav(before).nav
    nav_after = liquidatable_nav(after).nav
    with localcontext() as context:
        context.prec = 50
        expected_after = nav_before + attribution.total_nav_change
        reconciliation_error = nav_after - expected_after
        if abs(reconciliation_error) > tolerance:
            raise EpisodeAccountingError(
                "transition attribution does not reconcile with liquidatable NAV: "
                f"error={reconciliation_error} tolerance={tolerance}"
            )
        performance_after = nav_after - attribution.ending_external_cashflow
        if performance_after <= 0:
            raise EpisodeAccountingError(
                "performance NAV after external cashflow must be positive"
            )
        ratio = performance_after / nav_before
        simple_return = ratio - Decimal("1")
        log_return = ratio.ln()
    return EpisodeNavReward(
        nav_before=nav_before,
        nav_after=nav_after,
        performance_nav_after=performance_after,
        simple_return=simple_return,
        log_return=log_return,
        attribution_net_performance_change=attribution.net_performance_change,
        reconciliation_error=reconciliation_error,
    )


def verify_quants_lab_parity(
    snapshot: EpisodeAccountSnapshot,
    expectation: ParityExpectation,
    *,
    tolerance: Decimal,
) -> QuantsLabParityResult:
    if not isinstance(expectation, ParityExpectation):
        raise TypeError("expectation must be ParityExpectation")
    parsed_tolerance = _decimal(tolerance, "tolerance", nonnegative=True)
    actual = liquidatable_nav(snapshot)
    actual_values = {
        "close_notional": actual.close_notional,
        "unrealized_pnl_at_close": actual.unrealized_pnl_at_close,
        "close_fee_cost": actual.close_fee_cost,
        "close_impact_cost": actual.close_impact_cost,
        "liquidatable_nav": actual.nav,
    }
    expected_values = {
        name: getattr(expectation, name) for name in _PARITY_FIELDS
    }
    mismatches = tuple(
        name
        for name in _PARITY_FIELDS
        if abs(actual_values[name] - expected_values[name]) > parsed_tolerance
    )
    return QuantsLabParityResult(
        status=ParityStatus.MATCH if not mismatches else ParityStatus.MISMATCH,
        mismatches=mismatches,
        actual=actual,
        expectation=expectation,
        tolerance=parsed_tolerance,
        authority={},
    )
