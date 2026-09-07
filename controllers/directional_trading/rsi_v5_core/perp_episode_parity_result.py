from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from controllers.directional_trading.rsi_v5_core.perp_episode_accounting import (
    EpisodeAccountingError,
    LiquidatableNavBreakdown,
    ParityExpectation,
    ParityStatus,
    QuantsLabParityResult,
)

_DECIMAL_TEXT = re.compile(r"^-?(0|[1-9][0-9]*)(\.[0-9]+)?$")
_RESULT_KEYS = {
    "schema_version",
    "status",
    "mismatches",
    "actual",
    "expectation",
    "tolerance",
    "execution_authority",
    "authority",
    "result_hash",
}
_ACTUAL_KEYS = {
    "collateral_cash",
    "accrued_funding_cashflow",
    "unrealized_pnl_at_close",
    "close_notional",
    "close_fee_cost",
    "close_impact_cost",
    "nav",
    "position_count",
    "breakdown_hash",
}
_EXPECTATION_KEYS = {
    "close_notional",
    "unrealized_pnl_at_close",
    "close_fee_cost",
    "close_impact_cost",
    "liquidatable_nav",
    "quants_lab_artifact_hash",
    "expectation_hash",
}
_AUTHORITY_KEYS = {
    "paper_allowed",
    "canary_allowed",
    "live_allowed",
    "live_orders_allowed",
    "capital_authority",
    "risk_authority",
    "execution_authority",
    "trading_authority",
}


class ParityResultVerificationError(ValueError):
    pass


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ParityResultVerificationError(f"{label} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise ParityResultVerificationError(f"{label} keys must be strings")
    return value


def _exact_keys(
    value: object,
    expected: set[str],
    label: str,
) -> Mapping[str, Any]:
    mapping = _mapping(value, label)
    if set(mapping) != expected:
        missing = sorted(expected - set(mapping))
        extra = sorted(set(mapping) - expected)
        raise ParityResultVerificationError(
            f"{label} keys drift: missing={missing} extra={extra}"
        )
    return mapping


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ParityResultVerificationError(f"{label} must be non-empty text")
    return value


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ParityResultVerificationError(f"{label} must be a non-negative integer")
    return value


def _boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise ParityResultVerificationError(f"{label} must be boolean")
    return value


def _decimal(value: object, label: str) -> Decimal:
    if not isinstance(value, str) or _DECIMAL_TEXT.fullmatch(value) is None:
        raise ParityResultVerificationError(
            f"{label} must be canonical decimal text"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise ParityResultVerificationError(f"{label} is invalid decimal text") from error
    if not parsed.is_finite():
        raise ParityResultVerificationError(f"{label} must be finite decimal text")
    if parsed.is_zero() and parsed.is_signed():
        raise ParityResultVerificationError(f"{label} must not use negative zero")
    return parsed


def _authority(value: object) -> dict[str, bool]:
    payload = _exact_keys(value, _AUTHORITY_KEYS, "authority")
    result = {
        key: _boolean(payload[key], f"authority.{key}")
        for key in sorted(payload)
    }
    if any(result.values()):
        raise ParityResultVerificationError("authority escalation is forbidden")
    return result


def parity_result_from_wire(value: object) -> QuantsLabParityResult:
    payload = _exact_keys(value, _RESULT_KEYS, "result")
    if payload["schema_version"] != "hummingbot.quants_lab_parity_result.v1":
        raise ParityResultVerificationError("unsupported parity result schema")
    if _boolean(payload["execution_authority"], "execution_authority") is not False:
        raise ParityResultVerificationError("execution authority escalation is forbidden")
    actual_payload = _exact_keys(payload["actual"], _ACTUAL_KEYS, "actual")
    try:
        actual = LiquidatableNavBreakdown(
            collateral_cash=_decimal(
                actual_payload["collateral_cash"], "actual.collateral_cash"
            ),
            accrued_funding_cashflow=_decimal(
                actual_payload["accrued_funding_cashflow"],
                "actual.accrued_funding_cashflow",
            ),
            unrealized_pnl_at_close=_decimal(
                actual_payload["unrealized_pnl_at_close"],
                "actual.unrealized_pnl_at_close",
            ),
            close_notional=_decimal(
                actual_payload["close_notional"], "actual.close_notional"
            ),
            close_fee_cost=_decimal(
                actual_payload["close_fee_cost"], "actual.close_fee_cost"
            ),
            close_impact_cost=_decimal(
                actual_payload["close_impact_cost"], "actual.close_impact_cost"
            ),
            nav=_decimal(actual_payload["nav"], "actual.nav"),
            position_count=_integer(
                actual_payload["position_count"], "actual.position_count"
            ),
        )
    except (EpisodeAccountingError, TypeError) as error:
        raise ParityResultVerificationError(f"invalid actual result: {error}") from error
    if _text(actual_payload["breakdown_hash"], "actual.breakdown_hash") != actual.breakdown_hash:
        raise ParityResultVerificationError("actual breakdown hash mismatch")

    expectation_payload = _exact_keys(
        payload["expectation"],
        _EXPECTATION_KEYS,
        "expectation",
    )
    try:
        expectation = ParityExpectation(
            close_notional=_decimal(
                expectation_payload["close_notional"],
                "expectation.close_notional",
            ),
            unrealized_pnl_at_close=_decimal(
                expectation_payload["unrealized_pnl_at_close"],
                "expectation.unrealized_pnl_at_close",
            ),
            close_fee_cost=_decimal(
                expectation_payload["close_fee_cost"],
                "expectation.close_fee_cost",
            ),
            close_impact_cost=_decimal(
                expectation_payload["close_impact_cost"],
                "expectation.close_impact_cost",
            ),
            liquidatable_nav=_decimal(
                expectation_payload["liquidatable_nav"],
                "expectation.liquidatable_nav",
            ),
            quants_lab_artifact_hash=_text(
                expectation_payload["quants_lab_artifact_hash"],
                "expectation.quants_lab_artifact_hash",
            ),
        )
    except (EpisodeAccountingError, TypeError) as error:
        raise ParityResultVerificationError(f"invalid expectation: {error}") from error
    if (
        _text(
            expectation_payload["expectation_hash"],
            "expectation.expectation_hash",
        )
        != expectation.expectation_hash
    ):
        raise ParityResultVerificationError("expectation hash mismatch")

    mismatches_value = payload["mismatches"]
    if not isinstance(mismatches_value, list):
        raise ParityResultVerificationError("mismatches must be an array")
    mismatches = tuple(
        _text(item, f"mismatches[{index}]")
        for index, item in enumerate(mismatches_value)
    )
    try:
        result = QuantsLabParityResult(
            status=ParityStatus(_text(payload["status"], "status")),
            mismatches=mismatches,
            actual=actual,
            expectation=expectation,
            tolerance=_decimal(payload["tolerance"], "tolerance"),
            authority=_authority(payload["authority"]),
        )
    except (EpisodeAccountingError, TypeError, ValueError) as error:
        raise ParityResultVerificationError(f"invalid parity result: {error}") from error
    if _text(payload["result_hash"], "result_hash") != result.result_hash:
        raise ParityResultVerificationError("parity result hash mismatch")
    return result


def _read_regular_bytes(path: Path) -> bytes:
    candidate = Path(path)
    if candidate.is_symlink():
        raise ParityResultVerificationError("result must not be a symlink")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(candidate, flags)
    except OSError as error:
        raise ParityResultVerificationError(f"cannot open result: {error}") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ParityResultVerificationError("result must be a regular file")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def read_verified_parity_result(path: Path) -> QuantsLabParityResult:
    try:
        payload = json.loads(_read_regular_bytes(path).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ParityResultVerificationError(
            f"cannot parse parity result: {error}"
        ) from error
    return parity_result_from_wire(payload)
