from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from controllers.directional_trading.rsi_v5_core.perp_episode_accounting import (
    EpisodeAccountSnapshot,
    EpisodeAccountingError,
    LinearPerpPositionSnapshot,
    ParityExpectation,
    QuantsLabParityResult,
    verify_quants_lab_parity,
)

_DECIMAL_TEXT = re.compile(r"^-?(0|[1-9][0-9]*)(\.[0-9]+)?$")
_RUN_KEYS = {"schema_version", "snapshot", "expectation", "tolerance"}
_SNAPSHOT_KEYS = {
    "account_id",
    "as_of_ns",
    "collateral_cash",
    "accrued_funding_cashflow",
    "positions",
}
_POSITION_KEYS = {
    "instrument_id",
    "quantity",
    "entry_price",
    "executable_bid",
    "executable_ask",
    "close_fee_rate",
    "close_impact_rate",
    "contract_multiplier",
}
_EXPECTATION_KEYS = {
    "close_notional",
    "unrealized_pnl_at_close",
    "close_fee_cost",
    "close_impact_cost",
    "liquidatable_nav",
    "quants_lab_artifact_hash",
}


class ParityWireError(ValueError):
    pass


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ParityWireError(f"{label} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise ParityWireError(f"{label} keys must be strings")
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
        raise ParityWireError(
            f"{label} keys drift: missing={missing} extra={extra}"
        )
    return mapping


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ParityWireError(f"{label} must be non-empty text")
    return value


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ParityWireError(f"{label} must be a non-negative integer")
    return value


def _decimal(value: object, label: str) -> Decimal:
    if not isinstance(value, str) or _DECIMAL_TEXT.fullmatch(value) is None:
        raise ParityWireError(f"{label} must be canonical decimal text")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise ParityWireError(f"{label} is invalid decimal text") from error
    if not parsed.is_finite():
        raise ParityWireError(f"{label} must be finite decimal text")
    if parsed.is_zero() and parsed.is_signed():
        raise ParityWireError(f"{label} must not use negative zero")
    return parsed


def _array(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ParityWireError(f"{label} must be an array")
    return value


def _parse_position(value: object, index: int) -> LinearPerpPositionSnapshot:
    label = f"snapshot.positions[{index}]"
    payload = _exact_keys(value, _POSITION_KEYS, label)
    try:
        return LinearPerpPositionSnapshot(
            instrument_id=_text(payload["instrument_id"], f"{label}.instrument_id"),
            quantity=_decimal(payload["quantity"], f"{label}.quantity"),
            entry_price=_decimal(payload["entry_price"], f"{label}.entry_price"),
            executable_bid=_decimal(
                payload["executable_bid"], f"{label}.executable_bid"
            ),
            executable_ask=_decimal(
                payload["executable_ask"], f"{label}.executable_ask"
            ),
            close_fee_rate=_decimal(
                payload["close_fee_rate"], f"{label}.close_fee_rate"
            ),
            close_impact_rate=_decimal(
                payload["close_impact_rate"], f"{label}.close_impact_rate"
            ),
            contract_multiplier=_decimal(
                payload["contract_multiplier"], f"{label}.contract_multiplier"
            ),
        )
    except (EpisodeAccountingError, TypeError) as error:
        raise ParityWireError(f"invalid {label}: {error}") from error


def _parse_snapshot(value: object) -> EpisodeAccountSnapshot:
    payload = _exact_keys(value, _SNAPSHOT_KEYS, "snapshot")
    positions = tuple(
        _parse_position(item, index)
        for index, item in enumerate(_array(payload["positions"], "snapshot.positions"))
    )
    try:
        return EpisodeAccountSnapshot(
            account_id=_text(payload["account_id"], "snapshot.account_id"),
            as_of_ns=_integer(payload["as_of_ns"], "snapshot.as_of_ns"),
            collateral_cash=_decimal(
                payload["collateral_cash"], "snapshot.collateral_cash"
            ),
            accrued_funding_cashflow=_decimal(
                payload["accrued_funding_cashflow"],
                "snapshot.accrued_funding_cashflow",
            ),
            positions=positions,
        )
    except (EpisodeAccountingError, TypeError) as error:
        raise ParityWireError(f"invalid snapshot: {error}") from error


def _parse_expectation(value: object) -> ParityExpectation:
    payload = _exact_keys(value, _EXPECTATION_KEYS, "expectation")
    try:
        return ParityExpectation(
            close_notional=_decimal(
                payload["close_notional"], "expectation.close_notional"
            ),
            unrealized_pnl_at_close=_decimal(
                payload["unrealized_pnl_at_close"],
                "expectation.unrealized_pnl_at_close",
            ),
            close_fee_cost=_decimal(
                payload["close_fee_cost"], "expectation.close_fee_cost"
            ),
            close_impact_cost=_decimal(
                payload["close_impact_cost"], "expectation.close_impact_cost"
            ),
            liquidatable_nav=_decimal(
                payload["liquidatable_nav"], "expectation.liquidatable_nav"
            ),
            quants_lab_artifact_hash=_text(
                payload["quants_lab_artifact_hash"],
                "expectation.quants_lab_artifact_hash",
            ),
        )
    except (EpisodeAccountingError, TypeError) as error:
        raise ParityWireError(f"invalid expectation: {error}") from error


def parse_parity_payload(
    value: object,
) -> tuple[EpisodeAccountSnapshot, ParityExpectation, Decimal]:
    payload = _exact_keys(value, _RUN_KEYS, "run")
    if payload["schema_version"] != "hummingbot.perp_episode_parity_run.v1":
        raise ParityWireError("unsupported parity run schema")
    snapshot = _parse_snapshot(payload["snapshot"])
    expectation = _parse_expectation(payload["expectation"])
    tolerance = _decimal(payload["tolerance"], "tolerance")
    if tolerance < 0:
        raise ParityWireError("tolerance must be non-negative")
    return snapshot, expectation, tolerance


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def parity_result_to_wire(result: QuantsLabParityResult) -> dict[str, Any]:
    if not isinstance(result, QuantsLabParityResult):
        raise TypeError("result must be QuantsLabParityResult")
    return {
        "schema_version": "hummingbot.quants_lab_parity_result.v1",
        "status": result.status.value,
        "mismatches": list(result.mismatches),
        "actual": {
            "collateral_cash": _decimal_text(result.actual.collateral_cash),
            "accrued_funding_cashflow": _decimal_text(
                result.actual.accrued_funding_cashflow
            ),
            "unrealized_pnl_at_close": _decimal_text(
                result.actual.unrealized_pnl_at_close
            ),
            "close_notional": _decimal_text(result.actual.close_notional),
            "close_fee_cost": _decimal_text(result.actual.close_fee_cost),
            "close_impact_cost": _decimal_text(result.actual.close_impact_cost),
            "nav": _decimal_text(result.actual.nav),
            "position_count": result.actual.position_count,
            "breakdown_hash": result.actual.breakdown_hash,
        },
        "expectation": {
            "close_notional": _decimal_text(result.expectation.close_notional),
            "unrealized_pnl_at_close": _decimal_text(
                result.expectation.unrealized_pnl_at_close
            ),
            "close_fee_cost": _decimal_text(result.expectation.close_fee_cost),
            "close_impact_cost": _decimal_text(
                result.expectation.close_impact_cost
            ),
            "liquidatable_nav": _decimal_text(
                result.expectation.liquidatable_nav
            ),
            "quants_lab_artifact_hash": (
                result.expectation.quants_lab_artifact_hash
            ),
            "expectation_hash": result.expectation.expectation_hash,
        },
        "tolerance": _decimal_text(result.tolerance),
        "execution_authority": result.execution_authority,
        "authority": dict(result.authority),
        "result_hash": result.result_hash,
    }


def evaluate_parity_payload(value: object) -> dict[str, Any]:
    snapshot, expectation, tolerance = parse_parity_payload(value)
    result = verify_quants_lab_parity(
        snapshot,
        expectation,
        tolerance=tolerance,
    )
    return parity_result_to_wire(result)


def canonical_result_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ParityWireError(f"result is not canonical JSON: {error}") from error


def _read_regular_bytes(path: Path, label: str) -> bytes:
    candidate = Path(path)
    if candidate.is_symlink():
        raise ParityWireError(f"{label} must not be a symlink")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(candidate, flags)
    except OSError as error:
        raise ParityWireError(f"cannot open {label}: {error}") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ParityWireError(f"{label} must be a regular file")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def read_parity_payload(path: Path) -> object:
    try:
        return json.loads(_read_regular_bytes(path, "input").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ParityWireError(f"cannot parse parity input: {error}") from error


def write_parity_result(path: Path, result: Mapping[str, Any]) -> None:
    destination = Path(path)
    if destination.is_symlink():
        raise ParityWireError("output must not be a symlink")
    if destination.exists() and not destination.is_file():
        raise ParityWireError("output must be a regular file")
    payload = canonical_result_bytes(result)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and _read_regular_bytes(destination, "output") == payload:
        return
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.tmp-",
    )
    temporary = Path(temporary_name)
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise ParityWireError("output write made no progress")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        if destination.is_symlink():
            raise ParityWireError("output became a symlink during publication")
        os.replace(temporary, destination)
        directory_flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            directory_flags |= os.O_DIRECTORY
        directory = os.open(destination.parent, directory_flags)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError as error:
        raise ParityWireError(f"cannot publish parity result: {error}") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def run_parity_files(*, input_path: Path, output_path: Path) -> dict[str, Any]:
    source = Path(input_path)
    destination = Path(output_path)
    if source.resolve(strict=False) == destination.resolve(strict=False):
        raise ParityWireError("input and output paths must differ")
    payload = read_parity_payload(source)
    result = evaluate_parity_payload(payload)
    write_parity_result(destination, result)
    return result
