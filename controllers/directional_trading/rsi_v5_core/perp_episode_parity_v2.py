from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any

from controllers.directional_trading.rsi_v5_core.perp_episode_accounting import (
    EpisodeAccountingError,
    EpisodeAccountSnapshot,
    EpisodeTransitionAttribution,
    TerminalOutcome,
    TerminalSettlementMode,
    terminal_nav_evidence,
)
from controllers.directional_trading.rsi_v5_core.perp_episode_parity_io import (
    ParityWireError,
    _parse_snapshot,
    write_parity_result,
)

_DECIMAL_TEXT = re.compile(r"^-?(0|[1-9][0-9]*)(\.[0-9]+)?$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
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
_RUN_KEYS = {
    "schema_version",
    "episode_id",
    "before_snapshot",
    "after_snapshot",
    "attribution",
    "settlement_mode",
    "fill_identity",
    "quants_lab_artifact_sha256",
    "tolerance",
}
_ATTRIBUTION_FIELDS = (
    "gross_market_pnl",
    "funding_cashflow",
    "fee_cost",
    "spread_cost",
    "slippage_cost",
    "impact_cost",
    "borrow_cost",
    "liquidation_penalty",
    "close_cost_reserve_change",
    "ending_external_cashflow",
)
_ECONOMICS_KEYS = {
    "episode_id",
    "nav_before",
    "nav_after",
    "performance_nav_after",
    "reward",
    "attribution",
    "settlement_mode",
    "terminal_outcome",
    "fill_identity",
    "timestamp_identity",
}
_ARTIFACT_KEYS = _ECONOMICS_KEYS | {"schema_version", "authority"}
_RESULT_KEYS = {
    "schema_version",
    "status",
    "mismatches",
    "actual",
    "expectation",
    "quants_lab_artifact_sha256",
    "tolerance",
    "execution_authority",
    "authority",
    "result_hash",
}
_FILL_IDENTITY_KEYS = {
    "fill_ids",
    "fill_timestamps_ns",
    "first_fill_ns",
    "last_fill_ns",
}
_TIMESTAMP_IDENTITY_KEYS = {"before_as_of_ns", "after_as_of_ns"}
_REWARD_KEYS = {"kind", "value"}


class ParityV2Error(ValueError):
    pass


def evaluate_v2_payload(value: object, quants_artifact_bytes: bytes) -> dict[str, Any]:
    actual, tolerance, declared_artifact_hash = _actual_economics(value)
    observed_artifact_hash = hashlib.sha256(quants_artifact_bytes).hexdigest()
    if observed_artifact_hash != declared_artifact_hash:
        raise ParityV2Error(
            "quants artifact SHA-256 mismatch: "
            f"declared={declared_artifact_hash} observed={observed_artifact_hash}"
        )
    expectation = _parse_artifact_bytes(quants_artifact_bytes)
    mismatches = _compare_economics(actual, expectation, tolerance)
    result: dict[str, Any] = {
        "schema_version": "hummingbot.quants_lab_episode_parity_result.v2",
        "status": "MATCH" if not mismatches else "MISMATCH",
        "mismatches": list(mismatches),
        "actual": actual,
        "expectation": expectation,
        "quants_lab_artifact_sha256": observed_artifact_hash,
        "tolerance": _decimal_text(tolerance),
        "execution_authority": False,
        "authority": _authority_false(),
    }
    result["result_hash"] = _canonical_hash(result)
    return result


def run_parity_v2_files(
    *, input_path: Path, quants_artifact_path: Path, output_path: Path
) -> dict[str, Any]:
    source = Path(input_path)
    artifact = Path(quants_artifact_path)
    destination = Path(output_path)
    resolved = {
        source.resolve(strict=False),
        artifact.resolve(strict=False),
        destination.resolve(strict=False),
    }
    if len(resolved) != 3:
        raise ParityV2Error("input, quants artifact, and output paths must differ")
    value = _read_json_file(source, "input")
    artifact_bytes = _read_regular_bytes(artifact, "quants artifact")
    result = evaluate_v2_payload(value, artifact_bytes)
    try:
        write_parity_result(destination, result)
    except (OSError, ParityWireError) as error:
        raise ParityV2Error(str(error)) from error
    return result


def read_verified_parity_v2_result(path: Path) -> dict[str, Any]:
    return _verified_result(_read_json_file(Path(path), "result"))


def verify_parity_v2_pair(
    *, input_path: Path, quants_artifact_path: Path, result_path: Path
) -> dict[str, Any]:
    source = Path(input_path)
    artifact = Path(quants_artifact_path)
    result_file = Path(result_path)
    if len(
        {
            source.resolve(strict=False),
            artifact.resolve(strict=False),
            result_file.resolve(strict=False),
        }
    ) != 3:
        raise ParityV2Error("input, quants artifact, and result paths must differ")
    run_value = _read_json_file(source, "input")
    artifact_bytes = _read_regular_bytes(artifact, "quants artifact")
    expected = evaluate_v2_payload(run_value, artifact_bytes)
    recorded = read_verified_parity_v2_result(result_file)
    if expected != recorded:
        raise ParityV2Error(
            "persisted v2 parity result does not match exact input/artifact re-evaluation"
        )
    pair: dict[str, Any] = {
        "schema_version": "hummingbot.perp_episode_parity_pair.v2",
        "status": recorded["status"],
        "pair_match": True,
        "input_sha256": hashlib.sha256(_read_regular_bytes(source, "input")).hexdigest(),
        "quants_lab_artifact_sha256": hashlib.sha256(artifact_bytes).hexdigest(),
        "result_hash": recorded["result_hash"],
        "execution_authority": False,
        "authority": _authority_false(),
    }
    pair["pair_hash"] = _canonical_hash(pair)
    return pair


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ParityV2Error(f"{label} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise ParityV2Error(f"{label} keys must be strings")
    return value


def _exact_keys(value: object, expected: set[str], label: str) -> Mapping[str, Any]:
    payload = _mapping(value, label)
    if set(payload) != expected:
        raise ParityV2Error(
            f"{label} keys drift: missing={sorted(expected - set(payload))} "
            f"extra={sorted(set(payload) - expected)}"
        )
    return payload


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ParityV2Error(f"{label} must be non-empty text")
    return value


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ParityV2Error(f"{label} must be a non-negative integer")
    return value


def _decimal(value: object, label: str) -> Decimal:
    if not isinstance(value, str) or _DECIMAL_TEXT.fullmatch(value) is None:
        raise ParityV2Error(f"{label} must be canonical decimal text")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise ParityV2Error(f"{label} is invalid decimal text") from error
    if not parsed.is_finite():
        raise ParityV2Error(f"{label} must be finite decimal text")
    if parsed.is_zero() and parsed.is_signed():
        raise ParityV2Error(f"{label} must not use negative zero")
    return parsed


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ParityV2Error(f"{label} must be a lowercase SHA-256 hash")
    return value


def _false_authority(value: object, label: str = "authority") -> dict[str, bool]:
    payload = _exact_keys(value, _AUTHORITY_KEYS, label)
    result: dict[str, bool] = {}
    for key in sorted(payload):
        if type(payload[key]) is not bool:
            raise ParityV2Error(f"{label}.{key} must be boolean")
        result[key] = payload[key]
    if any(result.values()):
        raise ParityV2Error(f"{label} escalation is forbidden")
    return result


def _authority_false() -> dict[str, bool]:
    return {key: False for key in sorted(_AUTHORITY_KEYS)}


def _parse_attribution(
    value: object, label: str
) -> tuple[EpisodeTransitionAttribution, dict[str, str]]:
    payload = _exact_keys(value, set(_ATTRIBUTION_FIELDS), label)
    decimals = {
        name: _decimal(payload[name], f"{label}.{name}")
        for name in _ATTRIBUTION_FIELDS
    }
    try:
        attribution = EpisodeTransitionAttribution(**decimals)
    except (EpisodeAccountingError, TypeError) as error:
        raise ParityV2Error(f"invalid {label}: {error}") from error
    return attribution, {
        name: _decimal_text(decimals[name]) for name in _ATTRIBUTION_FIELDS
    }


def _parse_fill_identity(value: object, label: str) -> dict[str, Any]:
    payload = _exact_keys(value, _FILL_IDENTITY_KEYS, label)
    fill_ids_value = payload["fill_ids"]
    if not isinstance(fill_ids_value, list):
        raise ParityV2Error(f"{label}.fill_ids must be an array")
    fill_ids = [
        _text(item, f"{label}.fill_ids[{index}]")
        for index, item in enumerate(fill_ids_value)
    ]
    if len(fill_ids) != len(set(fill_ids)):
        raise ParityV2Error(f"{label}.fill_ids must be unique")
    timestamps_value = payload["fill_timestamps_ns"]
    if not isinstance(timestamps_value, list):
        raise ParityV2Error(f"{label}.fill_timestamps_ns must be an array")
    fill_timestamps = [
        _integer(item, f"{label}.fill_timestamps_ns[{index}]")
        for index, item in enumerate(timestamps_value)
    ]
    if len(fill_timestamps) != len(fill_ids):
        raise ParityV2Error(
            f"{label}.fill_timestamps_ns must align one-to-one with fill_ids"
        )
    if fill_timestamps != sorted(fill_timestamps):
        raise ParityV2Error(f"{label}.fill_timestamps_ns must be monotonic")
    if not fill_ids:
        if payload["first_fill_ns"] is not None or payload["last_fill_ns"] is not None:
            raise ParityV2Error(
                f"{label} empty fill IDs require null fill timestamps"
            )
        return {
            "fill_ids": [],
            "fill_timestamps_ns": [],
            "first_fill_ns": None,
            "last_fill_ns": None,
        }
    first_fill_ns = _integer(payload["first_fill_ns"], f"{label}.first_fill_ns")
    last_fill_ns = _integer(payload["last_fill_ns"], f"{label}.last_fill_ns")
    if first_fill_ns > last_fill_ns:
        raise ParityV2Error(f"{label} timestamps must be monotonic")
    if fill_timestamps[0] != first_fill_ns or fill_timestamps[-1] != last_fill_ns:
        raise ParityV2Error(
            f"{label} first/last timestamps must match the fill timestamp sequence"
        )
    return {
        "fill_ids": fill_ids,
        "fill_timestamps_ns": fill_timestamps,
        "first_fill_ns": first_fill_ns,
        "last_fill_ns": last_fill_ns,
    }


def _parse_timestamp_identity(value: object, label: str) -> dict[str, int]:
    payload = _exact_keys(value, _TIMESTAMP_IDENTITY_KEYS, label)
    before = _integer(payload["before_as_of_ns"], f"{label}.before_as_of_ns")
    after = _integer(payload["after_as_of_ns"], f"{label}.after_as_of_ns")
    if after <= before:
        raise ParityV2Error(f"{label} must increase monotonically")
    return {"before_as_of_ns": before, "after_as_of_ns": after}


def _parse_reward(value: object, label: str) -> dict[str, Any]:
    payload = _exact_keys(value, _REWARD_KEYS, label)
    kind = _text(payload["kind"], f"{label}.kind")
    if kind == "LOG_RETURN":
        reward_value = _decimal(payload["value"], f"{label}.value")
        return {"kind": kind, "value": _decimal_text(reward_value)}
    if kind not in {TerminalOutcome.LIQUIDATED.value, TerminalOutcome.INSOLVENT.value}:
        raise ParityV2Error(f"{label}.kind is invalid")
    if payload["value"] is not None:
        raise ParityV2Error(f"{label}.value must be null for terminal reward")
    return {"kind": kind, "value": None}


def _parse_economics(value: object, label: str) -> dict[str, Any]:
    payload = _exact_keys(value, _ECONOMICS_KEYS, label)
    attribution, attribution_wire = _parse_attribution(
        payload["attribution"], f"{label}.attribution"
    )
    nav_before = _decimal(payload["nav_before"], f"{label}.nav_before")
    nav_after = _decimal(payload["nav_after"], f"{label}.nav_after")
    performance_nav_after = _decimal(
        payload["performance_nav_after"],
        f"{label}.performance_nav_after",
    )
    try:
        settlement_mode = TerminalSettlementMode(
            _text(payload["settlement_mode"], f"{label}.settlement_mode")
        )
        terminal_outcome = TerminalOutcome(
            _text(payload["terminal_outcome"], f"{label}.terminal_outcome")
        )
    except ValueError as error:
        raise ParityV2Error(f"{label} terminal enum is invalid") from error
    reward = _parse_reward(payload["reward"], f"{label}.reward")
    if terminal_outcome is TerminalOutcome.SOLVENT and reward["kind"] != "LOG_RETURN":
        raise ParityV2Error(f"{label} solvent outcome requires LOG_RETURN reward")
    if terminal_outcome is not TerminalOutcome.SOLVENT and reward["kind"] != terminal_outcome.value:
        raise ParityV2Error(f"{label} terminal outcome and reward kind disagree")
    expected_outcome = (
        TerminalOutcome.SOLVENT
        if nav_after > 0
        else TerminalOutcome.LIQUIDATED
        if nav_after == 0
        else TerminalOutcome.INSOLVENT
    )
    if terminal_outcome is not expected_outcome:
        raise ParityV2Error(f"{label} terminal outcome does not match nav_after")
    if nav_before <= 0:
        raise ParityV2Error(f"{label} nav_before must be positive")
    if nav_after != nav_before + attribution.total_nav_change:
        raise ParityV2Error(f"{label} attribution does not reconcile with NAV")
    if performance_nav_after != nav_after - attribution.ending_external_cashflow:
        raise ParityV2Error(
            f"{label} performance NAV does not reconcile with external cashflow"
        )
    fill_identity = _parse_fill_identity(
        payload["fill_identity"], f"{label}.fill_identity"
    )
    timestamp_identity = _parse_timestamp_identity(
        payload["timestamp_identity"], f"{label}.timestamp_identity"
    )
    if fill_identity["fill_ids"] and not (
        timestamp_identity["before_as_of_ns"]
        <= fill_identity["first_fill_ns"]
        <= fill_identity["last_fill_ns"]
        <= timestamp_identity["after_as_of_ns"]
    ):
        raise ParityV2Error(f"{label} fill timestamps must be inside snapshot timestamps")
    return {
        "episode_id": _text(payload["episode_id"], f"{label}.episode_id"),
        "nav_before": _decimal_text(nav_before),
        "nav_after": _decimal_text(nav_after),
        "performance_nav_after": _decimal_text(performance_nav_after),
        "reward": reward,
        "attribution": attribution_wire,
        "settlement_mode": settlement_mode.value,
        "terminal_outcome": terminal_outcome.value,
        "fill_identity": fill_identity,
        "timestamp_identity": timestamp_identity,
    }


def _parse_artifact_bytes(value: bytes) -> dict[str, Any]:
    if not isinstance(value, bytes):
        raise TypeError("quants_artifact_bytes must be bytes")
    try:
        decoded = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ParityV2Error(f"cannot parse quants artifact: {error}") from error
    payload = _exact_keys(decoded, _ARTIFACT_KEYS, "quants artifact")
    if payload["schema_version"] != "quants_lab.perp_episode_parity_artifact.v2":
        raise ParityV2Error("unsupported quants artifact schema")
    _false_authority(payload["authority"], "quants artifact.authority")
    return _parse_economics(
        {key: payload[key] for key in _ECONOMICS_KEYS}, "quants artifact"
    )


def _parse_snapshot_v2(value: object, label: str) -> EpisodeAccountSnapshot:
    try:
        return _parse_snapshot(value)
    except ParityWireError as error:
        raise ParityV2Error(f"invalid {label}: {error}") from error


def _actual_economics(value: object) -> tuple[dict[str, Any], Decimal, str]:
    payload = _exact_keys(value, _RUN_KEYS, "run")
    if payload["schema_version"] != "hummingbot.perp_episode_parity_run.v2":
        raise ParityV2Error("unsupported parity run schema")
    episode_id = _text(payload["episode_id"], "run.episode_id")
    before = _parse_snapshot_v2(payload["before_snapshot"], "before_snapshot")
    after = _parse_snapshot_v2(payload["after_snapshot"], "after_snapshot")
    if before.account_id != after.account_id:
        raise ParityV2Error("snapshot account identity changed")
    if after.as_of_ns <= before.as_of_ns:
        raise ParityV2Error("snapshot time must increase monotonically")
    attribution, attribution_wire = _parse_attribution(
        payload["attribution"], "run.attribution"
    )
    try:
        settlement_mode = TerminalSettlementMode(
            _text(payload["settlement_mode"], "run.settlement_mode")
        )
    except ValueError as error:
        raise ParityV2Error("run.settlement_mode is invalid") from error
    fill_identity = _parse_fill_identity(payload["fill_identity"], "run.fill_identity")
    if fill_identity["fill_ids"] and not (
        before.as_of_ns
        <= fill_identity["first_fill_ns"]
        <= fill_identity["last_fill_ns"]
        <= after.as_of_ns
    ):
        raise ParityV2Error("fill timestamps must be inside snapshot timestamps")
    artifact_sha256 = _sha256(
        payload["quants_lab_artifact_sha256"],
        "run.quants_lab_artifact_sha256",
    )
    tolerance = _decimal(payload["tolerance"], "run.tolerance")
    if tolerance < 0:
        raise ParityV2Error("run.tolerance must be non-negative")

    before_nav = terminal_nav_evidence(before)
    after_nav = terminal_nav_evidence(after)
    if before_nav.terminal_outcome is not TerminalOutcome.SOLVENT:
        raise ParityV2Error("episode must start with positive liquidatable NAV")
    with localcontext() as context:
        context.prec = 50
        expected_after = before_nav.nav + attribution.total_nav_change
        reconciliation_error = after_nav.nav - expected_after
        if abs(reconciliation_error) > tolerance:
            raise ParityV2Error(
                "transition attribution does not reconcile with liquidatable NAV: "
                f"error={reconciliation_error} tolerance={tolerance}"
            )
        performance_after = after_nav.nav - attribution.ending_external_cashflow
        if after_nav.terminal_outcome is TerminalOutcome.SOLVENT:
            if performance_after <= 0:
                raise ParityV2Error(
                    "solvent terminal outcome requires positive performance NAV"
                )
            reward = {
                "kind": "LOG_RETURN",
                "value": _decimal_text((performance_after / before_nav.nav).ln()),
            }
        else:
            reward = {"kind": after_nav.terminal_outcome.value, "value": None}
    actual = {
        "episode_id": episode_id,
        "nav_before": _decimal_text(before_nav.nav),
        "nav_after": _decimal_text(after_nav.nav),
        "performance_nav_after": _decimal_text(performance_after),
        "reward": reward,
        "attribution": attribution_wire,
        "settlement_mode": settlement_mode.value,
        "terminal_outcome": after_nav.terminal_outcome.value,
        "fill_identity": fill_identity,
        "timestamp_identity": {
            "before_as_of_ns": before.as_of_ns,
            "after_as_of_ns": after.as_of_ns,
        },
    }
    return actual, tolerance, artifact_sha256


def _numeric_mismatch(actual: str, expected: str, tolerance: Decimal) -> bool:
    return abs(Decimal(actual) - Decimal(expected)) > tolerance


def _compare_economics(
    actual: Mapping[str, Any], expectation: Mapping[str, Any], tolerance: Decimal
) -> tuple[str, ...]:
    mismatches: list[str] = []
    if actual["episode_id"] != expectation["episode_id"]:
        mismatches.append("episode_id")
    for field in ("nav_before", "nav_after", "performance_nav_after"):
        if _numeric_mismatch(actual[field], expectation[field], tolerance):
            mismatches.append(field)
    if actual["reward"]["kind"] != expectation["reward"]["kind"]:
        mismatches.append("reward.kind")
    else:
        actual_reward = actual["reward"]["value"]
        expected_reward = expectation["reward"]["value"]
        if (actual_reward is None) != (expected_reward is None) or (
            actual_reward is not None
            and expected_reward is not None
            and _numeric_mismatch(actual_reward, expected_reward, tolerance)
        ):
            mismatches.append("reward.value")
    for field in _ATTRIBUTION_FIELDS:
        if _numeric_mismatch(
            actual["attribution"][field],
            expectation["attribution"][field],
            tolerance,
        ):
            mismatches.append(f"attribution.{field}")
    for field in ("settlement_mode", "terminal_outcome"):
        if actual[field] != expectation[field]:
            mismatches.append(field)
    for field in (
        "fill_ids",
        "fill_timestamps_ns",
        "first_fill_ns",
        "last_fill_ns",
    ):
        if actual["fill_identity"][field] != expectation["fill_identity"][field]:
            mismatches.append(f"fill_identity.{field}")
    for field in ("before_as_of_ns", "after_as_of_ns"):
        if (
            actual["timestamp_identity"][field]
            != expectation["timestamp_identity"][field]
        ):
            mismatches.append(f"timestamp_identity.{field}")
    return tuple(mismatches)


def _canonical_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _read_regular_bytes(path: Path, label: str) -> bytes:
    candidate = Path(path)
    if candidate.is_symlink():
        raise ParityV2Error(f"{label} must not be a symlink")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(candidate, flags)
    except OSError as error:
        raise ParityV2Error(f"cannot open {label}: {error}") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ParityV2Error(f"{label} must be a regular file")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _read_json_file(path: Path, label: str) -> object:
    try:
        return json.loads(_read_regular_bytes(path, label).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ParityV2Error(f"cannot parse {label}: {error}") from error


def read_quants_artifact_sha256(path: Path) -> str:
    return hashlib.sha256(_read_regular_bytes(Path(path), "quants artifact")).hexdigest()


def _verified_result(value: object) -> dict[str, Any]:
    payload = _exact_keys(value, _RESULT_KEYS, "result")
    if payload["schema_version"] != "hummingbot.quants_lab_episode_parity_result.v2":
        raise ParityV2Error("unsupported parity result schema")
    if payload["execution_authority"] is not False:
        raise ParityV2Error("execution authority escalation is forbidden")
    _false_authority(payload["authority"])
    actual = _parse_economics(payload["actual"], "result.actual")
    expectation = _parse_economics(payload["expectation"], "result.expectation")
    artifact_hash = _sha256(
        payload["quants_lab_artifact_sha256"],
        "result.quants_lab_artifact_sha256",
    )
    tolerance = _decimal(payload["tolerance"], "result.tolerance")
    if tolerance < 0:
        raise ParityV2Error("result.tolerance must be non-negative")
    mismatches_value = payload["mismatches"]
    if not isinstance(mismatches_value, list) or any(
        not isinstance(item, str) or not item for item in mismatches_value
    ):
        raise ParityV2Error("result.mismatches must be an array of text")
    expected_mismatches = list(_compare_economics(actual, expectation, tolerance))
    if mismatches_value != expected_mismatches:
        raise ParityV2Error("result mismatch list does not match complete comparison")
    expected_status = "MATCH" if not expected_mismatches else "MISMATCH"
    if payload["status"] != expected_status:
        raise ParityV2Error("result status does not match complete comparison")
    without_hash = {key: payload[key] for key in _RESULT_KEYS - {"result_hash"}}
    if _sha256(payload["result_hash"], "result.result_hash") != _canonical_hash(
        without_hash
    ):
        raise ParityV2Error("result hash mismatch")
    return {
        **without_hash,
        "quants_lab_artifact_sha256": artifact_hash,
        "result_hash": payload["result_hash"],
    }
