from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from decimal import Decimal, localcontext
from pathlib import Path

import pytest

from controllers.directional_trading.rsi_v5_core.perp_episode_accounting import (
    EpisodeAccountSnapshot,
    TerminalOutcome,
    terminal_nav_evidence,
)
from controllers.directional_trading.rsi_v5_core.perp_episode_parity_io import parse_parity_payload
from controllers.directional_trading.rsi_v5_core.perp_episode_parity_v2 import (
    ParityV2Error,
    evaluate_v2_payload,
    read_verified_parity_v2_result,
    run_parity_v2_files,
    verify_parity_v2_pair,
)

D = Decimal
ROOT = Path(__file__).resolve().parents[2]
V2_RUNNER = ROOT / "scripts/run_perp_episode_parity_v2.py"
V2_VERIFIER = ROOT / "scripts/verify_perp_episode_parity_v2.py"
V2_PAIR_VERIFIER = ROOT / "scripts/verify_perp_episode_parity_pair_v2.py"


def false_authority() -> dict[str, bool]:
    return {
        "paper_allowed": False,
        "canary_allowed": False,
        "live_allowed": False,
        "live_orders_allowed": False,
        "capital_authority": False,
        "risk_authority": False,
        "execution_authority": False,
        "trading_authority": False,
    }


def snapshot(*, as_of_ns: int, collateral_cash: str) -> dict[str, object]:
    return {
        "account_id": "paper-account",
        "as_of_ns": as_of_ns,
        "collateral_cash": collateral_cash,
        "accrued_funding_cashflow": "0",
        "positions": [],
    }


def attribution(**overrides: str) -> dict[str, str]:
    values = {
        "gross_market_pnl": "7",
        "funding_cashflow": "1",
        "fee_cost": "1",
        "spread_cost": "1",
        "slippage_cost": "1",
        "impact_cost": "1",
        "borrow_cost": "1",
        "liquidation_penalty": "1",
        "close_cost_reserve_change": "1",
        "ending_external_cashflow": "-1",
    }
    values.update(overrides)
    return values


def artifact_payload(**overrides: object) -> dict[str, object]:
    with localcontext() as context:
        context.prec = 50
        reward_value = str(D("1.01").ln())
    values: dict[str, object] = {
        "schema_version": "quants_lab.perp_episode_parity_artifact.v2",
        "episode_id": "episode-7",
        "nav_before": "100",
        "nav_after": "100",
        "performance_nav_after": "101",
        "reward": {"kind": "LOG_RETURN", "value": reward_value},
        "attribution": attribution(),
        "settlement_mode": "force_flat",
        "terminal_outcome": "FLAT",
        "fill_identity": {
            "ordered_fill_ids": ["fill-a", "fill-b", "fill-c"],
            "fill_timestamps_ns": [1200, 1500, 1800],
            "first_fill_timestamp_ns": 1200,
            "last_fill_timestamp_ns": 1800,
        },
        "timestamp_identity": {
            "before_as_of_ns": 1000,
            "after_as_of_ns": 2000,
        },
        "authority": false_authority(),
    }
    values.update(overrides)
    return values


def artifact_bytes(**overrides: object) -> bytes:
    return json.dumps(
        artifact_payload(**overrides),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def run_payload(artifact: bytes, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "schema_version": "hummingbot.perp_episode_parity_run.v2",
        "episode_id": "episode-7",
        "before_snapshot": snapshot(as_of_ns=1000, collateral_cash="100"),
        "after_snapshot": snapshot(as_of_ns=2000, collateral_cash="100"),
        "attribution": attribution(),
        "settlement_mode": "force_flat",
        "fill_identity": {
            "ordered_fill_ids": ["fill-a", "fill-b", "fill-c"],
            "fill_timestamps_ns": [1200, 1500, 1800],
            "first_fill_timestamp_ns": 1200,
            "last_fill_timestamp_ns": 1800,
        },
        "quants_lab_artifact_sha256": hashlib.sha256(artifact).hexdigest(),
        "tolerance": "0.000000000000000001",
    }
    values.update(overrides)
    return values


def legacy_v1_payload() -> dict[str, object]:
    return {
        "schema_version": "hummingbot.perp_episode_parity_run.v1",
        "snapshot": snapshot(as_of_ns=1000, collateral_cash="100"),
        "expectation": {
            "close_notional": "0",
            "unrealized_pnl_at_close": "0",
            "close_fee_cost": "0",
            "close_impact_cost": "0",
            "liquidatable_nav": "100",
            "quants_lab_artifact_hash": "1" * 64,
        },
        "tolerance": "0",
    }


def test_v1_parser_remains_legacy_authority_false_compatible() -> None:
    snapshot_value, expectation, tolerance = parse_parity_payload(legacy_v1_payload())
    assert snapshot_value.collateral_cash == D("100")
    assert expectation.quants_lab_artifact_hash == "1" * 64
    assert tolerance == D("0")


def test_terminal_nav_evidence_classifies_zero_and_negative_nav() -> None:
    zero = terminal_nav_evidence(
        EpisodeAccountSnapshot("paper-account", 1, D("0"), D("0"), ())
    )
    negative = terminal_nav_evidence(
        EpisodeAccountSnapshot("paper-account", 1, D("-1"), D("0"), ())
    )
    assert zero.nav == D("0")
    assert zero.terminal_outcome is TerminalOutcome.LIQUIDATED
    assert negative.nav == D("-1")
    assert negative.terminal_outcome is TerminalOutcome.INSOLVENT


def test_v2_match_binds_complete_episode_and_authority_false() -> None:
    artifact = artifact_bytes()
    result = evaluate_v2_payload(run_payload(artifact), artifact)
    assert result["status"] == "MATCH"
    assert result["mismatches"] == []
    assert result["quants_lab_artifact_sha256"] == hashlib.sha256(artifact).hexdigest()
    assert result["execution_authority"] is False
    assert not any(result["authority"].values())


def test_v2_rejects_substituted_artifact_bytes_before_semantic_comparison() -> None:
    artifact = artifact_bytes()
    substituted = artifact + b"\n"
    with pytest.raises(ParityV2Error, match="artifact SHA-256 mismatch"):
        evaluate_v2_payload(run_payload(artifact), substituted)


def test_v2_rejects_quants_artifact_authority_escalation() -> None:
    authority = false_authority()
    authority["execution_authority"] = True
    artifact = artifact_bytes(authority=authority)
    with pytest.raises(ParityV2Error, match="authority escalation"):
        evaluate_v2_payload(run_payload(artifact), artifact)


def test_v2_rejects_internally_inconsistent_quants_economics() -> None:
    artifact = artifact_bytes(
        terminal_outcome="INSOLVENT",
        reward={"kind": "INSOLVENT", "value": None},
    )
    with pytest.raises(ParityV2Error, match="terminal outcome does not match nav_after"):
        evaluate_v2_payload(run_payload(artifact), artifact)


def test_v2_identical_nav_and_reward_with_different_attribution_is_mismatch() -> None:
    changed_attribution = attribution(gross_market_pnl="8", fee_cost="2")
    artifact = artifact_bytes(attribution=changed_attribution)
    result = evaluate_v2_payload(run_payload(artifact), artifact)
    assert result["status"] == "MISMATCH"
    assert result["mismatches"] == [
        "attribution.gross_market_pnl",
        "attribution.fee_cost",
    ]
    assert result["actual"]["nav_after"] == result["expectation"]["nav_after"]
    assert result["actual"]["reward"] == result["expectation"]["reward"]


@pytest.mark.parametrize(
    "field",
    [
        "funding_cashflow",
        "spread_cost",
        "slippage_cost",
        "impact_cost",
        "borrow_cost",
        "liquidation_penalty",
        "close_cost_reserve_change",
        "ending_external_cashflow",
    ],
)
def test_v2_compares_every_attribution_component(field: str) -> None:
    changed = attribution()
    artifact_overrides: dict[str, object] = {}
    if field == "funding_cashflow":
        changed[field] = "2"
        changed["gross_market_pnl"] = "6"
    elif field == "ending_external_cashflow":
        changed[field] = "0"
        changed["gross_market_pnl"] = "6"
        artifact_overrides = {
            "performance_nav_after": "100",
            "reward": {"kind": "LOG_RETURN", "value": "0"},
        }
    else:
        changed[field] = "2"
        changed["gross_market_pnl"] = "8"
    artifact = artifact_bytes(attribution=changed, **artifact_overrides)
    result = evaluate_v2_payload(run_payload(artifact), artifact)
    assert result["status"] == "MISMATCH"
    assert f"attribution.{field}" in result["mismatches"]


@pytest.mark.parametrize(
    ("artifact_override", "mismatch"),
    [
        (
            {
                "settlement_mode": "mark_to_liquidation",
                "terminal_outcome": "ACTIVE",
            },
            "settlement_mode",
        ),
        (
            {
                "fill_identity": {
                    "ordered_fill_ids": ["fill-a", "fill-x", "fill-c"],
                    "fill_timestamps_ns": [1200, 1500, 1800],
                    "first_fill_timestamp_ns": 1200,
                    "last_fill_timestamp_ns": 1800,
                }
            },
            "fill_identity.ordered_fill_ids",
        ),
        (
            {
                "fill_identity": {
                    "ordered_fill_ids": ["fill-a", "fill-b", "fill-c"],
                    "fill_timestamps_ns": [1200, 1600, 1800],
                    "first_fill_timestamp_ns": 1200,
                    "last_fill_timestamp_ns": 1800,
                }
            },
            "fill_identity.fill_timestamps_ns",
        ),
        (
            {
                "timestamp_identity": {
                    "before_as_of_ns": 1000,
                    "after_as_of_ns": 2001,
                }
            },
            "timestamp_identity.after_as_of_ns",
        ),
    ],
)
def test_v2_compares_settlement_fill_and_timestamp_identity(
    artifact_override: dict[str, object], mismatch: str
) -> None:
    artifact = artifact_bytes(**artifact_override)
    result = evaluate_v2_payload(run_payload(artifact), artifact)
    assert result["status"] == "MISMATCH"
    assert mismatch in result["mismatches"]


def test_v2_represents_episode_without_fills_using_explicit_null_timestamps() -> None:
    no_fills = {
        "ordered_fill_ids": [],
        "fill_timestamps_ns": [],
        "first_fill_timestamp_ns": None,
        "last_fill_timestamp_ns": None,
    }
    artifact = artifact_bytes(fill_identity=no_fills)
    result = evaluate_v2_payload(
        run_payload(artifact, fill_identity=no_fills),
        artifact,
    )
    assert result["status"] == "MATCH"
    assert result["actual"]["fill_identity"] == no_fills


@pytest.mark.parametrize(
    ("after_nav", "gross_pnl", "outcome"),
    [("0", "-100", "LIQUIDATED"), ("-10", "-110", "INSOLVENT")],
)
def test_v2_persists_explicit_nonpositive_terminal_outcomes(
    after_nav: str, gross_pnl: str, outcome: str
) -> None:
    terminal_attribution = attribution(
        gross_market_pnl=gross_pnl,
        funding_cashflow="0",
        fee_cost="0",
        spread_cost="0",
        slippage_cost="0",
        impact_cost="0",
        borrow_cost="0",
        liquidation_penalty="0",
        close_cost_reserve_change="0",
        ending_external_cashflow="0",
    )
    terminal_reward = {"kind": outcome, "value": None}
    artifact = artifact_bytes(
        nav_after=after_nav,
        performance_nav_after=after_nav,
        reward=terminal_reward,
        attribution=terminal_attribution,
        terminal_outcome=outcome,
    )
    run = run_payload(
        artifact,
        after_snapshot=snapshot(as_of_ns=2000, collateral_cash=after_nav),
        attribution=terminal_attribution,
    )
    result = evaluate_v2_payload(run, artifact)
    assert result["status"] == "MATCH"
    assert result["actual"]["terminal_outcome"] == outcome
    assert result["actual"]["reward"] == terminal_reward


def test_v2_persisted_result_and_pair_recheck_exact_files(tmp_path: Path) -> None:
    artifact = artifact_bytes()
    artifact_path = tmp_path / "quants-artifact.json"
    input_path = tmp_path / "run.json"
    result_path = tmp_path / "result.json"
    artifact_path.write_bytes(artifact)
    input_path.write_text(json.dumps(run_payload(artifact)), encoding="utf-8")

    run_parity_v2_files(
        input_path=input_path,
        quants_artifact_path=artifact_path,
        output_path=result_path,
    )
    verified = read_verified_parity_v2_result(result_path)
    pair = verify_parity_v2_pair(
        input_path=input_path,
        quants_artifact_path=artifact_path,
        result_path=result_path,
    )
    assert verified["status"] == "MATCH"
    assert pair["pair_match"] is True
    assert pair["execution_authority"] is False

    artifact_path.write_bytes(artifact + b"\n")
    with pytest.raises(ParityV2Error, match="artifact SHA-256 mismatch"):
        verify_parity_v2_pair(
            input_path=input_path,
            quants_artifact_path=artifact_path,
            result_path=result_path,
        )


def test_v2_verifier_rejects_rehashed_forged_match_and_authority(tmp_path: Path) -> None:
    changed_attribution = attribution(gross_market_pnl="8", fee_cost="2")
    artifact = artifact_bytes(attribution=changed_attribution)
    artifact_path = tmp_path / "quants-artifact.json"
    input_path = tmp_path / "run.json"
    result_path = tmp_path / "result.json"
    artifact_path.write_bytes(artifact)
    input_path.write_text(json.dumps(run_payload(artifact)), encoding="utf-8")
    run_parity_v2_files(
        input_path=input_path,
        quants_artifact_path=artifact_path,
        output_path=result_path,
    )

    forged = json.loads(result_path.read_text(encoding="utf-8"))
    forged["status"] = "MATCH"
    forged["mismatches"] = []
    forged_without_hash = {key: value for key, value in forged.items() if key != "result_hash"}
    forged["result_hash"] = hashlib.sha256(
        json.dumps(
            forged_without_hash,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    result_path.write_text(json.dumps(forged), encoding="utf-8")
    with pytest.raises(ParityV2Error, match="mismatch list"):
        read_verified_parity_v2_result(result_path)

    run_parity_v2_files(
        input_path=input_path,
        quants_artifact_path=artifact_path,
        output_path=result_path,
    )
    escalated = json.loads(result_path.read_text(encoding="utf-8"))
    escalated["authority"]["execution_authority"] = True
    escalated_without_hash = {
        key: value for key, value in escalated.items() if key != "result_hash"
    }
    escalated["result_hash"] = hashlib.sha256(
        json.dumps(
            escalated_without_hash,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    result_path.write_text(json.dumps(escalated), encoding="utf-8")
    with pytest.raises(ParityV2Error, match="authority escalation"):
        read_verified_parity_v2_result(result_path)


def test_v2_cli_requires_exact_artifact_for_readiness(tmp_path: Path) -> None:
    artifact = artifact_bytes()
    artifact_path = tmp_path / "quants-artifact.json"
    input_path = tmp_path / "run.json"
    result_path = tmp_path / "result.json"
    artifact_path.write_bytes(artifact)
    input_path.write_text(json.dumps(run_payload(artifact)), encoding="utf-8")
    run = subprocess.run(
        [
            sys.executable,
            str(V2_RUNNER),
            "--input",
            str(input_path),
            "--quants-artifact",
            str(artifact_path),
            "--output",
            str(result_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert run.returncode == 0, run.stderr
    standalone = subprocess.run(
        [
            sys.executable,
            str(V2_VERIFIER),
            "--input",
            str(input_path),
            "--quants-artifact",
            str(artifact_path),
            "--result",
            str(result_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert standalone.returncode == 0, standalone.stderr
    assert "PARITY_V2_MATCH" in standalone.stdout
    verified = subprocess.run(
        [
            sys.executable,
            str(V2_PAIR_VERIFIER),
            "--input",
            str(input_path),
            "--quants-artifact",
            str(artifact_path),
            "--result",
            str(result_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert verified.returncode == 0, verified.stderr
    assert "PARITY_V2_PAIR_MATCH" in verified.stdout
    assert '"execution_authority":false' in verified.stdout

    artifact_path.write_bytes(artifact + b"\n")
    tampered = subprocess.run(
        [
            sys.executable,
            str(V2_PAIR_VERIFIER),
            "--input",
            str(input_path),
            "--quants-artifact",
            str(artifact_path),
            "--result",
            str(result_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert tampered.returncode == 1
    assert "artifact SHA-256 mismatch" in tampered.stderr


def test_v2_standalone_verifier_requires_hummingbot_input(tmp_path: Path) -> None:
    artifact = artifact_bytes()
    artifact_path = tmp_path / "quants-artifact.json"
    input_path = tmp_path / "run.json"
    result_path = tmp_path / "result.json"
    artifact_path.write_bytes(artifact)
    input_path.write_text(json.dumps(run_payload(artifact)), encoding="utf-8")
    run_parity_v2_files(
        input_path=input_path,
        quants_artifact_path=artifact_path,
        output_path=result_path,
    )
    verified = subprocess.run(
        [
            sys.executable,
            str(V2_VERIFIER),
            "--result",
            str(result_path),
            "--quants-artifact",
            str(artifact_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert verified.returncode == 2
    assert "--input" in verified.stderr


def test_v2_standalone_verifier_rejects_coordinated_semantic_rehash(
    tmp_path: Path,
) -> None:
    original_artifact = artifact_bytes()
    artifact_path = tmp_path / "quants-artifact.json"
    input_path = tmp_path / "run.json"
    result_path = tmp_path / "result.json"
    artifact_path.write_bytes(original_artifact)
    input_path.write_text(
        json.dumps(run_payload(original_artifact)),
        encoding="utf-8",
    )
    run_parity_v2_files(
        input_path=input_path,
        quants_artifact_path=artifact_path,
        output_path=result_path,
    )

    changed_artifact = artifact_bytes(
        settlement_mode="mark_to_liquidation",
        terminal_outcome="ACTIVE",
    )
    artifact_path.write_bytes(changed_artifact)
    changed_artifact_hash = hashlib.sha256(changed_artifact).hexdigest()
    run = json.loads(input_path.read_text(encoding="utf-8"))
    run["quants_lab_artifact_sha256"] = changed_artifact_hash
    input_path.write_text(json.dumps(run), encoding="utf-8")

    forged = json.loads(result_path.read_text(encoding="utf-8"))
    forged["actual"]["settlement_mode"] = "mark_to_liquidation"
    forged["actual"]["terminal_outcome"] = "ACTIVE"
    forged["expectation"]["settlement_mode"] = "mark_to_liquidation"
    forged["expectation"]["terminal_outcome"] = "ACTIVE"
    forged["quants_lab_artifact_sha256"] = changed_artifact_hash
    forged_without_hash = {
        key: value for key, value in forged.items() if key != "result_hash"
    }
    forged["result_hash"] = hashlib.sha256(
        json.dumps(
            forged_without_hash,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    result_path.write_text(json.dumps(forged), encoding="utf-8")

    verified = subprocess.run(
        [
            sys.executable,
            str(V2_VERIFIER),
            "--input",
            str(input_path),
            "--result",
            str(result_path),
            "--quants-artifact",
            str(artifact_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert verified.returncode == 1
    assert "does not match exact input/artifact re-evaluation" in verified.stderr
