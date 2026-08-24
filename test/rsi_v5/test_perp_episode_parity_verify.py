from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from controllers.directional_trading.rsi_v5_core.perp_episode_parity_result import (
    read_verified_parity_result,
)

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts/run_perp_episode_parity.py"
VERIFIER = ROOT / "scripts/verify_perp_episode_parity.py"


def payload(*, expected_nav: str = "8982.0070") -> dict[str, object]:
    return {
        "schema_version": "hummingbot.perp_episode_parity_run.v1",
        "snapshot": {
            "account_id": "paper-account",
            "as_of_ns": 1000,
            "collateral_cash": "9000",
            "accrued_funding_cashflow": "-1",
            "positions": [
                {
                    "instrument_id": "hyperliquid:perp:BTC",
                    "quantity": "1",
                    "entry_price": "10000",
                    "executable_bid": "9990",
                    "executable_ask": "10010",
                    "close_fee_rate": "0.0005",
                    "close_impact_rate": "0.0002",
                    "contract_multiplier": "1",
                }
            ],
        },
        "expectation": {
            "close_notional": "9990",
            "unrealized_pnl_at_close": "-10",
            "close_fee_cost": "4.9950",
            "close_impact_cost": "1.9980",
            "liquidatable_nav": expected_nav,
            "quants_lab_artifact_hash": "1" * 64,
        },
        "tolerance": "0.000000000000000001",
    }


def run(input_path: Path, output_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--input",
            str(input_path),
            "--output",
            str(output_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def verify(path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(VERIFIER), "--result", str(path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_verifier_reconstructs_hashes_and_accepts_only_match(tmp_path: Path) -> None:
    source = tmp_path / "input.json"
    result_path = tmp_path / "result.json"
    source.write_text(json.dumps(payload()), encoding="utf-8")
    assert run(source, result_path).returncode == 0
    verified = read_verified_parity_result(result_path)
    assert verified.status.value == "MATCH"
    completed = verify(result_path)
    assert completed.returncode == 0, completed.stderr
    assert "PARITY_MATCH" in completed.stdout


def test_verifier_returns_distinct_nonzero_for_valid_mismatch(tmp_path: Path) -> None:
    source = tmp_path / "input.json"
    result_path = tmp_path / "result.json"
    source.write_text(json.dumps(payload(expected_nav="8983")), encoding="utf-8")
    assert run(source, result_path).returncode == 0
    completed = verify(result_path)
    assert completed.returncode == 2
    assert "PARITY_MISMATCH" in completed.stderr


def test_verifier_rejects_tampered_hash_and_authority(tmp_path: Path) -> None:
    source = tmp_path / "input.json"
    result_path = tmp_path / "result.json"
    source.write_text(json.dumps(payload()), encoding="utf-8")
    assert run(source, result_path).returncode == 0

    tampered = json.loads(result_path.read_text(encoding="utf-8"))
    tampered["actual"]["nav"] = "9999"
    result_path.write_text(json.dumps(tampered), encoding="utf-8")
    assert verify(result_path).returncode == 1

    assert run(source, result_path).returncode == 0
    tampered = json.loads(result_path.read_text(encoding="utf-8"))
    tampered["authority"]["execution_authority"] = True
    result_path.write_text(json.dumps(tampered), encoding="utf-8")
    assert verify(result_path).returncode == 1
