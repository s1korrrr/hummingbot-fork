from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from controllers.directional_trading.rsi_v5_core.perp_episode_parity_pair import (
    verify_parity_pair,
)

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts/run_perp_episode_parity.py"
PAIR_VERIFIER = ROOT / "scripts/verify_perp_episode_parity_pair.py"


def payload() -> dict[str, object]:
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
            "liquidatable_nav": "8982.0070",
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


def verify(input_path: Path, result_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(PAIR_VERIFIER),
            "--input",
            str(input_path),
            "--result",
            str(result_path),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_pair_verifier_recomputes_exact_input_result_binding(tmp_path: Path) -> None:
    source = tmp_path / "input.json"
    result_path = tmp_path / "result.json"
    source.write_text(json.dumps(payload()), encoding="utf-8")
    assert run(source, result_path).returncode == 0
    pair = verify_parity_pair(input_path=source, result_path=result_path)
    assert pair.status.value == "MATCH"
    assert pair.pair_match is True
    completed = verify(source, result_path)
    assert completed.returncode == 0, completed.stderr
    assert "PARITY_PAIR_MATCH" in completed.stdout


def test_pair_verifier_rejects_input_substitution(tmp_path: Path) -> None:
    source = tmp_path / "input.json"
    result_path = tmp_path / "result.json"
    source.write_text(json.dumps(payload()), encoding="utf-8")
    assert run(source, result_path).returncode == 0
    substituted = payload()
    substituted["snapshot"]["collateral_cash"] = "9001"  # type: ignore[index]
    source.write_text(json.dumps(substituted), encoding="utf-8")
    completed = verify(source, result_path)
    assert completed.returncode == 1
    assert "PAIR_INVALID" in completed.stderr


def test_pair_verifier_rejects_symlink_input_or_result(tmp_path: Path) -> None:
    source_target = tmp_path / "input-target.json"
    source_target.write_text(json.dumps(payload()), encoding="utf-8")
    source = tmp_path / "input.json"
    source.symlink_to(source_target)
    result_path = tmp_path / "result.json"
    completed = verify(source, result_path)
    assert completed.returncode == 1
    assert "symlink" in completed.stderr
