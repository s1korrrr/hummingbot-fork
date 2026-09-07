from __future__ import annotations

import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pytest

from controllers.directional_trading.rsi_v5_core.perp_episode_parity_io import (
    ParityWireError,
    evaluate_parity_payload,
    parse_parity_payload,
)

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/run_perp_episode_parity.py"
D = Decimal


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


def test_strict_payload_evaluates_to_authority_false_match() -> None:
    snapshot, expectation, tolerance = parse_parity_payload(payload())
    result = evaluate_parity_payload(payload())
    assert snapshot.positions[0].entry_price == D("10000")
    assert expectation.liquidatable_nav == D("8982.0070")
    assert tolerance == D("0.000000000000000001")
    assert result["status"] == "MATCH"
    assert result["mismatches"] == []
    assert result["execution_authority"] is False
    assert not any(result["authority"].values())
    assert len(result["result_hash"]) == 64


def test_wire_rejects_float_decimal_unknown_key_and_authority_input() -> None:
    numeric = payload()
    numeric["snapshot"]["collateral_cash"] = 9000.0  # type: ignore[index]
    with pytest.raises(ParityWireError, match="decimal text"):
        parse_parity_payload(numeric)

    unknown = payload()
    unknown["snapshot"]["unknown"] = True  # type: ignore[index]
    with pytest.raises(ParityWireError, match="snapshot keys"):
        parse_parity_payload(unknown)

    authority = payload()
    authority["authority"] = {"execution_authority": False}
    with pytest.raises(ParityWireError, match="run keys"):
        parse_parity_payload(authority)


def test_cli_publishes_idempotent_canonical_result(tmp_path: Path) -> None:
    source = tmp_path / "input.json"
    output = tmp_path / "result.json"
    source.write_text(json.dumps(payload()), encoding="utf-8")
    command = [
        sys.executable,
        str(SCRIPT),
        "--input",
        str(source),
        "--output",
        str(output),
    ]
    first = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
    assert first.returncode == 0, first.stderr
    first_bytes = output.read_bytes()
    second = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
    assert second.returncode == 0, second.stderr
    assert output.read_bytes() == first_bytes
    assert first_bytes.endswith(b"\n")
    assert json.loads(first_bytes)["status"] == "MATCH"


def test_cli_rejects_symlink_output(tmp_path: Path) -> None:
    source = tmp_path / "input.json"
    source.write_text(json.dumps(payload()), encoding="utf-8")
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    output = tmp_path / "result.json"
    output.symlink_to(target)
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--input",
            str(source),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode != 0
    assert "symlink" in completed.stderr
