import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "licenses/image-license-policy.json"
VALIDATOR = ROOT / "scripts/validate_image_license_policy.py"


def _run(policy: Path, *extra_args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(VALIDATOR), str(policy), *extra_args],
        check=False,
        capture_output=True,
        text=True,
    )


def test_committed_policy_holds_publication_until_external_review():
    policy = json.loads(POLICY.read_text())

    assert policy["publication_allowed"] is False
    assert policy["decision"] == "HOLD_FOR_EVIDENCE"
    assert policy["approved_by"] is None
    assert policy["approved_at"] is None
    assert policy["expires_at"] is None
    assert policy["approval_reason"] is None
    assert policy["hold_reason"]
    result = _run(POLICY)
    assert result.returncode != 0
    assert "publication is held" in result.stderr


def test_validator_accepts_only_a_complete_unexpired_approval(tmp_path: Path):
    policy = json.loads(POLICY.read_text())
    policy.update(
        {
            "publication_allowed": True,
            "decision": "APPROVED",
            "approved_by": "named-reviewer",
            "approval_reason": "Reviewed package obligations and accepted the bound evidence sets.",
            "approved_at": "2026-08-24T00:00:00Z",
            "expires_at": "2026-09-24T00:00:00Z",
            "evidence_set_sha256": {
                "linux/amd64": "a" * 64,
                "linux/arm64": "b" * 64,
            },
            "hold_reason": None,
        }
    )
    approved = tmp_path / "approved.json"
    approved.write_text(json.dumps(policy))

    result = _run(approved)

    assert result.returncode == 0, result.stderr

    github_output = tmp_path / "github-output"
    result = _run(approved, "--github-output", str(github_output))
    assert result.returncode == 0, result.stderr
    assert github_output.read_text().splitlines() == [
        "evidence_set_sha256_amd64=" + "a" * 64,
        "evidence_set_sha256_arm64=" + "b" * 64,
    ]


def test_validator_rejects_expired_or_incomplete_approval(tmp_path: Path):
    policy = json.loads(POLICY.read_text())
    policy.update(
        {
            "publication_allowed": True,
            "decision": "APPROVED",
            "approved_by": "named-reviewer",
            "approval_reason": "Reviewed package obligations and accepted the bound evidence sets.",
            "approved_at": "2020-01-01T00:00:00Z",
            "expires_at": "2020-02-01T00:00:00Z",
            "evidence_set_sha256": {
                "linux/amd64": "a" * 64,
                "linux/arm64": "b" * 64,
            },
            "hold_reason": None,
        }
    )
    expired = tmp_path / "expired.json"
    expired.write_text(json.dumps(policy))
    result = _run(expired)
    assert result.returncode != 0
    assert "expired" in result.stderr

    policy["approved_at"] = "2026-08-24T00:00:00Z"
    policy["expires_at"] = "2026-09-24T00:00:00Z"
    policy["approved_by"] = None
    incomplete = tmp_path / "incomplete.json"
    incomplete.write_text(json.dumps(policy))
    result = _run(incomplete)
    assert result.returncode != 0
    assert "approved_by" in result.stderr

    policy["approved_by"] = "named-reviewer"
    policy["evidence_set_sha256"]["linux/arm64"] = None
    unbound = tmp_path / "unbound.json"
    unbound.write_text(json.dumps(policy))
    result = _run(unbound)
    assert result.returncode != 0
    assert "evidence_set_sha256['linux/arm64']" in result.stderr


def test_validator_rejects_drifted_gate_contract_and_long_lived_approval(tmp_path: Path):
    policy = json.loads(POLICY.read_text())
    policy.update(
        {
            "publication_allowed": True,
            "decision": "APPROVED",
            "approved_by": "named-reviewer",
            "approval_reason": "Reviewed package obligations and accepted the bound evidence sets.",
            "approved_at": "2026-08-24T00:00:00Z",
            "expires_at": "2026-09-24T00:00:00Z",
            "evidence_set_sha256": {
                "linux/amd64": "a" * 64,
                "linux/arm64": "b" * 64,
            },
            "hold_reason": None,
        }
    )

    mutations = (
        ("base_image", "example.invalid/base@sha256:" + "c" * 64, "release base"),
        ("required_evidence", ["something else"], "gate obligations"),
        ("approval_reason", "", "nonempty approval_reason"),
        ("hold_reason", "still incomplete", "clear hold_reason"),
        ("expires_at", "2027-08-24T00:00:00Z", "90-day maximum"),
    )
    for field, value, expected_error in mutations:
        candidate = {**policy, field: value}
        path = tmp_path / f"{field}.json"
        path.write_text(json.dumps(candidate))
        result = _run(path)
        assert result.returncode != 0
        assert expected_error in result.stderr
