import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERIFIER = ROOT / "scripts/verify_generated_image_license_inventory.py"


def _inventory() -> dict:
    evidence_set = {
        "base_image": "example.invalid/base@sha256:" + "a" * 64,
        "dpkg_status_sha256": "b" * 64,
        "notice_sha256": {"/notice": "e" * 64},
        "packages": [
            {
                "artifact_sha256": "c" * 64,
                "ecosystem": "conda",
                "name": "example",
                "version": "1.0",
            }
        ],
        "platform": "linux/arm64",
        "schema_version": 1,
        "unresolved_artifact_sources": [],
    }
    return {
        **evidence_set,
        "evidence_set_sha256": hashlib.sha256(
            json.dumps(evidence_set, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest(),
        "legal_clearance": False,
        "source_commit": "d" * 40,
    }


def _run(path: Path, *, required: bool, expected_arm64: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(VERIFIER),
            str(path),
            "--platform",
            "linux/arm64",
            "--require-approved-binding",
            str(required).lower(),
            "--expected-amd64",
            "e" * 64,
            "--expected-arm64",
            expected_arm64,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_unapproved_build_still_requires_an_internally_valid_evidence_hash(tmp_path: Path):
    inventory = _inventory()
    path = tmp_path / "inventory.json"
    path.write_text(json.dumps(inventory))

    result = _run(path, required=False)

    assert result.returncode == 0, result.stderr

    inventory["packages"][0]["version"] = "2.0"
    path.write_text(json.dumps(inventory))
    result = _run(path, required=False)
    assert result.returncode != 0
    assert "evidence_set_sha256 is invalid" in result.stderr


def test_publication_build_requires_the_exact_platform_approval_hash(tmp_path: Path):
    inventory = _inventory()
    path = tmp_path / "inventory.json"
    path.write_text(json.dumps(inventory))

    result = _run(
        path,
        required=True,
        expected_arm64=inventory["evidence_set_sha256"],
    )
    assert result.returncode == 0, result.stderr

    result = _run(path, required=True, expected_arm64="f" * 64)
    assert result.returncode != 0
    assert "does not match the approved hash" in result.stderr

    result = _run(path, required=True)
    assert result.returncode != 0
    assert "approved evidence hash is missing" in result.stderr
