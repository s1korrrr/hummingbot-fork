#!/usr/bin/env python3
"""Fail closed unless the human-owned image publication policy is approved."""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE_IMAGE_RE = re.compile(r"^\S+@sha256:[0-9a-f]{64}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
EXPECTED_PLATFORMS = ["linux/amd64", "linux/arm64"]
EXPECTED_BASE_IMAGE = "docker.io/condaforge/miniforge3:26.5.3-0@sha256:b230e51ddd0ebc4b08bb44d7442c20f9443973e3ddd09cf7f621b04fe449ca66"  # noqa: mock
EXPECTED_EVIDENCE = [
    "exact final-image evidence set for linux/amd64",
    "exact final-image evidence set for linux/arm64",
    "reviewed package-specific redistribution obligations",
    "named operator or counsel approval",
]
MAX_APPROVAL_SECONDS = 90 * 24 * 60 * 60


class PolicyError(ValueError):
    pass


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise PolicyError(f"{field} must be an RFC3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise PolicyError(f"{field} must be an RFC3339 UTC timestamp") from error
    return parsed


def validate(policy: dict[str, Any], *, now: datetime) -> None:
    if policy.get("schema_version") != 1:
        raise PolicyError("schema_version must equal 1")
    base_image = policy.get("base_image")
    if not isinstance(base_image, str) or BASE_IMAGE_RE.fullmatch(base_image) is None:
        raise PolicyError("base_image must be bound to an exact sha256 digest")
    if base_image != EXPECTED_BASE_IMAGE:
        raise PolicyError("base_image does not match the Dockerfile release base")
    if policy.get("platforms") != EXPECTED_PLATFORMS:
        raise PolicyError(f"platforms must equal {EXPECTED_PLATFORMS}")
    evidence = policy.get("required_evidence")
    if evidence != EXPECTED_EVIDENCE:
        raise PolicyError("required_evidence does not match the release gate obligations")
    publication_allowed = policy.get("publication_allowed")
    decision = policy.get("decision")
    hold_reason = policy.get("hold_reason")
    approval_reason = policy.get("approval_reason")
    if publication_allowed is not True:
        if decision != "HOLD_FOR_EVIDENCE":
            raise PolicyError("a held policy must use decision HOLD_FOR_EVIDENCE")
        if not isinstance(hold_reason, str) or not hold_reason.strip():
            raise PolicyError("a held policy must include a nonempty hold_reason")
        if approval_reason is not None:
            raise PolicyError("a held policy must not include approval_reason")
        raise PolicyError("publication is held by the image license policy")
    if decision != "APPROVED":
        raise PolicyError("decision must equal APPROVED when publication is allowed")
    if hold_reason is not None:
        raise PolicyError("an approved policy must clear hold_reason")
    if not isinstance(approval_reason, str) or not approval_reason.strip():
        raise PolicyError("an approved policy must include a nonempty approval_reason")
    approved_by = policy.get("approved_by")
    if not isinstance(approved_by, str) or not approved_by.strip():
        raise PolicyError("approved_by must name the human reviewer")
    approved_at = _timestamp(policy.get("approved_at"), "approved_at")
    expires_at = _timestamp(policy.get("expires_at"), "expires_at")
    if approved_at > now:
        raise PolicyError("approved_at must not be in the future")
    if expires_at <= approved_at:
        raise PolicyError("expires_at must be later than approved_at")
    if expires_at <= now:
        raise PolicyError("image license policy approval has expired")
    if (expires_at - approved_at).total_seconds() > MAX_APPROVAL_SECONDS:
        raise PolicyError("image license policy approval exceeds the 90-day maximum")
    evidence_set_sha256 = policy.get("evidence_set_sha256")
    if not isinstance(evidence_set_sha256, dict) or sorted(evidence_set_sha256) != sorted(
        EXPECTED_PLATFORMS
    ):
        raise PolicyError("evidence_set_sha256 must bind every expected platform")
    for platform in EXPECTED_PLATFORMS:
        digest = evidence_set_sha256.get(platform)
        if not isinstance(digest, str) or SHA256_RE.fullmatch(digest) is None:
            raise PolicyError(f"evidence_set_sha256[{platform!r}] must be an exact sha256")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("policy", type=Path)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    try:
        policy = json.loads(args.policy.read_text())
        if not isinstance(policy, dict):
            raise PolicyError("policy must be a JSON object")
        validate(policy, now=datetime.now(timezone.utc))
        if args.github_output is not None:
            evidence_hashes = policy["evidence_set_sha256"]
            with args.github_output.open("a") as output:
                output.write(f"evidence_set_sha256_amd64={evidence_hashes['linux/amd64']}\n")
                output.write(f"evidence_set_sha256_arm64={evidence_hashes['linux/arm64']}\n")
    except (OSError, json.JSONDecodeError, PolicyError) as error:
        print(f"image license policy rejected: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
