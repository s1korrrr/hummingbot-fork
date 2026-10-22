#!/usr/bin/env python3
"""Verify generated inventory integrity and optional approved evidence binding."""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PLATFORMS = {"linux/amd64", "linux/arm64"}


class VerificationError(ValueError):
    pass


def _evidence_set(inventory: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "base_image",
        "dpkg_status_sha256",
        "notice_sha256",
        "packages",
        "platform",
        "schema_version",
        "unresolved_artifact_sources",
    )
    if any(field not in inventory for field in fields):
        raise VerificationError("inventory is missing evidence-set fields")
    return {field: inventory[field] for field in fields}


def verify(
    inventory: dict[str, Any],
    *,
    platform: str,
    require_approved_binding: bool,
    expected_amd64: str,
    expected_arm64: str,
) -> None:
    if platform not in PLATFORMS:
        raise VerificationError(f"unsupported platform {platform!r}")
    if inventory.get("platform") != platform:
        raise VerificationError("inventory platform does not match the Docker target platform")
    evidence_sha256 = hashlib.sha256(
        json.dumps(_evidence_set(inventory), separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    if inventory.get("evidence_set_sha256") != evidence_sha256:
        raise VerificationError("inventory evidence_set_sha256 is invalid")

    if not require_approved_binding:
        return
    expected = {
        "linux/amd64": expected_amd64,
        "linux/arm64": expected_arm64,
    }[platform]
    if SHA256_RE.fullmatch(expected) is None:
        raise VerificationError(f"approved evidence hash is missing for {platform}")
    if evidence_sha256 != expected:
        raise VerificationError(
            f"generated evidence set for {platform} does not match the approved hash"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--require-approved-binding", choices=("true", "false"), required=True)
    parser.add_argument("--expected-amd64", default="")
    parser.add_argument("--expected-arm64", default="")
    args = parser.parse_args()
    try:
        inventory = json.loads(args.inventory.read_text())
        if not isinstance(inventory, dict):
            raise VerificationError("inventory must be a JSON object")
        verify(
            inventory,
            platform=args.platform,
            require_approved_binding=args.require_approved_binding == "true",
            expected_amd64=args.expected_amd64,
            expected_arm64=args.expected_arm64,
        )
    except (OSError, json.JSONDecodeError, VerificationError) as error:
        print(f"generated image license inventory rejected: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
