#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from controllers.directional_trading.rsi_v5_core.perp_episode_parity_v2 import (  # noqa: E402
    ParityV2Error,
    read_quants_artifact_sha256,
    read_verified_parity_v2_result,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a persisted v2 result and require the exact quants-lab "
            "artifact bytes it names. Use the pair verifier for readiness."
        )
    )
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--quants-artifact", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = read_verified_parity_v2_result(args.result)
        artifact_hash = read_quants_artifact_sha256(args.quants_artifact)
        if artifact_hash != result["quants_lab_artifact_sha256"]:
            raise ParityV2Error("quants artifact SHA-256 mismatch")
    except (OSError, ParityV2Error) as error:
        print(f"PARITY_V2_INVALID: {error}", file=sys.stderr)
        return 1
    summary = json.dumps(
        {
            "status": result["status"],
            "mismatches": result["mismatches"],
            "quants_lab_artifact_sha256": result["quants_lab_artifact_sha256"],
            "result_hash": result["result_hash"],
            "execution_authority": result["execution_authority"],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    if result["status"] == "MATCH":
        print(f"PARITY_V2_MATCH {summary}")
        return 0
    print(f"PARITY_V2_MISMATCH {summary}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
