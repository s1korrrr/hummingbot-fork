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
    run_parity_v2_files,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare complete Hummingbot episode economics with the exact bytes "
            "of one quants-lab v2 parity artifact. This read-only command grants "
            "no paper, canary, live, Risk, capital, order, or trading authority."
        )
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--quants-artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = run_parity_v2_files(
            input_path=args.input,
            quants_artifact_path=args.quants_artifact,
            output_path=args.output,
        )
    except (OSError, ParityV2Error) as error:
        print(f"perp episode parity v2 failed: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "status": result["status"],
                "mismatches": result["mismatches"],
                "quants_lab_artifact_sha256": result[
                    "quants_lab_artifact_sha256"
                ],
                "result_hash": result["result_hash"],
                "execution_authority": result["execution_authority"],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
