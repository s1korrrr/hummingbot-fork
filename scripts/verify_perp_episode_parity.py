#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from controllers.directional_trading.rsi_v5_core.perp_episode_accounting import ParityStatus  # noqa: E402
from controllers.directional_trading.rsi_v5_core.perp_episode_parity_result import (  # noqa: E402
    ParityResultVerificationError,
    read_verified_parity_result,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a persisted Hummingbot/quants-lab linear-perpetual parity "
            "artifact and return success only for an exact MATCH."
        )
    )
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = read_verified_parity_result(args.result)
    except (OSError, ParityResultVerificationError) as error:
        print(f"PARITY_INVALID: {error}", file=sys.stderr)
        return 1
    summary = json.dumps(
        {
            "status": result.status.value,
            "mismatches": list(result.mismatches),
            "result_hash": result.result_hash,
            "execution_authority": result.execution_authority,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    if result.status is ParityStatus.MATCH:
        print(f"PARITY_MATCH {summary}")
        return 0
    print(f"PARITY_MISMATCH {summary}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
