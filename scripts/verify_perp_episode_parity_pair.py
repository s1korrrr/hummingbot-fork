#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from controllers.directional_trading.rsi_v5_core.perp_episode_accounting import (  # noqa: E402
    ParityStatus,
)
from controllers.directional_trading.rsi_v5_core.perp_episode_parity_pair import (  # noqa: E402
    ParityPairVerificationError,
    verify_parity_pair,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Re-evaluate one strict parity input, verify one persisted parity result, "
            "and require exact canonical input/result binding."
        )
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    try:
        pair = verify_parity_pair(
            input_path=args.input,
            result_path=args.result,
        )
    except (OSError, ParityPairVerificationError) as error:
        print(f"PARITY_PAIR_INVALID: {error}", file=sys.stderr)
        return 1
    summary = json.dumps(
        {
            "status": pair.status.value,
            "input_hash": pair.input_hash,
            "result_hash": pair.result_hash,
            "pair_hash": pair.pair_hash,
            "execution_authority": pair.execution_authority,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    if pair.status is ParityStatus.MATCH:
        print(f"PARITY_PAIR_MATCH {summary}")
        return 0
    print(f"PARITY_PAIR_MISMATCH {summary}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
