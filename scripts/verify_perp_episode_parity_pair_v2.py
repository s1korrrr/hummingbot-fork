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
    verify_parity_v2_pair,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Re-evaluate exact Hummingbot v2 input and quants-lab artifact bytes "
            "against one persisted result. This is the v2 parity-readiness check."
        )
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--quants-artifact", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    try:
        pair = verify_parity_v2_pair(
            input_path=args.input,
            quants_artifact_path=args.quants_artifact,
            result_path=args.result,
        )
    except (OSError, ParityV2Error) as error:
        print(f"PARITY_V2_PAIR_INVALID: {error}", file=sys.stderr)
        return 1
    summary = json.dumps(pair, sort_keys=True, separators=(",", ":"))
    if pair["status"] == "MATCH":
        print(f"PARITY_V2_PAIR_MATCH {summary}")
        return 0
    print(f"PARITY_V2_PAIR_MISMATCH {summary}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
