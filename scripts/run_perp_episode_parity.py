#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from controllers.directional_trading.rsi_v5_core.perp_episode_parity_io import (  # noqa: E402
    ParityWireError,
    run_parity_files,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare one Hummingbot linear-perpetual account snapshot with one "
            "quants-lab liquidatable-NAV expectation and publish a deterministic "
            "read-only parity artifact."
        )
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = run_parity_files(
            input_path=args.input,
            output_path=args.output,
        )
    except (OSError, ParityWireError) as error:
        print(f"perp episode parity failed: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "status": result["status"],
                "mismatches": result["mismatches"],
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
