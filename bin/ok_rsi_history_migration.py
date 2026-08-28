#!/usr/bin/env python
"""Checksum-bound CLI for the one-shot OK RSI history migration."""

import argparse
import json
from dataclasses import asdict
from pathlib import Path

import path_util  # noqa: F401

from hummingbot.strategy_v2.migrations.ok_rsi_history import (
    apply_ok_rsi_migration,
    assert_receipt_matches_plan,
    load_migration_receipt,
    migration_plan_to_dict,
    plan_ok_rsi_migration,
    verify_ok_rsi_migration,
    write_applied_receipt,
    write_dry_run_receipt,
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--wallet-snapshot", type=Path)
    parser.add_argument("--receipt", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="plan only (default)")
    mode.add_argument("--apply", action="store_true", help="apply a matching dry-run receipt")
    mode.add_argument("--verify", action="store_true", help="verify an applied receipt without mutation")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.verify:
        report = verify_ok_rsi_migration(args.target, args.receipt)
        print(json.dumps({"status": "verified", **asdict(report)}, sort_keys=True, indent=2))
        return 0
    if args.source is None or args.wallet_snapshot is None:
        raise SystemExit("--source and --wallet-snapshot are required for dry-run/apply")
    plan = plan_ok_rsi_migration(args.source, args.target, args.wallet_snapshot)
    if args.apply:
        receipt = load_migration_receipt(args.receipt)
        if receipt.get("status") != "dry-run-approved":
            raise SystemExit("--apply requires a dry-run-approved receipt")
        assert_receipt_matches_plan(receipt, plan)
        result = apply_ok_rsi_migration(plan)
        write_applied_receipt(plan, result, args.receipt)
        print(json.dumps({"status": "applied", "result": asdict(result)}, sort_keys=True, indent=2))
        return 0
    write_dry_run_receipt(plan, args.receipt)
    print(json.dumps({"status": "dry-run-approved", "plan": migration_plan_to_dict(plan)}, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
