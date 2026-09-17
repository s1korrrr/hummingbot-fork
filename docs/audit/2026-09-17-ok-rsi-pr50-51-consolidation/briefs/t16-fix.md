# t16 — Fixer: covering-sum vs this leftover after store/restart

You are a grok-orchestra fixer. Close t15 Important. Do not reopen empty-bag, undersized-bag, or on-list two-claimant cases already closed.

## Work in

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

USER_REQUESTED_GIT=no. Do not commit/push/PR. Do not touch `/Users/s1kor/dev/trading/rsibot/hummingbot`.

## Read first

- `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t15-review.md`
- `controllers/directional_trading/hl_rsi.py`
- How `positions_held` rows are shaped (any executor id already there)
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t16-fix.md`

## Must fix (Important)

Prefix covering-sum over `executors_info` order is not proof this leftover transferred.

Runtime report is **active first, then stored** (newest `close_timestamp` first). A later freeze (`bag_max_concurrent_holds` default 2) or a restart with a live bag can sit **before** the prior hold in that list and consume the prior bag. If the prior freeze has already fallen out of stored executors, it no longer consumes the bag at all.

Fix (ponytail: smallest fail-closed):

- Do **not** let a still-live / SHUTTING_DOWN hold claimant consume `positions_held`. Only a done/TERMINATED hold claimant may be treated as released into the bag, and only up to leftover that is actually covered after older done claimants.
- Or attribute bag rows by executor id if that field already exists. Do not invent a new bag ledger unless the file already stores executor identity on the row.
- Prefix over raw `executors_info` order is forbidden.

Keep:

- empty-bag refuse
- leftover 0.4 + bag 0.1 refuse
- two on-list done/hold leftovers 0.4 + bag 0.4 → first released, second unmatched, refuse BUY
- TERMINATED leftover + bag of the same size = 40, no double count

Add tests that still refuse a new BUY:

1. New active/`SHUTTING_DOWN` POSITION_HOLD listed **before** a stored prior hold, bag covers only one leftover.
2. New POSITION_HOLD, prior claimant **absent** from `executors_info`, leftover-sized pre-existing bag.

## Tests

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest --import-mode=importlib -q \
  test/test_hl_rsi_entry_control.py test/test_ok_rsi_admission_safety.py
```

## Status

DONE | DONE_WITH_CONCERNS | NEEDS_CONTEXT | BLOCKED
