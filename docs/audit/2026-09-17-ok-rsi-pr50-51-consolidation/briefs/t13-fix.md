# t13 — Fixer: any-bag matching is not this leftover

You are a grok-orchestra fixer. One worker for the t12 finding list. Must close the Important finding. Do not reopen the empty-bag window t10 already closed. Minors stay Minor unless they fall out of the same few lines.

## Work in

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

USER_REQUESTED_GIT=no. Do not commit/push/PR. Do not touch `/Users/s1kor/dev/trading/rsibot/hummingbot`.

## Read first

- `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t12-review.md`
- `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t10-fix.md`
- `controllers/directional_trading/hl_rsi.py`
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t13-fix.md`

## Must fix (Important)

`_held_buy_inventory_observed()` is true on the first same connector/pair/BUY `positions_held` row with amount `> 0`. That releases **every** transferring executor.

Required: existence of some other BUY bag is not proof of this transfer. Keep the claimant on the capital walk (owned/outstanding `None` → Infinity, or leftover reserved) until held inventory **covers this leftover**.

Ponytail: smallest fail-closed check. A covering-sum of hold-claiming leftovers vs same-pair BUY `positions_held` is enough. Do not invent per-executor bag IDs unless the file already has them.

Keep:

- empty-bag `test_position_hold_without_held_bag_refuses_new_buy` (both files)
- TERMINATED leftover + bag of the same size = 40, no double count
- managing_fill 0.1/0.9/100

Add:

- `POSITION_HOLD` + **pre-existing or undersized** `positions_held` (bag smaller than this leftover, or a prior dust/partial bag) → new BUY refused or unmatched leftover reserved. Default freeze path: more than one hold claimant, bag only covers the first.

Do not rerun parent admission. Do not abandon owned fills. Do not invent a bag writer.

## Tests

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest --import-mode=importlib -q \
  test/test_hl_rsi_entry_control.py test/test_ok_rsi_admission_safety.py
```

## Status

DONE | DONE_WITH_CONCERNS | NEEDS_CONTEXT | BLOCKED
