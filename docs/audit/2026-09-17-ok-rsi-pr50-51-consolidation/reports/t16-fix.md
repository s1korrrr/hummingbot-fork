# t16 — Fixer: covering-sum vs this leftover after store/restart

Status: **DONE_WITH_CONCERNS**

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Git: no commit, no push, no PR. USER_REQUESTED_GIT=no.

Authority: `repository-process-only`. Did not touch `/Users/s1kor/dev/trading/rsibot/hummingbot`.

Source: `reports/t15-review.md` Important 1. Did not reopen empty-bag, undersized-bag, or on-list two-claimant cases.

## Important (closed)

`positions_held` rows have no executor id (`connector_name`, `trading_pair`, `side`, `amount` / buy-sell totals, fees). No bag ledger was added.

`_executor_hold_released_to_bag(executor)` no longer prefix-covers in `executors_info` / `_filter_same_side` report order.

- Live and `SHUTTING_DOWN` hold claimants never consume the bag. They stay unmatched (`owned`/`outstanding` `None` → Infinity, leftover stays on `_buy_executors_for_capital`, `can_create_executor` refuses `exposure_unavailable`).
- Only `_executor_is_done` / TERMINATED hold claimants may be treated as released.
- Those done claimants are ordered by `(close_timestamp or timestamp, id)`, oldest first. Raw report order is not used. Duplicate or missing age keys fail closed (no release).
- Covering-sum among that oldest-first done list: bag must cover this leftover after older done leftovers. Unmeasurable leftover fails closed.

Runtime active-first / newest-`close_timestamp` stored order therefore cannot hand a prior bag to a later freeze that is still live or shutting down.

## Kept

- empty bag → refuse (`test_position_hold_without_held_bag_refuses_new_buy` in both files)
- leftover `0.4` + bag `0.1` → refuse
- two on-list done/hold leftovers `0.4` + bag `0.4` → older released (`owned` 0), newer unmatched, refuse BUY. Listed newest-first to prove report order is not the allocator.
- TERMINATED leftover + bag of the same size = 40, no double count
- managing_fill 0.1/0.9/100; missing/conflict/over-authorized/non-finite → Infinity

## Tests added

Both capital files:

1. New active / `SHUTTING_DOWN` `POSITION_HOLD` listed **before** a stored prior done hold; bag covers one leftover. New claimant unmatched; prior may consume the bag; BUY refused `exposure_unavailable`.
2. New live / `SHUTTING_DOWN` `POSITION_HOLD`, prior claimant **absent** from `executors_info`, leftover-sized pre-existing bag. New claimant unmatched; BUY refused.

```bash
cd /Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest --import-mode=importlib -q \
  test/test_hl_rsi_entry_control.py test/test_ok_rsi_admission_safety.py
```

Result: **22 passed in 4.34s**. flake8 on the three owned files: clean. Not native CI/Docker.

## Files changed

- `controllers/directional_trading/hl_rsi.py`
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`

## Concerns

1. A later freeze that is already TERMINATED, with the prior done claimant dropped from `executors_info`, still matches a leftover-sized bag. That snapshot is the KEEP “TERMINATED leftover + bag = 40” case; bag rows have no executor id, so this worker did not invent attribution. The new tests close the live / `SHUTTING_DOWN` / listed-before-prior window, not that done-and-dropped window.
2. Identity match on the covering walk is still `is`. Age order uses executor `id` only as a tiebreak.
3. Working tree remains mixed `MM`/`AM`. This worker did not stage or commit.
