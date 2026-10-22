# t13 — Fixer: any-bag matching is not this leftover

Status: **DONE_WITH_CONCERNS**

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Branch: `feat/andrzej_ok_rsi_audit_consolidation`
Git: no commit, no push, no PR. USER_REQUESTED_GIT=no.

Authority: `repository-process-only`. Did not touch `/Users/s1kor/dev/trading/rsibot/hummingbot`, `position_executor.py`, or `in_flight_order.py`.

Source of findings: `reports/t12-review.md`. Prior empty-bag close: `reports/t10-fix.md`.

## Important (closed)

`_held_buy_inventory_observed()` (any same-pair BUY `positions_held` row with amount `> 0`) is gone. Existence of some other BUY bag is not a release.

`_executor_hold_released_to_bag(executor)` now walks same-pair BUY hold claimants in `_filter_same_side` order and prefix-covers leftovers against the summed BUY bag:

- empty bag → not released (owned/outstanding `None` → Infinity, leftover stays on the walk)
- bag smaller than this leftover (dust/partial) → this claimant stays unmatched
- two hold claimants, bag only covers the first leftover → first released (owned 0), second unmatched (`None` → Infinity)
- one leftover + bag of the same size → released, bag carries inventory, total `40`, no double count

Unmatched hold claimants stay on `_buy_executors_for_capital` via owned/outstanding `None`. `can_create_executor` refuses `exposure_unavailable`. No parent admission rerun. No bag writer. Owned fills stay `managing_fill` until covering succeeds or valuation fails closed.

Leftover size is `_executor_reported_owned_base` (same remaining/receipt/filled measures as owned, without the hold short-circuit). Unmeasurable leftover fails closed (not released).

## Tests

Kept:

- `test_position_hold_without_held_bag_refuses_new_buy` in both files
- TERMINATED leftover + bag of the same size = 40
- managing_fill 0.1/0.9/100
- missing/conflict/over-authorized/non-finite → Infinity

Added `test_position_hold_undersized_or_prior_bag_refuses_new_buy` in both files:

- leftover `0.4` + bag `0.1` → refuse `exposure_unavailable`
- two `POSITION_HOLD` claimants leftover `0.4` each + bag `0.4` → first owned `0`, second unmatched, refuse `exposure_unavailable`

```bash
cd /Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest --import-mode=importlib -q \
  test/test_hl_rsi_entry_control.py test/test_ok_rsi_admission_safety.py
```

Result: **18 passed in 4.12s**. flake8 on the three owned files: clean. Not native CI/Docker.

## Files changed

- `controllers/directional_trading/hl_rsi.py`
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`

## Minors (left)

Per brief: Minors stay Minor unless they fall out of the same few lines. None did.

## Concerns

1. Covering-sum is only among hold claimants still on `executors_info`. If a released claimant is dropped from that list, a later freeze can match a leftover-sized prior bag. Per-executor bag ids were not added (file has none). Identity match is `is`, not executor id.
2. Working tree remains mixed `MM`/`AM`. This worker did not stage or commit.
3. No one-shot parent admission rerun. Unmatched leftover stays Infinity, not a finite reserved leftover plus bag (that would still admit a BUY under default `max_total_position_usd`).
