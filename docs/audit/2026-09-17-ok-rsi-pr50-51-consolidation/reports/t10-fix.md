# t10 — Fixer: Important capital drop on POSITION_HOLD

Status: **DONE_WITH_CONCERNS**

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Branch: `feat/andrzej_ok_rsi_audit_consolidation`
Git: no commit, no push, no PR. USER_REQUESTED_GIT=no.

Authority: `repository-process-only`. Did not touch `/Users/s1kor/dev/trading/rsibot/hummingbot`, `position_executor.py`, or `in_flight_order.py`.

Source of findings: `reports/t9-review.md`. Capital context: `reports/t6-capital.md`.

## Important (closed)

`early_stop(keep_position=True)` still writes `CloseType.POSITION_HOLD` and `accounting_owner=held_position` immediately. That flag is no longer treated as a capital release.

In `hl_rsi.py`:

- `_executor_claims_held_position`: receipt `accounting_owner=held_position` or `close_type=POSITION_HOLD`.
- `_held_buy_inventory_observed`: same connector/pair/BUY `positions_held` row with finite amount `> 0`.
- `_executor_hold_released_to_bag`: both of the above.

Until a bag is observed:

- leftover stays on the buy-capital walk
- owned and outstanding are `None` → valuation `Infinity`
- `can_create_executor` refuses with `exposure_unavailable`

Once a matching bag exists, the executor drops (owned 0, outstanding 0, omitted from the walk). The bag carries the inventory. Existing TERMINATED leftover tests that pair the flag with a bag of the same size still value `40` with no double count.

No second bag writer. Missing bag fails closed rather than inventing `positions_held`.

## Tests

Empty-bag POSITION_HOLD cases:

- `test_position_hold_without_held_bag_refuses_new_buy` in `test_hl_rsi_entry_control.py` (SHUTTING_DOWN-shaped mock: active, not done, `accounting_owner=held_position`, empty `positions_held`).
- Same name in `test_ok_rsi_admission_safety.py` (live `PositionExecutor.early_stop` window: `SHUTTING_DOWN` + `CloseType.POSITION_HOLD`, empty bag).

Kept: managing_fill 0.1/0.9/100, TERMINATED leftover + bag = 40, missing/conflict/over-authorized/non-finite receipts → Infinity.

```bash
cd /Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest --import-mode=importlib -q \
  test/test_hl_rsi_entry_control.py test/test_ok_rsi_admission_safety.py
```

Result: **16 passed in 4.53s**. flake8 on the three owned files: clean. Not native CI/Docker.

## Files changed

- `controllers/directional_trading/hl_rsi.py`
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`

## Minors (left)

Per brief: only tiny same-file fixes. None of these were one-liners in the owned capital files without a second architecture or a 30-test bind_controller break.

1. Absent `data_health` still allows a child (executor file; skip).
2. Missing `controller.status` treated as allowed (leave).
3. First replacement after parent skips cooldown (leave).
4. `_intent_age_expired` swallows clock errors (leave).
5. Fee estimate mutates `_close_quantity` (position_executor; skip).
6. OKX WS delayed OPEN never hits the guard (leave).
7. `__eq__` omits `cancel_source` (did not touch `in_flight_order.py`).
8. Refused OPEN copying exchange id returns `updated=True` (leave).
9. Concatenated pytest asyncio isolation (not weakened).
10. Cython `.so` copies (not product).
11. Mixed `MM` staging (not committed).

## Concerns

1. "Matching bag" is any positive same-pair BUY hold, not leftover-size identity. If a prior bag already exists, a new `POSITION_HOLD` can still drop the transferring executor before that leftover is written. The named empty-bag window is closed; a second concurrent hold onto a non-empty ledger is not.
2. Working tree remains mixed `MM`. This worker did not stage or commit.
3. No one-shot parent admission rerun. Owned fills stay on the executor until a bag is observed or valuation fails closed.
