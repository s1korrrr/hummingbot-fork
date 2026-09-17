# t10 — Fixer: Important capital drop on POSITION_HOLD

You are a grok-orchestra fixer. One worker for the full t9 finding list. Must close the Important finding. Minors only if they are the same owned files and a few lines; do not start a second architecture.

## Work in

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

USER_REQUESTED_GIT=no. Do not commit/push/PR. Do not touch `/Users/s1kor/dev/trading/rsibot/hummingbot`.

## Read first

- `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t9-review.md` (source of findings)
- `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t6-capital.md`
- `controllers/directional_trading/hl_rsi.py`
- `test/test_ok_rsi_admission_safety.py`
- `test/test_hl_rsi_entry_control.py`

## Write report to

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t10-fix.md` (append if the file exists)

## Must fix (Important)

`early_stop(keep_position=True)` sets `CloseType.POSITION_HOLD` immediately. `get_custom_info` then publishes `accounting_owner=held_position`. Capital helpers drop that executor (owned 0, outstanding 0, omitted from the buy-capital walk) **before** `positions_held` has the bag. New BUY can be admitted. Bag freeze does not have kill/opposing's other gates.

Spec E: release only reconciled commitment.

Fix in the capital owner (`hl_rsi.py`), not by inventing a second bag writer:

- Keep executor owned in the sum until matching `positions_held` inventory is observed, **or** treat `accounting_owner=held_position` with no matching bag as transfer-in-progress (Infinity / refuse new buys), not as free capital.
- Do not treat the flag alone as release.
- Keep existing TERMINATED leftover tests that pair the flag **with** a bag of the same size (no double count).
- Add: `POSITION_HOLD` / `accounting_owner=held_position` + empty `positions_held` → leftover stays reserved **or** new BUY refused (`exposure_unavailable` / Infinity). Prefer fail-closed if the bag is missing rather than inventing a bag.

Do not rerun one-shot parent admission. Do not abandon owned fills.

## Full finding list (from t9)

Important: the hold-flag window above.

Minors (same files / tiny only; otherwise leave and name in the report):

1. Absent `data_health` still allows a child (executor file; skip unless a one-liner that does not break the 30 shared tests).
2. Missing `controller.status` treated as allowed (leave; production always has status).
3. First replacement after parent skips cooldown (leave; audit probe was 1s after a prior child).
4. `_intent_age_expired` swallows clock errors (leave unless trivial fail-closed).
5. Fee estimate mutates `_close_quantity` (position_executor; skip unless trivial).
6. OKX WS delayed OPEN never hits the guard (leave; REST path is the audit path).
7. `__eq__` omits `cancel_source` (optional one-liner in `in_flight_order.py` if you already touch it; otherwise leave).
8. Refused OPEN copying exchange id returns `updated=True` (leave unless trivial).
9. Concatenated pytest asyncio isolation (do not "fix" by weakening tests).
10. Cython `.so` copies (not product).
11. Mixed `MM` staging (do not commit).

## Tests

Add the empty-bag POSITION_HOLD case. Re-run:

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest --import-mode=importlib -q \
  test/test_hl_rsi_entry_control.py test/test_ok_rsi_admission_safety.py
```

Keep the managing_fill 0.1/0.9/100 and missing-receipt Infinity tests passing.

## Status

DONE | DONE_WITH_CONCERNS | NEEDS_CONTEXT | BLOCKED
