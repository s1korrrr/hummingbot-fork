# t6 — Capital release (E)

Do not start until t4 has checked PR51 `hl_rsi.py` into the consolidation worktree. If that file is still base-only, STOP with NEEDS_CONTEXT.

## Work in

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

USER_REQUESTED_GIT=no. Do not commit/push/PR. Do not touch `/Users/s1kor/dev/trading/rsibot/hummingbot`.

## Read

- spec.md, reports/t2-pr51-context.md (section E), reports/t4-merge-bcd.md
- `controllers/directional_trading/hl_rsi.py`
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t6-capital.md`

## Owned files

- `controllers/directional_trading/hl_rsi.py`
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`

Do not edit `down_only_entry_execution.py` or `position_executor.py`.

## E

Separate owned inventory, unresolved acquisition commitments, and report labels.

- `managing_fill` is not proof that no acquisition remains outstanding. If a working remainder or unreconciled order still exists, keep that commitment reserved.
- Include TERMINATED/non-active executors that still report leftover owned base and have not transferred to `positions_held` (`accounting_owner != held_position`).
- Missing, non-finite, conflicting remaining vs receipt vs filled−closed, or owned > authorized → unavailable (Infinity / fail closed for new acquisition), not 0.
- Do not treat missing filled as owned 0 on a still-responsible executor.
- Release only reconciled commitment. Do not invent free capital.

Composition probe to encode as a test (methods, not a merged runtime fantasy):

- Authorized 1.0 at 100, acquired 0.1, 0.9 unresolved, `managing_fill` label, no terminal confirmation → valuation must not collapse to owned-only 10.

Also: TERMINATED + leftover remaining vs TERMINATED + `positions_held` transfer. Missing/conflicting receipts.

Keep existing SHUTTING_DOWN reserve behavior unless it contradicts reconciled-only release.

## Status

DONE | DONE_WITH_CONCERNS | NEEDS_CONTEXT | BLOCKED
