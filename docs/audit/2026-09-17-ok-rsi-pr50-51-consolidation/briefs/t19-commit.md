# t19 — Commit the consolidated candidate plus review markdown

You are the PR manager in grok-orchestra.

## Repo

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

Branch: `feat/andrzej_ok_rsi_audit_consolidation`

USER_REQUESTED_GIT=yes for **commit only**. Do **not** push. Do **not** open a PR. Do not force push, skip hooks, amend, or update git config.

Do **not** touch `/Users/s1kor/dev/trading/rsibot/hummingbot` (dirty unrelated branch).
Do **not** commit anything in the rsibot root tree (dirty unrelated `feat/andrzej_reader_reopen_on_corrupt`).

## What to put on the branch

1. **All product working-tree contents vs `4a4372bc`**, not just the current index.
   - Staged snapshot is smaller (`+1283/−291`) than the reviewed tree (`+2604/−305`).
   - `test/hummingbot/core/data_type/test_in_flight_order.py` is unstaged-only. It must be in the commit.
   - Restage every intended product and test file so the commit matches `git diff 4a4372bc` (working tree).
2. **Review markdown** copied into this worktree so the branch is self-contained:

   Destination: `docs/audit/2026-09-17-ok-rsi-pr50-51-consolidation/`

   Copy from `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/`:
   - `spec.md`, `ledger.md`, `README.md`
   - `briefs/` (all)
   - `reports/` (all)
   - `reviews/` **except** `review-4a4372bc-worktree.diff` (generated, huge; `git diff 4a4372bc` is the source of truth)

3. Write `docs/audit/2026-09-17-ok-rsi-pr50-51-consolidation/HANDOFF.md` with:
   - Local candidate, not merge/deploy
   - A–G closed; split pytest 65/36/22/110/36; t18 spec PASS, quality APPROVED
   - Do not commit the old index; this commit must be the full WT
   - Do not merge/pin/restart until a PR’s CI actually runs
   - Mixed-staging warning is now historical if this commit is the full tree
   - Open PRs #50/#51 remain old heads until this branch is pushed
   - Cython `.so` copies are gitignored and must not be committed

## Git add

Stage only:

- `controllers/`
- `hummingbot/` (product `.py` only)
- `test/`
- `docs/audit/2026-09-17-ok-rsi-pr50-51-consolidation/`

Do **not** add `*.so`, `__pycache__`, `.pytest_cache`, secrets, or unrelated paths.

Use `git add` of those paths after copying docs. Then `git diff --cached --stat` and compare to `git diff --stat 4a4372bc` for product files. If cached is missing files that WT has, add them.

## Commit message (HEREDOC, conventional)

```
fix(ok-rsi): consolidate PR50/51 with audit lifecycle and capital guards

Fold post-only repair and entry-control onto 4a4372bc. Close delayed-OPEN
regression, STP vs reprice, wait-state deadline/cooldown, child permission,
capital release, residual close fees, and cancel_source JSON. Attach the
orchestra evidence under docs/audit/2026-09-17-ok-rsi-pr50-51-consolidation/.
```

After commit: `git status`, `git log -1`, `git diff --stat 4a4372bc HEAD`. Confirm HEAD moved and the worktree is clean except gitignored `.so` if present.

## Write report

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t19-commit.md`

Return: status, commit SHA, files in commit (stat), confirmation that the thin index was not used, concerns.
