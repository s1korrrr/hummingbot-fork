# t3 — Overlap and merge conflict map

Read-only. No edits, no git writes.

## Sources

- Base: `4a4372bcde4990355421a42df3b8ce6107299abb` in `/Users/s1kor/dev/trading/rsibot/hummingbot` (read-only; do not checkout)
- PR50: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-down-only-postonly-repair`
- PR51: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-entry-control-consistency`
- Spec: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/spec.md`

## Task

Produce a merge map for the two overlap files only:

- `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py`
- `test/test_ok_rsi_down_only_entry_execution.py`

You may run a *temporary* three-way merge in `/tmp` that you delete, or `git merge-file` / `git merge-tree`. Do not create branches in the hummingbot checkout. Do not write into either PR worktree.

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t3-overlap.md`

Include:

1. Exact conflict region count and line ranges (function names)
2. `_live_book` signature difference between PRs and which contract t4 should keep (typed execution evidence, not a display string)
3. How to keep #50 cancel/repair/deadline machinery and #51 permission/data gates in one class
4. Test-file overlap: which tests to keep from each side
5. Ordered resolution recipe for t4 (still no product edits)
