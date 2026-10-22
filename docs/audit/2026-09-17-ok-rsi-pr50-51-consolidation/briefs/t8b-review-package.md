# t8b — Working-tree review package

PR manager / git hygiene. No product edits. USER_REQUESTED_GIT=no.

`review_package.py` only diffs commits. This worktree HEAD is still `4a4372bc`; all product work is uncommitted. Write a working-tree package instead.

## Repo

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reviews/review-4a4372bc-worktree.diff`

Include:

```bash
git status --short
git diff --stat 4a4372bc
git diff -U10 4a4372bc
```

`git diff 4a4372bc` must reflect the current working tree vs that base (final file contents). If staged/unstaged split hides files, use a form that shows the full WT vs `4a4372bc`.

Also write a one-line report:
`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t8b-review-package.md`

Do not commit. Do not touch the dirty hummingbot checkout.
