# t0 — Create isolated consolidation worktree

## Role

PR manager. Git hygiene only. No product code.

## Repo

Hummingbot nested repo: `/Users/s1kor/dev/trading/rsibot/hummingbot`

## Allowed actions

- create-branch
- status

## Forbidden

- commit, push, PR, force push, skip hooks, git config
- checkout/switch of `/Users/s1kor/dev/trading/rsibot/hummingbot`
- any edit under that dirty checkout
- merge of PR #50/#51 in this task (next wave)

## Required result

Create a new worktree:

- path: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
- branch: `feat/andrzej_ok_rsi_audit_consolidation`
- start point: `4a4372bcde4990355421a42df3b8ce6107299abb`

If the path already exists, inspect it and reuse only if it is exactly that branch/SHA or empty enough to reset *that worktree only* to the start point. Never reset other worktrees.

Verify after:

```bash
git -C <worktree> rev-parse --abbrev-ref HEAD
git -C <worktree> rev-parse HEAD
git -C <worktree> status --short
git -C /Users/s1kor/dev/trading/rsibot/hummingbot status -sb
```

The original hummingbot checkout must remain `feat/andrzej_telegram_ssl_log_noise` with its existing dirty files.

Write report to:
`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t0-worktree.md`
