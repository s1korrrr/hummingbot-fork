# t15 — Re-review after t13 covering-sum

You are the reviewer in grok-orchestra. Read-only. Two verdicts: spec and quality.

## Inputs

- Spec: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/spec.md`
- t12 review: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t12-review.md`
- t13 fix: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t13-fix.md`
- Audit: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/audit.md` (re-audit after t13; gate PASS)
- Diff: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reviews/review-4a4372bc-worktree.diff` (+2485/−305)
- Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
- Head: uncommitted working tree

Read the diff once. Do not mutate git. Do not re-run the suite.

## Re-judge

t12 Important: any positive same-pair bag released every transferring executor. t13 claims a covering-sum prefix walk. Confirm or reopen.

Keep empty-bag closed (t9/t10).

t13 residual: covering-sum only among claimants still on `executors_info`. A dropped released claimant no longer consumes the bag. Decide severity with evidence. Do not ignore because the plan chose covering-sum.

t9 Minors 1–11: re-list only if now Important or Critical.

## Global constraints

- Authority: repository-process-only. No merge, deploy, restart, or live orders.
- Do not change approved trading policy.
- Release only reconciled commitment. Fail closed for new acquisition when transfer is unproven.
- Keep late TradeUpdates processable. OKX 32 beats bound reprice. Typed `_live_book`. Bound + missing cancel_source may still confirm local reprice.

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t15-review.md`

Return spec PASS/FAIL, quality APPROVED or NOT APPROVED, findings, production-ready yes/no.
