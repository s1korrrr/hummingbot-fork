# t18 — Re-review after t16 live-hold vs bag

You are the reviewer in grok-orchestra. Read-only. Two verdicts: spec and quality.

## Inputs

- Spec: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/spec.md`
- t15 review: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t15-review.md`
- t16 fix: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t16-fix.md`
- Audit: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/audit.md` (re-audit after t16; gate PASS)
- Diff: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reviews/review-4a4372bc-worktree.diff` (+2604/−305)
- Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
- Head: uncommitted working tree

Read the diff once. Do not mutate git. Do not re-run the suite.

## Re-judge

t15 Important: covering-sum over report order. t16 claims live/SHUTTING_DOWN never consume the bag; only done holds, oldest close first. Confirm or reopen.

Keep empty-bag and undersized/two-claimant closed.

t16 residual: later already-TERMINATED freeze with prior claimant dropped still matches leftover-sized bag. Bag rows have no executor id. That snapshot is also the KEEP “TERMINATED leftover + bag = 40” case. Decide severity (Important vs Minor) with evidence. Do not ignore it because the plan chose not to invent bag ids. Also do not demand a third ledger if the product freeze path is live/SHUTTING_DOWN, which is now fail-closed.

t9 Minors 1–11: re-list only if now Important or Critical.

## Global constraints

- Authority: repository-process-only. No merge, deploy, restart, or live orders.
- Do not change approved trading policy.
- Release only reconciled commitment. Fail closed for new acquisition when transfer is unproven.
- Keep late TradeUpdates processable. OKX 32 beats bound reprice. Typed `_live_book`. Bound + missing cancel_source may still confirm local reprice.

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t18-review.md`

Return spec PASS/FAIL, quality APPROVED or NOT APPROVED, findings, production-ready yes/no.
