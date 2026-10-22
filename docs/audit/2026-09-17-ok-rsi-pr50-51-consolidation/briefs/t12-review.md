# t12 — Re-review after t10 hold-flag fix

You are the reviewer in grok-orchestra. Read-only. Two verdicts required: spec compliance and quality.

## Inputs

- Spec: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/spec.md`
- Prior review: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t9-review.md`
- Fixer: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t10-fix.md`
- Audit: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/audit.md` (re-audit after t10; gate PASS)
- Diff file: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reviews/review-4a4372bc-worktree.diff` (refreshed after t10; 14 files +2377/−305)
- Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
- Head: uncommitted working tree

Read the diff file once. Do not mutate git. Treat implementer reports as claims. Do not re-run the suite.

## What must be re-judged

t9 Important: `POSITION_HOLD` / `accounting_owner=held_position` dropped executor capital before `positions_held` had the bag. t10 claims empty bag now values Infinity and refuses new BUY. Confirm or reopen.

t10 residual: matching bag is any positive same-pair BUY hold, so a prior bag can look like a completed transfer. Decide severity (Important vs Minor) with evidence. Do not ignore it because the plan chose this matching rule.

t9 Minors 1–11 were left on purpose. Re-list only if still Important or newly Critical.

## Global constraints (verbatim)

- Authority: repository-process-only. No merge, deploy, restart, or live orders.
- Do not change approved trading policy.
- Release only reconciled commitment. Fail closed for new acquisition when transfer is unproven.
- Keep late TradeUpdates processable. OKX 32 beats bound reprice. Typed `_live_book`. Bound + missing cancel_source may still confirm local reprice.

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t12-review.md`

Return:

- Spec: PASS or FAIL, with missing/extra items
- Quality: APPROVED or NOT APPROVED
- Findings: Critical / Important / Minor, each with evidence
- Production-ready for this scope: yes or no
