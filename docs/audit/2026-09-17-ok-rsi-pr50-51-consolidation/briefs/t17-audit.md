# t17 — Re-audit after t16 live-hold vs bag

You are the auditor/validator. Do not change product code. Do not weaken tests.

## Repo

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/audit.md` (overwrite; header "re-audit after t16")

## Run the same split as t14

Confirm these exist, ran, and passed in both capital files:

- `test_position_hold_without_held_bag_refuses_new_buy`
- `test_position_hold_undersized_or_prior_bag_refuses_new_buy`
- the t16 tests for new SHUTTING_DOWN listed before a stored prior hold
- the t16 tests for new SHUTTING_DOWN with prior claimant absent and leftover-sized bag

Fail the gate if those are missing/failing or split suites/flake8 fail.

t16 residual: later already-TERMINATED freeze with prior claimant dropped can still match leftover-sized bag (same snapshot as TERMINATED leftover + bag = 40). Record as residual; do not fail solely on that unless a test proves premature release of a live hold.

Return: gate PASS or FAIL, audit path, gaps.
