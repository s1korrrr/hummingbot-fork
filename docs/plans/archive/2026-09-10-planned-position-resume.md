# Planned position resume

## Contract

Implement an offline, explicitly selected, once-only resumption of fully filled
spot BUY PositionExecutors after a verified strict keep-position stop. Preserve
the original executor ID, configuration, absolute time barrier, terminal entry
receipt and full trailing state. No replacement entry is allowed. This work owns
only this isolated Hummingbot worktree; no production database, process, exchange,
commit, push or deployment actions are authorized for this worker.

## Design

The offline prepare/inspect CLI validates a preflight snapshot, terminal stopped
database records, explicit recent venue reconciliation evidence, source and
configuration file bindings, and exact receipt ownership. Preparation stores a
durable manifest without changing executor or held ownership. Native startup
validates the manifest and atomically claims it while marking the original
executor unfinished. Claim failure cannot silently restart or retry. Hydration
uses a separate sealed-entry state and native exit control. Claimed entry
receipts are excluded from all held reconstruction paths while owned by the
executor; unrelated receipts in the same pair remain held.

## Validation and rollback

Use owner Python /Users/s1kor/anaconda3/envs/hummingbot/bin/python. Focused tests
cover real InFlightOrder serialization through strict stop, SQLite preparation,
startup hydration, original timestamps, waiting and armed trailing, no BUY,
native SELL control, stale bindings, receipt conflicts, duplicate/failed claim,
and accounting ownership. Run focused existing executor/orchestrator regressions
and diff/style checks. Rollback before claim is the original stopped DB backup;
after claim, stop and reconcile before any restoration. A claimed manifest never
authorizes automatic rehydration following another crash.

## Progress

- [x] Isolated worktree clean at bc8b6e900; implementation ownership accepted.
- [x] Failing regression coverage.
- [x] Offline validation and durable registration.
- [x] Native startup claim, hydration and ownership transfer.
- [x] Focused tests, formatting and handoff evidence.

## September 12 continuation

PR 39 was merged at 1e35e675a outside this task while paused. PR 40 added the
fee-adjusted trailing floor; the continuation uses a fresh worktree based on
origin/master 4e8f0c6c6365b63daff77614cac2f0d78c0127e7 and preserves that behavior.
The old unfinished worktree is retained. The user reconfirmed completion and
explicitly prohibited stack restarts with active live owners. Read-only inspection
found one active SOL executor and both owners running the PR 40 image; no restart,
pause, venue request, or runtime mutation is part of this continuation.

Root reproduced and corrected two additional review findings: pending reports
reconstructed selected inventory before claim (four regression failures), and
after-claim failure followed by persistence marked unresolved inventory completed
(two regression failures). Pending reports now raise an explicit unavailable
error; failed hydrated objects are quiesced and excluded from persistence while
durable unfinished ownership remains and strict shutdown fails closed.

Observed owner validation before the final ownership fix: 109 tests plus 10
subtests for recovery and position executors; 199 tests plus 38 subtests for
orchestrator, held accounting, exact receipts, controller and OKX boundaries.
Final ownership validation and publication evidence will be appended below.

PR 39 CI limitations: image verify failed with no space left on device; a later
client-change job was blocked by account payments/spending limit. Both are
INFRA_FAILURE, not observed source regressions. No CI-green claim is made.

Final root ownership rerun: 55 passed in 5.69s across planned resume, executor
orchestrator and held accounting. All applicable pre-commit hooks passed,
including flake8, formatting, import ordering and private-key checks. Both staged
and unstaged git diff --check passed. Linux image verification and external
publication are tracked by the parent publication plan; runtime deployment is
blocked by the active-owner restriction.
