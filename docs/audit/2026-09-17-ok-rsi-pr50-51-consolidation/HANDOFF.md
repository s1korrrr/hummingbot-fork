# Handoff — OK-RSI PR #50/#51 consolidation

PR #52 is the source-only consolidated candidate on `feat/andrzej_ok_rsi_audit_consolidation`. This is not a merge, root pin change, deployment, or restart.

## Current status — REQUEST CHANGES

Start with [the PR52 follow-up review](reports/pr52-followup.md). It supersedes the original blanket statement that A–G are closed and that no Critical or Important findings remain.

The follow-up starts from `5a53fa52540a897ed6ea300eeba1be290f4df569` and adds bounded executor corrections plus `test/test_ok_rsi_entry_boundary_regressions.py`. Missing-receipt expiry/revocation, quote-read failure handling, explicit cancellation vetoes, and final candidate-book validation were corrected in source. The isolated regression selection changed from 10 passes/12 failures to 22 passes.

The complete follow-up diagnostic selection still has **24 passes and 8 failures**. These are source-level probes with substituted boundaries, NOT the native Hummingbot suite. The remaining merge blockers concern fail-open permission/freshness evidence, quantity-inferred held transfer, contradictory cumulative-fill accounting, and the first-repair cooldown. No native suite or image/startup validation was run in the follow-up environment. The PR is not approved for merge or deployment.

## Historical consolidation evidence at 5a53fa5

The original orchestra run reported split pytest results of 65 down-only / 36 native+OKX / 22 capital / 110 position_executor / 36 factory. It recorded t18 spec PASS and quality APPROVED. These historical reviews and counts remain in `reports/`; they are not fresh approval of the current candidate and do not override the new counterexamples.

The initial commit included the full working tree rather than the old mixed index. Before restage, the index was `+1283/−291` across 13 files; the reviewed product tree was `+2604/−305` across 14 files, including `test/hummingbot/core/data_type/test_in_flight_order.py`. Use the current exact Git diff rather than assuming those old counts still describe subsequent changes.

## Do not

- Merge, pin, deploy, or restart while code blockers or required native/build evidence remain open. Merely starting CI does not satisfy the gate.
- Treat #50 (`9334c20c`) or #51 (`d38705b0`) as this consolidated candidate. Preserve their branches/history; retirement requires a validated replacement.
- Commit Cython `.so` copies, credentials, exchange state, or production logs.
- Change capital, DCA, loss-realization, or execution-owner policy to make tests pass.

## Evidence and next action

Original evidence was copied from orchestra run `20260917T063417Z`: `spec.md`, `ledger.md`, `README.md`, `briefs/`, and `reports/`. The original ledger snapshot listing t19 as running is historical. The generated large worktree diff was omitted; source truth remains the exact Git diff from `4a4372bcde4990355421a42df3b8ce6107299abb`.

Use `reports/pr52-followup.md` for the current findings, evidence limitations, native test command, required owner/recorder regressions, and rollback boundary. Correct those findings on this same PR, validate the exact resulting commit, and obtain a genuinely separate review. No new PR or live service transition is required to continue the source audit.
