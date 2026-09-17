# Handoff — OK-RSI PR #50/#51 consolidation

PR #52 on `feat/andrzej_ok_rsi_audit_consolidation` is the source-only consolidation candidate. No merge, root pin update, deployment or execution restart is authorized by this handoff.

## Current status — REQUEST CHANGES at 60e73c371

Start with [the review of the latest product commit](reports/pr52-60e73c3-review.md), reviewed at `60e73c371d8cb6abc659018bd42b3f3880f209c7` against `4b46bd97526b660e9fe31374b731806f361a7083`. A documentation-only follow-up does not change those product bytes or constitute a product fix.

The explicit missing-status/missing-health guards, false readiness wait, first-repair cooldown, identity-based held lookup and position-executor over-authorized gross check are improved. Fresh isolated controls for those cases pass. Do not keep treating those exact older examples as unfixed.

Three accounting boundaries still block approval:

1. A terminal canceled child releases the still-authorized parent acquisition commitment during repair/reconciliation.
2. Present invalid or conflicting gross-fill measures can still be accepted; the shared OrderExecutor branch still clamps overfills.
3. One matching ledger row is accepted as complete held transfer without full receipt-set/cumulative coverage and owner-projection evidence.

Fresh source-level selection: **41 tests, 32 passed, 9 failed, zero errors/skips**. These execute verified executor/reducer source and selected controller excerpts with substituted runtime/connector/recorder boundaries. They are NOT native Hummingbot or service tests. The full native suite, source-bound image and non-live startup/recovery smoke were not run here. Hosted checks for the reviewed product commit exposed no executed step records and skipped the native build/test job.

## Historical evidence

- Original consolidation `5a53fa5`: reported split native counts 65 / 36 / 22 / 110 / 36. Original PASS/APPROVED reports are historical, not current approval.
- Follow-up `4b46bd9`: bounded executor fixes, 12 native-fixture regression cases (not run natively by that follow-up), historical isolated 24-pass/8-fail selection. See `reports/pr52-followup.md`.
- Latest product `60e73c371`: five changed files; reviewed in `reports/pr52-60e73c3-review.md`. The new counterexamples supersede any blanket closure claim, not the earlier retained evidence.

Do not compare pass counts across different test selections as if they measured progress on the same full native suite.

## Exit-policy and authority boundary

The user's September 10 exit-scope brief is research intake/design, not permission to change lot-versus-book profit policy. Its all-event diagnostic requires sealed local artifacts and independent review. No economic backtest, new portfolio-wide profit predicate, research registration or loss-policy change was performed by this source review.

Preserve #50 (`9334c20c`) and #51 (`d38705b0`) branches/history. Do not merge either independently or retire them as validated replacements before #52 passes its gate. Never commit credentials, exchange state, production logs or copied Cython binaries.

## Next implementation gate

Use the source-bound findings and native regression sequences in the latest review. Correct parent-intent reservation and cumulative receipt validation, then establish complete held-transfer provenance using the existing native owner/recorder. Verify per-pair execution-data usability separately from initialization flags. Run affected native suites plus connector/recorder and non-live startup/recovery checks against the exact final candidate. Obtain separate review after code changes. No new PR is needed.
