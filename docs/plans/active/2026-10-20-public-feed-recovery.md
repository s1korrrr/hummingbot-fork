# Public-feed recovery, native integration and guarded adoption

Objective: repair explicit OKX public subscription errors that leave a connection epoch valid; review and merge native PR #156 and required dependencies; integrate exact merged owner revisions, deploy and verify all three bots without strategy changes or forced trades.

Authority: live-adjacent. The user's October 20 instruction explicitly authorizes this public infrastructure repair, coordination, merges of PR #156 and its dependencies, deployment and verification. It narrows the frozen transitive-source exception to the shared OKX public source. No strategy, order sizing, allocation, risk, credentials, orders, cancellations, funding transfers or recorder resets. Preserve shared dirty checkouts and other tasks' work. The coordinated task transferred this clean worktree and branch; this task now owns cleanup. Support live identities remain fenced by agreement.

Owner: native Hummingbot. Adopted branch feat/andrzej_okx_public_ws_recovery_20261020 starts at fresh master 7eb366a and fast-forwards to private PR #156 head a4c48e2ea. Root/API integration changes occur separately in dedicated owner worktrees, only if required by actual package and gitlink contracts.

Cause/evidence: the peer's frozen network-isolated probe fails for scoped and arg-omitted error controls after a valid books snapshot. Public routing sends these to the base no-op unknown-channel hook. Processing never exits, so its existing finally does not invalidate the epoch. Public trades LastTrade can remain stale until the existing 180-second REST fallback; independent MarketDataProvider REST and BBO/Mid strategy pricing limit the economic claim. No live rejection or wrong entry price is established. Provider protocol reference: https://www.okx.com/docs-v5/en/ .

Repair: use the native public unknown-control handler to raise a fixed sanitized ConnectionError on explicit error events. Reuse existing invalidation/finally and reconnect; preserve benign controls, books/trades routing, snapshot readiness and heartbeat ownership. Correct initial and dynamic send-time logs to describe requests sent, not acknowledged success. Do not add a subscription manager or change trading policy.

Validation: fail-before/pass-after targeted processing and real native listener reconnect regressions; benign controls and data routing; public book sequencing/recovery and private regression suites. Independently review immutable source/test hashes. Qualify exact immutable runtime images derived from actual predecessors; prove only approved connector product files differ and strategy/config bytes match. Run each bot's native buy/sell, partial-fill, protected-floor, receipt, held recovery and restart tests with network none and no live state/credentials. Merge owner-first only after local CI-equivalent checks and independent audit; classify hosted budget failures as INFRA_FAILURE. Propagate exact merge SHAs through required package/gitlink consumers.

Deployment: frozen reviewed native owner_restart callers; sequential V2, V3, V1 with current-state fences, stopped-owner backups, continuity checks, API rebind and canonical pin persistence. Preserve V3 recipe, capital, runner and seals. Whole-stack verifier after each restart, then a bounded final native observation, exact receipt parity and financial-clock continuity. Never force a trade or infer an unobserved live sell from isolated simulation.

Rollback: retain actual predecessor images and guarded stopped-owner DB/WAL backups. On failure stop deployment progress, retain evidence and reconcile current obligations before recovery; never restore an older recorder over later fills. Revert owner source/integration commits if needed. Cleanup only owned clean, reachable worktrees without process/container references; retain virtualization-referenced worktrees without disrupting the live VM.

Private evidence: /Users/s1kor/.local/share/rsibot/releases/20261020-public-feed-recovery . No memory or lesson-store writes are authorized. Root sync uses --skip-research to avoid a publication writer during this execution-only repair; Research remains read-only verified by whole-stack checks.

## Gates

- [x] Contracts, ownership handoff, baseline identities and preservation fences.
- [ ] Fresh RED regression and smallest native repair.
- [ ] Independent source review and exact-image regressions.
- [ ] Reviewed PR #156 and public dependency merged; required downstream pins integrated.
- [ ] Guarded three-bot deployment and final native/stack evidence.
- [ ] Report, coordination handoff, root sync and safe cleanup disposition.

## Source and dependency gates

PR #156 independently reviewed CLEAN and merged as1355c09e42cbc967c60e78bdf65fb6719dfadfcc. Public regressions fail4/pass1 on the deployed baseline; final combined candidate82pass plus7subtests. R1 independent source review is CLEAN, with one test-coverage note addressed by delivering the replacement snapshot through the second socket and native queue/diff consumer; source bytes unchanged. R2 test refinement independently reviewed CLEAN. Repository hooks removed only a trailing blank line; exact formatting-only equality is recorded. Initial over-broad connect mock correctly refused recovery; that harness failure is retained. Ruff, compile and diff-check pass.

API dependency audit confirms its image installs exact Hummingbot source. Required API promotion is the native source lock plus matching release-manifest commit identity, retaining verified frozen controller hashes. The running API is a custom hybrid application image, so its application source must be preserved while adopting the verified native package; never equate API default-branch pins with current runtime labels. API/root dependencies will be reviewed and integrated owner-first after the final native merge.
