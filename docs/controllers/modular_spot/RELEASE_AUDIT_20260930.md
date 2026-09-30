# Native recovery and funding audit

This candidate repairs two reproduced persistence and order-admission defects.
It has not been promoted to paper, canary or live execution. The upstream
[Strategy V2 architecture](https://hummingbot.org/strategies/v2-strategies/)
assigns order management to executors; the custom fork preserves that authority.

A failed planned ownership transfer now records its unfinished claimed records
durably. They are reconstructed from the native recorder on the next boot and
reported through `recovery_required`, the pending reconciliation rows, the
native status snapshot (`position_resume_recovery`) and `format_status`. A
malformed or expired prepared manifest, or an unreadable manifest history, is
reported the same way.

These reports are never an order fence. Per the standing operator decision
(2026-09-25, restart continuity D2: "We never want to block orders"), entries
and exits keep running on every pair. An unproven position is not rebuilt as a
live executor; restart continuity keeps its existing held-ledger handling and
the profit floor still prevents loss-selling. No failed claim is retried
automatically. An earlier candidate of this PR fenced order admission and was
reverted before merge because it also blocked held-bag recovery SELLs.

`NativeFundingStore.assess()` and `entry_notional_ceiling()` do not upgrade source
bindings or write metadata. The runtime explicitly calls
`prepare_runtime_binding()` before assessment and sizing, preserving the existing
source-only upgrade contract and its receipt. Configuration or owner changes
still require the stopped transition with native and venue quiescence. A stopped
packaging reader must report mismatched bindings without modifying the ledger.

The intentional funding changes alter the legacy modular source closure from
`14e76708f297834597beaa7b7eb38b89aa00e5ce1f781b01a406a8264c625224` to
`3655f2a430c809ec56fe76fb8cb33ea53585efb8af64aff2d68fe73af7ce9379`.
The crash-policy isolation test is resealed to this candidate and continues to
reject inclusion of crash/recovery policy modules in the modular closure.
Historical research retains its actual old source identity; it is not evidence
for the new candidate. Native source closure and compiled engine/image identity
remain separate deployment inputs.

Validation uses the pinned deployed compiled runtime with the complete candidate
Python source and no changed compiled source. Network-disabled regressions cover
failed restore/claim boundaries, durable unfinished-claim reporting across a
restart, direct/executor/queued orders continuing while claims are reported,
funding metadata purity, explicit runtime preparation and stopped maintenance
transitions. This
does not establish a venue recovery rehearsal or a release image seal.

The final compiled-runtime qualification passed 327 tests and nine subtests;
the downstream stopped-recovery qualification passed 49 tests. The native run
retains 432 third-party hmmlearn/NumPy deprecation warnings. The qualified
container has a complete source-file manifest on the pinned compiled base.
That manifest and its image digest are retained in the local audit evidence;
they do not authorize deployment or prove a whole-stack release seal.

Rollback preserves current fills, receipts, claimed records and fences. Revert
the source change only after assessing the ownership and binding state; reverting
to the former admission bug is not a safe recovery. Never replace a current
recorder with an older database or remove claim/recovery markers to start trading.
Use the stopped, owner-scoped recovery procedure with fresh venue evidence.
