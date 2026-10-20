# Live buy/sell verification and private-stream recovery

Objective: verify V1/V2/V3 live buy/sell paths; fix a confirmed private WebSocket subscription failure without changing strategy or forcing trades.

Authority: live-adjacent; explicit user repair and earlier deploy/restart authorization. Native implementation belongs to Hummingbot. No strategy, sizing, allocation, credentials, forced orders, economic runs, cancellations or manual state resets. Shared dirty checkouts remain untouched. Owner worktree starts from freshly fetched origin/master 7eb366a.

Completion: regression fails before and passes after; confirmed account and SPOT orders subscriptions are required for private readiness. Reject subscription errors and connection-count revocation through existing reconnect; bound missing ACK; preserve early data. Exact-image buy/sell regressions pass for all affected bots. Review independently before adoption; guarded owner_restart preserves obligations, configurations, fill provenance, mounts and recovery identity. Validate API and Reporting after each owner, then whole stack. Do not infer live V3 sell from simulation or historical older-image sells.

## Evidence and issue register

| Area | Expected | Observed | State |
| --- | --- | --- | --- |
| V1/V2 trade lifecycle | Natural buys/sells persist exact receipts | Natural entry/exit receipts on current images; complete native terminal close evidence retained privately | VERIFIED before new repair |
| V3 lifecycle | Native profitable trailing and held recovery remain independent of entry pauses | Natural new entries; current positions below their exit conditions; exact-image paper round trip positive | VERIFIED with live-sell observation limit |
| Cash | No spending without actual free quote | Wallet-funded ceilings absent; available cash below the configured ticket; admission refusal consistent with cash | NOT A DEFECT; no funding change |
| Private subscriptions | Readiness requires successful account+SPOT orders subscription; rejection must reconnect | Six frozen regression failures; source sends and reports success before ACK, discards rejection | CONFIRMED; repair in progress |
| Research | Usable overview and bounded queue | Root-sync publication mismatch logged; one queue503, later200; whole30min47pass1fail historical Research log | Separate support issue; coordinated audit informed |

## Validation and rollback

Run native exact-image isolated tests with network none, no credentials/live state mounts, CPU1/memory1g: private subscription tests, native OKX connector/order-tracker/receipt tests, each bot's lifecycle, partial fills, protected exit floors, held recovery and restart continuity. Real missing-ACK deadline, reverse ACK order, early data, post-ACK revocation, cancellation and reconnect must be covered.

Use immutable image overlays over each actual predecessor; source manifests and non-label image config must match. Keep current/previous images and guarded stopped-owner DB/WAL backups. Prepare and audit exact deployment callers before writes. Adopt sequentially through native owner_restart, never reset or overwrite recorders. On failure preserve evidence and reconcile current obligations before rollback; never restore a stale DB over later fills. Push only task branches and open PR under standing authority; merges require task-specific authorization.

Private complete evidence: /Users/s1kor/.local/share/rsibot/releases/20261020-buy-sell-verification. Prior baseline read-only checks:715 tests +73 subtests pass, no native watchdog/error lines in >9h. Source/runtime and live/simulated claims stay separate.

## Review decisions and causal model

- R1 required both subscription ACKs but held early private updates in a second buffer. Independent review proved that a later second-channel failure cleared a received fill. Rejected this buffering design; R2 routes updates immediately into the existing native listener queue while readiness stays false. No second fill ledger or replay manager is added.
- Parent review reproduced the R1 text `pong` type failure and identified raw error-payload logging. R2 preserves expected text heartbeat controls and emits bounded errors without raw payloads. The exact-image focused result is29PASS; baseline targeted regressions17FAIL. R2 closes these findings.
- After the two distinct receive-boundary concerns, re-triaged the causal model: transport activity is not an application acknowledgement at either login or subscription. Independent review confirms the existing login `receive()` can wait indefinitely through transport PONGs because the underlying timeout restarts per frame. R3 adds the same existing24-second aggregate bound to login, with a real control-frame regression. Scope remains one private native connector module; no retry manager, heartbeat manager, credentials or strategy changes.
- R1/R2 evidence and HOLD receipts remain private and are not relabeled as final PASS. No live writes, source commits or image builds occurred before these review gates.

## Source gate

R3 independent review CLEAN; source4ee273a5, focused exact-image source-overlay30PASS/one pre-existing warning; frozen R2 fails the new repeated-transport-PONG regression. Ruff, compilation and diff-check pass. No product changes beyond the private connector; strategy files and real-money configurations remain untouched. Image packaging, full owner regression checks and live adoption are still pending at this source commit.
