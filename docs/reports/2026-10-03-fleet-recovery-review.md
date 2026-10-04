# Fleet recovery and PR review — 3 October 2026

## Result, authority and exact source

The combined source repair is independently reviewed and locally tested. The PR
remains **draft**: production adoption is **HOLD**, and this report does not claim
that all three live bots can buy or sell. The earlier terminal-dust report records
its original evidence; this report adds complete recovery ownership, signed fees
and native instrument reporting without rewriting those historical results.

Hummingbot base: `master` at `479b2d34b214573f82cb51623b99201f56c19469`.
Tested executable revision: `3030513324e2192e8263b1fd794f0783afb179d4`, branch
`feat/andrzej_terminal_dust_audit_20261003`, [PR 120](https://github.com/s1korrrr/hummingbot/pull/120).
The exact [PR 118](https://github.com/s1korrrr/hummingbot/pull/118) source
`eeb33677e429376320c81041589d8f35ca727991` is integrated by cherry-pick, followed
by the recovery corrections below. Do not deploy the earlier narrow images or
adopt PR 118 independently as a complete fleet recovery solution.

The root helper repair is separate in [rsibot PR 343](https://github.com/s1korrrr/rsibot/pull/343):
tested source `92351dab7fc77c45803524631dcb20fe87ba7df2`, documentation head
`f66438d64467b4a4dbc11edd1e74c2d6555ab04d`, target `main` at
`388a83fb711a887b31a5b1ae5027c51354868d8d`.

The latest operator instruction permits inspection, local repair, isolated tests,
push and PR review; it explicitly excludes deployment, merge, live restart,
orders/cancellation, credential changes, risk settings and production records.
None of those excluded actions was performed. Root and nested dirty work remained
in their original checkouts; repairs use existing isolated task worktrees.
Hummingbot remains execution and persistence truth. API, reporting and Condor are
consumers, not replacement accounting or order owners.

## Confirmed defects, root causes and corrections

| Defect and impact | Root cause | Complete source correction and retained guard |
| --- | --- | --- |
| V1 completed exits could make ETH exposure unavailable/infinite; a narrow fix could instead erase physical dust. | Managed remainder and physical inventory were conflated; a bounded recent report was insufficient for historical ownership. | Preserve the existing complete-history/held-ledger repair. Prove terminal orders, native fills/fees and lot residual; retain physical dust in exposure without granting managed remainder or sell authority. |
| V3 omitted individually sub-lot BTC residues whose sum exceeded a lot, producing an entry hold. | Finished non-held rows were absent from the inventory projection; gross quantity/configured price could not establish native remaining cost. | Preserve proven terminal dust in exact quantity and acquisition basis. Reject conflicting same-ID overlays, duplicate claims and missing native evidence. |
| Prepared fleet recovery could omit terminal dust and still claim complete ownership. | Residual derivation considered held/active records but not the complete native fill ledger. Signing incomplete totals did not prove conservation. | New shared recovery proof reads exact native fills, orders, terminal residues and independently supplied lot steps; reconcile the full per-asset ledger with held/active/dust ownership. Missing/deleted receipts, orphan exact companions, duplicate fills, conflicting identities and unowned quantities fail closed. The V3 terminal proof is shared rather than copied. |
| Old or incomplete recovery input could be accepted without binding all relevant source/evidence. | Source and owner observations did not seal the complete recovery dependency closure and lot input. | Bind the complete source closure, lot maps, owner observations and recovery inputs into the prepared evidence/HMAC. Legacy manifests must be regenerated after qualification; no live manifest or checkpoint is rewritten here. |
| Real native maker rebates could make V2 funding, V3 valuation or recovery unavailable. An intermediate sign-only fix would accept a fabricated valued fee. | Three funding parsers and adjacent recovery checks assumed nonnegative fees; agreement between duplicate numeric encodings did not prove consistency with actual fee currency/amount. | Accept finite signed native fees and require the recorded quote valuation to equal quote fees plus base fees times the exact fill price. Keep actual receipt identity, zero percentage encoding, supported base/quote currency, nonnegative remaining cost, no unowned sales and acquisition fee ceilings. |
| Multi-symbol V3 executor and held rows could report the controller identifier as their instrument. | The generic runtime snapshot propagated one controller-level pair fallback into every lifecycle row. | Use each executor's configured pair and each PositionSummary's pair. Keep controller identity separate; missing, blank or non-string instruments remain unavailable. Regressions invoke real StrategyV2Base methods. |
| The root restart helper could omit unresolved FINISHED/null-close records, including a preflight-to-snapshot race. | Terminal/null-close rows without a suspension marker were silently excluded. | PR 343 requires coherent native flat proof and repeats the predicate in the native-state transaction. Reject uncertain IDs, incomplete execution receipts, remaining quantity, held/snapshot/reconciliation markers, nonfinite values and contradictory resume evidence. |

The recovery reader uses native SQLite model result processors; it does not invent
precision by parsing rounded values as exact fills. Spot receipt identity includes
config/owner/order/market/pair/base/quote, 1x leverage and non-derivative position
semantics. A canceled order can have partial fills. Even a zero-fill predecessor
needs explicit terminal proof. Uncertain submissions cannot become blind retries.

The dust helper was first placed inside the frozen V2 source directory; the broad
matrix caught the accidental source expansion. It now belongs to V3's core. The
intentional shared funding correction changes only `native_funding.py` among the
same 67 V2 closure paths, from
`17cba31a66162942cf9a59c10ef6e68d82028e6e614b341f2521a5a6d13951c8` to
`9803842ea19120739feeb0c367c6999f5d19f2a1514cde41653dad5d66fc7885`.
See the [release audit](../controllers/modular_spot/RELEASE_AUDIT_20261003.md).
The intermediate signed-only digest is superseded. Existing live/research bindings
retain their historical source identities; no migration is inferred from a test.

## Intended contracts and primary documentation

The current [OKX API v5 contract](https://app.okx.com/docs-v5/en/) defines spot
lot/minimum size in base units and fill fee amounts together with fee currency.
OKX's fee sign is normalized by this fork's connector: a native negative fee is a
rebate. The local installed dependency contract is the exact source of
`TradeFeeBase.fee_amount_in_token`, `MarketsRecorder._did_fill_order`,
`PositionHold`, and `ExecutorOrchestrator._trade_fill_order_record`; the recorder
values base fees using the fill's price. Order-level aggregate fee fields are not
substituted for exact fill receipts.

The replay uses [Python 3.13 Decimal](https://docs.python.org/3.13/library/decimal.html)
and standalone backups made using the
[SQLite Backup API](https://docs.python.org/3.13/library/sqlite3.html#sqlite3.Connection.backup).
Immutable mode is used only on those sealed copies, not a running database/WAL.
Detached ORM handling follows the installed
[SQLAlchemy 2.0 session contract](https://docs.sqlalchemy.org/en/20/orm/session_state_management.html).
No dependencies, strategy thresholds, sizing or loss policy changed.

## Executed verification

All final Hummingbot containerized application tests and replays below bind source `3030513324e2192e8263b1fd794f0783afb179d4`
to test image `sha256:ff7d001a07578c40645b9121ea916e057ae39eea768550be3eca7dfda9f6857e`,
tracked-source manifest
`a215b64051756f2ca67be4184b9f8e2d0d555fe3eea89f1c2816f116ae8de6f3`
and an empty uncommitted patch. Later report/plan changes are documentation only.
Counts overlap between suites and must not be added as unique cases. Root helper
checks bind separately to PR 343 source `92351dab7fc77c45803524631dcb20fe87ba7df2`
and its host-side mocked test environment. Host lint and Git checks do not run in
the Hummingbot application image.

| Contract / check | Actual result and limitation |
| --- | --- |
| Regression RED before repairs | Retained failures for omitted native ownership, orphan exact fills, held signed fees, V3 BUY/SELL base/quote rebates, contradictory fee valuation, mislabeled V3 rows, and root unresolved terminal receipts. Original dust RED and real-paper baseline comparison remain in the earlier report. |
| Recovery, source seal, dust and actual paper lifecycle | **161 passed, 13 subtests**, no skips. Includes the actual security CLI with synthetic credentials in ephemeral configuration and no network. |
| Changed-path coverage group, acquisition-chain recovery, real native report methods | **490 passed, 15 subtests**; 956 existing hmmlearn/NumPy deprecation warnings. |
| Every pytest target from modular-native workflow | **1,961 passed, 21 skipped, 76 subtests**; 3,054 existing hmmlearn/NumPy and commlib/Pydantic deprecation warnings. The missing local candle cache remains unverified. |
| Required compiled/controller imports and optional candle serialization | Workflow import list passes. With the retained test-only PyArrow 20 wheel in executable disposable tmpfs, **76 candle tests pass**, including the 20 Arrow cases skipped above. The first noexec-tmpfs attempt failed to map Arrow; both harness failure and corrected run are retained. Pip's root-install warning is retained; installation occurred only in disposable tmpfs. |
| Real native paper/recorder round trip | NativePaperTradeExchange, native executors and MarketsRecorder complete two exits, retain .018 base dust and its cost, grant no sell authority, reload, and preserve four fills under replay. The test rejects application WARNING/ERROR output. No external venue. |
| Historical detached accounting replay | V1's 36-row ETH history, bounded 20-row report and three dust records replay with exact reserve; all five V3 pairs preserve exact quantity and native acquisition basis. Input backup hashes unchanged. |
| Historical complete recovery proof | V3: all five assets conserve across 25 exact native fills. V1: intentionally **HELD**, `native fill lacks exact event precision for recovery`. Historical lot receipts are not current venue qualification. No state rewrite. |
| Diff coverage | **89%**, 1,305 changed executable lines, 136 unexecuted; 80% threshold passes. This is changed-line coverage, not complete global coverage. |
| Root restart helper | **216 tests pass**, Ruff passes; independent review closes terminal-proof and transaction-race findings. Root helper tests mock Docker boundaries. |
| Pinned lint/format/secret hooks and diff checks | Pass across all intentional owner code/test paths; no dependency or generated runtime state changes. |
| Broad stable workflow | **FAIL at collection**, same 20 collection errors and missing-module set as the unchanged base. No full stable-suite or global 70% coverage claim. These are actual local failures, separate from hosted infrastructure status. |
| Independent final review | Separate read-only reviewer: no remaining new-source correctness blocker in the frozen combined diff; checked source/image binding, final receipts and remaining live gates. Source qualification only. |

Environment: Python 3.13.15, Pydantic 2.13.5, SQLAlchemy 2.0.52, aiohttp 3.13.5,
pytest 9.1.1, coverage 7.16.0. The image overlays tracked source onto the documented
compiled native environment (`812e1809…` through sanitized base `65c31034…`). No
pyx/pxd ABI changes were made. All application tests use disposable `--network none
--read-only` containers, bounded CPU/memory, ephemeral logs/config/test databases,
and no live credentials, socket or production mount. Historical native backups
alone are mounted read-only. Host execution is confined to tooling and mocked root
helper tests, not a credentialed trading application.

Workflow mapping: `.github/workflows/modular-native.yml` native imports/pytest were
executed locally as above. Its unchanged Rust 1.88 candle job was not rerun.
`.github/workflows/workflow.yml` pre-commit and stable collection were run; scoped
coverage replaces neither failed global collection nor global coverage.
`.github/workflows/docker_buildx_workflow.yml` clean amd64/arm64 source builds,
clean Conda compilation, alternate Python/architecture, release signing and image
publication were **not executed**. The local test image is not a release image.
Hosted Actions were not run or retried by this task and do not substitute for
the local qualification above.

Exact argv, timestamps, logs, RED runs, source manifests, coverage XML and sealed
backup hashes are retained under
`~/.local/share/rsibot/validation/fleet-review-20261003T110643Z`.
The committed [sanitized verification record](2026-10-03-fleet-recovery-verification.json)
contains commands and artifact hashes without credentials, raw financial rows or
private order identifiers. No economic backtest was run: these are accounting,
recovery and projection corrections, not a strategy-performance claim.

## Current production observation — 12:43–12:45 UTC

All 13 live containers run, with unchanged IDs/images/start times since the first
11:07 observation, zero restarts and no OOM. Configured health probes pass; V3 and
ingress have no health probe. The retired Telegram container became absent during
this run through an external action; this task did not remove it. Telegram and its
old supervisor remain retired per operator instruction.

| Bot | Current entry permission and actual gate | SELL / protective continuity | Qualification |
| --- | --- | --- | --- |
| V1 | Five entry-pause flags false. ETH still `exposure_unavailable` on the old live image. Other cached gates are not guarantees of a qualifying order. | Two SOL executors waiting for trailing activation; five held positions; native in-flight count zero. | Report age .613 s. BNB ownership, legacy exact-fill precision, SOL continuity and durable daily-loss policy remain unresolved for restart. |
| V2 | Five entry-pause flags false; all native admission states conditional with no reported reason. ETH/SOL `operator_pause` labels are historical last-attempt results. | No active executor, four held positions, native in-flight count zero. Can evaluate under normal strategy/funding/risk checks; future fill and venue-wide state are not guaranteed. | Report age .470 s. Configured 50-quote daily-loss rail exists and is not paused. No deployment or setting change. |
| V3 | Execution enabled and operator pause false, but `NATIVE_INVENTORY_MISMATCH:BTC-USDC` still holds entries on the old image. | One BNB executor waiting for trailing activation; native in-flight count zero. Its current report still has the instrument-label defect repaired in source. | Report age .681 s. Source/recipe migration, effective fence, BNB/funding continuity and durable daily-loss policy remain unqualified. |

Authenticated API GET reports current, verified identities for all owners. The
reporting readiness endpoint returns all three owners healthy with recent samples.
Broker/API/reporting transport health does not clear native entry holds. Native
in-flight counts do not prove there are no unknown venue orders.

All five candle feeds are online/ready; status age 4.58 s, newest closed bar 29.93 s,
zero reconnects/rejections in the sampled service state. Current native V1/V2
prices are populated. Direct current book freshness, complete authenticated venue
order/balance/funding reconciliation and a new fee schedule check remain
**UNVERIFIED**; host public instrument probes returned HTTP 403. That host response
is not treated as a failure of the running bots' transport.

Logs since 10:00 UTC retain one V1 price timeout (10:21:54) and eight candle
unavailable warnings (10:22:14–24), with all feeds recovered by 10:22:28. V2 has
price timeouts at 10:12:09, 10:22:13, 11:11:11, 11:59:20 and 12:23:20; subsequent
native prices are fresh. Other fleet stdout has no new warning/error in this
window. Bounded 4 MiB native file tails corroborate the V1/V2 warnings; V3's quiet
file is not evidence of a stalled loop because current reports expose its hold.
No logging severity or error handling was weakened. Timeout root cause is not
proven, and no speculative restart is represented as a repair.

Host Data volume remains 98% used, about 23 GiB free; reporting RSS is about
1.26 GiB with variable CPU. No broad Docker/data cleanup was authorized or used.

## Other PRs reviewed and material exclusions

This is source/diff/metadata review at the listed heads, not fresh whole-project,
economic or physical-device qualification of every open PR.

| PR | Reviewed head | Disposition |
| --- | --- | --- |
| [rsibot 333](https://github.com/s1korrrr/rsibot/pull/333) | `3795d5a26c1630fa3bcb3c9ae7ea026209ffd32a` | Historical launch artifacts, not current readiness. Bounded 15-file scan found no credential literals. Historical V2 API-password exposure was documented; current applicability/rotation remains UNVERIFIED. No credentials were printed or changed. |
| [rsibot 334](https://github.com/s1korrrr/rsibot/pull/334) | `8d0acdf185fc07452cd38b02f0e339eca6eb2688` | Exploratory Binance-proxy research; no OKX execution or promotion qualification; economics not rerun. |
| [rsibot 336](https://github.com/s1korrrr/rsibot/pull/336) | `1cf62f58fbc684c1602388d6ee96622c55e96c66` | Causal feature review; report says no model beats control. Temporary raw-input provenance and previously inspected window limit independent economics; no entry-policy adoption. |
| [rsibot 338](https://github.com/s1korrrr/rsibot/pull/338) | `f7fb505db9cbaa9835e9d59a826bbb53443ce69a` | No new P1/P2 in bounded fleet/mobile diff. Physical current-build install, Watch relay/widgets/complications and background behavior remain unverified. |
| [rsibot 344](https://github.com/s1korrrr/rsibot/pull/344) | `d90b7dc1bcf2263179357c17f19cba3c1221d26a` | Stacked on 338; earlier 63 focused reporting tests passed. Preserve dependency on Condor 38 and producer/consumer qualification. No current physical acceptance. |
| [Condor 38](https://github.com/s1korrrr/condor/pull/38) | `95a97c169c543038a60e458189d34c4b3613395a` | Recipient/installation binding and delayed-APNs race review found no blocking source defect. Owner evidence reports 218 scoped tests; not rerun here, no production APNs claim. |
| [Condor 39](https://github.com/s1korrrr/condor/pull/39) | `c807f6585a7289517ca0b887d9937998e170f613` | Appeared during final inventory refresh. Three-file Market Picture change surfaces existing component-fault names without upstream error text, preserves stale/lagging precedence and offers retry. No blocking source finding or dependency on 38. New regression is source-pattern based; no local application, rendered-browser or deployed qualification in this review. |
| [quants-lab 81](https://github.com/s1korrrr/quants-lab/pull/81) | `4e2b82ffccc21bc4cda38a72800b8b37c3a1f3ac` | Frozen Binance research profile rejects inappropriate OKX receipts. No new research run or economic acceptance. |

Other PR hosted failures include missing private-component access, not only
budget exhaustion. Those checks are not called green, and no protections were
bypassed. The root helper and owner repair are published separately; unrelated
PRs were neither merged nor rewritten.

A separate remaining consumer limitation is explicit: root
`dashboard/hummingbot-reporting/hummingbot_reporting/quant_ops/bot_projection.py`
assumes one controller per pair. V3 has one multi-symbol controller. Correcting
executor instrument labels alone cannot produce complete per-pair valuation:
the generic native snapshot omits completed physical dust and does not supply a
complete configured-pair universe with fresh numeric marks and non-overlapping
basis/PnL/fee coverage. An isolated consumer reproduction remains unavailable
with a pairless controller and correctly labeled held/active rows. Per-pair money
must remain **UNAVAILABLE** until that owner report contract and consumer are
qualified together; do not fill it from shared wallet balances or invent marks.
This is a reporting limitation separate from the source repair of entry inventory.

## Deployment prerequisites and rollback

1. Retain draft status. No restart, deployment or merge is authorized by this audit.
   Prepare an exact release image only after the remaining source-identity,
   native/venue and risk gates can be demonstrated on a current clone.
2. V1 requires authoritative native/venue reconciliation for legacy missing exact
   precision and the unresolved BNB ownership record, preserving both active SOL
   executors. Do not manufacture precision, erase rows or reconstruct risk history.
3. V3 requires a qualified source/recipe/held/funding migration, effective native
   entry fence, current BNB continuity and durable risk restoration. The shared
   funding change also requires deliberate V2 identity qualification if adopted.
4. V1/V3 daily-loss limits remain an unanswered operator policy decision. Do not
   infer numeric limits or use in-memory-only/reset loss state as durable evidence.
5. A fleet snapshot/HMAC is insufficient if stale: revalidate every owner's native
   state, orders, fills, balances, lot rules and reservations immediately before
   the documented guarded operation. Any failed prerequisite must prevent that
   operation rather than bypass funding, recovery or exits.
6. A separately approved rollout must verify exact images/mounts/source bindings,
   current entry permissions, native quantity/basis conservation, protective exits,
   fresh market/fee/book/funding evidence and report agreement without test trades.
7. Rollback preserves recorder/order continuity. Never restore a stale database
   after trading may have occurred. Keep receipts, old images and native backups;
   uncertain post-stop state stays fenced under the qualified procedure.

Repository-only rollback is to retain/revert intentional source commits on the
repair branch. It cannot undo orders or production state, and source publication
does not imply that the running fleet has adopted any fix.
