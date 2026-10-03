# Terminal dust capital accounting audit — 3 October 2026

## Result and scope

Source repair and isolated qualification completed. Live adoption is **HOLD**. This report does not certify live trading readiness or authorize deployment, risk changes, record repair, orders, or cancellation. No production services were restarted, and no production configuration or execution records were changed.

Owner: `s1korrrr/hummingbot`; intended base `master` at `479b2d34b214573f82cb51623b99201f56c19469`; repair branch `feat/andrzej_terminal_dust_audit_20261003`. Production code was frozen/reviewed at `124b6ca1cdeb79d97e2394afc913ff3eddf14615`. Final executable tree `f8e21a48444bdb7cdcfed9f7941a5fda71991988` adds only a reviewed paper-fixture correction. Subsequent report/plan commits change documentation only.

Audit boundary: completed spot executor → native order snapshots and TradeFill → recorder history/held ledger → V1 capital admission and report → V3 inventory/basis/commitment snapshot. Included fee-unit/sign helpers, cache overlays, detached ORM rows, exact lot boundaries, duplicate claims and restart constraints. Hummingbot remains execution truth; API/Condor projections do not reconstruct inventory or grant orders. Strategy optimization, economic backtesting, connector-wide redesign and the independent recovery migration work are excluded. No policy was inferred from historical profitability.

Canonical dirty root/nested work and existing worktrees were preserved by working in a separate owner worktree. Only listed intentional owner paths are published; no root gitlink, credentials, native databases, private order identifiers or memory stores are included. Initial dirty fingerprints were retained privately, but their original hashing recipe was unavailable at final handoff; no claim of matching reconstructed hashes is made.

## Confirmed defects and complete corrections

| Defect / capital impact | Cause | Correction and invariant |
| --- | --- | --- |
| V1 ETH completed exits produced unavailable/infinite exposure and blocked admission. The initial narrow correction would erase owned dust. | Managed remainder and physical residual were conflated; only the recent bounded executor report was consulted; persisted scalar sides and complete historical ownership were not handled consistently. | Read complete native owner history plus held ledger. Validate terminal lifecycle, native order/fill/fee conservation, owner/config/market/side and recorded lot. Retain physical dust in the exposure cap/report but grant no managed remainder, active commitment, DCA spacing or sell authority. Missing/contradictory proof stays unavailable. |
| V3 omitted two individually sub-lot BTC residues whose sum exceeds one lot, causing global inventory mismatch/entry hold. | Inventory projection dropped completed non-held rows; gross/configured entry price cannot establish remaining acquisition cost after base fees. | Retain independently proven terminal dust quantity and native PositionHold funding cost. Reconcile exact native fills, signed fee currencies and order snapshots. Aggregate inventory while leaving sellability/reduction/grants at zero. Detached recorder rows require no lazy session access. |
| Same-ID current evidence could erase or contradict persisted inventory/proof. | Unqualified replacement of historical state by a current overlay; duplicate ownership/fill claims were not always rejected. | Reject duplicate identities, conflicting config/ownership, reused fills and loss of persisted dust proof. V1 only accepts a reduction/release supported by native monotonic fill/terminal evidence. V3 requires equivalent persisted terminal proof. |
| Untokened SELL AddedToCost percentage fee was omitted from base quantity. | Base fee inference covered BUY DeductedFromReturns but omitted SELL base collateral semantics. | Infer base only for those native fee contracts; preserve explicit fee currency and signed rebates. Independently reject inconsistent dust receipts. |
| The new paper fixture could pass while native fee serialization logged four errors. | ClientConfigMap changes the oracle quote token and clears its seeded prices. | Seed the real offline conversion after client configuration, assert its inverse, and fail the lifecycle test on any WARNING/ERROR. No logging severity, production conversion, or network behavior was changed. |

Important boundaries: canceled orders can have fills; only positively proven CANCELED/FILLED states qualify as order-terminal proof. FAILED alone does not establish absence of uncertain submission. Zero-fill predecessors still need explicit terminal evidence. Position-held or unfinished owners remain capital owners. Dust below a lot remains capital, including dust in the held ledger. Native quantity and acquisition cost use actual fee currencies/signs rather than fabricated balances or fallback prices.

The native receipt producer currently derives some residuals without subtracting SELL base fees. The strict consumer deliberately refuses a contradictory receipt; it does not rewrite native history. Qualifying a producer/migration for those states requires separate evidence and is not asserted here.

## Contracts and primary references

Spot `fillSz` is base quantity; fee amount/currency must be interpreted together, and a canceled order may have partial fills ([OKX API v5](https://www.okx.com/docs-v5/en/)). Actual local `TradeFeeBase`, `PositionHold`, `ExecutorOrchestrator._validate_reconciliation_fee` and `MarketsRecorder` code at the pinned revision are the dependency contracts. Native reconciliation accepts materialized flat fees rather than nonzero percentage-only records.

Decimal arithmetic retains exact base units ([Python 3.13 Decimal](https://docs.python.org/3.13/library/decimal.html)). Production snapshots were created through SQLite Backup API, then mounted read-only; immutable mode was used only on those standalone backups ([SQLite documentation](https://docs.python.org/3.13/library/sqlite3.html)). Detached rows are validated from eagerly materialized fields, without lazy order relationship access ([SQLAlchemy 2.0 session state](https://docs.sqlalchemy.org/en/20/orm/session_state_management.html)). Optional Arrow validation used a test-only PyArrow 20 wheel ([Arrow 20 installation](https://arrow.apache.org/docs/20.0/python/install.html)); no dependency file changed.

## Verification matrix

| Contract / scenario | Executable evidence | Result / limit |
| --- | --- | --- |
| Original failure versus intended quantity/cost contract | Final real-paper regression mounted over unchanged baseline image `65c3103…`; same test in final image `dec99dd…` | Baseline fails both owner subtests: V1 reserve 0 instead of 1.8 quote; V3 unreconciled. Final 1 test + 2 subtests pass. |
| Native order/executor/recorder lifecycle, base BUY fee, quote SELL fee, aggregate dust | Actual NativePaperTradeExchange, DownOnlyMakerPositionExecutor, native MarketsRecorder and SQLite; two completed exits; recorder reload; market state restoration; repeated trade sequence | Exact .018 base retained, independently derived acquisition basis, no sell authority/commitment; 4 fills unchanged after replay. No warnings/errors accepted. No venue connection. |
| Malformed/missing/duplicate/stale/contradictory records; partial and zero canceled orders; fee sign/currency; lot threshold; held inventory; overlay | Expanded accounting pytest group | 467 tests + 7 subtests pass on frozen production source. |
| Native modular workflow, controller/remote interface adjacent behavior | Every pytest target from `.github/workflows/modular-native.yml` | Baseline and repaired source each 1,945 pass, 21 skip, 76 subtests, 3,054 dependency deprecation warnings. No claim of clean global logs. |
| Optional candle serialization cases | Candle aggregation suite with test-only PyArrow 20 in disposable tmpfs | Baseline and repaired source each 76 pass; covers 20 Arrow cases skipped above. The one absent local cache fixture remains unverified. |
| Exact recorded state, beyond synthetic fixtures | Read-only real ORM replay of consistent native backups | V1 ETH 36 history rows / 20 visible reports / 3 dust rows: exact reserve. V3 all five pairs: exact quantity and native fee basis retained. Backup hashes unchanged. No production record edits. |
| Coverage of changed production lines | 143 tests + 2 subtests; scoped branch coverage; diff-cover 10.5.1 against the full base→repair diff | 510 changed executable lines, 71 unexecuted, 86.08% diff coverage (80% threshold passes). Whole-file coverage is 50.65%, not complete coverage. |
| Repository lint/format/secret checks | Pinned repository pre-commit hooks over all intentional source/test paths; `git diff --check`; syntax compilation | Pass. Final report/plan checked separately. |
| Broad stable workflow / global coverage | Exact Makefile-style importlib/coverage collection command on baseline and repaired image | Both fail with the same 20 collection import errors. No full stable-suite/global 70% coverage success is claimed. |
| Final diff review | Fresh separate read-only reviewer, all three production diffs and caller contracts; subsequent fixture-only review | No reproducible blocking finding. Reviewer inspected receipts, did not independently rerun them. |

Broad tests at `124b6ca` used image `sha256:95f20822505431dc6f35d7e6f03027f0c1e05cd61987fd0173b49e538c2ed540` and tracked manifest `22a98144ec7574678cf6c7050bb032e325ded98e02051274f2eb31f3c4c3a853`. Final fixture tests, coverage and native replay used `f8e21a484` image `sha256:dec99dd6ed5e035babcde22a9e1760132f030909efd1640553712e79b86ee9f9`, manifest `89c438ff803fb4fa5ca6b3367cbf93552133bef902f050bbd0bc4b60ff619a8b`. Git comparison proves only the paper fixture changed between these executable trees; all production and broad-suite target bytes are identical.

Environment: Python 3.13.15, Pydantic 2.13.5, SQLAlchemy 2.0.52, aiohttp 3.13.5, pytest 9.1.1, coverage 7.16.0. Offline images overlay all tracked Python on a documented compiled environment; no `.pyx/.pxd` ABI change. A clean native compilation, alternate architecture/Python matrix and unchanged Rust job were not executed. Hosted Actions are unavailable under the budget policy; local results are separate from hosted status.

All application checks used `docker run --network none --read-only`, bounded CPU/memory, temporary writable logs/test data and no live credential/config/socket mount. Socket connect/DNS writes are explicitly forbidden in the paper fixture. Native backup replay alone mounts immutable read-only copies. Builds copy tracked source rather than live configuration. No executable startup with production credentials was attempted.

### Reproducible commands

These run inside the sanitized compiled environment with `PYTHONPATH=/home/hummingbot`; use the isolation above, not a production container:

```sh
python -m pytest test/test_hl_rsi_entry_control.py test/meridian_v3 test/test_ok_rsi_admission_safety.py test/test_ok_rsi_dust_inventory.py test/test_ok_rsi_entry_boundary_regressions.py test/hummingbot/strategy_v2/executors/test_order_accounting.py -q -rs --tb=short -p no:cacheprovider
python -m pytest test/hummingbot/connector/exchange/paper_trade/test_terminal_dust_roundtrip.py -q --tb=short --log-cli-level=INFO -p no:cacheprovider
python -m pytest test/test_ok_rsi_candle_aggregation.py -q -p no:cacheprovider
git diff --name-only --diff-filter=ACMR -z 479b2d34b214573f82cb51623b99201f56c19469 HEAD | xargs -0 pre-commit run --files
git diff --check 479b2d34b214573f82cb51623b99201f56c19469
```

Exact Docker argv, image IDs, UTC start/end, exit codes, workflow targets, baseline failures, coverage configuration/XML, source manifests and private replay arithmetic are retained locally under `~/.local/share/rsibot/validation/capital-audit-20261003T085300Z`. The sanitized report, command manifest and archived task plan are committed; private replay artifacts remain local. Expected adversarial/fault-injection logs are distinct from successful paper startup/lifecycle logs. The accounting run's inherited coverage configuration emitted an include-ignored harness warning; the separate scoped run fixes coverage measurement without suppressing application errors. Initial no-exec Arrow and coverage pre-import harness failures are preserved; neither is counted as a successful application check.

## Production observation — 10:53 UTC

The existing authenticated API GET returned current, verified native owner reports (ages .60–2.00 seconds). All 13 current services ran with zero restart counts and no OOM; configured health probes passed, while V3/ingress have no probe. These facts do not establish trading safety.

| Owner | Current operator permission | Observed readiness and outstanding qualification |
| --- | --- | --- |
| V1 | All five entry pause flags false | ETH reports `exposure_unavailable` on old live source. SOL/BNB cached gates ready; cached status is not a future fill promise. BNB ownership and two active SOL executor continuity must pass before a restart. No daily-loss policy configured. |
| V2 | All five entry pause flags false | ETH/SOL operator-pause labels are historical cached gate results, not current pause flags. Configured daily-loss limit is 50 quote. This task changed no V2 settings or runtime. New venue/order-book/funding readiness was not fully requalified here. |
| V3 | Entries configured enabled, pause false | Native `NATIVE_INVENTORY_MISMATCH:BTC-USDC`, target `ENTRY_HOLD`. Repair is not deployed. Source/recipe migration, effective fence, BNB continuity and durable risk restoration remain required; no daily-loss policy configured. |

Observed since 08:45:38 UTC: V1 had two recovered candle-staleness episodes (09:35 and 10:22), with all five feeds available again by 10:22:28; one price timeout at 10:21:54. V2 had three price timeouts (09:14:12, 10:12:09, 10:22:13). No ERROR/Traceback in this stdout window; other services had no WARNING/ERROR. Native file tails were also inspected, with the same V1/V2 issues; their bounded tail is not an exhaustive historical archive. V3's quiet native log does not imply a stopped loop: its native report continues advancing and exposes its actual entry hold. Transport root cause is unresolved; no speculative logging suppression/restart was used. Telegram and its old host supervisor remain intentionally retired.

APFS Data is about 98% used (~24 GiB free); only this task's disposable image-export archive/container was removed. No production image, volume, research data, backup or user worktree was pruned. No direct current book/fee/funding/venue readiness guarantee follows from these observations.

## Deployment and rollback prerequisites

1. Keep production untouched until the guarded procedure qualifies exact owner/native/venue state. Earlier narrow candidate images are superseded by this complete repair; this audit image is a test artifact, not a qualified live replacement.
2. V1: reconcile the unresolved BNB owner record through native evidence; preserve both active SOL executors and protective order continuity. Use a current native backup and reviewed owner restart helper; a failed pre-stop gate must leave the current owner running.
3. V3: clone-qualify sealed source/recipe migration and exact checkpoint restoration, acknowledge an effective native entry fence, prove current BNB/funding/grant continuity and restore durable risk state. Do not reset seals/checkpoints to make the candidate start.
4. Obtain the operator's explicit V1/V3 daily-loss limits and qualify their durable history/loss restoration. Never infer limits, clear history or rely on an in-memory-only rail. The unanswered policy request is a specific blocker, not authorization.
5. Following separately approved rollout, verify image/source/mount identities, live reports, full-history exposure/snapshot conservation, protective exits, funding, fee/price/book freshness and current entry gates. Do not submit a test trade or promise a fill.
6. Rollback must preserve recorder/fill continuity; do not restore a stale database after orders may have occurred. A post-stop uncertainty keeps the affected owner fenced/stopped under the reviewed procedure with evidence retained. Source PR rollback alone does not reverse production state.

Source publication can proceed as a reviewed **draft PR**. No merge/deployment is part of this audit. Outstanding live migration/risk/continuity gates, broad-suite collection and unknown transport cause are explicitly unverified.
