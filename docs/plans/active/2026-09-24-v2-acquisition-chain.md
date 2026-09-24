# V2 bounded acquisition-chain repair

Objective: reconcile child execution, shared funding and planned recovery around one bounded parent acquisition; preserve legacy singleton behavior. Reference: current master e302d244e, actual running V2 image sha256:b1d0865fc9405631522e91d01ae5a31041f002da8be8bd23902e339c45b9062c, and read-only BTC native terminal receipt from 24 September. Authority: live-adjacent repository implementation and network-disabled tests only. Forbidden: live engine restart, DB mutation, order submission, wallet edits, funding bypass, config activation. Owning worktree feat/andrzej_v2_acquisition_chain; shared dirty repos preserved. Rollback source commits only; never roll back native SQLite across real fills.

Acceptance: reproduce stale child-cancel false stop, replacement-chain funding rejection and recovery singleton restriction first. Test zero-fill predecessor plus two replacement fills/base fees and later XRP admission; partial-before-cancel no replacement; duplicate/delayed old cancel, async old REST completion; parent stop never reopens; authorization gaps retain budget; unsupported crash phase holds without order; completed sealed chain restores exact evidence/fees/trailing without fresh BUY. All acceptance claims distinguish source unit, native integration and deployment.

Working hypotheses (ordered): (1) executor-wide cancellation timestamp leaks from predecessor into child and inherited reconcile latches stop; (2) shared funding's literal single_attempt cardinality rejects allowed down-only repair; (3) planned resume independently assumes singleton and zero reprices. Market-data health wording is a separate path discrepancy; do not loosen freshness.

Three independent work streams: parent owns execution child-state and contract emission, chain_funding owns funding/recipe plus shared validation after schema agreement, quant_backend_inventory owns recovery validation after schema agreement. Workers must not delegate or touch live state.

Proposed contract: explicit versioned V2 opt-in, legacy single_attempt unchanged. Parent authorization conserves gross base, not fee-net amount. Child predecessor is terminally proven zero-fill before replacement, bounded count and non-increasing prices. Parent membership closes only with no pending/uncertain successor; authentic complete proofs required to release commitments. Existing old-config incidents require a distinct audited compatibility transition, never deleting predecessor history.

Validation: network-disabled disposable containers based on running image with scoped candidate module overlays; original legacy/down-only tests, native funding tests, planned-resume tests and combined scenario tests. No production activation from passing tests.

## Evidence collected during implementation

- Read-only byte comparison: the running V2 image matched baseline e302d244e for the six executor/funding/recovery/data-provider modules initially investigated. The shared and nested dirty working trees were not used as deployment sources.
- Red native regression reproduced a false `exchange_canceled` on a partially filled replacement carrying its predecessor's cancellation timestamp. After order-scoped state and generation handling: eight focused child-state tests pass, including two base-fee fills, delayed/duplicate cancel, obsolete async task, ambiguous submission and contradictory proof.
- Native execution/down-only/freshness/current chain selection: 199 tests and 15 subtests passed at the intermediate candidate. Final frozen-candidate rerun is required after recovery/funding changes finish.
- Existing pause/trailing test failed identically in the unchanged running image because its MagicMock strategy was passed to the real recorder. Its fixture now supplies a successful recorder and asserts checkpoint invocation. Existing native checkpoint-failure coverage still checks no sell on persistence failure.
- Two new red health cases proved `get_rate_health()` reported ready while the managed ticker reader rejected unavailable or 66-second-old rates. Health now uses that reader's existing 65-second deadline and failed-refresh state; legacy cached conversion retains its existing 120-second contract. Negative monotonic ages are rejected, not given a longer TTL. Focused refresh suite passed; final rerun pending.
- Extended provider/health selection: 71 passed, four subtests passed, one pre-existing fixture failure. The failing `test_okx_keeps_validated_source_timestamp_without_changing_price` creates an OKX instance without the running image's `_spot_ticker_lock`; it fails identically without candidate overlays. This test is not deployment evidence and was not silently suppressed.
- Offline read-only actual BTC executor row passed the native legacy attestation with two children and gross base 0.00047461. This is not fresh venue evidence, a prepared manifest, a funding release, or restart authority.

## Migration sequencing

The new optional contract changes neither legacy singleton behavior nor the original funded configuration hash. Existing BTC is not automatically opted in. Explicit quiescent funding transition may attest the exact historical one-repair native facts, retaining the original claim unreleased. Fresh venue proof for **every** child is required by the subsequent explicitly opted-in planned recovery. A manifest-bound compatibility marker then bridges original and sealed resume configurations. Any missing or changed proof holds. Repeated handovers and subsequent genuine SELL snapshots must preserve this bridge.

Reproducible native runner: `python scripts/validation/run_acquisition_chain_native.py --image <immutable-image-id> -- <focused-pytest-paths> -q`. It prints module hashes, overlays only listed modules and candidate tests read-only, disables networking, and mounts no live state/configuration.


## Final source behavior and review

- Filled active replacement chains now collect terminal entry receipts while the executor remains RUNNING. Previously this happened only on shutdown, so otherwise-correct replacement fills could leave funding unreconciled indefinitely.
- Shared funding keeps the parent reserve across authorized-but-unsubmitted successors. Closed-chain release corroborates every child's native snapshot, exact fills and quote fees, recorder identity/status and terminal proof. No fee-net quantity is reused for another BUY.
- Planned recovery uses explicit executor IDs for the legacy transition (`--legacy-post-only-repair-executor-id`, repeatable). The flag does not apply to other held positions. Original preflight and stopped digests remain sealed; terminal proof enrichment uses copies. Armed trailing triggers and movement history cannot regress between preflight and stop.
- Original manifest provenance survives subsequent claimed handovers; a later genuine SELL snapshot does not erase or redefine the bound BUY history.
- Native `TradeUpdate.from_json` now preserves the recorded maker/taker boolean. Missing legacy fields retain the former True default; malformed nonboolean values raise. The before/stop native integration reproduced False becoming True before this change, without changed amounts or fees.
- Independent review found no remaining reproducible high-severity code blocker in the reviewed patch. This is a source review, not live cutover qualification.

## Remaining qualification boundaries

The candidate does not establish automatic crash recovery at every possible write boundary. In-process unknown submission, stale asynchronous completion, delayed events, partial fills and contradictory proof are tested, but killing/rebooting a process between venue acceptance and durable child-ID persistence is not exercised. Such a state must retain the commitment and hold for reconciliation; it cannot justify another BUY or emergency SELL. A full process-crash matrix remains required before claiming that release bar.

The mixed two-position prepare selection regression uses real routing with mocked downstream validators. Separate native SQLite tests cover completed-chain prepare/claim/hydration and explicit legacy prepare/pending revalidation/claim. One combined mixed-position real-DB/venue integration remains unobserved.

No live OKX evidence was fetched, no live funding transition or manifest claim was applied, and no engine image was deployed/restarted. Before a separately authorized cutover: capture current read-only venue receipts for every child, reconcile native records and preserved inventory, take a WAL-consistent backup, review the explicit legacy funding proposal (still unreleased), prepare the selected recovery manifest, qualify the exact image and fresh cutover receipt, then observe ownership/trailing/funding after the controlled restart. Any disagreement holds; never roll back the DB across fills.

Rollback of this local candidate is source-only. Once native state has been migrated, reverting binaries also needs compatibility review of claimed manifests and funding identities; it is not safe to delete those records to fit older code.


## Frozen validation results

All commands use `python scripts/validation/run_acquisition_chain_native.py --image sha256:b1d0865fc9405631522e91d01ae5a31041f002da8be8bd23902e339c45b9062c --` followed by the arguments below. Logs and printed source hashes are in `docs/evidence/2026-09-24-v2-acquisition-chain/`. Counts overlap where the focused acquisition suite is included; do not sum them as unique tests.

| Arguments | Observed result |
| --- | --- |
| `test/test_ok_rsi_down_only_entry_execution.py test/hummingbot/strategy_v2/executors/position_executor/test_position_executor.py test/hummingbot/data_feed/test_market_data_provider.py test/hummingbot/data_feed/test_market_data_refresh_resilience.py test/acquisition_chain_v2 -q` | 270 passed, 15 subtests; before final rejected-recorder guard, which is included in the next group |
| `test/modular_spot/test_native_funding.py test/modular_spot/test_funding_assessment.py test/modular_spot/test_funding_transition.py test/modular_spot/test_strategy_funding_scope.py test/hummingbot/strategy_v2/paper/test_paper_funding.py test/modular_spot/test_native_composition.py test/modular_spot/test_native_composition_recovery.py test/acquisition_chain_v2 -q` | 105 passed, 14 subtests on final source |
| `test/hummingbot/strategy_v2/executors/test_planned_position_resume.py test/modular_spot/test_down_only_planned_resume.py test/crash_recovery_v2/test_funding.py -q` | 48 passed, 5 subtests; the two crash-recovery strategy tests are namespace isolation, not process-kill coverage |
| `test/hummingbot/core/data_type/test_in_flight_order.py -q` | 24 passed |

Static checks: `flake8` and `isort --check-only` on every changed/new Python file passed; `git diff --check` passed. Native warnings were dependency deprecations (hmmlearn/NumPy and old asyncio test initialization).

An earlier concurrent source edit correctly invalidated the paper runtime identity midway through one restart test (66 passed, one subtest failed). That run is replaced by the final frozen 105-test run. The old unittest order test class requires a separate process after async test suites because its class-level `asyncio.get_event_loop()` initialization otherwise finds a closed/no loop; separate 24-test validation passed without changing the fixture. The pre-existing native market-health connector fixture failure recorded above remains outside the passing groups.

Final state: local candidate implemented, regression tested and independently reviewed. No push/PR/merge, deployment, engine restart or live DB write. Read-only final Docker inspection still showed the original execution image and `2026-09-24T06:21:32.001678007Z` start. Process-kill crash matrix and live cutover evidence remain open; this plan stays active for that qualification.


## Edge-case and restart campaign (24 September, second pass)

Objective: test the repair beyond unit tests from every side and prove restart preservation. Authority unchanged: live-adjacent source and network-disabled containers only; no live restart, DB write, order, push or deploy. Image under test remains `sha256:b1d0865fc9405631522e91d01ae5a31041f002da8be8bd23902e339c45b9062c` with candidate modules overlaid read-only.

### New test surfaces (all under `test/acquisition_chain_v2/`)

- `test_paper_lifecycle_restart.py`: whole lifecycle in the offline native paper session (real orchestrator, recorder, funding store, controllers, executors, fake venue): zero-fill cancel, post-only repair, two base-fee fills, trailing arm and one exact SELL; clean restart after the closed chain; clean stop mid-chain (replacement resting, and partially filled) with funding release and re-admission; crash restarts from consistent SQLite snapshots at six distinct chain phases.
- `crash_kill_driver.py` plus `scripts/validation/run_crash_kill_matrix.py`: real process kills (`os._exit`) in disposable containers at three boundaries, each followed by a fresh-process restore and verification.
- `test_chain_edges.py`: three-child downward chain, reprice budget exhaustion, partial-then-cancel closure, duplicate fill event, foreign cancel, late predecessor cancel after closure, ten validator rejections, JSON-native persistence at every phase, real recorder round trip, legacy single-attempt regression.
- `test_funding_edges.py`: three-child release, full reserve during a pending successor with a competing admission, changed proofs after release, gross-fill release with base fees, legacy two- and three-proof holds, tampered rows without `acquisition_chain`.
- `test_restart_preservation.py`: orchestrator round trip for two- and three-child chains with armed trailing (orders, fills, fees, chain, reconciliation, trailing state, exact SELL, no BUY), persist idempotence after resume, second handover, seven prepare-time tamper rejections with database bytes unchanged, seven both-capture chain-damage rejections, unfinished chain rejection and plain-restart hold, duplicate fill and late predecessor fill after resume.

### Defects found and fixed in this pass

1. Planned resume replaced the owner's terminal reason with the marker `planned_resume_sealed_entry` (`position_executor.py` hydrate and the down-only override). The original stop reason now survives via `entry_terminal_reason` in the manifest position and the archived lifecycle; the marker is recorded as `resume_sealed_entry: True`.
2. `prepare` venue-checked the evidence's terminal proof set but sealed the stopped row's copy without binding the two, so a reordered persisted set could be sealed unchecked. `prepare` now requires the supplied set to equal the sealed set child for child.

No other source change. Legacy single-attempt behaviour verified unchanged.

### Restart facts established

- Clean stop (`early_stop(keep_position=True)`), then prepare, claim and hydrate restores a closed chain exactly: both children with states and fills, fees, terminal proofs, chain, stop reason, trailing trigger, move count and timestamps; no BUY is placed and the exit sells the exact net amount.
- A crash at any observed phase (initial resting, cancel reconciliation pending, child authorized with no order, replacement resting, partial fill, closed chain managing) leaves the persisted row intact, restores venue orders and fills, latches `recovery_required`, holds readiness and never creates a BUY. A mid-step kill also leaves the paper step receipt, which holds the session until explicit reconciliation.
- Operational boundary, unchanged by design: a crashed owner leaves the executor row RUNNING, and `prepare` refuses any database with unfinished executors. Automatic recreation stays unsupported (lifecycle repair runbook). Resuming trailing management after a crash therefore still needs the documented authorized reconciliation; there is no sealing tool for a crashed row.
- Partially filled chains are not planned-resume candidates (only zero-fill predecessors then one full fill); their inventory persists as a held bag and the strategy's average-entry gap governs re-entry.

### Frozen results (candidate source, network-disabled image)

| Arguments | Observed |
| --- | --- |
| `test/acquisition_chain_v2 test/modular_spot/test_down_only_planned_resume.py test/hummingbot/strategy_v2/executors/test_planned_position_resume.py -q` | 141 passed, 5 subtests (`edge-campaign-final.log`) |
| `test/acquisition_chain_v2/test_paper_lifecycle_restart.py -q` | 5 passed (`paper-lifecycle-restart.log`) |
| `scripts/validation/run_crash_kill_matrix.py --image <id>` | 3 phases killed with exit 137, 3 verifies ok, PASS (`process-kill-matrix.log`) |
| execution, data-feed, funding, composition and crash-recovery groups (see `regression-groups-final.log`) | 300 passed, 29 subtests |
| `test/hummingbot/core/data_type/test_in_flight_order.py -q` | 24 passed |

Static checks: `flake8` and `isort --check-only` clean on the three changed source files and six new test/script files; `git diff --check` clean.

Still open: live cutover evidence (fresh venue receipts for every child of the held BTC position, WAL-consistent backup, legacy funding transition review, manifest preparation, image qualification, controlled restart with the trailing high-water reset accepted). Hold alerting (push a Telegram alert on `entry_admission` blocked or `reconciliation_required`) remains a recommended follow-up outside this candidate.


## Live cutover, 24 September 13:42-13:44 UTC (executed)

Authorized by the user after a parallel session had stopped `execution-rsi` at 12:43:35 UTC with a plain SIGTERM (exit 143, no native strict stop, three RUNNING rows, no open venue orders). Coordination: that session held all live actions from 12:5x UTC and deferred the restart to this one.

Source: hummingbot master `ac5571987` (this branch merged with PR #84; chain contract kept, legacy single-attempt sets follow the single-filled-order rule), root main `ff0935dd` then `ea23f725` (chain-aware `recovery_reconcile.py` with `--legacy-transition-executor-id` and an OKX fills-history fallback for orders older than three days, `v2_recovery.py` forwarding legacy ids, `seal_stopped_owner.py`). Image `sha256:7329dedbe38bbf7b527d587fde0f00837580b4a4da7a11786eb012c615277f82` (base `b1d0865f…` plus the qualified modules and tools; engine seal `78536457…`; 193 in-image tests, 443+34 engine tests on the same module set, kill matrix pass). The seal tool used live carried one later fix (event loop binding, `ea23f725`) mounted over the image copy; it is not part of the launch identity.

Sequence and receipts (`~/.local/share/rsibot-stack-v2/recovery/v2-chain-20260924/receipts/`): precheck and backups while running (12:41-12:42Z); dead-owner seal of ETH `7YsZ…`, BTC `59Np…`, BTC chain `AUDG…` with GET-only venue proof against the 12:43:30Z preflight whose table content matched the dead database exactly (13:42:20Z); upgrade receipt; venue evidence `EVIDENCE_COMPLETE` (13:42:55Z); funding transition `c0364c63… -> 69be04e7…` revision 23 (13:43:08Z); fresh evidence (13:43:33Z); prepare, manifest `95a45bc1…` (13:43:40Z); scoped `up -d --no-deps --pull never execution-rsi` (13:43:40Z). Manifest claimed at 13:44:09Z. All three positions resumed as live executors with trailing waiting; AUDG resumed under the chain contract through the legacy transition with both children; BNB and SOL held bags recovered from the ledger behind their existing held-recovery gate; XRP gate `ready`; no orders placed by the restart.

Dead-database backup: `dead-backup/rsi_modular_v2.sqlite` (pre-seal), `pre-cutover-backup/` (running, 12:42Z), `stopped-backup/` unused (owner was not stopped by this session).
