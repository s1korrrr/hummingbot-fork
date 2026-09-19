# Modular spot programs and shared data — execution plan

> Living ExecPlan. On an authorized implementation run, resume the first eligible unchecked milestone, use fresh review checkpoints, and update progress, decisions, evidence and blockers here. A separate actor is required for an independent audit. Use the available executing-plans or subagent-driven-development workflow; do not start another task ledger.

**Goal:** Validate the isolated native modular foundation for reusable OK RSI/RSI V5 workflows. Preserve running controllers and native Hummingbot ownership; prove strategy parity before treating a modular policy as a replacement. Existing fixed-symbol, sell-only and screening code remains bounded integration scope.

**Architecture:** Hummingbot owns orders, fills, accounting, executors and lifecycle. Private policies compose observations, opportunities, acquisition, exits and risk. One read-only data publisher serves compatible local replicas; universe selection, capital permission and position management remain distinct.

**Tech stack:** The actual owning Hummingbot runtime and its installed Python/Pydantic, pandas/numpy, SQLAlchemy and aiohttp dependencies. No automatic dependency upgrade, new execution engine or additional broker.

**Spec:** [SPEC.md](SPEC.md). **PR:** [hummingbot#55](https://github.com/s1korrrr/hummingbot/pull/55). **Branch:** `feat/andrzej_isolated_modular_spot`.

## 2026-09-19 authorized validation and repair

- Objective: verify the preserved M0-M7 candidate, repair concrete modular-family defects, prove native OKX WebSocket candle collection and local reader availability where public connectivity permits, and reconcile this plan with the implementation receipt.
- Scope: existing PR #55 task branch and modular controller, private components, tests and documentation. User intent is reusable modular policies for working OK RSI/RSI V5 workflows; this candidate must not claim semantic parity without comparison.
- Authority: repository-process-only. Public unauthenticated market-data subscriptions and temporary loopback verification only. No authenticated exchange calls, orders, live owner enrollment, stack writes/restarts/deployments, component pins, backtests, commits, pushes or merges.
- Preservation: prior dirty candidate and docs archived with per-file SHA-256 in root `.audit/pr55-validation-20260919/before.tar.gz` and `before-sha256.json`; root/nested statuses and live container identity recorded. Continue in existing task branch `feat/andrzej_isolated_modular_spot`; do not switch the dirty worktree.
- Completion bar: fresh combined native tests without skips, targeted failure regressions, source review, isolated public feed evidence or precise external blocker, truthful strategy parity and operational handoff.
- Verification: native conda Python with `PYTHONPATH` bound to this worktree; `pytest -q test/modular_spot`; native PositionExecutor, OrderExecutor and orchestrator suites; focused native OKX candle tests; scoped lint; `git diff --check`; compare container IDs/images/start times before/after.
- Rollback: restore only this validation's touched files from the preserved archive after inspecting its incremental diff. Do not reset or discard the pre-existing uncommitted candidate. Stop only temporary test processes.
- Independent streams: native lifecycle audit; OK RSI/RSI V5 composition and spec comparison; fresh native baseline verification. Parent owns candle path, repairs, integration and handoff.

### Validation milestones

- [x] V1: preserve current candidate and collect fresh baseline/reviews.
- [x] V2: reproduce and repair verified scoped defects with regression checks.
- [x] V3: run isolated native public WebSocket to shared-reader smoke.
- [x] V4: synchronize specification/plan, record final evidence and next actions.

## 2026-09-19 full-USDC streaming and companion PR continuation

The subsequent user-approved Rust producer implementation is tracked in [the completed Rust candle plan](../../plans/archive/2026-09-19-rust-candle-service.md), with source-bound results in [RUST_CANDLE_VALIDATION.md](RUST_CANDLE_VALIDATION.md). Historical HTTP/default transport milestones below retain their original scope.

- User priority: test every currently listed live OKX USDC spot instrument at1m concurrently; measure freshness, delivery delay, gaps, errors and recovery. Public listing is observation scope, not account eligibility.
- User product requirement: preserve all existing OK RSI/RSI V5 capabilities as reusable policies and verify Hummingbot, Hummingbot API and rsibot-stack contracts. Inspect companion PR57 at068702141f1b and root PR188 atacd1f83; do not assume library-only delivery is native integration.
- Authority remains repository-process-only with isolated unauthenticated public reads. No live-stack writes/restarts, account calls, orders, activation, deployment, research economics, pins, publication or merge.
- Preserve preceding repaired candidate in root `.audit/pr55-universe-20260919/before.tar.gz`; existing dirty task branch retained.
- Catalogue: freeze public instrument response bytes, filter live USDC spot. Initial count294.
- Test contract: pacing below official3 connections/second/IP; bounded small socket count and subscription messages; TLS enabled, public endpoints only, text ping/pong and bounded reconnect. Do not create294 sockets at once.
- Baseline design limitation: current native per-pair collector has neither IP-wide connection pacing nor OKX text-ping watchdog. First benchmark uses native WSAssistant plus OKX parser on bounded multiplexed subscriptions; this is venue/transport evidence, not proof that the current production collector scales. Record this distinction.
- Measure candle-close-to-receipt with clock-offset/RTT uncertainty, local parse/publish/read durations, coverage per completed minute, duplicates/corrections/gaps, first-ready time, errors and expected versus unexpected reconnects. Raw candle payload has no exchange-send timestamp, so do not label close delay as network latency.
- Milestones: U1 frozen catalogue/docs/preflight; U2 all-pair public benchmark and targeted recovery; U3 reproduce/fix measured reader/publisher or PR compatibility defects locally; U4 native companion PR tests and owner/API/stack gap mapping; U5 independent review and source-bound handoff.
- Validation: deterministic metric/watchdog regressions, native tests for changed code, scoped hooks, bounded public soak, source manifests, preservation comparison. Any result is bounded observed evidence, never a promise of zero future failures.
- Rollback: stop only benchmark-owned tasks/sockets and revert only this continuation's patch using the preserved candidate.

## Historical immediate action — current source differed from the attachment

Historical instruction (completed by M0-M8 and freshly verified below): prepare the complete owner checkout, refresh the PR head and run the native baseline, then reconcile the current helper implementation with the attachment. **Do not blindly apply `pr55_shared_data_universe.patch` or reset the branch to its old head.**

During documentation publication the branch gained code checkpoint `b0fd5ef865fe88b87a9da8b8b10e3025d97576c2`, adding nine files and modifying `planner.py` relative to the original `6472dfece06ca71d9ebc6b887dda341ccdf5befb`. Several paths overlap the attachment but have different schemas. These changes were preserved; this documentation task does not claim authorship, native validation or integration completion for them.

The first documentation commits are `6ec32c4e2d5179b88a8f9f3cc711ad592e7797cb` and `51afd3b46744e1be3e83eb3d131031ba5f7cf53c`. Their source parent is the newer checkpoint, not the original PR head. This plan's current-source map supersedes its earlier attachment-first roadmap. Refresh again before execution; the branch can advance further.

## Scope, authority and completion bar

The original task published documentation only. The 2026-09-18 M0-M8 implementation and 2026-09-19 local validation/repair are authorized continuations, bounded by the current validation contract above. No account/runtime changes, existing-controller edits, activation, economic backtests, pin updates, deploy or merge are authorized.

When implementation is explicitly resumed, keep code in `controllers/generic/modular_spot.py` and `controllers/generic/modular_spot_core/`, tests in `test/modular_spot/`, and task documentation here. Add another native-discoverable entry point for this family only if composition requires it and its exact path is reviewed. Reuse pure OK RSI/RSI V5 policies with parity tests; do not inherit whole legacy controllers or mutate running owners. No global executor-map change or alternate fill/PnL ledger.

Shared-engine, API, stack, account-topology and legacy-owner changes are separate owner work, not implicit permission to expand PR #55. Preserve the distinct execution main/SUI owners. Keep default execution disabled; spot sell-only reduces owned base rather than opening a short. Never introduce below-floor market fallback or reset acquisition cost.

Source integration, native correctness, data quality, operational admission, economic validity and activation are separate gates. A helper implementation or unit-test pass cannot close the whole program. Use explicit statuses: observed-source, attachment-candidate, specified, passed, failed, not-run and blocked:external.

## Source, artifacts and evidence

| Checkpoint | Meaning |
|---|---|
| Native base `4df9ff3158a405b71d28b1c07446cfc7864a19fa` | Native runtime underlying the initial candidate |
| Original PR code `6472dfece06ca71d9ebc6b887dda341ccdf5befb` | Single-pair modular BUY controller, private policies and local PositionExecutor specialization |
| Newer source `b0fd5ef865fe88b87a9da8b8b10e3025d97576c2` | Overlapping data/profile/budget/selection helpers and planner change; source inspected, no new native validation established here |
| Current documentation | Target spec, ordered execution plan and source reconciliation; not additional trading functionality |

The original plan is retained in Git at [the original implementation parent](https://github.com/s1korrrr/hummingbot/blob/6472dfece06ca71d9ebc6b887dda341ccdf5befb/docs/controllers/modular_spot/PLAN.md). The attachment-first plan revision is retained at `51afd3b46744e1be3e83eb3d131031ba5f7cf53c`. Do not follow either as the current file inventory.

| Conversation artifact | SHA-256 |
|---|---|
| `pr55_shared_data_update.zip` | `4585bd99764efbe951a375a6c95cc8ea4e3b01cdc873d837ab104cb525ff6134` |
| `pr55_shared_data_universe.patch` | `a30dede9a1c0ad3a47be4936c4fecb5bdd55662faf2343a05c6d9f8485e92523` |
| `pr55_shared_data_validation.md` | `3be487817fe051027d2e3b5ffd00078db423cfe92e17e0bbb5239d8997dc31b0` |

Artifact hashes and bundle payload hashes were checked while preparing this handoff. The exact patch is a 16-file partial-reconstruction candidate, not the newer published variant. Its receipt reports 60 new focused tests passed and one entire native module skipped. The original PR separately reports 61 original tests passed and one native module skipped. Do not sum these counts, call them a new combined result, or attribute them to the newer checkpoint. The attachment's native-import log reports `ModuleNotFoundError: No module named 'hummingbot'`.

Attachments are not repository paths and may not be mounted in the next environment. Obtain the original handoff from the operator and verify its digest. If missing, continue source-backed tasks, record the absent comparison and implement requirements from SPEC with new tests; never claim byte-equivalence or successful patch integration without the artifact.

### Progress

- [x] Inspect original PR state, scoped plan, bundle interfaces, manifests, logs and artifact identities.
- [x] Publish the target spec and initial execution plan without changing code.
- [x] Detect the newer source checkpoint, preserve its code and revise the plan for overlapping implementations.
- [x] M0: Bind the actual owner checkout/runtime and establish the native baseline. Worktree `.worktrees/hummingbot-modular-spot-pr55`; HB_PYTHON conda hummingbot; native imports from worktree.
- [x] M1: Reconcile current source versus attachment into one coherent pipeline and run combined tests. Attachment not applied. HTTP snapshot polling. review-m1-fix PASS.
- [x] M2: Prove native admission, exits, partial fills, accounting and restart semantics. Planned-resume hold. review-m2-fix PASS.
- [x] M3: Prove shared data, feature provenance, dependency readiness and bounded load. Bounded recovery; hub retain. review-m3-fix PASS.
- [x] M4: Wire fixed-symbol programs through actual native-discoverable configurations. Selection does not grant capital. review-m4 PASS.
- [x] M5: Prove aggregate capital and slot enforcement for the admitted account topology. 2026-09-18 review-m5-fix Spec PASS / quality APPROVED. Process-local BudgetStore; reserved pair admitted; watchlist not a slot. Not a cross-process allocator.
- [x] M6: Implement external inventory handoff and genuine sell-only native reduction. 2026-09-18 review-m6-fix Spec PASS / quality APPROVED. OrderExecutor LIMIT SELL CLOSE through create_executor; planned-resume hold; no live SUI/main handoff.
- [x] M7: Wire broad screening to bounded native multi-symbol acquisition. 2026-09-19 review-m7-fix Spec PASS / quality APPROVED. Combined 230; default coverage keeps peer scores; closed-bar; no hot-enroll.
- [x] M8: Complete independent source review and separately gated release/research handoffs. 2026-09-19 review-m8 Spec PASS / quality APPROVED. Combined 230, native executors 182, skips 0. Operational/economic/activation/merge/deploy HOLD. Product uncommitted.

Append exact source/runtime, command, result and limitation when changing a box. A native skip or an unconnected helper keeps its milestone open.

### Decision log and discoveries

- 2026-09-18: The new controller remains independent; the historical OK RSI extraction proposal is superseded.
- 2026-09-18: Retain native per-tranche position accounting initially, not a synthetic basket-average ledger.
- 2026-09-18: First delivery is confirmed one-minute OKX spot over bounded snapshots. MQTT, intrabar inputs and a full point-in-time archive are distinct additions, not implied support.
- 2026-09-18: Observation, acquisition and management universes are separate; held, pending and recovery obligations remain subscribed and counted.
- 2026-09-18: Newer branch code arrived during docs publication. Prefer reconciling the current files and porting missing safeguards, not installing two incompatible data pipelines.
- 2026-09-18: Existing planned-resume behavior can bypass local executor selection. Do not remove refusal without proving native restoration; a required shared-engine repair is separate work.
- 2026-09-18: `program_budget.py` explicitly scopes reservations to one recorder/strategy configuration. It is not proof of cross-process account allocation.
- 2026-09-18: `profiles.py` emits acquisition/budget/data/universe fields while the original entry-point schema has not been changed in this checkpoint's delta. `native_bindings.py` expects additional report-age hooks. These are source-level integration concerns, not runtime failures reproduced here; M0/M1 must verify the actual wiring.

## Current-source reconciliation map

All paths below are under `controllers/generic/modular_spot_core/`.

| Newer branch path | Overlapping attachment path/contract | Required reconciliation |
|---|---|---|
| `candle_data.py` (`Dataset`, `CandleSnapshot`, `Replica`, `SnapshotHub`) | `candle_contracts.py`, `candle_reader.py`, `candle_store.py`, `candle_service.py` | Choose one dataset/finality/revision/history contract; preserve producer/consumer lifecycle, durable-state requirements and field availability |
| `candle_transport.py` | Same path, different implementation | Compare origins, credentials, permissions, limits, heartbeat/304 and cleanup semantics; do not overwrite |
| `data_reader.py` | `CandleViewProvider`/client and `shared_candles` integration | One candle-only provider view; suppress duplicate subscriptions and retain native execution access |
| `feature_cache.py` | Same path, richer attachment feature specification | Port required implementation/parameter/initialization/window provenance and freshness checks rather than accept name-only equivalence |
| `universe.py`, `selection_state.py` | Attachment `UniversePolicy/Evidence/Decision` and selector | Retain observation/entry/management separation, context, eligibility, pending inventory and deterministic selection |
| `profiles.py` (`ProgramProfile`, `build_profile`) | Earlier planned program compiler | Reuse and complete it; prove output validates against the actual controller configuration |
| `program_budget.py` (`BudgetPolicy`, `Reservation`, `BudgetStore`) | Earlier proposed capital adapter | Prove its stated single-recorder scope and integrate or separately resolve wider account authority |
| `native_bindings.py`, changed `planner.py` | Existing `runtime.py` and native controller seam | Verify constructor/config/report hooks, import path and admission wiring; do not assume additions are used |

The newer data schema permits intervals `1m`, `5m`, `15m`, `1h`, `4h`; the attachment has `1m`, `3m`, `5m`, `15m`, `30m`, `1h`. Their generic/native source vocabulary, snapshot models, token configuration and profile fields are not interchangeable. Do not silently union schemas or broaden venue support. Pin a reconciled schema version and capability matrix after native validation.

The published policy files remain `config.py`, `domain.py`, `signals.py`, `acquisition.py`, `portfolio.py`, `risk.py`, `inventory.py`, `trailing.py`, `state.py`, `planner.py`, `execution.py`, `runtime.py`. Reuse those boundaries. Do not create parallel `programs.py` or `capital.py` merely because the previous plan proposed them; current `profiles.py` and `program_budget.py` take precedence as inspection targets.

## M0 — Source and native baseline

**Owner/scope:** read-only Hummingbot checkout/runtime investigation, no account requests. Read current root/owning instructions, plans and relevant nested guidance. Run the user's local bootstrap only on the actual machine where it exists. Preserve unrelated dirty work using an isolated checkout rather than reset/stash/discard.

Record branch, HEAD, redacted origin, root pins and current PR comments. Compare the current head with both checkpoints above. Set `HB_PYTHON` to the actual native interpreter and `EVIDENCE_DIR` to an absolute dedicated existing evidence directory outside runtime state. Record installed versions; do not install arbitrary latest packages to obtain a pass.

```bash
set -euo pipefail
: "${HB_PYTHON:?Set the actual owning Hummingbot interpreter}"
: "${EVIDENCE_DIR:?Set an absolute dedicated evidence directory}"
git status --short
git branch --show-current
git rev-parse HEAD
"$HB_PYTHON" -c 'import sys; import hummingbot; from controllers.generic.modular_spot import ModularSpotController; from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor; print(sys.executable); print(hummingbot.__file__)'
"$HB_PYTHON" -m pytest -q test/modular_spot --junitxml="$EVIDENCE_DIR/m0-baseline.xml"
```

Inspect JUnit skips and collection errors explicitly. Record environment versus baseline/candidate failure with the actual evidence. Native imports and tests must use genuine native classes, not a fabricated package. **Accept:** current source/runtime identity and native baseline exist. On failure, preserve independent pure-policy progress, record the exact failing dependency/command and keep native-dependent gates open.

## M1 — Reconcile current code and attachment; no automatic apply

**Files:** current-source map, `modular_spot.py`, affected original core tests and the attachment's `test_shared_*.py`. No unrelated controller/shared-engine changes. **Output:** one coherent implementation with a versioned interface map and new combined validation receipt.

1. Inspect `git diff 6472dfece06ca71d9ebc6b887dda341ccdf5befb..HEAD -- controllers/generic/modular_spot_core` and the artifact patch. Bind the actual current head; neither checkpoint is permission to discard later work.
2. Verify the artifact checksum without applying it:

```bash
set -euo pipefail
: "${PATCH_FILE:?Set an absolute path to the original attachment patch}"
export PATCH_FILE
python3 - <<'PY'
import hashlib, os
from pathlib import Path
p = Path(os.environ['PATCH_FILE'])
if not p.is_absolute() or not p.is_file():
    raise SystemExit('An absolute existing patch file is required')
expected = 'a30dede9a1c0ad3a47be4936c4fecb5bdd55662faf2343a05c6d9f8485e92523'
if hashlib.sha256(p.read_bytes()).hexdigest() != expected:
    raise SystemExit('Artifact identity mismatch')
print('Artifact verified; not applied')
PY
```

3. Produce a path-by-path retained/ported/rejected-with-reason matrix. Prefer current branch paths when viable and port the attachment's missing tested safeguards into them. Do not install duplicate publishers, readers, ledgers, profile compilers or incompatible classes at one import path. Preserve original artifact evidence separately.
4. Add failing integration tests before repairs: `build_profile(ProgramProfile(...))` output must validate with `ModularSpotControllerConfig`; report-age hooks used by `NativeBindings` must exist with real fresh/stale behavior; planner/budget/data adapters must be actually connected. Unknown configuration keys must still be rejected, not silently ignored.
5. Port meaningful attachment regressions to the chosen schema. Update structural guards narrowly for intentionally added data-only behavior; do not remove ownership/no-orders guards to make tests pass. Validate shared-mode startup does not also initialize native duplicate candles.
6. Run the original plus reconciled new suite and actual repository lint/hooks on changed paths. Record a fresh combined result, including skips and native imports, rather than reuse either earlier count.

The old `apply_to_pr55.sh` intentionally requires exact HEAD `6472dfe...`; both documentation commits and actual source advances violate it. Do not rewind or generalize that guard. On a truly clean documentation-only descendant of the old code, verified `git apply --check --index` could establish applicability, but **the inspected newer source is not such a descendant**. Current execution requires reconciliation, not that recipe.

**Accept:** native-config and runtime seams work; one selected data/feature/universe contract remains; scoped combined tests and code review support it. The exact attachment is not called published merely because similar file names now exist.

## M2 — Native lifecycle, exit and recovery semantics

**Files:** `modular_spot.py`, `runtime.py`, `native_bindings.py`, `execution.py`, `inventory.py`, `state.py`; existing `test_native_contract.py`, plus planned `test_native_lifecycle.py` and `test_native_recovery.py`.

Characterize native discovery, config round-trip, default-disabled behavior, duplicate/unknown intent rejection, immutable policy identity and native event callbacks before repair. Then add failing partial-entry/cancel/late-fill, fee-correction, missing-receipt, repeated-start/stop, partial-exit, retained-dust and source/config-change cases using real native classes with disposable recorder state and no network/account calls.

Check per-tranche quantity and cost against native receipts, including base/quote fees and tick rounding. A signal cannot waive missing cost/fee evidence. Test the actual orchestrator planned-resume path, not a fake selector that always returns the specialization. Prove preservation of policy, ownership and trailing state or keep an explicit unsupported-resume hold and record the exact separate owner dependency.

Prove shared-candle failure blocks new dependent acquisitions without restarting the execution owner or manufacturing a loss sale. Existing native exits continue where their required native inputs remain valid.

```bash
"$HB_PYTHON" -m pytest -q test/modular_spot/test_native_contract.py test/modular_spot/test_native_lifecycle.py test/modular_spot/test_native_recovery.py
"$HB_PYTHON" -m pytest -q test/hummingbot/strategy_v2/executors/position_executor test/hummingbot/strategy_v2/executors/order_executor test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py
```

Create the two new test modules before running this command and verify existing paths in the actual checkout. **Accept:** native admission/accounting/failure behavior is demonstrated. A refusal alone does not prove fully recoverable production operation. Required shared-engine changes remain separately scoped.

## M3 — Shared data, features and bounded load

**Files:** reconciled data/transport/reader/cache/universe paths and controller integration; existing reconciled shared-data tests plus planned `test_shared_native_load.py`.

A concrete current-source characterization test is:

```python
from controllers.generic.modular_spot_core.candle_data import Dataset, SnapshotHub

def test_history_demands_share_one_dataset():
    small = Dataset(connector='okx', trading_pair='BTC-USDC', history=300)
    large = Dataset(connector='okx', trading_pair='BTC-USDC', history=1000)
    hub = SnapshotHub((small, large), factory=lambda spec: None, epoch='test')
    assert small.key == large.key
    assert len(hub.datasets) == 1
    assert hub.datasets[small.key].history == 1000
    assert not hub.started
```

This is only a constructor/demand test; the factory is not started. It is not a native feed test. Add real native adapter integration separately.

Required red/green cases: two consumers/different history; one reader stopping; failed update retaining a healthy snapshot; stale 304/heartbeat; epoch change; corrected historical candle; lost context dependency; unsupported field placeholders; token scope/revocation; oversized responses; redirects/origin changes; foreign/execution DB rejection; and cleanup after control-loop failure. Invalid updates must not renew freshness. Cache hits must validate full feature provenance and input/transport readiness.

Do not lose the attachment's durable-state and exact-content guarantees merely because the newer hub has fewer files. State explicitly whether selected storage supports only bounded recovery or full as-of replay. Accept confirmed one-minute OKX spot first. Other intervals/venues, MQTT and intrabar input require their own validated contract.

Use a deterministic local fixture with actual native adapters at the release manifest's admitted dataset/history/consumer maximum. Measure feed count, warmup, CPU, memory, response bytes, candle lag, backlog and reconnect recovery. Pin numeric release limits from measured evidence; a configured maximum is not proof of supported all-symbol capacity. A public-data soak requires its own operational permission.

**Accept:** actual native reuse, consumer isolation, current compatible features and bounded recovery/load are shown; no silently broadened capability. Run the full reconciled `test/modular_spot` suite and retain a distinct native load receipt.

## M4 — Fixed-symbol programs through native configuration

**Files:** existing `profiles.py` (`ProgramProfile`, `build_profile`), `universe.py`, `selection_state.py`, controller configuration/runtime as needed; planned `test_programs.py`.

Use `build_profile` as the existing compiler entry point rather than create a parallel compiler. Its output must validate against the actual controller schema and remain disabled until separately admitted. Bind account/program/policy revision, observation symbols, executable allowlist, context dependencies, pair budgets and native owner references. A five-symbol list is not a requirement for five open positions.

Add failing cases for five execution symbols plus two benchmarks, duplicate quote aliases/base concentration, conflicting owner IDs, missing context, invalid spot instruments, and a removed symbol with owned inventory/pending cancellation. Keep management subscriptions and obligations even after selection removal. Freeze each active position's admitted behavioral policy while selection changes.

Feed deterministic candle snapshots/native receipts through the actual controller discovery/configuration path, not just the dictionary compiler. No container-per-symbol lifecycle or changes to existing controllers' configurations.

**Accept:** fixed-symbol composition and management retention are wired and native-validated; symbol selection does not grant capital. Run `"$HB_PYTHON" -m pytest -q test/modular_spot/test_programs.py test/modular_spot/test_native_contract.py` and the combined suite.

## M5 — Capital and slot enforcement for the real topology

**Files:** existing `program_budget.py` (`BudgetPolicy`, `Reservation`, `BudgetStore`), `portfolio.py`, `state.py`, native/runtime admission adapters; planned `test_capital_admission.py`.

Do not call the existing single-recorder BudgetStore an account-wide distributed allocator. Inspect the actual native/stack authority and choose an admitted topology: genuinely segregated accounts/subaccounts, or one existing authority covering every spender of the account. Different symbols or different Metadata namespaces do not segregate quote cash. Wider/shared-owner changes require separate scope.

Use the existing `BudgetStore.reserve` and `contains` boundaries where applicable; prove requests bind account/connector, program, intent/executor, instrument, fee-inclusive quote/base caps and position slot. Journal reservations against native evidence, not independently maintained fills/PnL. Wire checks at proposal, queued dispatch and executor admission.

Before integration, write real concurrent transaction tests: two intents racing for the same remaining cash/slot; lost submission acknowledgement; cancellation plus late fill; expired signal with uncertain execution; restarted owner; retained inventory; reappearing historical inventory; wrong program/member; and base asset reached through multiple quote pairs. Expiry, process absence, a recovery label or unconfirmed cancellation cannot release commitments. Prove release only from applicable native terminal reconciliation.

**Accept:** aggregate cash/asset/slot conservation under the claimed scope and topology, with no legacy-owner enrollment. Segregation is an explicit supported topology, not a claim that cross-process coordination was implemented. Run `"$HB_PYTHON" -m pytest -q test/modular_spot/test_capital_admission.py` plus native orchestrator regressions. Keep acquisitions disabled when topology evidence is missing.

## M6 — External sell-only inventory admission and native reduction

**Files:** planned private `sell_only.py`, existing `inventory.py`, applicable `execution.py`/runtime adapters, profile constraints; planned `test_sell_only.py`. No actual SUI/main inventory handoff in implementation tests.

Target interfaces are `admit_inventory(handoff, native_reconciliation)` and `propose_reduction(scope, exit_observation, current_native_evidence)`. The first yields a bound management scope or rejection; the second yields a native-compatible bounded sale or hold. Bind account/instrument, prior/new owner, net available and reserved base, original native cost/fees, reconciliation receipt, policy revision and explicit authority evidence. Do not infer ownership from wallet balance or fabricate basis from current price.

Inspect actual native OrderExecutor/held-inventory capabilities and characterize min-price, fees, partial fills, terminal reconciliation and record attribution. Never use a SELL PositionExecutor assuming it means spot inventory reduction. When an adequate existing native path is absent, retain the hold and specify the separate owner repair.

Required failing-to-passing cases: three assigned symbols; missing/unknown basis; another owner's lot; duplicated/conflicting handoff; repeated sale signal; existing reservation; net-base fees; dust; partial sell/cancel/late fill; gap below profit floor; fee revision; restart. Check zero BUY/DCA/repurchase/rearm at configuration, proposal, budget, factory and retry boundaries.

**Accept:** actual native evidence shows only assigned inventory reduced with quantity/cost/fee conservation, approved profit eligibility and no second seller. An acquisition-disabled flag is not enough. Run `"$HB_PYTHON" -m pytest -q test/modular_spot/test_sell_only.py`, relevant native accounting/order tests and the combined suite.

## M7 — Broad screening to bounded native acquisition

**Files:** existing universe/selection/profile/runtime paths; planned private `screener.py` only if needed as a policy composition; planned `test_screener_dispatch.py`. Record any required new native entry-point path before adding it.

The target `screen(catalog_snapshot, feature_bundle, policy)` returns ranked opportunities without capital permission; `admit_candidates(opportunities, native_market_readiness, capital_authority)` returns bounded native proposals with input/config identity. Reuse the existing selector where it satisfies this contract; do not clone it under a new name.

Maintain an explicitly bounded observation catalog and warm minimum signal history. Freeze account-executable eligibility separately. Write failing tests for many simultaneous candidates but fewer slots, stale eligibility/context, unready native book/rules, delisting between selection/dispatch, rank churn and one unavailable instrument among valid peers. A slow or invalid symbol must not silently corrupt valid peer inputs.

Enforce deterministic tie-breaking, bounded work/queues and current grants at dispatch. Keep every owned/pending/recovery symbol in management. Selection updates cannot rewrite open positions' trailing, ownership or policy versions. No bot-container launch per signal. Initially select dynamically only within the already admitted native execution universe; newly observed listings do not hot-enroll new execution markets automatically.

**Accept:** a real native fixture shows all configured observations, fewer bounded acquisitions, no cash/slot oversubscription and persistent management. Keep the first impulse profile closed-bar; intrabar detection is not claimed. Run `"$HB_PYTHON" -m pytest -q test/modular_spot/test_screener_dispatch.py test/modular_spot/test_programs.py test/modular_spot/test_capital_admission.py` and the combined suite.

## M8 — Independent source review and release/research handoffs

Run the complete new-controller suite, relevant existing native regressions and actual repository lint/pre-commit on the exact final source. Inspect warnings/skips and negative paths. Compilation/AST checks are not substitutes for real native semantics. Review comes from a genuinely separate actor and records exact reviewed SHA, findings, repairs and verdict; author self-review is not independent.

Prepare separately scoped stack adoption evidence: source/image/config/state binding, private data-only state, scoped transport, capacity/freshness limits, unique owners, backups/reconciliation, alert delivery, tested restart/refusal and rollback. Retain native-only API and main/SUI ownership boundaries. Do not deploy from this source/doc task.

Prepare Research OS/quants-lab intake only under its own mandate. Read current research agent/workflow/program/data contracts, resume an existing supervisor when applicable, bind idea/candidate/runtime/data/cost/availability identities, retain failed outcomes, and use the mandated cached scored window when applicable. Technical fixtures are not economic backtests. No such backtest is run by this plan publication.

Report source-ready, native-verified, operationally-admitted, economically-evaluated and activation-authorized separately. A missing gate remains HOLD. Profit-only execution does not guarantee a fill, bounded drawdown or profitability. Activation and merge remain separate operator actions.

## Evidence, resumption, idempotence and rollback

At each milestone record date/time, repository, base/head, interpreter/image, exact argv, fixtures/input provenance, result including skips, raw-log path/hash, changed paths, reviewer and remaining limitations. Structured evidence is authoritative; Markdown summarizes it. Redact secrets and unnecessary account identifiers. Leave the first unfinished milestone, next command and exact external blocker at every stop.

Example wording: `M3 passed at the recorded fixture capacity; native public-data soak not-run`. Do not claim all-symbol capacity without its catalog and measured bounds. A blocker package includes the failing gate, observed error, one-command diagnosis and required owner action; do not invent filler work or silently relax the gate.

Before/after each slice inspect Git status, diff stat and diff check. Commit only intended files. Do not force-push, reset/stash/discard unrelated work, blindly apply the old patch, overwrite concurrent code or incidentally advance root pins. The original exact-head helper's refusal must never be resolved by deleting the newer code or these documents.

Documentation changes can be reverted as docs-only commits. Before activation, source changes can be reverted after review. After an authorized run, code rollback is not an owner migration: use the native stop/reconcile/backup procedure, preserve claims, pending/late fills and held inventory, then perform a separately admitted rollback. Never overwrite a running database or reset cost/trailing state to make restart pass.

### Outcomes and retrospective

This handoff preserves the original evidence and newly observed published helper checkpoint, explicitly identifies their schema overlap, and supplies a forward reconciliation path. Only documentation was changed by this task. No new implementation suite, native runtime, independent audit, economic experiment or production result is claimed. Publication is verified separately by the resulting commits and PR comment; no future commit ID is invented here.

### Copyable implementation handoff

> Read current root/owner instructions, SPEC.md, PLAN.md and the 2026-09-19 VALIDATION.md receipt. M0-M8 implementation has been revalidated and repaired in the uncommitted candidate. Follow the next implementation order below: reuse one selected existing controller policy and prove parity before strategy expansion. Preserve newer remote documentation and existing live owners. No activation, account mutation, backtest, publication, merge or deployment follows from source/test evidence.


## M9 — Mean-reversion context policy: shadow first, reduction-only enforcement later

**Authority:** research/specification and source validation only unless a later task explicitly authorizes an economic backtest or paper/canary/live activation. **Owners:** Hummingbot for controller/native decision semantics; Hummingbot API/Market Pulse is an input producer only; Quants Lab owns economic backtests; Research OS owns durable research evidence.

### M9.0 — Bind the input contract

Inspect the exact Market Pulse/component producer and the final M0-M8 modular interfaces before coding. Record source SHAs, component formula/schema, universe construction, finality, observation/receipt timestamps, coverage behavior and cross-venue identity. Add a versioned immutable context snapshot adapter; do not make the controller call dashboard/reporting code or create a second market-data/execution owner.

Red tests: stale/missing context, low coverage, universe revision, future/unfinished candle, changed producer epoch, cross-venue identity, out-of-order snapshot, duplicate snapshot and correction. Existing position management must remain independent when only entry context is unavailable.

**Accept:** the same eligible completed observation produces the same bound snapshot; unavailable/invalid context cannot authorize new risk.

### M9.1 — Separate validity from economic context

Expose freshness/readiness/coverage as hard validity metadata rather than a favorable economic component. Preserve the existing public Market Pulse scalar for compatibility; do not silently redefine its dashboard meaning. The new MR policy consumes explicit components and derivatives (level/change) under a new schema/version.

Add prefix-invariance tests: features attributed to decision time `t` computed from data available through `t` must not change when future data are appended. Explicitly test close-derived features are unavailable before candle close/receipt. Historical rebuilds that synthesize `event_age_seconds=0` are not accepted as live-freshness replay.

**Accept:** no lookahead through timestamp labeling, forward fill, universe membership or later corrections; validity cannot be offset by bullish components.

### M9.2 — Shadow policy for initial entries

Add an opt-in `MeanReversionContextPolicy` (name may adapt to current owner conventions) behind the strategy opportunity and before capital reservation. Output `ALLOW/DEFER/DENY`, `multiplier [0,1]`, expiry and reason codes. In shadow mode, persist/emit the proposed context decision and baseline proposal identity but do not change native actions.

Capture every baseline-eligible opportunity, including those shadow policy would reject, with causal context, strategy setup, current inventory/reservations and later outcome references. Do not label rejected trades as realized avoided losses.

**Accept:** with enforcement disabled, native action sequence is parity-equivalent to baseline; shadow records are deterministic, causal and attributable.

### M9.3 — Research: initial-entry incremental value

Through the existing Research OS/Quants Lab registration flow, preregister unchanged baseline, simple trend/volatility context control, current raw composite gate (diagnostic only) and component-based MR context candidate. Use the mandated cached May-August 2026 window, identical spot 1x accounting and realistic costs. Match/normalize exposure when interpreting risk improvement.

Primary evidence: after-cost portfolio return/equity, max drawdown and duration, tail loss/MAE, trade activity, deployed-capital and capital-days, inventory age/unfinished inventory, turnover/fees and regime/fold stability. Record trial count and parameter sensitivity. No threshold tuning after seeing the final comparison.

**Accept for next stage:** preregistered incremental criteria pass with adequate activity and robustness. Otherwise record contradicted/inconclusive evidence and keep the layer shadow-only.

### M9.4 — Shadow and research next-DCA-tranche admission

Do not change DCA spacing, tranche amounts and exit logic simultaneously. First evaluate only whether the baseline's already-planned next tranche is admitted/deferred. Bind the decision to current native per-tranche inventory, outstanding commitments and remaining program budget. A lower combined average price is not itself evidence of improved economics.

Required failure cases: accelerating systemic selloff, context recovery after defer, stale context, reservation already consumed/uncertain, price gaps, minimum-notional rounding, retained inventory and restart. Expired context never releases an uncertain native commitment.

**Accept:** DCA admission improves preregistered portfolio outcomes versus unchanged DCA under matched execution assumptions; per-tranche native accounting remains conserved.

### M9.5 — Candidate ranking for screener profile

Only after entry-context evidence, test ranking as `local MR setup × global context × executable cost/capacity`. The global component cannot alone distinguish candidates. Use deterministic ties, bounded queues, base-asset concentration and current executable-universe admission. Do not force unused budget into fewer survivors.

**Accept:** ranking improves opportunity selection under fixed aggregate risk without increasing concentration or bypassing M5 capital/slot authority.

### M9.6 — Optional bounded enforcement

Enforcement remains opt-in, default off and reduction-only. Before any paper/canary/live use, require exact source/image/config identity, native M0-M8 gates, causal context freshness/coverage monitoring, reconciliation, aggregate account limits, alerting, rollback and operator approval. On context failure, fail closed for dependent new acquisition; do not synthesize exits or restart execution owners.

Sizing multipliers above 1 and score-driven exit/trailing changes are explicitly out of this milestone and require separate hypotheses/specs/evidence.

### M9 validation matrix

At minimum retain tests for: baseline parity in shadow mode; no context-only BUY; multiplier bounds; minimum-order deny/defer; stale/missing/low-coverage fail-closed; future candle/lookahead refusal; snapshot/config/intent identity; universe revision; cross-venue labeling; duplicate/out-of-order/correction behavior; DCA uncertain reservation; restart determinism; management independence; and reason-code projection.

Record implementation, research, native, paper/canary/live and promotion status separately. Passing unit/native tests does not establish economic merit; passing a backtest does not authorize capital.


## Next implementation order after 2026-09-19 validation

1. Review/publish the repaired local candidate when explicitly authorized. Preserve
   the newer documentation-only remote additions at `2f9e4d0168df64d4c99a3d6e3055945a36ad572e`;
   no newer remote product code existed at inspection. The worktree is still at
   `808919077` plus uncommitted product/repair bytes; do not describe it as remote HEAD.
2. Freeze one existing controller configuration and implement a named compatibility
   policy by reusing its existing decision functions, not copying its order engine.
   Compare identical candle/native-receipt inputs, signal identity, entry type,
   amount, cooldown, expiry, trailing and reductions. Start with OK RSI, then RSI V5.
3. Validate crash/restart ownership through the exact native executor specialization.
   Keep planned-resume refusal until the separate shared-engine owner change is proven.
4. Bind one isolated operational owner/account and image, then use the existing
   Research OS owner workflow for a separately authorized cached May-August 2026
   comparison. Do not transfer legacy strategy evidence to the simple modular baseline.
5. Only a later operator mandate may activate an isolated canary or migrate a legacy
   owner. Preserve current rsibot-stack owner IDs, start times and inventory throughout.

M9 context and flash-crash specifications are preserved design work, not part of
this validation or a reason to expand strategy scope before parity and recovery.
A new account-wide allocator is not required for this local work. BudgetStore is
a small reservation guard inside one recorder/strategy; existing independent
wallet spenders remain outside that scope.


## 2026-09-19 final validation receipt

- V1-V4 complete for the authorized local validation/repair scope.
- Preserved M0-M8 candidate; fresh baseline: 230 modular,172 native +10 subtests.
- Added 11 failure regressions; final combined: 241 modular +172 native +33 candle
  tests = 446 passed, plus 10 subtests; zero skips/failures/warnings.
- Applicable actual pre-commit hooks and diff checks passed after scoped formatting.
- Independent final repair review APPROVED; no Critical/Important findings.
- Isolated public native OKX WebSocket smoke passed:2 feeds / 2 readers / 90-bar windows,
  three successive confirmed 1m bars per pair, reader reconnect, 2.15s final candle age.
- All 13 live container ID/image/start-time identities unchanged. Root and nested
  Hummingbot statuses unchanged. No live writes, orders, commits or pushes.
- Remote additions through 2f9e4d0 preserved; local HEAD remains 808919077 plus dirty
  product. Source hash manifest and exact commands/results in VALIDATION.md.
- Remaining gates: legacy policy parity, planned-resume restoration, operational
  owner/image/account binding, long soak and authorized economic/activation work.
- Rollback uses the pre-task archive for dirty paths and recorded HEAD for paths
  that were clean; task-only incremental patch is in root audit directory.


## Full-universe continuation receipt (2026-09-19)

- Integration owner moved to isolated branch `feat/andrzej_modular_stack_validation` at PR #57 `068702141f1b`, with preserved/repaired PR #55 source reconciled on top. Original PR #55 candidate retained. Scope includes isolated API schema/managed-source fixes and the PR #188 native-owner handoff seam; live surfaces remain forbidden.
- [x] U1: frozen 294-pair live USDC spot catalog and current official protocol limits.
- [x] U2: five-minute multiplexed public test, 1,176/1,176 eligible confirmed bars, expected-disconnect recovery; failed first attempt retained.
- [x] U3: measured publisher/reader responsiveness repaired and compared; concrete native recipe/API compatibility defects repaired.
- [x] U4: fresh combined native suite 563 tests +10 subtests, zero skips; API 112 tests; native/stack/parity boundaries documented.
- [x] U5: final exact-source default/custom-owner handoffs; root helper 19 regressions; independent reviews; unchanged 13 live container identities and nested Hummingbot status. Unrelated concurrent quants-lab status change recorded, left untouched. Source seal and receipts linked in UNIVERSE_VALIDATION.

See [UNIVERSE_VALIDATION.md](UNIVERSE_VALIDATION.md) for commands, measurements, limitations, native reuse matrix and next order. This closes the first all-symbol diagnostic, not all-pair production collector integration or legacy strategy parity. Current-source documentation supersedes stale attachment instructions while preserving the earlier receipt as historical evidence.

## Native completion continuation — 2026-09-19

The selected reusable native path is implemented in `modular_ok_rsi` and
`modular_rsi_v5`, with actual native configuration, executor/lifecycle inheritance,
optional confirmed stream candles and recorder-backed held-trail checkpointing.
See CONTROLLER_PARITY.md and the canonical root report
`rsibot/docs/reports/2026-09-19-modular-native-completion.md`.

Fresh host verification passed 1,029 tests plus 39 subtests without skips. Final
Linux validation passed 1,009 tests plus 39 subtests; twenty optional PyArrow-backed dtype aggregation cases
require PyArrow and passed in the host run. The Rust service passed 29 tests. The
294-pair Linux stream ran 900.020 steady seconds successfully; the final-image
four-wrapper, 7,514-row probe passed 150.069 steady seconds with native features,
independent higher-timeframe comparisons and cache restart.

Native integration required three narrow runner/accounting identity gates outside
the private controller directory. Their independent audit and actual runner/SQLite
regressions passed. Existing controller algorithms and native order/fill ownership
remain unchanged. API integration is separately owned and source/image-bound.

This completes the bounded native integration continuation, not the later M9/new
strategy and whole-program roadmap. The candidate is local and uncommitted.
Remote CI is blocked by GitHub billing; publication, economic research, live-owner
migration and activation remain separate. All thirteen live stack container IDs,
image IDs and start times were preserved.

## Publication stack reconciliation — 2026-09-19

PR #55 foundation commit `0c1aa65ed9e31c92f7d5d353ae7a574ec9a1e294` is now an ancestor of this integration. Its standalone [publication receipt](PUBLICATION_RECEIPT.md) records 446 passing tests plus 10 subtests, zero skips; those results describe the parent, not this larger candidate. The original public-candle diagnostic is retained from the parent.

The merge preserves this integration's already validated 10,000-row history, asynchronous bounded snapshot capture, WebSocket selection, recipe binding and transport regression tests. Older conflicting forms would remove those features. The 1,173-file final production manifest is unchanged. Parent publication documentation is retained in its receipt; current integration SPEC/PLAN/README/VALIDATION retain their newer evidence and limits. No source/history rewrite, component pin, merge into the default branch or live deployment is authorized.
