# Modular spot programs and shared data — execution plan

> Living ExecPlan. On an authorized implementation run, execute one eligible milestone at a time with fresh review checkpoints. Use the available executing-plans or subagent-driven-development workflow; a separate actor is required for an independent audit. Resume this document rather than starting another task ledger.

**Goal:** Complete the isolated modular controller, shared confirmed candles, fixed-symbol programs, sell-only inventory management and bounded universe-wide screening without changing existing controllers or bypassing native Hummingbot ownership.

**Architecture:** Native Hummingbot remains the execution and accounting owner. Private, explicit policies compose observations, acquisition, exits and risk; a read-only data service distributes candle snapshots. Universe selection and capital permission are separate from per-position lifecycle management.

**Tech stack:** The exact owning Hummingbot runtime and its installed Python, Pydantic, pandas/numpy, SQLAlchemy and aiohttp dependencies. Use native market-data, controller, executor and recorder interfaces. No package upgrade or new broker is implicitly authorized.

**Spec:** [SPEC.md](SPEC.md). **PR:** [hummingbot#55](https://github.com/s1korrrr/hummingbot/pull/55). **Branch:** `feat/andrzej_isolated_modular_spot`.

## Immediate next action

Obtain a clean, complete owner checkout of the current PR branch and run the native import gate in M0. Then reconcile and validate the attached patch in M1. Do not call the existing selector helpers a multi-symbol execution implementation, and do not begin with live deployment.

The shared-data code is still an attachment at this plan's starting checkpoint. Pushing this document does not apply that code. The old bundle helper requires the exact original PR head and will refuse a documentation-only descendant. M1 provides a checked path that preserves the new documentation; do not reset the branch to satisfy the old helper.

## Scope, authority and completion bar

Current publication is documentation-only in `docs/controllers/modular_spot/`. It changes no controller, shared engine, configuration, root gitlink, runtime database or running service.

When implementation is explicitly resumed, work inside these task boundaries:

- `controllers/generic/modular_spot.py` and `controllers/generic/modular_spot_core/`.
- `test/modular_spot/` and `docs/controllers/modular_spot/`.
- New native-discoverable entry points for this controller family only when the relevant milestone proves they are needed; record the exact path before adding it. Do not edit other controllers.

The completed source must satisfy all mandatory SPEC scenarios, pass the actual native and scoped regression gates, and receive independent review. Operational deployment, authenticated account work, economic backtests, image/pin rollout, merge and paper/canary/live activation are distinct authority decisions. Neither this file nor passing tests grants them.

Use explicit statuses: `observed-source`, `attachment-candidate`, `specified`, `passed`, `failed`, `not-run`, `blocked:external`. Do not silently promote one status to another. Scope expansion into shared engine, API, stack, account allocation or legacy migration requires its own owner change and authorization.

## Context and starting evidence

Published implementation parent: `6472dfece06ca71d9ebc6b887dda341ccdf5befb`. Native base: `4df9ff3158a405b71d28b1c07446cfc7864a19fa`. PR #55 is open and draft at the source inspection for this handoff. Its current code is single-pair BUY acquisition with local native PositionExecutor specialization, not external sell-only or multi-symbol routing.

The original scoped plan is preserved in Git at [the implementation parent](https://github.com/s1korrrr/hummingbot/blob/6472dfece06ca71d9ebc6b887dda341ccdf5befb/docs/controllers/modular_spot/PLAN.md). This revision supersedes its roadmap, not its isolation, ownership or approval constraints.

Attachment identities:

| Artifact | SHA-256 |
|---|---|
| `pr55_shared_data_update.zip` | `4585bd99764efbe951a375a6c95cc8ea4e3b01cdc873d837ab104cb525ff6134` |
| `pr55_shared_data_universe.patch` | `a30dede9a1c0ad3a47be4936c4fecb5bdd55662faf2343a05c6d9f8485e92523` |
| `pr55_shared_data_validation.md` | `3be487817fe051027d2e3b5ffd00078db423cfe92e17e0bbb5239d8997dc31b0` |

The bundle's own manifest identifies 16 patch files, 1,945 insertions and five deletions. It is a partial source reconstruction: it is not the complete native checkout. Its receipt reports 60 new focused tests passed and one native module skipped. PR #55 separately reports 61 original tests passed and one native module skipped. Neither is a new combined result; never sum these counts. The attached native-import log reports `ModuleNotFoundError: No module named 'hummingbot'`.

The attachments originate from this conversation and are not repository paths. Future environments may not have them mounted. Obtain the exact bundle/patch from the operator's handoff and check its digest. When unavailable, record the missing artifact and implement the specified contracts in bounded test-driven slices; do not claim to have applied or reproduced unseen bytes.

### Progress

- [x] Read the current PR metadata and scoped implementation plan; distinguish the published source from the attachment.
- [x] Inspect the attached manifest, validation receipt, logs, implementation interfaces and exact artifact hashes.
- [x] Write the target specification and resumable plan; retain the original implementation parent as historical evidence.
- [ ] M0: Bind the complete owner checkout/runtime and establish a real native baseline.
- [ ] M1: Reconcile the shared-data attachment on the branch and validate the combined implementation.
- [ ] M2: Prove/harden native lifecycle, exit, identity and recovery semantics.
- [ ] M3: Prove shared confirmed data, dependency handling and bounded service behavior.
- [ ] M4: Wire fixed-symbol programs and preserve management obligations.
- [ ] M5: Enforce aggregate capital/slot permission for the admitted account topology.
- [ ] M6: Implement explicit external sell-only inventory admission and native reduction.
- [ ] M7: Wire universe-wide screening to bounded native multi-symbol acquisition.
- [ ] M8: Finish independent source review and prepare separately gated operational/research handoffs.

Do not mark an implementation milestone done based on a helper's unit tests. Append the exact commit, command, environment, result and remaining limitations when a box changes.

### Decision log

- 2026-09-18: Keep the new controller separate; supersede the historical OK RSI extraction proposal.
- 2026-09-18: Keep native per-tranche positions initially, not a synthetic basket-average ledger.
- 2026-09-18: Accept the attachment only as a candidate. Start with confirmed OKX spot and bounded HTTP snapshots; do not imply MQTT, intrabar input or historical archive support.
- 2026-09-18: Separate observation, acquisition and management universes. Keep owned, pending and recovery symbols subscribed and counted.
- 2026-09-18: Separate fixed-symbol composition, account capital enforcement, external sell-only admission and dynamic screening into reviewable milestones.
- 2026-09-18: Publish spec/plan only. Old validation claims remain historical. Source integration, native semantics, economic validity and activation remain separate claims.

### Discoveries and unresolved blockers

- Native validation was unavailable in the attachment environment. The next worker must prove actual Hummingbot imports and use the owning environment, not substitute a fake native package.
- The current planned-resume path can bypass controller-local executor selection. Preserve refusal until exact native restoration is demonstrated; a necessary shared-engine repair is outside this PR's implicit scope.
- Selection helpers are not native multi-symbol dispatch. Disabling acquisition is not a complete wallet sell-only path. A controller budget is not cross-process account reservation authority.
- The snapshot store has two recent changed windows, not an as-of archive. Full-universe throughput and native collection were not run.
- Publication capabilities must be tested in the current session. The old read-only publication blocker is not assumed permanent.

## File and interface map

After M1, the candidate data interfaces should be present at the following exact paths. Verify them against the attachment before relying on signatures:

| Path under `controllers/generic/modular_spot_core/` | Existing attachment interface |
|---|---|
| `candle_contracts.py` | `Dataset`, `Bar`, `Snapshot`, `ConsumerRequest`, `CollectorManifest`, `SharedCandleConfig` |
| `candle_store.py` | `CandleStore.publish/get/unavailable/close`; isolated market-data SQLite ownership |
| `candle_service.py` | `NativeCandleCollector.start/tick/stop`; native buffer adapter |
| `candle_transport.py` | `make_app`, `SharedCandleClient.refresh/refresh_all/close`, `CandleViewProvider` |
| `candle_reader.py` | `CandleReplica.accept/ready/entry_ready/dataframe` |
| `universe.py` | `UniversePolicy`, `UniverseEvidence`, `UniverseDecision`, `select_universe` |
| `feature_cache.py` | `FeatureSpec`, `FeatureResult`, `FeatureCache.evaluate` |

The published core already has `config.py`, `domain.py`, `signals.py`, `acquisition.py`, `portfolio.py`, `risk.py`, `inventory.py`, `trailing.py`, `state.py`, `planner.py`, `execution.py` and `runtime.py`. Integrate through those boundaries; do not copy them into another strategy family.

Proposed new local module paths below are planned, not implemented: `programs.py`, `capital.py`, `sell_only.py` and `screener.py`. Their tests are respectively `test_programs.py`, `test_capital_admission.py`, `test_sell_only.py` and `test_screener_dispatch.py` under `test/modular_spot/`. Every milestone starts with characterization/failing tests against the actual native contracts and pins any necessary interface revision in the decision log.

## M0 — Bind source, runtime and safe test context

**Owner/scope:** Hummingbot checkout inspection; no exchange/account requests. Read current root `AGENTS.md`, relevant routing/task instructions and any nested instructions. Run the user's bootstrap only in the actual owner machine when its script exists; never claim to run that local path from a sandbox where it is absent.

1. Record `git status --short`, `git branch --show-current`, `git rev-parse HEAD`, `git remote get-url origin` and root component pins. Inspect current PR comments and head. Redact credentials from any remote URL before recording it. Preserve unrelated work; use an isolated clean checkout/worktree rather than reset/stash/discard.
2. Set `HB_PYTHON` to the actual native environment's interpreter and `EVIDENCE_DIR` to an absolute dedicated evidence directory outside runtime state. Record the interpreter and installed dependency versions; do not install arbitrary latest packages to make tests pass.
3. Run the offline native gate before accepting any native test status:

```bash
: "${HB_PYTHON:?Set the actual owning Hummingbot interpreter}"
: "${EVIDENCE_DIR:?Set an absolute dedicated evidence directory}"
"$HB_PYTHON" -c 'import sys; import hummingbot; from controllers.generic.modular_spot import ModularSpotController; from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor; print(sys.executable); print(hummingbot.__file__)'
"$HB_PYTHON" -m pytest -q test/modular_spot --junitxml="$EVIDENCE_DIR/m0-original.xml"
```

4. Inspect the JUnit skips explicitly. An import failure or skipped native module leaves the native gate failed/unavailable, even if pure tests pass. Record underlying failures before deciding whether they are environment, baseline or candidate defects.

**Accept:** exact branch/source/runtime identity and native baseline receipts exist; no secret/state copying; no manufactured compiled-runtime substitute. **Next on failure:** retain pure-policy progress, record the actual failing import/build dependency and the exact owner command needed; keep native-dependent milestones open.

## M1 — Reconcile the attachment without discarding plan commits

**Files:** the 16 paths declared in the bundle manifest; no other code paths. **Consumes:** original code blobs and verified artifact. **Produces:** a combined working tree and a new truthful integration receipt.

The old `apply_to_pr55.sh` insists on HEAD exactly `6472dfe...`. Once this plan is pushed, that exact-HEAD condition is expected to fail. Do not weaken it generally or rewind the branch. For a clean documentation-only descendant, use the following inspected preconditions and normal `git apply --index` path:

```bash
set -euo pipefail
: "${PATCH_FILE:?Set absolute path to pr55_shared_data_universe.patch}"
export PATCH_FILE
python3 - <<'PY'
import hashlib, os, pathlib, subprocess
base = '6472dfece06ca71d9ebc6b887dda341ccdf5befb'
def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()
if git('branch', '--show-current') != 'feat/andrzej_isolated_modular_spot':
    raise SystemExit('Use the authorized PR55 task branch in a clean checkout')
if git('status', '--porcelain'):
    raise SystemExit('Preserve dirty/untracked work; use an isolated checkout')
subprocess.run(['git', 'merge-base', '--is-ancestor', base, 'HEAD'], check=True)
changed = git('diff', '--name-only', base, 'HEAD').splitlines()
if any(not p.startswith('docs/controllers/modular_spot/') or not p.endswith('.md') for p in changed):
    raise SystemExit('Source changed since attachment; reconcile manually, do not auto-apply')
blobs = {
    'controllers/generic/modular_spot.py': '69627fced38287bd2691304f110c76ef1df4ab6f',
    'controllers/generic/modular_spot_core/domain.py': '913242c0cd2568da0faca946278bd286b1f44d85',
}
for path, expected in blobs.items():
    if git('rev-parse', 'HEAD:' + path) != expected:
        raise SystemExit('Prerequisite blob changed: ' + path)
patch = pathlib.Path(os.environ['PATCH_FILE'])
if not patch.is_absolute() or not patch.is_file():
    raise SystemExit('An absolute existing patch path is required')
if hashlib.sha256(patch.read_bytes()).hexdigest() != 'a30dede9a1c0ad3a47be4936c4fecb5bdd55662faf2343a05c6d9f8485e92523':
    raise SystemExit('Patch identity mismatch')
subprocess.run(['git', 'apply', '--check', '--index', str(patch)], check=True)
PY
# Only after the entire preflight above succeeds:
git apply --index "$PATCH_FILE"
git diff --cached --check
git diff --cached --stat
"$HB_PYTHON" -m pytest -q test/modular_spot --junitxml="$EVIDENCE_DIR/m1-combined.xml"
```

Run the block with shell fail-fast behavior (`set -euo pipefail` in Bash). Inspect the patch and manifest before executing it. Verify every bundle payload hash and reject paths escaping its root. If the patch was already applied or code changed, inspect ancestry/current bytes and reconcile deliberately; never force-apply it twice. A source divergence is not permission to overwrite another contributor's work.

Review the attached test files together with the original tests. In particular, structural isolation assertions written for the old source may need narrowly updated expectations for the intentionally new data-only adapter. Do not delete a guard just to turn a combined suite green. Retain historical `VALIDATION_SHARED_DATA.md`; append fresh environment/commit results rather than replacing earlier limitations with implied success.

**Accept:** combined suite actually ran in the owner environment; zero unexplained native skips; actual patch scope and payload identities match; data service and production-readiness claims remain accurate. **Publication:** commit/push code only under an active implementation/publication request; this documentation run does not perform M1.

## M2 — Harden the native controller and execution boundary

**Files:** `modular_spot.py`, `runtime.py`, `execution.py`, `inventory.py`, `state.py`; tests `test_native_contract.py`, `test_native_lifecycle.py`, `test_native_recovery.py`.

**Consumes:** native ControllerBase discovery, constructor/stop contracts, executor selector and serialized config/receipts. **Produces:** evidence that local policies cannot bypass native ownership or change exit semantics through restart.

1. Before modifications, characterize config round-trip, default-disabled behavior, unknown intent rejection, existing native callbacks, risk-latch persistence and missing receipt behavior with real native classes and a temporary recorder.
2. Add failing cases for partial entry then cancel/late fill, fee correction, duplicate factory admission, missing checkpoint, changed source/configuration, repeated start/stop, partial sale and retained dust. Never use a live DB or authenticated connector.
3. Check per-tranche cost and quantity against native receipts. Verify the tightened profit floor for supported fee currencies, tick sizes and partial fills. No indicator may waive an unavailable basis/fee.
4. Test the actual orchestrator planned-resume path, not a fake factory that always selects the subclass. Prove restoration of identity, inventory and trailing state or retain an explicit unsupported-resume hold and record a separate shared-engine dependency.
5. Prove a candle outage blocks new dependent acquisitions while existing native exits retain their owner and appropriate native execution inputs. No automatic liquidation or owner replacement.

```bash
"$HB_PYTHON" -m pytest -q test/modular_spot/test_native_contract.py test/modular_spot/test_native_lifecycle.py test/modular_spot/test_native_recovery.py
"$HB_PYTHON" -m pytest -q test/hummingbot/strategy_v2/executors/position_executor test/hummingbot/strategy_v2/executors/order_executor test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py
```

The two new test modules are created in this milestone before running that command. Verify existing native test paths in the current checkout; log any relocated replacement instead of silently skipping it.

**Accept:** real native admission, accounting, cancellation and restart/refusal behavior is demonstrated. A required shared-engine change outside scope blocks this gate until separately resolved; a refusal alone does not prove full recoverable production operation.

## M3 — Prove the shared-data path and exact feature dependencies

**Files:** the seven data/cache/universe modules in the interface map; `modular_spot.py`; existing `test_shared_*.py`; new `test_shared_native_load.py`.

**Consumes:** static `CollectorManifest` demands and native confirmed feeds. **Produces:** one admitted source per dataset, scoped immutable replicas, current feature inputs, and bounded service/recovery evidence.

Start with a characterization test using the existing contract (this is proposed test code, not a test run recorded here):

```python
from controllers.generic.modular_spot_core.candle_contracts import CollectorManifest, ConsumerRequest, Dataset

def test_shared_capacity_is_maximum_not_sum():
    dataset = Dataset(trading_pair='BTC-USDC', interval='1m')
    manifest = CollectorManifest(consumers={
        'five-symbol-program': (ConsumerRequest(dataset=dataset, records=300),),
        'screener': (ConsumerRequest(dataset=dataset, records=1000),),
    })
    assert manifest.capacities == {dataset.key: 1000}
```

The example uses the inspected attachment model fields and its `capacities` property. Keep a real native factory/buffer integration test in addition to this demand calculation test.

Add red/green cases for different consumer windows, one consumer closing, stalled collector, stale 304 response, restart epoch, corrected close, lost required benchmark, oversized response, revoked/scope-mismatched token and malicious/foreign DB path. Prove failed updates cannot renew a prior healthy claim. Require fresh input validation on feature-cache hits and deterministic initialization after restore.

Test old guard invariants alongside intentional data-adapter changes: no legacy imports/global executor mutation, no native candle initialization when shared mode is active, no mutation of the strategy's execution provider, and client cleanup even if the control loop raises before readiness.

Use a deterministic local fixture stream with the actual native adapter at the admitted manifest's maximum dataset/history/consumer counts. Measure startup warmup, native source count, memory, response sizes, end-to-end candle lag, CPU, consumer backlog and recovery. The release contract must pin numeric limits from this test before acceptance; there is no assumed universal all-symbol capacity. An authorized public-data soak is a later separate operational run, not part of offline unit tests.

```bash
"$HB_PYTHON" -m pytest -q test/modular_spot/test_shared_candles.py test/modular_spot/test_shared_transport.py test/modular_spot/test_shared_controller_boundary.py test/modular_spot/test_shared_native.py test/modular_spot/test_shared_universe.py test/modular_spot/test_shared_native_load.py
```

**Accept:** actual native source reuse and lifecycle proved at a stated capacity, data/transport/consumer ages distinguished, and unavailable fields never synthesized. HTTP-only/closed-bar limits remain explicit. MQTT, intrabar input and durable as-of archival replay are not silently marked done.

## M4 — Compose fixed-symbol programs without losing management scope

**Files:** new private `programs.py`, existing `universe.py`, `runtime.py` and `config.py` as necessary; `test_programs.py`; disabled examples in scoped docs.

**Target local interface:** `compile_program(manifest, catalog_snapshot, native_owned_scopes)` returns a deterministic tuple of native-discoverable per-pair modular configurations plus a data dependency manifest. Inputs carry the same account/program/policy revision, assigned pair budgets and native owner references. The manifest distinguishes observation symbols, executable allowlist, context inputs and management obligations. Output defaults to disabled execution and does not start controllers or provision credentials.

1. Write failing tests for five selected symbols with two benchmarks, no forced five-position allocation, conflicting native owner IDs, duplicated quote aliases, invalid spot instruments and incomplete mandatory context.
2. Add a removed-symbol scenario with owned inventory and a pending cancellation. The compiled management/dependency set must retain it and must not invent permission for new entry.
3. Compose per-pair native configurations using existing discovery. Do not add one container per symbol or change an existing controller's config. Freeze behavioral policy identity per active episode.
4. Feed recorded eligible snapshots and native lifecycle events through the real controller interface, verifying both no-signal and eligible-signal paths. Export reason codes rather than a single global healthy flag.

**Accept:** fixed five-symbol program, management retention and dependency completeness work through native configs; symbol selection is not execution authority. Run `"$HB_PYTHON" -m pytest -q test/modular_spot/test_programs.py test/modular_spot/test_native_contract.py` plus the combined suite before commit.

## M5 — Enforce account-wide capital and position slots

**Files:** private `capital.py`, `portfolio.py`, `state.py` and `runtime.py` adapters; `test_capital_admission.py`. No shared/global mutation or silent enrollment of legacy owners.

First inspect the existing native/stack reservation owner and choose the admitted topology: genuinely segregated accounts/subaccounts, or one existing account-wide authority. Record the actual integration point. Distinct symbols or a shared helper library do not segregate quote cash across processes.

**Target adapter contract:** `reserve(request, current_native_evidence)` returns a bounded grant or explicit rejection. A request binds account, program, intent, instrument, quote/base amount including fees, position slot and evidence revision. `reconcile(grant_id, native_terminal_receipt)` updates a commitment only from authoritative lifecycle evidence. The adapter may use a decision/reservation journal; it must not introduce independent fills, balances or PnL truth.

Write real concurrent transaction tests before integration. Two simultaneous intents must not reserve the same remaining quote or slot. Include a lost submit acknowledgement, a late fill after cancellation, quote aliases sharing one base limit, retained inventory, a restarted allocator and an expired signal with unresolved execution. Expiry alone cannot release the reservation. A segregated-account design must prove account separation instead of claiming a cross-process allocator was implemented.

Native dispatch must revalidate the current grant; the queue is not permission. Do not create a synthetic positive balance or assume missing processes stopped. If current account authority cannot support the topology without shared changes, keep the new path disabled and produce a bounded owner-change proposal rather than weakening the checks.

**Accept:** account-aggregate cash/asset/slot enforcement is demonstrated under concurrency and uncertainty, with native order truth retained. Run `"$HB_PYTHON" -m pytest -q test/modular_spot/test_capital_admission.py` and the relevant native orchestrator regression suite.

## M6 — Implement assigned-inventory sell-only through native reduction

**Files:** private `sell_only.py`, `inventory.py`, `execution.py`/`runtime.py` adapters; `test_sell_only.py`; native accounting/reduction tests. No migration of existing SUI or main inventory in this implementation test.

**Target contract:** `admit_inventory(handoff, native_reconciliation)` returns a bound management scope or rejection. Handoff must reference account/instrument, prior/new owner, native acquisition cost and fees, net available/reserved base, reconciliation receipt, approved policy revision and an explicit authority record. `propose_reduction(scope, exit_observation, current_native_evidence)` returns a native-compatible bounded sale request or hold. Neither interface may read an unassigned wallet quantity as owned inventory.

Start with actual native OrderExecutor/held-inventory capabilities. Characterize their exact fee, min-price, partial-fill and record attribution behavior; do not create a SELL PositionExecutor assuming it means spot reduction. If no existing native path satisfies the contract without shared changes, record the specific gap and separate the owner change.

Required failing-to-passing scenarios: three assigned symbols, no cost basis, another owner's lot, duplicated handoff, same inventory assigned twice, repeated signal, existing sell reservation, insufficient net base after fees, dust, partial sell plus cancel/late fill, gap below profit floor, fee revision and restart. Verify zero BUY actions at configuration, proposal, reservation, factory, retry and post-sale rearm boundaries. A sell-only profile must not reacquire after finishing.

**Accept:** real native receipts show only assigned quantity reduced with preserved cost/fee provenance and profit policy, no duplicate seller and no synthetic PnL. An `allow_acquisition=false` test alone cannot close M6. Run `"$HB_PYTHON" -m pytest -q test/modular_spot/test_sell_only.py` plus native order/accounting regressions and the combined suite.

## M7 — Connect broad screening to bounded native execution

**Files:** private `screener.py`, `programs.py`, `universe.py`, `runtime.py`; `test_screener_dispatch.py`; disabled manifest examples. Use an additive native-discoverable entry point only if pair-composition cannot express the observer; record and review that path separately.

**Target contract:** `screen(catalog_snapshot, feature_bundle, policy)` returns ranked opportunities with no capital permission. `admit_candidates(opportunities, native_market_readiness, capital_authority)` returns only bounded native proposals, each binding input and configuration identity. Do not interpret a full candidate list as a batch grant.

1. Maintain an explicit broad observation catalog and warm minimum signal history. Freeze the account-executable allowlist independently. Do not use today's catalog to reconstruct historical membership.
2. Add failing tests for a simultaneous many-symbol impulse with fewer slots, stale account eligibility, unready native order book/rules, missing context, delisting between selection and dispatch, rank churn and one stale instrument among valid peers.
3. Enforce deterministic ranking/tie-breaking and bounded preparation/queue budgets. Recheck eligibility and reservations at dispatch. Do not start a container or replace a controller owner per event.
4. Keep every owned/pending/recovery symbol in management after rank removal. Update selections without retroactively rewriting an existing position's exit policy or fingerprint.
5. For the first release, dynamically select only within the explicitly admitted native executable universe. New observations do not silently hot-enroll new execution markets. Continuous live market admission, if needed, gets its own verified native lifecycle step.

**Accept:** a real native fixture demonstrates many observed symbols, fewer permitted acquisitions, no oversubscription, persistent management and correct no-new-risk behavior. Run `"$HB_PYTHON" -m pytest -q test/modular_spot/test_screener_dispatch.py test/modular_spot/test_programs.py test/modular_spot/test_capital_admission.py` and the combined suite. Keep the program explicitly closed-bar; intrabar impulse is a distinct future contract.

## M8 — Independent review and release handoffs

**Files:** scoped docs/evidence receipts; future stack/API/research files only in separately admitted owner work. Do not mutate PR labels or deployment settings to manufacture readiness.

1. Run the entire new-controller suite in the exact native environment, relevant existing native regression paths and actual repository pre-commit/lint on changed files. Do not replace these with compileall or AST guards. Inspect skips and warnings, not only process exit status.
2. Obtain review from a genuinely separate actor against exact base/head and SPEC acceptance scenarios. Record actor identity, verdict, findings, fixes and final reviewed SHA. Author self-review is not an independent audit.
3. Prepare the stack adoption package: exact source/image/config/state identities, private data-only state and permissions, capability-scoped transport, admitted capacity/freshness limits, owner/backup/reconciliation receipts, tested alert delivery, startup/restart behavior and rollback. Existing execution main/SUI ownership and native-only API profile stay intact. No service launch in this docs/source run.
4. Prepare Research OS/quants-lab intake separately. Read current `RESEARCH_AGENT.md`, `RESEARCH_WORKFLOW.md`, `RESEARCH.md` and applicable program/data contracts. Resume an existing supervisor where applicable. No economic experiment without its own mandate; use the required cached scored window when current contracts require it, retain failures and unavailable comparisons, and distinguish technical replay from economic backtesting.
5. Report source-ready, native-verified, operationally-admitted, economically-evaluated and activation-authorized as separate fields. Keep a HOLD on each missing field. Profitability and hard maximum loss are not guaranteed by profit-only exits.

**Accept:** source review is evidenced; external handoffs are complete and honest. Operational or economic stages close only after their separately authorized actual runs, not when this plan is written.

## Validation record format and resumption

For every milestone append: date/time, owner repository, base/head SHA, runtime/image identity, exact argv and environment constraints, fixture/data provenance, exit code, pass/fail/skip counts, raw-log location/hash, changed paths, reviewer identity/verdict and remaining gate. Redact secrets and account identifiers not needed for verification. Structured evidence is authoritative; Markdown is its summary.

Example status wording: `M3 passed for the pinned 120-dataset fixture; native public-data soak not-run`. Do not state `all symbols supported` without the actual catalog and load bound. A native skip remains explicitly unavailable.

### Outcomes and retrospective

Documentation handoff prepared from the published PR and attached candidate on 2026-09-18. No implementation patch applied by this task. No new code, runtime, independent-audit, backtest or production result is claimed. Record documentation publication separately using the resulting Git commit and PR comment; do not invent a future commit SHA inside this file.

At each future stop, leave the first unfinished milestone, next exact command and blocker here. If blocked externally, include the failing gate, observed error, one-command diagnosis and exact required owner action. Continue independent authorized work but do not create filler tasks or relabel the blocker resolved.

## Idempotence, scope checks and rollback

Before and after every code slice inspect `git status --short`, `git diff --stat` and `git diff --check`. Commit only intended files. Never force-push, reset, stash/discard unrelated work, blindly reapply the patch or advance root gitlinks as an incidental step.

Documentation-only publication is reversible by reverting its commit. Unactivated source changes can be reverted after normal review. After any separately authorized execution, reverting code is not a safe owner migration: stop under the native procedure, reconcile pending/late fills and held inventory, retain backup/state receipts, and only then perform an admitted rollback. Never delete decision claims, lower accounting cost or overwrite a running database to permit restart.

### Copyable implementation handoff

> Read current root and owning instructions, then `docs/controllers/modular_spot/SPEC.md` and this PLAN.md. Resume at the first unchecked eligible milestone. Verify the current PR55 source and the attachment hashes; preserve docs-only descendants rather than rewinding to the old helper's exact head. Keep existing controllers, shared engine, live configuration and ownership untouched. Use real native tests and a separate reviewer. Record exact evidence and unresolved gates. Do not activate execution, touch accounts, run economic backtests, merge or deploy without separate authority.
