# Crash protection and recovery acquisition: v2 ExecPlan

Revision: 2 | Source review: 2026-09-20 | Status: PLAN ONLY; implementation milestones not started

**Goal:** Evaluate acquisition protection through isolated, versioned v2 recipes while preserving native ownership, legacy behavior and attributable research evidence.

**Architecture:** Canonical neutral components provide evidence, never spending authority. Strategy-owned recipes and an explicitly supported native adapter compose that evidence with existing capital/risk, order lifecycle and reconciliation. Research calls the same policies through Quants' existing native adapters.

**Tech stack:** The exact pinned Hummingbot Python/native environment, existing Pydantic/SQLAlchemy/market-data components, Quants BacktestingEngine and Research OS workflow. No new service, broker, feed, dependency upgrade or separate ledger.

**Spec:** [SPEC.md](SPEC.md). Read it with this plan. On a later authorized implementation, use the existing executing-plans or subagent-driven-development workflow and record task-level checks here. A real separate actor must perform an independent audit; author self-review is not a substitute.

## Authority, progress and continuity

The current request authorizes review and publication of documentation. It does not authorize implementation, economic execution, mutation of supervisor state, deployment, activation, source-pin changes or a merge. Use `execution.enabled=false` for investigation. Keep native candidate execution disabled.

- [x] Read the published crash spec and current canonical component/recipe/admission source.
- [x] Reconcile merged #55/#57/#59 and the root's actual component gitlinks.
- [x] Identify compatibility-profile inheritance, recursive component source seals and the economic-adapter gap.
- [x] Rewrite the spec and provide this gated plan with a redirect from the old document.
- [ ] M0: Admit an isolated v2 host, source package and research context for implementation.
- [ ] M1: Implement pure stress and recovery evidence contracts/reducers.
- [ ] M2: Compose a versioned v2 recipe and causal decision traces.
- [ ] M3: Prove native admission, acquisition revocation and legacy isolation.
- [ ] M4: Prove persistent state, funding, management and restore behavior.
- [ ] M5: Qualify Quants parity and the actual v2 economic-admission boundary.
- [ ] M6: Run one separately authorized, preregistered A/B comparison.
- [ ] M7: Admit only justified recovery/allocation follow-up trials.
- [ ] M8: Deliver source-bound evidence and a separate promotion handoff.

Progress boxes describe this task, not earlier foundation work. Do not mark a milestone complete because a foundation PR merged or a historical test passed. Before every resumed milestone refresh source identities and local state; preserve partial work and interrupted attempts.

## Global constraints and review focus

Freeze `ok_rsi`, `rsi_v5`, existing `rsi_modular` profiles, compatibility aliases, defaults, configurations, deployed images and persistence. New behavior lives in isolated v2 modules/recipes/adapters. Source hashing and exact-class admission are safety boundaries, not obstacles to bypass.

The five highest-priority review cases are: additive components changing old source seals; cancellation racing with late fills; same-event corrections/reconnects rearming a BUY; data/stress holds accidentally disabling exits; and an apparently healthy replay being misreported as a valid economic comparison. M0/M3, M3/M4, M1/M2, M3/M4 and M5/M6 respectively own those tests.

No numeric threshold, allocation or fee in a synthetic fixture is a recommended or calibrated trading parameter. The selected recipe must supply complete parameters explicitly. No experiment may use a narrower period to work around unavailable data or compute budget.

## M0 — source, host and capability admission

**Owners:** Hummingbot source/adapter; root integration and Research OS context. This stage does not change runtime or research records.

**Read:** root `AGENTS.md`, nested instructions when present, `RESEARCH.md`, `RESEARCH_AGENT.md`, `RESEARCH_WORKFLOW.md`, cached-data contract, current recipe/program registry, task-relevant receipts and lessons. Inspect exact source files linked in SPEC, root gitlinks, installed image identities when an authorized local environment is available, and concurrent work.

- [ ] Capture root and nested `git status --short --branch`, `git submodule status`, commit/tree IDs and dirty-file identities. Do not reset, stash or switch an unsafe dirty checkout.
- [ ] Select the existing isolated v2 host only when its actual source and capability tests support this recipe. Otherwise use the proposed `controllers/generic/crash_recovery_v2.py` thin adapter with `crash_recovery_v2_core/`; do not inherit an old RSI controller or copy a complete controller.
- [ ] Produce a capability matrix for native loader, recipe/source binding, runner risk, funding, acquisition revocation, per-tranche exits, restore and Quants replay. Unsupported entries remain HOLD. Document exactly which owner files need additive, narrowly scoped registration before M3.
- [ ] Freeze the baseline's entry policy, acquisition schedule, exits, universes and capital model. The candidate changes one treatment, not all entry logic simultaneously.
- [ ] Run the existing source-record generator on the baseline bundle and a disposable bundle containing one added component file. Record the changed composite seal. Preserve V1 packages/bindings and refuse a global reseal or hash-filter workaround.
- [ ] Inspect the existing research context and matching attempts using the established read/context procedures. A historical parity attempt is not this recipe's baseline. Resume related state rather than duplicate it; do not collect/rerun an owner without authority.

**Acceptance:** exact isolated host and source closure; explicit supported/unsupported matrix; unchanged old owner packages and semantics; an admitted engineering scope. If source or shared-owner isolation cannot be established, stop implementation with a concrete blocker. Resolving it must not migrate legacy owners.

## M1 — strict evidence contracts and pure reducers

**Proposed files:** `hummingbot/strategy_v2/components/market_stress_v1.py`, `recovery_evidence_v1.py`; `test/crash_recovery_v2/test_evidence.py`, `test_state.py`. No edits to canonical existing risk/acquisition algorithms or package startup behavior.

**Interfaces:** `evaluate_stress(state, snapshot, parameters, now_ms)` and `evaluate_recovery(state, snapshot, parameters, now_ms)` return immutable next state and evidence with status, reasons and observation references. Define the strict versioned input/output models in these files before consumers import them. All price/ratio/time fields have explicit units. No hidden clock, native balances or I/O.

- [ ] Write failing contract cases for boolean-as-integer, nonfinite prices/thresholds, nonpositive bounds, insufficient warmup, missing required parameters, unconfirmed candles, future availability, regressions and capacity exhaustion.
- [ ] Write transition tests for first-observation seeding, downside shock, stress cessation, stabilization, qualifying new low, reclaim, eligibility expiry and renewed shock. Data failure must preserve prior market state while blocking acquisition.
- [ ] Test identical observation replay and corrected same-event snapshots: neither advances a duration/count nor creates another acquisition event. Test epoch change with retained obligations and explicit cold-start recovery.
- [ ] Implement only the minimal deterministic reducers and explicit state validation. The absolute and normalized stress definitions must have separate identities; no hidden switching when volatility is unavailable.
- [ ] Run the focused cases; retain the original failing evidence and successful command/output in the milestone receipt.

A non-economic R0 test vector must pin the low and clock semantics. For example, with explicit fixture-only settings of a 120-second stabilization period and 3% reclaim: enter STABILIZING with an observed low of 90 and clock T; price 94 at T+60 seconds is still too early; unchanged valid low and price 94 at T+120 seconds may qualify; a qualifying new low resets the stabilization clock. The fixture binds each event's availability and cannot reuse the same event at multiple evaluation times to simulate new candles.

**Acceptance:** deterministic results and explicit invalidation for all transitions; neutral imports start no services and import no controller family; no orders, funds or economic claims.

## M2 — v2 recipe and trace composition

**Proposed files:** `controllers/generic/crash_recovery_v2_core/recipe.py`, `runtime.py`; `test/crash_recovery_v2/test_recipe.py`, `test_decisions.py`. Reuse `components/recipe_contract.py` and `decision_protocol.py` without changing their existing semantics.

- [ ] Test that the recipe binds module versions/parameters, feature dependencies, timing, universes, adapter capabilities, state schema and unchanged exit mandate. Reject unsupported combinations, incomplete parameters and accidental live mode.
- [ ] Reuse current `RecipeBinding` schema 1. Confirm stress mode and all thresholds affect behavioral configuration/source hashes. Do not add them to excluded control fields.
- [ ] Evaluate stress on every eligible observation even when no entry signal exists. Convert selected evidence to sourced PASS or HOLD/STALE/UNAVAILABLE prerequisites. Generate opportunities only from new eligible primary events.
- [ ] Test decision seeding, fixed evidence-relative expiry, duplicate consumption keys, correction/reconnect behavior, deterministic prerequisite ordering and rejection of future dependencies.
- [ ] Test that a recovery PASS cannot override runner pause, risk, budget, ownership or data failure. Multiple blockers remain visible. A sell-only scope cannot acquire, and BUY-only prerequisites are not attached to native exits.
- [ ] Test overlay-off parity against the frozen v2 baseline. Keep the overlay switch sealed; it is not a runtime escape from a hold.

**Acceptance:** one causal observation loop, one bounded opportunity path, immutable traces with `execution_authorized=false`; no widened `StrategyRecipe` family literals, legacy schema changes or copied signal logic.

## M3 — exact native admission and acquisition-only veto

**Owners:** selected Hummingbot v2 adapter plus narrowly scoped native support identified by M0. Relevant existing seams are `hummingbot/strategy_v2/controllers/native_policy_identity.py`, the real controller loaders/runner, and `controllers/generic/modular_spot_core/execution.py`.

**Proposed tests:** `test/crash_recovery_v2/test_native_admission.py`, `test_acquisition_revocation.py`, `test_legacy_isolation.py`.

- [ ] First prove a new adapter is rejected by unsupported existing paths. Then add only explicit genuine-type/capability support justified by M0; a controller name, prefix, subclass or spoofed legacy profile must not grant admission.
- [ ] Validate discovery and configuration round-trip through the actual native loader, selected executor factory, runner risk and source guard. Preserve the exact OK RSI accounting exception and old model identities.
- [ ] Recheck stress before proposing, at native admission/dispatch and while remaining acquisition is possible. Bind the permission evidence to the actual owner/recipe/intent; stale permission blocks new BUY effects.
- [ ] Use the native acquisition-only revocation path, not a new order callback or stop-all. Test revoke before submit, after submit/before acknowledgement, during partial fill, rejected cancel and a late fill after cancel request.
- [ ] In every case verify native commitments remain until terminal reconciliation and existing filled executors can continue approved protective exits. A fresh recovery signal cannot reuse uncertain capacity.
- [ ] Run transitive isolation tests, including old module/class identity, unchanged defaults/configs and source-seal invalidation. Test mutation rejection using actual package bytes, not mocked source hashes.

**Acceptance:** exact native capability support and all cancellation/exit cases in the real native environment without skips. A passing pure reducer does not close this gate. No new live owner is enrolled.

## M4 — state durability, management and funding

**Proposed tests:** `test/crash_recovery_v2/test_persistence.py`, `test_funding.py`, `test_management.py`, `test_restore.py`. Extend only the selected v2 adapter's state contract.

- [ ] Persist decision state before effects using the native recorder-owned pattern. Bind owner, recipe/source/config, dataset epoch/cursor and state schema. Missing/corrupt checkpoints with inventory or claims remain HOLD.
- [ ] Test restart in each market state, after intent persistence, during partial fill and pending cancellation. Restoring a checkpoint cannot clear a shock, replay a recovery event, reinitialize cost or release a commitment.
- [ ] Exercise actual controller/strategy funding scope with matching genuine peers; prove fees, retained basis, realized losses and pending quantities remain counted. Reject mixed modes/connectors, foreign owners and independently owned wallet state.
- [ ] Test removal from entry/screener selection while inventory/orders remain. Management data and exit ownership survive. Bound enrollment without evicting outstanding obligations.
- [ ] Test source/config change, same-shaped replacement controller instance, fee correction and additional native evidence after a hold. Market recovery cannot clear persistent risk or operational recovery restrictions.
- [ ] Test the selected executor subtype's actual restore path. New foundation resume support is not proof for this recipe. Unsupported restoration remains visibly held; do not call emergency recovery or reset the DB to make it pass.

**Acceptance:** native ownership and accounting stay authoritative through interruption; no second ledger, duplicate sale, automatic inventory adoption, budget reset or hidden exit-policy change.

## M5 — reuse Quants and close economic-admission gaps

**Existing owner files:** Quants `core/backtesting/native_modular.py`, `native_paired.py`, `native_session_replay.py`, `native_replay_process.py`, `native_receipt_verifier.py`, `engine.py`; `scripts/research/prepare_native_baseline.py`, `replay_native_modular.py`; corresponding `tests/test_native_*` suites. Extend supported capability dispatch rather than copy native policy or build another simulator.

- [ ] Prove the new recipe's decision/state parity using the same reducers, explicit clock and immutable snapshot inputs. Test native/Quants source/config drift, controller replacement, seconds-to-milliseconds conversion and cross-interpreter boundaries.
- [ ] Prove native lifecycle fixtures separately using genuine paper connector/session/recorder behavior. Mark synthetic data and absent runner rails explicitly; do not present those fixtures as historical economic evidence.
- [ ] Check existing Research OS program/result admission against the actual recipe. Record missing v2 binding as a blocker, never relabel it as `okx_spot_dca`, OK RSI output or a legacy import.
- [ ] Under a separately admitted owner scope, add the minimum schema-specific economic result/program adapter and regression tests. Validate actual full-window receipt and prepared/cache lineage, not merely declared timestamps or a process exit code.
- [ ] Verify existing cached stream bytes, continuity, instrument/venue, prices, event/availability semantics, warmup and execution-cost inputs. Missing required evidence blocks that capability; neither public fresh data nor a shorter window is a replacement.

**Acceptance:** a capability table separately marking decision parity, native lifecycle fidelity and economics. Economics may remain BLOCKED while engineering completes. No generic executable economic command is supplied here because no compatible crash-recipe runner/admission was established in this review.

## M6 — first authorized A/B comparison

Proceed only with explicit local research authority, a supported owner command, frozen data and a genuine independent design audit.

- [ ] Resume supervisor state and freeze one baseline/candidate comparison: A overlay-off versus B stress veto only. Freeze all source/config/data/cost inputs, seed, total trial/time/compute budget, activity floor and acceptance tradeoff before execution.
- [ ] Score the entire `2026-05-01T00:00:00Z` to `2026-09-01T00:00:00Z` half-open interval. Warmup is feature-only. Preserve one continuous capital/state process; no per-month or per-crash reset.
- [ ] Execute exactly the admitted Quants owner argv through the existing authorized Research OS runner. Retain failures, interruptions, holds and logs. An existing claimed process is inspected/collected, not silently relaunched.
- [ ] Audit results separately: marked equity, unfinished inventory, costs/fees, drawdown/tails, capital-days, activity, missed recoveries and execution assumptions. Zero exposure or completion cannot establish support.
- [ ] Append the supported/contradicted/inconclusive assessment through the authorized workflow; synchronize without rerunning the experiment. Existing source-scoped parity evidence retains its original identity.

**Acceptance:** valid owner-baseline comparison or a precise retained blocker/outcome. No automatic promotion or new trial follows from a favorable metric.

## M7 — conditional extensions, not an automatic search

Only after audited A/B evidence warrants more complexity, admit C (R0 recovery) separately; then D/E allocation treatments with identical campaign cap and stage count. Skip duplicates and retain all trial history. Compare parameter neighbors and dependent event/asset clusters under a preregistered budget, not an unbounded optimizer search.

R1 liquidity, R2 impact-decay and R3 learned recovery are separate future hypotheses requiring their own data/adapter evidence. Candle-only results do not qualify them. Composite, momentum or Kronos inputs must show incremental contribution; they cannot waive safety gates or supply averaged confidence as capital authority.

## M8 — verification and handoff

Record exact implementation/diff, affected recipes/legacy dependencies, command/exit/log receipts, source/package/config/state identities, remaining unsupported cases and rollback. Refresh admission after any changed transitive input. An independent reviewer must inspect the current artifacts, not approve an older checkpoint.

For later engineering validation, run the current owning workflow selection from `.github/workflows/modular-native.yml`; it includes the real native and legacy boundaries. A focused invocation from the Hummingbot owner checkout is:

```bash
: "${HB_PYTHON:?Set the verified native interpreter path}"
PYTHONPATH="$PWD" "$HB_PYTHON" -m pytest -q \
  test/test_strategy_components.py \
  test/test_strategy_component_source_identity.py \
  test/crash_recovery_v2 \
  test/modular_spot \
  test/hummingbot/strategy_v2/executors/position_executor \
  test/hummingbot/strategy_v2/executors/order_executor \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py
```

`test/crash_recovery_v2` is a proposed test directory created by M1, not a currently passing suite. The full current native/legacy selection and any affected API/root/Quants suites are additionally required for implementation acceptance. Run no services, install no missing dependencies and send no exchange requests as a test fallback.

For this documentation publication only, validate Markdown links/fences, source references, staged diff whitespace, exact three-file scope and remote blob identities. Native tests, independent audit, economic backtests and installed-runtime acceptance are NOT RUN by this publication.

## Rollback and stop conditions

Documentation rollback reverts only this three-file revision; the original spec stays recoverable at its pinned historical commit. Do not alter root pins, old PR metadata, source code or research stores.

For future code, separate small owner commits and retain source-bound checkpoints. A deployed owner with obligations requires authorized reconciliation and source/image-aware rollback while new entries remain held and native exits remain available. Never clear DBs, risk latches, claims or an active safety veto to bypass a blocker.

Stop for missing authority, unsupported owner/capital/restore capability, failed independent audit, incomplete cache/execution evidence, source drift or exhausted frozen research budget. Report the exact failing gate and next owner action; do not manufacture success with a new ID, narrower window or legacy label.
