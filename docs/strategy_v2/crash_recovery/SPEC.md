# rsibot-stack v2: crash protection and recovery acquisition

Revision: 2 | Reviewed source date: 2026-09-20 | Status: SPECIFIED, NOT IMPLEMENTED BY THIS CHANGE

Execution plan: [EXECPLAN.md](EXECPLAN.md). This replaces the controller-private direction in the [historical specification](https://github.com/s1korrrr/hummingbot/blob/cdafa2539e8bd6d51f4634e403b51192eb1cb2b7/docs/controllers/modular_spot/FLASH_CRASH_RECOVERY_SPEC.md). Its economic question remains; its implementation placement and readiness assumptions do not.

## 1. Objective, authority and legacy boundary

Test whether withholding new acquisition during market stress, then requiring causal recovery evidence, improves after-cost downside and capital efficiency without unacceptable loss of useful participation. Exact future-bottom identification and lowest average purchase price are not optimization objectives. A daily-chart sell-off and an intrasecond liquidity shock need distinct evidence capabilities.

This publication changes documentation only. It does not authorize implementation, experiments, graph mutations, paper/live activation, deployment, source-image replacement or inventory migration. For an investigation mandate use `execution.enabled=false`; native controller `execution_enabled` also remains false until separately admitted. These are different controls.

Freeze `ok_rsi`, `rsi_v5`, their configurations, defaults, persistence and deployed lifecycles. Existing `rsi_modular` profiles are compatibility consumers, not the new strategy architecture. Do not attach this behavior by inheriting, patching or reconfiguring them. Preserve all shared helpers and compatibility aliases they consume. Prove transitive isolation, not merely unchanged controller files.

The earlier attached modular-design discussion proposed OK RSI as the first refactor consumer. That recommendation is superseded by the user's v2 isolation decision. Its separation of acquisition, ownership, risk and execution remains useful; it is not current runtime evidence.

## 2. Current source evidence and superseded assumptions

Inspected root: `s1korrrr/rsibot@dbf2894ff95df3163df7d0b6ab2fff7ba8796e9b`. Its gitlinks select Hummingbot `0f19b464ccd112b21ac69c6de0e5515250239aac`, API `f77aac0864d409e907eabbcad7e8a040b64dcef1`, Quants Lab `894ba5295e49ea58145398a4c5c8dbf67a91db4d`, and Condor `a51a3d6498837ca606c0b7e2cebed4a53bbcd161`. Gitlinks are source selections, not proof of installed images or active owners.

Hummingbot PRs #55, #57 and #59 are merged. Do not push implementation to the old #55 branch or treat its earlier native-module skip as the current foundation result. Published validation receipts belong to their exact sources; none is a crash-policy economic result or a test rerun in this documentation task.

| Observed source | Consequence for this revision |
| --- | --- |
| `hummingbot/strategy_v2/components/README.md` | Reuse the canonical neutral library; stop proposing new generic machinery under `modular_spot_core`. |
| `controllers/generic/rsi_modular.py` | Its two profiles inherit genuine OK RSI/RSI V5 families. They remain unchanged references, not v2 crash-policy hosts. |
| `components/decision_protocol.py` | Reuse `ObservationRef`, `Opportunity`, `Prerequisite`, `DecisionCursor`, `DecisionTrace`; eligible never means authorized. |
| `components/recipe_contract.py`, `source_identity.py` | Reuse `RecipeBinding`. Source sealing includes every Python file under the component package when that dependency is detected. |
| `modular_spot_core/recipe_contract.py` | Existing `StrategyRecipe` schema 1 supports rebound, impulse and sell-only; it is not an unrestricted module-graph compiler. |
| `modular_spot_core/native_funding.py` | Controller and strategy scopes exist; strategy scope binds genuine configured peers in one recorder. This does not lock cash against independent V1 processes. |
| `hummingbot/strategy_v2/controllers/native_policy_identity.py` | Native modular admission uses exact model identities. A newly named v2 adapter is not automatically supported. |
| `modular_spot_core/execution.py` | The existing acquisition-permission hook and profit-only native specialization are reuse candidates, not universal adapter guarantees. |
| Quants `docs/backtesting/native_modular_replay.md` | Native feature/signal replay, bounded cross-ABI execution and paper-observation/session seams already exist. OHLC-only economics remains unsupported. |

The root research entry documents and cached policy retain their inspected earlier blob identities. The source registry still anchors `okx_spot_dca` to legacy OK RSI. A v2 recipe needs its actual owner/program/result binding; relabeling it as that legacy program is prohibited. Remote sources were inspected; local supervisor, cache, installed-runtime and live-account state were not observed.

## 3. Construction and proposed file ownership

Use the existing construction sequence:

`observations -> validated snapshots/features -> universe/context -> setup/entry -> sizing/DCA proposal -> risk/capital admission -> native execution -> reconciliation/reporting`

Exit and inventory management remain independent of new-entry selection and crash eligibility.

| Proposed addition; not present functionality | Responsibility |
| --- | --- |
| `hummingbot/strategy_v2/components/market_stress_v1.py` | Strict, bounded stress evidence and pure state reduction. No strategy thresholds chosen internally. |
| `hummingbot/strategy_v2/components/recovery_evidence_v1.py` | Causal running-low, stabilization and reclaim evidence. No order or capital authority. |
| `controllers/generic/crash_recovery_v2_core/recipe.py` | A versioned v2 composition selecting feature dependencies, evidence rules, acquisition schedule and compatible native adapter. |
| `controllers/generic/crash_recovery_v2_core/runtime.py` | Owner-scoped persistence and composition of evidence with existing native admission. No replacement ledger or order callbacks. |
| `controllers/generic/crash_recovery_v2.py` | A thin native adapter only if M0 finds no compatible isolated v2 host. Reuse a proven existing host instead of adding a redundant controller. |
| `test/crash_recovery_v2/` | Contract, isolation, state, admission and native failure-path tests. |

These names are proposed paths, not supported configuration keys or installed capabilities. Do not change canonical `acquisition.py`, `risk.py`, `config.py`, compatibility exports or `StrategyRecipe` defaults to make the feature appear everywhere. Keep strategy composition outside the neutral package. No service per module, new feed, supervisor, broker, wallet authority or fill/PnL ledger is needed.

Hummingbot owns native execution and inventory truth; API owns orchestration. Root owns pins, integration and packaging evidence. Quants Lab owns simulation/economic execution. Research OS owns intake, attempts and assessments; its reader remains read-only. Condor/reporting project owner-emitted traces and authorized controls only.

## 4. Evidence, timing and universe contracts

Reuse canonical datasets, snapshots, feature registry/cache, aggregation, stream/readers and subscription ownership. Do not create another socket per recipe. Deduplication is bounded by the actual supported publisher/process scope; a local cache is not cross-process infrastructure.

Bind dataset/venue/spot instrument/quote/interval/price type, producer epoch, revision, content hash, event time, availability time, feature implementation/parameters and initialization identity. Availability must precede the decision. Timestamp units are explicit: native replay inputs can use epoch seconds; decision records use integer milliseconds. Conversion belongs in a tested adapter, never a magnitude guess.

Use confirmed closed candles for the first candidate. Each required higher-timeframe row must already be final. Do not impute book, taker volume or trade fields as zero. Required input that is absent, stale, gapped, nonfinite or from a contradictory epoch blocks acquisition. Repeated snapshots do not advance counters or create opportunities. Corrections invalidate affected eligibility and evidence; they do not replay old buys or reset consumed intent.

Maintain observation O, entry E, management M and context C universes separately. Data obligations cover O union M union C. Removing an instrument from E never abandons its orders, inventory, pending cancellations or context required for safe management. Stress can make E temporarily empty while M remains active.

A fast book/trade veto is a distinct later capability. It requires causal native observation/replay support and measured detection-to-cancel-request delay. Finalized minute candles cannot promise subminute protection. Cancel acknowledgement and final fill reconciliation are separate measurements; neither detection nor cancellation guarantees prevention of fills.

## 5. Deterministic reducers and state semantics

Proposed pure interfaces are `evaluate_stress(state, snapshot, parameters, now_ms)` and `evaluate_recovery(state, snapshot, parameters, now_ms)`, returning immutable next state plus evidence status, reason codes and source references. Inputs include the clock; imports and evaluation perform no I/O. Validate finite values, units, strict types, resource limits and monotone event/clock cursors.

The recipe must explicitly bind downside horizon/threshold, volatility estimator and initialization, maximum input age, qualifying-low tolerance, stabilization duration, reclaim amount, recovery TTL and maximum state/history size. Thresholds are not calibrated by this spec. An incomplete parameter set is invalid, not a reason to select hidden defaults. Test absolute and lagged/frozen robust-volatility definitions separately; contemporaneous volatility must not silently normalize away the shock.

| State | Transition and acquisition meaning |
| --- | --- |
| `NORMAL` | Existing v2 acquisition prerequisites apply. A valid shock enters `SHOCK_HOLD`. |
| `SHOCK_HOLD` | Veto acquisition, invalidate recovery evidence, request native cessation of unfilled acquisition where supported; preserve inventory and commitments. Only valid new observations showing stress cessation enter `STABILIZING`. |
| `STABILIZING` | Track a causal low and stabilization evidence. A qualifying new low resets stabilization and invalidates prior reclaim evidence. Renewed shock returns to `SHOCK_HOLD`. A sustained, final reclaim can yield `RECOVERY_ELIGIBLE`. |
| `RECOVERY_ELIGIBLE` | A fresh, unconsumed opportunity may reach normal admission. TTL expiry, new low, renewed stress or invalid evidence removes eligibility. No automatic recovery-to-budget/risk reset exists. |

Data validity is an independent gate, not a fifth state that clears an existing shock. A missing checkpoint with inventory or unresolved commitments is a hold, not a new flat owner. A clean first observation seeds state and produces no immediate acquisition.

After consuming a recovery opportunity, later DCA requires a strictly newer primary event and fresh evidence. Persist market state, running low, entry time, evidence identity and policy hash through the existing native decision-state ownership pattern. Persist native claim references, not copied fills/cost basis. Bound state by declared datasets and active obligations; capacity exhaustion blocks enrollment rather than evicting managed inventory.

## 6. Recipe binding and deterministic conflict resolution

Reuse `RecipeBinding` schema 1 as implemented; product v2 does not imply changing its schema number. The new composition payload must seal ordered modules, versions/parameters, dependencies, feature availability rules, universes, account/capital model, adapter capability, state/decision semantics and unchanged exit/loss policy. New strategy fields stay in a separate versioned recipe schema, not a widening of existing family literals.

Use actual `Opportunity` identities, evidence-relative expiry and `consumption_key`. Revisions/reconnects must not rearm a previously observed event. `decide()` sorts prerequisite names, seeds a new cursor and returns only `HOLD` or `ELIGIBLE_FOR_NATIVE_ADMISSION`; `execution_authorized` remains false. Feed stress/recovery evidence into named `Prerequisite` records with sourced PASS and explicit HOLD/UNAVAILABLE/STALE. Keep evaluating protection even when no entry opportunity exists; the opportunity cursor is not the stress-observation loop.

Preserve all blockers and resolve deterministically: operational/ownership/data holds, native risk/capital restrictions and market-stress vetoes cannot be overridden by alpha. Recovery eligibility is necessary only for the selected recovery recipe, never sufficient for acquisition. It must not veto valid native reductions merely because a BUY feature is unavailable.

Kronos, momentum, composite context or other indicators may supply separately versioned evidence/proposals in later ablations. Do not average their confidence, let them clear safety vetoes or learn into live policy.

**Source-binding hazard:** current `source_records()` adds the complete component Python tree. Even unused additive module files change the seal of a component-dependent consumer in that package. Preserve old V1 images/source bundles and bindings; do not globally regenerate seals, filter files out of hashing, hot-reload a shared package or relabel old evidence. A fresh v2 bundle and explicit source-impact report are required. The documentation-only change adds no component Python files and therefore does not itself add that hazard.

## 7. Acquisition, funding and native execution

The sequence remains WAIT -> BOUNDED_PROBE -> REASSESS -> OPTIONAL_NEXT_STAGE. A cheaper quote alone never grants a stage. Every addition needs a fresh opportunity, current recovery evidence, a terminal/reconciled preceding acquisition, remaining campaign budget, fee-inclusive native cash availability and all applicable exposure limits.

The campaign cap is fixed before the event; another shock, restart, new signal, midnight or realized profit cannot create a new crash allowance. Reuse owner funding projections so held cost, pending quantity/fees and realized losses remain accounted for. Compare equal/back-loaded allocations with the same cap and stage count. Lower-price DCA must improve the actual executable purchase bound relative to the previous actual fill after costs. Recovery scale-in at higher prices is a separate treatment. Do not equate a displayed bid with an achievable fill.

Use `BudgetStore` or native funding only within its proven ownership contract; never count the same commitment twice or introduce a parallel reservation journal. Current strategy-scoped native funding binds genuine admitted peers with matching policies, unique owners/pairs and one connector/mode in one recorder. It does not constrain independent legacy owners in the same wallet. Candidate caps in source/config are not new allocation recommendations from this document.

Check the veto before proposal, again at native admission/dispatch, and while an admitted executor can still acquire. Reuse the existing acquisition-only permission mechanism only after proving it on the chosen adapter. A new v2 class is not currently recognized simply by name: exact native identity, source/config loader, runner risk, funding and restore support are separate owner gates. Never impersonate `rsi_modular`, widen its accounting exception or inherit a legacy controller to pass those gates.

Native cancellation must retain remaining commitments through ambiguous submission, partial fills, late fills, retries and fee corrections. Filled executors and permitted exits continue. No generic stop-all action, synthetic close or optimistic capacity release is allowed. Unsupported revocation/restore/funding behavior blocks execution of that recipe.

Retain native per-tranche accounting and the explicitly selected profit-only exit mandate for the first spot recipe. Average price is a report, not a new basket ledger or permission to sell an older lot below its floor. Market recovery eligibility is unrelated to operational recovery of an interrupted owner. Do not invoke emergency recovery, automatic restart or loss-selling from this module. A pause cannot guarantee a maximum loss for inventory already held.

## 8. Research and comparison gates

Resume existing Research OS state after reviewing related ideas, failures and source freshness. Do not rerun a frozen attempt or mutate the read-only knowledge service. The documentation task does not register a new experiment or claim to have inspected the local supervisor store.

Use existing Quants `BacktestingEngine`, native component/decision replay, native session/receipt verification and cross-ABI worker rather than another simulator. Feature/decision parity is distinct from native lifecycle fidelity and after-cost economics. Do not use historical `r11_inventory_free_modular_v2` or its ongoing/reported engineering comparison as the crash recipe's baseline.

First bind an actual isolated v2 baseline with unchanged exits, sizing and native semantics. Preregister:

| Arm | Sole intended treatment |
| --- | --- |
| A | Bound v2 baseline, crash overlay absent/off. |
| B | A plus stress acquisition veto, without recovery delay. |
| C | B plus R0 closed-candle stabilization/reclaim; baseline allocation schedule. |
| D | C with equal recovery-stage fractions. |
| E | C with back-loaded fractions; same budget and stage count. |

Skip an identity-equivalent arm and link the existing evidence rather than rerun it. Bind comparable source bundles and all transitive inputs; overlay-off is behavioral configuration, not an unsealed runtime control. Legacy references stay unchanged and separately labeled. After valid A/B evidence, C and allocation variants need separate bounded trial admission. R1 liquidity, R2 absorption/impact decay and R3 learned recovery remain deferred capabilities, not assumed fields.

All new economic runs score `2026-05-01T00:00:00Z` inclusive to `2026-09-01T00:00:00Z` exclusive under `cached-may-aug-2026-v1`. Existing cache bytes, continuity and prepared lineage must be verified before and after execution. Missing data blocks; no replacement download, shorter window or independent monthly state reset. Warmup is feature-only. Reused-corpus folds are not untouched OOS evidence.

Require a verified v2 program/result admission adapter; the current legacy OK RSI schema cannot be borrowed by changing labels. Cache-only candles may support engineering replay but cannot manufacture historical OKX books, latency, fills or fee lineage. Declare proxy execution assumptions and unsupported fields. Required missing execution evidence blocks the corresponding economic claim.

Freeze costs, latency/partial-fill/impact assumptions, venue rules, starting inventory, seed, budgets, activity floor, downside tradeoff and trial count before execution. Signal-close decisions cannot fill at that candle's earlier low. Include no-recovery paths and already-owned inventory. Preserve state/capital continuously and treat correlated symbols in one shock as dependent.

## 9. Scorecard and falsification

Primary: net marked equity on starting capital, drawdown magnitude/duration, tails, capital-days, unfinished inventory/value/age, concentration, useful entries/DCA/exits, turnover, fees and recovery. Unrealized losses remain risk.

Diagnostics: commitment before subsequent adverse lows, budget consumed within a causally defined event, fee-inclusive entry/MAE/MFE, delay to recovery acquisition, missed/unfilled recoveries and false recoveries. Hindsight event-low distance uses a frozen evaluation horizon and is never a runtime input or proof of an executable wick fill. Zero exposure cannot establish economic merit.

Reject or hold when benefit depends on negligible participation, one selected event/month, unstable neighboring parameters, omitted inventory, impossible fills, changed exits or source/owner mismatch. Register failed, interrupted, held, rejected and inconclusive attempts. Independent design and result audits require an actual separate actor. A self-review or process exit code is not independent economic support.

## 10. Acceptance, observability and rollback

Acceptance requires neutral-import and transitive-legacy isolation, deterministic data/state/clock/correction tests, native cancellation/late-fill/fee/restart tests, correct source binding, supported adapters and a reproducible comparison. Emit owner-bound reason codes with recipe/source/input/state identities; keep every blocker, not only a winning reason. Show market state separately from runner pause, funding hold, recorder recovery and data freshness. UI must not recompute signals or present stale health as readiness.

Engineering tests, merged foundations and native paper results do not promote this strategy. Deployment requires separate source/image/config/state admission, operator limits, reconciled real inventory, monitoring and rollback. Paper inventory cannot become real inventory by changing a mode flag.

For this publication, revert only the three documentation changes. For future code, preserve the admitted safety policy and safe native exits while entries are held; reconcile owner obligations before source rollback. Never reset DBs, erase checkpoints/claims or remove an active safety veto to clear a hold. No V1 owner restart is required by this design publication.

## Source locators

All source observations above are bounded to the inspected revisions; proposed paths and rules are design, not observed implementation.

- [Neutral component ownership](https://github.com/s1korrrr/hummingbot/blob/0f19b464ccd112b21ac69c6de0e5515250239aac/hummingbot/strategy_v2/components/README.md)
- [Decision protocol](https://github.com/s1korrrr/hummingbot/blob/0f19b464ccd112b21ac69c6de0e5515250239aac/hummingbot/strategy_v2/components/decision_protocol.py)
- [Recipe binding](https://github.com/s1korrrr/hummingbot/blob/0f19b464ccd112b21ac69c6de0e5515250239aac/hummingbot/strategy_v2/components/recipe_contract.py) and [full component source scan](https://github.com/s1korrrr/hummingbot/blob/0f19b464ccd112b21ac69c6de0e5515250239aac/hummingbot/strategy_v2/components/source_identity.py)
- [Compatibility profiles](https://github.com/s1korrrr/hummingbot/blob/0f19b464ccd112b21ac69c6de0e5515250239aac/controllers/generic/rsi_modular.py) and [bounded existing strategy recipe](https://github.com/s1korrrr/hummingbot/blob/0f19b464ccd112b21ac69c6de0e5515250239aac/controllers/generic/modular_spot_core/recipe_contract.py)
- [Funding scopes](https://github.com/s1korrrr/hummingbot/blob/0f19b464ccd112b21ac69c6de0e5515250239aac/controllers/generic/modular_spot_core/native_funding.py), [exact native identities](https://github.com/s1korrrr/hummingbot/blob/0f19b464ccd112b21ac69c6de0e5515250239aac/hummingbot/strategy_v2/controllers/native_policy_identity.py) and [existing executor hook](https://github.com/s1korrrr/hummingbot/blob/0f19b464ccd112b21ac69c6de0e5515250239aac/controllers/generic/modular_spot_core/execution.py)
- [Native replay capabilities](https://github.com/s1korrrr/quants-lab/blob/894ba5295e49ea58145398a4c5c8dbf67a91db4d/docs/backtesting/native_modular_replay.md)
- [Research entry](https://github.com/s1korrrr/rsibot/blob/dbf2894ff95df3163df7d0b6ab2fff7ba8796e9b/RESEARCH.md), [supervisor](https://github.com/s1korrrr/rsibot/blob/dbf2894ff95df3163df7d0b6ab2fff7ba8796e9b/RESEARCH_AGENT.md), [workflow](https://github.com/s1korrrr/rsibot/blob/dbf2894ff95df3163df7d0b6ab2fff7ba8796e9b/RESEARCH_WORKFLOW.md), [program registry](https://github.com/s1korrrr/rsibot/blob/dbf2894ff95df3163df7d0b6ab2fff7ba8796e9b/config/research_programs.json) and [cached-data contract](https://github.com/s1korrrr/rsibot/blob/dbf2894ff95df3163df7d0b6ab2fff7ba8796e9b/docs/research/cached-backtest-policy.md)
- [Historical source-scoped integration evidence](https://github.com/s1korrrr/rsibot/blob/dbf2894ff95df3163df7d0b6ab2fff7ba8796e9b/docs/reports/2026-09-20-strategy-components-integration.md); source merges [#55](https://github.com/s1korrrr/hummingbot/pull/55), [#57](https://github.com/s1korrrr/hummingbot/pull/57), [#59](https://github.com/s1korrrr/hummingbot/pull/59)
