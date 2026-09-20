# Modular spot programs and shared market data — specification

Date: 2026-09-18. Status: specified target with a partial implementation; not production approval.
Execution plan: [PLAN.md](PLAN.md). Owning PR: [s1korrrr/hummingbot#55](https://github.com/s1korrrr/hummingbot/pull/55).

## 1. Purpose, precedence and scope

Build independent spot strategy programs that reuse market observations and well-defined policy implementations while retaining native Hummingbot execution ownership. Support three operator-facing profiles: selected-symbol buy/sell, assigned-inventory sell-only, and broad-universe screening with bounded impulse acquisition.

**2026-09-19 scope clarification:** the intended product is reusable modular composition for the already working `ok_rsi` and `rsi_v5` workflows, retaining native Hummingbot execution. The current isolated controller is the integration foundation; its simple RSI/breakout policies are experimental baselines and do not establish strategy parity. The next strategy slice must reuse a selected existing policy and prove decision/configuration parity before adding new strategy families. Do not modify, redirect, migrate or enroll any running legacy owner during this work. The earlier instruction to develop an isolated family remains the implementation boundary, not a claim that different signals are equivalent to those controllers.

The initial publication was documentation only. M0-M8 subsequently produced an uncommitted implementation, and the 2026-09-19 task authorizes local validation and scoped repairs. This does not authorize economic backtests, deployment, live inventory handoff, PR publication, merge or activation. See PLAN.md and VALIDATION.md for source identities and observed evidence.

### Constraints retained from the approved direction

- Use native Hummingbot controllers, market-data/candle components, executor actions, connectors, executors, persistence and lifecycle interfaces. No alternate order-management engine or independently maintained fill/PnL ledger.
- Preserve the current execution owners, including the distinct main and SUI sell-only owners. Data sharing does not transfer inventory or capital ownership.
- Keep implementation changes in the new controller and its private components, tests and documentation. A required shared-engine, API, stack or legacy-controller change becomes a separately reviewed owner change; do not silently extend PR #55.
- Remain spot-only, long inventory, 1x cash accounting. A sell-only request reduces attributed inventory; it is not a short-position entry.
- Keep `execution_enabled=false` by default. Missing evidence, uncertain ownership or unavailable required inputs cannot admit new risk.
- Preserve approved profit-only/loss-realization policy. No automatic below-floor market fallback, cost-basis reset or loss-liquidation override.

## 2. Evidence baseline — do not confuse these layers

**Publication-time update:** while these documents were being prepared, the branch advanced to code checkpoint `b0fd5ef865fe88b87a9da8b8b10e3025d97576c2`. It adds overlapping data, profile, selection and budget helpers. The exact attachment below still has not been applied as a unit. The original code row is historical, not the complete current file inventory. Follow the current-source reconciliation in PLAN.md; do not overwrite or blindly reapply overlapping files.

| Layer | Identity and observed scope | What it establishes |
|---|---|---|
| Original published PR code | `6472dfece06ca71d9ebc6b887dda341ccdf5befb`, branch `feat/andrzej_isolated_modular_spot`, base `4df9ff3158a405b71d28b1c07446cfc7864a19fa` | Single-pair `ModularSpotController(ControllerBase)`, private policies, native PositionExecutor specialization, BUY acquisition, no external inventory adoption |
| Newer published code checkpoint | `b0fd5ef865fe88b87a9da8b8b10e3025d97576c2`: `candle_data.py`, `candle_transport.py`, `data_reader.py`, `feature_cache.py`, `native_bindings.py`, `profiles.py`, `program_budget.py`, `selection_state.py`, `universe.py`, and a `planner.py` change | Additional helper implementations observed in source; different schemas from the attachment; integration/native validation not established |
| Attached patch | `pr55_shared_data_universe.patch`, SHA-256 `a30dede9a1c0ad3a47be4936c4fecb5bdd55662faf2343a05c6d9f8485e92523` | Unpublished candidate shared-candle service/reader, universe-selection helpers and feature-result cache |
| Attached bundle | `pr55_shared_data_update.zip`, SHA-256 `4585bd99764efbe951a375a6c95cc8ea4e3b01cdc873d837ab104cb525ff6134` | Candidate bytes, manifest, patch helper, logs and prior validation receipt; not a remote commit |
| Prior patch receipt | `pr55_shared_data_validation.md`, SHA-256 `3be487817fe051027d2e3b5ffd00078db423cfe92e17e0bbb5239d8997dc31b0` | Reports 60 new focused tests passed and one entire native module skipped; partial reconstruction, not full native validation |
| Original design attachment | `Design Modular Trading Controllers.txt`, SHA-256 `4ef16c07cb2bce81b1c706efabbb76959c6cee52f6a82a2c3d9e7fd389062cc6` | Historical design rationale; later isolation instructions take precedence |

PR #55 separately records 61 original tests passed and one native module skipped. Do not add the two pass counts or describe either receipt as a newly executed combined suite. The patch receipt reports a failed native import. Native runtime, image, independent review, deployment and economic evidence remain unavailable in this handoff.

The attachment SHA-256 values were recalculated while preparing this specification. Historical test receipts were read, not rerun. The prior session's publication blocker describes that session; current publication success must be established by the new commit/PR receipt.

Read the original baseline and the newer checkpoint, then refresh the actual PR head before implementation. Do not assume latest upstream or the attachment describes current wiring:

- `controllers/generic/modular_spot.py` and `controllers/generic/modular_spot_core/`.
- `hummingbot/strategy_v2/controllers/controller_base.py` and `hummingbot/strategy/strategy_v2_base.py`.
- `hummingbot/data_feed/market_data_provider.py` and `hummingbot/data_feed/candles_feed/okx_spot_candles/okx_spot_candles.py`.
- `hummingbot/strategy_v2/executors/position_executor/`, `order_executor/`, `executor_orchestrator.py` and `planned_position_resume.py`.
- `hummingbot/connector/markets_recorder.py` and the native ownership/accounting models those paths use.

Repository pointers identify source, not the running image. Resolve root component pins, local instructions and deployment evidence again before implementation or operational work.

## 3. Profiles and universe semantics

A program is a versioned configuration and policy composition, not a new service or execution owner by itself. Names below describe target contracts, not currently supported YAML fields.

| Profile | Observations | New acquisition | Management |
|---|---|---|---|
| Fixed symbols | Selected symbols plus declared benchmark/context dependencies | Only eligible selected instruments within assigned limits; five selected symbols does not require five positions | All owned inventory and unresolved orders, including symbols removed from selection |
| Sell-only | Assigned inventory symbols and required exit context | Forbidden in proposal, reservation, factory, retry and rearm paths | Only explicitly admitted, reconciled inventory; three configured symbols is not proof of ownership |
| Screener/impulse | The explicitly configured broad observation catalog and required context | Ranked eligible candidates, limited by executable universe, capital, concentration and position slots | Retain every acquired/assigned position until native lifecycle obligations are resolved |

Let O be observation symbols, E entry candidates, M management obligations and C declared context dependencies. Required data is `O union M union C`. An entry additionally requires account eligibility, mandate eligibility, data readiness, current allocation and risk admission. Observation is never capital permission.

Management obligations include filled inventory, open orders, ambiguous submissions, pending cancellations and retained/recovery inventory. Removing a symbol from O or E cannot remove it from M. Position-slot and capital accounting must include these obligations. Multiple quote pairs for the same base asset must not evade base-asset concentration limits.

Membership evidence must bind catalog revision, source/account scope, observation time, effective time, expiry and instrument identity. Public listing is not account eligibility. New listings can become observable before they become executable. A suspended/delisted instrument is ineligible for new acquisition, but remains a tracked management obligation; do not synthesize a close or pretend it can still trade.

Shortlist hysteresis may reduce ranking churn only among eligible candidates. It cannot defer a hard exclusion. Broad screening calculates a small warm feature set; deeper features and native execution-market preparation apply to candidates. An unready execution connector/book/rule set blocks admission.

## 4. Market-data collection and distribution

### 4.1 Target ownership

Use one logical data publisher per identical dataset, independent of execution owners. Reuse native collection components rather than hand-writing exchange sockets. A collector has no trading credentials, writable execution DB, bot-start permission or execution-command topic permission. Its state is market-data state, not native order state.

Separate bot processes consume local replicas. Consumers cannot stop another consumer's source or mutate a shared DataFrame. Deduplication applies to an identical dataset subscription, not to all traffic or to the execution connector's private/account streams.

### 4.2 Candidate variants and first delivery contract

The attachment variant implements native OKX spot, trade-price, confirmed candles for `1m`, `3m`, `5m`, `15m`, `30m` and `1h`; a static consumer manifest; maximum required history per dataset; an exclusive-writer dedicated SQLite store; and scoped loopback HTTP snapshot reads. It polls native buffers, not a new exchange socket implementation.

It retains only two recent changed snapshots per stream. It is not MQTT distribution, an ordered raw-trade log, dynamic all-instrument discovery, an intrabar impulse feed or a historical point-in-time archive. None of those capabilities may be inferred from its existence.

The newer branch variant uses `Dataset`, `CandleSnapshot`, `Replica` and `SnapshotHub` in `candle_data.py`, with `data_reader.py` and a different `candle_transport.py`. Its interval vocabulary and profile/configuration fields differ from the attachment. Source presence is not equivalent integration or acceptance. M1 must choose one coherent contract, port missing safeguards and preserve required behavior; do not install both pipelines or silently union their configuration schemas. Start acceptance with confirmed one-minute OKX spot; additional intervals require explicit adapter validation.

**Exchange collection uses native OKX WebSockets; REST supplies initial/recovery history only.** Keep HTTP snapshot polling as the first local delivery contract. MQTT can be a later transport adapter only after capacity or latency evidence justifies it; its implementation must retain the same identity/recovery guarantees. Do not add multiple brokers or a centralized service per indicator.

### 4.3 Dataset and observation contract

Bind venue/source domain, spot market type, exact instrument/quote asset, interval, boundary convention and price type in dataset identity. Do not alias Binance BTC-USDT to OKX BTC-USDC. Requested window length is a consumer requirement, not a reason to duplicate an upstream stream.

Each snapshot must carry dataset identity, schema version, producer epoch, monotonic revision/cursor, complete ordered finalized bars, exact content identity and producer observation time. The reader records receipt time. A fresh local buffer read is not proof of a new exchange tick. Enforce candle age and transport age separately.

Use exchange confirmation semantics. Time passing alone is not candle finality. Missing, nonfinite, duplicate, misaligned or gapped rows invalidate the required window. Unsupported trade-count/taker-volume fields remain unavailable, not numeric zero evidence.

Corrections and changed epochs invalidate affected feature/admission state. Recovery obtains a coherent bounded window; it does not reissue historical BUY decisions. A correction should not be hidden by a cache hit or HTTP 304. If full as-of research replay is required, add a separately specified append-only observation archive preserving availability times; two snapshots cannot provide it.

### 4.4 Capacity and transport

Preaggregate consumer requirements before native startup. The initial manifest is static: reader requests cannot resize/restart a live shared feed. Future dynamic membership must use explicit versioned changes and native backfill/warmup, without interrupting unrelated active streams.

Bound dataset count, history, requests, response size, timeouts and concurrent work. Scope each consumer token to declared datasets and history. No tokens in YAML, examples or Git. Refuse redirects and unexpected origins; remote transport requires separately admitted TLS. Slow readers must not block the producer or another reader.

The proposed stack deployment remains a separate owner change. Do not assume the native-only API exposes the general API's market-data endpoints. Data-service restart must not restart or replace an execution owner.

## 5. Reusable feature contracts

Initially distribute candles and calculate strategy-specific features locally using reusable versioned code. Cache computed results only for genuinely identical contracts. The attachment cache is process-local, and the newer branch also has a different `feature_cache.py`. Compare their provenance contracts before choosing the implementation. Neither file name proves shared computation across processes.

Feature identity includes implementation hash/version, parameters, input fields/datasets, finality, initialization/checkpoint identity, missing-data policy and output schema. Result provenance additionally binds the actual input-window content/revisions and availability time. A name such as RSI(14) alone is insufficient. Learned outputs also bind model/scaler artifacts and training cutoff.

Check freshness and required-field availability on cache hits. Return immutable results. Calculate closed-bar features on eligible finalized updates or relevant corrections rather than treating unchanged histories as new signals. A dependency bundle must identify the eligible completed observation for every timeframe; never combine an unfinished hourly bar with finalized minute features accidentally.

The first impulse program is closed-bar only. Intrabar trading is a distinct future program requiring provisional/trade inputs, timing, revision and replay tests. Do not advertise sub-minute detection from finalized minute candles. Do not silently substitute resampled bars for native higher-timeframe bars.

## 6. Strategy composition and native execution

Keep two workflows explicit:

`eligible observations -> features -> opportunity -> acquisition proposal -> capital/risk admission -> native executor action`

`native fills/owned inventory + required current observations -> exit policy -> profit/quantity/ownership checks -> native execution`

A signal is a bounded opportunity with identity, instrument, timestamp, expiry, input references and configuration revision. It is not permission to spend. Exits and trailing continue independently of the presence of a new entry signal.

Keep the pair/position policy state independent; do not put every market into an unbounded controller dictionary. Fixed-symbol programs should first compose existing pair-specific modular controller configurations. A broad screener is a long-lived observer with bounded candidate dispatch through native controller/action interfaces, not a container launch per signal.

Exactly one native owner manages an attributed position's exits. Policies may propose competing reductions, but they cannot independently sell the same base inventory. Use the existing controller-local executor selector where its semantics are validated; never change global executor registration for this family.

The inspected planned-resume path can construct the base PositionExecutor directly rather than the local specialization. Until genuine native restore preserves the admitted policy, block unsupported resume. Do not remove the refusal, relabel a base executor or reset trailing/cost state to make restart pass. Any unavoidable shared-engine fix is a separate scoped change.

## 7. Capital, slots, inventory and sell-only admission

Local sizing consumes an assigned budget; it does not claim the whole wallet. The newer `program_budget.py` explicitly scopes its Metadata reservations to one native recorder/strategy configuration, not independently owned account databases. Its source presence cannot close the cross-process/account-wide gate. Use one existing logical capital authority per account or real account/subaccount segregation. An in-process lock does not coordinate separate processes, and allocating different symbols does not segregate shared quote cash.

Reservations bind account, owner/program, intent, asset, quantity/quote cap, fees, expiry and native order/executor identity. Admission and slot reservation must be atomic relative to competing new programs. Submission uncertainty, signal expiry, process absence or a cancellation request cannot release a commitment. Release follows authoritative terminal reconciliation. Risk limits include reserved acquisitions, marked inventory, retained inventory and per-base concentration; a recovery label cannot free capital or a slot.

Avoid a second fill ledger: any reservation journal references native receipts and does not invent balances, fills or PnL. Where existing authority cannot safely admit another process, require segregated accounts or keep the new acquisition path disabled. Do not enroll legacy owners silently.

External sell-only admission requires an explicit operator-approved native handoff with account/instrument, prior and new owner, available and reserved quantity, native acquisition-cost/fee provenance, reconciliation receipt and policy revision. Configuration declarations or current market price cannot supply missing acquisition cost.

The implemented sell-only profile is explicit profit-floor liquidation of an admitted lot, not RSI rollover selling or a trailing high-water policy. Its trailing configuration supplies the profit-margin floor only. Admission currently requires complete native quote-denominated acquisition fee evidence; unsupported base/third-token acquisition fees and nonzero flat/base-denominated exit fees hold until an exact native contract is supported. This restriction does not alter legacy sell-only owners.

A new sale must not exceed owned reconciled base minus outstanding sell commitments, with base-denominated fees and lot/minimum rules included. Use an applicable native reduce-inventory/order path, never a SELL PositionExecutor interpreted as a short entry. Disable buying, DCA, repurchase, pyramiding and automatic entry rearm at every boundary.

Profit eligibility uses native attributed cost and complete fee evidence. Preserve tick-rounded price-bounded execution without automatic market fallback. Unknown cost, unsupported fees, gaps below the floor and dust produce explicit holds. Price protection does not guarantee a fill or bounded mark-to-market loss. One program cannot sell another program's lot merely because the exchange exposes one net balance.

## 8. Observability, validation and promotion

Publish reason codes and identities for missing data, insufficient warmup, wrong epoch, incomplete fields, stale eligibility, no budget/slot, ownership conflict, unknown cost, recovery hold and blocked resume. Condor/reporting project these observations; they do not recompute strategy or accounting truth.

Required evidence is split into distinct gates:

| Gate | Minimum evidence |
|---|---|
| Source integration | Exact full checkout; combined original/new tests; scoped diff; actual lint/hooks; no legacy/shared-engine changes |
| Native semantics | Real native imports, discovery, factories, recorder, fills, fees, cancellation races, held inventory and restart behavior; no native-module skips counted as passing |
| Data service | Native feed reuse and confirmed values, bounded load at the admitted catalog/history size, reconnect/correction/gap behavior, consumer isolation and access controls |
| Program/capital semantics | Fixed-symbol routing, sell-only quantity and cost admission, bounded screener dispatch, atomic competing reservations and management retention |
| Operational readiness | Exact source/image/config/state receipts, unique owners, freshness, backup, actual alert delivery, verified recovery refusal and rollback |
| Economic evidence | Separately authorized quants-lab experiments with Research OS provenance, exact runtime/cost/timing assumptions and marked inventory outcomes |
| Activation | Separate operator approval and risk mandate; neither tests nor merge grant it |

Do not label the entire system production-ready because one gate passed. The existing receipts cover neither complete source integration nor native semantics. All new economic runs must follow the then-current research contracts, including the cached May–August 2026 scored window when still applicable; this documentation task runs none.

## 9. Acceptance scenarios and deliberate exclusions

The completed design must demonstrate: two isolated consumers sharing one identical upstream feed; different requested windows without feed churn; fixed five-symbol selection with bounded capital; three-symbol sell-only with no buy path; universe-wide observation with bounded eligible acquisitions; symbol removal retaining owned/pending/recovery management; per-instrument failure isolation; and no duplicate order/inventory ownership across restart or uncertainty.

Closed-bar functionality can be accepted without MQTT or intrabar strategies, provided that limitation is explicit. Full multi-symbol dispatch, external sell-only admission, aggregate capital enforcement and native recovery are not optional omissions hidden behind selection helpers. Keep their phases open until implemented and independently validated.

No legacy migration, perpetuals, forced loss sales, live-account inspection, deployment, root pin changes or economic experiments occur in this documentation publication. Follow [PLAN.md](PLAN.md) from its first eligible unchecked step, preserving current authority and recording the evidence from the actual next run.


## 10. Mean-reversion context policy — specified extension

Status: **research/specification only; no trading authority and no economic claim.** This extension reuses market-wide context as a secondary decision input for the modular controller. It does not make the existing Market Pulse scalar a standalone BUY/SELL signal and does not modify legacy controllers.

### 10.1 Decision boundary

For new acquisition, extend the pipeline to:

`eligible observations -> features -> strategy opportunity -> mean-reversion context -> acquisition proposal -> capital/risk admission -> native executor action`

The strategy remains responsible for identifying a dip/setup. The context layer may qualify the commitment; hard portfolio/risk/native checks remain authoritative. Position management and exits continue independently and are not disabled by loss of entry-context data.

The first implementation MUST be **reduction-only** relative to the strategy proposal: `0 <= acquisition_multiplier <= 1`. A favorable context cannot increase the strategy's baseline requested notional, bypass program/account limits, create a slot, or override ownership, freshness, reconciliation or profitability guards. If the reduced amount is below venue minimums, deny/defer rather than round exposure upward.

### 10.2 Do not bind authority to the current scalar

The existing Market Pulse composite mixes economic components (momentum, breadth, volume, volatility quality) with data freshness/readiness. Treat those as separate concerns:

1. **Context validity** — causal/final observations, coverage, universe identity, source/transport age, required fields and dependency alignment. Missing required validity fails closed for dependent *new acquisition*.
2. **Opportunity context** — broad momentum/breadth and their trajectory, volume/liquidity context, volatility state, and symbol-specific dislocation relative to a declared benchmark.
3. **Portfolio capacity** — authoritative remaining budget, reservations, inventory, concentration and slots. Context may reduce a proposal but cannot manufacture capacity.

Keep the current dashboard score unchanged for compatibility until evidence justifies a versioned replacement. New trading policy consumes a versioned component snapshot, not an undocumented threshold such as `composite_score > 70`.

### 10.3 Proposed contract

A context snapshot binds source/venue domain, quote universe, universe revision, component schema/version, finalized observation cutoff, availability/receipt time, content identity, coverage and expiry. Cross-venue context is explicitly labeled and never substitutes for local executable prices, rules or costs.

A policy evaluation binds the strategy opportunity/intent ID, instrument, program/config revision, snapshot identity and current portfolio-state version and returns only:

- `ALLOW | DEFER | DENY`;
- `acquisition_multiplier in [0,1]`;
- expiry;
- stable reason codes and diagnostics.

No component submits orders, mutates native balances, owns fills/PnL, releases reservations, resets cost basis, or transfers inventory ownership.

### 10.4 Initial use cases

Implement/evaluate in this order:

1. **Initial-entry admission** — qualify an already-valid mean-reversion entry; no context-only entries.
2. **Next-DCA-tranche admission** — decide whether the already-planned next tranche remains justified. Preserve current per-tranche native position/exit semantics; do not advertise basket-average DCA.
3. **Candidate ranking** — for broad screening, rank candidates from symbol-specific setup × global context × cost/capacity interactions. A single global score cannot rank symbols by itself.
4. **Sizing beyond reduction-only** — HOLD until calibrated incremental evidence exists.
5. **Exit/trailing adaptation** — separate later policy/research problem. Do not invert the entry score into SELL decisions or weaken the approved profit floor/protective state.

Flash-crash operational protection remains separate from experimental recovery-entry alpha. Closed-bar context must not be advertised as sub-minute bottom detection.

### 10.5 Causal research requirements

Before economic use, distinguish candle open, candle close, first availability/receipt and decision time. Historical reconstruction that marks rows ready/live with zero event age cannot validate live freshness behavior. Do not join a completed candle to its open timestamp as though close-derived features were known then.

Research must preserve point-in-time universe membership and missingness, include rejected/deferred opportunities as counterfactual observations rather than realized outcomes, and compare against unchanged owner baseline plus simpler context controls. Match exposure/cash when judging drawdown improvements so that merely trading less is not mislabeled alpha.

Use the repository's mandated cached May-August 2026 scored window and Research OS/Quants Lab workflow when an economic run is separately authorized. Register trial count, costs, partial fills, spread/slippage/latency, inventory mark-to-market, drawdown/duration, capital-days, unfinished inventory, activity and regime/fold stability. Internal folds are not untouched OOS evidence.

### 10.6 Acceptance and rejection

The layer earns enforcement only if source-bound evidence shows incremental value over the unchanged controller and a simpler trend/volatility/context rule after realistic costs and exposure matching. HOLD/REJECT enforcement if benefits are threshold-fragile, universe/missingness-dependent, dominated by reduced exposure, too stale for the intended horizon, or unsupported by causal availability data. It may remain an observability/research feature even if it never receives trading authority.
