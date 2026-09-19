# Flash-crash protection and recovery acquisition — specification

Date: 2026-09-19
Status: DESIGN ONLY — NOT IMPLEMENTED, BACKTESTED, DEPLOYED OR ACTIVATED
Owner: modular spot controller in Hummingbot
PR: s1korrrr/hummingbot#55
Inspected checkpoint: 808919077af8b0fa88736a0690a725def31fff44

## Objective

Add a reusable spot acquisition-protection capability that preserves capital during fast crashes and prolonged sell-offs, then permits bounded buying only after causal recovery evidence.

The goal is not to predict the exact future low. Optimize after-cost portfolio behavior: marked drawdown, capital committed before later adverse lows, stranded inventory/capital-days, net return, entry adverse excursion, and useful recovery participation. Average entry price and distance from a hindsight low are diagnostics only.

## Authority and ownership

This document grants no paper/canary/live, order, deployment, merge or capital authority.

Hummingbot remains authoritative for connectors, executors, orders, fills, cancellation, persistence and inventory lifecycle. The modular controller owns bounded decision state and policy composition. Quants Lab owns economic backtests. Research OS owns research evidence/outcomes. Condor/reporting are projections.

Remain 1x spot cash/inventory. No leverage, shorts, margin, funding or liquidation semantics. Do not add a second order engine, fill/PnL ledger, wallet authority or cross-process capital owner.

## Existing invariants to preserve

At the inspected checkpoint:

- later acquisition stages require fresh signal, terminal entry evidence, no distribution already started and a minimum drop from the previous actual fill;
- marked drawdown/daily-loss risk can latch acquisition;
- planner composes risk, spread, signal, acquisition and allocation checks;
- DCA tranches remain separate native positions, not a synthetic basket;
- profit-only selling may retain inventory and does not guarantee a maximum loss;
- execution_enabled=false remains default;
- uncertain native commitments survive signal expiry, cancellation request and restart until authoritative reconciliation.

Crash protection may make acquisition stricter. It must never weaken existing risk, budget, ownership, profitability, persistence or reconciliation rules.

## Scope

In scope: causal stress detection, acquisition veto, persistent stress/recovery state, recovery qualification, bounded recovery probe/DCA eligibility, renewed-shock invalidation, reason codes, deterministic/native failure tests, and research adapters required for valid comparison.

Out of scope: changing profit-only exits, basket liquidation, emergency loss selling, hedging/derivatives, new exchange sockets, account-wide allocator redesign, learned bottom prediction, increasing budget after a crash, or automatic activation.

## Core rule

Protection is asymmetric:

- stress evidence may remove acquisition permission;
- recovery evidence only makes acquisition eligible for normal admission.

recovery_eligible != acquisition_authorized

Recovery cannot clear a risk latch, create budget, release unresolved reservations, bypass spread/liquidity/data checks, override ownership, or enable disabled execution.

## Two-timescale evidence

### Fast protection

Where causally available, test short-horizon downside, robust volatility-normalized moves, spread expansion, executable depth deterioration, invalid/stale market data and sell-side flow pressure.

A candle-only candidate is allowed but must be identified separately. It cannot claim intrabar/order-flow protection.

### Slow context

Use finalized causal candles and admitted higher-timeframe context to distinguish transient shock from persistent repricing: drawdown from an already observed reference, lagged robust volatility, trend/regime context, time since qualifying low, reclaim/rebound, and stabilization persistence.

Never use future lows, future recovery paths, hindsight regimes or revised observations unavailable at decision time.

For normalized shock rules, use a lagged/frozen robust volatility reference or preregistered estimator that cannot immediately normalize away the crash.

## Market state machine

### NORMAL
Existing acquisition semantics apply. Valid stress -> SHOCK_HOLD.

### SHOCK_HOLD
Block new acquisition; revoke remaining acquisition permission through the existing native path where safely supported; preserve reservations/unresolved commitments; continue existing inventory/exit management. Exit only after the stress condition ceases under a causal rule.

### STABILIZING
Do not buy merely because price is lower. Track only post-transition observations, running low and recovery evidence. Renewed stress -> SHOCK_HOLD.

### RECOVERY_ELIGIBLE
The planner may consider a bounded stage, still subject to every existing risk/budget/execution check. Renewed stress invalidates eligibility. Recovery never resets consumed budget.

Restart must not reset SHOCK_HOLD, erase the running low, clear risk, free unresolved commitments, replay a consumed decision, or turn STABILIZING into RECOVERY_ELIGIBLE.

## Recovery families

R0 — deterministic time/price stabilization: no new qualifying low for a bounded interval + causal reclaim/rebound + acceptable spread/data quality.

R1 — R0 plus persistent executable liquidity recovery for intended order size. Narrow spread alone is insufficient.

R2 — R1 plus absorption/impact-decay evidence: selling pressure causes less downside impact and/or bid liquidity replenishes. Requires causal trade/book fields and fails closed if unavailable.

R3 — deferred learned recovery probability, e.g. P(recovery barrier before adverse barrier within H | information at t). It may support eligibility but never override hard rails.

Start with R0. Richer models require incremental evidence.

## Post-shock acquisition

Research: WAIT -> BOUNDED_PROBE -> REASSESS -> OPTIONAL_NEXT_STAGE.

Rules:

1. lower price alone never authorizes a stage;
2. each later stage needs fresh causal evidence;
3. total campaign budget is fixed before the shock;
4. stage sizing is finite and bounded;
5. compare equal vs back-loaded fractions with identical total budget and stage count;
6. later DCA retains meaningful improvement versus previous actual fill, including costs/executable price;
7. renewed shock invalidates recovery evidence but does not reset budget;
8. unresolved native reservations cannot be reused;
9. recovery scale-in and lower-price DCA are reported as distinct semantics.

A small deep resting-bid arm may be researched separately; it is not the default protection mechanism.

## Inventory behavior

This design protects new acquisition. It cannot promise a hard loss bound for already-owned inventory while profit-only exits remain mandatory.

During shock, existing inventory stays marked and counted; cost basis is unchanged; exit/trailing remains with its existing owner; retained inventory is not free capital; no synthetic liquidation or ownership adoption occurs.

All unfinished inventory remains in the economic result.

## Pure policy interfaces

Add private pure modules such as stress.py and recovery.py. Exact names follow current branch conventions.

Conceptually:

(previous_state, immutable_market_snapshot, versioned_config)
  -> (next_state, decision, reason_codes, evidence_refs)

Requirements: deterministic, no exchange/network calls, no balance reads, no hidden wall clock, no order submission, immutable inputs/outputs, explicit config identity, bounded persistent state, explicit correction/epoch semantics.

Persist decision state only: policy fingerprint, market state, transition time, observation identity, shock references, post-shock low, stabilization/recovery evidence, consumed recovery stage and invalidation reason. Never duplicate fill/accounting truth.

Corrupt/regressed/incompatible state fails closed for new acquisition.

## Native execution integration

Protection must be checked:

1. before a new acquisition proposal;
2. through the existing entry-permission mechanism for an admitted executor still acquiring, where native semantics support it.

Cancellation is not reconciliation. Tests must cover permission revocation before/after submission, partial fill then shock, cancel/late fill, rejected cancel, restart with unresolved cancellation, late fees/corrections, and recovered market while risk remains latched.

Do not add duplicate order callbacks/lifecycle ownership.

## Observability

Stable reason codes should include stress_downside_move, stress_volatility_move, stress_spread, stress_liquidity, stress_data_invalid, shock_hold, stabilizing, recovery_wait_new_low, recovery_wait_time, recovery_wait_reclaim, recovery_wait_liquidity, recovery_wait_flow, recovery_eligible, recovery_invalidated, recovery_eligible_risk_blocked and recovery_eligible_budget_blocked.

Condor may display these; it must not independently derive state.

## Research hypothesis

A causal stress veto followed by recovery-gated bounded acquisition improves downside/capital efficiency relative to unchanged modular acquisition without sacrificing enough useful recovery participation to erase the benefit after costs.

Falsify/hold if improvement is absent, caused by near-zero participation, unstable across neighboring parameters/assets/events, concentrated in one period, dependent on impossible fills/future information, or explained by omitted unfinished inventory/unresolved commitments.

## Initial experiment arms

Keep exits unchanged.

A — unchanged modular baseline.
B — A + stress acquisition veto only.
C — B + R0 recovery qualification with baseline stage sizing.
D — C + equal bounded recovery-stage fractions.
E — C + back-loaded fractions with identical total budget and stage count.

Proceed to R1/R2 only after simpler mechanisms have attributable evidence.

## Required corpus and provenance

Economic work must use rsibot policy cached-may-aug-2026-v1:

- 2026-05-01T00:00:00Z inclusive to 2026-09-01T00:00:00Z exclusive;
- existing verified cache only;
- no network replacement or shorter substitution;
- warmup is feature-only and creates no scored orders/PnL.

These months are already inspected; internal folds/months are diagnostics, not untouched OOS.

If current policy does not admit the modular owner/result schema, implement and independently verify the owner adapter before economic execution. Never relabel modular results as OK RSI evidence.

Where Binance spot proxy data supports an OKX hypothesis, label it proxy evidence, not native OKX queue/order-book validation.

## Scorecard

Primary: marked after-cost equity, max drawdown/duration, tail/downside diagnostics, capital-days deployed/stranded, unfinished inventory value/age, return on starting/deployed capital, turnover, fees and useful activity.

Crash-specific: quote committed before later adverse lows, budget consumed before event low, weighted-average entry, preregistered hindsight-low distance, MAE/MFE per fill, time to recovery acquisition, missed/unfilled recovery participation, false recoveries and opportunity cost versus baseline.

The hindsight low is evaluation-only.

Report by asset, month, causal event cluster and valid regime; account for dependence between correlated assets during one market-wide shock.

## Engineering gates

Before economic claims:

1. state-machine transition tests;
2. invariants for monotonic budget use and no risk bypass;
3. restart/corruption/config-drift tests;
4. stale/gapped/corrected-data tests;
5. partial-fill/cancel/late-fill/fee-correction tests;
6. native controller discovery/config round-trip;
7. native executor/orchestrator regressions;
8. diff/status checks;
9. genuinely independent design/code audit;
10. verified Quants Lab owner adapter and research-policy admission.

Synthetic tests are engineering evidence only.

## Implementation sequence after spec approval

M0 — refresh exact PR head, owner instructions, runtime/pins and research-policy state.

M1 — implement pure stress state machine, state schema and tests; no planner behavior change.

M2 — integrate stress veto into planner and native entry-permission path; prove cancellation/late-fill semantics.

M3 — implement R0 recovery state machine and persistence; recovery cannot clear risk or budget state.

M4 — compose bounded recovery acquisition with existing allocation/acquisition policies and observability.

M5 — implement/verify Quants Lab owner adapter if current cached policy cannot admit modular results.

M6 — preregister and execute A/B only. Audit result before adding recovery complexity.

M7 — if B remains justified, preregister C; then D/E sequentially only when predecessor evidence warrants them.

M8 — only after deterministic evidence, design R1/R2 richer-data candidates. R3 remains a separate later research program.

## Completion bar for this feature

Engineering completion requires deterministic/native failure-path evidence and no ownership regression.

Research support requires a valid owner-baseline comparison under identical data/capital/cost/execution assumptions and preregistered tradeoffs.

Neither state authorizes deployment or capital. Paper/canary/live promotion remains a separate operator-approved gate.

## Rollback

Until implementation, rollback is deletion/revert of this specification only.

For later code slices, every milestone must remain separately revertible. Reverting crash protection must restore the exact prior modular behavior without modifying native inventory, fill history or unrelated controller state.
