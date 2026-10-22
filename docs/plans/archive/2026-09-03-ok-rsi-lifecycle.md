# OK RSI inventory lifecycle implementation and comparison

## Goal

Implement the user-approved accumulation/recovery/distribution design as an
opt-in mode of the actual OKRSIController, shared by buy_sell and sell_only.
Produce fresh same-data baseline/candidate statistics with source/config/data
hashes and explicit execution limits. A negative result is a valid completion.
Owner runtime: Hummingbot; owner research: Quants Lab. Authority: research-only
local implementation, tests and backtests. No real bot start, operational YAML
change, exchange/authenticated call, database migration, push, PR or trading.
Spec: docs/rsi-v5/ok-rsi-episode-design.md (approved revision 2).

## Current State

Hummingbot baseline 91372522a8e4bb178a959f1b0c42ec7276cf89ba, original checkout
feat/andrzej_ok_rsi_history_migration. Isolated integration checkout:
/Users/s1kor/.codex/worktrees/ok-rsi-lifecycle-20260903 on
feat/andrzej_ok_rsi_lifecycle. Preserve every original/root/nested dirty file.
Prior tracked/untracked research lives in quants-lab/.worktrees/ok-rsi-sell-dca;
read/copy selected dependency source with hashes, never modify it.

Existing baseline calls HLRSI signal/DCA and per-buy trailing, held recovery,
and mark-based limits. Exact default remains unchanged. Existing clean owner
replay has strict aggregate-underwater guards and upper/lower fill worlds;
cached signal ledgers do not match current source and must not be reused as
current baseline. Current baseline validity will be measured afresh.

Baseline validation initially failed due absent compiled extensions in a fresh
Git checkout. Linked the 60 existing local .so files from the baseline checkout
without changing source. Then 51 tests + 19 subtests passed; four results failed
only because two HL test cases reference ignored operator YAMLs absent in a
clean checkout. Record or supply read-only fixture dependencies explicitly.

## Target State

### Frozen first candidate (no fitting to comparison results)

Use per-pair configured max_total_position_usd as episode all-in budget and
mark cap; keep starting cash 5,000 quote per independent asset and starting base
zero for buy_sell. Fractions .10/.15/.20/.25/.30, cumulative dislocations
2/3/4/5/6 times frozen 1h ATR14 from prior completed 20h rolling high. First
scout retains RSI reversal confirmation; later levels additionally require two
closed 1m bars without a new low and rebound max(.25 ATR1m,2 spreads), expiry
10 bars, new depth and at least .05 ATR1h basis improvement plus .25 ATR1m
last-fill gap. No catch-up stacking. Hostile trend permits .25 scout factor only;
cascade or invalid data blocks new requests. Other factors never exceed one.
The fixed cascade predicate is a closed 1m close below the preceding 1m close
minus three current completed-15m ATRs. Defensive/invalid/gapped observations
clear pending stabilization; a return to acceptable data needs fresh bars.

Recovery: completed 15m close above EMA20 with positive EMA20 slope. Markup:
completed 1h close above previous20h high, positive 1h EMA20 slope and 15m
EMA20 above EMA50; buy_sell also requires executable aggregate profit. Freeze
distribution reference/basis/ATR at activation. Extension stages 4/7/10 ATR1h
leave .8/.6/.4 of distribution-start inventory. Runner trail clipped 2 ATR15m
to .5%-4%, ratcheting only upwards; profit guard remains binding. Cooldown reset
requires a strictly later completed 1h close below EMA20. This is one test
candidate, not a tuned policy or operational setting. Quantile ladder, no-rebound
scout, learned model, portfolio optimization and broad parameter sweep are not
part of this first implementation/comparison.

### Interfaces and ownership

Pure core: dataclasses and Decimal; transition(config,state,market,updates)
returns Transition(state,requests,cancel_ids,reason). No filesystem/network/clock
reads. Config has mode, episode budget, mark cap, costs and explicit thresholds.
State contains active episode, level authorizations, pending requests, cumulative
native execution receipts, net base/remaining cost, gross spend, phase, ratchets,
setup and cooldown. OrderUpdate is cumulative per deterministic request ID,
gross base/quote, actual quote fee and actual base fee (never converted twice),
and final status. Duplicate receipts do not produce additional inventory.
MarketSnapshot binds completed-candle timestamp, BBO, OHLC, ATRs, prior HTF high,
causal context booleans, validity, actual wallet availability and trading rules.
Requests contain deterministic id, side, amount, limit price, all-in reserve,
episode/level/revision identity and expiry. Reserve before persistence/dispatch.

Runtime adapter overrides all action generation only when enabled. Native
order-only BUY with held-position completion avoids independent per-buy trailing
for new episodes. Unknown existing active/held inventory cannot be adopted
automatically. Explicit sell-only assignment is bounded by available base and
has its own identity/basis policy. Use existing RSIV5RuntimeStore append/load
under namespaced event types for atomic durable checkpoints, not its strategy
specific replay logic. Restore plus reconcile before actions. Native execution
must expose actual fills/fees and cap retries/partial amounts through a candidate
specific implementation so baseline semantics remain unchanged.

Workers use separate worktrees (one writer each). Root owns pure core and plan.
Runtime worker owns controller/config/context/store/episode executor integration
in its checkout. Research worker owns research runner/tests/reports in its
checkout. Integrate only intentional scoped changes after tests; local commits
may be used for transport, with no external push/merge.

## Risks and Failure Modes

- Actual OrderExecutor reports zero cumulative fees/filled quote; use native raw
  fill identity and currency, never silently infer fees from those zeros.
- Default sizing turns zero into 1.0 and uplifts minimums; candidate must reject
  before legacy factory and enforce final quantized all-in caps.
- Exit owners or retries may double reserve/sell. Keep one pending action and
  persist identity before dispatch, retain uncertain reservations, reconcile
  partial fills before replacements.
- New candle history may contain incomplete/gapped/lookahead observations. Use
  closed prefixes, complete HTF buckets, missing-input vetoes and prefix tests.
- Baseline aggregate underwater invariant may block economic admission. Preserve
  diagnostic results and label them; do not claim admitted improvement by
  silently changing its semantics or substituting a proxy baseline.
- Historical data is Binance proxy to OKX and lacks historical BBO/queue state.
  Report explicit modeled execution, bounds and cost sensitivity, not executable
  OKX returns. Never fabricate Sharpe/confidence from insufficient independent days.

## Milestones

### M1. Baseline, contract and source binding

- Confirm exact source, worktree state, current caches and runnable harness.
- Freeze comparison dates/data/cost world after metadata discovery, before runs.
- Save source/config/data identities; validate legacy focused tests.

### M2. Pure episode accounting and policy

- Files: ok_rsi_core/episode.py, test/test_ok_rsi_episode.py.
- RED/GREEN tests for 500 budget, 100 spent, mark75 => budget400; pending and
  partial receipts; fees; level idempotence; zero attenuation; sizing minimums;
  causal setup, defensive/exhausted, recovery/markup/distribution/cooldown;
  monotonic target/floor, profit guard, sell-only, serialization/restart.
- Add exact input validation, deterministic serialized state and config hash.

### M3. Actual controller integration and native execution

- Files: ok_rsi.py and focused ok_rsi_core modules; candidate order executor
  supporting owner files where native factory extension is necessary.
- RED/GREEN real controller action tests for both modes/default legacy,
  restart/unknown action/fill reconciliation, closed candles, loader behavior.
- Extract pure helpers only where cohesive; maintain old import/config identity.

### M4. Research and statistics

- Quants Lab runner imports actual baseline and candidate in isolated processes,
  drives identical causal source/cost/capital inputs and captures fill/state traces.
- Frozen baseline repeated deterministically. Accounting-only and full lifecycle
  candidate compared separately if harness allows exact one-factor isolation.
- Metrics: NAV/net PnL, drawdown, buys/sells, deployed/cash, fees/turnover,
  unresolved inventory and basis, episodes/levels, budget breaches, gate reasons,
  exit VWAP and same-horizon buy-and-hold. Include per-asset and chronological
  split rows, cost stress and limits; save JSON/CSV plus readable local report.

### M5. Independent review and verified handoff

- Review financial units, attribution, request persistence, owner runtime parity,
  baseline integrity, data causality and claim limits. Fix actionable regressions.
- Run final targeted tests and git diff --check/status for touched repositories.
- Record exact commands/results and run timestamps; retain unrelated work.

## Verification

From runtime checkout, with local Hummingbot env:

    /Users/s1kor/anaconda3/envs/hummingbot/bin/python -W error -m pytest -q test/test_ok_rsi_episode.py test/test_ok_rsi_controller.py --tb=short

Add actual integration/executor test filenames and exact replay commands when
implemented. Commands not yet run are never reported as passing. Native startup,
exchange fills and trading remain NOT RUN.

## Decision Log

- 2026-09-03: User explicitly approved building and backtesting revision 2; no
  repeated design permission is required. Local authority only.
- 2026-09-03: Fresh episodes use order-only executors; existing inventory/active
  executors require explicit reconciled assignment, not automatic migration.
- 2026-09-03: Reuse existing generic runtime event store, discovered after design;
  avoids a new database schema or file-based persistence implementation.
- 2026-09-03: One fixed ATR ladder is implemented first; optional quantiles and
  broader variants are not necessary to establish the first comparison.

## Progress Log

- 2026-09-03: Isolated runtime worktree ready, spec and earlier plans preserved.
  Baseline source and compiled-extension dependency identified. Runtime and
  research discovery agents running read-only. M1 in progress.

## Rollback / Recovery

Operational paths are untouched; retaining disabled candidate restores legacy
source behavior. Do not remove an active episode checkpoint or transfer exits
while outstanding commands exist. Reconcile/freeze at the same authority before
any runtime rollback. Preserve all source/data receipts and run logs. Worktree
changes can be inspected or reverted individually without changing original
repositories or their unrelated dirty state.

## Execution evidence and comparison contract

- Frozen data window: 2026-08-01 through 2026-08-28 inclusive; BTCUSDC,
  ETHUSDC and BNBUSDC independently. Use 72 hours of preceding 1m history for
  indicators, with no warm-up-period executions. Source is local Binance spot
  1m klines plus August aggTrade-derived seconds, with hashes in run artifacts.
- Model quote-denominated costs at 10/15/20 bps per side: assumed 8 bps fee,
  1 bp spread and 1/6/11 bps slippage. These are sensitivity assumptions, not
  measured OKX costs. Genuine trade seconds alone may prove modeled fills;
  synthetic empty-second backfill and observations after expiry may not.
- Preserve strict baseline invariant failures and separately show uncensored
  simulated runtime-action diagnostics, retaining losing actions. Neither row
  grants economic admission or trading authority. Add no-fill bounds and
  deterministic repeated-baseline evidence; perform no parameter fitting.
- Pure policy first implementation: 29 focused tests passed with warnings as
  errors. Review found late-fill/reset, restored boolean, stale defensive setup
  and missing-bar confirmation defects. Regression tests reproduced these;
  current policy preserves late inventory, old receipt identities and fresh
  confirmation requirements. Re-review is underway.
- Formatting: local Black (120 columns), isort and flake8 passed for pure core
  and its tests. The Hummingbot environment lacks Black; the existing global
  formatter was used without installing dependencies.
- Integrated runtime commit ee4d32a9f as 6fd4f36d3; pure core and runtime at
  6771156f1 passed 127 tests plus 19 subtests with warnings as errors. A further
  framework loader regression after config extraction was reproduced in both
  modes and fixed at 6f0cefa84 (two red, then two green tests).
- Review accounting amendments: validate restored totals and authorizations
  against cumulative receipts, restate remaining average cost for late fees,
  and adjust equity ratchets for accounting corrections. Missing-price base
  fees use the retained last valid causal bid. Pure suite now 36 tests green.
- Research adapter audit found native maker/chaser price adjustment was omitted
  for OrderExecutor actions. Apply the actual native get_order_price helper to
  both baseline and candidate before comparison. Initial baseline dollar rows
  are withdrawn and retained as superseded adapter diagnostics; final comparison
  must select only source-bound runs with the corrected shared adapter.
- Independent native review found incomplete factory authorization checks and
  falsely terminal zero receipts after cancellation without a native order
  object. Scoped runtime fixes and regression tests are in progress before
  final source freeze. Real exchange ordering remains outside this authority.
- Runtime source frozen at 01c234695800e84fbb2924eef83baed3c7a32a3c, including
  all reported factory/cancellation fixes. Final owner command:

      /Users/s1kor/anaconda3/envs/hummingbot/bin/python -W error -m pytest -q test/test_ok_rsi_episode.py test/test_ok_rsi_episode_runtime.py test/test_ok_rsi_episode_execution.py test/test_ok_rsi_episode_factory.py test/test_ok_rsi_episode_context.py test/test_ok_rsi_controller.py test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py --tb=short

  Observed: 159 passed and 19 subtests passed in 8.21 seconds. Scoped commit
  hooks (formatting/import order/lint/private-key detection) passed. Independent
  final review is rechecking the specific repaired cases, not widening scope.
- Fresh baseline signals and actual native market contexts completed for all
  three pairs, 40,320 decisions each. Corrected shared-adapter economic runs
  are now running against the frozen source with no candidate parameter edits.
- Bounded final review identified one linked recovery issue: explicitly transient
  missing native order/fill details must be rechecked without permanently
  stopping reconciliation. Reproduced two failures, fixed only that branch,
  then the final integrated command passed 161 tests plus 19 subtests in 6.07s
  at 102649fded90f8ae5c05ea12112b86990267dbec. Unsupported fee valuations and
  contradictory receipts retain hard holds. Independent reviewer cleared the
  fixes at this SHA with no remaining actionable findings in scope.
- Research was notified of the final source update. The synthetic complete
  receipts never exercise the new transient-error branch, but final source
  identities/equivalence must be recorded explicitly rather than silently
  assigning older results to newer bytes.

## Completed outcome

Local implementation and the predeclared comparison are complete. The final
runtime source is 102649fded90f8ae5c05ea12112b86990267dbec. Public ok_rsi.py
is 475 lines, compared with 702 in the baseline. Shared buy_sell/sell_only
policy is opt-in; original operator YAMLs and running systems were untouched.
The economic matrix evaluates buy_sell. Sell_only is covered by pure/native
runtime tests, not a separate historical inventory-assignment backtest.

Canonical research output:
/Users/s1kor/.codex/worktrees/ok-rsi-lifecycle-research-20260903/output/backtests/ok_rsi_episode_20260903/

Files: comparison.html, comparison.md, comparison.csv, weekly.csv, daily_pnl.png,
report_manifest.json, reproducibility.json, and cells/*/result.json with the
hashed source/config/data inputs and action/event/balance/decision Parquet ledgers.
The research preregistration is docs/reports/OK_RSI_EPISODE_COMPARISON_PREREGISTRATION_2026-09-03.md
in that worktree. Its frozen hypothesis requires higher PnL without worse
drawdown on each pair, retaining improvement at 20 bps per side.

All 33 canonical cells completed: 18 diagnostic cost rows, six strict audits,
six no-fill controls, and three baseline repeats. Five strict audits completed;
BTC baseline's strict audit stopped at an aggregate-inventory profit invariant.
The corresponding uncensored primary row retains six violations. All candidate
cells have zero native hold rows. No-fill books finish with 5,000 quote, zero
base and zero PnL. These are simulated diagnostics, not admitted owner economics.

Primary 15 bps results (USDC PnL; drawdown is uniform completed-1m NAV):

| Pair | Baseline PnL | Candidate PnL | Baseline DD | Candidate DD | Average deployed baseline/candidate |
| --- | ---: | ---: | ---: | ---: | ---: |
| BTC-USDC | 17.6412 | 38.6648 | 0.1063% | 0.4801% | 75.8791 / 238.4629 |
| ETH-USDC | 0.6066 | 33.7382 | 0.0259% | 0.3000% | 6.9349 / 150.2776 |
| BNB-USDC | 2.0782 | 31.4995 | 0.0854% | 0.2528% | 25.7335 / 151.9278 |

The fixed hypothesis is rejected on all three pairs: higher marked PnL comes
with higher deployment and worse drawdown. This also holds at 20 bps. Results
do not establish a timing improvement at matched deployment. Gains for BTC and
ETH concentrate in the Aug 15-21 rebound. No parameters were tuned in response.

Baseline repeats and final-source candidate reruns have byte-identical four
ledgers and metrics across each asset. Root independently verified all six
primary balance hashes, 40,320 equally spaced decision marks per book, exact
realized plus unrealized PnL reconciliation, and exact recomputed drawdowns.
Research verified all 33 bundles, hashing 10,802 unique files. The focused
research suite passed 16 tests; Ruff passed. The runtime suite passed 161 tests
and 19 subtests. Independent code review cleared every actionable finding in
scope. Existing runtime-store SQLite teardown warnings remain separately noted
in the runbook; that expanded invocation was not green.

Remaining limits: Binance proxy data; absent historical OKX BBO/queue/native
fill timing; optimistic full fills; assumed effective costs; one inspected
28-day window with no untouched holdout. Real native cancellation/fill ordering
is unverified. No paper/live activation or baseline promotion is authorized.
Retain the candidate disabled. Any later study should distinguish sizing from
entry/exit timing using a separately frozen comparison and untouched evidence.
