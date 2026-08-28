# PLAN

## OK RSI unified history and inventory migration (2026-08-28)

### Task

- Objective: rename the OKX USDC runtime to `ok_rsi.yml`/`ok_rsi.sqlite`, shorten controller IDs to `ok-rsi-{asset}`, preserve the complete RSI V5 history with truthful Binance provenance, and restore only wallet-verified open inventory as fee-aware OK RSI bags that can never be force-sold.
- Owner repo: `hummingbot/`. Capital impact: live-adjacent local configuration and persistence migration. Target stage is repository/local-runtime preparation; unlocked startup, orders, cancellations, credentials mutation, withdrawal, or balance mutation are forbidden without separate approval.
- Approved design: `docs/superpowers/specs/2026-08-28-ok-rsi-history-migration-design.md`.

### Assumptions and constraints

- Final venue/pairs are OKX spot BTC/ETH/SOL/XRP/BNB-USDC with leverage 1 and controller IDs `ok-rsi-{asset}`.
- Final local profiles have `manual_kill_switch: false`, but the unlocked process remains stopped after a separate locked verification projection.
- Complete historical rows are copied without rewriting venue, pair, controller, order, or trade identity. Current OK RSI inventory is seeded separately and explicitly; no fabricated OKX fills are allowed.
- `rsi_v5.sqlite` remains immutable. Verified SQLite backups, checksums, one-transaction apply, idempotency, and pre/post integrity gates are mandatory.
- Wallet balance access is authenticated read-only. Secrets must not enter source, logs, receipts, tests, or status output.
- Legacy history cannot contaminate new OK RSI cooldowns, active executors, loss streaks, drawdown, or trailing state.

### Options considered

1. Point OK RSI at `rsi_v5.sqlite` and reuse it directly. Rejected: exact controller/connector matching prevents restoration, while new metadata writes pollute the legacy source.
2. Rewrite legacy Binance fills and controller IDs as OKX history. Rejected: it falsifies execution provenance and can revive stale risk/executor state.
3. Build a new `ok_rsi.sqlite`, copy complete legacy history unchanged, and create separately reconciled current-position seeds. Chosen: it preserves audit history while giving the current controller an explicit, testable cost basis.

### Execution and validation

1. Commit and review the approved design, then write the TDD implementation plan.
2. Add fail-first config, migrator, position-restoration, reporting, and no-force-sell tests.
3. Implement the pure migration planner/validator, transactional importer, receipt, and verification modes.
4. Rename/update local configs, committed fixtures, docs, and status/report surfaces.
5. Run dry-run on database copies, then capture wallet balances in a temporary locked read-only runtime.
6. Back up and migrate the real target, verify restored averages and first-sell economics, and run the locked startup/status/log gate.
7. Stop, set final kill switches false, leave unlocked runtime stopped, and close out exact evidence and rollback.

### Risks and rollback

- Principal risks: scaled-decimal misinterpretation, duplicate/colliding rows, stale position state, wallet/source mismatch, fabricated provenance, accidental unlocked start, and incorrect sell thresholds. Controls are model decoding, independent reconstruction, explicit mappings, fail-closed reconciliation, checksum-bound receipts, locked runtime proof, and no-order database audits.
- Fees, spread, slippage, latency, partial fills, and market impact are not used to claim an exact future execution price. The report distinguishes eligibility/arm thresholds from dynamic execution.
- Rollback restores checksum-verified backups and the prior local config names while leaving inventory untouched. No rollback step sells or cancels anything.
- Memory impact: record the final runtime identity, migration/receipt command, position-provenance boundary, no-force-sell invariants, and verified wallet/status commands after completion.

### Progress and current blocker

- Implemented and committed the canonical `ok_rsi.yml` identity, short IDs,
  explicit legacy mappings, all-false final manual kill switches, and the
  strict no-force-sell fixture contract.
- Added a loader-verified, one-shot OKX wallet snapshot strategy whose source
  contains no order/cancel/executor action calls. The current shell does not
  have the config password, so it was not started.
- Added the checksum-bound planner/importer/verifier, deterministic position
  and ledger seeds, receipt binding, online backup helper, and conditional
  first-sell economics. The focused warning-clean matrix passed 89 tests plus
  22 subtests before documentation closeout.
- Created integrity-checked online backups of `rsi_v5.sqlite` and
  `ok_rsi_buy_sell.sqlite`. SQLite online backups are logically equivalent but
  need not have the same byte checksum because page layout/WAL consolidation
  may differ; each backup has its own recorded SHA-256 and integrity result.
- The latest authenticated OKX runtime snapshot at
  `2026-08-28T17:59:02Z` proves BTC=0 and XRP=0, while the legacy source tracks
  BTC=0.04057 and XRP=1981.7. This is the exact fail-closed condition from the
  approved design. The real dry-run/apply and locked restoration smoke remain
  blocked; no `ok_rsi.sqlite` has been created and no synthetic position was
  inserted.
- Reversal condition: obtain a fresh authenticated wallet snapshot that covers
  all five legacy amounts, or explicitly revise the approved policy to migrate
  complete history while seeding only ETH/SOL/BNB and treating BTC/XRP as
  historical-only closed/off-wallet positions.

## OK RSI EEA USDC and locally derived higher timeframes (2026-08-28)

### Task

- Objective: move the operator's eight OKX spot instruments from USDT to the authenticated EEA-account USDC surface and derive configured higher-timeframe trend candles from the single base candle feed per controller.
- Owner repo: `hummingbot/`. Capital impact: live-adjacent configuration and signal-input behavior; no bot start, order, cancellation, balance mutation, or deployment is authorized.
- Venue/instruments: OKX EEA spot, leverage 1, one-way mode. Authenticated read-only evidence returned BTC/ETH/SOL/XRP/BNB/SUI/XLM/ENA-USDC as live and did not return the corresponding USD instrument IDs.

### Hypothesis and invariants

- Exact UTC-aligned aggregation of complete 1m candles produces the same completed OHLCV bars as native OKX 5m/15m feeds while reducing public feed count and rate-limit pressure.
- Configured medium and long intervals remain operator-editable. Only their data source changes: no separate higher-timeframe feeds are created.
- A derived bar is valid only when it contains the exact expected number of consecutive base bars. Incomplete, duplicate, misaligned, unsupported, or gapped input fails closed instead of creating a partial trend bar.
- The current incomplete base candle is excluded by the existing causal preprocessing before aggregation reaches signal logic.

### Options considered

1. Keep native 5m/15m feeds and only change pairs to USDC. Rejected because it retains the observed multi-feed startup/rate-limit burden.
2. Resample inside the shared `hl_rsi` controller. Rejected for this slice because the requested and account-verified behavior is OKX-specific and would change every venue specialization.
3. Override OK RSI candle configuration and interval lookup, backed by one pure aggregation function and strict config validation. Chosen as the smallest reviewable venue-owned change.

### Execution and validation

1. Add fail-first tests for exact OHLCV aggregation, gap/incomplete-bucket rejection, one-feed controller configuration, configurable interval derivation, and USDC loader profiles.
2. Add explicit higher-timeframe derived-history configuration, validate interval multiples, create only the base feed, and aggregate medium/long frames locally.
3. Update committed fixtures and local operator YAMLs to the authenticated USDC instrument IDs while preserving full editable controller settings.
4. Run focused tests, loader/config tests, warning-clean dependency tests, compilation, pre-commit, YAML parsing, and diff hygiene. Run only public/read-only candle parity if external verification is needed; never start a trading strategy.

### Risks and rollback

- Risks: aggregation boundary drift, insufficient 1m history, gaps hidden inside a bar, or different volume semantics. Controls: UTC epoch alignment, exact row-count and timestamp-spacing checks, configurable derived history, literal parity fixtures, and fail-closed empty results.
- Costs: fees, spread, slippage, latency, partial fills, and market impact are unchanged because entry/exit execution logic is not modified. This change is not promotion evidence.
- Kill switch/rollback: existing manual kill switch remains unchanged. Roll back the controller/config commit to restore native higher-timeframe feeds and USDT configs; no external state is changed by implementation or validation.
- Memory impact: record the EEA USDC account shape, one-feed aggregation boundary, required base-history sizing, and verified commands after validation.

### Final notes

- Implemented one base feed per controller. With the default 1m/5m/15m and 100 derived records, each feed retains 1,500 one-minute rows and both higher frames are produced locally.
- Aggregation converts numeric candle inputs, aligns buckets by UTC epoch, excludes active incomplete buckets, and omits any bucket with missing, duplicate, non-consecutive, non-numeric, or non-finite input.
- Updated the five editable local buy/sell YAMLs and compact three-pair sell-only YAML to direct USDC instrument IDs. Committed loader fixtures mirror the same pairs; local ignored configs remain operator-owned.
- Validation: fail-first tests produced nine expected failures before implementation and a separate unavailable-feed regression failed before its guard. The final focused suite passed 39 tests plus 19 subtests. The warning-clean controller/dependency matrix passed 331 tests plus 22 subtests. Actual local loader smoke returned eight USDC controllers with exactly one 1m/1,500-row feed each. Public BTC-USDC parity matched native OKX bars for 19 completed 5m and 6 completed 15m bars within `1e-8` OHLCV tolerance. Compilation, scoped pre-commit, YAML, and diff checks passed.
- No strategy, service, authenticated balance, or order path was started. This remains repository/config readiness, not paper/canary/live promotion evidence.

## OK RSI positional runtime-constructor regression (2026-08-28)

### Task

- Objective: repair the startup failure `HLRSIController.__init__() got multiple values for argument 'config'` reproduced when `StrategyV2Base.add_controller()` constructs `OKRSIController(config, market_data_provider, actions_queue)`.
- Owner repo: `hummingbot/` only. Capital impact: local runtime correctness; no service restart, credentials, config deployment, or order authority.
- Root-cause hypothesis: `OKRSIController.__init__` binds the positional config correctly, but then forwards it to `HLRSIController` by keyword alongside positional `*args`; Python assigns the first forwarded positional argument to `config` and rejects the duplicate keyword.

### Options considered

1. Change `StrategyV2Base.add_controller()` to construct this controller with keywords. Rejected because it would alter the shared runtime for every controller and hide a subclass compatibility defect.
2. Remove the specialization constructor. Viable, but broader than necessary because the subclass initializes OKX-specific trailing state.
3. Forward `config` positionally, matching `HLRSIController` and the runtime constructor contract. Chosen as the smallest compatible repair.

### Execution and validation

1. Add a fail-first regression that constructs `OKRSIController` with the exact positional signature used by `StrategyV2Base`.
2. Change only the superclass call to positional forwarding.
3. Run the focused OK RSI suite, the real loader/config path tests, compilation, pre-commit for touched files, and diff hygiene.

### Risks and rollback

- Risk: keyword-based test construction could regress. Existing focused tests cover keyword construction and will remain green.
- Rollback: revert the one constructor-forwarding line and its regression test. No external runtime state is changed.
- Memory impact: record the positional controller-construction contract if the repair is verified.

### Final notes

- Root cause confirmed: `StrategyV2Base.add_controller()` calls every controller as `(config, market_data_provider, actions_queue)`. Forwarding `config` by keyword while forwarding the remaining positional arguments assigned the provider to the base `config` parameter a second time.
- Repair: `OKRSIController` now forwards `config` positionally and retains its OKX-specific trailing-state initialization. Existing keyword-based construction remains covered.
- Validation: the focused warning-clean gate passed 33 tests plus 19 subtests; the full scoped controller/dependency matrix passed 325 tests plus 22 subtests. Changed-file compilation, hooks, and diff hygiene are required before commit. No service or order path was started.

## OK RSI end-to-end controller audit and merge-readiness gate (2026-08-28)

### Task

- Objective: audit `ok_rsi.py` and every directly affected loader, config, executor, market-data, inventory, status, and shutdown path against the checked-out Hummingbot source, current first-party documentation, repository standards, and realistic spot-execution constraints; repair proven defects; publish and merge only a coherent, independently reviewable nested-repository PR.
- Owner repo: `hummingbot/`. `hummingbot-api/` and `quants-lab/` are read-only parity/evidence inputs unless a concrete cross-repo defect is proven and separately planned.
- Target stage: repository merge readiness only. Runtime validation remains non-trading/research-only; this task does not grant paper, canary, live-config, credential, order, withdrawal, or capital authority.
- Current state: the controller, launcher helper, sell-only launcher, focused tests, and owner-repo plan/memory files started untracked on `feat/andrzej_ok_rsi_okx_sell_mechanics`. Local runtime YAMLs exist under ignored `conf/` paths and must not be mistaken for portable PR contents.

### Assumptions and invariants

- The execution source of truth is this checked-out Hummingbot runtime, not a research adapter or public documentation for a different version.
- `ok_rsi` is spot-only, accepts only `okx`/`okx_paper_trade`, requires leverage 1 and one-way mode, never opens a short, and must never exceed available base inventory on any sell path.
- A BUY executor must have a valid exit contract; disabled or invalid trailing cannot silently remove the ordinary exit when fixed stop/take-profit/time-limit are absent.
- `sell_only` may create only SELL executors from available wallet inventory. At quote value below 20 it targets all available inventory; otherwise it targets the configured fraction, with trading rules and balance remaining hard bounds.
- Recovery/held-inventory exits, ordinary signal exits, and executor-managed trailing are distinct state machines and must not double-sell the same inventory.
- Public docs are comparison evidence. The exact local base classes, executor config models, connector rules, and loader code determine version-specific behavior.

### Constraints

- Capital: no real connector startup, authenticated exchange call, live bot restart, order placement, cancellation, config deployment, or balance mutation. A public-data smoke may run only after proving that it cannot submit orders.
- Operations: do not stop or restart unrelated running Hummingbot/API/Research OS services. If the only realistic runtime path would load credentials or trade, record the limitation rather than manufacturing a green smoke.
- Git: preserve unrelated dirty root/quants-lab work. Commit only intentional files in `hummingbot/`; push/PR/merge only after fresh owner checks, review, and successful CI.
- Config portability: ignored `conf/` files are local operator state. Tests and PR behavior must use safe committed fixtures rather than depend on local YAMLs.
- Security: no secrets in source, fixtures, logs, plan, or PR. No Codex Security scan is authorized; perform ordinary secure-code review only.

### Options considered

1. Review only the new Python files and merge after focused tests.
   - Pros: fastest.
   - Cons: misses loader/runtime boundaries, clean-checkout portability, executor behavior, service logs, and ignored-config assumptions; rejected.
2. Run the configured OKX bot and inspect live behavior.
   - Pros: strongest exchange-path evidence.
   - Cons: can place real orders and load credentials; unsafe without distinct live-capital approval; rejected.
3. Trace and test the complete local path, repair only proven defects, run isolated lifecycle and public-data-only startup smokes, then harden and publish one Hummingbot PR.
   - Pros: best balance of correctness, reproducibility, and capital safety.
   - Cons: cannot prove authenticated fills, exchange latency, or capital behavior.
   - Choice: selected.

### Scope and execution plan

1. Inventory source/config/caller ownership and compare with `origin/master`.
2. Trace config, market data, signal production, executor actions, inventory, recovery, trailing, status, loader, and shutdown against local source.
3. Verify current first-party Hummingbot and OKX contracts.
4. Add fail-first boundary tests and reproduce every suspected defect.
5. Repair confirmed correctness, portability, maintainability, observability, and performance problems without changing the shared signal strategy.
6. Validate native loader discovery, no-order controller lifecycle, and a public-candle-only process.
7. Run scoped compilation, pre-commit, warning-clean regression, dead-code/debug-marker, diff, and status gates.
8. Review the complete diff; publish and merge only if all repository gates remain green.

### Test and evidence matrix

- Config/model: venue, leverage, mode, pair, candle intervals, exit/trailing invariants, finite inputs, duplicate pairs, YAML, and defaults.
- Controller: causal completed candles, stale/missing data, BUY/DCA/SELL gating, balances, exposure, cooldown, recovery, telemetry, and lifecycle idempotency.
- Execution: action contracts, spot sides/amounts, order types, triple barrier, quantization/minimum notional, stale entry cancellation, and shutdown.
- Integration: `TradingCore.load_v2_class`, `v2_with_controllers`, sell-only discovery, market initialization, and five-pair/three-timeframe public OKX startup.
- Static: compilation, changed-file pre-commit, YAML/secret checks, `git diff --check`, and focused debug/dead-marker review.

### Risks, rollback, and merge gate

- Main risks: copied shared logic, ignored runtime configs, status/executor divergence, unsafe balance fallback, and bursty per-feed REST behavior.
- Shared-throttler invariant: key by candle connector, inject only the throttler, preserve per-feed connections, and cover same-connector reuse plus different-connector separation.
- Rollback: revert the focused Hummingbot merge and do not launch ignored runtime YAMLs. Local validation changed no external runtime or capital state.
- Merge blockers: unresolved P0/P1 correctness issues, warning/error on the real loader path, untracked fixture dependency, incoherent diff, failing relevant CI, or semantic mismatch between config, action, and executor behavior.

### Final notes

- Architecture: replaced the 5,030-line OKX copy with a 549-line specialization of `hl_rsi`. Shared signal, regime, DCA, inventory, recovery, and lifecycle behavior now has one source of truth; OKX owns only its venue/config contract, proportional sell-only sizing, continuous trailing selection, and fail-closed execution inputs.
- Confirmed repairs: paper execution maps to public `okx` candles; malformed/cross-pair/unsupported-interval/non-finite configs fail; buy/sell cannot silently lose its only ordinary exit; ignored inherited exits are rejected; LIMIT_MAKER entry orders expire after 30 seconds; missing ATR/price/trading rules/quantization block actions; SELL uses available balance only; loader tests use committed fixtures; trailing economics are published in `custom_info`; a private test-only status helper was removed.
- Runtime: a five-pair x three-timeframe public-only startup reproduced HTTP 429/code 50011. `MarketDataProvider` now injects one candle throttler per connector without sharing feed connections, and OKX spot candles reserve 50% of the documented IP-wide history/time budgets. The cold rerun reached 15/15 ready feeds with 20 records in 10.321 seconds, emitted no warnings/errors, used one throttler, and left zero candle tasks after stop.
- Official-doc alignment: the controller consumes `MarketDataProvider` and emits executor actions, native buy/sell configs load through `v2_with_controllers.py`, script YAMLs live in `conf/scripts`, controller YAMLs in `conf/controllers`, and BUY lifecycle is delegated to `PositionExecutor`/triple-barrier trailing.
- Validation: 324 tests plus 22 subtests passed in one ordered matrix with `python -W error -m pytest`; focused OK tests passed 32/32; scoped pre-commit, YAML, flake8, autopep8, isort, secret checks, Python compilation, and `git diff --check` passed. Python 3.13 owned-loop test setup and a fake AsyncMock task were repaired so the matrix is order-independent and warning-clean.
- Full-suite runner: `conda run -n hummingbot make test` initially collected with global Python 3.12 because the Makefile invoked a bare `coverage` entry point, while the active Hummingbot environment and compiled extensions use Python 3.13. After binding coverage to the active interpreter, collection exposed duplicate `test_data_types.py` module names in separate executor directories. The Makefile now invokes `python -m coverage` and pytest `--import-mode=importlib`, binding the environment correctly and isolating same-named tests.
- Full-suite result: the repaired command collected 6,731 cached node IDs and executed beyond collection, but the legacy repository suite did not produce a green terminal result: it retained a Vertex hard-coded timestamp failure and waited indefinitely in Python `_Py_Finalize` for background threads. A detached clean `origin/master` worktree reproduced the Vertex failure (`1 failed, 1 warning`), proving it is pre-existing. The event-loop wrapper regressions exposed during triage were repaired and now pass 15/15; the controller dependency matrix remains warning-clean at 324 tests plus 22 subtests.
- Publication gate: GitHub Actions could not start the required job because the account Actions budget prevented execution (`INFRA_FAILURE`), and the build job was skipped. On 2026-08-28 the operator explicitly authorized merging while ignoring Actions/CI. After the positional runtime-constructor repair, the scoped warning-clean controller/dependency gate passed 325 tests plus 22 subtests. The clean-base Vertex failure and owner-suite shutdown leak remain documented pre-existing risks; this override grants repository merge authority only and does not grant paper/canary/live or order authority.
- Review: no P0/P1 correctness, maintainability, ordinary-security, or performance issue remains in the scoped diff. No secrets, authenticated exchange calls, order placement, service mutation, silent executor fallback, debug prints, or new dependency were introduced.
- Residual non-merge blockers: `recent_trades_lookback` remains an inherited compatibility/no-op field in the shared RSI family; separate Hummingbot processes cannot coordinate one IP budget; authenticated balances/fills, spread, slippage, latency, partial fills, and exchange cancellation remain unverified. There is no portfolio-wide exposure allocator or calendar-day loss reset across scripts. These block paper/canary/live promotion, not repository merge.
