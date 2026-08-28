# PLAN

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
