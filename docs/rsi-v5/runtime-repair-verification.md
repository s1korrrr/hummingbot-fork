# RSI v5 Hyperliquid Runtime Repair Verification

Date: 2026-08-10
Component: `s1korrrr/hummingbot`
Branch: `feat/andrzej_rsi_runtime_repair`
Verified code SHA: `7a32222da304a392461f78493a94c9f71313ddb8`
Audited baseline: `1823dea50bef69aaebeff045fc4677a725420985`

## Readiness

This branch is locally `repo-ready` for review. It is not runtime-proven, paper-ready, canary-ready, beta-ready, or live-ready. No service was started, no connector or credential configuration was changed, and no order was submitted while producing this evidence. RSI signed execution remains disabled unless a separate deployment change activates it.

## Retained runtime invariants

- The persisted executor discriminator remains `position_executor`.
- RSI executor-class selection is scoped to the owning controller; importing the RSI controller does not mutate the process-wide executor map.
- A `SHUTTING_DOWN` executor contributes provisional settlement only. Episode PnL and loss streak change only after every member has an authoritative terminal settlement.
- Identical duplicate facts are idempotent. A changed final result, unknown owner, missing restored executor, corrupt replay, or persistence failure blocks new entries.
- Held short exposure and held long exposure without a reconstructable owner block new entries.
- The first executor atomically opens and joins an episode; an empty episode is not created.
- Episode open, executor registration, and stop intent are durably appended before their corresponding action is returned. A crash after the commit but before dispatch leaves a visible missing-executor hold rather than permitting a duplicate entry.
- Episode membership, provisional/final settlement revisions, pending exit retry timestamps, historical terminal identities, last episode outcome, consecutive losses, and last-loss time replay from the recorder database.
- Exits remain one `StopExecutorAction(..., keep_position=False)` per owning BUY executor. The controller does not create an aggregate opposite-side SELL executor against executor-owned exposure.

## Local commits

- `a8ab3c014f6f44744b87c5ec71c91da07866adbc` — controller-scoped executor construction.
- `f23620ca5c836edfc4d75175cab6366ac1f7fdf7` — revisioned settlement and deterministic lifecycle reduction.
- `7b33fa849620f7dee137d58f7bb183d5775a8c08` — signed exposure, orphan/short holds, and terminal-only accounting.
- `93c1bb50204432e5bdf4f44f9c7ad0c821201617` — append-only runtime persistence and replay.
- `7a32222da304a392461f78493a94c9f71313ddb8` — complete the Absolute Barrier test fixture state exposed by the broad Strategy V2 gate.

## Verification evidence

### Scoped formatting and safety hooks

```bash
conda run -n hummingbot pre-commit run --files \
  hummingbot/strategy_v2/controllers/controller_base.py \
  hummingbot/strategy_v2/executors/executor_orchestrator.py \
  hummingbot/strategy_v2/executors/rsi_position_executor/__init__.py \
  hummingbot/model/rsi_v5_runtime_event.py \
  hummingbot/model/__init__.py \
  hummingbot/connector/markets_recorder.py \
  controllers/directional_trading/rsi_v5.py \
  controllers/directional_trading/rsi_v5_hyperliquid.py \
  controllers/directional_trading/rsi_v5_runtime.py \
  controllers/directional_trading/rsi_v5_runtime_store.py \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py \
  test/hummingbot/strategy_v2/executors/absolute_barrier_position_executor/test_absolute_barrier_position_executor.py \
  test/test_rsi_v5_controller_import.py \
  test/test_rsi_v5_runtime.py \
  test/test_rsi_v5_runtime_store.py \
  test/test_rsi_v5_hyperliquid.py \
  test/test_rsi_v5_audit_edge_cases.py
git diff --check
```

Result: all configured hooks passed; `git diff --check` passed.

### Focused runtime and recorder gate

```bash
conda run -n hummingbot python -m pytest \
  test/test_rsi_v5_runtime_store.py \
  test/test_rsi_v5_runtime.py \
  test/test_rsi_v5_hyperliquid.py \
  test/hummingbot/connector/test_markets_recorder.py \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py \
  test/test_rsi_v5_controller_import.py \
  test/test_rsi_v5_audit_edge_cases.py \
  test/test_rsi_v5_live_configs.py \
  test/rsi_v5/test_position_executor_contract.py -q
```

Result: `194 passed, 1 skipped, 8 warnings in 6.40s`.

The skip is intentional in the existing suite. Warnings are existing Pydantic, pandas-ta, and event-loop deprecations; no new warning category was introduced by this branch.

### Broad Strategy V2 and RSI gate

```bash
conda run -n hummingbot python -m pytest --import-mode=importlib \
  test/hummingbot/strategy_v2 \
  test/rsi_v5 \
  test/test_rsi_v5_audit_edge_cases.py \
  test/test_rsi_v5_controller_import.py \
  test/test_rsi_v5_hyperliquid.py \
  test/test_rsi_v5_live_configs.py \
  test/test_rsi_v5_runtime.py \
  test/test_rsi_v5_runtime_store.py \
  test/test_rsi_v5_universe_shadow.py -q
```

Result after repairing the stale Absolute Barrier fixture: `544 passed, 1 skipped, 8 warnings in 6.16s`.

The first generated-file-list attempt was invalid because it duplicated paths and collected multiple modules named `test_data_types.py` with pytest's default import mode. The command above is the retained reproducible gate; the invalid collection attempt is not counted as a project test result.

## Approval-gated paper/testnet matrix

The following work was deliberately not executed. It requires a separate operator approval because it may start a connector-backed service or submit paper/testnet orders.

| Scenario | Required observation | Fail-closed acceptance |
| --- | --- | --- |
| Partial entry fill, then cancellation | Raw order/fill/cancel callbacks in both arrival orders | Filled base remains owned; no replacement entry; residual cancel is idempotent |
| Exit request, late close fill, late fee | Durable events and final connector PnL after restart | Episode stays open through provisional state and closes once with fee-inclusive final PnL |
| Duplicate and reordered callbacks | Exact event IDs and final state | Identical duplicates are no-ops; contradictory facts install a hold |
| Connector timeout/failure | Timeout, retry, and reconciliation logs | No blind duplicate order; missing outcome remains held for reconciliation |
| Restart during entry cancellation | Database ledger, executor restore, position restore | Missing executor or owner blocks entry; restored executor resumes without duplicate creation |
| Restart during exit | Pending stop timestamp and connector order state | Retry respects cooldown and never creates an aggregate opposite-side order |
| Unexpected short/net-negative position | Connector position and controller custom info | `unexpected_short` is visible and BUY admission stays blocked |
| Two unrelated controllers in one process | Resolved executor classes for both controller IDs | RSI uses `RSIPositionExecutor`; the unrelated controller still uses `PositionExecutor` |
| Spread, slippage, and latency | Bid/ask at decision, submission/ack/fill times, achieved price | Results include spread and slippage; latency is bounded by the approved paper policy |
| Fees and funding | Connector-reported fees and funding through final settlement | Net PnL is reconciled after all late adjustments; no preliminary promotion metric |
| Exposure and loss rails | Proposed/filled/pending/held notional and daily realized loss | Configured maximum exposure and daily-loss limits reject further entries |
| Monitoring and kill switch | Operator hold, connector health, ledger sequence, open episode | One documented command disables new entries and stops the paper service safely |

Before this matrix runs, the operator must approve the exact paper/testnet connector, account, symbols, maximum notional, maximum daily loss, duration, and kill-switch command. Credentials must come from the existing approved configuration path and must not be copied into logs or evidence.

## Rollback

While the feature remains disabled, revert the branch commits in reverse order. Preserve the runtime-event table for diagnosis; do not delete or rewrite its rows. A rollback removes restart durability, so the RSI Hyperliquid controller must remain disabled until either this implementation or a reviewed replacement is present.

## Residual risk

- Evidence is local and source-level; hosted checks and independent review have not yet run against a pushed head.
- SQLite sequence collisions fail closed. Multi-process writer behavior has not been promoted beyond that failure mode.
- The append-only table has no archival/retention job yet; operational growth must be measured during an approved soak.
- No connector-backed crash injection, paper/testnet soak, fee/funding reconciliation, or kill-switch exercise has run.
- Root integration must pin only a later Hummingbot commit that is merged to `master`; this local branch SHA is not eligible for a root gitlink update.
