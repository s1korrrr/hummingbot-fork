# OK RSI audit remediation

## Goal

Fix all findings from the 2026-09-08 personal OK RSI audit, verify the native runtime paths with adverse event sequences and manual source review, and hand off exact evidence. User authorized local repairs and validation after the read-only audit. Completion means every F1–F18 has a resolved defect or explicitly justified behavior contract plus relevant checks; no required repair silently deferred.

## Current State

Owner: `/Users/s1kor/dev/trading/rsibot/hummingbot`, starting HEAD `91372522a8e4bb178a959f1b0c42ec7276cf89ba`, branch `feat/andrzej_ok_rsi_lifecycle_repairs`. No tracked source changes existed. Preserve unrelated untracked ARTIFACTS.md, conf password copy, docs/reports, two episode docs, output. Root and other nested dirt remain outside scope.

Audit: `/Users/s1kor/.codex/visualizations/2026/09/08/01a080af-8174-7de2-834a-508609d0d5e6/ok-rsi-audit/audit.md`. Native reproductions beside it establish partial-fill loss, timeout bypass, stalled trailing fallback, PnL and inventory misvaluation, recovery cancel race, indicator API mismatch and partial config mutation. Earlier 380 assertions passed despite defects; no reliance on that total for readiness.

## Target State

Authority ceiling: live-adjacent local implementation and non-destructive validation. No orders, services connected to exchange, credentials, deployment, live/paper/canary activation, external publication, push or merge. Preserve spot cash/inventory semantics. Own source, focused tests, and this plan only; root gitlink unchanged. No new research/backtest campaign.

Preserve all fills once across cancellation and retries; wait for cancel acknowledgement before replacements; protect partial entries; execute trailing fallback through the real loop. Correct mark-to-market and reserve pending exposure. Admission must respect current risk state. Signals and reload must fail visibly on unavailable inputs. Restart must reconcile ownership and risk safely before admitting new orders. Recovery cannot silently violate its profitability condition.

## Risks and Failure Modes

Shared executors also serve other strategies; cover their compatibility. Fees may be base or quote, rebates, and partial/late fills. Reconciliation may double count active executors and historic held PnL. Restore must not place duplicate orders. Stop with keep_position must not claim flatness. Strict closed-candle or config changes need explicit documentation. Unknown exchange state must not become permission to retry. Native compiled owner dependencies must be used.

## Milestones

### M1. Position lifecycle and accounting (F1–F4, F14)

Add failing event-sequence regressions; preserve cumulative entry/close orders and fees, use remaining amount, value unsold inventory at current market, handle partial-entry timeout/protection and normal-loop fallback. Files: position_executor and focused tests. Expect original counterexamples corrected, late/duplicate events idempotent.

### M2. Recovery orders and exchange ambiguity (F13, F18)

Serialize chaser cancellation/replacement, preserve all partial fills and remaining quantity, retain profitability floor, and keep uncertain status failures reserved without retry. Files: order_executor/config, HL RSI recovery, OKX/ExchangePyBase and tests. Expect no overlapping replacement before acknowledgement, no sell below configured floor, transport outage cannot synthesize rejection.

### M3. Inventory, PnL and restart (F7, F9, F15)

Use net remaining ownership rather than turnover, exactly-once fee/net PnL across held and executor history; restore/reconcile active execution and risk state or explicitly latch safe recovery pending reconciliation. Files: orchestrator, recorder/model boundaries if required, strategy startup, tests. Verify temporary-database roundtrip and interrupted lifecycle without exchange.

### M4. Admission and loss controls (F5, F6, F10)

Reserve outstanding commitments including queued creates, fail closed on invalid valuation, recheck current stop/pause/risk at dispatch, account losses before creates. Cover shared wallet and delayed queues. Files: controller/shared strategy and tests.

### M5. Signals/configuration (F8, F11, F12, F16, F17)

Validate complete aligned features and chronology, make candle finality contract explicit, adapt Bollinger extraction to actual supported API, atomically apply runtime config and log restart-required differences. Files: OK/HL controller, controller_base, indicator utility and tests. Verify native indicator outputs and valid/invalid update cycles.

### M6. Integrated verification and handoff

Re-run audit counterexamples through native components with inert venue transport; focus-suite and realistic temporary persistence smoke. Review all changed control paths personally, inspect diff/status, record exact evidence and remaining external runtime limitations. Archive this plan only after authorized local scope completes.

## Verification

Working directory owner. Python `/Users/s1kor/anaconda3/envs/hummingbot/bin/python`, `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=.`. Use `python -m pytest -q -p no:cacheprovider` on new focused regressions first (observe expected red), affected existing suites after each slice, then combined controller/position/order/orchestrator/strategy/OKX/candle suites. Use installed lint tools consistent with .pre-commit-config.yaml; `git diff --check`. Exact commands/results appended per milestone. External authenticated runtime and financial performance remain unverified. Do not hide failed process teardown behind passing assertions.

## Decision Log

- 2026-09-08: Continue personally following user's preference for direct source understanding; no new delegation. Existing audit counterexamples supply initial red evidence; formal regressions preserve them.
- 2026-09-08: Fresh branch safe because owner has no tracked edits. No other repository edits planned. Initial authority already covers fixes; no repeated approval required.

## Progress Log

- 2026-09-08: Bootstrap passed core; agent-lab advisory unrelated. Loaded coding, diagnosis, TDD skill and test-quality reference. Rechecked owner state and created task branch. Implementation pending.

## Rollback / Recovery

No live process changes. Keep changes local and reviewable on task branch. If a slice regresses, revise only this task's patch using captured diffs; never reset/stash unrelated files. No automatic activation on passing checks. User can revert the eventual scoped commits if requested; committing/publishing is not required for this task.

### Implementation evidence (in progress)

- M1: Added five native `InFlightOrder`/`TradeUpdate` event regressions; all five failed on original code. Patched cumulative entry/exit ownership, VWAP, partial mark-to-market, partial timeout/barriers, cancel acknowledgement ordering, and shutdown fallback. Full position executor directory: `49 passed` (4.03s). Existing tests that expected stale partial entries to persist or a close before TP cancellation were corrected with explicit event sequencing.
- M2: Native chaser regression failed with full-amount replacement before cancel acknowledgement. Order executor directory now `28 passed`; combined order and four new real OKX tracker transport/cancel uncertainty cases `32 passed` (2.33s). Every transport case failed on original code by removing the still-uncertain order. Unknown status/cancel failures now retain reservations; explicit connector not-found classification remains separate.
- M3: Native reproductions confirmed unrelated held inventory 1 -> 0.2 and gross-fee reporting. Order-ID ownership now replaces turnover subtraction, closed executor orders are excluded from held reconstruction, flat realized history is retained, and continuous cost basis restarts after flatness. Additional counterexamples exposed order-level duplicate suppression dropping later cumulative fills and aggregate lifetime averages contaminating a new position; both have focused red/green checks. M3 integration and restart proof still in progress.
- Design: do not automatically recreate an unfinished persisted executor with missing lifecycle state. Persist intent before executor start and latch startup reconciliation for unfinished persisted execution. A clean local check never clears this ownership latch or authorizes live activity. Concrete recovery instructions and temporary-database validation remain part of M6.


## Completion receipt — 2026-09-08

All six authorized local milestones are complete. No external deployment, account reconciliation, orders or promotion occurred. The original audit remains an immutable record of the pre-fix source.

- M1 complete: native partial-entry/exit ownership, stale entry handling, trailing fallback through shutdown, partial mark-to-market, and residual inventory handoff. A non-finite executor metric invalidates reports rather than substituting zero.
- M2 complete: chaser cancellation is serialized; remaining quantities and all fills survive retries; minimum sell prices include remaining entry fees and estimated exit fees. OKX retains ambiguous submissions and validates terminal fill totals. Explicit negative acknowledgements still fail orders.
- M3 complete: continuous held cost basis and exactly-once fee/realized reporting; actual base fees reduce inventory; completed history cannot consume unrelated held inventory. Native temporary SQLite proves intent-before-start, disk-failure blocking, unfinished restart latching and durable risk-state restoration. No automatic recreation of an unfinished executor; the runbook documents the necessary external reconciliation.
- M4 complete: pending/shutdown commitment caps, shared-wallet reservations, orphan-order blocking, current queued stop/pause/drawdown checks, and historical loss ingestion before new proposals. Risk peaks and operator pauses survive restart.
- M5 complete: closed-candle policy, complete finite OHLCV/indicators, HMM observation/confidence corrections, explicit Bollinger calculation and atomic config reload. A fresh YAML reload exposed generated `candles_config` differing from file defaults; normalization fixed that regression. Feed topology/history changes require restart. OKX official candle docs confirmed spot `volCcy` is quote volume; that parser was not changed.
- M6 complete: personally reviewed changed paths and ran the native OKX tracker -> PositionExecutor -> held ledger smoke without pytest fixtures. Transport is replaced with inert functions; unexpected network calls raise. Two native submissions were acknowledged, entry fill 0.4 and exit fill 0.1 left 0.3, PnL at 80 was -6, final state POSITION_HOLD, native active orders zero. The first smoke harness lacked native order-book setup; corrected that harness and required acknowledged exchange IDs and both submissions before accepting its result.

Final combined command (owning Python 3.13 environment):

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest test/test_ok_rsi_controller.py test/test_hl_rsi_controller.py test/test_ok_rsi_admission_safety.py test/test_ok_rsi_signal_safety.py test/test_ok_rsi_restart_safety.py test/hummingbot/strategy_v2/executors/position_executor test/hummingbot/strategy_v2/executors/order_executor test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py test/hummingbot/strategy_v2/executors/test_held_inventory_accounting.py test/hummingbot/strategy/test_strategy_v2_base.py test/hummingbot/strategy_v2/utils/test_market_analysis.py test/hummingbot/connector/test_markets_recorder.py test/hummingbot/connector/exchange/okx test/hummingbot/data_feed/candles_feed/okx_spot_candles test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py -q --basetemp=/tmp/ok-rsi-verified-pytest
```

Observed: `378 passed, 22 subtests passed in 11.89s`, exit 0, no warnings. Earlier integrated runs exposed test event-loop/market-data task leakage; the final fixture explicitly isolates public rate-source startup and manages temporary event loops. No warning suppression was used for the passing result.

Formatting/lint: repository-selected autopep8 rules applied only to task Python files; flake8 passed; isort `--check-only --settings-path=pyproject.toml` passed. `git diff --check` passed for root, Hummingbot, Hummingbot API and Quants Lab. Owner HEAD remains 91372522a8e4bb178a959f1b0c42ec7276cf89ba on feat/andrzej_ok_rsi_lifecycle_repairs. No commits or external writes. Existing unrelated work was preserved.

Evidence: local `ok-rsi-audit/remediation/` contains raw passing suite output, standalone smoke and output, exact commands and source hashes. Runbook: `docs/rsi-v5/ok-rsi-lifecycle-repair-runbook.md`. External limitations: exact deployed engine/config, regional instrument/account behavior, authenticated reconnect/cancel/fill handling and actual fees remain unverified; no profitability/backtest conclusion or live authority follows from local completion.
