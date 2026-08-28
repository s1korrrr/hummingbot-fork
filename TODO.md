# TODO

## OK RSI unified history and inventory migration

- [x] Present and obtain approval for the architecture and authority boundary.
  DoD: runtime identity, history provenance, wallet reconciliation, no-force-sell behavior, locked validation, and unlocked-stop boundary are explicitly approved.
- [x] Write and review the repository design specification.
  DoD: the spec contains no placeholders or contradictions and the operator approves the written artifact before implementation planning.
- [x] Create the TDD implementation plan.
  DoD: every config, migration, restoration, reporting, database, wallet, and runtime requirement maps to fail-first steps and exact validation commands.
- [ ] Implement config identity and no-force-sell invariants.
  DoD: local runtime plus committed fixtures/docs use `ok_rsi.yml`, `ok_rsi.sqlite`, short IDs, OKX USDC pairs, final false kill switches, and no forced liquidation path.
- [ ] Implement and test the migration planner/importer/verifier.
  DoD: complete legacy history remains unchanged in provenance, target mutation is transactional/idempotent/checksum-bound, and failures preserve the target.
- [ ] Reconcile and seed verified OKX inventory.
  DoD: five eligible assets decode and cross-check source economics against read-only available balances; mismatches fail closed and excess is untracked.
- [ ] Verify actual databases and locked runtime.
  DoD: backups, integrity checks, row/receipt verification, restored cost basis/status, clean logs, and zero order/fill/cancel evidence pass before final configs are unlocked and left stopped.
- [ ] Produce average-price and first-sell economics report.
  DoD: each asset reports wallet/tracked/untracked amounts, breakeven, current price, minimum profit, recovery arm, partial amount, and dynamic conditions without claiming a guaranteed execution price.
- [ ] Complete validation, review, documentation, and memory harvest.
  DoD: focused/warning-clean tests, compilation, pre-commit, YAML/diff/status review, rollback evidence, and durable lessons are current.

## OK RSI EEA USDC and locally derived higher timeframes

- [x] Add fail-first aggregation, feed-count, config-validation, and USDC loader tests.
  DoD: tests fail for the missing local aggregation behavior and old USDT fixtures before production/config changes.
- [x] Implement strict local higher-timeframe derivation in `controllers/directional_trading/ok_rsi.py`.
  DoD: each controller initializes only its base feed; configured medium/long frames are derived only from complete consecutive base bars.
- [x] Update committed fixtures and local operator YAMLs to EEA-account-confirmed USDC pairs.
  DoD: five editable buy/sell profiles and three compact sell-only pairs use `*-USDC` without changing spot-only risk semantics.
- [x] Run focused, loader, warning-clean dependency, formatting, YAML, and diff validation.
  DoD: exact commands and observed results are recorded; no strategy or order path is started.
- [x] Close out plan and durable memory.
  DoD: final behavior, rollback, residual risk, and verified commands are current with no secret material.

## OK RSI positional runtime-constructor regression

- [x] Reproduce the exact `StrategyV2Base` positional constructor failure.
  DoD: a focused regression fails with the observed duplicate `config` error before production code changes.
- [x] Repair positional forwarding without changing controller semantics.
  DoD: positional and existing keyword construction both initialize successfully.
- [x] Run focused runtime-path validation and close out evidence.
  DoD: OK RSI tests, compile/lint, diff checks, and plan/memory notes are current; no service or order action occurs.

## OK RSI end-to-end controller audit and merge readiness

- [x] Establish exact ownership, branch/base, documentation, and authority boundaries.
  DoD: intentional files, ignored runtime state, source-of-truth classes, official docs, forbidden live surfaces, and merge target are explicit in `PLAN.md`.
- [x] Trace controller behavior end to end against local runtime contracts.
  DoD: config through market data, signals, actions, executors, inventory/recovery, status/logging, loader, and shutdown paths are mapped with concrete invariants.
- [x] Reproduce suspected defects and close test gaps.
  DoD: every repair has a failing behavior test or direct failure signal; clean-checkout portability and warnings are tested beyond the original suite.
- [x] Repair confirmed issues in the smallest safe slice.
  DoD: no silent executor fallback, hidden config dependency, unsafe balance/exposure behavior, known contract violation, or per-feed rate-limit burst remains.
- [x] Run safe integration and startup/log validation.
  DoD: real loader/config/controller lifecycle and a public-data-only 15-feed process complete without credentials or orders; capital-capable restarts are excluded.
- [x] Run final correctness, risk, execution, maintainability, security, and performance review.
  DoD: official-doc/local-source checklist, risk rails, execution assumptions, tests, compilation, hooks, warning gate, diff/status audit, and findings are resolved or block shipping.
- [x] Repair and verify the owner-repository full-suite entry point.
  DoD: `make test` uses the active Hummingbot interpreter, isolates duplicate test module names, and its pre-existing Vertex/shutdown blockers are reproduced and recorded separately from controller results.
- [x] Update owner-repo plans and workspace learning stores.
  DoD: verified facts, commands, blockers, rollback, and final gate are recorded without secrets.
- [x] Publish the focused Hummingbot PR and obtain explicit merge authorization.
  DoD: intentional commits only; PR #29 targets `master`, its description matches the diff, the operator explicitly accepted the documented CI/full-suite risks on 2026-08-28, and the final scoped warning-clean gate passed 325 tests plus 22 subtests immediately before merge. The resulting merge SHA is verified during root gitlink integration.
