# Runtime stability and truthful status

## Goal

Repair reload admission and price-refresh isolation, and replace misleading balance/status warnings. Completion requires regression tests and native offline boundary/status proof. Authority is repository-process-only: no bot restart, trading activation, orders, credentials, configuration or account-state changes; no external publication in this request.

## Current State

Owner Hummingbot starts at 659e74dfb. The original checkout has concurrent MQTT identity, native lifecycle/reconciliation and chart-snapshot edits. This task uses its own worktree and preserves the original checkout. The current loader calls importlib.reload before checking the protected OK RSI replacement policy. Price refresh gathers a whole batch behind one timeout and maps individual failures to zero. Generic balance warnings wrongly imply zero base inventory blocks buys.

## Target State

- Protected controller source changes are detected before reload, with a latched restart requirement and blocked new executor admission. Existing executor management and explicit stop actions continue.
- Configuration-only updates and reload-enabled controllers retain supported behavior. A file touch without content change does not require restart. Relevant OK RSI helper source changes are covered.
- Per-pair bounded price refresh retains valid successes, never publishes failed/invalid prices as zero, records health and bounded freshness, and propagates cancellation cleanly. Repeated identical failures do not flood logs; recovery is observable.
- Status shows blocked/restart/data-health conditions and side-specific spot balance limitations without altering admission or capital.
- Telegram single-owner behavior and startup throttle controls remain intact.

## Risks and Failure Modes

Guarding only after reload can mutate globals used by existing instances. Suppressing errors without status would hide unavailable data. Clearing a restart latch or risk state could resume execution. Cached conversion rates must not be treated as indefinitely fresh. Preserve unrelated edits and avoid live source reloads by working in isolation.

## Milestones

1. Reproduce protected reload before mutation and partial-price/zero-price defects. Implement bounded fixes and regressions.
2. Expose restart/data health and accurate spot warnings; exercise rendered status offline with native connectors.
3. Run focused owner tests, format/lint, diff checks and an integration check with preserved concurrent edits in an isolated copy where practical. Record exact evidence and limitations.

## Verification

Use /Users/s1kor/anaconda3/envs/hummingbot/bin/python. Focused suites: test/hummingbot/strategy/test_strategy_v2_base.py, test/hummingbot/data_feed/test_market_data_provider.py, OK RSI safety tests and notifier tests. Use repository pre-commit hooks for scoped files. Produce an offline status artifact; never claim live exchange validation.

## Decision Log

2026-09-08: Fresh managed rates expire after two configured refresh intervals, measured per response; backward clock movement fails closed. Inverse and intermediate conversion legs use the same managed-rate filter, while unmanaged/gateway oracle sources retain existing assumptions. Shared RateOracle state is not mutated. Source protection is checked on config load, not by a filesystem watcher; a fresh process remains necessary after implementation changes.

2026-09-08: Retain protection and stop authority; no automatic restart or risk reset. Implement in isolated branch feat/andrzej_ok_rsi_runtime_stability. Native .so files are linked from the owner environment without changing compiled sources. Scope includes displayed warnings because they obscure actual execution state.

## Progress Log

2026-09-08: Local implementation complete. Reproduced failures before changes and retained regression coverage for protected source preflight, helper changes/deletion, same-content touches, config recreation, admission bypasses, per-pair timeout/invalid-price handling, cancellation, sample timing, clock rollback, expiry/recovery, and rendered status.

2026-09-08: Independent hq_reviewer found an inverse/indirect conversion freshness bypass. Fixed by filtering a provider-local oracle snapshot and resolving with the canonical resolver. Reviewer independently verified both new regression cases and reported no further actionable issue.

2026-09-08: Final native owner-environment suite passed 635 tests and 32 subtests in 27.08s, with no warnings reported. Scoped pre-commit formatting/lint checks passed. Native offline smoke using real OkxExchange, OKRSIController, StrategyV2Base, ExecutorOrchestrator, MarketsRecorder and temporary SQLite passed with fixture public ticker responses, no real network and no orders. Status text displays blocked controller rows, a latched restart banner, stale/unavailable prices, and side-specific balance notes. Source bytes were restored after the isolated probe.

2026-09-08: Concurrent owner changes were copied into a separate validation checkout using content-based three-way merging; no conflict markers or discarded hunks. Final non-MQTT compatibility suite passed 109 tests and 9 subtests in 6.78s. An earlier combined run had 145 passes, 9 subtest passes and 6 MQTT failures. A control run without any of this task's four production file changes reproduced exactly those six MQTT failures (38 MQTT tests passed). They are PRE_EXISTING relative to this patch: two status fixture attribute errors, event payload expectation failures, and a gateway health restart expectation. They remain a compatibility limitation and are not described as flaky. The concurrent MQTT work is not included in this commit.

2026-09-08: Local completion only. Original owner checkout remains under concurrent development; this task made no writes there. No bot restarted, trading config/account/database altered, push or PR performed. The completed plan is archived with its local evidence and rollout boundary.

## Rollback / Recovery

Revert only this task's commits or discard its isolated worktree after preserving artifacts. The running checkout, database and configuration are unchanged. Deployment requires a separately controlled restart and ownership reconciliation; no generated evidence grants that authority.


## Exact validation commands and evidence

Run from this task's Hummingbot worktree with the existing native environment:

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest test/test_ok_rsi*.py test/test_hl_rsi_controller.py test/hummingbot/strategy/test_strategy_v2_base.py test/hummingbot/strategy/test_controller_source_guard.py test/hummingbot/data_feed/test_market_data_provider.py test/hummingbot/data_feed/test_market_data_refresh_resilience.py test/hummingbot/strategy_v2/executors/position_executor test/hummingbot/strategy_v2/executors/order_executor test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py test/hummingbot/strategy_v2/executors/test_held_inventory_accounting.py test/hummingbot/notifier -q --basetemp=/tmp/rsibot-runtime-stability-final
/Users/s1kor/anaconda3/envs/hummingbot/bin/pre-commit run --files controllers/directional_trading/ok_rsi.py hummingbot/data_feed/market_data_provider.py hummingbot/strategy/strategy_v2_base.py hummingbot/strategy_v2/executors/executor_orchestrator.py test/hummingbot/data_feed/test_market_data_provider.py test/hummingbot/strategy/test_strategy_v2_base.py test/test_ok_rsi_restart_safety.py test/hummingbot/data_feed/test_market_data_refresh_resilience.py test/hummingbot/strategy/test_controller_source_guard.py docs/rsi-v5/ok-rsi-runtime.md
git diff --check
```

Compatibility checkout: `/Users/s1kor/dev/trading/rsibot/.worktrees/ok-rsi-stability-compat-20260908`.

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest test/test_native_lifecycle.py test/test_native_sources.py test/test_okx_native_reconciliation.py test/test_ok_rsi_chart_snapshots.py test/test_hummingbot_runtime_identity.py test/test_hummingbot_quickstart.py test/hummingbot/strategy/test_strategy_v2_base.py test/hummingbot/strategy/test_controller_source_guard.py test/hummingbot/data_feed/test_market_data_refresh_resilience.py test/test_ok_rsi_restart_safety.py -q --basetemp=/tmp/rsibot-stability-compat-final
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest test/hummingbot/remote_iface/test_mqtt.py -q --basetemp=/tmp/rsibot-stability-mqtt-control
```

The second command above was run only with this task's production changes temporarily removed from the compatibility checkout; the combined snapshot was restored afterward.

Local artifact directory:
`/Users/s1kor/.codex/visualizations/2026/09/08/01a080af-8174-7de2-834a-508609d0d5e6/runtime-stability/`.

Evidence: `broad-tests-final.log`, `compat-final-tests.log`, `compat-tests.log`, `mqtt-control-tests.log`, `native_status_smoke.py`, `native_status_smoke.log`, `native_status.txt`, `native_status_result.json`, `compat-source-hashes.json`, and `owner-preservation-after.json`. The standalone smoke requires `PYTHONPATH=.` and an isolated task worktree; it asserts the worktree path before its temporary source probe. Native binaries were linked from the owner environment; no compiled code was changed. Eight existing ignored legacy YAML fixtures were copied into the isolated task checkout solely for existing tests. No secure config was copied.
