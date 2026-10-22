# Native persistence recovery

This change moves the qualified V1, V2 and V3 native recorder writes off the asyncio event loop. A real SQLite write lock reproduced a blocked event loop and failed synchronous financial receipt writes. The original production lock holder is not established by that reproduction.

## Scope and invariants

Async persistence is enabled only for the three named native OKX owner configurations, with the exact recorder strategy and connector identities. Controller source, strategy configuration, sizing, risk limits and signal rules are unchanged. Other strategies retain the existing synchronous recorder contract.

A bounded single-writer lane serializes recorder transactions. Financial and order events retain immutable receipt payloads until commit is acknowledged. Failed or pending receipts are visible and block new executor admission. Cancellation after an admitted transaction waits for its known outcome. Duplicate receipt identities must agree on their source payload.

Executor creation persists its intent before activation, validates controller permission once after the last await, and durably aborts refused unstarted intents. Terminal executor and position capture sequences prevent a delayed periodic snapshot from rewinding newer state. Operator command cancellation retains the queue head before a known commit; unknown targets are consumed as refusals.

## Validation

The final frozen source was exercised in an offline native image, with no network or live mounts, one CPU and 768 MiB memory. Ten focused modules passed 367 tests and 12 subtests: runner policy, entry risk enrollment, executor orchestration, StrategyV2Base, real SQLite concurrency, order creation/cancel acknowledgement, OKX exchange behavior, bounded SQL queue behavior, in-flight order persistence and durable OKX tracking. Per-module subprocesses avoid the Python 3.13 event-loop fixture interaction observed in an earlier combined run.

The order-creation callback regression failed against the original callback under a held SQLite write lock, then passed with queued persistence and post-commit indexing. Queue contention and financial receipt regressions use real SQLite files. No test submits a live trade.

Ruff passed for changed Python paths apart from four unchanged duplicate-key fixtures in test_okx_exchange.py. Git diff whitespace checks and AST parsing passed. Frozen hashes, command lines, failures and final outputs are retained in the private runtime-recovery release packet. Source tests do not establish deployment or live order execution. Each actual owner image requires separate source, ABI and buy/sell/recovery qualification before its guarded continuity restart.

## Operations and rollback

Monitor native_persistence acknowledgement, pending/failed counts, reconciliation status, heartbeat and watchdog logs. Admission remains held while qualified financial persistence is unacknowledged. Do not clear receipt markers or reset the recorder to make health appear green.

Deploy sequentially through native owner_restart continuity checks, preserving each recorder, mounts, financial rows and strategy configuration. V3 source identity resealing changes only code identity and preserves its strategy recipe. Roll back by preparing the prior qualified owner image through the same continuity mechanism; never restore an old database over current fills.
