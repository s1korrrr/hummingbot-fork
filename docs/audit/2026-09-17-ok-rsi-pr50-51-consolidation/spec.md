# Consolidated candidate for Hummingbot PRs #50 and #51

## Objective

Produce a corrected, consolidated local candidate that preserves useful changes from both PRs and resolves every REQUEST CHANGES blocker in the 2026-09-16 end-to-end audit. Do not merge upstream, deploy, restart execution, or claim native/runtime validation that was not actually run.

## Authority

- Ceiling: `repository-process-only`
- Forbidden: trades, paper/canary/live activation, deployment, service restart, credential/config changes, pushes, PRs, merges to protected branches
- User did not request commit/push/PR. Leave git history untouched except creating a local feature branch/worktree.
- Preserve unrelated dirty work in root `feat/andrzej_reader_reopen_on_corrupt` and hummingbot `feat/andrzej_telegram_ssl_log_noise`.

## Audit targets

- PR #50 head: `9334c20c3a09ffed92631b4fa1172285b776b116` (`feat/andrzej_down_only_postonly_repair`)
- PR #51 head: `d38705b0813a6e39747908cbce74f1f08015e0dc` (`feat/andrzej_ok_rsi_entry_control_consistency`)
- Shared base / current rsibot gitlink: `4a4372bcde4990355421a42df3b8ce6107299abb`
- Existing worktrees (read-only sources):
  - `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-down-only-postonly-repair`
  - `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-entry-control-consistency`
- Consolidation worktree (writers only): `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
- Do not edit `/Users/s1kor/dev/trading/rsibot/hummingbot`

## PR file sets

PR #50 only:

- `hummingbot/connector/exchange/okx/okx_exchange.py`
- `hummingbot/core/data_type/in_flight_order.py`
- `test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py`

PR #51 only:

- `controllers/directional_trading/hl_rsi.py`
- `controllers/directional_trading/ok_rsi.py`
- `hummingbot/strategy_v2/executors/position_executor/position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_trailing_no_loss_floor.py`
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`
- `test/test_ok_rsi_down_only_entry_factory.py`

Overlap (single writer at a time):

- `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py`
- `test/test_ok_rsi_down_only_entry_execution.py`

## Completion bar

1. Three-way merge both PRs onto `4a4372bc` with no unresolved conflicts
2. Resolve blockers A–G without changing approved trading policy
3. Tests for each corrected behavior
4. Focused native/unit tests that can run locally are executed and reported
5. No Docker/CI/live/exchange claim unless actually executed

## Blockers

### A. P1 — Terminal order states can regress after delayed OPEN updates

`hummingbot/core/data_type/in_flight_order.py` `update_with_order_update`.
Enforce permitted lifecycle transitions and evidence provenance. Keep late TradeUpdates and cumulative-fill reconciliation processable. Add the strongest ClientOrderTracker/OKX regression available.

### B. P1 — Cancellation veto loses to pending local reprice

`process_order_canceled_event`. Explicit prohibitive venue reasons win over local replacement intent. OKX source 32 is self-trade prevention, not post-only crossing (31).

### C. P1/P2 — Deadline and cooldown gaps

Evaluate deadline and revocation regardless of order wait state. Expiry prevents new children but does not prove the current submission absent. Enforce shared replacement count and cooldown at final submission.

### D. P1 — Stopped controller or unusable data still allows a child

Check current owner state and venue/pair execution-data evidence at the final child boundary. Do not rerun one-shot parent admission or abandon owned fills when entry inputs fail.

### E. P1 — Premature or unavailable capital release

`hl_rsi.py` acquisition helpers. Separate owned inventory, unresolved acquisition commitments, and report labels. Release only reconciled commitment. Reject missing/non-finite/conflicting/over-authorized receipts.

### F. P2 — Residual flat-fee economics

Estimate actual next quantized close quantity. Preserve no-loss interpretation, base-fee inventory conservation, unknown fee evidence, LIMIT/taker classification.

### G. P2 — Cancellation provenance is not serialized

Persist `InFlightOrder.cancel_source` through `to_json`/`from_json`. Persistence tests must round-trip native JSON. Persisting evidence must not authorize restart replay of an entry.

## Preserve

- Chronological compatibility helper
- Ordinary LIMIT/taker fee classification
- Cancel-source field once persisted
- Useful entry lifecycle/recovery work after corrections
- Approved trading policy unchanged
