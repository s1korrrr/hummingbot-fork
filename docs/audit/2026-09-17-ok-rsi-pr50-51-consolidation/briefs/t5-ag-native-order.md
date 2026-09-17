# t5 — Native order state (A) and cancel_source JSON (G)

Do not start until t4 has checked PR50-only files into the consolidation worktree. If those files are still base-only, STOP with NEEDS_CONTEXT.

## Work in

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

USER_REQUESTED_GIT=no. Do not commit/push/PR. Do not touch `/Users/s1kor/dev/trading/rsibot/hummingbot`.

## Read

- spec.md, reports/t1-pr50-context.md (sections A, G), reports/t4-merge-bcd.md
- `hummingbot/core/data_type/in_flight_order.py`
- `hummingbot/connector/client_order_tracker.py`
- `test/hummingbot/core/data_type/test_in_flight_order.py`
- `test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py`

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t5-ag-native-order.md`

## Owned files

- `hummingbot/core/data_type/in_flight_order.py`
- `test/hummingbot/core/data_type/test_in_flight_order.py`
- `test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py` (add tracker/OKX reorder coverage if the existing harness allows)
- Optional new test next to those, not the overlap executor file

Do not edit `down_only_entry_execution.py`, `hl_rsi.py`, or `position_executor.py`.

## A

In `update_with_order_update`, after identity match: if `current_state` is terminal (`CANCELED`, `FILLED`, `FAILED`) and `new_state` is a non-terminal open-ish state (`OPEN`, `PENDING_CREATE`, `PENDING_CANCEL`, `PARTIALLY_FILLED`), refuse the **state** assignment. Still copy `exchange_order_id` and `cancel_source` when present. Do not move `last_update_timestamp` backward on a refused regression.

Do not change `update_with_trade_update`. Late TradeUpdates and cumulative-fill reconciliation stay processable.

Do not invent a blanket ignore-everything-after-cancel. FILLED-after-CANCELED via OrderUpdate is unspecified; do not over-block it unless an existing native test already defines it.

Tests: CANCELED@20 then OPEN@10 stays CANCELED, timestamp does not become 10; FILLED@20 then OPEN@10 stays FILLED; PENDING_CREATE → OPEN still works; different order identity still applies. Strongest available ClientOrderTracker and/or OKX event-order regression: cached/stopped-tracked order must not revive on delayed OPEN.

## G

Add `cancel_source` to `to_json` and restore in `from_json` (blank/missing → None). Persistence test must `json.dumps`/`json.loads` the native dict then `from_json`. Restored CANCELED order must not be `is_open` (no restart replay of entry).

## Status

DONE | DONE_WITH_CONCERNS | NEEDS_CONTEXT | BLOCKED
