# t5 — Native order state (A) and cancel_source JSON (G)

Status: **DONE_WITH_CONCERNS**

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Branch: `feat/andrzej_ok_rsi_audit_consolidation` at `4a4372bcde4990355421a42df3b8ce6107299abb`
Git: no commit, no push, no PR. PR50 `in_flight_order.py` was already staged (`cancel_source` in memory). A/G edits are unstaged on top (`MM`). `/Users/s1kor/dev/trading/rsibot/hummingbot` was not touched.

## Preconditions

t4 had checked PR50-only files into this worktree. `in_flight_order.py` already had in-memory `cancel_source` and `misc_updates` copy. No lifecycle guard. No JSON field.

## Blocker A

`InFlightOrder.update_with_order_update` still matches identity, then copies a missing `exchange_order_id`. After that:

- If `current_state` is `CANCELED`, `FILLED`, or `FAILED` and `new_state` is `OPEN`, `PENDING_CREATE`, `PENDING_CANCEL`, or `PARTIALLY_FILLED`, the **state** assignment is skipped.
- `exchange_order_id` and `cancel_source` still copy when present.
- `last_update_timestamp` is not moved backward on a refused regression (including when `updated` is true only because `exchange_order_id` arrived).
- `update_with_trade_update` is unchanged. Late fills still record.

FILLED via OrderUpdate after CANCELED still applies. No blanket ignore-after-cancel.

`ClientOrderTracker` is unchanged. The native refuse is enough: `_process_order_update` still finds cached orders through `fetch_order`, then `update_with_order_update` keeps `CANCELED`.

## Blocker G

`to_json` writes `cancel_source` (None when unset). `from_json` restores a non-blank value; missing, `null`, and `""` stay `None`.

A restored CANCELED order is not `is_open`. `restore_tracking_states` still resumes only `is_open` orders. Persisted provenance does not restart an entry.

## Tests

Native `InFlightOrder`:

- CANCELED@20 then OPEN@10 stays CANCELED, timestamp stays 20
- Same refuse for PENDING_CREATE / PENDING_CANCEL / PARTIALLY_FILLED
- Different identity OPEN does not apply
- FILLED@20 then OPEN@10 stays FILLED
- PENDING_CREATE → OPEN still works (`test_update_with_order_update_open_order`)
- Refused OPEN still copies `exchange_order_id` and `cancel_source`, timestamp does not become 10
- FILLED after CANCELED still applies
- Late TradeUpdate after CANCELED still records the fill
- `json.dumps` / `json.loads` then `from_json`: `cancel_source == "31"`, not `is_open`
- Missing / blank / null `cancel_source` restores as None

Tracker / OKX (existing `OkxExchange` harness):

- CANCELED `process_order_update` `stop_tracking_order`s into the TTL cache; delayed OPEN@10 leaves the cached object CANCELED, not `is_open`, not in `in_flight_orders`
- REST `_request_order_status` canceled (`uTime` 20000, source 31) then delayed live (`uTime` 10000) stays CANCELED at timestamp 20.0

Command:

```bash
cd /Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest \
  test/hummingbot/core/data_type/test_in_flight_order.py \
  test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py \
  --import-mode=importlib -q
```

Result: **36 passed in 1.86s** (conda env `hummingbot`). flake8 on the three owned files: clean. Tracker smoke: `test_restore_tracking_states_only_registers_open_orders` and `test_process_order_update_trigger_order_cancelled_event` passed. Not native CI/Docker.

## Files changed

- `hummingbot/core/data_type/in_flight_order.py`
- `test/hummingbot/core/data_type/test_in_flight_order.py`
- `test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py`

Not edited: `client_order_tracker.py`, `okx_exchange.py`, `down_only_entry_execution.py`, `hl_rsi.py`, `position_executor.py`. No new test file.

## Concerns

1. OKX WS `_user_stream_event_listener` looks up `all_updatable_orders`, which excludes the TTL cache. A delayed WS `live` after cancel never reaches `update_with_order_update`. The audit path is REST / `process_order_update` on the cached object. That is what the new tests hit. A WS-only test would have passed even before A.
2. `InFlightOrder.attributes` / `__eq__` still omit `cancel_source`. Equality will not catch a dropped field. The `json.dumps` round-trip test will.
3. FILLED OrderUpdate after CANCELED is still applied (timestamp 30). That is the unspecified cancel/fill race. TradeUpdates stay processable either way.
