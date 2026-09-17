# t1 — PR #50 context packet

Read-only inspection of worktree `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-down-only-postonly-repair`.

- HEAD: `9334c20c3a09ffed92631b4fa1172285b776b116` (`feat(ok-rsi): repair confirmed OKX post-only entry cancels without reviving stopped intents`)
- Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
- Tree: clean. No extra local edits.
- `in_flight_order.py` blob: `c429ba3faeef2ec1a618cbc5627e204859bc178d` (matches the 2026-09-16 audit)

Audit source for "what is wrong": the 2026-09-16 end-to-end REQUEST CHANGES write-up (GitHub review 5228544437) as restated in the run spec. This packet does not re-run those probes.

## 1. Diffstat vs base

`git diff --stat 4a4372bc...9334c20c`

```
controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py       | 435 +++++++++++++++------
hummingbot/connector/exchange/okx/okx_exchange.py                             |  16 +
hummingbot/core/data_type/in_flight_order.py                                  |   5 +
test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py          |  56 +++
test/test_ok_rsi_down_only_entry_execution.py                                 | 188 ++++++++-
5 files changed, 573 insertions(+), 127 deletions(-)
```

What the five-line `in_flight_order.py` change actually is: add `self.cancel_source: Optional[str] = None` in `__init__`, and copy `misc_updates["cancel_source"]` inside `update_with_order_update`. No lifecycle guard. No `to_json`/`from_json` field.

What the 16-line OKX change actually is: `_okx_order_misc_updates` plus `misc_updates=` on REST `_request_order_status` and WS `_user_stream_event_listener`. REST also writes `tracked_order.cancel_source` **before** building the `OrderUpdate`.

## 2. Blockers A, B, C, G on this head

### A. P1 — Terminal order states can regress after delayed OPEN updates

**Functions**

| Symbol | File | Lines |
| --- | --- | --- |
| `InFlightOrder.update_with_order_update` | `hummingbot/core/data_type/in_flight_order.py` | 331–357 |
| `ClientOrderTracker.fetch_order` | `hummingbot/connector/client_order_tracker.py` | 172–184 |
| `ClientOrderTracker._process_order_update` | same | 270–306 |
| `ClientOrderTracker.all_orders` | same | 80–84 (active + TTL cache) |
| `OkxExchange._request_order_status` | `hummingbot/connector/exchange/okx/okx_exchange.py` | 420–440 |
| `OkxExchange._user_stream_event_listener` | same | 459–517 |

**Branch order in `update_with_order_update` (current)**

1. Identity mismatch (`client_order_id` and `exchange_order_id` both differ) → `return False`.
2. Snapshot `prev_data = (exchange_order_id, current_state)`.
3. If local `exchange_order_id` is missing and the update has one, set it.
4. **Unconditional** `self.current_state = order_update.new_state`.
5. If `misc_updates` has a non-empty `cancel_source`, assign `self.cancel_source`.
6. `check_processed_by_exchange_condition()`.
7. `updated` is true iff `(exchange_order_id, current_state)` changed.
8. If `updated`, **overwrite** `self.last_update_timestamp = order_update.update_timestamp` with no monotonic / provenance check.

There is no terminal-state set, no permitted-transition table, no "incoming timestamp older than stored" check.

**What the audit says is wrong**

The method assigns `current_state` from any matching `OrderUpdate`. Probes: `CANCELED` at timestamp 20 then delayed `OPEN` at 10 → local state `OPEN` and `last_update_timestamp` 10. Same for `FILLED` then delayed `OPEN`. Inherited native behavior, not introduced by `cancel_source`. `ClientOrderTracker._process_order_update` calls this method on `fetch_order`, which includes `_cached_orders`. Completing an order `stop_tracking_order`s it into that cache, so a late OPEN can still mutate the same object. The tracker has no second guard (`process_order_not_found` refuses FAILED only when already CANCELED/FILLED/FAILED; `_process_order_update` does not).

Correction required: enforce permitted lifecycle transitions and evidence provenance **in this native owner**. Keep late `TradeUpdate`s and cumulative-fill reconciliation processable. Do not drop every post-terminal message. Add a real ClientOrderTracker + OKX event-ordering regression, not only a direct method test.

**Trade-update path (must stay open)**

`update_with_trade_update` (359–379) never writes `current_state`. Fill accounting and `completely_filled_event` still work on a terminal order. `ClientOrderTracker.process_trade_update` uses `all_fillable_orders` (active + cached + lost). A state-transition guard in `update_with_order_update` does not by itself block late fills.

### B. P1 — Cancellation veto loses to pending local reprice

**Functions**

| Symbol | File | Lines |
| --- | --- | --- |
| `DownOnlyMakerPositionExecutor.process_order_canceled_event` | `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py` | 343–379 |
| `_replacement_bound_to` | same | 147–154 |
| `_is_confirmed_post_only_cancel` | same | 157–162 |
| `PositionExecutor.process_order_canceled_event` (super) | `hummingbot/strategy_v2/executors/position_executor/position_executor.py` | 1456–1485 |

Constant: `OKX_POST_ONLY_CANCEL_SOURCE = "31"`. Source `"32"` is never named in this file.

**Branch order in `process_order_canceled_event` (current)**

1. Event order_id ≠ tracked open order → `super()` and return.
2. Refresh tracked order. Missing native order → `reconciliation_required` and return.
3. Force `order.current_state = OrderState.CANCELED`.
4. **If `_replacement_bound_to(tracked.order_id)`** (local downward reprice or post-only repair already bound) → confirm replacement (`_open_order_entry_expired`, `_entry_reprice_cancel_confirmed`, lifecycle `reconciliation_pending`) and **return**. Cancel source is copied into the lifecycle dict but **does not veto**.
5. Else if `_acquisition_permitted()` and `_is_confirmed_post_only_cancel` (`is_cancelled` and `cancel_source == "31"`) → bind `ENTRY_REPLACEMENT_POST_ONLY_REPAIR` and return.
6. Else `super()`, which for `entry_order_policy == "single_attempt"` ends acquisition as `exchange_canceled` and keeps the tracked object.

**What the audit says is wrong**

Already-bound replacement runs before venue-reason classification. Reproduction: pending downward reprice, `cancelSource=32`, reconciled zero fill → child BUY reaches placement. OKX documents 32 as self-trade prevention, not post-only crossing (31). Bound order id proves **which** order we meant to replace, not **why** the venue canceled it.

Correction: explicit prohibitive venue reasons win over local replacement intent. Do not infer cause from a pending target.

### C. P1/P2 — Deadline and cooldown gaps

**Functions**

| Symbol | File | Lines |
| --- | --- | --- |
| `control_open_order` | `down_only_entry_execution.py` | 177–267 |
| `_order_awaits_exchange_update` | same | 164–170 |
| `_submit_reprice_child` | same | 442–546 |
| `_intent_allows_price` | same | 633–648 |
| `_live_maker_decision` | same | 590–631 |
| `decide_down_only_maker` | `controllers/directional_trading/ok_rsi_core/down_only_entry.py` | 149–229 |
| `_intent_age_expired` | `down_only_entry_execution.py` | 548–554 |

**`control_open_order` wait / deadline / cooldown flow (current)**

1. Sealed or closed → return.
2. If `_entry_child_submission_authorized` → `_submit_reprice_child()` and return (no wait-state check; this is the child-placement gate).
3. No tracked order → initial maker or `reconciliation_required`.
4. FILLED → clear replacement, parent single-attempt.
5. `order.is_cancelled` → bound replacement confirm, or `managing_fill`, or parent single-attempt.
6. If `_entry_replacement_purpose == downward_reprice` → `cancel_pending` **return**.
7. Failure or stop latch → parent single-attempt.
8. **If `_order_awaits_exchange_update`** (missing `exchange_order_id`, or state in `{PENDING_CREATE, PENDING_CANCEL}`) → set `awaiting_order_update` / `cancel_pending` and **return**.
9. If not OPEN/PARTIALLY_FILLED → `awaiting_order_update` and return.
10. Only then `_live_maker_decision` → `decide_down_only_maker`, which is the **only** place total-age expiry and reprice cooldown are evaluated for a resting order.

`_order_awaits_exchange_update` is true for `PENDING_CREATE` even after an exchange id exists.

**Child submission does not reuse that cooldown**

`_submit_reprice_child` checks `_entry_reprice_count >= entry_max_downward_reprices`, remaining size, maker-safe price, and `_intent_allows_price`. It does **not** compare `now` vs `_entry_last_reprice_at` + `entry_reprice_cooldown_seconds`.

`_intent_allows_price` calls `_live_maker_decision(..., use_intent_clock=True)`. That zeros `last_reprice_at` and uses intent-created-at as the order clock. In `decide_down_only_maker`, cooldown only gates `CANCEL_FOR_REPRICE` vs `HOLD`. `_intent_allows_price` only rejects `CANCEL_TERMINAL`. A HOLD (resting / cooldown-not-elapsed) is treated as "price allowed". So cooldown never blocks a repair/reprice **child**.

Deadline at child time: `_intent_allows_price` **does** see `intent_age >= total_max_age_seconds` and can `_request_terminal_cancel("total_age_expired")`. That is why `test_replacement_rechecks_book_and_deadline_after_reconciliation` at t=300 passes. The gap is the **wait** path: PENDING_CREATE at t=900 never reaches that check, so the stop latch stays unset and no cancel of the unresolved identity is requested.

**What the audit says is wrong**

`control_open_order` returns from unresolved acknowledgment/wait before policy lifetime evaluation. Probe: 300s intent limit, age 900s, `PENDING_CREATE` → stop latch unset. Separate probe: confirmed post-only repair submits one second after the prior child despite a 30s cooldown.

Correction: evaluate deadline and revocation regardless of wait state. Expiry prevents new children but does not prove the current submission absent. Keep ownership while resolving/canceling the same identity. Enforce shared replacement count **and** cooldown at final submission.

### G. P2 — Cancellation provenance is not serialized

**Functions**

| Symbol | File | Lines |
| --- | --- | --- |
| `InFlightOrder.__init__` (`cancel_source = None`) | `in_flight_order.py` | 119 |
| `InFlightOrder.from_json` | same | 227–256 |
| `InFlightOrder.to_json` | same | 258–281 |
| `InFlightOrder.update_with_order_update` (in-memory copy) | same | 345–349 |
| `ClientOrderTracker.restore_tracking_states` | `client_order_tracker.py` | 153–164 |
| `ClientOrderTracker._restore_order_from_json` | same | 444–446 |

**What the audit says is wrong**

`cancel_source` exists only in memory. The native JSON contract does not persist it. A test named as persistence does not round-trip `to_json`/`from_json`. Persisting evidence must not authorize restart replay of an entry (`restore_tracking_states` only resumes orders with `is_open`).

## 3. Tests that almost cover the bugs, and what they miss

### A — state regression

| Test | What it covers | What it misses |
| --- | --- | --- |
| `test_update_with_order_update_open_order` (`test/hummingbot/core/data_type/test_in_flight_order.py` 480–502) | PENDING_CREATE → OPEN, timestamp becomes 1 | Reverse: terminal → OPEN |
| `test_update_with_order_update_multiple_order_updates` (504–545) | PARTIALLY_FILLED then FILLED | FILLED then delayed OPEN; timestamp rewind |
| `test_in_flight_order_persists_cancel_source_from_misc_updates` (`test_okx_order_uncertainty.py` 108–137) | CANCELED + source 31; second identical CANCELED returns False | Does not send OPEN after CANCELED; does not assert timestamp |
| `test_request_order_status_carries_okx_cancel_source` (141–154) | REST canceled snapshot copies source onto the live order | No reorder vs a later OPEN WS/REST update |
| `test_submission_timeout_retains_client_order_identity_for_status_recovery` (42–62) | Uncertain submit then OPEN recovery | Happy OPEN, not post-terminal |

No ClientOrderTracker test in this PR. No OKX listener test that applies CANCELED then a delayed OPEN against a cached order. `update_with_trade_update_*` tests prove fills do not need a state change; they are the template for "late fills still apply".

### B — source 32 vs 31

**There is no `"32"` anywhere in the PR tests.** Every post-only path hardcodes `"31"`.

| Test | What it covers | What it misses |
| --- | --- | --- |
| `test_confirmed_zero_fill_cancel_waits_for_reconciliation_then_places_one_child` (467–481) | Pending **downward reprice**, cancel event, zero fill, one child | `cancel_source` left `None`. Bound-replacement branch wins without a venue reason. This is the **same branch order** the 32 probe abuses. |
| `test_unknown_cancel_without_pending_target_does_not_repair_or_rest` (766–776) | No bound replacement, no source → `exchange_canceled` | No pending reprice. Does not prove 32 loses to a bound target. |
| `test_post_only_repair_allows_same_price_when_book_is_unchanged` (691–701) | source 31, no prior reprice bind | Distinguishes 31 only by making repair succeed |
| `test_confirmed_post_only_cancel_permits_one_fresh_book_repair` (740–763) | source 31, lower book, one child | Same |
| `test_duplicate_cancel_and_late_reconciliation_do_not_revive_or_duplicate_child` (837–855) | Duplicate 31 cancel | Still 31 |

The missing test is exactly the audit probe: bind `ENTRY_REPLACEMENT_DOWNWARD_REPRICE`, set `cancel_source = "32"`, reconcile zero fill, assert **no** `place_order` and acquisition ends on the prohibitive path (not post-only repair).

### C — deadline / cooldown

| Test | What it covers | What it misses |
| --- | --- | --- |
| `test_pending_submission_does_not_resubmit_when_ask_crosses` (721–736) | PENDING_CREATE + missing exchange id → wait, no second BUY, stop unset | Clock stays ~0. Tests book cross, not 900s vs 300s limit |
| `test_hard_rails_cancel_terminally` (555–566) | t=300, **OPEN** order → `total_age_expired` | Not PENDING_CREATE / PENDING_CANCEL |
| `test_connector_clock_cannot_reset_total_intent_age` (253–262) | OPEN order, local clock 300 | Same |
| `test_replacement_rechecks_book_and_deadline_after_reconciliation` (617–629) | After child is **authorized**, t=300 blocks placement | Does not cover wait-state before authorization |
| `test_reprice_cooldown_uses_local_clock_despite_future_connector_timestamp` (265–280) | Live OPEN order: cooldown gates **cancel-for-reprice** | Does not gate `_submit_reprice_child` |
| `test_confirmed_post_only_repair_*` | Repair places on the next control tick (timestamps 0 then 12) | Default cooldown is 5s; 12s > 5s so it would not catch a 1s-after-child / 30s cooldown probe |

Missing tests: (1) PENDING_CREATE at t=900 with 300s limit → stop latch set, no new child, current identity still owned / cancel requested. (2) `_entry_last_reprice_at` 1s ago, `entry_reprice_cooldown_seconds=30`, authorized post-only or downward child → no `place_order`.

### G — JSON round-trip

| Test | What it covers | What it misses |
| --- | --- | --- |
| `test_in_flight_order_persists_cancel_source_from_misc_updates` | In-memory attribute after `OrderUpdate` | Name says persist. Never calls `to_json` / `from_json` / `json.dumps` |
| `InFlightOrderPyUnitTests.test_to_json` / `test_from_json` | Native field set: ids, amounts, `last_state`, timestamps, fills | No `cancel_source` key. Round-trip would drop it today |
| `test_entry_acquisition_snapshot_is_stable_and_json_native` | Executor `get_custom_info()["entry_acquisition"]` is JSON-serializable | That snapshot reads `getattr(open_native, "cancel_source")` from the live object, not MarketsRecorder JSON |
| `test_okx_order_misc_updates_ignores_blank_cancel_source` | `""` / missing / int 31 → dict | Connector parsing only |

`ClientOrderTracker.restore_tracking_states` is the restart path: `from_json` then `if order.is_open: start_tracking_order`. A persisted `cancel_source` on a **CANCELED** order would not resume as open (`is_open` is false for CANCELED). The audit still wants the field stored so restart does not **lose evidence**, while not treating that evidence as a license to fire a new BUY.

## 4. `update_with_order_update`: `current_state` and `last_update_timestamp`

Current assignment, lines 340–357:

```
prev_data = (self.exchange_order_id, self.current_state)
# maybe set exchange_order_id
self.current_state = order_update.new_state          # always
# maybe set cancel_source from misc_updates
updated = prev_data != (self.exchange_order_id, self.current_state)
if updated:
    self.last_update_timestamp = order_update.update_timestamp
```

Consequences:

- Delayed OPEN after CANCELED/FILLED/FAILED changes `current_state` and sets `last_update_timestamp` to the **incoming** time, even if it is older than the terminal event.
- Duplicate CANCELED with the same state does not move the timestamp (`updated` is False). `cancel_source` can still be written on that path.
- Timestamp is **not** updated when only `cancel_source` changes.
- `is_done` is `{CANCELED, FILLED, FAILED}` or base amount complete. After a regress-to-OPEN, `is_open` becomes true again (`is_open` includes OPEN).

`update_with_trade_update` always sets `last_update_timestamp = trade_update.fill_timestamp` when the trade is new. Leave that path alone.

## 5. `to_json` / `from_json` vs `cancel_source`

`to_json` keys today: `client_order_id`, `exchange_order_id`, `trading_pair`, `order_type`, `trade_type`, `price`, `amount`, `executed_amount_base`, `executed_amount_quote`, `last_state`, `leverage`, `position`, `creation_timestamp`, `last_update_timestamp`, `order_fills`, `cumulative_fee_paid_base`, `cumulative_fee_paid_quote`.

`from_json` reads those (plus optional `order_fills`) and reconstructs via constructor `initial_state=OrderState(int(data["last_state"]))`. `cancel_source` stays default `None`.

`InFlightOrder.attributes` (used by `__eq__`) also omits `cancel_source`, so equality tests will not catch a dropped field.

Restart: `restore_tracking_states` → `from_json` → resume only if `is_open`. Serializing `cancel_source` on a canceled order does not by itself replay an entry. The executor must still refuse child BUY from restored provenance alone.

## 6. `process_order_canceled_event` branch order (replacement vs cancel-source)

Exact order on this head (after identity / missing-order guards and the forced `CANCELED` write):

1. **Replacement first:** `_replacement_bound_to(order_id)` → confirm reprice/repair, return.
2. **Source classification second:** `_is_confirmed_post_only_cancel` (must be `"31"`) → bind post-only repair, return.
3. **Conservative stop:** `super()` → `exchange_canceled` for single-attempt.

Needed order: classify venue source first. `"32"` (STP) and other explicit prohibitive sources take the conservative stop even if a downward reprice was already bound. `"31"` may repair. Bound identity is necessary, not sufficient.

## 7. `control_open_order` wait / deadline / cooldown

Wait states that return before `decide_down_only_maker`:

- child already authorized (goes to `_submit_reprice_child` instead)
- missing native order
- canceled (replacement or parent handling)
- downward-reprice purpose still bound (`cancel_pending`)
- `_order_awaits_exchange_update`: no exchange id, `PENDING_CREATE`, `PENDING_CANCEL`
- not OPEN / PARTIALLY_FILLED

Deadline (`total_max_age_seconds`) and cooldown (`entry_reprice_cooldown_seconds`) live in `decide_down_only_maker`, reached only for live OPEN/PARTIALLY_FILLED orders.

`_submit_reprice_child` enforces shared **count** (`_entry_reprice_count` vs `entry_max_downward_reprices`) and increments both count and `_entry_last_reprice_at` **after** a successful place. It does not enforce cooldown. `_intent_allows_price` cannot substitute: `use_intent_clock=True` hides `last_reprice_at`, and cooldown does not produce `CANCEL_TERMINAL`.

Expiry-while-waiting must request cancel / set the stop latch without treating the in-flight identity as gone.

## 8. Smallest patch approach (no implementation)

Keep trading policy: one stop latch, HOLD while submission is unresolved, repair only on confirmed OKX 31, unknown/STP/manual stay conservative, no extra BUY after stop, fills stay owned.

**A — `update_with_order_update` only**

- After identity match, if `current_state` is terminal (`CANCELED`, `FILLED`, `FAILED`) and `new_state` is a non-terminal open-ish state (`OPEN`, `PENDING_CREATE`, `PENDING_CANCEL`, `PARTIALLY_FILLED`), refuse the state assignment. Still allow `exchange_order_id` and `cancel_source` copies. Do not move `last_update_timestamp` backward on a refused regression.
- Do not change `update_with_trade_update`.
- Optional: accept a later `FILLED` after `CANCELED` if that is how OKX reports a cancel/fill race via **OrderUpdate**. The audit probes are delayed **OPEN** only; do not invent a blanket "ignore everything after cancel".
- Tests: direct `InFlightOrder` CANCELED@20 then OPEN@10; FILLED@20 then OPEN@10; PENDING_CREATE → OPEN still works. Tracker test: `stop_tracking_order` then `process_order_update` OPEN against the cached object. OKX: WS/REST helper that emits canceled then delayed live, assert tracker state stays canceled.

**B — reorder `process_order_canceled_event`**

- After forcing CANCELED and reading `cancel_source`, if source is prohibitive (`"32"` at minimum) → `super()` / terminal, `_clear_replacement`.
- Else if source is `"31"` and acquisition still permitted → post-only repair bind.
- Else if `_replacement_bound_to` and source is empty/unknown-but-not-prohibitive: keep current local-reprice confirm only if the spec still wants user-initiated cancel of a bound reprice to proceed. The audit says prohibitive venue reasons win; it does not say every unknown source must kill a bound reprice. Smallest fix that matches the failed probe: **32 (and documented prohibitive codes) beat the bound branch**. Bound + missing source can stay as today's local-reprice confirm so existing `test_confirmed_zero_fill_cancel_*` still pass.
- Test: pending downward reprice + `cancel_source="32"` + zero-fill reconcile → no child.

**C — two call sites, no new policy object**

- In `control_open_order`, before the wait `return` at `_order_awaits_exchange_update` (and the downward-reprice `cancel_pending` return if the intent clock can expire while cancel is in flight), if `_intent_age_expired()` then `_request_terminal_cancel("total_age_expired")` and return. `_request_terminal_cancel` already clears replacement, sets the stop latch, and cancels the current open order without dropping ownership.
- In `_submit_reprice_child`, after purpose/count checks and before `place_order`, if `_entry_last_reprice_at` is set and `now - last < entry_reprice_cooldown_seconds`, do not place; leave authorized or wait (`awaiting_order_update`) without clearing the stop latch incorrectly. Apply to both `downward_reprice` and `post_only_repair`.
- Tests: PENDING_CREATE @ 900s / 300s limit; repair child 1s after last reprice with cooldown 30s.

**G — one field on the existing JSON object**

- Add `"cancel_source"` to `to_json`. In `from_json`, restore if present and not blank. Keep default `None` for old snapshots.
- Test must `json.dumps` / `json.loads` the native dict, then `from_json`, assert `cancel_source == "31"`. Also assert a canceled restored order is not `is_open` (no replay).
- Do not add `cancel_source` to restart-as-entry logic.

**Do not** expand this PR's surface into PR #51 permission/capital/fee files. Shared `down_only_entry_execution.py` will still three-way-conflict with #51; this packet is the #50-side truth those conflict hunks must preserve after A/B/C/G.

## Supporting notes for implementers

- REST `_request_order_status` writes `tracked_order.cancel_source` **and** puts it on `OrderUpdate.misc_updates`. After G, both must stay consistent; the JSON field is the recorder contract, not this extra REST mutation.
- `process_order_canceled_event` forcing `current_state = CANCELED` is executor-local and does not fix blocker A.
- OKX `_okx_order_misc_updates` already stringifies any `cancelSource` (31, 32, ints). Classification belongs in the executor, not the connector parser.

## Concerns (not blocking this packet)

1. Audit probes specify delayed **OPEN** over CANCELED/FILLED. Whether a later **FILLED OrderUpdate** after CANCELED should apply is unspecified. Over-blocking that transition would hide a cancel/fill race. Keep TradeUpdates processable either way.
2. Bound reprice + **missing** cancel_source is how current tests confirm local reprice. Tightening "any non-31 kills replacement" would break those tests and change policy. Smallest B is prohibitive codes (32) first, not "31 or die".
3. No native ClientOrderTracker + OKX reorder test exists today; adding only a unit method test would leave the audit's "real tracker regression" gap open.
4. Mechanical three-way merge with #51 still has five conflict regions in this executor file. This packet does not resolve them.
